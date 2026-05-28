"""
source_manager.py
M3U parsing, source refresh scheduling (APScheduler), stale entry cleanup.
"""

import logging
import os
import re
import io
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import PurePosixPath
from urllib.parse import urlparse
from datetime import datetime, timezone
import requests as http_requests

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger

import config
import db
from stream_manager import probe_stream

log = logging.getLogger(__name__)

_scheduler: BackgroundScheduler | None = None

# ── VOD detection helpers ─────────────────────────────────────────────────────

def _detect_stream_type(url: str, group: str, extinf_attrs: dict) -> str:
    """
    Return 'live', 'vod_movie', or 'vod_series'.

    Priority order:
      1. /movie/ or /series/ in the URL path  (most reliable)
      2. Explicit type="vod" EXTINF attribute
      3. catchup attribute present
      4. URL file extension (.mp4, .mkv, …)
      5. Group-title keywords
    """
    url_lower = url.lower()

    # 1. Path segment — definitive
    for seg in config.VOD_MOVIE_PATH_SEGS:
        if seg in url_lower:
            return "vod_movie"
    for seg in config.VOD_SERIES_PATH_SEGS:
        if seg in url_lower:
            return "vod_series"

    # 2. Explicit EXTINF attribute
    if extinf_attrs.get("type", "").lower() == "vod":
        # Try to sub-classify from group name
        g = (group or "").lower()
        if any(kw in g for kw in ("series", "show", "episode", "tv")):
            return "vod_series"
        return "vod_movie"

    # 3. Catchup marker
    if "catchup" in extinf_attrs and extinf_attrs.get("catchup-days"):
        return "vod_series"   # catchup is typically episodic

    # 4. File extension
    ext = PurePosixPath(urlparse(url).path).suffix.lower()
    if ext in config.VOD_URL_EXTENSIONS:
        g = (group or "").lower()
        if any(kw in g for kw in ("series", "show", "episode", "tv")):
            return "vod_series"
        return "vod_movie"

    # 5. Group-title keywords
    g = (group or "").lower()
    if any(kw in g for kw in ("series", "show", "episode")):
        return "vod_series"
    if any(kw in g for kw in config.VOD_GROUP_KEYWORDS):
        return "vod_movie"

    return "live"


def _stream_key(url: str) -> str:
    """Return the last path segment of a URL, e.g. '12345.ts'."""
    path = urlparse(url).path
    return PurePosixPath(path).name or path.lstrip("/").replace("/", "_")


# ── High-Speed M3U parser ─────────────────────────────────────────────────────

_ATTR_RE = re.compile(r'([\w-]+)="([^"]*)"')
_EXTINF_NAME_RE = re.compile(r',(.+)$')


def parse_m3u(text: str) -> list[dict]:
    """
    Parse an M3U playlist string using memory-efficient line iteration.
    Returns a list of dicts with keys:
        url, name, group, tvg_id, tvg_logo, stream_type, stream_key
    """
    entries = []
    
    # Wrap text in a StringIO stream to process line-by-line without splitting all text at once
    file_stream = io.StringIO(text)
    pending: dict | None = None

    for line in file_stream:
        line = line.strip()
        if not line or line == "#EXTM3U":
            continue

        if line.startswith("#EXTINF"):
            attrs = dict(_ATTR_RE.findall(line))
            name_match = _EXTINF_NAME_RE.search(line)
            name = name_match.group(1).strip() if name_match else ""
            pending = {
                "name":     name,
                "group":    attrs.get("group-title", ""),
                "tvg_id":   attrs.get("tvg-id", attrs.get("tvg_id", "")),
                "tvg_logo": attrs.get("tvg-logo", attrs.get("tvg_logo", "")),
                "_attrs":   attrs,
            }

        elif not line.startswith("#") and pending is not None:
            url = line
            stream_type = _detect_stream_type(
                url, pending["group"], pending["_attrs"]
            )
            entries.append({
                "url":         url,
                "name":        pending["name"],
                "group":       pending["group"],
                "tvg_id":      pending["tvg_id"],
                "tvg_logo":    pending["tvg_logo"],
                "stream_type": stream_type,
                "stream_key":  _stream_key(url),
            })
            pending = None

    return entries

def _fetch_m3u(source: db.sqlite3.Row) -> str:
    """Fetch M3U content efficiently using chunked streaming over HTTP requests."""
    if source["type"] == "file":
        with open(source["location"], encoding="utf-8", errors="replace") as fh:
            return fh.read()
    else:
        import requests
        
        headers = {
            "User-Agent": "Mozilla/5.0 (Linux; Android 9; AFTSS) AppleWebKit/537.36 (KHTML, like Gecko) Silk/112.5.1 like Chrome/112.0.5615.213 Safari/537.36",
            "Accept": "*/*",
            "Accept-Encoding": "gzip, deflate",
            "Connection": "keep-alive"
        }
        
        try:
            r = requests.get(
                source["location"], 
                headers=headers, 
                timeout=config.M3U_REQUEST_TIMEOUT,
                stream=True
            )
            r.raise_for_status()
            
            # Use iter_lines with automatic string decoding 
            buffer = io.StringIO()
            for line in r.iter_lines():
                if line is not None:
                    buffer.write(line.decode("utf-8", errors="replace") + "\n")
            return buffer.getvalue()
            
        except Exception as e:
            log.error("M3U HTTP requests fetch failed")
            raise

# ── Refresh logic ─────────────────────────────────────────────────────────────
def refresh_source(source_id: int):
    """
    Worker job executed by APScheduler or manual UI dispatch.
    Downloads/reads the raw M3U playlist file content, processes its tracks,
    performs an atomic batch upsert operation, flags missing entries as stale,
    and retires ancient entries beyond the source's retention period.
    """
    log.info("Refreshing source ID %d ...", source_id)
    source = db.get_source(source_id)
    if not source:
        log.error("Cannot refresh source %d: Source records missing from DB.", source_id)
        return

    try:
        db.set_source_status(source_id, "refreshing")
        location = source["location"]
        content = ""

        # 1. Ingest content from either a remote HTTP URL endpoint or a local directory file
        if location.startswith("http://") or location.startswith("https://"):
            headers = {}
            # Mirror the Android User Agent masquerade string configuration if present
            if hasattr(config, "FFMPEG_ANDROID_HEADERS"):
                # Clean up carriage returns for standard requests dictionary ingestion
                for line in config.FFMPEG_ANDROID_HEADERS.split("\r\n"):
                    if ":" in line:
                        k, v = line.split(":", 1)
                        headers[k.strip()] = v.strip()

            timeout = getattr(config, "M3U_REQUEST_TIMEOUT", 30)
            res = http_requests.get(location, headers=headers, timeout=timeout)
            res.raise_for_status()
            content = res.text
        else:
            if not os.path.isfile(location):
                raise FileNotFoundError(f"Local playlist asset location not found: {location}")
            with open(location, "r", encoding="utf-8", errors="ignore") as f:
                content = f.read()

        # 2. Parse streams line-by-line out of the raw text blob file payload
        parsed_tracks = parse_m3u(content)
        
        # 3. In-Memory Deduplication: Protects SQLite constraint structures
        # If the provider's playlist accidentally lists the same stream multiple times in a single dump,
        # we isolate and preserve only the first appearance of that key.
        entries_to_save = []
        seen_keys = set()
        
        for track in parsed_tracks:
            sk = track.get("stream_key")
            if not sk:
                continue
            if sk in seen_keys:
                continue
            seen_keys.add(sk)
            entries_to_save.append(track)

        log.info("Dumping %d unique tracks via fast batch database operations for source %d...", len(entries_to_save), source_id)

        # 4. Perform High-Speed Atomic Upsert (Insert new, Update existing changed attributes)
        db.save_source_entries_batch(source_id, entries_to_save)

        # 5. Retention Tracking Matrix: Identify items skipped / dropped during this sync run
        stale_days = source["stale_days"] if source["stale_days"] else getattr(config, "DEFAULT_STALE_DAYS", 7)
        db.mark_stale_entries(source_id, stale_days)

        # 6. Purge Cycle: Permanently drop records that have exceeded their retention life threshold limits
        db.delete_stale_entries(source_id)

        # 7. Finalize source properties counters cleanly
        db.set_source_refreshed(source_id, len(entries_to_save))
        log.info("Source %d refresh cycle completed successfully.", source_id)

    except Exception as e:
        log.error("Source %d error: %s", source_id, e)
        db.set_source_status(source_id, "error", str(e))


def probe_channel_entries(channel_id: str):
    """Probe all entries linked to a logical channel."""
    entries = db.list_entries(channel_id=channel_id)
    log.info("[channel:%s] Probing %d entries …", channel_id, len(entries))

    def _probe(entry):
        result = probe_stream(entry["full_url"])
        if result.ok:
            db.update_entry_probe(entry["id"], "ok",
                codec=result.codec, width=result.width,
                height=result.height, fps=result.fps, bitrate=result.bitrate)
        else:
            db.update_entry_probe(entry["id"], "error", error=result.error)
        return result

    with ThreadPoolExecutor(max_workers=config.MAX_PROBE_WORKERS) as pool:
        list(pool.map(_probe, entries))


# ── APScheduler ───────────────────────────────────────────────────────────────

def _job_id(source_id: int, suffix: str = "") -> str:
    return f"source_{source_id}{suffix}"


def schedule_source(source_id: int):
    """Add or replace scheduler jobs for a source based on its settings."""
    if _scheduler is None:
        return
    source = db.get_source(source_id)
    if not source:
        return

    # Remove existing jobs for this source
    for job in _scheduler.get_jobs():
        if job.id.startswith(f"source_{source_id}"):
            job.remove()

    # Interval job
    mins = source["refresh_interval_mins"]
    if mins and mins > 0:
        _scheduler.add_job(
            refresh_source,
            IntervalTrigger(minutes=mins),
            args=[source_id],
            id=_job_id(source_id, "_interval"),
            replace_existing=True,
        )
        log.info("Scheduled refresh every %d min", source_id, mins)

    # Daily time-based jobs
    times = [t.strip() for t in (source["refresh_times"] or "").split(",") if t.strip()]
    for i, t in enumerate(times):
        try:
            hour, minute = map(int, t.split(":"))
            _scheduler.add_job(
                refresh_source,
                CronTrigger(hour=hour, minute=minute),
                args=[source_id],
                id=_job_id(source_id, f"_cron{i}"),
                replace_existing=True,
            )
            log.info("Scheduled daily refresh at %02d:%02d", source_id, hour, minute)
        except ValueError:
            log.warning("Invalid refresh time %r", source_id, t)


def unschedule_source(source_id: int):
    if _scheduler is None:
        return
    for job in list(_scheduler.get_jobs()):
        if job.id.startswith(f"source_{source_id}"):
            job.remove()


def start_scheduler():
    global _scheduler
    _scheduler = BackgroundScheduler(daemon=True)
    _scheduler.start()

    # Schedule all existing sources
    for source in db.list_sources():
        schedule_source(source["id"])

    log.info("APScheduler started")


def stop_scheduler():
    if _scheduler and _scheduler.running:
        _scheduler.shutdown(wait=False)