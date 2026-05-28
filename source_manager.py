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
    """Refreshes an M3U source by parsing its content and doing a fast batch insert."""
    source = db.get_source(source_id)
    if not source:
        return

    log.info("Refreshing source '%s' (%s) …", source_id, source["name"])
    # FIXED: Uses set_source_status
    db.set_source_status(source_id, "refreshing", "Reading M3U contents...")

    try:
        # Handle local files vs remote URLs correctly
        if source["type"] == "file":
            if not os.path.exists(source["location"]):
                raise FileNotFoundError(f"Local file not found at: {source['location']}")
            with open(source["location"], "r", encoding="utf-8", errors="ignore") as f:
                content = f.read()
        else:
            # Remote URL
            resp = http_requests.get(source["location"], timeout=config.M3U_REQUEST_TIMEOUT)
            resp.raise_for_status()
            content = resp.text

        # Parse M3U
        lines = content.splitlines()
        if not lines or not lines[0].startswith("#EXTM3U"):
            raise ValueError("Invalid M3U playlist format: Missing #EXTM3U header.")

        current_attrs = {}
        current_title = ""
        batch_data = []
        now_str = datetime.now(timezone.utc).isoformat()

        for line in lines:
            line = line.strip()
            if not line:
                continue
            
            if line.startswith("#EXTINF:"):
                current_title = ""
                current_attrs = {}
                comma_idx = line.find(",")
                if comma_idx != -1:
                    current_title = line[comma_idx+1:].strip()
                    meta_part = line[:comma_idx]
                else:
                    meta_part = line
                
                for m in re.finditer(r'([a-zA-Z0-9_\-]+)=\"([^\"]*)\"', meta_part):
                    current_attrs[m.group(1).lower()] = m.group(2)
                    
            elif line.startswith("#") or line.startswith("http") or line.find("/") != -1:
                if line.startswith("#"):
                    continue
                
                url = line
                url_lower = url.lower()

                # Exclude VOD Content
                if "/movie/" in url_lower or "/series/" in url_lower:
                    current_attrs = {}
                    continue

                parsed = urlparse(url)
                stream_key = os.path.basename(parsed.path) or str(hash(url))
                detected_type = _detect_stream_type(url, current_attrs.get("group-title", ""), current_attrs)

                batch_data.append((
                    source_id,
                    stream_key,
                    url,
                    current_title or stream_key,
                    current_attrs.get("group-title", "Unsorted"),
                    current_attrs.get("tvg-id", ""),
                    current_attrs.get("tvg-logo", ""),
                    detected_type,
                    now_str
                ))
                current_attrs = {}

        log.info("Dumping data via fast batch database operations for source %d...", source_id)
        
        with db.get_db() as conn:
            # Clear old entries for this source
            conn.execute("DELETE FROM channel_entries WHERE source_id = ?", (source_id,))
            
            # Fast batch insert
            conn.executemany(
                """INSERT INTO channel_entries (
                    source_id, stream_key, full_url, raw_name, 
                    raw_group, tvg_id, tvg_logo, stream_type, last_seen
                   ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                batch_data
            )

            # Update metrics inline
            conn.execute(
                """UPDATE sources 
                   SET entry_count = ?, status = 'ok', status_msg = 'Refreshed successfully.', last_refreshed_at = ?
                   WHERE id = ?""",
                (len(batch_data), now_str, source_id)
            )
            
        log.info("Successfully loaded %d entries into database.", len(batch_data))

    except Exception as exc:
        msg = str(exc)
        log.error("Source %d error: %s", source_id, msg)
        # FIXED: Make sure the error handler block also points to set_source_status!
        db.set_source_status(source_id, "error", msg)

       
def probe_source_entries(source_id: int):
    """
    Probe all entries for a source concurrently and store resolution/fps.
    """
    entries = db.list_entries(source_id=source_id)
    if not entries:
        log.info("No entries to probe", source_id)
        return

    log.info("Probing %d entries (this may take a while)…",
             source_id, len(entries))
    db.set_source_status(source_id, "probing")

    def _probe(entry):
        result = probe_stream(entry["full_url"])
        if result.ok:
            db.update_entry_probe(
                entry["id"], "ok",
                codec   = result.codec,
                width   = result.width,
                height  = result.height,
                fps     = result.fps,
                bitrate = result.bitrate,
            )
            log.info("  [entry:%d] OK  %s %dx%d@%.2ffps  %s",
                     entry["id"], result.codec, result.width, result.height,
                     result.fps, entry["raw_name"] or entry["stream_key"])
        else:
            db.update_entry_probe(entry["id"], "error", error=result.error)
            log.info("  [entry:%d] ERR %s  %s",
                     entry["id"], result.error,
                     entry["raw_name"] or entry["stream_key"])
        return result.ok

    with ThreadPoolExecutor(max_workers=config.MAX_PROBE_WORKERS) as pool:
        futures = [pool.submit(_probe, e) for e in entries]
        ok = err = 0
        for fut in as_completed(futures):
            try:
                if fut.result():
                    ok += 1
                else:
                    err += 1
            except Exception as exc:
                log.warning("Probe worker error: %s", exc)
                err += 1

    db.set_source_probe_done(source_id, ok, err)
    db.set_source_status(source_id, "ok")
    log.info("Probe complete — %d ok, %d error out of %d entries",
             source_id, ok, err, len(entries))


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