"""
strm_manager.py
Generate and maintain .strm files for VOD channel entries.

A .strm file is a plain-text file containing a single URL.
Emby/Jellyfin treat them as local video files and stream from the URL inside.

Directory layout:
    <strm_root>/
        <movies_dir>/
            <channel_name> (<year>)/
                <channel_name> (<year>).strm   ← vod_movie
        <series_dir>/
            <show_name>/
                Season XX/
                    <show_name> - SXXEXX - <episode>.strm   ← vod_series

If we can't parse season/episode from the entry name, files go into
an "Unsorted" subdirectory so Emby can still pick them up.
"""

import logging
import os
import re
import unicodedata

import db
import config

log = logging.getLogger(__name__)


# ── Filename sanitisation ─────────────────────────────────────────────────────

_UNSAFE = re.compile(r'[<>:"/\\|?*\x00-\x1f]')
_MULTI_SPACE = re.compile(r'\s+')


def _safe(name: str) -> str:
    """Return a filesystem-safe version of *name*."""
    name = unicodedata.normalize("NFKC", name)
    name = _UNSAFE.sub(" ", name)
    name = _MULTI_SPACE.sub(" ", name).strip(" .")
    return name or "Unknown"


# ── Season / episode parsing ──────────────────────────────────────────────────

_SE_RE = re.compile(
    r'[Ss](\d{1,2})[Ee](\d{1,2})'          # S01E02
    r'|[Ss]eason\s*(\d{1,2}).*?[Ee]p(?:isode)?\s*(\d{1,2})'  # Season 1 Ep 2
    r'|\b(\d{1,2})[xX](\d{1,2})\b',         # 1x02
    re.IGNORECASE,
)

_YEAR_RE = re.compile(r'\((\d{4})\)|\b(19|20)\d{2}\b')


def _parse_se(name: str) -> tuple[int | None, int | None]:
    """Return (season, episode) or (None, None) if not found."""
    m = _SE_RE.search(name)
    if not m:
        return None, None
    groups = m.groups()
    # Which pair matched?
    for i in range(0, len(groups), 2):
        if groups[i] is not None:
            return int(groups[i]), int(groups[i + 1])
    return None, None


def _parse_year(name: str) -> str | None:
    m = _YEAR_RE.search(name)
    if m:
        return m.group(1) or m.group(0)
    return None


def _strip_se(name: str) -> str:
    """Remove S01E02-style tokens from a name to get the show title."""
    name = _SE_RE.sub("", name)
    name = re.sub(r'\s*[-–]\s*$', '', name)
    return _MULTI_SPACE.sub(" ", name).strip()


# ── Path builders ─────────────────────────────────────────────────────────────

def _movie_strm_path(strm_root: str, movies_dir: str, channel_name: str,
                     entry_name: str) -> str:
    """
    <strm_root>/<movies_dir>/<Title> (<year>)/<Title> (<year>).strm
    """
    name = entry_name or channel_name
    year = _parse_year(name)
    base = _safe(re.sub(r'\(\d{4}\)', '', name).strip())
    title = f"{base} ({year})" if year else base
    return os.path.join(strm_root, movies_dir, title, f"{title}.strm")


def _series_strm_path(strm_root: str, series_dir: str, channel_name: str,
                      entry_name: str) -> str:
    """
    <strm_root>/<series_dir>/<Show>/Season XX/<Show> - SXXEXX - <Episode>.strm
    Falls back to Unsorted/<Show>/<safe_name>.strm if no S/E detected.
    """
    name  = entry_name or channel_name
    s, e  = _parse_se(name)
    show  = _safe(_strip_se(name) or channel_name)

    if s is not None and e is not None:
        season_dir = f"Season {s:02d}"
        filename   = f"{show} - S{s:02d}E{e:02d} - {_safe(name)}.strm"
        return os.path.join(strm_root, series_dir, show, season_dir, filename)
    else:
        filename = f"{_safe(name)}.strm"
        return os.path.join(strm_root, series_dir, "Unsorted", show, filename)


# ── Single-entry STRM write ───────────────────────────────────────────────────

def write_strm(entry, channel_name: str, settings: dict) -> str | None:
    """
    Write a .strm file for *entry* and return its path.
    Returns None if the entry is not VOD or settings are incomplete.
    """
    strm_root   = (settings.get("strm_root") or "").strip()
    movies_dir  = settings.get("strm_movies_dir") or "Movies"
    series_dir  = settings.get("strm_series_dir") or "Series"

    if not strm_root:
        log.warning("strm_root not set — cannot write .strm files")
        return None

    stream_type = entry["stream_type"]
    entry_name  = entry["raw_name"] or entry["stream_key"] or channel_name

    if stream_type == "vod_movie":
        path = _movie_strm_path(strm_root, movies_dir, channel_name, entry_name)
    elif stream_type == "vod_series":
        path = _series_strm_path(strm_root, series_dir, channel_name, entry_name)
    else:
        return None   # not a VOD entry

    channel_id = entry["channel_id"] or ""
    strm_url   = f"{config.BASE_URL}/vod/{channel_id}" if channel_id else entry["full_url"]

    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as fh:
        fh.write(strm_url + "\n")

    log.debug("STRM written: %s", path)
    return path


# ── Bulk generation ───────────────────────────────────────────────────────────

def generate_all_strm(channel_id: str = None) -> dict:
    """
    Generate .strm files for all (or one) channel's VOD entries.
    Returns {"written": N, "skipped": N, "errors": N}.
    """
    settings = db.get_settings()
    if settings.get("strm_enabled", "0") != "1":
        return {"written": 0, "skipped": 0, "errors": 0, "message": "STRM disabled"}

    strm_root = (settings.get("strm_root") or "").strip()
    if not strm_root:
        return {"written": 0, "skipped": 0, "errors": 0, "message": "strm_root not set"}

    if channel_id:
        channels = [db.get_channel(channel_id)]
        channels = [c for c in channels if c]
    else:
        channels = db.list_channels()

    written = skipped = errors = 0

    for ch in channels:
        entries = db.list_entries(channel_id=ch["id"])
        for entry in entries:
            if entry["stream_type"] not in ("vod_movie", "vod_series"):
                skipped += 1
                continue
            try:
                path = write_strm(entry, ch["name"], settings)
                if path:
                    written += 1
                else:
                    skipped += 1
            except Exception as exc:
                log.error("STRM write error (entry %d): %s", entry["id"], exc)
                errors += 1

    log.info("STRM generation done — written=%d skipped=%d errors=%d",
             written, skipped, errors)
    return {"written": written, "skipped": skipped, "errors": errors}


def cleanup_strm(channel_id: str = None):
    """
    Remove .strm files whose entry no longer exists or is stale.
    Walks the strm_root and removes files whose URL points to a
    channel_id that no longer has a valid VOD entry.
    """
    settings  = db.get_settings()
    strm_root = (settings.get("strm_root") or "").strip()
    if not strm_root or not os.path.isdir(strm_root):
        return {"removed": 0}

    # Build set of valid VOD channel IDs
    valid_ids = {
        ch["id"] for ch in db.list_channels()
        if db.list_entries(channel_id=ch["id"])
    }

    removed = 0
    for dirpath, _, filenames in os.walk(strm_root):
        for fname in filenames:
            if not fname.endswith(".strm"):
                continue
            fpath = os.path.join(dirpath, fname)
            try:
                with open(fpath, encoding="utf-8") as fh:
                    url = fh.read().strip()
                # Extract channel_id from URL  /vod/<channel_id>
                m = re.search(r'/vod/([^/\s]+)', url)
                if m and m.group(1) not in valid_ids:
                    os.remove(fpath)
                    removed += 1
                    log.debug("Removed stale STRM: %s", fpath)
            except Exception as exc:
                log.warning("cleanup_strm: %s — %s", fpath, exc)

    # Remove empty directories
    for dirpath, dirnames, filenames in os.walk(strm_root, topdown=False):
        if not dirnames and not filenames:
            try:
                os.rmdir(dirpath)
            except OSError:
                pass

    log.info("STRM cleanup done — removed %d files", removed)
    return {"removed": removed}
