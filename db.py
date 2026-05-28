"""db.py — SQLite schema and all database helpers."""

import sqlite3
import logging
from contextlib import contextmanager
from datetime import datetime, timezone

import config

log = logging.getLogger(__name__)

# ── Schema ────────────────────────────────────────────────────────────────────
SCHEMA = """
PRAGMA journal_mode=WAL;
PRAGMA foreign_keys=ON;

-- ── M3U sources ──────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS sources (
    id                   INTEGER PRIMARY KEY AUTOINCREMENT,
    name                 TEXT    NOT NULL UNIQUE,
    type                 TEXT    NOT NULL DEFAULT 'url',
    location             TEXT    NOT NULL,
    max_concurrent       INTEGER NOT NULL DEFAULT 2,
    refresh_interval_mins INTEGER NOT NULL DEFAULT 0,
    refresh_times        TEXT    DEFAULT '',
    stale_days           INTEGER NOT NULL DEFAULT 7,
    last_refreshed_at    TEXT,
    last_probed_at       TEXT,
    probe_ok_count       INTEGER DEFAULT 0,
    probe_err_count      INTEGER DEFAULT 0,
    entry_count          INTEGER DEFAULT 0,
    status               TEXT    DEFAULT 'idle',
    status_msg           TEXT,
    notes                TEXT,
    created_at           TEXT    NOT NULL DEFAULT (datetime('now'))
);

-- ── Raw entries parsed from sources ──────────────────────────────────────────
CREATE TABLE IF NOT EXISTS channel_entries (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    source_id     INTEGER NOT NULL REFERENCES sources(id) ON DELETE CASCADE,
    channel_id    TEXT    REFERENCES channels(id) ON DELETE SET NULL,
    -- URL identification
    stream_key    TEXT    NOT NULL,     -- last path segment, e.g. "12345.ts"
    full_url      TEXT    NOT NULL,     -- full URL at last refresh
    -- Raw M3U metadata
    raw_name      TEXT,
    raw_group     TEXT,
    tvg_id        TEXT,
    tvg_logo      TEXT,
    stream_type   TEXT    NOT NULL DEFAULT 'live',  -- 'live' | 'vod_movie' | 'vod_series'
    -- Priority (lower = tried first when multiple sources for same channel)
    priority      INTEGER NOT NULL DEFAULT 10,
    -- Probe data
    probe_status  TEXT    NOT NULL DEFAULT 'unknown',  -- unknown|ok|error
    probe_codec   TEXT,
    probe_width   INTEGER DEFAULT 0,
    probe_height  INTEGER DEFAULT 0,
    probe_fps     REAL    DEFAULT 0,
    probe_bitrate INTEGER DEFAULT 0,
    probe_error   TEXT,
    quality_score REAL    DEFAULT 0,
    probed_at     TEXT,
    -- Lifecycle
    first_seen    TEXT    NOT NULL DEFAULT (datetime('now')),
    last_seen     TEXT    NOT NULL DEFAULT (datetime('now')),
    is_stale      INTEGER NOT NULL DEFAULT 0,
    is_hidden     INTEGER NOT NULL DEFAULT 0,
    UNIQUE(source_id, stream_key)
);

-- ── Logical channels (user-curated) ──────────────────────────────────────────
CREATE TABLE IF NOT EXISTS channels (
    id            TEXT    PRIMARY KEY,     -- slug, e.g. "bbc-one"
    number        INTEGER,                 -- manual channel number
    name          TEXT    NOT NULL,
    group_title   TEXT    DEFAULT '',
    tvg_id        TEXT    DEFAULT '',
    logo_url      TEXT    DEFAULT '',      -- external URL
    logo_local    TEXT    DEFAULT '',      -- local filename under logos/
    stream_type   TEXT    NOT NULL DEFAULT 'auto',  -- 'auto'|'live'|'vod'
    notes         TEXT,
    created_at    TEXT    NOT NULL DEFAULT (datetime('now'))
);

-- ── Tags ──────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS tags (
    id    INTEGER PRIMARY KEY AUTOINCREMENT,
    name  TEXT    NOT NULL UNIQUE,
    color TEXT    NOT NULL DEFAULT '#6c757d'
);

CREATE TABLE IF NOT EXISTS channel_tags (
    channel_id TEXT    NOT NULL REFERENCES channels(id) ON DELETE CASCADE,
    tag_id     INTEGER NOT NULL REFERENCES tags(id)     ON DELETE CASCADE,
    PRIMARY KEY (channel_id, tag_id)
);

-- ── Active streams (one ffmpeg process per channel) ───────────────────────────
CREATE TABLE IF NOT EXISTS streams (
    id             TEXT    PRIMARY KEY,
    channel_id     TEXT    REFERENCES channels(id) ON DELETE SET NULL,
    entry_id       INTEGER REFERENCES channel_entries(id) ON DELETE SET NULL,
    source_id      INTEGER REFERENCES sources(id)  ON DELETE SET NULL,
    channel_name   TEXT,
    stream_key     TEXT,
    stream_type    TEXT    NOT NULL DEFAULT 'live',
    source_url     TEXT    NOT NULL,
    status         TEXT    NOT NULL DEFAULT 'starting',
    -- starting | active | completed | error | stopped | falling_back
    output_dir     TEXT,
    ffmpeg_pid     INTEGER,
    viewer_count   INTEGER NOT NULL DEFAULT 0,
    -- Probe data captured at stream start
    res_width      INTEGER DEFAULT 0,
    res_height     INTEGER DEFAULT 0,
    fps            REAL    DEFAULT 0,
    codec          TEXT,
    bitrate        INTEGER DEFAULT 0,
    started_at     TEXT,
    last_heartbeat TEXT,
    error_msg      TEXT
);

-- ── Per-channel: which stream is currently active ─────────────────────────────
CREATE TABLE IF NOT EXISTS channel_active (
    channel_id TEXT    PRIMARY KEY REFERENCES channels(id) ON DELETE CASCADE,
    stream_id  TEXT    NOT NULL,
    updated_at TEXT    NOT NULL DEFAULT (datetime('now'))
);

-- ── Viewer sessions ───────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS sessions (
    id          TEXT    PRIMARY KEY,
    channel_id  TEXT    REFERENCES channels(id) ON DELETE CASCADE,
    stream_id   TEXT    REFERENCES streams(id)  ON DELETE SET NULL,
    client_ip   TEXT,
    created_at  TEXT    NOT NULL DEFAULT (datetime('now')),
    last_seen   TEXT    NOT NULL DEFAULT (datetime('now'))
);

-- ── App-wide settings (key/value) ────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS settings (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL DEFAULT ''
);

-- Defaults inserted once
INSERT OR IGNORE INTO settings (key, value) VALUES
  ('m3u_include_vod',  '1'),   -- include VOD entries in /api/m3u output
  ('strm_enabled',     '0'),   -- generate .strm files for VOD entries
  ('strm_root',        ''),    -- root directory for .strm files
  ('strm_movies_dir',  'Movies'),   -- subdir under strm_root for vod_movie
  ('strm_series_dir',  'Series');   -- subdir under strm_root for vod_series


CREATE INDEX IF NOT EXISTS idx_entries_channel   ON channel_entries(channel_id);
CREATE INDEX IF NOT EXISTS idx_entries_key       ON channel_entries(source_id, stream_key);
CREATE INDEX IF NOT EXISTS idx_entries_quality   ON channel_entries(channel_id, quality_score DESC);
CREATE INDEX IF NOT EXISTS idx_streams_channel   ON streams(channel_id);
CREATE INDEX IF NOT EXISTS idx_streams_status    ON streams(status);
CREATE INDEX IF NOT EXISTS idx_sessions_channel  ON sessions(channel_id);
"""

# ── Connection ────────────────────────────────────────────────────────────────

@contextmanager
def get_db():
    conn = sqlite3.connect(config.DB_PATH, timeout=15)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys=ON")
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def init_db():
    with get_db() as conn:
        conn.executescript(SCHEMA)
        # Migrate existing databases — add columns if they don't exist yet
        _migrate(conn)
    log.info("Database initialised: %s", config.DB_PATH)


def _migrate(conn):
    """Add new columns to existing tables without dropping data."""
    migrations = [
        ("sources", "last_probed_at",   "ALTER TABLE sources ADD COLUMN last_probed_at TEXT"),
        ("sources", "probe_ok_count",   "ALTER TABLE sources ADD COLUMN probe_ok_count INTEGER DEFAULT 0"),
        ("sources", "probe_err_count",  "ALTER TABLE sources ADD COLUMN probe_err_count INTEGER DEFAULT 0"),
    ]
    existing = {}
    for table, _, _ in migrations:
        if table not in existing:
            cols = {row[1] for row in conn.execute(f"PRAGMA table_info({table})")}
            existing[table] = cols
    for table, col, sql in migrations:
        if col not in existing.get(table, set()):
            try:
                conn.execute(sql)
                log.info("Migration: added %s.%s", table, col)
            except Exception as e:
                log.debug("Migration skip %s.%s: %s", table, col, e)


def _now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


def get_settings() -> dict:
    with get_db() as conn:
        rows = conn.execute("SELECT key, value FROM settings").fetchall()
        return {r["key"]: r["value"] for r in rows}


def set_setting(key: str, value: str):
    with get_db() as conn:
        conn.execute(
            "INSERT INTO settings (key,value) VALUES (?,?) "
            "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            (key, str(value)),
        )


# ── Sources ───────────────────────────────────────────────────────────────────

def add_source(name, type_, location, max_concurrent=2,
               refresh_interval_mins=0, refresh_times="",
               stale_days=7, notes="") -> int:
    with get_db() as conn:
        cur = conn.execute(
            """INSERT INTO sources
               (name,type,location,max_concurrent,refresh_interval_mins,
                refresh_times,stale_days,notes)
               VALUES (?,?,?,?,?,?,?,?)""",
            (name, type_, location, max_concurrent,
             refresh_interval_mins, refresh_times, stale_days, notes),
        )
        return cur.lastrowid


def update_source(id_, **kwargs):
    allowed = {"name","type","location","max_concurrent","refresh_interval_mins",
               "refresh_times","stale_days","notes"}
    fields  = {k: v for k, v in kwargs.items() if k in allowed}
    if not fields:
        return
    sets = ", ".join(f"{k}=?" for k in fields)
    with get_db() as conn:
        conn.execute(f"UPDATE sources SET {sets} WHERE id=?",
                     [*fields.values(), id_])


def delete_source(id_):
    with get_db() as conn:
        conn.execute("DELETE FROM sources WHERE id=?", (id_,))


def get_source(id_):
    with get_db() as conn:
        return conn.execute("SELECT * FROM sources WHERE id=?", (id_,)).fetchone()


def list_sources():
    with get_db() as conn:
        return conn.execute("SELECT * FROM sources ORDER BY name").fetchall()


def set_source_status(id_, status, msg=""):
    with get_db() as conn:
        conn.execute(
            "UPDATE sources SET status=?, status_msg=? WHERE id=?",
            (status, msg or None, id_),
        )


def set_source_refreshed(id_, entry_count: int):
    with get_db() as conn:
        conn.execute(
            "UPDATE sources SET last_refreshed_at=?, entry_count=?, status='ok', status_msg=NULL WHERE id=?",
            (_now(), entry_count, id_),
        )


def set_source_probe_done(id_: int, ok: int, err: int):
    with get_db() as conn:
        conn.execute(
            "UPDATE sources SET last_probed_at=?, probe_ok_count=?, probe_err_count=? WHERE id=?",
            (_now(), ok, err, id_),
        )


def get_source_active_count(source_id: int) -> int:
    with get_db() as conn:
        row = conn.execute(
            "SELECT COUNT(*) AS cnt FROM streams s "
            "JOIN channel_entries e ON e.id=s.entry_id "
            "WHERE e.source_id=? AND s.status IN ('starting','active')",
            (source_id,),
        ).fetchone()
        return row["cnt"]


# ── Channel entries ───────────────────────────────────────────────────────────

def upsert_entry(source_id: int, stream_key: str, full_url: str,
                 raw_name="", raw_group="", tvg_id="", tvg_logo="",
                 stream_type="live") -> int:
    """
    Insert or update a channel entry matched by (source_id, stream_key).
    Returns the row id.
    """
    with get_db() as conn:
        conn.execute(
            """INSERT INTO channel_entries
               (source_id, stream_key, full_url, raw_name, raw_group,
                tvg_id, tvg_logo, stream_type, last_seen)
               VALUES (?,?,?,?,?,?,?,?,?)
               ON CONFLICT(source_id, stream_key) DO UPDATE SET
                 full_url=excluded.full_url,
                 raw_name=excluded.raw_name,
                 raw_group=excluded.raw_group,
                 tvg_id=excluded.tvg_id,
                 tvg_logo=excluded.tvg_logo,
                 stream_type=excluded.stream_type,
                 last_seen=excluded.last_seen,
                 is_stale=0""",
            (source_id, stream_key, full_url, raw_name, raw_group,
             tvg_id or None, tvg_logo or None, stream_type, _now()),
        )
        row = conn.execute(
            "SELECT id FROM channel_entries WHERE source_id=? AND stream_key=?",
            (source_id, stream_key),
        ).fetchone()
        return row["id"]


def mark_stale_entries(source_id: int, stale_days: int):
    """Mark entries for source that haven't been seen for stale_days."""
    with get_db() as conn:
        conn.execute(
            """UPDATE channel_entries SET is_stale=1
               WHERE source_id=?
               AND (julianday('now') - julianday(last_seen)) > ?""",
            (source_id, stale_days),
        )


def delete_stale_entries(source_id: int):
    with get_db() as conn:
        conn.execute(
            "DELETE FROM channel_entries WHERE source_id=? AND is_stale=1",
            (source_id,),
        )


def get_entry(entry_id: int):
    with get_db() as conn:
        return conn.execute(
            """SELECT e.*, s.name AS source_name, s.max_concurrent
               FROM channel_entries e
               JOIN sources s ON s.id=e.source_id
               WHERE e.id=?""",
            (entry_id,),
        ).fetchone()


def list_entries(source_id: int = None, channel_id: str = None, unlinked: bool = False):
    clauses, params = [], []
    if source_id is not None:
        clauses.append("e.source_id=?"); params.append(source_id)
    if channel_id is not None:
        clauses.append("e.channel_id=?"); params.append(channel_id)
    if unlinked:
        clauses.append("e.channel_id IS NULL")
    where = ("WHERE " + " AND ".join(clauses)) if clauses else ""
    with get_db() as conn:
        return conn.execute(
            f"""SELECT e.*, s.name AS source_name
                FROM channel_entries e
                JOIN sources s ON s.id=e.source_id
                {where}
                ORDER BY e.quality_score DESC, e.raw_name""",
            params,
        ).fetchall()


def link_entry_to_channel(entry_id: int, channel_id: str):
    with get_db() as conn:
        conn.execute(
            "UPDATE channel_entries SET channel_id=? WHERE id=?",
            (channel_id, entry_id),
        )


def unlink_entry(entry_id: int):
    with get_db() as conn:
        conn.execute(
            "UPDATE channel_entries SET channel_id=NULL WHERE id=?",
            (entry_id,),
        )


def set_entry_priority(entry_id: int, priority: int):
    with get_db() as conn:
        conn.execute(
            "UPDATE channel_entries SET priority=? WHERE id=?",
            (priority, entry_id),
        )


def update_entry_probe(entry_id: int, probe_status: str,
                       codec="", width=0, height=0,
                       fps=0.0, bitrate=0, error=""):
    score = float(width * height) * (fps or 1.0) if probe_status == "ok" else 0.0
    with get_db() as conn:
        conn.execute(
            """UPDATE channel_entries SET
               probe_status=?, probe_codec=?, probe_width=?, probe_height=?,
               probe_fps=?, probe_bitrate=?, probe_error=?,
               quality_score=?, probed_at=?
               WHERE id=?""",
            (probe_status, codec, width, height, fps, bitrate, error or None,
             score, _now(), entry_id),
        )

def get_best_entries_for_channel(channel_id: str):
    """Fetches linked stream configurations prioritised by quality and user rules."""
    with get_db() as conn:
        return conn.execute(
            """SELECT e.*, s.name AS source_name 
               FROM channel_entries e
               JOIN sources s ON e.source_id = s.id
               WHERE e.channel_id = ? 
                 AND (e.probe_status = 'ok' OR e.probe_status = 'unknown')
               ORDER BY e.priority ASC, e.quality_score DESC""",
            (channel_id,)
        ).fetchall()

def auto_link_by_tvg_id():
    """
    Link unlinked entries to channels where tvg_id matches.
    Returns count of newly linked entries.
    """
    with get_db() as conn:
        cur = conn.execute(
            """UPDATE channel_entries SET channel_id=(
               SELECT id FROM channels WHERE channels.tvg_id=channel_entries.tvg_id
               LIMIT 1)
               WHERE channel_id IS NULL
               AND tvg_id IS NOT NULL
               AND tvg_id != ''
               AND EXISTS(SELECT 1 FROM channels WHERE channels.tvg_id=channel_entries.tvg_id)"""
        )
        return cur.rowcount


# ── Channels ──────────────────────────────────────────────────────────────────

def upsert_channel(id_, name, number=None, group_title="",
                   tvg_id="", logo_url="", stream_type="auto", notes=""):
    with get_db() as conn:
        conn.execute(
            """INSERT INTO channels (id,name,number,group_title,tvg_id,logo_url,stream_type,notes)
               VALUES (?,?,?,?,?,?,?,?)
               ON CONFLICT(id) DO UPDATE SET
                 name=excluded.name, number=excluded.number,
                 group_title=excluded.group_title, tvg_id=excluded.tvg_id,
                 logo_url=excluded.logo_url, stream_type=excluded.stream_type,
                 notes=excluded.notes""",
            (id_, name, number, group_title, tvg_id, logo_url, stream_type, notes),
        )


def get_channel(channel_id: str):
    with get_db() as conn:
        return conn.execute("SELECT * FROM channels WHERE id=?", (channel_id,)).fetchone()


def delete_channel(channel_id: str):
    with get_db() as conn:
        conn.execute("DELETE FROM channels WHERE id=?", (channel_id,))


def list_channels(group: str = None, tag: str = None):
    clauses, params = [], []
    if group:
        clauses.append("c.group_title=?"); params.append(group)
    if tag:
        clauses.append(
            "EXISTS(SELECT 1 FROM channel_tags ct JOIN tags t ON t.id=ct.tag_id "
            "WHERE ct.channel_id=c.id AND t.name=?)"
        )
        params.append(tag)
    where = ("WHERE " + " AND ".join(clauses)) if clauses else ""
    with get_db() as conn:
        return conn.execute(
            f"""SELECT c.*,
                  COUNT(DISTINCT e.id) AS source_count,
                  SUM(e.probe_status='ok') AS sources_ok,
                  MAX(e.quality_score) AS best_score,
                  MAX(e.probe_width) AS best_width,
                  MAX(e.probe_height) AS best_height,
                  MAX(e.probe_fps) AS best_fps,
                  GROUP_CONCAT(DISTINCT t.name) AS tag_names,
                  (SELECT e2.stream_type FROM channel_entries e2
                   WHERE e2.channel_id=c.id AND e2.is_stale=0
                   ORDER BY e2.quality_score DESC LIMIT 1
                  ) AS best_stream_type
                FROM channels c
                LEFT JOIN channel_entries e ON e.channel_id=c.id AND e.is_stale=0
                LEFT JOIN channel_tags ct ON ct.channel_id=c.id
                LEFT JOIN tags t ON t.id=ct.tag_id
                {where}
                GROUP BY c.id
                ORDER BY c.number IS NULL, c.number, c.name""",
            params,
        ).fetchall()


def list_groups():
    with get_db() as conn:
        return [r[0] for r in conn.execute(
            "SELECT DISTINCT group_title FROM channels WHERE group_title!='' ORDER BY group_title"
        ).fetchall()]


def set_channel_logo_local(channel_id: str, filename: str):
    with get_db() as conn:
        conn.execute(
            "UPDATE channels SET logo_local=? WHERE id=?",
            (filename, channel_id),
        )


# ── Tags ──────────────────────────────────────────────────────────────────────

def add_tag(name: str, color: str = "#6c757d") -> int:
    with get_db() as conn:
        cur = conn.execute(
            "INSERT OR IGNORE INTO tags (name,color) VALUES (?,?)",
            (name, color),
        )
        if cur.lastrowid:
            return cur.lastrowid
        return conn.execute("SELECT id FROM tags WHERE name=?", (name,)).fetchone()["id"]


def list_tags():
    with get_db() as conn:
        return conn.execute("SELECT * FROM tags ORDER BY name").fetchall()


def get_channel_tags(channel_id: str):
    with get_db() as conn:
        return conn.execute(
            """SELECT t.* FROM tags t
               JOIN channel_tags ct ON ct.tag_id=t.id
               WHERE ct.channel_id=?""",
            (channel_id,),
        ).fetchall()


def set_channel_tags(channel_id: str, tag_ids: list):
    with get_db() as conn:
        conn.execute("DELETE FROM channel_tags WHERE channel_id=?", (channel_id,))
        for tid in tag_ids:
            conn.execute(
                "INSERT OR IGNORE INTO channel_tags (channel_id,tag_id) VALUES (?,?)",
                (channel_id, tid),
            )


# ── Streams ───────────────────────────────────────────────────────────────────

def create_stream(stream_id, channel_id, entry_id, source_id,
                  channel_name, stream_key, source_url, stream_type,
                  output_dir):
    now = _now()
    with get_db() as conn:
        conn.execute(
            """INSERT INTO streams
               (id,channel_id,entry_id,source_id,channel_name,
                stream_key,stream_type,source_url,status,output_dir,
                started_at,last_heartbeat)
               VALUES (?,?,?,?,?,?,?,?,'starting',?,?,?)""",
            (stream_id, channel_id, entry_id, source_id, channel_name,
             stream_key, stream_type, source_url, output_dir, now, now),
        )


def set_stream_active(stream_id, ffmpeg_pid,
                      width=0, height=0, fps=0.0, codec="", bitrate=0):
    with get_db() as conn:
        conn.execute(
            """UPDATE streams SET status='active', ffmpeg_pid=?,
               res_width=?, res_height=?, fps=?, codec=?, bitrate=?,
               last_heartbeat=? WHERE id=?""",
            (ffmpeg_pid, width, height, fps, codec, bitrate, _now(), stream_id),
        )


def set_stream_error(stream_id, msg=""):
    with get_db() as conn:
        conn.execute(
            "UPDATE streams SET status='error', ffmpeg_pid=NULL, error_msg=? WHERE id=?",
            (msg or None, stream_id),
        )


def set_stream_completed(stream_id):
    with get_db() as conn:
        conn.execute(
            "UPDATE streams SET status='completed', ffmpeg_pid=NULL WHERE id=?",
            (stream_id,),
        )


def set_stream_stopped(stream_id):
    with get_db() as conn:
        conn.execute(
            "UPDATE streams SET status='stopped', ffmpeg_pid=NULL WHERE id=?",
            (stream_id,),
        )


def heartbeat_stream(stream_id):
    with get_db() as conn:
        conn.execute(
            "UPDATE streams SET last_heartbeat=? WHERE id=?",
            (_now(), stream_id),
        )


def get_stream(stream_id):
    with get_db() as conn:
        return conn.execute("SELECT * FROM streams WHERE id=?", (stream_id,)).fetchone()


def list_active_streams():
    with get_db() as conn:
        return conn.execute(
            """SELECT s.*, src.name AS source_name
               FROM streams s
               LEFT JOIN sources src ON src.id=s.source_id
               WHERE s.status IN ('starting','active')
               ORDER BY s.started_at""",
        ).fetchall()


def get_stale_streams(ttl_seconds: int):
    with get_db() as conn:
        return conn.execute(
            """SELECT * FROM streams
               WHERE status IN ('starting','active')
               AND NOT (stream_type IN ('vod_movie','vod_series') AND ffmpeg_pid IS NOT NULL)
               AND (julianday('now') - julianday(last_heartbeat)) * 86400 > ?""",
            (ttl_seconds,),
        ).fetchall()


def get_cached_vod(channel_id: str, stream_key: str):
    with get_db() as conn:
        return conn.execute(
            """SELECT * FROM streams
               WHERE channel_id=? AND stream_key=? AND stream_type IN ('vod_movie','vod_series')
               AND status='completed'
               ORDER BY started_at DESC LIMIT 1""",
            (channel_id, stream_key),
        ).fetchone()


def get_stream_stats():
    with get_db() as conn:
        return conn.execute(
            """SELECT
                 COUNT(*) FILTER (WHERE status='active') AS active,
                 SUM(viewer_count) FILTER (WHERE status='active') AS viewers,
                 COUNT(*) FILTER (WHERE status='completed') AS completed_vod
               FROM streams"""
        ).fetchone()


# ── Channel active mapping ────────────────────────────────────────────────────

def set_channel_active(channel_id: str, stream_id: str):
    with get_db() as conn:
        conn.execute(
            """INSERT INTO channel_active (channel_id, stream_id, updated_at)
               VALUES (?,?,?)
               ON CONFLICT(channel_id) DO UPDATE SET
                 stream_id=excluded.stream_id, updated_at=excluded.updated_at""",
            (channel_id, stream_id, _now()),
        )


def get_channel_active(channel_id: str):
    with get_db() as conn:
        return conn.execute(
            "SELECT * FROM channel_active WHERE channel_id=?",
            (channel_id,),
        ).fetchone()


def clear_channel_active(channel_id: str):
    with get_db() as conn:
        conn.execute("DELETE FROM channel_active WHERE channel_id=?", (channel_id,))


# ── Sessions ──────────────────────────────────────────────────────────────────

def create_session(session_id, channel_id, stream_id, client_ip=""):
    with get_db() as conn:
        conn.execute(
            "INSERT INTO sessions (id,channel_id,stream_id,client_ip) VALUES (?,?,?,?)",
            (session_id, channel_id, stream_id, client_ip),
        )
    _sync_viewer_count(stream_id)


def touch_session(session_id):
    with get_db() as conn:
        conn.execute(
            "UPDATE sessions SET last_seen=? WHERE id=?",
            (_now(), session_id),
        )


def delete_session(session_id) -> tuple:
    with get_db() as conn:
        row = conn.execute(
            "SELECT channel_id, stream_id FROM sessions WHERE id=?",
            (session_id,),
        ).fetchone()
        if not row:
            return None, None
        conn.execute("DELETE FROM sessions WHERE id=?", (session_id,))
    _sync_viewer_count(row["stream_id"])
    return row["channel_id"], row["stream_id"]


def count_channel_viewers(channel_id: str) -> int:
    with get_db() as conn:
        row = conn.execute(
            "SELECT COUNT(*) AS cnt FROM sessions WHERE channel_id=?",
            (channel_id,),
        ).fetchone()
        return row["cnt"]


def _sync_viewer_count(stream_id):
    if not stream_id:
        return
    with get_db() as conn:
        conn.execute(
            """UPDATE streams SET viewer_count=
               (SELECT COUNT(*) FROM sessions WHERE stream_id=?)
               WHERE id=?""",
            (stream_id, stream_id),
        )


def update_session_stream(channel_id: str, new_stream_id: str):
    """Redirect all active sessions for a channel to a new stream (fallback)."""
    with get_db() as conn:
        conn.execute(
            "UPDATE sessions SET stream_id=? WHERE channel_id=?",
            (new_stream_id, channel_id),
        )
    _sync_viewer_count(new_stream_id)

def update_channel(channel_id: str, name: str, number: int = None, group_title: str = "", tvg_id: str = "", logo_url: str = "", notes: str = ""):
    """Updates core metadata attributes for a structural channel."""
    with get_db() as conn:
        conn.execute(
            """UPDATE channels 
               SET name=?, number=?, group_title=?, tvg_id=?, logo_url=?, notes=?
               WHERE id=?""",
            (name, number, group_title, tvg_id, logo_url, notes, channel_id)
        )

def update_channel_stream_type(channel_id: str, stream_type: str):
    """Sets explicit stream type routing overrule options ('auto', 'live', 'vod_movie', etc)."""
    with get_db() as conn:
        conn.execute(
            "UPDATE channels SET stream_type=? WHERE id=?",
            (stream_type, channel_id)
        )

def toggle_entry_visibility(entry_id: int, is_hidden: bool):
    """Toggles whether an entry is hidden from standard mapping views."""
    with get_db() as conn:
        conn.execute(
            "UPDATE channel_entries SET is_hidden = ? WHERE id = ?",
            (1 if is_hidden else 0, entry_id)
        )

def list_entries(channel_id=None, unlinked=False, include_hidden=False):
    """Fetches raw entries, optionally filtered by status parameters."""
    clauses = []
    params = []

    if channel_id:
        clauses.append("channel_id = ?")
        params.append(channel_id)
    if unlinked:
        clauses.append("channel_id IS NULL")
    if not include_hidden:
        clauses.append("is_hidden = 0")

    where_str = f"WHERE {' AND '.join(clauses)}" if clauses else ""

    with get_db() as conn:
        return conn.execute(
            f"""SELECT e.*, s.name AS source_name 
               FROM channel_entries e
               JOIN sources s ON e.source_id = s.id
               {where_str}
               ORDER BY e.raw_name ASC""", 
            params
        ).fetchall()