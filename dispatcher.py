"""
dispatcher.py — Flask application, stream lifecycle, fallback, API and UI.

Channel proxy URLs (stable, give these to Emby):
  /live/<channel_id>/index.m3u8   — proxied HLS playlist
  /live/<channel_id>/seg/<file>   — proxied HLS segment

API:
  GET/POST  /api/sources
  GET/PUT/DELETE /api/sources/<id>
  POST      /api/sources/<id>/refresh
  POST      /api/sources/<id>/probe
  GET/POST  /api/channels
  GET/PUT/DELETE /api/channels/<id>
  POST      /api/channels/<id>/probe
  POST      /api/channels/<id>/link  (link an entry)
  POST      /api/channels/<id>/auto-link
  POST      /api/entries/<id>/unlink
  GET       /api/streams
  GET       /api/stats
  GET       /live/<channel_id>/index.m3u8
  GET       /live/<channel_id>/seg/<file>
  GET       /logos/<filename>
  POST      /api/channels/<id>/download-logo

UI (Flask-rendered):
  / → /ui/
  /ui/            dashboard
  /ui/sources      list / add
  /ui/sources/<id> detail / edit
  /ui/channels     list
  /ui/channels/new
  /ui/channels/<id>
  /ui/streams
"""

import logging
import os
import re
import threading
import time
import uuid
from pathlib import Path

import requests as http_requests
from flask import (Flask, Response, abort, flash, jsonify, redirect,
                   render_template, request, send_from_directory, url_for)

import config
import db
import source_manager as sm
import strm_manager
from stream_manager import (FFmpegStream, ProbeResult, VodPipeStream,
                             channel_hls_url, is_vod, kill_pid,
                             make_output_dir, playlist_path,
                             probe_stream, remove_output_dir)

# ── App setup ─────────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
log = logging.getLogger("dispatcher")

# Resolve paths relative to this file so the app works regardless of
# which directory it is launched from (e.g. python J:\AllInOne\dispatcher.py)
_HERE = os.path.dirname(os.path.abspath(__file__))
_TEMPLATE_DIR = os.path.join(_HERE, "templates")
_STATIC_DIR   = os.path.join(_HERE, "static")

# ── Validate folder layout at import time ─────────────────────────────────────
def _check_dirs():
    problems = []
    if not os.path.isdir(_TEMPLATE_DIR):
        problems.append(f"  MISSING templates dir : {_TEMPLATE_DIR}")
    else:
        required = ["base.html", "dashboard.html", "sources.html",
                    "source_edit.html", "channels.html", "channel_edit.html",
                    "streams.html", "settings.html"]
        for t in required:
            p = os.path.join(_TEMPLATE_DIR, t)
            if not os.path.isfile(p):
                problems.append(f"  MISSING template file : {p}")
    if not os.path.isdir(_STATIC_DIR):
        # static/ is optional — only warn
        log.warning("static/ dir not found at %s (non-fatal)", _STATIC_DIR)
    if problems:
        log.error("=" * 60)
        log.error("STARTUP PROBLEM — missing files detected:")
        for p in problems:
            log.error(p)
        log.error("dispatcher.py is at  : %s", os.path.abspath(__file__))
        log.error("Expected layout:")
        log.error("  J:\\AllInOne\\")
        log.error("  ├── dispatcher.py")
        log.error("  ├── templates\\")
        log.error("  │   ├── base.html")
        log.error("  │   ├── dashboard.html")
        log.error("  │   └── ... (all .html files)")
        log.error("  └── static\\")
        log.error("=" * 60)
        raise RuntimeError(
            f"Required templates folder not found at: {_TEMPLATE_DIR}\n"
            "Make sure the templates\\ folder is in the same directory as dispatcher.py"
        )

_check_dirs()

# Log resolved binary paths so it's obvious if ffmpeg isn't found
log.info("ffmpeg  : %s", config.FFMPEG_BIN)
log.info("ffprobe : %s", config.FFPROBE_BIN)
for _bin, _name in [(config.FFMPEG_BIN, "ffmpeg"), (config.FFPROBE_BIN, "ffprobe")]:
    if not os.path.isfile(_bin) and not __import__("shutil").which(_bin):
        log.error("BINARY NOT FOUND: %s — set FFMPEG_BIN/FFPROBE_BIN in config.py "
                  "or add ffmpeg to your system PATH", _bin)

app = Flask(__name__,
            template_folder=_TEMPLATE_DIR,
            static_folder=_STATIC_DIR)
app.secret_key = config.SECRET_KEY


@app.context_processor
def inject_globals():
    return {"config_base_url": config.BASE_URL, "app_title": config.UI_TITLE}


# In-memory process registry: stream_id → FFmpegStream
_procs: dict[str, FFmpegStream] = {}
# Failed entry_ids per channel for fallback ordering: channel_id → [entry_id, …]
_failed_entries: dict[str, list] = {}
_lock = threading.Lock()


# ── Startup / shutdown ────────────────────────────────────────────────────────

def startup():
    os.makedirs(config.HLS_ROOT, exist_ok=True)
    os.makedirs(config.LOGOS_DIR, exist_ok=True)
    db.init_db()

    # Clean up any streams left over from a previous run
    for row in db.list_active_streams():
        if row["ffmpeg_pid"]:
            kill_pid(row["ffmpeg_pid"])
        if row["output_dir"]:
            remove_output_dir(row["output_dir"])
        db.set_stream_stopped(row["id"])

    # Clean channel_active table (all stale after restart)
    with db.get_db() as conn:
        conn.execute("DELETE FROM channel_active")

    sm.start_scheduler()
    threading.Thread(target=_reaper_loop, daemon=True, name="reaper").start()
    log.info("Stream dispatcher ready — http://%s:%d", config.HOST, config.PORT)


# ── Background reaper + health check ─────────────────────────────────────────

def _reaper_loop():
    while True:
        time.sleep(config.REAPER_INTERVAL)
        try:
            _reap_idle_streams()
            _health_check_active_streams()
        except Exception:
            log.exception("Reaper error")


def _reap_idle_streams():
    for row in db.get_stale_streams(config.HEARTBEAT_TTL):
        log.info("[%s] Reaping idle stream (channel=%s)", row["id"], row["channel_id"])
        _stop_stream(row["id"], row["output_dir"], row["ffmpeg_pid"])
        if row["channel_id"]:
            db.clear_channel_active(row["channel_id"])


def _health_check_active_streams():
    """Detect unexpectedly dead ffmpeg processes and trigger fallback."""
    with _lock:
        snapshot = dict(_procs)

    for stream_id, ffmpeg in snapshot.items():
        if not ffmpeg.is_running():
            stream = db.get_stream(stream_id)
            if not stream or stream["status"] not in ("active", "starting"):
                continue
            # ffmpeg died unexpectedly
            channel_id = stream["channel_id"]
            entry_id   = stream["entry_id"]
            log.warning("[%s] ffmpeg died unexpectedly (channel=%s)", stream_id, channel_id)
            db.set_stream_error(stream_id, "ffmpeg process died")
            with _lock:
                _procs.pop(stream_id, None)
            if channel_id:
                if entry_id:
                    _failed_entries.setdefault(channel_id, []).append(entry_id)
                _try_fallback(channel_id)


# ── Core stream lifecycle ─────────────────────────────────────────────────────

def _stop_stream(stream_id: str, output_dir, ffmpeg_pid):
    with _lock:
        ffmpeg = _procs.pop(stream_id, None)
    if ffmpeg:
        ffmpeg.stop()
    elif ffmpeg_pid:
        kill_pid(ffmpeg_pid)
    if output_dir:
        remove_output_dir(output_dir)
    db.set_stream_stopped(stream_id)


def channel_vod_url(channel_id: str) -> str:
    return f"{config.BASE_URL}/vod/{channel_id}"


def _start_stream_for_entry(entry, channel_id: str,
                             channel_name: str) -> tuple[str | None, str]:
    """
    Probe entry, launch ffmpeg (HLS for live, registered-but-no-process for VOD pipe),
    update DB. Returns (stream_id, error_message).
    """
    source      = db.get_source(entry["source_id"])
    stream_type = entry["stream_type"]
    vod         = is_vod(stream_type)

    # VOD pipe streams don't use HLS segments or occupy a concurrent slot.
    # We just probe, record the stream, and return — the actual ffmpeg process
    # is spawned per-request in the /vod/<channel_id> endpoint.
    if vod:
        probe: ProbeResult = probe_stream(entry["full_url"])
        if not probe.ok:
            return None, f"Source unavailable: {probe.error}"

        stream_id = uuid.uuid4().hex
        # No output_dir needed for pipe streams
        db.create_stream(
            stream_id, channel_id, entry["id"], entry["source_id"],
            channel_name, entry["stream_key"], entry["full_url"],
            stream_type, output_dir="",
        )
        db.set_stream_active(
            stream_id, ffmpeg_pid=None,
            width=probe.width, height=probe.height,
            fps=probe.fps, codec=probe.codec, bitrate=probe.bitrate,
        )
        db.update_entry_probe(
            entry["id"], "ok",
            codec=probe.codec, width=probe.width, height=probe.height,
            fps=probe.fps, bitrate=probe.bitrate,
        )
        log.info("[%s] VOD pipe ready — %s %dx%d@%.2ffps  %s",
                 stream_id, probe.codec, probe.width, probe.height,
                 probe.fps, stream_type)
        return stream_id, ""

    # ── Live stream — HLS via ffmpeg ──────────────────────────────────────────
    if source:
        max_c = source["max_concurrent"]
        if max_c > 0 and db.get_source_active_count(entry["source_id"]) >= max_c:
            return None, f"Source '{source['name']}' concurrent limit reached ({max_c})"

    probe: ProbeResult = probe_stream(entry["full_url"])
    if not probe.ok:
        return None, f"Source unavailable: {probe.error}"

    stream_id  = uuid.uuid4().hex
    output_dir = make_output_dir(stream_id)
    db.create_stream(
        stream_id, channel_id, entry["id"], entry["source_id"],
        channel_name, entry["stream_key"], entry["full_url"],
        stream_type, output_dir,
    )

    ffmpeg = FFmpegStream(stream_id, entry["full_url"], output_dir, stream_type)
    pid    = ffmpeg.start()
    ready  = ffmpeg.wait_for_ready()

    if not ready:
        ffmpeg.stop()
        remove_output_dir(output_dir)
        db.set_stream_error(stream_id, "ffmpeg failed to produce HLS output")
        return None, "ffmpeg failed to start"

    with _lock:
        _procs[stream_id] = ffmpeg

    db.set_stream_active(
        stream_id, pid,
        width=probe.width, height=probe.height,
        fps=probe.fps, codec=probe.codec, bitrate=probe.bitrate,
    )
    db.update_entry_probe(
        entry["id"], "ok",
        codec=probe.codec, width=probe.width, height=probe.height,
        fps=probe.fps, bitrate=probe.bitrate,
    )
    log.info("[%s] Live stream active — %s %dx%d@%.2ffps",
             stream_id, probe.codec, probe.width, probe.height, probe.fps)
    return stream_id, ""


def _try_fallback(channel_id: str) -> bool:
    """
    Pick the next best source for channel_id, start it, update channel_active.
    Returns True if a new stream was started.
    """
    channel = db.get_channel(channel_id)
    if not channel:
        return False

    failed = _failed_entries.get(channel_id, [])
    entries = db.get_best_entries_for_channel(channel_id, exclude_entry_ids=failed)

    for entry in entries:
        log.info("[fallback] Trying entry %d (source=%s) for channel=%s",
                 entry["id"], entry["source_name"], channel_id)
        stream_id, err = _start_stream_for_entry(entry, channel_id, channel["name"])
        if stream_id:
            db.set_channel_active(channel_id, stream_id)
            db.update_session_stream(channel_id, stream_id)
            log.info("[fallback] Success — channel=%s new stream=%s", channel_id, stream_id)
            return True
        log.warning("[fallback] Entry %d failed: %s", entry["id"], err)
        _failed_entries.setdefault(channel_id, []).append(entry["id"])

    log.error("[fallback] No working sources left for channel=%s", channel_id)
    db.clear_channel_active(channel_id)
    return False


def _get_or_start_channel(channel_id: str) -> tuple[str | None, str]:
    """
    Return current active stream_id for channel, starting one if needed.
    Returns (stream_id, error).
    """
    channel = db.get_channel(channel_id)
    if not channel:
        log.error("[channel:%s] Not found in database", channel_id)
        return None, "Channel not found"

    # Check existing active stream
    active = db.get_channel_active(channel_id)
    if active:
        stream_id = active["stream_id"]
        stream    = db.get_stream(stream_id)
        if stream and stream["status"] in ("active", "starting", "completed"):
            with _lock:
                ffmpeg = _procs.get(stream_id)
                if ffmpeg is None or ffmpeg.is_running() or stream["status"] == "completed":
                    return stream_id, ""
        log.info("[channel:%s] Stale active record — restarting", channel_id)
        db.clear_channel_active(channel_id)

    # Reset failed list for fresh attempt
    _failed_entries.pop(channel_id, None)

    # Try sources best-first
    entries = db.get_best_entries_for_channel(channel_id)
    if not entries:
        log.error("[channel:%s] No sources linked. "
                  "Go to Link Streams and link entries to this channel.",
                  channel_id)
        return None, (f"No sources linked to channel '{channel['name']}'. "
                      "Use the Link Streams page to link entries.")

    log.info("[channel:%s] Starting stream — %d source(s) available",
             channel_id, len(entries))

    for entry in entries:
        log.info("[channel:%s] Trying entry %d (%s) source=%s",
                 channel_id, entry["id"],
                 entry["raw_name"] or entry["stream_key"],
                 entry["source_name"])
        stream_id, err = _start_stream_for_entry(entry, channel_id, channel["name"])
        if stream_id:
            db.set_channel_active(channel_id, stream_id)
            return stream_id, ""
        log.warning("[channel:%s] Entry %d failed: %s",
                    channel_id, entry["id"], err)
        _failed_entries.setdefault(channel_id, []).append(entry["id"])

    log.error("[channel:%s] All %d source(s) failed", channel_id, len(entries))
    return None, "All sources failed for this channel"


# ── Channel proxy endpoints ───────────────────────────────────────────────────

@app.route("/live/<channel_id>/index.m3u8")
def channel_playlist(channel_id: str):
    stream_id, err = _get_or_start_channel(channel_id)
    if not stream_id:
        log.error("[channel:%s] 503 — %s", channel_id, err)
        return Response(f"# ERROR: {err}\n", status=503,
                        mimetype="application/x-mpegurl")

    stream = db.get_stream(stream_id)
    if not stream:
        return Response("# Stream record lost\n", status=503,
                        mimetype="application/x-mpegurl")

    # VOD pipe channels don't use HLS — redirect to the pipe endpoint
    if is_vod(stream["stream_type"] if "stream_type" in stream.keys() else ""):
        return redirect(url_for("channel_vod_pipe", channel_id=channel_id))

    output_dir = stream["output_dir"] or ""
    m3u8 = playlist_path(output_dir) if output_dir else ""

    if not output_dir or not os.path.exists(m3u8):
        # ffmpeg still starting — retry hint for players that support it
        return Response("# Playlist not ready yet — retry in 2s\n",
                        status=503, mimetype="application/x-mpegurl",
                        headers={"Retry-After": "2"})

    db.heartbeat_stream(stream_id)

    with open(m3u8) as fh:
        content = fh.read()

    # Rewrite segment paths to go through our proxy.
    # ffmpeg writes bare filenames (00001.ts) — make them absolute.
    base = f"{config.BASE_URL}/live/{channel_id}/seg/"
    content = re.sub(
        r'^(?!#)(.+\.ts)$',
        lambda m: base + os.path.basename(m.group(1)),
        content,
        flags=re.MULTILINE,
    )
    return Response(content, mimetype="application/x-mpegurl")


@app.route("/live/<channel_id>/seg/<filename>")
def channel_segment(channel_id: str, filename: str):
    active = db.get_channel_active(channel_id)
    if not active:
        abort(404)
    stream = db.get_stream(active["stream_id"])
    if not stream or not stream["output_dir"]:
        abort(404)
    db.heartbeat_stream(active["stream_id"])
    return send_from_directory(stream["output_dir"], filename)


@app.route("/vod/<channel_id>")
def channel_vod_pipe(channel_id: str):
    """
    Stream a VOD channel directly as mpegts via ffmpeg pipe:1.
    A new ffmpeg process is spawned per request — no HLS segments on disk.
    Emby/VLC should be pointed at:  http://host:6077/vod/<channel_id>
    """
    stream_id, err = _get_or_start_channel(channel_id)
    if not stream_id:
        abort(503)

    stream = db.get_stream(stream_id)
    if not stream or not is_vod(stream.get("stream_type", "")):
        abort(404)

    db.heartbeat_stream(stream_id)

    pipe = VodPipeStream(stream["source_url"], stream["stream_type"])
    return pipe.flask_response()




@app.route("/logos/<path:filename>")
def serve_logo(filename: str):
    return send_from_directory(config.LOGOS_DIR, filename)


# ── API ───────────────────────────────────────────────────────────────────────

# -- Sources --

@app.route("/api/sources", methods=["GET"])
def api_list_sources():
    rows = db.list_sources()
    out  = []
    for r in rows:
        out.append({
            "id": r["id"], "name": r["name"], "type": r["type"],
            "location": r["location"], "max_concurrent": r["max_concurrent"],
            "refresh_interval_mins": r["refresh_interval_mins"],
            "refresh_times": r["refresh_times"], "stale_days": r["stale_days"],
            "last_refreshed_at": r["last_refreshed_at"],
            "entry_count": r["entry_count"], "status": r["status"],
            "status_msg": r["status_msg"],
        })
    return jsonify(out)


@app.route("/api/sources", methods=["POST"])
def api_add_source():
    d = request.get_json(silent=True) or {}
    required = ("name", "location")
    if not all(d.get(k, "").strip() for k in required):
        return jsonify({"error": "name and location are required"}), 400
    sid = db.add_source(
        name=d["name"].strip(), type_=d.get("type", "url"),
        location=d["location"].strip(),
        max_concurrent=int(d.get("max_concurrent", 2)),
        refresh_interval_mins=int(d.get("refresh_interval_mins", 0)),
        refresh_times=d.get("refresh_times", ""),
        stale_days=int(d.get("stale_days", config.DEFAULT_STALE_DAYS)),
        notes=d.get("notes", ""),
    )
    sm.schedule_source(sid)
    threading.Thread(target=sm.refresh_source, args=(sid,), daemon=True).start()
    return jsonify({"id": sid, "status": "refresh started"}), 201


@app.route("/api/sources/<int:sid>", methods=["GET"])
def api_get_source(sid):
    row = db.get_source(sid)
    if not row:
        return jsonify({"error": "Not found"}), 404
    return jsonify(dict(row))


@app.route("/api/sources/<int:sid>", methods=["PUT"])
def api_update_source(sid):
    d = request.get_json(silent=True) or {}
    db.update_source(sid, **d)
    sm.schedule_source(sid)
    return jsonify({"ok": True})


@app.route("/api/sources/<int:sid>", methods=["DELETE"])
def api_delete_source(sid):
    sm.unschedule_source(sid)
    db.delete_source(sid)
    return jsonify({"ok": True})


@app.route("/api/sources/<int:sid>/refresh", methods=["POST"])
def api_refresh_source(sid):
    threading.Thread(target=sm.refresh_source, args=(sid,), daemon=True).start()
    return jsonify({"ok": True, "message": "Refresh started"})


@app.route("/api/sources/<int:sid>/probe", methods=["POST"])
def api_probe_source(sid):
    threading.Thread(target=sm.probe_source_entries, args=(sid,), daemon=True).start()
    return jsonify({"ok": True, "message": "Probe started"})


# -- Channels --

@app.route("/api/channels", methods=["GET"])
def api_list_channels():
    group = request.args.get("group")
    tag   = request.args.get("tag")
    rows  = db.list_channels(group=group, tag=tag)
    return jsonify([dict(r) for r in rows])


@app.route("/api/channels", methods=["POST"])
def api_add_channel():
    d = request.get_json(silent=True) or {}
    cid = d.get("id", "").strip()
    if not cid or not d.get("name", "").strip():
        return jsonify({"error": "id and name are required"}), 400
    db.upsert_channel(
        cid, d["name"].strip(),
        number=d.get("number"), group_title=d.get("group_title", ""),
        tvg_id=d.get("tvg_id", ""), logo_url=d.get("logo_url", ""),
        stream_type=d.get("stream_type", "auto"), notes=d.get("notes", ""),
    )
    return jsonify({"id": cid}), 201


@app.route("/api/channels/<channel_id>", methods=["GET"])
def api_get_channel(channel_id):
    ch = db.get_channel(channel_id)
    if not ch:
        return jsonify({"error": "Not found"}), 404
    entries = db.list_entries(channel_id=channel_id)
    tags    = db.get_channel_tags(channel_id)
    return jsonify({
        **dict(ch),
        "sources": [dict(e) for e in entries],
        "tags":    [dict(t) for t in tags],
    })


@app.route("/api/channels/<channel_id>", methods=["PUT"])
def api_update_channel(channel_id):
    ch = db.get_channel(channel_id)
    if not ch:
        return jsonify({"error": "Not found"}), 404
    d = request.get_json(silent=True) or {}
    db.upsert_channel(
        channel_id,
        name        = d.get("name", ch["name"]),
        number      = d.get("number", ch["number"]),
        group_title = d.get("group_title", ch["group_title"]),
        tvg_id      = d.get("tvg_id", ch["tvg_id"]),
        logo_url    = d.get("logo_url", ch["logo_url"]),
        stream_type = d.get("stream_type", ch["stream_type"]),
        notes       = d.get("notes", ch["notes"]),
    )
    if "tag_ids" in d:
        db.set_channel_tags(channel_id, d["tag_ids"])
    return jsonify({"ok": True})


@app.route("/api/channels/<channel_id>", methods=["DELETE"])
def api_delete_channel(channel_id):
    db.delete_channel(channel_id)
    return jsonify({"ok": True})


@app.route("/api/channels/<channel_id>/probe", methods=["POST"])
def api_probe_channel(channel_id):
    threading.Thread(
        target=sm.probe_channel_entries, args=(channel_id,), daemon=True
    ).start()
    return jsonify({"ok": True, "message": "Probe started"})


@app.route("/api/channels/<channel_id>/status")
def api_channel_status(channel_id):
    """
    Diagnostic endpoint — shows exactly what sources are linked,
    their probe status, and the current active stream.
    Hit this in a browser if playback is returning 503.
    """
    ch = db.get_channel(channel_id)
    if not ch:
        return jsonify({"error": "Channel not found"}), 404

    entries = db.get_best_entries_for_channel(channel_id)
    active  = db.get_channel_active(channel_id)
    stream  = db.get_stream(active["stream_id"]) if active else None

    with _lock:
        ffmpeg_alive = (stream and stream["id"] in _procs
                        and _procs[stream["id"]].is_running())

    return jsonify({
        "channel_id":   channel_id,
        "channel_name": ch["name"],
        "stream_type":  ch["stream_type"],
        "live_url":     channel_hls_url(channel_id),
        "linked_sources": len(entries),
        "sources": [
            {
                "entry_id":     e["id"],
                "name":         e["raw_name"] or e["stream_key"],
                "source":       e["source_name"],
                "stream_type":  e["stream_type"],
                "probe_status": e["probe_status"],
                "resolution":   f"{e['probe_width']}x{e['probe_height']}" if e["probe_width"] else None,
                "fps":          e["probe_fps"],
                "quality_score":e["quality_score"],
                "url":          e["full_url"],
            }
            for e in entries
        ],
        "active_stream": {
            "id":       stream["id"],
            "status":   stream["status"],
            "ffmpeg_alive": ffmpeg_alive,
            "resolution": f"{stream['res_width']}x{stream['res_height']}",
            "fps":      stream["fps"],
        } if stream else None,
        "ffmpeg_bin":  config.FFMPEG_BIN,
        "ffprobe_bin": config.FFPROBE_BIN,
    })


@app.route("/api/channels/<channel_id>/link", methods=["POST"])
def api_link_entry(channel_id):
    f = request.form or request.json or {}
    entry_id = f.get("entry_id")
    if not entry_id:
        return {"error": "Missing entry_id"}, 400
        
    # Convert incoming form inputs to integers to match SQLite autoincrement keys
    db.link_entry_to_channel(int(entry_id), channel_id)
    return {"ok": True}


@app.route("/api/channels/<channel_id>/auto-link", methods=["POST"])
def api_auto_link(channel_id):
    n = db.auto_link_by_tvg_id()
    return jsonify({"ok": True, "linked": n})


@app.route("/api/channels/<channel_id>/download-logo", methods=["POST"])
def api_download_logo(channel_id):
    ch = db.get_channel(channel_id)
    if not ch or not ch["logo_url"]:
        return jsonify({"error": "No logo URL set"}), 400
    try:
        resp = http_requests.get(ch["logo_url"], timeout=10)
        resp.raise_for_status()
        ext      = os.path.splitext(ch["logo_url"])[-1].split("?")[0] or ".png"
        filename = f"{channel_id}{ext}"
        path     = os.path.join(config.LOGOS_DIR, filename)
        with open(path, "wb") as fh:
            fh.write(resp.content)
        db.set_channel_logo_local(channel_id, filename)
        return jsonify({"ok": True, "filename": filename})
    except Exception as exc:
        return jsonify({"error": str(exc)}), 502


@app.route("/api/entries/<int:entry_id>/unlink", methods=["POST"])
def api_unlink_entry(entry_id):
    db.unlink_entry(entry_id)
    return jsonify({"ok": True})


@app.route("/api/entries/<int:entry_id>/priority", methods=["PUT"])
def api_set_priority(entry_id):
    d = request.get_json(silent=True) or {}
    db.set_entry_priority(entry_id, int(d.get("priority", 10)))
    return jsonify({"ok": True})


# -- Streams --

@app.route("/api/streams", methods=["GET"])
def api_list_streams():
    rows = db.list_active_streams()
    out  = []
    for r in rows:
        with _lock:
            alive = r["id"] in _procs and _procs[r["id"]].is_running()
        out.append({
            "id": r["id"], "channel_id": r["channel_id"],
            "channel_name": r["channel_name"],
            "source_name": r["source_name"],
            "stream_type": r["stream_type"],
            "status": r["status"],
            "res_width": r["res_width"], "res_height": r["res_height"],
            "fps": r["fps"], "codec": r["codec"], "bitrate": r["bitrate"],
            "viewer_count": r["viewer_count"],
            "hls_url": channel_hls_url(r["channel_id"]) if r["channel_id"] else None,
            "ffmpeg_alive": alive,
            "started_at": r["started_at"], "last_heartbeat": r["last_heartbeat"],
        })
    return jsonify(out)


@app.route("/api/stats")
def api_stats():
    stats   = db.get_stream_stats()
    sources = db.list_sources()
    return jsonify({
        "active_streams":  stats["active"] or 0,
        "total_viewers":   stats["viewers"] or 0,
        "completed_vod":   stats["completed_vod"] or 0,
        "total_channels":  len(db.list_channels()),
        "total_sources":   len(sources),
        "sources_ok":      sum(1 for s in sources if s["status"] == "ok"),
    })


@app.route("/api/m3u")
def api_m3u():
    """
    Generate M3U playlist.

    Query params:
      include_vod=0|1   include VOD channels (default: from settings)
      vod_only=1        only VOD channels
    """
    settings       = db.get_settings()
    include_vod    = request.args.get(
        "include_vod",
        "1" if settings.get("m3u_include_vod", "1") == "1" else "0"
    ) == "1"
    vod_only       = request.args.get("vod_only", "0") == "1"

    rows  = db.list_channels()
    lines = ["#EXTM3U"]

    for r in rows:
        stype = r["stream_type"]
        vod   = stype in ("vod_movie", "vod_series") or (
            stype == "auto" and (r["best_stream_type"] if "best_stream_type" in r.keys() else "live") != "live"
        )

        if vod_only and not vod:
            continue
        if not include_vod and vod:
            continue

        logo_local = r["logo_local"] or ""
        logo_url   = r["logo_url"]   or ""
        logo = (f"{config.BASE_URL}/logos/{logo_local}"
                if logo_local else logo_url)

        num   = r["number"] or ""
        group = r["group_title"] or "Uncategorised"
        url   = (channel_vod_url(r["id"])
                 if vod else channel_hls_url(r["id"]))

        lines.append(
            f'#EXTINF:-1 tvg-id="{r["tvg_id"]}" tvg-name="{r["name"]}" '
            f'tvg-logo="{logo}" group-title="{group}" '
            f'channel-number="{num}",{r["name"]}'
        )
        lines.append(url)

    return Response("\n".join(lines) + "\n", mimetype="application/x-mpegurl")



@app.route("/api/health")
def api_health():
    with _lock:
        live = sum(1 for f in _procs.values() if f.is_running())
    return jsonify({"ok": True, "active_ffmpeg": live})


# -- Settings --

@app.route("/api/settings", methods=["GET"])
def api_get_settings():
    return jsonify(db.get_settings())


@app.route("/api/settings", methods=["POST"])
def api_update_settings():
    d = request.get_json(silent=True) or {}
    allowed = {"m3u_include_vod", "strm_enabled", "strm_root",
               "strm_movies_dir", "strm_series_dir"}
    for key, val in d.items():
        if key in allowed:
            db.set_setting(key, str(val))
    return jsonify({"ok": True})


# -- STRM --

@app.route("/api/strm/generate", methods=["POST"])
def api_strm_generate():
    """Generate .strm files for all VOD channels (or one if channel_id given)."""
    d          = request.get_json(silent=True) or {}
    channel_id = d.get("channel_id")
    result     = strm_manager.generate_all_strm(channel_id)
    return jsonify(result)


@app.route("/api/strm/cleanup", methods=["POST"])
def api_strm_cleanup():
    """Remove .strm files for stale/deleted channels."""
    result = strm_manager.cleanup_strm()
    return jsonify(result)


# ── UI routes ─────────────────────────────────────────────────────────────────

@app.route("/")
def root():
    return redirect(url_for("ui_dashboard"))


@app.route("/ui/")
def ui_dashboard():
    stats   = dict(db.get_stream_stats())
    stats["total_channels"] = len(db.list_channels())
    stats["total_sources"]  = len(db.list_sources())
    streams = db.list_active_streams()
    return render_template("dashboard.html",
                           title="Dashboard", stats=stats, streams=streams)


@app.route("/ui/sources", methods=["GET", "POST"])
def ui_sources():
    if request.method == "POST":
        f = request.form
        action = f.get("_action")

        # Handle the new Global Probe action payload
        if action == "probe_all":
            sources = db.list_sources()
            
            def run_mass_probe():
                import source_manager as sm
                for s in sources:
                    try:
                        # Invoke your native worker engine function for each ID
                        sm.probe_source_entries(s["id"])
                    except Exception as e:
                        log.error(f"Error mass probing source {s['id']}: {e}")

            threading.Thread(target=run_mass_probe, daemon=True).start()
            flash("Background analysis started for all streaming sources.", "info")
            return redirect(url_for("ui_sources"))

        # Your existing fallback save/add logic below...
        name = f.get("name", "").strip()
        location = f.get("location", "").strip()
        if name and location:
            # ... (your existing database source addition code remains here) ...
            pass

    sources = db.list_sources()
    return render_template("sources.html", title="Manage Sources", sources=sources)
    

@app.route("/ui/sources/new", methods=["GET", "POST"])
def ui_source_new():
    if request.method == "POST":
        f = request.form
        try:
            sid = db.add_source(
                name=f["name"].strip(), type_=f.get("type", "url"),
                location=f["location"].strip(),
                max_concurrent=int(f.get("max_concurrent", 2)),
                refresh_interval_mins=int(f.get("refresh_interval_mins", 0)),
                refresh_times=f.get("refresh_times", ""),
                stale_days=int(f.get("stale_days", config.DEFAULT_STALE_DAYS)),
                notes=f.get("notes", ""),
            )
            sm.schedule_source(sid)
            threading.Thread(target=sm.refresh_source, args=(sid,), daemon=True).start()
            flash("Source added — refresh started in background.", "success")
            return redirect(url_for("ui_source_detail", sid=sid))
        except Exception as exc:
            flash(f"Error: {exc}", "danger")
    return render_template("source_edit.html", title="Add Source", source=None)

@app.route("/ui/sources/<id>", methods=["GET", "POST"])
def ui_source_detail(id):
    source_id = int(id)
    source = db.get_source(source_id)
    if not source:
        abort(404)

    if request.method == "POST":
        f = request.form
        action = f.get("_action")

        if action == "refresh":
            import source_manager as sm
            threading.Thread(target=sm.refresh_source, args=(source_id,), daemon=True).start()
            flash("Source refresh triggered in background.", "info")
            return redirect(url_for("ui_source_detail", id=source_id))

        # FIX ISSUE 4: Handle standard source deletion requests
        elif action == "delete":
            import source_manager as sm
            sm.unschedule_source(source_id) # Unschedules any APScheduler loops running background checks
            
            with db.get_db() as conn:
                conn.execute("DELETE FROM sources WHERE id = ?", (source_id,))
            
            flash(f"Source '{source['name']}' has been permanently deleted.", "success")
            return redirect(url_for("ui_sources"))

        elif action == "save":
            # FIXED: Changed from db.update_source_metadata to db.update_source
            db.update_source(
                source_id,
                name=f.get("name").strip(),
                type=f.get("type"),
                location=f.get("location").strip(),
                max_concurrent=int(f.get("max_concurrent", 2)),
                refresh_interval_mins=int(f.get("refresh_interval_mins", 0)),
                refresh_times=f.get("refresh_times", ""),
                notes=f.get("notes", "").strip()
            )
            import source_manager as sm
            sm.schedule_source(source_id) # recalculate tasks
            flash("Source configuration saved.", "success")
            return redirect(url_for("ui_source_detail", id=source_id))

    # FIX ISSUE 5: Pass an empty list here instead of querying tens of thousands of channel entries.
    # This prevents the web application from bottlenecking and lagging.
    entries = [] 

    return render_template(
        "source_edit.html",
        title=f"Edit Source — {source['name']}",
        source=source,
        entries=entries
    )

@app.route("/ui/sources/<int:sid>/delete", methods=["POST"])
def ui_sources_delete(sid):
    # 1. Unschedule any active background refresh tasks for this source
    import source_manager as sm
    sm.unschedule_source(sid)
    
    # 2. Delete the source and all cascade entries from the database
    with db.get_db() as conn:
        # If your table has foreign key cascades set up, deleting from 'sources'
        # will automatically clear 'channel_entries'. Just to be safe, we clear both.
        conn.execute("DELETE FROM channel_entries WHERE source_id = ?", (sid,))
        conn.execute("DELETE FROM sources WHERE id = ?", (sid,))
        
    flash("Source and all associated entries deleted permanently.", "danger")
    return redirect(url_for("ui_sources"))  # Redirect back to the main sources list

@app.route("/ui/channels")
def ui_channels():
    group    = request.args.get("group", "")
    tag      = request.args.get("tag", "")
    channels = db.list_channels(group=group or None, tag=tag or None)
    groups   = db.list_groups()
    tags     = db.list_tags()
    return render_template("channels.html", title="Channels",
                           channels=channels, groups=groups, tags=tags,
                           current_group=group, current_tag=tag)


@app.route("/ui/channels/new", methods=["GET", "POST"])
def ui_channel_new():
    if request.method == "POST":
        f   = request.form
        cid = f.get("id", "").strip().lower().replace(" ", "-")
        if not cid or not f.get("name", "").strip():
            flash("ID and Name are required.", "danger")
        else:
            try:
                db.upsert_channel(
                    cid, f["name"].strip(),
                    number=int(f["number"]) if f.get("number") else None,
                    group_title=f.get("group_title", ""),
                    tvg_id=f.get("tvg_id", ""),
                    logo_url=f.get("logo_url", ""),
                    stream_type=f.get("stream_type", "auto"),
                    notes=f.get("notes", ""),
                )
                flash("Channel created.", "success")
                return redirect(url_for("ui_channel_detail", channel_id=cid))
            except Exception as exc:
                flash(f"Error: {exc}", "danger")
    return render_template("channel_edit.html", title="New Channel",
                           channel=None, entries=[], unlinked=[], all_tags=db.list_tags(),
                           channel_tags=[])

@app.route("/ui/channels/<channel_id>", methods=["GET", "POST"])
def ui_channel_edit(channel_id):
    if request.method == "POST":
        action = request.form.get("_action")

        if action == "save":
            name = request.form.get("name")
            number_raw = request.form.get("number")
            group_title = request.form.get("group_title")
            tvg_id = request.form.get("tvg_id")
            logo_url = request.form.get("logo_url")
            stream_type = request.form.get("stream_type", "auto")
            notes = request.form.get("notes")

            number = int(number_raw) if (number_raw and number_raw.strip()) else None

            # Update core properties
            db.update_channel(channel_id, name, number, group_title, tvg_id, logo_url, notes)
            # Update explicit routing type overrides
            db.update_channel_stream_type(channel_id, stream_type)

            flash(f"Channel '{name}' alignment updated successfully.", "success")
            return redirect(url_for("ui_channels"))

        elif action == "download-logo":
            success, msg = download_channel_logo(channel_id)
            flash(msg, "success" if success else "danger")
            return redirect(url_for("ui_channel_edit", channel_id=channel_id))

        elif action == "update-tags":
            selected_tags = request.form.getlist("tags")
            db.set_channel_tags(channel_id, selected_tags)
            flash("Assigned administration filtering tags updated.", "success")
            return redirect(url_for("ui_channel_edit", channel_id=channel_id))

        elif action == "unlink_entry":
            entry_id = request.form.get("entry_id")
            if entry_id:
                with db.get_db() as conn:
                    conn.execute(
                        "UPDATE channel_entries SET channel_id = NULL WHERE id = ?",
                        (int(entry_id),)
                    )
                flash("Stream connection severed successfully from this channel profile.", "success")
            return redirect(url_for("ui_channel_edit", channel_id=channel_id))

    # GET request processing
    channel = db.get_channel(channel_id)
    if not channel:
        flash(f"Channel ID '{channel_id}' not found.", "danger")
        return redirect(url_for("ui_channels"))

    # Convert row objects to mutable dicts to safely modify fields if needed
    ch_dict = dict(channel)

    # Fetch associated stream profiles linked to this layout asset slot
    entries = db.list_entries(channel_id=channel_id)
    groups = db.list_groups()
    all_tags = db.list_tags()
    current_tags = db.get_channel_tags(channel_id)

    return render_template(
        "channel_edit.html",
        title=f"Edit Channel — {ch_dict.get('name')}",
        channel=ch_dict,
        entries=entries,
        groups=groups,
        all_tags=all_tags,
        current_tags=current_tags
    )

@app.route("/ui/streams")
def ui_streams():
    streams = db.list_active_streams()
    with _lock:
        alive = set(sid for sid, f in _procs.items() if f.is_running())
    return render_template("streams.html", title="Live Streams",
                           streams=streams, alive=alive)


@app.route("/ui/settings", methods=["GET", "POST"])
def ui_settings():
    if request.method == "POST":
        f      = request.form
        action = f.get("_action", "save")

        if action == "save":
            db.set_setting("m3u_include_vod",  "1" if f.get("m3u_include_vod") else "0")
            db.set_setting("strm_enabled",      "1" if f.get("strm_enabled")    else "0")
            db.set_setting("strm_root",         f.get("strm_root", "").strip())
            db.set_setting("strm_movies_dir",   f.get("strm_movies_dir", "Movies").strip())
            db.set_setting("strm_series_dir",   f.get("strm_series_dir", "Series").strip())
            flash("Settings saved.", "success")

        elif action == "generate_strm":
            result = strm_manager.generate_all_strm()
            flash(
                f"STRM generation complete — "
                f"{result['written']} written, "
                f"{result['skipped']} skipped, "
                f"{result['errors']} errors.",
                "success" if result["errors"] == 0 else "warning",
            )

        elif action == "cleanup_strm":
            result = strm_manager.cleanup_strm()
            flash(f"STRM cleanup — {result['removed']} stale file(s) removed.", "info")

        return redirect(url_for("ui_settings"))

    settings = db.get_settings()
    sources  = db.list_sources()
    stats    = dict(db.get_stream_stats())
    return render_template("settings.html", title="Settings",
                           settings=settings, sources=sources, stats=stats)

@app.route("/ui/api-docs", methods=["GET"])
def ui_api_docs():
    """Renders the comprehensive interactive Swagger UI document dashboard."""
    return render_template("api_docs.html")

@app.route("/api/openapi.json", methods=["GET"])
def api_openapi_spec():
    """Returns the extended OpenAPI / Swagger specification structure for the system."""
    return jsonify({
        "openapi": "3.0.3",
        "info": {
            "title": "Stream Dispatcher API",
            "description": "Comprehensive developer console for fully automating sources, channels, stream visibilities, and mapping linkages.",
            "version": "1.1.0"
        },
        "paths": {
            "/api/sources": {
                "get": {
                    "tags": ["Sources"],
                    "summary": "List all configured M3U sources",
                    "responses": {
                        "200": {"description": "A JSON array of source objects."}
                    }
                },
                "post": {
                    "tags": ["Sources"],
                    "summary": "Create a new M3U source configuration",
                    "requestBody": {
                        "required": True,
                        "content": {
                            "application/json": {
                                "schema": {
                                    "type": "object",
                                    "required": ["name", "type", "location"],
                                    "properties": {
                                        "name": {"type": "string", "example": "UK Premium Line"},
                                        "type": {"type": "string", "enum": ["url", "file"], "example": "url"},
                                        "location": {"type": "string", "example": "http://line.iptv.com/get.php?auth=123"}
                                    }
                                }
                            }
                        }
                    },
                    "responses": {
                        "201": {"description": "Source created successfully."}
                    }
                }
            },
            "/api/sources/{id}": {
                "get": {
                    "tags": ["Sources"],
                    "summary": "Get details of a specific source",
                    "parameters": [{"name": "id", "in": "path", "required": True, "schema": {"type": "integer"}}],
                    "responses": {
                        "200": {"description": "Detailed source data payload."},
                        "404": {"description": "Source not found."}
                    }
                },
                "put": {
                    "tags": ["Sources"],
                    "summary": "Update an existing source configuration",
                    "parameters": [{"name": "id", "in": "path", "required": True, "schema": {"type": "integer"}}],
                    "requestBody": {
                        "required": True,
                        "content": {
                            "application/json": {
                                "schema": {
                                    "type": "object",
                                    "properties": {
                                        "name": {"type": "string"},
                                        "type": {"type": "string", "enum": ["url", "file"]},
                                        "location": {"type": "string"}
                                    }
                                }
                            }
                        }
                    },
                    "responses": {
                        "200": {"description": "Source updated successfully."},
                        "404": {"description": "Source not found."}
                    }
                },
                "delete": {
                    "tags": ["Sources"],
                    "summary": "Delete a source (cascades and drops unlinked entries)",
                    "parameters": [{"name": "id", "in": "path", "required": True, "schema": {"type": "integer"}}],
                    "responses": {
                        "200": {"description": "Source deleted successfully."},
                        "404": {"description": "Source not found."}
                    }
                }
            },
            "/api/sources/{id}/refresh": {
                "post": {
                    "tags": ["Sources"],
                    "summary": "Trigger background M3U synchronization and parsing task",
                    "parameters": [{"name": "id", "in": "path", "required": True, "schema": {"type": "integer"}}],
                    "responses": {
                        "202": {"description": "Refresh background task queued up safely."}
                    }
                }
            },
            "/api/channels": {
                "get": {
                    "tags": ["Channels"],
                    "summary": "List all structural target channels slots",
                    "responses": {
                        "200": {"description": "A JSON array of operational channel profiles."}
                    }
                },
                "post": {
                    "tags": ["Channels"],
                    "summary": "Create a new target structural channel slot",
                    "requestBody": {
                        "required": True,
                        "content": {
                            "application/json": {
                                "schema": {
                                    "type": "object",
                                    "required": ["id", "name"],
                                    "properties": {
                                        "id": {"type": "string", "example": "bbc-one-hd"},
                                        "name": {"type": "string", "example": "BBC One HD"},
                                        "number": {"type": "integer", "example": 101},
                                        "group_title": {"type": "string", "example": "UK Entertainment"}
                                    }
                                }
                            }
                        }
                    },
                    "responses": {
                        "201": {"description": "Channel created successfully."}
                    }
                }
            },
            "/api/channels/{id}": {
                "get": {
                    "tags": ["Channels"],
                    "summary": "Get details of a structural channel",
                    "parameters": [{"name": "id", "in": "path", "required": True, "schema": {"type": "string"}}],
                    "responses": {
                        "200": {"description": "Detailed metadata properties of channel slot."},
                        "404": {"description": "Channel slot not found."}
                    }
                },
                "put": {
                    "tags": ["Channels"],
                    "summary": "Update metadata properties of a channel slot",
                    "parameters": [{"name": "id", "in": "path", "required": True, "schema": {"type": "string"}}],
                    "requestBody": {
                        "required": True,
                        "content": {
                            "application/json": {
                                "schema": {
                                    "type": "object",
                                    "properties": {
                                        "name": {"type": "string"},
                                        "number": {"type": "integer"},
                                        "group_title": {"type": "string"},
                                        "tvg_id": {"type": "string"}
                                    }
                                }
                            }
                        }
                    },
                    "responses": {
                        "200": {"description": "Channel metadata updated successfully."},
                        "404": {"description": "Channel slot not found."}
                    }
                },
                "delete": {
                    "tags": ["Channels"],
                    "summary": "Delete a structural channel slot",
                    "parameters": [{"name": "id", "in": "path", "required": True, "schema": {"type": "string"}}],
                    "responses": {
                        "200": {"description": "Channel dropped successfully."},
                        "404": {"description": "Channel not found."}
                    }
                }
            },
            "/api/entries/{id}/visibility": {
                "post": {
                    "tags": ["Streams & Visibility"],
                    "summary": "Hide or unhide an raw incoming stream entry",
                    "parameters": [{"name": "id", "in": "path", "required": True, "schema": {"type": "integer"}}],
                    "requestBody": {
                        "required": True,
                        "content": {
                            "application/json": {
                                "schema": {
                                    "type": "object",
                                    "required": ["is_hidden"],
                                    "properties": {
                                        "is_hidden": {"type": "boolean", "example": True, "description": "True to hide, False to unhide raw entry"}
                                    }
                                }
                            }
                        }
                    },
                    "responses": {
                        "200": {"description": "Visibility flag state mutated successfully."}
                    }
                }
            },
            "/api/channels/{id}/link": {
                "post": {
                    "tags": ["Linkage Management"],
                    "summary": "Link / bind a raw stream entry to a channel slot",
                    "parameters": [{"name": "id", "in": "path", "required": True, "schema": {"type": "string"}, "description": "The target channel ID"}],
                    "requestBody": {
                        "required": True,
                        "content": {
                            "application/json": {
                                "schema": {
                                    "type": "object",
                                    "required": ["entry_id"],
                                    "properties": {
                                        "entry_id": {"type": "integer", "example": 1422, "description": "The raw stream entry ID to map"}
                                    }
                                }
                            }
                        }
                    },
                    "responses": {
                        "200": {"description": "Stream successfully linked onto channel slot."}
                    }
                }
            },
            "/api/entries/{id}/unlink": {
                "post": {
                    "tags": ["Linkage Management"],
                    "summary": "Remove linkage binding from an entry (returns it to unlinked pool)",
                    "parameters": [{"name": "id", "in": "path", "required": True, "schema": {"type": "integer"}, "description": "The raw stream entry ID to break mapping for"}],
                    "responses": {
                        "200": {"description": "Linkage broken completely. Entry dropped back into raw pool."}
                    }
                }
            },
            "/api/stats": {
                "get": {
                    "tags": ["System Stats"],
                    "summary": "Retrieve absolute system operation metadata metrics counters",
                    "responses": {
                        "200": {"description": "Current active streams, connection footprints, and viewer totals."}
                    }
                }
            }
        }
    })

@app.route("/api/entries/<int:entry_id>/visibility", methods=["POST"])
def api_toggle_entry_visibility(entry_id):
    data = request.get_json() or {}
    is_hidden = 1 if data.get("is_hidden") else 0
    with db.get_db() as conn:
        conn.execute("UPDATE channel_entries SET is_hidden=? WHERE id=?", (is_hidden, entry_id))
    return jsonify({"status": "success", "entry_id": entry_id, "is_hidden": is_hidden})


@app.route("/ui/link", methods=["GET", "POST"])
def ui_link():
    if request.method == "POST":
        f = request.form
        action = f.get("_action")
        
        if action == "link":
            entry_id = f.get("entry_id")
            channel_id = f.get("channel_id")
            if entry_id and channel_id:
                db.link_entry_to_channel(int(entry_id), channel_id)
                flash("Stream linked successfully.", "success")
                
        elif action == "auto_link":
            count = sm.auto_link_all_by_tvg_id() if hasattr(sm, 'auto_link_all_by_tvg_id') else 0
            flash(f"Auto-linked {count} streams.", "info")
            
        elif action == "toggle_hide":
            entry_id = int(f.get("entry_id"))
            current_state = int(f.get("current_state", 0))
            db.toggle_entry_visibility(entry_id, not current_state)
            return "", 204 # Instant AJAX silent return code
            
        return redirect(url_for("ui_link", group=request.args.get("group", ""), q=request.args.get("q", "")))

    group = request.args.get("group", "")
    channels = db.list_channels(group=group or None)
    groups = db.list_groups()
    
# Grab ALL entries from database to filter and paginate lightning-fast in the UI layer
    with db.get_db() as conn:
        # Using conn.row_factory = sqlite3.Row ensures we can serialize cleanly
        rows = conn.execute(
            """SELECT e.id, e.source_id, e.channel_id, e.raw_name, e.full_url, 
                      e.raw_group, e.is_hidden, s.name AS source_name 
               FROM channel_entries e
               JOIN sources s ON e.source_id = s.id
               ORDER BY e.raw_name ASC"""
        ).fetchall()
        
    # Convert SQLite Rows into a clean list of Python dictionaries for JavaScript usage
    all_entries = [dict(row) for row in rows]

    return render_template(
        "link.html", 
        title="Link Streams to Channels",
        channels=channels, 
        unlinked=all_entries,  # Sent as a safe list of dicts
        groups=groups, 
        current_group=group
    )


# ── Entry point ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    startup()
    app.run(host=config.HOST, port=config.PORT, threaded=True)
