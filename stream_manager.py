"""stream_manager.py — ffmpeg HLS process lifecycle.

Changes from original:
  - probe_stream() / ProbeResult retained for VOD and manual/batch probing only.
  - Live streams no longer call probe_stream() on the hot path.
  - Metadata (codec, resolution, fps, bitrate) is extracted asynchronously from
    ffmpeg's own stderr log after the stream is confirmed running.
  - wait_for_ready() polling tightened to 0.1 s intervals.
  - FFmpegStream.start() captures stderr to a pipe so metadata can be parsed
    without a separate ffprobe process.
  - extract_metadata_from_log() parses the ffmpeg startup banner inline.
  - All public API surface is unchanged — dispatcher.py needs no edits.
"""

import json
import logging
import os
import re
import shutil
import subprocess
import threading
import time
from pathlib import Path
from typing import Optional

import config

log = logging.getLogger(__name__)


# ── ffprobe (kept for VOD + manual/batch probe) ───────────────────────────────

class ProbeResult:
    def __init__(self, ok: bool, codec="", width=0, height=0,
                 fps=0.0, bitrate=0, error=""):
        self.ok      = ok
        self.codec   = codec
        self.width   = width
        self.height  = height
        self.fps     = fps
        self.bitrate = bitrate
        self.error   = error

    @property
    def quality_score(self) -> float:
        return float(self.width * self.height) * (self.fps or 1.0)

    def resolution_str(self) -> str:
        return f"{self.width}x{self.height}" if self.width else "unknown"

    def __repr__(self):
        if self.ok:
            return (f"<Probe {self.codec} {self.width}x{self.height}"
                    f"@{self.fps:.3f}fps {self.bitrate}bps>")
        return f"<Probe FAIL {self.error!r}>"


def _parse_fps(r_frame_rate: str) -> float:
    try:
        parts = str(r_frame_rate).split("/")
        if len(parts) == 2:
            n, d = float(parts[0]), float(parts[1])
            return round(n / d, 3) if d else 0.0
        return round(float(r_frame_rate), 3)
    except (ValueError, ZeroDivisionError):
        return 0.0


def is_vod(stream_type: str) -> bool:
    """True for any VOD subtype."""
    return stream_type in ("vod_movie", "vod_series")


def _headers_flags() -> list[str]:
    """Return [-headers, <value>] if Android UA masquerade is enabled, else []."""
    if config.USE_ANDROID_HEADERS:
        return ["-headers", config.FFMPEG_ANDROID_HEADERS]
    return []


def probe_stream(url: str) -> ProbeResult:
    """Full ffprobe — used for VOD validation and manual/batch probing only."""
    header_args = []
    if config.USE_ANDROID_HEADERS:
        header_args = ["-headers", config.FFMPEG_ANDROID_HEADERS]

    cmd = [
        config.FFPROBE_BIN,
        "-v", "quiet",
        "-print_format", "json",
        "-show_streams",
        "-show_format",
        "-timeout", str(config.FFPROBE_TIMEOUT * 1_000_000),
        *header_args,
        url,
    ]

    try:
        result = subprocess.run(
            cmd, capture_output=True, text=True,
            timeout=config.FFPROBE_TIMEOUT + 2,
        )
    except subprocess.TimeoutExpired:
        return ProbeResult(ok=False, error="ffprobe timed out")
    except FileNotFoundError:
        return ProbeResult(ok=False, error=f"{config.FFPROBE_BIN!r} not found")

    if result.returncode != 0:
        lines = result.stderr.strip().splitlines()
        return ProbeResult(ok=False, error=lines[-1] if lines else "unknown error")

    try:
        data = json.loads(result.stdout)
    except json.JSONDecodeError:
        return ProbeResult(ok=False, error="non-JSON ffprobe output")

    video = next(
        (s for s in data.get("streams", []) if s.get("codec_type") == "video"), {}
    )
    fmt = data.get("format", {})
    fps = _parse_fps(video.get("r_frame_rate", "0/1"))

    return ProbeResult(
        ok      = True,
        codec   = video.get("codec_name", "?"),
        width   = int(video.get("width", 0)),
        height  = int(video.get("height", 0)),
        fps     = fps,
        bitrate = int(fmt.get("bit_rate", 0)),
    )


# ── Metadata extraction from ffmpeg stderr ────────────────────────────────────

# Matches lines like:
#   Stream #0:0: Video: h264 (High), yuv420p, 1920x1080 [SAR 1:1 DAR 16:9], 4128 kb/s, 25 fps, ...
#   Stream #0:1: Audio: aac, 48000 Hz, stereo, fltp, 128 kb/s
_RE_VIDEO = re.compile(
    r"Stream #\d+:\d+.*?Video:\s*(\w+)"          # codec  e.g. h264
    r".*?(\d{3,5})x(\d{3,5})"                    # WxH
    r"(?:.*?(\d+)\s*kb/s)?"                       # bitrate (optional)
    r".*?(\d+(?:\.\d+)?)\s*(?:fps|tbr)",          # fps
    re.IGNORECASE,
)

def extract_metadata_from_log(log_path: str, timeout: float = 10.0) -> ProbeResult:
    """
    Read ffmpeg's log file until the video stream line appears or timeout expires.
    Returns a ProbeResult populated from ffmpeg's own startup banner — zero extra
    network round-trips.
    """
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            with open(log_path, "r", errors="replace") as fh:
                content = fh.read()
        except FileNotFoundError:
            time.sleep(0.1)
            continue

        m = _RE_VIDEO.search(content)
        if m:
            codec   = m.group(1).lower()
            width   = int(m.group(2))
            height  = int(m.group(3))
            bitrate = int(m.group(4)) * 1000 if m.group(4) else 0
            fps     = float(m.group(5))
            return ProbeResult(ok=True, codec=codec, width=width,
                               height=height, fps=fps, bitrate=bitrate)

        # ffmpeg exited before printing stream info — bail early
        if "No such file" in content or "Invalid data" in content or \
           "Connection refused" in content or "403 Forbidden" in content or \
           "404 Not Found" in content:
            # Extract last meaningful error line
            lines = [l.strip() for l in content.splitlines() if l.strip()]
            error = lines[-1] if lines else "ffmpeg error"
            return ProbeResult(ok=False, error=error)

        time.sleep(0.1)

    return ProbeResult(ok=False, error="metadata not found in ffmpeg log (timeout)")


# ── HLS path helpers ──────────────────────────────────────────────────────────

def make_output_dir(stream_id: str) -> str:
    path = os.path.join(config.HLS_ROOT, stream_id)
    os.makedirs(path, exist_ok=True)
    return path


def remove_output_dir(output_dir: str):
    try:
        shutil.rmtree(output_dir, ignore_errors=True)
    except Exception as exc:
        log.warning("remove_output_dir(%s): %s", output_dir, exc)


def playlist_path(output_dir: str) -> str:
    return os.path.join(output_dir, "index.m3u8")


def channel_hls_url(channel_id: str) -> str:
    return f"{config.BASE_URL}/live/{channel_id}/index.m3u8"


# ── ffmpeg process ────────────────────────────────────────────────────────────

class FFmpegStream:
    def __init__(self, stream_id: str, source_url: str,
                 output_dir: str, stream_type: str = "live"):
        self.stream_id   = stream_id
        self.source_url  = source_url
        self.output_dir  = output_dir
        self.stream_type = stream_type
        self._proc: Optional[subprocess.Popen] = None
        self._log_path   = os.path.join(output_dir, "ffmpeg.log")

    # ── public ────────────────────────────────────────────────────────────────

    def start(self) -> int:
        if self._proc and self._proc.poll() is None:
            return self._proc.pid

        cmd = self._build_cmd()

        # Write stderr to log file so extract_metadata_from_log() can read it,
        # and so the log is still available for debugging.
        log_fh = open(self._log_path, "w")
        self._proc = subprocess.Popen(
            cmd,
            stdout=log_fh,
            stderr=subprocess.STDOUT,
            close_fds=True,
        )
        log.info("[%s] ffmpeg PID %d %s",
                 self.stream_id, self._proc.pid, self.source_url[:80])
        return self._proc.pid

    def wait_for_ready(self) -> bool:
        """
        Poll for the HLS playlist file at 0.1 s intervals (was 0.4 s).
        Returns True as soon as index.m3u8 exists and is non-empty.
        """
        m3u8     = playlist_path(self.output_dir)
        deadline = time.monotonic() + config.FFMPEG_STARTUP_WAIT + 10

        while time.monotonic() < deadline:
            # Early-exit if ffmpeg died before writing anything
            if self._proc and self._proc.poll() is not None:
                log.error("[%s] ffmpeg exited early (rc=%d)",
                          self.stream_id, self._proc.returncode)
                return False
            if os.path.exists(m3u8) and os.path.getsize(m3u8) > 0:
                return True
            time.sleep(0.1)   # tightened from 0.4 s

        return False

    def fetch_metadata_async(self, on_result) -> threading.Thread:
        """
        Spawn a daemon thread that parses ffmpeg's log for stream metadata and
        calls on_result(ProbeResult) when done (or on timeout).
        Non-blocking — returns the thread immediately.
        """
        def _worker():
            result = extract_metadata_from_log(
                self._log_path,
                timeout=config.FFMPEG_STARTUP_WAIT + 10,
            )
            try:
                on_result(result)
            except Exception as exc:
                log.warning("[%s] metadata callback error: %s", self.stream_id, exc)

        t = threading.Thread(
            target=_worker,
            daemon=True,
            name=f"meta-{self.stream_id[:8]}",
        )
        t.start()
        return t

    def wait_for_completion(self):
        if self._proc:
            self._proc.wait()

    def stop(self):
        if not self._proc or self._proc.poll() is not None:
            return
        try:
            self._proc.terminate()
            try:
                self._proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                self._proc.kill()
                self._proc.wait(timeout=3)
        except ProcessLookupError:
            pass
        finally:
            self._proc = None

    def is_running(self) -> bool:
        return self._proc is not None and self._proc.poll() is None

    def has_completed(self) -> bool:
        return (self._proc is not None
                and self._proc.poll() is not None
                and self._proc.returncode == 0)

    @property
    def pid(self) -> Optional[int]:
        return self._proc.pid if self._proc else None

    # ── private ───────────────────────────────────────────────────────────────

    def _build_cmd(self) -> list[str]:
        m3u8        = playlist_path(self.output_dir)
        is_vod_type = is_vod(self.stream_type)

        # Start with loglevel "level+info" so ffmpeg prints the stream banner
        # (codec/resolution/fps) which we parse for metadata — but suppress
        # the per-packet noise that "verbose" would produce.
        cmd = [config.FFMPEG_BIN, "-y", "-hide_banner", "-loglevel", "level+info"]

        # Android UA masquerade — applied to all stream types
        cmd += _headers_flags()

        if not is_vod_type:
            cmd += config.FFMPEG_INPUT_FLAGS

        cmd += ["-i", self.source_url]
        cmd += ["-map", "0:v:0", "-map", "0:a:0", "-c:v", "copy", "-c:a", "copy"]

        if is_vod_type:
            cmd += [
                "-f", "hls",
                "-hls_time", str(config.HLS_TIME),
                "-hls_list_size", str(config.HLS_LIST_SIZE_VOD),
                "-hls_flags", config.HLS_FLAGS_VOD,
                "-hls_segment_filename",
                os.path.join(self.output_dir, "%05d.ts"),
            ]
        else:
            cmd += [
                "-f", "hls",
                "-hls_time", str(config.HLS_TIME),
                "-hls_list_size", str(config.HLS_LIST_SIZE),
                "-hls_flags", config.HLS_FLAGS,
                "-hls_segment_filename",
                os.path.join(self.output_dir, "%05d.ts"),
            ]

        cmd += config.FFMPEG_OUTPUT_FLAGS + [m3u8]
        return cmd


def kill_pid(pid: int):
    if not pid:
        return
    try:
        import signal
        os.kill(pid, signal.SIGTERM)
    except ProcessLookupError:
        pass
    except Exception as exc:
        log.debug("kill_pid(%d): %s", pid, exc)


# ── VOD pipe stream (direct mpegts, no HLS segments) ─────────────────────────

class VodPipeStream:
    """
    Streams a VOD source directly to the HTTP response as mpegts via pipe:1.
    No segments written to disk — the ffmpeg process lives only for the duration
    of the HTTP request, exactly like VodStream.py.

    Usage (inside a Flask route):
        pipe = VodPipeStream(url, stream_type)
        return pipe.flask_response()
    """

    CHUNK_SIZE = 65536  # 64 KB

    def __init__(self, source_url: str, stream_type: str = "vod_movie"):
        self.source_url  = source_url
        self.stream_type = stream_type
        self._proc: Optional[subprocess.Popen] = None

    def _build_cmd(self) -> list[str]:
        cmd = [
            config.FFMPEG_BIN,
            "-hide_banner",
            "-loglevel", "error",
            "-reconnect", "1",
            "-reconnect_at_eof", "1",
            "-reconnect_streamed", "1",
            "-reconnect_delay_max", "5",
        ]
        cmd += _headers_flags()
        cmd += [
            "-i", self.source_url,
            "-map", "0:v:0",
            "-map", "0:a:0",
            "-c:v", "copy",
            "-c:a", "copy",
            "-f", "mpegts",
            "pipe:1",
        ]
        return cmd

    def _start(self) -> subprocess.Popen:
        cmd = self._build_cmd()
        log.info("[VOD pipe] %s %s", self.stream_type, self.source_url[:80])
        log.debug("[VOD pipe] cmd: %s", " ".join(cmd))
        return subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
        )

    def generate(self):
        """
        Generator that yields mpegts chunks.
        Kills the ffmpeg process when the client disconnects or the file ends.
        """
        proc = self._start()
        try:
            while True:
                chunk = proc.stdout.read(self.CHUNK_SIZE)
                if not chunk:
                    break
                yield chunk
        finally:
            log.debug("[VOD pipe] Client done — stopping ffmpeg PID %d", proc.pid)
            try:
                proc.kill()
                proc.wait(timeout=3)
            except Exception:
                pass

    def flask_response(self):
        """Return a streaming Flask Response for this VOD source."""
        from flask import Response as FlaskResponse
        return FlaskResponse(
            self.generate(),
            mimetype="video/mp2t",
            headers={"Accept-Ranges": "none"},
            direct_passthrough=True,
        )
