"""config.py — all tunables in one place."""
import os

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# ── Server ────────────────────────────────────────────────────────────────────
HOST     = "0.0.0.0"
PORT     = 6077
BASE_URL = f"http://localhost:{PORT}"

# ── Paths ─────────────────────────────────────────────────────────────────────
DB_PATH   = os.path.join(BASE_DIR, "streamdispatcher.db")
HLS_ROOT  = os.path.join(BASE_DIR, "hls_segments")
LOGOS_DIR = os.path.join(BASE_DIR, "logos")

# ── Stream lifecycle ──────────────────────────────────────────────────────────
HEARTBEAT_TTL       = 20    # seconds — streams with no segment fetches are reaped
REAPER_INTERVAL     = 10    # seconds — how often the reaper / health-check runs
FFPROBE_TIMEOUT     = 8     # seconds
FFMPEG_STARTUP_WAIT = 8     # seconds — time to wait for first HLS segment

# ── HLS output — live ─────────────────────────────────────────────────────────
HLS_TIME      = 2
HLS_LIST_SIZE = 6
HLS_FLAGS     = "delete_segments+append_list"

# ── HLS output — VOD ──────────────────────────────────────────────────────────
HLS_LIST_SIZE_VOD = 0                    # keep ALL segments for seeking
HLS_FLAGS_VOD     = "independent_segments"
VOD_CACHE_ENABLED = True                 # skip re-transcode if segments still on disk

# ── ffmpeg / ffprobe ──────────────────────────────────────────────────────────
# Set full paths here if ffmpeg is not in your system PATH.
# Examples:
#   FFMPEG_BIN  = r"C:\ffmpeg\bin\ffmpeg.exe"
#   FFPROBE_BIN = r"C:\ffmpeg\bin\ffprobe.exe"
# Leave as bare name to rely on PATH.
FFMPEG_BIN  = "ffmpeg"
FFPROBE_BIN = "ffprobe"

def _find_binary(name: str) -> str:
    """
    Return the full path to an ffmpeg/ffprobe binary.
    Checks: 1) value as-is if it's absolute, 2) next to this script,
    3) system PATH (with .exe suffix on Windows).
    """
    import shutil, sys
    # Already absolute
    if os.path.isabs(name) and os.path.isfile(name):
        return name
    # Next to dispatcher / config
    for suffix in (["", ".exe"] if sys.platform == "win32" else [""]):
        local = os.path.join(BASE_DIR, name + suffix)
        if os.path.isfile(local):
            return local
    # System PATH — try with .exe on Windows
    for suffix in (["", ".exe"] if sys.platform == "win32" else [""]):
        found = shutil.which(name + suffix)
        if found:
            return found
    return name   # fall back to bare name; will fail at runtime with a clear error

FFMPEG_BIN  = _find_binary("ffmpeg")
FFPROBE_BIN = _find_binary("ffprobe")

FFMPEG_INPUT_FLAGS  = [
    "-reconnect", "1", "-reconnect_streamed", "1",
    "-reconnect_delay_max", "5", "-timeout", "10000000",
]
FFMPEG_OUTPUT_FLAGS = []

# ── Source management ─────────────────────────────────────────────────────────
M3U_REQUEST_TIMEOUT = 30    # seconds to fetch a remote M3U
DEFAULT_STALE_DAYS  = 7     # entries not seen for this long are removed
MAX_PROBE_WORKERS   = 4     # concurrent ffprobe threads during batch probe

# VOD detection heuristics
VOD_URL_EXTENSIONS  = {".mp4", ".mkv", ".avi", ".mov", ".m4v", ".flv", ".wmv"}
VOD_GROUP_KEYWORDS  = {"thisisatotallyrandomstringofwordsthatwillnevermatchtostopanerorronthecode"}
# URL path segments that definitively identify VOD subtype
VOD_MOVIE_PATH_SEGS  = {"/movie/"}
VOD_SERIES_PATH_SEGS = {"/series/"}

# ── Android UA masquerade ─────────────────────────────────────────────────────
# Passed to ffmpeg via -headers for all stream types.
# Bypasses provider-side desktop user-agent blocks.
# Each line must end with \r\n (ffmpeg requirement).
FFMPEG_ANDROID_HEADERS = (
    "User-Agent: Mozilla/5.0 (Linux; Android 13; SM-S901B) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/112.0.0.0 Mobile Safari/537.36\r\n"
    "Accept: */*\r\n"
    "Accept-Language: en-US,en;q=0.9\r\n"
    "Sec-Ch-Ua-Mobile: ?1\r\n"
    "Sec-Ch-Ua-Platform: \"Android\"\r\n"
)
# Set False to disable the UA spoof if your provider doesn't need it
USE_ANDROID_HEADERS = True

# ── UI ────────────────────────────────────────────────────────────────────────
UI_TITLE      = "Stream Dispatcher"
SECRET_KEY    = os.environ.get("SD_SECRET", "change-me-in-production")
