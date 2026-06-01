#!/usr/bin/env python3
"""
standalone_probe.py
Connects directly to the stream dispatcher database, detects all linked streams
requiring diagnostic updates (unprobed or failed), and probes them sequentially.
Updates database state attributes in batches of 20 tracking entries.
"""

import os
import sys
import sqlite3
import logging

# Ensure parent/current directory is in path to resolve local imports cleanly
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import config
from stream_manager import probe_stream

# Setup clean command line logging format
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
log = logging.getLogger("standalone_probe")


def get_db_connection():
    """Returns a standard connection to the configured SQLite database file."""
    db_path = getattr(config, "DB_PATH", "streamdispatcher.db")
    if not os.path.exists(db_path):
        log.error(f"Database file not found at path: {db_path}")
        sys.exit(1)
    
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn


def fetch_target_streams(conn):
    """
    Finds entries linked to a channel (channel_id IS NOT NULL) where:
    - probe_width IS NULL (Never probed)
    - OR probe_width = 0 / probe_codec = 'failed' / error message populated
    """
    query = """
        SELECT id, full_url, raw_name 
        FROM channel_entries 
        WHERE channel_id IS NOT NULL 
          AND (
               probe_width IS NULL 
               OR probe_width = 0 
               OR probe_codec = 'failed' 
               OR probe_error IS NOT NULL
              )
    """
    rows = conn.execute(query).fetchall()
    return [dict(row) for row in rows]


def flush_batch_updates(conn, batch_records):
    """Executes atomic batch synchronization to write the probe values to disk."""
    if not batch_records:
        return

    update_query = """
        UPDATE channel_entries 
        SET probe_codec = ?,
            probe_width = ?,
            probe_height = ?,
            probe_fps = ?,
            probe_bitrate = ?,
            quality_score = ?,
            probe_error = ?
        WHERE id = ?
    """
    
    try:
        with conn:  # Context manager automatically triggers COMMIT on success, ROLLBACK on error
            conn.executemany(update_query, batch_records)
        log.info(f"Successfully committed database update batch of {len(batch_records)} streams.")
    except Exception as e:
        log.error(f"Database error while saving batch updates: {e}")


def main():
    log.info("Starting standalone sequential stream diagnostic loop...")
    
    conn = get_db_connection()
    try:
        targets = fetch_target_streams(conn)
        total_targets = len(targets)
        
        if total_targets == 0:
            log.info("No unprobed or previously failed linked streams found. Work complete.")
            return

        log.info(f"Discovered {total_targets} linked stream profiles requiring validation.")
        
        batch_records = []
        processed_count = 0

        for index, item in enumerate(targets, start=1):
            entry_id = item["id"]
            url = item["full_url"]
            name = item["raw_name"]

            log.info(f"[{index}/{total_targets}] Probing: {name}...")
            
            try:
                # Executes your app's native ffprobe routine safely one at a time
                result = probe_stream(url)
                
                if result and result.ok:
                    log.info(f"    -> SUCCESS: {result.width}x{result.height} @ {result.fps:.2f} FPS")
                    batch_records.append((
                        result.codec,
                        result.width,
                        result.height,
                        result.fps,
                        result.bitrate,
                        int(result.quality_score),
                        None,  # Clear any previous error flags
                        entry_id
                    ))
                else:
                    err_msg = result.error if result else "Unknown empty validation response"
                    log.warning(f"    -> FAILED: {err_msg}")
                    batch_records.append((
                        "failed",
                        0,
                        0,
                        0.0,
                        0,
                        0,
                        err_msg,
                        entry_id
                    ))

            except Exception as e:
                log.error(f"    -> CRITICAL ERROR running probe function: {e}")
                batch_records.append(("failed", 0, 0, 0.0, 0, 0, str(e), entry_id))

            processed_count += 1

            # Database commit checkpoint after every 20 sequential items processed
            if len(batch_records) >= 20:
                flush_batch_updates(conn, batch_records)
                batch_records.clear()  # Empty out local tracking cache array

        # Clear remaining items left in final trailing batch block
        if batch_records:
            flush_batch_updates(conn, batch_records)

        log.info(f"Completed processing loop. Total checked streams: {processed_count}")

    finally:
        conn.close()
        log.info("Database connection closed cleanly.")


if __name__ == "__main__":
    main()