# NopenHeimer/worker/worker.py - LEAN VERSION (Minimal Logging)
import socket
import time
import uuid
import sys
import os
import redis
from celery import Celery
from celery.exceptions import MaxRetriesExceededError
# import logging # Logging import removed or commented

# --- Project Setup ---
script_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.abspath(os.path.join(script_dir, '..'))
sys.path.append(project_root)

# --- Imports ---
from shared.config import REDIS_URL
from shared import db # Use updated shared/db.py with pooling
from shared.mc_ping import ping_server # Use shared/mc_ping.py
# from shared.logger import get_logger # Logger import removed

# --- Minimal Logging Fallback ---
# Use print for CRITICAL errors if logger is removed
def log_critical(msg): print(f"CRITICAL: {msg}", file=sys.stderr)
def log_error(msg): print(f"ERROR: {msg}", file=sys.stderr)
# Info/Debug logs are intentionally removed

# --- Initialization ---
# log = get_logger("worker") # Logger removed
try:
    app = Celery("worker", broker=REDIS_URL)
    redis_client = redis.Redis.from_url(REDIS_URL, decode_responses=True)
    redis_client.ping()
    print(f"Worker ({socket.gethostname()}) connected to Redis.") # Minimal startup log
except Exception as e:
    log_critical(f"Worker ({socket.gethostname()}) failed to connect to Redis: {e}")
    sys.exit(1)

# --- Configuration ---
TARGET_PORT = int(os.getenv("TARGET_PORT", 25565))
PORT_CHECK_TIMEOUT = float(os.getenv("PORT_CHECK_TIMEOUT", 0.3))
MC_PING_TIMEOUT = float(os.getenv("MC_PING_TIMEOUT", 0.4))

# --- Helper Function ---
def is_port_open(ip, port=TARGET_PORT, timeout=PORT_CHECK_TIMEOUT):
    """Quickly checks if a TCP port is open on a given IP."""
    try:
        # Explicitly create/close socket to avoid potential resource leaks with many rapid calls
        sock = socket.create_connection((ip, port), timeout=timeout)
        sock.close()
        return True
    except (socket.timeout, ConnectionRefusedError, OSError):
        return False
    except Exception: # Catch other potential errors silently
        # log.debug(f"Unexpected error in is_port_open for {ip}:{port} - {e}") # Original log removed
        return False

# --- Celery Task ---
@app.task(name="worker.worker.scan_ip_batch", bind=True, max_retries=2, default_retry_delay=5)
def scan_ip_batch(self, ip_list, cidr_block):
    """
    LEAN worker task. Minimal logging.
    Checks port, pings if open, inserts ONLY found servers to DB.
    """
    hostname = socket.gethostname()
    batch_start_time = time.time()
    pings_succeeded = 0
    ports_checked = 0
    processed_count = 0

    scan_count_key = f"scan_count:{cidr_block}"

    # log.debug(f"[{hostname}] Task {self.request.id}: Received batch...") # Removed

    try:
        for ip in ip_list:
            processed_count += 1

            # --- Step 1: Quick Port Check ---
            if not is_port_open(ip, port=TARGET_PORT, timeout=PORT_CHECK_TIMEOUT):
                continue # Skip silently

            # --- Port IS open ---
            ports_checked += 1
            ping_result_data = None
            ping_success = False

            # --- Step 2: Attempt Minecraft Ping ---
            try:
                ping_result_data = ping_server(ip, port=TARGET_PORT, timeout=MC_PING_TIMEOUT)
                if ping_result_data:
                    ping_success = True
            except Exception: # Catch ping errors silently
                # log.debug(f"[{hostname}] Ping error for {ip}...") # Removed
                pass

            # --- Step 3: Process SUCCESSFUL Pings ONLY ---
            if ping_success:
                pings_succeeded += 1
                motd = ping_result_data.get("motd", "") or ""
                players_online = ping_result_data.get("players_online", 0) or 0
                players_max = ping_result_data.get("players_max", 0) or 0
                version = ping_result_data.get("version", "unknown") or "unknown"
                player_names = ping_result_data.get("player_names", []) or []

                # log.info(f"[{hostname}] [+] Found: {ip}...") # Removed

                # Add to Redis set (optional but keep for now)
                try:
                    redis_client.sadd("found_servers", ip)
                except Exception: # Catch silently
                    # log.warning(f"[{hostname}] Failed to add {ip} to Redis set...") # Removed
                    pass

                # Insert Found Server Record into DB
                try:
                    db.insert_server_info(
                        ip=str(ip), motd=str(motd), players_online=int(players_online),
                        players_max=int(players_max), version=str(version),
                        player_names=list(map(str, player_names)), cidr_ref=str(cidr_block)
                    )
                except Exception as db_err:
                     log_error(f"DB insert failed for FOUND server {ip}: {db_err}") # Keep critical DB errors
                     pings_succeeded -= 1 # Decrement if DB insert failed

            # --- End of per-IP processing ---

        # --- Redis Stats Update (End of Task) ---
        if pings_succeeded > 0:
             try:
                 redis_client.incrby(scan_count_key, pings_succeeded)
             except Exception: # Catch silently
                 # log.error(f"[{hostname}] Failed to increment Redis scan counter...") # Removed
                 pass

        # General Stats Update
        try:
            pipe = redis_client.pipeline()
            pipe.incrby("stats:total_scanned", len(ip_list))
            if pings_succeeded > 0:
                pipe.incrby("stats:total_found", pings_succeeded)
            scan_timestamp = int(batch_start_time)
            unique_score_value = f"{scan_timestamp}:{len(ip_list)}:{hostname}:{uuid.uuid4()}"
            pipe.zadd("stats:scans", {unique_score_value: scan_timestamp})
            pipe.setex(f"stats:worker:{hostname}", 90, "online") # Heartbeat
            pipe.execute()
        except Exception as redis_pipe_err:
             log_error(f"Failed to update Redis pipeline stats: {redis_pipe_err}") # Keep critical Redis errors

        # Minimal final log
        # elapsed = time.time() - batch_start_time
        # print(f"[{hostname}] Task {self.request.id} Done. Found:{pings_succeeded}/{ports_checked}/{len(ip_list)}. Time:{elapsed:.2f}s")

    except MaxRetriesExceededError as max_retry_err:
         log_error(f"FATAL: Max retries exceeded for task {self.request.id} processing batch for {cidr_block}. Error: {max_retry_err}") # Keep fatal errors

    except Exception as exc:
        log_error(f"UNHANDLED ERROR in task {self.request.id} for {cidr_block}: {exc}") # Keep fatal errors
        import traceback
        traceback.print_exc(file=sys.stderr) # Print traceback for unhandled errors
        try:
            # No retry logging
            self.retry(exc=exc)
        except MaxRetriesExceededError:
            pass # Silent failure after retry
        except Exception:
             pass # Silent failure during retry attempt