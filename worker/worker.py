# NopenHeimer/worker/worker.py
import socket
import time
import uuid
import sys
import os
import redis
from celery import Celery
from celery.exceptions import MaxRetriesExceededError
import logging

# --- Project Setup ---
script_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.abspath(os.path.join(script_dir, '..'))
sys.path.append(project_root)

# --- Imports ---
from shared.config import REDIS_URL
from shared import db # Use updated shared/db.py with pooling
from shared.mc_ping import ping_server # Use shared/mc_ping.py
from shared.logger import get_logger # Use shared logger

# --- Initialization ---
log = get_logger("worker")
try:
    app = Celery("worker", broker=REDIS_URL)
    redis_client = redis.Redis.from_url(REDIS_URL, decode_responses=True)
    redis_client.ping()
    log.info(f"Worker ({socket.gethostname()}) successfully connected to Redis at {REDIS_URL}")
except Exception as e:
    log.critical(f"FATAL: Worker ({socket.gethostname()}) failed to connect to Redis: {e}")
    sys.exit(1)

# --- Configuration ---
TARGET_PORT = int(os.getenv("TARGET_PORT", 25565))
PORT_CHECK_TIMEOUT = float(os.getenv("PORT_CHECK_TIMEOUT", 0.3)) # Timeout for quick port check
MC_PING_TIMEOUT = float(os.getenv("MC_PING_TIMEOUT", 0.4)) # Timeout for actual MC ping

# --- Helper Function ---
def is_port_open(ip, port=TARGET_PORT, timeout=PORT_CHECK_TIMEOUT):
    """Quickly checks if a TCP port is open on a given IP."""
    try:
        with socket.create_connection((ip, port), timeout=timeout):
            return True
    except (socket.timeout, ConnectionRefusedError, OSError):
        return False
    except Exception as e:
        log.debug(f"Unexpected error in is_port_open for {ip}:{port} - {e}")
        return False

# --- Celery Task ---
@app.task(name="worker.worker.scan_ip_batch", bind=True, max_retries=2, default_retry_delay=5)
def scan_ip_batch(self, ip_list, cidr_block):
    """
    Scans a batch of IPs efficiently:
    1. Quick port check.
    2. If open, Minecraft ping.
    3. If ping succeeds, log details and insert ONLY the found server into DB.
    Uses pooled DB connections.
    """
    hostname = socket.gethostname()
    batch_start_time = time.time()
    pings_succeeded = 0 # Count successful pings specifically
    ports_checked = 0 # Count IPs checked after port open test
    processed_count = 0 # Count IPs started processing in the loop

    # Redis key specific to the CIDR block for counting found servers within this scan run
    scan_count_key = f"scan_count:{cidr_block}"

    log.debug(f"[{hostname}] Task {self.request.id}: Received batch for {cidr_block}. Size: {len(ip_list)}")

    try:
        for ip in ip_list:
            processed_count += 1

            # --- Step 1: Quick Port Check ---
            if not is_port_open(ip, port=TARGET_PORT, timeout=PORT_CHECK_TIMEOUT):
                # Port closed or unreachable quickly, skip entirely.
                continue

            # --- If we reach here, port IS open ---
            ports_checked += 1
            ping_result_data = None
            ping_success = False

            # --- Step 2: Attempt Minecraft Ping (only if port was open) ---
            try:
                ping_result_data = ping_server(ip, port=TARGET_PORT, timeout=MC_PING_TIMEOUT)
                if ping_result_data:
                    ping_success = True
            except Exception as ping_err:
                log.debug(f"[{hostname}] Ping error for {ip} (port was open): {ping_err}")
                # No need to do anything else, just proceed to next IP

            # --- Step 3: Process SUCCESSFUL Pings ONLY ---
            if ping_success:
                pings_succeeded += 1
                # Extract data safely
                motd = ping_result_data.get("motd", "") or ""
                players_online = ping_result_data.get("players_online", 0) or 0
                players_max = ping_result_data.get("players_max", 0) or 0
                version = ping_result_data.get("version", "unknown") or "unknown"
                player_names = ping_result_data.get("player_names", []) or []

                log.info(f"[{hostname}] [+] Found: {ip} ({cidr_block}) | {players_online}/{players_max} | {version[:30]}")

                # Add to Redis set (optional)
                try:
                    redis_client.sadd("found_servers", ip)
                except Exception as redis_err:
                     log.warning(f"[{hostname}] Failed to add {ip} to Redis set 'found_servers': {redis_err}")

                # --- !!! Insert ONLY Found Server Record into DB !!! ---
                try:
                    db.insert_server_info(
                        ip=str(ip), motd=str(motd), players_online=int(players_online),
                        players_max=int(players_max), version=str(version),
                        player_names=list(map(str, player_names)), cidr_ref=str(cidr_block)
                    )
                except Exception as db_err:
                     log.error(f"[{hostname}] DB insert failed for FOUND server {ip}: {db_err}")
                     # If DB insert fails, should we decrement pings_succeeded? Affects stats.
                     pings_succeeded -= 1 # Let's decrement so stats reflect successful stores
            # else:
                # Ping failed or returned None. Do nothing, just loop to next IP.
                # log.debug(f"[{hostname}] [-] No valid MC ping from {ip} ({cidr_block}) (port was open)")


        # --- Redis Stats Update (End of Task) ---
        if pings_succeeded > 0:
             try:
                 # Update the controller's counter for this specific run
                 redis_client.incrby(scan_count_key, pings_succeeded)
             except Exception as redis_err:
                 log.error(f"[{hostname}] Failed to increment Redis scan counter {scan_count_key}: {redis_err}")

        # General Stats Update
        try:
            pipe = redis_client.pipeline()
            # total_scanned should reflect IPs attempted in the batch
            pipe.incrby("stats:total_scanned", len(ip_list))
            # total_found should reflect successful pings stored in DB
            if pings_succeeded > 0:
                pipe.incrby("stats:total_found", pings_succeeded)

            scan_timestamp = int(batch_start_time)
            unique_score_value = f"{scan_timestamp}:{len(ip_list)}:{hostname}:{uuid.uuid4()}"
            pipe.zadd("stats:scans", {unique_score_value: scan_timestamp})
            pipe.setex(f"stats:worker:{hostname}", 90, "online") # Heartbeat
            pipe.execute()
        except Exception as redis_pipe_err:
            log.error(f"[{hostname}] Failed to update Redis pipeline stats: {redis_pipe_err}")

        elapsed = time.time() - batch_start_time
        log.info(f"[{hostname}] Task {self.request.id}: Finished batch for {cidr_block}. "
                 f"Processed: {processed_count}/{len(ip_list)}. Ports Checked: {ports_checked}. Pings OK: {pings_succeeded}. Time: {elapsed:.2f}s")

    except MaxRetriesExceededError as max_retry_err:
         log.error(f"[{hostname}] FATAL: Max retries exceeded for task {self.request.id} processing batch for {cidr_block}. Error: {max_retry_err}")

    except Exception as exc:
        log.error(f"[{hostname}] UNHANDLED ERROR in task {self.request.id} for {cidr_block}: {exc}")
        import traceback
        traceback.print_exc()
        try:
            log.warning(f"[{hostname}] Retrying task {self.request.id} due to unhandled exception.")
            self.retry(exc=exc)
        except MaxRetriesExceededError:
            log.error(f"[{hostname}] Max retries exceeded for task {self.request.id} after unhandled exception.")
        except Exception as retry_err:
             log.error(f"[{hostname}] Error occurred during retry attempt for task {self.request.id}: {retry_err}")