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
    log.info(f"Worker successfully connected to Redis at {REDIS_URL}")
except Exception as e:
    log.critical(f"FATAL: Worker failed to connect to Redis: {e}")
    sys.exit(1)

# --- Configuration ---
TARGET_PORT = int(os.getenv("TARGET_PORT", 25565))
CONNECT_TIMEOUT = float(os.getenv("CONNECT_TIMEOUT", 0.4)) # Or use 0.3 like old code?
# chunk_size not directly used in worker logic itself

# --- Celery Task ---
@app.task(name="worker.worker.scan_ip_batch", bind=True, max_retries=2, default_retry_delay=5)
def scan_ip_batch(self, ip_list, cidr_block):
    """
    Scans a batch of IPs. Inserts servers individually into DB as they are found, using pooling.
    """
    hostname = socket.gethostname()
    batch_start_time = time.time()
    found_in_batch = 0
    processed_count = 0
    # No batch list needed anymore: servers_to_insert_batch = []

    scan_count_key = f"scan_count:{cidr_block}" # For controller count

    log.debug(f"[{hostname}] Task {self.request.id}: Received batch for {cidr_block}. Size: {len(ip_list)}")

    try:
        for ip in ip_list:
            processed_count += 1
            result = None
            ping_success = False # Flag to track if ping worked

            try:
                # Optional: Add back is_port_open if you think it helps filter faster
                if not is_port_open(ip): # From old code logic
                      continue

                # --- Ping Server ---
                result = ping_server(ip, port=TARGET_PORT, timeout=CONNECT_TIMEOUT)
                if result:
                    ping_success = True

            except Exception as ping_err:
                log.debug(f"[{hostname}] Ping error for {ip} ({cidr_block}): {ping_err}")
                # Continue to next IP on ping error

            # --- Handle Ping Result ---
            if ping_success:
                motd = result.get("motd", "") or ""
                players_online = result.get("players_online", 0) or 0
                players_max = result.get("players_max", 0) or 0
                version = result.get("version", "unknown") or "unknown"
                player_names = result.get("player_names", []) or []

                log.info(f"[{hostname}] [+] Found: {ip} ({cidr_block}) | {players_online}/{players_max} | {version[:30]}")

                # Add to Redis set (optional)
                try:
                    redis_client.sadd("found_servers", ip)
                except Exception as redis_err:
                     log.warning(f"[{hostname}] Failed to add {ip} to Redis set 'found_servers': {redis_err}")

                found_in_batch += 1

                # --- !!! Individual DB Insert (like old code, but uses pooling) !!! ---
                try:
                    db.insert_server_info( # Call the single insert function from shared/db.py
                        ip=str(ip),
                        motd=str(motd),
                        players_online=int(players_online),
                        players_max=int(players_max),
                        version=str(version),
                        player_names=list(map(str, player_names)),
                        cidr_ref=str(cidr_block) # Pass CIDR reference
                    )
                except Exception as db_err:
                    # Log error but continue processing other IPs in the batch
                    log.error(f"[{hostname}] DB insert failed for {ip}: {db_err}")
                    # Potentially decrement found_in_batch if DB insert fails? Optional.
                    # found_in_batch -= 1 # Decrement if DB fails

            # --- Optional: Handle offline server (like old code) ---
            # This logic runs if ping_server returned None/False but is_port_open was True
            # Uncomment the is_port_open check above if you want this behavior.
            # else:
            #     # Check if port was open (requires uncommenting the check above)
            #     # if is_port_open(ip):
            #     #     log.info(f"[{hostname}] [-] No MC ping from {ip} ({cidr_block}) (port open)")
            #     #     try:
            #     #         db.insert_server_info(
            #     #             ip=str(ip), motd="[OFFLINE]", players_online=0, players_max=0,
            #     #             version="unknown", player_names=[], cidr_ref=str(cidr_block)
            #     #         )
            #     #     except Exception as db_offline_err:
            #     #          log.error(f"[{hostname}] DB insert failed for offline server {ip}: {db_offline_err}")


        # --- Redis Stats Update (End of Task) ---
        # Increment the counter for *this specific scan run* if servers were found (and successfully inserted?)
        if found_in_batch > 0:
             try:
                 redis_client.incrby(scan_count_key, found_in_batch)
             except Exception as redis_err:
                 log.error(f"[{hostname}] Failed to increment Redis scan counter {scan_count_key}: {redis_err}")

        # General Stats Update
        try:
            pipe = redis_client.pipeline()
            pipe.incrby("stats:total_scanned", len(ip_list))
            scan_timestamp = int(batch_start_time)
            unique_score_value = f"{scan_timestamp}:{len(ip_list)}:{hostname}:{uuid.uuid4()}"
            pipe.zadd("stats:scans", {unique_score_value: scan_timestamp})
            pipe.setex(f"stats:worker:{hostname}", 90, "online")
            pipe.execute()
        except Exception as redis_pipe_err:
            log.error(f"[{hostname}] Failed to update Redis pipeline stats: {redis_pipe_err}")

        elapsed = time.time() - batch_start_time
        log.info(f"[{hostname}] Task {self.request.id}: Finished batch for {cidr_block}. Found: {found_in_batch}/{len(ip_list)}. Time: {elapsed:.2f}s")

    except MaxRetriesExceededError as max_retry_err:
         log.error(f"[{hostname}] FATAL: Max retries exceeded for task {self.request.id} processing batch for {cidr_block}. Error: {max_retry_err}")
         # Data for this batch might be lost or partially processed.

    except Exception as exc:
        log.error(f"[{hostname}] UNHANDLED ERROR in task {self.request.id} for {cidr_block}: {exc}")
        import traceback
        traceback.print_exc()
        try:
            log.warning(f"[{hostname}] Retrying task {self.request.id} due to unhandled exception.")
            # Decide carefully if retry is appropriate for all errors
            self.retry(exc=exc)
        except MaxRetriesExceededError:
            log.error(f"[{hostname}] Max retries exceeded for task {self.request.id} after unhandled exception.")
        except Exception as retry_err:
             log.error(f"[{hostname}] Error occurred during retry attempt for task {self.request.id}: {retry_err}")