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
# Replicate the single timeout value from the old code
WORKER_TIMEOUT = float(os.getenv("WORKER_TIMEOUT", 0.3))

# --- Helper Function (Copied from previous version for clarity) ---
def is_port_open(ip, port=TARGET_PORT, timeout=WORKER_TIMEOUT):
    """Quickly checks if a TCP port is open on a given IP, using the worker timeout."""
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
    Scans a batch of IPs replicating the logic flow of the original code:
    1. Quick port check.
    2. If open, Minecraft ping.
    3. Insert server OR offline record into DB for EVERY open port.
    Uses pooled DB connections.
    """
    hostname = socket.gethostname()
    batch_start_time = time.time()
    pings_succeeded = 0 # Count successful pings specifically
    ports_found_open = 0 # Count how many ports were open
    processed_count = 0

    # Redis key specific to the CIDR block for counting within this scan run
    scan_count_key = f"scan_count:{cidr_block}"

    log.debug(f"[{hostname}] Task {self.request.id}: Received batch for {cidr_block}. Size: {len(ip_list)}")

    try:
        for ip in ip_list:
            processed_count += 1
            ping_result_data = None # Store data if ping succeeds

            # --- Step 1: Check Port Open (like old code) ---
            if not is_port_open(ip, port=TARGET_PORT, timeout=WORKER_TIMEOUT):
                # Port not open, skip to next IP
                continue

            # --- If we reach here, port IS open ---
            ports_found_open += 1
            log.debug(f"[{hostname}] [*] Port open for {ip}. Attempting ping.")

            # --- Step 2: Attempt Minecraft Ping (like old code) ---
            try:
                # Pass the same timeout used for port check
                ping_result_data = ping_server(ip, port=TARGET_PORT, timeout=WORKER_TIMEOUT)
            except Exception as ping_err:
                log.debug(f"[{hostname}] Ping error for {ip} (port was open): {ping_err}")
                # Proceed to insert OFFLINE record below

            # --- Step 3: Insert DB Record (Found Server OR Offline Record) ---
            # This block runs for EVERY IP where the port was found open.
            db_insert_error = False
            try:
                if ping_result_data:
                    # Ping Succeeded: Extract data and insert
                    pings_succeeded += 1
                    motd = ping_result_data.get("motd", "") or ""
                    players_online = ping_result_data.get("players_online", 0) or 0
                    players_max = ping_result_data.get("players_max", 0) or 0
                    version = ping_result_data.get("version", "unknown") or "unknown"
                    player_names = ping_result_data.get("player_names", []) or []

                    log.info(f"[{hostname}] [+] Found: {ip} ({cidr_block}) | {players_online}/{players_max} | {version[:30]}")

                    # Add to Redis set (optional, but matches old code action)
                    try:
                        redis_client.sadd("found_servers", ip)
                    except Exception as redis_err:
                         log.warning(f"[{hostname}] Failed to add {ip} to Redis set 'found_servers': {redis_err}")

                    # Insert Found Server Record
                    db.insert_server_info(
                        ip=str(ip), motd=str(motd), players_online=int(players_online),
                        players_max=int(players_max), version=str(version),
                        player_names=list(map(str, player_names)), cidr_ref=str(cidr_block)
                    )

                else:
                    # Ping Failed (or returned None) but Port was Open: Insert OFFLINE record
                    log.info(f"[{hostname}] [-] No MC ping from {ip} ({cidr_block}) (port open)")
                    db.insert_server_info(
                        ip=str(ip), motd="[OFFLINE]", players_online=0, players_max=0,
                        version="unknown", player_names=[], cidr_ref=str(cidr_block)
                    )

            except Exception as db_err:
                 # Log error from either insert attempt
                 db_insert_error = True
                 log.error(f"[{hostname}] DB insert failed for {ip} (Result: {ping_result_data is not None}): {db_err}")
                 # If DB failed for a found server, we might not want to count it in scan_count_key
                 if ping_result_data:
                     pings_succeeded -= 1 # Decrement if DB failed for a *found* server

            # --- End of per-IP processing ---

        # --- Redis Stats Update (End of Task) ---
        # Increment the controller's counter based on *successful pings* where DB insert likely worked
        if pings_succeeded > 0:
             try:
                 redis_client.incrby(scan_count_key, pings_succeeded)
             except Exception as redis_err:
                 log.error(f"[{hostname}] Failed to increment Redis scan counter {scan_count_key}: {redis_err}")

        # General Stats Update
        try:
            pipe = redis_client.pipeline()
            # 'total_scanned' reflects IPs processed in the batch loop after port check maybe? Or total received? Let's stick to total received.
            pipe.incrby("stats:total_scanned", len(ip_list))
            # 'total_found' in old code used 'found' (successful pings). Let's mirror that.
            # Note: This might differ slightly if DB inserts failed for found servers.
            if pings_succeeded > 0:
                pipe.incrby("stats:total_found", pings_succeeded)

            scan_timestamp = int(batch_start_time)
            unique_score_value = f"{scan_timestamp}:{len(ip_list)}:{hostname}:{uuid.uuid4()}"
            pipe.zadd("stats:scans", {unique_score_value: scan_timestamp})
            pipe.setex(f"stats:worker:{hostname}", 90, "online")
            pipe.execute()
        except Exception as redis_pipe_err:
            log.error(f"[{hostname}] Failed to update Redis pipeline stats: {redis_pipe_err}")

        elapsed = time.time() - batch_start_time
        log.info(f"[{hostname}] Task {self.request.id}: Finished batch for {cidr_block}. "
                 f"Processed: {processed_count}/{len(ip_list)}. Ports Open: {ports_found_open}. Pings OK: {pings_succeeded}. Time: {elapsed:.2f}s")

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