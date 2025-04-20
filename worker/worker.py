# NopenHeimer/worker/worker.py - IMPROVED Version (Old Logic + Pooling/Logging)
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
from shared.logger import get_logger
from shared.config import REDIS_URL, TARGET_PORT, WORKER_TIMEOUT # Use unified timeout
from shared import db # Use improved db module (with pooling)
from shared.mc_ping import ping_server # Use improved mc_ping

# --- Initialization ---
log = get_logger("worker")
hostname = socket.gethostname()
try:
    app = Celery("worker", broker=REDIS_URL)
    redis_client = redis.Redis.from_url(REDIS_URL, decode_responses=True)
    redis_client.ping()
    log.info(f"Worker ({hostname}) connected to Redis.")
except Exception as e:
    log.critical(f"FATAL: Worker ({hostname}) failed to connect to Redis: {e}")
    sys.exit(1)

# --- Helper Function ---
def is_port_open(ip, port=TARGET_PORT, timeout=WORKER_TIMEOUT):
    """Quickly checks if a TCP port is open."""
    try:
        with socket.create_connection((ip, port), timeout=timeout):
            return True
    except (socket.timeout, ConnectionRefusedError, OSError): return False
    except Exception as e: log.debug(f"is_port_open error {ip}:{port}: {e}"); return False

# --- Celery Task ---
@app.task(name="worker.worker.scan_ip_batch", bind=True, max_retries=2, default_retry_delay=5)
def scan_ip_batch(self, ip_list, cidr_block): # Added cidr_block back for consistency
    """
    Worker task replicating OLD logic flow with IMPROVED components:
    - Uses connection pooling via shared.db.insert_server_info.
    - Uses standard logging.
    - Still inserts [OFFLINE] records.
    """
    batch_start_time = time.monotonic()
    pings_succeeded = 0
    ports_found_open = 0
    processed_count = 0
    scan_count_key = f"scan_count:{cidr_block}"

    log.debug(f"Task {self.request.id} Received Batch:{cidr_block} Size:{len(ip_list)}")

    try:
        for ip in ip_list:
            processed_count += 1
            ping_result_data = None

            # --- Step 1: Check Port Open ---
            if not is_port_open(ip, port=TARGET_PORT, timeout=WORKER_TIMEOUT):
                continue

            ports_found_open += 1
            log.debug(f"[*] Port open for {ip}. Pinging...")

            # --- Step 2: Attempt Minecraft Ping ---
            try:
                ping_result_data = ping_server(ip, port=TARGET_PORT, timeout=WORKER_TIMEOUT)
            except Exception as ping_err:
                log.debug(f"Ping error for {ip} (port open): {ping_err}")

            # --- Step 3: Insert Found Server OR Offline Record ---
            db_insert_error = False
            try:
                if ping_result_data:
                    pings_succeeded += 1
                    motd = ping_result_data.get("motd", "") or ""
                    players_online = ping_result_data.get("players_online", 0) or 0
                    players_max = ping_result_data.get("players_max", 0) or 0
                    version = ping_result_data.get("version", "unknown") or "unknown"
                    player_names = ping_result_data.get("player_names", []) or []
                    log.info(f"[+] Found: {ip} ({cidr_block}) | {players_online}/{players_max} | {version[:30]}")
                    try: redis_client.sadd("found_servers", ip)
                    except Exception as redis_err: log.warning(f"Redis sadd fail {ip}: {redis_err}")
                    # Insert Found Record (uses pooling via db module)
                    db.insert_server_info(ip, motd, players_online, players_max, version, player_names)
                else:
                    log.info(f"[-] No MC ping from {ip} ({cidr_block}) (port open)")
                    # Insert OFFLINE Record (uses pooling via db module)
                    db.insert_server_info(ip, "[OFFLINE]", 0, 0, "unknown", [])
            except Exception as db_err:
                 db_insert_error = True
                 log.error(f"DB insert failed for {ip} (Result: {ping_result_data is not None}): {db_err}")
                 if ping_result_data: pings_succeeded -= 1 # Don't count if DB failed for found server

        # --- Redis Stats Update ---
        if pings_succeeded > 0:
             try: redis_client.incrby(scan_count_key, pings_succeeded)
             except Exception as e: log.error(f"Redis incrby fail {scan_count_key}: {e}")
        try:
            pipe = redis_client.pipeline()
            pipe.incrby("stats:total_scanned", len(ip_list))
            if pings_succeeded > 0: pipe.incrby("stats:total_found", pings_succeeded)
            ts = int(time.time()); score_val = f"{ts}:{len(ip_list)}:{hostname}:{uuid.uuid4()}"
            pipe.zadd("stats:scans", {score_val: ts})
            pipe.setex(f"stats:worker:{hostname}", 90, "online")
            pipe.execute()
        except Exception as e: log.error(f"Redis pipeline stats fail: {e}")

        elapsed = time.monotonic() - batch_start_time
        log.info(f"Task {self.request.id} Finished Batch:{cidr_block} In:{len(ip_list)} Open:{ports_found_open} Found:{pings_succeeded} Time:{elapsed:.2f}s")

    except MaxRetriesExceededError as e: log.error(f"FATAL Max retries task {self.request.id} batch {cidr_block}: {e}")
    except Exception as e:
        log.error(f"UNHANDLED ERROR task {self.request.id} batch {cidr_block}: {e}", exc_info=True)
        try: log.warning(f"Retrying task {self.request.id}"); self.retry(exc=e)
        except MaxRetriesExceededError: log.error(f"Max retries task {self.request.id} after unhandled.")
        except Exception as re: log.error(f"Retry error task {self.request.id}: {re}")