# worker/worker.py
import socket
import time
import uuid
import sys
import os
import redis
from celery import Celery

# Adjust import path
script_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.abspath(os.path.join(script_dir, '..'))
sys.path.append(project_root)

from shared.config import REDIS_URL
from shared import db # Use updated db module with pooling
from tools.mc_ping import ping_server

# --- Celery and Redis Setup ---
app = Celery("worker", broker=REDIS_URL)
redis_client = redis.Redis.from_url(REDIS_URL)

# --- Configuration ---
target_port = 25565 # Make configurable via env?
timeout = 0.3      # Make configurable via env?
chunk_size = 20    # Ensure this matches controller if imported there

# --- Helper Functions ---
def is_port_open(ip, port=target_port):
    # (Keep existing logic)
    try:
        with socket.create_connection((ip, port), timeout=timeout):
            return True
    except Exception:
        return False

# --- Celery Task ---
@app.task(name="worker.worker.scan_ip_batch", bind=True, max_retries=3, default_retry_delay=10)
def scan_ip_batch(self, ip_list, cidr_block): # Added cidr_block argument
    """
    Scans a batch of IPs belonging to a specific CIDR block.
    Updates Redis stats and inserts found servers into PostgreSQL using pooling.
    Tracks found count per CIDR block in Redis for the controller.
    """
    hostname = socket.gethostname()
    batch_start_time = time.time()
    found_in_batch = 0
    processed_count = 0
    servers_to_insert = [] # Batch inserts to DB

    # Use a Redis key specific to the CIDR block for counting within this scan run
    scan_count_key = f"scan_count:{cidr_block}"

    print(f"[{hostname}] Received batch for {cidr_block}. Size: {len(ip_list)}. Task ID: {self.request.id}")

    try:
        for ip in ip_list:
            processed_count += 1
            # Basic port check first (optional optimization, ping_server handles connection too)
            # if not is_port_open(ip):
            #     continue

            result = ping_server(ip, port=target_port) # Pass port/timeout if configurable
            if result:
                motd = result.get("motd") or ""
                players_online = result.get("players_online") or 0
                players_max = result.get("players_max") or 0
                version = result.get("version") or "unknown"
                player_names = result.get("player_names") or []

                # print(f"[{hostname}] [+] Found: {ip} ({cidr_block}) - {motd} [{players_online}/{players_max}]") # Reduce noise?
                redis_client.sadd("found_servers", ip) # Still useful for quick export/dashboard view
                found_in_batch += 1

                # Prepare data for batch insert
                servers_to_insert.append((
                    ip, motd, players_online, players_max, player_names, version, cidr_block # Include cidr_ref
                ))

            # Optional: Handle offline but open port (if is_port_open check is done)
            # else:
            #     if is_port_open(ip): # Check again if ping failed but port was open
            #          print(f"[{hostname}] [-] No MC ping from {ip} ({cidr_block}) (port open)")
                     # Prepare offline server entry? Might clutter DB. Decide if needed.
                     # servers_to_insert.append((ip, "[OFFLINE]", 0, 0, [], "unknown", cidr_block))


        # --- Batch DB Insert ---
        if servers_to_insert:
            try:
                db.insert_server_batch(servers_to_insert)
            except Exception as db_err:
                print(f"[WORKER DB ERROR] Failed batch insert for {cidr_block}: {db_err}")
                # Optional: Retry the task? Celery's retry handles transient DB issues well.
                # self.retry(exc=db_err) # Uncomment to enable Celery retry on DB error

        # --- Redis Stats Update ---
        if found_in_batch > 0:
             # Increment the counter for this specific scan run
             redis_client.incrby(scan_count_key, found_in_batch)


        # General Stats (Pipeline for efficiency)
        pipe = redis_client.pipeline()
        pipe.incrby("stats:total_scanned", len(ip_list))
        pipe.incrby("stats:total_found", found_in_batch) # Use batch count for accuracy
        # Use a unique score value for ZADD to avoid overwriting entries with the same timestamp
        scan_timestamp = int(batch_start_time)
        unique_score_value = f"{scan_timestamp}:{len(ip_list)}:{uuid.uuid4()}"
        pipe.zadd("stats:scans", {unique_score_value: scan_timestamp})
        pipe.setex(f"stats:worker:{hostname}", 90, "online") # Worker heartbeat
        pipe.execute()

        elapsed = time.time() - batch_start_time
        print(f"[{hostname}] Finished batch for {cidr_block}. Found: {found_in_batch}/{len(ip_list)}. Time: {elapsed:.2f}s")

    except Exception as exc:
        print(f"[WORKER ERROR] Unhandled exception in scan_ip_batch for {cidr_block}: {exc}")
        import traceback
        traceback.print_exc()
        # Use Celery's retry mechanism for potential transient issues
        try:
            self.retry(exc=exc)
        except self.MaxRetriesExceededError:
            print(f"[WORKER FATAL] Max retries exceeded for task {self.request.id} processing batch for {cidr_block}")
            # Optionally mark the range as error? Requires passing task_id back or more DB calls.