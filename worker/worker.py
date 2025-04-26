import socket
import time
import uuid
import redis
import psycopg2 # Need this for retry exceptions later
from celery import Celery
from shared.db import insert_server_info, insert_server_batch, initialize_pool # Added batch insert and pool init
from shared.mc_ping import ping_server  # Unified Minecraft ping
from shared.config import REDIS_URL, TARGET_PORT, CONNECT_TIMEOUT # Import config vars
from shared.logger import logger # Import logger

# Initialize Celery and Redis
app = Celery("worker", broker=REDIS_URL)
redis_client = redis.Redis.from_url(REDIS_URL)

# Initialize DB Pool (call once at startup)
try:
    initialize_pool()
except Exception as e:
    logger.critical(f"Worker failed to initialize DB pool: {e}", exc_info=True)
    # Decide how to handle - maybe exit?
    exit(1)

# Configuration is now imported from shared.config
# target_port = 25565
# timeout = 0.3
# chunk_size = 20  # used by controller too

def is_port_open(ip, port=TARGET_PORT): # Use imported TARGET_PORT
    try:
        # Use imported CONNECT_TIMEOUT
        with socket.create_connection((ip, port), timeout=CONNECT_TIMEOUT):
            return True
    except (socket.timeout, ConnectionRefusedError, OSError):
        return False # Explicitly handle common non-open errors
    except Exception as e:
        logger.warning(f"Unexpected error in is_port_open for {ip}:{port} - {e}")
        return False

@app.task(name="worker.worker.scan_ip_batch",
          bind=True, # Needed for self.retry
          autoretry_for=(socket.timeout, psycopg2.OperationalError), # Example exceptions for retry
          retry_kwargs={'max_retries': 3},
          retry_backoff=True,
          retry_backoff_max=60, # seconds
          acks_late=True) # Acknowledge task *after* it runs successfully
def scan_ip_batch(self, ip_list):
    hostname = socket.gethostname()
    total_found_in_batch = 0
    db_batch_data = [] # List to hold data for batch insert
    timestamp = int(time.time()) # Use same timestamp for batch if needed

    # Optional: Get current CIDR range from Redis if controller sets it
    # cidr_ref = redis_client.get("current_range")
    # cidr_ref = cidr_ref.decode() if cidr_ref else None
    cidr_ref = None # Or determine this another way if needed

    logger.info(f"[{hostname}] Scanning {len(ip_list)} IPs...")

    for ip in ip_list:
        if not is_port_open(ip):
            continue

        try:
            # Note: ping_server might need retry logic too, or make it part of the task retry
            result = ping_server(ip, timeout=CONNECT_TIMEOUT) # Pass timeout
        except Exception as ping_exc:
            logger.warning(f"[{hostname}] Ping error for {ip}: {ping_exc}")
            result = None # Treat ping error same as no response

        if result:
            motd = result.get("motd") or ""
            players_online = result.get("players_online") or 0
            players_max = result.get("players_max") or 0
            version = result.get("version") or "unknown"
            # Ensure player_names is a list of strings for DB array
            player_names = result.get("player_names") or []
            player_names_str = [str(name) for name in player_names]

            logger.info(f"[{hostname}] [+] Found: {ip} - {motd} [{players_online}/{players_max}] - {version}")
            redis_client.sadd("found_servers", ip) # Keep Redis set for dashboard quick view
            total_found_in_batch += 1

            # Append data for batch insert
            db_batch_data.append((
                ip,
                motd,
                players_online,
                players_max,
                player_names_str, # Use list of strings
                version,
                cidr_ref
            ))

        else:
            # Only log offline if port was open but ping failed/timed out
            logger.debug(f"[{hostname}] [-] No MC ping from {ip} (port open)")
            # Append data for offline server batch insert
            db_batch_data.append((
                ip,
                "[OFFLINE]",
                0,
                0,
                [], # Empty list
                "unknown",
                cidr_ref
            ))

    # --- Batch Insert to Database ---
    if db_batch_data:
        try:
            inserted_count = insert_server_batch(db_batch_data)
            logger.info(f"[{hostname}] DB Batch Insert: {inserted_count}/{len(db_batch_data)} records.")
        except (psycopg2.OperationalError) as db_exc: # Catch retryable DB errors here
            logger.warning(f"[{hostname}] DB Operational Error during batch insert: {db_exc}. Task will retry.")
            raise self.retry(exc=db_exc) # Trigger Celery retry
        except Exception as db_exc:
            logger.error(f"[{hostname}] Unhandled DB Error during batch insert: {db_exc}", exc_info=True)
            # Decide if non-retryable DB errors should fail the task or just be logged

    # --- Redis Stats --- (Update stats even if DB insert has issues?)
    try:
        pipe = redis_client.pipeline()
        pipe.incrby("stats:total_scanned", len(ip_list))
        # Use the count of servers actually found (responded to ping)
        pipe.incrby("stats:total_found", total_found_in_batch)
        pipe.zadd("stats:scans", {f"{timestamp}:{len(ip_list)}:{uuid.uuid4()}": timestamp})
        pipe.setex(f"stats:worker:{hostname}", 90, "online") # Heartbeat
        pipe.execute()
    except Exception as redis_exc:
        logger.error(f"[{hostname}] Failed to update Redis stats: {redis_exc}", exc_info=True)

    logger.info(f"[{hostname}] Finished batch. Found responsive: {total_found_in_batch}, Scanned: {len(ip_list)}.")
