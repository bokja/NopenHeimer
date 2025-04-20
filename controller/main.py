# NopenHeimer/controller/main.py
import ipaddress
import os
import random
import time
import sys
from tqdm import tqdm
from celery import Celery
import redis
import logging

# --- Project Setup ---
# Add project root to sys.path to allow imports from shared/worker
script_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.abspath(os.path.join(script_dir, '..'))
sys.path.append(project_root)

# --- Imports ---
from shared.config import REDIS_URL, SKIP_ZERO_HISTORY_RANGES, RESCAN_INTERVAL_HOURS
from shared import db # Use the updated shared/db.py
from shared.logger import get_logger # Use shared logger
# Get chunk_size from worker config (ensure it's defined there or define here)
# If worker.py defines it, keep the import. If not, define it here.
try:
    from worker.worker import chunk_size
except ImportError:
    log.warning("Could not import chunk_size from worker.worker, defaulting to 20")
    chunk_size = 20


# --- Initialization ---
log = get_logger("controller")
app = Celery('controller', broker=REDIS_URL)
try:
    redis_client = redis.Redis.from_url(REDIS_URL, decode_responses=True) # Decode responses for easier handling
    redis_client.ping()
    log.info(f"Successfully connected to Redis at {REDIS_URL}")
except Exception as e:
    log.critical(f"FATAL: Failed to connect to Redis: {e}")
    sys.exit(1) # Exit if Redis connection fails


# --- Configuration & Constants ---
INITIAL_NETWORK_POOL = [
    # Keep your list, or generate more programmatically
    "172.65.0.0/12", "173.0.0.0/12", "192.241.0.0/16", "144.202.0.0/16",
    "51.81.0.0/16", "5.9.0.0/16", "167.114.0.0/16", "45.13.0.0/16"
    # Example large block expansion:
    # "10.0.0.0/8" # Uncomment to test expansion (will generate many /16s)
]

# Break down large blocks (like /12) into smaller, more manageable ones (e.g., /16)
GENERATED_POOL = []
log.info("Generating manageable CIDR blocks from initial pool...")
for block in INITIAL_NETWORK_POOL:
    try:
        net = ipaddress.ip_network(block, strict=False)
        target_prefix = 16 # Target prefix length for manageable blocks
        if net.prefixlen < target_prefix: # Break down anything larger than target
            GENERATED_POOL.extend([str(sub_net) for sub_net in net.subnets(new_prefix=target_prefix)])
            log.info(f"Expanded {block} into /{target_prefix} blocks.")
        elif net.prefixlen >= target_prefix : # Keep target size and smaller as is
            GENERATED_POOL.append(str(net))
        # else: # Should not happen with current logic
        #     log.warning(f"Skipping block {block} due to unexpected prefix length logic.")
    except ValueError as e:
        log.error(f"Invalid CIDR format in INITIAL_NETWORK_POOL: {block} - {e}")
log.info(f"Total manageable CIDR blocks generated: {len(GENERATED_POOL)}")


EXCLUDE_FILE = "exclude.conf" # Keep exclusion file for now

# --- Checkpoint Optimization ---
# Update checkpoint in DB every N chunks instead of every chunk
# Set via environment variable or default to 100. Tune based on performance.
CHECKPOINT_UPDATE_INTERVAL = int(os.getenv("CHECKPOINT_INTERVAL_CHUNKS", 100))


# --- Helper Functions ---
def load_exclusions():
    """Loads IP ranges/networks to exclude from scanning."""
    excluded = []
    if os.path.exists(EXCLUDE_FILE):
        try:
            with open(EXCLUDE_FILE, "r") as f:
                for line_num, line in enumerate(f, 1):
                    line = line.strip()
                    if not line or line.startswith("#"):
                        continue
                    try:
                         if "-" in line:
                             start, end = line.split("-")
                             excluded.append((ipaddress.IPv4Address(start.strip()), ipaddress.IPv4Address(end.strip())))
                         else:
                            excluded.append(ipaddress.IPv4Network(line, strict=False))
                    except ValueError as e:
                         log.warning(f"Exclusion Parse Error: Skipping invalid line {line_num} ('{line}') in {EXCLUDE_FILE}: {e}")
            log.info(f"Loaded {len(excluded)} exclusion rules from {EXCLUDE_FILE}.")
        except IOError as e:
            log.error(f"Could not read exclusion file {EXCLUDE_FILE}: {e}")
    return excluded

def is_excluded(ip_str, excluded_ranges):
    """Checks if a single IP address string is within the excluded ranges."""
    try:
        ip_obj = ipaddress.IPv4Address(ip_str)
        for r in excluded_ranges:
            if isinstance(r, tuple): # IP Range check
                if r[0] <= ip_obj <= r[1]:
                    return True
            elif isinstance(r, (ipaddress.IPv4Network, ipaddress.IPv6Network)): # CIDR check
                if ip_obj in r:
                    return True
    except ValueError:
        return False # Invalid IP format cannot be excluded (or included)
    except Exception as e:
        log.warning(f"Unexpected error checking exclusion for {ip_str}: {e}")
        return False # Fail safe: assume not excluded on error
    return False

def generate_ip_chunks(cidr_block_str, start_ip_str, chunk_sz=20):
    """
    Generates chunks of IP addresses for a given CIDR block,
    starting after the provided start_ip (checkpoint), and skipping excluded IPs.
    Yields lists of IP strings.
    """
    try:
        net = ipaddress.IPv4Network(cidr_block_str, strict=False)
        excluded_ranges = load_exclusions() # Load exclusions per range scan
        current_chunk = []
        start_ip = None
        start_processing = True # Assume starting from beginning unless checkpoint found

        if start_ip_str:
            try:
                start_ip = ipaddress.IPv4Address(start_ip_str)
                log.info(f"Resuming {cidr_block_str} after checkpoint: {start_ip}")
                start_processing = False # Need to seek past the checkpoint
            except ValueError:
                log.warning(f"Invalid start IP '{start_ip_str}' for {cidr_block_str}. Scanning from beginning.")
                start_ip = None

        # Using net.hosts() is generally safer as it skips network/broadcast addresses
        ip_iterator = net.hosts()

        for ip in ip_iterator:
            ip_str = str(ip)

            # --- Checkpoint Seeking Logic ---
            if not start_processing:
                if ip > start_ip:
                    log.debug(f"Reached IP {ip_str}, resuming chunk generation after checkpoint {start_ip}.")
                    start_processing = True # Start adding IPs from here
                else:
                    continue # Skip IPs until we are past the checkpoint

            # --- Process IP if past checkpoint (or if starting fresh) ---
            if start_processing:
                if is_excluded(ip_str, excluded_ranges):
                    continue # Skip excluded IPs silently or log debug

                current_chunk.append(ip_str)
                if len(current_chunk) >= chunk_sz:
                    yield current_chunk
                    current_chunk = [] # Reset chunk

        # Yield any remaining IPs in the last chunk
        if current_chunk:
            yield current_chunk

    except ipaddress.AddressValueError:
        log.error(f"Invalid CIDR format encountered: {cidr_block_str}")
    except Exception as e:
        log.error(f"Unexpected error generating chunks for {cidr_block_str}: {e}")
        import traceback
        traceback.print_exc()


# --- Main Logic ---

def populate_initial_ranges():
    """Adds ranges from the GENERATED_POOL to the database if they don't exist."""
    log.info("Populating database with initial CIDR ranges...")
    added = db.add_cidr_ranges(GENERATED_POOL)
    log.info(f"Finished initial range population. Added {added} new ranges.")

def main():
    """Main controller loop."""
    populate_initial_ranges()
    log.info("🚀 Starting NopenHeimer Controller...")
    log.info(f"[*] Config: Skip zero history ranges = {SKIP_ZERO_HISTORY_RANGES}, Rescan interval = {RESCAN_INTERVAL_HOURS} hours")
    log.info(f"[*] Config: Chunk Size = {chunk_size}, Checkpoint Interval = {CHECKPOINT_UPDATE_INTERVAL} chunks")


    while True:
        network = None # Ensure 'network' is defined in case of early loop exit/error
        try:
            next_range_info = db.get_next_range_to_scan(SKIP_ZERO_HISTORY_RANGES, RESCAN_INTERVAL_HOURS)

            if not next_range_info:
                log.info("⏸️ No ranges available to scan currently based on criteria. Sleeping for 60 seconds...")
                time.sleep(60)
                continue

            network = next_range_info["cidr_block"]
            start_ip = next_range_info["last_checkpoint_ip"] # Can be None

            log.info(f"🎯 Selected range: {network} (Resuming from: {start_ip or 'beginning'})")
            redis_client.set("current_range", network) # Update dashboard info (consider TTL?)

            total_ips_dispatched = 0
            scan_run_failed = False
            last_ip_processed_in_chunk = None # Track last IP in the *last successfully dispatched* chunk

            # Clear or initialize the Redis counter for this specific scan run
            scan_count_key = f"scan_count:{network}"
            redis_client.set(scan_count_key, 0) # Reset counter for this run

            # Estimate total chunks for progress bar (optional, can be inaccurate)
            try:
                 net_obj = ipaddress.ip_network(network)
                 # Estimate based on hosts if possible, otherwise total addresses
                 num_addr = getattr(net_obj, 'num_addresses', 0)
                 if hasattr(net_obj, 'hosts'):
                     # Calculating exact host count can be slow for large ranges
                     # Use total addresses as approximation for progress bar
                     pass
                 total_chunks_estimate = (num_addr // chunk_size) + 1 if num_addr > 0 else 1
            except Exception:
                 total_chunks_estimate = None

            log.info(f"Dispatching chunks for {network}...")
            # Create generator and wrap with tqdm
            chunk_generator = generate_ip_chunks(network, start_ip, chunk_size)
            pbar = tqdm(chunk_generator, desc=f"Scanning {network}", unit=" chunk", total=total_chunks_estimate, smoothing=0.1, mininterval=1.0) # Update progress bar less frequently

            chunk_counter = 0 # <-- Initialize counter for periodic checkpointing

            for chunk in pbar:
                if not chunk:
                    log.warning(f"Received empty chunk for {network}, stopping scan for this range.")
                    scan_run_failed = True
                    break

                # Send task to worker, including the network identifier
                try:
                    app.send_task("worker.worker.scan_ip_batch", args=[chunk, network])
                    total_ips_dispatched += len(chunk)
                    last_ip_processed_in_chunk = chunk[-1] # Track the last IP sent
                    chunk_counter += 1 # <-- Increment chunk counter

                    # --- !!! OPTIMIZED CHECKPOINT UPDATE !!! ---
                    if chunk_counter % CHECKPOINT_UPDATE_INTERVAL == 0:
                        log.debug(f"Updating checkpoint for {network} to {last_ip_processed_in_chunk} (Chunk #{chunk_counter})")
                        db.update_checkpoint(network, str(last_ip_processed_in_chunk))
                    # --- END OPTIMIZED CHECKPOINT ---

                except Exception as dispatch_error:
                    log.error(f"FATAL: Could not dispatch chunk #{chunk_counter+1} to Celery for {network}: {dispatch_error}")
                    log.error("Check Redis/Celery connection. Marking range as error.")
                    db.mark_range_error(network) # Mark range as error in DB
                    scan_run_failed = True
                    break # Stop processing this range

            pbar.close() # Close progress bar

            # --- Handle Scan Completion ---
            if scan_run_failed:
                log.error(f"❌ Scan failed prematurely for {network}. Status possibly set to 'error'.")
                # Error status should have been set in the loop if dispatch failed
            else:
                 # Scan finished successfully (either fully or reached end after resume)

                 # --- IMPORTANT: Update checkpoint one last time after loop finishes ---
                 if last_ip_processed_in_chunk: # Check if any chunk was processed at all
                     log.info(f"Updating final checkpoint for {network} to {last_ip_processed_in_chunk}")
                     db.update_checkpoint(network, str(last_ip_processed_in_chunk))
                 # --- END FINAL CHECKPOINT UPDATE ---

                 log.info(f"✅ Finished dispatching all chunks for {network}. Total IPs sent in this run: {total_ips_dispatched}.")

                 # Get the final count of servers found *during this specific run* from Redis
                 try:
                    found_count_this_run = int(redis_client.get(scan_count_key) or 0)
                 except (ValueError, TypeError, redis.RedisError) as e:
                    log.error(f"Could not get scan count from Redis key {scan_count_key}: {e}. Assuming 0.")
                    found_count_this_run = 0
                 finally:
                     try:
                        redis_client.delete(scan_count_key) # Clean up the temporary counter
                     except redis.RedisError as e:
                         log.warning(f"Failed to delete scan count key {scan_count_key}: {e}")


                 # Mark the range as completed in the database with the count found
                 db.mark_range_completed(network, found_count_this_run)


        except KeyboardInterrupt:
             log.info("\n🛑 Received Ctrl+C. Shutting down controller gracefully...")
             # If a range was being scanned, its status remains 'scanning'.
             # It will likely be picked up on next run after the RESCAN_INTERVAL_HOURS passes for its 'last_scanned_timestamp'.
             break # Exit the main loop

        except db.ConnectionError as db_conn_err: # Assuming db.py raises this on pool failure
            log.error(f"Database connection error in main loop: {db_conn_err}")
            log.error("Check PostgreSQL connection. Sleeping for 60 seconds before retry...")
            time.sleep(60)

        except redis.exceptions.ConnectionError as redis_conn_err:
            log.error(f"Redis connection error in main loop: {redis_conn_err}")
            log.error("Check Redis connection. Sleeping for 60 seconds before retry...")
            time.sleep(60)

        except Exception as e:
            log.critical(f"FATAL UNHANDLED ERROR in main loop processing range '{network}': {e}")
            import traceback
            traceback.print_exc()
            # Attempt to mark the current range as error if possible
            if network:
                 try:
                     # Ensure DB connection is attempted safely after potentially fatal error
                     log.warning(f"Attempting to mark range {network} as error due to unhandled exception.")
                     db.mark_range_error(network)
                 except Exception as mark_err:
                     log.error(f"Also failed to mark range {network} as error during exception handling: {mark_err}")
            log.critical("Sleeping for 60 seconds due to critical error...")
            time.sleep(60)


if __name__ == "__main__":
    # Ensure DB pool is ready before starting main loop
    if db.pool is None:
        log.critical("Database pool not initialized. Cannot continue. Exiting.")
        sys.exit(1)
    main()