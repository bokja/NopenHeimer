import ipaddress
import os
import random
import time
import sys # Import sys for exit on Redis error
from tqdm import tqdm
from celery import Celery
import redis

# --- Project Setup ---
# Add project root to sys.path to allow imports from shared/worker if needed elsewhere
script_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.abspath(os.path.join(script_dir, '..'))
# sys.path.append(project_root) # Only needed if importing sibling packages directly

# --- Imports ---
# Import config first to load .env
try:
    from shared.config import REDIS_URL, CHUNK_SIZE # Import uppercase CHUNK_SIZE
    from shared.logger import get_logger
except ImportError as e:
     # Provide more context on import error
     print(f"ERROR: Failed to import from shared module ({e}). Ensure shared/ directory exists and has necessary files.", file=sys.stderr)
     print("PYTHONPATH:", os.getenv("PYTHONPATH"), file=sys.stderr)
     print("sys.path:", sys.path, file=sys.stderr)
     sys.exit(1)

# --- Initialization ---
log = get_logger("controller") # Use the logger
app = Celery('controller', broker=REDIS_URL)
try:
    # Use decode_responses for easier handling if reading keys later
    redis_client = redis.Redis.from_url(REDIS_URL, decode_responses=True)
    redis_client.ping()
    log.info(f"Controller connected to Redis at {REDIS_URL}")
except redis.exceptions.ConnectionError as e:
    log.critical(f"FATAL: Controller failed to connect to Redis: {e}")
    sys.exit(1) # Exit if Redis connection fails
except Exception as e:
    log.critical(f"FATAL: Unexpected error connecting to Redis: {e}", exc_info=True)
    sys.exit(1)


# --- Filesystem State Management (Original Logic) ---
CHECKPOINT_DIR = "checkpoints"
EXCLUDE_FILE = "exclude.conf"
COMPLETED_FILE = "completed_ranges.txt"

# Ensure checkpoint directory exists
try:
    os.makedirs(CHECKPOINT_DIR, exist_ok=True)
    log.info(f"Checkpoint directory '{CHECKPOINT_DIR}' ensured.")
except OSError as e:
     log.error(f"Error creating checkpoint directory {CHECKPOINT_DIR}: {e}. Check permissions.")
     # Decide if this is fatal? Probably should be if checkpoints can't be saved.
     # sys.exit(1)


# Original network pool
NETWORK_POOL = [
    "172.65.0.0/12", "173.0.0.0/12", "192.241.0.0/16", "144.202.0.0/16",
    "51.81.0.0/16", "5.9.0.0/16", "167.114.0.0/16", "45.13.0.0/16"
]

# --- Helper Functions (Using Logging) ---

def load_exclusions():
    """Loads exclusion rules from file."""
    excluded = []
    if os.path.exists(EXCLUDE_FILE):
        try:
            with open(EXCLUDE_FILE, "r") as f:
                for line_num, line in enumerate(f, 1):
                    line = line.strip()
                    if not line or line.startswith("#"): continue
                    try:
                        if "-" in line:
                            start, end = line.split("-")
                            excluded.append((ipaddress.IPv4Address(start.strip()), ipaddress.IPv4Address(end.strip())))
                        else:
                           excluded.append(ipaddress.IPv4Network(line.strip(), strict=False))
                    except ValueError as e:
                        log.warning(f"Exclusion Parse Error: Skipping invalid line {line_num} ('{line}') in {EXCLUDE_FILE}: {e}")
            log.info(f"Loaded {len(excluded)} exclusion rules from {EXCLUDE_FILE}.")
        except IOError as e:
            log.error(f"Could not read exclusion file {EXCLUDE_FILE}: {e}")
    else:
        log.info(f"Exclusion file '{EXCLUDE_FILE}' not found. No exclusions loaded.")
    return excluded

def is_excluded(ip_str, excluded_ranges):
    """Checks if IP string is in excluded list."""
    try:
        ip_obj = ipaddress.IPv4Address(ip_str)
        for r in excluded_ranges:
            if isinstance(r, tuple):
                if r[0] <= ip_obj <= r[1]: return True
            elif isinstance(r, (ipaddress.IPv4Network, ipaddress.IPv6Network)):
                if ip_obj in r: return True
    except ValueError: return False # Invalid IP string
    return False

def load_checkpoint_filename(network_str):
    """Generates filename for checkpoint file."""
    # Sanitize filename slightly more
    safe_network_str = network_str.replace('/', '_').replace(':', '_')
    return os.path.join(CHECKPOINT_DIR, f"{safe_network_str}.txt")

def load_checkpoint(network_str):
    """Loads the last scanned IP address object from checkpoint file."""
    fpath = load_checkpoint_filename(network_str)
    if os.path.exists(fpath):
        try:
            with open(fpath, "r") as f:
                ip_str = f.read().strip()
                if ip_str:
                    return ipaddress.IPv4Address(ip_str)
        except (IOError, ValueError) as e:
            log.warning(f"Failed to load or parse checkpoint {fpath}: {e}")
    return None

def save_checkpoint(ip_obj, network_str):
    """Saves the last scanned IP address object to checkpoint file."""
    fpath = load_checkpoint_filename(network_str)
    try:
        with open(fpath, "w") as f:
            f.write(str(ip_obj))
        log.debug(f"Saved checkpoint for {network_str} to {ip_obj}")
    except IOError as e:
        log.error(f"Error saving checkpoint for {network_str} to {ip_obj} at {fpath}: {e}")

def mark_range_completed(network_str):
    """Appends network string to the completed ranges file."""
    try:
        # Use 'a+' to create if not exists, append otherwise
        with open(COMPLETED_FILE, "a+") as f:
            # Optional: Check if already present before writing? Might slow down.
            # f.seek(0) # Go to start to read
            # if network_str not in f.read(): # Very inefficient for large files
            #    f.seek(0, 2) # Go back to end to append
            f.write(f"{network_str}\n")
        log.info(f"Marked range {network_str} as completed.")
    except IOError as e:
         log.error(f"Error marking range completed for {network_str}: {e}")

def load_completed_ranges():
    """Loads the set of completed network strings."""
    completed = set()
    if os.path.exists(COMPLETED_FILE):
        try:
            with open(COMPLETED_FILE, "r") as f:
                for line in f:
                    stripped = line.strip()
                    if stripped:
                        completed.add(stripped)
            log.info(f"Loaded {len(completed)} completed ranges.")
        except IOError as e:
            log.error(f"Error loading completed ranges from {COMPLETED_FILE}: {e}")
    else:
        log.info(f"Completed ranges file '{COMPLETED_FILE}' not found.")
    return completed

def has_unscanned_ips(network_str):
    """Checks if a network still has IPs left based on checkpoint."""
    try:
        net = ipaddress.IPv4Network(network_str, strict=False)
        checkpoint = load_checkpoint(network_str)
        if checkpoint and checkpoint >= net.broadcast_address:
            log.debug(f"Range {network_str} fully scanned based on checkpoint {checkpoint}.")
            return False
    except ValueError as e:
        log.error(f"Invalid network format '{network_str}' in has_unscanned_ips: {e}")
        return False # Treat invalid network as unscannable
    return True

def generate_random_cidr():
    """Generates a random CIDR not in exclusions or completed list."""
    # Note: May not be efficient for large exclusion/completed lists
    excluded = load_exclusions()
    completed = load_completed_ranges()
    attempts = 0
    max_attempts = 1000 # Safety break

    while attempts < max_attempts:
        attempts += 1
        a = random.randint(1, 223) # Avoid 0.x, 127.x, 224+
        b = random.randint(0, 255)
        # Favor smaller blocks for random generation?
        prefix = random.choice([16, 20, 24]) # Example: focus on /16, /20, /24
        if prefix == 12: network = f"{a}.{b & 0xF0}.0.0/12"
        elif prefix == 16: network = f"{a}.{b}.0.0/16"
        elif prefix == 20: network = f"{a}.{b}.{random.randint(0, 255) & 0xF0}.0/20"
        else: network = f"{a}.{b}.{random.randint(0, 255)}.0/24"

        try:
            net = ipaddress.IPv4Network(network, strict=False)
        except ValueError: continue

        if network in completed: continue
        # Simple check: exclude if network address itself is excluded
        if is_excluded(str(net.network_address), excluded): continue

        log.info(f"Generated random CIDR: {network}")
        return network

    log.warning("Max attempts reached generating random CIDR. Sticking to pool.")
    return None


def generate_ip_chunks(network_str, chunk_sz=CHUNK_SIZE): # Use imported CHUNK_SIZE
    """Generates chunks of IP strings, respecting checkpoints and exclusions."""
    log.debug(f"Generating chunks for {network_str} with size {chunk_sz}")
    try:
        net = ipaddress.IPv4Network(network_str, strict=False)
    except ValueError as e:
        log.error(f"Error: Invalid network format '{network_str}' in generate_ip_chunks: {e}")
        return # Stop generation

    checkpoint = load_checkpoint(network_str)
    excluded = load_exclusions()
    current_chunk = []
    last_saved_ip = checkpoint

    try:
        # Use net.hosts() to iterate only usable IPs (more efficient)
        ip_iterator = net.hosts()
    except NotImplementedError: # Fallback for network types where .hosts() isn't simple (e.g., /31, /32)
        ip_iterator = net # Iterate all if hosts() fails

    for ip_obj in ip_iterator: # Keep ip_obj as IP object
        if checkpoint and ip_obj <= checkpoint:
            continue

        ip_str = str(ip_obj) # Convert to string only for exclusion check/adding to chunk
        if is_excluded(ip_str, excluded):
            continue

        current_chunk.append(ip_str)
        if len(current_chunk) >= chunk_sz:
            yield current_chunk
            save_checkpoint(ip_obj, network_str) # Save the IP object
            last_saved_ip = ip_obj
            current_chunk = []

    if current_chunk:
        yield current_chunk
        # Save checkpoint for the final chunk using the last IP object from loop
        last_ip_in_final_chunk = ip_obj
        if last_saved_ip is None or last_ip_in_final_chunk > last_saved_ip:
             save_checkpoint(last_ip_in_final_chunk, network_str)

# --- Main Logic (Using Logging) ---

def main():
    used_networks = set()
    log.info("🎲 Starting NopenHeimer Controller (Filesystem Mode)...")

    while True:
        network = None # Ensure network is defined for logging/error handling
        try:
            completed = load_completed_ranges()
            available_networks = [
                net for net in NETWORK_POOL
                if net not in completed and net not in used_networks and has_unscanned_ips(net)
            ]

            if not available_networks:
                log.info("✅ Pool exhausted or processed this run. Trying random generation...")
                network = generate_random_cidr()
                if network is None or network in completed or not has_unscanned_ips(network):
                    log.info("⏹️ No scannable networks found (pool & random). Sleeping 5m...")
                    time.sleep(300)
                    used_networks = set() # Reset for next cycle
                    continue
            else:
                network = random.choice(available_networks)

            used_networks.add(network)
            log.info(f"🚀 Scanning: {network}")
            try:
                redis_client.setex("current_range", 300, network) # Set with TTL (e.g., 5 mins)
            except redis.RedisError as e:
                 log.warning(f"Failed to set current_range in Redis: {e}")

            chunk_count = 0
            ip_chunk_generator = generate_ip_chunks(network, CHUNK_SIZE) # Use uppercase
            pbar = tqdm(ip_chunk_generator, desc=f"Dispatching {network}", unit=" chunk", mininterval=1.0)

            for chunk in pbar:
                if not chunk: continue # Skip empty chunks if generator yields them
                try:
                    # Send task with chunk (list of IPs) and network identifier
                    app.send_task("worker.worker.scan_ip_batch", args=[chunk, network])
                    chunk_count += 1
                except Exception as e:
                     log.error(f"FATAL: Failed to send task to Celery: {e}. Check Redis. Sleeping briefly...")
                     # Consider breaking or having a retry mechanism here
                     time.sleep(5)

            pbar.close()

            # Completion Logic
            if chunk_count == 0:
                 if not has_unscanned_ips(network):
                      log.info(f"✅ Network {network} already completed based on checkpoint.")
                      mark_range_completed(network)
                 else:
                      log.warning(f"⚠️ Nothing dispatched for {network}. Not marking completed.")
            else:
                 log.info(f"✅ Finished dispatching {chunk_count} chunks for {network}. Marking completed.")
                 mark_range_completed(network)

            log.info(f"--- Cycle finished for {network} ---\n")
            # time.sleep(1) # Optional delay

        except KeyboardInterrupt:
             log.info("\n🛑 Received Ctrl+C. Shutting down controller...")
             break # Exit main loop cleanly

        except redis.exceptions.ConnectionError as e:
             log.error(f"Redis connection error in main loop: {e}. Sleeping 60s...")
             time.sleep(60)
        except Exception as e:
             log.critical(f"Unhandled error in main loop (Network: {network}): {e}", exc_info=True)
             log.critical("Sleeping for 60 seconds before continuing...")
             time.sleep(60)


if __name__ == "__main__":
    main()