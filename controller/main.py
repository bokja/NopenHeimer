# main.py
import ipaddress
import os
import time
import math
from tqdm import tqdm
from celery import Celery
import redis
from shared.config import REDIS_URL, NETWORK_TO_SCAN # Import NETWORK_TO_SCAN from config
from worker.worker import chunk_size # Ensure chunk_size is consistent

# --- Celery and Redis Setup ---
app = Celery('controller', broker=REDIS_URL)
redis_client = redis.Redis.from_url(REDIS_URL, decode_responses=True) # Decode responses for easier handling

# --- Configuration ---
EXCLUDE_FILE = "exclude.conf"
# How often to save checkpoint (number of chunks)
CHECKPOINT_INTERVAL_CHUNKS = 50
# Key for storing checkpoints in Redis
REDIS_CHECKPOINT_KEY = "scan_checkpoints"
# Key for storing the currently scanned range
REDIS_CURRENT_RANGE_KEY = "current_range"


# --- Helper Functions ---

def load_exclusions():
    """Loads exclusion ranges from the exclude.conf file."""
    excluded = []
    if os.path.exists(EXCLUDE_FILE):
        with open(EXCLUDE_FILE, "r") as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#"):
                    continue
                try:
                    if "-" in line:
                        start, end = line.split("-")
                        excluded.append((ipaddress.IPv4Address(start.strip()), ipaddress.IPv4Address(end.strip())))
                    else:
                        # Allow specific IPs or networks
                        addr = ipaddress.ip_interface(line.strip())
                        if isinstance(addr, ipaddress.IPv4Interface):
                            excluded.append(addr.network) # Store as network even if it's /32
                        else: # Should be IPv4Network already
                            excluded.append(addr)
                except ValueError as e:
                    print(f"[!] Warning: Skipping invalid exclusion line '{line}': {e}")
                    continue
    print(f"[*] Loaded {len(excluded)} exclusion rules.")
    return excluded

def is_excluded(ip_addr, excluded_ranges):
    """Checks if a single IP address falls within any excluded range."""
    # ip_addr should be an ipaddress.IPv4Address object
    for r in excluded_ranges:
        if isinstance(r, tuple): # Range defined by start-end
            if r[0] <= ip_addr <= r[1]:
                return True
        elif isinstance(r, (ipaddress.IPv4Network, ipaddress.IPv4Interface)): # Network or single IP exclusion
             # Check network containment directly
            if isinstance(r, ipaddress.IPv4Interface): # Handle single IP (/32) correctly
                if ip_addr == r.ip: return True
            elif ip_addr in r: # Check if IP is in the network
                return True
    return False

def generate_networks_to_scan(start_block="0.0.0.0/0"):
    """
    Generates major network blocks (e.g., /8) to be scanned systematically.
    Adjust the prefixlen (e.g., 8, 10, 12) based on desired granularity.
    """
    # Using /8 as the major block size for systematic scanning
    base_net = ipaddress.ip_network(start_block, strict=False)
    # You could adjust this to /10, /12 etc. /8 is common for broad scans.
    major_block_prefix = 8
    print(f"[*] Generating target networks (breaking down {start_block} into /{major_block_prefix} blocks)")
    for network in base_net.subnets(new_prefix=major_block_prefix):
        yield str(network) # Yield network CIDR string

def get_last_scanned_ip(network_cidr):
    """Gets the last scanned IP for a network from Redis."""
    last_ip_str = redis_client.hget(REDIS_CHECKPOINT_KEY, network_cidr)
    if last_ip_str:
        try:
            return ipaddress.IPv4Address(last_ip_str)
        except ValueError:
            print(f"[!] Warning: Invalid IP address found in checkpoint for {network_cidr}: {last_ip_str}")
            return None
    return None

def save_checkpoint(network_cidr, last_ip_obj):
    """Saves the last scanned IP for a network to Redis."""
    redis_client.hset(REDIS_CHECKPOINT_KEY, network_cidr, str(last_ip_obj))
    # print(f"[*] Checkpoint saved for {network_cidr}: {last_ip_obj}") # Optional: for debugging

def is_network_fully_excluded(network, excluded_ranges):
    """Checks if the *entire* network range is covered by exclusions."""
    # This is a basic check, might not catch all edge cases with complex overlapping exclusions
    # A more robust check would involve complex interval math, but this is often sufficient.
    try:
        net = ipaddress.ip_network(network, strict=False)
        # Check if the network's start and end IPs are both excluded by the *same* rule (or covered by networks)
        start_excluded = is_excluded(net.network_address, excluded_ranges)
        end_excluded = is_excluded(net.broadcast_address, excluded_ranges)

        if start_excluded and end_excluded:
             # Further check: Ensure it's fully contained within *one* larger excluded block if possible
             for r in excluded_ranges:
                 if isinstance(r, ipaddress.IPv4Network) and net.subnet_of(r):
                     # print(f"[*] Network {network} fully contained within excluded {r}")
                     return True
                 # Simple range check (less precise for networks)
                 if isinstance(r, tuple) and r[0] <= net.network_address and net.broadcast_address <= r[1]:
                     # print(f"[*] Network {network} fully contained within excluded range {r[0]}-{r[1]}")
                     return True
        return False # Assume not fully excluded if start/end aren't clearly covered
    except ValueError:
        return True # Treat invalid networks as excluded

def generate_ip_chunks(network_cidr, start_ip, excluded_ranges, chunk_size=chunk_size):
    """Generates chunks of IPs to scan, skipping excluded ones."""
    try:
        net = ipaddress.ip_network(network_cidr, strict=False)
    except ValueError as e:
        print(f"[!] Error parsing network {network_cidr}: {e}")
        return # Stop generation for this invalid network

    current_chunk = []
    last_ip_processed = start_ip # Track the last IP actually added or skipped

    # Iterate through IPs starting from the checkpoint (or network start)
    ip_generator = net.hosts() if not start_ip else (ip for ip in net if ip > start_ip)

    for ip_obj in ip_generator:
        # Optimization: Check if current IP is potentially within an excluded range
        # This check is per-IP, ensuring we don't miss servers in partially excluded blocks
        if is_excluded(ip_obj, excluded_ranges):
            last_ip_processed = ip_obj # Mark as processed even if skipped
            continue # Skip excluded IP

        current_chunk.append(str(ip_obj))
        last_ip_processed = ip_obj # Mark as processed

        if len(current_chunk) >= chunk_size:
            yield current_chunk, last_ip_processed # Yield chunk and the last IP included/processed
            current_chunk = [] # Reset chunk

    # Yield any remaining IPs in the last chunk
    if current_chunk:
        yield current_chunk, last_ip_processed

    # After the loop, yield a special signal indicating the end, along with the final IP processed
    yield None, net.broadcast_address # Signal completion


# --- Main Logic ---

def main():
    print("🚀 Starting Systematic IP Scanner Controller...")
    exclusions = load_exclusions()
    # Determine the overall range to scan. Default to full internet or use config.
    # Using 0.0.0.0/0 implies scanning *everything* not excluded.
    # You might want to limit this initially, e.g., "1.0.0.0/8"
    initial_scan_range = NETWORK_TO_SCAN # Use the range from config
    print(f"[*] Base scanning range set to: {initial_scan_range}")


    # Iterate through major network blocks systematically
    for network_cidr in generate_networks_to_scan(initial_scan_range):

        # --- Pre-check: Skip entirely excluded networks ---
        if is_network_fully_excluded(network_cidr, exclusions):
            print(f"⏭️ Skipping fully excluded network: {network_cidr}")
            # Optionally mark as complete in checkpoint if needed, but skipping is usually enough
            save_checkpoint(network_cidr, ipaddress.ip_network(network_cidr).broadcast_address) # Mark as done
            continue

        # --- Resume or Start Scanning the Network ---
        last_scanned_ip = get_last_scanned_ip(network_cidr)
        network_obj = ipaddress.ip_network(network_cidr, strict=False)

        if last_scanned_ip and last_scanned_ip >= network_obj.broadcast_address:
            print(f"✅ Network already completed: {network_cidr}")
            continue # Move to the next network

        start_ip = last_scanned_ip # Will be None if starting fresh
        print(f"Scanning Network: {network_cidr} | Resuming from: {start_ip or 'Beginning'}")
        redis_client.set(REDIS_CURRENT_RANGE_KEY, network_cidr) # Update dashboard

        # --- Process Chunks ---
        chunk_count = 0
        total_ips_in_network = network_obj.num_addresses
        # Estimate total chunks for progress bar, considering exclusions makes this tricky
        # A rough estimate is fine for display purposes.
        estimated_chunks = math.ceil(total_ips_in_network / chunk_size) if total_ips_in_network > 0 else 1

        # Use tqdm for progress visualization
        with tqdm(total=estimated_chunks, desc=f"Dispatching {network_cidr}", unit="chk") as pbar:
            ip_chunk_generator = generate_ip_chunks(network_cidr, start_ip, exclusions, chunk_size)

            for chunk, last_ip_in_batch in ip_chunk_generator:
                if chunk is None: # End of network signal
                    # Ensure the final checkpoint reflects the end of the network
                    final_ip_to_save = last_ip_in_batch # Should be broadcast address here
                    save_checkpoint(network_cidr, final_ip_to_save)
                    print(f"\n✅ Finished dispatching for network: {network_cidr}")
                    break # Exit loop for this network

                # Dispatch the chunk to workers
                app.send_task("worker.worker.scan_ip_batch", args=[chunk])
                chunk_count += 1
                pbar.update(1) # Update progress bar

                # Save checkpoint periodically based on chunk count
                if chunk_count % CHECKPOINT_INTERVAL_CHUNKS == 0:
                    save_checkpoint(network_cidr, last_ip_in_batch)

                # Optional: small sleep to prevent overwhelming the broker if needed
                # time.sleep(0.01)

            # Final checkpoint save after the loop finishes naturally (if it wasn't triggered by the 'None' chunk)
            # This ensures the last processed IP is saved even if the loop ends mid-interval
            # Note: The 'None' chunk logic above should handle the clean completion case.
            # This is more of a safeguard.
            # We can get the last IP state from redis again in case something else updated it?
            # Or just rely on the logic inside the loop + the final 'None' chunk handling.
            # Let's rely on the loop logic + final signal for now.

        if chunk_count == 0 and not last_scanned_ip:
             print(f"⚠️ No scannable IPs found in {network_cidr} (possibly fully excluded or empty). Marking as done.")
             save_checkpoint(network_cidr, network_obj.broadcast_address) # Mark as complete

    print("🏁 All specified network ranges have been processed.")
    redis_client.set(REDIS_CURRENT_RANGE_KEY, "Completed")


if __name__ == "__main__":
    main()