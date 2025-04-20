# controller/main.py
import ipaddress
import os
import random
import time
import sys
from tqdm import tqdm
from celery import Celery
import redis

# Adjust import path based on your structure
# Assuming db.py is accessible, e.g., in a shared folder
script_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.abspath(os.path.join(script_dir, '..'))
sys.path.append(project_root)

from shared.config import REDIS_URL
from shared import db # Use the updated db module
from worker.worker import chunk_size # Get chunk_size from worker config

# --- Configuration ---
app = Celery('controller', broker=REDIS_URL)
redis_client = redis.Redis.from_url(REDIS_URL)

INITIAL_NETWORK_POOL = [
    "172.65.0.0/12", # Note: /12 is large, consider breaking down
    "173.0.0.0/12",
    "192.241.0.0/16",
    "144.202.0.0/16",
    "51.81.0.0/16",
    "5.9.0.0/16",
    "167.114.0.0/16",
    "45.13.0.0/16"
    # Add more or generate programmatically
]
# Consider generating /16s within the /12s for better granularity
GENERATED_POOL = []
for block in INITIAL_NETWORK_POOL:
    net = ipaddress.ip_network(block, strict=False)
    if net.prefixlen < 16:
        GENERATED_POOL.extend([str(sub_net) for sub_net in net.subnets(new_prefix=16)])
    else:
        GENERATED_POOL.append(str(net))

EXCLUDE_FILE = "exclude.conf" # Keep exclusion file for now, or move to DB

SKIP_ZERO_HISTORY_RANGES = os.getenv("SKIP_ZERO_HISTORY", "false").lower() == "true"
RESCAN_INTERVAL_HOURS = int(os.getenv("RESCAN_INTERVAL_HOURS", "24"))

# --- Helper Functions ---

def load_exclusions():
    # (Keep existing exclusion loading logic for now)
    # ... (same as before)
    excluded = []
    if os.path.exists(EXCLUDE_FILE):
        with open(EXCLUDE_FILE, "r") as f:
            # ... (rest of parsing logic) ...
            pass # Replace pass with actual parsing
    return excluded

def is_excluded(ip, excluded_ranges):
    # (Keep existing logic)
    # ... (same as before)
    ip_obj = ipaddress.IPv4Address(ip)
    # ... (rest of checking logic) ...
    return False # Replace with actual checking

def generate_ip_chunks(cidr_block_str, start_ip_str, chunk_size=20):
    """Generates IP chunks for a given CIDR, starting after a checkpoint."""
    try:
        net = ipaddress.IPv4Network(cidr_block_str, strict=False)
        excluded = load_exclusions() # Load exclusions each time (or cache)
        current_chunk = []
        start_ip = None
        if start_ip_str:
            try:
                start_ip = ipaddress.IPv4Address(start_ip_str)
            except ValueError:
                print(f"[Warning] Invalid start IP '{start_ip_str}' for {cidr_block_str}. Starting from beginning.")
                start_ip = None

        print(f"[Chunker] Generating chunks for {cidr_block_str}, starting after {start_ip}")

        ip_iterator = net.hosts() # Use hosts() to avoid network/broadcast unless intended

        # If start_ip exists, advance iterator past it
        if start_ip:
            try:
                # Consume iterator until we pass the start_ip
                # This can be slow for huge ranges, but necessary
                for ip in ip_iterator:
                   if ip > start_ip:
                       # Found the first IP after the checkpoint
                       if not is_excluded(str(ip), excluded):
                            current_chunk.append(str(ip))
                       break # Start processing from here
                else:
                    # Reached end of iterator while seeking, no IPs left after checkpoint
                     print(f"[Chunker] No IPs left to scan in {cidr_block_str} after checkpoint {start_ip}")
                     yield [] # Yield empty to signal completion immediately
                     return
            except Exception as e:
                 print(f"[Chunker Error] Error seeking start IP {start_ip} in {cidr_block_str}: {e}")
                 # Decide how to handle: skip range, start from beginning? For now, yield empty.
                 yield []
                 return


        # Process remaining IPs
        last_ip_in_batch = None
        for ip in ip_iterator:
            if is_excluded(str(ip), excluded):
                continue

            current_chunk.append(str(ip))
            if len(current_chunk) >= chunk_size:
                yield current_chunk
                last_ip_in_batch = ip # Keep track of the last IP yielded
                # Update checkpoint in DB - Do this *after* successfully sending the task
                # db.update_checkpoint(cidr_block_str, str(last_ip_in_batch)) # Moved after send_task
                current_chunk = []

        # Yield any remaining IPs in the last chunk
        if current_chunk:
            yield current_chunk
            last_ip_in_batch = ipaddress.IPv4Address(current_chunk[-1])
            # Update checkpoint for the last chunk
            # db.update_checkpoint(cidr_block_str, str(last_ip_in_batch)) # Moved after send_task

    except ipaddress.AddressValueError:
        print(f"[Error] Invalid CIDR format: {cidr_block_str}")
        yield [] # Yield empty on error
    except Exception as e:
        print(f"[Error] Unexpected error generating chunks for {cidr_block_str}: {e}")
        yield [] # Yield empty on error


# --- Main Logic ---

def populate_initial_ranges():
    """Add ranges from the pool to the database if they don't exist."""
    print("Populating initial CIDR ranges...")
    db.add_cidr_ranges(GENERATED_POOL) # Use the potentially expanded list

def main():
    populate_initial_ranges()
    print("🚀 Starting NopenHeimer Controller...")
    print(f"Config: Skip zero history ranges = {SKIP_ZERO_HISTORY_RANGES}, Rescan interval = {RESCAN_INTERVAL_HOURS} hours")

    while True:
        next_range_info = db.get_next_range_to_scan(SKIP_ZERO_HISTORY_RANGES, RESCAN_INTERVAL_HOURS)

        if not next_range_info:
            print("⏸️ No ranges available to scan currently. Sleeping for 60 seconds...")
            time.sleep(60)
            continue

        network = next_range_info["cidr_block"]
        start_ip = next_range_info["last_checkpoint_ip"]

        print(f"🎯 Selected range: {network} (Resuming from: {start_ip or 'beginning'})")
        redis_client.set("current_range", network) # Update dashboard info

        total_ips_dispatched = 0
        found_in_this_scan = 0 # Track count for this specific scan instance
        scan_failed = False
        last_ip_processed = start_ip # Keep track locally

        try:
            # Use tqdm for progress bar
            chunk_generator = generate_ip_chunks(network, start_ip, chunk_size)
            # Estimate total chunks (very rough, doesn't account for exclusions/checkpoint)
            try:
                 net_obj = ipaddress.ip_network(network)
                 total_potential_ips = net_obj.num_addresses
                 # Adjust if resuming
                 if start_ip:
                     start_int = int(ipaddress.ip_address(start_ip))
                     net_start_int = int(net_obj.network_address)
                     # This is approximate, doesn't account for non-host addresses if scanning full net
                     processed_ips = max(0, start_int - net_start_int)
                     total_potential_ips = max(0, total_potential_ips - processed_ips)

                 total_chunks_estimate = (total_potential_ips // chunk_size) + 1
            except:
                 total_chunks_estimate = None # Cannot estimate

            print(f"Dispatching chunks for {network}...")
            pbar = tqdm(chunk_generator, desc=f"Scanning {network}", unit="chunk", total=total_chunks_estimate)

            for chunk in pbar:
                if not chunk: # Handle empty chunk if generator signals error/completion early
                     if total_ips_dispatched == 0 and start_ip:
                         print(f"[Info] No IPs found after checkpoint {start_ip} for {network}.")
                         # Mark as completed immediately if we resumed and found nothing after checkpoint
                     elif not chunk and total_ips_dispatched > 0:
                         # Normal end of iteration after yielding chunks
                         pass
                     else:
                         # Empty chunk yielded unexpectedly at the start or after error
                         print(f"[Warning] Empty chunk received for {network}, potentially an error.")
                         scan_failed = True # Assume failure if unexpected empty chunk
                     break # Exit the loop for this network

                # Send task to worker
                try:
                    # Pass the network (cidr_block) to the worker task
                    app.send_task("worker.worker.scan_ip_batch", args=[chunk, network])
                    total_ips_dispatched += len(chunk)
                    last_ip_in_chunk = chunk[-1]
                    last_ip_processed = last_ip_in_chunk # Update our local tracker

                    # Update checkpoint *after* successful dispatch
                    db.update_checkpoint(network, str(last_ip_in_chunk))

                    # Optional: Add a small delay to prevent overwhelming the broker/workers
                    # time.sleep(0.01)

                except Exception as dispatch_error:
                    print(f"[FATAL] Could not dispatch chunk to Celery for {network}: {dispatch_error}")
                    print("Check Redis/Celery connection. Stopping scan for this range.")
                    scan_failed = True
                    db.mark_range_error(network) # Mark range as error
                    break # Stop processing this range

            pbar.close()

            if scan_failed:
                 print(f"❌ Scan failed for {network}.")
                 # Error status already set in dispatch loop if needed
            elif total_ips_dispatched == 0 and not start_ip:
                 print(f"⚠️ No dispatchable IPs found in {network} (check exclusions?). Marking completed.")
                 # Mark completed with 0 found if nothing was ever dispatched (and wasn't a resume)
                 db.mark_range_completed(network, 0)
            elif total_ips_dispatched == 0 and start_ip:
                 print(f"✅ Reached end of {network} after resuming from {start_ip}. Marking completed.")
                 # If we resumed and dispatched nothing, it means we were already done.
                 # We need the count from *before* this resume. How to get it?
                 # Easiest: Don't update count on resume completion, let the previous count stand.
                 # Or, require workers to report back count (complex). Let's stick to simple.
                 # Re-fetch current count before marking complete? No, that's not right either.
                 # DECISION: When marking complete after resuming with 0 dispatched, DO NOT update the count.
                 db.mark_range_completed(network, -1) # Use -1 or similar sentinel? No, let's just update status/time.
                 # Refined approach: Create a function `mark_range_scan_finished` that only updates status/time.
                 # Let's modify `mark_range_completed` for now. It resets checkpoint.
                 # TODO: Rethink how to accurately get `found_count_in_scan` reliably.
                 # Workaround: Use Redis counter per block, read it here.
                 found_count_in_scan = int(redis_client.get(f"scan_count:{network}") or 0)
                 redis_client.delete(f"scan_count:{network}") # Clear counter
                 db.mark_range_completed(network, found_count_in_scan)


            else:
                 # Scan finished normally (dispatched IPs)
                 print(f"✅ Finished dispatching for {network}. Total IPs: {total_ips_dispatched}.")
                 # Get final count from Redis counter updated by workers
                 found_count_in_scan = int(redis_client.get(f"scan_count:{network}") or 0)
                 redis_client.delete(f"scan_count:{network}") # Clear counter
                 db.mark_range_completed(network, found_count_in_scan)


        except KeyboardInterrupt:
             print("\n🛑 Received Ctrl+C. Shutting down controller.")
             # Optionally mark the current range as pending or error for restart?
             # For simplicity, current lock means it might be restarted on next run.
             break # Exit the main loop
        except Exception as e:
            print(f"[FATAL ERROR] Unhandled exception during scan loop for {network}: {e}")
            import traceback
            traceback.print_exc()
            # Mark the range as error so it doesn't get stuck
            db.mark_range_error(network)
            print("Sleeping for 30 seconds before trying next range...")
            time.sleep(30)


if __name__ == "__main__":
    main()