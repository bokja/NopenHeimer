import ipaddress
import os
import random
import time
from tqdm import tqdm
from celery import Celery
import redis
from shared.config import REDIS_URL, NETWORK_POOL, CONTROLLER_CHUNK_SIZE
from shared.logger import logger

app = Celery('controller', broker=REDIS_URL)
redis_client = redis.Redis.from_url(REDIS_URL)

CHECKPOINT_DIR = "checkpoints"
EXCLUDE_FILE = "exclude.conf"
COMPLETED_FILE = "completed_ranges.txt"

os.makedirs(CHECKPOINT_DIR, exist_ok=True)

# NETWORK_POOL is now imported from shared.config
# NETWORK_POOL = [
#     "172.65.0.0/12",
#     ...
# ]

# --------------------- Helper Functions ----------------------

def load_exclusions():
    excluded = []
    if os.path.exists(EXCLUDE_FILE):
        with open(EXCLUDE_FILE, "r") as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#"):
                    continue
                if "-" in line:
                    try:
                        start, end = line.split("-")
                        excluded.append((ipaddress.IPv4Address(start.strip()), ipaddress.IPv4Address(end.strip())))
                    except:
                        continue
                else:
                    try:
                        excluded.append(ipaddress.IPv4Network(line.strip(), strict=False))
                    except:
                        continue
    return excluded

def is_excluded(ip, excluded_ranges):
    ip_obj = ipaddress.IPv4Address(ip)
    for r in excluded_ranges:
        if isinstance(r, tuple):
            if r[0] <= ip_obj <= r[1]:
                return True
        elif ip_obj in r:
            return True
    return False

def load_checkpoint_filename(network_str):
    return os.path.join(CHECKPOINT_DIR, f"{network_str.replace('/', '_')}.txt")

def load_checkpoint(network_str):
    fpath = load_checkpoint_filename(network_str)
    if os.path.exists(fpath):
        with open(fpath, "r") as f:
            return ipaddress.IPv4Address(f.read().strip())
    return None

def save_checkpoint(ip, network_str):
    with open(load_checkpoint_filename(network_str), "w") as f:
        f.write(str(ip))

def mark_range_completed(network_str):
    with open(COMPLETED_FILE, "a") as f:
        f.write(f"{network_str}\n")

def load_completed_ranges():
    if not os.path.exists(COMPLETED_FILE):
        return set()
    with open(COMPLETED_FILE, "r") as f:
        return set(line.strip() for line in f)

def has_unscanned_ips(network_str):
    net = ipaddress.IPv4Network(network_str, strict=False)
    checkpoint = load_checkpoint(network_str)
    if checkpoint and checkpoint >= net.broadcast_address:
        return False
    return True

def generate_random_cidr():
    excluded = load_exclusions()
    completed = load_completed_ranges()
    while True:
        a = random.randint(1, 223)
        b = random.randint(0, 255)
        prefix = random.choice([12, 16])
        if prefix == 12:
            network = f"{a}.{b}.0.0/12"
        else:
            network = f"{a}.{b}.0.0/16"

        try:
            net = ipaddress.IPv4Network(network, strict=False)
        except:
            continue

        if network in completed:
            continue

        if any(is_excluded(str(ip), excluded) for ip in net):
            continue

        return network

def generate_ip_chunks(network_str, chunk_size=CONTROLLER_CHUNK_SIZE):
    net = ipaddress.IPv4Network(network_str, strict=False)
    checkpoint = load_checkpoint(network_str)
    excluded = load_exclusions()
    current_chunk = []

    for ip in net:
        if checkpoint and ip <= checkpoint:
            continue
        if is_excluded(str(ip), excluded):
            continue

        current_chunk.append(str(ip))
        if len(current_chunk) >= chunk_size:
            yield current_chunk
            save_checkpoint(ip, network_str)
            current_chunk = []

    if current_chunk:
        yield current_chunk
        save_checkpoint(current_chunk[-1], network_str)

# --------------------- Main Logic ----------------------

def main():
    used_networks = set()
    logger.info("🎲 Starting controller...")

    while True:
        completed = load_completed_ranges()
        available_networks = list(set(NETWORK_POOL) - used_networks - completed)
        available_networks = [net for net in available_networks if has_unscanned_ips(net)]

        if not available_networks:
            logger.info("✅ All networks in pool completed. Switching to random IP space...")
            while True:
                rand_net = generate_random_cidr()
                if has_unscanned_ips(rand_net):
                    network = rand_net
                    break
        else:
            network = random.choice(available_networks)
            used_networks.add(network)

        logger.info(f"🚀 Scanning: {network}")
        redis_client.set("current_range", network)

        chunk_count = 0
        for chunk in tqdm(generate_ip_chunks(network, CONTROLLER_CHUNK_SIZE), desc=f"Dispatching {network}"):
            app.send_task("worker.worker.scan_ip_batch", args=[chunk])
            chunk_count += 1

        if chunk_count == 0:
            logger.warning(f"⚠️ Nothing to scan in {network}. Marking as done.")
            mark_range_completed(network)
            continue

        logger.info(f"✅ Finished scanning {network}")
        mark_range_completed(network)

if __name__ == "__main__":
    main()
