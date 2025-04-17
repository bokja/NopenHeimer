# controller/main.py
import ipaddress
import os
import random
import time
from tqdm import tqdm
from celery import Celery
import redis
from shared.config import REDIS_URL
from worker.worker import chunk_size

app = Celery('controller', broker=REDIS_URL)
redis_client = redis.Redis.from_url(REDIS_URL)

CHECKPOINT_DIR = "checkpoints"
EXCLUDE_FILE = "exclude.conf"

os.makedirs(CHECKPOINT_DIR, exist_ok=True)

# Define a list of networks you want to scan randomly from
NETWORK_POOL = [
    "172.65.0.0/12",
    "173.0.0.0/12",
    "45.13.0.0/16",
    "192.241.0.0/16",
    "144.202.0.0/16",
    "51.81.0.0/16",
    "5.9.0.0/16",
    "167.114.0.0/16"
    # Add more known MC hosters here
]

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
                        start_ip, end_ip = line.split("-")
                        start = ipaddress.IPv4Address(start_ip.strip())
                        end = ipaddress.IPv4Address(end_ip.strip())
                        excluded.append((start, end))
                    except Exception as e:
                        print(f"[!] Invalid range '{line}': {e}")
                else:
                    try:
                        excluded.append(ipaddress.IPv4Network(line, strict=False))
                    except Exception as e:
                        print(f"[!] Invalid CIDR '{line}': {e}")
    return excluded

def is_excluded(ip, excluded_ranges):
    ip_obj = ipaddress.IPv4Address(ip)
    for item in excluded_ranges:
        if isinstance(item, tuple):
            if item[0] <= ip_obj <= item[1]:
                return True
        elif ip_obj in item:
            return True
    return False

def load_checkpoint_filename(network_str):
    return os.path.join(CHECKPOINT_DIR, f"{network_str.replace('/', '_')}.txt")

def load_checkpoint(network_str):
    checkpoint_file = load_checkpoint_filename(network_str)
    if os.path.exists(checkpoint_file):
        with open(checkpoint_file, "r") as f:
            return ipaddress.IPv4Address(f.read().strip())
    return None

def save_checkpoint(ip, network_str):
    with open(load_checkpoint_filename(network_str), "w") as f:
        f.write(str(ip))

def generate_ip_chunks(network_str, chunk_size=20):
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

def main():
    used_networks = set()
    print("🎲 Starting random IP scanning...")

    while True:
        available_networks = list(set(NETWORK_POOL) - used_networks)
        available_networks = [
            net for net in available_networks
            if has_unscanned_ips(net)
        ]

        
        if not available_networks:
            print("✅ All networks completed. Shuffling and restarting...")
            used_networks.clear()
            available_networks = list(NETWORK_POOL)

        network = random.choice(available_networks)
        used_networks.add(network)

        print(f"🚀 Scanning: {network}")
        redis_client.set("current_range", network)

        chunk_count = 0
        for chunk in tqdm(generate_ip_chunks(network, chunk_size), desc=f"Dispatching {network}"):
            app.send_task("worker.worker.scan_ip_batch", args=[chunk])
            chunk_count += 1

        if chunk_count == 0:
            print(f"⚠️ Nothing to scan in {network}. Skipping...")
            continue  # move on to another random network


        print(f"✅ Finished scanning {network}\n")

def has_unscanned_ips(network_str):
    net = ipaddress.IPv4Network(network_str, strict=False)
    checkpoint = load_checkpoint(network_str)
    if checkpoint and checkpoint >= net.broadcast_address:
        return False
    return True


if __name__ == "__main__":
    main()
