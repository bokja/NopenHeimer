# controller/main.py
import ipaddress
import time
import os
from tqdm import tqdm
from celery import Celery
from shared import config
from shared.config import REDIS_URL
from worker.worker import chunk_size
import redis

app = Celery('controller', broker=REDIS_URL)

NETWORK_TO_SCAN = config.NETWORK_TO_SCAN  # Full internet scan
CHECKPOINT_FILE = "checkpoint.txt"
EXCLUDE_FILE = "exclude.conf"

def load_checkpoint():
    if os.path.exists(CHECKPOINT_FILE):
        with open(CHECKPOINT_FILE, "r") as f:
            return ipaddress.IPv4Address(f.read().strip())
    return None

def save_checkpoint(ip):
    with open(CHECKPOINT_FILE, "w") as f:
        f.write(str(ip))

def load_exclusions():
    excluded = []
    if os.path.exists(EXCLUDE_FILE):
        with open(EXCLUDE_FILE, "r") as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#"):
                    continue
                try:
                    if '-' in line:
                        # Handle range format: 1.2.3.4-1.2.3.255
                        start_ip, end_ip = map(str.strip, line.split('-'))
                        start_ip = ipaddress.IPv4Address(start_ip)
                        end_ip = ipaddress.IPv4Address(end_ip)
                        excluded.append((start_ip, end_ip))  # Mark it as a tuple
                    else:
                        # Handle CIDR format
                        excluded.append(ipaddress.IPv4Network(line, strict=False))
                except Exception as e:
                    print(f"[!] Invalid exclude range '{line}': {e}")
    return excluded



def is_excluded(ip, excluded_ranges):
    ip = ipaddress.IPv4Address(ip)
    for rule in excluded_ranges:
        if isinstance(rule, tuple):
            # IP range: (start_ip, end_ip)
            if rule[0] <= ip <= rule[1]:
                return True
        elif ip in rule:
            return True
    return False


def generate_ip_chunks(network, chunk_size=20):
    checkpoint = load_checkpoint()
    network = ipaddress.IPv4Network(network, strict=False)
    excluded = load_exclusions()

    current_chunk = []

    for ip in network:
        if checkpoint and ip <= checkpoint:
            continue
        if is_excluded(ip, excluded):
            continue

        current_chunk.append(str(ip))
        if len(current_chunk) >= chunk_size:
            yield current_chunk
            save_checkpoint(ip)
            current_chunk = []

    if current_chunk:
        yield current_chunk
        save_checkpoint(ipaddress.IPv4Address(current_chunk[-1]))

def main():
    print(f"Dispatching IP scan batches from {NETWORK_TO_SCAN}")
    redis_client = redis.Redis.from_url(REDIS_URL)
    redis_client.set("current_range", NETWORK_TO_SCAN)
    total_dispatched = 0
    for chunk in tqdm(generate_ip_chunks(NETWORK_TO_SCAN, chunk_size), desc="Dispatching"):
        app.send_task("worker.worker.scan_ip_batch", args=[chunk])
        total_dispatched += len(chunk)
    print(f"Dispatched {total_dispatched} IPs total.")

if __name__ == "__main__":
    main()
