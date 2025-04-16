# controller/main.py
import ipaddress
import time
import os
from tqdm import tqdm
from celery import Celery
from shared.config import REDIS_URL
from worker.worker import chunk_size

app = Celery('controller', broker=REDIS_URL)

NETWORK_TO_SCAN = "172.65.0.0/12"
CHECKPOINT_FILE = "checkpoint.txt"

def load_checkpoint():
    if os.path.exists(CHECKPOINT_FILE):
        with open(CHECKPOINT_FILE, "r") as f:
            return ipaddress.IPv4Address(f.read().strip())
    return None

def save_checkpoint(ip):
    with open(CHECKPOINT_FILE, "w") as f:
        f.write(str(ip))

def generate_ip_chunks(network, chunk_size=20):
    checkpoint = load_checkpoint()
    network = ipaddress.IPv4Network(network, strict=False)

    current_chunk = []
    for ip in network:
        if checkpoint and ip <= checkpoint:
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
    total_dispatched = 0
    for chunk in tqdm(generate_ip_chunks(NETWORK_TO_SCAN, chunk_size), desc="Dispatching"):
        app.send_task("worker.worker.scan_ip_batch", args=[chunk])
        total_dispatched += len(chunk)
    print(f"Dispatched {total_dispatched} IPs total.")

if __name__ == "__main__":
    main()
