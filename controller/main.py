import ipaddress
import redis
import time
from tqdm import tqdm
from worker.worker import scan_ip
from shared.config import NETWORK_TO_SCAN, REDIS_URL

redis_client = redis.Redis.from_url(REDIS_URL)

CHUNK_SIZE = 100

def generate_new_ips():
    network = ipaddress.IPv4Network(NETWORK_TO_SCAN)
    for ip in network:
        ip_str = str(ip)
        if redis_client.exists(f"scanned:{ip_str}"):
            continue
        yield ip_str

def main():
    ip_gen = generate_new_ips()
    batch = []
    total = 0
    pbar = tqdm(desc="Scanning", unit="ip")

    for ip in ip_gen:
        batch.append(ip)
        if len(batch) < CHUNK_SIZE:
            continue

        results = [scan_ip.delay(ip) for ip in batch]
        total += len(batch)
        pbar.update(len(batch))
        batch = []
        time.sleep(5)

    print(f"Dispatched {total} IPs.")

if __name__ == "__main__":
    main()