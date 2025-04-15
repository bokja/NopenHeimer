import redis
import ipaddress
from worker.worker import scan_ip
from shared.config import REDIS_URL, NETWORK_TO_SCAN
from tqdm import tqdm

redis_client = redis.Redis.from_url(REDIS_URL)

def generate_new_ips():
    network = ipaddress.IPv4Network(NETWORK_TO_SCAN)
    for ip in network:
        ip_str = str(ip)
        if not redis_client.sismember("scanned_ips", ip_str):
            yield ip_str

logger = get_logger()

CHUNK_SIZE = 100
CHECKPOINT_FILE = "checkpoint.txt"


if redis_client.exists(f"scanned:{ip_str}"):
    continue  # already scanned recently


def load_checkpoint():
    if not os.path.exists(CHECKPOINT_FILE): return None
    with open(CHECKPOINT_FILE) as f:
        return ipaddress.IPv4Address(f.read().strip())

def save_checkpoint(ip):
    with open(CHECKPOINT_FILE, "w") as f:
        f.write(str(ip))

def chunked_ips():
    net = ipaddress.IPv4Network(NETWORK_TO_SCAN, strict=False)
    checkpoint = load_checkpoint()
    for ip in net:
        if checkpoint and ip <= checkpoint: continue
        yield str(ip)

def main():
    ip_gen = chunked_ips()
    batch = []
    total = 0
    pbar = tqdm(desc="Scanning", unit="ip")

    for ip in ip_gen:
        batch.append(ip)
        if len(batch) < CHUNK_SIZE: continue

        results = [scan_ip.delay(ip) for ip in batch]

        for r in results:
            try:
                output = r.get(timeout=5)
                if output:
                    ip, data = output
                    logger.info(f"[✓] {ip} - {data}")
            except Exception:
                pass

        total += len(batch)
        pbar.update(len(batch))
        save_checkpoint(batch[-1])
        batch = []

        time.sleep(5)  # pause between batches

    print(f"Dispatched {total} IPs.")

if __name__ == "__main__":
    main()
