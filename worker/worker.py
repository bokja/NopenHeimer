import socket
import time
from celery import Celery
import redis
from shared.config import REDIS_URL

app = Celery("worker", broker=REDIS_URL)
redis_client = redis.Redis.from_url(REDIS_URL)

target_port = 25565
timeout = 0.3
chunk_size = 100

def is_port_open(ip, port=target_port):
    try:
        with socket.create_connection((ip, port), timeout=timeout):
            return True
    except Exception:
        return False

def ping_minecraft(ip):
    try:
        with socket.create_connection((ip, target_port), timeout=timeout) as s:
            s.sendall(b'\xfe')  # Legacy ping
            response = s.recv(1024)
            if response and response.startswith(b'\xff'):
                return response
    except Exception:
        pass
    return None

import random

@app.task(name="worker.worker.scan_ip_batch")
def scan_ip_batch(ip_list):
    hostname = socket.gethostname()
    found = 0

    print(f"[{hostname}] Scanning {len(ip_list)} IPs...")

    for ip in ip_list:
        if not is_port_open(ip):
            continue

        response = ping_minecraft(ip)
        if response:
            redis_client.sadd("found_servers", ip)
            print(f"[{hostname}] [+] Found: {ip}")
            found += 1

    # Stats
    timestamp = int(time.time())
    pipe = redis_client.pipeline()
    pipe.incrby("stats:total_scanned", len(ip_list))
    pipe.incrby("stats:total_found", found)
    pipe.zadd("stats:scans", {timestamp: timestamp})
    pipe.setex(f"stats:worker:{hostname}", 90, "online")
    pipe.execute()

    print(f"[{hostname}] Finished. Found {found}, scanned {len(ip_list)}.")

    # ✅ Ratelimit between batches
    delay = random.uniform(1, 3)
    print(f"[{hostname}] Sleeping for {delay:.2f}s before next batch")
    time.sleep(delay)
