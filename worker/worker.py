from celery import Celery
import socket
import redis
import time
import os
from shared.config import REDIS_URL

app = Celery("worker", broker=REDIS_URL)
redis_client = redis.Redis.from_url(REDIS_URL)

TARGET_PORT = 25565
PING_MESSAGE = b'\xfe'
CONNECT_TIMEOUT = 1.0

WORKER_ID = os.getenv("WORKER_ID", os.uname()[1])  # EC2 hostname or Docker container ID

@app.task(name="worker.worker.scan_ip")
def scan_ip(ip_str):
    now = int(time.time())

    # Log every scan timestamp to sorted set for IPS calculation
    redis_client.zadd("stats:scans", {ip_str: now})
    redis_client.incr("stats:total_scanned")
    redis_client.hincrby(f"stats:worker:{WORKER_ID}", "scanned", 1)
    redis_client.hset(f"stats:worker:{WORKER_ID}", "last_seen", now)

    # Quick port check before deeper scan
    if not is_port_open(ip_str, TARGET_PORT):
        return None

    try:
        with socket.create_connection((ip_str, TARGET_PORT), timeout=CONNECT_TIMEOUT) as s:
            s.sendall(PING_MESSAGE)
            response = s.recv(1024)

            if response and response.startswith(b'\xff'):
                redis_client.sadd("found_servers", ip_str)
                redis_client.incr("stats:total_found")
                return ip_str
    except Exception:
        pass

    return None

def is_port_open(ip, port):
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.settimeout(0.1)
            return sock.connect_ex((ip, port)) == 0
    except Exception:
        return False

app.autodiscover_tasks(['worker'])  # Ensures Celery can find tasks in worker module
