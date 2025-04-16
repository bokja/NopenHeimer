import socket
from celery import Celery
import redis
from shared.config import REDIS_URL

app = Celery("worker", broker=REDIS_URL)

redis_client = redis.Redis.from_url(REDIS_URL)
target_port = 25565
timeout = 0.3
chunk_size = 100  # Also used in controller

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

@app.task(name="worker.worker.scan_ip_batch")
def scan_ip_batch(ip_list):
    found = 0
    for ip in ip_list:
        if not is_port_open(ip):
            continue

        response = ping_minecraft(ip)
        if response:
            redis_client.sadd("found_servers", ip)
            print(f"[+] Found Minecraft server: {ip}")
            found += 1

    timestamp = int(time.time())
    pipe = redis_client.pipeline()

    # Track total counts
    pipe.incrby("stats:total_scanned", len(ip_list))
    pipe.incrby("stats:total_found", found)

    # Add to sorted set for IPs/sec calc
    pipe.zadd("stats:scans", {timestamp: timestamp})  # one per batch

    # Optionally track active workers (hostname-based)
    import socket
    hostname = socket.gethostname()
    pipe.setex(f"stats:worker:{hostname}", 90, "online")  # expire after 90s

    pipe.execute()

