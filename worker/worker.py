import socket
import redis
from celery import Celery
from shared.logger import get_logger
from shared.config import REDIS_URL, TARGET_PORT, CONNECT_TIMEOUT, PING_MESSAGE

app = Celery('worker', broker=REDIS_URL)

# Connect to Redis directly
redis_client = redis.Redis.from_url(REDIS_URL)

# Avoid burst scanning
app.conf.task_annotations = {
    'worker.scan_ip': {'rate_limit': '5/s'}
}

logger = get_logger()

def is_port_open(ip, port):
    try:
        with socket.create_connection((ip, port), timeout=CONNECT_TIMEOUT):
            return True
    except Exception:
        return False

@app.task(name="worker.scan_ip")
def scan_ip(ip):


    SCAN_TTL = 7 * 24 * 60 * 60  # 7 days in seconds

    # Replace redis_client.sadd("scanned_ips", ip) with:
    redis_client.setex(f"scanned:{ip}", SCAN_TTL, 1)

    # Skip if already scanned
    if redis_client.sismember("scanned_ips", ip):
        return None

    # Fast check: is port open?
    if not is_port_open(ip, TARGET_PORT):
        redis_client.sadd("scanned_ips", ip)  # Save dead scan too
        return None

    # Minecraft ping
    try:
        with socket.create_connection((ip, TARGET_PORT), timeout=CONNECT_TIMEOUT) as s:
            s.sendall(PING_MESSAGE)
            response = s.recv(1024)
            if response and response.startswith(b'\xff'):
                redis_client.sadd("found_servers", ip)
                redis_client.sadd("scanned_ips", ip)
                logger.info(f"Found server: {ip} - {response.hex()}")
                return ip, response.hex()
    except Exception:
        pass

    redis_client.sadd("scanned_ips", ip)  # Mark scanned even if failed
    return None
