from celery import Celery
import socket
import redis
from shared.logger import get_logger
from shared.config import REDIS_URL, TARGET_PORT, CONNECT_TIMEOUT, PING_MESSAGE

app = Celery('worker', broker=REDIS_URL)
redis_client = redis.Redis.from_url(REDIS_URL)
SCAN_TTL = 7 * 24 * 60 * 60

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
    if redis_client.exists(f"scanned:{ip}"):
        return None

    if not is_port_open(ip, TARGET_PORT):
        redis_client.setex(f"scanned:{ip}", SCAN_TTL, 1)
        return None

    try:
        with socket.create_connection((ip, TARGET_PORT), timeout=CONNECT_TIMEOUT) as s:
            s.sendall(PING_MESSAGE)
            response = s.recv(1024)
            if response and response.startswith(b'\xff'):
                redis_client.sadd("found_servers", ip)
                logger.info(f"Found server: {ip} - {response.hex()}")
    except Exception:
        pass

    redis_client.setex(f"scanned:{ip}", SCAN_TTL, 1)
    return None
