from celery import Celery
import socket
import redis
from shared.config import REDIS_URL

app = Celery("worker", broker=REDIS_URL)
redis_client = redis.Redis.from_url(REDIS_URL)

TARGET_PORT = 25565
PING_MESSAGE = b'\xfe'
CONNECT_TIMEOUT = 1.0  # seconds

@app.task
def scan_ip(ip_str):
    # Quick port check to avoid full connection
    if not is_port_open(ip_str, TARGET_PORT):
        return None

    try:
        with socket.create_connection((ip_str, TARGET_PORT), timeout=CONNECT_TIMEOUT) as s:
            s.sendall(PING_MESSAGE)
            response = s.recv(1024)

            if response and response.startswith(b'\xff'):
                redis_client.sadd("found_servers", ip_str)
                return ip_str
    except Exception:
        pass

    return None

def is_port_open(ip, port):
    """Lightweight port availability check before full scan."""
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.settimeout(0.3)  # ⚡ very quick check
            result = sock.connect_ex((ip, port))
            return result == 0  # 0 means success
    except Exception:
        return False
