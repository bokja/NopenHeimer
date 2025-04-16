import socket
import time
from celery import Celery
import redis
from shared.config import REDIS_URL
from tools.db import insert_server_info  # PostgreSQL helper
import uuid

# Initialize Celery
app = Celery("worker", broker=REDIS_URL)
redis_client = redis.Redis.from_url(REDIS_URL)

# Config
target_port = 25565
timeout = 0.3
chunk_size = 20  # Defined here for controller as well

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

def parse_legacy_ping(response):
    try:
        data = response.decode("utf-16be")[3:].split("§")
        if len(data) >= 5:
            motd = data[0]
            players_online = int(data[1])
            players_max = int(data[2])
            version = data[3]
            player_names = data[4] if data[4] else None
            return {
                "motd": motd,
                "players_online": players_online,
                "players_max": players_max,
                "version": version,
                "player_names": player_names
            }
    except Exception:
        pass
    return None

@app.task(name="worker.worker.scan_ip_batch")
def scan_ip_batch(ip_list):
    hostname = socket.gethostname()
    found = 0
    timestamp = int(time.time())

    print(f"[{hostname}] Scanning {len(ip_list)} IPs...")

    for ip in ip_list:
        if not is_port_open(ip):
            continue

        response = ping_minecraft(ip)
        if response:
            parsed = parse_legacy_ping(response)
            redis_client.sadd("found_servers", ip)
            found += 1

            if parsed:
                print(f"[{hostname}] [+] {ip} - {parsed['motd']} [{parsed['players_online']}/{parsed['players_max']}]")

                # Save to PostgreSQL
                insert_server_info(
                    ip=ip,
                    motd=parsed.get("motd"),
                    players_online=parsed.get("players_online"),
                    players_max=parsed.get("players_max"),
                    version=parsed.get("version"),
                    player_names=parsed.get("player_names")
                )
            else:
                print(f"[{hostname}] [+] Found: {ip}")

    # Redis stats tracking
    pipe = redis_client.pipeline()
    pipe.incrby("stats:total_scanned", len(ip_list))
    pipe.incrby("stats:total_found", found)
    pipe.zadd("stats:scans", {f"{timestamp}:{len(ip_list)}:{uuid.uuid4()}": timestamp})

    pipe.setex(f"stats:worker:{hostname}", 90, "online")
    pipe.execute()

    print(f"[{hostname}] Finished. Found {found}, scanned {len(ip_list)}.")
