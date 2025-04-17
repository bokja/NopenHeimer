import socket
import time
import uuid
import redis
from celery import Celery
from shared.config import REDIS_URL
from tools.db import insert_server_info  # PostgreSQL helper
from tools.mc_ping import ping_server  # Unified Minecraft ping

# Initialize Celery and Redis
app = Celery("worker", broker=REDIS_URL)
redis_client = redis.Redis.from_url(REDIS_URL)

# Configuration
target_port = 25565
timeout = 0.3
chunk_size = 20  # used by controller too

def is_port_open(ip, port=target_port):
    try:
        with socket.create_connection((ip, port), timeout=timeout):
            return True
    except Exception:
        return False

@app.task(name="worker.worker.scan_ip_batch")
def scan_ip_batch(ip_list):
    hostname = socket.gethostname()
    found = 0
    timestamp = int(time.time())

    print(f"[{hostname}] Scanning {len(ip_list)} IPs...")

    for ip in ip_list:
        if not is_port_open(ip):
            continue

        result = ping_server(ip)
        if result:
            motd = result.get("motd") or ""
            players_online = result.get("players_online") or 0
            players_max = result.get("players_max") or 0
            version = result.get("version") or "unknown"
            player_names = result.get("player_names") or []

            print(f"[{hostname}] [+] {ip} - {motd} [{players_online}/{players_max}] - {version}")
            redis_client.sadd("found_servers", ip)
            found += 1

            try:
                insert_server_info(
                    ip=ip,
                    motd=motd,
                    players_online=players_online,
                    players_max=players_max,
                    version=version,
                    player_names=player_names
                )
            except Exception as e:
                print(f"[!] Failed to insert {ip} into DB: {e}")

        else:
            print(f"[{hostname}] [-] No MC ping from {ip} (port open)")
            
            # Insert as offline server (port open, no response)
            try:
                insert_server_info(
                    ip=ip,
                    motd="[OFFLINE]",
                    players_online=0,
                    players_max=0,
                    version="unknown",
                    player_names=[]
                )
                print(f"[{hostname}] [•] Offline server recorded: {ip}")
            except Exception as e:
                print(f"[!] Failed to insert offline {ip} into DB: {e}")


    # Redis Stats
    pipe = redis_client.pipeline()
    pipe.incrby("stats:total_scanned", len(ip_list))
    pipe.incrby("stats:total_found", found)
    pipe.zadd("stats:scans", {f"{timestamp}:{len(ip_list)}:{uuid.uuid4()}": timestamp})
    pipe.setex(f"stats:worker:{hostname}", 90, "online")
    pipe.execute()

    print(f"[{hostname}] Finished. Found {found}, scanned {len(ip_list)}.")
