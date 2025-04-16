import socket
import time
import json
from celery import Celery
import redis
from shared.config import REDIS_URL

app = Celery("worker", broker=REDIS_URL)
redis_client = redis.Redis.from_url(REDIS_URL)

target_port = 25565
timeout = 0.3
chunk_size = 20

def is_port_open(ip, port=target_port):
    try:
        with socket.create_connection((ip, port), timeout=timeout):
            return True
    except Exception:
        return False

def query_minecraft_status(ip, port=target_port):
    try:
        with socket.create_connection((ip, port), timeout=timeout) as s:
            s.sendall(b'\xFE\x01')
            response = s.recv(512)
            if not response or not response.startswith(b'\xff'):
                return None
            
            # Clean legacy response
            data = response[3:].decode('utf-16be', errors='ignore').split("\x00")
            if len(data) >= 6:
                return {
                    "description": {"text": data[3]},
                    "players": {
                        "online": int(data[4]),
                        "max": int(data[5]),
                        "sample": []  # Legacy ping doesn't support player names
                    },
                    "version": {"name": data[2]}
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

        response = query_minecraft_status(ip)
        if response:
            redis_client.sadd("found_servers", ip)

            # Extract player info
            players = response.get("players", {})
            sample = players.get("sample", [])
            player_names = [p.get("name") for p in sample]

            # Save server info in Redis
            info_key = f"server:{ip}"
            redis_client.hset(info_key, mapping={
                "motd": response.get("description", {}).get("text", ""),
                "players_online": players.get("online", 0),
                "players_max": players.get("max", 0),
                "version": response.get("version", {}).get("name", ""),
                "player_names": ", ".join(player_names)
            })

            print(f"[{hostname}] [+] {ip} - {players.get('online', 0)}/{players.get('max', 0)} players: {player_names}")
            found += 1

    # Redis stat tracking
    pipe = redis_client.pipeline()
    pipe.incrby("stats:total_scanned", len(ip_list))
    pipe.incrby("stats:total_found", found)
    pipe.zadd("stats:scans", {f"{timestamp}:{len(ip_list)}": timestamp})
    pipe.setex(f"stats:worker:{hostname}", 90, "online")
    pipe.execute()

    print(f"[{hostname}] Finished. Found {found}, scanned {len(ip_list)}.")
