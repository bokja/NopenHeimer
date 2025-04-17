import socket
import traceback
import redis
from tools.mc_ping import ping_server, ping_modern, ping_legacy, parse_legacy_ping
from tools.db import insert_server_info
from shared.config import REDIS_URL, POSTGRES_HOST, POSTGRES_DB, POSTGRES_USER, POSTGRES_PASSWORD

# Setup Redis client
redis_client = redis.Redis.from_url(REDIS_URL)

# List of IPs to test
targets = [
    "172.65.108.140",
    "172.65.108.8",
    "172.65.108.243",
    "172.65.112.0",
    "172.65.115.179"
]

# Util: Test raw socket connection
def test_socket(ip, port=25565, timeout=1.0):
    try:
        with socket.create_connection((ip, port), timeout=timeout):
            return True
    except Exception as e:
        print(f"[-] Socket test failed for {ip}:{port} - {e}")
        return False

for ip in targets:
    print(f"\n🔎 Pinging {ip}...")

    if not test_socket(ip):
        print(f"❌ [SOCKET] Can't connect to {ip}")
        continue

    # Try modern ping
    try:
        print("🌐 Trying modern ping...")
        modern = ping_modern(ip)
        if modern:
            print(f"✅ [MODERN] Success: {modern}")
        else:
            print("❌ [MODERN] No response.")
    except Exception as e:
        print(f"💥 [MODERN ERROR] {e}")
        traceback.print_exc()

    # Try legacy fallback
    try:
        print("📦 Trying legacy ping...")
        legacy_raw = ping_legacy(ip)
        if legacy_raw:
            print(f"[DEBUG] Raw legacy response: {legacy_raw.hex()}")
            legacy = parse_legacy_ping(legacy_raw)
            if legacy:
                print(f"✅ [LEGACY] Parsed: {legacy}")
            else:
                print("❌ [LEGACY] Unparsed response.")
        else:
            print("❌ [LEGACY] No response.")
    except Exception as e:
        print(f"💥 [LEGACY ERROR] {e}")
        traceback.print_exc()

    # Use smart unified ping
    print("🧠 Trying unified ping_server()...")
    try:
        result = ping_server(ip)
        print(f"[RESULT] {ip}: {result}")
        if result:
            # Insert to DB
            print("💾 Inserting into Postgres...")
            try:
                insert_server_info(
                    ip=ip,
                    motd=result.get("motd") or "",
                    players_online=result.get("players_online") or 0,
                    players_max=result.get("players_max") or 0,
                    version=result.get("version") or "unknown",
                    player_names=result.get("player_names") or []
                )
                print("✅ [DB] Inserted.")
            except Exception as e:
                print(f"💥 [DB INSERT ERROR] {e}")
                traceback.print_exc()

            # Add to Redis
            try:
                redis_client.sadd("found_servers", ip)
                print("✅ [REDIS] Added to found_servers.")
            except Exception as e:
                print(f"💥 [REDIS ERROR] {e}")
                traceback.print_exc()
        else:
            print("❌ No result from ping_server().")
    except Exception as e:
        print(f"💥 [ping_server ERROR] {e}")
        traceback.print_exc()
