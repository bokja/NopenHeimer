# controller/dashboard.py
import os
import time
import redis
import psycopg2
from flask import Flask, render_template, Response, jsonify
from shared.config import REDIS_URL, POSTGRES_HOST, POSTGRES_DB, POSTGRES_USER, POSTGRES_PASSWORD

# Flask + Redis
template_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'templates'))
app = Flask(__name__, template_folder=template_dir)
redis_client = redis.Redis.from_url(REDIS_URL)

@app.route("/")
def dashboard():
    servers = redis_client.smembers("found_servers")
    server_data = []

    for ip_bytes in servers:
        ip = ip_bytes.decode()
        info_key = f"server:{ip}"
        info = redis_client.hgetall(info_key)
        if info:
            server_data.append({
                "ip": ip,
                "motd": info.get(b"motd", b"").decode("utf-8", errors="ignore").strip(),
                "players_online": info.get(b"players_online", b"0").decode().strip(),
                "players_max": info.get(b"players_max", b"0").decode().strip(),
                "player_names": info.get(b"player_names", b"").decode("utf-8", errors="ignore").strip(),
                "version": info.get(b"version", b"").decode("utf-8", errors="ignore").strip()
            })

    return render_template("dashboard.html", servers=server_data, found=len(server_data))

@app.route("/export")
def export():
    limit = int(request.args.get("limit", 100))  # Default to 100
    servers = redis_client.smembers("found_servers")
    ip_list = sorted(ip.decode().strip() for ip in servers)
    lines = "\n".join(ip_list[:limit])
    return Response(lines, mimetype="text/plain")



@app.route("/stats")
def stats():
    now = int(time.time())
    window = 60
    start_time = now - window
    ips_recent = 0

    scan_entries = redis_client.zrangebyscore("stats:scans", start_time, now)
    for entry in scan_entries:
        try:
            parts = entry.decode().split(":")
            if len(parts) >= 2:
                ips_recent += int(parts[1])
        except:
            continue

    ips_per_second = ips_recent / window if window else 0

    return jsonify({
        "ips_per_sec": round(ips_per_second, 2),
        "total_scanned": int(redis_client.get("stats:total_scanned") or 0),
        "total_found": int(redis_client.get("stats:total_found") or 0),
        "active_workers": len(redis_client.keys("stats:worker:*"))
    })

@app.route("/server-details")
def server_details():
    try:
        conn = psycopg2.connect(
            host=POSTGRES_HOST,
            dbname=POSTGRES_DB,
            user=POSTGRES_USER,
            password=POSTGRES_PASSWORD
        )
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute("""
                SELECT ip, motd, players_online, players_max, player_names, version, timestamp
                FROM servers ORDER BY timestamp DESC LIMIT 100
            """)
            rows = cur.fetchall()
            return jsonify([
                {
                    "ip": r[0],
                    "motd": r[1],
                    "players_online": r[2],
                    "players_max": r[3],
                    "player_names": ", ".join(r[4]) if r[4] else "-",
                    "version": r[5],
                    "timestamp": r[6].isoformat()
                } for r in rows
            ])
    except Exception as e:
        print("DB Error:", e)
        return jsonify([])

@app.route("/api/servers")
def get_servers():
    return server_details()
    
@app.route("/range")
def get_range():
    current_range = redis_client.get("current_range")
    return jsonify({"range": current_range.decode() if current_range else "Unknown"})


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080)
