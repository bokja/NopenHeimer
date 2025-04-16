# controller/dashboard.py
import os
import time
from flask import Flask, render_template, Response, jsonify
import redis
from shared.config import REDIS_URL

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
                "motd": info.get(b"motd", b"").decode(),
                "players_online": info.get(b"players_online", b"0").decode(),
                "players_max": info.get(b"players_max", b"0").decode(),
                "player_names": info.get(b"player_names", b"").decode(),
                "version": info.get(b"version", b"").decode()
            })

    return render_template("dashboard.html", servers=server_data, found=len(server_data))


@app.route("/export")
def export():
    servers = redis_client.smembers("found_servers")
    lines = "\n".join(sorted(ip.decode() for ip in servers))
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
            _, count = entry.decode().split(":")
            ips_recent += int(count)
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
    servers = redis_client.smembers("found_servers")
    data = []

    for ip_bytes in servers:
        ip = ip_bytes.decode()
        info_key = f"server:{ip}"
        info = redis_client.hgetall(info_key)
        if info:
            data.append({
                "ip": ip,
                "motd": info.get(b"motd", b"").decode("utf-8", errors="ignore"),
                "players_online": info.get(b"players_online", b"0").decode(),
                "players_max": info.get(b"players_max", b"0").decode(),
                "player_names": info.get(b"player_names", b"").decode("utf-8", errors="ignore"),
                "version": info.get(b"version", b"").decode("utf-8", errors="ignore"),
            })

    return jsonify(data)



if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080)
