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
    servers_sorted = sorted(ip.decode() for ip in servers)

    return render_template(
        "dashboard.html",
        found=len(servers_sorted),
        servers=servers_sorted
    )

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
    scan_entries = redis_client.zrangebyscore("stats:scans", start_time, now, withscores=False)
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

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080)
