import os
from flask import Flask, render_template, Response
import redis
from shared.config import REDIS_URL
import time
from flask import jsonify

@app.route("/stats")
def stats():
    now = int(time.time())
    window = 60  # seconds
    start_time = now - window

    # Get all scan timestamps in the last 60 seconds
    ips_recent = redis_client.zcount("stats:scans", start_time, now)
    ips_per_second = ips_recent / window

    return jsonify({
        "ips_per_sec": round(ips_per_second, 2),
        "total_scanned": int(redis_client.get("stats:total_scanned") or 0),
        "total_found": int(redis_client.get("stats:total_found") or 0),
        "active_workers": len(redis_client.keys("stats:worker:*"))
    })


# Explicit path to the templates folder (rock solid in Docker)
template_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'templates'))
app = Flask(__name__, template_folder=template_dir)

# Redis client
redis_client = redis.Redis.from_url(REDIS_URL)

@app.route("/")
def dashboard():
    servers = redis_client.smembers("found_servers")

    # Decode and sort the IPs
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

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080)
