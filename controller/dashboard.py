import os
from flask import Flask, render_template, Response
import redis
from shared.config import REDIS_URL

template_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'templates'))
app = Flask(__name__, template_folder=template_dir)
redis_client = redis.Redis.from_url(REDIS_URL)

@app.route("/")
def dashboard():
    servers = redis_client.smembers("found_servers")
    return render_template("dashboard.html", found=len(servers), servers=sorted(ip.decode() for ip in servers))

@app.route("/export")
def export():
    servers = redis_client.smembers("found_servers")
    lines = "\n".join(sorted([ip.decode() for ip in servers]))
    return Response(lines, mimetype="text/plain")

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080)
