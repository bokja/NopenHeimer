import redis
import csv
import os
from shared.config import REDIS_URL

r = redis.Redis.from_url(REDIS_URL)

def export_to_csv(filename="found_servers.csv"):
    servers = r.smembers("found_servers")
    os.makedirs("exports", exist_ok=True)
    path = os.path.join("exports", filename)
    with open(path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["IP"])
        for ip in servers:
            writer.writerow([ip.decode()])
    print(f"✅ Exported {len(servers)} servers to {path}")

if __name__ == "__main__":
    export_to_csv()
