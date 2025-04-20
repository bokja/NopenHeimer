=======================
🔥 NopenHeimer Commands
=======================

📍 General Notes:
- Run commands from the project root (where docker-compose.yml lives)
- `.env` must point to your Redis/Postgres host
- Checkpoints are saved in `controller/checkpoints/`
- Completed CIDRs saved in `controller/completed_ranges.txt`

---------------------------------------
🎯 MAIN SERVER (controller + dashboard)
---------------------------------------

✅ Start only Redis, Postgres, Controller, Dashboard
docker-compose up -d redis postgres controller dashboard

🛑 Stop just the local worker
docker-compose stop worker

🗑 Remove the local worker completely
docker-compose rm -f worker

📊 View controller logs
docker-compose logs -f controller

📊 View dashboard logs
docker-compose logs -f dashboard

🌍 Access dashboard
http://<your-ip>:8080

---------------------------------------
⚙️ WORKER SERVERS (scanners only)
---------------------------------------

✅ Start only workers (no redis/postgres)
docker-compose up -d --no-deps worker

✅ Scale workers (e.g. 4 instances)
docker-compose up -d --no-deps --scale worker=2 worker

🛑 Stop workers
docker-compose stop worker

🗑 Remove workers
docker-compose rm -f worker

📊 View worker logs
docker-compose logs -f worker

👀 Check running containers
docker ps

---------------------------------------
📦 SYSTEM DEBUG / MAINTENANCE
---------------------------------------

📍 Show what’s currently scanning
docker exec -it redis redis-cli GET current_range

📍 Show all found servers
docker exec -it redis redis-cli SMEMBERS found_servers

📍 Clear Redis completely
docker exec -it redis redis-cli FLUSHALL

📍 View how many servers stored in Postgres
docker exec -it postgres psql -U mcscanner -d mcdata -c "SELECT COUNT(*) FROM servers;"

📍 Truncate (wipe) servers table
docker exec -it postgres psql -U mcscanner -d mcdata -c "TRUNCATE TABLE servers;"

📍 Remove a scan checkpoint manually
docker exec -it controller rm checkpoints/172.65.0.0_12.txt

📍 Remove all checkpoints
docker exec -it controller rm checkpoints/*

---------------------------------------
🧪 DEBUG / TESTING
---------------------------------------

📍 Run a test ping with full debugging
python tools/superdebug.py 172.65.108.140

📍 Check if Redis is reachable
docker exec -it redis redis-cli PING

📍 Rebuild everything
docker-compose up -d --build

📍 Restart controller/dashboard
docker-compose restart controller dashboard

---------------------------------------
📝 EXPORTING FOUND SERVERS
---------------------------------------

📍 Export default 100 IPs
http://<your-ip>:8080/export

📍 Export N IPs
http://<your-ip>:8080/export?limit=200

📍 (Optional route) If enabled: export via REST param
http://<your-ip>:8080/export/200

📦 Save to file:
curl http://<your-ip>:8080/export?limit=1000 -o servers.txt

---------------------------------------
🛠️ WORKER DEPLOYMENT EXAMPLES
---------------------------------------

📍 Launch a clean 4-worker-only node
docker-compose -f docker-compose.worker-only.yml up -d --scale worker=4

📍 Stop all workers on that node
docker-compose -f docker-compose.worker-only.yml down

📍 Bootstrap new worker-only EC2
1. Clone repo
2. Set up `.env` (pointing to main server)
3. `docker-compose -f docker-compose.worker-only.yml up -
