=======================
🔥 NopenHeimer Commands
=======================

📍 General Notes:
- Always run commands from the project root (where docker-compose.yml is)
- Make sure your .env is set properly on each machine (especially worker servers)

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

🌍 Access dashboard UI
http://<your-public-ip>:8080

---------------------------------------
⚙️ WORKER SERVERS (scanners only)
---------------------------------------

✅ Start only the worker
docker-compose up -d worker

✅ Scale workers on this node (e.g. 4 instances)
docker-compose up -d --no-deps --scale worker=4 worker

🛑 Stop all worker containers
docker-compose stop worker

🗑 Remove all worker containers
docker-compose rm -f worker

📊 View logs for all workers
docker-compose logs -f worker

👀 View active containers
docker ps

---------------------------------------
📦 SYSTEM DEBUG / MAINTENANCE
---------------------------------------

📍 Check what IP range is currently scanning
docker exec -it redis redis-cli GET current_range

📍 Check Redis stats (found servers)
docker exec -it redis redis-cli SMEMBERS found_servers

📍 Flush Redis entirely (stats + found_servers + checkpoints)
docker exec -it redis redis-cli FLUSHALL

📍 View # of servers in Postgres
docker exec -it postgres psql -U mcscanner -d mcdata -c "SELECT COUNT(*) FROM servers;"

📍 Truncate (wipe) the servers table in Postgres
docker exec -it postgres psql -U mcscanner -d mcdata -c "TRUNCATE TABLE servers;"

📍 Remove scan checkpoint (starts from beginning of range)
docker exec -it controller rm checkpoint.txt

---------------------------------------
🚀 OTHER TIPS
---------------------------------------

📍 Rebuild everything
docker-compose up -d --build

📍 Restart just controller & dashboard
docker-compose restart controller dashboard

📍 Start just Redis (quick test)
docker-compose up -d redis

