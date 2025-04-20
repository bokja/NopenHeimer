# NopenHeimer/shared/config.py
import os
from dotenv import load_dotenv

dotenv_path = os.path.join(os.path.dirname(__file__), '..', '.env')
load_dotenv(dotenv_path=dotenv_path)

# Redis config
REDIS_URL = os.getenv("REDIS_URL")

# PostgreSQL config
POSTGRES_HOST = os.getenv("POSTGRES_HOST")
POSTGRES_DB = os.getenv("POSTGRES_DB")
POSTGRES_USER = os.getenv("POSTGRES_USER")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")

# Worker config
TARGET_PORT = int(os.getenv("TARGET_PORT", 25565))
# Use a single timeout consistent with old worker code default
WORKER_TIMEOUT = float(os.getenv("WORKER_TIMEOUT", 0.3))

CHUNK_SIZE = int(os.getenv("CHUNK_SIZE", 20)) # Default to 20

# DB Pool config
DB_MIN_CONN = int(os.getenv("DB_MIN_CONN", 5))
# START CONSERVATIVELY - If 100 concurrency, maybe ~50 pool size? TUNE THIS.
DB_MAX_CONN = int(os.getenv("DB_MAX_CONN", 50))

# Validate required
required_vars = ['REDIS_URL', 'POSTGRES_HOST', 'POSTGRES_DB', 'POSTGRES_USER', 'POSTGRES_PASSWORD']
missing_vars = [var for var in required_vars if not globals().get(var)]
if missing_vars:
    raise ValueError(f"Missing required environment variables: {', '.join(missing_vars)}")

print(f"Config Loaded: REDIS_URL={REDIS_URL}, POSTGRES_HOST={POSTGRES_HOST}, DB_MAX_CONN={DB_MAX_CONN}, WORKER_TIMEOUT={WORKER_TIMEOUT}")