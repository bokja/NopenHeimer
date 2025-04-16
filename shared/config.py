import os
from dotenv import load_dotenv

load_dotenv()

# Redis config
REDIS_URL = os.getenv("REDIS_URL", "redis://172.31.28.192:6379/0")

# Scanning config
NETWORK_TO_SCAN = os.getenv("NETWORK_TO_SCAN", "172.65.0.0/12")
TARGET_PORT = 25565
CONNECT_TIMEOUT = 1
PING_MESSAGE = b'\xfe'

# PostgreSQL config
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "postgres")
POSTGRES_DB = os.getenv("POSTGRES_DB", "mcdata")
POSTGRES_USER = os.getenv("POSTGRES_USER", "mcscanner")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "mcscannerpass")
