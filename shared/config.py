import os
from dotenv import load_dotenv

load_dotenv()

REDIS_URL = os.getenv("REDIS_URL", "redis://172.31.28.192:6379/0")
NETWORK_TO_SCAN = os.getenv("NETWORK_TO_SCAN", "172.62.0.0/12")
TARGET_PORT = 25565
CONNECT_TIMEOUT = 1
PING_MESSAGE = b'\xfe'