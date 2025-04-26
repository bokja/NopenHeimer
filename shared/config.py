import os
import json # Added for network pool parsing
from dotenv import load_dotenv

load_dotenv()

# Redis config
# Use the specific private IP as default for AWS deployment
REDIS_URL = os.getenv("REDIS_URL", "redis://172.31.28.192:6379/0")

# Scanning config
# NETWORK_TO_SCAN = os.getenv("NETWORK_TO_SCAN", "172.65.0.0/12") # Removed, controller manages this
TARGET_PORT = int(os.getenv("TARGET_PORT", "25565")) # Ensure int
CONNECT_TIMEOUT = float(os.getenv("SCAN_TIMEOUT", "0.3")) # Changed name, ensure float
# PING_MESSAGE = b'\xfe' # Removed, likely handled in mc_ping

# Controller Config
CONTROLLER_CHUNK_SIZE = int(os.getenv("CONTROLLER_CHUNK_SIZE", "20"))

# Network Pool Loading
NETWORK_POOL_FILE = os.getenv("NETWORK_POOL_FILE", "network_pool.txt") # Configurable filename
_loaded_network_pool = []

if os.path.exists(NETWORK_POOL_FILE):
    print(f"Loading network pool from file: {NETWORK_POOL_FILE}") # Optional: Log this
    try:
        with open(NETWORK_POOL_FILE, "r") as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith("#"): # Ignore empty lines and comments
                    _loaded_network_pool.append(line)
    except Exception as e:
        print(f"Error loading network pool file {NETWORK_POOL_FILE}: {e}") # Optional: Log this error
        # Decide on behavior: Fallback? Raise error?

# If file loading failed or file doesn't exist, use default
if not _loaded_network_pool:
    print("Network pool file not found or empty/error, using default pool.") # Optional: Log this
    _loaded_network_pool = [
        "172.65.0.0/12", "173.0.0.0/12", "192.241.0.0/16", "144.202.0.0/16",
        "51.81.0.0/16", "5.9.0.0/16", "167.114.0.0/16", "45.13.0.0/16"
    ]

# Allow environment variable override (JSON format) as highest priority
NETWORK_POOL_JSON = os.getenv("NETWORK_POOL_JSON")
if NETWORK_POOL_JSON:
    print("Overriding network pool with NETWORK_POOL_JSON environment variable.") # Optional: Log this
    try:
        NETWORK_POOL = json.loads(NETWORK_POOL_JSON)
    except json.JSONDecodeError as e:
        print(f"Error decoding NETWORK_POOL_JSON: {e}. Using file/default pool.") # Log error
        NETWORK_POOL = _loaded_network_pool
else:
    NETWORK_POOL = _loaded_network_pool

# PostgreSQL config
# Consider changing this default too if Postgres is on a separate AWS instance
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "172.31.28.192") # Example: Point to same host or a dedicated DB host
POSTGRES_DB = os.getenv("POSTGRES_DB", "mcdata")
POSTGRES_USER = os.getenv("POSTGRES_USER", "mcscanner")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "mcscannerpass")
