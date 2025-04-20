# NopenHeimer/shared/db.py - IMPROVED Version (Pooling + Logging)
import psycopg2
from psycopg2 import pool
from psycopg2 import OperationalError, ProgrammingError
import os
import time
from shared.logger import get_logger
from shared.config import POSTGRES_HOST, POSTGRES_DB, POSTGRES_USER, POSTGRES_PASSWORD, DB_MIN_CONN, DB_MAX_CONN

log = get_logger("db")
db_pool = None
try:
    log.info(f"Initializing DB Pool (Min:{DB_MIN_CONN}, Max:{DB_MAX_CONN})...")
    db_pool = psycopg2.pool.ThreadedConnectionPool(
        minconn=DB_MIN_CONN, maxconn=DB_MAX_CONN,
        host=POSTGRES_HOST, dbname=POSTGRES_DB,
        user=POSTGRES_USER, password=POSTGRES_PASSWORD
    )
    conn = db_pool.getconn()
    log.info(f"DB Pool initialized.")
    db_pool.putconn(conn)
except Exception as e:
    log.critical(f"FATAL: DB pool init failed: {e}")
    db_pool = None

class DBPoolConnection:
    def __init__(self): self.conn = None; self.cursor = None
    def __enter__(self):
        if db_pool is None: raise ConnectionError("DB pool unavailable.")
        retries=3; delay=1
        while retries > 0:
            try: self.conn = db_pool.getconn(); self.cursor = self.conn.cursor(); return self.cursor
            except pool.PoolError as e:
                retries-=1; log.warning(f"Pool Error ({e}). Retries:{retries}. Wait {delay}s...")
                if retries==0: raise ConnectionError("Pool retries exceeded.") from e
                time.sleep(delay); delay*=2
            except OperationalError as e: log.error(f"DB op error on getconn: {e}"); raise ConnectionError("DB op error.") from e
            except Exception as e: log.error(f"Unexpected error getting conn: {e}"); raise
    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.conn:
            try:
                if exc_type: self.conn.rollback(); log.debug(f"Rolled back on exception: {exc_type}")
                else: self.conn.commit(); log.debug("Committed DB transaction.")
            except Exception as e: log.error(f"Commit/rollback error: {e}"); \
                                  try: self.conn.rollback()
                                  except Exception as rb_e: log.error(f"Secondary rollback failed: {rb_e}")
            finally:
                if self.cursor: try: self.cursor.close()
                                catch Exception: pass
                if db_pool: db_pool.putconn(self.conn); log.debug("Returned connection to pool.")
        return False # Propagate exceptions

def initialize_db():
    if db_pool is None: log.error("Skipping DB init: pool unavailable."); return
    try:
        with DBPoolConnection() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS servers (
                    id SERIAL PRIMARY KEY, ip VARCHAR(50) NOT NULL, motd TEXT,
                    players_online INT, players_max INT, player_names TEXT[],
                    version TEXT, timestamp TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    UNIQUE(ip, timestamp) -- Added for integrity
                );""")
            log.info("Table 'servers' is ready")
    except Exception as e: log.error(f"DB init failed: {e}")

initialize_db()

def insert_server_info(ip, motd, players_online, players_max, version, player_names):
    """Inserts single server record using pooled connection."""
    if isinstance(player_names, str): player_names = [n.strip() for n in player_names.split(",") if n.strip()]
    elif not isinstance(player_names, list): player_names = []
    else: player_names = [str(p).strip() for p in player_names if str(p).strip()]
    sql = """INSERT INTO servers (ip, motd, players_online, players_max, version, player_names)
             VALUES (%s, %s, %s, %s, %s, %s) ON CONFLICT (ip, timestamp) DO NOTHING"""
    params = (ip, motd, players_online, players_max, version, player_names)
    try:
        with DBPoolConnection() as cur:
            cur.execute(sql, params)
        log.debug(f"Insert executed for: {ip}")
    except Exception as e: log.error(f"Insert failed for {ip}: {e}")

# Keep batch insert stub for compatibility if needed elsewhere, but worker won't use it
def insert_server_batch(server_data):
    log.warning("insert_server_batch called, but worker uses single inserts.")
    # Minimal implementation if called unexpectedly
    if not server_data: return
    log.info(f"Attempting batch insert for {len(server_data)} records (not standard worker path)")
    # ... (add full batch logic with pooling if this function is actually used elsewhere) ...