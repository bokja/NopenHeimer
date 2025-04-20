# NopenHeimer/shared/db.py - IMPROVED Version (Pooling + Logging)
import psycopg2
from psycopg2 import pool # Import pool directly
from psycopg2 import OperationalError, ProgrammingError
from psycopg2.extras import execute_values # Keep for batch function stub
import os
import time

# Use shared logger and config
# Assuming logger.py and config.py are in the same shared/ directory
try:
    from shared.logger import get_logger
    from shared.config import POSTGRES_HOST, POSTGRES_DB, POSTGRES_USER, POSTGRES_PASSWORD, DB_MIN_CONN, DB_MAX_CONN
except ImportError:
    # Fallback if run standalone or structure issue
    print("ERROR: Could not import logger/config from shared.", file=sys.stderr)
    # Attempt to load directly - less ideal
    import logging as log
    log.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    from dotenv import load_dotenv
    load_dotenv() # Load .env from current dir or parent
    POSTGRES_HOST = os.getenv("POSTGRES_HOST")
    POSTGRES_DB = os.getenv("POSTGRES_DB")
    POSTGRES_USER = os.getenv("POSTGRES_USER")
    POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")
    DB_MIN_CONN = int(os.getenv("DB_MIN_CONN", 5))
    DB_MAX_CONN = int(os.getenv("DB_MAX_CONN", 50))


log = get_logger("db")

# --- Connection Pooling ---
db_pool = None
try:
    # Validate that config variables were loaded
    if not all([POSTGRES_HOST, POSTGRES_DB, POSTGRES_USER, POSTGRES_PASSWORD]):
        raise ValueError("Database connection details missing in environment/config.")

    log.info(f"Initializing DB Connection Pool (Min:{DB_MIN_CONN}, Max:{DB_MAX_CONN}) for {POSTGRES_HOST}/{POSTGRES_DB}...")
    # Using ThreadedConnectionPool as it's generally safe for multi-threaded apps like Celery workers
    db_pool = psycopg2.pool.ThreadedConnectionPool(
        minconn=DB_MIN_CONN,
        maxconn=DB_MAX_CONN,
        host=POSTGRES_HOST,
        dbname=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
        connect_timeout=10 # Add a connection timeout
    )
    # Test getting a connection
    conn = db_pool.getconn()
    log.info(f"Connection Pool test successful. Pool initialized.")
    db_pool.putconn(conn) # Return the test connection immediately
except OperationalError as e:
    log.critical(f"FATAL: Database connection error during pool initialization: {e}")
    db_pool = None
except ValueError as e:
    log.critical(f"FATAL: Configuration error during pool initialization: {e}")
    db_pool = None
except Exception as e:
    log.critical(f"FATAL: An unexpected error occurred during DB pool initialization: {e}", exc_info=True)
    db_pool = None


class DBPoolConnection:
    """Context manager for safely acquiring and releasing pool connections."""
    def __init__(self):
        self.conn = None
        self.cursor = None

    def __enter__(self):
        if db_pool is None:
            log.error("DB pool accessed before initialization or after failure.")
            raise ConnectionError("Database connection pool is not available.")

        retries = 3 # Number of retries to get a connection
        delay = 1 # Initial delay in seconds
        last_exception = None

        while retries > 0:
            try:
                self.conn = db_pool.getconn()
                # Optional: Check if connection is actually usable? (e.g., self.conn.closed)
                if self.conn.closed:
                     log.warning("Retrieved a closed connection from pool, trying again.")
                     # Discard this connection, don't put it back yet
                     self.conn = None # Prevent putconn in finally
                     raise pool.PoolError("Retrieved closed connection")

                self.conn.autocommit = False # Use standard transaction handling
                self.cursor = self.conn.cursor()
                log.debug("Acquired DB connection from pool.")
                return self.cursor # Return cursor for 'with ... as cur:' usage
            except pool.PoolError as e: # Catch pool specific errors (e.g., pool full)
                last_exception = e
                retries -= 1
                log.warning(f"Failed to get connection from pool ({e}). Retries left: {retries}. Waiting {delay}s...")
                if retries == 0:
                    log.error("Max retries exceeded trying to get DB connection from pool.")
                    raise ConnectionError("Could not get connection from DB pool.") from e
                time.sleep(delay)
                delay *= 2 # Exponential backoff
            except OperationalError as e:
                 log.error(f"Database operational error on getconn: {e}")
                 # Don't retry on operational errors like bad password? Or do? Let's retry for robustness.
                 last_exception = e
                 retries -= 1
                 log.warning(f"Operational error getting conn. Retries left: {retries}. Waiting {delay}s...")
                 if retries == 0:
                      raise ConnectionError("Database operational error prevented getting connection.") from e
                 time.sleep(delay)
                 delay *=2
            except Exception as e:
                 log.error(f"Unexpected error getting DB connection: {e}", exc_info=True)
                 raise # Re-raise unexpected errors immediately

        # Should not be reachable if loop finishes without returning/raising
        raise ConnectionError("Failed to acquire DB connection after retries.") from last_exception


    def __exit__(self, exc_type, exc_val, exc_tb):
        # This method is called after the 'with' block finishes or if an exception occurs inside it.
        if self.conn: # Ensure connection was successfully acquired
            conn_to_return = self.conn # Store ref before potentially setting self.conn = None
            cursor_to_close = self.cursor
            self.conn = None # Prevent accidental reuse
            self.cursor = None
            try:
                if exc_type: # An exception occurred within the 'with' block
                    log.warning(f"Exception occurred in DBPoolConnection block, rolling back transaction: {exc_type}")
                    conn_to_return.rollback()
                else: # No exception, commit the transaction
                    conn_to_return.commit()
                    log.debug("Committed DB transaction successfully.")
            except (OperationalError, ProgrammingError, Exception) as e:
                 log.error(f"Error during DB commit/rollback: {e}")
                 # Attempt rollback again just in case commit failed
                 try: conn_to_return.rollback()
                 except Exception as rb_e: log.error(f"Secondary rollback attempt failed: {rb_e}")
            finally:
                 if cursor_to_close:
                     try: cursor_to_close.close()
                     except Exception: pass # Ignore cursor close errors
                 if db_pool:
                     try:
                         db_pool.putconn(conn_to_return)
                         log.debug("Returned connection to pool.")
                     except Exception as e:
                          log.error(f"Failed to return connection {conn_to_return} to pool: {e}")
                          # If putconn fails, maybe close the connection directly?
                          try: conn_to_return.close()
                          except Exception: pass
        # Return False to propagate any exceptions that occurred in the 'with' block
        return False


def initialize_db():
    """Creates tables if they don't exist using pooled connection."""
    if db_pool is None:
        log.error("Skipping DB initialization because pool is not available.")
        return

    log.info("Ensuring database schema is initialized...")
    try:
        # Use the context manager to handle connection and cursor
        with DBPoolConnection() as cur:
            # Create servers table with UNIQUE constraint
            cur.execute("""
                CREATE TABLE IF NOT EXISTS servers (
                    id SERIAL PRIMARY KEY,
                    ip VARCHAR(50) NOT NULL,
                    motd TEXT,
                    players_online INT,
                    players_max INT,
                    player_names TEXT[],
                    version TEXT,
                    timestamp TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    UNIQUE(ip, timestamp) -- Prevent exact duplicate rows per second
                );
            """)
            log.info("Table 'servers' initialization check complete.")

            # Add checks/creation for other necessary tables here if any
            # e.g., cur.execute("CREATE TABLE IF NOT EXISTS other_table (...)")

    except (ConnectionError, OperationalError, ProgrammingError, Exception) as e:
        log.error(f"DB schema initialization failed: {e}", exc_info=True)


# Call initialize_db() when module loads (after pool setup)
initialize_db()


def insert_server_info(ip, motd, players_online, players_max, version, player_names):
    """
    Inserts a single server record (found or offline) using a pooled connection.
    Matches the logic required by the reverted worker.
    """
    # Sanitize player_names (essential step)
    if isinstance(player_names, str):
        player_names = [name.strip() for name in player_names.split(",") if name.strip()]
    elif not isinstance(player_names, list):
        player_names = []
    else:
        # Ensure all items in list are strings and stripped
        player_names = [str(p).strip() for p in player_names if str(p).strip()]

    sql = """
        INSERT INTO servers (ip, motd, players_online, players_max, version, player_names)
        VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT (ip, timestamp) DO NOTHING
    """
    params = (str(ip), str(motd), int(players_online), int(players_max), str(version), player_names)

    try:
        # Use the context manager - it handles getconn, cursor, commit/rollback, putconn
        with DBPoolConnection() as cur:
            cur.execute(sql, params)
        log.debug(f"Insert executed via pool for server: {ip}")
    except (ConnectionError, OperationalError, ProgrammingError, Exception) as e:
        # Log the error, but the context manager handles rollback/putconn
        log.error(f"Pooled insert failed for {ip}: {e}")
        # Re-raise the exception so the caller (worker) knows it failed? Optional.
        # raise


# Keep batch insert stub for compatibility if called from elsewhere (e.g., dashboard?)
# Note: The reverted worker does NOT use this.
def insert_server_batch(server_data):
    """Inserts a batch using pooled connection."""
    if not server_data:
        log.debug("insert_server_batch called with no data.")
        return
    log.warning("insert_server_batch function called - ensure this is intended use.")

    query = """
        INSERT INTO servers (ip, motd, players_online, players_max, player_names, version)
        VALUES %s
        ON CONFLICT (ip, timestamp) DO NOTHING
    """
    processed_data = []
    try:
        for row in server_data:
             # Adapt based on expected tuple format from caller
             ip, motd, online, max_p, names, version, *_ = row
             if isinstance(names, str): names = [n.strip() for n in names.split(",") if n.strip()]
             elif not isinstance(names, list): names = []
             else: names = [str(p).strip() for p in names if str(p).strip()]
             processed_data.append((ip, motd, online, max_p, names, version))

        with DBPoolConnection() as cur:
            # Use execute_values for efficiency if available
            execute_values(cur, query, processed_data, page_size=200) # Adjust page_size as needed
        log.info(f"Batch insert executed via pool for {len(processed_data)} potential entries.")

    except (ConnectionError, OperationalError, ProgrammingError, Exception) as e:
        log.error(f"Pooled batch insert failed: {e}")
        # Context manager handles rollback/putconn