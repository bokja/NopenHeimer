# shared/db.py (or wherever your db functions are)
import psycopg2
from psycopg2.pool import SimpleConnectionPool
from psycopg2.extras import execute_values
import os
import ipaddress # Import ipaddress here or pass string representations
from shared.logger import logger # Import logger

# --- Connection Pooling ---
DB_HOST = os.getenv("POSTGRES_HOST", "postgres")
DB_NAME = os.getenv("POSTGRES_DB", "mcdata")
DB_USER = os.getenv("POSTGRES_USER", "mcscanner")
DB_PASS = os.getenv("POSTGRES_PASSWORD", "mcscannerpass")
MIN_CONN = 1
MAX_CONN = 10 # Adjust as needed based on controller/worker count

# Initialize pool variable
pool = None

def initialize_pool():
    """Initializes the connection pool. Call this once per process."""
    global pool
    if pool is not None:
        logger.warning("[DB] Pool already initialized.")
        return

    logger.info("[DB] Initializing Connection Pool...")
    try:
        pool = SimpleConnectionPool(MIN_CONN, MAX_CONN,
                                    host=DB_HOST,
                                    dbname=DB_NAME,
                                    user=DB_USER,
                                    password=DB_PASS)
        logger.info(f"[DB] Connection Pool initialized (Min: {MIN_CONN}, Max: {MAX_CONN})")
    except Exception as e:
        logger.critical(f"[DB FATAL] Failed to initialize connection pool: {e}", exc_info=True)
        pool = None # Ensure pool is None if init fails
        raise # Re-raise exception to signal failure

def get_connection():
    if pool is None:
        logger.error("[DB] Attempted to get connection before pool was initialized!")
        raise Exception("Database connection pool is not available.")
    try:
        return pool.getconn()
    except Exception as e:
        logger.error(f"[DB Pool Error] Failed to get connection from pool: {e}", exc_info=True)
        raise

def put_connection(conn):
    if pool is not None and conn is not None:
        try:
            pool.putconn(conn)
        except Exception as e:
            logger.error(f"[DB Pool Error] Failed to return connection to pool: {e}", exc_info=True)
            # May need to close the connection explicitly if pool is unhappy
            try:
                conn.close()
            except Exception as close_e:
                logger.error(f"[DB Pool Error] Failed to close connection after putconn error: {close_e}")
    elif conn is None:
        logger.warning("[DB] Attempted to return a None connection to pool.")

# --- Initialize Tables (Definition Only) ---
def initialize_db():
    """Creates or updates DB tables. Should be run separately, e.g., via a script."""
    conn = None
    try:
        # Temporarily get a connection just for init
        # Assumes pool is NOT necessarily initialized yet, connect directly
        conn = psycopg2.connect(host=DB_HOST, dbname=DB_NAME, user=DB_USER, password=DB_PASS)
        logger.info("[DB Init] Connected directly for initialization.")
        with conn.cursor() as cur:
            # Create servers table (existing logic)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS servers (
                    id SERIAL PRIMARY KEY,
                    ip VARCHAR(50),
                    motd TEXT,
                    players_online INT,
                    players_max INT,
                    player_names TEXT[],
                    version TEXT,
                    timestamp TIMESTAMPTZ DEFAULT NOW(),
                    cidr_scan_ref TEXT, -- Optional: Reference back to the scan
                    UNIQUE(ip, timestamp)
                );
            """)
            logger.info("[DB Init] Table 'servers' is ready")

            # Create cidr_ranges table (New)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS cidr_ranges (
                    cidr_block TEXT PRIMARY KEY,
                    status VARCHAR(20) NOT NULL DEFAULT 'pending',
                    last_checkpoint_ip INET,
                    last_scanned_timestamp TIMESTAMPTZ,
                    found_server_count_history INTEGER DEFAULT 0,
                    priority_score INTEGER DEFAULT 0,
                    added_timestamp TIMESTAMPTZ DEFAULT NOW()
                );
            """)
            logger.info("[DB Init] Table 'cidr_ranges' is ready")

            # Create indexes (New)
            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_cidr_ranges_scan_order
                ON cidr_ranges (priority_score DESC, status, last_scanned_timestamp ASC NULLS FIRST);
            """)
            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_cidr_ranges_status ON cidr_ranges (status);
            """)
            logger.info("[DB Init] Indexes for 'cidr_ranges' ensured")
        conn.commit()
        logger.info("[DB Init] Database initialization committed successfully.")
    except Exception as e:
        logger.error(f"[DB Init ERROR] Initialization failed: {e}", exc_info=True)
        if conn: conn.rollback()
    finally:
        if conn: conn.close()
        logger.info("[DB Init] Direct connection closed.")

# --- CIDR Range Functions (Use Logging) ---
def add_cidr_ranges(cidr_list):
    """Adds a list of CIDR blocks to the table if they don't exist."""
    conn = None
    added_count = 0
    try:
        conn = get_connection()
        with conn.cursor() as cur:
            # Prepare data as tuples for execute_values
            data_to_insert = [(cidr,) for cidr in cidr_list]
            # Use ON CONFLICT DO NOTHING to avoid errors if a CIDR already exists
            insert_query = "INSERT INTO cidr_ranges (cidr_block) VALUES %s ON CONFLICT (cidr_block) DO NOTHING"
            execute_values(cur, insert_query, data_to_insert)
            added_count = cur.rowcount # Number of rows actually inserted
        conn.commit()
        logger.info(f"[DB] Added {added_count} new CIDR ranges.")
    except Exception as e:
        logger.error(f"[DB ERROR] Failed to add CIDR ranges: {e}", exc_info=True)
        if conn: conn.rollback()
    finally:
        if conn: put_connection(conn)
    return added_count

def get_next_range_to_scan(skip_zero_history=False, rescan_threshold_hours=24):
    """
    Finds the next available CIDR block to scan, marks it as 'scanning',
    and returns its details (cidr_block, last_checkpoint_ip).
    Uses locking via status update.
    """
    conn = None
    selected_range = None
    try:
        conn = get_connection()
        with conn.cursor() as cur:
            # Build the WHERE clause based on options
            where_clauses = [
                "(status = 'pending' OR status = 'completed' OR status = 'error')", # Base states eligible for scanning
                # Rescan completed/error blocks only after threshold
                f"(status = 'pending' OR last_scanned_timestamp IS NULL OR last_scanned_timestamp < NOW() - INTERVAL '{rescan_threshold_hours} hours')"
            ]
            if skip_zero_history:
                # If skipping zeros, only pick completed blocks if they had history > 0
                where_clauses.append("(status = 'pending' OR found_server_count_history > 0)")

            where_sql = " AND ".join(f"({clause})" for clause in where_clauses)

            # Query to find the highest priority available range
            # Use FOR UPDATE SKIP LOCKED if you anticipate multiple controller instances
            # For simplicity here, we rely on the atomic UPDATE RETURNING
            query = f"""
                SELECT cidr_block, last_checkpoint_ip
                FROM cidr_ranges
                WHERE {where_sql}
                ORDER BY priority_score DESC, last_scanned_timestamp ASC NULLS FIRST, status
                LIMIT 1
            """
            cur.execute(query)
            candidate = cur.fetchone()

            if candidate:
                cidr_to_scan, last_ip = candidate
                logger.info(f"[Controller] Candidate range found: {cidr_to_scan}")
                # Attempt to lock the range by setting status to 'scanning' atomically
                update_query = """
                    UPDATE cidr_ranges
                    SET status = 'scanning',
                        last_scanned_timestamp = NOW() -- Mark scan attempt time
                    WHERE cidr_block = %s AND status != 'scanning' -- Ensure it wasn't just grabbed
                    RETURNING cidr_block, last_checkpoint_ip;
                """
                cur.execute(update_query, (cidr_to_scan,))
                locked_range = cur.fetchone()
                if locked_range:
                    selected_range = {
                        "cidr_block": locked_range[0],
                        "last_checkpoint_ip": str(locked_range[1]) if locked_range[1] else None
                    }
                    logger.info(f"[Controller] Locked range for scanning: {selected_range['cidr_block']}")
                    conn.commit() # Commit the lock
                else:
                    logger.info(f"[Controller] Failed to lock {cidr_to_scan} (likely grabbed by another process). Will retry.")
                    conn.rollback() # Rollback the attempt
            else:
                logger.info("[Controller] No suitable ranges found to scan.")
                conn.rollback() # No changes made

    except Exception as e:
        logger.error(f"[DB ERROR] Failed to get next range: {e}", exc_info=True)
        if conn: conn.rollback()
    finally:
        if conn: put_connection(conn)
    return selected_range


def update_checkpoint(cidr_block, last_ip_scanned):
    """Updates the checkpoint IP for a given CIDR block."""
    conn = None
    try:
        conn = get_connection()
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE cidr_ranges SET last_checkpoint_ip = %s WHERE cidr_block = %s",
                (str(last_ip_scanned), cidr_block) # Ensure IP is string for INET conversion
            )
        conn.commit()
    except Exception as e:
        logger.error(f"[DB ERROR] Failed to update checkpoint for {cidr_block}: {e}", exc_info=True)
        if conn: conn.rollback()
    finally:
        if conn: put_connection(conn)

def mark_range_completed(cidr_block, found_count_in_scan):
    """Marks a range as completed and updates its historical count."""
    conn = None
    try:
        conn = get_connection()
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE cidr_ranges
                SET status = 'completed',
                    last_scanned_timestamp = NOW(),
                    found_server_count_history = %s,
                    last_checkpoint_ip = NULL -- Reset checkpoint on completion
                WHERE cidr_block = %s
                """,
                (found_count_in_scan, cidr_block)
            )
        conn.commit()
        logger.info(f"[Controller] Marked {cidr_block} as completed. Found: {found_count_in_scan}.")
    except Exception as e:
        logger.error(f"[DB ERROR] Failed to mark {cidr_block} completed: {e}", exc_info=True)
        if conn: conn.rollback()
    finally:
        if conn: put_connection(conn)

def mark_range_error(cidr_block):
    """Marks a range as 'error' if scanning failed irrecoverably."""
    conn = None
    try:
        conn = get_connection()
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE cidr_ranges SET status = 'error', last_scanned_timestamp = NOW() WHERE cidr_block = %s",
                (cidr_block,)
            )
        conn.commit()
        logger.info(f"[Controller] Marked {cidr_block} with status 'error'.")
    except Exception as e:
        logger.error(f"[DB ERROR] Failed to mark {cidr_block} as error: {e}", exc_info=True)
        if conn: conn.rollback()
    finally:
        if conn: put_connection(conn)

# --- Server Insert Functions (Use Logging & Pooling) ---
def insert_server_info(ip, motd, players_online, players_max, version, player_names, cidr_ref=None):
    conn = None
    try:
        conn = get_connection()
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO servers (ip, motd, players_online, players_max, version, player_names, cidr_scan_ref)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT DO NOTHING
            """, (ip, motd, players_online, players_max, version, player_names, cidr_ref)) # Add cidr_ref
        conn.commit()
        # print(f"[+] Inserted server: {ip}") # Maybe reduce logging noise
    except Exception as e:
        logger.error(f"[DB ERROR] Failed single insert for {ip}: {e}", exc_info=True)
        if conn: conn.rollback()
    finally:
        if conn: put_connection(conn)

def insert_server_batch(server_data):
    if not server_data:
        return 0
    conn = None
    inserted_count = 0
    try:
        conn = get_connection()
        with conn.cursor() as cur:
            # Assumes server_data is list of tuples: (ip, motd, online, max, names_list, version, cidr_ref)
            sql = """INSERT INTO servers (ip, motd, players_online, players_max, player_names, version, cidr_scan_ref)
                     VALUES %s ON CONFLICT (ip, timestamp) DO NOTHING"""
            execute_values(cur, sql, server_data, page_size=100) # page_size helps memory
            inserted_count = cur.rowcount
        conn.commit()
        logger.debug(f"[DB] Batch inserted {inserted_count}/{len(server_data)} server records.")
    except Exception as e:
        logger.error(f"[DB ERROR] Failed batch insert: {e}", exc_info=True)
        if conn: conn.rollback()
    finally:
        if conn: put_connection(conn)
    return inserted_count

# Call initialize_db() once when the module loads or app starts
initialize_db()