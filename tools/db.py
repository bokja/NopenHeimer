import psycopg2
from psycopg2.extras import execute_values
import os
import json

DB_HOST = os.getenv("POSTGRES_HOST", "postgres")
DB_NAME = os.getenv("POSTGRES_DB", "mcdata")
DB_USER = os.getenv("POSTGRES_USER", "mcscanner")
DB_PASS = os.getenv("POSTGRES_PASSWORD", "mcscannerpass")

def get_connection():
    return psycopg2.connect(
        host=DB_HOST,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASS
    )

# Only run table creation once at startup
def initialize_db():
    try:
        conn = get_connection()
        conn.autocommit = True
        with conn.cursor() as cur:
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
                    UNIQUE(ip, timestamp)
                );
            """)
            print("[+] PostgreSQL initialized & table 'servers' is ready")
    except Exception as e:
        print(f"[!] DB init failed: {e}")
    finally:
        conn.close()

initialize_db()

def insert_server_info(ip, motd, players_online, players_max, version, player_names):
    # Sanitize player_names
    if isinstance(player_names, str):
        player_names = [name.strip() for name in player_names.split(",")]
    elif not isinstance(player_names, list):
        player_names = []
    try:
        conn = get_connection()
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO servers (ip, motd, players_online, players_max, version, player_names)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT DO NOTHING
            """, (ip, motd, players_online, players_max, version, player_names))
        conn.commit()
        print(f"[+] Inserted server: {ip}")
    except Exception as e:
        print(f"[!] Insert failed for {ip}: {e}")
    finally:
        conn.close()

def insert_server_batch(server_data):
    if not server_data:
        return
    query = """
        INSERT INTO servers (ip, motd, players_online, players_max, player_names, version)
        VALUES %s
        ON CONFLICT DO NOTHING
    """
    try:
        conn = get_connection()
        with conn.cursor() as cur:
            execute_values(cur, query, server_data)
        conn.commit()
        print(f"[+] Batch inserted: {len(server_data)} entries")
    except Exception as e:
        print(f"[!] Batch insert failed: {e}")
    finally:
        conn.close()
