
import psycopg2
from psycopg2.extras import execute_values
import os

DB_HOST = os.getenv("POSTGRES_HOST", "postgres")
DB_NAME = os.getenv("POSTGRES_DB", "mcdata")
DB_USER = os.getenv("POSTGRES_USER", "mcscanner")
DB_PASS = os.getenv("POSTGRES_PASSWORD", "mcscannerpass")

conn = psycopg2.connect(
    host=DB_HOST,
    dbname=DB_NAME,
    user=DB_USER,
    password=DB_PASS
)
conn.autocommit = True

CREATE_TABLE_QUERY = """
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
"""

with conn.cursor() as cur:
    cur.execute(CREATE_TABLE_QUERY)


def insert_server_batch(server_data):
    if not server_data:
        return
    query = """
        INSERT INTO servers (ip, motd, players_online, players_max, player_names, version)
        VALUES %s
        ON CONFLICT DO NOTHING
    """
    with conn.cursor() as cur:
        execute_values(cur, query, server_data)

def insert_server_info(ip, motd, players_online, players_max, version, player_names):
    query = """
        INSERT INTO servers (ip, motd, players_online, players_max, version, player_names)
        VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT DO NOTHING
    """
    with conn.cursor() as cur:
        cur.execute(query, (
            ip,
            motd,
            players_online,
            players_max,
            version,
            [name.strip() for name in player_names.split(",")] if player_names else None
        ))
