"""
Auto Deal Generator - PostgreSQL Database Layer
Connection pooling and table initialization using psycopg2.
Falls back gracefully when DATABASE_URL is not configured.
"""

import logging
import os

logger = logging.getLogger(__name__)

_pool = None


def _get_pool():
    """Get or create the connection pool. Returns None if DATABASE_URL not set."""
    global _pool
    if _pool is not None:
        return _pool

    database_url = os.getenv("DATABASE_URL", "")
    if not database_url:
        return None

    try:
        from psycopg2 import pool as pg_pool
        _pool = pg_pool.ThreadedConnectionPool(1, 10, database_url)
        logger.info("PostgreSQL connection pool created")
        return _pool
    except Exception as e:
        logger.warning(f"Failed to create PostgreSQL pool: {e}")
        return None


def get_conn():
    """Get a connection from the pool. Returns None if unavailable."""
    p = _get_pool()
    if p is None:
        return None
    try:
        return p.getconn()
    except Exception as e:
        logger.warning(f"Failed to get DB connection: {e}")
        return None


def put_conn(conn):
    """Return a connection to the pool."""
    p = _get_pool()
    if p and conn:
        p.putconn(conn)


def init_db():
    """Create tables if they don't exist. No-op if no database configured."""
    conn = get_conn()
    if conn is None:
        logger.info("DATABASE_URL not set, skipping PostgreSQL init")
        return False

    try:
        cur = conn.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS connections (
                webhook_id TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                transcript_source TEXT NOT NULL DEFAULT 'fireflies',
                fireflies_api_key TEXT DEFAULT '',
                crm TEXT NOT NULL,
                crm_api_key TEXT NOT NULL,
                framework TEXT NOT NULL DEFAULT 'custom',
                auto_create_threshold INTEGER DEFAULT 70,
                notify_slack BOOLEAN DEFAULT FALSE,
                slack_webhook_url TEXT DEFAULT '',
                zoom_webhook_secret TEXT DEFAULT '',
                gong_api_key TEXT DEFAULT '',
                gong_api_secret TEXT DEFAULT '',
                teams_access_token TEXT DEFAULT '',
                google_access_token TEXT DEFAULT '',
                active BOOLEAN DEFAULT TRUE
            );
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS feedback (
                id SERIAL PRIMARY KEY,
                deal_id TEXT NOT NULL,
                vote TEXT NOT NULL,
                note TEXT DEFAULT '',
                timestamp TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS scored_deals (
                id SERIAL PRIMARY KEY,
                deal_id TEXT,
                deal_name TEXT NOT NULL,
                meeting_title TEXT,
                score INTEGER NOT NULL,
                recommendation TEXT NOT NULL,
                framework TEXT DEFAULT 'custom',
                breakdown JSONB DEFAULT '{}',
                analysis JSONB DEFAULT '{}',
                metadata JSONB DEFAULT '{}',
                key_insight TEXT DEFAULT '',
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );
        """)
        conn.commit()
        cur.close()
        logger.info("PostgreSQL tables initialized")
        return True
    except Exception as e:
        conn.rollback()
        logger.warning(f"Failed to initialize database tables: {e}")
        return False
    finally:
        put_conn(conn)


def is_available() -> bool:
    """Check if PostgreSQL is configured and reachable."""
    conn = get_conn()
    if conn is None:
        return False
    put_conn(conn)
    return True
