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
                company_name TEXT DEFAULT '',
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );
        """)
        # Migration: add company_name if table already existed without it
        cur.execute("""
            DO $$
            BEGIN
                IF NOT EXISTS (
                    SELECT 1 FROM information_schema.columns
                    WHERE table_name = 'scored_deals' AND column_name = 'company_name'
                ) THEN
                    ALTER TABLE scored_deals ADD COLUMN company_name TEXT DEFAULT '';
                END IF;
            END $$;
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS processed_transcripts (
                transcript_id TEXT NOT NULL,
                connection_name TEXT NOT NULL DEFAULT 'Default',
                processed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                score INTEGER,
                status TEXT DEFAULT 'success',
                error_message TEXT DEFAULT '',
                PRIMARY KEY (transcript_id, connection_name)
            );
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS users (
                id TEXT PRIMARY KEY,
                email TEXT UNIQUE NOT NULL,
                name TEXT DEFAULT '',
                created_at TIMESTAMPTZ DEFAULT NOW()
            );
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS sessions (
                token TEXT PRIMARY KEY,
                user_id TEXT NOT NULL,
                type TEXT NOT NULL,
                expires_at TIMESTAMPTZ NOT NULL,
                created_at TIMESTAMPTZ DEFAULT NOW()
            );
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS calibration_results (
                id SERIAL PRIMARY KEY,
                deal_id TEXT NOT NULL,
                deal_name TEXT,
                company_name TEXT,
                crm_stage TEXT,
                transcript_id TEXT,
                fairplay_score INTEGER,
                framework TEXT,
                recommendation TEXT,
                breakdown JSONB DEFAULT '{}',
                matched_by TEXT DEFAULT 'company_name',
                created_at TIMESTAMPTZ DEFAULT NOW()
            );
        """)
        # Migrations
        cur.execute("""
            DO $$
            BEGIN
                IF NOT EXISTS (
                    SELECT 1 FROM information_schema.columns
                    WHERE table_name = 'connections' AND column_name = 'user_id'
                ) THEN
                    ALTER TABLE connections ADD COLUMN user_id TEXT DEFAULT '';
                END IF;
                IF NOT EXISTS (
                    SELECT 1 FROM information_schema.columns
                    WHERE table_name = 'connections' AND column_name = 'shadow_mode'
                ) THEN
                    ALTER TABLE connections ADD COLUMN shadow_mode BOOLEAN DEFAULT FALSE;
                END IF;
                IF NOT EXISTS (
                    SELECT 1 FROM information_schema.columns
                    WHERE table_name = 'connections' AND column_name = 'zoom_account_id'
                ) THEN
                    ALTER TABLE connections ADD COLUMN zoom_account_id TEXT DEFAULT '';
                    ALTER TABLE connections ADD COLUMN zoom_client_id TEXT DEFAULT '';
                    ALTER TABLE connections ADD COLUMN zoom_client_secret TEXT DEFAULT '';
                    ALTER TABLE connections ADD COLUMN zoom_user_email TEXT DEFAULT '';
                END IF;
                IF NOT EXISTS (
                    SELECT 1 FROM information_schema.columns
                    WHERE table_name = 'connections' AND column_name = 'sale_type'
                ) THEN
                    ALTER TABLE connections ADD COLUMN sale_type TEXT DEFAULT '';
                    ALTER TABLE connections ADD COLUMN deal_value_range TEXT DEFAULT '';
                    ALTER TABLE connections ADD COLUMN avg_days_to_close TEXT DEFAULT '';
                    ALTER TABLE connections ADD COLUMN industry_vertical TEXT DEFAULT '';
                END IF;
                IF NOT EXISTS (
                    SELECT 1 FROM information_schema.columns
                    WHERE table_name = 'connections' AND column_name = 'company_website'
                ) THEN
                    ALTER TABLE connections ADD COLUMN company_website TEXT DEFAULT '';
                    ALTER TABLE connections ADD COLUMN company_icp TEXT DEFAULT '';
                END IF;
                IF NOT EXISTS (
                    SELECT 1 FROM information_schema.columns
                    WHERE table_name = 'connections' AND column_name = 'framework_weights'
                ) THEN
                    ALTER TABLE connections ADD COLUMN framework_weights TEXT DEFAULT '';
                END IF;
                IF NOT EXISTS (
                    SELECT 1 FROM information_schema.columns
                    WHERE table_name = 'connections' AND column_name = 'teams_webhook_url'
                ) THEN
                    ALTER TABLE connections ADD COLUMN teams_webhook_url TEXT DEFAULT '';
                END IF;
                IF NOT EXISTS (
                    SELECT 1 FROM information_schema.columns
                    WHERE table_name = 'connections' AND column_name = 'fathom_api_key'
                ) THEN
                    ALTER TABLE connections ADD COLUMN fathom_api_key TEXT DEFAULT '';
                END IF;
                IF NOT EXISTS (
                    SELECT 1 FROM information_schema.columns
                    WHERE table_name = 'connections' AND column_name = 'calibration_notes'
                ) THEN
                    ALTER TABLE connections ADD COLUMN calibration_notes TEXT DEFAULT '';
                END IF;
                IF NOT EXISTS (
                    SELECT 1 FROM information_schema.columns
                    WHERE table_name = 'connections' AND column_name = 'attio_stage_qualified'
                ) THEN
                    ALTER TABLE connections ADD COLUMN attio_stage_qualified TEXT DEFAULT '';
                    ALTER TABLE connections ADD COLUMN attio_stage_review TEXT DEFAULT '';
                    ALTER TABLE connections ADD COLUMN hubspot_stage_qualified TEXT DEFAULT '';
                    ALTER TABLE connections ADD COLUMN hubspot_stage_review TEXT DEFAULT '';
                END IF;
                IF NOT EXISTS (
                    SELECT 1 FROM information_schema.columns
                    WHERE table_name = 'connections' AND column_name = 'last_org_health_alert'
                ) THEN
                    ALTER TABLE connections ADD COLUMN last_org_health_alert TIMESTAMPTZ;
                END IF;
            END $$;
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
