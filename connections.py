"""
Connection Store - Manages user/team API key configurations.
Stores connection configs in PostgreSQL when DATABASE_URL is set,
otherwise falls back to a JSON file for local dev.

Each connection represents one team's setup:
  - Transcript source (Fireflies, Zoom, Gong, Teams, Google Meet)
  - Source-specific API keys
  - CRM choice + API key (to create deals)
  - Scoring preferences (framework, thresholds)
"""

import json
import logging
import secrets
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)

CONNECTIONS_FILE = Path(__file__).parent / ".connections.json"

# Column names matching the DB schema (excluding webhook_id which is the key)
_CONN_FIELDS = [
    "name", "transcript_source", "fireflies_api_key", "crm", "crm_api_key",
    "framework", "auto_create_threshold", "notify_slack", "slack_webhook_url",
    "zoom_webhook_secret", "zoom_account_id", "zoom_client_id", "zoom_client_secret",
    "zoom_user_email", "gong_api_key", "gong_api_secret",
    "teams_access_token", "google_access_token", "active", "shadow_mode",
]


def _use_pg() -> bool:
    """Check if PostgreSQL is available."""
    try:
        import database
        return database.is_available()
    except Exception:
        return False


# ── JSON file fallback ────────────────────────────────────────────────────────

def _load() -> dict:
    if CONNECTIONS_FILE.exists():
        return json.loads(CONNECTIONS_FILE.read_text())
    return {}


def _save(data: dict):
    CONNECTIONS_FILE.write_text(json.dumps(data, indent=2))


# ── PostgreSQL helpers ────────────────────────────────────────────────────────

def _row_to_dict(row, columns) -> dict:
    """Convert a DB row tuple to a dict."""
    return dict(zip(columns, row))


# ── Public API (same signatures as before) ────────────────────────────────────

def create_connection(
    name: str,
    crm: str,
    crm_api_key: str,
    transcript_source: str = "fireflies",
    fireflies_api_key: str = "",
    framework: str = "custom",
    auto_create_threshold: int = 70,
    notify_slack: bool = False,
    slack_webhook_url: str = "",
    zoom_webhook_secret: str = "",
    gong_api_key: str = "",
    gong_api_secret: str = "",
    teams_access_token: str = "",
    google_access_token: str = "",
    shadow_mode: bool = False,
) -> dict:
    """
    Register a new connection (team/user config).
    Returns the connection dict including a generated webhook_id
    that the transcript source will use to call back.
    """
    webhook_id = secrets.token_urlsafe(16)

    conn = {
        "name": name,
        "transcript_source": transcript_source,
        "fireflies_api_key": fireflies_api_key,
        "crm": crm,
        "crm_api_key": crm_api_key,
        "framework": framework,
        "auto_create_threshold": auto_create_threshold,
        "notify_slack": notify_slack,
        "slack_webhook_url": slack_webhook_url,
        "zoom_webhook_secret": zoom_webhook_secret,
        "gong_api_key": gong_api_key,
        "gong_api_secret": gong_api_secret,
        "teams_access_token": teams_access_token,
        "google_access_token": google_access_token,
        "webhook_id": webhook_id,
        "active": True,
        "shadow_mode": shadow_mode,
    }

    if _use_pg():
        import database
        db = database.get_conn()
        try:
            cur = db.cursor()
            cur.execute(
                """INSERT INTO connections (webhook_id, name, transcript_source, fireflies_api_key,
                   crm, crm_api_key, framework, auto_create_threshold, notify_slack,
                   slack_webhook_url, zoom_webhook_secret, gong_api_key, gong_api_secret,
                   teams_access_token, google_access_token, active, shadow_mode)
                   VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
                (webhook_id, name, transcript_source, fireflies_api_key,
                 crm, crm_api_key, framework, auto_create_threshold, notify_slack,
                 slack_webhook_url, zoom_webhook_secret, gong_api_key, gong_api_secret,
                 teams_access_token, google_access_token, True, shadow_mode),
            )
            db.commit()
            cur.close()
        except Exception as e:
            db.rollback()
            logger.error(f"Failed to create connection in DB: {e}")
            raise
        finally:
            database.put_conn(db)
    else:
        connections = _load()
        connections[webhook_id] = conn
        _save(connections)

    logger.info(f"Created connection '{name}' (source: {transcript_source}, webhook_id: {webhook_id})")
    return conn


def get_connection(webhook_id: str) -> Optional[dict]:
    """Look up a connection by its webhook_id."""
    if _use_pg():
        import database
        db = database.get_conn()
        try:
            cur = db.cursor()
            columns = ["webhook_id"] + _CONN_FIELDS
            cur.execute(
                f"SELECT {', '.join(columns)} FROM connections WHERE webhook_id = %s",
                (webhook_id,),
            )
            row = cur.fetchone()
            cur.close()
            if row:
                return _row_to_dict(row, columns)
            return None
        except Exception as e:
            logger.error(f"Failed to get connection from DB: {e}")
            return None
        finally:
            database.put_conn(db)
    else:
        connections = _load()
        return connections.get(webhook_id)


def list_connections() -> list[dict]:
    """List all connections (keys masked)."""
    if _use_pg():
        import database
        db = database.get_conn()
        try:
            cur = db.cursor()
            columns = ["webhook_id"] + _CONN_FIELDS
            cur.execute(f"SELECT {', '.join(columns)} FROM connections")
            rows = cur.fetchall()
            cur.close()
            raw = {r[0]: _row_to_dict(r, columns) for r in rows}
        except Exception as e:
            logger.error(f"Failed to list connections from DB: {e}")
            return []
        finally:
            database.put_conn(db)
    else:
        raw = _load()

    result = []
    for wid, conn in raw.items():
        masked = {
            "webhook_id": wid,
            "name": conn["name"],
            "transcript_source": conn.get("transcript_source", "fireflies"),
            "crm": conn["crm"],
            "framework": conn["framework"],
            "active": conn.get("active", True),
            "source_connected": bool(
                conn.get("fireflies_api_key")
                or conn.get("zoom_webhook_secret")
                or conn.get("gong_api_key")
                or conn.get("teams_access_token")
                or conn.get("google_access_token")
            ),
            "crm_connected": bool(conn.get("crm_api_key")),
        }
        result.append(masked)
    return result


def list_connections_full() -> list[dict]:
    """List all connections with full data including API keys. Internal use only (polling worker)."""
    if _use_pg():
        import database
        db = database.get_conn()
        try:
            cur = db.cursor()
            columns = ["webhook_id"] + _CONN_FIELDS
            cur.execute(f"SELECT {', '.join(columns)} FROM connections WHERE active = TRUE")
            rows = cur.fetchall()
            cur.close()
            return [_row_to_dict(r, columns) for r in rows]
        except Exception as e:
            logger.error(f"Failed to list full connections from DB: {e}")
            return []
        finally:
            database.put_conn(db)
    else:
        raw = _load()
        return [{"webhook_id": wid, **conn} for wid, conn in raw.items() if conn.get("active", True)]


def update_connection(webhook_id: str, updates: dict) -> Optional[dict]:
    """Update fields on an existing connection."""
    if _use_pg():
        import database
        # Filter to only known fields
        valid_updates = {k: v for k, v in updates.items() if k in _CONN_FIELDS}
        if not valid_updates:
            return get_connection(webhook_id)

        db = database.get_conn()
        try:
            cur = db.cursor()
            set_clause = ", ".join(f"{k} = %s" for k in valid_updates)
            values = list(valid_updates.values()) + [webhook_id]
            cur.execute(
                f"UPDATE connections SET {set_clause} WHERE webhook_id = %s",
                values,
            )
            if cur.rowcount == 0:
                cur.close()
                return None
            db.commit()
            cur.close()
        except Exception as e:
            db.rollback()
            logger.error(f"Failed to update connection in DB: {e}")
            return None
        finally:
            database.put_conn(db)
        return get_connection(webhook_id)
    else:
        connections = _load()
        if webhook_id not in connections:
            return None
        connections[webhook_id].update(updates)
        _save(connections)
        return connections[webhook_id]


def delete_connection(webhook_id: str) -> bool:
    """Delete a connection."""
    if _use_pg():
        import database
        db = database.get_conn()
        try:
            cur = db.cursor()
            cur.execute("DELETE FROM connections WHERE webhook_id = %s", (webhook_id,))
            deleted = cur.rowcount > 0
            db.commit()
            cur.close()
            return deleted
        except Exception as e:
            db.rollback()
            logger.error(f"Failed to delete connection from DB: {e}")
            return False
        finally:
            database.put_conn(db)
    else:
        connections = _load()
        if webhook_id not in connections:
            return False
        del connections[webhook_id]
        _save(connections)
        return True
