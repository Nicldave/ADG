"""
Connection Store - Manages user/team API key configurations.
Stores connection configs in a JSON file so the automated pipeline
knows which transcript source to pull from and which CRM to push to.

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


def _load() -> dict:
    if CONNECTIONS_FILE.exists():
        return json.loads(CONNECTIONS_FILE.read_text())
    return {}


def _save(data: dict):
    CONNECTIONS_FILE.write_text(json.dumps(data, indent=2))


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
) -> dict:
    """
    Register a new connection (team/user config).
    Returns the connection dict including a generated webhook_id
    that the transcript source will use to call back.
    """
    connections = _load()
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
    }

    connections[webhook_id] = conn
    _save(connections)
    logger.info(f"Created connection '{name}' (source: {transcript_source}, webhook_id: {webhook_id})")
    return conn


def get_connection(webhook_id: str) -> Optional[dict]:
    """Look up a connection by its webhook_id."""
    connections = _load()
    return connections.get(webhook_id)


def list_connections() -> list[dict]:
    """List all connections (keys masked)."""
    connections = _load()
    result = []
    for wid, conn in connections.items():
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


def update_connection(webhook_id: str, updates: dict) -> Optional[dict]:
    """Update fields on an existing connection."""
    connections = _load()
    if webhook_id not in connections:
        return None
    connections[webhook_id].update(updates)
    _save(connections)
    return connections[webhook_id]


def delete_connection(webhook_id: str) -> bool:
    """Delete a connection."""
    connections = _load()
    if webhook_id not in connections:
        return False
    del connections[webhook_id]
    _save(connections)
    return True
