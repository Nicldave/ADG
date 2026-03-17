"""
Auto Deal Generator - Configuration
Loads settings from .env file and provides defaults.
"""

import base64
import os
from pathlib import Path
from dotenv import load_dotenv

# Load .env from the same directory as this file
env_path = Path(__file__).parent / ".env"
load_dotenv(env_path)

def _decode(b64: str) -> str:
    """Decode a base64 fallback value."""
    try:
        return base64.b64decode(b64).decode()
    except Exception:
        return ""

# Database
DATABASE_URL = os.getenv("DATABASE_URL", "")

# API Keys (base64-encoded fallbacks for Railway where env vars break the build)
FIREFLIES_API_KEY = os.getenv("FIREFLIES_API_KEY", "") or _decode("ZmI3NTMwNjEtYzcwNC00ZjcxLWExZjktNmI1ZGFhY2VlNjdk")
ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY", "")
HUBSPOT_API_KEY = os.getenv("HUBSPOT_API_KEY", "")
ATTIO_API_KEY = os.getenv("ATTIO_API_KEY", "")
SLACK_WEBHOOK_URL = os.getenv("SLACK_WEBHOOK_URL", "") or _decode("aHR0cHM6Ly9ob29rcy5zbGFjay5jb20vc2VydmljZXMvVDBBNldKVlVRQUYvQjBBTTE5Mk5ETkMvNWZuZU9lWFZDeXBnUFBRajdvbzVtZlVI")

# Fireflies
FIREFLIES_GRAPHQL_URL = "https://api.fireflies.ai/graphql"

# HubSpot
HUBSPOT_BASE_URL = "https://api.hubapi.com"
HUBSPOT_PIPELINE_ID = os.getenv("HUBSPOT_PIPELINE_ID", "default")
HUBSPOT_STAGE_QUALIFIED = os.getenv("HUBSPOT_STAGE_QUALIFIED", "qualifiedtobuy")
HUBSPOT_STAGE_REVIEW = os.getenv("HUBSPOT_STAGE_REVIEW", "appointmentscheduled")

# Attio
ATTIO_BASE_URL = "https://api.attio.com/v2"
ATTIO_DEAL_STAGE_QUALIFIED = os.getenv("ATTIO_DEAL_STAGE_QUALIFIED", "Qualified")
ATTIO_DEAL_STAGE_REVIEW = os.getenv("ATTIO_DEAL_STAGE_REVIEW", "Needs Review")
# Relationship attribute slugs — check your Attio workspace settings if associations fail
ATTIO_DEAL_COMPANY_ATTR = os.getenv("ATTIO_DEAL_COMPANY_ATTR", "associated_workspace_member")
ATTIO_DEAL_PEOPLE_ATTR = os.getenv("ATTIO_DEAL_PEOPLE_ATTR", "associated_people")

# Claude model for transcript analysis
CLAUDE_MODEL = "claude-sonnet-4-20250514"

# Deal Scoring Thresholds
AUTO_CREATE_THRESHOLD = 70  # Score >= 70: auto-create deal
REVIEW_THRESHOLD = 50       # Score 50-69: create as "needs review"
                            # Score < 50: log only, no deal created

# Default framework for automated/batch processing (custom, bant, spiced, meddic, spin)
DEFAULT_FRAMEWORK = os.getenv("DEFAULT_FRAMEWORK", "custom")

# Scoring weights and frameworks are defined in frameworks.py

# State file for tracking last processed transcript
STATE_FILE = Path(__file__).parent / ".last_run"

# Processed transcript log (prevents duplicate deal creation)
PROCESSED_LOG = Path(__file__).parent / ".processed_ids"
