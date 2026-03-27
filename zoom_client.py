"""
Zoom Client - Cloud Recording & Transcript Access
Pulls historical cloud recordings and transcripts from Zoom API.

Requires a Server-to-Server OAuth app in Zoom Marketplace:
  - Account ID, Client ID, Client Secret
  - Scopes: cloud_recording:read:list_user_recordings, cloud_recording:read:recording

Alternative: If only webhook secret is available, historical pull is not possible.
Only real-time processing via webhooks works.
"""

import logging
import time
from datetime import datetime, timedelta
from typing import Optional

import requests

logger = logging.getLogger(__name__)

_token_cache = {"token": "", "expires": 0}


def _get_access_token(account_id: str, client_id: str, client_secret: str) -> str:
    """Get Zoom OAuth access token using Server-to-Server credentials."""
    now = time.time()
    if _token_cache["token"] and _token_cache["expires"] > now:
        return _token_cache["token"]

    try:
        resp = requests.post(
            "https://zoom.us/oauth/token",
            params={"grant_type": "account_credentials", "account_id": account_id},
            auth=(client_id, client_secret),
            timeout=15,
        )
        resp.raise_for_status()
        data = resp.json()
        _token_cache["token"] = data["access_token"]
        _token_cache["expires"] = now + data.get("expires_in", 3600) - 60
        return _token_cache["token"]
    except Exception as e:
        logger.error(f"Zoom OAuth failed: {e}")
        return ""


def list_users(
    account_id: str = "",
    client_id: str = "",
    client_secret: str = "",
) -> list:
    """List all users on the Zoom account."""
    token = _get_access_token(account_id, client_id, client_secret)
    if not token:
        return []
    try:
        resp = requests.get(
            "https://api.zoom.us/v2/users",
            headers={"Authorization": f"Bearer {token}"},
            params={"page_size": 300, "status": "active"},
            timeout=15,
        )
        resp.raise_for_status()
        return resp.json().get("users", [])
    except Exception as e:
        logger.error(f"Zoom list_users failed: {e}")
        return []


def list_recordings(
    user_email: str,
    since: Optional[datetime] = None,
    account_id: str = "",
    client_id: str = "",
    client_secret: str = "",
) -> list:
    """
    List cloud recordings for a Zoom user.
    Returns list of meetings with recording/transcript info.
    """
    token = _get_access_token(account_id, client_id, client_secret)
    if not token:
        return []

    if not since:
        since = datetime.now() - timedelta(days=30)

    from_date = since.strftime("%Y-%m-%d")
    to_date = datetime.now().strftime("%Y-%m-%d")

    try:
        resp = requests.get(
            f"https://api.zoom.us/v2/users/{user_email}/recordings",
            headers={"Authorization": f"Bearer {token}"},
            params={"from": from_date, "to": to_date, "page_size": 100},
            timeout=30,
        )
        resp.raise_for_status()
        data = resp.json()

        recordings = []
        for meeting in data.get("meetings", []):
            transcript_url = None
            for rf in meeting.get("recording_files", []):
                if rf.get("file_type") == "TRANSCRIPT" or rf.get("recording_type") == "audio_transcript":
                    transcript_url = rf.get("download_url")
                    break

            recordings.append({
                "id": str(meeting.get("id", "")),
                "title": meeting.get("topic", "Zoom Meeting"),
                "date": meeting.get("start_time", ""),
                "duration": meeting.get("duration", 0),
                "transcript_url": transcript_url,
                "has_transcript": transcript_url is not None,
                "participants": [p.get("name", "") for p in meeting.get("participant_audio_files", [])],
            })

        logger.info(f"Found {len(recordings)} Zoom recordings for {user_email} ({from_date} to {to_date})")
        return recordings
    except Exception as e:
        logger.error(f"Zoom list recordings failed: {e}")
        return []


def download_transcript(transcript_url: str, account_id: str = "", client_id: str = "", client_secret: str = "") -> str:
    """Download and parse a Zoom transcript from a recording URL."""
    token = _get_access_token(account_id, client_id, client_secret)
    if not token:
        return ""

    try:
        # Zoom download URLs need the access token as a query param
        url = f"{transcript_url}?access_token={token}"
        resp = requests.get(url, timeout=60)
        resp.raise_for_status()
        content = resp.text

        # Parse VTT if applicable
        if "WEBVTT" in content[:50]:
            return _parse_vtt(content)
        return content
    except Exception as e:
        logger.error(f"Zoom transcript download failed: {e}")
        return ""


def _parse_vtt(content: str) -> str:
    """Parse WebVTT subtitle format into readable transcript."""
    lines = content.strip().split("\n")
    result = []
    current_speaker = ""
    for line in lines:
        line = line.strip()
        if not line or line == "WEBVTT" or line.startswith("NOTE") or "-->" in line:
            continue
        if line[0].isdigit() and len(line) < 5:
            continue
        # Speaker lines often start with "Speaker Name:"
        if ":" in line and not line.startswith("http"):
            parts = line.split(":", 1)
            if len(parts[0]) < 40:
                speaker = parts[0].strip()
                text = parts[1].strip()
                if speaker != current_speaker:
                    current_speaker = speaker
                    result.append(f"\n**{speaker}:** {text}")
                else:
                    result.append(text)
                continue
        result.append(line)
    return " ".join(result)
