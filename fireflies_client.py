"""
Fireflies.ai API Client
Pulls meeting transcripts via GraphQL API.
"""

import json
import logging
from datetime import datetime, timedelta
from typing import Optional

import requests

from config import FIREFLIES_API_KEY, FIREFLIES_GRAPHQL_URL

logger = logging.getLogger(__name__)


def _graphql_request(query: str, variables: Optional[dict] = None, api_key: Optional[str] = None) -> dict:
    """Execute a GraphQL request against Fireflies API."""
    key = api_key or FIREFLIES_API_KEY
    headers = {
        "Authorization": f"Bearer {key}",
        "Content-Type": "application/json",
    }
    payload = {"query": query}
    if variables:
        payload["variables"] = variables

    response = requests.post(FIREFLIES_GRAPHQL_URL, headers=headers, json=payload)
    response.raise_for_status()

    data = response.json()
    if "errors" in data:
        raise Exception(f"Fireflies API error: {data['errors']}")
    return data["data"]


def list_transcripts(since: Optional[datetime] = None, limit: int = 20, api_key: Optional[str] = None) -> list[dict]:
    """
    List recent transcripts from Fireflies.

    Args:
        since: Only return transcripts after this datetime. Defaults to last 7 days.
        limit: Max number of transcripts to return.
        api_key: Optional Fireflies API key (overrides server default).

    Returns:
        List of transcript summaries with id, title, date, duration, participants.
    """
    query = """
    query Transcripts($limit: Int) {
        transcripts(limit: $limit) {
            id
            title
            date
            duration
            organizer_email
            participants
            transcript_url
        }
    }
    """
    data = _graphql_request(query, {"limit": limit}, api_key=api_key)
    transcripts = data.get("transcripts", [])

    # Filter by date if specified
    if since:
        since_ts = since.timestamp()
        transcripts = [
            t for t in transcripts
            if t.get("date") and t["date"] / 1000 >= since_ts
        ]

    logger.info(f"Found {len(transcripts)} transcripts")
    return transcripts


def get_transcript(transcript_id: str, api_key: Optional[str] = None) -> dict:
    """
    Get full transcript content with speaker labels and sentences.

    Args:
        transcript_id: Fireflies transcript ID.

    Returns:
        Full transcript data including sentences with speaker labels.
    """
    query = """
    query Transcript($transcriptId: String!) {
        transcript(id: $transcriptId) {
            id
            title
            date
            duration
            organizer_email
            participants
            transcript_url
            sentences {
                index
                speaker_name
                speaker_id
                text
                raw_text
                start_time
                end_time
            }
            summary {
                action_items
                outline
                shorthand_bullet
                overview
                keywords
            }
        }
    }
    """
    data = _graphql_request(query, {"transcriptId": transcript_id}, api_key=api_key)
    transcript = data.get("transcript")

    if not transcript:
        raise Exception(f"Transcript {transcript_id} not found")

    logger.info(
        f"Retrieved transcript: {transcript['title']} "
        f"({len(transcript.get('sentences', []))} sentences)"
    )
    return transcript


def format_transcript_text(transcript: dict) -> str:
    """
    Convert Fireflies transcript data into readable text with speaker labels.

    Args:
        transcript: Full transcript dict from get_transcript().

    Returns:
        Formatted transcript string with speaker labels.
    """
    sentences = transcript.get("sentences", [])
    if not sentences:
        return ""

    lines = []
    current_speaker = None

    for sentence in sentences:
        speaker = sentence.get("speaker_name", "Unknown")
        text = sentence.get("text", "").strip()

        if not text:
            continue

        if speaker != current_speaker:
            lines.append(f"\n**{speaker}:** {text}")
            current_speaker = speaker
        else:
            lines.append(text)

    return "\n".join(lines)


def get_meeting_metadata(transcript: dict) -> dict:
    """
    Extract meeting metadata from a transcript.

    Returns:
        Dict with title, date, duration_minutes, participants, organizer, summary.
    """
    date_val = transcript.get("date")
    meeting_date = None
    if date_val:
        meeting_date = datetime.fromtimestamp(date_val / 1000).isoformat()

    duration_seconds = transcript.get("duration", 0)
    summary_data = transcript.get("summary", {})

    return {
        "title": transcript.get("title", "Untitled Meeting"),
        "date": meeting_date,
        "duration_minutes": round(duration_seconds / 60, 1) if duration_seconds else 0,
        "participants": transcript.get("participants", []),
        "organizer": transcript.get("organizer_email", ""),
        "action_items": summary_data.get("action_items", ""),
        "summary": summary_data.get("overview", ""),
        "keywords": summary_data.get("keywords", ""),
        "transcript_url": transcript.get("transcript_url", ""),
    }
