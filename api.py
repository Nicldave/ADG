"""
Auto Deal Generator - FastAPI wrapper
Exposes the analysis and deal creation pipeline as HTTP endpoints
for external frontends (Lovable, custom React apps, etc.)

Run: uvicorn api:app --reload --port 8000
"""

import json
import logging
import os
import sys
from datetime import datetime
from pathlib import Path
from typing import Optional

from fastapi import FastAPI, HTTPException, BackgroundTasks, Request, UploadFile, File, Form
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field

# Add this directory to path so local modules resolve
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import transcript_analyzer
import deal_scorer
import crm as crm_factory
import fireflies_client
import connections
import database
from frameworks import FRAMEWORKS, FRAMEWORK_NAMES, get_framework
from config import AUTO_CREATE_THRESHOLD, REVIEW_THRESHOLD

logger = logging.getLogger(__name__)

# Initialize PostgreSQL tables on startup (no-op if DATABASE_URL not set)
try:
    database.init_db()
except Exception as e:
    logger.warning(f"Database init skipped: {e}")

app = FastAPI(
    title="Auto Deal Generator API",
    description="Analyze sales transcripts and create CRM deals.",
    version="1.0.0",
)

# Allow Lovable and local dev origins
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ── Request / Response models ────────────────────────────────────────────────

class AnalyzeRequest(BaseModel):
    transcript: str = Field(..., min_length=50, description="Full transcript text with speaker labels")
    framework: str = Field("custom", description="Scoring framework: custom, bant, spiced, meddic, spin")
    meeting_title: Optional[str] = None
    meeting_date: Optional[str] = None


class CreateDealRequest(BaseModel):
    analysis: dict = Field(..., description="Output from /analyze")
    score_result: dict = Field(..., description="Output from /analyze")
    crm: str = Field("attio", description="CRM target: hubspot or attio")
    dry_run: bool = Field(True, description="If true, simulates without creating")
    crm_api_key: Optional[str] = Field(None, description="User's own CRM API key. If omitted, uses server default.")


class AnalyzeResponse(BaseModel):
    analysis: dict
    score_result: dict
    score: int
    recommendation: str
    deal_name: str
    framework: str
    key_insight: Optional[str] = None


class CreateDealResponse(BaseModel):
    success: bool
    deal_id: Optional[str] = None
    deal_name: Optional[str] = None
    deal_url: Optional[str] = None
    dry_run: bool = False


class FrameworkInfo(BaseModel):
    key: str
    name: str
    description: str
    categories: dict


# ── Endpoints ────────────────────────────────────────────────────────────────

@app.get("/frameworks", response_model=list[FrameworkInfo])
def list_frameworks():
    """List all available scoring frameworks with their categories and weights."""
    result = []
    for key, fw in FRAMEWORKS.items():
        result.append(FrameworkInfo(
            key=key,
            name=fw["name"],
            description=fw.get("description", ""),
            categories={
                k: {"weight": v["weight"], "label": v["label"]}
                for k, v in fw["categories"].items()
            },
        ))
    return result


@app.post("/analyze", response_model=AnalyzeResponse)
def analyze(req: AnalyzeRequest):
    """
    Analyze a sales transcript. Returns structured analysis + Strike Zone score.
    Auto-creates deal in Attio (if score >= 50) and sends Slack notification
    using server default API keys.
    """
    if req.framework not in FRAMEWORK_NAMES:
        raise HTTPException(
            status_code=400,
            detail=f"Unknown framework: '{req.framework}'. Options: {', '.join(FRAMEWORK_NAMES)}",
        )

    metadata = {
        "title": req.meeting_title or "API Transcript",
        "date": req.meeting_date or datetime.now().isoformat(),
        "source": "api",
        "participants": [],
    }

    try:
        analysis = transcript_analyzer.analyze_transcript(
            req.transcript, metadata, framework=req.framework
        )
        score_result = deal_scorer.score_deal(analysis)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Analysis failed: {str(e)}")

    # Auto-create deal if it's a sales conversation with sufficient score
    deal_result = None
    if analysis.get("is_sales_conversation") and score_result["total_score"] >= REVIEW_THRESHOLD:
        try:
            crm_client = crm_factory.get_client("attio")
            deal_result = crm_client.create_deal(score_result, analysis, metadata, dry_run=False)
            if deal_result:
                logger.info(f"Auto-created Attio deal: {deal_result.get('deal_name')} (score: {score_result['total_score']})")
        except Exception as e:
            logger.warning(f"Auto deal creation failed: {e}")

    # Slack notification
    from config import SLACK_WEBHOOK_URL
    if SLACK_WEBHOOK_URL:
        try:
            _send_slack_notification(
                SLACK_WEBHOOK_URL, score_result, analysis, metadata,
                deal_id=deal_result.get("deal_id") if deal_result else None,
            )
        except Exception as e:
            logger.warning(f"Slack notification failed: {e}")

    return AnalyzeResponse(
        analysis=analysis,
        score_result=score_result,
        score=score_result["total_score"],
        recommendation=score_result["recommendation"],
        deal_name=score_result.get("deal_name_suggestion", ""),
        framework=req.framework,
        key_insight=score_result.get("key_insight"),
    )


@app.post("/create-deal", response_model=CreateDealResponse)
def create_deal(req: CreateDealRequest):
    """
    Create a deal in the selected CRM from a previously analyzed transcript.
    Pass the analysis and score_result from /analyze.
    """
    if req.crm not in ("hubspot", "attio"):
        raise HTTPException(status_code=400, detail=f"Unsupported CRM: '{req.crm}'. Options: hubspot, attio")

    try:
        crm_client = crm_factory.get_client(req.crm)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

    metadata = {
        "title": req.analysis.get("summary", "Deal"),
        "date": datetime.now().isoformat(),
        "source": "api",
    }

    try:
        result = crm_client.create_deal(
            req.score_result, req.analysis, metadata, dry_run=req.dry_run,
            api_key=req.crm_api_key,
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Deal creation failed: {str(e)}")

    if not result:
        return CreateDealResponse(success=False, dry_run=req.dry_run)

    return CreateDealResponse(
        success=True,
        deal_id=result.get("deal_id"),
        deal_name=result.get("deal_name"),
        deal_url=result.get("deal_url"),
        dry_run=result.get("dry_run", req.dry_run),
    )


# ── Connection management ────────────────────────────────────────────────────

SUPPORTED_SOURCES = {"fireflies", "zoom", "gong", "teams", "google_meet"}


class ConnectionRequest(BaseModel):
    name: str = Field(..., description="Team or user name")
    transcript_source: str = Field("fireflies", description="Transcript source: fireflies, zoom, gong, teams, google_meet")
    fireflies_api_key: Optional[str] = Field("", description="Fireflies.ai API key")
    crm: str = Field("attio", description="CRM: attio or hubspot")
    crm_api_key: str = Field(..., description="CRM API key")
    framework: str = Field("custom", description="Scoring framework")
    auto_create_threshold: int = Field(70, description="Score threshold for auto-creating deals")
    slack_webhook_url: Optional[str] = Field("", description="Slack webhook for notifications")
    # Source-specific keys
    zoom_webhook_secret: Optional[str] = Field("", description="Zoom webhook secret token")
    gong_api_key: Optional[str] = Field("", description="Gong API key (access key)")
    gong_api_secret: Optional[str] = Field("", description="Gong API secret (access key secret)")
    teams_access_token: Optional[str] = Field("", description="Microsoft Graph API access token")
    google_access_token: Optional[str] = Field("", description="Google OAuth access token")


class ConnectionResponse(BaseModel):
    webhook_id: str
    webhook_url: str
    name: str
    crm: str
    framework: str
    transcript_source: str
    active: bool


def _get_base_url() -> str:
    base = os.getenv("RAILWAY_PUBLIC_DOMAIN", "")
    if base:
        return f"https://{base}"
    return os.getenv("BASE_URL", "http://localhost:8000")


# Map source names to webhook path prefixes
SOURCE_WEBHOOK_PATHS = {
    "fireflies": "fireflies",
    "zoom": "zoom",
    "gong": "gong",
    "teams": "teams",
    "google_meet": "google-meet",
}


@app.post("/connections", response_model=ConnectionResponse)
def create_connection(req: ConnectionRequest):
    """
    Register a new connection. Returns a webhook_url to configure in your transcript source.
    Supports: Fireflies, Zoom, Gong, Microsoft Teams, Google Meet.
    """
    if req.crm not in ("hubspot", "attio"):
        raise HTTPException(status_code=400, detail=f"Unsupported CRM: '{req.crm}'")
    if req.framework not in FRAMEWORK_NAMES:
        raise HTTPException(status_code=400, detail=f"Unknown framework: '{req.framework}'")
    if req.transcript_source not in SUPPORTED_SOURCES:
        raise HTTPException(
            status_code=400,
            detail=f"Unsupported source: '{req.transcript_source}'. Options: {', '.join(SUPPORTED_SOURCES)}",
        )

    conn = connections.create_connection(
        name=req.name,
        transcript_source=req.transcript_source,
        fireflies_api_key=req.fireflies_api_key or "",
        crm=req.crm,
        crm_api_key=req.crm_api_key,
        framework=req.framework,
        auto_create_threshold=req.auto_create_threshold,
        slack_webhook_url=req.slack_webhook_url or "",
        zoom_webhook_secret=req.zoom_webhook_secret or "",
        gong_api_key=req.gong_api_key or "",
        gong_api_secret=req.gong_api_secret or "",
        teams_access_token=req.teams_access_token or "",
        google_access_token=req.google_access_token or "",
    )

    base_url = _get_base_url()
    source_path = SOURCE_WEBHOOK_PATHS.get(req.transcript_source, req.transcript_source)

    return ConnectionResponse(
        webhook_id=conn["webhook_id"],
        webhook_url=f"{base_url}/webhook/{source_path}/{conn['webhook_id']}",
        name=conn["name"],
        crm=conn["crm"],
        framework=conn["framework"],
        transcript_source=conn["transcript_source"],
        active=conn["active"],
    )


@app.get("/connections")
def list_all_connections():
    """List all registered connections (keys masked)."""
    return connections.list_connections()


@app.delete("/connections/{webhook_id}")
def delete_connection(webhook_id: str):
    """Remove a connection."""
    if connections.delete_connection(webhook_id):
        return {"deleted": True}
    raise HTTPException(status_code=404, detail="Connection not found")


# ── Fireflies webhook (automated pipeline) ──────────────────────────────────

def _process_fireflies_transcript(transcript_id: str, conn: dict):
    """
    Background task: pull transcript from Fireflies, analyze, score, create deal.
    This runs after the webhook returns 200 so Fireflies doesn't timeout.
    """
    try:
        ff_key = conn["fireflies_api_key"]
        crm_key = conn["crm_api_key"]
        crm_name = conn["crm"]
        framework = conn.get("framework", "custom")
        threshold = conn.get("auto_create_threshold", AUTO_CREATE_THRESHOLD)

        # 1. Pull transcript from Fireflies
        transcript = fireflies_client.get_transcript(transcript_id, api_key=ff_key)
        text = fireflies_client.format_transcript_text(transcript)
        metadata = fireflies_client.get_meeting_metadata(transcript)

        if not text or len(text) < 50:
            logger.warning(f"Transcript {transcript_id} too short ({len(text)} chars), skipping")
            return

        # 2. Analyze with Claude
        analysis = transcript_analyzer.analyze_transcript(text, metadata, framework=framework)

        if not analysis.get("is_sales_conversation"):
            logger.info(f"Transcript {transcript_id} is not a sales conversation, skipping deal creation")
            return

        # 3. Score
        score_result = deal_scorer.score_deal(analysis)
        score = score_result["total_score"]
        recommendation = score_result["recommendation"]

        logger.info(
            f"[{conn['name']}] Transcript '{metadata.get('title')}' scored {score}/100 "
            f"({recommendation})"
        )

        # 4. Create deal if score meets threshold
        deal_id = None
        if score >= REVIEW_THRESHOLD:
            crm_client = crm_factory.get_client(crm_name)
            result = crm_client.create_deal(
                score_result, analysis, metadata, dry_run=False, api_key=crm_key
            )
            if result:
                deal_id = result.get("deal_id")
                logger.info(
                    f"[{conn['name']}] Deal created: {result.get('deal_name')} "
                    f"(ID: {deal_id})"
                )
            else:
                logger.warning(f"[{conn['name']}] Deal creation returned None for transcript {transcript_id}")
        else:
            logger.info(f"[{conn['name']}] Score {score} below threshold {REVIEW_THRESHOLD}, no deal created")

        # 5. Slack notification (if configured)
        slack_url = conn.get("slack_webhook_url")
        if slack_url:
            _send_slack_notification(slack_url, score_result, analysis, metadata, deal_id=deal_id)

    except Exception as e:
        logger.error(f"[{conn.get('name', '?')}] Pipeline failed for transcript {transcript_id}: {e}")


def _send_slack_notification(
    webhook_url: str, score_result: dict, analysis: dict, metadata: dict,
    deal_id: Optional[str] = None,
):
    """Post a summary to Slack with feedback links."""
    import requests as req_lib
    score = score_result["total_score"]
    rec = score_result["recommendation"].replace("_", " ").title()
    deal_name = score_result.get("deal_name_suggestion", "Unknown")
    title = metadata.get("title", "Unknown Meeting")

    emoji = ":large_green_circle:" if score >= 70 else ":large_yellow_circle:" if score >= 50 else ":red_circle:"

    base_url = _get_base_url()
    feedback_id = deal_id or deal_name

    text = (
        f"{emoji} *DealSmart: {title}*\n"
        f"Score: *{score}/100* | Recommendation: *{rec}*\n"
        f"Deal: {deal_name}\n"
        f"Insight: _{score_result.get('key_insight', 'N/A')}_\n\n"
        f":thumbsup: <{base_url}/feedback/{feedback_id}?vote=good_deal|Good Deal>  "
        f":thumbsdown: <{base_url}/feedback/{feedback_id}?vote=not_a_deal|Not a Deal>  "
        f":arrows_counterclockwise: <{base_url}/feedback/{feedback_id}?vote=needs_review|Needs Review>"
    )
    try:
        req_lib.post(webhook_url, json={"text": text}, timeout=10)
    except Exception as e:
        logger.warning(f"Slack notification failed: {e}")


@app.post("/webhook/fireflies")
async def fireflies_webhook_default(request: Request, background_tasks: BackgroundTasks):
    """
    Default Fireflies webhook using server env var API keys.
    No connection setup needed. Configure in Fireflies:
    Settings > Integrations > Webhooks > Add webhook URL.
    """
    from config import FIREFLIES_API_KEY, ATTIO_API_KEY, SLACK_WEBHOOK_URL, DEFAULT_FRAMEWORK

    body = await request.json()
    logger.info(f"Fireflies default webhook received: {body}")

    transcript_id = (
        body.get("data", {}).get("transcriptId")
        or body.get("data", {}).get("transcript_id")
        or body.get("meetingId")
        or body.get("meeting_id")
        or body.get("transcriptId")
        or body.get("transcript_id")
    )

    if not transcript_id:
        logger.warning(f"Fireflies webhook: no transcript ID in payload: {body}")
        return {"status": "ignored", "reason": "no transcript_id in payload"}

    # Build a virtual connection from env vars
    conn = {
        "name": "Default",
        "fireflies_api_key": FIREFLIES_API_KEY,
        "crm": "attio",
        "crm_api_key": ATTIO_API_KEY,
        "framework": DEFAULT_FRAMEWORK,
        "auto_create_threshold": AUTO_CREATE_THRESHOLD,
        "slack_webhook_url": SLACK_WEBHOOK_URL,
    }

    logger.info(f"Processing Fireflies transcript {transcript_id} with default keys")
    background_tasks.add_task(_process_fireflies_transcript, transcript_id, conn)

    return {"status": "processing", "transcript_id": transcript_id}


@app.post("/webhook/fireflies/{webhook_id}")
async def fireflies_webhook(webhook_id: str, request: Request, background_tasks: BackgroundTasks):
    """
    Fireflies webhook for a specific connection (multi-tenant).
    Uses the connection's stored API keys.
    """
    conn = connections.get_connection(webhook_id)
    if not conn or not conn.get("active"):
        raise HTTPException(status_code=404, detail="Invalid or inactive webhook")

    body = await request.json()

    transcript_id = (
        body.get("data", {}).get("transcriptId")
        or body.get("data", {}).get("transcript_id")
        or body.get("meetingId")
        or body.get("meeting_id")
        or body.get("transcriptId")
        or body.get("transcript_id")
    )

    if not transcript_id:
        logger.warning(f"Fireflies webhook received but no transcript ID found in payload: {body}")
        return {"status": "ignored", "reason": "no transcript_id in payload"}

    logger.info(f"[{conn['name']}] Fireflies webhook received for transcript {transcript_id}")
    background_tasks.add_task(_process_fireflies_transcript, transcript_id, conn)

    return {"status": "processing", "transcript_id": transcript_id}


# ── File upload ───────────────────────────────────────────────────────────────

def _parse_vtt(content: str) -> str:
    """Parse WebVTT (.vtt) subtitle format into readable transcript."""
    lines = content.strip().split("\n")
    result = []
    skip_next = False
    for line in lines:
        line = line.strip()
        if line.startswith("WEBVTT") or line.startswith("NOTE") or not line:
            continue
        if "-->" in line:
            skip_next = False
            continue
        if line.isdigit():
            continue
        result.append(line)
    return "\n".join(result)


def _parse_srt(content: str) -> str:
    """Parse SRT subtitle format into readable transcript."""
    lines = content.strip().split("\n")
    result = []
    for line in lines:
        line = line.strip()
        if not line or line.isdigit() or "-->" in line:
            continue
        result.append(line)
    return "\n".join(result)


@app.post("/upload", response_model=AnalyzeResponse)
async def upload_transcript(
    file: UploadFile = File(...),
    framework: str = Form("custom"),
    meeting_title: Optional[str] = Form(None),
    meeting_date: Optional[str] = Form(None),
):
    """
    Upload a transcript file for analysis. Supports .txt, .vtt, .srt, .md formats.
    Returns the same analysis + score as /analyze.
    """
    if framework not in FRAMEWORK_NAMES:
        raise HTTPException(status_code=400, detail=f"Unknown framework: '{framework}'")

    filename = file.filename or "upload"
    ext = filename.rsplit(".", 1)[-1].lower() if "." in filename else "txt"

    if ext not in ("txt", "vtt", "srt", "md", "text"):
        raise HTTPException(
            status_code=400,
            detail=f"Unsupported file type: .{ext}. Supported: .txt, .vtt, .srt, .md",
        )

    raw = await file.read()
    try:
        content = raw.decode("utf-8")
    except UnicodeDecodeError:
        content = raw.decode("latin-1")

    if ext == "vtt":
        text = _parse_vtt(content)
    elif ext == "srt":
        text = _parse_srt(content)
    else:
        text = content

    if len(text.strip()) < 50:
        raise HTTPException(status_code=400, detail="Transcript too short (minimum 50 characters)")

    metadata = {
        "title": meeting_title or filename,
        "date": meeting_date or datetime.now().isoformat(),
        "source": f"upload:{ext}",
        "participants": [],
    }

    try:
        analysis = transcript_analyzer.analyze_transcript(text, metadata, framework=framework)
        score_result = deal_scorer.score_deal(analysis)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Analysis failed: {str(e)}")

    return AnalyzeResponse(
        analysis=analysis,
        score_result=score_result,
        score=score_result["total_score"],
        recommendation=score_result["recommendation"],
        deal_name=score_result.get("deal_name_suggestion", ""),
        framework=framework,
        key_insight=score_result.get("key_insight"),
    )


# ── Generic transcript processing (shared by all webhook sources) ─────────────

def _process_transcript_text(text: str, metadata: dict, conn: dict):
    """
    Background task: analyze transcript text, score, create deal.
    Used by all webhook sources after they extract the transcript text.
    """
    try:
        crm_key = conn["crm_api_key"]
        crm_name = conn["crm"]
        framework = conn.get("framework", "custom")

        if not text or len(text) < 50:
            logger.warning(f"[{conn['name']}] Transcript too short ({len(text)} chars), skipping")
            return

        analysis = transcript_analyzer.analyze_transcript(text, metadata, framework=framework)

        if not analysis.get("is_sales_conversation"):
            logger.info(f"[{conn['name']}] Not a sales conversation, skipping deal creation")
            return

        score_result = deal_scorer.score_deal(analysis)
        score = score_result["total_score"]
        recommendation = score_result["recommendation"]

        logger.info(
            f"[{conn['name']}] '{metadata.get('title')}' scored {score}/100 ({recommendation})"
        )

        deal_id = None
        if score >= REVIEW_THRESHOLD:
            crm_client = crm_factory.get_client(crm_name)
            result = crm_client.create_deal(
                score_result, analysis, metadata, dry_run=False, api_key=crm_key
            )
            if result:
                deal_id = result.get("deal_id")
                logger.info(f"[{conn['name']}] Deal created: {result.get('deal_name')}")
            else:
                logger.warning(f"[{conn['name']}] Deal creation returned None")
        else:
            logger.info(f"[{conn['name']}] Score {score} below threshold {REVIEW_THRESHOLD}, no deal created")

        slack_url = conn.get("slack_webhook_url")
        if slack_url:
            _send_slack_notification(slack_url, score_result, analysis, metadata, deal_id=deal_id)

    except Exception as e:
        logger.error(f"[{conn.get('name', '?')}] Pipeline failed: {e}")


# ── Zoom webhook ──────────────────────────────────────────────────────────────

def _process_zoom_recording(body: dict, conn: dict):
    """
    Background task: download Zoom cloud recording transcript, analyze, score, create deal.
    Zoom sends recording.transcript_completed or recording.completed events.
    """
    import requests as req_lib

    try:
        payload = body.get("payload", {}).get("object", {})
        topic = payload.get("topic", "Zoom Meeting")
        start_time = payload.get("start_time", "")

        # Find the transcript file in recording_files
        recording_files = payload.get("recording_files", [])
        transcript_url = None
        for rf in recording_files:
            if rf.get("file_type") == "TRANSCRIPT" or rf.get("recording_type") == "audio_transcript":
                transcript_url = rf.get("download_url")
                break

        if not transcript_url:
            logger.warning(f"[{conn['name']}] Zoom webhook: no transcript file found")
            return

        # Download transcript (Zoom provides a download token in the webhook)
        download_token = body.get("download_token", "")
        headers = {}
        if download_token:
            headers["Authorization"] = f"Bearer {download_token}"

        resp = req_lib.get(transcript_url, headers=headers, timeout=60)
        resp.raise_for_status()
        content = resp.text

        # Zoom transcripts are VTT format
        text = _parse_vtt(content) if "WEBVTT" in content[:50] else content

        metadata = {
            "title": topic,
            "date": start_time or datetime.now().isoformat(),
            "source": "zoom",
            "participants": [p.get("user_name", "") for p in payload.get("participant_audio_files", [])],
        }

        _process_transcript_text(text, metadata, conn)

    except Exception as e:
        logger.error(f"[{conn['name']}] Zoom processing failed: {e}")


@app.post("/webhook/zoom/{webhook_id}")
async def zoom_webhook(webhook_id: str, request: Request, background_tasks: BackgroundTasks):
    """
    Webhook for Zoom. Configure in Zoom Marketplace app:
    Event subscriptions > Add > recording.transcript_completed
    """
    conn = connections.get_connection(webhook_id)
    if not conn or not conn.get("active"):
        raise HTTPException(status_code=404, detail="Invalid or inactive webhook")

    body = await request.json()

    # Zoom sends a validation challenge on first setup
    if body.get("event") == "endpoint.url_validation":
        import hashlib, hmac
        plain_token = body.get("payload", {}).get("plainToken", "")
        zoom_secret = conn.get("zoom_webhook_secret", "")
        hash_value = hmac.HMAC(
            zoom_secret.encode(), plain_token.encode(), hashlib.sha256
        ).hexdigest()
        return {"plainToken": plain_token, "encryptedToken": hash_value}

    event = body.get("event", "")
    if event in ("recording.transcript_completed", "recording.completed"):
        logger.info(f"[{conn['name']}] Zoom webhook received: {event}")
        background_tasks.add_task(_process_zoom_recording, body, conn)
        return {"status": "processing"}

    return {"status": "ignored", "event": event}


# ── Gong webhook ──────────────────────────────────────────────────────────────

def _process_gong_call(body: dict, conn: dict):
    """
    Background task: pull Gong call transcript via API, analyze, score, create deal.
    """
    import requests as req_lib

    try:
        call_id = body.get("data", {}).get("callId") or body.get("callId", "")
        if not call_id:
            logger.warning(f"[{conn['name']}] Gong webhook: no callId found")
            return

        gong_key = conn.get("gong_api_key", "")
        gong_secret = conn.get("gong_api_secret", "")
        if not gong_key or not gong_secret:
            logger.warning(f"[{conn['name']}] Gong API credentials not configured")
            return

        # Pull transcript from Gong API
        resp = req_lib.post(
            "https://api.gong.io/v2/calls/transcript",
            auth=(gong_key, gong_secret),
            json={"filter": {"callIds": [call_id]}},
            timeout=30,
        )
        resp.raise_for_status()
        data = resp.json()

        transcripts = data.get("callTranscripts", [])
        if not transcripts:
            logger.warning(f"[{conn['name']}] No transcript returned for Gong call {call_id}")
            return

        # Build text from Gong transcript format
        lines = []
        for entry in transcripts[0].get("transcript", []):
            speaker = entry.get("speakerName", "Unknown")
            sentences = " ".join(s.get("text", "") for s in entry.get("sentences", []))
            if sentences:
                lines.append(f"**{speaker}:** {sentences}")

        text = "\n".join(lines)

        # Get call metadata
        meta_resp = req_lib.post(
            "https://api.gong.io/v2/calls/extensive",
            auth=(gong_key, gong_secret),
            json={"filter": {"callIds": [call_id]}, "contentSelector": {"exposedFields": {"content": {"structure": True}}}},
            timeout=30,
        )
        call_data = {}
        if meta_resp.ok:
            calls = meta_resp.json().get("calls", [])
            if calls:
                call_data = calls[0].get("metaData", {})

        metadata = {
            "title": call_data.get("title", f"Gong Call {call_id}"),
            "date": call_data.get("started", datetime.now().isoformat()),
            "source": "gong",
            "participants": [p.get("name", "") for p in call_data.get("parties", [])],
        }

        _process_transcript_text(text, metadata, conn)

    except Exception as e:
        logger.error(f"[{conn['name']}] Gong processing failed: {e}")


@app.post("/webhook/gong/{webhook_id}")
async def gong_webhook(webhook_id: str, request: Request, background_tasks: BackgroundTasks):
    """
    Webhook for Gong. Configure in Gong:
    Company Settings > Ecosystem > Webhooks > Add > call.transcript.ready
    """
    conn = connections.get_connection(webhook_id)
    if not conn or not conn.get("active"):
        raise HTTPException(status_code=404, detail="Invalid or inactive webhook")

    body = await request.json()
    logger.info(f"[{conn['name']}] Gong webhook received")
    background_tasks.add_task(_process_gong_call, body, conn)
    return {"status": "processing"}


# ── Microsoft Teams webhook ───────────────────────────────────────────────────

def _process_teams_transcript(body: dict, conn: dict):
    """
    Background task: pull Teams meeting transcript via Graph API.
    """
    import requests as req_lib

    try:
        resource = body.get("value", [{}])[0].get("resource", "")
        # resource format: communications/callRecords/{id}
        call_id = resource.split("/")[-1] if resource else ""

        if not call_id:
            logger.warning(f"[{conn['name']}] Teams webhook: no call ID found")
            return

        teams_token = conn.get("teams_access_token", "")
        if not teams_token:
            logger.warning(f"[{conn['name']}] Teams access token not configured")
            return

        headers = {"Authorization": f"Bearer {teams_token}"}

        # Get transcript content
        # First, list transcripts for the meeting
        resp = req_lib.get(
            f"https://graph.microsoft.com/v1.0/communications/callRecords/{call_id}/transcripts",
            headers=headers,
            timeout=30,
        )

        if not resp.ok:
            logger.warning(f"[{conn['name']}] Teams transcript fetch failed: {resp.status_code}")
            return

        transcripts = resp.json().get("value", [])
        if not transcripts:
            return

        # Download first transcript content
        transcript_id = transcripts[0].get("id")
        content_resp = req_lib.get(
            f"https://graph.microsoft.com/v1.0/communications/callRecords/{call_id}/transcripts/{transcript_id}/content",
            headers={**headers, "Accept": "text/vtt"},
            timeout=30,
        )

        if not content_resp.ok:
            return

        text = _parse_vtt(content_resp.text) if "WEBVTT" in content_resp.text[:50] else content_resp.text

        metadata = {
            "title": body.get("value", [{}])[0].get("resourceData", {}).get("subject", "Teams Meeting"),
            "date": datetime.now().isoformat(),
            "source": "teams",
            "participants": [],
        }

        _process_transcript_text(text, metadata, conn)

    except Exception as e:
        logger.error(f"[{conn['name']}] Teams processing failed: {e}")


@app.post("/webhook/teams/{webhook_id}")
async def teams_webhook(webhook_id: str, request: Request, background_tasks: BackgroundTasks):
    """
    Webhook for Microsoft Teams. Configure via Graph API subscriptions.
    Subscribe to: communications/callRecords
    """
    conn = connections.get_connection(webhook_id)

    # Teams sends a validation request with validationToken query param
    validation_token = request.query_params.get("validationToken")
    if validation_token:
        from fastapi.responses import PlainTextResponse
        return PlainTextResponse(content=validation_token)

    if not conn or not conn.get("active"):
        raise HTTPException(status_code=404, detail="Invalid or inactive webhook")

    body = await request.json()
    logger.info(f"[{conn['name']}] Teams webhook received")
    background_tasks.add_task(_process_teams_transcript, body, conn)
    return {"status": "processing"}


# ── Google Meet (via Google Workspace Events) ─────────────────────────────────

def _process_google_meet_transcript(body: dict, conn: dict):
    """
    Background task: pull Google Meet transcript via Drive API.
    Google Meet saves transcripts as Google Docs in the organizer's Drive.
    """
    import requests as req_lib

    try:
        event_data = body.get("protoPayload", {}) or body.get("data", {}) or body
        # The transcript doc ID comes from the event
        doc_id = (
            event_data.get("documentId")
            or event_data.get("transcript_doc_id")
            or ""
        )

        google_token = conn.get("google_access_token", "")
        if not google_token:
            logger.warning(f"[{conn['name']}] Google access token not configured")
            return

        if doc_id:
            # Fetch doc content from Drive
            headers = {"Authorization": f"Bearer {google_token}"}
            resp = req_lib.get(
                f"https://docs.googleapis.com/v1/documents/{doc_id}",
                headers=headers,
                timeout=30,
            )
            if resp.ok:
                doc = resp.json()
                # Extract text from Google Doc structure
                lines = []
                for element in doc.get("body", {}).get("content", []):
                    para = element.get("paragraph", {})
                    for el in para.get("elements", []):
                        text_run = el.get("textRun", {})
                        if text_run.get("content", "").strip():
                            lines.append(text_run["content"].strip())
                text = "\n".join(lines)
            else:
                logger.warning(f"[{conn['name']}] Google Doc fetch failed: {resp.status_code}")
                return
        else:
            # Fallback: transcript text might be in the webhook payload directly
            text = event_data.get("transcript_text", "")

        if not text:
            logger.warning(f"[{conn['name']}] No transcript text from Google Meet")
            return

        metadata = {
            "title": event_data.get("meeting_title", event_data.get("summary", "Google Meet")),
            "date": event_data.get("start_time", datetime.now().isoformat()),
            "source": "google_meet",
            "participants": event_data.get("attendees", []),
        }

        _process_transcript_text(text, metadata, conn)

    except Exception as e:
        logger.error(f"[{conn['name']}] Google Meet processing failed: {e}")


@app.post("/webhook/google-meet/{webhook_id}")
async def google_meet_webhook(webhook_id: str, request: Request, background_tasks: BackgroundTasks):
    """
    Webhook for Google Meet transcripts.
    Configure via Google Workspace Events API or Pub/Sub push subscription.
    """
    conn = connections.get_connection(webhook_id)
    if not conn or not conn.get("active"):
        raise HTTPException(status_code=404, detail="Invalid or inactive webhook")

    body = await request.json()
    logger.info(f"[{conn['name']}] Google Meet webhook received")
    background_tasks.add_task(_process_google_meet_transcript, body, conn)
    return {"status": "processing"}


# ── Feedback ─────────────────────────────────────────────────────────────────

FEEDBACK_FILE = Path(__file__).parent / ".feedback.json"


def _load_feedback() -> list:
    if database.is_available():
        conn = database.get_conn()
        try:
            cur = conn.cursor()
            cur.execute("SELECT deal_id, vote, note, timestamp FROM feedback ORDER BY timestamp DESC")
            rows = cur.fetchall()
            cur.close()
            return [
                {"deal_id": r[0], "vote": r[1], "note": r[2], "timestamp": r[3].isoformat()}
                for r in rows
            ]
        except Exception as e:
            logger.warning(f"Failed to load feedback from DB: {e}")
            return []
        finally:
            database.put_conn(conn)
    # Fallback to JSON
    if FEEDBACK_FILE.exists():
        return json.loads(FEEDBACK_FILE.read_text())
    return []


def _save_feedback(entry: dict):
    """Save a single feedback entry."""
    if database.is_available():
        conn = database.get_conn()
        try:
            cur = conn.cursor()
            cur.execute(
                "INSERT INTO feedback (deal_id, vote, note, timestamp) VALUES (%s, %s, %s, %s)",
                (entry["deal_id"], entry["vote"], entry["note"], entry["timestamp"]),
            )
            conn.commit()
            cur.close()
            return
        except Exception as e:
            conn.rollback()
            logger.warning(f"Failed to save feedback to DB: {e}")
        finally:
            database.put_conn(conn)
    # Fallback to JSON
    data = []
    if FEEDBACK_FILE.exists():
        data = json.loads(FEEDBACK_FILE.read_text())
    data.append(entry)
    FEEDBACK_FILE.write_text(json.dumps(data, indent=2))


@app.get("/feedback/{deal_id}")
def submit_feedback(deal_id: str, vote: str = "not_a_deal", note: str = ""):
    """
    Record feedback on a deal assessment and update the deal in Attio.
    Called from Slack notification links.

    Actions:
      - good_deal: Confirms the deal. No stage change.
      - not_a_deal: Moves deal to "Lost" in Attio.
      - needs_review: Moves deal to "Discovery Scheduled" in Attio.
    """
    valid_votes = {"good_deal", "not_a_deal", "needs_review"}
    if vote not in valid_votes:
        vote = "not_a_deal"

    entry = {
        "deal_id": deal_id,
        "vote": vote,
        "note": note,
        "timestamp": datetime.now().isoformat(),
    }
    _save_feedback(entry)

    logger.info(f"Feedback received: {deal_id} = {vote}")

    # Update deal stage in Attio based on feedback
    import attio_client
    action_taken = "Feedback logged."
    if vote == "not_a_deal":
        result = attio_client.update_deal_stage(deal_id, "Lost")
        action_taken = "Deal moved to Lost." if result else "Could not update deal stage."
    elif vote == "needs_review":
        from config import ATTIO_DEAL_STAGE_REVIEW
        result = attio_client.update_deal_stage(deal_id, ATTIO_DEAL_STAGE_REVIEW)
        action_taken = f"Deal moved to {ATTIO_DEAL_STAGE_REVIEW}." if result else "Could not update deal stage."
    elif vote == "good_deal":
        action_taken = "Deal confirmed. No changes made."

    emoji_map = {"good_deal": "Confirmed", "not_a_deal": "Moved to Lost", "needs_review": "Moved to Review"}
    from fastapi.responses import HTMLResponse
    return HTMLResponse(
        f"<html><body style='font-family:sans-serif;text-align:center;padding:60px;'>"
        f"<h1>Feedback Recorded</h1>"
        f"<p>Deal: <b>{deal_id}</b></p>"
        f"<p>Your vote: <b>{emoji_map.get(vote, vote)}</b></p>"
        f"<p>{action_taken}</p>"
        f"<p>Thanks! This helps DealSmart get smarter over time.</p>"
        f"</body></html>"
    )


@app.get("/feedback")
def list_feedback():
    """List all feedback entries. Useful for reviewing accuracy."""
    return _load_feedback()


# ── Health check ─────────────────────────────────────────────────────────────

@app.get("/health")
def health():
    """Health check."""
    return {"status": "ok", "version": "1.0.0"}
