"""
Auto Deal Generator - FastAPI wrapper
Exposes the analysis and deal creation pipeline as HTTP endpoints
for external frontends (Lovable, custom React apps, etc.)

Run: uvicorn api:app --reload --port 8000
"""

import logging
import os
import sys
from datetime import datetime
from typing import Optional

from fastapi import FastAPI, HTTPException, BackgroundTasks, Request
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field

# Add this directory to path so local modules resolve
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import transcript_analyzer
import deal_scorer
import crm as crm_factory
import fireflies_client
import connections
from frameworks import FRAMEWORKS, FRAMEWORK_NAMES, get_framework
from config import AUTO_CREATE_THRESHOLD, REVIEW_THRESHOLD

logger = logging.getLogger(__name__)

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
    This is the main endpoint. Calls Claude for analysis, then scores locally.
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

class ConnectionRequest(BaseModel):
    name: str = Field(..., description="Team or user name")
    fireflies_api_key: str = Field(..., description="Fireflies.ai API key")
    crm: str = Field("attio", description="CRM: attio or hubspot")
    crm_api_key: str = Field(..., description="CRM API key")
    framework: str = Field("custom", description="Scoring framework")
    auto_create_threshold: int = Field(70, description="Score threshold for auto-creating deals")
    slack_webhook_url: Optional[str] = Field("", description="Slack webhook for notifications")


class ConnectionResponse(BaseModel):
    webhook_id: str
    webhook_url: str
    name: str
    crm: str
    framework: str
    active: bool


@app.post("/connections", response_model=ConnectionResponse)
def create_connection(req: ConnectionRequest):
    """
    Register a new connection. Returns a webhook_url to configure in Fireflies.
    Fireflies Settings > Integrations > Webhooks > paste this URL.
    """
    if req.crm not in ("hubspot", "attio"):
        raise HTTPException(status_code=400, detail=f"Unsupported CRM: '{req.crm}'")
    if req.framework not in FRAMEWORK_NAMES:
        raise HTTPException(status_code=400, detail=f"Unknown framework: '{req.framework}'")

    conn = connections.create_connection(
        name=req.name,
        fireflies_api_key=req.fireflies_api_key,
        crm=req.crm,
        crm_api_key=req.crm_api_key,
        framework=req.framework,
        auto_create_threshold=req.auto_create_threshold,
        slack_webhook_url=req.slack_webhook_url or "",
    )

    base_url = os.getenv("RAILWAY_PUBLIC_DOMAIN", "")
    if base_url:
        base_url = f"https://{base_url}"
    else:
        base_url = os.getenv("BASE_URL", "http://localhost:8000")

    return ConnectionResponse(
        webhook_id=conn["webhook_id"],
        webhook_url=f"{base_url}/webhook/fireflies/{conn['webhook_id']}",
        name=conn["name"],
        crm=conn["crm"],
        framework=conn["framework"],
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
        if score >= REVIEW_THRESHOLD:
            crm_client = crm_factory.get_client(crm_name)
            result = crm_client.create_deal(
                score_result, analysis, metadata, dry_run=False, api_key=crm_key
            )
            if result:
                logger.info(
                    f"[{conn['name']}] Deal created: {result.get('deal_name')} "
                    f"(ID: {result.get('deal_id')})"
                )
            else:
                logger.warning(f"[{conn['name']}] Deal creation returned None for transcript {transcript_id}")
        else:
            logger.info(f"[{conn['name']}] Score {score} below threshold {REVIEW_THRESHOLD}, no deal created")

        # 5. Slack notification (if configured)
        slack_url = conn.get("slack_webhook_url")
        if slack_url:
            _send_slack_notification(slack_url, score_result, analysis, metadata)

    except Exception as e:
        logger.error(f"[{conn.get('name', '?')}] Pipeline failed for transcript {transcript_id}: {e}")


def _send_slack_notification(webhook_url: str, score_result: dict, analysis: dict, metadata: dict):
    """Post a summary to Slack."""
    import requests as req_lib
    score = score_result["total_score"]
    rec = score_result["recommendation"].replace("_", " ").title()
    deal_name = score_result.get("deal_name_suggestion", "Unknown")
    title = metadata.get("title", "Unknown Meeting")

    emoji = ":large_green_circle:" if score >= 70 else ":large_yellow_circle:" if score >= 50 else ":red_circle:"

    text = (
        f"{emoji} *Deal Intelligence: {title}*\n"
        f"Score: *{score}/100* | Recommendation: *{rec}*\n"
        f"Deal: {deal_name}\n"
        f"Insight: _{score_result.get('key_insight', 'N/A')}_"
    )
    try:
        req_lib.post(webhook_url, json={"text": text}, timeout=10)
    except Exception as e:
        logger.warning(f"Slack notification failed: {e}")


@app.post("/webhook/fireflies/{webhook_id}")
async def fireflies_webhook(webhook_id: str, request: Request, background_tasks: BackgroundTasks):
    """
    Webhook endpoint for Fireflies. Configure in Fireflies:
    Settings > Integrations > Webhooks > Add webhook URL.

    Fireflies sends a POST with the meeting/transcript ID when a call ends.
    We return 200 immediately and process in the background.
    """
    conn = connections.get_connection(webhook_id)
    if not conn or not conn.get("active"):
        raise HTTPException(status_code=404, detail="Invalid or inactive webhook")

    body = await request.json()

    # Fireflies webhook payload contains meetingId or transcriptId
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


# ── Health check ─────────────────────────────────────────────────────────────

@app.get("/health")
def health():
    """Health check."""
    return {"status": "ok", "version": "1.0.0"}
