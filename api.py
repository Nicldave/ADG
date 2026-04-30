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

from fastapi import FastAPI, HTTPException, BackgroundTasks, Request, UploadFile, File, Form, Depends, Security, Response
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import RedirectResponse
from fastapi.security import APIKeyHeader
from fastapi.staticfiles import StaticFiles
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

# ── API Key Authentication ───────────────────────────────────────────────────
# Set DEALSMART_API_KEY env var on Railway. Share this key with clients.
# Webhooks are excluded (they use unique IDs for security).

DEALSMART_API_KEY = os.getenv("DEALSMART_API_KEY", "")

api_key_header = APIKeyHeader(name="X-API-Key", auto_error=False)


async def require_api_key(api_key: str = Security(api_key_header)):
    """Dependency that enforces API key auth on protected endpoints."""
    if not DEALSMART_API_KEY:
        return  # No key configured = auth disabled (dev mode)
    if api_key != DEALSMART_API_KEY:
        raise HTTPException(status_code=401, detail="Invalid or missing API key")


# Initialize PostgreSQL tables on startup (no-op if DATABASE_URL not set)
try:
    database.init_db()
except Exception as e:
    logger.warning(f"Database init skipped: {e}")

app = FastAPI(
    title="Fairplay API",
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
    demo_mode: bool = Field(False, description="If true, score only. No deal creation, no Slack notification.")
    demo_email: Optional[str] = Field(None, description="Email to send score results to (demo mode only)")
    company_icp: Optional[str] = Field(None, description="JSON ICP context for scoring (demo mode). Overrides connection ICP.")
    custom_weights: Optional[dict] = Field(None, description="Custom framework weights, e.g. {\"budget\":30,\"authority\":30,\"need\":25,\"timeline\":15}")


class CreateDealRequest(BaseModel):
    analysis: dict = Field(..., description="Output from /analyze")
    score_result: dict = Field(..., description="Output from /analyze")
    crm: str = Field("attio", description="CRM target: hubspot or attio")
    dry_run: bool = Field(False, description="If true, simulates without creating")
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

@app.get("/frameworks", response_model=list[FrameworkInfo], dependencies=[Depends(require_api_key)])
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


# Rate limiter for /analyze (prevents abuse of demo page)
_rate_limit_store = {}  # {ip: [timestamp, timestamp, ...]}
RATE_LIMIT_MAX = 5  # max requests per window
RATE_LIMIT_WINDOW = 86400  # 24 hours in seconds


def _check_rate_limit(request: Request):
    """Check if the request IP has exceeded the rate limit. Skips for authenticated users."""
    # Skip rate limit for authenticated users (cookie or token)
    user = _get_user_from_session(request)
    if user:
        return
    # Skip if API key auth is valid
    api_key = request.headers.get("x-api-key", "")
    dealsmart_key = os.getenv("DEALSMART_API_KEY", "")
    if dealsmart_key and api_key == dealsmart_key:
        return

    ip = request.client.host if request.client else "unknown"
    now = datetime.now().timestamp()
    # Clean old entries
    if ip in _rate_limit_store:
        _rate_limit_store[ip] = [t for t in _rate_limit_store[ip] if now - t < RATE_LIMIT_WINDOW]
    else:
        _rate_limit_store[ip] = []

    if len(_rate_limit_store[ip]) >= RATE_LIMIT_MAX:
        raise HTTPException(status_code=429, detail="Rate limit exceeded. Maximum 5 scores per day. Sign up for unlimited access.")

    _rate_limit_store[ip].append(now)


@app.post("/analyze", response_model=AnalyzeResponse, dependencies=[Depends(require_api_key)])
def analyze(req: AnalyzeRequest, request: Request):
    """
    Analyze a sales transcript. Returns structured analysis + Strike Zone score.
    Auto-creates deal in Attio (if score >= 50) and sends Slack notification
    using server default API keys.
    """
    _check_rate_limit(request)

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

    # In demo mode, pass ICP context and custom weights if provided
    icp_context = None
    if req.demo_mode and req.company_icp:
        icp_context = req.company_icp

    try:
        analysis = transcript_analyzer.analyze_transcript(
            req.transcript, metadata, framework=req.framework,
            company_icp=icp_context,
        )
        score_result = deal_scorer.score_deal(analysis, custom_weights=req.custom_weights)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Analysis failed: {str(e)}")

    # In demo mode, skip deal creation, logging, and Slack notification
    deal_id = None
    if not req.demo_mode:
        # Check for existing deal and previous scores
        company_name = analysis.get("prospect_company", {}).get("name", "")
        existing_deal = _find_existing_deal(company_name, "attio") if company_name else None
        previous_scores = _get_previous_scores(company_name) if company_name else []

        # Auto-create deal if it's a sales conversation with sufficient score (and no existing deal)
        deal_result = None
        if existing_deal:
            deal_id = existing_deal.get("deal_id")
            logger.info(f"Existing deal found for '{company_name}', skipping creation")
        elif analysis.get("is_sales_conversation") and score_result["total_score"] >= REVIEW_THRESHOLD:
            try:
                crm_client = crm_factory.get_client("attio")
                deal_result = crm_client.create_deal(score_result, analysis, metadata, dry_run=False)
                if deal_result:
                    deal_id = deal_result.get("deal_id")
                    logger.info(f"Auto-created Attio deal: {deal_result.get('deal_name')} (score: {score_result['total_score']})")
            except Exception as e:
                logger.warning(f"Auto deal creation failed: {e}")

        # Log scored deal
        _save_scored_deal(score_result, analysis, metadata, deal_id=deal_id)

        # Slack notification
        from config import SLACK_WEBHOOK_URL
        if SLACK_WEBHOOK_URL:
            try:
                _send_slack_notification(
                    SLACK_WEBHOOK_URL, score_result, analysis, metadata,
                    deal_id=deal_id, existing_deal=existing_deal, previous_scores=previous_scores,
                )
            except Exception as e:
                logger.warning(f"Slack notification failed: {e}")

    # Send score results via email if demo_email provided
    if req.demo_mode and req.demo_email:
        try:
            _send_score_email(req.demo_email, score_result, analysis, req.framework)
        except Exception as e:
            logger.warning(f"Failed to send score email to {req.demo_email}: {e}")

    return AnalyzeResponse(
        analysis=analysis,
        score_result=score_result,
        score=score_result["total_score"],
        recommendation=score_result["recommendation"],
        deal_name=score_result.get("deal_name_suggestion", ""),
        framework=req.framework,
        key_insight=score_result.get("key_insight"),
    )


def _send_score_email(email: str, score_result: dict, analysis: dict, framework: str):
    """Send score results to the demo user via Resend."""
    from config import RESEND_API_KEY
    import requests as req_lib

    if not RESEND_API_KEY:
        logger.warning("RESEND_API_KEY not set, skipping score email")
        return

    score = score_result["total_score"]
    rec = score_result["recommendation"].replace("_", " ").title()
    deal_name = score_result.get("deal_name_suggestion", "")
    company = analysis.get("prospect_company", {}).get("name", "Unknown")
    fw_name = framework.upper()
    insight = score_result.get("key_insight", "")

    # Build breakdown text
    breakdown_lines = []
    fw_scores = analysis.get("framework_scores", {})
    for cat, data in score_result.get("breakdown", {}).items():
        label = data.get("label", cat)
        assessment = ""
        if isinstance(fw_scores.get(cat), dict):
            assessment = fw_scores[cat].get("assessment", "")
        line = f"<strong>{label}: {data['score']}/{data['max']}</strong>"
        if assessment:
            line += f" - {assessment}"
        breakdown_lines.append(f"<li>{line}</li>")

    color = "#22c55e" if score >= 70 else "#f59e0b" if score >= 50 else "#ef4444"
    rec_text = "This conversation qualifies as a deal." if score >= 70 else "This conversation needs further review." if score >= 50 else "This conversation does not qualify as a deal."

    html_body = f"""
    <div style="font-family: Inter, -apple-system, sans-serif; max-width: 600px; margin: 0 auto;">
        <div style="background: #0a0a0a; padding: 20px 24px; border-radius: 8px 8px 0 0;">
            <span style="color: white; font-weight: 700; font-size: 18px;">Fairplay</span>
        </div>
        <div style="border: 1px solid #eee; border-top: none; padding: 24px; border-radius: 0 0 8px 8px;">
            <h2 style="margin: 0 0 4px;">Your Score Results</h2>
            <p style="color: #888; margin: 0 0 20px;">Framework: {fw_name} | Company: {company}</p>

            <div style="text-align: center; padding: 20px; background: #f8f9fa; border-radius: 8px; margin-bottom: 20px;">
                <span style="font-size: 48px; font-weight: 800; color: {color};">{score}</span>
                <span style="font-size: 20px; color: #ccc;">/100</span>
                <div style="margin-top: 8px;">
                    <span style="display: inline-block; padding: 4px 12px; border-radius: 100px; font-size: 13px; font-weight: 600; background: {color}20; color: {color};">{rec}</span>
                </div>
                <p style="color: #666; margin-top: 12px; font-size: 14px;">{rec_text}</p>
            </div>

            <h3 style="margin: 0 0 12px; font-size: 14px; text-transform: uppercase; color: #888;">Breakdown</h3>
            <ul style="padding-left: 0; list-style: none; margin: 0 0 20px;">
                {''.join(breakdown_lines)}
            </ul>

            {"<p style='font-style: italic; color: #555;'><strong>Key Insight:</strong> " + insight + "</p>" if insight else ""}

            <hr style="border: none; border-top: 1px solid #eee; margin: 20px 0;">
            <p style="color: #888; font-size: 13px;">
                Want Fairplay scoring your team's calls automatically?
                <a href="https://fairplay-nicl.netlify.app" style="color: #0a0a0a; font-weight: 600;">Get started</a>
            </p>
        </div>
    </div>"""

    try:
        resp = req_lib.post(
            "https://api.resend.com/emails",
            headers={"Authorization": f"Bearer {RESEND_API_KEY}"},
            json={
                "from": "Fairplay <fairplay@nicl.ai>",
                "to": [email],
                "subject": f"Your Fairplay Score: {score}/100 ({fw_name})",
                "html": html_body,
                "headers": {
                    "X-Entity-Ref-ID": f"fairplay-demo-{email}",
                },
                "tags": [
                    {"name": "category", "value": "demo_score"},
                ],
            },
            timeout=10,
        )
        if resp.status_code in (200, 201):
            logger.info(f"Score email sent to {email}")
        else:
            logger.warning(f"Score email failed: {resp.status_code} {resp.text[:200]}")
    except Exception as e:
        logger.warning(f"Score email request failed: {e}")


@app.post("/create-deal", response_model=CreateDealResponse, dependencies=[Depends(require_api_key)])
def create_deal(req: CreateDealRequest):
    """
    Create a deal in the selected CRM from a previously analyzed transcript.
    Pass the analysis and score_result from /analyze.
    """
    if req.crm not in ("hubspot", "attio", "salesforce", "pipedrive", "close", "copper", "zoho", "freshsales", "monday", "keap", "webhook"):
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

SUPPORTED_SOURCES = {"fireflies", "zoom", "gong", "teams", "google_meet", "fathom"}


class ConnectionRequest(BaseModel):
    name: str = Field(..., description="Team or user name")
    transcript_source: str = Field("fireflies", description="Transcript source: fireflies, zoom, gong, teams, google_meet")
    fireflies_api_key: Optional[str] = Field("", description="Fireflies.ai API key")
    crm: str = Field("attio", description="CRM: attio or hubspot")
    crm_api_key: str = Field(..., description="CRM API key")
    framework: str = Field("custom", description="Scoring framework")
    auto_create_threshold: int = Field(70, description="Score threshold for auto-creating deals")
    slack_webhook_url: Optional[str] = Field("", description="Slack webhook for notifications")
    teams_webhook_url: Optional[str] = Field("", description="Microsoft Teams webhook for notifications")
    # Source-specific keys
    zoom_webhook_secret: Optional[str] = Field("", description="Zoom webhook secret token")
    gong_api_key: Optional[str] = Field("", description="Gong API key (access key)")
    gong_api_secret: Optional[str] = Field("", description="Gong API secret (access key secret)")
    teams_access_token: Optional[str] = Field("", description="Microsoft Graph API access token")
    google_access_token: Optional[str] = Field("", description="Google OAuth access token")
    fathom_api_key: Optional[str] = Field("", description="Fathom API key")
    zoom_account_id: Optional[str] = Field("", description="Zoom Server-to-Server OAuth Account ID")
    zoom_client_id: Optional[str] = Field("", description="Zoom Server-to-Server OAuth Client ID")
    zoom_client_secret: Optional[str] = Field("", description="Zoom Server-to-Server OAuth Client Secret")
    zoom_user_email: Optional[str] = Field("", description="Zoom user email for recording access")
    shadow_mode: bool = Field(False, description="Shadow mode: score calls without writing to CRM")
    # Business context for scoring calibration
    sale_type: Optional[str] = Field("", description="Type of sale: saas, services, both, hardware, other")
    deal_value_range: Optional[str] = Field("", description="Typical deal value: 0-1k, 1k-5k, 5k-25k, 25k+")
    avg_days_to_close: Optional[str] = Field("", description="Average days to close: 7, 14, 30, 60, 90+")
    industry_vertical: Optional[str] = Field("", description="Industry or vertical (optional)")
    framework_weights: Optional[str] = Field("", description="Custom framework weights as JSON, e.g. {\"budget\":30,\"authority\":30,\"need\":25,\"timeline\":15}")


class ConnectionResponse(BaseModel):
    webhook_id: str
    webhook_url: str
    name: str
    crm: str
    framework: str
    transcript_source: str
    active: bool
    shadow_mode: bool = False


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
def create_connection(req: ConnectionRequest, request: Request):
    """
    Register a new connection. Returns a webhook_url to configure in your transcript source.
    Supports: Fireflies, Zoom, Gong, Microsoft Teams, Google Meet.
    Accepts either API key auth or session cookie auth.
    """
    # Allow either API key or session auth
    user = _get_user_from_session(request)
    api_key = request.headers.get("x-api-key", "")
    dealsmart_key = os.getenv("DEALSMART_API_KEY", "")
    if not user and not (dealsmart_key and api_key == dealsmart_key) and dealsmart_key:
        raise HTTPException(status_code=401, detail="Not authenticated")

    if req.crm not in ("hubspot", "attio", "salesforce", "pipedrive", "close", "copper", "zoho", "freshsales", "monday", "keap", "webhook"):
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
        shadow_mode=req.shadow_mode,
    )
    # Store business context and Zoom OAuth fields if provided
    extra_updates = {}
    if req.sale_type:
        extra_updates["sale_type"] = req.sale_type
    if req.deal_value_range:
        extra_updates["deal_value_range"] = req.deal_value_range
    if req.avg_days_to_close:
        extra_updates["avg_days_to_close"] = req.avg_days_to_close
    if req.industry_vertical:
        extra_updates["industry_vertical"] = req.industry_vertical
    if req.framework_weights:
        extra_updates["framework_weights"] = req.framework_weights
    if req.teams_webhook_url:
        extra_updates["teams_webhook_url"] = req.teams_webhook_url
    if hasattr(req, 'fathom_api_key') and req.fathom_api_key:
        extra_updates["fathom_api_key"] = req.fathom_api_key
    if extra_updates and conn.get("webhook_id"):
        connections.update_connection(conn["webhook_id"], extra_updates)

    # Store Zoom OAuth fields if provided (not in create_connection params, update after)
    if req.zoom_account_id and conn.get("webhook_id"):
        zoom_updates = {
            "zoom_account_id": req.zoom_account_id or "",
            "zoom_client_id": req.zoom_client_id or "",
            "zoom_client_secret": req.zoom_client_secret or "",
            "zoom_user_email": req.zoom_user_email or "",
        }
        connections.update_connection(conn["webhook_id"], zoom_updates)

    # Link connection to user if logged in via session
    if user and database.is_available():
        db = database.get_conn()
        if db:
            try:
                cur = db.cursor()
                cur.execute(
                    "UPDATE connections SET user_id = %s WHERE webhook_id = %s",
                    (user["id"], conn["webhook_id"]),
                )
                db.commit()
                cur.close()
            except Exception:
                db.rollback()
            finally:
                database.put_conn(db)

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
        shadow_mode=conn.get("shadow_mode", False),
    )


@app.put("/connections/{webhook_id}")
def update_connection_endpoint(webhook_id: str, updates: dict, request: Request):
    """Update a connection. Use to toggle shadow_mode, change framework, etc."""
    user = _get_user_from_session(request)
    api_key = request.headers.get("x-api-key", "")
    dealsmart_key = os.getenv("DEALSMART_API_KEY", "")
    if not user and not (dealsmart_key and api_key == dealsmart_key) and dealsmart_key:
        raise HTTPException(status_code=401, detail="Not authenticated")

    result = connections.update_connection(webhook_id, updates)
    if not result:
        raise HTTPException(status_code=404, detail="Connection not found")
    return {"status": "updated", "webhook_id": webhook_id}


@app.get("/connections", dependencies=[Depends(require_api_key)])
def list_all_connections():
    """List all registered connections (keys masked)."""
    return connections.list_connections()


@app.delete("/connections/{webhook_id}", dependencies=[Depends(require_api_key)])
def delete_connection(webhook_id: str):
    """Remove a connection."""
    if connections.delete_connection(webhook_id):
        return {"deleted": True}
    raise HTTPException(status_code=404, detail="Connection not found")


class GenerateICPRequest(BaseModel):
    website_url: str = Field(..., description="Company website URL to scrape")


@app.post("/connections/{webhook_id}/generate-icp")
def generate_icp_endpoint(webhook_id: str, req: GenerateICPRequest):
    """Scrape website and generate ICP summary for scoring context."""
    import icp_generator

    conn = connections.get_connection(webhook_id)
    if not conn:
        raise HTTPException(status_code=404, detail="Connection not found")

    # Scrape website
    website_text = icp_generator.scrape_website(req.website_url)
    if not website_text or len(website_text) < 50:
        raise HTTPException(status_code=400, detail="Could not scrape enough content from the website")

    # Get business context from connection
    biz_ctx = {
        "sale_type": conn.get("sale_type", ""),
        "deal_value_range": conn.get("deal_value_range", ""),
        "industry_vertical": conn.get("industry_vertical", ""),
    }

    # Generate ICP
    icp = icp_generator.generate_icp(website_text, biz_ctx)
    if icp.get("error"):
        raise HTTPException(status_code=500, detail=icp["error"])

    # Store on connection
    import json as _json
    connections.update_connection(webhook_id, {
        "company_website": req.website_url,
        "company_icp": _json.dumps(icp),
    })

    return {"icp": icp, "website_url": req.website_url}


@app.get("/demo/frameworks")
def demo_list_frameworks():
    """List frameworks with categories/weights for demo page. No auth required."""
    result = []
    for key, fw in FRAMEWORKS.items():
        if key == "custom":
            continue  # Skip custom for demo
        result.append({
            "key": key,
            "name": fw["name"],
            "categories": {
                k: {"weight": v["weight"], "label": v["label"]}
                for k, v in fw["categories"].items()
            },
        })
    return result


@app.post("/demo/generate-icp")
def demo_generate_icp(req: GenerateICPRequest, request: Request):
    """Generate ICP from website URL for demo page. No connection required."""
    _check_rate_limit(request)
    import icp_generator

    website_text = icp_generator.scrape_website(req.website_url)
    if not website_text or len(website_text) < 50:
        raise HTTPException(status_code=400, detail="Could not scrape enough content from the website")

    icp = icp_generator.generate_icp(website_text)
    if icp.get("error"):
        raise HTTPException(status_code=500, detail=icp["error"])

    return {"icp": icp, "website_url": req.website_url}


# ── Helpers: dedup, error alerting, default connection ────────────────────────

def _is_processed(transcript_id: str, connection_name: str = "Default") -> bool:
    """Check if a transcript has been successfully processed. Allows retries through."""
    if database.is_available():
        conn = database.get_conn()
        if conn:
            try:
                cur = conn.cursor()
                # Check ALL rows for this transcript_id (may exist under multiple connection_names)
                cur.execute(
                    "SELECT status FROM processed_transcripts WHERE transcript_id = %s",
                    (transcript_id,),
                )
                rows = cur.fetchall()
                cur.close()
                if not rows:
                    return False
                # Block if ANY row has a terminal status (don't re-score successful transcripts)
                terminal = {"success", "processed", "error", "skipped_short", "no_show"}
                return any(row[0] in terminal for row in rows)
            except Exception as e:
                logger.warning(f"Failed to check processed status: {e}")
                return False
            finally:
                database.put_conn(conn)
        return False
    # File fallback
    from config import PROCESSED_LOG
    if PROCESSED_LOG.exists():
        return transcript_id in set(PROCESSED_LOG.read_text().strip().splitlines())
    return False


def _mark_processed(
    transcript_id: str, connection_name: str = "Default",
    score: Optional[int] = None, status: str = "success", error: str = "",
):
    """Record that a transcript has been processed."""
    if database.is_available():
        conn = database.get_conn()
        if conn:
            try:
                cur = conn.cursor()
                cur.execute(
                    """INSERT INTO processed_transcripts (transcript_id, connection_name, score, status, error_message)
                       VALUES (%s, %s, %s, %s, %s)
                       ON CONFLICT (transcript_id, connection_name) DO UPDATE
                       SET score = EXCLUDED.score, status = EXCLUDED.status, error_message = EXCLUDED.error_message,
                           processed_at = NOW()""",
                    (transcript_id, connection_name, score, status, error),
                )
                conn.commit()
                cur.close()
                return
            except Exception as e:
                conn.rollback()
                logger.warning(f"Failed to mark processed in DB: {e}")
            finally:
                database.put_conn(conn)
    # File fallback
    from config import PROCESSED_LOG
    with open(PROCESSED_LOG, "a") as f:
        f.write(f"{transcript_id}\n")


def _get_retry_count(transcript_id: str, connection_name: str = "Default") -> int:
    """Get how many times we've tried to process this transcript."""
    if database.is_available():
        conn = database.get_conn()
        if conn:
            try:
                cur = conn.cursor()
                cur.execute(
                    """SELECT COALESCE(
                        (SELECT (error_message)::int FROM processed_transcripts
                         WHERE transcript_id = %s AND connection_name = %s AND status = 'retrying'),
                    0)""",
                    (transcript_id, connection_name),
                )
                count = cur.fetchone()[0]
                cur.close()
                return count
            except Exception:
                return 0
            finally:
                database.put_conn(conn)
    return 0


def _increment_retry(transcript_id: str, connection_name: str = "Default"):
    """Track a retry attempt for a transcript."""
    current = _get_retry_count(transcript_id, connection_name)
    if database.is_available():
        conn = database.get_conn()
        if conn:
            try:
                cur = conn.cursor()
                cur.execute(
                    """INSERT INTO processed_transcripts (transcript_id, connection_name, status, error_message)
                       VALUES (%s, %s, 'retrying', %s)
                       ON CONFLICT (transcript_id, connection_name) DO UPDATE
                       SET status = 'retrying', error_message = %s::text, processed_at = NOW()""",
                    (transcript_id, connection_name, str(current + 1), str(current + 1)),
                )
                conn.commit()
                cur.close()
            except Exception as e:
                conn.rollback()
                logger.warning(f"Failed to increment retry: {e}")
            finally:
                database.put_conn(conn)


def _send_error_alert(error: Exception, context: str, connection_name: str = "Default",
                      meeting_title: str = "", reason: str = ""):
    """Post pipeline error to Slack for operator visibility with human-readable reason."""
    from config import ERROR_SLACK_WEBHOOK_URL
    url = ERROR_SLACK_WEBHOOK_URL
    if not url:
        return
    import requests as req_lib

    # Generate human-readable reason if not provided
    if not reason:
        err_str = str(error)
        if "'NoneType' object has no attribute 'get'" in err_str:
            reason = "Transcript was empty or not ready. The call recording service may have failed to transcribe this call."
        elif "too short" in err_str.lower():
            reason = "Transcript was too short to analyze (under 500 characters). Likely a very brief call or connection issue."
        elif "rate limit" in err_str.lower() or "429" in err_str:
            reason = "API rate limit hit. Will retry on the next polling cycle."
        elif "overloaded" in err_str.lower() or "529" in err_str:
            reason = "AI service is temporarily overloaded. Will retry automatically on the next cycle."
        elif "timeout" in err_str.lower():
            reason = "Request timed out. Will retry on the next polling cycle."
        elif "credit balance" in err_str.lower() or "insufficient" in err_str.lower():
            reason = "API credits exhausted. Scoring paused until credits are topped up."
        elif "invalid_request" in err_str.lower() and "json" in err_str.lower():
            reason = "AI returned an invalid response. Will retry on the next cycle."
        else:
            reason = "Unexpected error during processing. Will retry automatically."

    title_line = f"Meeting: {meeting_title}\n" if meeting_title else ""
    text = (
        f":red_circle: *Fairplay Pipeline Error*\n"
        f"Connection: {connection_name}\n"
        f"{title_line}"
        f"Context: {context}\n"
        f"*Reason:* {reason}\n"
        f"Error: `{type(error).__name__}: {str(error)[:300]}`\n"
        f"Time: {datetime.now().isoformat()}"
    )
    try:
        req_lib.post(url, json={"text": text}, timeout=10)
    except Exception:
        pass


def _build_default_connection() -> dict:
    """Build a virtual connection dict from server env vars."""
    from config import FIREFLIES_API_KEY, ATTIO_API_KEY, SLACK_WEBHOOK_URL, DEFAULT_FRAMEWORK
    return {
        "name": "Default",
        "fireflies_api_key": FIREFLIES_API_KEY,
        "crm": "attio",
        "crm_api_key": ATTIO_API_KEY,
        "framework": DEFAULT_FRAMEWORK,
        "auto_create_threshold": AUTO_CREATE_THRESHOLD,
        "slack_webhook_url": SLACK_WEBHOOK_URL,
    }


# ── Fireflies webhook (automated pipeline) ──────────────────────────────────

def _process_fireflies_transcript(transcript_id: str, conn: dict):
    """
    Background task: pull transcript from Fireflies, analyze, score, create deal.
    This runs after the webhook returns 200 so Fireflies doesn't timeout.
    """
    # Double-check dedup (belt and suspenders with the poller-level check)
    conn_name = conn.get("name", "Default")
    if _is_processed(transcript_id, conn_name):
        logger.info(f"[{conn_name}] Transcript {transcript_id} already processed (caught in processor), skipping")
        return

    try:
        ff_key = conn["fireflies_api_key"]
        crm_key = conn["crm_api_key"]
        crm_name = conn["crm"]
        framework = conn.get("framework", "custom")
        threshold = conn.get("auto_create_threshold", AUTO_CREATE_THRESHOLD)

        # 1. Pull transcript from Fireflies
        transcript = fireflies_client.get_transcript(transcript_id, api_key=ff_key)
        if not transcript:
            conn_name = conn.get("name", "Default")
            retry_count = _get_retry_count(transcript_id, conn_name)
            if retry_count >= 2:
                # After 3 attempts with no transcript, silently mark as error. No notification.
                _mark_processed(transcript_id, conn_name, status="error", error="Transcript empty after 3 attempts")
                logger.info(f"Transcript {transcript_id} still empty after 3 attempts, marking as error (no alert)")
            else:
                _increment_retry(transcript_id, conn_name)
                logger.info(f"Transcript {transcript_id} not ready yet (None), attempt {retry_count + 1}/3, will retry")
            return

        text = fireflies_client.format_transcript_text(transcript)
        metadata = fireflies_client.get_meeting_metadata(transcript) if transcript else {}

        if not text or len(text) < 500:
            conn_name = conn.get("name", "Default")
            retry_count = _get_retry_count(transcript_id, conn_name)
            title = metadata.get("title", "Unknown Meeting") if metadata else "Unknown Meeting"
            duration = metadata.get("duration", 0) if metadata else 0
            if retry_count >= 2:
                # After 3 attempts, mark as no-show and notify
                _mark_processed(transcript_id, conn_name, status="no_show")
                import requests as _req
                no_show_msg = (
                    f"*No Show* - Call was too short for scoring ({len(text) if text else 0} chars)\n"
                    f"The call recording had no usable transcript. This usually means the other party didn't show up or there was a connection issue."
                )
                slack_url = conn.get("slack_webhook_url")
                if slack_url:
                    try:
                        _req.post(slack_url, json={"text": f":no_entry: *Fairplay: {title}*\n{no_show_msg}"}, timeout=10)
                    except Exception:
                        pass
                teams_url = conn.get("teams_webhook_url")
                if teams_url:
                    try:
                        _req.post(teams_url, json={
                            "type": "message",
                            "attachments": [{"contentType": "application/vnd.microsoft.card.adaptive", "content": {
                                "$schema": "http://adaptivecards.io/schemas/adaptive-card.json",
                                "type": "AdaptiveCard", "version": "1.4",
                                "body": [
                                    {"type": "TextBlock", "text": f"Fairplay: {title}", "weight": "bolder"},
                                    {"type": "TextBlock", "text": no_show_msg, "wrap": True},
                                ],
                            }}],
                        }, timeout=10)
                    except Exception:
                        pass
                logger.info(f"Transcript {transcript_id} marked as no-show after 3 attempts ({len(text) if text else 0} chars)")
                return
            _increment_retry(transcript_id, conn_name)
            logger.info(f"Transcript {transcript_id} too short ({len(text) if text else 0} chars), attempt {retry_count + 1}/3, will retry")
            return

        # 2. Analyze with Claude (with business context if available)
        biz_ctx = {
            "sale_type": conn.get("sale_type", ""),
            "deal_value_range": conn.get("deal_value_range", ""),
            "avg_days_to_close": conn.get("avg_days_to_close", ""),
            "industry_vertical": conn.get("industry_vertical", ""),
        } if any(conn.get(k) for k in ("sale_type", "deal_value_range", "avg_days_to_close", "industry_vertical")) else None
        analysis = transcript_analyzer.analyze_transcript(text, metadata, framework=framework, business_context=biz_ctx, company_icp=conn.get("company_icp"), calibration_notes=conn.get("calibration_notes"))

        if not analysis or not isinstance(analysis, dict):
            logger.warning(f"Transcript {transcript_id} analysis returned invalid result, skipping")
            return

        if not analysis.get("is_sales_conversation"):
            logger.info(f"Transcript {transcript_id} is not a sales conversation, skipping deal creation")
            return

        # 3. Score (with custom weights if configured)
        custom_weights = None
        fw_weights_str = conn.get("framework_weights", "")
        if fw_weights_str:
            try:
                custom_weights = json.loads(fw_weights_str) if isinstance(fw_weights_str, str) else fw_weights_str
            except Exception:
                pass
        score_result = deal_scorer.score_deal(analysis, custom_weights=custom_weights)
        score = score_result["total_score"]
        recommendation = score_result["recommendation"]
        company_name = analysis.get("prospect_company", {}).get("name", "")

        logger.info(
            f"[{conn['name']}] Transcript '{metadata.get('title')}' scored {score}/100 "
            f"({recommendation})"
        )

        # 4. Check for existing deal and previous scores (follow-up intelligence)
        existing_deal = _find_existing_deal(company_name, crm_name, crm_key)
        previous_scores = _get_previous_scores(company_name)

        # Skip scoring notification if deal is already closed (won or lost)
        if _is_deal_closed(existing_deal):
            logger.info(
                f"[{conn['name']}] Deal for '{company_name}' is already closed "
                f"({existing_deal.get('stage')}), skipping"
            )
            # Still log the scored deal but don't notify
            _save_scored_deal(score_result, analysis, metadata, deal_id=existing_deal.get("deal_id"), connection_name=conn.get("name", ""))
            return

        deal_id = None
        if existing_deal:
            # Deal already exists but not closed, don't create a duplicate
            deal_id = existing_deal.get("deal_id")
            logger.info(
                f"[{conn['name']}] Existing deal found for '{company_name}': {existing_deal.get('deal_name')} "
                f"(call #{len(previous_scores) + 1})"
            )
        elif score >= REVIEW_THRESHOLD:
            # No existing deal, score meets threshold
            is_shadow = conn.get("shadow_mode", False)
            metadata["touchpoints"] = len(previous_scores) + 1
            crm_client = crm_factory.get_client(crm_name)
            result = crm_client.create_deal(
                score_result, analysis, metadata, dry_run=is_shadow, api_key=crm_key
            )
            if result and not is_shadow:
                deal_id = result.get("deal_id")
                logger.info(
                    f"[{conn['name']}] Deal created: {result.get('deal_name')} "
                    f"(ID: {deal_id})"
                )
            elif is_shadow:
                logger.info(f"[{conn['name']}] SHADOW: Would create deal '{result.get('deal_name')}' (score: {score})")
            else:
                logger.warning(f"[{conn['name']}] Deal creation returned None for transcript {transcript_id}")
        else:
            logger.info(f"[{conn['name']}] Score {score} below threshold {REVIEW_THRESHOLD}, no deal created")

        # 4b. Log scored deal
        _save_scored_deal(score_result, analysis, metadata, deal_id=deal_id, connection_name=conn.get("name", ""))

        # 4c. Mark transcript as processed
        _mark_processed(transcript_id, conn.get("name", "Default"), score=score, status="success")

        # 5. Notifications (Slack and/or Teams)
        _send_notification(
            conn, score_result, analysis, metadata,
            deal_id=deal_id, existing_deal=existing_deal, previous_scores=previous_scores,
            shadow_mode=conn.get("shadow_mode", False),
        )

    except transcript_analyzer.CreditExhaustedError:
        # Credits exhausted: mark silently, no error alert, no spam
        conn_name = conn.get("name", "Default")
        logger.warning(f"[{conn_name}] API credits exhausted, will retry transcript {transcript_id} when credits are available")
        _mark_processed(transcript_id, conn_name, status="credits_exhausted")
        return

    except transcript_analyzer.TemporaryAPIError:
        # API overloaded/rate limited: mark silently, no error alert, retry next cycle
        conn_name = conn.get("name", "Default")
        logger.warning(f"[{conn_name}] API temporarily unavailable, will retry transcript {transcript_id} next cycle")
        # Don't mark as processed, let it retry naturally
        return

    except Exception as e:
        err_str = str(e).lower()
        conn_name = conn.get("name", "Default")

        # Terminal errors: transcript permanently gone. Mark as error immediately, no retries, no alert.
        if "object_not_found" in err_str or "transcript not found" in err_str or "404" in err_str:
            logger.info(f"[{conn_name}] Transcript {transcript_id} not found in Fireflies (likely deduped server-side), marking as error silently")
            _mark_processed(transcript_id, conn_name, status="error", error="Transcript not found in Fireflies (404)")
            return

        logger.error(f"[{conn_name}] Pipeline failed for transcript {transcript_id}: {e}")
        # Track retry count. Only mark as permanent error after 3 attempts.
        retry_count = _get_retry_count(transcript_id, conn_name)
        if retry_count >= 2:
            _mark_processed(transcript_id, conn_name, status="error", error=str(e)[:500])
            # Try to get meeting title for the error alert
            _err_title = ""
            try:
                _err_t = fireflies_client.get_transcript(transcript_id, api_key=conn.get("fireflies_api_key", ""))
                _err_title = _err_t.get("title", "") if _err_t else ""
            except Exception:
                pass
            _send_error_alert(e, f"Fireflies transcript {transcript_id} (failed after 3 attempts)", conn_name, meeting_title=_err_title)
        else:
            _increment_retry(transcript_id, conn_name)
            logger.info(f"Transcript {transcript_id} attempt {retry_count + 1}/3 failed, will retry next cycle")


def _get_previous_scores(company_name: str) -> list:
    """Look up previous scored calls for a company. Returns list of {score, meeting_title, created_at, breakdown}."""
    if not company_name or not database.is_available():
        return []
    conn = database.get_conn()
    if not conn:
        return []
    try:
        cur = conn.cursor()
        cur.execute(
            """SELECT score, meeting_title, deal_id, created_at, breakdown
               FROM scored_deals
               WHERE LOWER(company_name) = LOWER(%s)
               ORDER BY created_at ASC""",
            (company_name,),
        )
        rows = cur.fetchall()
        cur.close()
        return [
            {"score": r[0], "meeting_title": r[1], "deal_id": r[2], "created_at": r[3].isoformat(),
             "breakdown": r[4] if r[4] else {}}
            for r in rows
        ]
    except Exception as e:
        logger.warning(f"Failed to get previous scores for '{company_name}': {e}")
        return []
    finally:
        database.put_conn(conn)


def _calculate_cumulative_score(current_breakdown: dict, previous_scores: list) -> dict:
    """Calculate cumulative deal score by taking the best score per category across all calls."""
    if not previous_scores:
        return {"score": sum(d.get("score", 0) for d in current_breakdown.values()), "breakdown": current_breakdown}

    # Collect best score per category across all calls + current
    best_per_category = {}
    for cat, data in current_breakdown.items():
        best_per_category[cat] = {
            "score": data.get("score", 0),
            "max": data.get("max", 25),
            "label": data.get("label", cat),
            "from_call": "current",
        }

    for prev in previous_scores:
        prev_bd = prev.get("breakdown", {})
        if not isinstance(prev_bd, dict):
            continue
        for cat, data in prev_bd.items():
            if not isinstance(data, dict):
                continue
            prev_score = data.get("score", 0)
            if cat in best_per_category:
                if prev_score > best_per_category[cat]["score"]:
                    best_per_category[cat]["score"] = prev_score
                    best_per_category[cat]["from_call"] = prev.get("meeting_title", "previous call")
            else:
                best_per_category[cat] = {
                    "score": prev_score,
                    "max": data.get("max", 25),
                    "label": data.get("label", cat),
                    "from_call": prev.get("meeting_title", "previous call"),
                }

    cumulative_total = sum(d["score"] for d in best_per_category.values())
    cumulative_total = min(100, cumulative_total)

    return {"score": cumulative_total, "breakdown": best_per_category}


def _find_existing_deal(company_name: str, crm_name: str, crm_key: Optional[str] = None) -> Optional[dict]:
    """Check if a deal already exists in the CRM for this company. Returns deal info including stage."""
    if not company_name or not crm_name:
        return None
    try:
        crm_client = crm_factory.get_client(crm_name)
        result = crm_client.find_deal_by_company(company_name, api_key=crm_key)
        return result
    except Exception as e:
        logger.warning(f"Failed to check existing deal for '{company_name}' in {crm_name}: {e}")
        return None


def _is_deal_closed(existing_deal: Optional[dict]) -> bool:
    """Check if an existing deal is in a closed state (won or lost)."""
    if not existing_deal:
        return False
    stage = (existing_deal.get("stage") or "").lower()
    closed_keywords = ["closed", "won", "lost", "dead", "churned", "abandoned"]
    return any(kw in stage for kw in closed_keywords)


def _send_slack_notification(
    webhook_url: str, score_result: dict, analysis: dict, metadata: dict,
    deal_id: Optional[str] = None, existing_deal: Optional[dict] = None,
    previous_scores: Optional[list] = None, shadow_mode: bool = False,
):
    """Post a summary to Slack with feedback links and follow-up context."""
    import requests as req_lib
    score = score_result["total_score"]
    rec = score_result["recommendation"].replace("_", " ").title()
    deal_name = score_result.get("deal_name_suggestion", "Unknown")
    title = metadata.get("title", "Unknown Meeting")

    emoji = ":large_green_circle:" if score >= 70 else ":large_yellow_circle:" if score >= 50 else ":red_circle:"

    base_url = _get_base_url()
    from urllib.parse import quote
    feedback_id = quote(deal_id or deal_name, safe='')

    # Build score breakdown with per-category assessments
    breakdown = score_result.get("breakdown", {})
    framework_name = score_result.get("framework", "custom").upper()
    fw_scores = analysis.get("framework_scores", {})
    breakdown_lines = []
    for cat, data in breakdown.items():
        label = data.get("label", cat)
        effective_max = data.get("effective_max", data["max"])
        depth_note = ""
        if effective_max < data["max"]:
            ev_count = data.get("evidence_count", 0)
            depth_note = f" (depth: {ev_count} signal{'s' if ev_count != 1 else ''}, capped at {effective_max})"
        score_str = f"{data['score']}/{data['max']}{depth_note}"
        # Get the assessment from the analysis framework_scores
        assessment = ""
        if isinstance(fw_scores.get(cat), dict):
            assessment = fw_scores[cat].get("assessment", "")
        if assessment:
            # Truncate at last complete word boundary, not mid-word
            if len(assessment) > 150:
                truncated = assessment[:150].rsplit(' ', 1)[0]
                assessment = truncated
            breakdown_lines.append(f"  {label}: *{score_str}* - {assessment}")
        else:
            breakdown_lines.append(f"  {label}: *{score_str}*")
    breakdown_block = "\n".join(breakdown_lines) if breakdown_lines else "N/A"

    # Shadow mode prefix
    shadow_line = ""
    if shadow_mode:
        shadow_line = ":ghost: *SHADOW MODE* (scoring only, no CRM writes)\n"
        if score >= 70:
            shadow_line += ":white_check_mark: _Would auto-create deal_\n"
        elif score >= 50:
            shadow_line += ":warning: _Would route to Needs Review_\n"
        else:
            shadow_line += ":no_entry_sign: _Would not create deal_\n"

    # Follow-up context
    followup_line = ""
    if existing_deal:
        followup_line = f":repeat: *Follow-up call* (existing deal: {existing_deal.get('deal_name', '?')}, stage: {existing_deal.get('stage', '?')})\n"
    if previous_scores:
        prev_scores_str = " > ".join(str(p["score"]) for p in previous_scores)
        call_num = len(previous_scores) + 1
        followup_line += f":chart_with_upwards_trend: Call #{call_num} | Score history: {prev_scores_str} > *{score}*\n"

    # Cumulative deal score (best-of per category across all calls)
    cumulative_line = ""
    if previous_scores:
        cumulative = _calculate_cumulative_score(score_result.get("breakdown", {}), previous_scores)
        cum_score = cumulative["score"]
        cum_emoji = ":large_green_circle:" if cum_score >= 70 else ":large_yellow_circle:" if cum_score >= 50 else ":red_circle:"
        cumulative_line = f"{cum_emoji} Deal Score: *{cum_score}/100* (cumulative across {len(previous_scores) + 1} calls)\n"

    header = "Fairplay SHADOW" if shadow_mode else "Fairplay"

    # Meeting type label (helps users filter follow-ups, client meetings, etc.)
    meeting_type = analysis.get("meeting_type", "")
    type_labels = {
        "discovery": "Discovery", "demo": "Demo", "follow_up": "Follow-up",
        "negotiation": "Negotiation", "internal": "Internal", "recruiting": "Recruiting",
        "vendor_eval": "Vendor Eval", "partner": "Partner",
    }
    type_label = type_labels.get(meeting_type, "")
    type_badge = f" _[{type_label}]_" if type_label else ""

    text = (
        f"{emoji} *{header}: {title}*{type_badge}\n"
        f"{shadow_line}"
        f"{followup_line}"
        f"Score: *{score}/100* ({framework_name}) | Recommendation: *{rec}*\n"
        f"{cumulative_line}"
        f"Deal: {deal_name}\n"
        f"Breakdown:\n{breakdown_block}\n"
        f"Insight: _{score_result.get('key_insight', 'N/A')}_\n\n"
        f"*Was this a deal?*  "
        f":white_check_mark: <{base_url}/feedback/{feedback_id}?vote=good_deal|Good Deal>  "
        f":x: <{base_url}/feedback/{feedback_id}?vote=not_a_deal|Not a Deal>  "
        f":arrows_counterclockwise: <{base_url}/feedback/{feedback_id}?vote=needs_review|Needs Review>\n"
        f"*How was the assessment?*  "
        f":+1: <{base_url}/feedback/{feedback_id}?vote=assessment_good|Accurate>  "
        f":-1: <{base_url}/feedback/{feedback_id}?vote=assessment_bad|Off the mark>"
    )
    try:
        req_lib.post(webhook_url, json={"text": text}, timeout=10)
    except Exception as e:
        logger.warning(f"Slack notification failed: {e}")


def _send_teams_notification(
    webhook_url: str, score_result: dict, analysis: dict, metadata: dict,
    deal_id: Optional[str] = None, existing_deal: Optional[dict] = None,
    previous_scores: Optional[list] = None, shadow_mode: bool = False,
):
    """Post a summary to Microsoft Teams via incoming webhook (Adaptive Card)."""
    import requests as req_lib
    score = score_result["total_score"]
    rec = score_result["recommendation"].replace("_", " ").title()
    deal_name = score_result.get("deal_name_suggestion", "Unknown")
    title = metadata.get("title", "Unknown Meeting")
    framework_name = score_result.get("framework", "custom").upper()

    color = "good" if score >= 70 else "warning" if score >= 50 else "attention"

    # Build breakdown text
    breakdown = score_result.get("breakdown", {})
    fw_scores = analysis.get("framework_scores", {})
    breakdown_lines = []
    for cat, data in breakdown.items():
        label = data.get("label", cat)
        effective_max = data.get("effective_max", data["max"])
        depth_note = ""
        if effective_max < data["max"]:
            ev_count = data.get("evidence_count", 0)
            depth_note = f" (depth: {ev_count} signal{'s' if ev_count != 1 else ''}, capped at {effective_max})"
        score_str = f"{data['score']}/{data['max']}{depth_note}"
        assessment = ""
        if isinstance(fw_scores.get(cat), dict):
            assessment = fw_scores[cat].get("assessment", "")
        if assessment:
            if len(assessment) > 150:
                assessment = assessment[:150].rsplit(' ', 1)[0]
            breakdown_lines.append(f"- **{label}:** {score_str} - {assessment}")
        else:
            breakdown_lines.append(f"- **{label}:** {score_str}")
    breakdown_block = "\n".join(breakdown_lines) if breakdown_lines else "N/A"

    # Context lines
    context_lines = ""
    if shadow_mode:
        context_lines += "**SHADOW MODE** (scoring only, no CRM writes)\n\n"
    if existing_deal:
        context_lines += f"Follow-up call (existing deal: {existing_deal.get('deal_name', '?')}, stage: {existing_deal.get('stage', '?')})\n\n"
    if previous_scores:
        prev_scores_str = " > ".join(str(p["score"]) for p in previous_scores)
        call_num = len(previous_scores) + 1
        context_lines += f"Call #{call_num} | Score history: {prev_scores_str} > **{score}**\n\n"

    cumulative_line = ""
    if previous_scores:
        cumulative = _calculate_cumulative_score(score_result.get("breakdown", {}), previous_scores)
        cum_score = cumulative["score"]
        cumulative_line = f"Deal Score: **{cum_score}/100** (cumulative across {len(previous_scores) + 1} calls)\n\n"

    base_url = _get_base_url()
    from urllib.parse import quote
    feedback_id = quote(deal_id or deal_name, safe='')

    # Meeting type label
    meeting_type = analysis.get("meeting_type", "")
    type_labels = {
        "discovery": "Discovery", "demo": "Demo", "follow_up": "Follow-up",
        "negotiation": "Negotiation", "internal": "Internal", "recruiting": "Recruiting",
        "vendor_eval": "Vendor Eval", "partner": "Partner",
    }
    type_label = type_labels.get(meeting_type, "")
    type_suffix = f"  [{type_label}]" if type_label else ""

    # Teams Adaptive Card payload
    card = {
        "type": "message",
        "attachments": [{
            "contentType": "application/vnd.microsoft.card.adaptive",
            "content": {
                "$schema": "http://adaptivecards.io/schemas/adaptive-card.json",
                "type": "AdaptiveCard",
                "version": "1.4",
                "body": [
                    {
                        "type": "TextBlock",
                        "size": "medium",
                        "weight": "bolder",
                        "text": f"Fairplay{'  SHADOW' if shadow_mode else ''}: {title}{type_suffix}",
                    },
                    {
                        "type": "ColumnSet",
                        "columns": [
                            {
                                "type": "Column",
                                "width": "auto",
                                "items": [{
                                    "type": "TextBlock",
                                    "text": f"{score}/100",
                                    "size": "extraLarge",
                                    "weight": "bolder",
                                    "color": color,
                                }],
                            },
                            {
                                "type": "Column",
                                "width": "stretch",
                                "items": [
                                    {"type": "TextBlock", "text": f"**{rec}** ({framework_name})", "wrap": True},
                                    {"type": "TextBlock", "text": deal_name, "isSubtle": True, "spacing": "none"},
                                ],
                            },
                        ],
                    },
                    {"type": "TextBlock", "text": context_lines + cumulative_line, "wrap": True}
                    if (context_lines or cumulative_line) else {"type": "TextBlock", "text": ""},
                    {
                        "type": "TextBlock",
                        "text": f"**Breakdown**\n\n{breakdown_block}",
                        "wrap": True,
                    },
                    {
                        "type": "TextBlock",
                        "text": f"*{score_result.get('key_insight', 'N/A')}*",
                        "wrap": True,
                        "isSubtle": True,
                    },
                ],
                "actions": [
                    {"type": "Action.OpenUrl", "title": "Good Deal", "url": f"{base_url}/feedback/{feedback_id}?vote=good_deal"},
                    {"type": "Action.OpenUrl", "title": "Not a Deal", "url": f"{base_url}/feedback/{feedback_id}?vote=not_a_deal"},
                    {"type": "Action.OpenUrl", "title": "Needs Review", "url": f"{base_url}/feedback/{feedback_id}?vote=needs_review"},
                    {"type": "Action.OpenUrl", "title": "Assessment: Accurate", "url": f"{base_url}/feedback/{feedback_id}?vote=assessment_good"},
                    {"type": "Action.OpenUrl", "title": "Assessment: Off", "url": f"{base_url}/feedback/{feedback_id}?vote=assessment_bad"},
                ],
            },
        }],
    }

    try:
        req_lib.post(webhook_url, json=card, timeout=10)
    except Exception as e:
        logger.warning(f"Teams notification failed: {e}")


def _send_notification(
    conn: dict, score_result: dict, analysis: dict, metadata: dict,
    deal_id: Optional[str] = None, existing_deal: Optional[dict] = None,
    previous_scores: Optional[list] = None, shadow_mode: bool = False,
):
    """Send notification to Slack and/or Teams based on connection config."""
    kwargs = dict(
        score_result=score_result, analysis=analysis, metadata=metadata,
        deal_id=deal_id, existing_deal=existing_deal,
        previous_scores=previous_scores, shadow_mode=shadow_mode,
    )
    slack_url = conn.get("slack_webhook_url")
    if slack_url:
        _send_slack_notification(slack_url, **kwargs)
    teams_url = conn.get("teams_webhook_url")
    if teams_url:
        _send_teams_notification(teams_url, **kwargs)


@app.post("/webhook/fireflies")
async def fireflies_webhook_default(request: Request, background_tasks: BackgroundTasks):
    """
    Default Fireflies webhook using server env var API keys.
    No connection setup needed. Configure in Fireflies:
    Settings > Integrations > Webhooks > Add webhook URL.
    """
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

    # Idempotency check: skip if already processed to avoid duplicate deals
    if _is_processed(transcript_id):
        logger.info(f"Fireflies default webhook: transcript {transcript_id} already processed, skipping")
        return {"status": "already_processed", "transcript_id": transcript_id}

    conn = _build_default_connection()
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

    # Idempotency check: skip if already processed to avoid duplicate deals
    conn_name = conn.get("name", "Default")
    if _is_processed(transcript_id, conn_name):
        logger.info(f"[{conn_name}] Fireflies webhook: transcript {transcript_id} already processed, skipping")
        return {"status": "already_processed", "transcript_id": transcript_id}

    logger.info(f"[{conn['name']}] Fireflies webhook received for transcript {transcript_id}")
    background_tasks.add_task(_process_fireflies_transcript, transcript_id, conn)

    return {"status": "processing", "transcript_id": transcript_id}


# ── Process latest (manual trigger from Slack) ───────────────────────────────

@app.post("/process-latest", dependencies=[Depends(require_api_key)])
def process_latest_call(background_tasks: BackgroundTasks):
    """
    Pull the most recent Fireflies transcript and run the full pipeline.
    Use this when the webhook doesn't fire. Returns immediately, processes in background.
    """
    conn = _build_default_connection()
    if not conn["fireflies_api_key"]:
        raise HTTPException(status_code=400, detail="FIREFLIES_API_KEY not configured")

    try:
        transcripts = fireflies_client.list_transcripts(limit=1, api_key=conn["fireflies_api_key"])
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to list transcripts: {e}")

    if not transcripts:
        raise HTTPException(status_code=404, detail="No transcripts found")

    latest = transcripts[0]
    transcript_id = latest.get("id")
    title = latest.get("title", "Unknown")

    background_tasks.add_task(_process_fireflies_transcript, transcript_id, conn)

    return {
        "status": "processing",
        "transcript_id": transcript_id,
        "title": title,
        "message": f"Processing '{title}'. Results will appear in Slack.",
    }


@app.post("/slack/score-call")
async def slack_score_call(request: Request, background_tasks: BackgroundTasks):
    """
    Slack slash command endpoint. Configure in Slack:
    /score-call -> POST https://web-production-9afb1.up.railway.app/slack/score-call

    Pulls the most recent Fireflies transcript and runs the pipeline.
    Responds immediately to Slack (within 3s), processes in background.
    """
    conn = _build_default_connection()
    if not conn["fireflies_api_key"]:
        return {"response_type": "ephemeral", "text": "Error: FIREFLIES_API_KEY not configured on server."}

    try:
        transcripts = fireflies_client.list_transcripts(limit=1, api_key=conn["fireflies_api_key"])
    except Exception as e:
        return {"response_type": "ephemeral", "text": f"Error fetching transcripts: {e}"}

    if not transcripts:
        return {"response_type": "ephemeral", "text": "No transcripts found in Fireflies."}

    latest = transcripts[0]
    transcript_id = latest.get("id")
    title = latest.get("title", "Unknown")

    background_tasks.add_task(_process_fireflies_transcript, transcript_id, conn)

    return {
        "response_type": "in_channel",
        "text": f":hourglass_flowing_sand: Scoring *{title}*... results will appear shortly.",
    }


# ── Slack Events (bot replies for calibration matching) ──────────────────────

SLACK_BOT_TOKEN = os.getenv("SLACK_BOT_TOKEN", "")


@app.post("/slack/events")
async def slack_events(request: Request):
    """Handle Slack Events API including URL verification challenge."""
    body = await request.json()

    # URL verification challenge
    if body.get("type") == "url_verification":
        return {"challenge": body.get("challenge", "")}

    # Event callback
    if body.get("type") == "event_callback":
        event = body.get("event", {})
        # Only process channel messages (not bot messages)
        if event.get("type") == "message" and not event.get("bot_id") and not event.get("subtype"):
            text = event.get("text", "").strip()
            thread_ts = event.get("thread_ts", "")
            channel = event.get("channel", "")

            # Only process threaded replies
            if thread_ts and text:
                # First try scoring-notification feedback (newer flow), fall back to calibration matching
                handled = _handle_scoring_feedback_reply(text, channel, thread_ts)
                if not handled:
                    _handle_calibration_reply(text, channel, thread_ts)

    return {"ok": True}


def _fetch_slack_thread_parent(channel: str, thread_ts: str) -> Optional[str]:
    """Fetch the parent message of a Slack thread. Returns text or None."""
    if not SLACK_BOT_TOKEN:
        return None
    try:
        import requests as req_lib
        resp = req_lib.get(
            "https://slack.com/api/conversations.replies",
            headers={"Authorization": f"Bearer {SLACK_BOT_TOKEN}"},
            params={"channel": channel, "ts": thread_ts, "limit": 1},
            timeout=10,
        )
        if not resp.ok:
            return None
        data = resp.json()
        messages = data.get("messages", [])
        if not messages:
            return None
        return messages[0].get("text", "")
    except Exception as e:
        logger.warning(f"Failed to fetch Slack thread parent: {e}")
        return None


def _handle_scoring_feedback_reply(reply_text: str, channel: str, thread_ts: str) -> bool:
    """
    Process a Slack reply on a Fairplay scoring notification thread.
    Parses the parent message for the deal_id, runs the reply through Claude
    to extract a calibration note, and appends to the connection's calibration_notes.
    Returns True if handled (a Fairplay notification thread), False otherwise.
    """
    parent = _fetch_slack_thread_parent(channel, thread_ts)
    if not parent or "Fairplay" not in parent:
        return False

    # Parse the deal_id from the feedback URL pattern in the parent message
    import re
    from urllib.parse import unquote
    m = re.search(r"/feedback/([^?>|\s]+)\?vote=", parent)
    if not m:
        return False
    deal_id = unquote(m.group(1))

    if not database.is_available():
        return True

    # Look up the scored deal and the connection
    db = database.get_conn()
    if not db:
        return True
    try:
        cur = db.cursor()
        cur.execute(
            """SELECT deal_name, meeting_title, score, recommendation, framework, breakdown,
                      key_insight, company_name, metadata
               FROM scored_deals
               WHERE deal_id = %s OR deal_name = %s
               ORDER BY created_at DESC LIMIT 1""",
            (deal_id, deal_id),
        )
        row = cur.fetchone()
        cur.close()
        if not row:
            logger.info(f"Scoring feedback reply: no scored deal found for {deal_id}")
            return True

        deal_name, meeting_title, score, recommendation, framework, breakdown, key_insight, company_name, metadata = row

        # Find the connection from metadata
        connection_name = ""
        if isinstance(metadata, dict):
            connection_name = metadata.get("connection_name", "")
        elif isinstance(metadata, str):
            try:
                meta_obj = json.loads(metadata)
                connection_name = meta_obj.get("connection_name", "")
            except Exception:
                pass

        conn_obj = None
        if connection_name:
            for c in connections.list_connections_full():
                if c.get("name") == connection_name:
                    conn_obj = c
                    break

        # Run reply through Claude to extract a calibration insight
        breakdown_str = " | ".join(
            f"{k}: {v.get('score', 0)}/{v.get('max', 0)}" for k, v in (breakdown or {}).items()
        ) if isinstance(breakdown, dict) else ""

        calibration_note = _extract_calibration_note(
            reply_text=reply_text,
            meeting_title=meeting_title or "",
            framework=framework or "custom",
            score=score,
            breakdown_str=breakdown_str,
            key_insight=key_insight or "",
            company_name=company_name or "",
        )

        if not calibration_note:
            return True

        # Append to connection's calibration_notes
        if conn_obj:
            existing = conn_obj.get("calibration_notes", "") or ""
            timestamp = datetime.now().strftime("%Y-%m-%d")
            new_entry = f"[{timestamp}] {calibration_note}"
            updated = (existing + "\n" + new_entry).strip() if existing else new_entry
            # Cap at ~8000 chars to keep prompt size sane
            if len(updated) > 8000:
                updated = updated[-8000:]
                idx = updated.find("\n")
                if idx > 0:
                    updated = updated[idx + 1:]
            connections.update_connection(conn_obj["webhook_id"], {"calibration_notes": updated})

        # Confirm in thread
        if SLACK_BOT_TOKEN:
            import requests as req_lib
            try:
                req_lib.post(
                    "https://slack.com/api/chat.postMessage",
                    headers={"Authorization": f"Bearer {SLACK_BOT_TOKEN}"},
                    json={
                        "channel": channel,
                        "thread_ts": thread_ts,
                        "text": f":brain: Calibration noted for future scoring: _{calibration_note}_",
                    },
                    timeout=10,
                )
            except Exception:
                pass

        # Also log to feedback table as a freeform note
        _save_feedback({
            "deal_id": deal_id,
            "vote": "calibration_note",
            "note": f"User: {reply_text[:300]} | Extracted: {calibration_note[:300]}",
            "timestamp": datetime.now().isoformat(),
        })

        logger.info(f"Calibration note saved for {company_name} via Slack reply")
        return True
    except Exception as e:
        logger.warning(f"Scoring feedback reply handling failed: {e}")
        return True
    finally:
        database.put_conn(db)


def _extract_calibration_note(
    reply_text: str, meeting_title: str, framework: str, score: int,
    breakdown_str: str, key_insight: str, company_name: str,
) -> str:
    """Use Claude to convert natural-language Slack feedback into a structured calibration note."""
    try:
        import anthropic
        from config import ANTHROPIC_API_KEY, CLAUDE_MODEL
        client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)

        prompt = f"""A sales leader gave feedback on a Fairplay scoring decision via Slack. Convert their feedback into a SHORT calibration note that will be added to this company's scoring context for future calls.

The note should be 1-2 sentences, in the form of a rule or pattern Fairplay should remember. Examples:
- "When a prospect mentions specific dollar amounts and timeline together, that's a strong budget+timeline signal regardless of brevity."
- "For this company, post-acquisition founders count as decision_makers even without an explicit title statement."

Do NOT just restate the feedback. Extract the underlying rule.

CALL CONTEXT:
- Meeting: {meeting_title}
- Company: {company_name}
- Framework: {framework.upper()}
- Score: {score}/100
- Breakdown: {breakdown_str}
- Key insight: {key_insight}

USER FEEDBACK:
{reply_text}

Return ONLY the calibration note as plain text. No quotes, no markdown, no preamble. If the feedback is not actionable for future scoring (e.g., just "thanks" or "ok"), return the literal string SKIP."""

        message = client.messages.create(
            model=CLAUDE_MODEL,
            max_tokens=300,
            messages=[{"role": "user", "content": prompt}],
        )
        note = message.content[0].text.strip()
        if note.upper().startswith("SKIP"):
            return ""
        # Strip markdown wrapping if Claude added it
        if note.startswith("```"):
            note = note.split("```")[1].strip()
        return note[:400]
    except Exception as e:
        logger.warning(f"Calibration note extraction failed: {e}")
        return ""


def _handle_calibration_reply(reply_text: str, channel: str, thread_ts: str):
    """Process a Slack reply that might be linking a transcript to a deal."""
    if not database.is_available():
        return

    # Look for unmatched calibration results (no deal linked)
    # The reply should contain a deal name or company name
    # Try to find matching deal in calibration_results where deal_id is empty or null
    conn = database.get_conn()
    if not conn:
        return

    try:
        cur = conn.cursor()
        # Find recent unmatched scored transcripts
        cur.execute("""
            SELECT id, company_name, fairplay_score, transcript_id
            FROM calibration_results
            WHERE deal_id IS NULL OR deal_id = ''
            ORDER BY created_at DESC LIMIT 20
        """)
        unmatched = cur.fetchall()

        if not unmatched:
            cur.close()
            return

        # Try to match the reply text to a deal name
        reply_norm = _normalize_company(reply_text)
        if not reply_norm:
            cur.close()
            return

        # Update the first unmatched entry that the reply seems to reference
        for row in unmatched:
            cal_id, company, score, tid = row
            company_norm = _normalize_company(company or "")
            if company_norm and (company_norm in reply_norm or reply_norm in company_norm):
                # Update this calibration entry with the deal info from the reply
                cur.execute(
                    "UPDATE calibration_results SET deal_name = %s, matched_by = 'slack_reply' WHERE id = %s",
                    (reply_text.strip(), cal_id),
                )
                conn.commit()

                # Post confirmation
                if SLACK_BOT_TOKEN:
                    import requests as req_lib
                    req_lib.post(
                        "https://slack.com/api/chat.postMessage",
                        headers={"Authorization": f"Bearer {SLACK_BOT_TOKEN}"},
                        json={
                            "channel": channel,
                            "thread_ts": thread_ts,
                            "text": f":white_check_mark: Linked *{company}* (score: {score}) to deal: *{reply_text.strip()}*",
                        },
                        timeout=10,
                    )
                logger.info(f"[Calibration] Slack reply linked {company} to deal: {reply_text.strip()}")
                break

        cur.close()
    except Exception as e:
        logger.warning(f"Calibration reply handling failed: {e}")
        conn.rollback()
    finally:
        database.put_conn(conn)


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


@app.post("/upload", response_model=AnalyzeResponse, dependencies=[Depends(require_api_key)])
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

        if not text or len(text) < 500:
            logger.warning(f"[{conn['name']}] Transcript too short ({len(text)} chars), skipping")
            return

        biz_ctx = {
            "sale_type": conn.get("sale_type", ""),
            "deal_value_range": conn.get("deal_value_range", ""),
            "avg_days_to_close": conn.get("avg_days_to_close", ""),
            "industry_vertical": conn.get("industry_vertical", ""),
        } if any(conn.get(k) for k in ("sale_type", "deal_value_range", "avg_days_to_close", "industry_vertical")) else None
        analysis = transcript_analyzer.analyze_transcript(text, metadata, framework=framework, business_context=biz_ctx, company_icp=conn.get("company_icp"), calibration_notes=conn.get("calibration_notes"))

        if not analysis.get("is_sales_conversation"):
            logger.info(f"[{conn['name']}] Not a sales conversation, skipping deal creation")
            return

        custom_weights = None
        fw_weights_str = conn.get("framework_weights", "")
        if fw_weights_str:
            try:
                custom_weights = json.loads(fw_weights_str) if isinstance(fw_weights_str, str) else fw_weights_str
            except Exception:
                pass
        score_result = deal_scorer.score_deal(analysis, custom_weights=custom_weights)
        score = score_result["total_score"]
        recommendation = score_result["recommendation"]
        company_name = analysis.get("prospect_company", {}).get("name", "")

        logger.info(
            f"[{conn['name']}] '{metadata.get('title')}' scored {score}/100 ({recommendation})"
        )

        # Check for existing deal and previous scores (follow-up intelligence)
        existing_deal = _find_existing_deal(company_name, crm_name, crm_key)
        previous_scores = _get_previous_scores(company_name)

        # Skip if deal is already closed
        if _is_deal_closed(existing_deal):
            logger.info(
                f"[{conn['name']}] Deal for '{company_name}' is already closed "
                f"({existing_deal.get('stage')}), skipping"
            )
            _save_scored_deal(score_result, analysis, metadata, deal_id=existing_deal.get("deal_id"), connection_name=conn.get("name", ""))
            return

        deal_id = None
        if existing_deal:
            deal_id = existing_deal.get("deal_id")
            logger.info(
                f"[{conn['name']}] Existing deal found for '{company_name}': {existing_deal.get('deal_name')} "
                f"(call #{len(previous_scores) + 1})"
            )
        elif score >= REVIEW_THRESHOLD:
            is_shadow = conn.get("shadow_mode", False)
            metadata["touchpoints"] = len(previous_scores) + 1
            crm_client = crm_factory.get_client(crm_name)
            result = crm_client.create_deal(
                score_result, analysis, metadata, dry_run=is_shadow, api_key=crm_key
            )
            if result and not is_shadow:
                deal_id = result.get("deal_id")
                logger.info(f"[{conn['name']}] Deal created: {result.get('deal_name')}")
            elif is_shadow:
                logger.info(f"[{conn['name']}] SHADOW: Would create deal '{result.get('deal_name')}' (score: {score})")
            else:
                logger.warning(f"[{conn['name']}] Deal creation returned None")
        else:
            logger.info(f"[{conn['name']}] Score {score} below threshold {REVIEW_THRESHOLD}, no deal created")

        _save_scored_deal(score_result, analysis, metadata, deal_id=deal_id, connection_name=conn.get("name", ""))

        _send_notification(
            conn, score_result, analysis, metadata,
            deal_id=deal_id, existing_deal=existing_deal, previous_scores=previous_scores,
            shadow_mode=conn.get("shadow_mode", False),
        )

    except transcript_analyzer.CreditExhaustedError:
        logger.warning(f"[{conn.get('name', '?')}] API credits exhausted, skipping silently")
        return

    except transcript_analyzer.TemporaryAPIError:
        logger.warning(f"[{conn.get('name', '?')}] API temporarily unavailable, skipping silently")
        return

    except Exception as e:
        logger.error(f"[{conn.get('name', '?')}] Pipeline failed: {e}")
        _send_error_alert(e, "Transcript text processing", conn.get("name", "Default"))


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
    body = await request.json()

    # Zoom sends a validation challenge on first setup - handle before connection check
    if body.get("event") == "endpoint.url_validation":
        import hashlib, hmac
        plain_token = body.get("payload", {}).get("plainToken", "")
        # Try connection secret first, fall back to checking all connections
        conn = connections.get_connection(webhook_id)
        zoom_secret = ""
        if conn:
            zoom_secret = conn.get("zoom_webhook_secret", "")
        if not zoom_secret:
            # Try to find any connection with a zoom secret for validation
            all_conns = connections.list_connections_full() if hasattr(connections, 'list_connections_full') else []
            for c in all_conns:
                if c.get("zoom_webhook_secret"):
                    zoom_secret = c["zoom_webhook_secret"]
                    break
        if not zoom_secret:
            zoom_secret = webhook_id  # Last resort fallback
        hash_value = hmac.HMAC(
            zoom_secret.encode(), plain_token.encode(), hashlib.sha256
        ).hexdigest()
        return {"plainToken": plain_token, "encryptedToken": hash_value}

    conn = connections.get_connection(webhook_id)
    if not conn or not conn.get("active"):
        raise HTTPException(status_code=404, detail="Invalid or inactive webhook")

    event = body.get("event", "")
    if event in ("recording.transcript_completed", "recording.completed"):
        # Idempotency check: extract recording ID and skip if already processed
        payload_obj = body.get("payload", {}).get("object", {})
        recording_id = payload_obj.get("uuid") or payload_obj.get("id", "")
        if recording_id:
            zoom_tid = f"zoom_{recording_id}"
            conn_name = conn.get("name", "Default")
            if _is_processed(zoom_tid, conn_name):
                logger.info(f"[{conn_name}] Zoom webhook: recording {recording_id} already processed, skipping")
                return {"status": "already_processed", "recording_id": recording_id}

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


# ── Fathom webhook ────────────────────────────────────────────────────────────

def _process_fathom_recording(body: dict, conn: dict):
    """
    Background task: pull Fathom recording transcript via API, analyze, score, create deal.
    """
    import requests as req_lib

    try:
        # Extract recording ID from webhook payload
        recording_id = body.get("recording_id") or body.get("data", {}).get("recording_id", "")
        if not recording_id:
            logger.warning(f"[{conn['name']}] Fathom webhook: no recording_id found")
            return

        fathom_tid = f"fathom_{recording_id}"
        conn_name = conn.get("name", "Default")
        if _is_processed(fathom_tid, conn_name):
            logger.info(f"[{conn_name}] Fathom recording {recording_id} already processed, skipping")
            return

        fathom_key = conn.get("fathom_api_key", "")
        if not fathom_key:
            logger.warning(f"[{conn['name']}] Fathom API key not configured")
            return

        # Pull transcript from Fathom API
        headers = {"X-Api-Key": fathom_key}
        resp = req_lib.get(
            f"https://api.fathom.video/external/v1/recordings/{recording_id}/transcript",
            headers=headers,
            timeout=30,
        )
        resp.raise_for_status()
        data = resp.json()

        # Build text from Fathom transcript format
        transcript_entries = data if isinstance(data, list) else data.get("transcript", [])
        lines = []
        for entry in transcript_entries:
            speaker_obj = entry.get("speaker", {})
            speaker = speaker_obj.get("display_name", "Unknown") if isinstance(speaker_obj, dict) else str(speaker_obj)
            text = entry.get("text", "")
            if text:
                lines.append(f"**{speaker}:** {text}")

        text = "\n".join(lines)

        # Get recording metadata
        meta_resp = req_lib.get(
            f"https://api.fathom.video/external/v1/recordings/{recording_id}",
            headers=headers,
            timeout=15,
        )
        rec_data = meta_resp.json() if meta_resp.ok else {}

        metadata = {
            "title": rec_data.get("title", f"Fathom Recording {recording_id}"),
            "date": rec_data.get("created_at", datetime.now().isoformat()),
            "source": "fathom",
            "participants": [
                p.get("display_name", "") for p in rec_data.get("participants", [])
                if isinstance(p, dict)
            ],
        }

        # Store fathom transcript ID in metadata for dedup tracking
        metadata["transcript_id"] = fathom_tid
        _process_transcript_text(text, metadata, conn)

    except Exception as e:
        logger.error(f"[{conn['name']}] Fathom processing failed: {e}")


@app.post("/webhook/fathom/{webhook_id}")
async def fathom_webhook(webhook_id: str, request: Request, background_tasks: BackgroundTasks):
    """
    Webhook for Fathom. Configure in Fathom:
    Settings > Integrations > Webhooks > new-meeting-content-ready
    URL: https://your-domain/webhook/fathom/{webhook_id}
    """
    conn = connections.get_connection(webhook_id)
    if not conn or not conn.get("active"):
        raise HTTPException(status_code=404, detail="Invalid or inactive webhook")

    body = await request.json()
    logger.info(f"[{conn['name']}] Fathom webhook received")
    background_tasks.add_task(_process_fathom_recording, body, conn)
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
    Called from Slack/Teams notification links.

    Deal-quality votes (change Attio deal stage):
      - good_deal: Confirms the deal. No stage change.
      - not_a_deal: Moves deal to "Lost" in Attio.
      - needs_review: Moves deal to "Discovery Scheduled" in Attio.

    Assessment-quality votes (feedback on Fairplay itself, no CRM changes):
      - assessment_good: Fairplay's scoring was accurate.
      - assessment_bad: Fairplay's scoring was off the mark.
    """
    deal_votes = {"good_deal", "not_a_deal", "needs_review"}
    assessment_votes = {"assessment_good", "assessment_bad"}
    valid_votes = deal_votes | assessment_votes
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

    action_taken = "Feedback logged."
    if vote in deal_votes:
        # Deal-quality feedback: update Attio stage if deal_id looks like a real Attio record ID (UUID)
        # If it's a deal name (e.g. "NN-Street Talk-KO-2026.04"), skip CRM update but still log feedback
        is_attio_id = len(deal_id) == 36 and deal_id.count("-") == 4
        if is_attio_id:
            try:
                import attio_client
                if vote == "not_a_deal":
                    result = attio_client.update_deal_stage(deal_id, "Lost")
                    action_taken = "Deal moved to Lost." if result else "Feedback logged (stage update failed)."
                elif vote == "needs_review":
                    from config import ATTIO_DEAL_STAGE_REVIEW
                    result = attio_client.update_deal_stage(deal_id, ATTIO_DEAL_STAGE_REVIEW)
                    action_taken = f"Deal moved to {ATTIO_DEAL_STAGE_REVIEW}." if result else "Feedback logged (stage update failed)."
                elif vote == "good_deal":
                    action_taken = "Deal confirmed. No changes made."
            except Exception as e:
                logger.warning(f"Attio stage update failed: {e}")
                action_taken = "Feedback logged (CRM update failed)."
        else:
            # Not a real Attio ID (shadow mode or failed deal creation). Just log the feedback.
            action_taken = "Feedback logged. Deal was not created in CRM."
    else:
        # Assessment-quality feedback: no CRM changes, used for calibration
        if vote == "assessment_good":
            action_taken = "Thanks. This confirms Fairplay's scoring was on target."
        elif vote == "assessment_bad":
            action_taken = "Thanks. This signals Fairplay's scoring was off. We'll use this to calibrate."

    emoji_map = {
        "good_deal": "Confirmed as Deal",
        "not_a_deal": "Moved to Lost",
        "needs_review": "Moved to Review",
        "assessment_good": "Assessment: Accurate",
        "assessment_bad": "Assessment: Off the mark",
    }
    from fastapi.responses import HTMLResponse
    return HTMLResponse(
        f"<html><body style='font-family:sans-serif;text-align:center;padding:60px;'>"
        f"<h1>Feedback Recorded</h1>"
        f"<p>Deal: <b>{deal_id}</b></p>"
        f"<p>Your vote: <b>{emoji_map.get(vote, vote)}</b></p>"
        f"<p>{action_taken}</p>"
        f"<p>Thanks! This helps Fairplay get smarter over time.</p>"
        f"</body></html>"
    )


@app.get("/feedback", dependencies=[Depends(require_api_key)])
def list_feedback():
    """List all feedback entries. Useful for reviewing accuracy."""
    return _load_feedback()


# ── Deal Log (scored deals history) ──────────────────────────────────────────

DEALS_LOG_FILE = Path(__file__).parent / ".deals_log.json"


def _save_scored_deal(score_result: dict, analysis: dict, metadata: dict, deal_id: Optional[str] = None, connection_name: str = ""):
    """Save a scored deal to the log for history tracking."""
    company_name = analysis.get("prospect_company", {}).get("name", "")
    entry = {
        "deal_id": deal_id,
        "deal_name": score_result.get("deal_name_suggestion", "Unknown"),
        "meeting_title": metadata.get("title", "Unknown"),
        "score": score_result["total_score"],
        "recommendation": score_result["recommendation"],
        "framework": score_result.get("framework", "custom"),
        "breakdown": score_result.get("breakdown", {}),
        "key_insight": score_result.get("key_insight", ""),
        "company": company_name,
        "participants": metadata.get("participants", []),
        "connection_name": connection_name,
        "created_at": datetime.now().isoformat(),
    }

    if database.is_available():
        conn = database.get_conn()
        try:
            cur = conn.cursor()
            cur.execute(
                """INSERT INTO scored_deals
                   (deal_id, deal_name, meeting_title, score, recommendation, framework, breakdown, analysis, metadata, key_insight, company_name)
                   VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""",
                (
                    deal_id, entry["deal_name"], entry["meeting_title"],
                    entry["score"], entry["recommendation"], entry["framework"],
                    json.dumps(entry["breakdown"]), json.dumps({"company": entry["company"]}),
                    json.dumps({"participants": entry["participants"], "connection_name": entry.get("connection_name", "")}), entry["key_insight"],
                    company_name,
                ),
            )
            conn.commit()
            cur.close()
            return
        except Exception as e:
            conn.rollback()
            logger.warning(f"Failed to save deal to DB: {e}")
        finally:
            database.put_conn(conn)

    # Fallback to JSON
    data = []
    if DEALS_LOG_FILE.exists():
        data = json.loads(DEALS_LOG_FILE.read_text())
    data.append(entry)
    DEALS_LOG_FILE.write_text(json.dumps(data, indent=2))


def _load_deals_log() -> list:
    if database.is_available():
        conn = database.get_conn()
        try:
            cur = conn.cursor()
            cur.execute(
                """SELECT deal_id, deal_name, meeting_title, score, recommendation,
                          framework, breakdown, key_insight, created_at
                   FROM scored_deals ORDER BY created_at DESC"""
            )
            rows = cur.fetchall()
            cur.close()
            return [
                {
                    "deal_id": r[0], "deal_name": r[1], "meeting_title": r[2],
                    "score": r[3], "recommendation": r[4], "framework": r[5],
                    "breakdown": r[6] if isinstance(r[6], dict) else json.loads(r[6] or "{}"),
                    "key_insight": r[7], "created_at": r[8].isoformat(),
                }
                for r in rows
            ]
        except Exception as e:
            logger.warning(f"Failed to load deals from DB: {e}")
            return []
        finally:
            database.put_conn(conn)

    if DEALS_LOG_FILE.exists():
        return json.loads(DEALS_LOG_FILE.read_text())
    return []


@app.get("/deals", dependencies=[Depends(require_api_key)])
def list_deals():
    """List all scored deals with their breakdown. Used by the Lovable UI for the deal log."""
    return _load_deals_log()


# ── Calibration (historical deal scoring) ─────────────────────────────────────

class CalibrateRequest(BaseModel):
    days_back: int = Field(90, ge=7, le=365, description="How far back to look for closed deals")
    framework: str = Field("bant", description="Framework to score with")
    stages_won: list[str] = Field(default=["closedwon", "Won"], description="Stage names for won deals")
    stages_lost: list[str] = Field(default=["closedlost", "Lost"], description="Stage names for lost deals")


def _normalize_company(name: str) -> str:
    """Normalize a company name for fuzzy matching."""
    if not name:
        return ""
    import re
    name = name.lower().strip()
    # Strip common suffixes
    for suffix in [" inc", " inc.", " corp", " corp.", " llc", " ltd", " ltd.", " co", " co.",
                   " corporation", " company", " group", " holdings", " solutions", " technologies"]:
        if name.endswith(suffix):
            name = name[: -len(suffix)].strip()
    name = re.sub(r"[^a-z0-9 ]", "", name).strip()
    return name


def _match_transcript_to_deal(deal_company: str, transcripts: list) -> Optional[dict]:
    """Find the best matching transcript for a deal based on company name."""
    norm_deal = _normalize_company(deal_company)
    if not norm_deal:
        return None
    for t in transcripts:
        title = (t.get("title", "") or "").lower()
        if norm_deal in title or title in norm_deal:
            return t
        # Check participants
        for p in t.get("participants", []):
            if norm_deal in (p or "").lower():
                return t
    return None


def _run_calibration(req_data: dict, conn_dict: dict):
    """Background task: pull closed deals, match to transcripts, score, produce report."""
    import time as _time
    import attio_client
    import hubspot_client

    framework = req_data["framework"]
    days_back = req_data["days_back"]
    crm_name = conn_dict["crm"]
    crm_key = conn_dict.get("crm_api_key", "")
    ff_key = conn_dict.get("fireflies_api_key", "")
    slack_url = conn_dict.get("slack_webhook_url", "")

    # 1. Pull closed deals from CRM
    won_stages = req_data.get("stages_won", ["closedwon", "Won"])
    lost_stages = req_data.get("stages_lost", ["closedlost", "Lost"])

    if crm_name == "hubspot":
        won_deals = hubspot_client.query_deals_by_stage(won_stages, limit=50, api_key=crm_key)
        lost_deals = hubspot_client.query_deals_by_stage(lost_stages, limit=50, api_key=crm_key)
    else:
        won_deals = attio_client.query_deals_by_stage(won_stages, limit=50, api_key=crm_key)
        lost_deals = attio_client.query_deals_by_stage(lost_stages, limit=50, api_key=crm_key)

    all_deals = won_deals + lost_deals
    logger.info(f"[Calibration] Found {len(won_deals)} won + {len(lost_deals)} lost = {len(all_deals)} total deals")

    if not all_deals:
        if slack_url:
            import requests as req_lib
            req_lib.post(slack_url, json={"text": ":warning: *Fairplay Calibration:* No closed deals found in the last {days_back} days."}, timeout=10)
        return

    # 2. Pull transcripts from available source
    since = datetime.now() - __import__("datetime").timedelta(days=days_back)
    transcripts = []
    transcript_source = conn_dict.get("transcript_source", "fireflies")

    if transcript_source == "zoom" and conn_dict.get("zoom_account_id"):
        # Pull from Zoom cloud recordings (support multiple user emails, comma-separated)
        import zoom_client
        zoom_emails = [e.strip() for e in conn_dict.get("zoom_user_email", "me").split(",") if e.strip()]
        zoom_recordings = []
        seen_ids = set()
        for _zoom_email in zoom_emails:
            recs = zoom_client.list_recordings(
                user_email=_zoom_email,
                since=since,
                account_id=conn_dict.get("zoom_account_id", ""),
                client_id=conn_dict.get("zoom_client_id", ""),
                client_secret=conn_dict.get("zoom_client_secret", ""),
            )
            for rec in recs:
                if rec["id"] not in seen_ids:
                    zoom_recordings.append(rec)
                    seen_ids.add(rec["id"])
        # Convert to common format
        for rec in zoom_recordings:
            if rec.get("has_transcript"):
                transcripts.append({
                    "id": rec["id"],
                    "title": rec["title"],
                    "date": rec["date"],
                    "participants": rec.get("participants", []),
                    "_source": "zoom",
                    "_transcript_url": rec["transcript_url"],
                })
    elif ff_key:
        # Pull from Fireflies
        try:
            ff_transcripts = fireflies_client.list_transcripts(since=since, limit=100, api_key=ff_key)
            for t in ff_transcripts:
                t["_source"] = "fireflies"
            transcripts = ff_transcripts
        except Exception as e:
            logger.error(f"[Calibration] Failed to list transcripts: {e}")

    logger.info(f"[Calibration] Found {len(transcripts)} transcripts in last {days_back} days (source: {transcript_source})")

    # 3. Score ALL transcripts first, then match to deals by extracted company name
    scored_transcripts = []
    for t in transcripts:
        tid = t.get("id")
        try:
            if t.get("_source") == "zoom":
                import zoom_client
                text = zoom_client.download_transcript(
                    t["_transcript_url"],
                    account_id=conn_dict.get("zoom_account_id", ""),
                    client_id=conn_dict.get("zoom_client_id", ""),
                    client_secret=conn_dict.get("zoom_client_secret", ""),
                )
                metadata = {
                    "title": t.get("title", "Zoom Meeting"),
                    "date": t.get("date", ""),
                    "source": "zoom",
                    "participants": t.get("participants", []),
                }
            else:
                transcript = fireflies_client.get_transcript(tid, api_key=ff_key)
                if not transcript:
                    continue
                text = fireflies_client.format_transcript_text(transcript)
                metadata = fireflies_client.get_meeting_metadata(transcript)

            if not text or len(text) < 500:
                continue

            analysis = transcript_analyzer.analyze_transcript(text, metadata, framework=framework)
            if not analysis or not isinstance(analysis, dict):
                continue
            if not analysis.get("is_sales_conversation"):
                logger.info(f"[Calibration] {t.get('title', tid)} is not a sales conversation, skipping")
                continue

            score_result = deal_scorer.score_deal(analysis)
            company_from_analysis = analysis.get("prospect_company", {}).get("name", "")
            participants = [p.get("name", "") for p in analysis.get("participants", []) if p.get("is_prospect")]

            scored_transcripts.append({
                "transcript_id": tid,
                "title": t.get("title", ""),
                "date": t.get("date", ""),
                "company": company_from_analysis,
                "participants": participants,
                "score": score_result["total_score"],
                "recommendation": score_result["recommendation"],
                "breakdown": score_result.get("breakdown", {}),
                "key_insight": score_result.get("key_insight", ""),
            })
            logger.info(f"[Calibration] Scored: {t.get('title', tid)} -> company: {company_from_analysis}, score: {score_result['total_score']}")
            _time.sleep(2)
        except Exception as e:
            logger.warning(f"[Calibration] Failed to score transcript {tid}: {e}")

    logger.info(f"[Calibration] Scored {len(scored_transcripts)} sales transcripts")

    # 4. Match scored transcripts to deals by company name from analysis
    results = []
    matched = 0
    for deal in all_deals:
        deal_company = _normalize_company(deal.get("company_name", ""))
        deal_name_norm = _normalize_company(deal.get("name", ""))
        best_match = None

        for st in scored_transcripts:
            transcript_company = _normalize_company(st.get("company", ""))
            if not transcript_company:
                continue
            # Match if company names overlap
            if (transcript_company and deal_company and
                (transcript_company in deal_company or deal_company in transcript_company or
                 transcript_company in deal_name_norm or deal_name_norm in transcript_company)):
                best_match = st
                break
            # Also check participant names against deal name
            for p in st.get("participants", []):
                p_norm = _normalize_company(p)
                if p_norm and (p_norm in deal_name_norm or deal_name_norm in p_norm):
                    best_match = st
                    break
            if best_match:
                break

        if best_match:
            matched += 1
            results.append({
                "deal": deal,
                "matched": True,
                "transcript_id": best_match["transcript_id"],
                "score": best_match["score"],
                "recommendation": best_match["recommendation"],
                "breakdown": best_match["breakdown"],
            })
            # Save to calibration_results table
            if database.is_available():
                db = database.get_conn()
                if db:
                    try:
                        cur = db.cursor()
                        cur.execute(
                            """INSERT INTO calibration_results
                               (deal_id, deal_name, company_name, crm_stage, transcript_id,
                                fairplay_score, framework, recommendation, breakdown)
                               VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)""",
                            (deal["deal_id"], deal["name"], best_match["company"], deal["stage"],
                             best_match["transcript_id"], best_match["score"], framework,
                             best_match["recommendation"], json.dumps(best_match["breakdown"])),
                        )
                        db.commit()
                        cur.close()
                    except Exception:
                        db.rollback()
                    finally:
                        database.put_conn(db)
        else:
            results.append({"deal": deal, "matched": False, "score": None})

    # 4. Generate report
    scored_won = [r for r in results if r["deal"]["stage"] in won_stages and r.get("score") is not None]
    scored_lost = [r for r in results if r["deal"]["stage"] in lost_stages and r.get("score") is not None]
    avg_won = sum(r["score"] for r in scored_won) / len(scored_won) if scored_won else 0
    avg_lost = sum(r["score"] for r in scored_lost) / len(scored_lost) if scored_lost else 0

    # Accuracy: would Fairplay's recommendation have matched the actual outcome?
    correct = 0
    total_scored = len(scored_won) + len(scored_lost)
    for r in scored_won:
        if r.get("recommendation") in ("auto_create", "needs_review"):
            correct += 1
    for r in scored_lost:
        if r.get("recommendation") == "not_a_deal":
            correct += 1
    accuracy = (correct / total_scored * 100) if total_scored > 0 else 0

    logger.info(f"[Calibration] Complete. {matched}/{len(all_deals)} matched, accuracy: {accuracy:.0f}%")

    # 5. Post to Slack
    if slack_url:
        import requests as req_lib
        report_text = (
            f":bar_chart: *Fairplay Calibration Report*\n"
            f"Framework: {framework.upper()} | Last {days_back} days\n\n"
            f"*Deals analyzed:* {len(all_deals)} ({len(won_deals)} won, {len(lost_deals)} lost)\n"
            f"*Transcripts matched:* {matched}/{len(all_deals)}\n"
            f"*Scored:* {total_scored}\n\n"
            f"*Avg score (won deals):* {avg_won:.0f}/100\n"
            f"*Avg score (lost deals):* {avg_lost:.0f}/100\n"
            f"*Score gap:* {avg_won - avg_lost:.0f} points\n\n"
            f"*Accuracy:* {accuracy:.0f}% (Fairplay recommendation matched actual outcome)\n"
        )
        if scored_won:
            top_won = sorted(scored_won, key=lambda x: x["score"], reverse=True)[:3]
            report_text += "\n*Top scored won deals:*\n"
            for r in top_won:
                report_text += f"  {r['deal']['name']}: {r['score']}/100\n"
        if scored_lost:
            top_lost = sorted(scored_lost, key=lambda x: x["score"], reverse=True)[:3]
            report_text += "\n*Highest scored lost deals (potential false positives):*\n"
            for r in top_lost:
                report_text += f"  {r['deal']['name']}: {r['score']}/100\n"

        # Show unmatched scored transcripts and ask user to identify them
        matched_tids = {r.get("transcript_id") for r in results if r.get("matched")}
        unmatched_scored = [st for st in scored_transcripts if st["transcript_id"] not in matched_tids]
        if unmatched_scored:
            report_text += f"\n:question: *{len(unmatched_scored)} scored call(s) could not be matched to a deal:*\n"
            report_text += "_Reply with the deal name for any of these so we can link them for calibration._\n\n"
            for st in unmatched_scored:
                emoji = ":large_green_circle:" if st["score"] >= 70 else ":large_yellow_circle:" if st["score"] >= 50 else ":red_circle:"
                report_text += (
                    f"{emoji} *{st.get('company') or st.get('title', '?')}* ({st.get('date', '?')[:10]})\n"
                    f"    Score: {st['score']}/100 | _{st.get('key_insight', '')}_\n"
                )

        try:
            req_lib.post(slack_url, json={"text": report_text}, timeout=10)
        except Exception as e:
            logger.warning(f"Calibration Slack notification failed: {e}")


@app.post("/calibrate", dependencies=[Depends(require_api_key)])
def calibrate(req: CalibrateRequest, background_tasks: BackgroundTasks):
    """Run calibration: score historical closed deals and produce accuracy report."""
    conn = _build_default_connection()
    if not conn["fireflies_api_key"]:
        raise HTTPException(status_code=400, detail="FIREFLIES_API_KEY not configured")

    req_data = {
        "days_back": req.days_back,
        "framework": req.framework,
        "stages_won": req.stages_won,
        "stages_lost": req.stages_lost,
    }
    background_tasks.add_task(_run_calibration, req_data, conn)
    return {"status": "calibrating", "message": "Results will appear in Slack and /calibrate/report"}


@app.get("/calibrate/report", dependencies=[Depends(require_api_key)])
def calibration_report():
    """Get the latest calibration results."""
    if not database.is_available():
        return {"results": [], "summary": {}}

    conn = database.get_conn()
    if not conn:
        return {"results": [], "summary": {}}

    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT deal_id, deal_name, company_name, crm_stage, transcript_id,
                   fairplay_score, framework, recommendation, breakdown, created_at
            FROM calibration_results
            ORDER BY created_at DESC
            LIMIT 200
        """)
        rows = cur.fetchall()
        cur.close()

        results = []
        won_scores = []
        lost_scores = []
        for r in rows:
            entry = {
                "deal_id": r[0], "deal_name": r[1], "company_name": r[2],
                "crm_stage": r[3], "transcript_id": r[4], "fairplay_score": r[5],
                "framework": r[6], "recommendation": r[7],
                "breakdown": r[8] if isinstance(r[8], dict) else json.loads(r[8] or "{}"),
                "created_at": r[9].isoformat() if r[9] else "",
            }
            results.append(entry)
            if "won" in (r[3] or "").lower():
                won_scores.append(r[5] or 0)
            elif "lost" in (r[3] or "").lower():
                lost_scores.append(r[5] or 0)

        summary = {
            "total": len(results),
            "won_count": len(won_scores),
            "lost_count": len(lost_scores),
            "avg_won_score": round(sum(won_scores) / len(won_scores)) if won_scores else 0,
            "avg_lost_score": round(sum(lost_scores) / len(lost_scores)) if lost_scores else 0,
            "score_gap": round((sum(won_scores) / len(won_scores)) - (sum(lost_scores) / len(lost_scores))) if won_scores and lost_scores else 0,
        }

        return {"results": results, "summary": summary}
    except Exception as e:
        logger.warning(f"Calibration report failed: {e}")
        return {"results": [], "summary": {}}
    finally:
        database.put_conn(conn)


# ── Transcript polling worker ─────────────────────────────────────────────────

def _poll_all_connections():
    """Poll all transcript sources (Fireflies + Zoom) across all connections."""
    from config import POLLING_INTERVAL_MINUTES
    import time as _time

    logger.info("Polling for new transcripts...")

    # Gather connections to poll
    conns_to_poll = []

    # Check registered connections
    try:
        all_conns = connections.list_connections_full() if hasattr(connections, 'list_connections_full') else []
        for c in all_conns:
            if not c.get("active", True):
                continue
            source = c.get("transcript_source", "fireflies")
            if source == "fireflies" and c.get("fireflies_api_key"):
                conns_to_poll.append(c)
            elif source == "zoom" and c.get("zoom_account_id"):
                conns_to_poll.append(c)
    except Exception as e:
        logger.warning(f"Failed to list connections for polling: {e}")

    # If no registered connections, use default env var connection (Fireflies)
    if not conns_to_poll:
        default = _build_default_connection()
        if default.get("fireflies_api_key"):
            conns_to_poll.append(default)

    if not conns_to_poll:
        logger.info("No pollable connections found, skipping")
        return

    lookback = datetime.now() - __import__("datetime").timedelta(minutes=POLLING_INTERVAL_MINUTES * 2)
    total_processed = 0

    for conn in conns_to_poll:
        conn_name = conn.get("name", "Default")
        source = conn.get("transcript_source", "fireflies")

        try:
            if source == "zoom":
                # Poll Zoom recordings
                import zoom_client
                zoom_emails = [e.strip() for e in conn.get("zoom_user_email", "me").split(",") if e.strip()]
                seen_ids = set()
                for zoom_email in zoom_emails:
                    try:
                        recs = zoom_client.list_recordings(
                            user_email=zoom_email,
                            since=lookback,
                            account_id=conn.get("zoom_account_id", ""),
                            client_id=conn.get("zoom_client_id", ""),
                            client_secret=conn.get("zoom_client_secret", ""),
                        )
                        for rec in recs:
                            rid = rec.get("id", "")
                            if not rid or rid in seen_ids or not rec.get("has_transcript"):
                                continue
                            seen_ids.add(rid)
                            zoom_tid = f"zoom_{rid}"
                            if _is_processed(zoom_tid, conn_name):
                                continue
                            # Download transcript
                            text = zoom_client.download_transcript(
                                rec.get("transcript_url", ""),
                                account_id=conn.get("zoom_account_id", ""),
                                client_id=conn.get("zoom_client_id", ""),
                                client_secret=conn.get("zoom_client_secret", ""),
                            )
                            if len(text) < 500:
                                logger.info(f"[Poller] Zoom transcript too short ({len(text)} chars), skipping: {rec.get('title')}")
                                _mark_processed(zoom_tid, conn_name, status="skipped_short")
                                continue
                            # Score it
                            metadata = {"title": rec.get("title", "Zoom Call"), "date": rec.get("date", ""), "source": "zoom"}
                            logger.info(f"[Poller] Scoring Zoom transcript: {rec.get('title')} ({zoom_tid}) for {conn_name}")
                            try:
                                _process_transcript_text(text, metadata, conn)
                                _mark_processed(zoom_tid, conn_name, status="success")
                                total_processed += 1
                                logger.info(f"[Poller] Successfully scored and marked: {zoom_tid}")
                            except Exception as score_err:
                                logger.error(f"[Poller] Scoring failed for {zoom_tid}: {score_err}")
                                _mark_processed(zoom_tid, conn_name, status="error", error=str(score_err))
                            _time.sleep(2)
                    except Exception as e:
                        logger.error(f"[Poller] Failed polling Zoom user {zoom_email}: {e}")

            else:
                # Poll Fireflies
                transcripts = fireflies_client.list_transcripts(
                    since=lookback, limit=10, api_key=conn["fireflies_api_key"]
                )
                for t in transcripts:
                    tid = t.get("id")
                    if not tid or _is_processed(tid, conn_name):
                        continue
                    logger.info(f"[Poller] Processing Fireflies transcript: {t.get('title', tid)} for {conn_name}")
                    _process_fireflies_transcript(tid, conn)
                    total_processed += 1
                    _time.sleep(2)

        except Exception as e:
            err_str = str(e).lower()
            # Suppress Slack alerts for transient third-party API errors
            transient_keywords = [
                "internal_server_error", "500", "overloaded", "429",
                "request_timeout", "408", "timed out", "timeout",
                "connection reset", "connection refused", "service unavailable", "503",
                "bad gateway", "502", "gateway timeout", "504",
                "520", "521", "522", "523", "524", "525", "526", "527",  # Cloudflare errors
                "ssl error", "ssl: ", "remote disconnected", "max retries exceeded",
            ]
            if any(kw in err_str for kw in transient_keywords):
                logger.warning(f"[Poller] Transient API error for {conn_name}, will retry next cycle: {e}")
            else:
                logger.error(f"[Poller] Failed polling for {conn_name}: {e}")
                _send_error_alert(e, f"Polling for connection {conn_name}", conn_name)

    # Retry stuck transcripts (retrying status, older than lookback window)
    # Only retry once per cycle, and permanently mark as error if retry fails
    if database.is_available():
        db_conn = database.get_conn()
        if db_conn:
            try:
                cur = db_conn.cursor()
                cur.execute(
                    "SELECT transcript_id, connection_name FROM processed_transcripts "
                    "WHERE status IN ('retrying', 'credits_exhausted') ORDER BY processed_at ASC LIMIT 3"
                )
                retries = cur.fetchall()
                cur.close()
                if retries:
                    logger.info(f"[Poller] Found {len(retries)} stuck transcripts to retry")
                for tid, cname in retries:
                    retry_conn = None
                    for c in conns_to_poll:
                        if c.get("name", "Default") == cname or cname == "Default":
                            retry_conn = c
                            break
                    if not retry_conn:
                        retry_conn = _build_default_connection()

                    if tid.startswith("zoom_"):
                        logger.info(f"[Poller] Marking Zoom retry as error: {tid}")
                        _mark_processed(tid, cname, status="error", error="Zoom retry not supported")
                        continue

                    logger.info(f"[Poller] Retrying stuck Fireflies transcript: {tid}")
                    try:
                        _process_fireflies_transcript(tid, retry_conn)
                        total_processed += 1
                    except Exception as e:
                        # Permanently mark as error on retry failure, don't loop
                        logger.error(f"[Poller] Retry failed permanently for {tid}: {e}")
                        _mark_processed(tid, cname, status="error", error=f"Retry failed: {str(e)[:300]}")
                    _time.sleep(2)
            except Exception as e:
                logger.warning(f"[Poller] Failed to check retrying transcripts: {e}")
            finally:
                database.put_conn(db_conn)

    logger.info(f"Polling complete. Processed {total_processed} new transcript(s).")


# Start polling scheduler on app startup
from config import POLLING_ENABLED, POLLING_INTERVAL_MINUTES as _POLL_MINS

if POLLING_ENABLED:
    try:
        from apscheduler.schedulers.background import BackgroundScheduler
        _scheduler = BackgroundScheduler()
        _scheduler.add_job(
            _poll_all_connections,
            "interval",
            minutes=_POLL_MINS,
            id="transcript_poller",
            max_instances=1,
        )

        def _daily_health_checks():
            """Run org health alerts and rapid close detection for all connections."""
            logger.info("Running daily health checks...")
            all_conns = connections.list_connections_full()
            for conn in all_conns:
                try:
                    _check_org_health(conn)
                    _check_rapid_closes(conn)
                except Exception as e:
                    logger.warning(f"Health check failed for {conn.get('name', '?')}: {e}")
            logger.info(f"Daily health checks complete for {len(all_conns)} connection(s)")

        _scheduler.add_job(
            _daily_health_checks,
            "interval",
            hours=24,
            id="daily_health_checks",
            max_instances=1,
        )

        @app.on_event("startup")
        def _start_poller():
            _scheduler.start()
            logger.info(f"Transcript poller started (every {_POLL_MINS} min)")

        @app.on_event("shutdown")
        def _stop_poller():
            if _scheduler.running:
                _scheduler.shutdown(wait=False)
    except ImportError:
        logger.warning("apscheduler not installed, polling disabled")


@app.post("/poll-now", dependencies=[Depends(require_api_key)])
def poll_now():
    """Manually trigger a polling cycle. For debugging."""
    _poll_all_connections()
    return {"status": "poll complete"}


@app.get("/debug/processed")
def debug_processed():
    """Show processed transcripts table for debugging dedup issues."""
    if database.is_available():
        conn = database.get_conn()
        if conn:
            try:
                cur = conn.cursor()
                # Fix any "processed" status records to "success" so dedup catches them
                cur.execute("UPDATE processed_transcripts SET status = 'success' WHERE status = 'processed'")
                conn.commit()
                cur.execute("SELECT transcript_id, connection_name, status, score, processed_at FROM processed_transcripts ORDER BY processed_at DESC LIMIT 30")
                rows = cur.fetchall()
                cur.close()
                return {"count": len(rows), "records": [
                    {"transcript_id": r[0], "connection_name": r[1], "status": r[2], "score": r[3], "processed_at": str(r[4])}
                    for r in rows
                ]}
            except Exception as e:
                return {"error": str(e)}
            finally:
                database.put_conn(conn)
    return {"error": "database not available"}


@app.get("/debug/hubspot-identify-app")
def debug_hubspot_identify_app(app_id: str, connection_name: str = "Ascent CFO"):
    """Look up the name of a HubSpot integration/app by its ID."""
    import requests as req_lib
    all_conns = connections.list_connections_full()
    conn = None
    for c in all_conns:
        if c.get("name") == connection_name:
            conn = c
            break
    if not conn:
        return {"error": f"Connection '{connection_name}' not found"}

    hs_key = conn.get("crm_api_key", "")
    if not hs_key:
        return {"error": "No HubSpot API key on this connection"}

    headers = {"Authorization": f"Bearer {hs_key}"}

    # Try the OAuth apps endpoint
    results = {}
    try:
        resp = req_lib.get(
            f"https://api.hubapi.com/oauth/v1/access-tokens/{hs_key}",
            headers=headers,
            timeout=15,
        )
        if resp.ok:
            results["token_info"] = resp.json()
    except Exception as e:
        results["token_err"] = str(e)

    # Get info on this specific app via the integrations endpoint
    try:
        resp = req_lib.get(
            f"https://api.hubapi.com/integrations/v1/me",
            headers=headers,
            timeout=15,
        )
        if resp.ok:
            results["integration_me"] = resp.json()
    except Exception as e:
        results["me_err"] = str(e)

    # Try to get public app info by ID
    try:
        resp = req_lib.get(
            f"https://api.hubapi.com/integrations/v1/{app_id}",
            headers=headers,
            timeout=15,
        )
        results["app_lookup_status"] = resp.status_code
        if resp.ok:
            results["app_info"] = resp.json()
        else:
            results["app_lookup_body"] = resp.text[:500]
    except Exception as e:
        results["app_err"] = str(e)

    return results


@app.get("/debug/hubspot-deal-history")
def debug_hubspot_deal_history(deal_name: str = "", deal_id: str = "", connection_name: str = "Ascent CFO"):
    """Look up a HubSpot deal and its stage history to trace who/what moved it."""
    import requests as req_lib
    all_conns = connections.list_connections_full()
    conn = None
    for c in all_conns:
        if c.get("name") == connection_name:
            conn = c
            break
    if not conn:
        return {"error": f"Connection '{connection_name}' not found"}

    hs_key = conn.get("crm_api_key", "")
    if not hs_key:
        return {"error": "No HubSpot API key on this connection"}

    headers = {"Authorization": f"Bearer {hs_key}", "Content-Type": "application/json"}

    # Step 1: find the deal by name or ID
    deal = None
    if deal_id:
        resp = req_lib.get(
            f"https://api.hubapi.com/crm/v3/objects/deals/{deal_id}",
            headers=headers,
            params={"properties": "dealname,dealstage,pipeline,hs_lastmodifieddate,closedate,amount,hs_deal_stage_probability"},
            timeout=15,
        )
        if resp.ok:
            deal = resp.json()
    elif deal_name:
        # Search by name
        resp = req_lib.post(
            "https://api.hubapi.com/crm/v3/objects/deals/search",
            headers=headers,
            json={
                "filterGroups": [{
                    "filters": [{"propertyName": "dealname", "operator": "CONTAINS_TOKEN", "value": deal_name}]
                }],
                "properties": ["dealname", "dealstage", "pipeline", "hs_lastmodifieddate", "closedate", "amount"],
                "limit": 5,
            },
            timeout=15,
        )
        if resp.ok:
            results = resp.json().get("results", [])
            if results:
                deal = results[0]

    if not deal:
        return {"error": "Deal not found"}

    deal_hs_id = deal.get("id")

    # Step 2: get stage history via property history endpoint
    history_resp = req_lib.get(
        f"https://api.hubapi.com/crm/v3/objects/deals/{deal_hs_id}",
        headers=headers,
        params={
            "propertiesWithHistory": "dealstage,dealname,closedate,amount",
            "properties": "dealname,dealstage,pipeline,closedate",
        },
        timeout=15,
    )
    history_data = history_resp.json() if history_resp.ok else {}
    stage_history = history_data.get("propertiesWithHistory", {}).get("dealstage", [])

    # Step 3: recent engagements/activities on the deal
    activities_resp = req_lib.get(
        f"https://api.hubapi.com/crm/v4/objects/deals/{deal_hs_id}/associations/notes",
        headers=headers,
        params={"limit": 10},
        timeout=15,
    )
    notes_associations = activities_resp.json().get("results", []) if activities_resp.ok else []

    return {
        "deal": {
            "id": deal_hs_id,
            "name": deal.get("properties", {}).get("dealname"),
            "stage": deal.get("properties", {}).get("dealstage"),
            "last_modified": deal.get("properties", {}).get("hs_lastmodifieddate"),
            "pipeline": deal.get("properties", {}).get("pipeline"),
        },
        "stage_history": [
            {
                "value": h.get("value"),
                "timestamp": h.get("timestamp"),
                "source_type": h.get("sourceType"),
                "source_id": h.get("sourceId"),
                "updated_by_user_id": h.get("updatedByUserId"),
            }
            for h in stage_history
        ],
        "note_associations": len(notes_associations),
    }


@app.post("/debug/resend-notification")
def debug_resend_notification(meeting_title: str, connection_name: str = "Ascent CFO"):
    """Re-send the Slack/Teams notification for a previously scored deal by meeting title."""
    if not database.is_available():
        return {"error": "Database not available"}

    # Find the connection
    all_conns = connections.list_connections_full()
    conn = None
    for c in all_conns:
        if c.get("name") == connection_name:
            conn = c
            break
    if not conn:
        return {"error": f"Connection '{connection_name}' not found"}

    db = database.get_conn()
    try:
        cur = db.cursor()
        cur.execute(
            """SELECT deal_id, deal_name, meeting_title, score, recommendation, framework,
                      breakdown, analysis, metadata, key_insight, company_name, created_at
               FROM scored_deals
               WHERE meeting_title ILIKE %s
               ORDER BY created_at DESC LIMIT 1""",
            (f"%{meeting_title}%",),
        )
        row = cur.fetchone()
        cur.close()
        if not row:
            return {"error": f"No scored deal found matching '{meeting_title}'"}

        # Rebuild score_result and analysis from stored data
        (deal_id, deal_name, title, score, recommendation, framework,
         breakdown, analysis_blob, metadata_blob, key_insight, company_name, created_at) = row

        score_result = {
            "total_score": score,
            "recommendation": recommendation,
            "framework": framework,
            "breakdown": breakdown if isinstance(breakdown, dict) else {},
            "key_insight": key_insight,
            "deal_name_suggestion": deal_name,
        }
        # Reconstruct minimal analysis for notification (meeting_type etc.)
        analysis = analysis_blob if isinstance(analysis_blob, dict) else {}
        if "prospect_company" not in analysis:
            analysis["prospect_company"] = {"name": company_name}
        metadata = metadata_blob if isinstance(metadata_blob, dict) else {}
        metadata["title"] = title

        # Get previous scores for cumulative
        previous_scores = _get_previous_scores(company_name) if company_name else []
        # Drop the current scoring from previous (it's already in the list)
        previous_scores = [p for p in previous_scores if p.get("meeting_title") != title]

        # Send via unified notification
        is_shadow = conn.get("shadow_mode", False)
        _send_notification(
            conn, score_result, analysis, metadata,
            deal_id=deal_id, existing_deal=None, previous_scores=previous_scores,
            shadow_mode=is_shadow,
        )

        return {
            "status": "sent",
            "meeting_title": title,
            "score": score,
            "deal_id": deal_id or deal_name,
            "slack": bool(conn.get("slack_webhook_url")),
            "teams": bool(conn.get("teams_webhook_url")),
        }
    except Exception as e:
        return {"error": str(e)}
    finally:
        database.put_conn(db)


@app.get("/debug/attio-diagnostic")
def debug_attio_diagnostic(connection_name: str = "My Team", company_name: str = "Test Co"):
    """Diagnose Attio integration: auth check, find_deal test, full create_deal dry run."""
    import attio_client
    all_conns = connections.list_connections_full()
    conn = None
    for c in all_conns:
        if c.get("name") == connection_name:
            conn = c
            break
    if not conn:
        return {"error": f"Connection '{connection_name}' not found"}

    crm_key = conn.get("crm_api_key", "")
    if not crm_key:
        return {"error": "No Attio API key on connection"}

    results = {"connection": connection_name, "tests": {}}

    # 1. Auth check via /self
    try:
        import requests as req_lib
        from config import ATTIO_BASE_URL
        resp = req_lib.get(
            f"{ATTIO_BASE_URL}/self",
            headers={"Authorization": f"Bearer {crm_key}"},
            timeout=15,
        )
        results["tests"]["auth"] = {
            "status_code": resp.status_code,
            "ok": resp.ok,
            "body": resp.json() if resp.ok else resp.text[:300],
        }
    except Exception as e:
        results["tests"]["auth"] = {"error": str(e)}

    # 2. Find deal by company name (exact path used in pipeline)
    try:
        existing = attio_client.find_deal_by_company(company_name, api_key=crm_key)
        results["tests"]["find_deal_by_company"] = {
            "company_name": company_name,
            "existing_deal_returned": existing,
        }
    except Exception as e:
        results["tests"]["find_deal_by_company"] = {"error": str(e), "type": type(e).__name__}

    # 3. Query deals object directly to see what's in Attio
    try:
        import requests as req_lib
        from config import ATTIO_BASE_URL
        resp = req_lib.post(
            f"{ATTIO_BASE_URL}/objects/deals/records/query",
            headers={"Authorization": f"Bearer {crm_key}", "Content-Type": "application/json"},
            json={"filter": {"name": {"$contains": company_name}}, "limit": 3},
            timeout=15,
        )
        results["tests"]["deals_query"] = {
            "status_code": resp.status_code,
            "ok": resp.ok,
            "result_count": len(resp.json().get("data", [])) if resp.ok else None,
            "error_body": resp.text[:300] if not resp.ok else None,
        }
    except Exception as e:
        results["tests"]["deals_query"] = {"error": str(e), "type": type(e).__name__}

    # 3b. Direct create_deal test with minimal payload
    try:
        import requests as req_lib
        from config import ATTIO_BASE_URL, ATTIO_DEAL_STAGE_QUALIFIED
        # Try minimal create — just name and stage, no Fairplay fields
        test_payload = {
            "data": {
                "values": {
                    "name": [{"value": f"FAIRPLAY-DIAGNOSTIC-{datetime.now().strftime('%Y%m%d%H%M%S')}"}],
                    "stage": [{"status": ATTIO_DEAL_STAGE_QUALIFIED}],
                }
            }
        }
        resp = req_lib.post(
            f"{ATTIO_BASE_URL}/objects/deals/records",
            headers={"Authorization": f"Bearer {crm_key}", "Content-Type": "application/json"},
            json=test_payload,
            timeout=15,
        )
        results["tests"]["create_minimal"] = {
            "status_code": resp.status_code,
            "ok": resp.ok,
            "body": resp.json() if resp.ok else resp.text[:600],
        }
    except Exception as e:
        results["tests"]["create_minimal"] = {"error": str(e), "type": type(e).__name__}

    # 3c. Try create with Fairplay custom fields to see if those are the problem
    try:
        import requests as req_lib
        from config import (
            ATTIO_BASE_URL, ATTIO_DEAL_STAGE_QUALIFIED,
            ATTIO_FIELD_FAIRPLAY_SCORE, ATTIO_FIELD_FRAMEWORK,
        )
        test_payload = {
            "data": {
                "values": {
                    "name": [{"value": f"FAIRPLAY-FIELD-TEST-{datetime.now().strftime('%H%M%S')}"}],
                    "stage": [{"status": ATTIO_DEAL_STAGE_QUALIFIED}],
                    ATTIO_FIELD_FAIRPLAY_SCORE: [{"value": 75}],
                    ATTIO_FIELD_FRAMEWORK: [{"value": "BANT"}],
                }
            }
        }
        resp = req_lib.post(
            f"{ATTIO_BASE_URL}/objects/deals/records",
            headers={"Authorization": f"Bearer {crm_key}", "Content-Type": "application/json"},
            json=test_payload,
            timeout=15,
        )
        results["tests"]["create_with_fairplay_fields"] = {
            "status_code": resp.status_code,
            "ok": resp.ok,
            "fairplay_score_slug": ATTIO_FIELD_FAIRPLAY_SCORE,
            "framework_slug": ATTIO_FIELD_FRAMEWORK,
            "body": resp.json() if resp.ok else resp.text[:600],
        }
    except Exception as e:
        results["tests"]["create_with_fairplay_fields"] = {"error": str(e), "type": type(e).__name__}

    # 4. Recently scored deals on this connection - check deal_id presence
    if database.is_available():
        db = database.get_conn()
        if db:
            try:
                cur = db.cursor()
                cur.execute("""
                    SELECT deal_name, deal_id, score, recommendation, created_at
                    FROM scored_deals
                    WHERE metadata->>'connection_name' = %s
                      AND recommendation = 'auto_create'
                    ORDER BY created_at DESC LIMIT 5
                """, (connection_name,))
                rows = cur.fetchall()
                cur.close()
                results["tests"]["recent_auto_create_deals"] = [
                    {
                        "deal_name": r[0],
                        "deal_id_present": bool(r[1]),
                        "deal_id": r[1],
                        "score": r[2],
                        "created": str(r[4]),
                    }
                    for r in rows
                ]
            except Exception as e:
                results["tests"]["recent_auto_create_deals"] = {"error": str(e)}
            finally:
                database.put_conn(db)

    return results


@app.get("/debug/connection-fingerprints")
def debug_connection_fingerprints(connection_name: str = "My Team"):
    """Compare connections with the same name by API key fingerprints to identify true duplicates."""
    import hashlib
    all_conns = connections.list_connections_full()
    matches = [c for c in all_conns if c.get("name") == connection_name]
    if not matches:
        return {"error": f"No connections found for '{connection_name}'"}

    def fp(s: str) -> str:
        if not s:
            return ""
        return hashlib.sha256(s.encode()).hexdigest()[:8]

    return {
        "connection_name": connection_name,
        "count": len(matches),
        "connections": [
            {
                "webhook_id": c["webhook_id"],
                "transcript_source": c.get("transcript_source"),
                "crm": c.get("crm"),
                "framework": c.get("framework"),
                "shadow_mode": c.get("shadow_mode"),
                "fireflies_key_fp": fp(c.get("fireflies_api_key", "")),
                "crm_key_fp": fp(c.get("crm_api_key", "")),
                "slack_webhook_fp": fp(c.get("slack_webhook_url", "")),
                "has_calibration_notes": bool(c.get("calibration_notes", "")),
                "calibration_chars": len(c.get("calibration_notes", "") or ""),
                "company_icp_set": bool(c.get("company_icp", "")),
                "framework_weights_set": bool(c.get("framework_weights", "")),
            }
            for c in matches
        ],
    }


@app.delete("/debug/connection/{webhook_id}")
def debug_delete_connection(webhook_id: str, confirm: str = ""):
    """Delete a connection by webhook_id. Requires confirm=YES to actually delete."""
    if confirm != "YES":
        return {"error": "Pass confirm=YES to actually delete"}
    deleted = connections.delete_connection(webhook_id)
    return {"deleted": deleted, "webhook_id": webhook_id}


@app.get("/debug/calibration-notes")
def debug_calibration_notes(connection_name: str = "My Team", webhook_id: str = ""):
    """Show calibration notes accumulated on a connection. Use webhook_id to target a specific connection."""
    all_conns = connections.list_connections_full()
    matches = []
    for c in all_conns:
        if webhook_id:
            if c.get("webhook_id") == webhook_id:
                matches = [c]
                break
        elif c.get("name") == connection_name:
            matches.append(c)
    if not matches:
        return {"error": f"Connection not found"}
    if len(matches) > 1:
        return {
            "warning": f"{len(matches)} connections match name '{connection_name}'. Pass webhook_id to disambiguate.",
            "connections": [
                {"webhook_id": c["webhook_id"], "char_count": len(c.get("calibration_notes", "") or "")}
                for c in matches
            ],
        }
    conn = matches[0]
    notes = conn.get("calibration_notes", "") or ""
    return {
        "connection": conn.get("name"),
        "webhook_id": conn["webhook_id"],
        "char_count": len(notes),
        "note_count": len([l for l in notes.split("\n") if l.strip()]),
        "notes": notes,
    }


@app.post("/debug/add-calibration-note")
def debug_add_calibration_note(connection_name: str = "My Team", note: str = "", webhook_id: str = ""):
    """Manually add a calibration note for testing prompt injection."""
    if not note:
        return {"error": "note query param required"}
    all_conns = connections.list_connections_full()
    conn = None
    if webhook_id:
        for c in all_conns:
            if c.get("webhook_id") == webhook_id:
                conn = c
                break
    else:
        for c in all_conns:
            if c.get("name") == connection_name:
                conn = c
                break
    if not conn:
        return {"error": f"Connection not found"}
    existing = conn.get("calibration_notes", "") or ""
    timestamp = datetime.now().strftime("%Y-%m-%d")
    new_entry = f"[{timestamp}] {note}"
    updated = (existing + "\n" + new_entry).strip() if existing else new_entry

    # Direct DB write so we can surface SQL errors instead of swallowing them
    if not database.is_available():
        return {"error": "Database not available"}
    db = database.get_conn()
    if not db:
        return {"error": "Database connection failed"}
    try:
        cur = db.cursor()
        # First check if column exists
        cur.execute(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_name='connections' AND column_name='calibration_notes'"
        )
        col_exists = cur.fetchone() is not None
        if not col_exists:
            cur.close()
            return {"error": "calibration_notes column does not exist in DB", "migration_needed": True}

        cur.execute(
            "UPDATE connections SET calibration_notes = %s WHERE webhook_id = %s",
            (updated, conn["webhook_id"]),
        )
        rowcount = cur.rowcount
        db.commit()
        cur.close()
        return {"status": "added", "total_chars": len(updated), "rows_updated": rowcount}
    except Exception as e:
        db.rollback()
        return {"error": str(e), "type": type(e).__name__}
    finally:
        database.put_conn(db)


@app.get("/debug/zoom-recent")
def debug_zoom_recent(connection_name: str = "Ascent CFO", days: int = 2):
    """List recent Zoom recordings for a connection to find specific calls."""
    import zoom_client
    from datetime import timedelta

    all_conns = connections.list_connections_full()
    conn = None
    for c in all_conns:
        if c.get("name") == connection_name:
            conn = c
            break
    if not conn:
        return {"error": f"Connection '{connection_name}' not found"}

    zoom_email = conn.get("zoom_user_email", "")
    if not zoom_email:
        return {"error": "No Zoom user email on this connection"}

    since_dt = datetime.now() - timedelta(days=days)
    try:
        recordings = zoom_client.list_recordings(
            user_email=zoom_email,
            since=since_dt,
            account_id=conn.get("zoom_account_id", ""),
            client_id=conn.get("zoom_client_id", ""),
            client_secret=conn.get("zoom_client_secret", ""),
        )
        return {
            "count": len(recordings),
            "recordings": [
                {
                    "id": r["id"],
                    "title": r["title"],
                    "date": r["date"],
                    "duration": r["duration"],
                    "has_transcript": r["has_transcript"],
                }
                for r in recordings
            ],
        }
    except Exception as e:
        return {"error": str(e)}


@app.get("/debug/zoom-transcript/{recording_id}")
def debug_zoom_transcript(recording_id: str, connection_name: str = "Ascent CFO"):
    """Download and return the transcript text for a specific Zoom recording."""
    import zoom_client
    from datetime import timedelta

    all_conns = connections.list_connections_full()
    conn = None
    for c in all_conns:
        if c.get("name") == connection_name:
            conn = c
            break
    if not conn:
        return {"error": f"Connection '{connection_name}' not found"}

    # Find the recording
    since_dt = datetime.now() - timedelta(days=7)
    recordings = zoom_client.list_recordings(
        user_email=conn.get("zoom_user_email", ""),
        since=since_dt,
        account_id=conn.get("zoom_account_id", ""),
        client_id=conn.get("zoom_client_id", ""),
        client_secret=conn.get("zoom_client_secret", ""),
    )
    target = None
    for r in recordings:
        if r["id"] == recording_id:
            target = r
            break
    if not target:
        return {"error": f"Recording {recording_id} not found"}

    if not target.get("transcript_url"):
        return {"error": "No transcript available for this recording", "title": target["title"]}

    text = zoom_client.download_transcript(
        target["transcript_url"],
        account_id=conn.get("zoom_account_id", ""),
        client_id=conn.get("zoom_client_id", ""),
        client_secret=conn.get("zoom_client_secret", ""),
    )
    return {
        "title": target["title"],
        "date": target["date"],
        "duration": target["duration"],
        "transcript": text,
    }


@app.get("/debug/fireflies-recent")
def debug_fireflies_recent(connection_name: str = "My Team", days: int = 3):
    """List recent Fireflies transcripts with titles to help find specific calls."""
    all_conns = connections.list_connections_full()
    conn = None
    for c in all_conns:
        if c.get("name") == connection_name:
            conn = c
            break
    if not conn:
        return {"error": f"Connection '{connection_name}' not found"}

    ff_key = conn.get("fireflies_api_key", "")
    if not ff_key:
        return {"error": "No Fireflies API key on this connection"}

    try:
        from datetime import timedelta
        since_dt = datetime.now() - timedelta(days=days)
        transcripts = fireflies_client.list_transcripts(since=since_dt, limit=50, api_key=ff_key)
        return {
            "count": len(transcripts),
            "transcripts": [
                {
                    "id": t.get("id"),
                    "title": t.get("title", ""),
                    "date": t.get("date", ""),
                    "duration": t.get("duration", 0),
                }
                for t in transcripts
            ],
        }
    except Exception as e:
        return {"error": str(e)}


@app.post("/debug/force-process/{transcript_id}")
def debug_force_process(transcript_id: str, connection_name: str = "My Team", background_tasks: BackgroundTasks = None):
    """Force process a specific transcript by ID, bypassing dedup."""
    all_conns = connections.list_connections_full()
    conn = None
    for c in all_conns:
        if c.get("name") == connection_name:
            conn = c
            break
    if not conn:
        return {"error": f"Connection '{connection_name}' not found"}

    # Clear any existing processed_transcripts row for this transcript+connection
    if database.is_available():
        db_conn = database.get_conn()
        if db_conn:
            try:
                cur = db_conn.cursor()
                cur.execute(
                    "DELETE FROM processed_transcripts WHERE transcript_id = %s",
                    (transcript_id,),
                )
                db_conn.commit()
                cur.close()
            except Exception:
                db_conn.rollback()
            finally:
                database.put_conn(db_conn)

    # Process in background
    if background_tasks:
        background_tasks.add_task(_process_fireflies_transcript, transcript_id, conn)
        return {"status": "processing", "transcript_id": transcript_id, "connection": connection_name}
    else:
        _process_fireflies_transcript(transcript_id, conn)
        return {"status": "processed", "transcript_id": transcript_id}


@app.post("/debug/clear-old-retries")
def debug_clear_old_retries():
    """Mark old retrying transcripts as error so the poller focuses on recent ones."""
    if database.is_available():
        conn = database.get_conn()
        if conn:
            try:
                cur = conn.cursor()
                # Keep transcripts from today, mark everything older as error
                cur.execute(
                    "UPDATE processed_transcripts SET status = 'error', error_message = 'Cleared: old retry from credit outage' "
                    "WHERE status IN ('retrying', 'credits_exhausted') "
                    "AND processed_at < NOW() - INTERVAL '6 hours'"
                )
                cleared = cur.rowcount
                conn.commit()
                cur.close()
                return {"cleared": cleared}
            except Exception as e:
                conn.rollback()
                return {"error": str(e)}
            finally:
                database.put_conn(conn)
    return {"error": "database not available"}


# ── Batch scoring ────────────────────────────────────────────────────────────

class BatchScoreRequest(BaseModel):
    transcript_ids: Optional[list[str]] = None
    count: int = Field(10, ge=1, le=50, description="Number of recent transcripts to score (if no IDs provided)")


@app.post("/batch-score", dependencies=[Depends(require_api_key)])
def batch_score(req: BatchScoreRequest, background_tasks: BackgroundTasks):
    """
    Score multiple transcripts in batch. For warm start / retroactive scoring.
    Processes in background, results appear in deal log and Slack.
    """
    conn = _build_default_connection()
    if not conn["fireflies_api_key"]:
        raise HTTPException(status_code=400, detail="FIREFLIES_API_KEY not configured")

    if req.transcript_ids:
        ids_to_process = req.transcript_ids
    else:
        try:
            transcripts = fireflies_client.list_transcripts(limit=req.count, api_key=conn["fireflies_api_key"])
            ids_to_process = [t["id"] for t in transcripts]
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Failed to list transcripts: {e}")

    # Filter out already processed
    conn_name = conn.get("name", "Default")
    new_ids = [tid for tid in ids_to_process if not _is_processed(tid, conn_name)]
    skipped = len(ids_to_process) - len(new_ids)

    def _batch_worker(transcript_ids, connection):
        import time as _time
        for tid in transcript_ids:
            try:
                _process_fireflies_transcript(tid, connection)
            except Exception as e:
                logger.error(f"[Batch] Failed processing {tid}: {e}")
            _time.sleep(3)  # Rate limit protection

    if new_ids:
        background_tasks.add_task(_batch_worker, new_ids, conn)

    return {
        "status": "processing",
        "queued": len(new_ids),
        "skipped_duplicates": skipped,
        "transcript_ids": new_ids,
    }


# ── Auth: Magic Link ──────────────────────────────────────────────────────────

import secrets
import uuid


class MagicLinkRequest(BaseModel):
    email: str


def _get_user_from_session(request: Request) -> Optional[dict]:
    """Extract user from session cookie or Authorization header. Returns None if not authenticated."""
    # Check Authorization header first (cross-domain / Lovable support)
    auth_header = request.headers.get("authorization", "")
    if auth_header.startswith("Bearer "):
        token = auth_header[7:]
    else:
        # Fall back to cookie (same-domain)
        token = request.cookies.get("fp_session")
    if not token or not database.is_available():
        return None
    conn = database.get_conn()
    if not conn:
        return None
    try:
        cur = conn.cursor()
        cur.execute(
            """SELECT u.id, u.email, u.name FROM sessions s
               JOIN users u ON s.user_id = u.id
               WHERE s.token = %s AND s.type = 'session' AND s.expires_at > NOW()""",
            (token,),
        )
        row = cur.fetchone()
        cur.close()
        if row:
            return {"id": row[0], "email": row[1], "name": row[2]}
        return None
    except Exception:
        return None
    finally:
        database.put_conn(conn)


def require_user(request: Request) -> dict:
    """Dependency that requires an authenticated user session."""
    user = _get_user_from_session(request)
    if not user:
        raise HTTPException(status_code=401, detail="Not authenticated")
    return user


@app.post("/auth/magic-link")
def send_magic_link(req: MagicLinkRequest):
    """Send a magic login link to the user's email."""
    from config import APP_URL, RESEND_API_KEY, MAGIC_LINK_EXPIRY_MINUTES
    import requests as req_lib

    email = req.email.strip().lower()
    if not email or "@" not in email:
        raise HTTPException(status_code=400, detail="Invalid email")

    # Read allowlist fresh each time (Railway env vars may not be available at module load)
    allowed_raw = os.getenv("ALLOWED_EMAILS", "")
    allowed = [e.strip().lower() for e in allowed_raw.split(",") if e.strip()]
    if allowed and email not in allowed:
        raise HTTPException(status_code=403, detail="This email is not authorized. Contact your admin for access.")

    if not database.is_available():
        raise HTTPException(status_code=500, detail="Database not available")

    conn = database.get_conn()
    try:
        cur = conn.cursor()
        # Create user if not exists
        cur.execute("SELECT id FROM users WHERE email = %s", (email,))
        row = cur.fetchone()
        if row:
            user_id = row[0]
        else:
            user_id = str(uuid.uuid4())
            cur.execute(
                "INSERT INTO users (id, email) VALUES (%s, %s)",
                (user_id, email),
            )

        # Create magic link token
        token = secrets.token_urlsafe(32)
        cur.execute(
            """INSERT INTO sessions (token, user_id, type, expires_at)
               VALUES (%s, %s, 'magic_link', NOW() + INTERVAL '%s minutes')""",
            (token, user_id, MAGIC_LINK_EXPIRY_MINUTES),
        )
        conn.commit()
        cur.close()
    except Exception as e:
        conn.rollback()
        raise HTTPException(status_code=500, detail=f"Failed to create login link: {e}")
    finally:
        database.put_conn(conn)

    link = f"{APP_URL}/auth/verify?token={token}"

    # Send email via Resend if configured
    if RESEND_API_KEY:
        try:
            req_lib.post(
                "https://api.resend.com/emails",
                headers={"Authorization": f"Bearer {RESEND_API_KEY}"},
                json={
                    "from": "Fairplay <noreply@nicl.ai>",
                    "to": [email],
                    "subject": "Your Fairplay login link",
                    "html": f"""
                        <div style="font-family: -apple-system, sans-serif; max-width: 480px; margin: 0 auto; padding: 40px 20px;">
                            <h2 style="font-weight: 700; margin-bottom: 16px;">Sign in to Fairplay</h2>
                            <p style="color: #666; margin-bottom: 24px;">Click the button below to sign in. This link expires in {MAGIC_LINK_EXPIRY_MINUTES} minutes.</p>
                            <a href="{link}" style="display: inline-block; background: #0a0a0a; color: white; padding: 12px 32px; border-radius: 8px; text-decoration: none; font-weight: 600;">Sign In</a>
                            <p style="color: #999; font-size: 13px; margin-top: 32px;">If you didn't request this, you can safely ignore this email.</p>
                        </div>
                    """,
                },
                timeout=10,
            )
        except Exception as e:
            logger.warning(f"Failed to send magic link email: {e}")

    # Always return success (don't reveal if email exists) + include link for dev mode
    result = {"status": "sent", "message": "Check your email for the login link"}
    if not RESEND_API_KEY:
        result["dev_link"] = link  # Show link in dev mode when email isn't configured
    return result


@app.get("/auth/verify")
def verify_magic_link(token: str, response: Response):
    """Verify magic link token, create session, redirect to app."""
    from config import SESSION_EXPIRY_HOURS

    if not database.is_available():
        raise HTTPException(status_code=500, detail="Database not available")

    conn = database.get_conn()
    try:
        cur = conn.cursor()
        # Find and validate magic link
        cur.execute(
            """SELECT user_id FROM sessions
               WHERE token = %s AND type = 'magic_link' AND expires_at > NOW()""",
            (token,),
        )
        row = cur.fetchone()
        if not row:
            raise HTTPException(status_code=400, detail="Invalid or expired login link")

        user_id = row[0]

        # Delete used magic link
        cur.execute("DELETE FROM sessions WHERE token = %s", (token,))

        # Create session token
        session_token = secrets.token_urlsafe(32)
        cur.execute(
            """INSERT INTO sessions (token, user_id, type, expires_at)
               VALUES (%s, %s, 'session', NOW() + INTERVAL '%s hours')""",
            (session_token, user_id, SESSION_EXPIRY_HOURS),
        )
        conn.commit()
        cur.close()
    except HTTPException:
        raise
    except Exception as e:
        conn.rollback()
        raise HTTPException(status_code=500, detail=f"Verification failed: {e}")
    finally:
        database.put_conn(conn)

    # Redirect to app with token in URL fragment (for localStorage) + set cookie (for same-domain)
    redirect = RedirectResponse(url=f"/static/dashboard.html#token={session_token}", status_code=302)
    redirect.set_cookie(
        key="fp_session",
        value=session_token,
        httponly=True,
        secure=True,
        samesite="lax",
        max_age=SESSION_EXPIRY_HOURS * 3600,
    )
    return redirect


@app.get("/auth/me")
def get_current_user(user: dict = Depends(require_user)):
    """Return current authenticated user info."""
    # Check if user has any connections
    if database.is_available():
        conn = database.get_conn()
        if conn:
            try:
                cur = conn.cursor()
                cur.execute(
                    "SELECT COUNT(*) FROM connections WHERE user_id = %s",
                    (user["id"],),
                )
                count = cur.fetchone()[0]
                cur.close()
                user["has_connections"] = count > 0
                user["connection_count"] = count
            except Exception:
                user["has_connections"] = False
                user["connection_count"] = 0
            finally:
                database.put_conn(conn)
    return user


@app.post("/auth/logout")
def logout(request: Request, response: Response):
    """Clear session."""
    token = request.cookies.get("fp_session")
    if token and database.is_available():
        conn = database.get_conn()
        if conn:
            try:
                cur = conn.cursor()
                cur.execute("DELETE FROM sessions WHERE token = %s", (token,))
                conn.commit()
                cur.close()
            except Exception:
                conn.rollback()
            finally:
                database.put_conn(conn)
    resp = {"status": "logged out"}
    response.delete_cookie("fp_session")
    return resp


# ── Dashboard stats ──────────────────────────────────────────────────────────

@app.get("/dashboard/stats")
def dashboard_stats(user: dict = Depends(require_user)):
    """Aggregate stats for the current user's connections."""
    if not database.is_available():
        return {"total_scored": 0, "auto_created": 0, "needs_review": 0, "avg_score": 0}

    conn = database.get_conn()
    if not conn:
        return {"total_scored": 0, "auto_created": 0, "needs_review": 0, "avg_score": 0}

    try:
        cur = conn.cursor()
        # Get user's connection names
        cur.execute(
            "SELECT name FROM connections WHERE user_id = %s",
            (user["id"],),
        )
        conn_names = [r[0] for r in cur.fetchall()]

        if not conn_names:
            # Fall back to all deals if no connections (admin/default)
            cur.execute("""
                SELECT COUNT(*), COALESCE(AVG(score), 0),
                       COUNT(*) FILTER (WHERE recommendation = 'auto_create'),
                       COUNT(*) FILTER (WHERE recommendation = 'needs_review')
                FROM scored_deals
            """)
        else:
            placeholders = ",".join(["%s"] * len(conn_names))
            cur.execute(f"""
                SELECT COUNT(*), COALESCE(AVG(score), 0),
                       COUNT(*) FILTER (WHERE recommendation = 'auto_create'),
                       COUNT(*) FILTER (WHERE recommendation = 'needs_review')
                FROM scored_deals sd
                WHERE EXISTS (
                    SELECT 1 FROM processed_transcripts pt
                    WHERE pt.connection_name IN ({placeholders})
                )
            """, conn_names)

        row = cur.fetchone()
        cur.close()
        return {
            "total_scored": row[0] or 0,
            "avg_score": round(row[1] or 0),
            "auto_created": row[2] or 0,
            "needs_review": row[3] or 0,
        }
    except Exception as e:
        logger.warning(f"Dashboard stats failed: {e}")
        return {"total_scored": 0, "auto_created": 0, "needs_review": 0, "avg_score": 0}
    finally:
        database.put_conn(conn)


# ── Org Health Alert ──────────────────────────────────────────────────────────

def _check_org_health(conn: dict):
    """
    Check if >15% of recent conversations land in Needs Review for a connection.
    If so, alert admin via Slack/Teams that framework thresholds may need recalibration.
    """
    if not database.is_available():
        return
    db = database.get_conn()
    if not db:
        return
    try:
        cur = db.cursor()
        # Look at scored deals from the last 30 days for this connection
        conn_name = conn.get("name", "Default")
        cur.execute("""
            SELECT recommendation, COUNT(*)
            FROM scored_deals
            WHERE created_at > NOW() - INTERVAL '30 days'
              AND metadata->>'connection_name' = %s
            GROUP BY recommendation
        """, (conn_name,))
        rows = cur.fetchall()
        cur.close()

        total = sum(r[1] for r in rows)
        if total < 10:
            return  # Not enough data to be meaningful

        review_count = sum(r[1] for r in rows if r[0] == "needs_review")
        review_pct = (review_count / total) * 100

        if review_pct <= 15:
            return

        import requests as req_lib
        alert_msg = (
            f"{review_count} of {total} conversations ({review_pct:.0f}%) landed in Needs Review "
            f"over the last 30 days. This may indicate that framework thresholds need recalibration. "
            f"Consider adjusting the auto-create threshold or framework weights in Settings."
        )

        slack_url = conn.get("slack_webhook_url")
        if slack_url:
            try:
                req_lib.post(slack_url, json={"text": f":warning: *Fairplay Org Health Alert*\n{alert_msg}"}, timeout=10)
            except Exception:
                pass

        teams_url = conn.get("teams_webhook_url")
        if teams_url:
            try:
                req_lib.post(teams_url, json={
                    "type": "message",
                    "attachments": [{"contentType": "application/vnd.microsoft.card.adaptive", "content": {
                        "$schema": "http://adaptivecards.io/schemas/adaptive-card.json",
                        "type": "AdaptiveCard", "version": "1.4",
                        "body": [
                            {"type": "TextBlock", "text": "Fairplay Org Health Alert", "weight": "bolder", "color": "warning"},
                            {"type": "TextBlock", "text": alert_msg, "wrap": True},
                        ],
                    }}],
                }, timeout=10)
            except Exception:
                pass

        logger.info(f"[{conn_name}] Org health alert: {review_pct:.0f}% needs review ({review_count}/{total})")
    except Exception as e:
        logger.warning(f"Org health check failed: {e}")
    finally:
        database.put_conn(db)


# ── Rapid Close Detection ────────────────────────────────────────────────────

def _check_rapid_closes(conn: dict):
    """
    Find deals that Fairplay auto-created but were closed-lost quickly (within 14 days).
    Alert admin via Slack/Teams so they can calibrate.
    """
    if not database.is_available():
        return
    db = database.get_conn()
    if not db:
        return
    try:
        cur = db.cursor()
        # Find auto-created deals from the last 30 days that have a deal_id
        cur.execute("""
            SELECT deal_id, deal_name, company_name, score, created_at
            FROM scored_deals
            WHERE recommendation = 'auto_create'
              AND deal_id IS NOT NULL
              AND deal_id != ''
              AND created_at > NOW() - INTERVAL '30 days'
              AND metadata->>'connection_name' = %s
        """, (conn.get("name", "Default"),))
        auto_deals = cur.fetchall()
        cur.close()

        if not auto_deals:
            return

        # Check CRM for closed-lost status
        crm_name = conn.get("crm", "attio")
        crm_key = conn.get("crm_api_key", "")
        if not crm_key:
            return

        try:
            crm_client = crm_factory.get_client(crm_name)
            closed_deals = crm_client.query_deals_by_stage(
                ["closedlost", "lost", "Closed Lost", "Lost"], limit=50, api_key=crm_key
            )
        except Exception as e:
            logger.warning(f"Rapid close CRM check failed: {e}")
            return

        # Match auto-created deals to closed-lost deals by deal_id or company name
        closed_ids = {d.get("deal_id") for d in closed_deals}
        closed_companies = {d.get("company_name", "").lower().strip() for d in closed_deals}

        rapid_closes = []
        for deal_id, deal_name, company, score, created_at in auto_deals:
            if deal_id in closed_ids or company.lower().strip() in closed_companies:
                rapid_closes.append({
                    "deal_name": deal_name,
                    "company": company,
                    "score": score,
                    "created": str(created_at),
                })

        if not rapid_closes:
            return

        import requests as req_lib
        lines = [f"- {rc['deal_name']} (score: {rc['score']}, company: {rc['company']})" for rc in rapid_closes[:5]]
        alert_msg = (
            f"{len(rapid_closes)} deal(s) auto-created by Fairplay were closed-lost quickly:\n"
            + "\n".join(lines)
            + "\n\nThis may indicate the scoring threshold is too low or certain deal patterns need different evaluation."
        )

        slack_url = conn.get("slack_webhook_url")
        if slack_url:
            try:
                req_lib.post(slack_url, json={"text": f":rotating_light: *Fairplay Rapid Close Alert*\n{alert_msg}"}, timeout=10)
            except Exception:
                pass

        teams_url = conn.get("teams_webhook_url")
        if teams_url:
            try:
                req_lib.post(teams_url, json={
                    "type": "message",
                    "attachments": [{"contentType": "application/vnd.microsoft.card.adaptive", "content": {
                        "$schema": "http://adaptivecards.io/schemas/adaptive-card.json",
                        "type": "AdaptiveCard", "version": "1.4",
                        "body": [
                            {"type": "TextBlock", "text": "Fairplay Rapid Close Alert", "weight": "bolder", "color": "attention"},
                            {"type": "TextBlock", "text": alert_msg, "wrap": True},
                        ],
                    }}],
                }, timeout=10)
            except Exception:
                pass

        logger.info(f"[{conn.get('name')}] Rapid close alert: {len(rapid_closes)} deals")
    except Exception as e:
        logger.warning(f"Rapid close check failed: {e}")
    finally:
        database.put_conn(db)


@app.post("/check-health", dependencies=[Depends(require_api_key)])
def check_health_now():
    """Manually trigger org health + rapid close checks for all connections."""
    all_conns = connections.list_connections_full()
    results = []
    for conn in all_conns:
        _check_org_health(conn)
        _check_rapid_closes(conn)
        results.append(conn.get("name", "?"))
    return {"status": "checked", "connections": results}


# ── Shadow Mode Gap Report ───────────────────────────────────────────────────

@app.get("/connections/{webhook_id}/gap-report", dependencies=[Depends(require_api_key)])
def shadow_gap_report(webhook_id: str, days: int = 30):
    """
    Shadow mode gap report: compare what Fairplay scored vs what reps actually created.
    Shows conversations that scored above threshold but have no CRM deal,
    and CRM deals that Fairplay scored below threshold.
    """
    conn = connections.get_connection(webhook_id)
    if not conn:
        raise HTTPException(status_code=404, detail="Connection not found")

    if not database.is_available():
        raise HTTPException(status_code=503, detail="Database not available")

    conn_name = conn.get("name", "Default")
    crm_name = conn.get("crm", "attio")
    crm_key = conn.get("crm_api_key", "")
    threshold = conn.get("auto_create_threshold", 70)

    # 1. Get all Fairplay-scored deals from the period
    db = database.get_conn()
    if not db:
        raise HTTPException(status_code=503, detail="Database connection failed")

    try:
        cur = db.cursor()
        cur.execute("""
            SELECT deal_name, company_name, score, recommendation, deal_id, key_insight, created_at
            FROM scored_deals
            WHERE created_at > NOW() - INTERVAL '%s days'
            ORDER BY created_at DESC
        """, (days,))
        scored = cur.fetchall()
        cur.close()
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Query failed: {e}")
    finally:
        database.put_conn(db)

    scored_deals = [
        {
            "deal_name": r[0], "company_name": r[1], "score": r[2],
            "recommendation": r[3], "deal_id": r[4], "key_insight": r[5],
            "created_at": str(r[6]),
        }
        for r in scored
    ]

    # 2. Get CRM deals created in the period
    crm_deals = []
    if crm_key:
        try:
            crm_client = crm_factory.get_client(crm_name)
            # Pull open deals + recently closed
            for stages in [["open", "Open", "qualified", "Qualified", "demo", "Demo", "proposal", "Proposal"],
                           ["closedwon", "Won", "Closed Won"], ["closedlost", "Lost", "Closed Lost"]]:
                try:
                    batch = crm_client.query_deals_by_stage(stages, limit=50, api_key=crm_key)
                    crm_deals.extend(batch)
                except Exception:
                    pass
        except Exception as e:
            logger.warning(f"Gap report CRM pull failed: {e}")

    # 3. Build the gap analysis
    # Normalize company names for matching
    def _norm(name):
        return (name or "").lower().strip().replace(",", "").replace(".", "").replace(" inc", "").replace(" llc", "").replace(" ltd", "")

    crm_companies = {_norm(d.get("company_name", "")): d for d in crm_deals if d.get("company_name")}
    scored_companies = {_norm(d["company_name"]): d for d in scored_deals if d.get("company_name")}

    # Fairplay would create, but no CRM deal exists (missed by reps)
    missed_by_reps = []
    for sd in scored_deals:
        if sd["score"] >= threshold and sd["recommendation"] in ("auto_create", "needs_review"):
            norm_name = _norm(sd["company_name"])
            if norm_name and norm_name not in crm_companies:
                missed_by_reps.append({
                    "company": sd["company_name"],
                    "score": sd["score"],
                    "recommendation": sd["recommendation"],
                    "insight": sd["key_insight"],
                    "date": sd["created_at"],
                })

    # CRM deals that Fairplay scored low (reps created deals Fairplay wouldn't)
    inflated_by_reps = []
    for company_norm, crm_deal in crm_companies.items():
        if company_norm in scored_companies:
            sd = scored_companies[company_norm]
            if sd["score"] < threshold:
                inflated_by_reps.append({
                    "company": crm_deal.get("company_name", ""),
                    "crm_stage": crm_deal.get("stage", "unknown"),
                    "fairplay_score": sd["score"],
                    "recommendation": sd["recommendation"],
                    "insight": sd.get("key_insight", ""),
                })

    # Summary stats
    total_scored = len(scored_deals)
    above_threshold = sum(1 for sd in scored_deals if sd["score"] >= threshold)
    below_threshold = total_scored - above_threshold
    needs_review_count = sum(1 for sd in scored_deals if sd["recommendation"] == "needs_review")
    review_pct = (needs_review_count / total_scored * 100) if total_scored > 0 else 0

    return {
        "period_days": days,
        "connection": conn_name,
        "threshold": threshold,
        "summary": {
            "total_conversations_scored": total_scored,
            "above_threshold": above_threshold,
            "below_threshold": below_threshold,
            "needs_review": needs_review_count,
            "needs_review_pct": round(review_pct, 1),
            "crm_deals_found": len(crm_deals),
        },
        "gaps": {
            "missed_by_reps": missed_by_reps[:20],
            "missed_by_reps_count": len(missed_by_reps),
            "inflated_by_reps": inflated_by_reps[:20],
            "inflated_by_reps_count": len(inflated_by_reps),
        },
        "scored_deals": scored_deals[:50],
    }


# ── Shadow Mode Report (deliverable) ──────────────────────────────────────────

@app.post("/connections/{webhook_id}/shadow-report", dependencies=[Depends(require_api_key)])
def generate_shadow_report(webhook_id: str, days: int = 30):
    """
    Generate and email a formatted shadow mode report.
    Compares Fairplay scores vs CRM reality over the period.
    Returns the report data and sends it via email to the connection owner.
    """
    # Reuse the gap report data
    report = shadow_gap_report(webhook_id, days)

    conn = connections.get_connection(webhook_id)
    if not conn:
        raise HTTPException(status_code=404, detail="Connection not found")

    s = report["summary"]
    gaps = report["gaps"]
    conn_name = conn.get("name", "Default")

    # Build HTML report
    missed_rows = ""
    for m in gaps["missed_by_reps"][:10]:
        color = "#22c55e" if m["score"] >= 70 else "#f59e0b" if m["score"] >= 50 else "#ef4444"
        missed_rows += f"""<tr>
            <td style="padding:8px 12px;border-bottom:1px solid #f3f4f6;font-weight:600;">{m['company']}</td>
            <td style="padding:8px 12px;border-bottom:1px solid #f3f4f6;"><span style="color:{color};font-weight:700;">{m['score']}</span>/100</td>
            <td style="padding:8px 12px;border-bottom:1px solid #f3f4f6;font-size:13px;color:#666;">{m.get('insight', '')[:80]}</td>
        </tr>"""

    inflated_rows = ""
    for i in gaps["inflated_by_reps"][:10]:
        color = "#22c55e" if i["fairplay_score"] >= 70 else "#f59e0b" if i["fairplay_score"] >= 50 else "#ef4444"
        inflated_rows += f"""<tr>
            <td style="padding:8px 12px;border-bottom:1px solid #f3f4f6;font-weight:600;">{i['company']}</td>
            <td style="padding:8px 12px;border-bottom:1px solid #f3f4f6;">{i['crm_stage']}</td>
            <td style="padding:8px 12px;border-bottom:1px solid #f3f4f6;"><span style="color:{color};font-weight:700;">{i['fairplay_score']}</span>/100</td>
        </tr>"""

    base_url = _get_base_url()

    html_body = f"""
    <div style="font-family: Inter, -apple-system, sans-serif; max-width: 640px; margin: 0 auto;">
        <div style="background: #0a0a0a; padding: 20px 24px; border-radius: 8px 8px 0 0;">
            <span style="color: white; font-weight: 700; font-size: 18px;">Fairplay Shadow Mode Report</span>
            <span style="color: #888; font-size: 13px; margin-left: 12px;">{conn_name} | Last {days} days</span>
        </div>
        <div style="border: 1px solid #eee; border-top: none; padding: 24px; border-radius: 0 0 8px 8px;">

            <div style="display:flex;gap:16px;margin-bottom:24px;">
                <div style="flex:1;background:#f9fafb;border-radius:8px;padding:16px;text-align:center;">
                    <div style="font-size:28px;font-weight:800;">{s['total_conversations_scored']}</div>
                    <div style="font-size:12px;color:#6b7280;">Conversations Scored</div>
                </div>
                <div style="flex:1;background:#ecfdf5;border-radius:8px;padding:16px;text-align:center;">
                    <div style="font-size:28px;font-weight:800;color:#059669;">{s['above_threshold']}</div>
                    <div style="font-size:12px;color:#6b7280;">Above Threshold</div>
                </div>
                <div style="flex:1;background:#fef2f2;border-radius:8px;padding:16px;text-align:center;">
                    <div style="font-size:28px;font-weight:800;color:#dc2626;">{s['below_threshold']}</div>
                    <div style="font-size:12px;color:#6b7280;">Below Threshold</div>
                </div>
            </div>

            <div style="background:#fffbeb;border:1px solid #fde68a;border-radius:8px;padding:14px;margin-bottom:24px;">
                <strong>Pipeline Review Summary:</strong> {s['needs_review_pct']}% of conversations landed in Needs Review.
                {gaps['missed_by_reps_count']} conversation(s) scored as deals but have no CRM record.
                {gaps['inflated_by_reps_count']} CRM deal(s) scored below Fairplay's threshold.
            </div>

            {"<h3 style='margin:0 0 8px;font-size:15px;'>Missed by Reps (" + str(gaps['missed_by_reps_count']) + ")</h3><p style=font-size:13px;color:#6b7280;margin:0 0 12px;>Conversations that scored above threshold but have no deal in the CRM.</p><table style=width:100%;border-collapse:collapse;font-size:14px;><tr style=background:#f9fafb;><th style=text-align:left;padding:8px 12px;font-size:11px;color:#9ca3af;>Company</th><th style=text-align:left;padding:8px 12px;font-size:11px;color:#9ca3af;>Score</th><th style=text-align:left;padding:8px 12px;font-size:11px;color:#9ca3af;>Insight</th></tr>" + missed_rows + "</table><br>" if missed_rows else ""}

            {"<h3 style='margin:0 0 8px;font-size:15px;'>Inflated by Reps (" + str(gaps['inflated_by_reps_count']) + ")</h3><p style=font-size:13px;color:#6b7280;margin:0 0 12px;>CRM deals that Fairplay scored below threshold.</p><table style=width:100%;border-collapse:collapse;font-size:14px;><tr style=background:#f9fafb;><th style=text-align:left;padding:8px 12px;font-size:11px;color:#9ca3af;>Company</th><th style=text-align:left;padding:8px 12px;font-size:11px;color:#9ca3af;>CRM Stage</th><th style=text-align:left;padding:8px 12px;font-size:11px;color:#9ca3af;>Fairplay Score</th></tr>" + inflated_rows + "</table><br>" if inflated_rows else ""}

            <hr style="border:none;border-top:1px solid #eee;margin:20px 0;">
            <p style="font-size:13px;color:#888;">
                <a href="{base_url}/static/gap-report.html" style="color:#0a0a0a;font-weight:600;">View full interactive report</a> |
                This report was generated by Fairplay for your shadow mode pipeline review.
            </p>
        </div>
    </div>"""

    # Send via email if we can find the owner
    email_sent = False
    owner_email = _get_connection_owner_email(webhook_id)
    if owner_email:
        try:
            from config import RESEND_API_KEY
            import requests as req_lib
            if RESEND_API_KEY:
                resp = req_lib.post(
                    "https://api.resend.com/emails",
                    headers={"Authorization": f"Bearer {RESEND_API_KEY}"},
                    json={
                        "from": "Fairplay <fairplay@nicl.ai>",
                        "to": [owner_email],
                        "subject": f"Fairplay Shadow Mode Report: {s['total_conversations_scored']} conversations scored ({conn_name})",
                        "html": html_body,
                        "tags": [{"name": "category", "value": "shadow_report"}],
                    },
                    timeout=10,
                )
                email_sent = resp.status_code in (200, 201)
                if email_sent:
                    logger.info(f"Shadow report sent to {owner_email}")
        except Exception as e:
            logger.warning(f"Shadow report email failed: {e}")

    return {
        "report": report,
        "email_sent": email_sent,
        "email_to": owner_email,
        "html_preview": html_body,
    }


def _get_connection_owner_email(webhook_id: str) -> Optional[str]:
    """Get the email of the connection owner from users table."""
    if not database.is_available():
        return None
    db = database.get_conn()
    if not db:
        return None
    try:
        cur = db.cursor()
        cur.execute(
            """SELECT u.email FROM connections c
               JOIN users u ON c.user_id = u.id
               WHERE c.webhook_id = %s""",
            (webhook_id,),
        )
        row = cur.fetchone()
        cur.close()
        return row[0] if row else None
    except Exception:
        return None
    finally:
        database.put_conn(db)


# ── Warm Start (retroactive scoring) ─────────────────────────────────────────

@app.post("/connections/{webhook_id}/warm-start", dependencies=[Depends(require_api_key)])
def warm_start(webhook_id: str, count: int = 20):
    """
    Warm Start onboarding: retroactively score recent transcripts.
    Pulls the most recent transcripts from the configured source and scores them
    without creating CRM deals. Returns a summary report for calibration.
    """
    conn = connections.get_connection(webhook_id)
    if not conn:
        raise HTTPException(status_code=404, detail="Connection not found")

    source = conn.get("transcript_source", "fireflies")
    results = []

    if source == "fireflies":
        ff_key = conn.get("fireflies_api_key", "")
        if not ff_key:
            raise HTTPException(status_code=400, detail="Fireflies API key not configured")

        transcripts = fireflies_client.list_transcripts(
            since=90, limit=min(count, 50), api_key=ff_key
        )

        framework = conn.get("framework", "custom")
        biz_ctx = {
            "sale_type": conn.get("sale_type", ""),
            "deal_value_range": conn.get("deal_value_range", ""),
            "avg_days_to_close": conn.get("avg_days_to_close", ""),
            "industry_vertical": conn.get("industry_vertical", ""),
        } if any(conn.get(k) for k in ("sale_type", "deal_value_range", "avg_days_to_close", "industry_vertical")) else None

        custom_weights = None
        fw_weights_str = conn.get("framework_weights", "")
        if fw_weights_str:
            try:
                custom_weights = json.loads(fw_weights_str) if isinstance(fw_weights_str, str) else fw_weights_str
            except Exception:
                pass

        for t in transcripts:
            tid = t.get("id")
            if not tid:
                continue
            try:
                transcript = fireflies_client.get_transcript(tid, api_key=ff_key)
                if not transcript:
                    continue
                text = fireflies_client.format_transcript_text(transcript)
                if not text or len(text) < 500:
                    continue
                metadata = fireflies_client.get_meeting_metadata(transcript) if transcript else {}

                analysis = transcript_analyzer.analyze_transcript(
                    text, metadata, framework=framework,
                    business_context=biz_ctx, company_icp=conn.get("company_icp"), calibration_notes=conn.get("calibration_notes"),
                )
                score_result = deal_scorer.score_deal(analysis, custom_weights=custom_weights)

                results.append({
                    "transcript_id": tid,
                    "title": metadata.get("title", "Unknown"),
                    "date": metadata.get("date", ""),
                    "company": analysis.get("prospect_company", {}).get("name", "Unknown"),
                    "score": score_result["total_score"],
                    "recommendation": score_result["recommendation"],
                    "framework": framework,
                    "key_insight": score_result.get("key_insight", ""),
                    "is_sales": analysis.get("is_sales_conversation", False),
                    "meeting_type": analysis.get("meeting_type", "other"),
                })

                # Save to scored_deals for history (but no CRM writes)
                _save_scored_deal(score_result, analysis, metadata, connection_name=conn.get("name", ""))

            except transcript_analyzer.CreditExhaustedError:
                logger.warning("Warm start stopped: API credits exhausted")
                break
            except Exception as e:
                logger.warning(f"Warm start: failed to score transcript {tid}: {e}")
                continue

    else:
        raise HTTPException(status_code=400, detail=f"Warm start not yet supported for source: {source}. Currently supports Fireflies.")

    # Summary stats
    sales_count = sum(1 for r in results if r["is_sales"])
    above_threshold = sum(1 for r in results if r["score"] >= conn.get("auto_create_threshold", 70))
    avg_score = sum(r["score"] for r in results) / len(results) if results else 0

    return {
        "scored": len(results),
        "sales_conversations": sales_count,
        "non_sales": len(results) - sales_count,
        "above_threshold": above_threshold,
        "below_threshold": len(results) - above_threshold,
        "avg_score": round(avg_score, 1),
        "results": results,
    }


# ── Needs Review Queue ────────────────────────────────────────────────────────

@app.get("/needs-review", dependencies=[Depends(require_api_key)])
def get_needs_review(days: int = 30):
    """Get all conversations in Needs Review status for the review queue."""
    if not database.is_available():
        raise HTTPException(status_code=503, detail="Database not available")

    db = database.get_conn()
    if not db:
        raise HTTPException(status_code=503, detail="Database connection failed")

    try:
        cur = db.cursor()
        cur.execute("""
            SELECT id, deal_id, deal_name, meeting_title, score, recommendation,
                   framework, breakdown, key_insight, company_name, created_at
            FROM scored_deals
            WHERE recommendation = 'needs_review'
              AND created_at > NOW() - INTERVAL '%s days'
            ORDER BY created_at DESC
        """, (days,))
        rows = cur.fetchall()
        cur.close()

        return [{
            "id": r[0],
            "deal_id": r[1],
            "deal_name": r[2],
            "meeting_title": r[3],
            "score": r[4],
            "recommendation": r[5],
            "framework": r[6],
            "breakdown": r[7] if isinstance(r[7], dict) else {},
            "key_insight": r[8],
            "company_name": r[9],
            "created_at": str(r[10]),
        } for r in rows]
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Query failed: {e}")
    finally:
        database.put_conn(db)


@app.post("/needs-review/{deal_db_id}/resolve", dependencies=[Depends(require_api_key)])
def resolve_needs_review(deal_db_id: int, action: str = "approve"):
    """
    Resolve a Needs Review item. Actions: approve, reject, dismiss.
    - approve: updates recommendation to auto_create, creates deal in CRM if connection is live
    - reject: updates recommendation to not_a_deal
    - dismiss: updates recommendation to dismissed
    """
    if action not in ("approve", "reject", "dismiss"):
        raise HTTPException(status_code=400, detail="Action must be: approve, reject, or dismiss")

    if not database.is_available():
        raise HTTPException(status_code=503, detail="Database not available")

    db = database.get_conn()
    if not db:
        raise HTTPException(status_code=503, detail="Database connection failed")

    try:
        cur = db.cursor()
        new_rec = {"approve": "auto_create", "reject": "not_a_deal", "dismiss": "dismissed"}[action]

        cur.execute(
            "UPDATE scored_deals SET recommendation = %s WHERE id = %s AND recommendation = 'needs_review' RETURNING deal_name, score, company_name",
            (new_rec, deal_db_id),
        )
        row = cur.fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="Needs Review item not found or already resolved")
        db.commit()
        cur.close()

        deal_name, score, company_name = row

        # Log feedback
        _save_feedback({
            "deal_id": str(deal_db_id),
            "vote": action,
            "note": "Resolved from Needs Review queue",
            "timestamp": datetime.now().isoformat(),
        })

        return {
            "id": deal_db_id,
            "action": action,
            "new_recommendation": new_rec,
            "deal_name": deal_name,
            "company_name": company_name,
        }
    except HTTPException:
        raise
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=f"Failed to resolve: {e}")
    finally:
        database.put_conn(db)


# ── Health check ─────────────────────────────────────────────────────────────

@app.get("/health")
def health():
    """Health check."""
    from config import FIREFLIES_API_KEY, SLACK_WEBHOOK_URL, DATABASE_URL, POLLING_ENABLED
    return {
        "status": "ok",
        "version": "3.4.0",
        "fireflies_configured": bool(FIREFLIES_API_KEY),
        "slack_configured": bool(SLACK_WEBHOOK_URL),
        "database_configured": bool(DATABASE_URL),
        "polling_enabled": POLLING_ENABLED,
    }


# ── Static files (must be last, catches all /static/* routes) ────────────────
import os as _os
_static_dir = _os.path.join(_os.path.dirname(_os.path.abspath(__file__)), "static")
if _os.path.isdir(_static_dir):
    app.mount("/static", StaticFiles(directory=_static_dir, html=True), name="static")
