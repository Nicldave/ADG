"""
Auto Deal Generator - FastAPI wrapper
Exposes the analysis and deal creation pipeline as HTTP endpoints
for external frontends (Lovable, custom React apps, etc.)

Run: uvicorn api:app --reload --port 8000
"""

import os
import sys
from datetime import datetime
from typing import Optional

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field

# Add this directory to path so local modules resolve
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import transcript_analyzer
import deal_scorer
import crm as crm_factory
from frameworks import FRAMEWORKS, FRAMEWORK_NAMES, get_framework

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


@app.get("/health")
def health():
    """Health check."""
    return {"status": "ok", "version": "1.0.0"}
