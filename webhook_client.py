"""
Webhook CRM Client - Custom/Generic Integration
Sends scored deal data as a JSON POST to any webhook URL.

Works with any system that can receive webhooks: Zapier, Make, n8n,
custom APIs, or any CRM with an incoming webhook feature.

The api_key field stores the webhook URL.
"""

import json
import logging
import re
from datetime import datetime
from typing import Optional

import requests

logger = logging.getLogger(__name__)


def create_deal(
    score_result: dict,
    analysis: dict,
    metadata: Optional[dict] = None,
    dry_run: bool = False,
    api_key: Optional[str] = None,
) -> Optional[dict]:
    """Send scored deal data to a webhook URL."""
    webhook_url = api_key
    if not webhook_url:
        logger.error("Webhook: No URL provided")
        return None

    deal_name = score_result.get("deal_name_suggestion", "New Deal")
    recommendation = score_result.get("recommendation", "needs_review")
    score = score_result.get("total_score", 0)

    company = analysis.get("prospect_company", {})
    participants = analysis.get("participants", [])
    prospect_participants = [p for p in participants if isinstance(p, dict) and p.get("is_prospect")]
    seller_participants = [p for p in participants if isinstance(p, dict) and not p.get("is_prospect")]

    # Build the payload
    payload = {
        "event": "deal_scored",
        "timestamp": datetime.now().isoformat(),
        "dry_run": dry_run,
        "deal": {
            "name": deal_name,
            "score": score,
            "recommendation": recommendation,
            "framework": score_result.get("framework", "custom"),
            "key_insight": score_result.get("key_insight", ""),
        },
        "breakdown": {},
        "company": {
            "name": company.get("name"),
            "industry": company.get("industry"),
            "estimated_size": company.get("estimated_size"),
            "estimated_revenue": company.get("estimated_revenue"),
            "domain": company.get("domain"),
        },
        "contacts": [
            {
                "name": p.get("name"),
                "role": p.get("role"),
                "company": p.get("company"),
                "is_prospect": p.get("is_prospect"),
            }
            for p in participants if isinstance(p, dict)
        ],
        "meeting": {
            "title": metadata.get("title", "") if metadata else "",
            "date": metadata.get("date", "") if metadata else "",
            "source": metadata.get("source", "") if metadata else "",
        },
        "analysis": {
            "meeting_type": analysis.get("meeting_type"),
            "is_sales_conversation": analysis.get("is_sales_conversation"),
            "summary": analysis.get("summary", ""),
            "next_steps": analysis.get("next_steps", []),
            "objections": analysis.get("objections", []),
        },
    }

    # Add breakdown with assessments
    fw_scores = analysis.get("framework_scores", {})
    for cat, data in score_result.get("breakdown", {}).items():
        assessment = ""
        if isinstance(fw_scores.get(cat), dict):
            assessment = fw_scores[cat].get("assessment", "")
        payload["breakdown"][cat] = {
            "label": data.get("label", cat),
            "score": data.get("score", 0),
            "max": data.get("max", 25),
            "assessment": assessment,
        }

    # Add budget indicators
    budget = analysis.get("budget_indicators", {})
    if budget:
        payload["budget"] = {
            "mentioned": budget.get("mentioned", False),
            "range": budget.get("range"),
        }

    if dry_run:
        logger.info(f"[DRY RUN] Would send webhook for: {deal_name}")
        return {"dry_run": True, "deal_name": deal_name}

    try:
        resp = requests.post(webhook_url, json=payload, timeout=30)
        if resp.status_code in (200, 201, 202, 204):
            logger.info(f"Webhook sent for: {deal_name} (status: {resp.status_code})")
            return {
                "deal_id": f"webhook_{datetime.now().strftime('%Y%m%d%H%M%S')}",
                "deal_name": deal_name,
                "deal_url": webhook_url,
                "company_id": None,
                "associated_contacts": [],
                "stage": recommendation,
                "score": score,
            }
        else:
            logger.error(f"Webhook failed: {resp.status_code} {resp.text[:300]}")
            return None
    except Exception as e:
        logger.error(f"Webhook request failed: {e}")
        return None


def find_or_create_company(company_name: str, industry: Optional[str] = None,
                           domain: Optional[str] = None, api_key: Optional[str] = None) -> Optional[str]:
    """Not applicable for webhook integration."""
    return None


def find_contact_by_name(name: str, company_name: Optional[str] = None,
                         api_key: Optional[str] = None) -> Optional[str]:
    """Not applicable for webhook integration."""
    return None


def find_or_create_contact(name: str, email: Optional[str] = None,
                           company_name: Optional[str] = None,
                           api_key: Optional[str] = None) -> Optional[str]:
    """Not applicable for webhook integration."""
    return None


def find_deal_by_company(company_name: str, api_key: Optional[str] = None) -> Optional[dict]:
    """Not applicable for webhook integration."""
    return None


def update_deal_stage(deal_id: str, stage: str, api_key: Optional[str] = None) -> Optional[dict]:
    """Send a stage update event to the webhook."""
    webhook_url = api_key
    if not webhook_url:
        return None

    payload = {
        "event": "deal_stage_updated",
        "timestamp": datetime.now().isoformat(),
        "deal_id": deal_id,
        "new_stage": stage,
    }

    try:
        resp = requests.post(webhook_url, json=payload, timeout=30)
        if resp.status_code in (200, 201, 202, 204):
            return {"deal_id": deal_id, "stage": stage}
    except Exception as e:
        logger.error(f"Webhook stage update failed: {e}")
    return None


def query_deals_by_stage(stages: list, limit: int = 20, api_key: Optional[str] = None) -> list:
    """Not applicable for webhook integration."""
    return []
