"""
Pipedrive Client - Deal Creation
Creates Deals in Pipedrive CRM from scored transcript analysis.

Uses Pipedrive REST API v1 with api_token query parameter authentication.
Pass your Pipedrive API token as the api_key parameter.

Required scopes: deals:full, organizations:full, persons:full
"""

import json
import logging
import re
from datetime import datetime
from typing import Optional

import requests

logger = logging.getLogger(__name__)

PIPEDRIVE_BASE = "https://api.pipedrive.com/v1"


# --- Internal helpers ---

def _pd_request(method: str, path: str, payload=None, params=None,
                api_key: str = "") -> dict:
    """Execute a Pipedrive API request with api_token auth."""
    if not api_key:
        logger.error("Pipedrive: No api_token provided")
        return {}

    url = f"{PIPEDRIVE_BASE}{path}"
    if params is None:
        params = {}
    params["api_token"] = api_key

    try:
        response = requests.request(
            method, url, json=payload, params=params, timeout=30
        )
        if response.status_code not in (200, 201):
            logger.error(f"Pipedrive {method} {path} failed: {response.status_code} {response.text[:300]}")
            response.raise_for_status()
        data = response.json() if response.content else {}
        if data.get("success") is False:
            logger.error(f"Pipedrive API error: {data.get('error', 'unknown')}")
            return {}
        return data
    except Exception as e:
        logger.error(f"Pipedrive request failed: {e}")
        raise


def _normalize_company_name(name: str) -> str:
    """Normalize company name for matching."""
    if not name:
        return ""
    name = name.lower().strip()
    for suffix in [" inc", " inc.", " corp", " corporation", " llc", " ltd", " co",
                   " company", " group", " solutions", " services"]:
        if name.endswith(suffix):
            name = name[:-len(suffix)].strip()
    return name


# --- Company (Organization) Lookup/Creation ---

def find_or_create_company(company_name: str, industry: Optional[str] = None,
                           domain: Optional[str] = None, api_key: Optional[str] = None) -> Optional[str]:
    """Find an Organization in Pipedrive by name, or create one. Returns Organization ID."""
    if not company_name or not api_key:
        return None

    # Search by name
    try:
        data = _pd_request("GET", "/organizations/search", params={"term": company_name}, api_key=api_key)
        items = data.get("data", {}).get("items", [])
        if items:
            org_id = str(items[0]["item"]["id"])
            logger.info(f"Pipedrive Organization found: {company_name} (ID: {org_id})")
            return org_id
    except Exception as e:
        logger.warning(f"Pipedrive org search failed: {e}")

    # Create
    try:
        payload = {"name": company_name}
        data = _pd_request("POST", "/organizations", payload, api_key=api_key)
        org_id = data.get("data", {}).get("id")
        if org_id:
            org_id = str(org_id)
            logger.info(f"Pipedrive Organization created: {company_name} (ID: {org_id})")
            return org_id
    except Exception as e:
        logger.warning(f"Pipedrive Organization create failed for '{company_name}': {e}")

    return None


# --- Contact (Person) Lookup/Creation ---

def find_contact_by_name(name: str, company_name: Optional[str] = None,
                         api_key: Optional[str] = None) -> Optional[str]:
    """Search for a Person in Pipedrive by name. Returns Person ID or None."""
    if not name or not api_key:
        return None

    try:
        data = _pd_request("GET", "/persons/search", params={"term": name}, api_key=api_key)
        items = data.get("data", {}).get("items", [])
        if items:
            if company_name:
                norm = _normalize_company_name(company_name)
                for item in items:
                    org_name = item["item"].get("organization", {}).get("name", "") if item["item"].get("organization") else ""
                    if norm in _normalize_company_name(org_name):
                        return str(item["item"]["id"])
            return str(items[0]["item"]["id"])
    except Exception as e:
        logger.warning(f"Pipedrive person search failed: {e}")

    return None


def find_or_create_contact(name: str, email: Optional[str] = None,
                           company_name: Optional[str] = None,
                           api_key: Optional[str] = None) -> Optional[str]:
    """Find or create a Person in Pipedrive. Returns Person ID."""
    if not name or not api_key:
        return None

    # Search first
    existing = find_contact_by_name(name, company_name, api_key)
    if existing:
        return existing

    # Search by email
    if email:
        try:
            data = _pd_request("GET", "/persons/search", params={"term": email, "fields": "email"}, api_key=api_key)
            items = data.get("data", {}).get("items", [])
            if items:
                return str(items[0]["item"]["id"])
        except Exception:
            pass

    return None


# --- Deal Creation ---

def create_deal(
    score_result: dict,
    analysis: dict,
    metadata: Optional[dict] = None,
    dry_run: bool = False,
    api_key: Optional[str] = None,
) -> Optional[dict]:
    """Create a Pipedrive Deal from a scored transcript analysis."""
    deal_name = score_result.get("deal_name_suggestion", "New Deal")
    recommendation = score_result.get("recommendation", "needs_review")
    score = score_result.get("total_score", 0)

    stage = "Qualification" if recommendation == "auto_create" else "Prospecting"

    if dry_run:
        logger.info(f"[DRY RUN] Would create Pipedrive Deal: {deal_name} (stage: {stage})")
        return {"dry_run": True, "deal_name": deal_name}

    if not api_key:
        logger.error("Pipedrive: No api_key provided for deal creation")
        return None

    # Find or create company (Organization)
    company = analysis.get("prospect_company", {})
    org_id = None
    if company.get("name"):
        org_id = find_or_create_company(
            company["name"], company.get("industry"),
            company.get("domain"), api_key=api_key,
        )

    # Find contacts
    decision_makers = analysis.get("decision_makers", [])
    contact_ids = []
    for dm in decision_makers[:3]:
        dm_name = dm.get("name")
        if dm_name:
            cid = find_or_create_contact(
                dm_name, dm.get("email"), company.get("name"), api_key=api_key,
            )
            if cid:
                contact_ids.append(cid)

    # Build description
    description = _build_description(score_result, analysis, metadata)

    # Build Deal payload
    payload = {
        "title": deal_name,
    }

    if org_id:
        payload["org_id"] = int(org_id)

    if contact_ids:
        payload["person_id"] = int(contact_ids[0])

    # Amount from budget indicators
    budget = analysis.get("budget_indicators", {})
    if budget.get("range"):
        numbers = re.findall(r"\d+", budget["range"].replace(",", ""))
        if numbers:
            try:
                payload["value"] = max(int(n) for n in numbers)
            except Exception:
                pass

    try:
        data = _pd_request("POST", "/deals", payload, api_key=api_key)
        deal_id = data.get("data", {}).get("id")
        if not deal_id:
            logger.error(f"Pipedrive Deal creation returned no ID: {data}")
            return None

        deal_id = str(deal_id)

        # Add description as a note on the deal
        try:
            _pd_request("POST", "/notes", {"deal_id": int(deal_id), "content": description}, api_key=api_key)
        except Exception as e:
            logger.warning(f"Failed to add note to deal: {e}")

        logger.info(f"Created Pipedrive Deal: {deal_name} (ID: {deal_id})")

        return {
            "deal_id": deal_id,
            "deal_name": deal_name,
            "deal_url": f"https://app.pipedrive.com/deal/{deal_id}",
            "company_id": org_id,
            "associated_contacts": contact_ids,
            "stage": stage,
            "score": score,
        }
    except Exception as e:
        logger.error(f"Failed to create Pipedrive Deal '{deal_name}': {e}")
        return None


# --- Deal Stage Update ---

def update_deal_stage(deal_id: str, stage: str, api_key: Optional[str] = None) -> Optional[dict]:
    """Update a Deal's stage in Pipedrive. Stage should be a stage_id."""
    if not api_key:
        return None

    try:
        _pd_request("PUT", f"/deals/{deal_id}", {"stage_id": stage}, api_key=api_key)
        logger.info(f"Updated Pipedrive Deal {deal_id} to stage: {stage}")
        return {"deal_id": deal_id, "stage": stage}
    except Exception as e:
        logger.error(f"Failed to update Pipedrive Deal {deal_id}: {e}")
        return None


# --- Deal Lookup ---

def find_deal_by_company(company_name: str, api_key: Optional[str] = None) -> Optional[dict]:
    """Find an existing Deal by company name."""
    if not company_name or not api_key:
        return None

    try:
        data = _pd_request("GET", "/deals/search", params={"term": company_name}, api_key=api_key)
        items = data.get("data", {}).get("items", [])
        if items:
            deal = items[0]["item"]
            return {
                "deal_id": str(deal["id"]),
                "deal_name": deal.get("title", ""),
                "stage": str(deal.get("stage_id", "")),
            }
    except Exception as e:
        logger.warning(f"Pipedrive deal search failed: {e}")

    return None


# --- Query Deals by Stage ---

def query_deals_by_stage(stages: list, limit: int = 20, api_key: Optional[str] = None) -> list:
    """Query Deals by stage ID in Pipedrive."""
    if not api_key:
        return []

    results = []
    for stage_id in stages:
        try:
            data = _pd_request(
                "GET", "/deals",
                params={"stage_id": stage_id, "limit": limit, "status": "open"},
                api_key=api_key,
            )
            deals = data.get("data") or []
            for d in deals:
                results.append({
                    "deal_id": str(d["id"]),
                    "name": d.get("title", ""),
                    "stage": str(d.get("stage_id", "")),
                    "company_name": d.get("org_name", ""),
                    "close_date": d.get("expected_close_date", ""),
                    "create_date": d.get("add_time", ""),
                    "amount": str(d.get("value", "")) if d.get("value") else "",
                })
        except Exception as e:
            logger.warning(f"Pipedrive query stage {stage_id} failed: {e}")

    return results[:limit]


# --- Description Builder ---

def _build_description(score_result: dict, analysis: dict, metadata: Optional[dict]) -> str:
    """Build a description string for the Pipedrive Deal."""
    framework_name = score_result.get("framework", "custom").upper()
    score_breakdown = "\n".join(
        f"  {k}: {v['score']}/{v['max']}"
        for k, v in score_result.get("breakdown", {}).items()
    )

    next_steps_list = [
        f"- {s.get('action', '')} (owner: {s.get('owner', '?')})"
        for s in analysis.get("next_steps", [])[:3]
    ]

    return f"""FAIRPLAY | Score: {score_result['total_score']}/100 ({framework_name})

MEETING: {metadata.get('title', '?') if metadata else '?'}
DATE: {metadata.get('date', '?') if metadata else '?'}
SOURCE: Fairplay

SUMMARY: {analysis.get('summary', '')}

SCORE BREAKDOWN ({framework_name}):
{score_breakdown}

NEXT STEPS:
{chr(10).join(next_steps_list) or '  None defined'}

KEY SIGNAL: {score_result.get('key_insight', 'N/A')}"""
