"""
Copper Client - Deal Creation
Creates Opportunities in Copper CRM (formerly ProsperWorks) from scored transcript analysis.

Uses Copper Developer API v1 with token-based authentication.
Pass api_key in format: user_email|access_token

Required headers on every request:
  X-PW-AccessToken: <access_token>
  X-PW-Application: developer_api
  X-PW-UserEmail: <user_email>
"""

import json
import logging
import re
from datetime import datetime
from typing import Optional

import requests

logger = logging.getLogger(__name__)

COPPER_BASE = "https://api.copper.com/developer_api/v1"


# --- Internal helpers ---

def _parse_api_key(api_key: str) -> tuple:
    """Parse combined key format: email|token"""
    if "|" in api_key:
        parts = api_key.split("|", 1)
        return parts[0].strip(), parts[1].strip()
    logger.error("Copper: api_key must be in format 'email|token'")
    return "", api_key


def _headers(api_key: str) -> dict:
    """Build Copper auth headers."""
    email, token = _parse_api_key(api_key)
    if not email or not token:
        logger.error("Copper: Invalid api_key format")
        return {}
    return {
        "X-PW-AccessToken": token,
        "X-PW-Application": "developer_api",
        "X-PW-UserEmail": email,
        "Content-Type": "application/json",
    }


def _copper_request(method: str, path: str, payload=None, params=None,
                    api_key: str = "") -> dict:
    """Execute a Copper API request."""
    if not api_key:
        logger.error("Copper: No API key provided")
        return {}

    url = f"{COPPER_BASE}{path}"
    hdrs = _headers(api_key)
    if not hdrs:
        return {}

    try:
        response = requests.request(
            method, url, headers=hdrs, json=payload, params=params, timeout=30
        )
        if response.status_code == 204:
            return {"success": True}
        if response.status_code not in (200, 201):
            logger.error(f"Copper {method} {path} failed: {response.status_code} {response.text[:300]}")
            response.raise_for_status()
        return response.json() if response.content else {}
    except Exception as e:
        logger.error(f"Copper request failed: {e}")
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


# --- Company Lookup/Creation ---

def find_or_create_company(company_name: str, industry: Optional[str] = None,
                           domain: Optional[str] = None, api_key: Optional[str] = None) -> Optional[str]:
    """Find a Company in Copper by name, or create one. Returns Company ID."""
    if not company_name or not api_key:
        return None

    # Search
    try:
        data = _copper_request(
            "POST", "/companies/search",
            payload={"name": company_name},
            api_key=api_key,
        )
        # Search returns a list
        if isinstance(data, list) and data:
            company_id = str(data[0]["id"])
            logger.info(f"Copper Company found: {company_name} (ID: {company_id})")
            return company_id
    except Exception as e:
        logger.warning(f"Copper company search failed: {e}")

    # Create
    try:
        payload = {"name": company_name}
        if domain:
            payload["email_domain"] = domain.replace("https://", "").replace("http://", "").split("/")[0]

        data = _copper_request("POST", "/companies", payload, api_key=api_key)
        company_id = data.get("id")
        if company_id:
            company_id = str(company_id)
            logger.info(f"Copper Company created: {company_name} (ID: {company_id})")
            return company_id
    except Exception as e:
        logger.warning(f"Copper Company create failed for '{company_name}': {e}")

    return None


# --- Contact (People) Lookup/Creation ---

def find_contact_by_name(name: str, company_name: Optional[str] = None,
                         api_key: Optional[str] = None) -> Optional[str]:
    """Search for a Person in Copper by name. Returns Person ID or None."""
    if not name or not api_key:
        return None

    try:
        data = _copper_request(
            "POST", "/people/search",
            payload={"name": name},
            api_key=api_key,
        )
        if isinstance(data, list) and data:
            if company_name:
                norm = _normalize_company_name(company_name)
                for person in data:
                    p_company = person.get("company_name", "") or ""
                    if norm in _normalize_company_name(p_company):
                        return str(person["id"])
            return str(data[0]["id"])
    except Exception as e:
        logger.warning(f"Copper people search failed: {e}")

    return None


def find_or_create_contact(name: str, email: Optional[str] = None,
                           company_name: Optional[str] = None,
                           api_key: Optional[str] = None) -> Optional[str]:
    """Find or create a Person in Copper. Returns Person ID."""
    if not name or not api_key:
        return None

    # Search by name
    existing = find_contact_by_name(name, company_name, api_key)
    if existing:
        return existing

    # Search by email
    if email:
        try:
            data = _copper_request(
                "POST", "/people/search",
                payload={"emails": [email]},
                api_key=api_key,
            )
            if isinstance(data, list) and data:
                return str(data[0]["id"])
        except Exception:
            pass

    return None


# --- Deal (Opportunity) Creation ---

def create_deal(
    score_result: dict,
    analysis: dict,
    metadata: Optional[dict] = None,
    dry_run: bool = False,
    api_key: Optional[str] = None,
) -> Optional[dict]:
    """Create a Copper Opportunity from a scored transcript analysis."""
    deal_name = score_result.get("deal_name_suggestion", "New Deal")
    recommendation = score_result.get("recommendation", "needs_review")
    score = score_result.get("total_score", 0)

    stage = "Qualification" if recommendation == "auto_create" else "Prospecting"

    if dry_run:
        logger.info(f"[DRY RUN] Would create Copper Opportunity: {deal_name} (stage: {stage})")
        return {"dry_run": True, "deal_name": deal_name}

    if not api_key:
        logger.error("Copper: No api_key provided for deal creation")
        return None

    # Find or create company
    company = analysis.get("prospect_company", {})
    company_id = None
    if company.get("name"):
        company_id = find_or_create_company(
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

    # Build Opportunity payload
    close_date_epoch = int(datetime.now().timestamp())
    payload = {
        "name": deal_name,
        "close_date": close_date_epoch,
        "details": description,
    }

    if company_id:
        payload["company_id"] = int(company_id)

    if contact_ids:
        payload["primary_contact_id"] = int(contact_ids[0])

    # Amount from budget indicators
    budget = analysis.get("budget_indicators", {})
    if budget.get("range"):
        numbers = re.findall(r"\d+", budget["range"].replace(",", ""))
        if numbers:
            try:
                payload["monetary_value"] = max(int(n) for n in numbers)
            except Exception:
                pass

    try:
        data = _copper_request("POST", "/opportunities", payload, api_key=api_key)
        deal_id = data.get("id")
        if not deal_id:
            logger.error(f"Copper Opportunity creation returned no ID: {data}")
            return None

        deal_id = str(deal_id)
        logger.info(f"Created Copper Opportunity: {deal_name} (ID: {deal_id})")

        return {
            "deal_id": deal_id,
            "deal_name": deal_name,
            "deal_url": f"https://app.copper.com/companies/app/opportunity/{deal_id}",
            "company_id": company_id,
            "associated_contacts": contact_ids,
            "stage": stage,
            "score": score,
        }
    except Exception as e:
        logger.error(f"Failed to create Copper Opportunity '{deal_name}': {e}")
        return None


# --- Deal Stage Update ---

def update_deal_stage(deal_id: str, stage: str, api_key: Optional[str] = None) -> Optional[dict]:
    """Update an Opportunity's pipeline stage in Copper."""
    if not api_key:
        return None

    try:
        _copper_request(
            "PUT", f"/opportunities/{deal_id}",
            payload={"pipeline_stage_id": stage},
            api_key=api_key,
        )
        logger.info(f"Updated Copper Opportunity {deal_id} to stage: {stage}")
        return {"deal_id": deal_id, "stage": stage}
    except Exception as e:
        logger.error(f"Failed to update Copper Opportunity {deal_id}: {e}")
        return None


# --- Deal Lookup ---

def find_deal_by_company(company_name: str, api_key: Optional[str] = None) -> Optional[dict]:
    """Find an existing Opportunity by company name."""
    if not company_name or not api_key:
        return None

    try:
        data = _copper_request(
            "POST", "/opportunities/search",
            payload={"company_name": company_name},
            api_key=api_key,
        )
        if isinstance(data, list) and data:
            opp = data[0]
            return {
                "deal_id": str(opp["id"]),
                "deal_name": opp.get("name", ""),
                "stage": str(opp.get("pipeline_stage_id", "")),
            }
    except Exception as e:
        logger.warning(f"Copper opportunity search failed: {e}")

    return None


# --- Query Deals by Stage ---

def query_deals_by_stage(stages: list, limit: int = 20, api_key: Optional[str] = None) -> list:
    """Query Opportunities by pipeline stage in Copper."""
    if not api_key:
        return []

    results = []
    for stage_id in stages:
        try:
            data = _copper_request(
                "POST", "/opportunities/search",
                payload={"pipeline_stage_ids": [int(stage_id)], "page_size": limit},
                api_key=api_key,
            )
            opps = data if isinstance(data, list) else []
            for o in opps:
                results.append({
                    "deal_id": str(o["id"]),
                    "name": o.get("name", ""),
                    "stage": str(o.get("pipeline_stage_id", "")),
                    "company_name": o.get("company_name", ""),
                    "close_date": str(o.get("close_date", "")),
                    "create_date": str(o.get("date_created", "")),
                    "amount": str(o.get("monetary_value", "")) if o.get("monetary_value") else "",
                })
        except Exception as e:
            logger.warning(f"Copper query stage {stage_id} failed: {e}")

    return results[:limit]


# --- Description Builder ---

def _build_description(score_result: dict, analysis: dict, metadata: Optional[dict]) -> str:
    """Build a description string for the Copper Opportunity."""
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
SOURCE: Fairplay Auto Deal Generator

SUMMARY: {analysis.get('summary', '')}

SCORE BREAKDOWN ({framework_name}):
{score_breakdown}

NEXT STEPS:
{chr(10).join(next_steps_list) or '  None defined'}

KEY SIGNAL: {score_result.get('key_insight', 'N/A')}"""
