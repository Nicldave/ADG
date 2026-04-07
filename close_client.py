"""
Close Client - Deal Creation
Creates Opportunities in Close CRM from scored transcript analysis.

Uses Close REST API v1 with Basic authentication.
Pass your Close API key as the api_key parameter (used as username, empty password).

Note: Close uses a combined Lead object (company + contacts). Opportunities
are attached to Leads. There is no separate Company or Contact entity.
"""

import json
import logging
import re
from datetime import datetime
from typing import Optional

import requests

logger = logging.getLogger(__name__)

CLOSE_BASE = "https://api.close.com/api/v1"


# --- Internal helpers ---

def _close_request(method: str, path: str, payload=None, params=None,
                   api_key: str = "") -> dict:
    """Execute a Close API request with Basic auth."""
    if not api_key:
        logger.error("Close: No API key provided")
        return {}

    url = f"{CLOSE_BASE}{path}"

    try:
        response = requests.request(
            method, url, json=payload, params=params,
            auth=(api_key, ""), timeout=30,
        )
        if response.status_code == 204:
            return {"success": True}
        if response.status_code not in (200, 201):
            logger.error(f"Close {method} {path} failed: {response.status_code} {response.text[:300]}")
            response.raise_for_status()
        return response.json() if response.content else {}
    except Exception as e:
        logger.error(f"Close request failed: {e}")
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


# --- Company (Lead) Lookup/Creation ---

def find_or_create_company(company_name: str, industry: Optional[str] = None,
                           domain: Optional[str] = None, api_key: Optional[str] = None) -> Optional[str]:
    """Find a Lead in Close by name, or create one. Returns Lead ID.

    In Close, a Lead represents both the company and its contacts.
    """
    if not company_name or not api_key:
        return None

    # Search by name
    try:
        data = _close_request(
            "GET", "/lead/",
            params={"query": f'name:"{company_name}"'},
            api_key=api_key,
        )
        leads = data.get("data", [])
        if leads:
            lead_id = leads[0]["id"]
            logger.info(f"Close Lead found: {company_name} (ID: {lead_id})")
            return lead_id
    except Exception as e:
        logger.warning(f"Close lead search failed: {e}")

    # Create
    try:
        payload = {"name": company_name}
        if domain:
            payload["url"] = domain if domain.startswith("http") else f"https://{domain}"

        data = _close_request("POST", "/lead/", payload, api_key=api_key)
        lead_id = data.get("id")
        if lead_id:
            logger.info(f"Close Lead created: {company_name} (ID: {lead_id})")
            return lead_id
    except Exception as e:
        logger.warning(f"Close Lead create failed for '{company_name}': {e}")

    return None


# --- Contact Lookup/Creation ---

def find_contact_by_name(name: str, company_name: Optional[str] = None,
                         api_key: Optional[str] = None) -> Optional[str]:
    """Search for a Contact within Close Leads. Returns Contact ID or None.

    In Close, contacts live inside Lead objects.
    """
    if not name or not api_key:
        return None

    try:
        data = _close_request(
            "GET", "/contact/",
            params={"query": f'name:"{name}"'},
            api_key=api_key,
        )
        contacts = data.get("data", [])
        if contacts:
            return contacts[0]["id"]
    except Exception as e:
        logger.warning(f"Close contact search failed: {e}")

    return None


def find_or_create_contact(name: str, email: Optional[str] = None,
                           company_name: Optional[str] = None,
                           api_key: Optional[str] = None) -> Optional[str]:
    """Find or create a Contact in Close. Returns Contact ID.

    Contacts in Close are nested within Leads. If a Lead exists for the company,
    the contact is added to it.
    """
    if not name or not api_key:
        return None

    # Search by name first
    existing = find_contact_by_name(name, company_name, api_key)
    if existing:
        return existing

    # Search by email
    if email:
        try:
            data = _close_request(
                "GET", "/contact/",
                params={"query": f'email:"{email}"'},
                api_key=api_key,
            )
            contacts = data.get("data", [])
            if contacts:
                return contacts[0]["id"]
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
    """Create a Close Opportunity from a scored transcript analysis."""
    deal_name = score_result.get("deal_name_suggestion", "New Deal")
    recommendation = score_result.get("recommendation", "needs_review")
    score = score_result.get("total_score", 0)

    stage = "Qualification" if recommendation == "auto_create" else "Prospecting"

    if dry_run:
        logger.info(f"[DRY RUN] Would create Close Opportunity: {deal_name} (stage: {stage})")
        return {"dry_run": True, "deal_name": deal_name}

    if not api_key:
        logger.error("Close: No api_key provided for deal creation")
        return None

    # Find or create company (Lead)
    company = analysis.get("prospect_company", {})
    lead_id = None
    if company.get("name"):
        lead_id = find_or_create_company(
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
    payload = {
        "note": f"{deal_name}\n\n{description}",
        "confidence": min(score, 100),
    }

    if lead_id:
        payload["lead_id"] = lead_id

    # Amount from budget indicators
    budget = analysis.get("budget_indicators", {})
    if budget.get("range"):
        numbers = re.findall(r"\d+", budget["range"].replace(",", ""))
        if numbers:
            try:
                payload["value"] = max(int(n) for n in numbers) * 100  # Close uses cents
                payload["value_currency"] = "USD"
                payload["value_period"] = "one_time"
            except Exception:
                pass

    try:
        data = _close_request("POST", "/opportunity/", payload, api_key=api_key)
        deal_id = data.get("id")
        if not deal_id:
            logger.error(f"Close Opportunity creation returned no ID: {data}")
            return None

        logger.info(f"Created Close Opportunity: {deal_name} (ID: {deal_id})")

        return {
            "deal_id": deal_id,
            "deal_name": deal_name,
            "deal_url": f"https://app.close.com/lead/{lead_id}" if lead_id else None,
            "company_id": lead_id,
            "associated_contacts": contact_ids,
            "stage": stage,
            "score": score,
        }
    except Exception as e:
        logger.error(f"Failed to create Close Opportunity '{deal_name}': {e}")
        return None


# --- Deal Stage Update ---

def update_deal_stage(deal_id: str, stage: str, api_key: Optional[str] = None) -> Optional[dict]:
    """Update an Opportunity's status in Close."""
    if not api_key:
        return None

    try:
        _close_request("PUT", f"/opportunity/{deal_id}/", {"status_id": stage}, api_key=api_key)
        logger.info(f"Updated Close Opportunity {deal_id} to stage: {stage}")
        return {"deal_id": deal_id, "stage": stage}
    except Exception as e:
        logger.error(f"Failed to update Close Opportunity {deal_id}: {e}")
        return None


# --- Deal Lookup ---

def find_deal_by_company(company_name: str, api_key: Optional[str] = None) -> Optional[dict]:
    """Find an existing Opportunity by company name (via Lead search)."""
    if not company_name or not api_key:
        return None

    # First find the lead
    try:
        data = _close_request(
            "GET", "/lead/",
            params={"query": f'name:"{company_name}"'},
            api_key=api_key,
        )
        leads = data.get("data", [])
        if not leads:
            return None

        lead_id = leads[0]["id"]

        # Get opportunities for this lead
        opp_data = _close_request(
            "GET", "/opportunity/",
            params={"lead_id": lead_id},
            api_key=api_key,
        )
        opps = opp_data.get("data", [])
        if opps:
            opp = opps[0]
            return {
                "deal_id": opp["id"],
                "deal_name": opp.get("note", "")[:100],
                "stage": opp.get("status_id", ""),
            }
    except Exception as e:
        logger.warning(f"Close deal search failed: {e}")

    return None


# --- Query Deals by Stage ---

def query_deals_by_stage(stages: list, limit: int = 20, api_key: Optional[str] = None) -> list:
    """Query Opportunities by status in Close."""
    if not api_key:
        return []

    results = []
    for status_id in stages:
        try:
            data = _close_request(
                "GET", "/opportunity/",
                params={"status_id": status_id, "_limit": limit},
                api_key=api_key,
            )
            opps = data.get("data", [])
            for o in opps:
                results.append({
                    "deal_id": o["id"],
                    "name": o.get("note", "")[:100],
                    "stage": o.get("status_id", ""),
                    "company_name": o.get("lead_name", ""),
                    "close_date": o.get("expected_close_date", ""),
                    "create_date": o.get("date_created", ""),
                    "amount": str(o.get("value", "")) if o.get("value") else "",
                })
        except Exception as e:
            logger.warning(f"Close query status {status_id} failed: {e}")

    return results[:limit]


# --- Description Builder ---

def _build_description(score_result: dict, analysis: dict, metadata: Optional[dict]) -> str:
    """Build a description string for the Close Opportunity."""
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
