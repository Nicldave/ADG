"""
Attio Client - Deal Creation
Creates deals in Attio CRM from scored transcript analysis.

Attio uses a "assert" (upsert) pattern: PUT /v2/objects/{object}/records
with ?matching_attribute=name creates or updates records by name.

Required Attio token scopes:
  record_permission:read-write
  object_configuration:read

Configure relationship attribute slugs via env vars if your workspace
uses non-default names (ATTIO_DEAL_COMPANY_ATTR, ATTIO_DEAL_PEOPLE_ATTR).
"""

import json
import logging
import re
from datetime import datetime, timedelta
from typing import Optional

import requests

from config import (
    ATTIO_API_KEY,
    ATTIO_BASE_URL,
    ATTIO_DEAL_STAGE_QUALIFIED,
    ATTIO_DEAL_STAGE_REVIEW,
    ATTIO_DEAL_COMPANY_ATTR,
    ATTIO_DEAL_PEOPLE_ATTR,
)

logger = logging.getLogger(__name__)


# --- Internal helpers ---

def _headers(api_key: Optional[str] = None) -> dict:
    key = api_key or ATTIO_API_KEY
    return {
        "Authorization": f"Bearer {key}",
        "Content-Type": "application/json",
    }


def _attio_request(method: str, path: str, payload=None, params=None, api_key: Optional[str] = None) -> dict:
    """Execute an Attio API v2 request."""
    url = f"{ATTIO_BASE_URL}{path}"
    response = requests.request(
        method, url, headers=_headers(api_key), json=payload, params=params, timeout=30
    )
    if response.status_code not in (200, 201):
        logger.error(
            f"Attio {method} {path} failed: {response.status_code} {response.text}"
        )
        response.raise_for_status()
    return response.json() if response.content else {}


# --- Company Lookup/Creation ---

def find_or_create_company(company_name: str, industry: Optional[str] = None, api_key: Optional[str] = None) -> Optional[str]:
    """
    Upsert a company in Attio by name. Returns record ID.
    Uses assert (PUT) pattern — creates if not found, updates if found.
    """
    if not company_name:
        return None

    values = {"name": [{"value": company_name}]}
    if industry:
        # Attio uses free-text or select for industry depending on workspace config
        values["categories"] = [{"value": industry}]

    try:
        data = _attio_request(
            "PUT",
            "/objects/companies/records",
            {"data": {"values": values}},
            params={"matching_attribute": "name"},
            api_key=api_key,
        )
        record_id = data["data"]["id"]["record_id"]
        logger.info(f"Attio company upserted: {company_name} (ID: {record_id})")
        return record_id
    except Exception as e:
        logger.warning(f"Attio company upsert failed for '{company_name}': {e}")
        return None


# --- Contact (People) Lookup ---

def find_contact_by_name(name: str, company_name: Optional[str] = None, api_key: Optional[str] = None) -> Optional[str]:
    """
    Search for a person in Attio by name. Returns record ID or None.
    Uses the records query endpoint with a name filter.
    """
    if not name:
        return None

    try:
        payload = {
            "filter": {"name": {"$str_contains": name}},
            "limit": 5,
        }
        data = _attio_request("POST", "/objects/people/records/query", payload, api_key=api_key)
        results = data.get("data", [])
        if results:
            return results[0]["id"]["record_id"]
    except Exception as e:
        logger.warning(f"Attio people lookup failed for '{name}': {e}")
    return None


# --- Deal Creation ---

def create_deal(
    score_result: dict,
    analysis: dict,
    metadata: Optional[dict] = None,
    dry_run: bool = False,
    api_key: Optional[str] = None,
) -> Optional[dict]:
    """
    Create an Attio deal from a scored transcript analysis.

    Args:
        score_result: Output from deal_scorer.score_deal()
        analysis: Output from transcript_analyzer.analyze_transcript()
        metadata: Meeting metadata (title, date, participants, etc.)
        dry_run: If True, log what would be created without making API calls.

    Returns:
        Dict with deal_id, deal_url, company_id, associated_contacts on success.
        None on failure.
    """
    deal_name = score_result.get("deal_name_suggestion", "New Deal")
    recommendation = score_result.get("recommendation", "needs_review")
    stage = (
        ATTIO_DEAL_STAGE_QUALIFIED
        if recommendation == "auto_create"
        else ATTIO_DEAL_STAGE_REVIEW
    )

    if dry_run:
        logger.info(f"[DRY RUN] Would create Attio deal: {deal_name} (stage: {stage})")
        logger.info(f"[DRY RUN] Score: {score_result.get('total_score')}/100")
        return {"dry_run": True, "deal_name": deal_name}

    # Find or create company
    company = analysis.get("prospect_company", {})
    company_id = None
    if company.get("name"):
        company_id = find_or_create_company(company["name"], company.get("industry"), api_key=api_key)

    # Find contacts before deal creation (so we can include in initial PUT)
    decision_makers = analysis.get("decision_makers", [])
    contact_ids = []
    for dm in decision_makers[:3]:
        dm_name = dm.get("name")
        if dm_name:
            contact_id = find_contact_by_name(dm_name, company.get("name"), api_key=api_key)
            if contact_id:
                contact_ids.append((dm_name, contact_id))

    # Close date
    urgency = analysis.get("timeline_indicators", {}).get("urgency", "medium")
    days_to_close = {"critical": 14, "high": 21, "medium": 30, "low": 60}.get(urgency, 30)
    close_date = (datetime.now() + timedelta(days=days_to_close)).strftime("%Y-%m-%d")

    # Build deal values
    description = _build_description(score_result, analysis, metadata)
    values = {
        "name": [{"value": deal_name}],
        "stage": [{"status": stage}],
        "close_date": [{"value": close_date}],
        "description": [{"value": description}],
    }

    # Amount from budget indicators
    budget = analysis.get("budget_indicators", {})
    if budget.get("range"):
        numbers = re.findall(r"\d+", budget["range"].replace(",", ""))
        if numbers:
            try:
                amount = max(int(n) for n in numbers)
                values["value"] = [{"currency_value": amount, "currency_code": "USD"}]
            except Exception:
                pass

    # Company association (inline during creation)
    if company_id and ATTIO_DEAL_COMPANY_ATTR:
        values[ATTIO_DEAL_COMPANY_ATTR] = [
            {"target_object": "companies", "target_record_id": company_id}
        ]

    # People associations (inline during creation)
    if contact_ids and ATTIO_DEAL_PEOPLE_ATTR:
        values[ATTIO_DEAL_PEOPLE_ATTR] = [
            {"target_object": "people", "target_record_id": cid}
            for _, cid in contact_ids
        ]

    try:
        data = _attio_request(
            "PUT",
            "/objects/deals/records",
            {"data": {"values": values}},
            params={"matching_attribute": "name"},
            api_key=api_key,
        )
        deal_id = data["data"]["id"]["record_id"]
        logger.info(f"Created Attio deal: {deal_name} (ID: {deal_id})")

        for name, cid in contact_ids:
            logger.info(f"Associated contact {name} ({cid}) with Attio deal")

        return {
            "deal_id": deal_id,
            "deal_name": deal_name,
            "deal_url": f"https://app.attio.com/deals/{deal_id}",
            "company_id": company_id,
            "associated_contacts": [cid for _, cid in contact_ids],
            "stage": stage,
            "score": score_result["total_score"],
        }
    except Exception as e:
        logger.error(f"Failed to create Attio deal '{deal_name}': {e}")
        return None


def _build_description(score_result: dict, analysis: dict, metadata: Optional[dict]) -> str:
    """Build a description string for the Attio deal record."""
    pain_quotes = [
        f"- [{s.get('category', '?')}] \"{s.get('quote', '')}\" (severity {s.get('severity', '?')}/5)"
        for s in analysis.get("pain_signals", [])[:5]
    ]
    buying_signals = [
        f"- [{s.get('strength', '?')}] {s.get('signal', '')} -- \"{s.get('evidence', '')}\""
        for s in analysis.get("buying_signals", [])[:3]
    ]
    objections_list = [
        f"- \"{o.get('objection', '')}\" -> {'Resolved' if o.get('resolved') else 'Unresolved'}"
        + (f" ({o.get('response', '')})" if o.get('response') else "")
        for o in analysis.get("objections", [])[:5]
    ]
    next_steps_list = [
        f"- {s.get('action', '')} (owner: {s.get('owner', '?')}, deadline: {s.get('deadline', 'TBD')})"
        for s in analysis.get("next_steps", [])[:3]
    ]
    score_breakdown = "\n".join(
        f"  {k}: {v['score']}/{v['max']}"
        for k, v in score_result.get("breakdown", {}).items()
    )

    framework_name = score_result.get("framework", "custom").upper()
    recording_url = metadata.get("recording_url", "") if metadata else ""

    return f"""AUTO DEAL GENERATOR | Score: {score_result['total_score']}/100 ({framework_name})

MEETING: {metadata.get('title', '?') if metadata else '?'}
DATE: {metadata.get('date', '?') if metadata else '?'}
SOURCE: Auto Deal Generator
{f'RECORDING: {recording_url}' if recording_url else ''}
SUMMARY: {analysis.get('summary', '')}

PAIN SIGNALS:
{chr(10).join(pain_quotes) or '  None identified'}

BUYING SIGNALS:
{chr(10).join(buying_signals) or '  None identified'}

OBJECTIONS:
{chr(10).join(objections_list) or '  None raised'}

NEXT STEPS:
{chr(10).join(next_steps_list) or '  None defined'}

DEAL SCORE BREAKDOWN ({framework_name}):
{score_breakdown}

KEY SIGNAL: {score_result.get('key_insight', 'N/A')}"""
