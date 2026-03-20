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
from datetime import datetime
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


def _get_owner_id(api_key: Optional[str] = None) -> Optional[str]:
    """Get the workspace member ID of the token owner (for deal owner field)."""
    try:
        data = _attio_request("GET", "/self", api_key=api_key)
        return data.get("authorized_by_workspace_member_id")
    except Exception:
        return None


# --- Company Lookup/Creation ---

def find_or_create_company(company_name: str, industry: Optional[str] = None, api_key: Optional[str] = None) -> Optional[str]:
    """
    Find a company in Attio by name, or create one. Returns record ID.
    """
    if not company_name:
        return None

    # Search first
    try:
        payload = {
            "filter": {"name": {"$contains": company_name}},
            "limit": 1,
        }
        data = _attio_request("POST", "/objects/companies/records/query", payload, api_key=api_key)
        results = data.get("data", [])
        if results:
            record_id = results[0]["id"]["record_id"]
            logger.info(f"Attio company found: {company_name} (ID: {record_id})")
            return record_id
    except Exception as e:
        logger.warning(f"Attio company search failed for '{company_name}': {e}")

    # Create if not found
    try:
        data = _attio_request(
            "POST",
            "/objects/companies/records",
            {"data": {"values": {"name": [{"value": company_name}]}}},
            api_key=api_key,
        )
        record_id = data["data"]["id"]["record_id"]
        logger.info(f"Attio company created: {company_name} (ID: {record_id})")
        return record_id
    except Exception as e:
        logger.warning(f"Attio company create failed for '{company_name}': {e}")
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
            "filter": {"name": {"$contains": name}},
            "limit": 5,
        }
        data = _attio_request("POST", "/objects/people/records/query", payload, api_key=api_key)
        results = data.get("data", [])
        if results:
            return results[0]["id"]["record_id"]
    except Exception as e:
        logger.warning(f"Attio people lookup failed for '{name}': {e}")
    return None


def query_deals_by_stage(stages: list, limit: int = 50, api_key: Optional[str] = None) -> list:
    """
    Query closed deals from Attio by stage name.
    stages: list of stage names like ['Won', 'Lost', 'Former Customer']
    Returns list of dicts with deal_id, name, stage, company_name, close_date.
    """
    all_deals = []
    for stage in stages:
        try:
            payload = {
                "filter": {"stage": {"status": {"title": stage}}},
                "limit": limit,
            }
            data = _attio_request("POST", "/objects/deals/records/query", payload, api_key=api_key)
            for deal in data.get("data", []):
                record_id = deal["id"]["record_id"]
                values = deal.get("values", {})
                name_vals = values.get("name", [])
                deal_name = name_vals[0].get("value", "") if name_vals else ""
                # Extract company name from deal name (format: NN-Company-Rep-Date)
                parts = deal_name.split("-")
                company = parts[1] if len(parts) >= 2 else deal_name
                created = deal["id"].get("created_at", "")
                all_deals.append({
                    "deal_id": record_id,
                    "name": deal_name,
                    "stage": stage,
                    "company_name": company.strip(),
                    "close_date": created,
                    "create_date": created,
                    "amount": "",
                })
        except Exception as e:
            logger.warning(f"Attio deal query failed for stage '{stage}': {e}")
    logger.info(f"Found {len(all_deals)} Attio deals across stages {stages}")
    return all_deals


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

    # Build deal values
    # Owner is required — use the workspace member who authorized the API token
    owner_id = _get_owner_id(api_key)
    values = {
        "name": [{"value": deal_name}],
        "stage": [{"status": stage}],
    }
    if owner_id:
        values["owner"] = [{"referenced_actor_type": "workspace-member", "referenced_actor_id": owner_id}]

    # Amount from budget indicators
    budget = analysis.get("budget_indicators", {})
    if budget.get("range"):
        numbers = re.findall(r"\d+", budget["range"].replace(",", ""))
        if numbers:
            try:
                amount = max(int(n) for n in numbers)
                values["value"] = [{"currency_value": amount}]
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

    # Fairplay custom fields (structured data on the deal object)
    from config import (
        ATTIO_FIELD_FAIRPLAY_SCORE, ATTIO_FIELD_FRAMEWORK,
        ATTIO_FIELD_SCORED_AT, ATTIO_FIELD_AUTO_CREATED,
        ATTIO_FIELD_CREATION_METHOD, ATTIO_FIELD_BREAKDOWN,
        ATTIO_FIELD_KEY_INSIGHT,
    )
    breakdown_text = " | ".join(
        f"{d.get('label', k)}: {d['score']}/{d['max']}"
        for k, d in score_result.get("breakdown", {}).items()
    )
    fairplay_fields = {
        ATTIO_FIELD_FAIRPLAY_SCORE: [{"value": score_result["total_score"]}],
        ATTIO_FIELD_FRAMEWORK: [{"value": score_result.get("framework", "custom").upper()}],
        ATTIO_FIELD_SCORED_AT: [{"value": datetime.now().isoformat()}],
        ATTIO_FIELD_AUTO_CREATED: [{"value": True}],
        ATTIO_FIELD_CREATION_METHOD: [{"value": f"Fairplay {recommendation}"}],
        ATTIO_FIELD_KEY_INSIGHT: [{"value": score_result.get("key_insight", "")}],
        ATTIO_FIELD_BREAKDOWN: [{"value": breakdown_text}],
    }
    values.update(fairplay_fields)

    try:
        data = _attio_request(
            "POST",
            "/objects/deals/records",
            {"data": {"values": values}},
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
    except requests.exceptions.HTTPError as e:
        # If Fairplay custom fields don't exist in workspace, retry without them
        if hasattr(e, 'response') and e.response is not None and e.response.status_code in (400, 422):
            for field_slug in fairplay_fields:
                values.pop(field_slug, None)
            logger.warning("Fairplay custom fields not found in Attio workspace, retrying without them")
            try:
                data = _attio_request(
                    "POST",
                    "/objects/deals/records",
                    {"data": {"values": values}},
                    api_key=api_key,
                )
                deal_id = data["data"]["id"]["record_id"]
                logger.info(f"Created Attio deal (without Fairplay fields): {deal_name} (ID: {deal_id})")
                return {
                    "deal_id": deal_id,
                    "deal_name": deal_name,
                    "deal_url": f"https://app.attio.com/deals/{deal_id}",
                    "company_id": company_id,
                    "associated_contacts": [cid for _, cid in contact_ids],
                    "stage": stage,
                    "score": score_result["total_score"],
                }
            except Exception as e2:
                logger.error(f"Failed to create Attio deal '{deal_name}' (retry): {e2}")
                return None
        logger.error(f"Failed to create Attio deal '{deal_name}': {e}")
        return None
    except Exception as e:
        logger.error(f"Failed to create Attio deal '{deal_name}': {e}")
        return None


def find_deal_by_company(company_name: str, api_key: Optional[str] = None) -> Optional[dict]:
    """
    Search for an existing deal in Attio by company name.
    Returns the first matching deal's record_id, name, and stage, or None.
    """
    if not company_name:
        return None

    try:
        # Search deals with a filter on the deal name containing the company name
        payload = {
            "filter": {"name": {"$contains": company_name}},
            "limit": 5,
        }
        data = _attio_request("POST", "/objects/deals/records/query", payload, api_key=api_key)
        results = data.get("data", [])
        if results:
            deal = results[0]
            record_id = deal["id"]["record_id"]
            name_vals = deal.get("values", {}).get("name", [])
            deal_name = name_vals[0].get("value", "") if name_vals else ""
            stage_vals = deal.get("values", {}).get("stage", [])
            stage = stage_vals[0].get("status", {}).get("title", "") if stage_vals else ""
            logger.info(f"Found existing Attio deal for '{company_name}': {deal_name} (ID: {record_id}, stage: {stage})")
            return {
                "deal_id": record_id,
                "deal_name": deal_name,
                "stage": stage,
            }
    except Exception as e:
        logger.warning(f"Attio deal search failed for company '{company_name}': {e}")
    return None


def update_deal_stage(deal_id: str, stage: str, api_key: Optional[str] = None) -> Optional[dict]:
    """
    Update a deal's stage in Attio.

    Args:
        deal_id: Attio record ID of the deal.
        stage: Stage name (e.g., "Lost", "Discovery Scheduled").

    Returns:
        Updated deal data or None on failure.
    """
    try:
        data = _attio_request(
            "PATCH",
            f"/objects/deals/records/{deal_id}",
            {"data": {"values": {"stage": [{"status": stage}]}}},
            api_key=api_key,
        )
        logger.info(f"Updated Attio deal {deal_id} to stage: {stage}")
        return data
    except Exception as e:
        logger.error(f"Failed to update Attio deal {deal_id} stage to '{stage}': {e}")
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
