"""
HubSpot Client - Transcript Ingestion + Deal Creation
Pulls call transcripts from HubSpot CRM and creates deals.
Extends the pattern from clients/ascent-cfo/hubspot_transcript_fetcher.py.

Required HubSpot token scopes:
  crm.objects.calls.read    (for pulling transcripts)
  crm.objects.deals.write
  crm.objects.deals.read
  crm.objects.contacts.read
  crm.objects.companies.read
  crm.objects.notes.write
"""

import json
import logging
from datetime import datetime, timedelta
from typing import Optional

import requests

from config import (
    HUBSPOT_API_KEY,
    HUBSPOT_BASE_URL,
    HUBSPOT_PIPELINE_ID,
    HUBSPOT_STAGE_QUALIFIED,
    HUBSPOT_STAGE_REVIEW,
)

logger = logging.getLogger(__name__)

CALL_PROPERTIES = (
    "hs_call_title,hs_call_body,hs_call_duration,hs_call_status,"
    "hs_call_disposition,hs_call_recording_url,hs_timestamp,"
    "hs_call_from_number,hs_call_to_number,hubspot_owner_id,hs_createdate"
)


# --- Transcript Ingestion ---

def list_calls(since: Optional[datetime] = None, limit: int = 20) -> list[dict]:
    """
    List recent calls from HubSpot CRM.

    Args:
        since: Only return calls after this datetime.
        limit: Max number of calls to return.

    Returns:
        List of call summaries.
    """
    url = f"{HUBSPOT_BASE_URL}/crm/v3/objects/calls"
    params = {"limit": limit, "properties": CALL_PROPERTIES}
    headers = _headers()

    all_calls = []
    while True:
        response = requests.get(url, headers=headers, params=params)
        response.raise_for_status()
        data = response.json()
        all_calls.extend(data.get("results", []))

        next_link = data.get("paging", {}).get("next", {}).get("link")
        if not next_link or len(all_calls) >= limit:
            break
        url = next_link
        params = {}

    # Filter by date if provided
    if since:
        since_ms = since.timestamp() * 1000
        all_calls = [
            c for c in all_calls
            if _call_timestamp_ms(c) >= since_ms
        ]

    logger.info(f"Found {len(all_calls)} HubSpot calls")
    return all_calls


def query_deals_by_stage(stages: list, limit: int = 50, api_key: Optional[str] = None) -> list:
    """
    Query closed deals from HubSpot by deal stage.
    stages: list of stage IDs like ['closedwon', 'closedlost']
    Returns list of dicts with deal_id, name, stage, company_name, close_date, create_date, amount.
    """
    all_deals = []
    for stage in stages:
        try:
            payload = {
                "filterGroups": [{
                    "filters": [{
                        "propertyName": "dealstage",
                        "operator": "EQ",
                        "value": stage,
                    }]
                }],
                "properties": ["dealname", "dealstage", "closedate", "createdate", "amount", "pipeline"],
                "limit": limit,
                "sorts": [{"propertyName": "closedate", "direction": "DESCENDING"}],
            }
            url = f"{HUBSPOT_BASE_URL}/crm/v3/objects/deals/search"
            response = requests.post(url, headers=_headers(api_key), json=payload, timeout=30)
            if response.status_code != 200:
                logger.warning(f"HubSpot deal search failed for stage '{stage}': {response.status_code} {response.text}")
                continue
            data = response.json()
            for deal in data.get("results", []):
                props = deal.get("properties", {})
                all_deals.append({
                    "deal_id": deal["id"],
                    "name": props.get("dealname", ""),
                    "stage": stage,
                    "company_name": props.get("dealname", "").split(" - ")[0] if " - " in props.get("dealname", "") else props.get("dealname", ""),
                    "close_date": props.get("closedate", ""),
                    "create_date": props.get("createdate", ""),
                    "amount": props.get("amount", ""),
                })
        except Exception as e:
            logger.error(f"HubSpot deal query failed for stage '{stage}': {e}")
    logger.info(f"Found {len(all_deals)} HubSpot deals across stages {stages}")
    return all_deals


def get_call(call_id: str) -> dict:
    """Fetch a single call record by ID."""
    url = f"{HUBSPOT_BASE_URL}/crm/v3/objects/calls/{call_id}"
    response = requests.get(
        url, headers=_headers(), params={"properties": CALL_PROPERTIES}
    )
    response.raise_for_status()
    return response.json()


def format_hubspot_transcript(call: dict) -> str:
    """
    Extract transcript text from a HubSpot call record.
    HubSpot stores transcripts in hs_call_body (notes/AI transcript).
    """
    props = call.get("properties", {})
    body = props.get("hs_call_body", "")
    if not body:
        return ""
    return body.strip()


def get_call_metadata(call: dict) -> dict:
    """Extract meeting metadata from a HubSpot call record."""
    props = call.get("properties", {})
    ts_ms = _call_timestamp_ms(call)
    date = datetime.fromtimestamp(ts_ms / 1000).isoformat() if ts_ms else None
    duration_ms = props.get("hs_call_duration")

    return {
        "title": props.get("hs_call_title") or "HubSpot Call",
        "date": date,
        "duration_minutes": round(int(duration_ms) / 60000, 1) if duration_ms else 0,
        "participants": [],  # HubSpot requires association lookup for contacts
        "organizer": "",
        "source": "hubspot",
        "call_id": call.get("id"),
        "recording_url": props.get("hs_call_recording_url", ""),
        "disposition": props.get("hs_call_disposition", ""),
    }


def _call_timestamp_ms(call: dict) -> float:
    """Extract call timestamp in milliseconds."""
    props = call.get("properties", {})
    ts = props.get("hs_timestamp") or props.get("hs_createdate")
    if ts:
        try:
            return float(ts)
        except (ValueError, TypeError):
            pass
    return 0.0


def _headers(api_key: Optional[str] = None) -> dict:
    key = api_key or HUBSPOT_API_KEY
    return {
        "Authorization": f"Bearer {key}",
        "Content-Type": "application/json",
    }


def _hs_request(method: str, path: str, payload=None, api_key: Optional[str] = None) -> dict:
    """Execute a HubSpot CRM API request. Payload can be dict or list."""
    url = f"{HUBSPOT_BASE_URL}{path}"
    response = requests.request(method, url, headers=_headers(api_key), json=payload)

    if response.status_code not in (200, 201):
        logger.error(f"HubSpot {method} {path} failed: {response.status_code} {response.text}")
        response.raise_for_status()

    return response.json() if response.content else {}


# --- Company Lookup ---

def find_company(company_name: str, api_key: Optional[str] = None) -> Optional[str]:
    """
    Search for an existing company in HubSpot by name.
    Returns the company ID if found, None otherwise.
    """
    payload = {
        "filterGroups": [
            {
                "filters": [
                    {"propertyName": "name", "operator": "EQ", "value": company_name}
                ]
            }
        ],
        "properties": ["name", "domain"],
        "limit": 1,
    }
    try:
        data = _hs_request("POST", "/crm/v3/objects/companies/search", payload, api_key=api_key)
        results = data.get("results", [])
        if results:
            company_id = results[0]["id"]
            logger.info(f"Found existing company: {company_name} (ID: {company_id})")
            return company_id
    except Exception as e:
        logger.warning(f"Company lookup failed for '{company_name}': {e}")
    return None


def create_company(company_name: str, industry: Optional[str] = None, api_key: Optional[str] = None) -> str:
    """Create a new company in HubSpot. Returns the new company ID."""
    properties = {"name": company_name}
    if industry:
        properties["industry"] = industry

    data = _hs_request("POST", "/crm/v3/objects/companies", {"properties": properties}, api_key=api_key)
    company_id = data["id"]
    logger.info(f"Created company: {company_name} (ID: {company_id})")
    return company_id


def find_or_create_company(company_name: str, industry: Optional[str] = None, api_key: Optional[str] = None) -> str:
    """Find an existing company or create a new one. Returns company ID."""
    if not company_name:
        return None
    existing = find_company(company_name, api_key=api_key)
    if existing:
        return existing
    return create_company(company_name, industry, api_key=api_key)


# --- Contact Lookup ---

def find_contact_by_email(email: str) -> Optional[str]:
    """Find a HubSpot contact by email. Returns contact ID or None."""
    try:
        data = _hs_request("GET", f"/crm/v3/objects/contacts/{email}?idProperty=email")
        return data["id"]
    except Exception:
        return None


def find_contact_by_name(name: str, company_name: Optional[str] = None, api_key: Optional[str] = None) -> Optional[str]:
    """Search for a contact by name (and optionally company)."""
    filters = [{"propertyName": "firstname", "operator": "CONTAINS_TOKEN", "value": name.split()[0]}]
    if len(name.split()) > 1:
        filters.append({"propertyName": "lastname", "operator": "EQ", "value": name.split()[-1]})

    payload = {
        "filterGroups": [{"filters": filters}],
        "properties": ["firstname", "lastname", "email", "company"],
        "limit": 5,
    }
    try:
        data = _hs_request("POST", "/crm/v3/objects/contacts/search", payload, api_key=api_key)
        results = data.get("results", [])
        if results:
            # If company specified, try to match
            if company_name:
                for r in results:
                    if company_name.lower() in (r["properties"].get("company") or "").lower():
                        return r["id"]
            return results[0]["id"]
    except Exception as e:
        logger.warning(f"Contact lookup failed for '{name}': {e}")
    return None


# --- Deal Creation ---

def _build_deal_properties(
    score_result: dict,
    analysis: dict,
    metadata: Optional[dict],
    stage_override: Optional[str] = None,
) -> dict:
    """Build the HubSpot deal properties dict from score and analysis data."""
    recommendation = score_result.get("recommendation", "needs_review")
    stage = stage_override or (
        HUBSPOT_STAGE_QUALIFIED if recommendation == "auto_create" else HUBSPOT_STAGE_REVIEW
    )

    # Deal name
    deal_name = score_result.get("deal_name_suggestion", "New Deal")

    # Close date: 30 days out as default, adjusted by timeline urgency
    urgency = analysis.get("timeline_indicators", {}).get("urgency", "medium")
    days_to_close = {"critical": 14, "high": 21, "medium": 30, "low": 60}.get(urgency, 30)
    close_date = (datetime.now() + timedelta(days=days_to_close)).strftime("%Y-%m-%d")

    # Amount: try to extract from budget indicators
    budget = analysis.get("budget_indicators", {})
    amount = None
    if budget.get("range"):
        # Parse rough amount from budget range string (e.g., "$5,000-$10,000")
        import re
        numbers = re.findall(r"[\d,]+", budget["range"].replace(",", ""))
        if numbers:
            try:
                amount = str(max(int(n) for n in numbers))
            except Exception:
                pass

    # Summary note
    pain_quotes = [
        f"- [{s.get('category', '?')}] \"{s.get('quote', '')}\" (severity {s.get('severity', '?')}/5)"
        for s in analysis.get("pain_signals", [])[:5]
    ]
    buying_signals = [
        f"- [{s.get('strength', '?')}] {s.get('signal', '')} — \"{s.get('evidence', '')}\""
        for s in analysis.get("buying_signals", [])[:3]
    ]
    next_steps_list = [
        f"- {s.get('action', '')} (owner: {s.get('owner', '?')}, deadline: {s.get('deadline', 'TBD')})"
        for s in analysis.get("next_steps", [])[:3]
    ]

    score_breakdown = "\n".join(
        f"  {k}: {v['score']}/{v['max']}"
        for k, v in score_result.get("breakdown", {}).items()
    )

    objections_list = [
        f"- \"{o.get('objection', '')}\" → {'Resolved' if o.get('resolved') else 'Unresolved'}"
        + (f" ({o.get('response', '')})" if o.get('response') else "")
        for o in analysis.get("objections", [])[:5]
    ]

    framework_name = score_result.get("framework", "custom").upper()
    recording_url = metadata.get("recording_url", "") if metadata else ""

    description = f"""AUTO DEAL GENERATOR | Score: {score_result['total_score']}/100 ({framework_name})

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

KEY SIGNAL: {score_result.get('key_insight', 'N/A')}
"""

    properties = {
        "dealname": deal_name,
        "pipeline": HUBSPOT_PIPELINE_ID,
        "dealstage": stage,
        "closedate": close_date,
        "description": description,
    }
    if amount:
        properties["amount"] = amount

    return properties


def create_deal(
    score_result: dict,
    analysis: dict,
    metadata: Optional[dict] = None,
    dry_run: bool = False,
    api_key: Optional[str] = None,
) -> Optional[dict]:
    """
    Create a HubSpot deal from a scored transcript analysis.

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

    if dry_run:
        properties = _build_deal_properties(score_result, analysis, metadata)
        logger.info(f"[DRY RUN] Would create deal: {deal_name}")
        logger.info(f"[DRY RUN] Properties: {json.dumps(properties, indent=2)}")
        return {"dry_run": True, "deal_name": deal_name}

    # Find or create company
    company = analysis.get("prospect_company", {})
    company_id = None
    if company.get("name"):
        company_id = find_or_create_company(
            company["name"], company.get("industry"), api_key=api_key
        )

    # Build deal properties
    properties = _build_deal_properties(score_result, analysis, metadata)

    # Create the deal
    deal_data = _hs_request(
        "POST", "/crm/v3/objects/deals", {"properties": properties}, api_key=api_key
    )
    deal_id = deal_data["id"]
    logger.info(f"Created deal: {deal_name} (ID: {deal_id})")

    # Associate company (v4 API — requires associationTypeId in body)
    # Type ID 5 = Deal to Company (HubSpot defined)
    if company_id:
        try:
            _hs_request(
                "PUT",
                f"/crm/v4/objects/deal/{deal_id}/associations/company/{company_id}",
                [{"associationCategory": "HUBSPOT_DEFINED", "associationTypeId": 5}],
                api_key=api_key,
            )
            logger.info(f"Associated deal {deal_id} with company {company_id}")
        except Exception as e:
            logger.warning(f"Company association failed: {e}")

    # Find and associate contacts (v4 API)
    # Type ID 3 = Deal to Contact (HubSpot defined)
    associated_contacts = []
    decision_makers = analysis.get("decision_makers", [])
    for dm in decision_makers[:3]:  # Max 3 contacts
        name = dm.get("name")
        if not name:
            continue
        contact_id = find_contact_by_name(name, company.get("name"), api_key=api_key)
        if contact_id:
            try:
                _hs_request(
                    "PUT",
                    f"/crm/v4/objects/deal/{deal_id}/associations/contact/{contact_id}",
                    [{"associationCategory": "HUBSPOT_DEFINED", "associationTypeId": 3}],
                    api_key=api_key,
                )
                associated_contacts.append(contact_id)
                logger.info(f"Associated contact {name} ({contact_id}) with deal")
            except Exception as e:
                logger.warning(f"Contact association failed for {name}: {e}")

    portal_id_guess = "your_portal"  # Replace with actual portal ID if known
    return {
        "deal_id": deal_id,
        "deal_name": deal_name,
        "deal_url": f"https://app.hubspot.com/contacts/{portal_id_guess}/deal/{deal_id}",
        "company_id": company_id,
        "associated_contacts": associated_contacts,
        "stage": properties["dealstage"],
        "score": score_result["total_score"],
    }
