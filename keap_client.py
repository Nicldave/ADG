"""
Keap CRM Client - Deal Creation
Creates Opportunities in Keap (formerly Infusionsoft) from scored transcript analysis.

Uses Keap REST API v1 with OAuth 2.0 Bearer token authentication.
Auth: Authorization: Bearer {api_key} (OAuth access token)

Objects used:
  - Opportunity (deals)
  - Company
  - Contact
"""

import json
import logging
import re
from datetime import datetime
from typing import Optional

import requests

logger = logging.getLogger(__name__)

KEAP_API_BASE = "https://api.infusionsoft.com/crm/rest/v1"


# --- Internal helpers ---

def _headers(api_key: Optional[str] = None) -> dict:
    if not api_key:
        logger.error("Keap: No access token provided")
        return {}
    return {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
    }


def _keap_request(method: str, path: str, payload=None,
                  params=None, api_key: str = "") -> dict:
    """Execute a Keap API request."""
    url = f"{KEAP_API_BASE}{path}"
    headers = _headers(api_key)
    if not headers:
        return {}

    try:
        response = requests.request(
            method, url, headers=headers, json=payload, params=params, timeout=30
        )
        if response.status_code == 204:
            return {"success": True}
        if response.status_code not in (200, 201):
            logger.error(
                f"Keap {method} {path} failed: {response.status_code} {response.text[:300]}"
            )
            response.raise_for_status()
        return response.json() if response.content else {}
    except Exception as e:
        logger.error(f"Keap request failed: {e}")
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
                           domain: Optional[str] = None,
                           api_key: Optional[str] = None) -> Optional[str]:
    """Find a Company in Keap by name, or create one. Returns Company ID."""
    if not company_name or not api_key:
        return None

    # Search by name
    try:
        data = _keap_request(
            "GET", "/companies",
            params={"company_name": company_name, "limit": 1},
            api_key=api_key,
        )
        companies = data.get("companies", [])
        if companies:
            company_id = str(companies[0]["id"])
            logger.info(f"Keap Company found: {company_name} (ID: {company_id})")
            return company_id
    except Exception as e:
        logger.warning(f"Keap Company search failed for '{company_name}': {e}")

    # Create
    try:
        payload = {"company_name": company_name}
        if domain:
            website = domain if domain.startswith("http") else f"https://{domain}"
            payload["website"] = website

        data = _keap_request("POST", "/companies", payload, api_key=api_key)
        company_id = data.get("id")
        if company_id:
            company_id = str(company_id)
            logger.info(f"Keap Company created: {company_name} (ID: {company_id})")
            return company_id
    except Exception as e:
        logger.warning(f"Keap Company create failed for '{company_name}': {e}")

    return None


# --- Contact Lookup/Creation ---

def find_contact_by_name(name: str, company_name: Optional[str] = None,
                         api_key: Optional[str] = None) -> Optional[str]:
    """Search for a Contact in Keap by name. Returns Contact ID or None."""
    return find_or_create_contact(name, None, company_name, api_key)


def find_or_create_contact(name: str, email: Optional[str] = None,
                           company_name: Optional[str] = None,
                           api_key: Optional[str] = None) -> Optional[str]:
    """Find or create a Contact in Keap. Returns Contact ID."""
    if not name or not api_key:
        return None

    # Search by email first if available
    if email:
        try:
            data = _keap_request(
                "GET", "/contacts",
                params={"email": email, "limit": 1},
                api_key=api_key,
            )
            contacts = data.get("contacts", [])
            if contacts:
                return str(contacts[0]["id"])
        except Exception as e:
            logger.warning(f"Keap Contact email search failed: {e}")

    # Search by name
    parts = name.strip().split()
    if len(parts) >= 2:
        first = parts[0]
        last = parts[-1]
        try:
            data = _keap_request(
                "GET", "/contacts",
                params={"given_name": first, "family_name": last, "limit": 1},
                api_key=api_key,
            )
            contacts = data.get("contacts", [])
            if contacts:
                return str(contacts[0]["id"])
        except Exception as e:
            logger.warning(f"Keap Contact name search failed: {e}")

    return None


# --- Deal (Opportunity) Creation ---

def create_deal(
    score_result: dict,
    analysis: dict,
    metadata: Optional[dict] = None,
    dry_run: bool = False,
    api_key: Optional[str] = None,
) -> Optional[dict]:
    """Create a Keap Opportunity from a scored transcript analysis."""
    deal_name = score_result.get("deal_name_suggestion", "New Deal")
    recommendation = score_result.get("recommendation", "needs_review")
    score = score_result.get("total_score", 0)

    # Keap uses integer stage IDs. Default stage assignments:
    # stage 1 = Prospecting, stage 2 = Qualification (common defaults)
    stage_id = 2 if recommendation == "auto_create" else 1
    stage_label = "Qualification" if recommendation == "auto_create" else "Prospecting"

    if dry_run:
        logger.info(f"[DRY RUN] Would create Keap Opportunity: {deal_name} (stage: {stage_label})")
        return {"dry_run": True, "deal_name": deal_name}

    if not api_key:
        logger.error("Keap: No api_key provided for deal creation")
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
    close_date = datetime.now().strftime("%Y-%m-%dT23:59:59.000Z")
    payload = {
        "opportunity_title": deal_name,
        "opportunity_notes": description,
        "stage": {"id": stage_id},
        "estimated_close_date": close_date,
    }

    # Associate primary contact
    if contact_ids:
        payload["contact"] = {"id": int(contact_ids[0])}

    # Amount from budget indicators
    budget = analysis.get("budget_indicators", {})
    if budget.get("range"):
        numbers = re.findall(r"\d+", budget["range"].replace(",", ""))
        if numbers:
            try:
                payload["projected_revenue_high"] = max(int(n) for n in numbers)
            except Exception:
                pass

    try:
        data = _keap_request("POST", "/opportunities", payload, api_key=api_key)
        deal_id = data.get("id")
        if not deal_id:
            logger.error(f"Keap Opportunity creation returned no ID: {data}")
            return None

        deal_id = str(deal_id)
        logger.info(f"Created Keap Opportunity: {deal_name} (ID: {deal_id})")

        return {
            "deal_id": deal_id,
            "deal_name": deal_name,
            "deal_url": f"https://app.keap.com/opportunity/{deal_id}",
            "company_id": company_id,
            "associated_contacts": contact_ids,
            "stage": stage_label,
            "score": score,
        }
    except Exception as e:
        logger.error(f"Failed to create Keap Opportunity '{deal_name}': {e}")
        return None


# --- Deal Stage Update ---

def update_deal_stage(deal_id: str, stage: str,
                      api_key: Optional[str] = None) -> Optional[dict]:
    """Update an Opportunity's stage in Keap.

    The stage parameter should be a stage ID (integer as string) or a label.
    If a numeric string is provided, it is used as the stage ID directly.
    """
    if not api_key:
        return None

    # Determine stage_id: if numeric use directly, otherwise try common mappings
    stage_id = None
    if stage.isdigit():
        stage_id = int(stage)
    else:
        # Common stage label to ID mapping (varies by Keap account)
        stage_map = {
            "Prospecting": 1,
            "Qualification": 2,
            "Proposal": 3,
            "Negotiation": 4,
            "Closed Won": 5,
            "Closed Lost": 6,
        }
        stage_id = stage_map.get(stage)
        if stage_id is None:
            logger.error(f"Keap: Unknown stage '{stage}'. Pass a numeric stage ID.")
            return None

    try:
        _keap_request(
            "PATCH", f"/opportunities/{deal_id}",
            {"stage": {"id": stage_id}},
            api_key=api_key,
        )
        logger.info(f"Updated Keap Opportunity {deal_id} to stage: {stage}")
        return {"deal_id": deal_id, "stage": stage}
    except Exception as e:
        logger.error(f"Failed to update Keap Opportunity {deal_id}: {e}")
        return None


# --- Deal Lookup ---

def find_deal_by_company(company_name: str,
                         api_key: Optional[str] = None) -> Optional[dict]:
    """Find an existing Opportunity in Keap by searching opportunities."""
    if not company_name or not api_key:
        return None

    try:
        data = _keap_request(
            "GET", "/opportunities",
            params={"search_term": company_name, "limit": 1, "order": "date_created",
                    "order_direction": "descending"},
            api_key=api_key,
        )
        opportunities = data.get("opportunities", [])
        if opportunities:
            opp = opportunities[0]
            stage_name = ""
            if opp.get("stage"):
                stage_name = opp["stage"].get("name", str(opp["stage"].get("id", "")))
            return {
                "deal_id": str(opp["id"]),
                "deal_name": opp.get("opportunity_title", ""),
                "stage": stage_name,
            }
    except Exception as e:
        logger.error(f"Keap Opportunity search failed for '{company_name}': {e}")

    return None


# --- Query Deals by Stage ---

def query_deals_by_stage(stages: list, limit: int = 20,
                         api_key: Optional[str] = None) -> list:
    """Query Opportunities in Keap filtered by stage IDs or names."""
    if not api_key:
        return []

    results = []
    for stage in stages:
        try:
            params = {"limit": limit, "order": "date_created",
                      "order_direction": "descending"}
            # Filter by stage_id if numeric
            if str(stage).isdigit():
                params["stage_id"] = int(stage)

            data = _keap_request(
                "GET", "/opportunities", params=params, api_key=api_key,
            )
            opportunities = data.get("opportunities", [])
            for opp in opportunities:
                stage_name = ""
                if opp.get("stage"):
                    stage_name = opp["stage"].get("name",
                                                  str(opp["stage"].get("id", "")))

                # If filtering by label, skip non-matching
                if not str(stage).isdigit() and stage_name != stage:
                    continue

                contact_name = ""
                if opp.get("contact"):
                    c = opp["contact"]
                    contact_name = f"{c.get('first_name', '')} {c.get('last_name', '')}".strip()

                results.append({
                    "deal_id": str(opp["id"]),
                    "name": opp.get("opportunity_title", ""),
                    "stage": stage_name,
                    "company_name": contact_name,
                    "close_date": opp.get("estimated_close_date", ""),
                    "create_date": opp.get("date_created", ""),
                    "amount": str(opp.get("projected_revenue_high", ""))
                            if opp.get("projected_revenue_high") else "",
                })
        except Exception as e:
            logger.error(f"Keap stage query failed for '{stage}': {e}")

    return results[:limit]


# --- Description Builder ---

def _build_description(score_result: dict, analysis: dict,
                       metadata: Optional[dict]) -> str:
    """Build a description string for the Keap Opportunity."""
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
