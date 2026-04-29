"""
Zoho CRM Client - Deal Creation
Creates Deals in Zoho CRM from scored transcript analysis.

Uses Zoho CRM REST API v2 with OAuth 2.0 Bearer token authentication.
Pass your OAuth access token as the api_key parameter.

Required OAuth scopes:
  ZohoCRM.modules.deals.ALL
  ZohoCRM.modules.accounts.ALL
  ZohoCRM.modules.contacts.ALL
"""

import json
import logging
import re
from datetime import datetime
from typing import Optional

import requests

logger = logging.getLogger(__name__)

ZOHO_BASE = "https://www.zohoapis.com/crm/v2"


# --- Internal helpers ---

def _headers(api_key: str) -> dict:
    """Build Zoho auth headers."""
    if not api_key:
        logger.error("Zoho: No access token provided")
        return {}
    return {
        "Authorization": f"Zoho-oauthtoken {api_key}",
        "Content-Type": "application/json",
    }


def _zoho_request(method: str, path: str, payload=None, params=None,
                  api_key: str = "") -> dict:
    """Execute a Zoho CRM API request."""
    if not api_key:
        logger.error("Zoho: No API key provided")
        return {}

    url = f"{ZOHO_BASE}{path}"
    hdrs = _headers(api_key)
    if not hdrs:
        return {}

    try:
        response = requests.request(
            method, url, headers=hdrs, json=payload, params=params, timeout=30
        )
        if response.status_code == 204:
            return {"success": True}
        if response.status_code not in (200, 201, 202):
            logger.error(f"Zoho {method} {path} failed: {response.status_code} {response.text[:300]}")
            response.raise_for_status()
        return response.json() if response.content else {}
    except Exception as e:
        logger.error(f"Zoho request failed: {e}")
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


# --- Company (Account) Lookup/Creation ---

def find_or_create_company(company_name: str, industry: Optional[str] = None,
                           domain: Optional[str] = None, api_key: Optional[str] = None) -> Optional[str]:
    """Find an Account in Zoho CRM by name, or create one. Returns Account ID."""
    if not company_name or not api_key:
        return None

    # Search by name
    try:
        data = _zoho_request(
            "GET", "/Accounts/search",
            params={"criteria": f"(Account_Name:equals:{company_name})"},
            api_key=api_key,
        )
        records = data.get("data", [])
        if records:
            account_id = records[0]["id"]
            logger.info(f"Zoho Account found: {company_name} (ID: {account_id})")
            return account_id
    except Exception as e:
        logger.warning(f"Zoho account search failed: {e}")

    # Create
    try:
        payload = {
            "data": [{
                "Account_Name": company_name,
            }]
        }
        if industry:
            payload["data"][0]["Industry"] = industry
        if domain:
            payload["data"][0]["Website"] = domain if domain.startswith("http") else f"https://{domain}"

        data = _zoho_request("POST", "/Accounts", payload, api_key=api_key)
        records = data.get("data", [])
        if records and records[0].get("details", {}).get("id"):
            account_id = records[0]["details"]["id"]
            logger.info(f"Zoho Account created: {company_name} (ID: {account_id})")
            return account_id
    except Exception as e:
        logger.warning(f"Zoho Account create failed for '{company_name}': {e}")

    return None


# --- Contact Lookup/Creation ---

def find_contact_by_name(name: str, company_name: Optional[str] = None,
                         api_key: Optional[str] = None) -> Optional[str]:
    """Search for a Contact in Zoho CRM by name. Returns Contact ID or None."""
    if not name or not api_key:
        return None

    parts = name.strip().split()
    if len(parts) < 2:
        return None

    first = parts[0]
    last = parts[-1]

    try:
        data = _zoho_request(
            "GET", "/Contacts/search",
            params={"criteria": f"(First_Name:equals:{first})and(Last_Name:equals:{last})"},
            api_key=api_key,
        )
        records = data.get("data", [])
        if records:
            return records[0]["id"]
    except Exception as e:
        logger.warning(f"Zoho contact search failed: {e}")

    return None


def find_or_create_contact(name: str, email: Optional[str] = None,
                           company_name: Optional[str] = None,
                           api_key: Optional[str] = None) -> Optional[str]:
    """Find or create a Contact in Zoho CRM. Returns Contact ID."""
    if not name or not api_key:
        return None

    # Search by name
    existing = find_contact_by_name(name, company_name, api_key)
    if existing:
        return existing

    # Search by email
    if email:
        try:
            data = _zoho_request(
                "GET", "/Contacts/search",
                params={"criteria": f"(Email:equals:{email})"},
                api_key=api_key,
            )
            records = data.get("data", [])
            if records:
                return records[0]["id"]
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
    """Create a Zoho CRM Deal from a scored transcript analysis."""
    deal_name = score_result.get("deal_name_suggestion", "New Deal")
    recommendation = score_result.get("recommendation", "needs_review")
    score = score_result.get("total_score", 0)

    stage = "Qualification" if recommendation == "auto_create" else "Prospecting"

    if dry_run:
        logger.info(f"[DRY RUN] Would create Zoho Deal: {deal_name} (stage: {stage})")
        return {"dry_run": True, "deal_name": deal_name}

    if not api_key:
        logger.error("Zoho: No api_key provided for deal creation")
        return None

    # Find or create company (Account)
    company = analysis.get("prospect_company", {})
    account_id = None
    if company.get("name"):
        account_id = find_or_create_company(
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
    close_date = datetime.now().strftime("%Y-%m-%d")
    deal_data = {
        "Deal_Name": deal_name,
        "Stage": stage,
        "Closing_Date": close_date,
        "Description": description,
    }

    if account_id:
        deal_data["Account_Name"] = {"id": account_id}

    if contact_ids:
        deal_data["Contact_Name"] = {"id": contact_ids[0]}

    # Amount from budget indicators
    budget = analysis.get("budget_indicators", {})
    if budget.get("range"):
        numbers = re.findall(r"\d+", budget["range"].replace(",", ""))
        if numbers:
            try:
                deal_data["Amount"] = max(int(n) for n in numbers)
            except Exception:
                pass

    try:
        data = _zoho_request("POST", "/Deals", {"data": [deal_data]}, api_key=api_key)
        records = data.get("data", [])
        if not records or not records[0].get("details", {}).get("id"):
            logger.error(f"Zoho Deal creation returned no ID: {data}")
            return None

        deal_id = records[0]["details"]["id"]
        logger.info(f"Created Zoho Deal: {deal_name} (ID: {deal_id})")

        return {
            "deal_id": deal_id,
            "deal_name": deal_name,
            "deal_url": f"https://crm.zoho.com/crm/tab/Potentials/{deal_id}",
            "company_id": account_id,
            "associated_contacts": contact_ids,
            "stage": stage,
            "score": score,
        }
    except Exception as e:
        logger.error(f"Failed to create Zoho Deal '{deal_name}': {e}")
        return None


# --- Deal Stage Update ---

def update_deal_stage(deal_id: str, stage: str, api_key: Optional[str] = None) -> Optional[dict]:
    """Update a Deal's stage in Zoho CRM."""
    if not api_key:
        return None

    try:
        _zoho_request(
            "PUT", "/Deals",
            payload={"data": [{"id": deal_id, "Stage": stage}]},
            api_key=api_key,
        )
        logger.info(f"Updated Zoho Deal {deal_id} to stage: {stage}")
        return {"deal_id": deal_id, "stage": stage}
    except Exception as e:
        logger.error(f"Failed to update Zoho Deal {deal_id}: {e}")
        return None


# --- Deal Lookup ---

def find_deal_by_company(company_name: str, api_key: Optional[str] = None) -> Optional[dict]:
    """Find an existing Deal by company name."""
    if not company_name or not api_key:
        return None

    try:
        data = _zoho_request(
            "GET", "/Deals/search",
            params={"criteria": f"(Account_Name:equals:{company_name})"},
            api_key=api_key,
        )
        records = data.get("data", [])
        if records:
            r = records[0]
            return {
                "deal_id": r["id"],
                "deal_name": r.get("Deal_Name", ""),
                "stage": r.get("Stage", ""),
            }
    except Exception as e:
        logger.warning(f"Zoho deal search failed: {e}")

    return None


# --- Query Deals by Stage ---

def query_deals_by_stage(stages: list, limit: int = 20, api_key: Optional[str] = None) -> list:
    """Query Deals by stage in Zoho CRM."""
    if not api_key:
        return []

    results = []
    for stage_name in stages:
        try:
            data = _zoho_request(
                "GET", "/Deals/search",
                params={
                    "criteria": f"(Stage:equals:{stage_name})",
                    "per_page": limit,
                },
                api_key=api_key,
            )
            records = data.get("data", [])
            for r in records:
                account = r.get("Account_Name") or {}
                results.append({
                    "deal_id": r["id"],
                    "name": r.get("Deal_Name", ""),
                    "stage": r.get("Stage", ""),
                    "company_name": account.get("name", "") if isinstance(account, dict) else str(account),
                    "close_date": r.get("Closing_Date", ""),
                    "create_date": r.get("Created_Time", ""),
                    "amount": str(r.get("Amount", "")) if r.get("Amount") else "",
                })
        except Exception as e:
            logger.warning(f"Zoho query stage {stage_name} failed: {e}")

    return results[:limit]


# --- Description Builder ---

def _build_description(score_result: dict, analysis: dict, metadata: Optional[dict]) -> str:
    """Build a description string for the Zoho Deal."""
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
