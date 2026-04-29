"""
Freshsales Client - Deal Creation
Creates Deals in Freshsales (Freshworks CRM) from scored transcript analysis.

Uses Freshsales REST API with token-based authentication.
Pass api_key in format: domain|token (domain is your Freshworks subdomain).

Required header: Authorization: Token token=<api_token>
API base: https://<domain>.myfreshworks.com/crm/sales/api
"""

import json
import logging
import re
from datetime import datetime
from typing import Optional

import requests

logger = logging.getLogger(__name__)


# --- Internal helpers ---

def _parse_api_key(api_key: str) -> tuple:
    """Parse combined key format: domain|token"""
    if "|" in api_key:
        parts = api_key.split("|", 1)
        return parts[0].strip(), parts[1].strip()
    logger.error("Freshsales: api_key must be in format 'domain|token'")
    return "", api_key


def _base_url(domain: str) -> str:
    """Build the Freshsales API base URL."""
    return f"https://{domain}.myfreshworks.com/crm/sales/api"


def _headers(token: str) -> dict:
    """Build Freshsales auth headers."""
    if not token:
        logger.error("Freshsales: No token provided")
        return {}
    return {
        "Authorization": f"Token token={token}",
        "Content-Type": "application/json",
    }


def _fs_request(method: str, domain: str, path: str, payload=None, params=None,
                token: str = "") -> dict:
    """Execute a Freshsales API request."""
    if not domain or not token:
        logger.error("Freshsales: Missing domain or token")
        return {}

    url = f"{_base_url(domain)}{path}"
    hdrs = _headers(token)
    if not hdrs:
        return {}

    try:
        response = requests.request(
            method, url, headers=hdrs, json=payload, params=params, timeout=30
        )
        if response.status_code == 204:
            return {"success": True}
        if response.status_code not in (200, 201):
            logger.error(f"Freshsales {method} {path} failed: {response.status_code} {response.text[:300]}")
            response.raise_for_status()
        return response.json() if response.content else {}
    except Exception as e:
        logger.error(f"Freshsales request failed: {e}")
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


# --- Company (Sales Account) Lookup/Creation ---

def find_or_create_company(company_name: str, industry: Optional[str] = None,
                           domain: Optional[str] = None, api_key: Optional[str] = None) -> Optional[str]:
    """Find a Sales Account in Freshsales by name, or create one. Returns Account ID."""
    if not company_name or not api_key:
        return None

    fs_domain, token = _parse_api_key(api_key)
    if not fs_domain:
        logger.error("Freshsales: No domain in api_key")
        return None

    # Search by name (lookup endpoint)
    try:
        data = _fs_request(
            "GET", fs_domain, "/lookup",
            params={"q": company_name, "f": "name", "entities": "sales_account"},
            token=token,
        )
        accounts = data.get("sales_accounts", {}).get("sales_accounts", [])
        if accounts:
            account_id = str(accounts[0]["id"])
            logger.info(f"Freshsales Account found: {company_name} (ID: {account_id})")
            return account_id
    except Exception as e:
        logger.warning(f"Freshsales account search failed: {e}")

    # Create
    try:
        payload = {
            "sales_account": {
                "name": company_name,
            }
        }
        if industry:
            payload["sales_account"]["industry_type"] = {"name": industry}
        if domain:
            website = domain if domain.startswith("http") else f"https://{domain}"
            payload["sales_account"]["website"] = website

        data = _fs_request("POST", fs_domain, "/sales_accounts", payload, token=token)
        account = data.get("sales_account", {})
        account_id = account.get("id")
        if account_id:
            account_id = str(account_id)
            logger.info(f"Freshsales Account created: {company_name} (ID: {account_id})")
            return account_id
    except Exception as e:
        logger.warning(f"Freshsales Account create failed for '{company_name}': {e}")

    return None


# --- Contact Lookup/Creation ---

def find_contact_by_name(name: str, company_name: Optional[str] = None,
                         api_key: Optional[str] = None) -> Optional[str]:
    """Search for a Contact in Freshsales by name. Returns Contact ID or None."""
    if not name or not api_key:
        return None

    fs_domain, token = _parse_api_key(api_key)
    if not fs_domain:
        return None

    try:
        data = _fs_request(
            "GET", fs_domain, "/lookup",
            params={"q": name, "f": "name", "entities": "contact"},
            token=token,
        )
        contacts = data.get("contacts", {}).get("contacts", [])
        if contacts:
            if company_name:
                norm = _normalize_company_name(company_name)
                for c in contacts:
                    c_company = c.get("sales_account", {}).get("name", "") if c.get("sales_account") else ""
                    if norm in _normalize_company_name(c_company):
                        return str(c["id"])
            return str(contacts[0]["id"])
    except Exception as e:
        logger.warning(f"Freshsales contact search failed: {e}")

    return None


def find_or_create_contact(name: str, email: Optional[str] = None,
                           company_name: Optional[str] = None,
                           api_key: Optional[str] = None) -> Optional[str]:
    """Find or create a Contact in Freshsales. Returns Contact ID."""
    if not name or not api_key:
        return None

    # Search by name
    existing = find_contact_by_name(name, company_name, api_key)
    if existing:
        return existing

    # Search by email
    if email:
        fs_domain, token = _parse_api_key(api_key)
        if fs_domain:
            try:
                data = _fs_request(
                    "GET", fs_domain, "/lookup",
                    params={"q": email, "f": "email", "entities": "contact"},
                    token=token,
                )
                contacts = data.get("contacts", {}).get("contacts", [])
                if contacts:
                    return str(contacts[0]["id"])
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
    """Create a Freshsales Deal from a scored transcript analysis."""
    deal_name = score_result.get("deal_name_suggestion", "New Deal")
    recommendation = score_result.get("recommendation", "needs_review")
    score = score_result.get("total_score", 0)

    stage = "Qualification" if recommendation == "auto_create" else "Prospecting"

    if dry_run:
        logger.info(f"[DRY RUN] Would create Freshsales Deal: {deal_name} (stage: {stage})")
        return {"dry_run": True, "deal_name": deal_name}

    if not api_key:
        logger.error("Freshsales: No api_key provided for deal creation")
        return None

    fs_domain, token = _parse_api_key(api_key)
    if not fs_domain:
        logger.error("Freshsales: No domain in api_key")
        return None

    # Find or create company (Sales Account)
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
        "name": deal_name,
        "expected_close": close_date,
    }

    if account_id:
        deal_data["sales_account_id"] = int(account_id)

    if contact_ids:
        deal_data["contacts_added_ids"] = [int(cid) for cid in contact_ids]

    # Amount from budget indicators
    budget = analysis.get("budget_indicators", {})
    if budget.get("range"):
        numbers = re.findall(r"\d+", budget["range"].replace(",", ""))
        if numbers:
            try:
                deal_data["amount"] = max(int(n) for n in numbers)
            except Exception:
                pass

    try:
        data = _fs_request("POST", fs_domain, "/deals", {"deal": deal_data}, token=token)
        deal = data.get("deal", {})
        deal_id = deal.get("id")
        if not deal_id:
            logger.error(f"Freshsales Deal creation returned no ID: {data}")
            return None

        deal_id = str(deal_id)

        # Add description as a note
        try:
            _fs_request(
                "POST", fs_domain, "/notes",
                {"note": {"description": description, "targetable_type": "Deal", "targetable_id": int(deal_id)}},
                token=token,
            )
        except Exception as e:
            logger.warning(f"Failed to add note to deal: {e}")

        logger.info(f"Created Freshsales Deal: {deal_name} (ID: {deal_id})")

        return {
            "deal_id": deal_id,
            "deal_name": deal_name,
            "deal_url": f"https://{fs_domain}.myfreshworks.com/crm/sales/deals/{deal_id}",
            "company_id": account_id,
            "associated_contacts": contact_ids,
            "stage": stage,
            "score": score,
        }
    except Exception as e:
        logger.error(f"Failed to create Freshsales Deal '{deal_name}': {e}")
        return None


# --- Deal Stage Update ---

def update_deal_stage(deal_id: str, stage: str, api_key: Optional[str] = None) -> Optional[dict]:
    """Update a Deal's stage in Freshsales. Stage should be a deal_stage_id."""
    if not api_key:
        return None

    fs_domain, token = _parse_api_key(api_key)
    if not fs_domain:
        return None

    try:
        _fs_request(
            "PUT", fs_domain, f"/deals/{deal_id}",
            payload={"deal": {"deal_stage_id": int(stage)}},
            token=token,
        )
        logger.info(f"Updated Freshsales Deal {deal_id} to stage: {stage}")
        return {"deal_id": deal_id, "stage": stage}
    except Exception as e:
        logger.error(f"Failed to update Freshsales Deal {deal_id}: {e}")
        return None


# --- Deal Lookup ---

def find_deal_by_company(company_name: str, api_key: Optional[str] = None) -> Optional[dict]:
    """Find an existing Deal by company name."""
    if not company_name or not api_key:
        return None

    fs_domain, token = _parse_api_key(api_key)
    if not fs_domain:
        return None

    try:
        data = _fs_request(
            "GET", fs_domain, "/lookup",
            params={"q": company_name, "f": "name", "entities": "deal"},
            token=token,
        )
        deals = data.get("deals", {}).get("deals", [])
        if deals:
            d = deals[0]
            return {
                "deal_id": str(d["id"]),
                "deal_name": d.get("name", ""),
                "stage": str(d.get("deal_stage_id", "")),
            }
    except Exception as e:
        logger.warning(f"Freshsales deal search failed: {e}")

    return None


# --- Query Deals by Stage ---

def query_deals_by_stage(stages: list, limit: int = 20, api_key: Optional[str] = None) -> list:
    """Query Deals by stage in Freshsales."""
    if not api_key:
        return []

    fs_domain, token = _parse_api_key(api_key)
    if not fs_domain:
        return []

    results = []
    for stage_id in stages:
        try:
            data = _fs_request(
                "GET", fs_domain, "/deals/view/2",  # "All Deals" view
                params={"filter_rule": json.dumps([{"attribute": "deal_stage_id", "operator": "is_in", "value": stage_id}]),
                        "per_page": limit},
                token=token,
            )
            deals = data.get("deals", [])
            for d in deals:
                account = d.get("sales_account") or {}
                results.append({
                    "deal_id": str(d["id"]),
                    "name": d.get("name", ""),
                    "stage": str(d.get("deal_stage_id", "")),
                    "company_name": account.get("name", "") if isinstance(account, dict) else "",
                    "close_date": d.get("expected_close", ""),
                    "create_date": d.get("created_at", ""),
                    "amount": str(d.get("amount", "")) if d.get("amount") else "",
                })
        except Exception as e:
            logger.warning(f"Freshsales query stage {stage_id} failed: {e}")

    return results[:limit]


# --- Description Builder ---

def _build_description(score_result: dict, analysis: dict, metadata: Optional[dict]) -> str:
    """Build a description string for the Freshsales Deal."""
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
