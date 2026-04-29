"""
Salesforce Client - Deal Creation
Creates Opportunities in Salesforce CRM from scored transcript analysis.

Uses Salesforce REST API with OAuth 2.0 Bearer token authentication.
Requires: instance_url + access_token from a Connected App.

Required Connected App scopes:
  api (REST API access)
  full (for SOQL queries)
"""

import json
import logging
import re
from datetime import datetime
from typing import Optional

import requests

logger = logging.getLogger(__name__)

SALESFORCE_API_VERSION = "v60.0"


# --- Internal helpers ---

def _headers(api_key: Optional[str] = None) -> dict:
    if not api_key:
        logger.error("Salesforce: No access token provided")
        return {}
    return {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
    }


def _parse_api_key(api_key: str) -> tuple:
    """Parse combined key format: instance_url|access_token"""
    if "|" in api_key:
        parts = api_key.split("|", 1)
        return parts[0].rstrip("/"), parts[1]
    # Assume it's just a token and try to get instance_url from config
    return "", api_key


def _sf_request(method: str, instance_url: str, path: str, payload=None,
                params=None, api_key: str = "") -> dict:
    """Execute a Salesforce API request."""
    url = f"{instance_url}/services/data/{SALESFORCE_API_VERSION}{path}"
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
            logger.error(f"Salesforce {method} {path} failed: {response.status_code} {response.text[:300]}")
            response.raise_for_status()
        return response.json() if response.content else {}
    except Exception as e:
        logger.error(f"Salesforce request failed: {e}")
        raise


def _soql_query(instance_url: str, query: str, api_key: str = "") -> list:
    """Run a SOQL query and return records."""
    url = f"{instance_url}/services/data/{SALESFORCE_API_VERSION}/query"
    headers = _headers(api_key)
    if not headers:
        return []
    try:
        resp = requests.get(url, headers=headers, params={"q": query}, timeout=30)
        if resp.status_code != 200:
            logger.error(f"Salesforce SOQL failed: {resp.status_code} {resp.text[:300]}")
            return []
        return resp.json().get("records", [])
    except Exception as e:
        logger.error(f"Salesforce SOQL query failed: {e}")
        return []


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
    """Find an Account in Salesforce by name, or create one. Returns Account ID."""
    if not company_name or not api_key:
        return None

    instance_url, token = _parse_api_key(api_key)
    if not instance_url:
        logger.error("Salesforce: No instance_url in api_key")
        return None

    # Search by name
    safe_name = company_name.replace("'", "\\'")
    records = _soql_query(
        instance_url,
        f"SELECT Id, Name FROM Account WHERE Name LIKE '%{safe_name}%' LIMIT 1",
        api_key=token,
    )
    if records:
        record_id = records[0]["Id"]
        logger.info(f"Salesforce Account found: {company_name} (ID: {record_id})")
        return record_id

    # Create
    try:
        payload = {"Name": company_name}
        if industry:
            payload["Industry"] = industry
        if domain:
            payload["Website"] = domain if domain.startswith("http") else f"https://{domain}"

        data = _sf_request("POST", instance_url, "/sobjects/Account", payload, api_key=token)
        record_id = data.get("id")
        if record_id:
            logger.info(f"Salesforce Account created: {company_name} (ID: {record_id})")
            return record_id
    except Exception as e:
        logger.warning(f"Salesforce Account create failed for '{company_name}': {e}")

    return None


# --- Contact Lookup/Creation ---

def find_contact_by_name(name: str, company_name: Optional[str] = None,
                         api_key: Optional[str] = None) -> Optional[str]:
    """Search for a Contact in Salesforce by name. Returns Contact ID or None."""
    return find_or_create_contact(name, None, company_name, api_key)


def find_or_create_contact(name: str, email: Optional[str] = None,
                           company_name: Optional[str] = None,
                           api_key: Optional[str] = None) -> Optional[str]:
    """Find or create a Contact in Salesforce. Returns Contact ID."""
    if not name or not api_key:
        return None

    instance_url, token = _parse_api_key(api_key)
    if not instance_url:
        return None

    # Search by email first if available
    if email:
        safe_email = email.replace("'", "\\'")
        records = _soql_query(
            instance_url,
            f"SELECT Id FROM Contact WHERE Email = '{safe_email}' LIMIT 1",
            api_key=token,
        )
        if records:
            return records[0]["Id"]

    # Search by name
    parts = name.strip().split()
    if len(parts) >= 2:
        first = parts[0].replace("'", "\\'")
        last = parts[-1].replace("'", "\\'")
        records = _soql_query(
            instance_url,
            f"SELECT Id FROM Contact WHERE FirstName LIKE '%{first}%' AND LastName LIKE '%{last}%' LIMIT 1",
            api_key=token,
        )
        if records:
            return records[0]["Id"]

    return None


# --- Deal (Opportunity) Creation ---

def create_deal(
    score_result: dict,
    analysis: dict,
    metadata: Optional[dict] = None,
    dry_run: bool = False,
    api_key: Optional[str] = None,
) -> Optional[dict]:
    """Create a Salesforce Opportunity from a scored transcript analysis."""
    deal_name = score_result.get("deal_name_suggestion", "New Deal")
    recommendation = score_result.get("recommendation", "needs_review")
    score = score_result.get("total_score", 0)

    stage = "Qualification" if recommendation == "auto_create" else "Prospecting"

    if dry_run:
        logger.info(f"[DRY RUN] Would create Salesforce Opportunity: {deal_name} (stage: {stage})")
        return {"dry_run": True, "deal_name": deal_name}

    if not api_key:
        logger.error("Salesforce: No api_key provided for deal creation")
        return None

    instance_url, token = _parse_api_key(api_key)
    if not instance_url:
        logger.error("Salesforce: No instance_url in api_key")
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

    # Build Opportunity payload
    close_date = datetime.now().strftime("%Y-%m-%d")
    payload = {
        "Name": deal_name,
        "StageName": stage,
        "CloseDate": close_date,
        "Description": description,
    }

    if account_id:
        payload["AccountId"] = account_id

    # Amount from budget indicators
    budget = analysis.get("budget_indicators", {})
    if budget.get("range"):
        numbers = re.findall(r"\d+", budget["range"].replace(",", ""))
        if numbers:
            try:
                payload["Amount"] = max(int(n) for n in numbers)
            except Exception:
                pass

    try:
        data = _sf_request("POST", instance_url, "/sobjects/Opportunity", payload, api_key=token)
        deal_id = data.get("id")
        if not deal_id:
            logger.error(f"Salesforce Opportunity creation returned no ID: {data}")
            return None

        logger.info(f"Created Salesforce Opportunity: {deal_name} (ID: {deal_id})")

        # Associate primary contact as OpportunityContactRole
        if contact_ids:
            for cid in contact_ids[:1]:  # Primary contact only
                try:
                    _sf_request(
                        "POST", instance_url, "/sobjects/OpportunityContactRole",
                        {"OpportunityId": deal_id, "ContactId": cid, "IsPrimary": True},
                        api_key=token,
                    )
                    logger.info(f"Associated contact {cid} with Opportunity {deal_id}")
                except Exception as e:
                    logger.warning(f"Failed to associate contact: {e}")

        return {
            "deal_id": deal_id,
            "deal_name": deal_name,
            "deal_url": f"{instance_url}/lightning/r/Opportunity/{deal_id}/view",
            "company_id": account_id,
            "associated_contacts": contact_ids,
            "stage": stage,
            "score": score,
        }
    except Exception as e:
        logger.error(f"Failed to create Salesforce Opportunity '{deal_name}': {e}")
        return None


# --- Deal Stage Update ---

def update_deal_stage(deal_id: str, stage: str, api_key: Optional[str] = None) -> Optional[dict]:
    """Update an Opportunity's stage in Salesforce."""
    if not api_key:
        return None

    instance_url, token = _parse_api_key(api_key)
    if not instance_url:
        return None

    try:
        _sf_request(
            "PATCH", instance_url, f"/sobjects/Opportunity/{deal_id}",
            {"StageName": stage}, api_key=token,
        )
        logger.info(f"Updated Salesforce Opportunity {deal_id} to stage: {stage}")
        return {"deal_id": deal_id, "stage": stage}
    except Exception as e:
        logger.error(f"Failed to update Salesforce Opportunity {deal_id}: {e}")
        return None


# --- Deal Lookup ---

def find_deal_by_company(company_name: str, api_key: Optional[str] = None) -> Optional[dict]:
    """Find an existing Opportunity by company name."""
    if not company_name or not api_key:
        return None

    instance_url, token = _parse_api_key(api_key)
    if not instance_url:
        return None

    safe_name = company_name.replace("'", "\\'")
    records = _soql_query(
        instance_url,
        f"SELECT Id, Name, StageName FROM Opportunity WHERE Account.Name LIKE '%{safe_name}%' "
        f"AND IsClosed = false ORDER BY CreatedDate DESC LIMIT 1",
        api_key=token,
    )
    if records:
        r = records[0]
        return {
            "deal_id": r["Id"],
            "deal_name": r["Name"],
            "stage": r["StageName"],
        }
    return None


# --- Query Deals by Stage ---

def query_deals_by_stage(stages: list, limit: int = 20, api_key: Optional[str] = None) -> list:
    """Query closed Opportunities by stage name."""
    if not api_key:
        return []

    instance_url, token = _parse_api_key(api_key)
    if not instance_url:
        return []

    stage_list = ", ".join(f"'{s}'" for s in stages)
    records = _soql_query(
        instance_url,
        f"SELECT Id, Name, StageName, Account.Name, CloseDate, CreatedDate, Amount "
        f"FROM Opportunity WHERE StageName IN ({stage_list}) "
        f"ORDER BY CloseDate DESC LIMIT {limit}",
        api_key=token,
    )

    return [
        {
            "deal_id": r["Id"],
            "name": r["Name"],
            "stage": r["StageName"],
            "company_name": r.get("Account", {}).get("Name", "") if r.get("Account") else "",
            "close_date": r.get("CloseDate", ""),
            "create_date": r.get("CreatedDate", ""),
            "amount": str(r.get("Amount", "")) if r.get("Amount") else "",
        }
        for r in records
    ]


# --- Description Builder ---

def _build_description(score_result: dict, analysis: dict, metadata: Optional[dict]) -> str:
    """Build a description string for the Salesforce Opportunity."""
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
