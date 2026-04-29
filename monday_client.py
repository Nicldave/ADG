"""
Monday CRM Client - Deal Creation
Creates Items on a Monday.com CRM board from scored transcript analysis.

Uses Monday.com GraphQL API v2 with API token authentication.
Auth: Authorization: Bearer {api_key}

api_key format: board_id|token
  - board_id: The numeric ID of your CRM Deals board
  - token: Your Monday.com API token

Monday CRM does not have separate Company or Contact objects.
Deals are items on a board. Company/contact functions return None.
"""

import json
import logging
import re
from datetime import datetime
from typing import Optional

import requests

logger = logging.getLogger(__name__)

MONDAY_API_URL = "https://api.monday.com/v2"


# --- Internal helpers ---

def _headers(token: Optional[str] = None) -> dict:
    if not token:
        logger.error("Monday: No API token provided")
        return {}
    return {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }


def _parse_api_key(api_key: str) -> tuple:
    """Parse combined key format: board_id|token"""
    if "|" in api_key:
        parts = api_key.split("|", 1)
        return parts[0].strip(), parts[1].strip()
    logger.error("Monday: api_key must be in format board_id|token")
    return "", api_key


def _graphql_request(query: str, token: str, variables: Optional[dict] = None) -> dict:
    """Execute a Monday.com GraphQL request."""
    headers = _headers(token)
    if not headers:
        return {}

    payload = {"query": query}
    if variables:
        payload["variables"] = variables

    try:
        response = requests.post(
            MONDAY_API_URL, headers=headers, json=payload, timeout=30
        )
        if response.status_code != 200:
            logger.error(
                f"Monday GraphQL failed: {response.status_code} {response.text[:300]}"
            )
            response.raise_for_status()
        data = response.json()
        if data.get("errors"):
            logger.error(f"Monday GraphQL errors: {data['errors']}")
            return {}
        return data.get("data", {})
    except Exception as e:
        logger.error(f"Monday request failed: {e}")
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
    """Monday CRM does not have separate Company objects. Returns None."""
    return None


# --- Contact Lookup/Creation ---

def find_contact_by_name(name: str, company_name: Optional[str] = None,
                         api_key: Optional[str] = None) -> Optional[str]:
    """Monday CRM does not have separate Contact objects. Returns None."""
    return None


def find_or_create_contact(name: str, email: Optional[str] = None,
                           company_name: Optional[str] = None,
                           api_key: Optional[str] = None) -> Optional[str]:
    """Monday CRM does not have separate Contact objects. Returns None."""
    return None


# --- Deal (Item) Creation ---

def create_deal(
    score_result: dict,
    analysis: dict,
    metadata: Optional[dict] = None,
    dry_run: bool = False,
    api_key: Optional[str] = None,
) -> Optional[dict]:
    """Create a Monday.com CRM item from a scored transcript analysis."""
    deal_name = score_result.get("deal_name_suggestion", "New Deal")
    recommendation = score_result.get("recommendation", "needs_review")
    score = score_result.get("total_score", 0)

    stage = "Qualification" if recommendation == "auto_create" else "Prospecting"

    if dry_run:
        logger.info(f"[DRY RUN] Would create Monday item: {deal_name} (stage: {stage})")
        return {"dry_run": True, "deal_name": deal_name}

    if not api_key:
        logger.error("Monday: No api_key provided for deal creation")
        return None

    board_id, token = _parse_api_key(api_key)
    if not board_id:
        logger.error("Monday: No board_id in api_key")
        return None

    # Build description
    description = _build_description(score_result, analysis, metadata)

    # Build column values for the item
    column_values = json.dumps({
        "status": {"label": stage},
        "text": description[:2000],
    })

    # Escape for GraphQL string embedding
    escaped_name = deal_name.replace('"', '\\"')
    escaped_columns = column_values.replace('"', '\\"')

    query = f'''
    mutation {{
        create_item(
            board_id: {board_id},
            item_name: "{escaped_name}",
            column_values: "{escaped_columns}"
        ) {{
            id
            name
        }}
    }}
    '''

    try:
        data = _graphql_request(query, token)
        item = data.get("create_item", {})
        deal_id = item.get("id")

        if not deal_id:
            logger.error(f"Monday item creation returned no ID: {data}")
            return None

        logger.info(f"Created Monday item: {deal_name} (ID: {deal_id})")

        return {
            "deal_id": deal_id,
            "deal_name": deal_name,
            "deal_url": f"https://monday.com/boards/{board_id}/pulses/{deal_id}",
            "company_id": None,
            "associated_contacts": [],
            "stage": stage,
            "score": score,
        }
    except Exception as e:
        logger.error(f"Failed to create Monday item '{deal_name}': {e}")
        return None


# --- Deal Stage Update ---

def update_deal_stage(deal_id: str, stage: str,
                      api_key: Optional[str] = None) -> Optional[dict]:
    """Update an item's status column on a Monday.com board."""
    if not api_key:
        return None

    board_id, token = _parse_api_key(api_key)
    if not board_id:
        return None

    value = json.dumps({"label": stage}).replace('"', '\\"')

    query = f'''
    mutation {{
        change_column_value(
            board_id: {board_id},
            item_id: {deal_id},
            column_id: "status",
            value: "{value}"
        ) {{
            id
        }}
    }}
    '''

    try:
        data = _graphql_request(query, token)
        if data.get("change_column_value", {}).get("id"):
            logger.info(f"Updated Monday item {deal_id} to stage: {stage}")
            return {"deal_id": deal_id, "stage": stage}
        logger.error(f"Monday stage update returned unexpected data: {data}")
        return None
    except Exception as e:
        logger.error(f"Failed to update Monday item {deal_id}: {e}")
        return None


# --- Deal Lookup ---

def find_deal_by_company(company_name: str,
                         api_key: Optional[str] = None) -> Optional[dict]:
    """Find an existing item on the Monday board by searching item names."""
    if not company_name or not api_key:
        return None

    board_id, token = _parse_api_key(api_key)
    if not board_id:
        return None

    escaped_name = company_name.replace('"', '\\"')

    query = f'''
    query {{
        items_page_by_column_values(
            board_id: {board_id},
            limit: 1,
            columns: [{{column_id: "name", column_values: ["{escaped_name}"]}}]
        ) {{
            items {{
                id
                name
                column_values {{
                    id
                    text
                }}
            }}
        }}
    }}
    '''

    try:
        data = _graphql_request(query, token)
        items = data.get("items_page_by_column_values", {}).get("items", [])
        if items:
            item = items[0]
            # Extract stage from status column
            stage_val = ""
            for col in item.get("column_values", []):
                if col.get("id") == "status":
                    stage_val = col.get("text", "")
                    break
            return {
                "deal_id": item["id"],
                "deal_name": item["name"],
                "stage": stage_val,
            }
    except Exception as e:
        logger.error(f"Monday search failed for '{company_name}': {e}")

    return None


# --- Query Deals by Stage ---

def query_deals_by_stage(stages: list, limit: int = 20,
                         api_key: Optional[str] = None) -> list:
    """Query items on a Monday board filtered by status column values."""
    if not api_key:
        return []

    board_id, token = _parse_api_key(api_key)
    if not board_id:
        return []

    results = []
    for stage in stages:
        escaped_stage = stage.replace('"', '\\"')
        query = f'''
        query {{
            items_page_by_column_values(
                board_id: {board_id},
                limit: {limit},
                columns: [{{column_id: "status", column_values: ["{escaped_stage}"]}}]
            ) {{
                items {{
                    id
                    name
                    column_values {{
                        id
                        text
                    }}
                    created_at
                }}
            }}
        }}
        '''

        try:
            data = _graphql_request(query, token)
            items = data.get("items_page_by_column_values", {}).get("items", [])
            for item in items:
                stage_text = ""
                for col in item.get("column_values", []):
                    if col.get("id") == "status":
                        stage_text = col.get("text", "")
                        break

                results.append({
                    "deal_id": item["id"],
                    "name": item["name"],
                    "stage": stage_text,
                    "company_name": "",
                    "close_date": "",
                    "create_date": item.get("created_at", ""),
                    "amount": "",
                })
        except Exception as e:
            logger.error(f"Monday stage query failed for '{stage}': {e}")

    return results[:limit]


# --- Description Builder ---

def _build_description(score_result: dict, analysis: dict,
                       metadata: Optional[dict]) -> str:
    """Build a description string for the Monday CRM item."""
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
