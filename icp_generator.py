"""
ICP Generator - Auto-generate company profile from website
Scrapes a company's website and uses Claude to generate a concise
product/service + ICP summary for scoring context.
"""

import json
import logging
import re
from typing import Optional

import anthropic
import requests

from config import ANTHROPIC_API_KEY, CLAUDE_MODEL

logger = logging.getLogger(__name__)

client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)


def scrape_website(url: str) -> str:
    """Scrape a website and extract clean text content."""
    if not url:
        return ""

    # Ensure URL has protocol
    if not url.startswith("http"):
        url = f"https://{url}"

    try:
        resp = requests.get(
            url,
            timeout=15,
            headers={"User-Agent": "Mozilla/5.0 (Fairplay ICP Generator)"},
        )
        resp.raise_for_status()
        html = resp.text

        # Strip scripts, styles, nav, footer
        html = re.sub(r"<script[^>]*>.*?</script>", "", html, flags=re.DOTALL | re.IGNORECASE)
        html = re.sub(r"<style[^>]*>.*?</style>", "", html, flags=re.DOTALL | re.IGNORECASE)
        html = re.sub(r"<nav[^>]*>.*?</nav>", "", html, flags=re.DOTALL | re.IGNORECASE)
        html = re.sub(r"<footer[^>]*>.*?</footer>", "", html, flags=re.DOTALL | re.IGNORECASE)
        html = re.sub(r"<header[^>]*>.*?</header>", "", html, flags=re.DOTALL | re.IGNORECASE)

        # Strip all remaining HTML tags
        text = re.sub(r"<[^>]+>", " ", html)

        # Clean up whitespace
        text = re.sub(r"\s+", " ", text).strip()

        # Cap at 5000 chars
        return text[:5000]

    except Exception as e:
        logger.error(f"Failed to scrape {url}: {e}")
        return ""


def generate_icp(website_text: str, business_context: Optional[dict] = None) -> dict:
    """Use Claude to generate a company profile from website content."""
    if not website_text or len(website_text) < 50:
        return {"error": "Not enough website content to generate profile"}

    context_info = ""
    if business_context:
        parts = []
        if business_context.get("sale_type"):
            parts.append(f"Sale type: {business_context['sale_type']}")
        if business_context.get("deal_value_range"):
            parts.append(f"Deal value: {business_context['deal_value_range']}")
        if business_context.get("industry_vertical"):
            parts.append(f"Industry: {business_context['industry_vertical']}")
        if parts:
            context_info = "\n\nAdditional context from the user:\n" + "\n".join(parts)

    prompt = f"""Based on this website content, generate a concise company profile that will be used to evaluate whether sales conversations are relevant to this company's business.

Keep the total output under 300 words. Be specific, not generic.

Return ONLY valid JSON matching this structure:
{{
  "products": "What this company sells. 2-3 sentences describing their products or services, pricing model if visible, and key differentiators.",
  "icp": "Who they sell to. Target industries, company sizes, job titles, and any qualifying criteria. 2-3 sentences.",
  "deal_characteristics": "What a real deal looks like. Typical engagement scope, decision-making process, common pain points that trigger a purchase. 2-3 sentences.",
  "not_a_deal": "What is NOT a deal for this company. Types of conversations that should score low even if they discuss budget/authority/need/timeline. 1-2 sentences."
}}

Website content:
{website_text}{context_info}"""

    try:
        message = client.messages.create(
            model=CLAUDE_MODEL,
            max_tokens=1024,
            messages=[{"role": "user", "content": prompt}],
        )
        response_text = message.content[0].text.strip()

        # Parse JSON
        if response_text.startswith("```"):
            response_text = response_text.split("```")[1]
            if response_text.startswith("json"):
                response_text = response_text[4:]
            response_text = response_text.strip()

        result = json.loads(response_text)
        logger.info(f"ICP generated successfully ({len(json.dumps(result))} chars)")
        return result

    except json.JSONDecodeError as e:
        logger.error(f"Claude returned invalid JSON for ICP: {e}")
        return {"error": f"Failed to parse ICP response: {e}"}
    except Exception as e:
        logger.error(f"ICP generation failed: {e}")
        return {"error": str(e)}


def format_icp_for_prompt(icp: dict) -> str:
    """Format the ICP dict into a string for injection into the transcript analysis prompt.
    Values are JSON-serialized to escape any prompt injection attempts."""
    if not icp or icp.get("error"):
        return ""

    parts = []
    if icp.get("products"):
        parts.append(f"This company sells: {json.dumps(icp['products'])}")
    if icp.get("icp"):
        parts.append(f"Their ideal customer: {json.dumps(icp['icp'])}")
    if icp.get("deal_characteristics"):
        parts.append(f"What a real deal looks like: {json.dumps(icp['deal_characteristics'])}")
    if icp.get("not_a_deal"):
        parts.append(f"What is NOT a deal: {json.dumps(icp['not_a_deal'])}")

    if not parts:
        return ""

    return (
        "## Company Profile\n"
        + "\n".join(parts)
        + "\n\nScore this conversation based on whether it represents a real opportunity "
        "for THIS company's products/services. If the conversation is about something "
        "unrelated to what this company sells, score it low regardless of BANT signals."
    )
