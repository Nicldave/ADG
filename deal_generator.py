"""
Auto Deal Generator - Main Orchestrator
Run this to process new meeting transcripts and create CRM deals.

Usage:
  python deal_generator.py                             # Process transcripts since last run
  python deal_generator.py --days 7                    # Process last 7 days
  python deal_generator.py --transcript-id <id>        # Process a specific Fireflies transcript
  python deal_generator.py --file transcript.txt       # Process a local transcript file
  python deal_generator.py --crm attio                 # Use Attio instead of HubSpot
  python deal_generator.py --framework bant            # Score using BANT instead of Custom
  python deal_generator.py --source hubspot --crm attio  # Pull from HubSpot, create deals in Attio
  python deal_generator.py --dry-run                   # Preview without creating deals
"""

import argparse
import logging
import sys
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

import requests

from config import (
    SLACK_WEBHOOK_URL,
    STATE_FILE,
    PROCESSED_LOG,
    REVIEW_THRESHOLD,
    DEFAULT_FRAMEWORK,
)
import fireflies_client
import transcript_analyzer
import deal_scorer
import hubspot_client
import crm as crm_factory
from frameworks import FRAMEWORK_NAMES

# Logging setup
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("deal_generator")


# --- State Management ---

def load_last_run() -> Optional[datetime]:
    """Load the datetime of the last successful run."""
    if STATE_FILE.exists():
        try:
            ts = STATE_FILE.read_text().strip()
            return datetime.fromisoformat(ts)
        except Exception:
            pass
    return None


def save_last_run(dt: datetime = None):
    """Save the current datetime as last run timestamp."""
    STATE_FILE.write_text((dt or datetime.now()).isoformat())


# --- Deduplication ---

def load_processed_ids() -> set:
    """Load set of already-processed transcript/call IDs."""
    if PROCESSED_LOG.exists():
        return set(PROCESSED_LOG.read_text().strip().splitlines())
    return set()


def mark_processed(transcript_id: str):
    """Append a transcript ID to the processed log."""
    with open(PROCESSED_LOG, "a") as f:
        f.write(f"{transcript_id}\n")


# --- Slack Notifications ---

def post_to_slack(result: dict, score_result: dict, analysis: dict, dry_run: bool = False):
    """Post deal result to Slack channel."""
    if not SLACK_WEBHOOK_URL:
        return

    recommendation = score_result.get("recommendation", "not_a_deal")
    score = score_result.get("total_score", 0)

    if recommendation == "auto_create":
        emoji = "🎯"
        status = "DEAL CREATED"
        color = "#2eb886"
    elif recommendation == "needs_review":
        emoji = "👀"
        status = "NEEDS REVIEW"
        color = "#e6a817"
    else:
        emoji = "📋"
        status = "LOGGED (no deal)"
        color = "#cccccc"

    if dry_run:
        status = f"[DRY RUN] {status}"

    company = analysis.get("prospect_company", {}).get("name", "Unknown Company")
    summary = analysis.get("summary", "")
    key_signal = score_result.get("key_insight", "")
    deal_name = score_result.get("deal_name_suggestion", "")

    # Build score bar
    filled = round(score / 10)
    score_bar = "█" * filled + "░" * (10 - filled)

    lines = [
        f"*{emoji} {status}* | Score: {score}/100 `{score_bar}`",
        f"*Company:* {company}",
        f"*Deal:* {deal_name}",
        "",
        f"*Summary:* {summary}",
    ]

    if key_signal:
        lines.append(f'\n*Key Signal:* _"{key_signal}"_')

    if result.get("deal_url"):
        lines.append(f"\n<{result['deal_url']}|→ View Deal>")

    payload = {
        "attachments": [
            {
                "color": color,
                "text": "\n".join(lines),
                "footer": "Fairplay",
                "ts": int(datetime.now().timestamp()),
            }
        ]
    }

    try:
        resp = requests.post(SLACK_WEBHOOK_URL, json=payload, timeout=10)
        resp.raise_for_status()
        logger.info("Slack notification sent")
    except Exception as e:
        logger.warning(f"Slack notification failed: {e}")


# --- Core Pipeline ---

def process_transcript(
    transcript_text: str,
    metadata: dict,
    dry_run: bool = False,
    crm_client=None,
    framework: str = "custom",
) -> dict:
    """
    Run a single transcript through the full pipeline:
    analyze → score → create deal (if qualified).

    Args:
        crm_client: CRM module to use (hubspot_client or attio_client).
                    Defaults to hubspot_client if not provided.
        framework: Scoring framework (custom, bant, spiced, meddic, spin).

    Returns a summary dict of what happened.
    """
    if crm_client is None:
        crm_client = hubspot_client

    title = metadata.get("title", "Unknown Meeting")
    logger.info(f"Processing: {title}")

    # Step 1: Analyze transcript
    analysis = transcript_analyzer.analyze_transcript(transcript_text, metadata, framework=framework)

    if not analysis.get("is_sales_conversation"):
        logger.info(f"Skipping '{title}': not a sales conversation")
        return {
            "title": title,
            "action": "skipped",
            "reason": "Not a sales conversation",
        }

    # Step 2: Score
    score_result = deal_scorer.score_deal(analysis)
    logger.info("\n" + deal_scorer.format_score_report(score_result))

    # Step 3: Create deal (or not)
    deal_result = None
    action = "logged"

    if score_result["recommendation"] in ("auto_create", "needs_review"):
        deal_result = crm_client.create_deal(
            score_result, analysis, metadata, dry_run=dry_run
        )
        action = "deal_created" if not dry_run else "dry_run"
    else:
        logger.info(
            f"Score {score_result['total_score']}/100 below threshold "
            f"({REVIEW_THRESHOLD}) - logging only"
        )

    # Step 4: Notify Slack
    post_to_slack(
        deal_result or {},
        score_result,
        analysis,
        dry_run=dry_run,
    )

    return {
        "title": title,
        "action": action,
        "score": score_result["total_score"],
        "recommendation": score_result["recommendation"],
        "deal": deal_result,
        "company": analysis.get("prospect_company", {}).get("name"),
    }


def run(
    since: datetime = None,
    transcript_id: str = None,
    filepath: str = None,
    source: str = "fireflies",
    crm: str = "hubspot",
    framework: str = "custom",
    dry_run: bool = False,
) -> list[dict]:
    """
    Main entry point. Returns list of processing results.

    Priority: filepath > transcript_id > since (date-based batch)
    source:    "fireflies" (default) or "hubspot" — where to pull transcripts from
    crm:       "hubspot" (default) or "attio"     — where to create deals
    framework: "custom" (default), "bant", "spiced", "meddic", "spin"
    """
    crm_client = crm_factory.get_client(crm)
    logger.info(f"CRM target: {crm.upper()} | Framework: {framework.upper()}")
    results = []

    # --- Single file mode (source-agnostic) ---
    if filepath:
        logger.info(f"Processing local file: {filepath}")
        text = Path(filepath).read_text()
        metadata = {"title": Path(filepath).stem, "date": datetime.now().isoformat()}
        result = process_transcript(text, metadata, dry_run=dry_run, crm_client=crm_client, framework=framework)
        results.append(result)
        return results

    # --- Single transcript mode ---
    if transcript_id:
        if source == "hubspot":
            logger.info(f"Processing HubSpot call: {transcript_id}")
            call = hubspot_client.get_call(transcript_id)
            text = hubspot_client.format_hubspot_transcript(call)
            metadata = hubspot_client.get_call_metadata(call)
        else:
            logger.info(f"Processing Fireflies transcript: {transcript_id}")
            transcript = fireflies_client.get_transcript(transcript_id)
            text = fireflies_client.format_transcript_text(transcript)
            metadata = fireflies_client.get_meeting_metadata(transcript)

        if not text:
            logger.warning(f"No transcript content found for {transcript_id}")
            return [{"title": transcript_id, "action": "skipped", "reason": "No transcript content"}]

        result = process_transcript(text, metadata, dry_run=dry_run, crm_client=crm_client, framework=framework)
        results.append(result)
        return results

    # --- Batch mode: calls since last run (or specified date) ---
    if since is None:
        since = load_last_run() or (datetime.now() - timedelta(days=7))

    processed_ids = load_processed_ids()

    if source == "hubspot":
        logger.info(f"Processing HubSpot calls since {since.isoformat()}")
        calls = hubspot_client.list_calls(since=since)

        if not calls:
            logger.info("No new HubSpot calls found")
            return []

        for call in calls:
            call_id = call.get("id", "?")
            if f"hubspot:{call_id}" in processed_ids:
                logger.info(f"Skipping call {call_id}: already processed")
                continue
            try:
                text = hubspot_client.format_hubspot_transcript(call)
                if not text:
                    logger.info(f"Skipping call {call_id}: no transcript body")
                    continue
                metadata = hubspot_client.get_call_metadata(call)
                result = process_transcript(
                    text, metadata, dry_run=dry_run, crm_client=crm_client, framework=framework
                )
                results.append(result)
                if not dry_run:
                    mark_processed(f"hubspot:{call_id}")
            except Exception as e:
                logger.error(f"Failed to process HubSpot call {call_id}: {e}")
                results.append({"title": call_id, "action": "error", "error": str(e)})
    else:
        logger.info(f"Processing Fireflies transcripts since {since.isoformat()}")
        transcripts = fireflies_client.list_transcripts(since=since)

        if not transcripts:
            logger.info("No new Fireflies transcripts found")
            return []

        for t in transcripts:
            tid = t["id"]
            if f"fireflies:{tid}" in processed_ids:
                logger.info(f"Skipping transcript {tid}: already processed")
                continue
            try:
                full = fireflies_client.get_transcript(tid)
                text = fireflies_client.format_transcript_text(full)
                metadata = fireflies_client.get_meeting_metadata(full)
                result = process_transcript(
                    text, metadata, dry_run=dry_run, crm_client=crm_client, framework=framework
                )
                results.append(result)
                if not dry_run:
                    mark_processed(f"fireflies:{tid}")
            except Exception as e:
                logger.error(f"Failed to process transcript {t.get('id', '?')}: {e}")
                results.append({"title": t.get("title", "?"), "action": "error", "error": str(e)})

    if not dry_run:
        save_last_run()

    return results


def print_summary(results: list[dict]):
    """Print a summary of all processed transcripts."""
    print("\n" + "=" * 60)
    print("AUTO DEAL GENERATOR - RUN SUMMARY")
    print("=" * 60)

    totals = {"deal_created": 0, "logged": 0, "skipped": 0, "error": 0, "dry_run": 0}

    for r in results:
        action = r.get("action", "unknown")
        totals[action] = totals.get(action, 0) + 1

        score_str = f" | Score: {r['score']}/100" if "score" in r else ""
        company_str = f" | {r['company']}" if r.get("company") else ""
        print(f"  [{action.upper()}] {r.get('title', '?')}{company_str}{score_str}")

    print("\nTotals:")
    for k, v in totals.items():
        if v > 0:
            print(f"  {k}: {v}")
    print("=" * 60)


# --- CLI ---

def main():
    parser = argparse.ArgumentParser(
        description="Fairplay - analyze meeting transcripts and create CRM deals"
    )
    parser.add_argument("--days", type=int, help="Process transcripts from last N days")
    parser.add_argument("--transcript-id", help="Process a specific transcript/call ID")
    parser.add_argument("--file", help="Process a local transcript text file")
    parser.add_argument(
        "--source", choices=["fireflies", "hubspot"], default="fireflies",
        help="Transcript source (default: fireflies)"
    )
    parser.add_argument(
        "--crm", choices=["hubspot", "attio"], default="hubspot",
        help="CRM to create deals in (default: hubspot)"
    )
    parser.add_argument(
        "--framework", choices=FRAMEWORK_NAMES, default=DEFAULT_FRAMEWORK,
        help=f"Scoring framework (default: {DEFAULT_FRAMEWORK})"
    )
    parser.add_argument("--dry-run", action="store_true", help="Preview without creating deals")
    args = parser.parse_args()

    if args.dry_run:
        logger.info("*** DRY RUN MODE - no deals will be created ***")

    since = None
    if args.days:
        since = datetime.now() - timedelta(days=args.days)

    results = run(
        since=since,
        transcript_id=args.transcript_id,
        filepath=args.file,
        source=args.source,
        crm=args.crm,
        framework=args.framework,
        dry_run=args.dry_run,
    )

    print_summary(results)

    # Exit with error code if any errors occurred
    errors = [r for r in results if r.get("action") == "error"]
    sys.exit(1 if errors else 0)


if __name__ == "__main__":
    main()
