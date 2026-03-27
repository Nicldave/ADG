"""
Transcript Analyzer
Uses Claude to extract structured sales intelligence from meeting transcripts.
This is the "brain" of the auto deal generator.
"""

import json
import logging
from typing import Optional

import anthropic

from config import ANTHROPIC_API_KEY, CLAUDE_MODEL
from frameworks import get_framework

logger = logging.getLogger(__name__)

client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)

# Base prompt used for all frameworks
BASE_PROMPT = """You are a sales intelligence analyst. Your job is to analyze a sales meeting transcript and extract structured data that determines whether this conversation represents a potential deal.

You must be precise and evidence-based. Every field you fill in must be backed by something actually said in the transcript. Do not infer or fabricate. If information is not present, leave it empty or null.

## Meeting Type Classification
- **discovery**: First real sales conversation, exploring pain and fit
- **demo**: Showing product/service capabilities
- **follow_up**: Continuing a previous conversation, progressing toward decision
- **negotiation**: Discussing terms, pricing, scope
- **internal**: Internal team meeting (not a sales opportunity)
- **other**: Doesn't fit above categories
"""

# Custom framework keeps the original Ascent-specific pain categories
CUSTOM_PROMPT_SECTION = """
## Pain Signal Categories
When identifying pain signals, classify them into these categories (derived from real closed-won deals):

1. **just_bookkeeping** - They have recordkeeping but no strategic finance ("just punching numbers", "no strategic thought")
2. **zero_insight** - No visibility into business performance ("zero insight", "can't see where we stand")
3. **jerry_rigged_systems** - Finance stack held together with duct tape ("jerry-rigged", "can't match up to bank statements")
4. **wearing_too_many_hats** - CEO/founder doing finance work ("I have essentially 3 jobs", "spending time on this when she shouldn't be")
5. **transaction_urgency** - Active deal, departure, or audit driving urgency ("we needed her yesterday", "speed is imperative")
6. **outgrown_skill_set** - Business complexity exceeding team capability ("outgrown our skill set", "need to take it to the next level")
7. **strategic_partner_need** - Want a thinking partner, not just numbers ("strategic partner", "adult in the room")
8. **emotional_stakes** - Existential business pressure ("it's either that or we go out of business")
9. **budget_value_focus** - Explicit discussion of spending and value ("high end for us", "value we're getting")
10. **growth_trajectory** - Active expansion, fundraise, or M&A ("expecting to grow 60%", "M&A coming")
"""

# Output format for custom framework (original)
CUSTOM_OUTPUT_FORMAT = """
## Output Format
Return ONLY valid JSON (no markdown, no commentary) matching this exact structure:

{
  "meeting_type": "discovery|demo|follow_up|negotiation|internal|other",
  "is_sales_conversation": true/false,
  "participants": [
    {"name": "string", "role": "string or null", "company": "string or null", "is_prospect": true/false}
  ],
  "prospect_company": {
    "name": "string or null",
    "industry": "string or null",
    "estimated_size": "string or null",
    "estimated_revenue": "string or null",
    "domain": "company website domain if mentioned, or null",
    "website": "full URL if mentioned, or null"
  },
  "pain_signals": [
    {"category": "one of the 10 categories above", "quote": "verbatim quote from transcript", "severity": 1-5, "speaker": "name"}
  ],
  "buying_signals": [
    {"signal": "description", "evidence": "verbatim quote", "strength": "weak|moderate|strong"}
  ],
  "objections": [
    {"objection": "what they pushed back on", "response": "how it was handled", "resolved": true/false}
  ],
  "next_steps": [
    {"action": "what was agreed", "owner": "who owns it", "deadline": "when, or null"}
  ],
  "budget_indicators": {
    "mentioned": true/false,
    "range": "string or null",
    "concerns": "string or null",
    "willingness": "eager|neutral|hesitant|unknown"
  },
  "timeline_indicators": {
    "urgency": "low|medium|high|critical",
    "target_date": "string or null",
    "trigger_event": "string or null",
    "evidence": "verbatim quote or null"
  },
  "decision_makers": [
    {"name": "string", "title": "string or null", "influence": "champion|evaluator|decision_maker|blocker|unknown", "email": "email address if mentioned, or null"}
  ],
  "competitors_mentioned": ["list of competitor names or services mentioned"],
  "summary": "2-3 sentence summary of the meeting and its sales significance"
}
"""


def _build_framework_output_format(framework_key: str) -> str:
    """Build the JSON output format dynamically based on selected framework."""
    fw = get_framework(framework_key)
    categories = fw["categories"]

    # Build the framework_scores object dynamically
    score_fields = []
    for key, cat in categories.items():
        score_fields.append(
            f'    "{key}": {{"score": 0-{cat["weight"]}, "evidence": ["verbatim quotes"], "assessment": "one sentence"}}'
        )
    scores_json = ",\n".join(score_fields)

    return f"""
## Output Format
Return ONLY valid JSON (no markdown, no commentary) matching this exact structure:

{{
  "meeting_type": "discovery|demo|follow_up|negotiation|internal|other",
  "is_sales_conversation": true/false,
  "framework": "{framework_key}",
  "participants": [
    {{"name": "string", "role": "string or null", "company": "string or null", "is_prospect": true/false}}
  ],
  "prospect_company": {{
    "name": "string or null",
    "industry": "string or null",
    "estimated_size": "string or null",
    "estimated_revenue": "string or null"
  }},
  "framework_scores": {{
{scores_json}
  }},
  "objections": [
    {{"objection": "what they pushed back on", "response": "how it was handled", "resolved": true/false}}
  ],
  "next_steps": [
    {{"action": "what was agreed", "owner": "who owns it", "deadline": "when, or null"}}
  ],
  "competitors_mentioned": ["list of competitor names or services mentioned"],
  "summary": "2-3 sentence summary of the meeting and its sales significance"
}}
"""


def _build_prompt(framework_key: str) -> str:
    """Assemble the full analysis prompt for the given framework."""
    if framework_key == "custom":
        return BASE_PROMPT + CUSTOM_PROMPT_SECTION + CUSTOM_OUTPUT_FORMAT

    fw = get_framework(framework_key)
    return BASE_PROMPT + fw["prompt_addendum"] + _build_framework_output_format(framework_key)


def analyze_transcript(
    transcript_text: str,
    meeting_metadata: Optional[dict] = None,
    framework: str = "custom",
) -> dict:
    """
    Analyze a meeting transcript using Claude to extract structured sales intelligence.

    Args:
        transcript_text: The full transcript text with speaker labels.
        meeting_metadata: Optional metadata (title, date, participants, etc.)
        framework: Scoring framework to use (custom, bant, spiced, meddic, spin).

    Returns:
        Structured analysis dict matching the schema above.
    """
    context_parts = []

    if meeting_metadata:
        context_parts.append(
            f"Meeting: {meeting_metadata.get('title', 'Unknown')}\n"
            f"Date: {meeting_metadata.get('date', 'Unknown')}\n"
            f"Duration: {meeting_metadata.get('duration_minutes', 'Unknown')} minutes\n"
            f"Participants: {', '.join(meeting_metadata.get('participants', []))}"
        )

    context_parts.append(f"## Transcript\n\n{transcript_text}")
    full_context = "\n\n".join(context_parts)

    prompt = _build_prompt(framework)
    logger.info(f"Sending transcript to Claude for analysis (framework: {framework})...")

    message = client.messages.create(
        model=CLAUDE_MODEL,
        max_tokens=4096,
        messages=[
            {
                "role": "user",
                "content": f"{prompt}\n\n---\n\n{full_context}",
            }
        ],
    )

    response_text = message.content[0].text.strip()

    # Parse JSON response
    try:
        # Handle case where Claude wraps in markdown code block
        if response_text.startswith("```"):
            response_text = response_text.split("```")[1]
            if response_text.startswith("json"):
                response_text = response_text[4:]
            response_text = response_text.strip()

        analysis = json.loads(response_text)
    except json.JSONDecodeError as e:
        logger.error(f"Failed to parse Claude response as JSON: {e}")
        logger.error(f"Raw response: {response_text[:500]}")
        raise Exception(f"Claude returned invalid JSON: {e}")

    # Validate required fields
    required_fields = [
        "meeting_type", "is_sales_conversation", "participants",
        "pain_signals", "buying_signals", "summary",
    ]
    for field in required_fields:
        if field not in analysis:
            logger.warning(f"Missing required field in analysis: {field}")
            analysis[field] = [] if field in ("pain_signals", "buying_signals", "participants") else None

    # Tag with framework used
    analysis["framework"] = framework

    logger.info(
        f"Analysis complete: framework={framework}, type={analysis.get('meeting_type')}, "
        f"is_sales={analysis.get('is_sales_conversation')}"
    )

    return analysis


def analyze_transcript_from_file(filepath: str) -> dict:
    """Convenience method to analyze a transcript from a local file."""
    with open(filepath, "r") as f:
        text = f.read()
    return analyze_transcript(text)
