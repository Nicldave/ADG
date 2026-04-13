"""
Deal Scorer - The Strike Zone
Applies consistent qualification criteria to transcript analysis.
Score 0-100. Same criteria every time, regardless of who ran the call.

Supports multiple frameworks: Custom, BANT, SPICED, MEDDIC, SPIN.
When using Custom framework, uses hardcoded scoring logic (original behavior).
When using named frameworks (BANT/SPICED/MEDDIC/SPIN), Claude provides
per-category scores directly and we normalize them against framework weights.
"""

import logging
from config import AUTO_CREATE_THRESHOLD, REVIEW_THRESHOLD
from frameworks import get_framework, get_weights

logger = logging.getLogger(__name__)

# ── Custom framework scoring (original hardcoded logic) ─────────────────────

URGENCY_CATEGORIES = {"transaction_urgency", "emotional_stakes", "outgrown_skill_set"}
STRATEGIC_CATEGORIES = {"just_bookkeeping", "zero_insight", "wearing_too_many_hats"}


def _score_pain_signals(analysis: dict) -> tuple[float, list[str]]:
    signals = analysis.get("pain_signals", [])
    notes = []
    if not signals:
        return 0, ["No pain signals identified"]
    total_severity = sum(max(1, min(5, s.get("severity", 3))) for s in signals)
    avg_severity = total_severity / len(signals)
    signal_count = len(signals)
    if signal_count == 1:
        base = avg_severity * 2
    elif signal_count <= 3:
        base = avg_severity * 3
    elif signal_count <= 5:
        base = avg_severity * 3.5
    else:
        base = avg_severity * 4
    categories = {s.get("category", "") for s in signals}
    urgency_hit = categories & URGENCY_CATEGORIES
    strategic_hit = categories & STRATEGIC_CATEGORIES
    bonus = 0
    if urgency_hit:
        bonus += 3
        notes.append(f"Urgency signal: {', '.join(urgency_hit)}")
    if strategic_hit:
        bonus += 2
        notes.append(f"Core pain: {', '.join(strategic_hit)}")
    score = min(25, base + bonus)
    notes.append(f"{signal_count} pain signal(s), avg severity {avg_severity:.1f}/5")
    return score, notes


def _score_buying_intent(analysis: dict) -> tuple[float, list[str]]:
    signals = analysis.get("buying_signals", [])
    notes = []
    if not signals:
        return 0, ["No buying signals identified"]
    strength_map = {"strong": 7, "moderate": 4, "weak": 2}
    total = sum(strength_map.get(s.get("strength", "weak"), 2) for s in signals)
    score = min(20, total)
    strong = [s for s in signals if s.get("strength") == "strong"]
    moderate = [s for s in signals if s.get("strength") == "moderate"]
    notes.append(f"{len(signals)} buying signal(s): {len(strong)} strong, {len(moderate)} moderate")
    return score, notes


def _score_budget_fit(analysis: dict) -> tuple[float, list[str]]:
    budget = analysis.get("budget_indicators", {})
    notes = []
    score = 0
    if budget.get("mentioned"):
        score += 5
        notes.append(f"Budget discussed: {budget.get('range', 'range not specified')}")
    else:
        notes.append("Budget not discussed")
    willingness = budget.get("willingness", "unknown")
    if willingness == "eager":
        score += 7
        notes.append("Eager about investment")
    elif willingness == "neutral":
        score += 5
        notes.append("Neutral on budget")
    elif willingness == "hesitant":
        score += 1
        notes.append("Budget hesitation noted")
    else:
        score += 3
        notes.append("Budget disposition unknown")
    objections = analysis.get("objections", [])
    price_objections = [
        o for o in objections
        if any(w in o.get("objection", "").lower() for w in ["price", "cost", "expensive", "budget", "afford"])
    ]
    if not price_objections:
        score += 3
        notes.append("No price objections")
    elif all(o.get("resolved") for o in price_objections):
        score += 1
        notes.append("Price objections resolved")
    return min(15, score), notes


def _score_timeline_urgency(analysis: dict) -> tuple[float, list[str]]:
    timeline = analysis.get("timeline_indicators", {})
    notes = []
    urgency = timeline.get("urgency", "low")
    trigger = timeline.get("trigger_event")
    evidence = timeline.get("evidence")
    urgency_map = {"critical": 15, "high": 11, "medium": 6, "low": 2}
    score = urgency_map.get(urgency, 2)
    if trigger:
        notes.append(f"Trigger event: {trigger}")
        if urgency in ("low", "medium"):
            score = min(15, score + 3)
    if evidence:
        notes.append(f"Evidence: \"{evidence[:80]}...\"" if len(evidence) > 80 else f"Evidence: \"{evidence}\"")
    notes.append(f"Urgency level: {urgency}")
    return score, notes


def _score_decision_maker(analysis: dict) -> tuple[float, list[str]]:
    dms = analysis.get("decision_makers", [])
    notes = []
    if not dms:
        notes.append("No decision-makers identified")
        return 2, notes
    influence_map = {"decision_maker": 12, "champion": 9, "evaluator": 5, "unknown": 2, "blocker": 0}
    best_score = max(influence_map.get(dm.get("influence", "unknown"), 2) for dm in dms)
    # Multiple senior attendees bonus: shows organizational buy-in
    senior_count = sum(1 for dm in dms if dm.get("influence") in ("decision_maker", "champion"))
    if senior_count >= 2:
        best_score = min(15, best_score + 3)
        notes.append(f"{senior_count} senior stakeholders on call")
    roles = [f"{dm.get('name', '?')} ({dm.get('influence', '?')})" for dm in dms]
    notes.append(f"Decision makers: {', '.join(roles)}")
    return min(15, best_score), notes


def _score_next_steps(analysis: dict) -> tuple[float, list[str]]:
    steps = analysis.get("next_steps", [])
    notes = []
    if not steps:
        return 0, ["No next steps defined"]
    concrete = [s for s in steps if s.get("owner") and s.get("action")]
    if len(concrete) >= 2:
        score = 10
        notes.append(f"{len(concrete)} concrete next steps defined")
    elif len(concrete) == 1:
        score = 7
        notes.append(f"1 concrete next step: {concrete[0].get('action', '')[:60]}")
    else:
        score = 3
        notes.append(f"{len(steps)} vague next step(s) - no clear owner/action")
    return score, notes


CUSTOM_SCORERS = {
    "pain_signals": _score_pain_signals,
    "buying_intent": _score_buying_intent,
    "budget_fit": _score_budget_fit,
    "timeline_urgency": _score_timeline_urgency,
    "decision_maker": _score_decision_maker,
    "next_steps": _score_next_steps,
}


# ── Framework-based scoring (BANT, SPICED, MEDDIC, SPIN) ───────────────────

def _score_framework_categories(analysis: dict, framework_key: str, custom_weights: dict = None) -> dict:
    """
    Score using Claude-provided framework_scores from the analysis.
    Claude scores each category directly; we cap at the weight max.
    Optional custom_weights overrides default framework weights.
    """
    fw = get_framework(framework_key)
    categories = fw["categories"]
    # Apply custom weights if provided (must sum to 100)
    if custom_weights:
        for key in categories:
            if key in custom_weights:
                categories[key] = dict(categories[key])  # copy to avoid mutating original
                categories[key]["weight"] = custom_weights[key]
    framework_scores = analysis.get("framework_scores", {})

    breakdown = {}
    for key, cat in categories.items():
        max_pts = cat["weight"]
        fs = framework_scores.get(key, {})

        if isinstance(fs, dict):
            raw_score = fs.get("score", 0)
            evidence = fs.get("evidence", [])
            assessment = fs.get("assessment", "")
        else:
            raw_score = 0
            evidence = []
            assessment = ""

        # Depth multiplier: more evidence = higher eligible max
        evidence_count = len(evidence) if isinstance(evidence, list) else 0
        if evidence_count == 0:
            depth_ratio = 0.0
        elif evidence_count == 1:
            depth_ratio = 0.5
        elif evidence_count == 2:
            depth_ratio = 0.75
        else:
            depth_ratio = 1.0

        effective_max = int(max_pts * depth_ratio)
        score = min(effective_max, max(0, int(raw_score)))

        notes = []
        if assessment:
            notes.append(assessment)
        if evidence:
            for e in evidence[:2]:
                notes.append(f'"{e[:80]}"')

        breakdown[key] = {
            "score": score,
            "max": max_pts,
            "effective_max": effective_max,
            "depth_ratio": depth_ratio,
            "evidence_count": evidence_count,
            "label": cat.get("label", key),
            "notes": notes or ["No evidence provided"],
        }

    return breakdown


# ── Main scoring entry point ────────────────────────────────────────────────

def score_deal(analysis: dict, custom_weights: dict = None) -> dict:
    """
    Apply the Strike Zone to a transcript analysis. Returns a structured score report.
    Automatically uses the framework stored in the analysis dict.
    """
    if not analysis.get("is_sales_conversation"):
        return {
            "total_score": 0,
            "recommendation": "not_a_deal",
            "confidence": "high",
            "framework": analysis.get("framework", "custom"),
            "breakdown": {},
            "deal_name_suggestion": None,
            "key_insight": "Not a sales conversation",
        }

    framework_key = analysis.get("framework", "custom")
    fw = get_framework(framework_key)
    breakdown = {}
    total = 0

    if framework_key == "custom":
        weights = get_weights("custom")
        for name, scorer_fn in CUSTOM_SCORERS.items():
            raw_score, notes = scorer_fn(analysis)
            component_score = round(raw_score)
            breakdown[name] = {
                "score": component_score,
                "max": weights[name],
                "notes": notes,
            }
            total += component_score
    else:
        breakdown = _score_framework_categories(analysis, framework_key, custom_weights=custom_weights)
        total = sum(d["score"] for d in breakdown.values())

    total = min(100, round(total))

    # Recommendation
    if total >= AUTO_CREATE_THRESHOLD:
        recommendation = "auto_create"
        confidence = "high" if total >= 80 else "medium"
    elif total >= REVIEW_THRESHOLD:
        recommendation = "needs_review"
        confidence = "medium"
    else:
        recommendation = "not_a_deal"
        confidence = "high" if total < 30 else "low"

    # Deal name — matches Kevin's Attio convention: NN-{Company}-{Rep Initials}-{YYYY.MM}
    from datetime import datetime as _dt
    company = analysis.get("prospect_company", {})
    company_name = company.get("name") or "Unknown Company"
    month_str = _dt.now().strftime("%Y.%m")
    # Extract rep initials from the analysis (first internal/seller participant)
    rep_initials = "KO"  # fallback
    seller_name = ""
    # Find the non-prospect participant (the rep/seller)
    participants = analysis.get("participants", [])
    for p in participants:
        if isinstance(p, dict) and p.get("is_prospect") is False and p.get("name"):
            seller_name = p["name"]
            break
    # Fallback: check for a seller field
    if not seller_name:
        seller = analysis.get("seller", {})
        seller_name = seller.get("name", "") if isinstance(seller, dict) else ""
    if seller_name:
        parts = seller_name.strip().split()
        if len(parts) >= 2:
            rep_initials = (parts[0][0] + parts[-1][0]).upper()
        elif len(parts) == 1 and len(parts[0]) >= 2:
            rep_initials = parts[0][:2].upper()
    deal_name = f"NN-{company_name}-{rep_initials}-{month_str}"

    # Key insight: framework summary of what was covered and what's missing
    key_insight = _extract_key_insight(analysis, framework_key, breakdown)

    result = {
        "total_score": total,
        "recommendation": recommendation,
        "confidence": confidence,
        "framework": framework_key,
        "breakdown": breakdown,
        "deal_name_suggestion": deal_name,
        "key_insight": key_insight,
    }

    logger.info(
        f"Score: {total}/100 | Framework: {fw['name']} | "
        f"Recommendation: {recommendation} | Company: {company_name}"
    )
    return result


def _extract_key_insight(analysis: dict, framework_key: str, breakdown: dict = None) -> str:
    """Generate a one-line framework summary: what was covered well and what's missing."""
    if not breakdown:
        return ""

    # Categorize each criterion as strong, moderate, or weak based on score percentage
    strong = []
    weak = []
    for key, data in breakdown.items():
        label = data.get("label", key).lower()
        score = data.get("score", 0)
        max_score = data.get("max", 25)
        pct = score / max_score if max_score > 0 else 0
        if pct >= 0.75:
            strong.append(label)
        elif pct < 0.5:
            weak.append(label)

    # Build the summary
    parts = []
    if strong:
        parts.append(f"Strong {', '.join(strong)}")
    if weak:
        parts.append(f"{'but ' if strong else ''}{', '.join(weak)} {'need' if len(weak) > 1 else 'needs'} more clarity")

    if not parts:
        return ""

    insight = ". ".join(parts)

    # Add a specific detail from the analysis if available
    detail = ""
    fw_scores = analysis.get("framework_scores", {})
    # Find the weakest category and pull its assessment for context
    if weak and fw_scores:
        for key, val in fw_scores.items():
            if isinstance(val, dict) and val.get("label", key).lower() in weak:
                assessment = val.get("assessment", "")
                if assessment:
                    detail = assessment[:80]
                    break

    if detail:
        return f"{insight}. {detail}"[:200]
    return insight[:200]


def format_score_report(score_result: dict) -> str:
    """Format score result as a readable text report."""
    fw_name = score_result.get("framework", "custom").upper()
    lines = [
        f"DEAL SCORE: {score_result['total_score']}/100 ({fw_name})",
        f"Recommendation: {score_result['recommendation'].upper().replace('_', ' ')}",
        f"Confidence: {score_result['confidence']}",
        "",
        "BREAKDOWN:",
    ]

    for component, data in score_result.get("breakdown", {}).items():
        bar = "+" * data["score"] + "-" * (data["max"] - data["score"])
        lines.append(f"  {component:<20} {data['score']:>2}/{data['max']} [{bar}]")
        for note in data.get("notes", []):
            lines.append(f"                       {note}")

    if score_result.get("key_insight"):
        lines.extend(["", f"KEY SIGNAL: \"{score_result['key_insight']}\""])

    return "\n".join(lines)
