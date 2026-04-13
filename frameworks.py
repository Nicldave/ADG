"""
Sales Qualification Frameworks
Each framework defines scoring categories, weights, and prompt instructions
so the deal generator adapts to whichever methodology the user follows.
"""

FRAMEWORKS = {
    "custom": {
        "name": "Custom (Strike Zone)",
        "description": "Nicl.ai default scoring based on closed-won deal patterns.",
        "categories": {
            "pain_signals":     {"weight": 25, "label": "Pain Signals"},
            "buying_intent":    {"weight": 20, "label": "Buying Intent"},
            "budget_fit":       {"weight": 15, "label": "Budget Fit"},
            "timeline_urgency": {"weight": 15, "label": "Timeline Urgency"},
            "decision_maker":   {"weight": 15, "label": "Decision Maker"},
            "next_steps":       {"weight": 10, "label": "Next Steps"},
        },
        "prompt_addendum": "",
    },

    "bant": {
        "name": "BANT",
        "description": "Budget, Authority, Need, Timeline.",
        "categories": {
            "budget":    {"weight": 25, "label": "Budget"},
            "authority": {"weight": 25, "label": "Authority"},
            "need":      {"weight": 25, "label": "Need"},
            "timeline":  {"weight": 25, "label": "Timeline"},
        },
        "prompt_addendum": """
## BANT Framework Scoring
Score this conversation against the BANT methodology:

1. **Budget** (25 pts) - Has the prospect discussed budget? Do they have budget allocated? Any price sensitivity?
2. **Authority** (25 pts) - Is the person on the call the decision maker? Do they need approval from others? What's the buying committee look like? IMPORTANT: Authority is not just about titles. Score authority HIGH if: multiple senior people attend (shows organizational commitment), the prospect speaks for the company ("we decided", "our team"), they discuss budget/resources they control, they make commitments without deferring, or the entire leadership team is present even if titles are not stated. A founder, CEO, or solo GTM leader on a small team IS the decision maker by default. Only score authority LOW if the person explicitly says they need someone else's approval or cannot make decisions.
3. **Need** (25 pts) - How acute is their need? Is it a nice-to-have or a must-have? What specific problems are they trying to solve?
4. **Timeline** (25 pts) - Do they have a timeline for making a decision? Is there a trigger event or deadline? How urgent is this?

For each category, include:
- "score": 0-25 (how well this category was satisfied)
- "evidence": list of verbatim quotes supporting the score
- "assessment": one-sentence summary of where they stand
""",
    },

    "spiced": {
        "name": "SPICED",
        "description": "Situation, Pain, Impact, Critical Event, Decision.",
        "categories": {
            "situation":      {"weight": 15, "label": "Situation"},
            "pain":           {"weight": 25, "label": "Pain"},
            "impact":         {"weight": 20, "label": "Impact"},
            "critical_event": {"weight": 20, "label": "Critical Event"},
            "decision":       {"weight": 20, "label": "Decision"},
        },
        "prompt_addendum": """
## SPICED Framework Scoring
Score this conversation against the SPICED methodology:

1. **Situation** (15 pts) - What is the prospect's current state? Team size, tools, processes, growth stage?
2. **Pain** (25 pts) - What specific problems are they experiencing? How severe? Who is affected? What have they tried?
3. **Impact** (20 pts) - What is the business impact of these problems? Revenue loss, wasted time, risk exposure? Can they quantify it?
4. **Critical Event** (20 pts) - Is there a forcing function? A deadline, board meeting, funding round, audit, departure, or crisis driving urgency?
5. **Decision** (20 pts) - What does their decision process look like? Who's involved? What criteria matter? Have they evaluated alternatives? If the decision maker is on the call and can approve without committees, that scores HIGH. Small teams where the founder/leader runs GTM have short decision processes by default.

For each category, include:
- "score": 0 to the category max (how well this category was satisfied)
- "evidence": list of verbatim quotes supporting the score
- "assessment": one-sentence summary of where they stand
""",
    },

    "meddic": {
        "name": "MEDDIC",
        "description": "Metrics, Economic Buyer, Decision Criteria, Decision Process, Identify Pain, Champion.",
        "categories": {
            "metrics":           {"weight": 15, "label": "Metrics"},
            "economic_buyer":    {"weight": 20, "label": "Economic Buyer"},
            "decision_criteria": {"weight": 15, "label": "Decision Criteria"},
            "decision_process":  {"weight": 15, "label": "Decision Process"},
            "identify_pain":     {"weight": 20, "label": "Identify Pain"},
            "champion":          {"weight": 15, "label": "Champion"},
        },
        "prompt_addendum": """
## MEDDIC Framework Scoring
Score this conversation against the MEDDIC methodology:

1. **Metrics** (15 pts) - Can the prospect quantify the impact? Do they have numbers around the problem or desired outcome? Revenue, cost savings, time, headcount?
2. **Economic Buyer** (20 pts) - Has the economic buyer been identified? Are they on the call? Do we have access to them? IMPORTANT: In small companies, the person on the call often IS the economic buyer even without a C-suite title. If they discuss budget, make resource commitments, or speak with authority about organizational decisions, treat them as the economic buyer. Multiple senior attendees signals strong economic buyer access.
3. **Decision Criteria** (15 pts) - What criteria will they use to evaluate solutions? Technical requirements, ROI threshold, integration needs?
4. **Decision Process** (15 pts) - What steps do they go through to buy? Legal review, procurement, board approval? What's the typical cycle?
5. **Identify Pain** (20 pts) - Has a clear, compelling pain been identified? Is it a top-3 priority? Does it have organizational visibility?
6. **Champion** (15 pts) - Is there someone internally who will advocate for us? Do they have influence? Are they actively selling internally?

For each category, include:
- "score": 0 to the category max (how well this category was satisfied)
- "evidence": list of verbatim quotes supporting the score
- "assessment": one-sentence summary of where they stand
""",
    },

    "spin": {
        "name": "SPIN",
        "description": "Situation, Problem, Implication, Need-Payoff.",
        "categories": {
            "situation":   {"weight": 20, "label": "Situation"},
            "problem":     {"weight": 25, "label": "Problem"},
            "implication": {"weight": 30, "label": "Implication"},
            "need_payoff": {"weight": 25, "label": "Need-Payoff"},
        },
        "prompt_addendum": """
## SPIN Framework Scoring
Score this conversation against the SPIN Selling methodology:

1. **Situation** (20 pts) - How well do we understand their current situation? Team, tools, processes, context? Were good situation questions asked?
2. **Problem** (25 pts) - Were specific problems uncovered? Did the prospect acknowledge them? How many problem areas were explored?
3. **Implication** (30 pts) - Were the implications of those problems explored? Does the prospect understand the cost of inaction? Were "what happens if" questions asked? This is the most important SPIN category.
4. **Need-Payoff** (25 pts) - Did the prospect articulate the value of solving these problems? Did they describe their ideal future state? Are they mentally bought in to the solution?

For each category, include:
- "score": 0 to the category max (how well this category was satisfied)
- "evidence": list of verbatim quotes supporting the score
- "assessment": one-sentence summary of where they stand
""",
    },
}

FRAMEWORK_NAMES = list(FRAMEWORKS.keys())


def get_framework(name: str) -> dict:
    name = name.lower().strip()
    if name not in FRAMEWORKS:
        raise ValueError(f"Unknown framework: '{name}'. Choose from: {', '.join(FRAMEWORK_NAMES)}")
    return FRAMEWORKS[name]


def get_weights(name: str) -> dict:
    fw = get_framework(name)
    return {k: v["weight"] for k, v in fw["categories"].items()}


def get_labels(name: str) -> dict:
    fw = get_framework(name)
    return {k: v["label"] for k, v in fw["categories"].items()}
