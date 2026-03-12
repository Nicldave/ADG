# Auto Deal Generator

## What It Does

The Auto Deal Generator removes human variability from deal qualification. It takes a sales meeting transcript, runs it through Claude for structured analysis, scores it against a consistent qualification framework (the "Strike Zone"), and optionally creates a deal in your CRM with full context attached.

Same criteria, every time, regardless of who ran the call.

---

## How It Works

```
Transcript Source           Analysis              Scoring              Output
─────────────────     ──────────────────     ────────────────     ──────────────
                      Claude extracts:        Strike Zone          CRM Deal:
  Paste text    ─┐    - Pain signals          weights each    ─┐   - Deal name
  Fireflies API ─┤──> - Buying signals   ──>  category and    │──> - Stage
  HubSpot calls ─┘    - Budget indicators     produces a      │    - Close date
                       - Timeline urgency      0-100 score    ─┘   - Amount
                       - Decision makers                           - Description
                       - Objections                                - Company
                       - Next steps                                - Contacts
                       - Company info                              + Slack alert
```

### Step 1: Transcript Ingestion

Three input methods:

| Method | How | When to Use |
|--------|-----|-------------|
| **Paste** | Copy/paste transcript text into the UI or pass a file via CLI | Testing, one-off analysis, transcripts from any source |
| **Fireflies API** | Enter a Fireflies meeting ID or batch-process since last run | Automated pipeline with Fireflies-recorded meetings |
| **HubSpot Calls** | Enter a HubSpot call ID or batch-process recent calls | Teams already recording calls in HubSpot |

### Step 2: Claude Analysis

The transcript is sent to Claude (claude-sonnet-4-20250514) with a structured prompt. Claude extracts:

**Participants & Company**
- Each person's name, role, company, and whether they're a prospect
- Company name, industry, estimated size, estimated revenue

**Pain Signals** (Custom framework)
- Mapped to 10 categories derived from real closed-won deals:
  1. `just_bookkeeping` - Recordkeeping but no strategic finance
  2. `zero_insight` - No visibility into business performance
  3. `jerry_rigged_systems` - Finance stack held together with duct tape
  4. `wearing_too_many_hats` - CEO/founder doing finance work
  5. `transaction_urgency` - Active deal/departure/audit driving urgency
  6. `outgrown_skill_set` - Business complexity exceeding team capability
  7. `strategic_partner_need` - Want a thinking partner, not just numbers
  8. `emotional_stakes` - Existential business pressure
  9. `budget_value_focus` - Explicit discussion of spending and value
  10. `growth_trajectory` - Active expansion, fundraise, or M&A
- Each signal includes: verbatim quote, severity (1-5), speaker attribution

**Buying Signals**
- Explicit interest indicators (pricing questions, process questions, timeline asks)
- Each rated: weak, moderate, or strong, with verbatim evidence

**Budget Indicators**
- Whether budget was discussed, range mentioned, concerns raised
- Willingness: eager, neutral, hesitant, or unknown

**Timeline Indicators**
- Urgency level: low, medium, high, critical
- Trigger event (if any), target date, verbatim evidence

**Decision Makers**
- Name, title, and influence level: champion, evaluator, decision_maker, blocker, unknown

**Objections**
- What they pushed back on, how it was handled, whether it was resolved

**Next Steps**
- Action agreed, who owns it, deadline (if stated)

**Competitors Mentioned**
- Any competitor names or alternative services referenced

**Summary**
- 2-3 sentence summary of the meeting and its sales significance

### Step 3: Scoring (The Strike Zone)

The analysis is scored 0-100 using weighted criteria. Five scoring frameworks are available:

#### Custom (Strike Zone) - Default

Built from analysis of real closed-won deal transcripts. Uses hardcoded Python scoring logic with nuanced rules (e.g., urgency category signals get bonus points, price objections that were resolved score differently than unresolved ones).

| Category | Weight | What It Measures |
|----------|--------|-----------------|
| Pain Signals | 25 | Number + severity of pain signals, category bonuses for urgency/strategic signals |
| Buying Intent | 20 | Explicit interest signals, strength-weighted (strong=7, moderate=4, weak=2) |
| Budget Fit | 15 | Budget discussed + willingness + absence of price objections |
| Timeline Urgency | 15 | Urgency level (critical=15, high=11, medium=6, low=2) + trigger event bonus |
| Decision Maker | 15 | Highest influence level present (decision_maker=12, champion=9, evaluator=5) |
| Next Steps | 10 | Concrete next steps with clear owner and action (2+=10, 1=7, vague=3) |

#### BANT

| Category | Weight | What It Measures |
|----------|--------|-----------------|
| Budget | 25 | Budget discussed, allocated, price sensitivity |
| Authority | 25 | Decision maker on call, buying committee clarity |
| Need | 25 | Acuteness of need, must-have vs nice-to-have |
| Timeline | 25 | Decision timeline, trigger events, urgency |

#### SPICED

| Category | Weight | What It Measures |
|----------|--------|-----------------|
| Situation | 15 | Current state: team size, tools, processes, growth stage |
| Pain | 25 | Specific problems, severity, who's affected, what they've tried |
| Impact | 20 | Business impact: revenue loss, wasted time, risk, quantified cost |
| Critical Event | 20 | Forcing function: deadline, board meeting, funding, audit, crisis |
| Decision | 20 | Decision process, who's involved, criteria, alternatives evaluated |

#### MEDDIC

| Category | Weight | What It Measures |
|----------|--------|-----------------|
| Metrics | 15 | Quantified impact: revenue, cost savings, time, headcount |
| Economic Buyer | 20 | Identified, on call, accessible |
| Decision Criteria | 15 | Technical requirements, ROI threshold, integration needs |
| Decision Process | 15 | Buying steps: legal, procurement, board approval, cycle length |
| Identify Pain | 20 | Clear compelling pain, top-3 priority, organizational visibility |
| Champion | 15 | Internal advocate with influence, actively selling internally |

#### SPIN

| Category | Weight | What It Measures |
|----------|--------|-----------------|
| Situation | 20 | Understanding of current state, quality of situation questions |
| Problem | 25 | Specific problems uncovered and acknowledged |
| Implication | 30 | Cost of inaction explored, "what happens if" questions asked |
| Need-Payoff | 25 | Prospect articulates value of solution, mental buy-in |

**For named frameworks** (BANT/SPICED/MEDDIC/SPIN), Claude provides per-category scores directly with evidence and assessments. The scorer caps each at the category weight maximum.

**For Custom framework**, scoring uses hardcoded Python logic with detailed rules per category (the original behavior).

### Step 4: Recommendation

Based on the total score:

| Score | Recommendation | Action |
|-------|---------------|--------|
| 70-100 | **Auto Create** | Deal created automatically in CRM |
| 50-69 | **Needs Review** | Deal created but flagged for human review |
| 0-49 | **Not a Deal** | Logged only, no deal created |

### Step 5: CRM Deal Creation

When the score qualifies (50+), a deal is created in the selected CRM.

**Deal record contains:**

| Field | Source | Logic |
|-------|--------|-------|
| Deal Name | Company + trigger | e.g. "Acme Corp - Tax season scaling" |
| Stage | Score threshold | 70+ = Qualified, 50-69 = Needs Review |
| Close Date | Timeline urgency | Critical=14d, High=21d, Medium=30d, Low=60d |
| Amount | Budget indicators | Parsed from budget range if mentioned |
| Description | Full analysis | See below |

**The description field includes:**
- Score with framework name (e.g. "Score: 82/100 (BANT)")
- Meeting title and date
- Recording link (if available from source)
- AI-generated summary
- Up to 5 pain signals with category, verbatim quote, and severity
- Up to 3 buying signals with strength and evidence
- Up to 5 objections with resolved/unresolved status and response
- Up to 3 next steps with owner and deadline
- Full score breakdown by category
- Key signal (single most impactful quote)

**Associations:**
- Company: looked up by name, created if not found
- Contacts: up to 3 decision makers matched by name

### Step 6: Slack Notification

If a Slack webhook is configured, a formatted message is posted with:
- Score bar visualization (e.g. `Score: 82/100 [████████░░]`)
- Company name and deal name
- Summary and key signal quote
- Link to the CRM deal record

---

## Supported CRMs

| CRM | Status | API Pattern |
|-----|--------|-------------|
| **HubSpot** | Ready (needs API key) | REST v3/v4, private app token |
| **Attio** | Ready (configured) | REST v2, upsert pattern with matching_attribute |

Both CRM clients expose the same interface:
- `create_deal(score_result, analysis, metadata, dry_run)`
- `find_or_create_company(name, industry)`
- `find_contact_by_name(name, company_name)`

The `--crm` flag (CLI) or dropdown (UI) selects the target. A factory module (`crm.py`) returns the correct client.

---

## Interfaces

### Streamlit UI

```bash
cd resources/auto-deal-generator
streamlit run app.py
```

Features:
- Three input tabs: Paste, Fireflies, HubSpot
- Sidebar: framework selector, CRM target, dry-run toggle, threshold reference, weight display
- Score visualization: metric + progress bar + recommendation badge
- Score breakdown: per-category metrics with notes
- Meeting intel: company info, competitors mentioned
- Detail expanders: framework-aware (Custom shows pain/buying/DM expanders; named frameworks show per-category evidence)
- One-click deal creation with CRM confirmation

### CLI

```bash
# Process a local transcript file
python deal_generator.py --file transcript.txt

# Process a specific Fireflies transcript
python deal_generator.py --transcript-id abc123

# Process a HubSpot call
python deal_generator.py --source hubspot --transcript-id 12345678

# Batch: process all Fireflies transcripts from last 7 days
python deal_generator.py --days 7

# Use BANT framework, target Attio, dry run
python deal_generator.py --file call.txt --framework bant --crm attio --dry-run

# Pull from HubSpot, create deals in Attio
python deal_generator.py --source hubspot --crm attio
```

**CLI flags:**
| Flag | Options | Default | Description |
|------|---------|---------|-------------|
| `--days` | integer | Since last run (or 7) | Process transcripts from last N days |
| `--transcript-id` | string | — | Process a specific transcript/call ID |
| `--file` | path | — | Process a local transcript file |
| `--source` | fireflies, hubspot | fireflies | Where to pull transcripts from |
| `--crm` | hubspot, attio | hubspot | Where to create deals |
| `--framework` | custom, bant, spiced, meddic, spin | From env or custom | Scoring framework |
| `--dry-run` | flag | false | Preview without creating deals |

---

## File Structure

```
auto-deal-generator/
├── app.py                    # Streamlit UI
├── deal_generator.py         # CLI orchestrator + main pipeline
├── transcript_analyzer.py    # Claude-powered structured analysis
├── deal_scorer.py            # Strike Zone scoring logic (all frameworks)
├── frameworks.py             # Framework definitions (categories, weights, prompts)
├── crm.py                    # CRM factory (returns hubspot or attio client)
├── hubspot_client.py         # HubSpot API: calls ingestion + deal creation
├── attio_client.py           # Attio API: deal creation + company/contact lookup
├── fireflies_client.py       # Fireflies GraphQL API: transcript ingestion
├── config.py                 # Environment config loader
├── requirements.txt          # Python dependencies
├── .env                      # API keys (not committed)
└── .env.example              # Template for API keys
```

---

## Configuration

### Environment Variables

```bash
# Required
ANTHROPIC_API_KEY=            # Claude API for transcript analysis

# CRM (at least one)
HUBSPOT_API_KEY=              # HubSpot private app token
ATTIO_API_KEY=                # Attio API token

# Transcript source (optional - for automated ingestion)
FIREFLIES_API_KEY=            # Fireflies.ai GraphQL API key

# Notifications (optional)
SLACK_WEBHOOK_URL=            # Slack incoming webhook for deal alerts

# Scoring
DEFAULT_FRAMEWORK=custom      # Default framework for batch/automated processing
                              # Options: custom, bant, spiced, meddic, spin

# HubSpot pipeline config
HUBSPOT_PIPELINE_ID=default
HUBSPOT_STAGE_QUALIFIED=qualifiedtobuy
HUBSPOT_STAGE_REVIEW=appointmentscheduled

# Attio stage config
ATTIO_DEAL_STAGE_QUALIFIED=Qualified
ATTIO_DEAL_STAGE_REVIEW=Needs Review

# Attio relationship attributes (workspace-specific)
ATTIO_DEAL_COMPANY_ATTR=associated_workspace_member
ATTIO_DEAL_PEOPLE_ATTR=associated_people
```

### HubSpot Required Scopes
- `crm.objects.deals.read`
- `crm.objects.deals.write`
- `crm.objects.contacts.read`
- `crm.objects.contacts.write` (required by v4 association API)
- `crm.objects.companies.read`
- `crm.objects.companies.write` (required by v4 association API + company creation)
- `crm.objects.calls.read` (required for HubSpot transcript ingestion)

### Attio Required Scopes
- `record_permission:read-write`
- `object_configuration:read`

---

## Scoring Thresholds

| Threshold | Value | Changeable In |
|-----------|-------|---------------|
| Auto Create | 70+ | `config.py` → `AUTO_CREATE_THRESHOLD` |
| Needs Review | 50-69 | `config.py` → `REVIEW_THRESHOLD` |
| Not a Deal | < 50 | (implicit) |

---

## Dependencies

```
anthropic>=0.40.0
requests>=2.31.0
python-dotenv>=1.0.0
streamlit>=1.32.0
```

Python 3.9+. Install with:
```bash
pip install -r requirements.txt
```

---

## Scoring Validation

Tested against 25 Ascent CFO transcripts (13 closed-won + 12 closed-lost):
- All scored 77-89 on Custom framework
- System scores **conversation quality**, not deal outcomes
- A well-run discovery call scores high regardless of whether the deal eventually closed — the win/loss decision happens post-discovery
- BANT cross-validated: same transcript scored 82/100 (BANT) vs 88/100 (Custom) — different lens, consistent quality assessment

---

## Architecture Decisions

**Why hardcoded scoring for Custom, Claude-scored for named frameworks?**
The Custom framework was built from analysis of specific closed-won patterns (Ascent CFO). The scoring rules encode domain knowledge (e.g., urgency category signals get bonus points, resolved price objections score differently than unresolved). Named frameworks are industry-standard — Claude understands them natively and scores accurately against the defined criteria.

**Why a CRM factory pattern?**
Both CRM clients expose the same interface (`create_deal`, `find_or_create_company`, `find_contact_by_name`). The factory (`crm.py`) returns the right module based on a flag. Adding a new CRM means writing one client file and adding it to the factory.

**Why three tiers instead of a binary?**
The "needs review" tier (50-69) catches conversations that have some qualification signals but not enough for full confidence. This prevents both false positives (auto-creating deals from weak conversations) and false negatives (discarding conversations that a human might recognize as opportunities).

**Why not separate "nurture" from "needs review"?**
"Needs review" means the system isn't sure — it found some signals but not enough. A human needs to look. "Nurture" would imply a decision has already been made (it's not a deal yet). That decision belongs to the rep after review, not to the scoring system.
