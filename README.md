# Auto Deal Generator

Removes human variability from deal qualification. Every sales meeting gets scored against the same criteria — the strike zone doesn't change based on who ran the call.

## How It Works

```
Fireflies meeting ends
       ↓
Pull transcript via Fireflies GraphQL API
       ↓
Claude analyzes: extracts pain signals, buying intent,
decision makers, next steps, budget indicators, urgency
       ↓
Strike Zone scoring (0-100) across 6 weighted criteria
       ↓
Score ≥ 70  → Auto-create HubSpot deal (Qualified stage)
Score 50-69 → Create deal in "Needs Review" stage + Slack alert
Score < 50  → Log only, no deal created
       ↓
Slack notification with score, key signal, HubSpot link
```

## Setup

**1. Install dependencies**
```bash
cd resources/auto-deal-generator
pip install -r requirements.txt
```

**2. Configure API keys**
```bash
cp .env.example .env
```
Edit `.env` with your actual keys:
- `FIREFLIES_API_KEY` — from [Fireflies Integrations](https://app.fireflies.ai/integrations/custom/fireflies)
- `ANTHROPIC_API_KEY` — from [Anthropic Console](https://console.anthropic.com/settings/keys)
- `HUBSPOT_API_KEY` — Private App token (see scopes below)
- `SLACK_WEBHOOK_URL` — optional, for notifications

**HubSpot token scopes required:**
- `crm.objects.deals.write`
- `crm.objects.contacts.read`
- `crm.objects.companies.read`
- `crm.objects.notes.write`

**HubSpot pipeline config** — add your pipeline/stage IDs to `.env`:
```
HUBSPOT_PIPELINE_ID=default
HUBSPOT_STAGE_QUALIFIED=qualifiedtobuy
HUBSPOT_STAGE_REVIEW=appointmentscheduled
```
Find IDs in: HubSpot → Settings → Objects → Deals → Pipelines

## Usage

**Process transcripts since last run (default behavior)**
```bash
python deal_generator.py
```

**Process last N days**
```bash
python deal_generator.py --days 7
```

**Process a specific Fireflies transcript**
```bash
python deal_generator.py --transcript-id abc123def
```

**Process a local transcript file (for testing)**
```bash
python deal_generator.py --file path/to/transcript.txt
```

**Dry run — preview without creating deals**
```bash
python deal_generator.py --dry-run
python deal_generator.py --days 7 --dry-run
```

## Strike Zone Scoring

| Criteria | Weight | What Triggers High Scores |
|---|---|---|
| Pain Signal Strength | 25 pts | Multiple signals, high severity, urgency categories |
| Buying Intent | 20 pts | Questions about pricing/process/timing, explicit interest |
| Budget Fit | 15 pts | Budget discussed, no price shock, willing to invest |
| Timeline Urgency | 15 pts | Active trigger event (departure, acquisition, audit, deadline) |
| Decision Maker Present | 15 pts | Champion or decision-maker on the call |
| Next Steps Defined | 10 pts | Concrete actions with owners, not "we'll follow up" |

**Thresholds** (adjustable in `config.py`):
- 70+ → Auto-create deal
- 50-69 → Create as "needs review"
- Below 50 → Log only

## Pain Signal Categories

Calibrated against Ascent CFO's 13 closed-won transcripts:

1. `just_bookkeeping` — "no strategic thought"
2. `zero_insight` — "zero insight on what our fund recycling capability is"
3. `jerry_rigged_systems` — "I cannot get them to match up"
4. `wearing_too_many_hats` — "I have essentially 3 jobs"
5. `transaction_urgency` — "We needed her yesterday"
6. `outgrown_skill_set` — "this thing has outgrown our skill set"
7. `strategic_partner_need` — "an operational brain alongside me"
8. `emotional_stakes` — "it's either that, or we go out of business"
9. `budget_value_focus` — explicit investment discussion
10. `growth_trajectory` — M&A, fundraise, major expansion

## Automation (Optional)

**Run every morning at 8am (cron)**
```bash
# Add to crontab: crontab -e
0 8 * * * cd /path/to/auto-deal-generator && python deal_generator.py
```

**Fireflies webhook → instant processing**
Set up in Fireflies: Settings → Webhooks → "Meeting Processed"
Point to a small webhook receiver that calls `deal_generator.run(transcript_id=id)`

## Extending for Other Clients

The scoring weights and thresholds in `config.py` are tunable. To configure for a new client:
1. Copy `.env.example` to a client-specific `.env`
2. Adjust `SCORING_WEIGHTS` based on their ICP
3. Update `PAIN_CATEGORIES` in `config.py` to match their prospect language patterns
4. Set client-specific HubSpot pipeline/stage IDs

## Files

| File | Purpose |
|---|---|
| `deal_generator.py` | Main orchestrator, CLI entry point |
| `fireflies_client.py` | Fireflies GraphQL API (transcript ingestion) |
| `transcript_analyzer.py` | Claude-powered structured analysis |
| `deal_scorer.py` | Strike Zone scoring logic |
| `hubspot_client.py` | HubSpot deal/company/contact operations |
| `config.py` | Settings, thresholds, weights |
| `.env.example` | API key template |
