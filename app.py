"""
Auto Deal Generator - Streamlit UI
Run: streamlit run app.py
"""

import os
import sys
from datetime import datetime

import streamlit as st

# Add this directory to path so local modules resolve
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# ── Page config (must be first Streamlit call) ─────────────────────────────
st.set_page_config(
    page_title="Fairplay",
    page_icon="🎯",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── Module imports with graceful error ─────────────────────────────────────
try:
    import transcript_analyzer
    import deal_scorer
    import crm as crm_factory
    import fireflies_client
    import hubspot_client
    from frameworks import FRAMEWORKS, get_framework, get_labels
    MODULES_OK = True
    MODULE_ERROR = None
except ImportError as e:
    MODULES_OK = False
    MODULE_ERROR = str(e)

# ── Session state init ─────────────────────────────────────────────────────
for key in ("analysis", "score_result", "metadata", "deal_result"):
    if key not in st.session_state:
        st.session_state[key] = None

# ── Helper renderers ───────────────────────────────────────────────────────
SEV_ICON = {1: "🟢", 2: "🟡", 3: "🟠", 4: "🔴", 5: "🔴"}
STR_ICON = {"high": "💪", "medium": "👍", "low": "👌"}
INFLUENCE_ICON = {"champion": "🏆", "evaluator": "🔍", "blocker": "🚧"}


def show_recommendation(rec: str, score: int):
    if rec == "auto_create":
        st.success(f"✅  AUTO CREATE  —  Score: {score}/100")
    elif rec == "needs_review":
        st.warning(f"👀  NEEDS REVIEW  —  Score: {score}/100")
    else:
        st.error(f"❌  NOT A DEAL  —  Score: {score}/100")


def show_breakdown(breakdown: dict):
    if not breakdown:
        return
    cols = st.columns(len(breakdown))
    for i, (key, val) in enumerate(breakdown.items()):
        label = key.replace("_", " ").title()
        score = val.get("score", 0)
        max_score = val.get("max", 0)
        notes = val.get("notes", [])
        with cols[i]:
            st.metric(label=label, value=f"{score}", delta=f"/ {max_score}", delta_color="off")
            if notes:
                st.caption(notes[0])


def show_pain_signals(signals: list):
    if not signals:
        st.caption("None identified.")
        return
    for s in signals:
        sev = s.get("severity", 1)
        cat = s.get("category", "unknown").replace("_", " ")
        quote = s.get("quote", "")
        st.markdown(f"{SEV_ICON.get(sev, '⚪')} **{cat}** — severity {sev}/5")
        if quote:
            st.caption(f'"{quote}"')


def show_buying_signals(signals: list):
    if not signals:
        st.caption("None identified.")
        return
    for s in signals:
        strength = s.get("strength", "low")
        st.markdown(
            f"{STR_ICON.get(strength, '')} **{s.get('signal', '')}** _{strength}_"
        )
        if s.get("evidence"):
            st.caption(f'"{s["evidence"]}"')


def show_decision_makers(dms: list):
    if not dms:
        st.caption("None identified.")
        return
    for dm in dms:
        influence = dm.get("influence", "")
        st.markdown(
            f"{INFLUENCE_ICON.get(influence, '👤')} **{dm.get('name', '?')}**"
            f" — {dm.get('title', '')} *({influence})*"
        )


def show_next_steps(steps: list):
    if not steps:
        st.caption("None defined.")
        return
    for s in steps:
        owner = s.get("owner", "TBD")
        deadline = s.get("deadline", "TBD")
        st.markdown(
            f"- {s.get('action', '')}  \n  *owner: {owner} · by: {deadline}*"
        )


def show_objections(objs: list):
    if not objs:
        st.caption("None recorded.")
        return
    for obj in objs:
        icon = "✅" if obj.get("resolved") else "❌"
        st.markdown(f"{icon} **{obj.get('objection', '')}**")
        if obj.get("response"):
            st.caption(f"Response: {obj['response']}")


# ── Header ─────────────────────────────────────────────────────────────────
st.title("Fairplay")
st.caption(
    "The Strike Zone — consistent deal qualification from every sales conversation. "
    "Same criteria every time, regardless of who ran the call."
)

if not MODULES_OK:
    st.error(
        f"**Module import error:** {MODULE_ERROR}\n\n"
        "Run `pip install -r requirements.txt` from the `auto-deal-generator/` directory."
    )
    st.stop()

# ── Sidebar ────────────────────────────────────────────────────────────────
with st.sidebar:
    st.header("⚙️ Settings")

    framework_key = st.selectbox(
        "Scoring Framework",
        list(FRAMEWORKS.keys()),
        format_func=lambda x: FRAMEWORKS[x]["name"],
    )
    st.caption(FRAMEWORKS[framework_key]["description"])

    crm_target = st.selectbox(
        "CRM Target",
        ["hubspot", "attio"],
        format_func=lambda x: {"hubspot": "HubSpot", "attio": "Attio"}[x],
    )

    dry_run = st.checkbox("Dry Run (analyze only, don't create deals)", value=True)

    st.divider()
    st.markdown("**Strike Zone Thresholds**")
    st.markdown("🟢 70+ → Auto Create")
    st.markdown("🟡 50–69 → Needs Review")
    st.markdown("🔴 < 50 → Not a Deal")

    st.divider()
    _fw = get_framework(framework_key)
    st.markdown(f"**{_fw['name']} Weights**")
    for _key, _cat in _fw["categories"].items():
        st.caption(f"{_cat['label']}: {_cat['weight']}")

# ── Input ──────────────────────────────────────────────────────────────────
tab_paste, tab_fireflies, tab_hubspot = st.tabs(
    ["📋 Paste Transcript", "🔥 Fireflies Meeting", "🟠 HubSpot Call"]
)

transcript_text = None
metadata = None

with tab_paste:
    col_txt, col_meta = st.columns([3, 1])
    with col_txt:
        pasted = st.text_area(
            "Paste transcript",
            height=220,
            placeholder=(
                "Speaker 1 (Dave): Thanks for joining, tell me what's been going on...\n"
                "Speaker 2 (Prospect): Honestly we've been struggling with visibility into..."
            ),
            label_visibility="collapsed",
        )
    with col_meta:
        meeting_title = st.text_input("Meeting title", placeholder="Acme - Discovery Call")
        meeting_date = st.date_input("Meeting date", value=datetime.today())

    if st.button("▶  Analyze", type="primary", key="btn_paste", use_container_width=True):
        if not pasted.strip():
            st.error("Paste a transcript first.")
        else:
            transcript_text = pasted
            metadata = {
                "title": meeting_title or "Pasted Transcript",
                "date": meeting_date.isoformat(),
                "source": "manual",
                "participants": [],
            }

with tab_fireflies:
    ff_id = st.text_input("Fireflies Meeting ID", placeholder="abc123xyz")
    if st.button("Fetch & Analyze", type="primary", key="btn_ff"):
        if not ff_id.strip():
            st.error("Enter a Fireflies meeting ID.")
        else:
            with st.spinner("Fetching from Fireflies..."):
                try:
                    t = fireflies_client.get_transcript(ff_id.strip())
                    transcript_text = fireflies_client.format_transcript_text(t)
                    metadata = fireflies_client.get_meeting_metadata(t)
                    if transcript_text:
                        st.success(f"Fetched: {metadata.get('title', ff_id)}")
                    else:
                        st.warning("Meeting found but transcript is empty.")
                except Exception as e:
                    st.error(f"Fireflies error: {e}")

with tab_hubspot:
    hs_id = st.text_input("HubSpot Call ID", placeholder="12345678")
    if st.button("Fetch & Analyze", type="primary", key="btn_hs"):
        if not hs_id.strip():
            st.error("Enter a HubSpot call ID.")
        else:
            with st.spinner("Fetching from HubSpot..."):
                try:
                    call = hubspot_client.get_call(hs_id.strip())
                    transcript_text = hubspot_client.format_hubspot_transcript(call)
                    metadata = hubspot_client.get_call_metadata(call)
                    if transcript_text:
                        st.success(f"Fetched: {metadata.get('title', hs_id)}")
                    else:
                        st.warning(
                            "Call found but no transcript — AI transcription may not be enabled for this call."
                        )
                        transcript_text = None
                except Exception as e:
                    st.error(f"HubSpot error: {e}")

# ── Run Analysis ───────────────────────────────────────────────────────────
if transcript_text:
    with st.spinner(f"Running Claude analysis ({_fw['name']})..."):
        try:
            analysis = transcript_analyzer.analyze_transcript(transcript_text, metadata, framework=framework_key)
            score_result = deal_scorer.score_deal(analysis)
            st.session_state.analysis = analysis
            st.session_state.score_result = score_result
            st.session_state.metadata = metadata
            st.session_state.deal_result = None  # Reset previous result
        except Exception as e:
            st.error(f"Analysis failed: {e}")
            st.session_state.analysis = None

# ── Results ────────────────────────────────────────────────────────────────
if st.session_state.analysis and st.session_state.score_result:
    analysis = st.session_state.analysis
    score_result = st.session_state.score_result
    metadata = st.session_state.metadata

    st.divider()

    if not analysis.get("is_sales_conversation"):
        st.warning(
            "⚠️ This does not appear to be a sales conversation. "
            "No deal qualification applied."
        )
        if analysis.get("summary"):
            st.info(analysis["summary"])
    else:
        active_fw_key = analysis.get("framework", "custom")
        active_fw = get_framework(active_fw_key)
        labels = get_labels(active_fw_key)

        # ── Score + recommendation ─────────────────────────────────────────
        col_a, col_b = st.columns([1, 2])
        with col_a:
            total = score_result["total_score"]
            st.metric("Strike Zone Score", f"{total} / 100")
            st.progress(total / 100)
            st.caption(f"Framework: {active_fw['name']}")
            st.caption(f"Confidence: {score_result.get('confidence', '—').upper()}")

        with col_b:
            show_recommendation(score_result["recommendation"], total)
            st.markdown(f"**Deal Name:** {score_result.get('deal_name_suggestion', '—')}")
            if score_result.get("key_insight"):
                st.markdown(f"**Key Signal:** _{score_result['key_insight']}_")

        # ── Score breakdown (dynamic labels from framework) ──────────────
        st.subheader("Score Breakdown")
        breakdown = score_result.get("breakdown", {})
        if breakdown:
            cols = st.columns(len(breakdown))
            for i, (key, val) in enumerate(breakdown.items()):
                label = labels.get(key, key.replace("_", " ").title())
                score = val.get("score", 0)
                max_score = val.get("max", 0)
                notes = val.get("notes", [])
                with cols[i]:
                    st.metric(label=label, value=f"{score}", delta=f"/ {max_score}", delta_color="off")
                    if notes:
                        st.caption(notes[0])

        # ── Company info ────────────────────────────────────────────────
        st.subheader("Meeting Intel")
        company = analysis.get("prospect_company", {})
        col1, col2 = st.columns(2)
        with col1:
            st.markdown("**Company**")
            st.write(f"Name: **{company.get('name', '—')}**")
            st.write(f"Industry: {company.get('industry', '—')}")
            st.write(f"Size: {company.get('estimated_size', '—')}")
            st.write(f"Meeting type: {analysis.get('meeting_type', '—')}")
        with col2:
            competitors = analysis.get("competitors_mentioned", [])
            st.markdown("**Competitors**")
            if competitors:
                for c in competitors:
                    st.write(f"- {c}")
            else:
                st.caption("None mentioned")

        # ── Summary ──────────────────────────────────────────────────────
        if analysis.get("summary"):
            st.info(f"**Summary:** {analysis['summary']}")

        # ── Detail expanders (framework-aware) ───────────────────────────
        if active_fw_key == "custom":
            # Original custom layout: pain signals, buying signals, DMs, etc.
            col_left, col_right = st.columns(2)
            with col_left:
                n_pain = len(analysis.get("pain_signals", []))
                with st.expander(f"Pain Signals ({n_pain})", expanded=n_pain > 0):
                    show_pain_signals(analysis.get("pain_signals", []))
                n_dm = len(analysis.get("decision_makers", []))
                with st.expander(f"Decision Makers ({n_dm})", expanded=n_dm > 0):
                    show_decision_makers(analysis.get("decision_makers", []))
                n_obj = len(analysis.get("objections", []))
                with st.expander(f"Objections ({n_obj})"):
                    show_objections(analysis.get("objections", []))
            with col_right:
                n_buy = len(analysis.get("buying_signals", []))
                with st.expander(f"Buying Signals ({n_buy})", expanded=n_buy > 0):
                    show_buying_signals(analysis.get("buying_signals", []))
                n_ns = len(analysis.get("next_steps", []))
                with st.expander(f"Next Steps ({n_ns})", expanded=n_ns > 0):
                    show_next_steps(analysis.get("next_steps", []))
        else:
            # Named framework: show per-category evidence from framework_scores
            fw_scores = analysis.get("framework_scores", {})
            cat_keys = list(active_fw["categories"].keys())
            mid = (len(cat_keys) + 1) // 2
            col_left, col_right = st.columns(2)
            for i, key in enumerate(cat_keys):
                col = col_left if i < mid else col_right
                with col:
                    label = labels.get(key, key)
                    fs = fw_scores.get(key, {})
                    evidence = fs.get("evidence", []) if isinstance(fs, dict) else []
                    assessment = fs.get("assessment", "") if isinstance(fs, dict) else ""
                    with st.expander(f"{label}", expanded=True):
                        if assessment:
                            st.markdown(f"**{assessment}**")
                        if evidence:
                            for e in evidence:
                                st.caption(f'"{e}"')
                        elif not assessment:
                            st.caption("No evidence captured.")

            # Still show objections and next steps for all frameworks
            col_left2, col_right2 = st.columns(2)
            with col_left2:
                n_obj = len(analysis.get("objections", []))
                with st.expander(f"Objections ({n_obj})"):
                    show_objections(analysis.get("objections", []))
            with col_right2:
                n_ns = len(analysis.get("next_steps", []))
                with st.expander(f"Next Steps ({n_ns})", expanded=n_ns > 0):
                    show_next_steps(analysis.get("next_steps", []))

        # ── Create Deal ───────────────────────────────────────────────────
        st.divider()
        rec = score_result["recommendation"]

        if rec not in ("auto_create", "needs_review"):
            st.info("Score below threshold (50) — deal not created.")
        elif st.session_state.deal_result:
            dr = st.session_state.deal_result
            if dr.get("dry_run"):
                st.success(
                    f"✅ [DRY RUN] Would create **{dr['deal_name']}** in "
                    f"{crm_target.upper()}"
                )
            elif dr.get("deal_id"):
                st.success(f"✅ Deal created: **{dr['deal_name']}**")
                if dr.get("deal_url"):
                    st.markdown(f"[→ View in {crm_target.upper()}]({dr['deal_url']})")
                col_d1, col_d2 = st.columns(2)
                with col_d1:
                    st.write(f"Deal ID: `{dr['deal_id']}`")
                    st.write(f"Stage: {dr.get('stage', '—')}")
                with col_d2:
                    st.write(f"Company ID: `{dr.get('company_id', '—')}`")
                    n_contacts = len(dr.get("associated_contacts", []))
                    st.write(f"Contacts linked: {n_contacts}")
            else:
                st.error("Deal creation returned no result — check logs.")
        else:
            crm_label = crm_target.upper()
            dry_prefix = "[DRY RUN] " if dry_run else ""
            if st.button(
                f"{dry_prefix}Create Deal in {crm_label}",
                type="primary",
                use_container_width=True,
            ):
                crm_client = crm_factory.get_client(crm_target)
                with st.spinner(f"{'Simulating' if dry_run else 'Creating'} deal..."):
                    try:
                        result = crm_client.create_deal(
                            score_result,
                            analysis,
                            st.session_state.metadata,
                            dry_run=dry_run,
                        )
                        st.session_state.deal_result = result
                        st.rerun()
                    except Exception as e:
                        st.error(f"Deal creation failed: {e}")
