"""
Email Building Agent.

Reads approved decision_makers from Supabase, writes personalised email
drafts + 3 Instantly personalisation lines to the outreach table.

No web calls — uses only the research_notes already gathered by the enrichment agent.

Usage:
    python -m agents.email.agent --campaign <uuid> [--dry-run] [--limit N]
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import re
import time
import uuid
from typing import Optional

import anthropic
from dotenv import load_dotenv
from supabase import Client, create_client

from agents.config import CampaignConfig, OutreachConfig, load_campaign_config
from agents.results import EmailRunResult

load_dotenv()
log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

SYSTEM_PROMPT = """You are writing a short, highly personalised cold outreach email on behalf of {sender_name} at {sender_company}.

Context: {sender_context}
Tone: {tone}
Length: {length} (under 100 words for the body)
Call to action: {call_to_action}

Phrases to avoid:
{avoid_phrases}

Rules:
- Write to ONE specific person. Use their name and reference something specific about their business.
- Never use generic opener phrases. Start with something specific.
- The subject line should be 5 words or fewer, not clickbait, not a question.
- Personalisation lines (1, 2, 3) are SHORT fragments used as variables in an email sequence.
  Line 1: A specific observation about their business (1 sentence)
  Line 2: A relevant pain point or opportunity for this type of business (1 sentence)
  Line 3: A compliment or social proof hook (1 sentence)

Return ONLY valid JSON — no preamble, no markdown:
{{
  "subject": "Short subject line",
  "body": "Full email body here",
  "personalisation_line_1": "Specific observation about their business",
  "personalisation_line_2": "Pain point or opportunity",
  "personalisation_line_3": "Compliment or hook"
}}"""


async def draft_email(
    dm: dict,
    lead: dict,
    config: OutreachConfig,
    anthropic_client: anthropic.Anthropic,
    semaphore: asyncio.Semaphore,
    index: int,
    total: int,
) -> Optional[dict]:
    """Generate one email draft for a single decision maker."""
    business = lead.get("business_name", "this business")
    name = dm.get("name") or "there"
    first_name = name.split()[0] if name != "there" else "there"
    title = dm.get("title", "")
    category = lead.get("category", "")
    website = lead.get("website", "")
    research_notes = dm.get("research_notes", "")
    icp_score = dm.get("icp_score", 0)
    linkedin_url = dm.get("linkedin_url", "")
    gmaps_rating = lead.get("gmaps_rating")
    gmaps_reviews = lead.get("gmaps_reviews")

    system = SYSTEM_PROMPT.format(
        sender_name=config.sender_name,
        sender_company=config.sender_company,
        sender_context=config.sender_context,
        tone=config.tone,
        length=config.length,
        call_to_action=config.call_to_action,
        avoid_phrases="\n".join(f"- {p}" for p in config.avoid_phrases),
    )

    # Build optional context lines
    extra_context_lines: list[str] = []
    if config.include_gmaps_context and gmaps_rating is not None:
        reviews_note = f" ({gmaps_reviews} reviews)" if gmaps_reviews else ""
        extra_context_lines.append(f"Google Maps rating: {gmaps_rating}★{reviews_note}")
    if config.include_linkedin_context and linkedin_url:
        extra_context_lines.append(f"LinkedIn: {linkedin_url}")

    extra_context = (
        "\nAdditional context:\n" + "\n".join(extra_context_lines)
        if extra_context_lines else ""
    )

    user_message = (
        f"Write an email to:\n"
        f"Name: {first_name} ({name})\n"
        f"Title: {title or 'Owner/Director'}\n"
        f"Business: {business}\n"
        f"Category: {category}\n"
        f"Website: {website or 'none'}\n"
        f"ICP Score: {icp_score}/100"
        f"{extra_context}\n\n"
        f"Research notes (use this to personalise):\n{research_notes or 'No notes available — write based on business name and category.'}"
    )

    async with semaphore:
        for attempt in range(4):
            try:
                loop = asyncio.get_event_loop()
                response = await loop.run_in_executor(
                    None,
                    lambda: anthropic_client.messages.create(
                        model=config.anthropic_model,
                        max_tokens=1024,
                        system=system,
                        messages=[{"role": "user", "content": user_message}],
                        metadata={"user_id": f"email-agent-{dm.get('id', 'unknown')}"},
                    ),
                )
                break
            except anthropic.RateLimitError:
                import random
                wait = (2 ** attempt) + random.uniform(0, 1)
                log.info("Rate limit — backing off %.1fs", wait)
                await asyncio.sleep(wait)
        else:
            log.error("[%d/%d] %s — rate limit exhausted", index, total, business)
            return None

    result_text = _extract_text(response)
    if not result_text:
        log.warning("[%d/%d] %s — no text from Claude", index, total, business)
        return None

    data = _parse_json(result_text)
    if not data:
        log.warning("[%d/%d] %s — JSON parse failed: %s", index, total, business, result_text[:100])
        return None

    # Validate required fields
    required = ("subject", "body", "personalisation_line_1", "personalisation_line_2", "personalisation_line_3")
    if not all(data.get(k) for k in required):
        log.warning("[%d/%d] %s — missing required fields in response", index, total, business)
        return None

    print(f"  ✓ [{index}/{total}] {business} → {first_name} — '{data['subject']}'")
    return data


async def run_email_batch(
    supabase: Client,
    config: CampaignConfig,
    anthropic_client: anthropic.Anthropic,
    dry_run: bool = False,
    limit: Optional[int] = None,
    headless: bool = False,
) -> EmailRunResult:
    # Fetch approved decision makers for this campaign
    query = (
        supabase.table("decision_makers")
        .select("*, leads!inner(id, business_name, category, website, location, campaign_id, gmaps_rating, gmaps_reviews)")
        .eq("status", "approved")
        .gte("icp_score", config.lead_filter.min_icp_score_for_email)
        .eq("leads.campaign_id", config.campaign_id)
        .is_("email", "not.null")
    )
    if limit:
        query = query.limit(limit)

    result = query.execute()
    dms = result.data or []

    total = len(dms)
    est_cost = total * 0.005  # ~$0.005/draft with Sonnet

    print(f"\n  Email Building Agent {'— DRY RUN' if dry_run else ''}")
    print(f"  {'─' * 50}")
    print(f"  Campaign:     {config.campaign_name}")
    print(f"  DMs approved: {total}")
    print(f"  Min ICP:      {config.lead_filter.min_icp_score_for_email}")
    print(f"  Tone:         {config.outreach.tone}")
    print(f"  Est. cost:    ~${est_cost:.2f}")
    print()

    run_result = EmailRunResult(campaign_id=config.campaign_id)

    if dry_run:
        print("  Dry run complete. Pass --no-dry-run to execute.")
        return run_result

    if total == 0:
        print("  No approved decision makers to draft for.")
        return run_result

    if not headless:
        answer = input("  Proceed? [y/N]: ").strip().lower()
        if answer not in ("y", "yes"):
            print("  Aborted.")
            return run_result

    print(f"\n  Drafting emails for {total} decision makers...\n")

    semaphore = asyncio.Semaphore(3)  # cap concurrent Anthropic calls
    tasks = [
        draft_email(
            dm=dm,
            lead=dm["leads"],
            config=config.outreach,
            anthropic_client=anthropic_client,
            semaphore=semaphore,
            index=i + 1,
            total=total,
        )
        for i, dm in enumerate(dms)
    ]
    drafts = await asyncio.gather(*tasks)

    # Write drafts to Supabase
    written = 0
    skipped = 0
    for dm, draft in zip(dms, drafts):
        if not draft:
            skipped += 1
            continue
        supabase.table("outreach").upsert({
            "decision_maker_id": dm["id"],
            "campaign_id": config.campaign_id,
            "subject": draft["subject"],
            "body": draft["body"],
            "personalisation_line_1": draft["personalisation_line_1"],
            "personalisation_line_2": draft["personalisation_line_2"],
            "personalisation_line_3": draft["personalisation_line_3"],
            "status": "drafted",
        }, on_conflict="decision_maker_id").execute()
        written += 1

    run_result.drafts_written = written
    run_result.drafts_skipped = skipped
    print(f"\n  Done. {written}/{total} drafts written to outreach table.")
    print("  Review in Supabase — set status = 'approved_for_send' to export.")
    return run_result


def _extract_text(response) -> Optional[str]:
    for block in response.content:
        if hasattr(block, "text"):
            return block.text
    return None


def _parse_json(text: str) -> Optional[dict]:
    text = re.sub(r"```(?:json)?", "", text).strip().rstrip("`").strip()
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        match = re.search(r"\{.*\}", text, re.DOTALL)
        if match:
            try:
                return json.loads(match.group(0))
            except json.JSONDecodeError:
                pass
    return None


def main() -> None:
    import sys
    parser = argparse.ArgumentParser(description="Email Building Agent")
    parser.add_argument("--campaign", required=True, help="Campaign UUID")
    parser.add_argument("--dry-run", action="store_true", default=False)
    parser.add_argument("--headless", action="store_true", default=False,
                        help="Skip confirmation prompts (for scheduled/routine runs)")
    parser.add_argument("--limit", type=int, default=None)
    args = parser.parse_args()

    supabase: Client = create_client(
        os.environ["SUPABASE_URL"],
        os.environ["SUPABASE_SERVICE_ROLE_KEY"],
    )
    anthropic_client = anthropic.Anthropic(api_key=os.environ["ANTHROPIC_API_KEY"])
    config = load_campaign_config(supabase, args.campaign)

    try:
        asyncio.run(run_email_batch(supabase, config, anthropic_client,
                                    args.dry_run, args.limit, args.headless))
    except Exception as e:
        log.error("Email run failed: %s", e)
        sys.exit(1)
    sys.exit(0)


if __name__ == "__main__":
    main()
