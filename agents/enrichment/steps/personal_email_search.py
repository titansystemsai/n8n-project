"""
Personal Email Discovery via 3× GPT-4.1-mini + Brave Search.

For a known decision maker, searches for their personal/professional email.
Mirrors DMOS: GPT Personal Email Run 1/2/3 → Consolidate Personal Email.

Consensus: first unique email found across 3 runs.
"""
from __future__ import annotations

import concurrent.futures
import json
import logging
import re
import time
from typing import Optional

from openai import OpenAI

from ._base import StepResult
from ._brave import call_brave
from agents.config import EnrichmentConfig

log = logging.getLogger(__name__)

_SYSTEM_PROMPT = (
    "You are a lead generation researcher. Your ONLY task is to find the personal "
    "or professional email address of the specific person at the business provided. "
    "Use Brave Search to find it. Check their website, LinkedIn, social profiles, "
    "and directory listings.\n\n"
    "Return ONLY valid JSON with no preamble or markdown:\n"
    "{\"email\": \"address@domain.com\"}\n\n"
    "If no email is found: {\"email\": \"NA\"}\n"
    "DO NOT make up email addresses. Only return emails you can confirm from search results.\n\n"
    "MAKE SURE YOU ACCOUNT FOR THE LOCATION OF THE BUSINESS AND USE THAT IN YOUR SEARCH."
)

_BRAVE_TOOL = {
    "type": "function",
    "function": {
        "name": "brave_search",
        "description": "Search the web using Brave Search. Returns snippets from relevant pages.",
        "parameters": {
            "type": "object",
            "properties": {
                "query": {"type": "string", "description": "The search query"},
                "count": {"type": "integer", "description": "Number of results (1-10)", "default": 5},
            },
            "required": ["query"],
        },
    },
}

_INPUT_COST  = 0.40 / 1_000_000
_OUTPUT_COST = 1.60 / 1_000_000



def _run_single_agent(
    openai_client: OpenAI,
    model: str,
    name: str,
    title: str,
    business: str,
    location: str,
) -> tuple[str, int, int, list[str], int]:
    messages = [
        {"role": "system", "content": _SYSTEM_PROMPT},
        {
            "role": "user",
            "content": (
                f"Business Name: {business}\n"
                f"Business Location: {location}\n"
                f"Decision Maker: {name} ({title})"
            ),
        },
    ]
    total_in = total_out = 0
    brave_queries: list[str] = []
    gpt_turns = 0

    for _ in range(6):
        response = openai_client.chat.completions.create(
            model=model, messages=messages, tools=[_BRAVE_TOOL], tool_choice="auto",
        )
        msg = response.choices[0].message
        total_in  += response.usage.prompt_tokens
        total_out += response.usage.completion_tokens
        gpt_turns += 1

        if msg.tool_calls:
            messages.append(msg)
            for tc in msg.tool_calls:
                args = json.loads(tc.function.arguments)
                brave_queries.append(args["query"])
                try:
                    results = call_brave(args["query"], args.get("count", 5))
                    content = json.dumps(results)
                except Exception as e:
                    content = json.dumps({"error": str(e)})
                messages.append({"role": "tool", "tool_call_id": tc.id, "content": content})
        else:
            text = (msg.content or "").strip()
            text = re.sub(r"```(?:json)?", "", text).rstrip("`").strip()
            try:
                data = json.loads(text)
                email = (data.get("email") or "").strip()
                if email and email.upper() != "NA" and "@" in email:
                    return email, total_in, total_out, brave_queries, gpt_turns
            except json.JSONDecodeError:
                match = re.search(r'"email"\s*:\s*"([^"]+)"', text)
                if match:
                    email = match.group(1).strip()
                    if email.upper() != "NA" and "@" in email:
                        return email, total_in, total_out, brave_queries, gpt_turns
            return "", total_in, total_out, brave_queries, gpt_turns

    return "", total_in, total_out, brave_queries, gpt_turns


def run_personal_email_search(
    lead: dict,
    name: str,
    title: str,
    openai_client: OpenAI,
    config: EnrichmentConfig,
) -> StepResult:
    """
    3× parallel GPT-4.1-mini + Brave agents search for the named person's email.
    Consensus: first unique email found across 3 runs.
    Mirrors DMOS: GPT Personal Email Run 1/2/3 → Consolidate Personal Email.
    """
    if not name:
        return StepResult(
            step="personal_email_search", success=False,
            error_code="NO_NAME", error_message="No name provided",
        )

    start = time.monotonic()
    business = lead.get("business_name", "")
    location = lead.get("location", "")

    with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
        futures = [
            executor.submit(
                _run_single_agent,
                openai_client, config.openai_model, name, title or "", business, location,
            )
            for _ in range(3)
        ]
        raw_emails: list[str] = []
        total_in = total_out = 0
        all_brave_queries: list[str] = []
        total_gpt_turns = 0
        for f in concurrent.futures.as_completed(futures):
            try:
                email, inp, out, queries, turns = f.result()
                if email:
                    raw_emails.append(email)
                total_in  += inp
                total_out += out
                all_brave_queries.extend(queries)
                total_gpt_turns   += turns
            except Exception as e:
                log.warning("Personal email agent failed: %s", e)

    cost = total_in * _INPUT_COST + total_out * _OUTPUT_COST

    # Deduplicate, preserve order (DMOS: unique[0])
    seen: set[str] = set()
    unique: list[str] = []
    for e in raw_emails:
        if e.lower() not in seen:
            seen.add(e.lower())
            unique.append(e)

    debug = {
        "agent_runs": 3,
        "brave_queries": all_brave_queries,
        "gpt_turns": total_gpt_turns,
    }

    if not unique:
        return StepResult(
            step="personal_email_search", success=False,
            error_code="NO_RESULT",
            error_message=f"No personal email found for {name}",
            raw=debug,
            duration_sec=time.monotonic() - start,
            cost_usd=cost,
        )

    return StepResult(
        step="personal_email_search",
        success=True,
        email=unique[0],
        email_confidence="medium",
        email_source="gpt_brave",
        notes=f"Found for {name}: {unique[0]}",
        raw={"emails_found": unique, **debug},
        duration_sec=time.monotonic() - start,
        cost_usd=cost,
    )
