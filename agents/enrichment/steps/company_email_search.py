"""
Company Email Discovery via 3× GPT-4.1-mini + Brave Search.

Searches for the business's main contact/business email address(es).
Mirrors DMOS: GPT Company Email Run 1/2/3 → Consolidate Company Emails.

Returns ALL unique emails found across all 3 runs (not just one).
"""
from __future__ import annotations

import concurrent.futures
import json
import logging
import re
import time

from openai import OpenAI

from ._base import StepResult
from ._brave import call_brave
from agents.config import EnrichmentConfig

log = logging.getLogger(__name__)

_SYSTEM_PROMPT = (
    "You are a lead generation researcher. Your ONLY task is to find the main contact "
    "or business email address(es) for the company provided. Use Brave Search to check "
    "their website contact page, Google Maps listing, and business directories.\n\n"
    "Return ONLY valid JSON with no preamble or markdown:\n"
    "{\"emails\": [\"contact@business.com\"]}\n\n"
    "If none found: {\"emails\": []}\n"
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
    business: str,
    location: str,
    category: str,
) -> tuple[list[str], int, int, list[str], int]:
    messages = [
        {"role": "system", "content": _SYSTEM_PROMPT},
        {
            "role": "user",
            "content": (
                f"Business Name: {business}\n"
                f"Business Type: {category}\n"
                f"Business Location: {location}"
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
                emails = [e.strip() for e in (data.get("emails") or []) if e and "@" in str(e)]
                return emails, total_in, total_out, brave_queries, gpt_turns
            except json.JSONDecodeError:
                pass
            return [], total_in, total_out, brave_queries, gpt_turns

    return [], total_in, total_out, brave_queries, gpt_turns


def run_company_email_search(
    lead: dict,
    openai_client: OpenAI,
    config: EnrichmentConfig,
) -> StepResult:
    """
    3× parallel GPT-4.1-mini + Brave agents search for business contact emails.
    Returns ALL unique emails found across all 3 runs.
    Mirrors DMOS: GPT Company Email Run 1/2/3 → Consolidate Company Emails.
    """
    start = time.monotonic()
    business = lead.get("business_name", "")
    location = lead.get("location", "")
    category = lead.get("category", "")

    with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
        futures = [
            executor.submit(
                _run_single_agent,
                openai_client, config.openai_model, business, location, category,
            )
            for _ in range(3)
        ]
        all_emails: list[str] = []
        total_in = total_out = 0
        all_brave_queries: list[str] = []
        total_gpt_turns = 0
        for f in concurrent.futures.as_completed(futures):
            try:
                emails, inp, out, queries, turns = f.result()
                all_emails.extend(emails)
                total_in  += inp
                total_out += out
                all_brave_queries.extend(queries)
                total_gpt_turns   += turns
            except Exception as e:
                log.warning("Company email agent failed: %s", e)

    cost = total_in * _INPUT_COST + total_out * _OUTPUT_COST

    seen: set[str] = set()
    unique: list[str] = []
    for e in all_emails:
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
            step="company_email_search", success=False,
            error_code="NO_RESULT",
            error_message=f"No company email found for {business}",
            raw=debug,
            duration_sec=time.monotonic() - start,
            cost_usd=cost,
        )

    return StepResult(
        step="company_email_search",
        success=True,
        email=unique[0],
        email_confidence="medium",
        email_source="company_email_search",
        notes=f"{len(unique)} email(s): {', '.join(unique)}",
        raw={"all_emails": unique, **debug},
        duration_sec=time.monotonic() - start,
        cost_usd=cost,
    )
