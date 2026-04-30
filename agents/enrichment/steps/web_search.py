"""
Step 1 — DM Discovery via 3× GPT-4.1-mini + Brave Search.

Mirrors the DMOS n8n workflow exactly:
  - 3 parallel GPT-4.1-mini agents, each with Brave Search as a tool
  - GPT decides what to search and how many results to fetch
  - Consensus: if 2+ runs agree on a name, that wins; otherwise first found
  - Token cost: ~$0.003–0.006/lead vs ~$0.05–0.10 with Claude Sonnet web_search

No Anthropic calls in this step.
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

# Exact system prompt from DMOS workflow
_SYSTEM_PROMPT = (
    "You are an expert Lead Generation Researcher. Your goal is to identify the real "
    "owner, director, founder, or principal decision-maker of the business provided.\n\n"
    "YOU MUST perform at least one web search before answering. Never return empty results "
    "without searching first.\n\n"
    "Step-by-step:\n"
    "1. Search for \"[Business Name] owner\" or \"[Business Name] director\" or "
    "\"[Business Name] founder\"\n"
    "2. Search the business website (if provided) for About/Team/Contact pages\n"
    "3. If the first search returns nothing useful, try a different query "
    "(e.g. \"[Business Name] [Location] principal\")\n\n"
    "Return ONLY valid JSON with no preamble or markdown:\n"
    "{\"decision_makers\": [{\"firstName\": \"...\", \"lastName\": \"...\", "
    "\"position\": \"owner/director/etc\"}]}\n\n"
    "If after searching you genuinely cannot find a verifiable person: "
    "{\"decision_makers\": []}\n"
    "Only include people you found via search — do not invent names.\n\n"
    "MAKE SURE YOU ACCOUNT FOR THE LOCATION OF THE BUSINESS AND USE THAT IN YOUR SEARCH."
)

# Brave search tool definition — GPT decides query and count (mirrors DMOS Brave sub-node)
_BRAVE_TOOL = {
    "type": "function",
    "function": {
        "name": "brave_search",
        "description": "Search the web using Brave Search. Returns snippets from relevant pages.",
        "parameters": {
            "type": "object",
            "properties": {
                "query": {"type": "string", "description": "The search query"},
                "count": {
                    "type": "integer",
                    "description": "Number of results to return (1–10)",
                    "default": 5,
                },
            },
            "required": ["query"],
        },
    },
}

# GPT-4.1-mini pricing
_INPUT_COST_PER_TOKEN  = 0.40 / 1_000_000
_OUTPUT_COST_PER_TOKEN = 1.60 / 1_000_000




def _run_single_agent(
    openai_client: OpenAI,
    model: str,
    business: str,
    location: str,
    category: str,
) -> tuple[dict, int, int, list[str], int]:
    """
    One GPT-4.1-mini agent with Brave as a tool. Mirrors one DMOS GPT DM Discovery Run.
    Returns (parsed_result, input_tokens, output_tokens, brave_queries, gpt_turns).
    """
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
    total_input = 0
    total_output = 0
    brave_queries: list[str] = []
    gpt_turns = 0

    for _ in range(6):
        response = openai_client.chat.completions.create(
            model=model,
            messages=messages,
            tools=[_BRAVE_TOOL],
            tool_choice="auto",
        )
        msg = response.choices[0].message
        total_input  += response.usage.prompt_tokens
        total_output += response.usage.completion_tokens
        gpt_turns    += 1

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
                messages.append({
                    "role": "tool",
                    "tool_call_id": tc.id,
                    "content": content,
                })
        else:
            text = (msg.content or "").strip()
            text = re.sub(r"```(?:json)?", "", text).rstrip("`").strip()
            try:
                return json.loads(text), total_input, total_output, brave_queries, gpt_turns
            except json.JSONDecodeError:
                match = re.search(r"\{.*\}", text, re.DOTALL)
                if match:
                    try:
                        return json.loads(match.group(0)), total_input, total_output, brave_queries, gpt_turns
                    except json.JSONDecodeError:
                        pass
            return {}, total_input, total_output, brave_queries, gpt_turns

    return {}, total_input, total_output, brave_queries, gpt_turns


def run_web_search(
    lead: dict,
    openai_client: OpenAI,
    config: EnrichmentConfig,
) -> StepResult:
    """
    Run 3 parallel GPT-4.1-mini+Brave agents and apply consensus.
    Mirrors DMOS: GPT DM Discovery Run 1/2/3 → Merge → Consolidate DM Results.
    """
    start = time.monotonic()
    business = lead.get("business_name", "Unknown")
    location  = lead.get("location", "")
    category  = lead.get("category", "")

    # 3 parallel agents — same as DMOS running Run 1, 2, 3 concurrently
    with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
        futures = [
            executor.submit(
                _run_single_agent,
                openai_client, config.openai_model, business, location, category,
            )
            for _ in range(3)
        ]
        runs: list[dict] = []
        total_input_tokens  = 0
        total_output_tokens = 0
        all_brave_queries:  list[str] = []
        total_gpt_turns = 0
        for f in concurrent.futures.as_completed(futures):
            try:
                result, inp, out, queries, turns = f.result()
                runs.append(result)
                total_input_tokens  += inp
                total_output_tokens += out
                all_brave_queries.extend(queries)
                total_gpt_turns     += turns
            except Exception as e:
                log.warning("GPT DM agent run failed: %s", e)
                runs.append({})

    # Consensus logic — mirrors DMOS Consolidate DM Results node exactly
    counts: dict[str, int]  = {}
    dm_data: dict[str, dict] = {}
    for result in runs:
        for dm in result.get("decision_makers", []):
            first = (dm.get("firstName") or "").strip()
            last  = (dm.get("lastName") or "").strip()
            if not first and not last:
                continue
            key = f"{first} {last}".lower().strip()
            counts[key]  = counts.get(key, 0) + 1
            if key not in dm_data:
                dm_data[key] = dm

    consensus  = [dm_data[k] for k, v in counts.items() if v >= 2]
    final_dms  = consensus if consensus else list(dm_data.values())

    cost = total_input_tokens * _INPUT_COST_PER_TOKEN + total_output_tokens * _OUTPUT_COST_PER_TOKEN

    debug = {
        "agent_runs": 3,
        "brave_queries": all_brave_queries,
        "gpt_turns": total_gpt_turns,
    }

    if not final_dms:
        return StepResult(
            step="web_search",
            success=False,
            error_code="NO_RESULT",
            error_message="All 3 GPT+Brave agents returned no decision makers",
            raw=debug,
            duration_sec=time.monotonic() - start,
            cost_usd=cost,
        )

    dm    = final_dms[0]
    first = (dm.get("firstName") or "").strip()
    last  = (dm.get("lastName") or "").strip()
    name  = f"{first} {last}".strip() or None

    consensus_note = f"{len(consensus)}/3 runs agreed" if consensus else "no consensus — used first found"

    return StepResult(
        step="web_search",
        success=True,
        name=name,
        title=dm.get("position"),
        notes=consensus_note,
        raw={"runs": runs, "final": dm, "all_dms": list(dm_data.values()), **debug},
        duration_sec=time.monotonic() - start,
        cost_usd=cost,
    )
