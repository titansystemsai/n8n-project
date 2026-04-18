"""
Step 1 — Company Context via Web Search.

Goal: Identify the decision maker's name, title, and whether the business
is independent vs franchise. Reads full page content, not just snippets.

Uses: Claude Agent SDK with web_search tool (Brave Search via Anthropic).
"""
from __future__ import annotations

import logging
import time
from typing import Optional

import anthropic

from ._base import StepResult
from agents.config import EnrichmentConfig

log = logging.getLogger(__name__)

SYSTEM_PROMPT = """You are a lead generation researcher. Your goal is to identify the real
owner, director, founder, or principal decision-maker of the business provided.

You MUST search the web and read at least 2–3 actual pages before answering. Never return
results based only on search snippets.

Focus ONLY on:
1. The decision-maker's full name and title
2. Whether this business is independent or a franchise/chain
3. Any LinkedIn URL for the decision maker

Do NOT research email addresses in this step.

Return a JSON object with these exact keys:
{
  "name": "Full Name or null",
  "title": "Job title or null",
  "linkedin_url": "https://linkedin.com/in/... or null",
  "is_franchise": true or false,
  "is_independent": true or false,
  "confidence": "high" or "medium" or "low",
  "notes": "1-2 sentences on what you found and why you're confident"
}"""


def run_web_search(
    lead: dict,
    anthropic_client: anthropic.Anthropic,
    config: EnrichmentConfig,
) -> StepResult:
    """
    Search for the business decision maker using Claude's web_search tool.
    Reads actual pages — not just Brave snippets.
    """
    start = time.monotonic()
    business = lead.get("business_name", "Unknown")
    location = lead.get("location", "")
    category = lead.get("category", "")

    search_context = f"{business}"
    if location:
        search_context += f" {location}"
    if category:
        search_context += f" {category}"

    user_message = (
        f"Find the owner or director of this business:\n\n"
        f"Business name: {business}\n"
        f"Location: {location}\n"
        f"Category: {category}\n\n"
        f"Search for '{search_context} owner director' and read the results. "
        f"Also search for '{business} about us team'."
    )

    depth_searches = {"quick": 2, "standard": 4, "deep": 8}
    max_searches = depth_searches.get(config.research_depth, 4)

    try:
        response = anthropic_client.messages.create(
            model=config.anthropic_model,
            max_tokens=1024,
            system=SYSTEM_PROMPT,
            tools=[{"type": "web_search_20250305", "name": "web_search"}],
            messages=[{"role": "user", "content": user_message}],
            tool_choice={"type": "auto"},
            metadata={"user_id": f"enrichment-web-search-{lead.get('id', 'unknown')}"},
        )

        # Extract the final text block (Claude's JSON response)
        result_text = _extract_text(response)
        if not result_text:
            return StepResult(
                step="web_search", success=False,
                error_code="NO_RESULT",
                error_message="Claude returned no text content",
                duration_sec=time.monotonic() - start,
            )

        data = _parse_json_response(result_text)
        if not data:
            return StepResult(
                step="web_search", success=False,
                error_code="PARSE_ERROR",
                error_message=f"Could not parse JSON from response: {result_text[:200]}",
                duration_sec=time.monotonic() - start,
            )

        # Estimate token cost (rough: Sonnet input ~$3/MTok, output ~$15/MTok)
        input_tokens = response.usage.input_tokens if response.usage else 0
        output_tokens = response.usage.output_tokens if response.usage else 0
        cost = (input_tokens * 3 + output_tokens * 15) / 1_000_000

        return StepResult(
            step="web_search",
            success=True,
            name=data.get("name"),
            title=data.get("title"),
            linkedin_url=data.get("linkedin_url"),
            notes=data.get("notes"),
            raw=data,
            duration_sec=time.monotonic() - start,
            cost_usd=cost,
        )

    except anthropic.AuthenticationError:
        return StepResult(
            step="web_search", success=False,
            error_code="CREDENTIAL_INVALID",
            error_message="ANTHROPIC_API_KEY is invalid or expired. Update in .env",
            duration_sec=time.monotonic() - start,
        )
    except anthropic.RateLimitError:
        # Re-raise so the orchestrator can back off globally
        raise
    except Exception as e:
        return StepResult(
            step="web_search", success=False,
            error_code="SERVICE_DOWN",
            error_message=str(e),
            duration_sec=time.monotonic() - start,
        )


def _extract_text(response) -> Optional[str]:
    for block in response.content:
        if hasattr(block, "text"):
            return block.text
    return None


def _parse_json_response(text: str) -> Optional[dict]:
    import json, re
    # Strip markdown code fences if present
    text = re.sub(r"```(?:json)?", "", text).strip().rstrip("`").strip()
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        # Try to extract the first {...} block
        match = re.search(r"\{.*\}", text, re.DOTALL)
        if match:
            try:
                return json.loads(match.group(0))
            except json.JSONDecodeError:
                pass
    return None
