"""
Step 4d — Domain Guess (last resort).

When all other email sources fail, construct a plausible email address
from the person's name and the business domain. Flagged as LOW confidence.

Pattern: firstname@domain.com.au
Fallback: first.last@domain.com.au
"""
from __future__ import annotations

import re
import time
from typing import Optional
from urllib.parse import urlparse

from ._base import StepResult

EMAIL_REGEX = re.compile(r'^[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}$')


def run_domain_guess(
    lead: dict,
    name: Optional[str] = None,
) -> StepResult:
    """
    Construct a guessed email from first name + domain.
    Always returns low confidence. Only used when all other sources fail.
    """
    start = time.monotonic()
    website = lead.get("website", "")

    if not website:
        return StepResult(
            step="domain_guess", success=False,
            error_code="NO_DOMAIN",
            error_message="No website — cannot guess domain email",
            duration_sec=time.monotonic() - start,
        )

    if not name:
        return StepResult(
            step="domain_guess", success=False,
            error_code="NO_NAME",
            error_message="No name found in prior steps — cannot construct email",
            duration_sec=time.monotonic() - start,
        )

    domain = _extract_domain(website)
    first, last = _split_name(name)

    candidates = []
    if first:
        candidates.append(f"{first}@{domain}")
    if first and last:
        candidates.append(f"{first}.{last}@{domain}")
        candidates.append(f"{first[0]}{last}@{domain}")

    # Use the simplest valid guess
    for email in candidates:
        email = email.lower()
        if EMAIL_REGEX.match(email):
            return StepResult(
                step="domain_guess",
                success=True,
                email=email,
                email_confidence="low",
                email_source="domain_guess",
                notes=f"Domain guess — not verified. Pattern: {email}",
                duration_sec=time.monotonic() - start,
                cost_usd=0.0,
            )

    return StepResult(
        step="domain_guess", success=False,
        error_code="NO_RESULT",
        error_message=f"Could not construct valid email from name='{name}' domain='{domain}'",
        duration_sec=time.monotonic() - start,
    )


def _extract_domain(url: str) -> str:
    if not url.startswith(("http://", "https://")):
        url = "https://" + url
    parsed = urlparse(url)
    domain = parsed.netloc or parsed.path
    return domain.lstrip("www.").split("/")[0]


def _split_name(name: str) -> tuple[Optional[str], Optional[str]]:
    parts = name.strip().split()
    if not parts:
        return None, None
    # Strip non-alpha characters (e.g. "O'Brien" → "obrien")
    first = re.sub(r"[^a-zA-Z]", "", parts[0]).lower() or None
    last  = re.sub(r"[^a-zA-Z]", "", parts[-1]).lower() if len(parts) > 1 else None
    return first, last
