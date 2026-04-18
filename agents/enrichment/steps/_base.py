"""
Shared types for enrichment step results.
"""
from __future__ import annotations
from dataclasses import dataclass, field
from typing import Optional


@dataclass
class StepResult:
    step: str
    success: bool
    # Populated on success
    name: Optional[str] = None           # decision maker full name
    title: Optional[str] = None
    linkedin_url: Optional[str] = None
    email: Optional[str] = None
    email_confidence: Optional[str] = None   # high | medium | low
    email_source: Optional[str] = None
    notes: Optional[str] = None          # free-form research notes
    raw: Optional[dict] = None           # full API/scrape payload for debugging
    # Populated on failure
    error_code: Optional[str] = None     # CREDENTIAL_INVALID, QUOTA_EXHAUSTED, etc.
    error_message: Optional[str] = None
    # Timing/cost
    duration_sec: float = 0.0
    cost_usd: float = 0.0

    @property
    def has_email(self) -> bool:
        return bool(self.email)

    @property
    def has_name(self) -> bool:
        return bool(self.name)
