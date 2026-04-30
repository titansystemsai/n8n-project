"""
Return-type dataclasses for all agent entry points.

Every agent function returns one of these so callers (CLI __main__, scheduler,
and future API routes) can inspect the outcome without parsing stdout.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import List, Optional


@dataclass
class EnrichmentRunResult:
    campaign_id: str
    leads_processed: int = 0
    leads_enriched: int = 0
    leads_failed: int = 0
    leads_dead: int = 0
    total_cost_usd: float = 0.0
    errors: List[str] = field(default_factory=list)

    @property
    def success(self) -> bool:
        """True if the run completed without a hard error (dead leads are a soft failure)."""
        return len(self.errors) == 0

    @property
    def exit_code(self) -> int:
        """0 = clean, 1 = hard error, 2 = partial (some leads dead)."""
        if self.errors:
            return 1
        if self.leads_dead > 0:
            return 2
        return 0


@dataclass
class EmailRunResult:
    campaign_id: str
    drafts_written: int = 0
    drafts_skipped: int = 0
    errors: List[str] = field(default_factory=list)

    @property
    def success(self) -> bool:
        return len(self.errors) == 0

    @property
    def exit_code(self) -> int:
        return 1 if self.errors else 0


@dataclass
class IngestionRunResult:
    campaign_id: str
    source: str
    leads_fetched: int = 0
    leads_inserted: int = 0
    leads_skipped: int = 0   # duplicates
    leads_invalid: int = 0   # missing business_name etc.
    errors: List[str] = field(default_factory=list)

    @property
    def success(self) -> bool:
        return len(self.errors) == 0

    @property
    def exit_code(self) -> int:
        if self.errors:
            return 1
        if self.leads_invalid > 0:
            return 2
        return 0


@dataclass
class ExportResult:
    campaign_id: str
    rows_exported: int = 0
    rows_skipped: int = 0
    batch_id: Optional[str] = None
    file_path: Optional[str] = None
    errors: List[str] = field(default_factory=list)

    @property
    def success(self) -> bool:
        return len(self.errors) == 0

    @property
    def exit_code(self) -> int:
        return 1 if self.errors else 0
