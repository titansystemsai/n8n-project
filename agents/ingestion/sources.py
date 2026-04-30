"""
Lead source adapters — Apify actors and file readers.

All public functions return list[LeadRecord]. No LLM calls anywhere in this module.
All decisions are deterministic: actor selection is a dict lookup, field mapping is fixed.

Supported sources:
    google_maps      — compass~crawler-google-places
    linkedin_company — curious_coder~linkedin-company-search-export
    linkedin_profile — curious_coder~linkedin-profile-scraper
    facebook         — apify~facebook-pages-scraper
    csv              — CSV / XLSX / XLS file on disk
"""

from __future__ import annotations

import csv
import io
import json
import logging
import os
import time
import urllib.parse
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

import httpx

from agents.quota import (
    CredentialInvalidError,
    ServiceDownError,
    TimeoutError as ApifyTimeoutError,
)

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Actor registry — adding a new source = one line here + one adapter function
# ---------------------------------------------------------------------------

ACTOR_IDS: dict[str, str] = {
    "google_maps":      "compass~crawler-google-places",
    "linkedin_company": "curious_coder~linkedin-company-search-export",
    "linkedin_profile": "curious_coder~linkedin-profile-scraper",
    "facebook":         "apify~facebook-pages-scraper",
}

SUPPORTED_SOURCES = list(ACTOR_IDS.keys()) + ["csv"]


# ---------------------------------------------------------------------------
# Output type — fields match leads table columns exactly
# ---------------------------------------------------------------------------

@dataclass
class LeadRecord:
    business_name: str
    source: str
    location: Optional[str] = None
    phone: Optional[str] = None
    website: Optional[str] = None
    email: Optional[str] = None
    category: Optional[str] = None
    gmaps_place_id: Optional[str] = None
    gmaps_rating: Optional[float] = None
    gmaps_reviews: Optional[int] = None
    source_data: Optional[dict] = field(default=None, repr=False)
    apify_raw_json: Optional[dict] = field(default=None, repr=False)

    def to_db_row(self, org_id: str, campaign_id: str) -> dict:
        """Serialise to a dict ready for Supabase upsert."""
        row = {
            "org_id": org_id,
            "campaign_id": campaign_id,
            "business_name": self.business_name,
            "source": self.source,
            "enrichment_status": "queued",
        }
        optional_cols = [
            "location", "phone", "website", "email", "category",
            "gmaps_place_id", "gmaps_rating", "gmaps_reviews",
        ]
        for col in optional_cols:
            val = getattr(self, col)
            if val is not None:
                row[col] = val
        if self.source_data:
            row["source_data"] = json.dumps(self.source_data)
        if self.apify_raw_json is not None:
            row["apify_raw_json"] = json.dumps(self.apify_raw_json)
        return row


# ---------------------------------------------------------------------------
# Shared Apify HTTP caller with retry + backoff
# ---------------------------------------------------------------------------

_APIFY_BASE = "https://api.apify.com/v2"
_MAX_RETRIES = 3
_RETRY_BACKOFF = [2, 5, 10]  # seconds between retries


def _call_apify(actor_id: str, input_json: dict, timeout_secs: int = 180) -> list[dict]:
    """
    POST to Apify run-sync-get-dataset-items and return the parsed list.
    Retries up to 3 times on transient 5xx errors with exponential backoff.
    """
    api_key = os.environ.get("APIFY_API_KEY", "")
    if not api_key:
        raise CredentialInvalidError("APIFY_API_KEY is not set in .env")

    url = f"{_APIFY_BASE}/acts/{actor_id}/run-sync-get-dataset-items"
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
    }

    last_exc: Exception = RuntimeError("No attempts made")

    for attempt in range(_MAX_RETRIES):
        try:
            resp = httpx.post(
                url,
                json=input_json,
                headers=headers,
                timeout=timeout_secs,
            )
        except httpx.TimeoutException:
            raise ApifyTimeoutError(
                f"Apify actor {actor_id} timed out after {timeout_secs}s"
            )

        if resp.status_code == 401:
            raise CredentialInvalidError(
                "APIFY_API_KEY is invalid or expired. Update in .env"
            )

        if resp.status_code in (500, 502, 503, 504):
            last_exc = ServiceDownError(
                f"Apify returned {resp.status_code} on attempt {attempt + 1}"
            )
            if attempt < _MAX_RETRIES - 1:
                wait = _RETRY_BACKOFF[attempt]
                log.warning("Apify %s error, retrying in %ss…", resp.status_code, wait)
                time.sleep(wait)
            continue

        if not resp.is_success:
            raise ServiceDownError(
                f"Apify actor {actor_id} returned unexpected status {resp.status_code}: "
                f"{resp.text[:200]}"
            )

        return resp.json()

    raise last_exc


# ---------------------------------------------------------------------------
# Per-source adapter functions
# ---------------------------------------------------------------------------

def fetch_google_maps(keyword: str, location: str, limit: int = 50) -> list[LeadRecord]:
    """Scrape Google Maps via Apify. Returns one LeadRecord per place."""
    raw_items = _call_apify(
        ACTOR_IDS["google_maps"],
        {
            "searchStringsArray": [f"{keyword} {location}"],
            "maxCrawledPlacesPerSearch": limit,
            "language": "en",
        },
    )
    records: list[LeadRecord] = []
    for item in raw_items:
        name = item.get("title") or item.get("name")
        if not name:
            log.debug("google_maps: skipping record with no name: %s", item)
            continue
        rating = item.get("totalScore") or item.get("rating")
        reviews = item.get("reviewsCount") or item.get("userRatingsTotal")
        records.append(LeadRecord(
            business_name=name.strip(),
            source="google_maps",
            location=item.get("address") or item.get("vicinity"),
            phone=item.get("phone") or item.get("phoneUnformatted"),
            website=item.get("website") or item.get("url"),
            category=item.get("categoryName") or item.get("category"),
            gmaps_place_id=item.get("placeId") or item.get("id"),
            gmaps_rating=float(rating) if rating is not None else None,
            gmaps_reviews=int(reviews) if reviews is not None else None,
            apify_raw_json=item,
        ))
    return records


def fetch_linkedin_company(query: str, limit: int = 50) -> list[LeadRecord]:
    """Scrape LinkedIn company search via Apify."""
    cookie = os.environ.get("LINKEDIN_COOKIE", "")
    if not cookie:
        raise CredentialInvalidError(
            "LINKEDIN_COOKIE is not set in .env — add your LinkedIn li_at session cookie"
        )
    raw_items = _call_apify(
        ACTOR_IDS["linkedin_company"],
        {
            "searchUrl": f"https://www.linkedin.com/search/results/companies/?keywords={urllib.parse.quote_plus(query)}",
            "maxResults": limit,
            "cookie": cookie,
        },
    )
    records: list[LeadRecord] = []
    for item in raw_items:
        name = item.get("name") or item.get("companyName")
        if not name:
            log.debug("linkedin_company: skipping record with no name: %s", item)
            continue
        records.append(LeadRecord(
            business_name=name.strip(),
            source="linkedin_company",
            location=item.get("headquarters") or item.get("location"),
            phone=item.get("phone"),
            website=item.get("website") or item.get("companyUrl"),
            category=item.get("industry") or item.get("industries"),
            apify_raw_json=item,
        ))
    return records


def fetch_linkedin_profile(query: str, limit: int = 50) -> list[LeadRecord]:
    """Scrape LinkedIn profile search via Apify — maps company as business_name."""
    cookie = os.environ.get("LINKEDIN_COOKIE", "")
    if not cookie:
        raise CredentialInvalidError(
            "LINKEDIN_COOKIE is not set in .env — add your LinkedIn li_at session cookie"
        )
    raw_items = _call_apify(
        ACTOR_IDS["linkedin_profile"],
        {
            "searchUrl": f"https://www.linkedin.com/search/results/people/?keywords={urllib.parse.quote_plus(query)}",
            "maxResults": limit,
            "cookie": cookie,
        },
    )
    records: list[LeadRecord] = []
    for item in raw_items:
        # Profile scrapes give person records; use company as business identifier
        name = item.get("company") or item.get("currentCompany") or item.get("name")
        if not name:
            log.debug("linkedin_profile: skipping record with no company name: %s", item)
            continue
        records.append(LeadRecord(
            business_name=name.strip(),
            source="linkedin_profile",
            location=item.get("location"),
            website=item.get("companyUrl") or item.get("profileUrl"),
            category=item.get("headline") or item.get("title"),
            apify_raw_json=item,
        ))
    return records


def fetch_facebook(query: str, limit: int = 50) -> list[LeadRecord]:
    """Scrape Facebook Pages via Apify."""
    raw_items = _call_apify(
        ACTOR_IDS["facebook"],
        {
            "searchQuery": query,
            "maxResults": limit,
        },
    )
    records: list[LeadRecord] = []
    for item in raw_items:
        name = item.get("title") or item.get("name") or item.get("pageName")
        if not name:
            log.debug("facebook: skipping record with no name: %s", item)
            continue
        categories = item.get("categories") or item.get("category")
        if isinstance(categories, list):
            categories = ", ".join(str(c) for c in categories)
        records.append(LeadRecord(
            business_name=name.strip(),
            source="facebook",
            location=item.get("address") or item.get("location"),
            phone=item.get("phone"),
            website=item.get("website"),
            category=str(categories) if categories else None,
            apify_raw_json=item,
        ))
    return records


# ---------------------------------------------------------------------------
# File reader — CSV / XLSX / XLS
# ---------------------------------------------------------------------------

# Column aliases — case-insensitive. First match wins.
_COLUMN_ALIASES: dict[str, list[str]] = {
    "business_name": [
        "business_name", "business name", "company", "company name", "name",
        "organisation", "organization", "org", "trader", "brand", "account",
        "client", "client name", "merchant",
    ],
    "location": [
        "location", "address", "full address", "street address", "street",
        "city", "suburb", "town", "region", "state", "postcode", "zip",
        "zip code", "postal code",
    ],
    "phone": [
        "phone", "phone number", "phone no", "telephone", "tel", "mobile",
        "cell", "contact number", "number",
    ],
    "website": [
        "website", "url", "web", "site", "homepage", "domain",
        "website url", "web address",
    ],
    "email": [
        "email", "email address", "e-mail", "e-mail address",
        "contact email", "business email",
    ],
    "category": [
        "category", "industry", "sector", "type", "business type",
        "niche", "vertical", "service type",
    ],
}


def _detect_delimiter(first_line: str) -> str:
    counts = {",": 0, ";": 0, "\t": 0}
    for ch in first_line:
        if ch in counts:
            counts[ch] += 1
    return max(counts, key=lambda k: counts[k])


def _map_headers(headers: list[str]) -> dict[str, str]:
    """Return {normalised_field: original_header} for headers we recognise."""
    mapping: dict[str, str] = {}
    normalised = {h.strip().lower(): h for h in headers}
    for field_name, aliases in _COLUMN_ALIASES.items():
        for alias in aliases:
            if alias in normalised:
                mapping[field_name] = normalised[alias]
                break
    return mapping


def _rows_to_records(rows: list[dict], mapping: dict[str, str], source_label: str) -> list[LeadRecord]:
    records: list[LeadRecord] = []
    for row in rows:
        name_col = mapping.get("business_name")
        name = row.get(name_col, "").strip() if name_col else ""
        if not name:
            log.debug("csv: skipping row with no business_name: %s", row)
            continue

        def _get(field_name: str) -> Optional[str]:
            col = mapping.get(field_name)
            if col and col in row:
                val = str(row[col]).strip()
                return val if val else None
            return None

        records.append(LeadRecord(
            business_name=name,
            source=source_label,
            location=_get("location"),
            phone=_get("phone"),
            website=_get("website"),
            email=_get("email"),
            category=_get("category"),
        ))
    return records


# ---------------------------------------------------------------------------
# Raw file readers — return (headers, rows) without LeadRecord conversion
# ---------------------------------------------------------------------------

def _csv_raw(p: Path) -> tuple[list[str], list[dict]]:
    raw = p.read_text(encoding="utf-8-sig")
    if not raw.strip():
        raise ValueError(f"File is empty: {p}")
    first_line = raw.split("\n")[0]
    delim = _detect_delimiter(first_line)
    reader = csv.DictReader(io.StringIO(raw), delimiter=delim)
    rows = list(reader)
    if not rows:
        raise ValueError(f"No data rows found in {p}")
    return list(rows[0].keys()), rows


def _xlsx_raw(p: Path) -> tuple[list[str], list[dict]]:
    try:
        import openpyxl
    except ImportError:
        raise ImportError("openpyxl is required for .xlsx files. Run: pip install openpyxl")
    wb = openpyxl.load_workbook(p, read_only=True, data_only=True)
    ws = wb.active
    rows_iter = ws.iter_rows(values_only=True)
    headers_raw = next(rows_iter, None)
    if not headers_raw:
        raise ValueError(f"No headers found in {p}")
    headers = [str(h).strip() if h is not None else "" for h in headers_raw]
    rows = [{headers[i]: (str(v).strip() if v is not None else "") for i, v in enumerate(row)}
            for row in rows_iter]
    wb.close()
    if not rows:
        raise ValueError(f"No data rows found in {p}")
    return headers, rows


def _xls_raw(p: Path) -> tuple[list[str], list[dict]]:
    try:
        import xlrd
    except ImportError:
        raise ImportError("xlrd is required for .xls files. Run: pip install xlrd")
    wb = xlrd.open_workbook(str(p))
    ws = wb.sheet_by_index(0)
    if ws.nrows < 2:
        raise ValueError(f"No data rows found in {p}")
    headers = [str(ws.cell_value(0, c)).strip() for c in range(ws.ncols)]
    rows = [{headers[c]: str(ws.cell_value(r, c)).strip() for c in range(ws.ncols)}
            for r in range(1, ws.nrows)]
    return headers, rows


def _assert_business_name(mapping: dict[str, str], headers: list[str], filename: str) -> None:
    if "business_name" not in mapping:
        raise ValueError(
            f"Could not find a business name column in '{filename}'.\n"
            f"  Available headers: {headers}\n"
            f"  Expected one of: {_COLUMN_ALIASES['business_name']}"
        )


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def analyse_file(path: str) -> dict:
    """
    Return column names, up to 3 sample values per column, and a suggested
    field mapping for the column mapper UI.
    """
    p = Path(path)
    suffix = p.suffix.lower()
    if suffix == ".xlsx":
        headers, rows = _xlsx_raw(p)
    elif suffix == ".xls":
        headers, rows = _xls_raw(p)
    else:
        headers, rows = _csv_raw(p)

    sample_values: dict[str, list[str]] = {
        h: [str(row.get(h, "") or "").strip() for row in rows[:3]]
        for h in headers
    }
    field_to_col = _map_headers(headers)           # {field_name: original_header}
    suggested = {v: k for k, v in field_to_col.items()}  # {original_header: field_name}

    return {"columns": headers, "sample_values": sample_values, "suggested_mapping": suggested}


def _apply_column_mapping(
    rows: list[dict],
    column_mapping: dict[str, str],
    source_label: str,
) -> list[LeadRecord]:
    """Build LeadRecords from an explicit {csv_column: target_field} mapping."""
    _SCHEMA_FIELDS = {"business_name", "location", "phone", "website", "email", "category"}
    records: list[LeadRecord] = []
    for row in rows:
        kwargs: dict = {"source_data": {}}
        for csv_col, target in column_mapping.items():
            if not target or target == "ignore":
                continue
            val = str(row.get(csv_col, "") or "").strip()
            if not val:
                continue
            if target in _SCHEMA_FIELDS:
                kwargs[target] = val
            elif target.startswith("source_data."):
                key = target[len("source_data."):]
                if key:
                    kwargs["source_data"][key] = val
        name = kwargs.get("business_name", "")
        if not name:
            log.debug("csv: skipping row with no business_name after mapping: %s", row)
            continue
        records.append(LeadRecord(
            business_name=name,
            source=source_label,
            location=kwargs.get("location"),
            phone=kwargs.get("phone"),
            website=kwargs.get("website"),
            email=kwargs.get("email"),
            category=kwargs.get("category"),
            source_data=kwargs["source_data"] or None,
        ))
    return records


def read_file(path: str, column_mapping: Optional[dict[str, str]] = None) -> list[LeadRecord]:
    """
    Parse a CSV, XLSX, or XLS file and return LeadRecords.

    If column_mapping is provided (from the UI column mapper), it is applied
    directly: {csv_column: target_field} where target_field is one of the
    schema field names, "source_data.<key>", or "ignore".

    Without a mapping, falls back to auto-detection via _COLUMN_ALIASES.
    """
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(f"File not found: {path}")

    suffix = p.suffix.lower()
    source_label = f"csv:{p.name}"

    if suffix == ".xlsx":
        headers, rows = _xlsx_raw(p)
    elif suffix == ".xls":
        headers, rows = _xls_raw(p)
    elif suffix in (".csv", ".tsv", ".txt", ""):
        headers, rows = _csv_raw(p)
    else:
        raise ValueError(f"Unsupported file type '{suffix}'. Supported: .csv, .xlsx, .xls")

    if column_mapping is not None:
        return _apply_column_mapping(rows, column_mapping, source_label)

    mapping = _map_headers(headers)
    _assert_business_name(mapping, headers, p.name)
    return _rows_to_records(rows, mapping, source_label)
