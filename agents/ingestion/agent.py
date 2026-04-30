"""
Lead Ingestion Agent — deterministic scrape-to-Supabase pipeline.

Zero LLM calls. All decisions are deterministic:
  - Source type chosen via interactive form or CLI flag
  - Actor selection is a dict lookup
  - Field mapping is fixed per source
  - Deduplication is handled by the DB UNIQUE constraint

Running without --source triggers an interactive form that lists your campaigns
and walks through every required input. With --source and all flags provided,
runs non-interactively (useful for scripts and cron jobs).

Usage (interactive — recommended):
    python -m agents.ingestion.agent

Usage (scripted):
    python -m agents.ingestion.agent --campaign <uuid> \\
      --source google_maps --keyword "plumbers" --location "Adelaide, SA" \\
      --limit 50 --no-dry-run

    python -m agents.ingestion.agent --campaign <uuid> \\
      --source linkedin_company --query "plumbing companies Adelaide" \\
      --limit 50 --no-dry-run

    python -m agents.ingestion.agent --campaign <uuid> \\
      --source csv --file /path/to/leads.xlsx --no-dry-run

    Add --dry-run (default) to preview without writing to Supabase.
    Add --json-log <path> for a machine-readable JSONL run summary.

Exit codes:
    0 — all leads inserted cleanly (or nothing to do)
    1 — hard error (credential invalid, campaign not found, Apify failure)
    2 — partial: some leads were skipped due to missing required fields
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import httpx
from dotenv import load_dotenv
from rich.console import Console
from rich.live import Live
from rich.panel import Panel
from rich.table import Table
from supabase import Client, create_client

from agents.ingestion.sources import (
    ACTOR_IDS,
    SUPPORTED_SOURCES,
    LeadRecord,
    fetch_facebook,
    fetch_google_maps,
    fetch_linkedin_company,
    fetch_linkedin_profile,
    read_file,
)
from agents.quota import (
    CredentialInvalidError,
    ServiceDownError,
    TimeoutError as ApifyTimeoutError,
)
from agents.results import IngestionRunResult

load_dotenv()
log = logging.getLogger(__name__)
logging.basicConfig(level=logging.WARNING, format="%(asctime)s %(levelname)s %(message)s")

console = Console()

_UPSERT_BATCH_SIZE = 50


def _find_project_root() -> Path:
    for parent in Path(__file__).resolve().parents:
        if (parent / ".git").exists() or (parent / "CLAUDE.md").exists():
            return parent
    raise RuntimeError("Could not find project root")

_SOURCE_LABELS = {
    "google_maps":      "Google Maps",
    "linkedin_company": "LinkedIn (Company search)",
    "linkedin_profile": "LinkedIn (People search)",
    "facebook":         "Facebook Pages",
    "csv":              "CSV / XLSX / XLS file",
}


# ---------------------------------------------------------------------------
# Apify credential check (local to ingestion — does not need quota.py's private fn)
# ---------------------------------------------------------------------------

def _check_apify_key(api_key: str) -> tuple[bool, str]:
    if not api_key:
        return False, "CREDENTIAL_INVALID — APIFY_API_KEY is not set in .env"
    try:
        resp = httpx.get(
            "https://api.apify.com/v2/users/me",
            headers={"Authorization": f"Bearer {api_key}"},
            timeout=8,
        )
        if resp.status_code == 401:
            return False, "CREDENTIAL_INVALID — update APIFY_API_KEY in .env"
        if resp.status_code == 402:
            return False, "BILLING_ERROR — Apify subscription lapsed; check console.apify.com"
        if not resp.is_success:
            return False, f"SERVICE_DOWN ({resp.status_code})"
        return True, "valid"
    except Exception as e:
        return False, f"unreachable ({e})"


def _run_apify_preflight() -> bool:
    """Print preflight result and return True if key is valid."""
    api_key = os.environ.get("APIFY_API_KEY", "")
    ok, msg = _check_apify_key(api_key)
    icon = "[green]✓[/green]" if ok else "[red]✗[/red]"
    console.print(f"\n  Pre-flight check:")
    console.print(f"  {'─' * 44}")
    console.print(f"  {icon} APIFY_API_KEY{' ' * 20}{msg}")
    console.print()
    if not ok:
        console.print(
            f"  [bold red]✗ Cannot proceed:[/bold red] fix APIFY_API_KEY in "
            f"[cyan]n8n-project/.env[/cyan] then re-run.\n"
        )
    return ok


# ---------------------------------------------------------------------------
# Campaign helpers
# ---------------------------------------------------------------------------

def _list_campaigns(supabase: Client) -> list[dict]:
    result = (
        supabase.table("campaigns")
        .select("id, name, org_id, status")
        .order("name")
        .execute()
    )
    return result.data or []


def _verify_campaign(supabase: Client, campaign_id: str) -> tuple[str, str]:
    """Return (org_id, campaign_name). Raises ValueError if not found."""
    result = (
        supabase.table("campaigns")
        .select("id, name, org_id")
        .eq("id", campaign_id)
        .single()
        .execute()
    )
    if not result.data:
        raise ValueError(
            f"Campaign not found: {campaign_id}\n"
            "  Create a campaign row in Supabase before ingesting leads."
        )
    return result.data["org_id"], result.data["name"]


# ---------------------------------------------------------------------------
# Interactive startup form
# ---------------------------------------------------------------------------

def _prompt(label: str, default: Optional[str] = None, required: bool = True) -> str:
    """Simple stdin prompt with optional default."""
    suffix = f" [{default}]" if default else ""
    while True:
        value = input(f"  {label}{suffix}: ").strip()
        if not value and default:
            return default
        if value:
            return value
        if not required:
            return ""
        console.print("  [red]Required — please enter a value.[/red]")


def _prompt_int(label: str, default: int) -> int:
    while True:
        raw = input(f"  {label} [{default}]: ").strip()
        if not raw:
            return default
        try:
            return int(raw)
        except ValueError:
            console.print("  [red]Please enter a whole number.[/red]")


def _prompt_yn(label: str, default: bool = False) -> bool:
    default_str = "Y/n" if default else "y/N"
    raw = input(f"  {label} [{default_str}]: ").strip().lower()
    if not raw:
        return default
    return raw in ("y", "yes")


def _run_interactive_form(supabase: Client, args: argparse.Namespace) -> argparse.Namespace:
    """
    Walk the user through all required inputs. Mutates args in-place.
    Called when --source is not provided on the command line.
    """
    console.print(Panel.fit(
        "[bold cyan]Titan Systems — Lead Ingestion[/bold cyan]\n"
        "Scrape leads into Supabase. Dry-run is on by default — nothing writes until you confirm.",
        border_style="cyan",
    ))
    console.print()

    # --- Campaign selection ---
    campaigns = _list_campaigns(supabase)
    if campaigns:
        console.print("  [bold]Available campaigns:[/bold]")
        for i, c in enumerate(campaigns, 1):
            status_colour = "green" if c["status"] == "active" else "yellow"
            console.print(
                f"  [dim]{i}.[/dim] {c['name']}  "
                f"[{status_colour}]{c['status']}[/{status_colour}]  "
                f"[dim]{c['id']}[/dim]"
            )
        console.print()
        raw = input("  Select campaign number (or paste UUID): ").strip()
        if raw.isdigit() and 1 <= int(raw) <= len(campaigns):
            args.campaign = campaigns[int(raw) - 1]["id"]
        else:
            args.campaign = raw
    else:
        console.print("  [yellow]No campaigns found in Supabase.[/yellow]")
        console.print("  Create one at your Supabase dashboard first, then re-run.\n")
        sys.exit(1)

    # --- Source selection ---
    console.print()
    console.print("  [bold]Lead source:[/bold]")
    source_keys = list(_SOURCE_LABELS.keys())
    for i, key in enumerate(source_keys, 1):
        console.print(f"  [dim]{i}.[/dim] {_SOURCE_LABELS[key]}")
    console.print()
    while True:
        raw = input("  Select source number: ").strip()
        if raw.isdigit() and 1 <= int(raw) <= len(source_keys):
            args.source = source_keys[int(raw) - 1]
            break
        console.print("  [red]Enter a number from the list above.[/red]")

    # --- Source-specific inputs ---
    console.print()
    if args.source == "google_maps":
        args.keyword = _prompt("Keyword (e.g. plumbers, electricians)")
        args.location = _prompt("Location (e.g. Adelaide, SA)")
        args.limit = _prompt_int("Max leads to scrape", 50)
        args.query = None
        args.file = None
    elif args.source in ("linkedin_company", "linkedin_profile", "facebook"):
        args.query = _prompt(f"Search query (e.g. plumbers Adelaide)")
        args.limit = _prompt_int("Max leads to scrape", 50)
        args.keyword = None
        args.location = None
        args.file = None
    elif args.source == "csv":
        args.file = _prompt("Path to file (.csv / .xlsx / .xls)")
        args.keyword = None
        args.location = None
        args.query = None
        args.limit = 0

    # --- Dry-run ---
    console.print()
    args.dry_run = _prompt_yn("Dry-run first? (preview without writing)", default=True)

    return args


# ---------------------------------------------------------------------------
# Live table display during upsert
# ---------------------------------------------------------------------------

def _build_table(rows_so_far: list[tuple[str, str, str, str]]) -> Table:
    """Build a Rich table from accumulated (name, location, phone, status) tuples."""
    table = Table(
        title="[bold]Leads being written to Supabase[/bold]",
        show_lines=False,
        header_style="bold cyan",
        border_style="dim",
        expand=False,
    )
    table.add_column("#", style="dim", width=4, justify="right")
    table.add_column("Business Name", min_width=28, max_width=40)
    table.add_column("Location", min_width=20, max_width=30)
    table.add_column("Phone", min_width=14, max_width=18)
    table.add_column("Status", width=12)

    for i, (name, loc, phone, status) in enumerate(rows_so_far, 1):
        colour = "green" if "inserted" in status else "yellow"
        table.add_row(
            str(i),
            name[:40],
            (loc or "")[:30],
            (phone or "")[:18],
            f"[{colour}]{status}[/{colour}]",
        )
    return table


def _upsert_with_live_table(
    supabase: Client,
    records: list[LeadRecord],
    org_id: str,
    campaign_id: str,
) -> tuple[int, int, int]:
    """
    Upsert in batches of _UPSERT_BATCH_SIZE, updating a Rich live table after each batch.
    Returns (inserted, skipped_duplicates, error_count).
    """
    inserted_total = skipped_total = errors_total = 0
    rows_display: list[tuple[str, str, str, str]] = []

    with Live(console=console, refresh_per_second=4, vertical_overflow="visible") as live:
        for i in range(0, len(records), _UPSERT_BATCH_SIZE):
            batch = records[i : i + _UPSERT_BATCH_SIZE]
            rows = [r.to_db_row(org_id, campaign_id) for r in batch]

            try:
                resp = (
                    supabase.table("leads")
                    .upsert(
                        rows,
                        on_conflict="campaign_id,business_name,location",
                        ignore_duplicates=True,
                    )
                    .execute()
                )
                inserted_ids = {r["business_name"] for r in (resp.data or [])}
                inserted_count = len(resp.data) if resp.data else 0
                skipped_count = len(batch) - inserted_count
                inserted_total += inserted_count
                skipped_total += skipped_count

                for record in batch:
                    status = "✓ inserted" if record.business_name in inserted_ids else "↷ duplicate"
                    rows_display.append((
                        record.business_name,
                        record.location or "",
                        record.phone or "",
                        status,
                    ))

            except Exception as e:
                log.error("Upsert batch %d–%d failed: %s", i + 1, i + len(batch), e)
                errors_total += len(batch)
                for record in batch:
                    rows_display.append((
                        record.business_name,
                        record.location or "",
                        record.phone or "",
                        "✗ error",
                    ))

            live.update(_build_table(rows_display))

    return inserted_total, skipped_total, errors_total


# ---------------------------------------------------------------------------
# JSONL run log (matches scheduler.py pattern)
# ---------------------------------------------------------------------------

def _write_json_log(path: str, payload: dict) -> None:
    try:
        os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
        with open(path, "a", encoding="utf-8") as f:
            f.write(json.dumps(payload) + "\n")
    except Exception as e:
        log.warning("Failed to write JSON log: %s", e)


# ---------------------------------------------------------------------------
# Post-ingestion enrichment prompt
# ---------------------------------------------------------------------------

def _prompt_continue_enrichment(campaign_id: str, leads_inserted: int) -> None:
    """Ask user if they want to hand off to the enrichment agent now."""
    if leads_inserted == 0:
        return

    console.print()
    console.rule("[dim]Pipeline handoff[/dim]")
    console.print(
        f"\n  [bold]{leads_inserted} lead(s)[/bold] are now queued in Supabase "
        f"with [cyan]enrichment_status = 'queued'[/cyan]."
    )
    console.print(
        "  The enrichment agent will find decision-maker names, emails, and ICP scores.\n"
    )

    run_now = _prompt_yn("  Continue to enrichment now?", default=False)
    if run_now:
        cmd = [
            sys.executable, "-m", "agents.enrichment.agent",
            "--campaign", campaign_id,
        ]
        console.print(f"\n  Launching enrichment agent…\n  [dim]{' '.join(cmd)}[/dim]\n")
        # Run as subprocess — keeps ingestion and enrichment fully decoupled
        result = subprocess.run(cmd, cwd=_find_project_root())
        if result.returncode != 0:
            console.print(
                f"\n  [yellow]⚠ Enrichment exited with code {result.returncode}. "
                f"Check output above for details.[/yellow]"
            )
    else:
        console.print(
            f"\n  To run enrichment later:\n"
            f"  [cyan]python -m agents.enrichment.agent --campaign {campaign_id}[/cyan]\n"
        )


# ---------------------------------------------------------------------------
# Dry-run summary display
# ---------------------------------------------------------------------------

def _print_dry_run_summary(
    campaign_name: str,
    campaign_id: str,
    source: str,
    args: argparse.Namespace,
    records: list[LeadRecord],
) -> None:
    actor = ACTOR_IDS.get(source, "file")

    console.print(Panel.fit(
        f"[bold yellow]DRY RUN — nothing has been written to Supabase[/bold yellow]",
        border_style="yellow",
    ))
    console.print(f"  Campaign:    {campaign_name} [dim]({campaign_id})[/dim]")
    console.print(f"  Source:      {_SOURCE_LABELS.get(source, source)}")

    if source == "csv":
        console.print(f"  File:        {args.file}")
    else:
        if getattr(args, "keyword", None):
            console.print(f"  Keyword:     {args.keyword}")
        if getattr(args, "query", None):
            console.print(f"  Query:       {args.query}")
        if getattr(args, "location", None):
            console.print(f"  Location:    {args.location}")
        console.print(f"  Limit:       {args.limit}")
        console.print(f"  Apify actor: [dim]{actor}[/dim]")

    console.print(f"\n  Leads fetched: [bold]{len(records)}[/bold]")

    # Sample table
    if records:
        sample = records[:10]
        table = Table(show_lines=False, header_style="bold", border_style="dim")
        table.add_column("#", width=4, justify="right", style="dim")
        table.add_column("Business Name", min_width=28)
        table.add_column("Location", min_width=20)
        table.add_column("Phone", min_width=14)
        table.add_column("Website", min_width=20)
        for i, r in enumerate(sample, 1):
            table.add_row(str(i), r.business_name, r.location or "", r.phone or "", r.website or "")
        console.print(table)
        if len(records) > 10:
            console.print(f"  [dim]… and {len(records) - 10} more[/dim]")

    if getattr(args, "output", None):
        import json as _json
        with open(args.output, "w", encoding="utf-8") as f:
            _json.dump([r.to_db_row("DRY_RUN_ORG", campaign_id) for r in records], f, indent=2)
        console.print(f"\n  Dry-run payload written to: [cyan]{args.output}[/cyan]")

    console.print(
        f"\n  Re-run with [cyan]--no-dry-run[/cyan] to write to Supabase.\n"
    )


# ---------------------------------------------------------------------------
# Main ingestion orchestration
# ---------------------------------------------------------------------------

def run_ingestion(
    supabase: Client,
    campaign_id: str,
    source: str,
    args: argparse.Namespace,
    dry_run: bool = True,
    json_log_path: Optional[str] = None,
) -> IngestionRunResult:
    started_at = datetime.now(timezone.utc).isoformat()
    result = IngestionRunResult(campaign_id=campaign_id, source=source)

    # 1. Campaign pre-flight
    try:
        org_id, campaign_name = _verify_campaign(supabase, campaign_id)
    except ValueError as e:
        console.print(f"\n  [bold red]✗[/bold red] {e}\n")
        result.errors.append(str(e))
        return result

    # 2. Apify pre-flight (skip for CSV — no external API needed)
    if source != "csv":
        ok = _run_apify_preflight()
        if not ok:
            msg = "APIFY_API_KEY is invalid or missing"
            result.errors.append(msg)
            return result

    # 3. Fetch leads from source
    console.print(f"  Fetching leads from [cyan]{_SOURCE_LABELS.get(source, source)}[/cyan]…")
    try:
        if source == "google_maps":
            records = fetch_google_maps(args.keyword, args.location, args.limit)
        elif source == "linkedin_company":
            records = fetch_linkedin_company(args.query, args.limit)
        elif source == "linkedin_profile":
            records = fetch_linkedin_profile(args.query, args.limit)
        elif source == "facebook":
            records = fetch_facebook(args.query, args.limit)
        elif source == "csv":
            records = read_file(args.file)
        else:
            raise ValueError(f"Unknown source: {source}")
    except CredentialInvalidError as e:
        console.print(f"\n  [bold red]✗ Credential error:[/bold red] {e}\n")
        result.errors.append(str(e))
        return result
    except ApifyTimeoutError as e:
        console.print(f"\n  [bold red]✗ Timeout:[/bold red] {e}\n")
        result.errors.append(str(e))
        return result
    except ServiceDownError as e:
        console.print(f"\n  [bold red]✗ Apify service error:[/bold red] {e}\n")
        result.errors.append(str(e))
        return result
    except (ValueError, FileNotFoundError) as e:
        console.print(f"\n  [bold red]✗[/bold red] {e}\n")
        result.errors.append(str(e))
        return result

    result.leads_fetched = len(records)

    # Guard: exclude records missing business_name
    valid = [r for r in records if r.business_name]
    result.leads_invalid = len(records) - len(valid)
    if result.leads_invalid:
        console.print(
            f"  [yellow]⚠ {result.leads_invalid} record(s) skipped — missing business name[/yellow]"
        )

    if not valid:
        console.print("  [yellow]No valid leads to ingest.[/yellow]\n")
        return result

    # 4. Dry-run: show summary and exit without writing
    if dry_run:
        _print_dry_run_summary(campaign_name, campaign_id, source, args, valid)
        return result

    # 5. Upsert to Supabase with live table
    console.print(f"\n  Writing [bold]{len(valid)}[/bold] leads to Supabase…\n")
    inserted, skipped, upsert_errors = _upsert_with_live_table(supabase, valid, org_id, campaign_id)
    result.leads_inserted = inserted
    result.leads_skipped = skipped
    if upsert_errors:
        result.errors.append(f"{upsert_errors} rows failed to upsert")

    # 6. Summary
    console.print()
    console.rule("[green]Ingestion complete[/green]")
    console.print(f"  Fetched:    [bold]{result.leads_fetched}[/bold]")
    console.print(f"  Inserted:   [bold green]{result.leads_inserted}[/bold green]")
    console.print(f"  Duplicates: [bold yellow]{result.leads_skipped}[/bold yellow]")
    if result.leads_invalid:
        console.print(f"  Invalid:    [bold red]{result.leads_invalid}[/bold red]  (missing business name)")
    if upsert_errors:
        console.print(f"  Errors:     [bold red]{upsert_errors}[/bold red]")

    if json_log_path:
        _write_json_log(json_log_path, {
            "timestamp": started_at,
            "campaign_id": campaign_id,
            "source": source,
            "leads_fetched": result.leads_fetched,
            "leads_inserted": result.leads_inserted,
            "leads_skipped": result.leads_skipped,
            "leads_invalid": result.leads_invalid,
            "errors": result.errors,
        })

    return result


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Lead Ingestion Agent — run without --source to open the browser UI",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--campaign", default=None, metavar="UUID",
                        help="Campaign UUID (prompted interactively if omitted)")
    parser.add_argument("--source", default=None, choices=SUPPORTED_SOURCES,
                        help="Lead source — omit to open the browser UI")
    parser.add_argument("--no-ui", action="store_true", default=False,
                        help="Skip browser UI and use the terminal form instead")

    # Apify source args
    apify_group = parser.add_argument_group("Apify source options")
    apify_group.add_argument("--keyword", default=None,
                             help="Search keyword (google_maps only)")
    apify_group.add_argument("--location", default=None,
                             help="Location string (google_maps only, e.g. 'Adelaide, SA')")
    apify_group.add_argument("--query", default=None,
                             help="Search query (linkedin_*, facebook)")
    apify_group.add_argument("--limit", type=int, default=50,
                             help="Max leads to scrape (default: 50)")

    # CSV source args
    csv_group = parser.add_argument_group("File source options")
    csv_group.add_argument("--file", default=None, metavar="PATH",
                           help="Path to .csv, .xlsx, or .xls file")

    # Run options
    parser.add_argument("--dry-run", action="store_true", default=True,
                        help="Preview without writing to Supabase (default: on)")
    parser.add_argument("--no-dry-run", dest="dry_run", action="store_false",
                        help="Execute: write leads to Supabase")
    parser.add_argument("--output", default=None, metavar="PATH",
                        help="Dry-run only: write payload as JSON to this path for inspection")
    parser.add_argument("--json-log", default=None, metavar="PATH",
                        help="Append a JSONL run summary to this file")

    args = parser.parse_args()

    supabase: Client = create_client(
        os.environ["SUPABASE_URL"],
        os.environ["SUPABASE_SERVICE_ROLE_KEY"],
    )

    # If --source is not provided, open the browser UI (unless --no-ui requested)
    if not args.source:
        if args.no_ui:
            args = _run_interactive_form(supabase, args)
        else:
            from agents.ingestion.ui import launch
            launch(supabase)
            return
    else:
        # Validate required flags for the given source when running non-interactively
        if args.source == "google_maps" and (not args.keyword or not args.location):
            parser.error("--source google_maps requires --keyword and --location")
        elif args.source in ("linkedin_company", "linkedin_profile", "facebook") and not args.query:
            parser.error(f"--source {args.source} requires --query")
        elif args.source == "csv" and not args.file:
            parser.error("--source csv requires --file <path>")

    result = run_ingestion(
        supabase=supabase,
        campaign_id=args.campaign,
        source=args.source,
        args=args,
        dry_run=args.dry_run,
        json_log_path=args.json_log,
    )

    # Offer enrichment handoff after a successful real run
    if not args.dry_run and result.success and result.leads_inserted > 0:
        _prompt_continue_enrichment(args.campaign, result.leads_inserted)

    sys.exit(result.exit_code)


if __name__ == "__main__":
    main()
