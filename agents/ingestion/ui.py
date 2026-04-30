"""
Browser-based UI for lead ingestion and enrichment.

Launched automatically when running `python -m agents.ingestion.agent` without --source.
Opens http://localhost:5100 in your default browser.

Routes:
  GET  /                            — main form
  GET  /api/organisations           — list orgs (for campaign creation)
  GET  /api/campaigns               — list campaigns (includes queued_count)
  POST /api/campaigns               — create campaign
  DELETE /api/campaigns/<id>        — archive campaign
  POST /api/upload-csv              — accept uploaded/dropped CSV → return temp path
  POST /api/ingest                  — start ingestion → return job_id
  GET  /api/ingest/<job_id>/stream  — SSE progress stream
  POST /api/enrich                  — start enrichment → return job_id
  GET  /api/enrich/<job_id>/stream  — SSE progress stream
"""

from __future__ import annotations

import asyncio
import json
import os
import queue
import tempfile
import threading
import uuid
import webbrowser
from pathlib import Path
from typing import TYPE_CHECKING

from dotenv import load_dotenv

if TYPE_CHECKING:
    from supabase import Client

load_dotenv()

_PORT = 5100

# ---------------------------------------------------------------------------
# Embedded HTML (single-file UI — no templates directory needed)
# ---------------------------------------------------------------------------

_HTML = r"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1.0" />
  <title>Titan Systems AI — Lead Pipeline</title>
  <link rel="preconnect" href="https://fonts.googleapis.com" />
  <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin />
  <link href="https://fonts.googleapis.com/css2?family=Barlow+Condensed:wght@400;500;600&family=Outfit:wght@300;400;500&display=swap" rel="stylesheet" />
  <style>
    :root {
      --primary:       #2997ff;
      --primary-hover: #1a82e6;
      --bg:            #05010d;
      --surface:       #1d1d1f;
      --border:        rgba(255,255,255,0.06);
      --border-mid:    rgba(255,255,255,0.10);
      --text:          #f5f5f7;
      --text-sub:      #b8b8bd;
      --text-muted:    rgba(255,255,255,0.40);
      --font-display:  'Barlow Condensed', sans-serif;
      --font-body:     'Outfit', 'Inter', sans-serif;
    }
    *, *::before, *::after { box-sizing: border-box; margin: 0; padding: 0; }
    html { font-family: var(--font-body); font-size: 15px; }
    body {
      background: radial-gradient(ellipse at 20% 50%, rgba(41,151,255,0.06) 0%, transparent 60%),
                  radial-gradient(ellipse at 80% 20%, rgba(99,66,199,0.08) 0%, transparent 55%),
                  radial-gradient(ellipse at 60% 80%, rgba(52,211,153,0.04) 0%, transparent 50%),
                  #05010d;
      color: var(--text);
      min-height: 100vh;
      font-family: var(--font-body);
      font-weight: 400;
      -webkit-font-smoothing: antialiased;
    }

    /* Nav */
    .site-nav {
      position: sticky; top: 0; z-index: 100;
      display: flex; align-items: center; justify-content: space-between;
      padding: 0 2rem;
      height: 64px;
      background: rgba(5,1,13,0.75);
      backdrop-filter: blur(24px) saturate(1.8);
      border-bottom: 1px solid rgba(255,255,255,0.07);
      box-shadow: 0 1px 0 rgba(255,255,255,0.03), 0 4px 20px rgba(0,0,0,0.4);
    }
    .nav-logo-wrap {
      height: 40px; overflow: hidden;
      display: flex; align-items: center;
    }
    .nav-logo-wrap img {
      height: 120px; width: auto;
      display: block; margin-top: -10px;
    }
    .nav-right { display: flex; align-items: center; gap: 1rem; }

    /* Page wrapper */
    .page { max-width: 860px; margin: 0 auto; padding: 2rem 1.5rem 4rem; }

    /* Tabs */
    .tabs { display: flex; border-bottom: 1px solid var(--border-mid); margin-bottom: 2rem; gap: 0; }
    .tab-btn {
      font-family: var(--font-display);
      font-size: 13px; font-weight: 600;
      letter-spacing: 1.5px; text-transform: uppercase;
      color: var(--text-muted);
      background: transparent; border: none; cursor: pointer;
      padding: 0.75rem 1.5rem;
      border-bottom: 2px solid transparent;
      transition: color 0.15s, border-color 0.15s;
      margin-bottom: -1px;
    }
    .tab-btn:hover { color: var(--text-sub); }
    .tab-btn-active { color: var(--primary); border-bottom-color: var(--primary); }

    /* Cards */
    .card {
      background: rgba(29,29,31,0.55);
      backdrop-filter: blur(20px) saturate(1.4);
      border: 1px solid rgba(255,255,255,0.08);
      border-radius: 16px;
      padding: 1.5rem;
      box-shadow: 0 4px 24px rgba(0,0,0,0.3), inset 0 1px 0 rgba(255,255,255,0.04);
      transition: border-color 0.2s, box-shadow 0.2s;
    }
    .card:hover {
      border-color: rgba(255,255,255,0.13);
      box-shadow: 0 8px 32px rgba(0,0,0,0.4), inset 0 1px 0 rgba(255,255,255,0.06);
    }
    .card + .card { margin-top: 1.25rem; }
    .space-y > * + * { margin-top: 1.25rem; }

    /* Section headings */
    .section-label {
      font-family: var(--font-display);
      font-size: 11px; font-weight: 600;
      letter-spacing: 2px; text-transform: uppercase;
      color: var(--primary);
      margin-bottom: 1.25rem;
    }
    .card-title {
      font-family: var(--font-display);
      font-size: 17px; font-weight: 600;
      letter-spacing: 1px; text-transform: uppercase;
      color: var(--text);
    }

    /* Form labels */
    label.field-label {
      font-family: var(--font-display);
      font-size: 11px; font-weight: 600;
      letter-spacing: 1.5px; text-transform: uppercase;
      color: var(--text-muted);
      display: block; margin-bottom: 0.4rem;
    }
    .field-group { margin-bottom: 1.1rem; }

    /* Inputs */
    input[type=text], input[type=number], select, textarea {
      font-family: var(--font-body);
      background: rgba(255,255,255,0.03);
      border: 1px solid var(--border-mid);
      color: var(--text);
      border-radius: 8px;
      padding: 0.55rem 0.9rem;
      font-size: 14px;
      outline: none;
      width: 100%;
      transition: border-color 0.15s;
      appearance: none; -webkit-appearance: none;
    }
    input:focus, select:focus { border-color: var(--primary); box-shadow: 0 0 0 3px rgba(41,151,255,0.12); }
    input::placeholder { color: var(--text-muted); }
    select { background-image: url("data:image/svg+xml,%3Csvg xmlns='http://www.w3.org/2000/svg' width='12' height='12' fill='none' viewBox='0 0 24 24'%3E%3Cpath stroke='%23ffffff40' stroke-linecap='round' stroke-linejoin='round' stroke-width='2' d='M6 9l6 6 6-6'/%3E%3C/svg%3E"); background-repeat: no-repeat; background-position: right 0.75rem center; padding-right: 2.25rem; }
    select option { background: var(--surface); color: var(--text); }

    /* Grid helpers */
    .grid-2 { display: grid; grid-template-columns: 1fr 1fr; gap: 1rem; }
    .grid-3 { display: grid; grid-template-columns: 1fr 1fr 1fr; gap: 0.75rem; }
    .grid-4 { display: grid; grid-template-columns: 1fr 1fr 1fr 1fr; gap: 0.75rem; }
    @media (max-width: 600px) { .grid-2, .grid-3, .grid-4 { grid-template-columns: 1fr; } }

    /* Buttons */
    .btn {
      display: inline-flex; align-items: center; gap: 0.4rem;
      font-family: var(--font-display);
      font-size: 13px; font-weight: 600;
      letter-spacing: 1px; text-transform: uppercase;
      cursor: pointer; border: none;
      border-radius: 980px;
      padding: 0.55rem 1.5rem;
      transition: all 0.2s;
      white-space: nowrap;
    }
    .btn-primary {
      background: linear-gradient(135deg, #2997ff, #1a82e6);
      color: #fff;
      box-shadow: 0 2px 12px rgba(41,151,255,0.25);
    }
    .btn-primary:hover:not(:disabled) {
      background: linear-gradient(135deg, #3aa0ff, #2997ff);
      box-shadow: 0 4px 20px rgba(41,151,255,0.4);
      transform: translateY(-1px);
    }
    .btn-primary:disabled { opacity: 0.45; cursor: not-allowed; transform: none; }
    .btn-ghost {
      background: transparent; color: var(--text-sub);
      border: 1px solid var(--border-mid);
      font-size: 12px; padding: 0.4rem 1rem;
    }
    .btn-ghost:hover { background: rgba(255,255,255,0.05); color: var(--text); border-color: rgba(255,255,255,0.2); }
    .btn-danger { background: rgba(220,38,38,0.15); color: #f87171; border: 1px solid rgba(248,113,113,0.2); font-size: 12px; padding: 0.4rem 1rem; }
    .btn-danger:hover { background: rgba(220,38,38,0.25); }

    /* Radio cards */
    .radio-card {
      border: 1px solid var(--border-mid);
      border-radius: 8px;
      padding: 0.55rem 0.875rem;
      cursor: pointer;
      transition: all 0.15s;
      display: flex; align-items: center; gap: 0.5rem;
      font-size: 13px; color: var(--text-sub);
    }
    .radio-card:has(input:checked) { border-color: var(--primary); background: rgba(41,151,255,0.08); color: var(--text); }
    .radio-card:hover { border-color: rgba(41,151,255,0.3); color: var(--text); background: rgba(255,255,255,0.03); transform: translateY(-1px); }
    .radio-card input { accent-color: var(--primary); }

    /* Toggle */
    .toggle-wrap { display: flex; align-items: center; gap: 0.75rem; }
    .toggle { position: relative; display: inline-flex; align-items: center; cursor: pointer; }
    .toggle input { position: absolute; opacity: 0; width: 0; height: 0; }
    .toggle-track {
      width: 40px; height: 22px;
      background: rgba(255,255,255,0.12);
      border-radius: 11px;
      transition: background 0.2s;
      position: relative;
    }
    .toggle input:checked ~ .toggle-track { background: var(--primary); }
    .toggle-thumb {
      position: absolute; top: 3px; left: 3px;
      width: 16px; height: 16px;
      background: white; border-radius: 50%;
      transition: transform 0.2s;
    }
    .toggle input:checked ~ .toggle-track .toggle-thumb { transform: translateX(18px); }
    .toggle-label { font-size: 13px; color: var(--text-sub); }
    .toggle-label span { color: var(--text-muted); font-size: 12px; }

    /* Drop zone */
    .drop-zone {
      border: 1.5px dashed var(--border-mid);
      border-radius: 10px; padding: 2.5rem;
      text-align: center; cursor: pointer; transition: all 0.2s;
    }
    .drop-zone:hover { border-color: rgba(255,255,255,0.2); }
    .drop-zone.drag-over { border-color: var(--primary); background: rgba(41,151,255,0.05); }
    .drop-zone.has-file { border-color: rgba(41,151,255,0.5); background: rgba(41,151,255,0.04); border-style: solid; }

    /* Campaign list badges */
    .badge {
      display: inline-block; padding: 0.15rem 0.6rem;
      border-radius: 980px; font-size: 10px; font-weight: 600;
      font-family: var(--font-display); letter-spacing: 1px; text-transform: uppercase;
    }
    .badge-active   { background: rgba(41,151,255,0.15); color: var(--primary); border: 1px solid rgba(41,151,255,0.25); }
    .badge-paused   { background: rgba(251,191,36,0.12); color: #fbbf24; border: 1px solid rgba(251,191,36,0.2); }
    .badge-archived { background: rgba(255,255,255,0.05); color: var(--text-muted); border: 1px solid var(--border); }

    /* Stat tiles */
    .stat-tile {
      background: rgba(255,255,255,0.03);
      border: 1px solid var(--border);
      border-radius: 10px; padding: 1rem;
      text-align: center;
      transition: transform 0.2s, border-color 0.2s, box-shadow 0.2s;
    }
    .stat-tile:hover {
      transform: translateY(-2px);
      border-color: rgba(41,151,255,0.2);
      box-shadow: 0 4px 16px rgba(41,151,255,0.08);
    }
    .stat-tile .stat-val {
      font-family: var(--font-display);
      font-size: 32px; font-weight: 600;
      letter-spacing: 1px; line-height: 1;
      color: var(--primary); margin-bottom: 0.3rem;
    }
    .stat-tile .stat-val.green { color: #34d399; }
    .stat-tile .stat-val.amber { color: #fbbf24; }
    .stat-tile .stat-val.red   { color: #f87171; }
    .stat-tile .stat-key {
      font-family: var(--font-display); font-size: 10px;
      letter-spacing: 1.5px; text-transform: uppercase;
      color: var(--text-muted);
    }

    /* Tables */
    table { width: 100%; border-collapse: collapse; }
    thead th {
      font-family: var(--font-display);
      font-size: 10px; font-weight: 600;
      letter-spacing: 1.5px; text-transform: uppercase;
      color: var(--text-muted);
      text-align: left; padding: 0.5rem 0.75rem 0.5rem 0;
      border-bottom: 1px solid var(--border);
    }
    tbody tr { border-bottom: 1px solid rgba(255,255,255,0.03); transition: background 0.1s; }
    tbody tr:hover { background: rgba(255,255,255,0.025); }
    tbody tr:last-child { border-bottom: none; }
    tbody td { padding: 0.5rem 0.75rem 0.5rem 0; font-size: 13px; color: var(--text-sub); }

    /* Mapper */
    .mapper-select { width: 155px !important; padding: 3px 6px !important; font-size: 12px !important; }
    .mapper-key { width: 115px !important; padding: 3px 6px !important; font-size: 12px !important; }

    /* Log output */
    .log-wrap {
      background: rgba(0,0,0,0.4);
      border: 1px solid var(--border);
      border-radius: 8px;
      padding: 0.75rem 1rem;
      max-height: 220px; overflow-y: auto;
      font-family: ui-monospace, 'SFMono-Regular', Menlo, monospace;
      font-size: 12px;
    }
    .log-line { padding: 0.1rem 0; border-bottom: 1px solid rgba(255,255,255,0.03); }
    .log-line:last-child { border-bottom: none; }
    .log-error   { color: #f87171; }
    .log-success { color: #34d399; }
    .log-info    { color: var(--text-muted); }

    /* Enrichment row status colors */
    .enrich-row-enriched  { color: #34d399; }
    .enrich-row-no_dm     { color: #fbbf24; }
    .enrich-row-no_result { color: #fb923c; }
    .enrich-row-failed    { color: #f87171; }

    /* Step trace expand toggle */
    .expand-btn {
      background: none; border: none; cursor: pointer;
      color: var(--text-muted); font-size: 11px; padding: 0 0.25rem;
      transition: color 0.15s;
    }
    .expand-btn:hover { color: var(--primary); }

    /* Step sub-table */
    .step-sub-row td { padding: 0 !important; }
    .step-trace {
      background: rgba(0,0,0,0.35);
      border-top: 1px solid var(--border);
      padding: 0.75rem 1rem 0.75rem 2rem;
    }
    .step-trace table { width: 100%; }
    .step-trace thead th {
      font-size: 9px; letter-spacing: 1.5px; padding: 0.3rem 0.6rem 0.3rem 0;
      color: rgba(255,255,255,0.25);
    }
    .step-trace tbody td {
      font-size: 11px; padding: 0.3rem 0.6rem 0.3rem 0;
      border-bottom: 1px solid rgba(255,255,255,0.03);
      color: var(--text-muted); vertical-align: top;
    }
    .step-trace tbody tr:last-child td { border-bottom: none; }
    .step-trace .step-ok   { color: #34d399; }
    .step-trace .step-fail { color: #f87171; }
    .step-trace .step-skip { color: rgba(255,255,255,0.2); }
    .step-trace .step-name {
      font-family: var(--font-display); font-size: 10px;
      letter-spacing: 1px; text-transform: uppercase; color: var(--text-sub);
      white-space: nowrap;
    }
    .brave-query {
      display: inline-block;
      background: rgba(41,151,255,0.10);
      border: 1px solid rgba(41,151,255,0.20);
      border-radius: 4px;
      padding: 1px 5px;
      font-size: 10px; color: #60b8ff;
      margin: 1px 2px 1px 0;
    }
    .supabase-btn {
      display: inline-flex; align-items: center; gap: 0.4rem;
      font-family: var(--font-display);
      font-size: 12px; font-weight: 600;
      letter-spacing: 1px; text-transform: uppercase;
      color: #34d399; border: 1px solid rgba(52,211,153,0.3);
      background: rgba(52,211,153,0.08);
      border-radius: 980px; padding: 0.45rem 1.2rem;
      text-decoration: none; transition: all 0.2s;
      cursor: pointer;
    }
    .supabase-btn:hover { background: rgba(52,211,153,0.15); border-color: rgba(52,211,153,0.5); }

    /* Spinner */
    .spinner {
      display: inline-block; width: 16px; height: 16px;
      border: 2px solid rgba(255,255,255,0.10);
      border-top-color: var(--primary);
      border-radius: 50%;
      animation: spin 0.7s linear infinite;
    }
    @keyframes spin { to { transform: rotate(360deg); } }

    /* Apify status dot */
    .status-dot {
      font-family: var(--font-display);
      font-size: 11px; letter-spacing: 1px; text-transform: uppercase;
      color: var(--text-muted);
    }
    .status-dot.ok  { color: #34d399; }
    .status-dot.err { color: #f87171; }

    /* Divider */
    .divider { border: none; border-top: 1px solid var(--border); margin: 1.25rem 0; }

    /* Scrollbar */
    ::-webkit-scrollbar { width: 5px; height: 5px; }
    ::-webkit-scrollbar-track { background: transparent; }
    ::-webkit-scrollbar-thumb { background: rgba(255,255,255,0.12); border-radius: 3px; }

    /* Campaign row */
    .campaign-row {
      display: flex; align-items: center; justify-content: space-between;
      padding: 0.65rem 0.5rem;
      border-bottom: 1px solid rgba(255,255,255,0.04);
      border-radius: 6px;
      transition: background 0.1s;
    }
    .campaign-row:last-child { border-bottom: none; }
    .campaign-row:hover { background: rgba(255,255,255,0.03); }

    /* Preflight rows */
    .preflight-row {
      display: flex; align-items: flex-start; gap: 0.75rem;
      padding: 0.7rem 0.9rem;
      border-radius: 8px;
      border-left: 3px solid transparent;
      background: rgba(255,255,255,0.02);
      transition: background 0.15s;
      margin-bottom: 0.4rem;
    }
    .preflight-row:hover { background: rgba(255,255,255,0.045); }
    .preflight-row:last-child { margin-bottom: 0; }
    .preflight-row.ok   { border-left-color: #34d399; }
    .preflight-row.fail { border-left-color: #f87171; }
    .preflight-row.warn { border-left-color: #fbbf24; }
    .preflight-icon { font-size: 14px; flex-shrink: 0; margin-top: 1px; }
    .preflight-name {
      font-family: var(--font-display); font-size: 11px;
      font-weight: 600; letter-spacing: 1px; text-transform: uppercase;
      color: var(--text-sub); min-width: 130px;
    }
    .preflight-msg { font-size: 12px; color: var(--text-muted); flex: 1; line-height: 1.4; }
    .preflight-tag {
      font-family: var(--font-display); font-size: 10px;
      letter-spacing: 1px; text-transform: uppercase;
      padding: 0.15rem 0.5rem; border-radius: 4px;
      white-space: nowrap; flex-shrink: 0;
    }
    .preflight-tag.blocking { background: rgba(248,113,113,0.15); color: #f87171; }

    .hidden { display: none !important; }
  </style>
</head>
<body>

<!-- Nav -->
<nav class="site-nav">
  <div class="nav-logo-wrap">
    <img src="/logo" alt="Titan Systems AI" />
  </div>
  <div class="nav-right">
    <span class="status-dot" id="apify-status">checking Apify…</span>
  </div>
</nav>

<!-- Page -->
<div class="page">

  <!-- Page heading -->
  <div style="margin-bottom:2rem">
    <p class="section-label">Internal Tools</p>
    <h1 style="font-family:var(--font-display);font-size:32px;font-weight:600;letter-spacing:2px;text-transform:uppercase;color:var(--text);line-height:1.1">Lead Pipeline</h1>
    <p style="font-size:13px;color:var(--text-muted);margin-top:0.4rem;font-weight:300">Ingest leads from any source · Enrich with AI · Export to outreach</p>
  </div>

  <!-- Tabs -->
  <div class="tabs">
    <button class="tab-btn tab-btn-active" id="tab-btn-ingest" onclick="switchTab('ingest')">Ingest</button>
    <button class="tab-btn" id="tab-btn-enrich" onclick="switchTab('enrich')">Enrich</button>
  </div>

  <!-- ===== INGEST TAB ===== -->
  <div id="tab-ingest" class="space-y">

  <!-- Campaign Management -->
  <div class="card">
    <div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:1.1rem">
      <span class="card-title">Campaigns</span>
      <button class="btn btn-ghost" onclick="toggleAddCampaign()">+ New</button>
    </div>

    <!-- Add campaign form -->
    <div id="add-campaign-form" class="hidden" style="margin-top:1rem">
      <hr class="divider" />
      <p class="section-label" style="margin-bottom:0.9rem">New Campaign</p>
      <div class="grid-2" style="margin-bottom:1rem">
        <div class="field-group">
          <label class="field-label">Organisation</label>
          <select id="new-org-id"></select>
        </div>
        <div class="field-group">
          <label class="field-label">Name <span style="color:#f87171">*</span></label>
          <input type="text" id="new-campaign-name" placeholder="e.g. Plumbers Adelaide Q3" />
        </div>
        <div class="field-group">
          <label class="field-label">Vertical</label>
          <input type="text" id="new-campaign-vertical" placeholder="e.g. plumbing, real_estate" />
        </div>
        <div class="field-group">
          <label class="field-label">Target City</label>
          <input type="text" id="new-campaign-city" placeholder="e.g. Adelaide, SA" />
        </div>
      </div>
      <div style="display:flex;gap:0.6rem;align-items:center">
        <button class="btn btn-primary" onclick="createCampaign()">Create</button>
        <button class="btn btn-ghost" onclick="toggleAddCampaign()">Cancel</button>
      </div>
      <p id="campaign-form-error" class="hidden" style="margin-top:0.5rem;font-size:12px;color:#f87171"></p>
    </div>

    <!-- Campaign list -->
    <div id="campaign-list">
      <p style="color:var(--text-muted);font-size:13px" id="campaigns-loading">Loading…</p>
    </div>
  </div>

  <!-- Ingestion Form -->
  <div class="card">
    <p class="section-label">Ingestion Settings</p>

    <div class="field-group">
      <label class="field-label">Campaign <span style="color:#f87171">*</span></label>
      <select id="campaign-select">
        <option value="">— select a campaign —</option>
      </select>
    </div>

    <div class="field-group">
      <label class="field-label">Lead Source <span style="color:#f87171">*</span></label>
      <div class="grid-3" id="source-group">
        <label class="radio-card"><input type="radio" name="source" value="google_maps" onchange="onSourceChange()" /> Google Maps</label>
        <label class="radio-card"><input type="radio" name="source" value="linkedin_company" onchange="onSourceChange()" /> LinkedIn Co.</label>
        <label class="radio-card"><input type="radio" name="source" value="linkedin_profile" onchange="onSourceChange()" /> LinkedIn People</label>
        <label class="radio-card"><input type="radio" name="source" value="facebook" onchange="onSourceChange()" /> Facebook Pages</label>
        <label class="radio-card"><input type="radio" name="source" value="csv" onchange="onSourceChange()" /> CSV / XLSX</label>
      </div>
    </div>

    <!-- Google Maps fields -->
    <div id="fields-google_maps" class="hidden grid-2" style="margin-bottom:1rem">
      <div class="field-group">
        <label class="field-label">Keyword <span style="color:#f87171">*</span></label>
        <input type="text" id="f-keyword" placeholder="e.g. plumbers, electricians" />
      </div>
      <div class="field-group">
        <label class="field-label">Location <span style="color:#f87171">*</span></label>
        <input type="text" id="f-location" placeholder="e.g. Adelaide, SA" />
      </div>
      <div class="field-group">
        <label class="field-label">Max Leads</label>
        <input type="number" id="f-limit-maps" value="50" min="1" max="500" />
      </div>
    </div>

    <!-- LinkedIn / Facebook fields -->
    <div id="fields-query" class="hidden grid-2" style="margin-bottom:1rem">
      <div class="field-group" style="grid-column:span 2">
        <label class="field-label">Search Query <span style="color:#f87171">*</span></label>
        <input type="text" id="f-query" placeholder="e.g. plumbers Adelaide" />
      </div>
      <div class="field-group">
        <label class="field-label">Max Leads</label>
        <input type="number" id="f-limit-query" value="50" min="1" max="500" />
      </div>
    </div>

    <!-- CSV drop zone + mapper -->
    <div id="fields-csv" class="hidden" style="margin-bottom:1rem">
      <div class="field-group">
        <label class="field-label">File</label>
        <div class="drop-zone" id="drop-zone"
             ondragover="onDragOver(event)" ondragleave="onDragLeave(event)" ondrop="onDrop(event)"
             onclick="document.getElementById('file-input').click()">
          <div id="drop-zone-inner">
            <svg style="margin:0 auto 0.75rem;display:block;color:rgba(255,255,255,0.2)" width="36" height="36" fill="none" viewBox="0 0 24 24" stroke="currentColor">
              <path stroke-linecap="round" stroke-linejoin="round" stroke-width="1.5" d="M7 16a4 4 0 01-.88-7.903A5 5 0 1115.9 6h.1a5 5 0 011 9.9M15 13l-3-3m0 0l-3 3m3-3v8"/>
            </svg>
            <p style="color:var(--text-sub);font-size:13px">Drag &amp; drop CSV, XLSX, or XLS here</p>
            <p style="color:var(--text-muted);font-size:12px;margin-top:0.25rem">or click to browse</p>
          </div>
        </div>
        <input type="file" id="file-input" accept=".csv,.xlsx,.xls,.tsv" class="hidden" onchange="onFileSelected(event)" />
        <input type="hidden" id="f-csv-path" />
      </div>
      <!-- Column mapper -->
      <div id="column-mapper" class="hidden">
        <div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:0.6rem">
          <span style="font-family:var(--font-display);font-size:12px;font-weight:600;letter-spacing:1.5px;text-transform:uppercase;color:var(--text-sub)">Map Columns</span>
          <span style="font-size:11px;color:var(--text-muted)" id="mapper-summary"></span>
        </div>
        <div style="overflow-x:auto">
          <table>
            <thead>
              <tr>
                <th>CSV Column</th>
                <th>Sample</th>
                <th>Maps To</th>
                <th>Key Name</th>
              </tr>
            </thead>
            <tbody id="mapper-rows"></tbody>
          </table>
        </div>
        <p id="mapper-warning" class="hidden" style="margin-top:0.5rem;font-size:12px;color:#fbbf24">⚠ Assign at least one column to Business Name before running.</p>
      </div>
    </div>

    <hr class="divider" />

    <div class="toggle-wrap" style="margin-bottom:1.25rem">
      <label class="toggle">
        <input type="checkbox" id="dry-run-toggle" checked />
        <div class="toggle-track"><div class="toggle-thumb"></div></div>
      </label>
      <span class="toggle-label">Dry run <span>(preview without writing to Supabase)</span></span>
    </div>

    <div style="display:flex;align-items:center;gap:1rem">
      <button id="run-btn" class="btn btn-primary" onclick="startIngestion()">
        <svg width="12" height="12" fill="none" viewBox="0 0 24 24" stroke="currentColor"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2.5" d="M5 3l14 9-14 9V3z"/></svg>
        Run Ingestion
      </button>
      <p id="run-error" class="hidden" style="font-size:12px;color:#f87171"></p>
    </div>
  </div>

  <!-- Ingestion Output -->
  <div id="output-section" class="card hidden">
    <div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:1.1rem">
      <span class="card-title">Progress</span>
      <div id="output-spinner" class="spinner hidden"></div>
    </div>

    <div id="summary-stats" class="hidden grid-3" style="margin-bottom:1.25rem">
      <div class="stat-tile">
        <div class="stat-val" id="stat-total">0</div>
        <div class="stat-key">Fetched</div>
      </div>
      <div class="stat-tile">
        <div class="stat-val green" id="stat-inserted">0</div>
        <div class="stat-key">Inserted</div>
      </div>
      <div class="stat-tile">
        <div class="stat-val amber" id="stat-skipped">0</div>
        <div class="stat-key">Duplicates</div>
      </div>
    </div>

    <div id="lead-table-wrap" class="hidden" style="margin-bottom:1rem">
      <table>
        <thead>
          <tr>
            <th>Business Name</th>
            <th>Location</th>
            <th>Phone</th>
            <th>Status</th>
          </tr>
        </thead>
        <tbody id="lead-table-body"></tbody>
      </table>
    </div>

    <div id="log-output" class="log-wrap"></div>
  </div>

  </div><!-- /tab-ingest -->

  <!-- ===== ENRICH TAB ===== -->
  <div id="tab-enrich" class="space-y hidden">

  <!-- Service Health -->
  <div class="card" id="preflight-card">
    <div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:0.4rem">
      <span class="card-title">Service Health</span>
      <button class="btn btn-ghost" onclick="loadPreflight(document.getElementById('enrich-campaign-select').value)" style="font-size:11px;padding:0.3rem 0.85rem">↻ Refresh</button>
    </div>
    <p id="preflight-subtitle" style="font-size:12px;color:var(--text-muted);margin-bottom:1.1rem">Select a campaign to see projected usage.</p>
    <div id="preflight-body">
      <p style="color:var(--text-muted);font-size:13px">Select a campaign to check service status.</p>
    </div>
    <p id="preflight-blocker-msg" class="hidden" style="margin-top:0.75rem;font-size:12px;color:#f87171">⚠ One or more blocking services are unavailable. Resolve them before running enrichment.</p>
  </div>

  <!-- Enrichment Settings -->
  <div class="card">
    <p class="section-label">Enrichment Settings</p>

    <div class="field-group">
      <label class="field-label">Campaign <span style="color:#f87171">*</span></label>
      <select id="enrich-campaign-select" onchange="onEnrichCampaignChange()">
        <option value="">— select a campaign —</option>
      </select>
      <p id="enrich-queued-label" style="margin-top:0.4rem;font-size:12px;color:var(--text-muted)"></p>
    </div>

    <div class="field-group">
      <label class="field-label">Website Filter</label>
      <div style="display:flex;flex-wrap:wrap;gap:0.6rem">
        <label class="radio-card"><input type="radio" name="enrich-website" value="all" checked /> All leads</label>
        <label class="radio-card"><input type="radio" name="enrich-website" value="has_website" /> Has website</label>
        <label class="radio-card"><input type="radio" name="enrich-website" value="no_website" /> No website</label>
      </div>
    </div>

    <div class="field-group" style="max-width:200px">
      <label class="field-label">Batch Limit <span style="color:var(--text-muted);font-size:10px">(blank = all)</span></label>
      <input type="number" id="enrich-limit" placeholder="e.g. 20" min="1" max="500" />
    </div>

    <hr class="divider" />

    <div class="toggle-wrap" style="margin-bottom:1.25rem">
      <label class="toggle">
        <input type="checkbox" id="enrich-dry-run-toggle" checked />
        <div class="toggle-track"><div class="toggle-thumb"></div></div>
      </label>
      <span class="toggle-label">Dry run <span>(preview queued count without enriching)</span></span>
    </div>

    <div style="display:flex;align-items:center;gap:1rem">
      <button id="enrich-run-btn" class="btn btn-primary" onclick="startEnrichment()">
        <svg width="12" height="12" fill="none" viewBox="0 0 24 24" stroke="currentColor"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2.5" d="M5 3l14 9-14 9V3z"/></svg>
        Run Enrichment
      </button>
      <p id="enrich-run-error" class="hidden" style="font-size:12px;color:#f87171"></p>
    </div>
  </div>

  <!-- Enrichment Output -->
  <div id="enrich-output-section" class="card hidden">
    <div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:1.1rem">
      <span class="card-title">Enrichment Progress</span>
      <div id="enrich-output-spinner" class="spinner hidden"></div>
    </div>

    <div id="enrich-summary-stats" class="hidden grid-4" style="margin-bottom:1.25rem">
      <div class="stat-tile">
        <div class="stat-val" id="enrich-stat-total">0</div>
        <div class="stat-key">Queued</div>
      </div>
      <div class="stat-tile">
        <div class="stat-val green" id="enrich-stat-enriched">0</div>
        <div class="stat-key">Enriched</div>
      </div>
      <div class="stat-tile">
        <div class="stat-val amber" id="enrich-stat-no-dm">0</div>
        <div class="stat-key">No DM</div>
      </div>
      <div class="stat-tile">
        <div class="stat-val red" id="enrich-stat-failed">0</div>
        <div class="stat-key">Failed</div>
      </div>
    </div>

    <div id="enrich-table-wrap" class="hidden" style="margin-bottom:1rem">
      <table>
        <thead>
          <tr>
            <th style="width:20px"></th>
            <th>Business</th>
            <th>DM Found</th>
            <th>Email</th>
            <th>ICP</th>
            <th>Status</th>
          </tr>
        </thead>
        <tbody id="enrich-table-body"></tbody>
      </table>
    </div>

    <!-- Supabase link (shown after done) -->
    <div id="enrich-supabase-wrap" class="hidden" style="margin-top:1rem;display:flex;align-items:center;gap:1rem">
      <a id="enrich-supabase-btn" class="supabase-btn" href="#" target="_blank">
        <svg width="12" height="12" fill="none" viewBox="0 0 24 24" stroke="currentColor"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M10 6H6a2 2 0 00-2 2v10a2 2 0 002 2h10a2 2 0 002-2v-4M14 4h6m0 0v6m0-6L10 14"/></svg>
        View in Supabase
      </a>
    </div>

    <div id="enrich-log-output" class="log-wrap"></div>
  </div>

  </div><!-- /tab-enrich -->

</div><!-- /page -->

<script>
// ---------------------------------------------------------------------------
// State
// ---------------------------------------------------------------------------
let campaigns = [];
let organisations = [];
let csvTempPath = null;
let csvMappingValid = false;
let enrichSteps = {};      // business_name → [step_event, ...]
let enrichRowIdx = {};     // business_name → row index (for DOM ids)
let enrichRowCount = 0;

// ---------------------------------------------------------------------------
// Init
// ---------------------------------------------------------------------------
document.addEventListener('DOMContentLoaded', async () => {
  await Promise.all([loadOrgs(), loadCampaigns()]);
  checkApify();
  loadEnrichCampaigns();
});

async function loadOrgs() {
  const res = await fetch('/api/organisations');
  organisations = await res.json();
  const sel = document.getElementById('new-org-id');
  sel.innerHTML = organisations.map(o =>
    `<option value="${o.id}">${o.name}</option>`
  ).join('');
}

async function loadCampaigns() {
  const res = await fetch('/api/campaigns');
  campaigns = await res.json();
  renderCampaignList();
  renderCampaignSelect();
}

async function checkApify() {
  try {
    const res = await fetch('/api/apify-check');
    const data = await res.json();
    const el = document.getElementById('apify-status');
    if (data.ok) {
      el.className = 'status-dot ok';
      el.textContent = '✓ Apify';
    } else {
      el.className = 'status-dot err';
      el.textContent = '✗ Apify';
    }
  } catch { }
}

// ---------------------------------------------------------------------------
// Campaign list rendering
// ---------------------------------------------------------------------------
function renderCampaignList() {
  const el = document.getElementById('campaign-list');
  if (!campaigns.length) {
    el.innerHTML = '<p style="color:var(--text-muted);font-size:13px">No campaigns yet. Create one above.</p>';
    return;
  }
  el.innerHTML = campaigns.map(c => `
    <div class="campaign-row" id="camp-row-${c.id}">
      <div style="display:flex;align-items:center;gap:0.6rem;min-width:0">
        <span class="badge badge-${c.status}">${c.status}</span>
        <span style="font-size:13px;color:var(--text);white-space:nowrap;overflow:hidden;text-overflow:ellipsis">${escHtml(c.name)}</span>
        ${c.vertical ? `<span style="font-size:12px;color:var(--text-muted)">${escHtml(c.vertical)}</span>` : ''}
        ${c.target_city ? `<span style="font-size:12px;color:var(--text-muted)">· ${escHtml(c.target_city)}</span>` : ''}
      </div>
      <div style="flex-shrink:0;margin-left:0.75rem">
        ${c.status !== 'archived'
          ? `<button class="btn btn-ghost" onclick="archiveCampaign('${c.id}', '${escHtml(c.name)}')">Archive</button>`
          : ''}
      </div>
    </div>
  `).join('');
}

function renderCampaignSelect() {
  const sel = document.getElementById('campaign-select');
  const active = campaigns.filter(c => c.status !== 'archived');
  sel.innerHTML = '<option value="">— select a campaign —</option>' +
    active.map(c =>
      `<option value="${c.id}">${escHtml(c.name)}${c.target_city ? ' · ' + escHtml(c.target_city) : ''}</option>`
    ).join('');
}

// ---------------------------------------------------------------------------
// Campaign CRUD
// ---------------------------------------------------------------------------
function toggleAddCampaign() {
  const form = document.getElementById('add-campaign-form');
  form.classList.toggle('hidden');
  if (!form.classList.contains('hidden')) {
    document.getElementById('new-campaign-name').focus();
  }
}

async function createCampaign() {
  const name = document.getElementById('new-campaign-name').value.trim();
  const org_id = document.getElementById('new-org-id').value;
  const vertical = document.getElementById('new-campaign-vertical').value.trim();
  const target_city = document.getElementById('new-campaign-city').value.trim();
  const errEl = document.getElementById('campaign-form-error');

  if (!name) { showError(errEl, 'Campaign name is required'); return; }
  if (!org_id) { showError(errEl, 'Select an organisation'); return; }
  errEl.classList.add('hidden');

  const res = await fetch('/api/campaigns', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ name, org_id, vertical: vertical || null, target_city: target_city || null }),
  });
  const data = await res.json();
  if (!res.ok) { showError(errEl, data.error || 'Create failed'); return; }

  // Reset form and reload
  document.getElementById('new-campaign-name').value = '';
  document.getElementById('new-campaign-vertical').value = '';
  document.getElementById('new-campaign-city').value = '';
  document.getElementById('add-campaign-form').classList.add('hidden');
  await loadCampaigns();
}

async function archiveCampaign(id, name) {
  if (!confirm(`Archive campaign "${name}"?\n\nExisting leads will not be deleted.`)) return;

  const res = await fetch(`/api/campaigns/${id}`, { method: 'DELETE' });
  if (!res.ok) { alert('Failed to archive campaign'); return; }
  await loadCampaigns();
}

// ---------------------------------------------------------------------------
// Source switching
// ---------------------------------------------------------------------------
function onSourceChange() {
  const source = document.querySelector('input[name="source"]:checked')?.value;
  const allFields = ['google_maps', 'query', 'csv'];
  allFields.forEach(f => document.getElementById('fields-' + f).classList.add('hidden'));

  if (source === 'google_maps') {
    document.getElementById('fields-google_maps').classList.remove('hidden');
  } else if (['linkedin_company', 'linkedin_profile', 'facebook'].includes(source)) {
    document.getElementById('fields-query').classList.remove('hidden');
  } else if (source === 'csv') {
    document.getElementById('fields-csv').classList.remove('hidden');
  }
}

// ---------------------------------------------------------------------------
// CSV drag-and-drop
// ---------------------------------------------------------------------------
function onDragOver(e) {
  e.preventDefault();
  document.getElementById('drop-zone').classList.add('drag-over');
}

function onDragLeave(e) {
  document.getElementById('drop-zone').classList.remove('drag-over');
}

function onDrop(e) {
  e.preventDefault();
  document.getElementById('drop-zone').classList.remove('drag-over');
  const file = e.dataTransfer.files[0];
  if (file) uploadCsvFile(file);
}

function onFileSelected(e) {
  const file = e.target.files[0];
  if (file) uploadCsvFile(file);
}

async function uploadCsvFile(file) {
  const zone = document.getElementById('drop-zone');
  const inner = document.getElementById('drop-zone-inner');
  inner.innerHTML = '<div class="spinner mx-auto mb-2"></div><p class="text-slate-400 text-sm">Uploading…</p>';

  const fd = new FormData();
  fd.append('file', file);

  try {
    const res = await fetch('/api/upload-csv', { method: 'POST', body: fd });
    const data = await res.json();
    if (!res.ok) {
      inner.innerHTML = `<p class="text-red-400 text-sm">✗ ${escHtml(data.error)}</p>`;
      zone.classList.remove('has-file');
      return;
    }
    csvTempPath = data.path;
    csvMappingValid = false;
    document.getElementById('f-csv-path').value = data.path;
    zone.classList.add('has-file');
    inner.innerHTML = `
      <svg class="mx-auto mb-2 text-green-400" width="32" height="32" fill="none" viewBox="0 0 24 24" stroke="currentColor">
        <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M9 12l2 2 4-4m6 2a9 9 0 11-18 0 9 9 0 0118 0z"/>
      </svg>
      <p class="text-green-400 text-sm font-medium">${escHtml(data.filename)}</p>
      <p class="text-slate-500 text-xs mt-1">Click to replace</p>`;
    renderColumnMapper(data);
  } catch (err) {
    inner.innerHTML = `<p class="text-red-400 text-sm">✗ Upload failed</p>`;
  }
}

// ---------------------------------------------------------------------------
// Column mapper
// ---------------------------------------------------------------------------

const MAPPER_FIELDS = [
  { value: 'business_name', label: 'Business Name ✱' },
  { value: 'location',      label: 'Location' },
  { value: 'phone',         label: 'Phone' },
  { value: 'email',         label: 'Email' },
  { value: 'website',       label: 'Website' },
  { value: 'category',      label: 'Category' },
  { value: 'source_data',   label: '+ Additional data' },
  { value: 'ignore',        label: '— ignore —' },
];

function renderColumnMapper(data) {
  if (!data.columns || !data.columns.length) return;
  const tbody = document.getElementById('mapper-rows');
  tbody.innerHTML = '';
  const suggested = data.suggested_mapping || {};
  const samples   = data.sample_values   || {};

  data.columns.forEach((col, i) => {
    const target  = suggested[col] || 'ignore';
    const preview = (samples[col] || []).filter(Boolean).slice(0, 2)
                      .map(v => escHtml(String(v).slice(0, 35))).join(', ');
    const defaultKey = toSnakeCase(col);

    const opts = MAPPER_FIELDS.map(f =>
      `<option value="${f.value}"${target === f.value ? ' selected' : ''}>${f.label}</option>`
    ).join('');

    const tr = document.createElement('tr');
    tr.className = 'border-b border-slate-800 last:border-0';
    tr.innerHTML = `
      <td class="py-1.5 pr-3 text-slate-200 truncate max-w-[160px]" title="${escHtml(col)}">${escHtml(col)}</td>
      <td class="py-1.5 pr-3 text-slate-500 truncate max-w-[140px]">${preview}</td>
      <td class="py-1.5 pr-2">
        <select class="mapper-select" data-col="${escHtml(col)}" onchange="onMapperChange(this,${i})">${opts}</select>
      </td>
      <td class="py-1.5">
        <input type="text" id="mapper-key-${i}" class="mapper-key${target !== 'source_data' ? ' hidden' : ''}"
               value="${escHtml(defaultKey)}" placeholder="field_name" oninput="validateMapper()" />
      </td>`;
    tbody.appendChild(tr);
  });

  document.getElementById('column-mapper').classList.remove('hidden');
  validateMapper();
}

function onMapperChange(select, i) {
  document.getElementById(`mapper-key-${i}`).classList.toggle('hidden', select.value !== 'source_data');
  validateMapper();
}

function validateMapper() {
  const selects = document.querySelectorAll('.mapper-select');
  csvMappingValid = Array.from(selects).some(s => s.value === 'business_name');
  document.getElementById('mapper-warning').classList.toggle('hidden', csvMappingValid);
  // summary
  let schema = 0, extra = 0, ignored = 0;
  selects.forEach(s => { if (s.value === 'ignore') ignored++; else if (s.value === 'source_data') extra++; else schema++; });
  if (selects.length) document.getElementById('mapper-summary').textContent =
    `${schema} schema · ${extra} additional · ${ignored} ignored`;
}

function getColumnMapping() {
  const mapping = {};
  document.querySelectorAll('.mapper-select').forEach((sel, i) => {
    const col = sel.dataset.col;
    const target = sel.value;
    if (!col || target === 'ignore') return;
    if (target === 'source_data') {
      const keyEl = document.getElementById(`mapper-key-${i}`);
      const key = (keyEl ? keyEl.value.trim() : '') || toSnakeCase(col);
      mapping[col] = `source_data.${key}`;
    } else {
      mapping[col] = target;
    }
  });
  return mapping;
}

function toSnakeCase(str) {
  return str.toLowerCase().replace(/[^a-z0-9]+/g, '_').replace(/^_+|_+$/g, '');
}

// ---------------------------------------------------------------------------
// Ingestion run
// ---------------------------------------------------------------------------
async function startIngestion() {
  const runBtn = document.getElementById('run-btn');
  const runError = document.getElementById('run-error');
  runError.classList.add('hidden');

  const campaignId = document.getElementById('campaign-select').value;
  const source = document.querySelector('input[name="source"]:checked')?.value;
  const dryRun = document.getElementById('dry-run-toggle').checked;

  if (!campaignId) { showError(runError, 'Select a campaign'); return; }
  if (!source)     { showError(runError, 'Select a lead source'); return; }

  const payload = { campaign_id: campaignId, source, dry_run: dryRun };

  if (source === 'google_maps') {
    const kw = document.getElementById('f-keyword').value.trim();
    const loc = document.getElementById('f-location').value.trim();
    if (!kw || !loc) { showError(runError, 'Keyword and location are required for Google Maps'); return; }
    payload.keyword = kw;
    payload.location = loc;
    payload.limit = parseInt(document.getElementById('f-limit-maps').value) || 50;
  } else if (['linkedin_company', 'linkedin_profile', 'facebook'].includes(source)) {
    const q = document.getElementById('f-query').value.trim();
    if (!q) { showError(runError, 'Search query is required'); return; }
    payload.query = q;
    payload.limit = parseInt(document.getElementById('f-limit-query').value) || 50;
  } else if (source === 'csv') {
    if (!csvTempPath) { showError(runError, 'Please upload a CSV file first'); return; }
    const mapperShown = !document.getElementById('column-mapper').classList.contains('hidden');
    if (mapperShown && !csvMappingValid) {
      showError(runError, 'Assign at least one column to Business Name before running');
      return;
    }
    payload.file_path = csvTempPath;
    if (mapperShown) payload.column_mapping = getColumnMapping();
  }

  // Reset output section
  document.getElementById('output-section').classList.remove('hidden');
  document.getElementById('summary-stats').classList.add('hidden');
  document.getElementById('lead-table-wrap').classList.add('hidden');
  document.getElementById('lead-table-body').innerHTML = '';
  document.getElementById('log-output').innerHTML = '';
  document.getElementById('output-spinner').classList.remove('hidden');
  runBtn.disabled = true;
  runBtn.innerHTML = '<div class="spinner" style="width:12px;height:12px;border-width:2px"></div> Running…';

  const res = await fetch('/api/ingest', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(payload),
  });
  const { job_id, error } = await res.json();
  if (error) {
    showError(runError, error);
    resetRunBtn();
    return;
  }

  // Open SSE stream
  const evSource = new EventSource(`/api/ingest/${job_id}/stream`);
  evSource.onmessage = (e) => {
    const event = JSON.parse(e.data);
    handleProgressEvent(event);
    if (event.type === 'done') {
      evSource.close();
      resetRunBtn();
      document.getElementById('output-spinner').classList.add('hidden');
    }
  };
  evSource.onerror = () => {
    evSource.close();
    resetRunBtn();
    document.getElementById('output-spinner').classList.add('hidden');
    appendLog('Connection to server lost.', 'error');
  };
}

function handleProgressEvent(event) {
  if (event.type === 'ping') return;

  if (event.type === 'log') {
    appendLog(event.text, 'info');
  } else if (event.type === 'error') {
    appendLog('✗ ' + event.text, 'error');
  } else if (event.type === 'lead') {
    const tbody = document.getElementById('lead-table-body');
    document.getElementById('lead-table-wrap').classList.remove('hidden');
    const statusColor = event.status === 'inserted' ? '#34d399' :
                        event.status === 'duplicate' ? '#fbbf24' : 'var(--text-muted)';
    const statusIcon  = event.status === 'inserted' ? '✓' :
                        event.status === 'duplicate' ? '↷' : '·';
    const tr = document.createElement('tr');
    tr.innerHTML = `
      <td style="color:var(--text);max-width:180px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap">${escHtml(event.business_name)}</td>
      <td style="max-width:140px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap">${escHtml(event.location || '')}</td>
      <td>${escHtml(event.phone || '')}</td>
      <td style="color:${statusColor};font-family:ui-monospace,monospace;font-size:12px">${statusIcon} ${event.status || ''}</td>`;
    tbody.appendChild(tr);
    tbody.scrollIntoView({ behavior: 'smooth', block: 'nearest' });
  } else if (event.type === 'done') {
    document.getElementById('summary-stats').classList.remove('hidden');
    document.getElementById('stat-total').textContent    = event.total    ?? 0;
    document.getElementById('stat-inserted').textContent = event.inserted ?? 0;
    document.getElementById('stat-skipped').textContent  = event.skipped  ?? 0;
    if (event.dry_run) {
      appendLog('Dry run complete — nothing was written to Supabase. Remove the toggle to commit.', 'info');
    } else {
      appendLog(`Done. ${event.inserted} inserted, ${event.skipped} duplicates.`, 'success');
    }
  }
}

function appendLog(text, kind) {
  const el = document.getElementById('log-output');
  const line = document.createElement('div');
  line.className = `log-line log-${kind}`;
  line.textContent = text;
  el.appendChild(line);
  el.scrollTop = el.scrollHeight;
}

function resetRunBtn() {
  const btn = document.getElementById('run-btn');
  btn.disabled = false;
  btn.innerHTML = '<svg width="12" height="12" fill="none" viewBox="0 0 24 24" stroke="currentColor"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2.5" d="M5 3l14 9-14 9V3z"/></svg> Run Ingestion';
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------
function showError(el, msg) {
  el.textContent = msg;
  el.classList.remove('hidden');
}

function escHtml(s) {
  return String(s).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/"/g,'&quot;');
}

// ---------------------------------------------------------------------------
// Tab switching
// ---------------------------------------------------------------------------
function switchTab(name) {
  ['ingest', 'enrich'].forEach(t => {
    document.getElementById('tab-' + t).classList.toggle('hidden', t !== name);
    document.getElementById('tab-btn-' + t).classList.toggle('tab-btn-active', t === name);
  });
  if (name === 'enrich') {
    const cid = document.getElementById('enrich-campaign-select').value;
    loadPreflight(cid);
  }
}

// ---------------------------------------------------------------------------
// Service health preflight
// ---------------------------------------------------------------------------
let _preflightHasBlockers = false;

async function loadPreflight(campaignId) {
  const body = document.getElementById('preflight-body');
  const blockerMsg = document.getElementById('preflight-blocker-msg');
  body.innerHTML = '<div style="display:flex;align-items:center;gap:0.6rem;color:var(--text-muted);font-size:13px"><div class="spinner" style="width:14px;height:14px;border-width:2px"></div>Checking services…</div>';
  blockerMsg.classList.add('hidden');

  try {
    const url = campaignId
      ? `/api/preflight?campaign_id=${encodeURIComponent(campaignId)}`
      : '/api/preflight';
    const res = await fetch(url);
    const data = await res.json();
    _preflightHasBlockers = data.has_blockers;

    // Update card subtitle with queued lead count
    const subtitle = document.getElementById('preflight-subtitle');
    if (subtitle && data.queued_leads != null) {
      subtitle.textContent = data.queued_leads > 0
        ? `${data.queued_leads} leads queued — projected usage shown below`
        : 'No leads queued for this campaign';
    }

    body.innerHTML = data.checks.map(c => {
      const rowClass = c.ok ? 'ok' : (c.blocking ? 'fail' : 'warn');
      const icon = c.ok ? '✓' : '✗';
      const iconColor = c.ok ? '#34d399' : (c.blocking ? '#f87171' : '#fbbf24');
      const tag = (!c.ok && c.blocking)
        ? '<span class="preflight-tag blocking">Blocking</span>'
        : '';
      // Warning line: amber if it contains "run out" / "quota" / "Top up", green otherwise
      const warningHtml = c.warning
        ? `<div style="margin-top:0.3rem;font-size:11px;color:${
            /run out|Top up|quota will|not enough/i.test(c.warning) ? '#fbbf24' : 'rgba(255,255,255,0.35)'
          };line-height:1.4">${escHtml(c.warning)}</div>`
        : '';
      return `
        <div class="preflight-row ${rowClass}">
          <span class="preflight-icon" style="color:${iconColor}">${icon}</span>
          <span class="preflight-name">${escHtml(c.name)}</span>
          <div class="preflight-msg">
            ${escHtml(c.message)}
            ${warningHtml}
          </div>
          ${tag}
        </div>`;
    }).join('');

    if (data.has_blockers) {
      blockerMsg.classList.remove('hidden');
    }
    document.getElementById('enrich-run-btn').disabled = data.has_blockers;
  } catch (e) {
    body.innerHTML = '<p style="color:#f87171;font-size:13px">Failed to reach server. Check that the agent is running.</p>';
    _preflightHasBlockers = false;
  }
}

// ---------------------------------------------------------------------------
// Enrichment — campaign selector
// ---------------------------------------------------------------------------
async function loadEnrichCampaigns() {
  const res = await fetch('/api/campaigns');
  const data = await res.json();
  const sel = document.getElementById('enrich-campaign-select');
  const active = data.filter(c => c.status !== 'archived');
  sel.innerHTML = '<option value="">— select a campaign —</option>' +
    active.map(c => {
      const queued = c.queued_count != null ? ` (${c.queued_count} queued)` : '';
      return `<option value="${c.id}" data-queued="${c.queued_count || 0}">${escHtml(c.name)}${queued ? escHtml(queued) : ''}</option>`;
    }).join('');
}

function onEnrichCampaignChange() {
  const sel = document.getElementById('enrich-campaign-select');
  const opt = sel.options[sel.selectedIndex];
  const queued = opt ? parseInt(opt.dataset.queued || '0') : 0;
  const label = document.getElementById('enrich-queued-label');
  if (sel.value) {
    label.textContent = queued > 0
      ? `${queued} lead${queued === 1 ? '' : 's'} queued for enrichment`
      : 'No leads currently queued for this campaign';
    loadPreflight(sel.value);
  } else {
    label.textContent = '';
  }
}

// ---------------------------------------------------------------------------
// Enrichment — run
// ---------------------------------------------------------------------------
async function startEnrichment() {
  const runBtn = document.getElementById('enrich-run-btn');
  const runError = document.getElementById('enrich-run-error');
  runError.classList.add('hidden');

  const campaignId = document.getElementById('enrich-campaign-select').value;
  const websiteFilter = document.querySelector('input[name="enrich-website"]:checked')?.value || 'all';
  const limitVal = document.getElementById('enrich-limit').value.trim();
  const dryRun = document.getElementById('enrich-dry-run-toggle').checked;

  if (!campaignId) { showError(runError, 'Select a campaign'); return; }

  if (_preflightHasBlockers) {
    showError(runError, 'Fix blocking service issues before running (see Service Health above)');
    return;
  }

  const payload = {
    campaign_id: campaignId,
    website_filter: websiteFilter,
    dry_run: dryRun,
    limit: limitVal ? parseInt(limitVal) : null,
  };

  // Reset output
  enrichSteps = {}; enrichRowIdx = {}; enrichRowCount = 0;
  const outSec = document.getElementById('enrich-output-section');
  outSec.classList.remove('hidden');
  document.getElementById('enrich-summary-stats').classList.add('hidden');
  document.getElementById('enrich-table-wrap').classList.add('hidden');
  document.getElementById('enrich-supabase-wrap').classList.add('hidden');
  document.getElementById('enrich-table-body').innerHTML = '';
  document.getElementById('enrich-log-output').innerHTML = '';
  document.getElementById('enrich-output-spinner').classList.remove('hidden');
  runBtn.disabled = true;
  runBtn.innerHTML = '<div class="spinner" style="width:12px;height:12px;border-width:2px"></div> Running…';

  const res = await fetch('/api/enrich', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(payload),
  });
  const { job_id, error } = await res.json();
  if (error) {
    showError(runError, error);
    resetEnrichBtn();
    return;
  }

  const evSource = new EventSource(`/api/enrich/${job_id}/stream`);
  evSource.onmessage = (e) => {
    const event = JSON.parse(e.data);
    handleEnrichEvent(event);
    if (event.type === 'done') {
      evSource.close();
      resetEnrichBtn();
      document.getElementById('enrich-output-spinner').classList.add('hidden');
      loadEnrichCampaigns();
    }
  };
  evSource.onerror = () => {
    evSource.close();
    resetEnrichBtn();
    document.getElementById('enrich-output-spinner').classList.add('hidden');
    appendEnrichLog('Connection to server lost.', 'error');
  };
}

function handleEnrichEvent(event) {
  if (event.type === 'ping') return;

  if (event.type === 'log') {
    appendEnrichLog(event.text, 'info');
    return;
  }
  if (event.type === 'error') {
    appendEnrichLog('✗ ' + event.text, 'error');
    return;
  }

  if (event.type === 'step') {
    // Buffer steps keyed by business name
    const key = event.business_name || '';
    if (!enrichSteps[key]) enrichSteps[key] = [];
    enrichSteps[key].push(event);
    // If the lead row already exists (shouldn't normally happen), append step live
    const idx = enrichRowIdx[key];
    if (idx != null) {
      const stepBody = document.getElementById(`steps-tbody-${idx}`);
      if (stepBody) stepBody.appendChild(buildStepRow(event));
    }
    return;
  }

  if (event.type === 'lead') {
    document.getElementById('enrich-table-wrap').classList.remove('hidden');
    const tbody = document.getElementById('enrich-table-body');
    const key = event.business_name || '';
    const idx = enrichRowCount++;
    enrichRowIdx[key] = idx;

    const statusClass = `enrich-row-${event.status}`;
    const statusLabel = {
      enriched:  '✓ enriched',
      no_dm:     '~ no DM',
      no_result: '~ no result',
      failed:    '✗ failed',
    }[event.status] || event.status;

    const steps = enrichSteps[key] || [];

    // Main lead row
    const tr = document.createElement('tr');
    tr.innerHTML = `
      <td style="width:20px;padding-right:0">
        <button class="expand-btn" id="expand-btn-${idx}" onclick="toggleSteps(${idx})" title="Show step trace">▶</button>
      </td>
      <td style="color:var(--text);max-width:150px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap">${escHtml(key)}</td>
      <td style="max-width:120px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap">${escHtml(event.dm_name || '')}</td>
      <td style="max-width:150px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap">${escHtml(event.email || '')}</td>
      <td>${event.icp_score != null ? event.icp_score : ''}</td>
      <td class="${statusClass}" style="font-family:ui-monospace,monospace;font-size:12px">${statusLabel}</td>`;
    tbody.appendChild(tr);

    // Hidden step sub-row
    const subTr = document.createElement('tr');
    subTr.className = 'step-sub-row';
    subTr.id = `steps-row-${idx}`;
    subTr.style.display = 'none';
    subTr.innerHTML = `
      <td colspan="6">
        <div class="step-trace">
          <table>
            <thead>
              <tr>
                <th>Step</th>
                <th>Result</th>
                <th>Found / Notes</th>
                <th>Brave Queries</th>
                <th>GPT Turns</th>
                <th>Duration</th>
                <th>Cost</th>
              </tr>
            </thead>
            <tbody id="steps-tbody-${idx}"></tbody>
          </table>
        </div>
      </td>`;
    tbody.appendChild(subTr);

    // Populate buffered steps
    const stepBody = document.getElementById(`steps-tbody-${idx}`);
    steps.forEach(s => stepBody.appendChild(buildStepRow(s)));

    tbody.scrollIntoView({ behavior: 'smooth', block: 'nearest' });
    return;
  }

  if (event.type === 'done') {
    document.getElementById('enrich-summary-stats').classList.remove('hidden');
    document.getElementById('enrich-stat-total').textContent    = event.total    ?? 0;
    document.getElementById('enrich-stat-enriched').textContent = event.enriched ?? 0;
    document.getElementById('enrich-stat-no-dm').textContent    = event.dead     ?? 0;
    document.getElementById('enrich-stat-failed').textContent   = event.failed   ?? 0;
    const msg = event.dry_run
      ? `Dry run — ${event.total} lead(s) queued. Remove dry run to start enrichment.`
      : `Done. ${event.enriched} enriched · ${event.dead} no DM · ${event.failed} failed.`;
    appendEnrichLog(msg, event.dry_run ? 'info' : 'success');
    showSupabaseLink();
  }
}

function buildStepRow(s) {
  const tr = document.createElement('tr');
  const icon = s.success ? '<span class="step-ok">✓</span>' : '<span class="step-fail">✗</span>';
  const queries = (s.brave_queries || []).map(q =>
    `<span class="brave-query">${escHtml(q)}</span>`
  ).join('');
  const turns = s.gpt_turns != null ? `${s.gpt_turns} turn${s.gpt_turns !== 1 ? 's' : ''}` : '—';
  const dur   = s.duration_sec != null ? `${s.duration_sec}s` : '—';
  const cost  = s.cost_usd ? `$${s.cost_usd.toFixed(4)}` : '—';
  const notes = escHtml((s.found || s.notes || s.error_code || '').slice(0, 120));
  tr.innerHTML = `
    <td class="step-name">${escHtml(s.step.replace(/_/g,' '))}</td>
    <td>${icon}</td>
    <td style="max-width:200px">${notes}</td>
    <td style="max-width:260px;word-break:break-word">${queries || '<span style="color:rgba(255,255,255,0.15)">—</span>'}</td>
    <td style="white-space:nowrap">${turns}</td>
    <td style="white-space:nowrap">${dur}</td>
    <td style="white-space:nowrap">${cost}</td>`;
  return tr;
}

function toggleSteps(idx) {
  const row = document.getElementById(`steps-row-${idx}`);
  const btn = document.getElementById(`expand-btn-${idx}`);
  const open = row.style.display !== 'none';
  row.style.display = open ? 'none' : '';
  btn.textContent = open ? '▶' : '▼';
}

async function showSupabaseLink() {
  try {
    const res = await fetch('/api/supabase-url');
    const data = await res.json();
    if (data.url) {
      const wrap = document.getElementById('enrich-supabase-wrap');
      document.getElementById('enrich-supabase-btn').href = data.url;
      wrap.style.display = 'flex';
      wrap.classList.remove('hidden');
    }
  } catch {}
}

function appendEnrichLog(text, kind) {
  const el = document.getElementById('enrich-log-output');
  const line = document.createElement('div');
  line.className = `log-line log-${kind}`;
  line.textContent = text;
  el.appendChild(line);
  el.scrollTop = el.scrollHeight;
}

function resetEnrichBtn() {
  const btn = document.getElementById('enrich-run-btn');
  btn.disabled = false;
  btn.innerHTML = '<svg width="12" height="12" fill="none" viewBox="0 0 24 24" stroke="currentColor"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2.5" d="M5 3l14 9-14 9V3z"/></svg> Run Enrichment';
}
</script>
</body>
</html>"""


# ---------------------------------------------------------------------------
# Flask app factory
# ---------------------------------------------------------------------------

def create_app(supabase: "Client") -> "Flask":
    try:
        from flask import Flask, Response, jsonify, request, stream_with_context
    except ImportError:
        raise ImportError(
            "Flask is required for the web UI.\n"
            "Run: pip install flask"
        )

    app = Flask(__name__)
    app.config["MAX_CONTENT_LENGTH"] = 50 * 1024 * 1024  # 50 MB upload limit

    _jobs: dict[str, dict] = {}
    _enrich_jobs: dict[str, dict] = {}

    # ------------------------------------------------------------------
    # Static pages
    # ------------------------------------------------------------------

    @app.route("/")
    def index():
        return _HTML

    @app.route("/logo")
    def serve_logo():
        from flask import send_file
        logo_path = Path(__file__).parent / "static_logo.jpg"
        if logo_path.exists():
            return send_file(str(logo_path), mimetype="image/jpeg")
        return "", 404

    # ------------------------------------------------------------------
    # Organisations
    # ------------------------------------------------------------------

    @app.route("/api/organisations")
    def list_organisations():
        result = supabase.table("organisations").select("id, name").order("name").execute()
        return jsonify(result.data or [])

    # ------------------------------------------------------------------
    # Campaigns
    # ------------------------------------------------------------------

    @app.route("/api/campaigns")
    def list_campaigns():
        result = (
            supabase.table("campaigns")
            .select("id, name, org_id, status, vertical, target_city")
            .order("name")
            .execute()
        )
        campaigns = result.data or []
        # Annotate each campaign with queued lead count
        for c in campaigns:
            try:
                r = (
                    supabase.table("leads")
                    .select("id", count="exact")
                    .eq("campaign_id", c["id"])
                    .eq("enrichment_status", "queued")
                    .lt("retry_count", 3)
                    .execute()
                )
                c["queued_count"] = r.count or 0
            except Exception:
                c["queued_count"] = None
        return jsonify(campaigns)

    @app.route("/api/campaigns", methods=["POST"])
    def create_campaign():
        data = request.get_json(force=True)
        name = (data.get("name") or "").strip()
        org_id = (data.get("org_id") or "").strip()
        vertical = (data.get("vertical") or None)
        target_city = (data.get("target_city") or None)

        if not name:
            return jsonify({"error": "name is required"}), 400
        if not org_id:
            return jsonify({"error": "org_id is required"}), 400

        try:
            result = (
                supabase.table("campaigns")
                .insert({
                    "name": name,
                    "org_id": org_id,
                    **({"vertical": vertical} if vertical else {}),
                    **({"target_city": target_city} if target_city else {}),
                })
                .execute()
            )
            return jsonify(result.data[0]), 201
        except Exception as e:
            return jsonify({"error": str(e)}), 500

    @app.route("/api/campaigns/<campaign_id>", methods=["DELETE"])
    def archive_campaign(campaign_id: str):
        try:
            supabase.table("campaigns").update({"status": "archived"}).eq("id", campaign_id).execute()
            return jsonify({"ok": True})
        except Exception as e:
            return jsonify({"error": str(e)}), 500

    # ------------------------------------------------------------------
    # Supabase dashboard URL
    # ------------------------------------------------------------------

    @app.route("/api/supabase-url")
    def supabase_url_endpoint():
        import re as _re
        raw_url = os.environ.get("SUPABASE_URL", "")
        m = _re.match(r"https://([a-z0-9]+)\.supabase\.co", raw_url)
        if m:
            ref = m.group(1)
            url = f"https://supabase.com/dashboard/project/{ref}/editor"
            return jsonify({"url": url, "ref": ref})
        return jsonify({"url": None})

    # ------------------------------------------------------------------
    # Apify preflight check
    # ------------------------------------------------------------------

    @app.route("/api/apify-check")
    def apify_check():
        from agents.ingestion.agent import _check_apify_key
        api_key = os.environ.get("APIFY_API_KEY", "")
        ok, msg = _check_apify_key(api_key)
        return jsonify({"ok": ok, "message": msg})

    # ------------------------------------------------------------------
    # Service health preflight
    # ------------------------------------------------------------------

    @app.route("/api/preflight")
    def run_preflight_check():
        from agents.quota import (
            _check_anthropic, _check_supabase, _check_openai,
            _check_brave, _check_hunter, _check_apify,
            check_apify_credits, get_hunter_quota_status,
            CreditsLowError, has_blocking_failures,
        )
        campaign_id = request.args.get("campaign_id", "")

        # --- resolve campaign metadata ---
        org_id = None
        queued_leads = 0
        if campaign_id:
            try:
                r = supabase.table("campaigns").select("org_id").eq("id", campaign_id).single().execute()
                org_id = (r.data or {}).get("org_id")
            except Exception:
                pass
            try:
                r2 = (
                    supabase.table("leads")
                    .select("id", count="exact")
                    .eq("campaign_id", campaign_id)
                    .eq("enrichment_status", "queued")
                    .lt("retry_count", 3)
                    .execute()
                )
                queued_leads = r2.count or 0
            except Exception:
                pass

        # Estimated Brave Search calls per lead:
        # 3 steps (web_search, company_email, personal_email) × 3 agents × ~2 calls avg + 1 FB search ≈ 19
        BRAVE_CALLS_PER_LEAD = 19
        HUNTER_CALLS_PER_LEAD = 1
        APIFY_COST_PER_LEAD = 0.01  # USD, Facebook scrape

        checks = []

        ok, msg = _check_supabase(
            os.environ.get("SUPABASE_URL", ""),
            os.environ.get("SUPABASE_SERVICE_ROLE_KEY", ""),
        )
        checks.append({"name": "Supabase", "ok": ok, "message": msg, "blocking": True})

        ok, msg = _check_anthropic(os.environ.get("ANTHROPIC_API_KEY", ""))
        checks.append({"name": "Anthropic", "ok": ok, "message": msg, "blocking": True})

        ok, msg = _check_openai(os.environ.get("OPENAI_API_KEY", ""))
        checks.append({"name": "OpenAI", "ok": ok, "message": msg, "blocking": True})

        ok, msg = _check_brave(os.environ.get("BRAVE_API_KEY", ""))
        brave_warning = None
        if ok and queued_leads:
            est = queued_leads * BRAVE_CALLS_PER_LEAD
            brave_warning = (
                f"This batch needs ~{est:,} calls ({queued_leads} leads × ~{BRAVE_CALLS_PER_LEAD}). "
                f"Brave free tier is 2,000/month — recharge if you're close to the limit."
            )
        checks.append({"name": "Brave Search", "ok": ok, "message": msg, "blocking": True, "warning": brave_warning})

        ok, msg = _check_hunter(os.environ.get("HUNTER_IO_API_KEY", ""))
        hunter_warning = None
        if org_id:
            try:
                q = get_hunter_quota_status(supabase, org_id)
                used, limit = q.get("used", 0), q.get("limit", 100)
                remaining = limit - used
                msg += f"  ({used}/{limit} used this month)"
                if ok and queued_leads:
                    needed = queued_leads * HUNTER_CALLS_PER_LEAD
                    if needed > remaining:
                        hunter_warning = (
                            f"Only {remaining} lookups left — quota will run out after ~{remaining} of "
                            f"{queued_leads} leads. Top up or Hunter steps will be skipped."
                        )
                    else:
                        hunter_warning = f"{remaining} lookups remaining — enough for all {queued_leads} leads."
            except Exception:
                pass
        checks.append({"name": "Hunter.io", "ok": ok, "message": msg, "blocking": False, "warning": hunter_warning})

        ok, msg = _check_apify(os.environ.get("APIFY_API_KEY", ""))
        apify_warning = None
        credits = None
        if ok:
            try:
                credits = check_apify_credits()
                if credits < 0:
                    msg = "valid  (credits not reported — free plan)"
                else:
                    msg = f"valid  ({credits:.0f} credits remaining)"
                    if queued_leads:
                        est_cost = queued_leads * APIFY_COST_PER_LEAD
                        # Apify credits ≈ USD (roughly $1 = 1 credit on starter)
                        if est_cost > credits:
                            apify_warning = (
                                f"Facebook step needs ~${est_cost:.2f} in credits for {queued_leads} leads "
                                f"but only {credits:.0f} credits remain. Top up at console.apify.com/billing."
                            )
                        else:
                            apify_warning = (
                                f"Facebook step estimated ~${est_cost:.2f} for {queued_leads} leads "
                                f"({credits:.0f} credits remaining — sufficient)."
                            )
            except CreditsLowError as e:
                ok = False
                msg = str(e)
            except Exception:
                msg = "valid (credit check unavailable)"
        checks.append({"name": "Apify", "ok": ok, "message": msg, "blocking": False, "warning": apify_warning})

        return jsonify({
            "checks": checks,
            "has_blockers": has_blocking_failures(checks),
            "queued_leads": queued_leads,
        })

    # ------------------------------------------------------------------
    # CSV upload
    # ------------------------------------------------------------------

    @app.route("/api/upload-csv", methods=["POST"])
    def upload_csv():
        if "file" not in request.files:
            return jsonify({"error": "no file provided"}), 400
        f = request.files["file"]
        suffix = Path(f.filename).suffix.lower() if f.filename else ""
        if suffix not in (".csv", ".xlsx", ".xls", ".tsv", ".txt"):
            return jsonify({"error": f"Unsupported file type '{suffix}'. Use .csv, .xlsx, or .xls"}), 400

        tmp = tempfile.NamedTemporaryFile(delete=False, suffix=suffix)
        f.save(tmp.name)
        tmp.close()

        try:
            from agents.ingestion.sources import analyse_file
            analysis = analyse_file(tmp.name)
        except Exception:
            analysis = {"columns": [], "sample_values": {}, "suggested_mapping": {}}

        return jsonify({"path": tmp.name, "filename": f.filename, **analysis})

    # ------------------------------------------------------------------
    # Ingestion
    # ------------------------------------------------------------------

    @app.route("/api/ingest", methods=["POST"])
    def start_ingest():
        data = request.get_json(force=True)
        job_id = str(uuid.uuid4())
        q: queue.Queue = queue.Queue()
        _jobs[job_id] = {"queue": q, "done": False}

        def run() -> None:
            try:
                _do_ingest(supabase, data, q)
            except Exception as exc:
                q.put({"type": "error", "text": f"Unexpected error: {exc}"})
            finally:
                _jobs[job_id]["done"] = True

        t = threading.Thread(target=run, daemon=True)
        t.start()
        _jobs[job_id]["thread"] = t
        return jsonify({"job_id": job_id})

    @app.route("/api/ingest/<job_id>/stream")
    def stream_ingest(job_id: str):
        if job_id not in _jobs:
            return jsonify({"error": "job not found"}), 404

        job = _jobs[job_id]

        def generate():
            while True:
                try:
                    event = job["queue"].get(timeout=0.4)
                    yield f"data: {json.dumps(event)}\n\n"
                    if event.get("type") == "done":
                        break
                except queue.Empty:
                    if job["done"]:
                        break
                    yield f"data: {json.dumps({'type': 'ping'})}\n\n"

        return Response(
            stream_with_context(generate()),
            mimetype="text/event-stream",
            headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
        )

    # ------------------------------------------------------------------
    # Enrichment
    # ------------------------------------------------------------------

    @app.route("/api/enrich", methods=["POST"])
    def start_enrich():
        data = request.get_json(force=True)
        job_id = str(uuid.uuid4())
        q: queue.Queue = queue.Queue()
        _enrich_jobs[job_id] = {"queue": q, "done": False}

        def run() -> None:
            try:
                _do_enrich(supabase, data, q)
            except Exception as exc:
                q.put({"type": "error", "text": f"Unexpected error: {exc}"})
            finally:
                _enrich_jobs[job_id]["done"] = True

        t = threading.Thread(target=run, daemon=True)
        t.start()
        _enrich_jobs[job_id]["thread"] = t
        return jsonify({"job_id": job_id})

    @app.route("/api/enrich/<job_id>/stream")
    def stream_enrich(job_id: str):
        if job_id not in _enrich_jobs:
            return jsonify({"error": "job not found"}), 404

        job = _enrich_jobs[job_id]

        def generate():
            while True:
                try:
                    event = job["queue"].get(timeout=0.4)
                    yield f"data: {json.dumps(event)}\n\n"
                    if event.get("type") == "done":
                        break
                except queue.Empty:
                    if job["done"]:
                        break
                    yield f"data: {json.dumps({'type': 'ping'})}\n\n"

        return Response(
            stream_with_context(generate()),
            mimetype="text/event-stream",
            headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
        )

    return app


# ---------------------------------------------------------------------------
# Core ingestion logic (queue-aware, no Rich console)
# ---------------------------------------------------------------------------

def _do_ingest(supabase: "Client", data: dict, q: queue.Queue) -> None:
    from agents.ingestion.agent import _check_apify_key, _verify_campaign
    from agents.ingestion.sources import (
        ACTOR_IDS,
        fetch_facebook,
        fetch_google_maps,
        fetch_linkedin_company,
        fetch_linkedin_profile,
        read_file,
    )

    source = data.get("source", "")
    campaign_id = data.get("campaign_id", "")
    dry_run = bool(data.get("dry_run", True))

    def log(text: str) -> None:
        q.put({"type": "log", "text": text})

    def err(text: str) -> None:
        q.put({"type": "error", "text": text})

    # 1. Campaign preflight
    try:
        org_id, campaign_name = _verify_campaign(supabase, campaign_id)
        log(f"Campaign: {campaign_name}")
    except ValueError as e:
        err(str(e))
        q.put({"type": "done", "total": 0, "inserted": 0, "skipped": 0, "dry_run": dry_run})
        return

    # 2. Apify preflight (skip for CSV)
    if source != "csv":
        api_key = os.environ.get("APIFY_API_KEY", "")
        ok, msg = _check_apify_key(api_key)
        if not ok:
            err(f"Apify key: {msg}")
            q.put({"type": "done", "total": 0, "inserted": 0, "skipped": 0, "dry_run": dry_run})
            return
        log("Apify key: valid")

    # 3. Fetch leads
    log(f"Fetching leads from {source}…")
    try:
        if source == "google_maps":
            records = fetch_google_maps(data["keyword"], data["location"], int(data.get("limit", 50)))
        elif source == "linkedin_company":
            records = fetch_linkedin_company(data["query"], int(data.get("limit", 50)))
        elif source == "linkedin_profile":
            records = fetch_linkedin_profile(data["query"], int(data.get("limit", 50)))
        elif source == "facebook":
            records = fetch_facebook(data["query"], int(data.get("limit", 50)))
        elif source == "csv":
            records = read_file(data["file_path"], data.get("column_mapping") or None)
        else:
            err(f"Unknown source: {source}")
            q.put({"type": "done", "total": 0, "inserted": 0, "skipped": 0, "dry_run": dry_run})
            return
    except Exception as e:
        err(str(e))
        q.put({"type": "done", "total": 0, "inserted": 0, "skipped": 0, "dry_run": dry_run})
        return

    valid = [r for r in records if r.business_name]
    invalid_count = len(records) - len(valid)
    log(f"Fetched {len(records)} leads" + (f" ({invalid_count} skipped — no business name)" if invalid_count else ""))

    if not valid:
        log("No valid leads to ingest.")
        q.put({"type": "done", "total": 0, "inserted": 0, "skipped": 0, "dry_run": dry_run})
        return

    # 4. Dry run — preview only
    if dry_run:
        log(f"[DRY RUN] Would write {len(valid)} lead(s) to Supabase — nothing written.")
        for r in valid:
            q.put({"type": "lead", "business_name": r.business_name,
                   "location": r.location or "", "phone": r.phone or "",
                   "website": r.website or "", "status": "preview"})
        q.put({"type": "done", "total": len(valid), "inserted": 0, "skipped": 0, "dry_run": True})
        return

    # 5. Upsert to Supabase
    log(f"Writing {len(valid)} leads to Supabase…")
    inserted = skipped = errors = 0
    batch_size = 50

    for i in range(0, len(valid), batch_size):
        batch = valid[i : i + batch_size]
        rows = [r.to_db_row(org_id, campaign_id) for r in batch]
        try:
            resp = (
                supabase.table("leads")
                .upsert(rows, on_conflict="campaign_id,business_name,location", ignore_duplicates=True)
                .execute()
            )
            batch_inserted = len(resp.data) if resp.data else 0
            inserted_names = {r["business_name"] for r in (resp.data or [])}
            inserted += batch_inserted
            skipped += len(batch) - batch_inserted
            for record in batch:
                status = "inserted" if record.business_name in inserted_names else "duplicate"
                q.put({"type": "lead", "business_name": record.business_name,
                       "location": record.location or "", "phone": record.phone or "",
                       "website": record.website or "", "status": status})
        except Exception as e:
            errors += len(batch)
            err(f"Batch {i // batch_size + 1} failed: {e}")

    q.put({"type": "done", "total": len(valid), "inserted": inserted, "skipped": skipped,
           "errors": errors, "dry_run": False})


# ---------------------------------------------------------------------------
# Core enrichment logic (queue-aware)
# ---------------------------------------------------------------------------

def _do_enrich(supabase: "Client", data: dict, q: queue.Queue) -> None:
    import anthropic as _anthropic
    from openai import OpenAI as _OpenAI
    from agents.config import load_campaign_config
    from agents.enrichment.agent import run_batch

    campaign_id   = data.get("campaign_id", "")
    website_filter = data.get("website_filter", "all")
    dry_run       = bool(data.get("dry_run", True))
    limit         = data.get("limit") or None

    def log(text: str) -> None:
        q.put({"type": "log", "text": text})

    def err(text: str) -> None:
        q.put({"type": "error", "text": text})

    if not campaign_id:
        err("No campaign selected")
        q.put({"type": "done", "total": 0, "enriched": 0, "failed": 0, "dead": 0, "dry_run": dry_run})
        return

    try:
        cfg = load_campaign_config(supabase, campaign_id)
    except ValueError as e:
        err(str(e))
        q.put({"type": "done", "total": 0, "enriched": 0, "failed": 0, "dead": 0, "dry_run": dry_run})
        return

    # Apply website filter onto config
    if website_filter == "has_website":
        cfg.lead_filter.has_website = True
    elif website_filter == "no_website":
        cfg.lead_filter.has_website = False
    else:
        cfg.lead_filter.has_website = None

    anthropic_key = os.environ.get("ANTHROPIC_API_KEY", "")
    openai_key    = os.environ.get("OPENAI_API_KEY", "")

    if not anthropic_key:
        err("ANTHROPIC_API_KEY not set")
        q.put({"type": "done", "total": 0, "enriched": 0, "failed": 0, "dead": 0, "dry_run": dry_run})
        return
    if not openai_key:
        err("OPENAI_API_KEY not set")
        q.put({"type": "done", "total": 0, "enriched": 0, "failed": 0, "dead": 0, "dry_run": dry_run})
        return

    anthropic_client = _anthropic.Anthropic(api_key=anthropic_key)
    openai_client    = _OpenAI(api_key=openai_key)

    log(f"Starting enrichment for {cfg.campaign_name}…")

    asyncio.run(
        run_batch(
            supabase=supabase,
            config=cfg,
            anthropic_client=anthropic_client,
            openai_client=openai_client,
            dry_run=dry_run,
            limit=int(limit) if limit else None,
            headless=True,
            progress_queue=q,
        )
    )


# ---------------------------------------------------------------------------
# Launch helper
# ---------------------------------------------------------------------------

def launch(supabase: "Client") -> None:
    """Start the Flask dev server and open the browser."""
    import logging as _logging
    _logging.getLogger("werkzeug").setLevel(_logging.WARNING)

    app = create_app(supabase)

    def open_browser() -> None:
        import time as _time
        _time.sleep(0.8)
        webbrowser.open(f"http://localhost:{_PORT}")

    threading.Thread(target=open_browser, daemon=True).start()

    print(f"\n  Titan Systems — Lead Ingestion UI")
    print(f"  Open: http://localhost:{_PORT}")
    print(f"  Press Ctrl+C to exit.\n")

    app.run(host="0.0.0.0", port=_PORT, debug=False, use_reloader=False, threaded=True)
