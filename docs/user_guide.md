# Miners Platform — Operator User Guide

**Last Updated:** 2026-03-13
**Audience:** Analysts and operators running the platform at localhost:5004

---

## 1. Platform Overview

The platform has four tabs under **Ops** (`/ops`):

| Tab | Purpose |
|-----|---------|
| **Companies** | Configure scrapers, EDGAR anchors, regime windows, trigger scrape jobs |
| **Registry** | Browse all ingested documents (asset_manifest) |
| **Explorer** | Coverage heatmap, cell detail, inline edit, re-extract |
| **Review** | Approve/reject/edit LLM candidates in the review queue |

The home page (`/`) shows the sector scorecard — one latest value per metric per company.

---

## 2. Button Reference

### Acquisition buttons (top of Companies tab)

| Button | What it does |
|--------|-------------|
| **Acquire Archive** | Walks `OffChain/Miner/Miner Monthly/`, parses PDFs/HTMLs, inserts into `reports`. Runs extraction inline for monthly archive files. Skips already-ingested files unless force is set. |
| **Acquire IR** | Fetches live IR press releases for all active companies using each company's `scraper_mode`. Stores raw text in `reports`. |
| **Acquire EDGAR** | Fetches 8-K/10-Q/10-K (or 6-K/20-F/40-F for foreign filers) from SEC EDGAR for all companies with a CIK. Deduplicates by accession number. Uses `btc_first_filing_date` as the earliest fetch date. |
| **Auto-extract checkbox** | When checked, IR and EDGAR acquisition automatically run the extraction stage on newly-ingested reports before marking the job complete. Saves a manual step. |
| **Start Extraction** | Runs LLM extraction on all unextracted reports (or one ticker if filtered). Writes agreed values to `data_points`; low-confidence or outlier results go to `review_queue`. |
| **Probe Targets** | Sends HTTP probes to IR URLs, checks RSS/index/template availability. Writes evidence to `source_audit`. Does not change modes. |
| **Probe + Apply Modes** | Same as Probe Targets, then auto-updates `scraper_mode` for companies where evidence supports it. |
| **Sync Config** | Re-reads `config/companies.json` and upserts all company rows. Run after editing that file directly. |
| **Scan Manifest** | Walks the archive directory and records every file in `asset_manifest`. Run after adding new PDFs/HTMLs to the archive folder before ingesting. |
| **EDGAR Bridge** | Fills monthly coverage gaps by carrying forward or inferring values from quarterly/annual EDGAR filings when no monthly press release exists. |

### Per-company controls (expand a row in Companies tab)

| Control | What it does |
|---------|-------------|
| **Reporting cadence** dropdown + Save | Sets `reporting_cadence` (monthly/quarterly/annual). Affects how the coverage grid interprets gaps. |
| **BTC mining pivot date** field + Save | Overrides `btc_first_filing_date` — the earliest date used as `since_date` for EDGAR ingestion. Set this earlier to recover historical filings. |
| **Detect** | Runs `detect_btc_first_filing_date()` — scans EDGAR for the earliest 8-K mentioning BTC keywords and sets the pivot date automatically. Only runs if not already set. |
| **Re-detect** | Forces re-detection even if a pivot date is already stored. Useful after adding new metric keywords. |
| **Trigger Scrape** | Enqueues a scrape job for this company (IR + EDGAR). Processed by the background ScrapeWorker within a few seconds. **Ingest only — does not trigger extraction.** Run Start Extraction separately after it completes. |

---

## 3. Standard Workflow: Full Refresh for One Company

Use this when you want to pull all available data for a ticker and extract it.

1. **Ops > Companies tab** — verify the company's `scraper_mode` is not `skip` and a CIK is present for EDGAR-eligible companies.
2. Check **BTC mining pivot date** — if blank or too recent, set it manually (see Section 4 below).
3. Tick **Auto-extract**.
4. Click **Acquire EDGAR** — fetches all 8-K/10-Q/10-K filings from pivot date forward.
5. Click **Acquire IR** — fetches latest press releases.
6. Click **Acquire Archive** — ingests any local PDF/HTML files in `OffChain/Miner/Miner Monthly/<TICKER>/`.
7. Monitor the progress log. When ingest completes, extraction runs automatically (if auto-extract was on).
8. Switch to **Explorer tab** — set ticker filter and check cell states. Cells should move from `missing` to `extracted` or `review_pending`.
9. Switch to **Review tab** — work through any queued items (J/K to navigate, A to approve, R to reject).

---

## 4. Recovering Historical Filings (Backfill)

Use this when a company's EDGAR history was fetched with a pivot date that was too recent, leaving older periods empty.

**Symptom:** Explorer shows `missing` cells for early periods even though the company was filing 8-Ks at that time.

**Example: MARA 2020 – April 2021**

MARA's `btc_first_filing_date` was set to `2021-05-12` by auto-detection, which cut off all filings before that date. Monthly production 8-Ks exist on EDGAR from around late 2020.

**Steps:**

1. **Ops > Companies tab** — find MARA, click to expand its row.
2. In the **"BTC mining pivot date"** field, type `2020-01-01`.
3. Click **Save** — this writes the override to `companies.btc_first_filing_date`.
4. Tick **Auto-extract** at the top of the page.
5. Click **Acquire EDGAR** — the fetch window now starts from `2020-01-01`.
6. Wait for the ingest + extraction job to complete (monitor the ops log).
7. Open **Explorer tab**, filter to MARA — early 2020/2021 cells should now populate.

**Notes:**
- Setting the pivot date to an earlier value is always safe — EDGAR deduplicates by accession number, so no duplicates are created.
- If no additional filings appear, MARA may not have filed BTC production 8-Ks before May 2021. Check the EDGAR EDGAR full-text search manually for CIK `0001507605`.
- Archive files (local PDFs) only go back to May 2021 — periods before that require EDGAR or manual file placement.

---

## 5. Adding Local Archive Files

If you have a PDF or HTML press release that is not in the archive folder:

1. Drop the file into `OffChain/Miner/Miner Monthly/<TICKER>/`.
2. Name it with the date prefix: `YYYY-MM-DD_<description>.pdf` or `.html`.
3. **Ops > Companies tab** — click **Scan Manifest** to register it in `asset_manifest`.
4. Click **Acquire Archive** — the new file is ingested.
5. Click **Start Extraction** (or use Auto-extract on the next acquisition run).

---

## 6. Explorer Tab: Reading Cell States

| State | Meaning |
|-------|---------|
| `extracted` | LLM extracted a value with confidence >= 0.75; written to `data_points` |
| `review_pending` | Value extracted but confidence < 0.75 or outlier detected; awaiting analyst decision |
| `missing` | No report ingested for this period |
| `keyword_gated` | Report exists but no mining keywords found; extraction skipped |
| `parse_failed` | Parser could not read the document |
| `llm_empty` | LLM returned no value for this metric from this report |
| `analyst` | Value was manually set or approved by an analyst; pipeline will not overwrite it |

Double-click any cell to edit the value inline. Use the **Re-extract** button to force the LLM to re-run on a selection.

---

## 7. Review Queue Keyboard Shortcuts

| Key | Action |
|-----|--------|
| `J` | Next item |
| `K` | Previous item |
| `A` | Approve current item |
| `R` | Reject current item |

Approved items are written to `data_points`. Rejected items are discarded. Both update the coverage state immediately.

---

## 8. Trigger Scrape vs Acquire EDGAR — When to Use Each

| | Trigger Scrape (per-company button) | Acquire EDGAR (top-level button) |
|--|--|--|
| Scope | Single company | All companies with a CIK |
| IR included | Yes | No (EDGAR only) |
| Auto-extract option | No — ingest only | Yes — checkbox available |
| Runs via | Background ScrapeWorker (async) | Inline task (polled by ops log) |
| Best for | Routine refresh; scheduled scrapes | Backfills; when you need extraction to run immediately after |

**Rule of thumb:** Use **Trigger Scrape** for day-to-day refreshes when you run extraction on a schedule. Use **Acquire EDGAR** (with Auto-extract checked) when you need to backfill history or want ingest + extraction in a single step.

---

## 9. Troubleshooting

### Extraction silently skips everything

Reports already marked `extraction_status = 'done'` are skipped by `get_unextracted_reports()`. This can happen if a prior run completed with the LLM down (regex-only pass still marks `done`).

**Fix:** On the Ops page, use the **Force re-extract** checkbox before clicking Start Extraction, or reset a specific report's status via the Registry tab.

### EDGAR returns no new filings

Check `btc_first_filing_date` — if it is set to a recent date, EDGAR will not look further back. Override it manually (see Section 4).

### Ollama not responding

If the extraction log shows `Ollama is not reachable`, the local model server is down. Start it with:
```bash
ollama serve
```
Then retry extraction. The platform does a warmup check before each extraction run and will return a 503 if the model is unreachable.

### Company shows `skip` mode

`scraper_mode = skip` means the IR scraper will not fetch for that company. This is usually set because the IR page is unreachable (502/503). Check the `scraper_issues_log` field in the Companies tab row. To re-enable: update `scraper_mode` to `rss`, `index`, or `template` and run **Probe Targets** to verify reachability.
