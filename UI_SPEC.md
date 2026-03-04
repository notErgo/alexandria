# Miners Platform — UI/API Component Spec

Version: 1.1
Port: 5004

## Convention

| Level | Format | Meaning |
|-------|--------|---------|
| `X.0` | integer.0 | Page (route) |
| `X.Y` | integer.integer | Tab or major section within a page |
| `X.Y.Z` | integer.integer.integer | Panel, table, or form within a section |

Data source key:
- **CONFIG** — seeded from `companies.json`, pattern files, or `config.py` at server startup; survives purge + restart by design
- **DATA** — created by scraping/extraction pipeline; cleared by Purge All
- **n/a** — stateless UI element (filter, button, form)

---

## 1.0  `/`  — Landing Page

Template: `landing.html`

| ID    | Component         | Source  | API endpoint(s) | Script(s) |
|-------|-------------------|---------|-----------------|-----------|
| 1.1   | Scorecard table   | DATA    | `GET /api/scorecard` | — |

---

## 2.0  `/ops`  — Ops Page

Template: `ops.html`

### 2.1  Companies tab  (`/ops?tab=companies`)

| ID    | Component              | Source  | API endpoint(s) | Script(s) |
|-------|------------------------|---------|-----------------|-----------|
| 2.1.1 | Companies table        | **CONFIG** | `GET /api/companies` | `sync_companies_from_config()` in `db.py`; auto-runs at boot from `companies.json`. Rows always present after restart. Status + Last Scrape columns are DATA — reset by purge. |
| 2.1.2 | Regime editor panel    | CONFIG  | `GET /api/regime/<ticker>` · `POST /api/regime/<ticker>` · `DELETE /api/regime/<ticker>/<id>` | — |
| 2.1.3 | Scrape Queue table     | DATA    | `GET /api/scrape/queue` | `ScrapeWorker` thread (`run_web.py`); `IRScraper` (`scrapers/ir_scraper.py`) |
| 2.1.4 | Danger Zone purge form | n/a     | `POST /api/data/purge` | `db.purge_all()` in `db.py` |
| 2.1.5 | Add Company form       | n/a     | `POST /api/companies` | — |
| 2.1.6 | Sync Config button     | n/a     | `POST /api/companies/sync` | `db.sync_companies_from_config()` |
| 2.1.7 | Scrape trigger button  | n/a     | `POST /api/scrape/trigger/<ticker>` | Enqueues job → `ScrapeWorker` |

**Note on 2.1.1:** Companies rows reappear after every server restart because `_init_db()` always calls `sync_companies_from_config()`. This is CONFIG seeding, not data. After purge + restart, expect rows present but Status = `never_run` and Last Scrape = blank.

### 2.2  Registry tab  (`/ops?tab=registry`)

| ID    | Component       | Source | API endpoint(s) | Script(s) |
|-------|-----------------|--------|-----------------|-----------|
| 2.2.1 | Filter bar      | n/a    | — | — |
| 2.2.2 | Registry table  | DATA   | `GET /api/registry` | `ManifestScanner` (`scrapers/manifest_scanner.py`); `POST /api/manifest/scan` to trigger |

### 2.3  Explorer tab  (`/ops?tab=explorer`)

| ID    | Component           | Source | API endpoint(s) | Script(s) |
|-------|---------------------|--------|-----------------|-----------|
| 2.3.1 | Filter bar          | n/a    | — | — |
| 2.3.2 | Coverage heatmap    | DATA   | `GET /api/explorer/grid` | `coverage_logic.py` (pure functions) |
| 2.3.3 | Cell detail panel   | DATA   | `GET /api/explorer/cell/<ticker>/<period>/<metric>` | — |
| 2.3.4 | Cell save action    | n/a    | `POST /api/explorer/cell/.../save` | `db.save_analyst_edit()` |
| 2.3.5 | Cell gap action     | n/a    | `POST /api/explorer/cell/.../gap` | `db.mark_analyst_gap()` |
| 2.3.6 | Re-extract action   | n/a    | `POST /api/explorer/reextract` | `extraction_pipeline.extract_report()` |

### 2.4  Metric Rules tab  (`/ops?tab=rules`)

| ID    | Component     | Source | API endpoint(s) | Script(s) |
|-------|---------------|--------|-----------------|-----------|
| 2.4.1 | Rules table   | CONFIG | `GET /api/metric_schema` | Seeded from `config.py` via `db._seed_metric_rules()` |

### 2.5  Review Queue tab

Redirects to 3.0.

---

## 3.0  `/review`  — Review Queue Page

Template: `review.html`

| ID    | Component        | Source | API endpoint(s) | Script(s) |
|-------|------------------|--------|-----------------|-----------|
| 3.1   | Filter bar       | n/a    | — | — |
| 3.2   | Review table     | DATA   | `GET /api/review` | `AgreementEngine` (`extractors/agreement.py`) populates via `extraction_pipeline.py` |
| 3.3   | Doc panel        | DATA   | `GET /api/review/<id>/source` | `doc_panel.js` |
| 3.4   | Approve action   | n/a    | `POST /api/review/<id>/approve` | `db.approve_review_item()` |
| 3.5   | Reject action    | n/a    | `POST /api/review/<id>/reject` | `db.reject_review_item()` |
| 3.6   | Re-extract action | n/a   | `POST /api/review/<id>/reextract` | `extraction_pipeline.extract_report()` |
| 3.7   | Bulk approve     | n/a    | `POST /api/review/bulk_approve` | — |

---

## 4.0  `/data-explorer`  — Data Explorer Page

Template: `data_explorer.html` (or equivalent)

| ID    | Component          | Source | API endpoint(s) | Script(s) |
|-------|--------------------|--------|-----------------|-----------|
| 4.1   | Filter bar         | n/a    | — | — |
| 4.2   | Data points table  | DATA   | `GET /api/data` | `db.query_data_points()` |
| 4.3   | Export button      | n/a    | `GET /api/export.csv` | — |
| 4.4   | Lineage panel      | DATA   | `GET /api/data/lineage` | — |

---

## 5.0  `/miner-data`  — Miner Data Page

| ID    | Component     | Source | API endpoint(s) | Script(s) |
|-------|---------------|--------|-----------------|-----------|
| 5.1   | Reports table | DATA   | `GET /api/reports` (if exposed) | `IRScraper`, `ArchiveIngestor`, `EdgarConnector` write to `reports` table |
| 5.2   | Doc panel     | DATA   | `GET /api/operations/manifest/<id>/preview` | `doc_panel.js` |

---

## 6.0  `/dashboard`  — Dashboard Page

| ID    | Component       | Source | API endpoint(s) | Script(s) |
|-------|-----------------|--------|-----------------|-----------|
| 6.1   | Metric panels   | DATA   | `GET /api/data` (aggregated) | — |

---

## 7.0  `/patterns`  — Patterns Page

| ID    | Component      | Source | API endpoint(s) | Script(s) |
|-------|----------------|--------|-----------------|-----------|
| 7.1   | Patterns table | CONFIG | (internal)      | `PatternRegistry` (`extractors/pattern_registry.py`) loads from `config/patterns/*.json` |

---

## 8.0  `/diagnostics`  — Diagnostics Page

| ID    | Component        | Source | API endpoint(s) | Script(s) |
|-------|------------------|--------|-----------------|-----------|
| 8.1   | Diagnostic output | DATA  | (internal)      | `cli.py diagnose --ticker` |

---

## 9.0  `/company/<ticker>`  — Company Detail Page

| ID    | Component            | Source | API endpoint(s) | Script(s) |
|-------|----------------------|--------|-----------------|-----------|
| 9.1   | Company header       | CONFIG | `GET /api/companies/<ticker>` | — |
| 9.2   | Metric history charts | DATA  | `GET /api/data?ticker=X` | — |
| 9.3   | Reports list         | DATA   | `GET /api/reports?ticker=X` | — |

---

## Background scripts and workers

| ID   | Name             | Trigger | What it does | Writes to |
|------|------------------|---------|--------------|-----------|
| S.1  | `ScrapeWorker`   | Startup (daemon thread); `POST /api/scrape/trigger/<ticker>` | Polls `scrape_queue`, runs `IRScraper` per job, auto-triggers extraction | `reports`, `scrape_queue` |
| S.2  | `IRScraper`      | Called by S.1 | Fetches IR press releases (RSS/index/template mode) | `reports` |
| S.3  | `ArchiveIngestor` | `POST /api/ingest/archive`; `cli.py ingest --source archive` | Walks `OffChain/Miner/` archive, parses PDFs/HTMLs | `reports`, `asset_manifest` |
| S.4  | `EdgarConnector` | `POST /api/ingest/edgar` | Fetches EDGAR 8-K filings | `reports` |
| S.5  | `ManifestScanner` | `POST /api/manifest/scan` | Walks archive directory, upserts manifest entries | `asset_manifest` |
| S.6  | Extraction pipeline | `POST /api/operations/extract`; `cli.py extract` | Runs LLM+regex+agreement on stored reports | `data_points`, `review_queue` |

---

## Purge scope summary

| Cleared by Purge All (2.1.4) | NOT cleared |
|------------------------------|-------------|
| `reports` | `companies` rows (CONFIG — re-seeded at boot) |
| `data_points` | `regime_config` |
| `review_queue` | `llm_prompts` |
| `scrape_queue` (2.1.3) | `llm_ticker_hints` |
| `asset_manifest` (2.2.2) | `metric_schema` |
| `document_chunks` | `config_settings` |
| `raw_extractions` | `patterns` |
| `btc_loans` | `metric_rules` |
| `facilities` | |
| `source_audit` | |
| `llm_benchmark_runs` | |
| companies operational fields → `never_run` / NULL | |
