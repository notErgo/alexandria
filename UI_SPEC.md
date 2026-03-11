# Miners Platform — UI/API Component Spec

Version: 1.5
Port: 5004

## Convention

| Level | Format | Meaning |
|-------|--------|---------|
| `X.0` | integer.0 | Page (route) or top-level pane within ops.html |
| `X.Y` | integer.integer | Sub-tab or major section within a pane |
| `X.Y.Z` | integer.integer.integer | Panel, table, or form within a section |
| `X.Y.Z.W` | integer.integer.integer.integer | Sub-component within a panel |

**SSOT**: Machine-readable spec is `static/data/ui_spec.json`. `UI_SPEC.md` is the human-readable mirror. Tests in `tests/test_ui_spec.py` enforce that every `id` in `ui_spec.json` has a matching `data-spec-id` attribute in the template.

**Ops page namespace**: The `/ops` page uses a flat internal namespace where the top-level panes are `1.X` (Track Config), `2.X` (Ingest), `3.X` (Interpret), `4.X` (Review), `5.X` (Data), `6.X` (Interrogate). The `/` landing page uses `1.X`, all other pages use prefixed IDs (R3.X, DE4.X, MD5.X, DB6.X, 7.X, 8.X, 9.X).

**Critical path**: Each component has a `path` field in `ui_spec.json` (`critical` | `optional` | `later`). The ops.html template shows `wf-path-badge` labels on all sub-tab buttons and on individual cards in the Ingest pane. Off-critical-path cards within panes are wrapped in `<details class="offpath-details">` and collapsed by default.

Data source key:
- **CONFIG** — seeded from `companies.json` or `config.py` at server startup; survives purge + restart by design
- **DATA** — created by scraping/extraction pipeline; cleared by Purge All
- **n/a** — stateless UI element (filter, button, form)

## Canonical Derivation Requirement

Any UI component that lists companies or tickers must derive that list from the
canonical chain — never from a hardcoded local constant (AP-043 / CHK-046):

```
config/companies.json
  → config.py::get_all_tickers()
    → Python (scrapers, routes)
      → GET /api/companies
        → render_template(all_tickers=get_all_tickers())
          → {{ all_tickers | tojson }} in template JS
```

Adding a company to `companies.json` must be sufficient to make it appear in
every ticker-bearing component — companies table, ticker bars, dashboard
checkboxes, diagnostics, manifest scanner — with zero additional code changes.

---

## 1.0  `/`  — Landing Page

Template: `landing.html`

| ID    | Component         | Source  | API endpoint(s) | Script(s) |
|-------|-------------------|---------|-----------------|-----------|
| 1.1   | Scorecard table   | DATA    | `GET /api/scorecard` | — |

---

## 2.0  `/ops`  — Ops Page

Template: `ops.html`

**Track Config sub-tab layout (ops internal namespace):**

| Sub-tab | ops ID | Label | Path |
|---------|--------|-------|------|
| 1.1 | Companies | Add company, governance probe | Critical |
| 1.2 | Companies Table | Live table, scrape queue, danger zone | Critical |
| 1.3 | Metrics & Keywords | Metric SSOT, keyword editor | Optional |
| 1.4 | Settings | Runtime config, metric rules | Optional |
| 1.5 | Crawl Setup | LLM crawl config | Optional |

### 2.1  Companies tab  (`/ops?tab=companies`)

#### 1.1  Companies sub-tab

| ID    | Component | Source | API endpoint(s) | Script(s) |
|-------|-----------|--------|-----------------|-----------|
| 1.1.4 | Add Company form + bulk spreadsheet paste | n/a | `POST /api/companies` · `PUT /api/companies/<ticker>` · `GET /api/companies` | Single-add includes CIK, reporting_cadence, scraper mode. Bulk paste accepts TSV/CSV. Mode contract enforced at API: `rss -> rss_url`, `index -> ir_url`, `template -> url_template + pr_start_year`, `skip -> optional skip_reason` |
| 1.1.5 | Scraper governance + discovery probes | n/a | `GET /api/companies/scraper_governance` · `POST /api/companies/bootstrap_probe_all` · `POST /api/companies/<ticker>/bootstrap_probe` | Agent-proposed candidates probed deterministically, written to `source_audit`, produce governed mode recommendations |

#### 1.2  Companies Table sub-tab

| ID    | Component | Source | API endpoint(s) | Script(s) |
|-------|-----------|--------|-----------------|-----------|
| 1.2.1 | Companies table | **CONFIG** | `GET /api/companies` · `PUT /api/companies/<ticker>` | `sync_companies_from_config()` in `db.py` auto-runs at boot. Expandable detail row: reporting_cadence dropdown, BTC anchor date with Detect/Save/Clear. Sync Config = update-only when cleared; Restore from Config = full re-insert. |
| 1.2.2 | Scrape Queue table | DATA | `GET /api/scrape/queue` · `POST /api/scrape/trigger/<ticker>` | `ScrapeWorker` thread (`run_web.py`); `IRScraper` (`scrapers/ir_scraper.py`) |
| 1.2.3 | SCRAPE stage delete form | n/a | `POST /api/delete/scrape` | Deletes scraped sources and downstream layers. Keeps companies. |

**Companies table cleared-state contract:**
- `hard_delete` (full scope): clears companies, sets `config_settings.auto_sync_companies_on_startup='0'`
- `POST /api/companies/sync`: update-only when cleared (`insert_new=False`); returns `cleared_state: true` in response
- `POST /api/companies/sync/restore`: explicit operator action; calls `sync_companies_from_config(insert_new=True)`, re-enables flag; requires `{confirm: true}`
- `companies.btc_first_filing_date`: set by `POST /api/companies/<ticker>/detect_btc_anchor`; used as floor date for EDGAR ingest and extraction gate; force-detect with `{force: true}`
- `companies.reporting_cadence`: `monthly` | `quarterly` | `annual`; drives auto-gap-fill in pipeline for non-monthly reporters

#### 1.3  Metrics & Keywords sub-tab

| ID    | Component | Source | API endpoint(s) | Script(s) |
|-------|-----------|--------|-----------------|-----------|
| 1.3.1 | Metrics & Keywords table + add form | **CONFIG** | `GET /api/metric_schema` · `POST /api/metric_schema` · `PATCH /api/metric_schema/<key>` · `DELETE /api/metric_schema/<key>` · `GET /api/metric_schema/<key>/keywords` · `POST /api/metric_schema/<key>/keywords` | SSOT for all metrics. Active metrics feed LLM prompt and UI dropdowns. Keywords are per-metric anchor phrases used for EDGAR detection and LLM context. |

#### 1.4  Settings sub-tab

| ID    | Component | Source | API endpoint(s) | Script(s) |
|-------|-----------|--------|-----------------|-----------|
| 1.4   | Config settings form | CONFIG | `GET /api/config/settings` · `POST /api/config/settings` | Runtime overrides for extraction thresholds, LLM params, crawl limits, pipeline paths |
| 1.4.R | Metric Rules card | CONFIG | `GET /api/metric_schema` | Tune agreement/outlier thresholds per metric after first extraction cycle |
| 1.4.1 | Rules table | CONFIG | `GET /api/metric_schema` | Per-metric: agree_threshold, outlier_threshold, valid_range_min/max, enabled |
| 1.4.P | Rendered Prompt Preview card | n/a | `GET /api/llm_prompts` | Assembled LLM prompt with ticker context — verify before running extraction |
| 1.4.E | Prompt Editor card | CONFIG | `GET /api/llm_prompts/<metric>` · `POST /api/llm_prompts/<metric>` | Per-metric DB override; takes effect on next extraction run |

#### 1.5  Crawl Setup sub-tab

| ID    | Component | Source | API endpoint(s) | Script(s) |
|-------|-----------|--------|-----------------|-----------|
| 1.5.1 | Crawl ticker bar | n/a | — | Reads `_companies` in-memory |
| 1.5.2 | Crawl provider/model selector | n/a | — | Sets provider used by `startCrawlAll()` |
| 1.5.3 | Crawl prompt editor | CONFIG | `GET /api/crawl/prompt/<ticker>` | Per-ticker crawl prompt; falls back to master template |

#### 1.6  Purge Stages sub-tab

| ID    | Component | Source | API endpoint(s) | Script(s) |
|-------|-----------|--------|-----------------|-----------|
| 1.6   | Purge Stages sub-tab | n/a | `POST /api/delete/all` · `POST /api/delete/scrape` · `POST /api/delete/review` · `POST /api/delete/final` | Stage model: `ALL -> SCRAPE -> REVIEW -> FINAL`. Deleting a stage deletes that stage and all downstream layers. |

**Note:** 2.1.7–2.1.9 (Data Acquisition, Extraction Monitor, Pipeline Observability) moved to Research tab (2.7) in v1.2.

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
| 2.4.2 | Keyword dictionary editor | CONFIG | `GET /api/config/keyword_dictionary` · `POST /api/config/keyword_dictionary` | Global highlight packs used by Explorer and Review source views |

### 2.5  Review Queue tab

Redirects to 3.0.
Queue operations may also purge review artifacts only via `POST /api/delete/review`; this preserves scraped `reports`.

### 2.6  Pipeline Guide tab  (`/ops?tab=guide`)

Static reference panel — no API calls, no data source.

| ID    | Component           | Source | API endpoint(s) | Script(s) |
|-------|---------------------|--------|-----------------|-----------|
| 2.6.1 | Pipeline flow diagram | n/a  | — | — |
| 2.6.2 | Source-type reference table | n/a | — | — |

### 2.7  Research tab  (`/ops?tab=research`)

Three sub-tabs: **Input** (configure) → **Crawl** (acquire) → **Interpret** (extract + observe).

#### 2.7.1  Input sub-pane

| ID      | Component          | Source | API endpoint(s) | Script(s) |
|---------|--------------------|--------|-----------------|-----------|
| 2.7.1.1 | Ticker selector bar | n/a   | — | Reads `_companies` in-memory; populated by `loadCompanies()` |
| 2.7.1.2 | Model provider selector | n/a | — | Sets `crawl-provider` value used by `startCrawlAll()` |
| 2.7.1.3 | Crawl prompt editor | CONFIG | `GET /api/crawl/prompt/<ticker>` | Loads per-ticker `scripts/crawl_prompts/{TICKER}_crawl.md`; falls back to master template |

#### 2.7.2  Crawl sub-pane

| ID      | Component              | Source | API endpoint(s) | Script(s) |
|---------|------------------------|--------|-----------------|-----------|
| 2.7.2.1 | Outside acquisition panel | n/a | `POST /api/crawl/start` · `POST /api/ingest/ir` · `POST /api/ingest/archive` · `GET /api/ingest/<id>/progress` | LLM Crawl All + Acquire IR + Acquire Archive; shared `acq-status`/`acq-log` for deterministic acquires |
| 2.7.2.2 | LLM crawl status grid  | DATA   | `GET /api/crawl/status` | Per-ticker cards polled every 2 s while running |
| 2.7.2.3 | LLM crawl log panel    | DATA   | `GET /api/crawl/<task_id>/progress` | Shown on ticker card click; closed by Close button |
| 2.7.2.4 | SEC EDGAR panel        | n/a    | `POST /api/ingest/edgar` · `GET /api/ingest/<id>/progress` | Acquire EDGAR button + auto-extract checkbox |

#### 2.7.3  Interpret sub-pane

| ID      | Component                  | Source | API endpoint(s) | Script(s) |
|---------|----------------------------|--------|-----------------|-----------|
| 2.7.3.1 | LLM extraction run monitor | DATA   | `POST /api/operations/extract` · `GET /api/operations/extract/<id>/progress` | Live extraction progress; includes interpretation-only date window on stored docs (`report_date >= from`, `report_date <= to`); model selector calls `saveOllamaModel()` |
| 2.7.3.2 | Pipeline observability card | DATA  | `GET /api/operations/pipeline_observability` | Auto-loaded on Interpret sub-tab activate |
| 2.7.3.3 | Pipeline table             | DATA   | `GET /api/operations/pipeline_observability` | Per-ticker discovered/ingested/parsed/extracted counts + scraper config health |

### 2.8  Settings tab  (`/ops?tab=settings`)

| ID    | Component             | Source | API endpoint(s) | Script(s) |
|-------|-----------------------|--------|-----------------|-----------|
| 2.8.1 | Config settings form  | CONFIG | `GET /api/config/settings` · `POST /api/config/settings` | Grouped editable config: extraction thresholds, LLM params, crawl limits, pipeline paths |

### 2.9  QC / Pipeline tab  (`/ops?tab=qc`)

| ID    | Component           | Source | API endpoint(s) | Script(s) |
|-------|---------------------|--------|-----------------|-----------|
| 2.9.1 | QC snapshot table   | DATA   | `GET /api/qc/summary` | `src/routes/qc.py`; snapshots written by `POST /api/qc/snapshot` |
| 2.9.2 | Pipeline run controls | n/a  | `POST /api/pipeline/run` | Trigger full pipeline run with ticker/date-range scope |

---

## 3.0  `/review`  — Review Queue Page

Template: `review.html`

| ID    | Component        | Source | API endpoint(s) | Script(s) |
|-------|------------------|--------|-----------------|-----------|
| 3.1   | Filter bar       | n/a    | — | — |
| 3.2   | Review table     | DATA   | `GET /api/review` | `AgreementEngine` (`extractors/agreement.py`) populates via `extraction_pipeline.py` |
| 3.3   | Doc panel        | DATA   | `GET /api/review/<id>/document` | `doc_panel.js` |
| 3.4   | Approve action   | n/a    | `POST /api/review/<id>/approve` | `db.approve_review_item()` |
| 3.5   | Reject action    | n/a    | `POST /api/review/<id>/reject` | `db.reject_review_item()` |
| 3.6   | Re-extract action | n/a   | `POST /api/review/<id>/reextract` | `extraction_pipeline.extract_report()` |
| 3.7   | Bulk approve     | n/a    | `POST /api/review/<id>/approve` (looped client-side for selected rows) | — |

---

## 4.0  `/data-explorer`  — Data Explorer Page

Template: `index.html`

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
| 9.3   | Reports panel (company context) | DATA   | — (navigational/report context panel on company page) | — |

---

## Background scripts and workers

| ID   | Name             | Trigger | What it does | Writes to |
|------|------------------|---------|--------------|-----------|
| S.1  | `ScrapeWorker`   | Startup (daemon thread); `POST /api/scrape/trigger/<ticker>` | Polls `scrape_queue`, runs `IRScraper`, then EDGAR fetch for tickers with CIK; ingest-only queue worker (no extraction trigger) | `reports`, `scrape_queue`, `companies.last_scrape_*` |
| S.2  | `IRScraper`      | Called by S.1 or `POST /api/ingest/ir` | Fetches IR press releases (RSS/index/template mode), stores raw text | `reports` |
| S.3  | `ArchiveIngestor` | `POST /api/ingest/archive`; `cli.py ingest --source archive` | Walks archive, stores reports, then runs extraction inline for monthly docs | `reports`, `asset_manifest`, `data_points`, `review_queue` |
| S.4  | `EdgarConnector` | `POST /api/ingest/edgar`; S.1 EDGAR follow-up | Fetches EDGAR 8-K/10-Q/10-K filings, stores raw text only | `reports` |
| S.5  | `ManifestScanner` | `POST /api/manifest/scan` | Walks archive directory, upserts manifest entries | `asset_manifest` |
| S.6  | Extraction pipeline | `POST /api/operations/extract`; `cli.py extract` | Runs LLM extraction on stored reports, with regex used only as a monthly gate | `data_points`, `review_queue` |
| S.7  | Ingest auto-extract chain | `POST /api/ingest/ir` or `/api/ingest/edgar` with body `{ "auto_extract": true }` | Runs extraction stage over newly unextracted reports immediately after ingest | `data_points`, `review_queue`, `reports.extracted_at` |

---

## Purge scope summary

| Mode | Cleared | Not Cleared |
|------------------------------|-------------|
| `reset` | reports, data_points, review_queue, scrape_queue, asset_manifest, document_chunks, raw_extractions, btc_loans, facilities, source_audit, llm_benchmark_runs | companies, regime_config, llm_prompts, llm_ticker_hints, metric_schema, config_settings, patterns, metric_rules |
| `archive` | same as `reset`, plus writes deleted rows to `purge_archive.db` | same as `reset` |
| `hard_delete` (full scope) | all `reset` tables plus companies + regime_config | llm_prompts, llm_ticker_hints, metric_schema, config_settings, patterns, metric_rules |
