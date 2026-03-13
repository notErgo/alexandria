# Miners Platform — UI/API Component Spec

Version: 1.6
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
| 1.1.4 | Add Company form + bulk spreadsheet paste | n/a | `POST /api/companies` · `PUT /api/companies/<ticker>` · `GET /api/companies` | Single-add includes CIK, reporting_cadence, scraper mode. Bulk paste accepts TSV/CSV. Mode contract enforced at API: `rss -> rss_url`, `index -> ir_url`, `template -> url_template + pr_start_date`, `skip -> optional skip_reason` |
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

### 2.2  Ingest tab  (`/ops?tab=ingest`)

Sub-tabs: **IR** → **EDGAR** → **Archive** → **Crawl**.

| ID    | Component | Source | API endpoint(s) | Script(s) |
|-------|-----------|--------|-----------------|-----------|
| 2.1   | IR acquire panel | n/a | `POST /api/ingest/ir` · `GET /api/ingest/<id>/progress` | `IRScraper` (`scrapers/ir_scraper.py`); optional `auto_extract` body param chains to extraction immediately after |
| 2.2   | EDGAR acquire panel | n/a | `POST /api/ingest/edgar` · `GET /api/ingest/<id>/progress` | `EdgarConnector` (`scrapers/edgar_connector.py`); uses `btc_first_filing_date` as floor date; optional `auto_extract` |
| 2.3   | Archive acquire panel | n/a | `POST /api/ingest/archive` · `GET /api/ingest/<id>/progress` | `ArchiveIngestor` (`scrapers/archive_ingestor.py`); walks `OffChain/Miner/Miner Monthly/` |
| 2.4   | Pipeline observability table | DATA | `GET /api/operations/pipeline_observability` | Per-ticker discovered/ingested/parsed/extracted counts + scraper config health |
| 2.5   | Crawl setup panel | CONFIG | `POST /api/crawl/start` · `GET /api/crawl/status` · `GET /api/crawl/<task_id>/progress` · `GET /api/crawl/prompt/<ticker>` | `LLMCrawler` (`scrapers/llm_crawler.py`); per-ticker crawl prompt editor; provider/model selector |

---

## 3.0  Interpret tab  (`/ops?tab=interpret`)

**Template:** `ops.html` — pane `id="pane-interpret"`.

Sub-tabs: **Extract** (3.1, Critical) → **QC** (3.2, Optional).

> Interpret runs LLM extraction on documents that are **already stored** in `reports`.
> It does **not** scrape or ingest new source documents.
> Run an Ingest path first (tab 2.x) if the document is not yet present.

### 3.1  Extract sub-tab  (`/ops?tab=interpret#extract`)

Pane `id="spane-interpret-extract"`.

| ID    | Component | Source | API endpoint(s) | Script(s) |
|-------|-----------|--------|-----------------|-----------|
| 1.4.P | Prompt Preview card | n/a | `GET /api/llm_prompts` | Assembled prompt with ticker context injected. Refresh before every new extraction run. |
| 1.4.E | Prompt Editor card | CONFIG | `GET /api/llm_prompts/<metric>` · `POST /api/llm_prompts/<metric>` | Per-metric DB override; `active=0` = reset to hardcoded default. |
| 3.1.1 | LLM Extraction panel | DATA | `POST /api/operations/interpret` · `GET /api/operations/interpret/<id>/progress` | `startExtractionRun()`. Controls: ticker bar (shared `_selectedCrawlTickers`), source docs cadence (all/monthly/quarterly/annual), date window (from/to), expected granularity radio, gap-fill checkbox (quarterly/annual only — math-based inference, not LLM), sample mode, extract workers, run mode dropdown, model selector. Run mode: **Resume** (new docs only), **Force** (re-run all, keep data), **Full Reset** (purge data+review queue then re-extract; requires ticker selection). |
| 3.1.2 | Re-extract gap months panel | DATA | `POST /api/operations/requeue-missing` · `GET /api/operations/interpret/<id>/progress` | `runRequeueMissing()`. Finds `extraction_status='done'` reports missing data for selected metrics; resets them to `pending`; re-runs LLM on only those documents. Does not scrape. Analyst-protected rows are never overwritten. Ticker scope is independent of 3.1.1 (`_selectedGapTickers`). Metric list is SSOT-driven from `GET /api/metric_schema`. |

**Run mode decision guide (3.1.1):**

| Goal | Run mode |
|------|----------|
| Process only new/unextracted docs | Resume |
| Re-run all docs after a prompt change | Force re-extract |
| Start fresh for a ticker (destructive) | Full Reset + ticker selection |
| Backfill a specific missing metric | Use **3.1.2** instead |

**Gap-fill checkbox vs 3.1.2:**

| Tool | What it does | Uses LLM? |
|------|-------------|-----------|
| "Run gap-fill after extraction" checkbox (3.1.1) | Derives missing monthly rows from quarterly totals already in DB | No — pure math |
| Re-extract gap months panel (3.1.2) | Re-sends already-ingested docs missing a metric back to the LLM | Yes |

### 3.2  QC sub-tab

| ID    | Component | Source | API endpoint(s) | Script(s) |
|-------|-----------|--------|-----------------|-----------|
| 3.2   | QC snapshot table | DATA | `GET /api/qc/summary` · `POST /api/qc/snapshot` | Precision/recall metrics per snapshot. Capture Snapshot button. |

---

## 4.0  Review tab  (`/ops?tab=review`)

**Template:** `ops.html` — pane `id="pane-review"`.

| ID    | Component        | Source | API endpoint(s) | Script(s) |
|-------|------------------|--------|-----------------|-----------|
| 4.1   | Filter bar       | n/a    | — | — |
| 4.2   | Review table     | DATA   | `GET /api/review` | Populated by `interpret_pipeline.py` via `db.insert_review_item()` |
| 4.3   | Doc panel        | DATA   | `GET /api/review/<id>/document` | `doc_panel.js`; controls collapsed by default |
| 4.4   | Approve action   | n/a    | `POST /api/review/<id>/approve` | `db.approve_review_item()`; also writes `has_data` verdict to `report_metric_verdict` |
| 4.5   | Reject action    | n/a    | `POST /api/review/<id>/reject` | `db.reject_review_item()` |
| 4.6   | No-data action   | n/a    | `POST /api/review/<id>/no_data` | Writes `no_data` verdict to `report_metric_verdict`; rejects item. LLM_EMPTY items: no confirmation. Others: require `{confirmed: true}`. |
| 4.7   | Re-extract action | n/a   | `POST /api/review/<id>/reextract` | `extraction_pipeline.extract_report()` |
| 4.8   | Bulk approve     | n/a    | `POST /api/review/<id>/approve` (looped) | — |

---

## 5.0  Data tab  (`/ops?tab=data`)

**Template:** `ops.html` — pane `id="pane-data"`.

Sub-tabs: **Explorer** (5.1) · **Registry** (5.2) · **Documents**.

| ID    | Component | Source | API endpoint(s) | Script(s) |
|-------|-----------|--------|-----------------|-----------|
| 5.1   | Coverage heatmap (Explorer) | DATA | `GET /api/explorer/grid` | `coverage_logic.py`; 9-state cells; cell click opens detail panel |
| 5.1.1 | Cell detail panel | DATA | `GET /api/explorer/cell/<ticker>/<period>/<metric>` | Save, Gap, Re-extract per-cell actions |
| 5.1.2 | Period pipeline trace panel | DATA | `GET /api/coverage/period_trace?ticker=X&period=Y` | Shows why an empty cell is empty (no doc / keyword-gated / LLM_EMPTY / pending). `cell_state` field is the canonical 9-state value. |
| 5.2   | Registry table | DATA | `GET /api/registry` | `ManifestScanner`; `POST /api/manifest/scan` to trigger; columns: parse quality, extraction status, char count, scan-keywords button |

---

## DE4.0  `/data-explorer`  — Data Explorer Page

Template: `index.html`

| ID    | Component          | Source | API endpoint(s) | Script(s) |
|-------|--------------------|--------|-----------------|-----------|
| DE4.1 | Filter bar         | n/a    | — | — |
| DE4.2 | Data points table  | DATA   | `GET /api/data` | `db.query_data_points()` |
| DE4.3 | Export button      | n/a    | `GET /api/export.csv` | — |
| DE4.4 | Lineage panel      | DATA   | `GET /api/data/lineage` | — |

---

## MD5.0  `/miner-data`  — Miner Data Page

Template: `miner_data.html`.

| ID      | Component | Source | API endpoint(s) | Script(s) |
|---------|-----------|--------|-----------------|-----------|
| MD5.1   | Company selector + month timeline table | DATA | `GET /api/miner/<ticker>/timeline` | Per-company monthly data grid; method badges; inline cell edit |
| MD5.2   | Doc panel | DATA | `GET /api/operations/manifest/<id>/preview` | `doc_panel.js` |
| MD5.3   | Interpret sub-view | DATA | `GET /api/miner/<ticker>/sec` · `GET /api/miner/<ticker>/interpret` | Reconciliation summary, analyst commentary, LLM reprompt, finalize staging |
| MD5.3.1 | Start extraction | DATA | `POST /api/operations/interpret` · `GET /api/operations/interpret/<id>/progress` | `doExtract()` in `miner_data.js`; source scope (IR+SEC / IR only / SEC only) |
| MD5.4   | LLM CSV export (unreviewed) | DATA | `GET /api/export_llm_csv?ticker=X` | Direct LLM output — not validated by agreement engine |

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
