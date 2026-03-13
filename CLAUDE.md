# CLAUDE.md — Bitcoin Miner Data Platform

> **Prerequisite**: Read the global [`CLAUDE.md`](../../CLAUDE.md) first.

## Coding Standards (applies to all agents including Codex)

- **No emojis anywhere in the codebase** — not in comments, log messages, template text, button labels, toast messages, error strings, or variable names. Zero exceptions.
- **No inline CSS** — all styles go in `/static/css/style.css` or a `{% block styles %}` block.
- **No `str(e)` in `jsonify()`** — log the full exception server-side, return a generic fixed string to the caller.
- Tests are written before implementation. A failing test must exist before any new function is written.

## Worktree Policy

Follow the shared, canonical worktree policy in the global workspace docs:
- `~/Documents/Hermeneutic/CODEX.md`
- `~/Documents/Hermeneutic/CLAUDE.md`

Project-local default remains single-folder `OffChain/miners` unless parallel agent execution is explicitly requested.

## Purpose

Bitcoin miner intelligence platform. LLM-only extraction (Qwen3.5-35B-A3B via Ollama) for 13 public mining companies. Ingests archived PDFs/HTMLs, live IR press releases, and SEC EDGAR 8-K/10-Q/10-K filings. Keyword gate filters non-production documents; LLM confidence + outlier detection routes to data_points (auto-accept) or review_queue (low confidence/outlier).

## Build & Run

```bash
cd OffChain/miners

# Create and activate venv
python3 -m venv venv && source venv/bin/activate

# Install dependencies
pip install --trusted-host pypi.org --trusted-host files.pythonhosted.org -r requirements.txt

# Run web server
python3 run_web.py  # → localhost:5004

# OR use CLI
python3 cli.py ingest --source archive
python3 cli.py extract                        # run extraction pipeline on stored reports
python3 cli.py extract --ticker MARA --force  # re-extract (force, after pattern changes)
python3 cli.py query --ticker MARA --metric production_btc
python3 cli.py export --out results.csv --ticker MARA
```

## Architecture

```
OffChain/miners/
├── run_web.py          Flask entry point (port 5004, threaded=True)
├── cli.py              CLI: ingest / query / export
├── src/
│   ├── app_globals.py  MinerDB singleton
│   ├── config.py       Constants (CONFIDENCE_REVIEW_THRESHOLD=0.75, etc.)
│   ├── miner_types.py  Shared dataclasses and enums (renamed from types.py)
│   ├── infra/
│   │   ├── db.py               MinerDB — all SQLite CRUD
│   │   └── logging_config.py   setup_logging() — call before create_app()
│   ├── interpreters/
│   │   ├── interpret_pipeline.py  extract_report(report, db) → ExtractionSummary
│   │   ├── llm_interpreter.py     LLMInterpreter(session, db) — Ollama Qwen3.5-35B-A3B
│   │   ├── outlier.py             detect_outlier(candidate, trailing, threshold_pct) → (bool, float|None)
│   │   ├── table_interpreter.py   TableInterpreter — table extraction
│   │   ├── unit_normalizer.py     normalize_hashrate/btc/percent/value
│   │   └── confidence.py          score_extraction(weight, distance, value, metric) → float
│   ├── scrapers/
│   │   ├── archive_ingestor.py ArchiveIngestor.ingest_all(force?) — walks OffChain/Miner/
│   │   ├── manifest_scanner.py scan_archive_directory() — upserts asset_manifest entries
│   │   ├── ir_scraper.py       IRScraper.scrape_company(company) — fetch+store only
│   │   └── edgar_connector.py  EdgarConnector.fetch_production_filings(...)
│   ├── parsers/
│   │   ├── document_parser.py      get_parser(source_type) — dispatcher
│   │   ├── press_release_parser.py PressReleaseParser — wraps pdfplumber/BS4 for monthly reports
│   │   └── annual_report_parser.py AnnualReportParser — EDGAR HTML sections + pymupdf PDF
│   ├── coverage_logic.py       Pure functions: generate_month_range, compute_cell_state, summarize_grid
│   └── routes/
│       ├── data_points.py      GET /api/data, GET /api/data/lineage, GET /api/export.csv
│       ├── companies.py        GET /api/companies, GET /api/companies/<ticker>
│       ├── reports.py          POST /api/ingest/{archive,ir,edgar,reaudit}, GET /api/ingest/<id>/progress
│       ├── review.py           GET /api/review, POST /api/review/<id>/{approve,reject,reextract}
│       ├── facilities.py       GET/POST /api/facilities, /api/btc_loans, /api/source_audit
│       ├── llm_prompts.py      GET/POST /api/llm_prompts[/<metric>]
│       ├── coverage.py         GET /api/coverage/summary|grid|assets, POST /api/manifest/scan
│       └── operations.py       GET /api/operations/queue, POST /api/operations/extract|assign_period
├── config/
│   ├── companies.json          13 companies with CIKs
│   └── patterns/               Active pattern JSON files (hodl_btc, production_btc, sold_btc); others archived under patterns/archived/
├── docs/
│   └── architecture/
│       ├── dag.json            Module import graph (layer 0-5, edges = imports)
│       ├── operations.json     Operation contracts: SSOT read priority + pipeline stage DAG + per-operation code paths (UI → route → DB). Validated by tests/test_ui_spec.py.
│       └── README.md           Architecture docs index
└── tests/
    └── 532 passing unit tests (T3)
```

## Schema Version History

| Version | Added |
|---------|-------|
| v1 | companies, reports, data_points, patterns, review_queue |
| v2 | source_audit, btc_loans, facilities, llm_prompts; review_queue.llm_value/regex_value/agreement_status |
| v3 | reports.extracted_at (two-stage pipeline) |
| v4 | config_settings, llm_ticker_hints |
| v5 | asset_manifest, document_chunks, data_points.chunk_id, reports.parse_quality |
| v6 | (see migration history) |
| v7 | companies: sector, scraper_mode, scraper_issues_log, scraper_status, last_scrape_at, last_scrape_error, probe_completed_at; asset_manifest.mutation_log; regime_config; metric_schema; scrape_queue |
| v29 | companies.btc_first_filing_date (EDGAR first-filing anchor date) |
| v30 | metric_keywords table (per-metric anchor phrases); search_keywords retired (emptied) |

## Data Flow (v3 — two-stage pipeline + optional ingest chain)

Reference map: `docs/general_data_ui_llm_interaction_documentation.md`

```
Stage 1 — Ingest (fetch + store raw text):
  Archive PDFs/HTMLs → ArchiveIngestor.ingest_all()  → reports table
                     ↳ monthly files: extraction runs inline via LLM pipeline
                     ↳ quarterly files: stored as pending (deferred to Stage 2)
  IR press releases  → IRScraper.scrape_company()    → reports table (fetch+store)
  EDGAR filings      → EdgarConnector.fetch_all_filings()
                     → reports table (8-K + 10-Q + 10-K; fetch+store)

Stage 2 — Extract (LLM-only on stored reports):
  db.get_unextracted_reports()  OR  cli.py extract
  → interpret_pipeline.extract_report(report, db)
      → Keyword gate: any mining phrase in text? → NO → skip (keyword_gated)
                                                 → YES → continue
      → LLM batch extract (Ollama Qwen3.5-35B, all metrics in one call)
      → Per-metric routing (confidence + outlier detection):
            confidence ≥ 0.75 AND not outlier → data_points (LLM value)
            confidence < 0.75 OR outlier      → review_queue
            LLM returned nothing              → period gap
      → db.mark_report_extracted(report_id)
  Analyst protection: extraction_method IN ('analyst','analyst_approved',
      'review_approved','review_edited') → never overwritten by pipeline

Optional ingest→extract chain:
  POST /api/ingest/ir    { "auto_extract": true }
  POST /api/ingest/edgar { "auto_extract": true }
  These run acquisition first, then run extraction over unextracted reports
  before the ingest task is marked complete.
```

## Key Patterns

### Naming conflict: `src/types.py` → `src/miner_types.py`
Python stdlib has a `types` module. Our types file is named `miner_types.py`. Import as:
```python
from miner_types import ExtractionResult, Metric
```

### pytest.ini pythonpath includes `tests`
`pythonpath = src tests` — required so test helper functions in `tests/helpers.py` are importable.

### Confidence scoring
`score = pattern_weight × distance_factor × range_factor`, clamped to [0.0, 1.0].
- `distance_factor = max(0, 1 - context_distance / 500)`
- `range_factor = 1.0` if value within `METRIC_VALID_RANGES[metric]`, else `0.0`

### EDGAR API requires User-Agent header
Bare requests get HTTP 403. Always set `User-Agent: Hermeneutic Research Platform ...`

### Old RIOT files ("Riot Blockchain" era)
Files titled "Riot Blockchain Announces April Production..." have no year in filename.
`infer_period_from_filename` uses strategy 4 (read_body=True for HTML) to recover these:
it parses the HTML with BeautifulSoup, extracts visible text, then calls
`infer_period_from_text()`. Strategy 4 MUST use BeautifulSoup — raw `f.read(3000)` fails
because navigation markup pushes actual content past the raw byte sampling window
(see Anti-pattern #29 in global CLAUDE.md).

### IR scrape_mode dispatch
`IRScraper.scrape_company()` dispatches on normalized mode:
`company["scraper_mode"]` (preferred) with fallback to `company["scrape_mode"]` (legacy).
- `"rss"` → `_scrape_rss()` — fetches Equisolve RSS feed, filters production PRs, stores raw text
- `"index"` / `"template"` → `_scrape_index()` / `_scrape_template()` — parses HTML listing, stores raw text
- `"skip"` → no-op (logs skip_reason)

`IRScraper` does not do extraction — it is Stage 1 (fetch+store) only. Extraction goes through
`interpret_pipeline.extract_report(report, db)`. Use either:
- explicit stage 2 (`POST /api/operations/extract` or `cli.py extract`), or
- ingest auto-chain (`POST /api/ingest/ir|edgar` with `auto_extract=true`).

Active RSS companies: MARA (`ir.mara.com/.../rss`), WULF (`investors.terawulf.com/.../rss`).
Unreachable companies (502 at 2026-03): CORZ, ARBK, IREN — set to `"skip"`.

### Global design pattern: Mode Contract pattern
For any mode-driven feature (scrapers, parsers, exporters), enforce one source of truth:
1. UI labels each mode with intent and required fields.
2. API validates mode-specific required fields before DB writes.
3. DB stores one canonical mode key (`*_mode`) and all required companion fields.
4. Runtime dispatch reads the same canonical key with explicit legacy fallback only.
5. Tests cover create/update validation plus runtime dispatch wiring.

Apply this pattern to new mode families by default. It prevents "valid-looking UI state"
that cannot execute at runtime.

### Global design pattern: Pipeline Stage Counters
For any ingestion pipeline, publish one shared observability contract:
1. `discovered` (asset inventory discovered on disk/API),
2. `ingested` (raw documents stored),
3. `parsed` (parser completed),
4. `extracted` (LLM/regex pass completed),
5. `queued_for_review` (pending analyst decisions).

Expose the same counters at global and per-entity scopes, plus config health
checks for required mode fields. This prevents hidden backlog and misconfigured
scrapers from appearing as "empty data".

### Global design pattern: Structured Logging Contract
For all long-running or state-changing routes/workers, emit structured logs with
stable `event=` prefixes and core context fields:
- `event` (machine-parseable action key),
- entity scope (`ticker`, `task_id`, `route`),
- control inputs (`apply_mode`, `stale_days`, `timeout`),
- outcome fields (`status`, `recommended_mode`, `applied`, `error`),
- counters (`targeted`, `completed`, `failed`).

Rules:
- Log `*_start` and `*_end` at `INFO`.
- Log per-item failures at `WARNING`.
- Keep debug details (`per-candidate`, parser internals) at `DEBUG`.
- Never rely on UI status alone for operations monitoring; logs must independently
  reconstruct execution flow.

### Global design pattern: Agent Propose, Rules Verify
Use agents for source discovery only (candidate URLs + rationale), then run
deterministic probes before mode changes:
- Store proposals in `scraper_discovery_candidates`.
- Probe and write evidence to `source_audit`.
- Recommend mode from verified evidence (`rss`, `template`, `index`, else `skip`).
- Apply mode automatically only when policy permits; keep `skip` high-friction.

### Startup company sync control
Company config auto-sync is startup-gated:
- Env default: `MINERS_AUTO_SYNC_COMPANIES=1|0`
- Runtime override: `config_settings.auto_sync_companies_on_startup` (`'1'` or `'0'`)

`hard_delete` purge can set the runtime flag to `'0'` so company rows do not repopulate
after restart until an operator explicitly runs `Sync Config`.

### hodl_btc_4 pattern window
`hodl_btc_4` uses `{0,50}` with `(?!\s+as\s+of)` negative lookahead.
The wider window covers MARA 2021 prose: "total bitcoin holdings to approximately 5,518"
(18-char separator). The lookahead prevents capturing day numbers from
"holdings as of January 31: 10,556" (hodl_btc_6 handles the colon format instead).

### sold_btc data is correct
DB shows 15 non-zero MARA sold_btc months (2023-01 through 2024-05). These are real
sales — MARA explicitly sold BTC to cover operating expenses before returning to 100% HODL.
Not false positives.

### Extractor group selection is metric-dependent
`_apply_pattern` passes `m.group(1)` (the captured group) for BTC-type metrics
(`production_btc`, `hodl_btc`, `sold_btc`) and `m.group(0)` (full match including unit)
for all other metrics (`hashrate_eh`, `realization_rate`). The BTC patterns include an
optional capturing group to isolate the numeric value — this prevents a PDF footnote
digit between the keyword and value (e.g. "BTC Produced 2 750") from being picked up
as the first number by `normalize_btc`. Non-BTC metrics need the full match so the
normalizer can parse unit suffixes like "EH/s" or "%".

## Data Locations

| Data Type | Location |
|-----------|----------|
| Main DB | `~/Documents/Hermeneutic/data/miners/minerdata.db` |
| Archive source | `OffChain/Miner/Miner Monthly/` |
| Pattern configs | `OffChain/miners/config/patterns/*.json` |
| Company config | `OffChain/miners/config/companies.json` |

## Companies (13)

MARA, RIOT, CLSK, CORZ, BITF, BTBT, CIFR, HIVE, HUT8, ARBK, SDIG, WULF, IREN

### Parser routing table (source_type → parser)

| source_type | Parser class |
|-------------|-------------|
| archive_html | PressReleaseParser |
| archive_pdf | PressReleaseParser |
| edgar_10k | AnnualReportParser |
| edgar_10q | AnnualReportParser |
| ir_press_release | PressReleaseParser |

pymupdf (fitz) is required for AnnualReportParser.parse_pdf(). Listed in requirements.txt as `pymupdf>=1.23.0`.
If not installed, parse_pdf() returns ParseResult with parse_quality='parse_failed'.

### Unified Ops Page (Schema v7)

Landing page (`/`) → `landing.html` — sector scorecard (latest metric per company).
Ops page (`/ops?tab=<tab>`) → `ops.html` — 4-tab unified interface.

**Tabs:**
- `companies` — company list, scraper config, regime windows, scrape queue. Redirected from `/operations`.
- `registry` — asset_manifest browser (filter by ticker/period/doc_type/extraction_status). Redirected from `/coverage`.
- `explorer` — coverage heatmap (7-state cells), cell detail, edit value, mark gap, re-extract. Filter by state/metric/months.
- `review` — review queue with J/K/A/R keyboard nav + bulk approve. Redirected from `/review`.

**New API endpoints (v7):**
- `GET /api/regime/<ticker>` — list regime windows
- `POST /api/regime/<ticker>` — add window ({cadence, start_date, end_date?, notes})
- `DELETE /api/regime/<ticker>/<window_id>` — remove window
- `POST /api/scrape/trigger/<ticker>` — enqueue scrape job (returns 202)
- `GET /api/scrape/queue` — all recent scrape jobs
- `GET /api/explorer/grid?ticker&months&state&metric&min_confidence` — cell grid
- `GET /api/explorer/cell/<ticker>/<period>/<metric>` — cell detail with raw_text + matches
- `POST /api/explorer/cell/.../save` — analyst edit (mutation hierarchy enforced)
- `POST /api/explorer/cell/.../gap` — analyst gap sentinel
- `POST /api/explorer/reextract` — regex re-extraction on selection
- `GET /api/registry?ticker&period&doc_type&extraction_status` — asset_manifest browser
- `GET /api/scorecard` — latest 7 metrics per company
- `GET /api/metric_schema?sector` — list metric schema
- `POST /api/metric_schema` — add metric (409 on duplicate key)
- `GET /api/companies` — list companies (active_only=False)
- `POST /api/companies` — add company
- `PUT /api/companies/<ticker>` — update company config

**Background worker:**
`ScrapeWorker` thread started in `run_web.py`. Polls scrape_queue every 5s. Claims job → runs `IRScraper` and EDGAR fetch (for companies with CIK) → completes/fails. It is ingest-only and does not trigger extraction. Startup scrub resets orphaned `running` jobs.

**Shared components:**
- `static/js/doc_panel.js` — `DocPanel.init/open/close/buildHighlightedSource`
- `static/css/style.css` — `.doc-panel`, `.doc-panel-body`, `.doc-source-view`, `.doc-hl` classes

Initial manifest scan after deployment: `POST http://localhost:5004/api/manifest/scan`

## SSOT Registry (Non-Negotiable)

| Concept | SSOT | Purpose |
|---|---|---|
| Metrics | `metric_schema` DB table | What to extract; populates all UI dropdowns, LLM extraction prompts |
| Keywords | `metric_keywords` DB table (v30) | Per-metric EDGAR anchor phrases; used for first-filing detection and LLM extraction context. `search_keywords` is retired (kept empty for compat). |
| BTC activity start | `companies.btc_first_filing_date` | Derived from keyword scan; drives EDGAR ingestion window and LLM crawl context |
| Extraction patterns | `config/patterns/<key>.json` | Regex patterns keyed by metric |
| Thresholds | `metric_rules` DB table | Agreement and outlier thresholds per metric |

Rules:
- No template, JS file, or Python module may contain a hardcoded list of metric keys.
- All metric lists must be fetched from `GET /api/metric_schema` at runtime.
- EDGAR 8-K ingestion must NOT filter by keyword content — use `btc_first_filing_date` as `since_date`.
- Keywords are not metrics. Do not conflate them.
- `detect_btc_first_filing_date()` is called once per company; result cached in `companies.btc_first_filing_date`.

## Running Tests

```bash
venv/bin/pytest tests/ -v  # 532 tests, all should pass
```

## Pipeline Commands

```bash
# Force re-ingest all archive files with updated patterns
venv/bin/python3 cli.py ingest --source archive --force

# Coverage report for one ticker
venv/bin/python3 cli.py diagnose --ticker MARA

# Full pipeline (scrape + ingest + coverage)
./refresh.sh MARA
./refresh.sh        # all companies
```
