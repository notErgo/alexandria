# CLAUDE.md — Bitcoin Miner Data Platform

> **Prerequisite**: Read the global [`CLAUDE.md`](../../CLAUDE.md) first.

## Coding Standards (applies to all agents including Codex)

- **No emojis anywhere in the codebase** — not in comments, log messages, template text, button labels, toast messages, error strings, or variable names. Zero exceptions.
- **No inline CSS** — all styles go in `/static/css/style.css` or a `{% block styles %}` block.
- **No `str(e)` in `jsonify()`** — log the full exception server-side, return a generic fixed string to the caller.
- Tests are written before implementation. A failing test must exist before any new function is written.

## Purpose

Bitcoin miner intelligence platform. LLM-first extraction (Qwen3.5-35B-A3B via Ollama) + regex validation for 13 public mining companies. Ingests archived PDFs/HTMLs, live IR press releases, and SEC EDGAR 8-K filings. Agreement engine routes to data_points (auto-accept) or review_queue (disagreement/LLM-only).

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
│   ├── app_globals.py  MinerDB + PatternRegistry singletons
│   ├── config.py       Constants (CONFIDENCE_REVIEW_THRESHOLD=0.75, etc.)
│   ├── miner_types.py  Shared dataclasses and enums (renamed from types.py)
│   ├── infra/
│   │   ├── db.py               MinerDB — all SQLite CRUD
│   │   └── logging_config.py   setup_logging() — call before create_app()
│   ├── extractors/
│   │   ├── pattern_registry.py  PatternRegistry.load(config_dir)
│   │   ├── extractor.py         extract_all(text, patterns, metric) → [ExtractionResult]
│   │   ├── unit_normalizer.py   normalize_hashrate/btc/percent/value
│   │   ├── confidence.py        score_extraction(weight, distance, value, metric) → float
│   │   ├── llm_extractor.py     LLMExtractor(session, db) — Ollama Qwen3.5-35B-A3B
│   │   ├── agreement.py         evaluate_agreement(regex, llm) → AgreementDecision
│   │   └── extraction_pipeline.py  extract_report(report, db, registry) → ExtractionSummary
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
│   └── patterns/               5 metric pattern JSON files
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

## Data Flow (v3 — two-stage pipeline)

```
Stage 1 — Ingest (fetch + store raw text):
  Archive PDFs/HTMLs → ArchiveIngestor.ingest_all()  → reports table
  IR press releases  → IRScraper.scrape_company()    → reports table (fetch+store only)
  EDGAR 8-K filings  → EdgarConnector               → reports table

Stage 2 — Extract (LLM+regex+agreement on stored reports):
  db.get_unextracted_reports()  OR  cli.py extract
  → extraction_pipeline.extract_report(report, db, registry)
      → regex (PatternExtractor) + LLM (Ollama Qwen3.5-35B)
      → AgreementEngine per metric:
            Both agree (≤2%)      → data_points  (regex value stored)
            Disagree / LLM-only   → review_queue (both candidates)
            Neither found         → period gap
      → db.mark_report_extracted(report_id)
  Analyst protection: extraction_method IN ('analyst','analyst_approved',
      'review_approved','review_edited') → never overwritten by pipeline
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
`IRScraper.scrape_company()` dispatches on `company["scrape_mode"]`:
- `"rss"` → `_scrape_rss()` — fetches Equisolve RSS feed, filters production PRs, stores raw text
- `"index"` / `"template"` → `_scrape_index()` / `_scrape_template()` — parses HTML listing, stores raw text
- `"skip"` → no-op (logs skip_reason)

`IRScraper` no longer takes a `registry` argument — all extraction now goes through
`extraction_pipeline.extract_report()`. After IR ingest, run `cli.py extract` (or the
extraction pipeline) to extract data points from the stored reports.

Active RSS companies: MARA (`ir.mara.com/.../rss`), WULF (`investors.terawulf.com/.../rss`).
Unreachable companies (502 at 2026-03): CORZ, ARBK, IREN — set to `"skip"`.

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
`ScrapeWorker` thread started in `run_web.py`. Polls scrape_queue every 5s. Claims job → runs `IRScraper` → auto-triggers extraction → completes/fails. Startup scrub resets orphaned `running` jobs.

**Shared components:**
- `static/js/doc_panel.js` — `DocPanel.init/open/close/buildHighlightedSource`
- `static/css/style.css` — `.doc-panel`, `.doc-panel-body`, `.doc-source-view`, `.doc-hl` classes

Initial manifest scan after deployment: `POST http://localhost:5004/api/manifest/scan`

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
