# Miner Data Platform — Test Plan

## T1: Infrastructure Health

### 1.1 EDGAR Full-Text Search API
```bash
curl -s -H "User-Agent: Research contact@example.com" \
  "https://efts.sec.gov/LATEST/search-index?q=%22bitcoin+production%22&forms=8-K&dateRange=custom&startdt=2024-01-01&enddt=2024-12-31" \
  | python3 -c "import json,sys; d=json.load(sys.stdin); print('hits:', d['hits']['total']['value'])"
# Expected: hits: N (N > 0)
```

### 1.2 Flask Status Endpoint
```bash
curl -s http://localhost:5003/api/status | python3 -m json.tool
# Expected: {"success": true, "data": {"app": "miners", "companies": 13, ...}}
```

### 1.3 pdfplumber on MARA PDFs
```bash
python3 -c "
import sys; sys.path.insert(0,'src')
from scrapers.archive_ingestor import _parse_pdf
from pathlib import Path
p = list(Path('../Miner/Miner Monthly/MARA MONTHLY').glob('*.pdf'))[0]
text = _parse_pdf(str(p))
print('chars:', len(text), '| bitcoin in text:', 'bitcoin' in text.lower())
"
# Expected: chars: >500 | bitcoin in text: True
```

### 1.4 SQLite DB accessible and seeded
```bash
sqlite3 ~/Documents/Hermeneutic/data/miners/minerdata.db \
  "SELECT COUNT(*) as companies FROM companies; SELECT COUNT(*) as data_points FROM data_points;"
# Expected: 13 companies; ≥0 data points (>0 after archive ingest)
```

### 1.5 Coverage summary endpoint reachable
```bash
curl -s http://localhost:5004/api/coverage/summary | python3 -m json.tool
# Expected: {"success": true, "data": {"total_reports": N, "manifest_total": M, ...}}
```

### 1.6 Manifest scan can reach archive directory
```bash
ls ~/Documents/Hermeneutic/OffChain/Miner/Miner\ Monthly/ | head -5
# Expected: 13 ticker subdirectories
```

---

## T2: Smoke Tests

### 2.1 Landing Page (`/`)
- [ ] `GET /` → 200, scorecard renders with all 13 company cards
- [ ] Stats strip shows companies, data_points, pending_review from `/api/status`
- [ ] Each company card shows latest metric values from `/api/scorecard`

### 2.2 Ops Page (`/ops`)
- [ ] `GET /ops` → 200, 4 tabs render: Companies, Registry, Explorer, Review Queue
- [ ] `GET /ops?tab=companies` → Companies tab active on load
- [ ] `GET /ops?tab=review` → Review tab active on load
- [ ] `GET /ops?tab=explorer&state=review_pending` → Explorer tab with review_pending pre-selected
- [ ] `/review` redirect → 302 to `/ops?tab=review`
- [ ] `/coverage` redirect → 302 to `/ops?tab=registry`
- [ ] `/operations` redirect → 302 to `/ops?tab=companies`

#### Companies Tab
- [ ] Company list loads from `/api/companies`
- [ ] Scraper mode badges render correctly (rss/index/skip)
- [ ] Regime windows editor opens inline on Regime button click
- [ ] Add window form posts to `/api/regime/<ticker>` and refreshes tags
- [ ] Delete window button calls `DELETE /api/regime/<ticker>/<id>`
- [ ] Scrape button posts to `/api/scrape/trigger/<ticker>` and shows queue update
- [ ] Add Company form validates ticker (max 10) and name (max 100)
- [ ] Scrape queue loads from `/api/scrape/queue` and renders status badges

#### Registry Tab
- [ ] Registry loads on tab activation with default filters
- [ ] Filter by ticker/period/doc_type/extraction_status and re-fetch
- [ ] Item count shown in result header

#### Explorer Tab
- [ ] Metric selector pre-loads from `/api/metric_schema`
- [ ] Grid loads on tab activation with 36-month default
- [ ] Cells colored by state (data=green, review_pending=orange, no_document=dark)
- [ ] Clicking a cell loads detail panel from `/api/explorer/cell/<ticker>/<period>/<metric>`
- [ ] Edit value form posts to `.../save`, shows 409 message for analyst-protected values
- [ ] Mark Gap button posts to `.../gap` and reloads cell
- [ ] Re-extract panel accepts pasted text and calls `/api/explorer/reextract`

#### Review Tab
- [ ] Review queue loads from `/api/review?status=PENDING`
- [ ] J/K keys navigate rows (highlight changes)
- [ ] A key approves focused row; R key opens reject prompt
- [ ] Checkboxes enable bulk approve button; Bulk Approve processes all checked items
- [ ] On total failure, error shown — UI stays open (Anti-pattern #15 compliance)

### 2.3 Company Chart (`/company/<ticker>`)
- [ ] `GET /company/MARA` → 200, ECharts renders with data
- [ ] `GET /company/INVALID` → 404 error page
- [ ] Metric toggle buttons visible; clicking toggles series

### 2.4 Data API Endpoints
```bash
# List all companies
curl -s http://localhost:5003/api/companies | python3 -m json.tool

# Query MARA production
curl -s "http://localhost:5003/api/data?ticker=MARA&metric=production_btc" | python3 -m json.tool

# Invalid ticker
curl -s "http://localhost:5003/api/data?ticker=INVALID" | python3 -m json.tool
# Expected: 400 {"success": false, "error": {"code": "INVALID_TICKER", ...}}

# Invalid date format
curl -s "http://localhost:5003/api/data?from_period=2024" | python3 -m json.tool
# Expected: 400 {"success": false, "error": {"code": "INVALID_DATE", ...}}

# CSV export
curl -s "http://localhost:5003/api/export.csv?ticker=MARA" | head -3
```

### 2.5 Ingest Endpoints
```bash
# Start archive ingest
curl -s -X POST http://localhost:5003/api/ingest/archive | python3 -m json.tool
# Expected: {"success": true, "data": {"task_id": "..."}}

# Check progress (use task_id from above)
curl -s http://localhost:5003/api/ingest/<task_id>/progress | python3 -m json.tool

# 409 if already running
curl -s -X POST http://localhost:5003/api/ingest/archive | python3 -m json.tool
# (immediately after first; may or may not get 409 depending on timing)
```

### 2.6 Review API Endpoints
```bash
# List pending items
curl -s "http://localhost:5003/api/review?status=PENDING&limit=5" | python3 -m json.tool

# Approve item (replace ID)
curl -s -X POST http://localhost:5003/api/review/1/approve | python3 -m json.tool

# Reject item
curl -s -X POST http://localhost:5003/api/review/2/reject \
  -H "Content-Type: application/json" -d '{"note":"Value out of range"}' | python3 -m json.tool

# Edit item
curl -s -X POST http://localhost:5003/api/review/3/edit \
  -H "Content-Type: application/json" -d '{"corrected_value":450.0,"note":"OCR error"}' | python3 -m json.tool
```

### 2.7 Cross-Cutting Concerns
- [ ] Navigation links correct on all pages (Scorecard, Operations, Miner Data, Data Explorer)
- [ ] Dark/light theme toggle persists across page refresh (localStorage)
- [ ] 404 page renders for unknown routes
- [ ] `/api/*` routes return JSON errors; page routes render HTML errors

### 2.8 New API Endpoints (v7)
```bash
# Regime windows
curl -s http://localhost:5004/api/regime/MARA | python3 -m json.tool
curl -s -X POST http://localhost:5004/api/regime/MARA \
  -H "Content-Type: application/json" \
  -d '{"cadence":"monthly","start_date":"2021-01-01"}' | python3 -m json.tool

# Scrape queue
curl -s http://localhost:5004/api/scrape/queue | python3 -m json.tool
curl -s -X POST http://localhost:5004/api/scrape/trigger/MARA | python3 -m json.tool
# Expected: 400 if scraper_mode=skip; 202 if mode=rss/index/template

# Explorer grid
curl -s "http://localhost:5004/api/explorer/grid?ticker=MARA&months=12" | python3 -m json.tool

# Explorer cell
curl -s "http://localhost:5004/api/explorer/cell/MARA/2024-09-01/production_btc" | python3 -m json.tool

# Registry (asset manifest browser)
curl -s "http://localhost:5004/api/registry?ticker=MARA" | python3 -m json.tool

# Scorecard
curl -s http://localhost:5004/api/scorecard | python3 -m json.tool

# Metric schema
curl -s http://localhost:5004/api/metric_schema | python3 -m json.tool
```

### Quick Smoke Test
```bash
# 60-second sanity check after starting server
1. curl -s http://localhost:5004/api/status
2. curl -s http://localhost:5004/api/companies
3. curl -s "http://localhost:5004/api/data?ticker=MARA"
4. curl -s "http://localhost:5004/api/review?status=PENDING"
5. curl -s http://localhost:5004/api/coverage/summary
6. curl -s http://localhost:5004/api/operations/queue
7. curl -s http://localhost:5004/unknown-route  # → 404
```

---

## T3: Unit Tests

```bash
cd OffChain/miners
venv/bin/pytest tests/ -v
# Expected: 532 passed, 38 skipped
```

| Test file | Coverage |
|-----------|----------|
| `test_db.py` (16) | Schema tables, WAL mode, FK enforcement, company/report/datapoint/review CRUD, upsert, approve/reject/edit |
| `test_types.py` (4) | ExtractionResult fields, Metric enum count, ReviewStatus names, IngestSummary defaults |
| `test_unit_normalizer.py` (9) | EH, PH→EH, TH→EH conversions; BTC with comma/negative; percent→ratio; unknown→None; dispatch |
| `test_confidence.py` (6) | High weight = high score; distance degrades; out-of-range degrades; clamped [0,1]; unknown metric; ratio in range |
| `test_extractor.py` (7) | Clear match, no match, PH conversion, multi-match sort, snippet length, conflict dedup, pattern_id |
| `test_pattern_registry.py` (5) | 5 metrics loaded, sorted by priority, required keys, get_patterns, KeyError for unknown |
| `test_archive_ingestor.py` (13) | Period inference (ISO/month-name), ticker from path, production filename detection, EDGAR hit parsing, CIK registry |
| `test_ir_scraper.py` (8) | Production PR detection, financial results rejection, period from title |
| `test_migrations_v7.py` (16) | Schema v7 migration: new company columns, asset_manifest.mutation_log, regime_config, metric_schema (UNIQUE), scrape_queue, seed count=13 |
| `test_coverage_logic.py` (new) | compute_cell_state_v2 (7-state), compute_expected_periods (monthly/quarterly), rank_extractions (analyst > confidence > recency) |
| `test_regime_db.py` (4) | Regime window CRUD: upsert, list, delete, 404 on missing ticker |
| `test_scrape_queue_db.py` (7) | Enqueue, claim, complete, fail, reset_interrupted, reject skip-mode, status listing |
| `test_metric_schema_db.py` (3) | Seed count=13, add custom metric, duplicate key rejected |
| `test_scrape_worker.py` (4) | Worker starts/stops, processes one job, marks error on failure, resets interrupted on startup |

---

## Test Data

Real files in archive for manual testing:
- `OffChain/Miner/Miner Monthly/MARA MONTHLY/2024-09-03_Marathon_Digital_Holdings_Announces_Bitcoin_1403.pdf`
- `OffChain/Miner/Miner Monthly/RIOT MONTHLY/Riot Announces September 2024 Production and Operations Updates _ Riot Platforms.html`

Real tickers with data (after archive ingest): MARA, RIOT
