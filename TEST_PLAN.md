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

### 2.1 Web UI (`/`)
- [ ] `GET /` → 200, filter form renders with all 13 companies in dropdown
- [ ] Submitting empty form → table shows data (if DB populated) or "No data found"
- [ ] Export CSV button → downloads `miners_export.csv`

### 2.2 Review Queue (`/review`)
- [ ] `GET /review` → 200, tabs render (Pending / Approved / Rejected / Edited)
- [ ] Pending tab loads items from `/api/review?status=PENDING`
- [ ] Approve button → card changes to "Approved" without page reload
- [ ] Edit form → accepts corrected value + note; card changes to "Edited"

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
- [ ] Navigation links correct on all pages (Data Explorer, Review Queue, Coverage, Operations)
- [ ] Dark/light theme toggle persists across page refresh (localStorage)
- [ ] 404 page renders for unknown routes
- [ ] `/api/*` routes return JSON errors; page routes render HTML errors

### 2.8 Coverage Dashboard (`/coverage`)
- [ ] `GET /coverage` → 200, coverage.html renders
- [ ] Summary strip loads 6 stat boxes from `/api/coverage/summary`
- [ ] Grid loads for default 36 months with all 13 tickers visible
- [ ] Clicking a cell shows cell detail panel with manifest + report info
- [ ] Close button hides cell detail panel

#### 2.8 API Endpoints
```bash
# Coverage summary
curl -s http://localhost:5004/api/coverage/summary | python3 -m json.tool

# Coverage grid (default 36 months)
curl -s "http://localhost:5004/api/coverage/grid" | python3 -c "import json,sys; d=json.load(sys.stdin); print('tickers:', len([k for k in d['data']['grid'] if k != 'summary']))"

# Coverage grid (custom months)
curl -s "http://localhost:5004/api/coverage/grid?months=12" | python3 -m json.tool

# Invalid months
curl -s "http://localhost:5004/api/coverage/grid?months=0"
# Expected: 400

# Cell detail
curl -s "http://localhost:5004/api/coverage/assets/MARA/2024-01-01" | python3 -m json.tool

# Manifest scan (dry run — does real filesystem scan)
curl -s -X POST http://localhost:5004/api/manifest/scan | python3 -m json.tool
```

### 2.9 Operations Panel (`/operations`)
- [ ] `GET /operations` → 200, operations.html renders
- [ ] Queue loads with pending extraction table + legacy files
- [ ] Extract button triggers extraction and shows progress
- [ ] Assign period form validates YYYY-MM-01 format before submitting
- [ ] Sync Archive button triggers manifest scan and refreshes queue

#### 2.9 API Endpoints
```bash
# Operations queue
curl -s http://localhost:5004/api/operations/queue | python3 -m json.tool

# Trigger extraction (MARA)
curl -s -X POST http://localhost:5004/api/operations/extract \
  -H "Content-Type: application/json" -d '{"ticker":"MARA"}' | python3 -m json.tool

# Poll progress (use task_id from above)
curl -s "http://localhost:5004/api/operations/extract/<task_id>/progress" | python3 -m json.tool

# Assign period
curl -s -X POST http://localhost:5004/api/operations/assign_period \
  -H "Content-Type: application/json" -d '{"manifest_id":1,"period":"2024-01-01"}' | python3 -m json.tool
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
# Expected: 454 passed
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

---

## Test Data

Real files in archive for manual testing:
- `OffChain/Miner/Miner Monthly/MARA MONTHLY/2024-09-03_Marathon_Digital_Holdings_Announces_Bitcoin_1403.pdf`
- `OffChain/Miner/Miner Monthly/RIOT MONTHLY/Riot Announces September 2024 Production and Operations Updates _ Riot Platforms.html`

Real tickers with data (after archive ingest): MARA, RIOT
