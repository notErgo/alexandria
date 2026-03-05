# Data, UI, and LLM Interaction Documentation

Status: Complete  
Last Updated: 2026-03-05

## Scope

This document describes how report data moves through the Miners system from ingestion UI actions to LLM extraction writes.

## Runtime Components

| Component | File | Role |
|---|---|---|
| Ops UI | `templates/ops.html` | Starts ingestion and extraction tasks, polls progress endpoints |
| Ingest routes | `src/routes/reports.py` | Launches background acquisition tasks |
| Extraction route | `src/routes/operations.py` | Launches background extraction tasks |
| Scrape queue route | `src/routes/scrape.py` | Enqueues ticker scrape jobs |
| Scrape worker | `src/scrapers/scrape_worker.py` | Processes queued jobs (ingest only) |
| Archive ingestor | `src/scrapers/archive_ingestor.py` | Ingests archive files; runs inline extraction for monthly archive docs |
| IR scraper | `src/scrapers/ir_scraper.py` | Fetches IR PR text and stores reports |
| EDGAR connector | `src/scrapers/edgar_connector.py` | Fetches 8-K, 10-Q, 10-K text and stores reports |
| Extraction pipeline | `src/extractors/extraction_pipeline.py` | LLM + regex + agreement routing into data tables |
| DB API | `src/infra/db.py` | Stores reports and extracted outputs, tracks `reports.extracted_at` |

## Data Stores and Write Ownership

| Table | Written by | Meaning |
|---|---|---|
| `reports` | archive ingestor, IR scraper, EDGAR connector | Raw ingested report text |
| `reports.extracted_at` | extraction pipeline | Timestamp indicating report has been processed by extraction |
| `data_points` | extraction pipeline | Accepted metric values |
| `review_queue` | extraction pipeline | Disagreement or low-confidence outputs |
| `scrape_queue` | scrape API + scrape worker | Pending/running/completed scrape jobs |

## UI to Backend Flow

### 1. Ops: Acquire Archive

- UI action: `Acquire Archive` button (2.1.7).
- Endpoint: `POST /api/ingest/archive`.
- Worker: `ArchiveIngestor.ingest_all()`.
- Behavior:
  - Ingests archive files into `reports`.
  - Runs extraction inline for monthly archive files during ingest.
  - Marks processed reports via `reports.extracted_at`.

### 2. Ops: Acquire IR

- UI action: `Acquire IR` button (2.1.7).
- Endpoint: `POST /api/ingest/ir`.
- Request options: `{ "auto_extract": true|false, "warm_model": true|false }`.
- Worker: `_run_ir_ingest()`.
- Behavior:
  - Always ingests IR reports into `reports`.
  - If `auto_extract=true`, same task then runs extraction over unextracted reports before completion.
  - When extraction is about to run and `warm_model=true` (default), Ollama model warmup is attempted once per TTL window.
  - If `auto_extract=false`, extraction must be started separately.

### 3. Ops: Acquire EDGAR

- UI action: `Acquire EDGAR` button (2.1.7).
- Endpoint: `POST /api/ingest/edgar`.
- Request options: `{ "auto_extract": true|false, "warm_model": true|false }`.
- Worker: `_run_edgar_ingest()`.
- Behavior:
  - Ingests EDGAR 8-K, 10-Q, and 10-K filings into `reports`.
  - If `auto_extract=true`, same task then runs extraction over unextracted reports before completion.
  - When extraction is about to run and `warm_model=true` (default), Ollama model warmup is attempted once per TTL window.
  - If `auto_extract=false`, extraction must be started separately.

### 4. Ops: Start Extraction

- UI action: `Start Extraction` button (2.1.8).
- Endpoint: `POST /api/operations/extract`.
- Request option: `{ "warm_model": true|false }` (default `true`; only used when there are reports to process).
- Worker: `operations_extract()` background thread.
- Behavior:
  - Reads from `db.get_unextracted_reports()` unless `force=true`.
  - Optionally warms Ollama just before extraction when `warm_model=true`.
  - Calls `extract_report()` per report.
  - Writes accepted values to `data_points` and review items to `review_queue`.

### 5. Scrape Queue Trigger

- UI/API action: `POST /api/scrape/trigger/<ticker>`.
- Worker: `ScrapeWorker` daemon.
- Behavior:
  - Runs IR scrape and EDGAR fetch for that ticker.
  - Ingest only. Does not run extraction automatically.

## Overnight Processing Guarantees

### Guaranteed with one click flow

If you run `Acquire IR` or `Acquire EDGAR` with `auto_extract=true`, the task does both phases in order:
1. Ingest into `reports`.
2. Extract unextracted reports into `data_points` and `review_queue`.

Task status remains running until both phases complete.

### Not guaranteed without auto-extract

If `auto_extract=false`, ingestion completion only means reports were stored. LLM extraction will not run until `POST /api/operations/extract` is triggered.

### Scrape queue behavior

Queued scrape jobs ingest data only. For full ingest+LLM processing after queue jobs, run extraction explicitly or use the ingest endpoints with `auto_extract=true`.

## Monthly Reports and Corporate Filings Coverage

- Monthly IR reports: ingested by IR scraper; extracted when extraction stage runs.
- Corporate filings:
  - 8-K: ingested by EDGAR connector.
  - 10-Q and 10-K: ingested by EDGAR connector.
  - All are eligible for extraction when extraction stage runs.

## Verification Checklist After Overnight Run

1. Confirm ingest task completed: `GET /api/ingest/<task_id>/progress` returns `status=complete`.
2. Confirm extraction happened in same task when expected: progress payload includes `reports_extracted` and extraction counters.
3. Confirm no remaining unextracted backlog in DB: `reports` rows with `extracted_at IS NULL` should be zero or expected carry-over from active ingest.
4. Confirm output tables advanced: `data_points` count and `review_queue` count increased relative to pre-run baseline.

## Operational Best-Practice Pattern

Use a two-stage contract with explicit chaining control:
- Stage A (`ingest`): fetch + store report text.
- Stage B (`extract`): LLM/regex extraction and routing.
- Chain flag (`auto_extract`): when true, stage B is executed in the same background task after stage A.

This pattern keeps failure boundaries clear while supporting unattended end-to-end runs.

## Logging and Monitoring Contract

Operational monitoring must combine API counters with structured logs.

### Log Location

- Default file: `~/Documents/Hermeneutic/data/miners/logs/miners.log`
- Rotation: size-based via `RotatingFileHandler`.
- Config:
  - `MINERS_LOG_FILE` (exact path override),
  - `MINERS_LOG_DIR` (directory override),
  - `MINERS_LOG_MAX_BYTES`,
  - `MINERS_LOG_BACKUP_COUNT`.

### Required Structured Events

For probe/scrape/extract flows, logs should emit:
- `*_start` with control inputs,
- per-item progress/failures,
- `*_end` with outcome counters.

Examples of fields:
- `event`, `ticker`, `task_id`,
- `apply_mode`, `stale_days`, `timeout`,
- `status`, `recommended_mode`, `applied`,
- `targeted`, `completed`, `failed`, `error`.

### Why This Is Required

UI snapshots are current-state views only. Logs are the source of truth for:
- execution chronology,
- root-cause debugging,
- post-run auditability,
- unattended overnight run verification.
