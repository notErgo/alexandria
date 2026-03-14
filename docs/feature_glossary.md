# Miners Platform — Feature Glossary

Companion to [`UI_SPEC.md`](../UI_SPEC.md). Spec IDs (`3.1.1`, `MD5.3.1`, etc.) are defined there.

This document establishes canonical names for features, pipeline stages, data entities, and status
values. Use these terms when writing code, comments, bug reports, tests, and log messages.
Where a term is ambiguous, the **Avoid** rows say which word not to use.

---

## Pipeline Stages

The platform operates a strict two-stage pipeline. The stages are separate and cannot be
reversed.

| Stage | Canonical name | What it does | Writes to |
|-------|---------------|--------------|-----------|
| 1 | **Ingest** | Fetch and store raw source documents | `reports`, `asset_manifest` |
| 2 | **Extract** | Run LLM on stored reports; route results to data store or review queue | `data_points`, `review_queue`, `reports.extracted_at` |

> Ingest never runs the LLM. Extract never fetches from the internet.
> The one exception is the ingest auto-chain (`auto_extract: true` in the ingest body), which
> runs both stages sequentially but keeps the stage boundary intact.

---

## Ingest Sub-paths (Stage 1)

| Sub-path | Canonical name | Source |
|----------|---------------|--------|
| IR scrape | **IR Ingest** | Company investor-relations pages (RSS, index, or template mode) |
| EDGAR fetch | **EDGAR Ingest** | SEC EDGAR API (8-K, 10-Q, 10-K, 6-K, 20-F, 40-F) |
| Archive scan | **Archive Ingest** | Local `OffChain/Miner/Miner Monthly/` PDF/HTML files |
| LLM crawl | **Crawl** | LLM-directed web crawl; outputs candidate documents, not extraction results |

**Avoid**: "scrape" as a generic term for all of Stage 1. It is correct only for IR Ingest.

---

## Extract Sub-paths (Stage 2)

| Sub-path | Canonical name | Spec ID | Scope | Trigger |
|----------|---------------|---------|-------|---------|
| Bulk LLM Extraction | **Bulk Extraction** | 3.1.1 | Multi-ticker or ALL; runs on stored unextracted reports | `startExtractionRun()` → `POST /api/operations/interpret` |
| Single-ticker extraction | **Miner Extraction** | MD5.3.1 | One ticker at a time; always force-mode; launched from Miner Data page | `doExtract()` → `POST /api/operations/interpret` |
| Gap re-extraction | **Gap Re-extract** | 3.1.2 | Finds done-reports missing a metric; resets and re-runs LLM | `runRequeueMissing()` → `POST /api/operations/requeue-missing` |
| Math gap-fill | **Gap-fill** | 3.1.1 checkbox | Derives missing monthly rows from quarterly totals already in DB; no LLM | `POST /api/operations/gap-fill` |

**Avoid**: "interpret" as a synonym for extraction in UI copy and bug reports. "Interpret" is the
tab name (ops tab 3.0). The operation itself is **Extraction**. Say "Bulk Extraction ran" not
"the interpret layer ran."

---

## Core Data Entities

| Entity | DB table | What it represents |
|--------|----------|--------------------|
| **Report** | `reports` | One stored source document (press release, 8-K, archive file). Stage 1 output. |
| **Data Point** | `data_points` | One auto-accepted metric value for a ticker+period. High-confidence extraction output. |
| **Review Item** | `review_queue` | One candidate metric value routed for analyst decision. Low-confidence or outlier. |
| **Final Data Point** | `final_data_points` | Analyst-finalized value; takes precedence over raw data points in all read paths. |
| **Asset Manifest Entry** | `asset_manifest` | One trackable document discovered by the manifest scanner. Used for coverage accounting. |
| **Regime Window** | `regime_config` | A declared date range with a known reporting cadence for a company. |

---

## Report `extraction_status` Values

| Value | Meaning |
|-------|---------|
| `pending` | Stored but not yet run through Stage 2 |
| `running` | Extraction thread has claimed this report |
| `done` | Stage 2 completed (LLM ran, or keyword-gated skip recorded) |
| `keyword_gated` | Keyword gate found no mining phrases; skipped by design |
| `parse_failed` | Parser could not extract usable text from the document |

**Silent-skip pattern**: `get_unextracted_reports()` returns only `pending` reports. If all
reports are `done`, Bulk Extraction finds zero reports and completes immediately with
`reports=0 data_points=0`. This looks like nothing happened. Use **Force** or **Full Reset**
mode to re-run on already-extracted reports.

---

## Bulk Extraction Run Modes (3.1.1)

| Mode | Canonical name | What it does |
|------|---------------|--------------|
| `resume` | **Resume** | Processes only `pending` reports. Default. |
| `force` | **Force re-extract** | Re-runs all reports regardless of `extraction_status`. Does not delete existing data points. |
| `reset` | **Full Reset** | Purges `data_points` and `review_queue` for the selected tickers, then re-runs all reports. Requires a ticker selection. Destructive. |

---

## Review Item `status` Values

| Value | Meaning |
|-------|---------|
| `PENDING` | Awaiting analyst decision |
| `APPROVED` | Analyst accepted the LLM value; written to `data_points` |
| `EDITED` | Analyst overrode the LLM value; written to `data_points` with analyst method |
| `REJECTED` | Analyst discarded; not written to `data_points` |

The Review Queue filter bar (4.1) defaults to **All statuses**. Filtering to `PENDING` shows
only undecided items; an empty result with this filter means the queue has been fully processed,
not that extraction failed.

---

## Coverage Cell States

Nine-state cell vocabulary used in the Explorer (5.1) and period pipeline trace (5.1.2).
Defined in `src/coverage_logic.py:compute_cell_state_v2`.

| State | Meaning |
|-------|---------|
| `has_data` | Accepted data point exists |
| `review_pending` | One or more review items waiting for analyst decision |
| `analyst_gap` | Analyst explicitly marked this cell as a known gap |
| `llm_empty` | LLM ran and returned no value for this metric |
| `keyword_gated` | Document present but no mining keywords found; LLM not run |
| `parse_failed` | Parser could not extract text from the document |
| `no_document` | No ingested report covers this ticker+period |
| `pending_extraction` | Document ingested but extraction has not run yet |
| `out_of_range` | Period is outside the company's declared activity window |

---

## Shared Ticker Selection State

The ticker bar visible in **Ingest** (2.x), **Bulk Extraction** (3.1.1), and the
**pipeline overnight run** all read from and write to the same JS state: `_selectedCrawlTickers`.

Toggling a ticker in any of those three panels immediately syncs all others. The shared
state name is a historical artifact of the crawl feature; its canonical function is
**shared pipeline ticker scope**.

**Miner Extraction** (MD5.3.1) and **Gap Re-extract** (3.1.2) each have their own independent
ticker scope (`_ticker` and `_selectedGapTickers` respectively).

---

## Element ID Conventions

To prevent collisions across the single `ops.html` document, element IDs must be scoped to
their panel using the spec-id prefix.

| Pattern | Applies to |
|---------|-----------|
| `3-1-1-<name>` | Panel 3.1.1 (Bulk Extraction) |
| `miner-<name>` | MD5.3.1 (Miner Extraction, miner_data.js) |
| `gap-<name>` | Panel 3.1.2 (Gap Re-extract) |
| `rv-<name>` | Panel 4.1 (Review Queue) |
| `ex-<name>` | Panel 5.1 (Explorer) |
| `reg-<name>` | Panel 5.2 (Registry) |

**Avoid**: unscoped generic IDs (`extract-status`, `extract-log`) when the same semantic exists
in more than one panel. Duplicate IDs silently break `getElementById`; only the first match
in DOM order is returned.

---

## Terms to Avoid

| Ambiguous term | Use instead | Why |
|----------------|-------------|-----|
| "interpret layer" | **Bulk Extraction** or **Extraction pipeline** | "Interpret" is a tab name, not an operation name |
| "scrape" (generic) | **Ingest** (generic) or the specific sub-path | Scrape is IR-specific; the generic stage is Ingest |
| "extract" when meaning ingest | **Ingest** | Extract = Stage 2 LLM operation only |
| "the review panel" | **Review Queue** (4.1) or **Miner Timeline** (4.0) | The Review tab has two sub-panes |
| "re-extract" without qualification | **Gap Re-extract** (3.1.2) or **Force re-extract** mode | These are different operations with different scopes |
| "run again" | Use the run mode name (**Resume / Force / Full Reset**) | Unambiguous for bug reports and operator instructions |
