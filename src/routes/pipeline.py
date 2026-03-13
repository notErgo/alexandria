"""Overnight pipeline orchestration routes."""
import json
import logging
import os
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from datetime import datetime, timezone

from flask import Blueprint, jsonify, request

from config import MONTHLY_EXTRACTION_SOURCE_TYPES, IR_REQUEST_DELAY_SECONDS, EDGAR_REQUEST_DELAY_SECONDS

log = logging.getLogger('miners.routes.pipeline')
bp = Blueprint('pipeline', __name__)

_run_cancel_flags: dict[int, threading.Event] = {}
_run_threads: dict[int, threading.Thread] = {}
_run_lock = threading.Lock()

# UI contract: maps each user-facing pipeline parameter to the HTML element id
# that reads and sends it.  test_pipeline_ui_params_wired enforces that every
# entry here has a matching element in templates/ops.html.  Add a row here
# whenever a new boolean/select is added to start_overnight_pipeline, so the
# gap between backend capability and UI exposure is caught at test time.
PIPELINE_UI_PARAMS: dict[str, str] = {
    'include_ir':       'pipeline-include-ir',
    'include_crawl':    'pipeline-include-crawl',
    'force_reextract':  'pipeline-force-reextract',
}


# Source-type sets used by _build_extraction_batch.
# EDGAR types are gated by btc_first_filing_date (pre-mining-pivot guard).
# IR/archive types are fetched from mining-specific IR pages and are always
# relevant regardless of the pivot date.
_EDGAR_SOURCE_TYPES = (
    'edgar_8k', 'edgar_10k', 'edgar_10q',
    'edgar_6k', 'edgar_20f', 'edgar_40f',
)
_NON_EDGAR_SOURCE_TYPES = MONTHLY_EXTRACTION_SOURCE_TYPES + ('archive_html', 'archive_pdf')


def _report_chronology_key(report: dict) -> tuple:
    report_date = report.get('report_date') or ''
    source_type = report.get('source_type') or ''
    if source_type in MONTHLY_EXTRACTION_SOURCE_TYPES:
        source_rank = 0
    elif source_type in ('edgar_8k', 'edgar_8ka'):
        source_rank = 1
    elif source_type in ('edgar_10q', 'edgar_6k'):
        source_rank = 2
    elif source_type in ('edgar_10k', 'edgar_20f', 'edgar_40f'):
        source_rank = 3
    else:
        source_rank = 4
    return (report_date, source_rank, int(report.get('id') or 0))


def _sort_reports_chronologically(reports: list[dict]) -> list[dict]:
    return sorted(reports, key=_report_chronology_key)


class _BufferedExtractionDB:
    """Record extraction writes so they can be committed in chronological order."""

    _BUFFERED_METHODS = {
        'insert_data_point',
        'insert_review_item',
        'insert_benchmark_run',
        'update_report_summary',
        'refresh_review_precedence_for_month',
        'mark_report_extracted',
        'mark_report_extraction_failed',
    }

    def __init__(self, base_db):
        self._base_db = base_db
        self._actions: list[dict] = []

    def __getattr__(self, name):
        if name in self._BUFFERED_METHODS:
            def _buffered(*args, **kwargs):
                payload = kwargs if kwargs else list(args)
                self._actions.append({'method': name, 'payload': payload})
                return len(self._actions)
            return _buffered
        return getattr(self._base_db, name)

    def staged_payload(self) -> dict:
        return {'actions': list(self._actions)}


def _replay_staged_payload(db, payload: dict) -> None:
    for action in (payload or {}).get('actions', []):
        method = action.get('method')
        op_payload = action.get('payload')
        fn = getattr(db, method)
        if isinstance(op_payload, list):
            fn(*op_payload)
        elif isinstance(op_payload, dict):
            fn(**op_payload)
        else:
            fn(op_payload)


def _summary_to_dict(summary) -> dict:
    return {
        'reports_processed': int(summary.reports_processed or 0),
        'data_points_extracted': int(summary.data_points_extracted or 0),
        'review_flagged': int(summary.review_flagged or 0),
        'errors': int(summary.errors or 0),
        'keyword_gated': int(summary.keyword_gated or 0),
        'prompt_tokens': int(summary.prompt_tokens or 0),
        'response_tokens': int(summary.response_tokens or 0),
        'temporal_rejects': int(getattr(summary, 'temporal_rejects', 0) or 0),
    }


def _staged_status_for_payload(payload: dict) -> str:
    for action in (payload or {}).get('actions', []):
        if action.get('method') == 'mark_report_extraction_failed':
            return 'failed'
    return 'staged'


def _build_extraction_batch(db, ticker: str, first_filing, force_reextract: bool = False) -> list:
    """Collect reports eligible for extraction for one ticker.

    EDGAR source types are gated by btc_first_filing_date so pre-pivot filings
    are never re-processed.  IR and archive types are NOT date-gated — they are
    scraped from mining-specific IR pages and are always mining-relevant.
    """
    if force_reextract:
        batch = db.get_all_reports_for_extraction(ticker=ticker)
        for r in batch:
            db.reset_report_extraction_status(r['id'])
        if first_filing:
            batch = [
                r for r in batch
                if not r.get('source_type', '').startswith('edgar')
                or r.get('report_date', '') >= first_filing
            ]
        return _sort_reports_chronologically(batch)
    else:
        edgar_batch = db.get_unextracted_reports(
            ticker=ticker,
            source_types=list(_EDGAR_SOURCE_TYPES),
            from_period=first_filing,
        )
        non_edgar_batch = db.get_unextracted_reports(
            ticker=ticker,
            source_types=list(_NON_EDGAR_SOURCE_TYPES),
        )
        return _sort_reports_chronologically(edgar_batch + non_edgar_batch)


class _RunCancelled(Exception):
    """Internal signal for cooperative pipeline cancellation."""


def _mark_cancel_requested(run_id: int) -> None:
    with _run_lock:
        evt = _run_cancel_flags.get(run_id)
        if evt is None:
            evt = threading.Event()
            _run_cancel_flags[run_id] = evt
        evt.set()


def _is_cancelled(run_id: int) -> bool:
    with _run_lock:
        evt = _run_cancel_flags.get(run_id)
        return bool(evt and evt.is_set())


def _is_run_thread_alive(run_id: int) -> bool:
    with _run_lock:
        thread = _run_threads.get(run_id)
        return bool(thread and thread.is_alive())


def _register_run_thread(run_id: int, thread: threading.Thread) -> None:
    with _run_lock:
        _run_threads[run_id] = thread


def _clear_run_thread(run_id: int) -> None:
    with _run_lock:
        _run_threads.pop(run_id, None)


def _cleanup_orphaned_process_runs(db) -> list[int]:
    """Mark DB runs as stopped when this process has no live thread for them."""
    orphaned = []
    for run in db.list_pipeline_runs_by_status(['queued', 'running']):
        if not _is_run_thread_alive(int(run['id'])):
            orphaned.append(int(run['id']))
    for run_id in orphaned:
        db.update_pipeline_run(
            run_id,
            status='stopped',
            ended_at=datetime.now(timezone.utc).isoformat(),
            summary={'recovered': True, 'reason': 'thread_missing'},
            error='thread_missing',
        )
        db.add_pipeline_run_event(
            run_id,
            stage='run',
            event='pipeline_run_recovered',
            level='WARNING',
            details={'reason': 'thread_missing'},
        )
    return orphaned


def _recover_stale_report_claims(db) -> int:
    """Release report rows left in extraction_status='running' without a live run."""
    active_runs = [
        run for run in db.list_pipeline_runs_by_status(['queued', 'running'])
        if _is_run_thread_alive(int(run['id']))
    ]
    if active_runs:
        return 0
    return db.reset_interrupted_report_extractions()


def _event(db, run_id: int, stage: str, event: str, *, ticker: str = None, level: str = 'INFO', **details) -> None:
    db.add_pipeline_run_event(
        run_id=run_id,
        stage=stage,
        event=event,
        ticker=ticker,
        level=level,
        details=details or {},
    )
    msg = f"event={event} run_id={run_id} stage={stage}"
    if ticker:
        msg += f" ticker={ticker}"
    if details:
        msg += " " + " ".join(f"{k}={v}" for k, v in details.items())
    if level == 'WARNING':
        log.warning(msg)
    else:
        log.info(msg)


def _count_reports_for_tickers(db, tickers: list[str]) -> int:
    if not tickers:
        return 0
    placeholders = ",".join("?" for _ in tickers)
    with db._get_connection() as conn:  # private helper is acceptable inside service boundary
        row = conn.execute(
            f"SELECT COUNT(*) AS c FROM reports WHERE ticker IN ({placeholders})",
            tickers,
        ).fetchone()
        return int(row['c'] or 0)


def _normalize_keywords(raw) -> list[str]:
    if raw is None:
        return []
    if isinstance(raw, str):
        parts = [p.strip() for p in raw.split(',')]
        return [p.lower() for p in parts if p]
    if isinstance(raw, list):
        out = []
        for item in raw:
            s = str(item).strip()
            if s:
                out.append(s.lower())
        return out
    return []


def _should_run_scout(
    *,
    mode: str,
    tickers: list[str],
    output_dir: str,
    max_age_hours: int,
    keywords: list[str],
) -> tuple[bool, str]:
    mode = (mode or 'auto').strip().lower()
    if mode == 'never':
        return False, 'mode_never'
    if mode == 'always':
        return True, 'mode_always'
    # auto mode: only run when useful or stale
    if keywords:
        return True, 'keywords_requested'

    out_dir = Path(output_dir)
    now = time.time()
    max_age_seconds = max(1, int(max_age_hours)) * 3600
    for ticker in tickers:
        p = out_dir / f'coverage_scout_{ticker}.json'
        if not p.exists():
            return True, f'missing_artifact:{ticker}'
        age = now - p.stat().st_mtime
        if age > max_age_seconds:
            return True, f'stale_artifact:{ticker}'
    return False, 'fresh_artifacts'


def _run_coverage_scout_stage(
    db,
    run_id: int,
    tickers: list[str],
    config: dict,
):
    # Late import to avoid adding script path unless stage is enabled.
    project_root = Path(__file__).resolve().parents[2]
    if str(project_root) not in os.sys.path:
        os.sys.path.insert(0, str(project_root))
    from scripts.run_coverage_scout import build_coverage_scout_for_ticker
    import requests
    from datetime import date

    output_dir = str(config.get('scout_output_dir') or '/private/tmp/claude-501/miners_progress')
    Path(output_dir).mkdir(parents=True, exist_ok=True)
    metric = str(config.get('scout_metric') or 'production_btc')
    keywords = _normalize_keywords(config.get('scout_keywords'))
    as_of_raw = str(config.get('scout_as_of_date') or date.today().isoformat())
    as_of = date.fromisoformat(as_of_raw)

    session = requests.Session()
    session.headers['User-Agent'] = os.environ.get(
        'EDGAR_USER_AGENT',
        'Hermeneutic Research Platform contact@example.com',
    )
    session.headers['Accept'] = 'application/json'

    _event(
        db, run_id, 'coverage_scout', 'stage_start',
        tickers=len(tickers), metric=metric, keywords=len(keywords),
    )
    results = []
    failures = []
    for t in tickers:
        if _is_cancelled(run_id):
            raise _RunCancelled()
        try:
            result = build_coverage_scout_for_ticker(
                db=db,
                ticker=t,
                as_of=as_of,
                metric=metric,
                session=session,
                keywords=keywords,
            )
            out_path = Path(output_dir) / f'coverage_scout_{t}.json'
            with open(out_path, 'w') as f:
                json.dump(result, f, indent=2)
            s = result.get('summary') or {}
            results.append({
                'ticker': t,
                'coverage_ratio': s.get('coverage_ratio'),
                'high_priority_gaps': s.get('high_priority_gaps'),
                'expected_period_count': s.get('expected_period_count'),
                'edgar_error': s.get('edgar_error'),
                'artifact': str(out_path),
            })
            _event(
                db, run_id, 'coverage_scout', 'ticker_scout_done',
                ticker=t,
                coverage_ratio=s.get('coverage_ratio'),
                high_priority_gaps=s.get('high_priority_gaps'),
            )
        except Exception as e:
            failures.append({'ticker': t, 'error': str(e)})
            _event(
                db, run_id, 'coverage_scout', 'ticker_scout_failed',
                ticker=t, level='WARNING', error=str(e),
            )

    summary_path = Path(output_dir) / 'coverage_scout_summary.json'
    with open(summary_path, 'w') as f:
        json.dump({
            'as_of_date': as_of.isoformat(),
            'tickers': tickers,
            'metric': metric,
            'keywords': keywords,
            'results': results,
            'failures': failures,
        }, f, indent=2)
    _event(
        db, run_id, 'coverage_scout', 'stage_end',
        completed=len(results), failed=len(failures), summary_path=str(summary_path),
    )
    return {
        'ran': True,
        'metric': metric,
        'keywords': keywords,
        'output_dir': output_dir,
        'summary_path': str(summary_path),
        'results': results,
        'failures': failures,
    }


def _warm_pipeline_ollama_if_needed(db, run_id: int) -> None:
    from infra.ollama_warmup import warm_ollama_for_extraction, ensure_ollama_running

    def _pipeline_ollama_log(msg: str) -> None:
        _event(db, run_id, 'extract', 'ollama_status', message=msg)

    ensure_ollama_running(log_fn=_pipeline_ollama_log)
    warmup_result = warm_ollama_for_extraction(
        db=db,
        reason='pipeline_overnight_extract',
        force=True,
    )
    _event(
        db, run_id, 'extract', 'ollama_warmup',
        warmed=int(bool(warmup_result.get('warmed'))),
        model=warmup_result.get('model', ''),
        reason=warmup_result.get('reason', ''),
    )
    if warmup_result.get('warmed'):
        # Invalidate the LLM availability cache so the first extract_report
        # call gets a fresh check (not a stale False from a prior failed run).
        try:
            import interpreters.interpret_pipeline as _ipl
            _ipl._llm_available_cache_time = 0.0
        except Exception:
            pass


def prepare_extraction_runtime(
    db,
    *,
    warm_model: bool,
    reason: str,
    log_fn=None,
    run_id: int | None = None,
) -> dict:
    """Shared extraction runtime prep for pipeline and manual extraction flows."""
    released = _recover_stale_report_claims(db)
    if released:
        msg = f"Released {released} stale extraction claims"
        if run_id is not None:
            _event(db, run_id, 'extract', 'stale_claims_recovered', released=released)
        elif log_fn:
            log_fn(msg)

    if not warm_model:
        return {'released_claims': released, 'warmed': False}

    if run_id is not None:
        _warm_pipeline_ollama_if_needed(db, run_id)
        return {'released_claims': released, 'warmed': True}

    from infra.ollama_warmup import warm_ollama_for_extraction, ensure_ollama_running

    ensure_ollama_running(log_fn=log_fn)
    warmup_result = warm_ollama_for_extraction(db=db, reason=reason, force=True)
    if warmup_result.get('warmed'):
        try:
            import interpreters.interpret_pipeline as _ipl
            _ipl._invalidate_llm_availability_cache()
        except Exception:
            pass
    return {'released_claims': released, 'warmed': bool(warmup_result.get('warmed'))}


def _extract_reports_for_ticker(
    db,
    run_id: int,
    ticker: str,
    reports: list,
    registry,
    counters: dict,
    failures: list,
    num_workers: int,
    *,
    run_config=None,
    force_reextract: bool = False,
) -> None:
    from interpreters.interpret_pipeline import extract_report
    from infra.db import MinerDB

    if not reports:
        _event(db, run_id, 'extract', 'ticker_skipped', ticker=ticker, reason='no_pending_reports')
        return

    ordered_reports = _sort_reports_chronologically(reports)
    effective_workers = max(1, min(int(num_workers), len(ordered_reports)))
    ticker_processed = ticker_data_points = ticker_errors = 0
    ticker_keyword_gated = 0
    counter_lock = threading.Lock()
    _event(
        db, run_id, 'extract', 'ticker_start',
        ticker=ticker, pending_reports=len(ordered_reports), workers=effective_workers,
        strict_chronology=1,
    )

    def _record_report_success(report: dict, summary, worker_id: int) -> None:
        nonlocal ticker_processed, ticker_data_points, ticker_errors
        nonlocal ticker_keyword_gated
        period = report.get('report_date', '')
        source_type = report.get('source_type', '')
        llm_used = bool((summary.prompt_tokens or 0) > 0 or (summary.response_tokens or 0) > 0)
        with counter_lock:
            counters['processed'] += summary.reports_processed
            counters['data_points'] += summary.data_points_extracted
            counters['errors'] += summary.errors
            counters['keyword_gated'] += summary.keyword_gated
            counters['report_done_count'] += 1

            ticker_processed += summary.reports_processed
            ticker_data_points += summary.data_points_extracted
            ticker_errors += summary.errors
            ticker_keyword_gated += summary.keyword_gated
            progress = counters['report_done_count']
            running_total_dp = counters['data_points']

        _event(
            db, run_id, 'extract', 'report_done',
            ticker=ticker, report_id=report.get('id'), period=period, source_type=source_type,
            worker_id=worker_id,
            llm_used=int(llm_used),
            data_points=summary.data_points_extracted,
            review_flagged=summary.review_flagged,
            progress=progress,
            total=counters['total_reports'],
            running_total_dp=running_total_dp,
            prompt_tokens=summary.prompt_tokens,
            response_tokens=summary.response_tokens,
        )

    def _record_report_failure(report: dict, error: Exception, worker_id: int) -> None:
        nonlocal ticker_errors
        period = report.get('report_date', '')
        source_type = report.get('source_type', '')
        with counter_lock:
            counters['errors'] += 1
            ticker_errors += 1
            failures.append({'ticker': ticker, 'error': str(error)})
        _event(
            db, run_id, 'extract', 'report_extract_failed',
            ticker=ticker, level='WARNING',
            worker_id=worker_id,
            report_id=report.get('id'), period=period,
            source_type=source_type, error=str(error),
        )

    claim_db = MinerDB(db.db_path)
    claim_index = 0
    ordered_iter = iter(ordered_reports)

    def _run_buffered_extraction(claimed_report: dict, worker_id: int) -> dict:
        local_db = MinerDB(db.db_path)
        buffered_db = _BufferedExtractionDB(local_db)
        summary = extract_report(claimed_report, buffered_db, registry, config=run_config)
        payload = buffered_db.staged_payload()
        return {
            'report': claimed_report,
            'worker_id': worker_id,
            'summary': summary,
            'payload': payload,
            'queue_status': _staged_status_for_payload(payload),
        }

    def _claim_next_report() -> tuple[dict, int] | None:
        nonlocal claim_index
        for report in ordered_iter:
            if _is_cancelled(run_id):
                return None
            report_id = report.get('id')
            if report_id is None:
                continue
            report_id = int(report_id)
            worker_id = claim_index % effective_workers
            claim_index += 1
            if force_reextract:
                claim_db.mark_report_extraction_running(report_id)
            else:
                if not claim_db.claim_report_for_extraction(report_id):
                    log.debug("pipeline worker=%d skipping report %d (already claimed)", worker_id, report_id)
                    continue
            claimed_report = claim_db.get_report(report_id)
            if not claimed_report:
                continue
            _event(
                db, run_id, 'extract', 'report_start',
                ticker=ticker,
                worker_id=worker_id,
                report_id=claimed_report.get('id'),
                period=claimed_report.get('report_date', ''),
                source_type=claimed_report.get('source_type', ''),
            )
            return claimed_report, worker_id
        return None

    with ThreadPoolExecutor(max_workers=effective_workers) as pool:
        inflight: list[tuple[dict, int, object]] = []
        while len(inflight) < effective_workers:
            claimed = _claim_next_report()
            if claimed is None:
                break
            claimed_report, worker_id = claimed
            inflight.append(
                (claimed_report, worker_id, pool.submit(_run_buffered_extraction, claimed_report, worker_id))
            )

        while inflight:
            claimed_report, worker_id, future = inflight.pop(0)
            if _is_cancelled(run_id):
                break
            try:
                result = future.result()
                summary = result['summary']
                payload = result['payload']
                queue_status = result['queue_status']
                db.enqueue_extraction_commit(
                    run_id=run_id,
                    ticker=ticker,
                    report_id=int(claimed_report['id']),
                    period=claimed_report.get('report_date', ''),
                    sequence_key="|".join(str(p) for p in _report_chronology_key(claimed_report)),
                    payload=payload,
                    summary=_summary_to_dict(summary),
                    status=queue_status,
                    error=None,
                )
                _replay_staged_payload(db, payload)
                db.finalize_extraction_commit(
                    int(claimed_report['id']),
                    status='failed' if queue_status == 'failed' else 'committed',
                )
                _record_report_success(claimed_report, summary, worker_id)
            except Exception as e:
                db.enqueue_extraction_commit(
                    run_id=run_id,
                    ticker=ticker,
                    report_id=int(claimed_report['id']),
                    period=claimed_report.get('report_date', ''),
                    sequence_key="|".join(str(p) for p in _report_chronology_key(claimed_report)),
                    payload=None,
                    summary=None,
                    status='failed',
                    error=str(e),
                )
                db.finalize_extraction_commit(int(claimed_report['id']), status='failed')
                db.mark_report_extraction_failed(int(claimed_report['id']), str(e)[:500])
                _record_report_failure(claimed_report, e, worker_id)
            if _is_cancelled(run_id):
                break
            claimed = _claim_next_report()
            if claimed is None:
                continue
            next_report, next_worker_id = claimed
            inflight.append(
                (next_report, next_worker_id, pool.submit(_run_buffered_extraction, next_report, next_worker_id))
            )

    db.upsert_pipeline_run_ticker(run_id, ticker, extracted=1)
    _event(
        db, run_id, 'extract', 'ticker_done',
        ticker=ticker,
        reports_processed=ticker_processed,
        data_points=ticker_data_points,
        errors=ticker_errors,
        keyword_gated=ticker_keyword_gated,
        workers=effective_workers,
    )


def _build_extraction_batch_for_source_types(
    db,
    ticker,
    source_types: list,
    force_reextract: bool = False,
) -> list:
    """Build extraction batch for an explicit source_types list.

    Unlike _build_extraction_batch, this does NOT apply btc_first_filing_date
    date-gating. Callers supply source_types to restrict scope; date-gating is
    their responsibility.
    """
    if force_reextract:
        batch = db.get_all_reports_for_extraction(ticker=ticker, source_types=source_types)
        for r in batch:
            db.reset_report_extraction_status(r['id'])
    else:
        batch = db.get_unextracted_reports(ticker=ticker, source_types=source_types)
    return _sort_reports_chronologically(batch)


def run_extraction_phase(
    db,
    run_id: int,
    tickers: list,
    registry,
    *,
    source_types=None,
    force_reextract: bool = False,
    warm_model: bool = True,
    extract_workers: int = 2,
    run_config_factory=None,
    cancel_check=None,
    progress_callback=None,
    prebuilt_batches=None,
    failures=None,
) -> dict:
    """Canonical extraction phase used by all extraction entry points.

    Emits stage_start, ticker_preflight (per ticker), and stage_end events.
    Handles Ollama warmup once (first ticker with pending reports).
    Calls _extract_reports_for_ticker per ticker.

    source_types=None: use _build_extraction_batch (EDGAR date-gated by btc_first_filing_date).
    source_types=<list>: use _build_extraction_batch_for_source_types (no date-gating).
    prebuilt_batches: if provided, skip batch-building entirely and use caller-supplied map.

    extract_workers: parallel LLM workers per ticker (default 2).
    cancel_check: callable() -> bool; called before each ticker; if True, stops.
    progress_callback: callable(counters_copy) -> None; called after each ticker.
    run_config_factory: callable(ticker) -> ExtractionRunConfig | None; called per ticker.
    failures: optional caller-supplied list for failure accumulation; creates new list if None.
    """
    counters = {
        'total_reports': 0,
        'report_done_count': 0,
        'processed': 0,
        'data_points': 0,
        'errors': 0,
        'keyword_gated': 0,
        'review_flagged': 0,
    }
    if failures is None:
        failures = []

    if not tickers:
        _event(db, run_id, 'extract', 'stage_start',
               mode='batched_by_ticker', total_tickers=0, total_reports=0)
        _event(db, run_id, 'extract', 'stage_end',
               reports_processed=0, data_points=0, errors=0, keyword_gated=0)
        return counters

    # Build all batches first (separated-phases contract: batch-build before any extraction)
    if prebuilt_batches is not None:
        ticker_batches = {t: prebuilt_batches.get(t, []) for t in tickers}
    else:
        ticker_batches = {}
        for ticker in tickers:
            if source_types is None:
                first_filing = db.get_btc_first_filing_date(ticker)
                batch = _build_extraction_batch(db, ticker, first_filing, force_reextract)
            else:
                batch = _build_extraction_batch_for_source_types(
                    db, ticker, source_types, force_reextract
                )
            ticker_batches[ticker] = batch

    counters['total_reports'] = sum(len(b) for b in ticker_batches.values())

    _event(db, run_id, 'extract', 'stage_start',
           mode='batched_by_ticker',
           total_tickers=len(tickers),
           total_reports=counters['total_reports'])

    ollama_prepared = False
    for ticker in tickers:
        if cancel_check and cancel_check():
            break
        reports = ticker_batches.get(ticker, [])
        _event(db, run_id, 'extract', 'ticker_preflight',
               ticker=ticker,
               pending_reports=len(reports),
               force_reextract=int(force_reextract),
               running_total_pending=counters['total_reports'])
        if reports and warm_model and not ollama_prepared:
            prepare_extraction_runtime(
                db, warm_model=True, reason='run_extraction_phase', run_id=run_id,
            )
            ollama_prepared = True
        run_config = run_config_factory(ticker) if run_config_factory else None
        _extract_reports_for_ticker(
            db=db,
            run_id=run_id,
            ticker=ticker,
            reports=reports,
            registry=registry,
            counters=counters,
            failures=failures,
            num_workers=max(1, int(extract_workers)),
            run_config=run_config,
            force_reextract=force_reextract,
        )
        if progress_callback:
            progress_callback(dict(counters))

    _event(db, run_id, 'extract', 'stage_end',
           reports_processed=counters['processed'],
           data_points=counters['data_points'],
           errors=counters['errors'],
           keyword_gated=counters['keyword_gated'])
    return counters


def _scrape_ticker_for_pipeline(
    *,
    db_path: str,
    run_id: int,
    ticker: str,
    include_ir: bool,
    ir_semaphore,
    edgar_semaphore,
    ir_throttle,
    edgar_throttle,
    host_backoff_seconds: float,
    max_retries: int,
) -> dict:
    import requests
    from datetime import date
    from config import ARCHIVE_DIR, CONFIG_DIR
    from infra.db import MinerDB
    from interpreters.pattern_registry import PatternRegistry
    from scrapers.archive_ingestor import ArchiveIngestor
    from scrapers.ir_scraper import IRScraper
    from scrapers.edgar_connector import EdgarConnector

    local_db = MinerDB(db_path)
    company = local_db.get_company(ticker)
    if company is None:
        return {'ticker': ticker, 'before_reports': 0, 'after_reports': 0, 'ingested_delta': 0, 'failures': ['ticker_not_found']}

    failures = []
    before_reports = _count_reports_for_tickers(local_db, [ticker])
    _event(local_db, run_id, 'ingest', 'ticker_start', ticker=ticker, before_reports=before_reports)

    try:
        archive_ingestor = ArchiveIngestor(
            archive_dir=ARCHIVE_DIR,
            db=local_db,
            registry=PatternRegistry.load(CONFIG_DIR),
        )
        archive_result = archive_ingestor.ingest_all(
            tickers=[ticker],
            auto_extract_monthly=False,
        )
        _event(
            local_db, run_id, 'ingest', 'ticker_source_done',
            ticker=ticker, source='archive',
            reports_ingested=archive_result.reports_ingested,
            errors=archive_result.errors,
            data_points_extracted=archive_result.data_points_extracted,
        )
    except Exception as e:
        archive_result = None
        failures.append(f'archive:{e}')
        _event(local_db, run_id, 'ingest', 'ticker_source_failed', ticker=ticker, source='archive', level='WARNING', error=str(e))

    if include_ir:
        with ir_semaphore:
            if _is_cancelled(run_id):
                raise _RunCancelled()
            _event(local_db, run_id, 'ingest', 'ticker_source_start', ticker=ticker, source='ir')
            try:
                ir_scraper = IRScraper(db=local_db, session=requests.Session())
                ir_scraper._pipeline_run_id = run_id
                ir_scraper._request_throttle = ir_throttle
                ir_scraper._host_backoff_seconds = host_backoff_seconds
                ir_scraper._max_retries = max_retries
                ir_result = ir_scraper.scrape_company(company)
                _event(
                    local_db, run_id, 'ingest', 'ticker_source_done',
                    ticker=ticker, source='ir',
                    reports_ingested=ir_result.reports_ingested,
                    errors=ir_result.errors,
                )
            except Exception as e:
                failures.append(f'ir:{e}')
                _event(local_db, run_id, 'ingest', 'ticker_source_failed', ticker=ticker, source='ir', level='WARNING', error=str(e))
    else:
        _event(local_db, run_id, 'ingest', 'ir_skipped', ticker=ticker, reason='include_ir=false')

    with edgar_semaphore:
        if _is_cancelled(run_id):
            raise _RunCancelled()
        _event(local_db, run_id, 'ingest', 'ticker_source_start', ticker=ticker, source='edgar')
        try:
            edgar = EdgarConnector(db=local_db, session=requests.Session())
            edgar._pipeline_run_id = run_id
            edgar._request_throttle = edgar_throttle
            edgar._host_backoff_seconds = host_backoff_seconds
            edgar._max_retries = max_retries
            if company.get('cik'):
                edgar_result = edgar.fetch_all_filings(
                    cik=company['cik'],
                    ticker=ticker,
                    since_date=date(2019, 1, 1),
                    filing_regime=company.get('filing_regime', 'domestic'),
                )
                _event(
                    local_db, run_id, 'ingest', 'ticker_source_done',
                    ticker=ticker, source='edgar',
                    reports_ingested=edgar_result.reports_ingested,
                    errors=edgar_result.errors,
                )
            else:
                _event(local_db, run_id, 'ingest', 'ticker_source_skipped', ticker=ticker, source='edgar', reason='no_cik')
        except Exception as e:
            failures.append(f'edgar:{e}')
            _event(local_db, run_id, 'ingest', 'ticker_source_failed', ticker=ticker, source='edgar', level='WARNING', error=str(e))

    after_reports = _count_reports_for_tickers(local_db, [ticker])
    ingested_delta = max(0, after_reports - before_reports)
    _event(
        local_db, run_id, 'ingest', 'ticker_done',
        ticker=ticker,
        before_reports=before_reports,
        after_reports=after_reports,
        ingested_delta=ingested_delta,
        failures=len(failures),
    )
    return {
        'ticker': ticker,
        'before_reports': before_reports,
        'after_reports': after_reports,
        'ingested_delta': ingested_delta,
        'failures': failures,
        'archive_reports_ingested': int(getattr(archive_result, 'reports_ingested', 0)),
        'archive_data_points_extracted': int(getattr(archive_result, 'data_points_extracted', 0)),
    }


def _execute_overnight_run(run_id: int, config: dict, requested_tickers: list[str]) -> None:
    from app_globals import get_db, get_registry
    from orchestration import run_bootstrap_probe_for_ticker as _run_bootstrap_probe_for_ticker

    db = get_db()
    db.update_pipeline_run(run_id, status='running')
    _event(db, run_id, 'run', 'pipeline_run_start', config=config, requested_count=len(requested_tickers))

    recommendations = {}
    failures = []
    probe_failures = []
    scout_summary = {'ran': False, 'reason': 'not_evaluated'}
    try:
        # Stage: probe + apply skip-mode companies before target selection
        probe_skip = bool(config.get('probe_skip_companies', False))
        probe_timeout = int(config.get('probe_timeout_seconds', 12))
        if probe_skip and not requested_tickers:
            all_companies = db.get_companies(active_only=True)
            skip_candidates = [c['ticker'] for c in all_companies if (c.get('scraper_mode') or 'skip') == 'skip']
            _event(db, run_id, 'bootstrap_probe', 'stage_start', candidates=len(skip_candidates))
            for t in skip_candidates:
                if _is_cancelled(run_id):
                    raise _RunCancelled()
                try:
                    result = _run_bootstrap_probe_for_ticker(
                        db,
                        ticker=t,
                        apply_mode=True,
                        allow_apply_skip=False,
                        timeout=probe_timeout,
                    )
                    _event(
                        db, run_id, 'bootstrap_probe', 'ticker_done',
                        ticker=t,
                        recommended_mode=result.get('recommended_mode'),
                        applied=int(bool(result.get('applied'))),
                    )
                except Exception as e:
                    _event(db, run_id, 'bootstrap_probe', 'ticker_failed', ticker=t, level='WARNING', error=str(e))
            _event(db, run_id, 'bootstrap_probe', 'stage_end', candidates=len(skip_candidates))
        else:
            _event(db, run_id, 'bootstrap_probe', 'stage_skipped',
                   reason='probe_skip_companies=false' if not probe_skip else 'explicit_tickers_provided')

        if requested_tickers:
            targets = []
            for t in requested_tickers:
                c = db.get_company(t)
                if c is None:
                    failures.append({'ticker': t, 'error': 'ticker_not_found'})
                    continue
                targets.append(t)
        else:
            companies = db.get_companies(active_only=True)
            targets = [c['ticker'] for c in companies if (c.get('scraper_mode') or 'skip') != 'skip']

        for t in targets:
            db.upsert_pipeline_run_ticker(run_id, t, targeted=1)

        _event(db, run_id, 'select_targets', 'stage_end', targeted=len(targets), failures=len(failures))

        # Stage: deterministic probe + recommendation (skipped when skip_probe=True)
        skip_probe = bool(config.get('skip_probe', False))
        timeout = int(config.get('probe_timeout_seconds', 12))
        if skip_probe:
            scrape_targets = list(targets)
            _event(db, run_id, 'probe_verify', 'stage_skipped', reason='skip_probe=true', targets=len(targets))
        else:
            for t in targets:
                if _is_cancelled(run_id):
                    raise _RunCancelled()
                try:
                    result = _run_bootstrap_probe_for_ticker(
                        db,
                        ticker=t,
                        apply_mode=False,
                        allow_apply_skip=False,
                        timeout=timeout,
                    )
                    recommendations[t] = result.get('recommended_mode')
                    db.upsert_pipeline_run_ticker(run_id, t, probed=1)
                    _event(
                        db, run_id, 'probe_verify', 'ticker_probe_done',
                        ticker=t,
                        recommended_mode=result.get('recommended_mode'),
                        active_candidates=result.get('active_candidates', 0),
                    )
                except Exception as e:
                    db.upsert_pipeline_run_ticker(run_id, t, failed_reason=str(e))
                    failures.append({'ticker': t, 'error': str(e)})
                    probe_failures.append({'ticker': t, 'error': str(e)})
                    _event(db, run_id, 'probe_verify', 'ticker_probe_failed', ticker=t, level='WARNING', error=str(e))

            if bool(config.get('require_probe_success', True)) and probe_failures:
                preflight_summary = {
                    'targeted_tickers': targets,
                    'recommendations': recommendations,
                    'probe_failures': probe_failures,
                    'failures': failures,
                    'aborted_stage': 'probe_verify',
                    'aborted_reason': 'probe_failures',
                }
                db.update_pipeline_run(
                    run_id,
                    status='failed_preflight',
                    ended_at=datetime.now(timezone.utc).isoformat(),
                    summary=preflight_summary,
                    error='probe_preflight_failed',
                )
                _event(
                    db, run_id, 'probe_verify', 'preflight_abort',
                    level='WARNING', failed=len(probe_failures), targeted=len(targets),
                )
                return

            if bool(config.get('require_non_skip_recommendation', True)):
                scrape_targets = [t for t in targets if recommendations.get(t) and recommendations.get(t) != 'skip']
                skipped_preflight = [t for t in targets if t not in scrape_targets]
                for t in skipped_preflight:
                    db.upsert_pipeline_run_ticker(run_id, t, failed_reason='preflight_skip_recommendation')
                    _event(
                        db, run_id, 'probe_verify', 'ticker_preflight_skipped',
                        ticker=t, recommended_mode=recommendations.get(t),
                    )
                if not scrape_targets:
                    preflight_summary = {
                        'targeted_tickers': targets,
                        'recommendations': recommendations,
                        'failures': failures,
                        'aborted_stage': 'probe_verify',
                        'aborted_reason': 'no_non_skip_recommendations',
                    }
                    db.update_pipeline_run(
                        run_id,
                        status='failed_preflight',
                        ended_at=datetime.now(timezone.utc).isoformat(),
                        summary=preflight_summary,
                        error='probe_preflight_no_ready_targets',
                    )
                    _event(
                        db, run_id, 'probe_verify', 'preflight_abort',
                        level='WARNING', reason='no_non_skip_recommendations', targeted=len(targets),
                    )
                    return
            else:
                scrape_targets = list(targets)

        # Optional stage: coverage scout (policy-gated)
        scout_mode = str(config.get('scout_mode') or 'auto').strip().lower()
        scout_keywords = _normalize_keywords(config.get('scout_keywords'))
        run_scout, scout_reason = _should_run_scout(
            mode=scout_mode,
            tickers=scrape_targets,
            output_dir=str(config.get('scout_output_dir') or '/private/tmp/claude-501/miners_progress'),
            max_age_hours=int(config.get('scout_max_age_hours', 168)),
            keywords=scout_keywords,
        )
        if run_scout and scrape_targets:
            scout_summary = _run_coverage_scout_stage(db, run_id, scrape_targets, config)
            if bool(config.get('require_scout_success', False)) and scout_summary.get('failures'):
                preflight_summary = {
                    'targeted_tickers': targets,
                    'scrape_targets': scrape_targets,
                    'recommendations': recommendations,
                    'failures': failures,
                    'scout': scout_summary,
                    'aborted_stage': 'coverage_scout',
                    'aborted_reason': 'scout_failures',
                }
                db.update_pipeline_run(
                    run_id,
                    status='failed_preflight',
                    ended_at=datetime.now(timezone.utc).isoformat(),
                    summary=preflight_summary,
                    error='scout_preflight_failed',
                )
                _event(
                    db, run_id, 'coverage_scout', 'preflight_abort',
                    level='WARNING', failed=len(scout_summary.get('failures') or []),
                )
                return
        else:
            scout_summary = {'ran': False, 'reason': scout_reason}
            _event(
                db, run_id, 'coverage_scout', 'stage_skipped',
                reason=scout_reason, mode=scout_mode, targets=len(scrape_targets),
            )

        # Optional stage: apply mode changes
        if bool(config.get('apply_mode_changes', False)):
            for t in scrape_targets:
                if _is_cancelled(run_id):
                    raise _RunCancelled()
                try:
                    result = _run_bootstrap_probe_for_ticker(
                        db,
                        ticker=t,
                        apply_mode=True,
                        allow_apply_skip=False,
                        timeout=timeout,
                    )
                    if result.get('applied'):
                        db.upsert_pipeline_run_ticker(run_id, t, mode_applied=1)
                    _event(
                        db, run_id, 'apply_modes', 'ticker_apply_done',
                        ticker=t, applied=int(bool(result.get('applied'))),
                        mode=result.get('recommended_mode'),
                    )
                except Exception as e:
                    failures.append({'ticker': t, 'error': str(e)})
                    _event(db, run_id, 'apply_modes', 'ticker_apply_failed', ticker=t, level='WARNING', error=str(e))

        include_ir = bool(config.get('include_ir', True))

        # Stage: LLM crawl (optional — gated on include_crawl flag)
        include_crawl = bool(config.get('include_crawl', False))
        if include_crawl and scrape_targets:
            import scrapers.llm_crawler as _crawler
            crawl_provider = config.get('crawl_provider') or None
            crawl_task_id = str(uuid.uuid4())
            _event(db, run_id, 'crawl', 'stage_start', tickers=len(scrape_targets), provider=crawl_provider or 'default')
            per_ticker = _crawler.start_crawl(
                scrape_targets,
                task_id=crawl_task_id,
                provider=crawl_provider,
                db=db,
            )
            # Poll until all ticker crawls reach a terminal state
            _TERMINAL = {'complete', 'failed', 'stopped'}
            crawl_timeout = int(config.get('crawl_timeout_seconds', 1800))
            poll_start = time.monotonic()
            while True:
                if _is_cancelled(run_id):
                    _crawler.stop_crawl(crawl_task_id)
                    raise _RunCancelled()
                task = _crawler.get_crawl_task(crawl_task_id) or {}
                done = all(v.get('status') in _TERMINAL for v in task.values()) if task else True
                if done:
                    break
                if time.monotonic() - poll_start > crawl_timeout:
                    _crawler.stop_crawl(crawl_task_id)
                    _event(db, run_id, 'crawl', 'stage_timeout', level='WARNING',
                           timeout_seconds=crawl_timeout)
                    break
                time.sleep(5)
            # Summarise results
            task = _crawler.get_crawl_task(crawl_task_id) or {}
            crawl_docs = sum(v.get('docs_stored', 0) for v in task.values())
            crawl_errors = sum(1 for v in task.values() if v.get('status') == 'failed')
            _event(db, run_id, 'crawl', 'stage_end',
                   docs_stored=crawl_docs, ticker_errors=crawl_errors,
                   tickers_completed=sum(1 for v in task.values() if v.get('status') == 'complete'))
        else:
            _event(db, run_id, 'crawl', 'stage_skipped', reason='include_crawl=false' if not include_crawl else 'no_targets')

        # Stage: ingest (IR + EDGAR, or EDGAR-only when include_ir=False)
        # Uses the same _run_ir_ingest / _run_edgar_ingest functions as individual UI buttons.
        # The scrape_queue / ScrapeWorker is for manual per-company triggers only.
        registry = get_registry()
        force_reextract = bool(config.get('force_reextract', False))
        _default_workers = 2
        try:
            v = db.get_config('ollama_num_parallel')
            if v:
                _default_workers = max(1, int(v))
        except Exception:
            pass
        extract_workers = max(1, int(config.get('extract_workers', _default_workers)))
        ir_workers = max(1, int(config.get('ir_workers', 2)))
        edgar_workers = max(1, int(config.get('edgar_workers', 1)))
        edgar_min_interval_ms = max(0, int(config.get('edgar_min_interval_ms', int(EDGAR_REQUEST_DELAY_SECONDS * 1000))))
        host_backoff_seconds = max(0.0, float(config.get('host_backoff_seconds', 15)))
        max_retries = max(1, int(config.get('max_retries', 2)))
        total_before_reports = total_after_reports = total_ingested_delta = 0
        _event(
            db, run_id, 'ingest', 'stage_start',
            tickers=len(scrape_targets), include_ir=int(include_ir),
            ir_workers=ir_workers, edgar_workers=edgar_workers,
        )
        from scrapers.request_throttle import HostThrottle
        ir_semaphore = threading.Semaphore(ir_workers)
        edgar_semaphore = threading.Semaphore(edgar_workers)
        ir_throttle = HostThrottle(
            min_interval_ms=int(IR_REQUEST_DELAY_SECONDS * 1000),
            cooldown_seconds=host_backoff_seconds,
        )
        edgar_throttle = HostThrottle(
            min_interval_ms=edgar_min_interval_ms,
            cooldown_seconds=host_backoff_seconds,
        )
        # Collect (ticker, batch) pairs after scraping all tickers — separated phases.
        ticker_batches: list = []
        for t in scrape_targets:
            if _is_cancelled(run_id):
                raise _RunCancelled()
            result = _scrape_ticker_for_pipeline(
                db_path=db.db_path,
                run_id=run_id,
                ticker=t,
                include_ir=include_ir,
                ir_semaphore=ir_semaphore,
                edgar_semaphore=edgar_semaphore,
                ir_throttle=ir_throttle,
                edgar_throttle=edgar_throttle,
                host_backoff_seconds=host_backoff_seconds,
                max_retries=max_retries,
            )
            total_before_reports += result['before_reports']
            total_after_reports += result['after_reports']
            total_ingested_delta += result['ingested_delta']
            for failure in result.get('failures') or []:
                failures.append({'ticker': t, 'error': failure})
            if result['ingested_delta'] > 0:
                db.upsert_pipeline_run_ticker(run_id, t, ingested=1)
            first_filing = db.get_btc_first_filing_date(t)
            batch = _build_extraction_batch(db, t, first_filing, force_reextract)
            ticker_batches.append((t, batch))

        _event(
            db, run_id, 'ingest', 'stage_end',
            before_reports=total_before_reports,
            after_reports=total_after_reports,
            ingested_delta=total_ingested_delta,
        )

        # Stage: extract — runs after all scraping is complete (separated phases).
        extraction_tickers = [t for t, _ in ticker_batches]
        prebuilt = {t: batch for t, batch in ticker_batches}
        extraction_counters = run_extraction_phase(
            db,
            run_id,
            tickers=extraction_tickers,
            registry=registry,
            prebuilt_batches=prebuilt,
            force_reextract=force_reextract,
            warm_model=bool(config.get('warm_model', True)),
            extract_workers=extract_workers,
            cancel_check=lambda: _is_cancelled(run_id),
            failures=failures,
        )

        # Stage: auto-gap-fill for quarterly/annual reporters.
        # Expands quarterly data_points into monthly inferred rows so all companies
        # share a common monthly time spine regardless of reporting cadence.
        # Runs for every non-monthly company in scrape_targets (idempotent — skips
        # months that already have real or analyst data).
        from interpreters.gap_fill import fill_quarterly_gaps
        gf_tickers = [
            t for t in scrape_targets
            if (db.get_company(t) or {}).get('reporting_cadence', 'monthly') != 'monthly'
        ]
        if gf_tickers:
            _event(db, run_id, 'gap_fill', 'stage_start', tickers=gf_tickers)
            gf_filled = gf_skipped = gf_errors = 0
            for t in gf_tickers:
                if _is_cancelled(run_id):
                    raise _RunCancelled()
                try:
                    gf = fill_quarterly_gaps(t, db)
                    gf_filled += gf.get('filled', 0)
                    gf_skipped += gf.get('skipped', 0)
                    gf_errors += gf.get('errors', 0)
                    _event(db, run_id, 'gap_fill', 'ticker_done',
                           ticker=t,
                           filled=gf.get('filled', 0),
                           skipped=gf.get('skipped', 0),
                           errors=gf.get('errors', 0))
                except Exception as _gf_err:
                    gf_errors += 1
                    _event(db, run_id, 'gap_fill', 'ticker_error',
                           ticker=t, level='WARNING', error=str(_gf_err))
            _event(db, run_id, 'gap_fill', 'stage_end',
                   tickers_processed=len(gf_tickers),
                   filled=gf_filled, skipped=gf_skipped, errors=gf_errors)
        else:
            _event(db, run_id, 'gap_fill', 'stage_skipped',
                   reason='no_quarterly_or_annual_reporters_in_targets')

        snapshot = db.get_pipeline_observability()
        summary = {
            'targeted_tickers': targets,
            'scrape_targets': scrape_targets,
            'recommendations': recommendations,
            'scout': scout_summary,
            'failures': failures,
            'reports_processed': extraction_counters['processed'],
            'data_points_extracted': extraction_counters['data_points'],
            'extract_errors': extraction_counters['errors'],
            'totals': snapshot.get('totals', {}),
        }
        final_status = 'partial_complete' if failures else 'complete'
        db.update_pipeline_run(
            run_id,
            status=final_status,
            ended_at=datetime.now(timezone.utc).isoformat(),
            summary=summary,
            error=None,
        )
        _event(db, run_id, 'run', 'pipeline_run_end', status=final_status, failures=len(failures))

    except _RunCancelled:
        db.update_pipeline_run(
            run_id,
            status='cancelled',
            ended_at=datetime.now(timezone.utc).isoformat(),
            summary={'cancelled': True},
            error='cancelled_by_user',
        )
        _event(db, run_id, 'run', 'pipeline_run_cancelled', level='WARNING')
    except Exception as e:
        log.error("Overnight pipeline run %s failed", run_id, exc_info=True)
        db.update_pipeline_run(
            run_id,
            status='failed',
            ended_at=datetime.now(timezone.utc).isoformat(),
            summary={'failures': failures},
            error=str(e),
        )
        _event(db, run_id, 'run', 'pipeline_run_failed', level='WARNING', error=str(e))
    finally:
        _clear_run_thread(run_id)
        with _run_lock:
            _run_cancel_flags.pop(run_id, None)


@bp.route('/api/pipeline/preflight')
def pipeline_preflight():
    """Return pipeline readiness summary for display before starting a run.

    Exposes:
    - pending_report_count: reports with extraction_status IN ('pending','failed')
    - already_extracted_count: reports with extraction_status = 'done'
    - llm_available: whether Ollama is reachable right now
    - ollama_model: active model name
    - keyword_count: total active metric keywords configured
    - companies_targeted: active companies count
    - scraper_mode_skip_count: companies with scraper_mode='skip'
    """
    from app_globals import get_db
    from infra.ollama_warmup import warm_ollama_for_extraction
    db = get_db()

    with db._get_connection() as conn:
        pending_count = conn.execute(
            "SELECT COUNT(*) FROM reports WHERE raw_text IS NOT NULL AND raw_text != ''"
            " AND extraction_status IN ('pending','failed')"
        ).fetchone()[0] or 0
        extracted_count = conn.execute(
            "SELECT COUNT(*) FROM reports WHERE extraction_status = 'done'"
        ).fetchone()[0] or 0

    try:
        from infra.keyword_service import get_all_active_rows as _get_kw_rows
        kw_rows = _get_kw_rows(db)
        keyword_count = len(kw_rows)
    except Exception:
        keyword_count = 0

    try:
        companies = db.list_companies(active_only=True)
        companies_targeted = len(companies)
        scraper_mode_skip_count = sum(
            1 for c in companies
            if (c.get('scraper_mode') or c.get('scrape_mode') or '') == 'skip'
        )
    except Exception:
        companies_targeted = 0
        scraper_mode_skip_count = 0

    warmup = warm_ollama_for_extraction(db=db, reason='preflight_check', force=False)
    llm_available = bool(warmup.get('warmed'))
    ollama_model = warmup.get('model', '')

    return jsonify({'success': True, 'data': {
        'pending_report_count': int(pending_count),
        'already_extracted_count': int(extracted_count),
        'llm_available': llm_available,
        'ollama_model': ollama_model,
        'keyword_count': keyword_count,
        'companies_targeted': companies_targeted,
        'scraper_mode_skip_count': scraper_mode_skip_count,
    }})


@bp.route('/api/pipeline/overnight/start', methods=['POST'])
def start_overnight_pipeline():
    """Start an overnight pipeline run in a background thread."""
    from app_globals import get_db
    db = get_db()
    _cleanup_orphaned_process_runs(db)
    body = request.get_json(silent=True) or {}
    tickers = body.get('tickers') or []
    if not isinstance(tickers, list):
        return jsonify({'success': False, 'error': {'message': "'tickers' must be a list"}}), 400
    requested_tickers = [str(t).strip().upper() for t in tickers if str(t).strip()]

    active_runs = [
        run for run in db.list_pipeline_runs_by_status(['queued', 'running'])
        if _is_run_thread_alive(int(run['id']))
    ]
    if active_runs:
        active = active_runs[0]
        return jsonify({'success': False, 'error': {
            'code': 'ALREADY_RUNNING',
            'message': f"Pipeline run {active['id']} is already active",
        }, 'data': {'active_run_id': int(active['id'])}}), 409

    _recover_stale_report_claims(db)

    config = {
        'skip_probe': bool(body.get('skip_probe', False)),
        'apply_mode_changes': bool(body.get('apply_mode_changes', False)),
        'warm_model': bool(body.get('warm_model', True)),
        'probe_timeout_seconds': int(body.get('probe_timeout_seconds', 12)),
        'require_probe_success': bool(body.get('require_probe_success', True)),
        'require_non_skip_recommendation': bool(body.get('require_non_skip_recommendation', True)),
        'include_ir': bool(body.get('include_ir', True)),
        'include_crawl': bool(body.get('include_crawl', False)),
        'crawl_provider': body.get('crawl_provider') or None,
        'crawl_timeout_seconds': int(body.get('crawl_timeout_seconds', 1800)),
        'scout_mode': str(body.get('scout_mode', 'auto')),
        'scout_metric': str(body.get('scout_metric', 'production_btc')),
        'scout_keywords': _normalize_keywords(body.get('scout_keywords')),
        'scout_output_dir': str(body.get('scout_output_dir') or db.get_config('pipeline_output_dir') or '/private/tmp/claude-501/miners_progress'),
        'scout_max_age_hours': int(body.get('scout_max_age_hours', 168)),
        'require_scout_success': bool(body.get('require_scout_success', False)),
        'scout_as_of_date': body.get('scout_as_of_date'),
        'probe_skip_companies': bool(body.get('probe_skip_companies', False)),
        'force_reextract': bool(body.get('force_reextract', False)),
        'extract_workers': max(1, int(body.get('extract_workers', max(1, int(db.get_config('ollama_num_parallel') or 2))))),
        'ir_workers': max(1, int(body.get('ir_workers', 2))),
        'edgar_workers': max(1, int(body.get('edgar_workers', 1))),
        'edgar_min_interval_ms': max(0, int(body.get('edgar_min_interval_ms', int(EDGAR_REQUEST_DELAY_SECONDS * 1000)))),
        'host_backoff_seconds': max(0.0, float(body.get('host_backoff_seconds', 15))),
        'max_retries': max(1, int(body.get('max_retries', 2))),
    }
    run = db.create_pipeline_run(
        triggered_by=str(body.get('triggered_by') or 'ops_ui'),
        scope={'tickers': requested_tickers},
        config=config,
    )
    run_id = int(run['id'])
    with _run_lock:
        _run_cancel_flags[run_id] = threading.Event()
    t = threading.Thread(
        target=_execute_overnight_run,
        args=(run_id, config, requested_tickers),
        daemon=True,
        name=f"overnight-run-{run_id}",
    )
    _register_run_thread(run_id, t)
    t.start()
    return jsonify({'success': True, 'data': {'run_id': run_id, 'status': 'queued'}}), 202


@bp.route('/api/pipeline/overnight/latest')
def overnight_pipeline_latest():
    """Return the most recently created overnight run (id + status).

    Used by the UI on page load to restore the active run_id from the server
    instead of relying solely on localStorage, which may be stale or missing.
    """
    from app_globals import get_db
    db = get_db()
    run = db.get_latest_pipeline_run()
    if run is None:
        return jsonify({'success': False, 'error': {'message': 'No runs found'}}), 404
    return jsonify({'success': True, 'data': {
        'run': run,
        'tickers': db.list_pipeline_run_tickers(run['id']),
        'cancel_requested': _is_cancelled(run['id']),
    }})


@bp.route('/api/pipeline/overnight/<int:run_id>/status')
def overnight_pipeline_status(run_id: int):
    """Return one overnight run with per-ticker records."""
    from app_globals import get_db
    db = get_db()
    run = db.get_pipeline_run(run_id)
    if run is None:
        return jsonify({'success': False, 'error': {'message': 'Run not found'}}), 404
    return jsonify({'success': True, 'data': {
        'run': run,
        'tickers': db.list_pipeline_run_tickers(run_id),
        'cancel_requested': _is_cancelled(run_id),
    }})


@bp.route('/api/pipeline/overnight/<int:run_id>/events')
def overnight_pipeline_events(run_id: int):
    """Return structured event stream for a run."""
    from app_globals import get_db
    db = get_db()
    run = db.get_pipeline_run(run_id)
    if run is None:
        return jsonify({'success': False, 'error': {'message': 'Run not found'}}), 404
    try:
        limit = int(request.args.get('limit', 500))
    except ValueError:
        return jsonify({'success': False, 'error': {'message': 'limit must be an integer'}}), 400
    rows = db.list_pipeline_run_events(run_id, limit=limit)
    return jsonify({'success': True, 'data': rows})


@bp.route('/api/pipeline/overnight/<int:run_id>/cancel', methods=['POST'])
def overnight_pipeline_cancel(run_id: int):
    """Request cooperative cancellation for a running overnight run."""
    from app_globals import get_db
    db = get_db()
    run = db.get_pipeline_run(run_id)
    if run is None:
        return jsonify({'success': False, 'error': {'message': 'Run not found'}}), 404
    _mark_cancel_requested(run_id)
    db.add_pipeline_run_event(run_id, stage='run', event='cancel_requested', level='WARNING', details={})
    return jsonify({'success': True, 'data': {'run_id': run_id, 'cancel_requested': True}})


@bp.route('/api/pipeline/overnight/<int:run_id>/apply_modes', methods=['POST'])
def overnight_pipeline_apply_modes(run_id: int):
    """Analyst-triggered apply pass for recommended modes on run tickers."""
    from app_globals import get_db
    from orchestration import run_bootstrap_probe_for_ticker as _run_bootstrap_probe_for_ticker

    db = get_db()
    run = db.get_pipeline_run(run_id)
    if run is None:
        return jsonify({'success': False, 'error': {'message': 'Run not found'}}), 404

    body = request.get_json(silent=True) or {}
    tickers = body.get('tickers') or []
    if tickers and not isinstance(tickers, list):
        return jsonify({'success': False, 'error': {'message': "'tickers' must be a list"}}), 400
    selected = {str(t).strip().upper() for t in tickers if str(t).strip()}

    run_tickers = db.list_pipeline_run_tickers(run_id)
    targets = [r['ticker'] for r in run_tickers if (not selected or r['ticker'] in selected)]
    timeout = int(body.get('probe_timeout_seconds', 12))

    applied = 0
    failures = []
    for ticker in targets:
        try:
            result = _run_bootstrap_probe_for_ticker(
                db,
                ticker=ticker,
                apply_mode=True,
                allow_apply_skip=bool(body.get('allow_apply_skip', False)),
                timeout=timeout,
            )
            if result.get('applied'):
                applied += 1
                db.upsert_pipeline_run_ticker(run_id, ticker, mode_applied=1)
            db.add_pipeline_run_event(
                run_id, stage='apply_modes', event='ticker_apply_done', ticker=ticker,
                details={'applied': int(bool(result.get('applied'))), 'mode': result.get('recommended_mode')},
            )
        except Exception as e:
            failures.append({'ticker': ticker, 'error': str(e)})
            db.add_pipeline_run_event(
                run_id, stage='apply_modes', event='ticker_apply_failed', ticker=ticker,
                level='WARNING', details={'error': str(e)},
            )

    return jsonify({'success': True, 'data': {
        'run_id': run_id,
        'targeted': len(targets),
        'applied': applied,
        'failed': len(failures),
        'failures': failures,
    }})
