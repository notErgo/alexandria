"""Overnight pipeline orchestration routes."""
import json
import logging
import os
import threading
import time
import uuid
from pathlib import Path
from datetime import datetime, timezone

from flask import Blueprint, jsonify, request

log = logging.getLogger('miners.routes.pipeline')
bp = Blueprint('pipeline', __name__)

_run_cancel_flags: dict[int, threading.Event] = {}
_run_lock = threading.Lock()

# UI contract: maps each user-facing pipeline parameter to the HTML element id
# that reads and sends it.  test_pipeline_ui_params_wired enforces that every
# entry here has a matching element in templates/ops.html.  Add a row here
# whenever a new boolean/select is added to start_overnight_pipeline, so the
# gap between backend capability and UI exposure is caught at test time.
PIPELINE_UI_PARAMS: dict[str, str] = {
    'include_ir':    'pipeline-include-ir',
    'include_crawl': 'pipeline-include-crawl',
}


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


def _execute_overnight_run(run_id: int, config: dict, requested_tickers: list[str]) -> None:
    from app_globals import get_db, get_registry
    from interpreters.interpret_pipeline import extract_report
    from infra.ollama_warmup import warm_ollama_for_extraction, ensure_ollama_running
    from routes.companies import _run_bootstrap_probe_for_ticker
    from routes.reports import _run_ir_ingest, _run_edgar_ingest

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
        before_reports = _count_reports_for_tickers(db, scrape_targets)
        if include_ir:
            _run_ir_ingest(str(uuid.uuid4()), auto_extract=False, warm_model=bool(config.get('warm_model', True)), pipeline_run_id=run_id)
        else:
            _event(db, run_id, 'ingest', 'ir_skipped', reason='include_ir=false')
        _run_edgar_ingest(str(uuid.uuid4()), auto_extract=False, warm_model=bool(config.get('warm_model', True)), pipeline_run_id=run_id)
        after_reports = _count_reports_for_tickers(db, scrape_targets)
        ingested_delta = max(0, after_reports - before_reports)
        _event(db, run_id, 'ingest', 'stage_end', before_reports=before_reports, after_reports=after_reports, ingested_delta=ingested_delta)
        if ingested_delta > 0:
            for t in scrape_targets:
                db.upsert_pipeline_run_ticker(run_id, t, ingested=1)

        # Stage: extraction
        registry = get_registry()
        reports = []
        for t in scrape_targets:
            reports.extend(db.get_unextracted_reports(ticker=t))
        if reports and bool(config.get('warm_model', True)):
            def _pipeline_ollama_log(msg: str) -> None:
                _event(db, run_id, 'extract', 'ollama_status', message=msg)
            ensure_ollama_running(log_fn=_pipeline_ollama_log)
            warmup_result = warm_ollama_for_extraction(db=db, reason='pipeline_overnight_extract', force=True)
            _event(db, run_id, 'extract', 'ollama_warmup',
                   warmed=int(bool(warmup_result.get('warmed'))),
                   model=warmup_result.get('model', ''),
                   reason=warmup_result.get('reason', ''))
            if warmup_result.get('warmed'):
                # Invalidate the LLM availability cache so the first extract_report
                # call gets a fresh check (not a stale False from a prior failed run).
                try:
                    import interpreters.interpret_pipeline as _ipl
                    _ipl._llm_available_cache_time = 0.0
                except Exception:
                    pass
        total_reports = len(reports)
        processed = data_points = errors = 0
        extracted_tickers = set()
        _event(db, run_id, 'extract', 'stage_start', total_reports=total_reports)
        for i, report in enumerate(reports):
            if _is_cancelled(run_id):
                raise _RunCancelled()
            ticker = report.get('ticker', '')
            period = report.get('report_date', '')
            source_type = report.get('source_type', '')
            try:
                summary = extract_report(report, db, registry)
                processed += summary.reports_processed
                dp_delta = summary.data_points_extracted
                rv_delta = summary.review_flagged
                data_points += dp_delta
                errors += summary.errors
                extracted_tickers.add(ticker)
                _event(
                    db, run_id, 'extract', 'report_done',
                    ticker=ticker, period=period, source_type=source_type,
                    data_points=dp_delta, review_flagged=rv_delta,
                    progress=i + 1, total=total_reports,
                    running_total_dp=data_points,
                    prompt_tokens=summary.prompt_tokens,
                    response_tokens=summary.response_tokens,
                )
            except Exception as e:
                errors += 1
                failures.append({'ticker': ticker, 'error': str(e)})
                _event(
                    db, run_id, 'extract', 'report_extract_failed',
                    ticker=ticker, level='WARNING',
                    report_id=report.get('id'), period=period,
                    source_type=source_type, error=str(e),
                )
        for t in extracted_tickers:
            db.upsert_pipeline_run_ticker(run_id, t, extracted=1)
        _event(db, run_id, 'extract', 'stage_end', reports_processed=processed, data_points=data_points, errors=errors)

        snapshot = db.get_pipeline_observability()
        summary = {
            'targeted_tickers': targets,
            'scrape_targets': scrape_targets,
            'recommendations': recommendations,
            'scout': scout_summary,
            'failures': failures,
            'reports_processed': processed,
            'data_points_extracted': data_points,
            'extract_errors': errors,
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
        with _run_lock:
            _run_cancel_flags.pop(run_id, None)


@bp.route('/api/pipeline/overnight/start', methods=['POST'])
def start_overnight_pipeline():
    """Start an overnight pipeline run in a background thread."""
    from app_globals import get_db
    db = get_db()
    body = request.get_json(silent=True) or {}
    tickers = body.get('tickers') or []
    if not isinstance(tickers, list):
        return jsonify({'success': False, 'error': {'message': "'tickers' must be a list"}}), 400
    requested_tickers = [str(t).strip().upper() for t in tickers if str(t).strip()]

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
    from routes.companies import _run_bootstrap_probe_for_ticker

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
