"""
Operations panel API routes.

  GET  /api/operations/queue                       — pending extraction queue + legacy files
  GET  /api/operations/pipeline_observability      — end-to-end ingest/extract counts + config health
  POST /api/operations/observer_swarm/start        — trigger observer swarm discovery/scrape run
  GET  /api/operations/observer_swarm/<id>/status  — observer swarm run status
  POST /api/operations/interpret                     — trigger extraction for a ticker
  GET  /api/operations/interpret/<task_id>/progress  — extraction progress
  POST /api/operations/assign_period               — assign period to a legacy_undated file
  GET  /api/operations/manifest/<id>/preview       — serve raw file content for inline viewer
  POST /api/operations/manifest/<id>/detect_period — infer period via rules + LLM fallback
  GET  /operations                                 — render operations.html
"""
import logging
import threading
import uuid
import re
import json
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from pathlib import Path
from textwrap import dedent

from flask import Blueprint, jsonify, request, render_template, Response, redirect
from config import MONTHLY_EXTRACTION_SOURCE_TYPES

log = logging.getLogger('miners.routes.operations')

bp = Blueprint('operations', __name__)

# ── In-memory state for background extraction tasks ──────────────────────────
_active_tickers: set = set()
_active_tickers_lock = threading.Lock()
_extraction_progress: dict = {}
_progress_lock = threading.Lock()
_observer_swarm_progress: dict = {}
_observer_swarm_lock = threading.Lock()
_observer_swarm_running_task_id: str | None = None

_OBSERVER_PROMPT_REFERENCES = [
    Path(__file__).resolve().parents[2] / "scripts" / "prompts" / "00_wire_services.md",
    Path(__file__).resolve().parents[2] / "scripts" / "prompts" / "agent_B_clsk_bitf_btbt.md",
]


def _safe_read(path: Path) -> str:
    try:
        return path.read_text()
    except Exception as exc:  # noqa: BLE001
        return f"[unavailable: {path} :: {exc}]"


def _write_observer_prompt_artifacts(
    *,
    run_id: str,
    output_dir: Path,
    tickers: list[str],
    scout_count: int,
    max_attempts_source: int,
    max_no_yield: int,
    execute_scrape: bool,
    scouts: list[dict],
) -> dict:
    prompts_dir = output_dir / "prompts"
    prompts_dir.mkdir(parents=True, exist_ok=True)

    refs = {str(p): _safe_read(p) for p in _OBSERVER_PROMPT_REFERENCES}
    observer_prompt = dedent(
        f"""
        # Observer Prompt Trace
        run_id: {run_id}
        objective: Discover + validate PRNewswire/GlobeNewswire/IR source schema and execute scraping where allowed.
        tickers: {", ".join(tickers)}
        scout_count: {scout_count}

        ## Deterministic Core
        - Source order: IR -> GlobeNewswire -> PRNewswire
        - Exhaustion gate: max_attempts_per_source={max_attempts_source}, max_consecutive_no_yield={max_no_yield}
        - Coverage gate: block if no IR source and wire sample_count == 0
        - Execute scrape: {execute_scrape}

        ## Prompt References (verbatim)
        """
    ).strip() + "\n"
    for path, body in refs.items():
        observer_prompt += f"\n### {path}\n\n{body}\n"

    observer_prompt_path = prompts_dir / f"observer_prompt_{run_id}.md"
    observer_prompt_path.write_text(observer_prompt)

    scout_paths = []
    for scout in scouts:
        scout_id = scout.get("scout_id", "scout-unknown")
        scout_tickers = scout.get("tickers", [])
        scout_prompt = dedent(
            f"""
            # Scout Prompt Trace
            run_id: {run_id}
            scout_id: {scout_id}
            assigned_tickers: {", ".join(scout_tickers)}

            ## Execution Rules
            - Use deterministic source order and contracts.
            - Respect exhaustion + coverage gates.
            - Emit evidence URLs and structured blockers.
            - Do not silently skip a source family.
            """
        ).strip() + "\n"
        for path, body in refs.items():
            scout_prompt += f"\n### {path}\n\n{body}\n"
        scout_path = prompts_dir / f"{scout_id}_prompt_{run_id}.md"
        scout_path.write_text(scout_prompt)
        scout_paths.append(str(scout_path))

    index = {
        "run_id": run_id,
        "observer_prompt": str(observer_prompt_path),
        "scout_prompts": scout_paths,
        "generated_at": datetime.now(timezone.utc).isoformat(),
    }
    index_path = prompts_dir / f"prompt_trace_{run_id}.json"
    index_path.write_text(json.dumps(index, indent=2))
    return {"index": str(index_path), "observer_prompt": str(observer_prompt_path), "scout_prompts": scout_paths}


def _write_observer_decision_trace(*, run_id: str, output_dir: Path, merged_contracts_path: str) -> dict:
    path = Path(merged_contracts_path or "")
    trace_path = output_dir / "prompts" / f"decision_trace_{run_id}.json"
    try:
        payload = json.loads(path.read_text())
    except Exception as exc:  # noqa: BLE001
        trace = {
            "run_id": run_id,
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "error": f"failed_to_read_merged_contracts:{exc}",
            "merged_contracts_path": str(path),
            "tickers": [],
        }
        trace_path.write_text(json.dumps(trace, indent=2))
        return {"path": str(trace_path)}

    contracts = payload.get("contracts", []) if isinstance(payload, dict) else []
    rows = []
    for c in contracts:
        rows.append({
            "ticker": c.get("ticker"),
            "status": c.get("status"),
            "attempts_by_family": c.get("attempts_by_family", {}),
            "sources": [
                {
                    "family": s.get("family"),
                    "method": s.get("discovery_method"),
                    "sample_count": (s.get("validation") or {}).get("sample_count", 0),
                    "entry_url": s.get("entry_url"),
                }
                for s in c.get("sources", [])
            ],
            "blockers": c.get("blockers", []),
        })
    trace = {
        "run_id": run_id,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "merged_contracts_path": str(path),
        "tickers": sorted(rows, key=lambda r: (r.get("ticker") or "")),
    }
    trace_path.write_text(json.dumps(trace, indent=2))
    return {"path": str(trace_path)}


@bp.route('/api/operations/queue')
def operations_queue():
    """Return pending extraction queue grouped by ticker + legacy undated files."""
    try:
        from app_globals import get_db
        db = get_db()
        queue = db.get_operations_queue()
        return jsonify({'success': True, 'data': queue})
    except Exception:
        log.error('Error in operations_queue', exc_info=True)
        return jsonify({'success': False, 'error': {'message': 'Internal server error'}}), 500


@bp.route('/api/operations/pipeline_observability')
def operations_pipeline_observability():
    """Return global/ticker pipeline counts and scraper configuration health."""
    try:
        from app_globals import get_db
        db = get_db()
        snapshot = db.get_pipeline_observability()
        return jsonify({'success': True, 'data': snapshot})
    except Exception:
        log.error('Error in operations_pipeline_observability', exc_info=True)
        return jsonify({'success': False, 'error': {'message': 'Internal server error'}}), 500


@bp.route('/api/operations/observer_swarm/start', methods=['POST'])
def operations_observer_swarm_start():
    """Trigger background observer swarm run."""
    try:
        body = request.get_json(silent=True) or {}
        tickers_raw = body.get('tickers') or []
        if isinstance(tickers_raw, str):
            tickers = [t.strip().upper() for t in tickers_raw.split(',') if t.strip()]
        elif isinstance(tickers_raw, list):
            tickers = [str(t).strip().upper() for t in tickers_raw if str(t).strip()]
        else:
            tickers = []

        scout_count = max(1, int(body.get('scout_count', 4)))
        max_attempts_source = max(1, int(body.get('max_attempts_source', 5)))
        max_no_yield = max(1, int(body.get('max_no_yield', 3)))
        execute_scrape = bool(body.get('execute_scrape', True))
        run_feedback_loop = bool(body.get('run_feedback_loop', True))
        apply_validated_primitives = bool(body.get('apply_validated_primitives', False))
        run_id = str(body.get('run_id') or f"observer_ui_{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}")
        output_dir = Path(str(body.get('output_dir') or '.data/miners_progress'))
        if not output_dir.is_absolute():
            output_dir = Path(__file__).resolve().parents[2] / output_dir

        global _observer_swarm_running_task_id
        with _observer_swarm_lock:
            if _observer_swarm_running_task_id:
                active = _observer_swarm_progress.get(_observer_swarm_running_task_id) or {}
                if active.get('status') in {'queued', 'running'}:
                    return jsonify({'success': False, 'error': {
                        'code': 'ALREADY_RUNNING',
                        'message': f"Observer swarm already running (task_id={_observer_swarm_running_task_id})",
                    }}), 409

            task_id = str(uuid.uuid4())
            _observer_swarm_running_task_id = task_id
            _observer_swarm_progress[task_id] = {
                'task_id': task_id,
                'run_id': run_id,
                'status': 'queued',
                'tickers': tickers,
                'scout_count': scout_count,
                'max_attempts_source': max_attempts_source,
                'max_no_yield': max_no_yield,
                'execute_scrape': execute_scrape,
                'run_feedback_loop': run_feedback_loop,
                'apply_validated_primitives': apply_validated_primitives,
                'output_dir': str(output_dir),
                'created_at': datetime.now(timezone.utc).isoformat(),
            }

        def _run() -> None:
            global _observer_swarm_running_task_id
            try:
                from config import CONFIG_DIR
                from scrapers.observer_swarm import ScoutConfig, run_observer

                output_dir.mkdir(parents=True, exist_ok=True)
                rows = json.loads((Path(CONFIG_DIR) / "companies.json").read_text())
                companies_by_ticker = {r["ticker"].upper(): r for r in rows}
                targets = tickers or sorted([r["ticker"].upper() for r in rows])
                cfg = ScoutConfig(
                    max_attempts_per_source=max_attempts_source,
                    max_consecutive_no_yield=max_no_yield,
                    execute_scrape=execute_scrape,
                    run_feedback_loop=run_feedback_loop,
                    apply_validated_primitives=apply_validated_primitives,
                )
                with _observer_swarm_lock:
                    _observer_swarm_progress[task_id]['status'] = 'running'
                    _observer_swarm_progress[task_id]['started_at'] = datetime.now(timezone.utc).isoformat()
                    _observer_swarm_progress[task_id]['tickers'] = targets

                summary = run_observer(
                    run_id=run_id,
                    tickers=targets,
                    scout_count=scout_count,
                    output_dir=output_dir,
                    config=cfg,
                    companies_by_ticker=companies_by_ticker,
                )
                prompt_trace = _write_observer_prompt_artifacts(
                    run_id=run_id,
                    output_dir=output_dir,
                    tickers=targets,
                    scout_count=scout_count,
                    max_attempts_source=max_attempts_source,
                    max_no_yield=max_no_yield,
                    execute_scrape=execute_scrape,
                    scouts=summary.get('scouts', []),
                )
                decision_trace = _write_observer_decision_trace(
                    run_id=run_id,
                    output_dir=output_dir,
                    merged_contracts_path=summary.get('artifacts', {}).get('merged_source_contracts', ''),
                )
                summary['prompt_trace'] = prompt_trace
                summary['decision_trace'] = decision_trace

                with _observer_swarm_lock:
                    _observer_swarm_progress[task_id]['status'] = 'complete'
                    _observer_swarm_progress[task_id]['completed_at'] = datetime.now(timezone.utc).isoformat()
                    _observer_swarm_progress[task_id]['summary'] = summary
            except Exception as exc:  # noqa: BLE001
                log.error('Observer swarm task %s failed: %s', task_id, exc, exc_info=True)
                with _observer_swarm_lock:
                    _observer_swarm_progress[task_id]['status'] = 'error'
                    _observer_swarm_progress[task_id]['completed_at'] = datetime.now(timezone.utc).isoformat()
                    _observer_swarm_progress[task_id]['error_message'] = str(exc)
            finally:
                with _observer_swarm_lock:
                    if _observer_swarm_running_task_id == task_id:
                        _observer_swarm_running_task_id = None

        t = threading.Thread(target=_run, daemon=True, name=f"observer-swarm-{task_id[:8]}")
        t.start()
        return jsonify({'success': True, 'data': {'task_id': task_id, 'run_id': run_id}})
    except Exception:
        log.error('Error in operations_observer_swarm_start', exc_info=True)
        return jsonify({'success': False, 'error': {'message': 'Internal server error'}}), 500


@bp.route('/api/operations/observer_swarm/<task_id>/status')
def operations_observer_swarm_status(task_id: str):
    """Return observer swarm task status."""
    try:
        with _observer_swarm_lock:
            state = dict(_observer_swarm_progress.get(task_id) or {})
        if not state:
            return jsonify({'success': False, 'error': {'message': 'Task not found'}}), 404
        return jsonify({'success': True, 'data': state})
    except Exception:
        log.error('Error in operations_observer_swarm_status', exc_info=True)
        return jsonify({'success': False, 'error': {'message': 'Internal server error'}}), 500


# Cadence → source_type sets for report filtering.
_CADENCE_SOURCE_TYPES = {
    'monthly':   list(MONTHLY_EXTRACTION_SOURCE_TYPES),
    'quarterly': ['edgar_10q'],
    'annual':    ['edgar_10k'],
    # 'all' / None: no filter (all source_types)
}


def _normalize_extract_tickers(body: dict) -> list[str]:
    raw_tickers = body.get('tickers')
    tickers: list[str] = []
    if isinstance(raw_tickers, list):
        for value in raw_tickers:
            if value is None:
                continue
            ticker = str(value).strip().upper()
            if ticker and ticker not in tickers:
                tickers.append(ticker)
    legacy_ticker = (body.get('ticker') or '').strip().upper()
    if legacy_ticker and legacy_ticker not in tickers:
        tickers.append(legacy_ticker)
    return tickers


def _extract_scope_label(tickers: list[str]) -> str:
    return ','.join(tickers) if tickers else 'ALL'


@bp.route('/api/operations/interpret', methods=['POST'])
def operations_extract():
    """Trigger background extraction for a ticker (or all tickers). Returns task_id.

    Body parameters:
        ticker (str, optional): Legacy single-ticker scope.
        tickers (list[str], optional): Explicit ticker scope. Empty = all tickers.
        force (bool): Re-extract already-extracted reports (default false).
        warm_model (bool): Warm Ollama before starting (default true).
        cadence (str): 'monthly' | 'quarterly' | 'annual' | 'all' (default 'all').
            Filters reports to the matching source_types.
        from_period (str): Inclusive earliest report_date to include (YYYY-MM or YYYY-MM-DD).
        to_period (str): Inclusive latest report_date to include (YYYY-MM or YYYY-MM-DD).
        extract_workers (int): Parallel LLM workers for one ticker scope (default 1, max 12).
        sample (int): If > 0, randomly pick at most this many reports (max 10).
            Use for prompt debugging — quick feedback without a full run.
    """
    try:
        body = request.get_json(silent=True) or {}
        from app_globals import get_db
        from infra.keyword_service import get_mining_detection_phrases
        tickers = _normalize_extract_tickers(body)
        ticker = tickers[0] if len(tickers) == 1 else None
        scope_label = _extract_scope_label(tickers)
        force = bool(body.get('force', False))
        warm_model = bool(body.get('warm_model', True))
        cadence = (body.get('cadence') or 'all').strip().lower()
        from_period = (body.get('from_period') or '').strip() or None
        to_period = (body.get('to_period') or '').strip() or None
        extract_workers = max(1, min(int(body.get('extract_workers') or 1), 12))
        sample_n = int(body.get('sample') or 0)
        sample_n = max(0, min(sample_n, 10))  # clamp 0-10
        # expected_granularity: caller may supply explicitly; otherwise derive from cadence.
        # When cadence='all', granularity is inferred per-report inside extract_report().
        _cadence_grain_map = {'monthly': 'monthly', 'quarterly': 'quarterly', 'annual': 'annual'}
        expected_granularity = (body.get('expected_granularity') or '').strip().lower() or None
        if expected_granularity is None and cadence in _cadence_grain_map:
            expected_granularity = _cadence_grain_map[cadence]
        run_key = scope_label or '__ALL__'
        db = get_db()
        keyword_phrases = get_mining_detection_phrases(db)
        if not keyword_phrases:
            return jsonify({'success': False, 'error': {
                'code': 'MISSING_METRIC_KEYWORDS',
                'message': (
                    "Extraction requires at least one active metric keyword in metric_schema.keywords. "
                    "Add keywords in the metric keyword UI before starting LLM extraction."
                ),
            }}), 400

        # 409 guard — prevent duplicate extraction runs
        with _active_tickers_lock:
            if run_key in _active_tickers:
                return jsonify({'success': False, 'error': {
                    'code': 'ALREADY_RUNNING',
                    'message': f"Extraction already running for {scope_label}",
                }}), 409
            _active_tickers.add(run_key)

        task_id = str(uuid.uuid4())
        with _progress_lock:
            _extraction_progress[task_id] = {
                'status': 'running',
                'ticker': ticker or 'ALL',
                'tickers': tickers or None,
                'scope_label': scope_label,
                'cadence': cadence,
                'from_period': from_period,
                'to_period': to_period,
                'extract_workers': extract_workers,
                'sample': sample_n if sample_n > 0 else None,
                'reports_processed': 0,
                'reports_total': 0,
                'data_points': 0,
                'errors': 0,
                'logs': [],
            }

        log.info(
            "Starting extraction task %s for scope %s (force=%s, warm_model=%s, extract_workers=%s)",
            task_id, scope_label, force, warm_model, extract_workers
        )

        # Capture loop-local values for the thread closure.
        _tickers = list(tickers)
        _scope_label = scope_label
        _cadence = cadence
        _from_period = from_period
        _to_period = to_period
        _extract_workers = extract_workers
        _sample_n = sample_n
        _expected_granularity = expected_granularity

        def _run():
            try:
                import random as _random
                from app_globals import get_db
                from interpreters.interpret_pipeline import extract_report
                from app_globals import get_registry
                from infra.ollama_warmup import warm_ollama_for_extraction, ensure_ollama_running
                from infra.db import MinerDB
                from routes.pipeline import _BufferedExtractionDB, _replay_staged_payload, _sort_reports_chronologically, _staged_status_for_payload

                registry = get_registry()

                source_types = _CADENCE_SOURCE_TYPES.get(_cadence) if _cadence != 'all' else None
                reports: list[dict] = []
                if _tickers:
                    for selected_ticker in _tickers:
                        _effective_from = _from_period or db.get_btc_first_filing_date(selected_ticker)
                        getter = db.get_all_reports_for_extraction if force else db.get_unextracted_reports
                        reports.extend(
                            getter(
                                ticker=selected_ticker,
                                source_types=source_types,
                                from_period=_effective_from,
                                to_period=_to_period,
                            )
                        )
                else:
                    getter = db.get_all_reports_for_extraction if force else db.get_unextracted_reports
                    reports = getter(
                        ticker=None,
                        source_types=source_types,
                        from_period=_from_period,
                        to_period=_to_period,
                    )

                if _sample_n > 0 and len(reports) > _sample_n:
                    reports = _random.sample(reports, _sample_n)
                    log.info(
                        "Task %s: sample mode — picked %d of %d reports",
                        task_id, _sample_n, len(reports) + (_sample_n - len(reports)),
                    )

                with _progress_lock:
                    _extraction_progress[task_id]['reports_total'] = len(reports)
                    if not reports:
                        logs = _extraction_progress[task_id]['logs']
                        logs.append(
                            "No stored reports matched the selected filters. "
                            "Ingest first if the source documents have not been added to reports yet."
                        )
                        if len(logs) > 200:
                            logs.pop(0)

                if warm_model and reports:
                    def _ops_log(msg: str) -> None:
                        ts = datetime.now(timezone.utc).strftime('%H:%M:%S')
                        with _progress_lock:
                            logs = _extraction_progress[task_id]['logs']
                            logs.append(f'[{ts}] [Ollama] {msg}')
                            if len(logs) > 200:
                                logs.pop(0)
                    ensure_ollama_running(log_fn=_ops_log)
                    warm_ollama_for_extraction(db=db, reason='operations_extract')

                from collections import OrderedDict
                grouped_reports: OrderedDict[str, list[dict]] = OrderedDict()
                if _tickers:
                    for selected_ticker in _tickers:
                        grouped_reports[selected_ticker] = []
                for report in reports:
                    grouped_reports.setdefault(report.get('ticker', '?'), []).append(report)

                processed_count = 0

                def _append_progress_line(line: str, pts: int = 0, errors: int = 0) -> None:
                    nonlocal processed_count
                    processed_count += 1
                    with _progress_lock:
                        _extraction_progress[task_id]['reports_processed'] = processed_count
                        _extraction_progress[task_id]['data_points'] += pts
                        _extraction_progress[task_id]['errors'] += errors
                        logs = _extraction_progress[task_id]['logs']
                        logs.append(line)
                        if len(logs) > 200:
                            logs.pop(0)

                def _build_run_config(report_ticker: str):
                    if not _expected_granularity:
                        return None
                    from miner_types import ExtractionRunConfig
                    return ExtractionRunConfig(
                        expected_granularity=_expected_granularity,
                        ticker=report_ticker,
                    )

                def _format_success_line(report_row: dict, summary) -> str:
                    ts = datetime.now(timezone.utc).strftime('%H:%M:%S')
                    r_ticker = report_row.get('ticker', '?')
                    r_period = report_row.get('covering_period') or report_row.get('report_date') or '?'
                    r_type = report_row.get('source_type', '?')
                    line = f'[{ts}] {r_ticker} {r_period} ({r_type}) -> {summary.data_points_extracted} pts, {summary.review_flagged} review'
                    if summary.errors:
                        line += f', {summary.errors} err'
                    try:
                        fresh = db.get_report(int(report_row['id']))
                    except Exception:
                        fresh = None
                    llm_summary = (fresh or {}).get('llm_summary') or report_row.get('llm_summary') or ''
                    if llm_summary:
                        line += f' | {llm_summary[:120]}'
                    return line

                for group_ticker, ticker_reports in grouped_reports.items():
                    ordered_reports = _sort_reports_chronologically(ticker_reports)
                    if not ordered_reports:
                        continue
                    effective_workers = max(1, min(int(_extract_workers), len(ordered_reports)))
                    log.info(
                        "Task %s: processing %d report(s) for %s with %d worker(s)",
                        task_id, len(ordered_reports), group_ticker, effective_workers
                    )
                    if effective_workers == 1:
                        for report in ordered_reports:
                            try:
                                summary = extract_report(report, db, registry, config=_build_run_config(report.get('ticker', '')))
                                _append_progress_line(
                                    _format_success_line(report, summary),
                                    pts=summary.data_points_extracted,
                                    errors=summary.errors,
                                )
                            except Exception as e:
                                ts = datetime.now(timezone.utc).strftime('%H:%M:%S')
                                period_label = report.get('covering_period') or report.get('report_date') or '?'
                                line = f'[{ts}] {group_ticker} {period_label} ERROR: {type(e).__name__}'
                                log.error("Task %s: error on report %d: %s", task_id, report.get('id'), e, exc_info=True)
                                _append_progress_line(line, errors=1)
                        continue

                    claim_db = MinerDB(db.db_path)
                    staged_reports: list[tuple[dict, int]] = []
                    for idx, report in enumerate(ordered_reports):
                        report_id = report.get('id')
                        if report_id is None:
                            continue
                        report_id = int(report_id)
                        worker_id = idx % effective_workers
                        if force:
                            claim_db.mark_report_extraction_running(report_id)
                            claimed = True
                        else:
                            claimed = claim_db.claim_report_for_extraction(report_id)
                        if not claimed:
                            continue
                        claimed_report = claim_db.get_report(report_id)
                        if claimed_report:
                            staged_reports.append((claimed_report, worker_id))

                    def _run_buffered_extraction(claimed_report: dict, worker_id: int) -> dict:
                        local_db = MinerDB(db.db_path)
                        buffered_db = _BufferedExtractionDB(local_db)
                        summary = extract_report(
                            claimed_report,
                            buffered_db,
                            registry,
                            config=_build_run_config(claimed_report.get('ticker', '')),
                        )
                        payload = buffered_db.staged_payload()
                        return {
                            'report': claimed_report,
                            'worker_id': worker_id,
                            'summary': summary,
                            'payload': payload,
                            'queue_status': _staged_status_for_payload(payload),
                        }

                    with ThreadPoolExecutor(max_workers=effective_workers) as pool:
                        futures = [
                            pool.submit(_run_buffered_extraction, claimed_report, worker_id)
                            for claimed_report, worker_id in staged_reports
                        ]
                        for (claimed_report, _worker_id), future in zip(staged_reports, futures):
                            try:
                                result = future.result()
                                payload = result['payload']
                                queue_status = result['queue_status']
                                if payload:
                                    _replay_staged_payload(db, payload)
                                if queue_status == 'failed':
                                    db.mark_report_extraction_failed(int(claimed_report['id']), 'staged extraction failed')
                                line = _format_success_line(claimed_report, result['summary'])
                                _append_progress_line(
                                    line,
                                    pts=result['summary'].data_points_extracted,
                                    errors=result['summary'].errors,
                                )
                            except Exception as e:
                                db.mark_report_extraction_failed(int(claimed_report['id']), str(e)[:500])
                                ts = datetime.now(timezone.utc).strftime('%H:%M:%S')
                                period_label = claimed_report.get('covering_period') or claimed_report.get('report_date') or '?'
                                line = f'[{ts}] {group_ticker} {period_label} ERROR: {type(e).__name__}'
                                log.error("Task %s: error on report %d: %s", task_id, claimed_report.get('id'), e, exc_info=True)
                                _append_progress_line(line, errors=1)

                with _progress_lock:
                    _extraction_progress[task_id]['status'] = 'complete'
                log.info("Task %s complete for %s", task_id, _scope_label)
            except Exception as e:
                log.error("Task %s failed: %s", task_id, e, exc_info=True)
                with _progress_lock:
                    _extraction_progress[task_id]['status'] = 'error'
                    _extraction_progress[task_id]['error_message'] = 'Internal error'
            finally:
                with _active_tickers_lock:
                    _active_tickers.discard(run_key)

        t = threading.Thread(target=_run, daemon=True, name=f"extract-{run_key}")
        t.start()

        return jsonify({'success': True, 'data': {
            'task_id': task_id,
            'ticker': ticker or 'ALL',
            'tickers': tickers or None,
            'scope_label': scope_label,
            'extract_workers': extract_workers,
        }})
    except Exception:
        log.error('Error in operations_extract', exc_info=True)
        return jsonify({'success': False, 'error': {'message': 'Internal server error'}}), 500


@bp.route('/api/operations/interpret/<task_id>/progress')
def operations_extract_progress(task_id: str):
    """Return extraction task progress."""
    try:
        with _progress_lock:
            progress = _extraction_progress.get(task_id)
        if progress is None:
            return jsonify({'success': False, 'error': {'message': 'Task not found'}}), 404
        return jsonify({'success': True, 'data': progress})
    except Exception:
        log.error('Error in operations_extract_progress', exc_info=True)
        return jsonify({'success': False, 'error': {'message': 'Internal server error'}}), 500


_PERIOD_RE = re.compile(r'^\d{4}-\d{2}-01$')


@bp.route('/api/operations/assign_period', methods=['POST'])
def operations_assign_period():
    """Assign a period to a legacy_undated manifest entry."""
    try:
        body = request.get_json(silent=True) or {}
        manifest_id = body.get('manifest_id')
        period = (body.get('period') or '').strip()

        if manifest_id is None:
            return jsonify({'success': False, 'error': {
                'code': 'INVALID_INPUT', 'message': "'manifest_id' is required",
            }}), 400
        if not period or not _PERIOD_RE.match(period):
            return jsonify({'success': False, 'error': {
                'code': 'INVALID_INPUT',
                'message': "'period' must be in YYYY-MM-01 format",
            }}), 400

        from app_globals import get_db
        db = get_db()
        db.update_manifest_period(int(manifest_id), period)
        log.info("Assigned period %s to manifest_id %s", period, manifest_id)
        return jsonify({'success': True, 'data': {'manifest_id': manifest_id, 'period': period}})
    except Exception:
        log.error('Error in operations_assign_period', exc_info=True)
        return jsonify({'success': False, 'error': {'message': 'Internal server error'}}), 500


@bp.route('/api/operations/manifest/<int:manifest_id>/preview')
def manifest_preview(manifest_id: int):
    """
    Serve the raw content of an undated archive file for inline viewing.

    HTML files are returned as-is (text/html) for iframe embedding.
    PDF files are parsed and returned as plain text (text/plain).
    """
    try:
        from app_globals import get_db
        from pathlib import Path

        db = get_db()
        entry = db.get_manifest_by_id(manifest_id)
        if entry is None:
            return jsonify({'success': False, 'error': {'message': 'Manifest entry not found'}}), 404

        file_path = entry.get('file_path') or ''
        if not file_path:
            return jsonify({'success': False, 'error': {'message': 'No file path recorded for this entry'}}), 404

        p = Path(file_path)
        if not p.exists():
            return jsonify({'success': False, 'error': {
                'message': f'File not found on disk: {p.name}'
            }}), 404

        suffix = p.suffix.lower()

        if suffix == '.html':
            content = p.read_text(encoding='utf-8', errors='replace')
            return Response(content, mimetype='text/html; charset=utf-8')

        if suffix == '.pdf':
            # Return parsed plain text so the viewer can display it without a PDF plugin.
            from parsers.press_release_parser import PressReleaseParser
            result = PressReleaseParser().parse(p)
            text = result.text or '(no text could be extracted from this PDF)'
            return Response(text, mimetype='text/plain; charset=utf-8')

        return jsonify({'success': False, 'error': {
            'message': f'Unsupported file type: {suffix}'
        }}), 400

    except Exception:
        log.error('Error in manifest_preview for id=%s', manifest_id, exc_info=True)
        return jsonify({'success': False, 'error': {'message': 'Internal server error'}}), 500


@bp.route('/api/operations/manifest/<int:manifest_id>/detect_period', methods=['POST'])
def manifest_detect_period(manifest_id: int):
    """
    Infer the reporting period for an undated archive file.

    Detection order:
      1. Rule-based: infer_period_from_filename() with read_body=True.
         Fast, no LLM call; works for most undated RIOT HTML files because
         the month name appears in the visible text near the document opening.
      2. LLM fallback: if rules return None, parse the file, take the first
         3000 chars of visible text, and ask Ollama for the reporting month.

    Response:
      {
        "success": true,
        "data": {
          "period": "2021-05-01",        // null if neither method found anything
          "confidence": 0.9,
          "method": "rule_based"         // or "llm" or "not_found"
        }
      }
    """
    try:
        from app_globals import get_db
        from pathlib import Path
        from scrapers.archive_ingestor import infer_period_from_filename

        db = get_db()
        entry = db.get_manifest_by_id(manifest_id)
        if entry is None:
            return jsonify({'success': False, 'error': {'message': 'Manifest entry not found'}}), 404

        file_path = entry.get('file_path') or ''
        if not file_path:
            return jsonify({'success': False, 'error': {'message': 'No file path recorded for this entry'}}), 404

        p = Path(file_path)
        if not p.exists():
            return jsonify({'success': False, 'error': {
                'message': f'File not found on disk: {p.name}'
            }}), 404

        # ── Stage 1: rule-based ────────────────────────────────────────────────
        rule_result = infer_period_from_filename(str(p), read_body=True)
        if rule_result is not None:
            period_str = rule_result.strftime('%Y-%m-01')
            log.info("manifest_detect_period: rule_based → %s for %s", period_str, p.name)
            return jsonify({'success': True, 'data': {
                'period': period_str,
                'confidence': 0.85,
                'method': 'rule_based',
            }})

        # ── Stage 2: LLM fallback ─────────────────────────────────────────────
        try:
            from parsers.press_release_parser import PressReleaseParser
            from config import LLM_BASE_URL, LLM_MODEL_ID, LLM_TIMEOUT_SECONDS
            import requests as _requests

            parse_result = PressReleaseParser().parse(p)
            sample_text = (parse_result.text or '')[:3000]

            if not sample_text.strip():
                return jsonify({'success': True, 'data': {
                    'period': None, 'confidence': 0.0, 'method': 'not_found',
                }})

            prompt = (
                "You are a financial document analyst. Read the following excerpt from a Bitcoin "
                "mining company press release and determine the single calendar month it reports "
                "production data for.\n\n"
                "Return ONLY a JSON object in this exact format, no other text:\n"
                '{"period": "YYYY-MM-01", "confidence": 0.0}\n\n'
                "Use null for period if you cannot determine it. "
                "Confidence should be 0.0-1.0 based on how clearly the month is stated.\n\n"
                f"Document excerpt:\n{sample_text}"
            )

            resp = _requests.post(
                f"{LLM_BASE_URL}/api/generate",
                json={
                    'model': LLM_MODEL_ID,
                    'prompt': prompt,
                    'stream': False,
                    'options': {'temperature': 0.0},
                },
                timeout=LLM_TIMEOUT_SECONDS,
            )
            resp.raise_for_status()
            raw = resp.json().get('response', '').strip()

            # Strip markdown code fences if present
            raw = re.sub(r'^```[a-z]*\s*', '', raw, flags=re.MULTILINE)
            raw = re.sub(r'```\s*$', '', raw, flags=re.MULTILINE).strip()

            parsed = json.loads(raw)
            period = parsed.get('period')
            confidence = float(parsed.get('confidence', 0.0))

            # Validate YYYY-MM-01 format
            if period and not re.match(r'^\d{4}-\d{2}-01$', period):
                period = None
                confidence = 0.0

            log.info("manifest_detect_period: LLM → %s (conf=%.2f) for %s", period, confidence, p.name)
            return jsonify({'success': True, 'data': {
                'period': period,
                'confidence': confidence,
                'method': 'llm' if period else 'not_found',
            }})

        except Exception as llm_err:
            log.warning("manifest_detect_period: LLM failed for %s: %s", p.name, llm_err)
            return jsonify({'success': True, 'data': {
                'period': None, 'confidence': 0.0, 'method': 'not_found',
            }})

    except Exception:
        log.error('Error in manifest_detect_period for id=%s', manifest_id, exc_info=True)
        return jsonify({'success': False, 'error': {'message': 'Internal server error'}}), 500


@bp.route('/api/delete/scrape/ticker', methods=['POST'])
@bp.route('/api/operations/purge_ticker', methods=['POST'])
def purge_ticker():
    """Canonical ticker-scoped SCRAPE-stage delete endpoint.

    Cascades through every downstream layer in FK-safe order:
      data_points → review_queue → final_data_points
    Also resets reports.extraction_status = 'pending'.

    Body: { "ticker": "MARA" }
    Returns: { "data_points_deleted": N, "review_queue_deleted": N, "final_data_points_deleted": N }
    """
    from app_globals import get_db as _get_db

    body = request.get_json(silent=True) or {}
    ticker = (body.get('ticker') or '').strip().upper()
    if not ticker:
        return jsonify({'error': 'ticker is required'}), 400

    try:
        db = _get_db()
        dp_count = db.purge_data_points(ticker=ticker)
        rq_count = db.purge_review_queue(ticker=ticker)
        fp_result = db.purge_final_data_points(ticker=ticker, mode='clear')
        fp_count = fp_result.get('deleted', 0)
        log.info(
            'purge_ticker ticker=%s data_points=%d review_queue=%d final_data_points=%d',
            ticker, dp_count, rq_count, fp_count,
        )
        return jsonify({
            'data_points_deleted': dp_count,
            'review_queue_deleted': rq_count,
            'final_data_points_deleted': fp_count,
        })
    except Exception:
        log.error('purge_ticker_error ticker=%s', ticker, exc_info=True)
        return jsonify({'error': 'Internal server error'}), 500


@bp.route('/api/operations/gap-fill', methods=['POST'])
def gap_fill():
    """POST /api/operations/gap-fill — infer missing monthly data_points from quarterly.

    Body: { "ticker": "MARA", "dry_run": false }
    Returns: { "filled": N, "skipped": N, "errors": N, "rows": [...] }
    """
    from app_globals import get_db as _get_db
    from interpreters.gap_fill import fill_quarterly_gaps

    body = request.get_json(silent=True) or {}
    ticker = (body.get('ticker') or '').strip().upper()
    dry_run = bool(body.get('dry_run', False))

    if not ticker:
        return jsonify({'error': 'ticker is required'}), 400

    try:
        db = _get_db()
        result = fill_quarterly_gaps(ticker=ticker, db=db, dry_run=dry_run)
        return jsonify(result)
    except Exception:
        log.error('gap_fill_error ticker=%s', ticker, exc_info=True)
        return jsonify({'error': 'Internal server error'}), 500


@bp.route('/operations')
def operations_page():
    """Redirect to unified ops page, companies tab."""
    return redirect('/ops?tab=companies')
