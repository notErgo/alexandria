"""
Operations panel API routes.

  GET  /api/operations/queue                       — pending extraction queue + legacy files
  POST /api/operations/extract                     — trigger extraction for a ticker
  GET  /api/operations/extract/<task_id>/progress  — extraction progress
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

from flask import Blueprint, jsonify, request, render_template, Response, redirect

log = logging.getLogger('miners.routes.operations')

bp = Blueprint('operations', __name__)

# ── In-memory state for background extraction tasks ──────────────────────────
_active_tickers: set = set()
_active_tickers_lock = threading.Lock()
_extraction_progress: dict = {}
_progress_lock = threading.Lock()


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


@bp.route('/api/operations/extract', methods=['POST'])
def operations_extract():
    """Trigger background extraction for a ticker (or all tickers). Returns task_id."""
    try:
        body = request.get_json(silent=True) or {}
        ticker = (body.get('ticker') or '').strip().upper() or None
        force = bool(body.get('force', False))
        run_key = ticker or '__ALL__'

        # 409 guard — prevent duplicate extraction runs
        with _active_tickers_lock:
            if run_key in _active_tickers:
                return jsonify({'success': False, 'error': {
                    'code': 'ALREADY_RUNNING',
                    'message': f"Extraction already running for {ticker or 'ALL'}",
                }}), 409
            _active_tickers.add(run_key)

        task_id = str(uuid.uuid4())
        with _progress_lock:
            _extraction_progress[task_id] = {
                'status': 'running',
                'ticker': ticker or 'ALL',
                'reports_processed': 0,
                'reports_total': 0,
                'data_points': 0,
                'errors': 0,
            }

        log.info("Starting extraction task %s for ticker %s (force=%s)", task_id, ticker or 'ALL', force)

        def _run():
            try:
                from app_globals import get_db
                from extractors.extraction_pipeline import extract_report
                from extractors.pattern_registry import PatternRegistry
                from app_globals import get_registry

                db = get_db()
                registry = get_registry()

                reports = db.get_all_reports_for_extraction(ticker=ticker) if force \
                    else db.get_unextracted_reports(ticker=ticker)

                with _progress_lock:
                    _extraction_progress[task_id]['reports_total'] = len(reports)

                for i, report in enumerate(reports):
                    try:
                        summary = extract_report(report, db, registry)
                        with _progress_lock:
                            _extraction_progress[task_id]['reports_processed'] = i + 1
                            _extraction_progress[task_id]['data_points'] += summary.data_points_extracted
                            _extraction_progress[task_id]['errors'] += summary.errors
                        log.info("Task %s: processed report %d/%d for %s", task_id, i + 1, len(reports), ticker or 'ALL')
                    except Exception as e:
                        log.error("Task %s: error on report %d: %s", task_id, report.get('id'), e, exc_info=True)
                        with _progress_lock:
                            _extraction_progress[task_id]['errors'] += 1

                with _progress_lock:
                    _extraction_progress[task_id]['status'] = 'complete'
                log.info("Task %s complete for %s", task_id, ticker or 'ALL')
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

        return jsonify({'success': True, 'data': {'task_id': task_id, 'ticker': ticker or 'ALL'}})
    except Exception:
        log.error('Error in operations_extract', exc_info=True)
        return jsonify({'success': False, 'error': {'message': 'Internal server error'}}), 500


@bp.route('/api/operations/extract/<task_id>/progress')
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


@bp.route('/operations')
def operations_page():
    """Redirect to unified ops page, companies tab."""
    return redirect('/ops?tab=companies')
