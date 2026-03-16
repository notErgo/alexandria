"""
Review queue API routes.

Provides endpoints for the analyst review UI:
  GET  /api/review                    — list review items (paginated, filterable by status)
  GET  /api/review/<id>/document      — full document text + snippet positions
  POST /api/delete/review             — purge review artifacts without touching scraped reports
  POST /api/review/<id>/reextract     — re-run LLM extraction on analyst-selected text (requires item)
  POST /api/review/reextract_selection — re-run LLM extraction by metric + selection (no item needed)
  POST /api/review/<id>/approve       — approve and store the value
  POST /api/review/<id>/reject        — reject with required note
  POST /api/review/<id>/edit          — correct value and approve
"""
import logging

from flask import Blueprint, jsonify, request
from infra.text_utils import extract_document_title

log = logging.getLogger('miners.routes.review')

bp = Blueprint('review', __name__)


@bp.route('/api/delete/review', methods=['POST'])
@bp.route('/api/review/purge', methods=['POST'])
def purge_review_artifacts():
    """Purge review-layer artifacts while preserving scraped source reports."""
    try:
        from app_globals import get_db
        db = get_db()

        body = request.get_json(silent=True) or {}
        if not body.get('confirm'):
            return jsonify({'success': False, 'error': {
                'code': 'CONFIRM_REQUIRED',
                'message': 'Request body must include {"confirm": true}',
            }}), 400

        ticker = body.get('ticker')
        if ticker is not None:
            ticker = str(ticker).strip().upper() or None
            if ticker and not db.get_company(ticker):
                return jsonify({'success': False, 'error': {
                    'code': 'INVALID_TICKER',
                    'message': f'Ticker {ticker!r} not recognized',
                }}), 400

        raw_targets = body.get('targets')
        if raw_targets is None:
            targets = {'queue', 'final'}
        elif isinstance(raw_targets, list):
            targets = {str(t).strip().lower() for t in raw_targets if str(t).strip()}
        else:
            return jsonify({'success': False, 'error': {
                'code': 'INVALID_INPUT',
                'message': "'targets' must be a list containing 'queue' and/or 'final'",
            }}), 400

        valid_targets = {'queue', 'final'}
        if not targets or not targets.issubset(valid_targets):
            return jsonify({'success': False, 'error': {
                'code': 'INVALID_INPUT',
                'message': "'targets' must contain at least one of ['final', 'queue']",
            }}), 400

        final_mode = str(body.get('final_mode') or 'clear').strip().lower()
        if final_mode not in {'clear', 'archive'}:
            return jsonify({'success': False, 'error': {
                'code': 'INVALID_INPUT',
                'message': "'final_mode' must be one of ['archive', 'clear']",
            }}), 400

        reason = str(body.get('reason') or '').strip() or None
        counts = {
            'data_points_deleted': 0,
            'review_queue_deleted': 0,
            'final_data_points_deleted': 0,
            'final_archive_batch_id': None,
        }

        counts['data_points_deleted'] = db.purge_data_points(ticker=ticker)
        if 'queue' in targets:
            counts['review_queue_deleted'] = db.purge_review_queue(ticker=ticker)
        if 'final' in targets:
            result = db.purge_final_data_points(ticker=ticker, mode=final_mode, reason=reason)
            counts['final_data_points_deleted'] = int(result.get('deleted', 0))
            counts['final_archive_batch_id'] = result.get('archive_batch_id')

        log.info(
            "event=review_purge_complete ticker=%s targets=%s final_mode=%s counts=%s",
            ticker or 'ALL', sorted(targets), final_mode, counts,
        )
        return jsonify({'success': True, 'data': {
            'ticker': ticker or 'ALL',
            'targets': sorted(targets),
            'reports_preserved': True,
            'counts': counts,
        }})
    except Exception:
        log.error("Error purging review artifacts", exc_info=True)
        return jsonify({'success': False, 'error': {'message': 'Internal server error'}}), 500


@bp.route('/api/review')
def list_review_items():
    """Return paginated review queue items."""
    try:
        from app_globals import get_db
        db = get_db()

        status = request.args.get('status')
        ticker = request.args.get('ticker') or None
        period = request.args.get('period') or None
        metric = request.args.get('metric') or None
        try:
            limit = int(request.args.get('limit', 50))
            offset = int(request.args.get('offset', 0))
            if limit < 1 or limit > 200:
                raise ValueError
        except (ValueError, TypeError):
            return jsonify({'success': False, 'error': {
                'code': 'INVALID_PARAM',
                'message': "'limit' must be an integer between 1 and 200"
            }}), 400

        items = db.get_review_items(
            status=status, limit=limit, offset=offset,
            ticker=ticker, period=period, metric=metric,
        )
        total = db.count_review_items(status=status or 'PENDING')
        return jsonify({'success': True, 'data': {'items': items, 'total': total}})
    except Exception:
        log.error("Error listing review items", exc_info=True)
        return jsonify({'success': False, 'error': {'message': 'Internal server error'}}), 500


@bp.route('/api/review/<int:item_id>/document')
def get_review_document(item_id):
    """Return the full document text for a review item."""
    try:
        from app_globals import get_db
        db = get_db()

        item = db.get_review_item(item_id)
        if item is None:
            return jsonify({'success': False, 'error': {'message': 'Review item not found'}}), 404

        # Use report_id for direct lookup when available; fall back to period-based lookup.
        report_id = item.get('report_id')
        if report_id:
            report = db.get_report(report_id)
        else:
            report = db.find_report_for_period(item['ticker'], item['period'])
        if report is None:
            return jsonify({'success': True, 'data': {
                'raw_text': '',
                'source_url': None,
                'llm_snippet': item.get('source_snippet'),
                'regex_snippet': item.get('source_snippet'),
            }})

        raw_text = db.get_report_raw_text(report['id']) or ''
        raw_html = db.get_report_raw_html(report['id'])
        source_type = report.get('source_type') or ''
        if source_type.startswith('edgar_'):
            if raw_html:
                from infra.text_utils import edgar_to_plain, strip_edgar_boilerplate
                raw_text = strip_edgar_boilerplate(edgar_to_plain(raw_html))
            else:
                from infra.text_utils import strip_edgar_boilerplate
                raw_text = strip_edgar_boilerplate(raw_text)
        document_title = extract_document_title(raw_html, raw_text)
        return jsonify({'success': True, 'data': {
            'raw_text': raw_text,
            'document_title': document_title,
            'source_url': report.get('source_url'),
            'source_type': report.get('source_type'),
            'report_id': report.get('id'),
            'ticker': item.get('ticker'),
            'period': item.get('period'),
            'metric': item.get('metric'),
            'candidate_value': item.get('llm_value') if item.get('llm_value') is not None else item.get('raw_value'),
            'review_reason': item.get('agreement_status'),
            'source_snippet': item.get('source_snippet'),
        }})
    except Exception:
        log.error("Error fetching document for review item %d", item_id, exc_info=True)
        return jsonify({'success': False, 'error': {'message': 'Internal server error'}}), 500


@bp.route('/api/review/<int:item_id>/reextract', methods=['POST'])
def reextract_from_selection(item_id):
    """
    Re-run extraction on analyst-selected text.

    Request body: {"selection": "<selected text>"}
    Returns: {"metric": "...", "value": ..., "unit": "...", "confidence": ...}
    Does NOT write to DB — result is shown in UI for analyst to review.
    """
    try:
        from app_globals import get_db
        import requests as req_lib
        from interpreters.llm_interpreter import LLMInterpreter

        db = get_db()
        item = db.get_review_item(item_id)
        if item is None:
            return jsonify({'success': False, 'error': {'message': 'Review item not found'}}), 404

        body = request.get_json(silent=True) or {}
        selection = body.get('selection', '').strip()
        if not selection:
            return jsonify({'success': False, 'error': {
                'code': 'INVALID_INPUT',
                'message': "'selection' is required and must be non-empty"
            }}), 400
        if len(selection) > 5000:
            return jsonify({'success': False, 'error': {
                'code': 'INVALID_INPUT',
                'message': "'selection' must be ≤5000 characters"
            }}), 400

        metric = item['metric']
        ticker = item.get('ticker')
        session = req_lib.Session()
        llm = LLMInterpreter(session=session, db=db)
        candidates = []
        if llm.check_connectivity():
            batch = llm.extract_batch(selection, [metric], ticker=ticker)
            llm_result = batch.get(metric)
            if llm_result:
                candidates.append({
                    'source': 'llm',
                    'value': llm_result.value,
                    'unit': llm_result.unit,
                    'confidence': llm_result.confidence,
                    'pattern_id': llm_result.pattern_id,
                })

        return jsonify({'success': True, 'data': {
            'metric': metric,
            'candidates': candidates,
        }})
    except Exception:
        log.error("Error re-extracting for review item %d", item_id, exc_info=True)
        return jsonify({'success': False, 'error': {'message': 'Internal server error'}}), 500


@bp.route('/api/review/reextract_selection', methods=['POST'])
def reextract_selection():
    """
    Re-run extraction on analyst-selected text without a review item ID.

    Used from the miner_data.html cell panel when no pending review item exists.

    Request body: {"metric": "<metric>", "selection": "<selected text>"}
    Returns: {"metric": "...", "candidates": [...]}
    Does NOT write to DB.
    """
    try:
        from app_globals import get_db
        import requests as req_lib
        from interpreters.llm_interpreter import LLMInterpreter

        body = request.get_json(silent=True) or {}
        metric = (body.get('metric') or '').strip()
        selection = (body.get('selection') or '').strip()
        ticker = (body.get('ticker') or None)

        if not metric:
            return jsonify({'success': False, 'error': {
                'code': 'INVALID_INPUT', 'message': "'metric' is required"
            }}), 400
        if not selection:
            return jsonify({'success': False, 'error': {
                'code': 'INVALID_INPUT', 'message': "'selection' is required"
            }}), 400
        if len(selection) > 5000:
            return jsonify({'success': False, 'error': {
                'code': 'INVALID_INPUT', 'message': "'selection' must be <=5000 characters"
            }}), 400

        db = get_db()
        session = req_lib.Session()
        llm = LLMInterpreter(session=session, db=db)
        candidates = []
        if llm.check_connectivity():
            batch = llm.extract_batch(selection, [metric], ticker=ticker)
            llm_result = batch.get(metric)
            if llm_result:
                candidates.append({
                    'source': 'llm',
                    'value': llm_result.value,
                    'unit': llm_result.unit,
                    'confidence': llm_result.confidence,
                    'pattern_id': llm_result.pattern_id,
                })

        return jsonify({'success': True, 'data': {'metric': metric, 'candidates': candidates}})
    except Exception:
        log.error("Error in reextract_selection", exc_info=True)
        return jsonify({'success': False, 'error': {'message': 'Internal server error'}}), 500


@bp.route('/api/review/<int:item_id>/approve', methods=['POST'])
def approve_review_item(item_id):
    """
    Approve a review item.

    Optional body: {"value": <float>} — if provided, stores this value instead of raw_value.
    If value is provided, calls edit_review_item (stores as 'review_edited').
    If no value, calls approve_review_item (stores raw_value as 'review_approved').
    """
    try:
        from app_globals import get_db
        db = get_db()

        item = db.get_review_item(item_id)
        if item is None:
            return jsonify({'success': False, 'error': {'message': 'Review item not found'}}), 404

        body = request.get_json(silent=True) or {}
        corrected_value = body.get('value')

        if corrected_value is not None:
            try:
                corrected_value = float(corrected_value)
            except (TypeError, ValueError):
                return jsonify({'success': False, 'error': {
                    'code': 'INVALID_INPUT',
                    'message': "'value' must be a number"
                }}), 400
            note = body.get('note', 'Corrected by analyst')
            dp = db.edit_review_item(item_id, corrected_value, note)
        else:
            dp = db.approve_review_item(item_id)

        return jsonify({'success': True, 'data': {'data_point_id': dp.get('id')}})
    except ValueError as e:
        log.error("Approve review item %d: %s", item_id, e, exc_info=True)
        return jsonify({'success': False, 'error': {'message': 'Review item not found'}}), 404
    except Exception:
        log.error("Error approving review item %d", item_id, exc_info=True)
        return jsonify({'success': False, 'error': {'message': 'Internal server error'}}), 500


@bp.route('/api/review/batch-finalize', methods=['POST'])
def batch_finalize():
    """Finalize multiple review items in one call.

    Body: {"ids": [int, ...]}
    Returns: {"success": true, "data": {"finalized": N, "failed": N}}
    """
    try:
        from app_globals import get_db
        db = get_db()

        body = request.get_json(silent=True) or {}
        ids = body.get('ids')
        if not isinstance(ids, list) or not ids:
            return jsonify({'success': False, 'error': {
                'code': 'INVALID_INPUT',
                'message': "'ids' must be a non-empty list"
            }}), 400
        if len(ids) > 200:
            return jsonify({'success': False, 'error': {
                'code': 'INVALID_INPUT',
                'message': "'ids' must contain at most 200 items"
            }}), 400
        if not all(isinstance(i, int) for i in ids):
            return jsonify({'success': False, 'error': {
                'code': 'INVALID_INPUT',
                'message': "'ids' must be a list of integers"
            }}), 400

        finalized = 0
        failed = 0
        for item_id in ids:
            try:
                db.approve_review_item(item_id)
                finalized += 1
            except ValueError:
                failed += 1

        return jsonify({'success': True, 'data': {'finalized': finalized, 'failed': failed}})
    except Exception:
        log.error("Error in batch-finalize", exc_info=True)
        return jsonify({'success': False, 'error': {'message': 'Internal server error'}}), 500


@bp.route('/api/review/<int:item_id>/reject', methods=['POST'])
def reject_review_item(item_id):
    """
    Reject a review item. Note is required.

    Request body: {"note": "<reason for rejection>"}
    """
    try:
        from app_globals import get_db
        db = get_db()

        item = db.get_review_item(item_id)
        if item is None:
            return jsonify({'success': False, 'error': {'message': 'Review item not found'}}), 404

        body = request.get_json(silent=True) or {}
        note = body.get('note', '').strip()
        if not note:
            return jsonify({'success': False, 'error': {
                'code': 'NOTE_REQUIRED',
                'message': "'note' is required when rejecting a review item"
            }}), 400

        db.reject_review_item(item_id, note)
        return jsonify({'success': True})
    except Exception:
        log.error("Error rejecting review item %d", item_id, exc_info=True)
        return jsonify({'success': False, 'error': {'message': 'Internal server error'}}), 500


@bp.route('/api/review/batches', methods=['GET'])
def review_batches():
    """Return review_queue grouped by ingestion batch (date x ticker).

    Query params:
        ticker (optional) — limit to a single ticker
        status (optional, default PENDING) — row status to query

    Returns:
        {"success": true, "data": {"batches": [
          {"batch_date": "2026-03-15", "ticker": "MARA",
           "item_count": 403, "overlap_final": 285}, ...
        ]}}
    """
    try:
        from app_globals import get_db
        db = get_db()
        ticker = (request.args.get('ticker') or '').strip().upper() or None
        status = (request.args.get('status') or 'PENDING').strip().upper()
        batches = db.get_review_batches(ticker=ticker, status=status)
        return jsonify({'success': True, 'data': {'batches': batches}})
    except Exception:
        log.exception("review_batches failed")
        return jsonify({'success': False, 'error': {'message': 'Internal server error'}}), 500


@bp.route('/api/review/batch-delete', methods=['POST'])
def review_batch_delete():
    """Delete review_queue rows for a specific ingestion date without touching extraction_status.

    Body (JSON):
        created_date (str, required) — ISO date 'YYYY-MM-DD' to target
        ticker (str, optional)       — limit to one ticker
        dry_run (bool, optional)     — if true, return count without deleting

    Returns:
        {"success": true, "data": {"deleted": N, "dry_run": false}}
    """
    try:
        from app_globals import get_db
        db = get_db()

        body = request.get_json(silent=True) or {}
        created_date = (body.get('created_date') or '').strip()
        if not created_date:
            return jsonify({'success': False, 'error': {
                'code': 'MISSING_CREATED_DATE',
                'message': 'created_date is required',
            }}), 400

        ticker = (body.get('ticker') or '').strip().upper() or None
        dry_run = bool(body.get('dry_run', False))

        if dry_run:
            with db._get_connection() as conn:
                clauses = ["status='PENDING'", "date(created_at)=?"]
                params: list = [created_date]
                if ticker:
                    clauses.append('ticker=?')
                    params.append(ticker)
                where = ' AND '.join(clauses)
                count = conn.execute(
                    f"SELECT COUNT(*) FROM review_queue WHERE {where}", params
                ).fetchone()[0]
            return jsonify({'success': True, 'data': {'deleted': count, 'dry_run': True}})

        deleted = db.delete_review_items_by_filter(
            ticker=ticker, created_date=created_date, status='PENDING',
        )
        return jsonify({'success': True, 'data': {'deleted': deleted, 'dry_run': False}})
    except Exception:
        log.exception("review_batch_delete failed")
        return jsonify({'success': False, 'error': {'message': 'Internal server error'}}), 500


@bp.route('/api/review/<int:item_id>/no_data', methods=['POST'])
def no_data_review_item(item_id):
    """
    Mark a review item as 'no data' — analyst confirms this document contains
    no data for this metric. Writes a no_data verdict so the pipeline will
    not re-surface this (report, metric) pair.

    For LLM_EMPTY items: no confirmation required.
    For all other items: requires {"confirmed": true} in the request body.
    """
    try:
        from app_globals import get_db
        db = get_db()

        item = db.get_review_item(item_id)
        if item is None:
            return jsonify({'success': False, 'error': {'message': 'Review item not found'}}), 404

        if item.get('agreement_status') != 'LLM_EMPTY':
            body = request.get_json(silent=True) or {}
            if not body.get('confirmed'):
                return jsonify({'success': False, 'error': {
                    'code': 'CONFIRMATION_REQUIRED',
                    'message': "This item has data — set confirmed=true to mark as no_data",
                }}), 400

        report_id = item.get('report_id')
        metric = item['metric']

        if report_id is not None:
            db.upsert_report_metric_verdict(report_id, metric, 'no_data')

        db.reject_review_item(item_id, note='no_data')
        return jsonify({'success': True, 'data': {'verdict': 'no_data', 'item_id': item_id}})
    except Exception:
        log.error("Error in no_data_review_item %d", item_id, exc_info=True)
        return jsonify({'success': False, 'error': {'message': 'Internal server error'}}), 500
