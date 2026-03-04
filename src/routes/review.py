"""
Review queue API routes.

Provides endpoints for the analyst review UI:
  GET  /api/review                    — list review items (paginated, filterable by status)
  GET  /api/review/<id>/document      — full document text + snippet positions
  POST /api/review/<id>/reextract     — re-run extraction on analyst-selected text
  POST /api/review/<id>/approve       — approve and store the value
  POST /api/review/<id>/reject        — reject with required note
  POST /api/review/<id>/edit          — correct value and approve
"""
import logging

from flask import Blueprint, jsonify, request

log = logging.getLogger('miners.routes.review')

bp = Blueprint('review', __name__)


@bp.route('/api/review')
def list_review_items():
    """Return paginated review queue items with LLM/regex comparison fields."""
    try:
        from app_globals import get_db
        db = get_db()

        status = request.args.get('status')
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

        items = db.get_review_items(status=status, limit=limit, offset=offset)
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

        # Find the report associated with this ticker + period
        report = db.find_report_for_period(item['ticker'], item['period'])
        if report is None:
            return jsonify({'success': True, 'data': {
                'raw_text': '',
                'source_url': None,
                'llm_snippet': item.get('source_snippet'),
                'regex_snippet': item.get('source_snippet'),
            }})

        raw_text = db.get_report_raw_text(report['id']) or ''
        return jsonify({'success': True, 'data': {
            'raw_text': raw_text,
            'source_url': report.get('source_url'),
            'llm_value': item.get('llm_value'),
            'regex_value': item.get('regex_value'),
            'agreement_status': item.get('agreement_status'),
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
        from app_globals import get_db, get_registry
        import requests as req_lib
        from extractors.llm_extractor import LLMExtractor
        from extractors.extractor import extract_all

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
        registry = get_registry()
        patterns = registry.metrics.get(metric, [])

        # Run regex on selection
        regex_results = extract_all(selection, patterns, metric)
        regex_best = regex_results[0] if regex_results else None

        # Run LLM on selection
        session = req_lib.Session()
        llm = LLMExtractor(session=session, db=db)
        llm_result = None
        if llm.check_connectivity():
            llm_result = llm.extract(selection, metric)

        candidates = []
        if regex_best:
            candidates.append({
                'source': 'regex',
                'value': regex_best.value,
                'unit': regex_best.unit,
                'confidence': regex_best.confidence,
                'pattern_id': regex_best.pattern_id,
            })
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
