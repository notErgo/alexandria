"""
Search Keywords API — legacy shim (search_keywords table, v26–v31).

This route exists for backward compatibility only. search_keywords is empty
after v30 and the table is kept for schema compatibility. Keyword management
has moved to /api/metric_schema/<key>/keywords (per-metric, v30+).
"""
import logging
import sqlite3

from flask import Blueprint, jsonify, request

log = logging.getLogger('miners.routes.keywords')

bp = Blueprint('keywords', __name__)


@bp.route('/api/keywords')
def list_keywords():
    try:
        from app_globals import get_db
        db = get_db()
        active_only = request.args.get('all') != '1'
        keywords = db.get_search_keywords(active_only=active_only)
        return jsonify({'success': True, 'data': {'keywords': keywords, 'total': len(keywords)}})
    except Exception:
        log.error('Error listing search keywords', exc_info=True)
        return jsonify({'success': False, 'error': {'message': 'Internal server error'}}), 500


@bp.route('/api/keywords', methods=['POST'])
def add_keyword():
    try:
        from app_globals import get_db
        db = get_db()

        body = request.get_json(silent=True) or {}
        phrase = body.get('phrase', '')
        if not phrase or not phrase.strip():
            return jsonify({'success': False, 'error': {
                'code': 'INVALID_INPUT',
                'message': "'phrase' is required and must be non-empty",
            }}), 400

        phrase = phrase.strip()
        if not (phrase.startswith('"') and phrase.endswith('"')):
            phrase = f'"{phrase}"'
        notes = body.get('notes', '')

        try:
            kw_id = db.add_search_keyword(phrase, notes=notes)
        except sqlite3.IntegrityError:
            return jsonify({'success': False, 'error': {
                'code': 'DUPLICATE',
                'message': f"Keyword '{phrase}' already exists",
            }}), 409

        return jsonify({'success': True, 'data': {'id': kw_id, 'phrase': phrase}}), 201
    except Exception:
        log.error('Error adding search keyword', exc_info=True)
        return jsonify({'success': False, 'error': {'message': 'Internal server error'}}), 500


@bp.route('/api/keywords/<int:kw_id>', methods=['PATCH'])
def update_keyword(kw_id):
    try:
        from app_globals import get_db
        db = get_db()

        body = request.get_json(silent=True) or {}
        active = body.get('active')
        notes = body.get('notes')

        updated = db.update_search_keyword(kw_id, active=active, notes=notes)
        if not updated:
            return jsonify({'success': False, 'error': {
                'code': 'NOT_FOUND',
                'message': f"Keyword {kw_id} not found",
            }}), 404

        return jsonify({'success': True})
    except Exception:
        log.error('Error updating search keyword %s', kw_id, exc_info=True)
        return jsonify({'success': False, 'error': {'message': 'Internal server error'}}), 500


@bp.route('/api/keywords/<int:kw_id>', methods=['DELETE'])
def delete_keyword(kw_id):
    try:
        from app_globals import get_db
        db = get_db()

        deleted = db.delete_search_keyword(kw_id)
        if not deleted:
            return jsonify({'success': False, 'error': {
                'code': 'NOT_FOUND',
                'message': f"Keyword {kw_id} not found",
            }}), 404

        return jsonify({'success': True})
    except Exception:
        log.error('Error deleting search keyword %s', kw_id, exc_info=True)
        return jsonify({'success': False, 'error': {'message': 'Internal server error'}}), 500
