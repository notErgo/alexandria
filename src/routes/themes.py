"""Theme discovery endpoint — scans static/themes/*.json."""
import os
import logging
from flask import Blueprint, jsonify, current_app

log = logging.getLogger('miners.routes.themes')
bp = Blueprint('themes', __name__)


@bp.route('/api/themes')
def list_themes():
    try:
        themes_dir = os.path.join(current_app.static_folder, 'themes')
        if not os.path.isdir(themes_dir):
            return jsonify({'success': True, 'data': []})
        out = []
        for fname in sorted(os.listdir(themes_dir)):
            if not fname.endswith('.json'):
                continue
            theme_id = fname[:-5]                         # strip .json
            label = theme_id.replace('-', ' ').title()    # "hermeneutic-dark" → "Hermeneutic Dark"
            out.append({'id': theme_id, 'label': label})
        return jsonify({'success': True, 'data': out})
    except Exception as e:
        log.error("list_themes failed: %s", e, exc_info=True)
        return jsonify({'success': False, 'error': {'message': 'Internal server error'}}), 500
