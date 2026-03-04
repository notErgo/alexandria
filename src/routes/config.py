"""
Config settings and ticker hint routes.

  GET  /api/config                    — list all config_settings entries
  POST /api/config/<key>              — upsert a config_settings key
  GET  /api/config/<key>/default      — return hardcoded default for a known key
  GET  /api/config/hints              — list all ticker hints
  GET  /api/config/hints/<ticker>     — get hint for a specific ticker
  POST /api/config/hints/<ticker>     — upsert hint for a ticker
  GET  /api/ollama/models             — list models available in Ollama (live or disk)
"""
import logging
import os
from pathlib import Path

from flask import Blueprint, jsonify, request

log = logging.getLogger('miners.routes.config')

bp = Blueprint('config', __name__)

# Keys with hardcoded defaults
_KNOWN_CONFIG_KEYS = {'llm_batch_preamble', 'ollama_model'}

# Standard Ollama manifest directory (model weights live here regardless of
# how the binary was installed — Homebrew puts the binary at
# /opt/homebrew/bin/ollama but models are always stored in ~/.ollama/models/).
_OLLAMA_MANIFEST_DIR = Path.home() / '.ollama' / 'models' / 'manifests' / 'registry.ollama.ai' / 'library'


def _get_default_for_key(key: str):
    if key == 'llm_batch_preamble':
        from extractors.llm_extractor import _DEFAULT_BATCH_PREAMBLE
        return _DEFAULT_BATCH_PREAMBLE
    if key == 'ollama_model':
        from config import LLM_MODEL_ID
        return LLM_MODEL_ID
    return None


def _list_models_from_disk() -> list[dict]:
    """Scan ~/.ollama/models/manifests to enumerate installed models.

    Returns a list of {"name": "model:tag"} dicts, sorted alphabetically.
    Used as a fallback when the Ollama daemon is not running.
    """
    models = []
    if not _OLLAMA_MANIFEST_DIR.exists():
        return models
    for model_dir in sorted(_OLLAMA_MANIFEST_DIR.iterdir()):
        if not model_dir.is_dir():
            continue
        model_name = model_dir.name
        for tag_file in sorted(model_dir.iterdir()):
            if tag_file.is_file():
                models.append({"name": f"{model_name}:{tag_file.name}"})
    return models


@bp.route('/api/config')
def list_config():
    try:
        from app_globals import get_db
        db = get_db()
        entries = db.list_config()
        return jsonify({'success': True, 'data': {'config': entries}})
    except Exception:
        log.error('Error listing config settings', exc_info=True)
        return jsonify({'success': False, 'error': {'message': 'Internal server error'}}), 500


@bp.route('/api/config/<key>', methods=['POST'])
def set_config(key):
    try:
        from app_globals import get_db
        db = get_db()

        if len(key) > 100:
            return jsonify({'success': False, 'error': {
                'code': 'INVALID_INPUT',
                'message': 'key must be ≤100 characters'
            }}), 400

        body = request.get_json(silent=True) or {}
        value = body.get('value', '')
        if not isinstance(value, str):
            return jsonify({'success': False, 'error': {
                'code': 'INVALID_INPUT',
                'message': "'value' must be a string"
            }}), 400
        if len(value) > 20000:
            return jsonify({'success': False, 'error': {
                'code': 'INVALID_INPUT',
                'message': "'value' must be ≤20000 characters"
            }}), 400

        db.set_config(key, value)
        return jsonify({'success': True})
    except Exception:
        log.error('Error setting config key %s', key, exc_info=True)
        return jsonify({'success': False, 'error': {'message': 'Internal server error'}}), 500


@bp.route('/api/config/<key>/default')
def get_config_default(key):
    try:
        default = _get_default_for_key(key)
        if default is None:
            return jsonify({'success': False, 'error': {
                'code': 'NOT_FOUND',
                'message': f"No hardcoded default for key '{key}'"
            }}), 404
        return jsonify({'success': True, 'data': {'default': default}})
    except Exception:
        log.error('Error fetching default for config key %s', key, exc_info=True)
        return jsonify({'success': False, 'error': {'message': 'Internal server error'}}), 500


@bp.route('/api/ollama/models')
def list_ollama_models():
    """Return available Ollama models.

    Tries the live Ollama daemon first (/api/tags).  Falls back to scanning
    the manifest directory on disk so the endpoint works even when the daemon
    is not running.
    """
    try:
        import requests as _requests
        from config import LLM_BASE_URL
        try:
            resp = _requests.get(f"{LLM_BASE_URL}/api/tags", timeout=3)
            if resp.ok:
                data = resp.json()
                models = [{"name": m["name"]} for m in data.get("models", [])]
                return jsonify({'success': True, 'data': {'models': models, 'source': 'daemon'}})
        except Exception:
            pass  # Daemon not reachable — fall through to disk scan

        models = _list_models_from_disk()
        return jsonify({'success': True, 'data': {'models': models, 'source': 'disk'}})
    except Exception:
        log.error('Error listing Ollama models', exc_info=True)
        return jsonify({'success': False, 'error': {'message': 'Internal server error'}}), 500


@bp.route('/api/config/hints')
def list_hints():
    try:
        from app_globals import get_db
        db = get_db()
        hints = db.list_ticker_hints()
        return jsonify({'success': True, 'data': {'hints': hints}})
    except Exception:
        log.error('Error listing ticker hints', exc_info=True)
        return jsonify({'success': False, 'error': {'message': 'Internal server error'}}), 500


@bp.route('/api/config/hints/<ticker>')
def get_hint(ticker):
    try:
        from app_globals import get_db
        db = get_db()
        hint = db.get_ticker_hint(ticker.upper())
        return jsonify({'success': True, 'data': {'hint': hint}})
    except Exception:
        log.error('Error fetching hint for %s', ticker, exc_info=True)
        return jsonify({'success': False, 'error': {'message': 'Internal server error'}}), 500


@bp.route('/api/config/hints/<ticker>', methods=['POST'])
def upsert_hint(ticker):
    try:
        from app_globals import get_db
        db = get_db()

        body = request.get_json(silent=True) or {}
        hint = body.get('hint', '').strip()
        if len(hint) > 2000:
            return jsonify({'success': False, 'error': {
                'code': 'INVALID_INPUT',
                'message': "'hint' must be ≤2000 characters"
            }}), 400

        if hint:
            db.upsert_ticker_hint(ticker.upper(), hint)
        else:
            # Empty hint = delete (set active=0)
            with db._get_connection() as conn:
                conn.execute(
                    "UPDATE llm_ticker_hints SET active=0 WHERE ticker=?",
                    (ticker.upper(),)
                )
        return jsonify({'success': True})
    except Exception:
        log.error('Error upserting hint for %s', ticker, exc_info=True)
        return jsonify({'success': False, 'error': {'message': 'Internal server error'}}), 500
