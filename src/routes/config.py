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
import json
import logging
import os
from pathlib import Path

from flask import Blueprint, jsonify, request

log = logging.getLogger('miners.routes.config')

bp = Blueprint('config', __name__)

# Keys with hardcoded defaults
_KNOWN_CONFIG_KEYS = {
    # Existing
    'llm_batch_preamble', 'ollama_model', 'keyword_dictionary',
    # Extraction
    'confidence_review_threshold', 'agreement_threshold_default',
    'outlier_min_history', 'context_char_budget', 'context_char_budget_quarterly',
    'context_max_windows', 'context_fallback_confidence',
    # LLM
    'llm_timeout_seconds',
    'llm_quarterly_batch_preamble', 'llm_annual_batch_preamble',
    # Crawl
    'crawl_max_iterations', 'crawl_max_fetch_chars',
    'bitcoin_mining_keywords',
    # Pipeline
    'pipeline_output_dir',
}

_DEFAULT_KEYWORD_DICTIONARY = {
    'active_pack': 'btc_activity',
    'packs': {
        'btc_activity': [
            'bitcoin', 'btc', 'mined', 'production', 'hodl', 'holdings',
            'sold', 'treasury', 'encumbered', 'hashrate'
        ],
        'miners_deployed': [
            'miners', 'deployed', 'fleet', 'machines', 'rigs',
            'energized', 'installed', 'asic', 'efficiency', 'j/th'
        ],
        'ai_hpc_compute': [
            'ai', 'hpc', 'gpu', 'compute', 'cluster', 'hosting',
            'capacity', 'data center', 'megawatt', 'mw'
        ],
    },
}

# Standard Ollama manifest directory (model weights live here regardless of
# how the binary was installed — Homebrew puts the binary at
# /opt/homebrew/bin/ollama but models are always stored in ~/.ollama/models/).
_OLLAMA_MANIFEST_DIR = Path.home() / '.ollama' / 'models' / 'manifests' / 'registry.ollama.ai' / 'library'


def _get_default_for_key(key: str):
    if key == 'llm_batch_preamble':
        from interpreters.llm_interpreter import _DEFAULT_BATCH_PREAMBLE
        return _DEFAULT_BATCH_PREAMBLE
    if key == 'llm_quarterly_batch_preamble':
        from interpreters.llm_interpreter import _QUARTERLY_BATCH_PREAMBLE
        return _QUARTERLY_BATCH_PREAMBLE
    if key == 'llm_annual_batch_preamble':
        from interpreters.llm_interpreter import _ANNUAL_BATCH_PREAMBLE
        return _ANNUAL_BATCH_PREAMBLE
    if key == 'ollama_model':
        from config import LLM_MODEL_ID
        return LLM_MODEL_ID
    if key == 'keyword_dictionary':
        return json.dumps(_DEFAULT_KEYWORD_DICTIONARY)
    # Extraction knobs
    if key == 'confidence_review_threshold':
        from config import CONFIDENCE_REVIEW_THRESHOLD
        return str(CONFIDENCE_REVIEW_THRESHOLD)
    if key == 'agreement_threshold_default':
        from config import METRIC_AGREEMENT_THRESHOLD_DEFAULT
        return str(METRIC_AGREEMENT_THRESHOLD_DEFAULT)
    if key == 'outlier_min_history':
        from config import OUTLIER_MIN_HISTORY
        return str(OUTLIER_MIN_HISTORY)
    if key == 'context_char_budget':
        from config import CONTEXT_CHAR_BUDGET
        return str(CONTEXT_CHAR_BUDGET)
    if key == 'context_char_budget_quarterly':
        from config import CONTEXT_CHAR_BUDGET_QUARTERLY
        return str(CONTEXT_CHAR_BUDGET_QUARTERLY)
    if key == 'context_max_windows':
        return '3'
    if key == 'context_fallback_confidence':
        return '0.5'
    if key == 'llm_timeout_seconds':
        from config import LLM_TIMEOUT_SECONDS
        return str(LLM_TIMEOUT_SECONDS)
    if key == 'crawl_max_iterations':
        return '80'
    if key == 'crawl_max_fetch_chars':
        return '12000'
    if key == 'bitcoin_mining_keywords':
        from infra.db import MinerDB
        return ','.join(MinerDB._DEFAULT_BITCOIN_MINING_KEYWORDS)
    if key == 'pipeline_output_dir':
        return '/private/tmp/claude-501/miners_progress'
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


@bp.route('/api/config/keyword_dictionary')
def get_keyword_dictionary():
    """Return global keyword highlight dictionary used across review/explorer panels."""
    try:
        from app_globals import get_db
        db = get_db()
        raw = db.get_config('keyword_dictionary')
        if not raw:
            return jsonify({'success': True, 'data': {'dictionary': _DEFAULT_KEYWORD_DICTIONARY}})
        try:
            parsed = json.loads(raw)
            if not isinstance(parsed, dict):
                raise ValueError("dictionary must be object")
            return jsonify({'success': True, 'data': {'dictionary': parsed}})
        except Exception:
            log.warning("Invalid keyword_dictionary config; falling back to default")
            return jsonify({'success': True, 'data': {'dictionary': _DEFAULT_KEYWORD_DICTIONARY}})
    except Exception:
        log.error('Error fetching keyword dictionary', exc_info=True)
        return jsonify({'success': False, 'error': {'message': 'Internal server error'}}), 500


@bp.route('/api/config/keyword_dictionary', methods=['POST'])
def set_keyword_dictionary():
    """Upsert keyword dictionary config with normalized lowercase term lists."""
    try:
        from app_globals import get_db
        db = get_db()

        body = request.get_json(silent=True) or {}
        dictionary = body.get('dictionary')
        if not isinstance(dictionary, dict):
            return jsonify({'success': False, 'error': {
                'code': 'INVALID_INPUT',
                'message': "'dictionary' must be an object"
            }}), 400

        packs = dictionary.get('packs')
        if not isinstance(packs, dict) or not packs:
            return jsonify({'success': False, 'error': {
                'code': 'INVALID_INPUT',
                'message': "'packs' must be a non-empty object"
            }}), 400

        normalized_packs = {}
        for pack_name, terms in packs.items():
            if not isinstance(pack_name, str) or not pack_name.strip():
                return jsonify({'success': False, 'error': {
                    'code': 'INVALID_INPUT',
                    'message': 'pack names must be non-empty strings'
                }}), 400
            if not isinstance(terms, list):
                return jsonify({'success': False, 'error': {
                    'code': 'INVALID_INPUT',
                    'message': f"pack '{pack_name}' must be an array of terms"
                }}), 400
            clean = []
            for t in terms:
                if not isinstance(t, str):
                    continue
                term = t.strip().lower()
                if term and term not in clean:
                    clean.append(term)
            normalized_packs[pack_name.strip()] = clean

        active_pack = dictionary.get('active_pack')
        if not isinstance(active_pack, str) or active_pack not in normalized_packs:
            active_pack = next(iter(normalized_packs.keys()))

        payload = {
            'active_pack': active_pack,
            'packs': normalized_packs,
        }
        db.set_config('keyword_dictionary', json.dumps(payload))
        return jsonify({'success': True, 'data': {'dictionary': payload}})
    except Exception:
        log.error('Error setting keyword dictionary', exc_info=True)
        return jsonify({'success': False, 'error': {'message': 'Internal server error'}}), 500
