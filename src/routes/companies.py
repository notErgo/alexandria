"""Company and metric schema API routes."""
import logging
import sqlite3
from pathlib import Path
from flask import Blueprint, jsonify, request

log = logging.getLogger('miners.routes.companies')

bp = Blueprint('companies', __name__)

_VALID_SCRAPER_MODES = {'rss', 'index', 'template', 'skip'}


def _normalize_optional_text(body: dict, key: str) -> str | None:
    val = body.get(key)
    if val is None:
        return None
    txt = str(val).strip()
    return txt or None


def _parse_optional_start_year(body: dict) -> tuple[int | None, str | None]:
    raw = body.get('pr_start_year')
    if raw in (None, ''):
        return None, None
    try:
        year = int(raw)
    except (TypeError, ValueError):
        return None, 'pr_start_year must be an integer'
    if year < 2009 or year > 2100:
        return None, 'pr_start_year must be between 2009 and 2100'
    return year, None


def _validate_mode_requirements(mode: str, fields: dict) -> str | None:
    if mode == 'rss' and not fields.get('rss_url'):
        return "rss mode requires non-empty rss_url"
    if mode == 'template':
        if not fields.get('url_template'):
            return "template mode requires non-empty url_template"
        if not fields.get('pr_start_year'):
            return "template mode requires pr_start_year"
    if mode == 'index' and not fields.get('ir_url'):
        return "index mode requires non-empty ir_url"
    return None


@bp.route('/api/companies')
def list_companies():
    from app_globals import get_db
    db = get_db()
    companies = db.get_companies(active_only=False)
    return jsonify({'success': True, 'data': companies})


@bp.route('/api/companies', methods=['POST'])
def create_company():
    from app_globals import get_db
    db = get_db()
    body = request.get_json(silent=True) or {}
    ticker = body.get('ticker', '').strip().upper()
    name = body.get('name', '').strip()
    sector = body.get('sector', 'BTC-miners').strip()
    scraper_mode = body.get('scraper_mode', 'skip').strip().lower()
    ir_url = _normalize_optional_text(body, 'ir_url') or ''
    rss_url = _normalize_optional_text(body, 'rss_url')
    url_template = _normalize_optional_text(body, 'url_template')
    skip_reason = _normalize_optional_text(body, 'skip_reason')
    sandbox_note = _normalize_optional_text(body, 'sandbox_note')
    pr_start_year, year_err = _parse_optional_start_year(body)

    if not ticker or len(ticker) > 10:
        return jsonify({'success': False, 'error': {'message': 'ticker required (max 10 chars)'}}), 400
    if not name or len(name) > 100:
        return jsonify({'success': False, 'error': {'message': 'name required (max 100 chars)'}}), 400
    if scraper_mode not in _VALID_SCRAPER_MODES:
        return jsonify({'success': False, 'error': {'message': f'scraper_mode must be one of {sorted(_VALID_SCRAPER_MODES)}'}}), 400
    if year_err:
        return jsonify({'success': False, 'error': {'message': year_err}}), 400

    field_map = {
        'ir_url': ir_url,
        'rss_url': rss_url,
        'url_template': url_template,
        'pr_start_year': pr_start_year,
        'skip_reason': skip_reason,
    }
    mode_err = _validate_mode_requirements(scraper_mode, field_map)
    if mode_err:
        return jsonify({'success': False, 'error': {'message': mode_err}}), 400

    try:
        company = db.add_company(
            ticker=ticker, name=name, sector=sector,
            scraper_mode=scraper_mode,
            ir_url=ir_url,
            pr_base_url=body.get('pr_base_url'),
            cik=body.get('cik'),
            scraper_issues_log=body.get('scraper_issues_log', ''),
            rss_url=rss_url,
            url_template=url_template,
            pr_start_year=pr_start_year,
            skip_reason=skip_reason,
            sandbox_note=sandbox_note,
        )
    except Exception:
        log.error("Failed to create company %s", ticker, exc_info=True)
        return jsonify({'success': False, 'error': {'message': 'Internal server error'}}), 500

    return jsonify({'success': True, 'data': company}), 201


@bp.route('/api/companies/<ticker>', methods=['GET'])
def get_company(ticker):
    from app_globals import get_db
    db = get_db()
    company = db.get_company(ticker.upper())
    if company is None:
        return jsonify({'success': False, 'error': {
            'code': 'NOT_FOUND', 'message': f'Company {ticker!r} not found'
        }}), 404
    return jsonify({'success': True, 'data': company})


@bp.route('/api/companies/sync', methods=['POST'])
def sync_companies():
    """Re-sync companies table from companies.json config file.

    Upserts all config fields (name, URLs, scraper settings).
    Preserves operational fields (scraper_status, last_scrape_at, etc.).
    """
    from app_globals import get_db
    db = get_db()
    config_path = Path(__file__).parent.parent.parent / 'config' / 'companies.json'
    if not config_path.exists():
        return jsonify({'success': False, 'error': {'message': 'companies.json not found'}}), 404
    try:
        result = db.sync_companies_from_config(str(config_path))
    except Exception:
        log.error("Failed to sync companies from config", exc_info=True)
        return jsonify({'success': False, 'error': {'message': 'Internal server error'}}), 500
    return jsonify({'success': True, 'data': result})


@bp.route('/api/companies/<ticker>', methods=['PUT'])
def update_company(ticker):
    from app_globals import get_db
    db = get_db()
    ticker = ticker.upper()
    if db.get_company(ticker) is None:
        return jsonify({'success': False, 'error': {'message': f'Company {ticker!r} not found'}}), 404
    body = request.get_json(silent=True) or {}
    allowed = {
        'name', 'ir_url', 'pr_base_url', 'scraper_mode', 'scraper_issues_log', 'cik', 'sector',
        'rss_url', 'url_template', 'pr_start_year', 'skip_reason', 'sandbox_note',
    }
    kwargs = {k: v for k, v in body.items() if k in allowed}
    for key in ('name', 'ir_url', 'pr_base_url', 'scraper_issues_log', 'cik', 'sector',
                'rss_url', 'url_template', 'skip_reason', 'sandbox_note'):
        if key in kwargs:
            kwargs[key] = _normalize_optional_text(kwargs, key)
    if 'scraper_mode' in kwargs:
        kwargs['scraper_mode'] = str(kwargs['scraper_mode']).strip().lower()
    if 'scraper_mode' in kwargs and kwargs['scraper_mode'] not in _VALID_SCRAPER_MODES:
        return jsonify({'success': False, 'error': {'message': f'scraper_mode must be one of {sorted(_VALID_SCRAPER_MODES)}'}}), 400
    if 'pr_start_year' in kwargs:
        year, year_err = _parse_optional_start_year(kwargs)
        if year_err:
            return jsonify({'success': False, 'error': {'message': year_err}}), 400
        kwargs['pr_start_year'] = year

    existing = db.get_company(ticker) or {}
    effective_mode = (kwargs.get('scraper_mode') or existing.get('scraper_mode') or 'skip').strip().lower()
    mode_fields = {
        'ir_url': kwargs.get('ir_url', existing.get('ir_url')),
        'rss_url': kwargs.get('rss_url', existing.get('rss_url')),
        'url_template': kwargs.get('url_template', existing.get('url_template')),
        'pr_start_year': kwargs.get('pr_start_year', existing.get('pr_start_year')),
        'skip_reason': kwargs.get('skip_reason', existing.get('skip_reason')),
    }
    mode_err = _validate_mode_requirements(effective_mode, mode_fields)
    if mode_err:
        return jsonify({'success': False, 'error': {'message': mode_err}}), 400

    try:
        company = db.update_company_config(ticker, **kwargs)
    except Exception:
        log.error("Failed to update company %s", ticker, exc_info=True)
        return jsonify({'success': False, 'error': {'message': 'Internal server error'}}), 500
    return jsonify({'success': True, 'data': company})


# ── Metric schema routes ──────────────────────────────────────────────────────

@bp.route('/api/metric_schema')
def list_metric_schema():
    from app_globals import get_db
    db = get_db()
    sector = request.args.get('sector', 'BTC-miners')
    rows = db.get_metric_schema(sector)
    return jsonify({'success': True, 'data': rows})


@bp.route('/api/metric_schema', methods=['POST'])
def add_metric_schema():
    from app_globals import get_db
    db = get_db()
    body = request.get_json(silent=True) or {}
    key = body.get('key', '').strip()
    label = body.get('label', '').strip()
    unit = body.get('unit', '').strip()
    sector = body.get('sector', 'BTC-miners').strip()

    if not key or len(key) > 50 or ' ' in key:
        return jsonify({'success': False, 'error': {'message': 'key required (max 50 chars, no spaces)'}}), 400
    if not label or len(label) > 100:
        return jsonify({'success': False, 'error': {'message': 'label required (max 100 chars)'}}), 400

    try:
        row = db.add_analyst_metric(key, label, unit, sector)
    except sqlite3.IntegrityError:
        return jsonify({'success': False, 'error': {
            'message': f'A column named {key!r} already exists in this sector\'s schema.'
        }}), 409
    except Exception:
        log.error("Failed to add metric schema %s", key, exc_info=True)
        return jsonify({'success': False, 'error': {'message': 'Internal server error'}}), 500

    return jsonify({'success': True, 'data': row}), 201
