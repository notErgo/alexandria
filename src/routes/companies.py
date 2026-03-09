"""Company and metric schema API routes."""
import json
import logging
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

import requests
from flask import Blueprint, jsonify, request

from config import _VALID_SCRAPER_MODES
from orchestration import (
    _expand_template_url,
    _probe_candidate_url,
    run_bootstrap_probe_for_ticker,
    _run_bootstrap_probe_for_ticker,
)

log = logging.getLogger('miners.routes.companies')

bp = Blueprint('companies', __name__)
_VALID_SOURCE_TYPES = {'IR_PRIMARY', 'RSS', 'TEMPLATE', 'EDGAR', 'PRNEWSWIRE', 'GLOBENEWSWIRE'}


def _companies_json_path() -> Path:
    """Return the canonical path to companies.json."""
    return Path(__file__).parent.parent.parent / 'config' / 'companies.json'


def _write_active_to_companies_json(ticker: str, active: bool) -> None:
    """Update the active field for one ticker in companies.json.

    Single writer: PUT /api/companies/<ticker> via companies.py route.
    """
    path = _companies_json_path()
    companies = json.loads(path.read_text())
    for c in companies:
        if c.get('ticker', '').upper() == ticker.upper():
            c['active'] = active
            break
    path.write_text(json.dumps(companies, indent=2))


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
    if mode == 'rss':
        has_rss = bool(fields.get('rss_url') or fields.get('prnewswire_url') or fields.get('globenewswire_url'))
        if not has_rss:
            return "rss mode requires rss_url or aggregator feed URL (prnewswire_url/globenewswire_url)"
    if mode == 'template':
        if not fields.get('url_template'):
            return "template mode requires non-empty url_template"
        if not fields.get('pr_start_year'):
            return "template mode requires pr_start_year"
    if mode == 'index' and not fields.get('ir_url'):
        return "index mode requires non-empty ir_url"
    if mode == 'playwright' and not fields.get('ir_url'):
        return "playwright mode requires non-empty ir_url"
    return None


@bp.route('/api/companies')
def list_companies():
    from app_globals import get_db
    db = get_db()
    active_param = request.args.get('active', 'true').strip().lower()
    active_only = active_param not in ('false', '0', 'no')
    companies = db.get_companies(active_only=active_only)
    return jsonify({'success': True, 'data': companies})


@bp.route('/api/companies/scraper_governance')
def scraper_governance():
    """Return governance status for scraper modes/skip decisions."""
    from app_globals import get_db
    db = get_db()
    try:
        stale_days = int(request.args.get('stale_days', 30))
    except ValueError:
        return jsonify({'success': False, 'error': {'message': 'stale_days must be an integer'}}), 400
    snapshot = db.get_scraper_governance_snapshot(stale_days=stale_days)
    log.info(
        "event=scraper_governance_snapshot stale_days=%s total=%s needs_probe=%s stale_skip=%s conflicts=%s",
        stale_days,
        snapshot.get('total', 0),
        snapshot.get('needs_probe', 0),
        snapshot.get('stale_skip', 0),
        snapshot.get('skip_conflict_active_source', 0),
    )
    return jsonify({'success': True, 'data': snapshot})


@bp.route('/api/companies/<ticker>/discovery_candidates', methods=['GET'])
def list_discovery_candidates(ticker):
    """List agent-proposed discovery candidates for a ticker."""
    from app_globals import get_db
    db = get_db()
    ticker = ticker.upper()
    if db.get_company(ticker) is None:
        return jsonify({'success': False, 'error': {'message': f'Company {ticker!r} not found'}}), 404
    verified_only = str(request.args.get('verified_only', '0')).strip().lower() in {'1', 'true', 'yes'}
    rows = db.list_discovery_candidates(ticker, verified_only=verified_only)
    return jsonify({'success': True, 'data': {'ticker': ticker, 'candidates': rows}})


@bp.route('/api/companies/<ticker>/discovery_candidates', methods=['POST'])
def add_discovery_candidates(ticker):
    """Store discovery candidates produced by an agent or analyst."""
    from app_globals import get_db
    db = get_db()
    ticker = ticker.upper()
    if db.get_company(ticker) is None:
        return jsonify({'success': False, 'error': {'message': f'Company {ticker!r} not found'}}), 404

    body = request.get_json(silent=True) or {}
    proposed_by = (body.get('proposed_by') or 'agent').strip() or 'agent'
    candidates = body.get('candidates')
    if not isinstance(candidates, list) or not candidates:
        return jsonify({'success': False, 'error': {'message': "'candidates' must be a non-empty list"}}), 400

    stored = 0
    for c in candidates:
        if not isinstance(c, dict):
            continue
        source_type = (c.get('source_type') or '').strip().upper()
        url = (c.get('url') or '').strip()
        if source_type not in _VALID_SOURCE_TYPES or not url:
            continue
        conf = c.get('confidence')
        if conf is not None:
            try:
                conf = float(conf)
            except (TypeError, ValueError):
                conf = None
        db.upsert_discovery_candidate({
            'ticker': ticker,
            'source_type': source_type,
            'url': url,
            'pr_start_year': c.get('pr_start_year'),
            'confidence': conf,
            'rationale': c.get('rationale'),
            'proposed_by': proposed_by,
            'verified': 0,
        })
        stored += 1

    if stored == 0:
        log.warning("event=discovery_candidates_store ticker=%s proposed_by=%s stored=0", ticker, proposed_by)
        return jsonify({'success': False, 'error': {'message': 'No valid candidates to store'}}), 400
    rows = db.list_discovery_candidates(ticker, verified_only=False)
    log.info(
        "event=discovery_candidates_store ticker=%s proposed_by=%s stored=%s total_candidates=%s",
        ticker, proposed_by, stored, len(rows)
    )
    return jsonify({'success': True, 'data': {'stored': stored, 'candidates': rows}})


@bp.route('/api/companies/<ticker>/bootstrap_probe', methods=['POST'])
def bootstrap_probe(ticker):
    """Probe discovery candidates and recommend/apply scraper mode for one ticker."""
    from app_globals import get_db
    db = get_db()
    ticker = ticker.upper()
    if db.get_company(ticker) is None:
        return jsonify({'success': False, 'error': {'message': f'Company {ticker!r} not found'}}), 404

    body = request.get_json(silent=True) or {}
    apply_mode = bool(body.get('apply_mode', False))
    allow_apply_skip = bool(body.get('allow_apply_skip', False))
    try:
        timeout = int(body.get('timeout_seconds', 12))
    except (TypeError, ValueError):
        return jsonify({'success': False, 'error': {'message': 'timeout_seconds must be an integer'}}), 400
    timeout = max(3, min(timeout, 60))
    log.info(
        "event=bootstrap_probe_request ticker=%s apply_mode=%s allow_apply_skip=%s timeout=%s",
        ticker, int(apply_mode), int(allow_apply_skip), timeout
    )

    try:
        result = _run_bootstrap_probe_for_ticker(
            db,
            ticker=ticker,
            apply_mode=apply_mode,
            allow_apply_skip=allow_apply_skip,
            timeout=timeout,
        )
    except ValueError as e:
        log.warning("event=bootstrap_probe_rejected ticker=%s reason=%s", ticker, str(e))
        return jsonify({'success': False, 'error': {'message': str(e)}}), 400

    return jsonify({'success': True, 'data': result})


@bp.route('/api/companies/bootstrap_probe_all', methods=['POST'])
def bootstrap_probe_all():
    """Probe a filtered set of tickers based on governance statuses."""
    from app_globals import get_db
    db = get_db()
    body = request.get_json(silent=True) or {}
    try:
        stale_days = int(body.get('stale_days', 30))
    except (TypeError, ValueError):
        return jsonify({'success': False, 'error': {'message': 'stale_days must be an integer'}}), 400
    try:
        timeout = int(body.get('timeout_seconds', 12))
    except (TypeError, ValueError):
        return jsonify({'success': False, 'error': {'message': 'timeout_seconds must be an integer'}}), 400
    timeout = max(3, min(timeout, 60))

    apply_mode = bool(body.get('apply_mode', False))
    allow_apply_skip = bool(body.get('allow_apply_skip', False))
    requested_tickers = body.get('tickers')
    ticker_filter = None
    if isinstance(requested_tickers, list) and requested_tickers:
        ticker_filter = {str(t).strip().upper() for t in requested_tickers if str(t).strip()}
    requested_statuses = body.get('statuses')
    if not isinstance(requested_statuses, list) or not requested_statuses:
        requested_statuses = ['needs_probe', 'stale_skip', 'skip_conflict_active_source']
    target_statuses = {str(s).strip() for s in requested_statuses if str(s).strip()}

    snapshot = db.get_scraper_governance_snapshot(stale_days=stale_days)
    targets = []
    for item in snapshot['items']:
        ticker = item['ticker']
        if item['governance_status'] not in target_statuses:
            continue
        if ticker_filter is not None and ticker not in ticker_filter:
            continue
        targets.append(ticker)

    log.info(
        "event=bootstrap_probe_all_start target_count=%s apply_mode=%s allow_apply_skip=%s stale_days=%s timeout=%s statuses=%s ticker_filter_count=%s",
        len(targets), int(apply_mode), int(allow_apply_skip), stale_days, timeout,
        ','.join(sorted(target_statuses)), 0 if ticker_filter is None else len(ticker_filter),
    )

    results = []
    failures = []
    for ticker in targets:
        try:
            results.append(_run_bootstrap_probe_for_ticker(
                db,
                ticker=ticker,
                apply_mode=apply_mode,
                allow_apply_skip=allow_apply_skip,
                timeout=timeout,
            ))
        except Exception as e:
            log.warning("event=bootstrap_probe_all_ticker_failed ticker=%s error=%s", ticker, str(e))
            failures.append({'ticker': ticker, 'error': str(e)})

    log.info(
        "event=bootstrap_probe_all_end targeted=%s completed=%s failed=%s apply_mode=%s",
        len(targets), len(results), len(failures), int(apply_mode),
    )

    return jsonify({'success': True, 'data': {
        'target_statuses': sorted(target_statuses),
        'ticker_filter': sorted(ticker_filter) if ticker_filter is not None else [],
        'targeted_tickers': targets,
        'targeted': len(targets),
        'completed': len(results),
        'failed': len(failures),
        'failures': failures,
        'results': results,
    }})


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
    prnewswire_url = _normalize_optional_text(body, 'prnewswire_url')
    globenewswire_url = _normalize_optional_text(body, 'globenewswire_url')
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
        'prnewswire_url': prnewswire_url,
        'globenewswire_url': globenewswire_url,
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
            prnewswire_url=prnewswire_url,
            globenewswire_url=globenewswire_url,
            url_template=url_template,
            pr_start_year=pr_start_year,
            skip_reason=skip_reason,
            sandbox_note=sandbox_note,
        )
    except sqlite3.IntegrityError:
        return jsonify({'success': False, 'error': {'message': f'Company {ticker!r} already exists'}}), 409
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
        # Manual sync is an explicit operator action; re-enable startup auto-sync.
        db.set_config('auto_sync_companies_on_startup', '1')
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
        'rss_url', 'prnewswire_url', 'globenewswire_url',
        'url_template', 'pr_start_year', 'skip_reason', 'sandbox_note',
        'active',
    }
    kwargs = {k: v for k, v in body.items() if k in allowed}
    # Normalize active flag: coerce to bool, then to 0/1 for the DB
    new_active = None
    if 'active' in kwargs:
        new_active = bool(kwargs.pop('active'))
        kwargs['active'] = 1 if new_active else 0
    for key in ('name', 'ir_url', 'pr_base_url', 'scraper_issues_log', 'cik', 'sector',
                'rss_url', 'prnewswire_url', 'globenewswire_url', 'url_template',
                'skip_reason', 'sandbox_note'):
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
        'prnewswire_url': kwargs.get('prnewswire_url', existing.get('prnewswire_url')),
        'globenewswire_url': kwargs.get('globenewswire_url', existing.get('globenewswire_url')),
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

    # Write active flag back to companies.json so startup sync respects it.
    # Only write when the caller explicitly supplied the active field.
    if new_active is not None:
        try:
            _write_active_to_companies_json(ticker, new_active)
        except Exception:
            log.error("Failed to write active flag to companies.json for %s", ticker, exc_info=True)
            # Non-fatal: DB is updated; JSON write failure is logged only.

    return jsonify({'success': True, 'data': company})


# ── Metric schema routes ──────────────────────────────────────────────────────

@bp.route('/api/metric_schema')
def list_metric_schema():
    from app_globals import get_db
    db = get_db()
    sector = request.args.get('sector', 'BTC-miners')
    active_param = request.args.get('active', '').lower()
    active_only = active_param in ('true', '1', 'yes')
    rows = db.get_metric_schema(sector, active_only=active_only)
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


@bp.route('/api/metric_schema/<int:row_id>', methods=['DELETE'])
def delete_metric_schema(row_id):
    """Permanently delete a metric_schema row.

    Returns 200 on success, 404 if row not found.
    """
    try:
        from app_globals import get_db
        db = get_db()
        deleted = db.delete_metric_schema(row_id)
        if not deleted:
            return jsonify({'success': False, 'error': {
                'code': 'NOT_FOUND',
                'message': f'Metric schema row {row_id} not found',
            }}), 404
        return jsonify({'success': True})
    except Exception:
        log.error('Error deleting metric_schema row %s', row_id, exc_info=True)
        return jsonify({'success': False, 'error': {'message': 'Internal server error'}}), 500


@bp.route('/api/metric_schema/<int:row_id>', methods=['PATCH'])
def update_metric_schema(row_id):
    """Update active flag, label, or unit for a metric_schema row.

    Body: {active?: 0|1, label?: str, unit?: str}
    Returns 200 on success, 404 if row not found.
    """
    try:
        from app_globals import get_db
        db = get_db()
        body = request.get_json(silent=True) or {}
        active = body.get('active')
        label = body.get('label')
        unit = body.get('unit')

        if active is not None:
            active = int(active)
        if label is not None:
            label = str(label).strip()
            if not label or len(label) > 100:
                return jsonify({'success': False, 'error': {
                    'message': 'label must be 1-100 chars'
                }}), 400
        if unit is not None:
            unit = str(unit).strip()

        updated = db.update_metric_schema(row_id, active=active, label=label, unit=unit)
        if not updated:
            return jsonify({'success': False, 'error': {
                'code': 'NOT_FOUND',
                'message': f'Metric schema row {row_id} not found',
            }}), 404

        return jsonify({'success': True})
    except Exception:
        log.error('Error updating metric_schema row %s', row_id, exc_info=True)
        return jsonify({'success': False, 'error': {'message': 'Internal server error'}}), 500


# ── Metric Keywords routes ──────────────────────────────────────────────────

def _metric_key_exists(db, key: str) -> bool:
    """Return True if the given key exists in metric_schema."""
    rows = db.get_metric_schema(sector='BTC-miners', active_only=False)
    return any(r['key'] == key for r in rows)


def _normalize_phrase(phrase: str) -> str:
    """Normalize a keyword phrase: strip surrounding whitespace and quote characters."""
    return phrase.strip().strip('"')


@bp.route('/api/metric_keywords', methods=['GET'])
def list_all_metric_keywords():
    """List all metric keywords across all metrics.

    ?all=1 to include inactive rows; default is active only.
    Returns rows sorted by metric_key then id.
    """
    try:
        from app_globals import get_db
        db = get_db()
        active_only = request.args.get('all', '0') != '1'
        rows = db.get_all_metric_keywords(active_only=active_only)
        return jsonify({'success': True, 'data': {'keywords': rows, 'total': len(rows)}})
    except Exception:
        log.error('Error listing all metric keywords', exc_info=True)
        return jsonify({'success': False, 'error': {'message': 'Internal server error'}}), 500


@bp.route('/api/metric_schema/<string:metric_key>/keywords', methods=['GET'])
def list_metric_keywords(metric_key):
    """List keywords for a specific metric.

    ?all=1 to include inactive rows; default is active only.
    Returns 404 if metric_key is not in metric_schema.
    """
    try:
        from app_globals import get_db
        db = get_db()
        if not _metric_key_exists(db, metric_key):
            return jsonify({'success': False, 'error': {
                'code': 'NOT_FOUND', 'message': f'Unknown metric key: {metric_key}',
            }}), 404
        active_only = request.args.get('all', '0') != '1'
        rows = db.get_metric_keywords(metric_key, active_only=active_only)
        return jsonify({'success': True, 'data': {
            'metric_key': metric_key,
            'keywords': rows,
            'total': len(rows),
        }})
    except Exception:
        log.error('Error listing metric keywords for %s', metric_key, exc_info=True)
        return jsonify({'success': False, 'error': {'message': 'Internal server error'}}), 500


def _parse_bulk_phrases(body: dict) -> list:
    """Extract a list of phrases from POST body.

    Accepts three input forms:
    - phrase: str          — single phrase (legacy / single-add)
    - phrases: list[str]   — explicit array
    - csv: str             — comma-separated or newline-separated string

    Returns a list of stripped, non-empty strings.  Does NOT normalise quotes
    (caller does that per-phrase).
    """
    if 'phrases' in body:
        raw = body['phrases']
        if not isinstance(raw, list):
            return []
        return [str(p).strip() for p in raw if str(p).strip()]
    if 'csv' in body:
        raw = body['csv']
        if not isinstance(raw, str):
            return []
        # Split on commas or newlines, strip whitespace
        import re as _re
        parts = _re.split(r'[,\n]+', raw)
        return [p.strip() for p in parts if p.strip()]
    if 'phrase' in body:
        p = body.get('phrase', '')
        return [str(p).strip()] if isinstance(p, str) and p.strip() else []
    return []


@bp.route('/api/metric_schema/<string:metric_key>/keywords', methods=['POST'])
def add_metric_keyword(metric_key):
    """Add one or more keyword phrases to a specific metric.

    Single add:
      Body: {phrase: str, exclude_terms?: str}
      Returns 201 with {id, metric_key, phrase, exclude_terms}.

    Bulk add (phrases array or csv string):
      Body: {phrases: [str, ...]} or {csv: "phrase1, phrase2"}
      Returns 201 with {added: int, skipped: int, ids: [int]}.
      Duplicate phrases are silently skipped (not an error).

    Returns 400 if no valid phrases, 404 if metric unknown.
    """
    import sqlite3 as _sqlite3
    try:
        from app_globals import get_db
        db = get_db()
        if not _metric_key_exists(db, metric_key):
            return jsonify({'success': False, 'error': {
                'code': 'NOT_FOUND', 'message': f'Unknown metric key: {metric_key}',
            }}), 404
        body = request.get_json(silent=True) or {}
        phrases = _parse_bulk_phrases(body)
        if not phrases:
            return jsonify({'success': False, 'error': {
                'code': 'MISSING_PHRASE', 'message': 'phrase, phrases, or csv is required',
            }}), 400

        is_single = 'phrase' in body and 'phrases' not in body and 'csv' not in body
        exclude_terms = str(body.get('exclude_terms', '')).strip() if is_single else ''

        added, skipped, ids = 0, 0, []
        for raw_phrase in phrases:
            phrase = _normalize_phrase(raw_phrase)
            try:
                kw_id = db.add_metric_keyword(metric_key, phrase, exclude_terms=exclude_terms)
                added += 1
                ids.append(kw_id)
            except _sqlite3.IntegrityError:
                skipped += 1

        if is_single and added == 1:
            return jsonify({'success': True, 'data': {
                'id': ids[0], 'metric_key': metric_key, 'phrase': _normalize_phrase(phrases[0]),
                'exclude_terms': exclude_terms,
            }}), 201

        if added == 0 and skipped > 0:
            return jsonify({'success': False, 'error': {
                'code': 'DUPLICATE', 'message': 'All phrases already exist for this metric',
            }}), 409

        return jsonify({'success': True, 'data': {
            'added': added, 'skipped': skipped, 'ids': ids, 'metric_key': metric_key,
        }}), 201
    except Exception:
        log.error('Error adding metric keyword for %s', metric_key, exc_info=True)
        return jsonify({'success': False, 'error': {'message': 'Internal server error'}}), 500


@bp.route('/api/metric_schema/<string:metric_key>/keywords/<int:kw_id>', methods=['PATCH'])
def update_metric_keyword(metric_key, kw_id):
    """Update active flag, phrase, and/or exclude_terms for a metric keyword.

    Body: {active?: 0|1, phrase?: str, exclude_terms?: str}
    Returns 200, 400, or 404.
    """
    try:
        from app_globals import get_db
        db = get_db()
        body = request.get_json(silent=True) or {}
        active = body.get('active')
        phrase = body.get('phrase')
        exclude_terms = body.get('exclude_terms')
        if active is None and phrase is None and exclude_terms is None:
            return jsonify({'success': False, 'error': {
                'message': 'Provide active, phrase, or exclude_terms to update',
            }}), 400
        if active is not None:
            active = int(active)
        if phrase is not None:
            phrase = _normalize_phrase(phrase)
        if exclude_terms is not None:
            exclude_terms = str(exclude_terms).strip()
        updated = db.update_metric_keyword(
            kw_id, active=active, phrase=phrase, exclude_terms=exclude_terms,
        )
        if not updated:
            return jsonify({'success': False, 'error': {
                'code': 'NOT_FOUND', 'message': f'Keyword {kw_id} not found',
            }}), 404
        return jsonify({'success': True})
    except Exception:
        log.error('Error updating metric keyword %s', kw_id, exc_info=True)
        return jsonify({'success': False, 'error': {'message': 'Internal server error'}}), 500


@bp.route('/api/metric_schema/<string:metric_key>/keywords/<int:kw_id>', methods=['DELETE'])
def delete_metric_keyword(metric_key, kw_id):
    """Permanently delete a metric keyword.

    Returns 200 on success, 404 if not found.
    """
    try:
        from app_globals import get_db
        db = get_db()
        deleted = db.delete_metric_keyword(kw_id)
        if not deleted:
            return jsonify({'success': False, 'error': {
                'code': 'NOT_FOUND', 'message': f'Keyword {kw_id} not found',
            }}), 404
        return jsonify({'success': True})
    except Exception:
        log.error('Error deleting metric keyword %s', kw_id, exc_info=True)
        return jsonify({'success': False, 'error': {'message': 'Internal server error'}}), 500
