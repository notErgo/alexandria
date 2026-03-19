"""Company and metric schema API routes."""
import json
import logging
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

import requests
from flask import Blueprint, jsonify, request

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


def _remove_from_companies_json(ticker: str) -> None:
    """Remove a ticker entry from companies.json.

    Called after a successful DB delete so the company does not repopulate
    on the next server restart via sync_companies_from_config().
    """
    path = _companies_json_path()
    companies = json.loads(path.read_text())
    filtered = [c for c in companies if c.get('ticker', '').upper() != ticker.upper()]
    path.write_text(json.dumps(filtered, indent=2))


def _normalize_optional_text(body: dict, key: str) -> str | None:
    val = body.get(key)
    if val is None:
        return None
    txt = str(val).strip()
    return txt or None


def _parse_optional_start_date(body: dict) -> tuple[str | None, str | None]:
    raw = body.get('pr_start_date')
    # backward compat: accept pr_start_year as integer
    if raw is None and body.get('pr_start_year') is not None:
        yr = body.get('pr_start_year')
        try:
            raw = f"{int(yr):04d}-01-01"
        except (TypeError, ValueError):
            return None, 'pr_start_year must be an integer'
    if raw in (None, ''):
        return None, None
    txt = str(raw).strip()
    if not txt:
        return None, None
    from datetime import date as _date
    try:
        parsed = _date.fromisoformat(txt[:10])
    except ValueError:
        return None, 'pr_start_date must be a valid date in YYYY-MM-DD format'
    if parsed.year < 2009 or parsed.year > 2100:
        return None, 'pr_start_date year must be between 2009 and 2100'
    return txt[:10], None



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
            'pr_start_date': c.get('pr_start_date'),
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
    reporting_cadence = (body.get('reporting_cadence') or 'monthly').strip().lower()
    ir_url = _normalize_optional_text(body, 'ir_url') or ''
    prnewswire_url = _normalize_optional_text(body, 'prnewswire_url')
    globenewswire_url = _normalize_optional_text(body, 'globenewswire_url')
    sandbox_note = _normalize_optional_text(body, 'sandbox_note')
    pr_start_date, year_err = _parse_optional_start_date(body)

    if not ticker or len(ticker) > 10:
        return jsonify({'success': False, 'error': {'message': 'ticker required (max 10 chars)'}}), 400
    if not name or len(name) > 100:
        return jsonify({'success': False, 'error': {'message': 'name required (max 100 chars)'}}), 400
    if reporting_cadence not in ('monthly', 'quarterly', 'annual'):
        return jsonify({'success': False, 'error': {'message': "reporting_cadence must be 'monthly', 'quarterly', or 'annual'"}}), 400
    if year_err:
        return jsonify({'success': False, 'error': {'message': year_err}}), 400

    try:
        company = db.add_company(
            ticker=ticker, name=name, sector=sector,
            reporting_cadence=reporting_cadence,
            ir_url=ir_url,
            pr_base_url=body.get('pr_base_url'),
            cik=body.get('cik'),
            scraper_issues_log=body.get('scraper_issues_log', ''),
            prnewswire_url=prnewswire_url,
            globenewswire_url=globenewswire_url,
            pr_start_date=pr_start_date,
            sandbox_note=sandbox_note,
        )
    except sqlite3.IntegrityError:
        return jsonify({'success': False, 'error': {'message': f'Company {ticker!r} already exists'}}), 409
    except Exception:
        log.error("Failed to create company %s", ticker, exc_info=True)
        return jsonify({'success': False, 'error': {'message': 'Internal server error'}}), 500

    return jsonify({'success': True, 'data': company}), 201


@bp.route('/api/companies/<ticker>/detect_btc_anchor', methods=['POST'])
def detect_btc_anchor(ticker):
    """Detect and store btc_first_filing_date for ticker via EDGAR full-text search.

    Idempotent — returns the stored date immediately if already set.
    Pass {"force": true} to re-detect even when already stored.
    """
    from app_globals import get_db
    import requests as _req
    from scrapers.edgar_connector import EdgarConnector

    db = get_db()
    ticker = ticker.upper()
    company = db.get_company(ticker)
    if company is None:
        return jsonify({'success': False, 'error': {'code': 'NOT_FOUND', 'message': f'Company {ticker!r} not found'}}), 404
    cik = company.get('cik')
    if not cik:
        return jsonify({'success': False, 'error': {'code': 'NO_CIK', 'message': f'{ticker} has no CIK — EDGAR detection requires a CIK'}}), 400

    body = request.get_json(silent=True) or {}
    if body.get('force'):
        db.set_btc_first_filing_date(ticker, '')  # clear cached value to force re-detect
    try:
        connector = EdgarConnector(db=db, session=_req.Session())
        detected = connector.detect_btc_first_filing_date(cik=cik, ticker=ticker)
    except Exception:
        log.error("detect_btc_anchor failed for %s", ticker, exc_info=True)
        return jsonify({'success': False, 'error': {'message': 'Internal server error'}}), 500

    return jsonify({
        'success': True,
        'data': {
            'ticker': ticker,
            'btc_first_filing_date': detected,
            'detected': detected is not None,
        },
    })


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
    """Update existing companies from companies.json. Respects cleared state.

    When auto_sync_companies_on_startup='0' (set by hard delete), runs in
    update-only mode: existing rows are updated but no new rows are inserted.
    The companies table stays empty if it was cleared by a hard delete.

    To restore deleted companies from JSON, use POST /api/companies/sync/restore.
    """
    from app_globals import get_db
    db = get_db()
    config_path = Path(__file__).parent.parent.parent / 'config' / 'companies.json'
    if not config_path.exists():
        return jsonify({'success': False, 'error': {'message': 'companies.json not found'}}), 404
    try:
        cleared = db.get_config('auto_sync_companies_on_startup') == '0'
        result = db.sync_companies_from_config(str(config_path), insert_new=not cleared)
        result['cleared_state'] = cleared
        db.set_config('auto_sync_companies_on_startup', '1')
    except Exception:
        log.error("Failed to sync companies from config", exc_info=True)
        return jsonify({'success': False, 'error': {'message': 'Internal server error'}}), 500
    return jsonify({'success': True, 'data': result})


@bp.route('/api/companies/sync/restore', methods=['POST'])
def restore_companies_from_config():
    """Restore companies from companies.json, including rows deleted by hard delete.

    Explicitly re-enables startup auto-sync and inserts any missing companies.
    This is the only route that re-populates the table after a hard delete.
    Requires {"confirm": true} in the request body.
    """
    from app_globals import get_db
    db = get_db()
    body = request.get_json(silent=True) or {}
    if not body.get('confirm'):
        return jsonify({'success': False, 'error': {
            'message': 'Pass {"confirm": true} to restore companies from config'
        }}), 400
    config_path = Path(__file__).parent.parent.parent / 'config' / 'companies.json'
    if not config_path.exists():
        return jsonify({'success': False, 'error': {'message': 'companies.json not found'}}), 404
    try:
        result = db.sync_companies_from_config(str(config_path), insert_new=True)
        db.set_config('auto_sync_companies_on_startup', '1')
        result['cleared_state'] = False
    except Exception:
        log.error("Failed to restore companies from config", exc_info=True)
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
        'name', 'ir_url', 'pr_base_url', 'scraper_issues_log', 'cik', 'sector',
        'prnewswire_url', 'globenewswire_url',
        'pr_start_date', 'sandbox_note',
        'active', 'btc_first_filing_date', 'reporting_cadence',
    }
    _VALID_REPORTING_CADENCES = ('monthly', 'quarterly', 'annual')
    kwargs = {k: v for k, v in body.items() if k in allowed}
    # Normalize active flag: coerce to bool, then to 0/1 for the DB
    new_active = None
    if 'active' in kwargs:
        new_active = bool(kwargs.pop('active'))
        kwargs['active'] = 1 if new_active else 0
    for key in ('name', 'ir_url', 'pr_base_url', 'scraper_issues_log', 'cik', 'sector',
                'prnewswire_url', 'globenewswire_url', 'sandbox_note'):
        if key in kwargs:
            kwargs[key] = _normalize_optional_text(kwargs, key)
    if 'pr_start_date' in kwargs:
        date_val, year_err = _parse_optional_start_date(kwargs)
        if year_err:
            return jsonify({'success': False, 'error': {'message': year_err}}), 400
        kwargs['pr_start_date'] = date_val
    if 'reporting_cadence' in kwargs:
        rc = (kwargs['reporting_cadence'] or '').strip().lower()
        if rc not in _VALID_REPORTING_CADENCES:
            return jsonify({'success': False, 'error': {
                'message': f'reporting_cadence must be one of {list(_VALID_REPORTING_CADENCES)}'
            }}), 400
        kwargs['reporting_cadence'] = rc
    if 'btc_first_filing_date' in kwargs:
        raw_date = (kwargs['btc_first_filing_date'] or '').strip()
        if raw_date:
            import re as _re
            if not _re.fullmatch(r'\d{4}-\d{2}-\d{2}', raw_date):
                return jsonify({'success': False, 'error': {'message': 'btc_first_filing_date must be YYYY-MM-DD or empty'}}), 400
            kwargs['btc_first_filing_date'] = raw_date
        else:
            kwargs['btc_first_filing_date'] = None  # clear → re-enables auto-detect

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


@bp.route('/api/companies/<ticker>', methods=['DELETE'])
def delete_company(ticker):
    from app_globals import get_db
    db = get_db()
    ticker = ticker.upper()
    body = request.get_json(silent=True) or {}
    cascade = bool(body.get('cascade', False))

    try:
        result = db.delete_company(ticker, cascade=cascade)
    except Exception:
        log.error("Failed to delete company %s", ticker, exc_info=True)
        return jsonify({'success': False, 'error': {'message': 'Internal server error'}}), 500

    if result.get('error') == 'not_found':
        return jsonify({'success': False, 'error': {'message': f'Company {ticker!r} not found'}}), 404

    if result.get('error') == 'has_children':
        return jsonify({
            'success': False,
            'error': {'message': 'Company has linked data. Pass cascade=true to delete all.'},
            'counts': result['counts'],
        }), 409

    # Remove from companies.json so startup sync does not repopulate the row.
    try:
        _remove_from_companies_json(ticker)
    except Exception:
        log.error("Failed to remove %s from companies.json", ticker, exc_info=True)
        # Non-fatal: DB delete succeeded; JSON write failure is logged only.

    return jsonify({'success': True, 'data': result}), 200


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
    metric_group = body.get('metric_group', 'other') or 'other'
    metric_group = str(metric_group).strip() or 'other'

    if not key or len(key) > 50 or ' ' in key:
        return jsonify({'success': False, 'error': {'message': 'key required (max 50 chars, no spaces)'}}), 400
    if not label or len(label) > 100:
        return jsonify({'success': False, 'error': {'message': 'label required (max 100 chars)'}}), 400

    try:
        row = db.add_analyst_metric(key, label, unit, sector, metric_group=metric_group)
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
    """Update active flag, label, unit, and/or prompt fields for a metric_schema row.

    Body: {active?: 0|1, label?: str, unit?: str,
           prompt_instructions?: str|null, quarterly_prompt?: str|null}
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

        # New prompt fields (None means no update, empty string means clear to NULL)
        prompt_instructions = None
        quarterly_prompt = None
        if 'prompt_instructions' in body:
            val = body['prompt_instructions']
            if val is not None and not isinstance(val, str):
                return jsonify({'success': False, 'error': {
                    'message': 'prompt_instructions must be a string or null'
                }}), 400
            if val is not None and len(val) > 8000:
                return jsonify({'success': False, 'error': {
                    'message': 'prompt_instructions must be <= 8000 chars'
                }}), 400
            prompt_instructions = val if val is not None else ''
        if 'quarterly_prompt' in body:
            val = body['quarterly_prompt']
            if val is not None and not isinstance(val, str):
                return jsonify({'success': False, 'error': {
                    'message': 'quarterly_prompt must be a string or null'
                }}), 400
            if val is not None and len(val) > 8000:
                return jsonify({'success': False, 'error': {
                    'message': 'quarterly_prompt must be <= 8000 chars'
                }}), 400
            quarterly_prompt = val if val is not None else ''

        metric_group = None
        if 'metric_group' in body:
            val = body['metric_group']
            if val is not None:
                metric_group = str(val).strip() or 'other'
            else:
                metric_group = 'other'

        updated = db.update_metric_schema(
            row_id, active=active, label=label, unit=unit,
            prompt_instructions=prompt_instructions,
            quarterly_prompt=quarterly_prompt,
            metric_group=metric_group,
        )
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
    return db._metric_key_exists_in_schema(key)


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


# ── Metric examples routes ─────────────────────────────────────────────────────

@bp.route('/api/metric_schema/<string:metric_key>/snippet_analysis', methods=['GET'])
def snippet_analysis(metric_key):
    """Analyze source_snippets from historical data_points for a metric.

    Query params:
      ticker (optional) — scope to a single company
      limit  (optional, default 500) — max snippets to fetch from DB
    Returns: {success, data: {table_rows, prose_ngrams, total_snippets, unique_snippets}}
    """
    try:
        from app_globals import get_db
        from interpreters.snippet_analyzer import analyze_snippets
        db = get_db()
        if not _metric_key_exists(db, metric_key):
            return jsonify({'success': False, 'error': {
                'code': 'NOT_FOUND', 'message': f'Unknown metric key: {metric_key}',
            }}), 404
        ticker = request.args.get('ticker') or None
        limit = int(request.args.get('limit', 500))
        snippets = db.get_snippets_for_metric(metric_key, ticker=ticker, limit=limit)
        result = analyze_snippets(snippets)
        return jsonify({'success': True, 'data': result})
    except Exception:
        log.error('Error analyzing snippets for %s', metric_key, exc_info=True)
        return jsonify({'success': False, 'error': {'message': 'Internal server error'}}), 500


@bp.route('/api/metric_schema/<string:metric_key>/examples', methods=['GET'])
def list_metric_examples(metric_key):
    """List stored metric examples.

    Query params:
      ticker (optional) — filter to ticker + metric-wide rows
      all    (optional, 1) — include inactive rows
    """
    try:
        from app_globals import get_db
        db = get_db()
        if not _metric_key_exists(db, metric_key):
            return jsonify({'success': False, 'error': {
                'code': 'NOT_FOUND', 'message': f'Unknown metric key: {metric_key}',
            }}), 404
        ticker = request.args.get('ticker') or None
        active_only = request.args.get('all') != '1'
        rows = db.get_metric_examples(metric_key, ticker=ticker, active_only=active_only)
        return jsonify({'success': True, 'data': rows})
    except Exception:
        log.error('Error listing metric examples for %s', metric_key, exc_info=True)
        return jsonify({'success': False, 'error': {'message': 'Internal server error'}}), 500


@bp.route('/api/metric_schema/<string:metric_key>/examples', methods=['POST'])
def add_metric_example_route(metric_key):
    """Add a metric example.

    Body: {snippet (required), ticker?, label?, source_type?}
    Returns 201 on success with {success, data: {id}}.
    """
    try:
        from app_globals import get_db
        db = get_db()
        if not _metric_key_exists(db, metric_key):
            return jsonify({'success': False, 'error': {
                'code': 'NOT_FOUND', 'message': f'Unknown metric key: {metric_key}',
            }}), 404
        body = request.get_json(silent=True) or {}
        snippet = (body.get('snippet') or '').strip()
        if not snippet:
            return jsonify({'success': False, 'error': {
                'code': 'MISSING_FIELD', 'message': 'snippet is required',
            }}), 400
        ticker = body.get('ticker') or None
        label = body.get('label') or None
        source_type = body.get('source_type') or None
        try:
            eid = db.add_metric_example(metric_key, snippet, ticker=ticker,
                                         label=label, source_type=source_type)
        except ValueError as e:
            return jsonify({'success': False, 'error': {
                'code': 'NOT_FOUND', 'message': str(e),
            }}), 404
        return jsonify({'success': True, 'data': {'id': eid}}), 201
    except Exception:
        log.error('Error adding metric example for %s', metric_key, exc_info=True)
        return jsonify({'success': False, 'error': {'message': 'Internal server error'}}), 500


@bp.route('/api/metric_schema/<string:metric_key>/examples/<int:example_id>', methods=['PATCH'])
def update_metric_example_route(metric_key, example_id):
    """Update a metric example (snippet, label, active, source_type)."""
    try:
        from app_globals import get_db
        db = get_db()
        body = request.get_json(silent=True) or {}
        updated = db.update_metric_example(
            example_id,
            snippet=body.get('snippet') or None,
            label=body.get('label') or None,
            active=body.get('active'),
            source_type=body.get('source_type') or None,
        )
        if not updated:
            return jsonify({'success': False, 'error': {
                'code': 'NOT_FOUND', 'message': f'Example {example_id} not found',
            }}), 404
        return jsonify({'success': True})
    except Exception:
        log.error('Error updating metric example %s', example_id, exc_info=True)
        return jsonify({'success': False, 'error': {'message': 'Internal server error'}}), 500


@bp.route('/api/metric_schema/<string:metric_key>/examples/<int:example_id>', methods=['DELETE'])
def delete_metric_example_route(metric_key, example_id):
    """Delete a metric example."""
    try:
        from app_globals import get_db
        db = get_db()
        deleted = db.delete_metric_example(example_id)
        if not deleted:
            return jsonify({'success': False, 'error': {
                'code': 'NOT_FOUND', 'message': f'Example {example_id} not found',
            }}), 404
        return jsonify({'success': True})
    except Exception:
        log.error('Error deleting metric example %s', example_id, exc_info=True)
        return jsonify({'success': False, 'error': {'message': 'Internal server error'}}), 500
