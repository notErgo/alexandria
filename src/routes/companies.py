"""Company and metric schema API routes."""
import json
import logging
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

import requests
from flask import Blueprint, jsonify, request

log = logging.getLogger('miners.routes.companies')

bp = Blueprint('companies', __name__)

_VALID_SCRAPER_MODES = {'rss', 'index', 'template', 'skip'}
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
    return None


def _expand_template_url(url_template: str) -> str:
    """Convert a template URL into a probeable sample URL."""
    return (url_template
            .replace('{Month}', 'January')
            .replace('{month}', 'january')
            .replace('{year}', '2025'))


def _probe_candidate_url(source_type: str, url: str, timeout: int = 12) -> dict:
    """Probe a candidate URL and return deterministic evidence."""
    probe_url = _expand_template_url(url) if source_type == 'TEMPLATE' else url
    checked_at = datetime.now(timezone.utc).isoformat()
    try:
        resp = requests.get(probe_url, timeout=timeout, allow_redirects=True, headers={
            'User-Agent': 'Hermeneutic Miner Probe/1.0'
        })
        status = int(resp.status_code)
        body = (resp.text or '')[:4000]
        if status >= 500:
            return {
                'probe_status': 'ERROR',
                'http_status': status,
                'last_checked': checked_at,
                'evidence_title': None,
                'evidence_date': None,
            }
        if status in (401, 403):
            return {
                'probe_status': 'BLOCKED',
                'http_status': status,
                'last_checked': checked_at,
                'evidence_title': None,
                'evidence_date': None,
            }
        if status >= 400:
            return {
                'probe_status': 'DEAD',
                'http_status': status,
                'last_checked': checked_at,
                'evidence_title': None,
                'evidence_date': None,
            }

        evidence_title = None
        lowered = body.lower()
        if source_type in {'RSS', 'GLOBENEWSWIRE'}:
            ok = ('<rss' in lowered) or ('<feed' in lowered)
            if source_type == 'GLOBENEWSWIRE' and not ok:
                ok = 'globenewswire' in lowered and any(k in lowered for k in ('release', 'news', 'announcement'))
            probe_status = 'ACTIVE' if ok else 'ERROR'
            if ok:
                evidence_title = 'RSS/Atom feed detected' if ('<rss' in lowered or '<feed' in lowered) else 'GlobeNewswire content detected'
        elif source_type == 'PRNEWSWIRE':
            ok = ('<rss' in lowered) or ('<feed' in lowered) or (
                ('prnewswire' in lowered) and any(k in lowered for k in ('release', 'news', 'announcement'))
            )
            probe_status = 'ACTIVE' if ok else 'ERROR'
            if ok:
                evidence_title = 'PRNewswire content detected'
        else:
            # Simple HTML/newsroom evidence without brittle parser dependencies.
            keywords = ('press release', 'news', 'investor', 'production', 'operations update')
            ok = any(k in lowered for k in keywords)
            probe_status = 'ACTIVE' if ok else 'ERROR'
            if ok:
                evidence_title = 'newsroom-like content detected'

        return {
            'probe_status': probe_status,
            'http_status': status,
            'last_checked': checked_at,
            'evidence_title': evidence_title,
            'evidence_date': None,
        }
    except requests.Timeout:
        return {
            'probe_status': 'TIMEOUT',
            'http_status': None,
            'last_checked': checked_at,
            'evidence_title': None,
            'evidence_date': None,
        }
    except requests.RequestException:
        return {
            'probe_status': 'ERROR',
            'http_status': None,
            'last_checked': checked_at,
            'evidence_title': None,
            'evidence_date': None,
        }


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


def _run_bootstrap_probe_for_ticker(db, ticker: str, apply_mode: bool, allow_apply_skip: bool, timeout: int) -> dict:
    """Probe discovery candidates and recommend/apply scraper mode for one ticker."""
    company = db.get_company(ticker)
    if company is None:
        raise ValueError(f'Company {ticker!r} not found')

    # Start with stored candidates; if none exist, synthesize from current config.
    candidates = db.list_discovery_candidates(ticker, verified_only=False)
    if not candidates:
        seed_candidates = []
        if company.get('rss_url'):
            seed_candidates.append({'source_type': 'RSS', 'url': company['rss_url']})
        if company.get('prnewswire_url'):
            seed_candidates.append({'source_type': 'PRNEWSWIRE', 'url': company['prnewswire_url']})
        if company.get('globenewswire_url'):
            seed_candidates.append({'source_type': 'GLOBENEWSWIRE', 'url': company['globenewswire_url']})
        if company.get('url_template'):
            seed_candidates.append({
                'source_type': 'TEMPLATE',
                'url': company['url_template'],
                'pr_start_year': company.get('pr_start_year'),
            })
        if company.get('ir_url'):
            seed_candidates.append({'source_type': 'IR_PRIMARY', 'url': company['ir_url']})
        if not seed_candidates:
            raise ValueError('No discovery candidates or seed URLs available')
        for c in seed_candidates:
            db.upsert_discovery_candidate({
                'ticker': ticker,
                'source_type': c['source_type'],
                'url': c['url'],
                'pr_start_year': c.get('pr_start_year'),
                'proposed_by': 'bootstrap_seed',
                'verified': 0,
            })
        candidates = db.list_discovery_candidates(ticker, verified_only=False)

    log.info(
        "event=bootstrap_probe_ticker_start ticker=%s apply_mode=%s allow_apply_skip=%s timeout=%s candidate_count=%s",
        ticker, int(apply_mode), int(allow_apply_skip), timeout, len(candidates),
    )

    probed = []
    for c in candidates:
        result = _probe_candidate_url(c['source_type'], c['url'], timeout=timeout)
        verified = 1 if result['probe_status'] == 'ACTIVE' else 0
        db.upsert_discovery_candidate({
            'ticker': ticker,
            'source_type': c['source_type'],
            'url': c['url'],
            'pr_start_year': c.get('pr_start_year'),
            'confidence': c.get('confidence'),
            'rationale': c.get('rationale'),
            'proposed_by': c.get('proposed_by') or 'agent',
            'verified': verified,
            **result,
        })
        db.upsert_source_audit({
            'ticker': ticker,
            'source_type': c['source_type'],
            'url': c['url'],
            'last_checked': result.get('last_checked'),
            'http_status': result.get('http_status'),
            'status': result.get('probe_status', 'NOT_TRIED'),
            'notes': result.get('evidence_title'),
        })
        probed.append({**c, **result, 'verified': verified})
        log.debug(
            "event=bootstrap_probe_candidate ticker=%s source_type=%s probe_status=%s http_status=%s verified=%s",
            ticker, c['source_type'], result.get('probe_status'), result.get('http_status'), verified
        )

    active = [p for p in probed if p.get('probe_status') == 'ACTIVE']
    recommended_mode = 'skip'
    chosen = None
    for st in ('RSS', 'GLOBENEWSWIRE', 'PRNEWSWIRE', 'TEMPLATE', 'IR_PRIMARY'):
        chosen = next((p for p in active if p['source_type'] == st), None)
        if chosen:
            recommended_mode = {
                'RSS': 'rss',
                'GLOBENEWSWIRE': 'rss',
                'PRNEWSWIRE': 'rss',
                'TEMPLATE': 'template',
                'IR_PRIMARY': 'index',
            }[st]
            break

    applied = False
    if apply_mode and (recommended_mode != 'skip' or allow_apply_skip):
        updates = {'scraper_mode': recommended_mode}
        if recommended_mode == 'rss' and chosen:
            updates['rss_url'] = chosen['url']
            if chosen['source_type'] == 'PRNEWSWIRE':
                updates['prnewswire_url'] = chosen['url']
            if chosen['source_type'] == 'GLOBENEWSWIRE':
                updates['globenewswire_url'] = chosen['url']
        elif recommended_mode == 'template' and chosen:
            updates['url_template'] = chosen['url']
            if chosen.get('pr_start_year'):
                updates['pr_start_year'] = chosen.get('pr_start_year')
        elif recommended_mode == 'index' and chosen:
            updates['ir_url'] = chosen['url']
        if recommended_mode == 'skip':
            updates['skip_reason'] = (
                "Bootstrap probe found no ACTIVE candidates. Re-check sources or provide manual candidate URLs."
            )
        db.update_company_config(ticker, **updates)
        applied = True

    db.update_company_scraper_fields(
        ticker,
        probe_completed_at=datetime.now(timezone.utc).isoformat(),
        scraper_status='probe_ok' if active else 'probe_failed',
        last_scrape_error=None if active else 'bootstrap probe found no ACTIVE sources',
    )

    log.info(
        "event=bootstrap_probe_ticker_end ticker=%s active_candidates=%s recommended_mode=%s applied=%s",
        ticker, len(active), recommended_mode, int(applied),
    )

    return {
        'ticker': ticker,
        'recommended_mode': recommended_mode,
        'active_candidates': len(active),
        'applied': applied,
        'probed': probed,
    }


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
