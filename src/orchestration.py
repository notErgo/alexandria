"""
Orchestration guardrails for the ingest pipeline.

EDGAR-first policy: IR and archive extraction should only run after EDGAR
has been fetched, so that cross-source agreement scoring has full context.
"""
import logging
from dataclasses import dataclass, field
from typing import Optional

log = logging.getLogger('miners.orchestration')


@dataclass
class EdgarCheckResult:
    """Result of an EDGAR prerequisite check."""
    complete: bool
    ticker: Optional[str]
    last_run: Optional[dict] = None
    warning: Optional[str] = None


def check_edgar_complete(db, ticker: Optional[str] = None) -> EdgarCheckResult:
    """Check whether a successful EDGAR pipeline run exists for ticker (or any ticker).

    Args:
        db: MinerDB instance
        ticker: Ticker symbol to check, or None for a global check

    Returns:
        EdgarCheckResult with complete=True if a successful run is found.
    """
    last_run = db.get_last_successful_pipeline_run(source='edgar', ticker=ticker)
    if last_run:
        return EdgarCheckResult(
            complete=True,
            ticker=ticker,
            last_run=last_run,
        )
    scope = ticker or 'any'
    warning = (
        f"No successful EDGAR run found for {scope}. "
        "Run POST /api/ingest/edgar before IR or archive extraction "
        "to ensure cross-source agreement has full EDGAR context."
    )
    log.warning("event=edgar_prereq_missing ticker=%s", scope)
    return EdgarCheckResult(
        complete=False,
        ticker=ticker,
        last_run=None,
        warning=warning,
    )


# ---------------------------------------------------------------------------
# Bootstrap probe helpers — moved from routes.companies (L5) to here (L3)
# so that routes.pipeline can import without creating L5->L5 coupling.
# routes.companies re-exports these for backward compatibility.
# ---------------------------------------------------------------------------
import re as _re
import requests as _requests
from datetime import datetime as _datetime, timezone as _timezone


def _expand_template_url(url_template: str) -> str:
    """Convert a template URL into a probeable sample URL."""
    return (url_template
            .replace('{Month}', 'January')
            .replace('{month}', 'january')
            .replace('{year}', '2025'))


def _probe_candidate_url(source_type: str, url: str, timeout: int = 12) -> dict:
    """Probe a candidate URL and return deterministic evidence."""
    probe_url = _expand_template_url(url) if source_type == 'TEMPLATE' else url
    checked_at = _datetime.now(_timezone.utc).isoformat()
    try:
        resp = _requests.get(probe_url, timeout=timeout, allow_redirects=True, headers={
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
    except _requests.Timeout:
        return {
            'probe_status': 'TIMEOUT',
            'http_status': None,
            'last_checked': checked_at,
            'evidence_title': None,
            'evidence_date': None,
        }
    except _requests.RequestException:
        return {
            'probe_status': 'ERROR',
            'http_status': None,
            'last_checked': checked_at,
            'evidence_title': None,
            'evidence_date': None,
        }


def run_bootstrap_probe_for_ticker(
    db, ticker: str, apply_mode: bool, allow_apply_skip: bool, timeout: int
) -> dict:
    """Probe discovery candidates and recommend/apply scraper mode for one ticker.

    Moved from routes.companies to orchestration (L3) so routes.pipeline can
    import without creating same-layer (L5->L5) coupling.
    """
    company = db.get_company(ticker)
    if company is None:
        raise ValueError(f'Company {ticker!r} not found')

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
                'pr_start_date': company.get('pr_start_date'),
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
                'pr_start_date': c.get('pr_start_date'),
                'proposed_by': 'bootstrap_seed',
                'verified': 0,
            })
        candidates = db.list_discovery_candidates(ticker, verified_only=False)

    log.info(
        "event=bootstrap_probe_ticker_start ticker=%s apply_mode=%s allow_apply_skip=%s "
        "timeout=%s candidate_count=%s",
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
            'pr_start_date': c.get('pr_start_date'),
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
            "event=bootstrap_probe_candidate ticker=%s source_type=%s probe_status=%s "
            "http_status=%s verified=%s",
            ticker, c['source_type'], result.get('probe_status'), result.get('http_status'), verified,
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
            if chosen.get('pr_start_date'):
                updates['pr_start_date'] = chosen.get('pr_start_date')
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
        probe_completed_at=_datetime.now(_timezone.utc).isoformat(),
        scraper_status='probe_ok' if active else 'probe_failed',
        last_scrape_error=None if active else 'bootstrap probe found no ACTIVE sources',
    )

    log.info(
        "event=bootstrap_probe_ticker_end ticker=%s active_candidates=%s "
        "recommended_mode=%s applied=%s",
        ticker, len(active), recommended_mode, int(applied),
    )

    return {
        'ticker': ticker,
        'recommended_mode': recommended_mode,
        'active_candidates': len(active),
        'applied': applied,
        'probed': probed,
    }


# Backward-compat alias — routes.companies imported this private name historically.
_run_bootstrap_probe_for_ticker = run_bootstrap_probe_for_ticker
