"""
Diagnostics API route: aggregated extraction analytics.

GET /api/diagnostics
Returns pattern usage, metric coverage, confidence distribution,
keyword frequency computed from source snippets, and per-company status.
"""
import json
import os
import re
import logging
from collections import Counter
from flask import Blueprint, jsonify

log = logging.getLogger('miners.routes.diagnostics')

bp = Blueprint('diagnostics', __name__)

# All metrics and 13 tickers — used to fill zero-cells in the coverage heatmap
_ALL_METRICS = [
    'production_btc', 'hodl_btc', 'sold_btc', 'hashrate_eh', 'realization_rate',
    # v2 metrics
    'net_btc_balance_change', 'encumbered_btc',
    'mining_mw', 'ai_hpc_mw', 'hpc_revenue_usd', 'gpu_count',
]
from config import get_all_tickers as _get_all_tickers
_ALL_TICKERS = _get_all_tickers()
_METRIC_LABELS = {
    'production_btc':         'Production',
    'hodl_btc':               'Holdings',
    'sold_btc':               'Sold',
    'hashrate_eh':            'Hashrate',
    'realization_rate':       'Realization',
    # v2 metrics
    'net_btc_balance_change': 'Net BTC Change',
    'encumbered_btc':         'Encumbered BTC',
    'mining_mw':              'Mining MW',
    'ai_hpc_mw':              'AI/HPC MW',
    'hpc_revenue_usd':        'HPC Revenue',
    'gpu_count':              'GPU Count',
}

# Common English stopwords plus domain/document boilerplate to ignore
_STOPWORDS = {
    'the', 'a', 'an', 'and', 'or', 'but', 'in', 'on', 'at', 'to', 'for', 'of',
    'with', 'by', 'from', 'as', 'is', 'was', 'are', 'were', 'be', 'been', 'being',
    'have', 'has', 'had', 'do', 'does', 'did', 'will', 'would', 'could', 'should',
    'may', 'might', 'shall', 'can', 'its', 'it', 'this', 'that', 'these', 'those',
    'we', 'our', 'us', 'they', 'their', 'them', 'he', 'she', 'his', 'her',
    'not', 'no', 'nor', 'so', 'yet', 'both', 'either', 'neither', 'each',
    'than', 'too', 'very', 'just', 'also', 'more', 'most', 'other', 'into',
    'during', 'before', 'after', 'above', 'below', 'between', 'through',
    'while', 'although', 'because', 'if', 'when', 'where', 'which', 'who',
    'what', 'how', 'all', 'any', 'few', 'same', 'such', 'only', 'new', 'per',
    'one', 'two', 'three', 'four', 'five', 'six', 'seven', 'eight', 'nine', 'ten',
    # document boilerplate
    'inc', 'llc', 'corp', 'ltd', 'plc', 'back', 'list', 'page', 'www',
    'com', 'http', 'https', 'documents', 'document', 'update', 'updates',
    'announces', 'announce', 'announced', 'company', 'following', 'including',
    'approximately', 'million', 'billion', 'based', 'period', 'quarter',
    'compared', 'year', 'month', 'date', 'first', 'second', 'third', 'fourth',
}
_WORD_RE = re.compile(r'[a-z]{3,}')

# companies.json path (relative to this file: src/routes/ → ../../config/)
_COMPANIES_JSON = os.path.join(
    os.path.dirname(__file__), '..', '..', 'config', 'companies.json'
)

def _load_company_config() -> dict:
    """Return dict of ticker → {name, tier, scrape_mode, skip_reason} from companies.json."""
    try:
        with open(_COMPANIES_JSON) as f:
            companies = json.load(f)
        return {
            c['ticker']: {
                'name':        c.get('name', c['ticker']),
                'tier':        c.get('tier', 2),
                'scrape_mode': c.get('scrape_mode', 'template'),
                'skip_reason': c.get('skip_reason'),
                'active':      c.get('active', True),
            }
            for c in companies
        }
    except Exception:
        log.warning("Could not load companies.json for diagnostics", exc_info=True)
        return {}


def _compute_keywords(snippets: list, top_n: int = 40) -> list:
    counter = Counter()
    for snippet in snippets:
        if not snippet:
            continue
        for word in _WORD_RE.findall(snippet.lower()):
            if word not in _STOPWORDS:
                counter[word] += 1
    return [{'word': w, 'count': c} for w, c in counter.most_common(top_n)]


@bp.route('/api/diagnostics')
def get_diagnostics():
    try:
        from app_globals import get_db
        db = get_db()

        pattern_usage = db.get_pattern_usage()
        raw_coverage = db.get_metric_coverage()
        confidence_buckets = db.get_confidence_buckets()
        snippets = db.get_snippets()
        keywords = _compute_keywords(snippets)
        raw_status = db.get_company_status()
        company_config = _load_company_config()

        # Build complete coverage matrix: all 13 tickers × 5 metrics, fill 0 for gaps
        coverage_index = {(r['ticker'], r['metric']): r['period_count'] for r in raw_coverage}
        coverage = []
        for ticker in _ALL_TICKERS:
            for metric in _ALL_METRICS:
                coverage.append({
                    'ticker': ticker,
                    'metric': _METRIC_LABELS.get(metric, metric),
                    'count': coverage_index.get((ticker, metric), 0),
                })

        # Build per-company status for all 13 companies: merge DB stats with config
        db_status = {r['ticker']: r for r in raw_status}
        company_status = []
        for ticker in _ALL_TICKERS:
            cfg = company_config.get(ticker, {})
            row = db_status.get(ticker, {})
            scrape_mode = cfg.get('scrape_mode', 'template')
            skip_reason = cfg.get('skip_reason')
            # Determine why a company has no data
            if skip_reason:
                data_state = 'skip'
            elif scrape_mode == 'index' and not row:
                data_state = 'index_no_data'
            elif row and row.get('prod_months', 0) > 0:
                data_state = 'ok'
            elif row and row.get('report_count', 0) > 0:
                data_state = 'ingested_no_extraction'
            else:
                data_state = 'no_data'
            company_status.append({
                'ticker':           ticker,
                'name':             cfg.get('name', ticker),
                'tier':             cfg.get('tier', 2),
                'scrape_mode':      scrape_mode,
                'skip_reason':      skip_reason,
                'active':           cfg.get('active', True),
                'data_state':       data_state,
                'report_count':     row.get('report_count', 0),
                'data_point_count': row.get('data_point_count', 0),
                'prod_months':      row.get('prod_months', 0),
                'first_period':     row.get('first_period'),
                'last_period':      row.get('last_period'),
                'avg_confidence':   row.get('avg_confidence'),
            })

        return jsonify({
            'success': True,
            'data': {
                'pattern_usage':   pattern_usage,
                'coverage':        coverage,
                'all_tickers':     _ALL_TICKERS,
                'all_metrics':     [_METRIC_LABELS[m] for m in _ALL_METRICS],
                'confidence_buckets': confidence_buckets,
                'keywords':        keywords,
                'company_status':  company_status,
            }
        })
    except Exception as e:
        log.error("Diagnostics query failed: %s", e, exc_info=True)
        return jsonify({'success': False, 'error': {'message': 'Internal server error'}}), 500
