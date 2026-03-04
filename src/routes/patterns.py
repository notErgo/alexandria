"""
Pattern management API routes.

GET    /api/patterns                       — all patterns + fire counts + keyword gaps
PUT    /api/patterns/<metric>/<pattern_id> — update regex / confidence_weight
POST   /api/patterns/<metric>              — add new pattern to a metric
DELETE /api/patterns/<metric>/<pattern_id> — remove a pattern
POST   /api/patterns/test                  — test a regex against provided text
"""
import json
import re
import logging
from collections import Counter
from pathlib import Path
from flask import Blueprint, jsonify, request

log = logging.getLogger('miners.routes.patterns')

bp = Blueprint('patterns', __name__)

_METRIC_ORDER = [
    'production_btc', 'hodl_btc', 'sold_btc', 'hashrate_eh', 'realization_rate'
]
_METRIC_LABELS = {
    'production_btc': 'Production BTC',
    'hodl_btc':       'Holdings BTC',
    'sold_btc':'Sold BTC',
    'hashrate_eh':    'Hashrate EH/s',
    'realization_rate':'Realization Rate',
}

# Words that are high-frequency noise in snippets but not extraction signals
_GAP_NOISE = {
    'riot', 'marathon', 'mara', 'operations', 'mining', 'unaudited', 'total',
    'leader', 'comparison', 'december', 'september', 'august', 'july', 'june',
    'april', 'march', 'price', 'increased', 'day', 'credits', 'nasdaq',
    'digital', 'back', 'list',
}

def _patterns_dir() -> Path:
    from config import CONFIG_DIR
    return Path(CONFIG_DIR) / 'patterns'

def _load_metric_file(metric: str) -> dict:
    path = _patterns_dir() / f"{metric}.json"
    with open(path) as f:
        return json.load(f)

def _save_metric_file(metric: str, data: dict) -> None:
    path = _patterns_dir() / f"{metric}.json"
    with open(path, 'w') as f:
        json.dump(data, f, indent=2)

def _all_pattern_regex_text() -> str:
    """Concatenate all regex strings for keyword gap analysis."""
    text = ""
    for metric in _METRIC_ORDER:
        try:
            d = _load_metric_file(metric)
            for p in d['patterns']:
                text += " " + p['regex'].lower()
        except Exception:
            pass
    return text

def _compute_keyword_gaps(snippets: list, top_n: int = 50) -> list:
    """Return top keywords from snippets that don't appear in any pattern regex."""
    stopwords = {
        'the', 'a', 'an', 'and', 'or', 'but', 'in', 'on', 'at', 'to', 'for',
        'of', 'with', 'by', 'from', 'as', 'is', 'was', 'are', 'were', 'be',
        'been', 'have', 'has', 'had', 'do', 'does', 'did', 'will', 'would',
        'could', 'should', 'may', 'might', 'can', 'its', 'it', 'this', 'that',
        'these', 'those', 'we', 'our', 'us', 'they', 'their', 'not', 'no',
        'than', 'very', 'just', 'also', 'more', 'most', 'other', 'into', 'all',
        'any', 'few', 'same', 'such', 'only', 'new', 'per', 'one', 'two',
        'three', 'inc', 'llc', 'corp', 'ltd', 'plc', 'page', 'www', 'com',
        'update', 'updates', 'announces', 'company', 'following', 'including',
        'approximately', 'million', 'billion', 'based', 'period', 'quarter',
        'compared', 'year', 'month', 'date', 'first', 'second', 'third',
    }
    word_re = re.compile(r'[a-z]{3,}')
    counter = Counter()
    for snippet in snippets:
        if not snippet:
            continue
        for word in word_re.findall(snippet.lower()):
            if word not in stopwords:
                counter[word] += 1

    regex_text = _all_pattern_regex_text()
    result = []
    for word, count in counter.most_common(top_n * 2):
        if word in _GAP_NOISE:
            continue
        covered = word in regex_text
        result.append({'word': word, 'count': count, 'covered': covered})
        if len(result) >= top_n:
            break
    return result


@bp.route('/api/patterns', methods=['GET'])
def get_patterns():
    try:
        from app_globals import get_db
        db = get_db()

        # Fire counts and avg confidence per pattern id from DB
        usage_rows = db.get_pattern_usage()
        usage = {r['extraction_method']: r for r in usage_rows}

        # Load pattern files
        metrics_out = []
        for metric in _METRIC_ORDER:
            try:
                data = _load_metric_file(metric)
            except FileNotFoundError:
                continue
            patterns_out = []
            for p in sorted(data['patterns'], key=lambda x: x['priority']):
                u = usage.get(p['id'], {})
                patterns_out.append({
                    'id':               p['id'],
                    'regex':            p['regex'],
                    'confidence_weight': p['confidence_weight'],
                    'priority':         p['priority'],
                    'fires':            u.get('count', 0),
                    'avg_conf':         u.get('avg_conf', None),
                })
            metrics_out.append({
                'metric':  metric,
                'label':   _METRIC_LABELS.get(metric, metric),
                'unit':    data.get('unit', ''),
                'patterns': patterns_out,
            })

        # Keyword gap analysis
        snippets = db.get_snippets(limit=2000)
        gaps = _compute_keyword_gaps(snippets, top_n=40)

        return jsonify({'success': True, 'data': {
            'metrics': metrics_out,
            'keyword_gaps': gaps,
        }})
    except Exception as e:
        log.error("get_patterns failed: %s", e, exc_info=True)
        return jsonify({'success': False, 'error': {'message': 'Internal server error'}}), 500


@bp.route('/api/patterns/<metric>/<pattern_id>', methods=['PUT'])
def update_pattern(metric, pattern_id):
    if metric not in _METRIC_ORDER:
        return jsonify({'success': False, 'error': {'message': 'Unknown metric'}}), 400
    body = request.get_json(silent=True) or {}
    new_regex  = body.get('regex', '').strip()
    new_weight = body.get('confidence_weight')
    if not new_regex:
        return jsonify({'success': False, 'error': {'message': 'regex is required'}}), 400
    try:
        re.compile(new_regex)
    except re.error as e:
        return jsonify({'success': False, 'error': {'message': f'Invalid regex: {e}'}}), 400
    if new_weight is not None:
        try:
            new_weight = float(new_weight)
            if not (0.0 < new_weight <= 1.0):
                raise ValueError
        except (TypeError, ValueError):
            return jsonify({'success': False, 'error': {'message': 'confidence_weight must be 0–1'}}), 400
    try:
        data = _load_metric_file(metric)
        found = False
        for p in data['patterns']:
            if p['id'] == pattern_id:
                p['regex'] = new_regex
                if new_weight is not None:
                    p['confidence_weight'] = new_weight
                found = True
                break
        if not found:
            return jsonify({'success': False, 'error': {'message': 'Pattern not found'}}), 404
        _save_metric_file(metric, data)
        from app_globals import reload_registry
        reload_registry()
        log.info("Updated pattern %s in %s", pattern_id, metric)
        return jsonify({'success': True})
    except Exception as e:
        log.error("update_pattern failed: %s", e, exc_info=True)
        return jsonify({'success': False, 'error': {'message': 'Internal server error'}}), 500


@bp.route('/api/patterns/apply', methods=['POST'])
def apply_pattern():
    """
    Apply a regex pattern to all reports with raw_text and create data_points.

    Input:  {regex, metric, confidence_weight (default 0.80)}
    Output: {applied_to, created, skipped_existing, low_confidence, no_match, per_ticker}
    """
    body = request.get_json(silent=True) or {}
    regex = str(body.get('regex', '')).strip()
    metric = str(body.get('metric', '')).strip()
    weight = body.get('confidence_weight', 0.80)

    if not regex:
        return jsonify({'success': False, 'error': {'message': 'regex is required'}}), 400

    try:
        re.compile(regex)
    except re.error as exc:
        return jsonify({'success': False, 'error': {'message': f'Invalid regex: {exc}'}}), 400

    if metric not in _METRIC_ORDER:
        return jsonify({'success': False, 'error': {'message': 'Unknown metric'}}), 400

    try:
        weight = float(weight)
        if not (0.0 < weight <= 1.0):
            raise ValueError
    except (TypeError, ValueError):
        weight = 0.80

    try:
        from app_globals import get_db
        from extractors.extractor import extract_all
        from config import CONFIDENCE_REVIEW_THRESHOLD

        db = get_db()
        pattern = {'id': 'applied_pattern', 'regex': regex,
                   'confidence_weight': weight, 'priority': 0}

        reports = db.get_reports_with_text()

        counts = {'applied_to': 0, 'created': 0, 'skipped_existing': 0,
                  'low_confidence': 0, 'no_match': 0}
        per_ticker: dict = {}

        for report in reports:
            raw_text = db.get_report_raw_text(report['id'])
            if not raw_text:
                continue

            counts['applied_to'] += 1
            ticker = report['ticker']
            period = report['report_date']

            if ticker not in per_ticker:
                per_ticker[ticker] = {'created': 0, 'skipped': 0, 'low_conf': 0}

            results = extract_all(raw_text, [pattern], metric)
            if not results:
                counts['no_match'] += 1
                continue

            best = results[0]

            if db.data_point_exists(ticker, period, metric):
                counts['skipped_existing'] += 1
                per_ticker[ticker]['skipped'] += 1
                continue

            if best.confidence >= CONFIDENCE_REVIEW_THRESHOLD:
                db.insert_data_point({
                    'report_id': report['id'],
                    'ticker': ticker,
                    'period': period,
                    'metric': metric,
                    'value': best.value,
                    'unit': best.unit,
                    'confidence': best.confidence,
                    'extraction_method': 'applied_pattern',
                    'source_snippet': best.source_snippet,
                })
                counts['created'] += 1
                per_ticker[ticker]['created'] += 1
            else:
                db.insert_review_item({
                    'data_point_id': None,
                    'ticker': ticker,
                    'period': period,
                    'metric': metric,
                    'raw_value': str(best.value),
                    'confidence': best.confidence,
                    'source_snippet': best.source_snippet,
                    'status': 'pending',
                })
                counts['low_confidence'] += 1
                per_ticker[ticker]['low_conf'] += 1

        return jsonify({'success': True, 'data': {**counts, 'per_ticker': per_ticker}})

    except Exception as exc:
        log.error("apply_pattern failed: %s", exc, exc_info=True)
        return jsonify({'success': False, 'error': {'message': 'Internal server error'}}), 500


@bp.route('/api/patterns/<metric>', methods=['POST'])
def add_pattern(metric):
    if metric not in _METRIC_ORDER:
        return jsonify({'success': False, 'error': {'message': 'Unknown metric'}}), 400
    body = request.get_json(silent=True) or {}
    new_regex  = body.get('regex', '').strip()
    new_weight = body.get('confidence_weight', 0.80)
    if not new_regex:
        return jsonify({'success': False, 'error': {'message': 'regex is required'}}), 400
    try:
        re.compile(new_regex)
    except re.error as e:
        return jsonify({'success': False, 'error': {'message': f'Invalid regex: {e}'}}), 400
    try:
        new_weight = float(new_weight)
        if not (0.0 < new_weight <= 1.0):
            raise ValueError
    except (TypeError, ValueError):
        return jsonify({'success': False, 'error': {'message': 'confidence_weight must be 0–1'}}), 400
    try:
        data = _load_metric_file(metric)
        existing_ids = {p['id'] for p in data['patterns']}
        # Auto-generate id: metric_prefix + next integer
        prefix = metric.replace('_btc', '').replace('_eh', '').replace('_rate', '')
        prefix = {'production': 'prod_btc', 'hodl': 'hodl_btc', 'liquidation': 'liq_btc',
                  'hashrate': 'hash_eh', 'realization': 'real_rate'}.get(prefix, metric[:8])
        n = len(data['patterns'])
        new_id = f"{prefix}_{n}"
        while new_id in existing_ids:
            n += 1
            new_id = f"{prefix}_{n}"
        max_priority = max((p['priority'] for p in data['patterns']), default=-1)
        data['patterns'].append({
            'id': new_id,
            'regex': new_regex,
            'confidence_weight': new_weight,
            'priority': max_priority + 1,
        })
        _save_metric_file(metric, data)
        from app_globals import reload_registry
        reload_registry()
        log.info("Added pattern %s to %s", new_id, metric)
        return jsonify({'success': True, 'data': {'id': new_id}})
    except Exception as e:
        log.error("add_pattern failed: %s", e, exc_info=True)
        return jsonify({'success': False, 'error': {'message': 'Internal server error'}}), 500


@bp.route('/api/patterns/<metric>/<pattern_id>', methods=['DELETE'])
def delete_pattern(metric, pattern_id):
    if metric not in _METRIC_ORDER:
        return jsonify({'success': False, 'error': {'message': 'Unknown metric'}}), 400
    try:
        data = _load_metric_file(metric)
        before = len(data['patterns'])
        data['patterns'] = [p for p in data['patterns'] if p['id'] != pattern_id]
        if len(data['patterns']) == before:
            return jsonify({'success': False, 'error': {'message': 'Pattern not found'}}), 404
        _save_metric_file(metric, data)
        from app_globals import reload_registry
        reload_registry()
        log.info("Deleted pattern %s from %s", pattern_id, metric)
        return jsonify({'success': True})
    except Exception as e:
        log.error("delete_pattern failed: %s", e, exc_info=True)
        return jsonify({'success': False, 'error': {'message': 'Internal server error'}}), 500


@bp.route('/api/patterns/generate', methods=['POST'])
def generate_pattern():
    """
    Generate a regex from a text selection containing a numeric value.

    Input:  {selected_text, metric, doc_text (optional)}
    Output: {regex, confidence_weight, match_count, matches}

    Algorithm:
    1. Find numeric value in selected_text.
    2. Split into prefix (before number) + capture group + suffix (after number).
    3. Escape prefix/suffix; collapse whitespace runs to \\s+.
    4. For BTC metrics: append (?:bitcoin|btc) suffix if not already in selected_text suffix.
    5. If doc_text provided: run finditer and return match count + contexts.
    """
    body = request.get_json(silent=True) or {}
    selected_text = str(body.get('selected_text', '')).strip()
    metric = str(body.get('metric', 'production_btc')).strip()
    doc_text = body.get('doc_text', '')

    if not selected_text:
        return jsonify({'success': False, 'error': {'message': 'selected_text is required'}}), 400

    # Find numeric value (with optional commas/decimal)
    num_re = re.compile(r'[\d,]+(?:\.\d+)?')
    m = num_re.search(selected_text)
    if not m:
        return jsonify({'success': False, 'error': {
            'message': 'No numeric value found in selected_text'}}), 400

    raw_prefix = selected_text[:m.start()].rstrip()
    raw_suffix = selected_text[m.end():].lstrip()

    def _escape_and_collapse(s: str) -> str:
        """Escape regex special chars, then collapse whitespace runs to \\s+."""
        escaped = re.escape(s)
        # re.escape converts spaces to \ (literal space in py3.7+), collapse them
        collapsed = re.sub(r'((?:\\ )+)', r'\\s+', escaped)
        return collapsed

    escaped_prefix = _escape_and_collapse(raw_prefix)
    escaped_suffix = _escape_and_collapse(raw_suffix)

    # Capture group matches comma-formatted numbers with optional decimal
    capture = r'([\d,]+(?:\.\d+)?)'

    # For BTC metrics: ensure suffix contains bitcoin|btc pattern
    _BTC_METRICS = {'production_btc', 'hodl_btc', 'sold_btc'}
    btc_suffix_re = re.compile(r'(?:bitcoin|btc)', re.IGNORECASE)

    if metric in _BTC_METRICS and not btc_suffix_re.search(raw_suffix):
        # Append BTC suffix
        if escaped_suffix:
            suffix_part = r'\s*' + escaped_suffix
        else:
            suffix_part = r'\s*(?:bitcoin|btc)'
    else:
        suffix_part = (r'\s*' + escaped_suffix) if escaped_suffix else ''

    # Assemble pattern
    if escaped_prefix:
        regex = f'(?i){escaped_prefix}\\s+{capture}{suffix_part}'
    else:
        regex = f'(?i){capture}{suffix_part}'

    # Validate assembled regex compiles
    try:
        compiled = re.compile(regex)
    except re.error:
        # Fallback: simpler pattern using only the numeric value
        regex = f'(?i){capture}\\s*(?:bitcoin|btc)' if metric in _BTC_METRICS else f'(?i){capture}'
        compiled = re.compile(regex)

    # Determine confidence weight: more specific = higher weight
    prefix_words = len(raw_prefix.split()) if raw_prefix else 0
    confidence_weight = 0.87 if prefix_words >= 1 else 0.75

    # Test against doc_text if provided
    matches_out = []
    match_count = 0
    if doc_text:
        try:
            from extractors.unit_normalizer import normalize_value
        except ImportError:
            normalize_value = None

        for match in compiled.finditer(doc_text):
            raw_val_str = match.group(1) if match.lastindex and match.lastindex >= 1 else match.group(0)
            # Normalize: strip commas
            try:
                normalized_val = float(raw_val_str.replace(',', ''))
            except (ValueError, TypeError):
                normalized_val = None

            snippet_start = max(0, match.start() - 60)
            snippet_end = min(len(doc_text), match.end() + 60)
            matches_out.append({
                'value':   normalized_val,
                'context': doc_text[snippet_start:snippet_end],
            })
        match_count = len(matches_out)

    return jsonify({'success': True, 'data': {
        'regex':            regex,
        'confidence_weight': confidence_weight,
        'match_count':      match_count,
        'matches':          matches_out,
    }})


@bp.route('/api/patterns/test', methods=['POST'])
def test_pattern():
    body = request.get_json(silent=True) or {}
    regex  = body.get('regex', '').strip()
    text   = body.get('text', '').strip()
    metric = body.get('metric', 'production_btc')
    if not regex:
        return jsonify({'success': False, 'error': {'message': 'regex is required'}}), 400
    if not text:
        return jsonify({'success': False, 'error': {'message': 'text is required'}}), 400
    try:
        compiled = re.compile(regex)
    except re.error as e:
        return jsonify({'success': False, 'error': {'message': f'Invalid regex: {e}'}}), 400
    try:
        from extractors.unit_normalizer import normalize_value
        matches = []
        for m in compiled.finditer(text):
            raw = m.group(0)
            normalized = normalize_value(raw, metric)
            snippet_start = max(0, m.start() - 60)
            snippet_end   = min(len(text), m.end() + 60)
            matches.append({
                'full_match':  raw,
                'group1':      m.group(1) if m.lastindex and m.lastindex >= 1 else None,
                'value':       normalized[0] if normalized else None,
                'unit':        normalized[1] if normalized else None,
                'context':     text[snippet_start:snippet_end],
            })
        return jsonify({'success': True, 'data': {'matches': matches}})
    except Exception as e:
        log.error("test_pattern failed: %s", e, exc_info=True)
        return jsonify({'success': False, 'error': {'message': 'Internal server error'}}), 500
