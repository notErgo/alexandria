"""
Tests for pattern suggester — TDD.

Covers:
  - _normalize_to_pattern
  - _detect_pattern_type
  - _extract_found_patterns
  - _extract_missed_patterns
  - _cluster_patterns
  - generate_suggestions
  - GET /api/suggestions/<ticker>
  - POST /api/suggestions/<ticker>/apply
"""
import pytest
from unittest.mock import MagicMock, patch


# ── Pure-function helpers ──────────────────────────────────────────────────────

def test_normalize_numbers():
    from interpreters.pattern_suggester import _normalize_to_pattern
    result = _normalize_to_pattern("1,284 BTC mined")
    assert result == "[N] BTC mined"


def test_normalize_numbers_plain():
    from interpreters.pattern_suggester import _normalize_to_pattern
    result = _normalize_to_pattern("produced 750 bitcoin in March")
    assert result == "produced [N] bitcoin in March"


def test_detect_pattern_type_prose():
    from interpreters.pattern_suggester import _detect_pattern_type
    assert _detect_pattern_type("the company mined 1284 BTC in the quarter") == 'prose'


def test_detect_pattern_type_table_row():
    from interpreters.pattern_suggester import _detect_pattern_type
    assert _detect_pattern_type("Bitcoin Mined | 1,284 | BTC") == 'table_row'


def test_detect_pattern_type_header_value():
    from interpreters.pattern_suggester import _detect_pattern_type
    # normalized form: "Production: [N]"
    assert _detect_pattern_type("Production: 1,284") == 'header_value'


def test_detect_pattern_type_table_row_spaces():
    from interpreters.pattern_suggester import _detect_pattern_type
    # 3+ consecutive spaces between tokens
    assert _detect_pattern_type("Bitcoin Mined     1,284") == 'table_row'


# ── Found-pattern extraction ───────────────────────────────────────────────────

def test_found_patterns_grouped_by_metric():
    """source_snippets from 3 data_points produce 1 cluster when normalized form is identical."""
    from interpreters.pattern_suggester import _extract_found_patterns, _cluster_patterns

    dp_rows = [
        {'metric': 'production_btc', 'source_snippet': '1,284 BTC mined', 'report_id': 1},
        {'metric': 'production_btc', 'source_snippet': '2,100 BTC mined', 'report_id': 2},
        {'metric': 'production_btc', 'source_snippet': '980 BTC mined',   'report_id': 3},
    ]
    hits = _extract_found_patterns(dp_rows)
    clusters = _cluster_patterns(hits)
    production_clusters = [c for c in clusters if c['metric'] == 'production_btc']
    assert len(production_clusters) == 1
    assert production_clusters[0]['normalized_pattern'] == '[N] BTC mined'


def test_frequency_count_deduplicates():
    """Two identical normalized patterns produce frequency=2, not 2 separate suggestions."""
    from interpreters.pattern_suggester import _extract_found_patterns, _cluster_patterns

    dp_rows = [
        {'metric': 'production_btc', 'source_snippet': '1,284 BTC mined', 'report_id': 1},
        {'metric': 'production_btc', 'source_snippet': '2,100 BTC mined', 'report_id': 2},
    ]
    hits = _extract_found_patterns(dp_rows)
    clusters = _cluster_patterns(hits)
    assert len(clusters) == 1
    assert clusters[0]['frequency'] == 2
    assert clusters[0]['report_count'] == 2


# ── Missed-pattern extraction ─────────────────────────────────────────────────

def test_missed_patterns_from_raw_text():
    """Given raw_text with keyword nearby, extract a window."""
    from interpreters.pattern_suggester import _extract_missed_patterns

    db = MagicMock()
    db.get_report_raw_text.return_value = (
        "The company successfully mined 3,450 Bitcoin during the reporting period, "
        "bringing total production to 9,800 BTC year-to-date."
    )
    db.get_all_metric_keywords.return_value = [
        {'metric_key': 'production_btc', 'phrase': 'mined'},
    ]

    rq_rows = [
        {'metric': 'production_btc', 'report_id': 42, 'ticker': 'MARA'}
    ]
    hits = _extract_missed_patterns(db, 'MARA', rq_rows)
    assert len(hits) >= 1
    assert all(h['signal'] == 'missed' for h in hits)
    assert all(h['metric'] == 'production_btc' for h in hits)


# ── Route: GET /api/suggestions/<ticker> ─────────────────────────────────────

def _make_suggestions_app():
    from flask import Flask
    from routes.suggestions import bp
    app = Flask(__name__)
    app.register_blueprint(bp)
    app.config['TESTING'] = True
    return app


def test_suggestions_route_200():
    """GET /api/suggestions/MARA returns 200 with data.suggestions list."""
    app = _make_suggestions_app()
    db = MagicMock()
    db.get_company.return_value = {'ticker': 'MARA', 'name': 'MARA Holdings'}
    db.query_data_points.return_value = [
        {'metric': 'production_btc', 'source_snippet': '1,284 BTC mined', 'report_id': 1},
    ]
    db.get_review_items.return_value = []
    db.get_all_metric_keywords.return_value = []

    with app.test_client() as client:
        with patch('routes.suggestions.get_db', return_value=db):
            resp = client.get('/api/suggestions/MARA')

    assert resp.status_code == 200
    body = resp.get_json()
    assert body['success'] is True
    assert 'suggestions' in body['data']
    assert isinstance(body['data']['suggestions'], list)


def test_suggestions_route_unknown_ticker_404():
    """Unknown ticker returns 404 TICKER_NOT_FOUND."""
    app = _make_suggestions_app()
    db = MagicMock()
    db.get_company.return_value = None

    with app.test_client() as client:
        with patch('routes.suggestions.get_db', return_value=db):
            resp = client.get('/api/suggestions/UNKNOWN')

    assert resp.status_code == 404
    body = resp.get_json()
    assert body['success'] is False
    assert body['error']['code'] == 'TICKER_NOT_FOUND'


# ── Route: POST /api/suggestions/<ticker>/apply ───────────────────────────────

def test_apply_to_ticker_hint():
    """POST apply with target='ticker_hint' calls upsert_ticker_hint."""
    app = _make_suggestions_app()
    db = MagicMock()
    db.get_company.return_value = {'ticker': 'MARA'}
    db.get_ticker_hint.return_value = 'existing hint'
    db.upsert_ticker_hint.return_value = None

    with app.test_client() as client:
        with patch('routes.suggestions.get_db', return_value=db):
            resp = client.post('/api/suggestions/MARA/apply', json={
                'target': 'ticker_hint',
                'append_text': 'Pattern: Bitcoin mined: [N] BTC',
            })

    assert resp.status_code == 200
    body = resp.get_json()
    assert body['success'] is True
    db.upsert_ticker_hint.assert_called_once()
    call_args = db.upsert_ticker_hint.call_args[0]
    assert call_args[0] == 'MARA'
    assert 'Pattern: Bitcoin mined: [N] BTC' in call_args[1]
    assert 'existing hint' in call_args[1]


def test_apply_to_metric_prompt():
    """POST apply with target='metric_prompt' calls upsert_llm_prompt."""
    app = _make_suggestions_app()
    db = MagicMock()
    db.get_company.return_value = {'ticker': 'MARA'}
    db.get_llm_prompt.return_value = {'prompt_text': 'existing prompt'}
    db.upsert_llm_prompt.return_value = None

    with app.test_client() as client:
        with patch('routes.suggestions.get_db', return_value=db), \
             patch('routes.suggestions._get_valid_metrics', return_value=frozenset({'production_btc'})):
            resp = client.post('/api/suggestions/MARA/apply', json={
                'target': 'metric_prompt',
                'metric': 'production_btc',
                'append_text': 'Look for: Bitcoin mined in [Month]: [N] BTC',
            })

    assert resp.status_code == 200
    body = resp.get_json()
    assert body['success'] is True
    db.upsert_llm_prompt.assert_called_once()
    call_args = db.upsert_llm_prompt.call_args[0]
    assert call_args[0] == 'production_btc'
    assert 'Look for: Bitcoin mined in [Month]: [N] BTC' in call_args[1]


def test_cluster_passes_examples_list():
    from interpreters.pattern_suggester import _extract_found_patterns, _cluster_patterns
    dp_rows = [
        {'metric': 'production_btc', 'source_snippet': '1,284 BTC mined', 'report_id': 1},
        {'metric': 'production_btc', 'source_snippet': '2,100 BTC mined', 'report_id': 2},
        {'metric': 'production_btc', 'source_snippet': '980 BTC mined',   'report_id': 3},
    ]
    hits = _extract_found_patterns(dp_rows)
    clusters = _cluster_patterns(hits)
    assert len(clusters) == 1
    c = clusters[0]
    assert 'examples' in c
    assert isinstance(c['examples'], list)
    assert len(c['examples']) <= 5
    assert {'1,284 BTC mined', '2,100 BTC mined', '980 BTC mined'} == set(c['examples'])


def test_suggested_prompt_addition_includes_examples():
    from interpreters.pattern_suggester import _extract_found_patterns, _cluster_patterns
    dp_rows = [
        {'metric': 'production_btc', 'source_snippet': 'we mined 1,284 BTC in March', 'report_id': 1},
        {'metric': 'production_btc', 'source_snippet': 'we mined 2,100 BTC in April', 'report_id': 2},
    ]
    hits = _extract_found_patterns(dp_rows)
    clusters = _cluster_patterns(hits)
    sug = clusters[0]
    assert 'production_btc' in sug['suggested_prompt_addition']
    assert 'mined' in sug['suggested_prompt_addition']
    assert '\n' in sug['suggested_prompt_addition']


def test_apply_metric_prompt_returns_preview():
    app = _make_suggestions_app()
    db = MagicMock()
    db.get_company.return_value = {'ticker': 'MARA'}
    db.get_llm_prompt.return_value = {'prompt_text': 'existing text'}
    db.upsert_llm_prompt.return_value = None
    with app.test_client() as client:
        with patch('routes.suggestions.get_db', return_value=db), \
             patch('routes.suggestions._get_valid_metrics', return_value=frozenset({'production_btc'})):
            resp = client.post('/api/suggestions/MARA/apply', json={
                'target': 'metric_prompt',
                'metric': 'production_btc',
                'append_text': 'Look for patterns like: "mined [N] BTC"',
            })
    assert resp.status_code == 200
    body = resp.get_json()
    assert body['success'] is True
    assert 'new_prompt_preview' in body['data']
    assert 'Look for patterns like' in body['data']['new_prompt_preview']


def test_apply_missing_append_text_400():
    """POST apply with empty append_text returns 400."""
    app = _make_suggestions_app()
    db = MagicMock()
    db.get_company.return_value = {'ticker': 'MARA'}

    with app.test_client() as client:
        with patch('routes.suggestions.get_db', return_value=db):
            resp = client.post('/api/suggestions/MARA/apply', json={
                'target': 'ticker_hint',
                'append_text': '',
            })

    assert resp.status_code == 400


def test_apply_metric_prompt_missing_metric_400():
    """POST apply metric_prompt without metric field returns 400."""
    app = _make_suggestions_app()
    db = MagicMock()
    db.get_company.return_value = {'ticker': 'MARA'}

    with app.test_client() as client:
        with patch('routes.suggestions.get_db', return_value=db):
            resp = client.post('/api/suggestions/MARA/apply', json={
                'target': 'metric_prompt',
                'append_text': 'Some pattern text',
            })

    assert resp.status_code == 400
