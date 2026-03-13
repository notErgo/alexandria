"""Tests for metric_examples feature — Step 1 TDD (all should fail before implementation).

Covers:
- DB layer: add/get/update/delete/filter/cap examples
- Snippet analyzer: table row detection, normalization, prose n-grams, filtering
- Route layer: CRUD on /api/metric_schema/<key>/examples and snippet_analysis endpoint
- Prompt injection: examples block in batch and quarterly prompts
"""
import pytest
import json
import sys
import os
import importlib
import requests


# ── DB-layer fixtures ──────────────────────────────────────────────────────────

@pytest.fixture
def db_with_metric(db):
    """DB with production_btc metric row seeded (comes from MinerDB init)."""
    rows = db.get_metric_schema('BTC-miners', active_only=False)
    assert any(r['key'] == 'production_btc' for r in rows), \
        "production_btc must be seeded by MinerDB init"
    return db


# ── DB-layer tests ─────────────────────────────────────────────────────────────

def test_add_and_get_metric_example(db_with_metric):
    db = db_with_metric
    eid = db.add_metric_example('production_btc', 'BTC Produced | 713')
    assert isinstance(eid, int) and eid > 0
    rows = db.get_metric_examples('production_btc')
    assert len(rows) == 1
    assert rows[0]['snippet'] == 'BTC Produced | 713'
    assert rows[0]['metric_key'] == 'production_btc'
    assert rows[0]['ticker'] is None
    assert rows[0]['active'] == 1


def test_get_examples_active_only_default(db_with_metric):
    db = db_with_metric
    db.add_metric_example('production_btc', 'Active snippet')
    eid2 = db.add_metric_example('production_btc', 'Inactive snippet')
    db.update_metric_example(eid2, active=0)
    rows = db.get_metric_examples('production_btc', active_only=True)
    snippets = [r['snippet'] for r in rows]
    assert 'Active snippet' in snippets
    assert 'Inactive snippet' not in snippets


def test_get_examples_ticker_filter_includes_metric_wide(db_with_metric):
    db = db_with_metric
    db.add_metric_example('production_btc', 'Global snippet')               # ticker=None
    db.add_metric_example('production_btc', 'MARA snippet', ticker='MARA')  # MARA-scoped
    db.add_metric_example('production_btc', 'RIOT snippet', ticker='RIOT')  # RIOT-scoped
    rows = db.get_metric_examples('production_btc', ticker='MARA')
    snippets = [r['snippet'] for r in rows]
    assert 'Global snippet' in snippets
    assert 'MARA snippet' in snippets
    assert 'RIOT snippet' not in snippets


def test_get_examples_ticker_RIOT_excludes_MARA_scoped(db_with_metric):
    db = db_with_metric
    db.add_metric_example('production_btc', 'MARA only', ticker='MARA')
    rows = db.get_metric_examples('production_btc', ticker='RIOT')
    snippets = [r['snippet'] for r in rows]
    assert 'MARA only' not in snippets


def test_update_metric_example_label(db_with_metric):
    db = db_with_metric
    eid = db.add_metric_example('production_btc', 'Some snippet')
    result = db.update_metric_example(eid, label='My label')
    assert result is True
    rows = db.get_metric_examples('production_btc', active_only=False)
    row = next(r for r in rows if r['id'] == eid)
    assert row['label'] == 'My label'


def test_delete_metric_example(db_with_metric):
    db = db_with_metric
    eid = db.add_metric_example('production_btc', 'To delete')
    deleted = db.delete_metric_example(eid)
    assert deleted is True
    rows = db.get_metric_examples('production_btc')
    assert not any(r['id'] == eid for r in rows)


def test_delete_metric_example_nonexistent(db_with_metric):
    result = db_with_metric.delete_metric_example(99999)
    assert result is False


def _insert_report_and_dp(db, ticker, period, metric, value, snippet):
    """Helper: insert a report + data_point with source_snippet for tests."""
    # Ensure company exists
    try:
        db.insert_company({'ticker': ticker, 'name': f'{ticker} Test', 'tier': 1,
                           'ir_url': '', 'pr_base_url': '', 'cik': None, 'active': 1})
    except Exception:
        pass
    with db._get_connection() as conn:
        cur = conn.execute(
            "INSERT INTO reports (ticker, report_date, source_type, source_url, raw_text) "
            "VALUES (?, ?, 'archive_pdf', 'http://example.com', 'text')",
            (ticker, period),
        )
        report_id = cur.lastrowid
        conn.execute(
            "INSERT INTO data_points (report_id, ticker, period, metric, value, unit, "
            "confidence, extraction_method, source_snippet) "
            "VALUES (?, ?, ?, ?, ?, 'BTC', 0.9, 'llm', ?)",
            (report_id, ticker, period, metric, value, snippet),
        )


def test_get_snippets_for_metric_filters_by_metric(db_with_metric):
    db = db_with_metric
    _insert_report_and_dp(db, 'MARA', '2024-01', 'production_btc', 713.0, 'BTC Produced | 713')
    _insert_report_and_dp(db, 'MARA', '2024-01', 'hashrate_eh', 28.5, 'Hash rate 28.5 EH/s')
    snippets = db.get_snippets_for_metric('production_btc')
    assert 'BTC Produced | 713' in snippets
    assert 'Hash rate 28.5 EH/s' not in snippets


def test_get_snippets_for_metric_filters_by_ticker(db_with_metric):
    db = db_with_metric
    _insert_report_and_dp(db, 'MARA', '2024-01', 'production_btc', 713.0, 'MARA BTC | 713')
    _insert_report_and_dp(db, 'RIOT', '2024-01', 'production_btc', 500.0, 'RIOT BTC | 500')
    mara_snips = db.get_snippets_for_metric('production_btc', ticker='MARA')
    assert 'MARA BTC | 713' in mara_snips
    assert 'RIOT BTC | 500' not in mara_snips


def test_get_active_examples_for_prompt_caps_at_five(db_with_metric):
    db = db_with_metric
    for i in range(7):
        db.add_metric_example('production_btc', f'Snippet number {i}')
    results = db.get_active_examples_for_prompt('production_btc')
    assert len(results) == 5


def test_add_metric_example_raises_for_unknown_metric(db_with_metric):
    with pytest.raises(ValueError):
        db_with_metric.add_metric_example('nonexistent_metric_xyz', 'some snippet')


# ── Snippet analyzer tests ─────────────────────────────────────────────────────

def test_analyze_pipe_table_rows_detected():
    from interpreters.snippet_analyzer import analyze_snippets
    snippets = [
        '| BTC Produced | 713 |',
        '| BTC Produced | 850 |',
        '| BTC Produced | 692 |',
    ]
    result = analyze_snippets(snippets)
    assert result['total_snippets'] == 3
    table_rows = result['table_rows']
    assert len(table_rows) > 0
    # All three normalize to the same template
    templates = [row['template'] for row in table_rows]
    assert any('X' in t for t in templates)


def test_numeric_values_normalized_to_X():
    from interpreters.snippet_analyzer import analyze_snippets
    snippets = [
        'Bitcoin mined in October: 713 BTC',
        'Bitcoin mined in November: 850 BTC',
    ]
    result = analyze_snippets(snippets)
    # Should detect at least one pattern with frequency >= 2 after normalization
    all_items = result['table_rows'] + result['prose_ngrams']
    assert any(item['frequency'] >= 2 for item in all_items)


def test_prose_ngrams_extracted():
    from interpreters.snippet_analyzer import analyze_snippets
    snippets = [
        'The company mined bitcoin during the month',
        'The company mined bitcoin last month',
        'The company mined bitcoin in October',
    ]
    result = analyze_snippets(snippets)
    ngrams = result['prose_ngrams']
    assert len(ngrams) > 0
    # "The company mined bitcoin" is a 4-gram appearing 3 times
    assert any(item['frequency'] >= 2 for item in ngrams)


def test_empty_input_returns_empty_results():
    from interpreters.snippet_analyzer import analyze_snippets
    result = analyze_snippets([])
    assert result['total_snippets'] == 0
    assert result['unique_snippets'] == 0
    assert result['table_rows'] == []
    assert result['prose_ngrams'] == []


def test_top_n_table_rows_bounded_by_MAX():
    from interpreters.snippet_analyzer import analyze_snippets
    # Create 15 distinct table templates each appearing twice
    snippets = []
    for i in range(15):
        for _ in range(2):
            snippets.append(f'| Metric {i} | {100 + i} |')
    result = analyze_snippets(snippets)
    assert len(result['table_rows']) <= 10


def test_below_min_frequency_filtered_out():
    from interpreters.snippet_analyzer import analyze_snippets
    snippets = [
        '| BTC Produced | 713 |',  # unique
        'Something completely different',
    ]
    result = analyze_snippets(snippets)
    # Single-occurrence table rows should be excluded (frequency < 2)
    for row in result['table_rows']:
        assert row['frequency'] >= 2


# ── Flask app fixture for route tests ─────────────────────────────────────────

@pytest.fixture
def app(tmp_path):
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))
    import app_globals
    from infra.db import MinerDB
    db = MinerDB(str(tmp_path / 'test.db'))
    app_globals._db = db

    import run_web
    importlib.reload(run_web)
    flask_app = run_web.create_app()
    flask_app.config['TESTING'] = True
    return flask_app


@pytest.fixture
def client(app):
    return app.test_client()


# ── Route tests ────────────────────────────────────────────────────────────────

def test_snippet_analysis_200_known_metric(client):
    resp = client.get('/api/metric_schema/production_btc/snippet_analysis')
    assert resp.status_code == 200
    data = resp.get_json()
    assert data['success'] is True
    assert 'table_rows' in data['data']
    assert 'prose_ngrams' in data['data']


def test_snippet_analysis_404_unknown_metric(client):
    resp = client.get('/api/metric_schema/no_such_metric/snippet_analysis')
    assert resp.status_code == 404


def test_list_examples_empty(client):
    resp = client.get('/api/metric_schema/production_btc/examples')
    assert resp.status_code == 200
    data = resp.get_json()
    assert data['success'] is True
    assert data['data'] == []


def test_add_example_201(client):
    resp = client.post(
        '/api/metric_schema/production_btc/examples',
        json={'snippet': 'BTC Produced | 713', 'ticker': 'MARA'},
    )
    assert resp.status_code == 201
    data = resp.get_json()
    assert data['success'] is True
    assert 'id' in data['data']


def test_add_example_missing_snippet_400(client):
    resp = client.post(
        '/api/metric_schema/production_btc/examples',
        json={'ticker': 'MARA'},
    )
    assert resp.status_code == 400


def test_add_example_unknown_metric_404(client):
    resp = client.post(
        '/api/metric_schema/no_such_metric/examples',
        json={'snippet': 'some text'},
    )
    assert resp.status_code == 404


def test_patch_example_label(client):
    create_resp = client.post(
        '/api/metric_schema/production_btc/examples',
        json={'snippet': 'A snippet'},
    )
    eid = create_resp.get_json()['data']['id']
    patch_resp = client.patch(
        f'/api/metric_schema/production_btc/examples/{eid}',
        json={'label': 'My label'},
    )
    assert patch_resp.status_code == 200
    assert patch_resp.get_json()['success'] is True


def test_delete_example_200(client):
    create_resp = client.post(
        '/api/metric_schema/production_btc/examples',
        json={'snippet': 'Delete me'},
    )
    eid = create_resp.get_json()['data']['id']
    del_resp = client.delete(f'/api/metric_schema/production_btc/examples/{eid}')
    assert del_resp.status_code == 200
    assert del_resp.get_json()['success'] is True


def test_delete_example_unknown_id_404(client):
    resp = client.delete('/api/metric_schema/production_btc/examples/99999')
    assert resp.status_code == 404


# ── Prompt injection tests ─────────────────────────────────────────────────────

def test_build_batch_prompt_includes_example_patterns_block(db_with_metric):
    db = db_with_metric
    db.add_metric_example('production_btc', 'BTC Produced | 713', ticker='MARA')
    from interpreters.llm_interpreter import LLMInterpreter
    session = requests.Session()
    interp = LLMInterpreter(session=session, db=db)
    prompt = interp._build_batch_prompt(
        text='Some document text', metrics=['production_btc'], ticker='MARA'
    )
    assert '=== EXAMPLE PATTERNS ===' in prompt
    assert 'BTC Produced | 713' in prompt


def test_build_batch_prompt_omits_block_when_no_examples(db_with_metric):
    db = db_with_metric
    from interpreters.llm_interpreter import LLMInterpreter
    session = requests.Session()
    interp = LLMInterpreter(session=session, db=db)
    prompt = interp._build_batch_prompt(
        text='Some document text', metrics=['production_btc'], ticker='MARA'
    )
    assert '=== EXAMPLE PATTERNS ===' not in prompt


def test_build_quarterly_batch_prompt_includes_examples(db_with_metric):
    db = db_with_metric
    db.add_metric_example('production_btc', 'Quarterly BTC | 2100', ticker='RIOT')
    from interpreters.llm_interpreter import LLMInterpreter
    session = requests.Session()
    interp = LLMInterpreter(session=session, db=db)
    prompt = interp._build_quarterly_batch_prompt(
        text='Some quarterly report text', metrics=['production_btc'], ticker='RIOT'
    )
    assert '=== EXAMPLE PATTERNS ===' in prompt
    assert 'Quarterly BTC | 2100' in prompt


def test_build_batch_prompt_survives_db_failure_on_examples(db_with_metric):
    """Exception in examples fetch is swallowed — prompt still builds."""
    db = db_with_metric
    from interpreters.llm_interpreter import LLMInterpreter
    session = requests.Session()
    interp = LLMInterpreter(session=session, db=db)

    original = db.get_active_examples_for_prompt

    def boom(metric_key, ticker=None):
        raise RuntimeError("DB exploded")

    db.get_active_examples_for_prompt = boom
    try:
        prompt = interp._build_batch_prompt(
            text='text', metrics=['production_btc'], ticker='MARA'
        )
        assert 'production_btc' in prompt
    finally:
        db.get_active_examples_for_prompt = original
