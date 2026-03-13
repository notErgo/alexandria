"""
Track C — Period Pipeline Trace: TDD tests.

Tests MUST FAIL before implementation of:
  - compute_cell_state_v2 has_llm_empty_rq param
  - get_coverage_grid using compute_cell_state_v2
  - GET /api/coverage/period_trace endpoint
"""
import pytest
import json


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def db(tmp_path):
    from infra.db import MinerDB
    return MinerDB(str(tmp_path / 'test.db'))


@pytest.fixture
def db_with_company(db):
    db.insert_company({
        'ticker': 'MARA',
        'name': 'MARA Holdings, Inc.',
        'tier': 1,
        'ir_url': 'https://www.marathondh.com/news',
        'pr_base_url': 'https://www.marathondh.com',
        'cik': '0001437491',
        'active': 1,
    })
    return db


@pytest.fixture
def app(tmp_path):
    import sys
    import os
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))
    import app_globals
    from infra.db import MinerDB
    db = MinerDB(str(tmp_path / 'test.db'))
    db.insert_company({
        'ticker': 'MARA',
        'name': 'MARA Holdings, Inc.',
        'tier': 1,
        'ir_url': 'https://www.marathondh.com/news',
        'pr_base_url': 'https://www.marathondh.com',
        'cik': '0001437491',
        'active': 1,
    })
    app_globals._db = db

    import importlib
    import run_web
    importlib.reload(run_web)
    flask_app = run_web.create_app()
    flask_app.config['TESTING'] = True
    return flask_app


@pytest.fixture
def client(app):
    return app.test_client()


def _get_app_db(app):
    """Retrieve the MinerDB instance wired into a Flask test app."""
    import app_globals
    return app_globals._db


# ---------------------------------------------------------------------------
# 1. compute_cell_state_v2 — new has_llm_empty_rq param
# ---------------------------------------------------------------------------

def test_compute_cell_state_v2_llm_empty_state():
    """has_llm_empty_rq=True with no other signals returns 'llm_empty'."""
    from coverage_logic import compute_cell_state_v2
    state = compute_cell_state_v2(
        is_analyst_gap=False,
        has_data_point=False,
        has_review_pending=False,
        has_manifest=True,
        has_parse_error=False,
        has_extract_error=False,
        has_scraper_error=False,
        has_llm_empty_rq=True,
    )
    assert state == 'llm_empty'


def test_compute_cell_state_v2_llm_empty_lower_priority_than_review_pending():
    """has_llm_empty_rq=True but has_review_pending=True returns 'review_pending'."""
    from coverage_logic import compute_cell_state_v2
    state = compute_cell_state_v2(
        is_analyst_gap=False,
        has_data_point=False,
        has_review_pending=True,
        has_manifest=True,
        has_parse_error=False,
        has_extract_error=False,
        has_scraper_error=False,
        has_llm_empty_rq=True,
    )
    assert state == 'review_pending'


def test_compute_cell_state_v2_llm_empty_lower_priority_than_data():
    """has_llm_empty_rq=True but has_data_point=True returns 'data'."""
    from coverage_logic import compute_cell_state_v2
    state = compute_cell_state_v2(
        is_analyst_gap=False,
        has_data_point=True,
        has_review_pending=False,
        has_manifest=True,
        has_parse_error=False,
        has_extract_error=False,
        has_scraper_error=False,
        has_llm_empty_rq=True,
    )
    assert state == 'data'


# ---------------------------------------------------------------------------
# 4. get_coverage_grid calls compute_cell_state_v2
# ---------------------------------------------------------------------------

def test_get_coverage_grid_uses_v2_state_function(db_with_company, monkeypatch):
    """get_coverage_grid must call compute_cell_state_v2 (not compute_cell_state)."""
    import coverage_logic

    call_count = {'n': 0}
    original_v2 = coverage_logic.compute_cell_state_v2

    def spy_v2(*args, **kwargs):
        call_count['n'] += 1
        return original_v2(*args, **kwargs)

    monkeypatch.setattr(coverage_logic, 'compute_cell_state_v2', spy_v2)

    db_with_company.get_coverage_grid(months=1)

    assert call_count['n'] > 0, (
        "get_coverage_grid did not call compute_cell_state_v2; "
        "it must be updated to use the v2 function"
    )


# ---------------------------------------------------------------------------
# 5. get_coverage_grid reflects llm_empty cell state
# ---------------------------------------------------------------------------

def test_get_coverage_grid_llm_empty_cell_state(db_with_company):
    """Coverage grid returns 'llm_empty' for a period with only an LLM_EMPTY review item."""
    from datetime import date

    period = date.today().replace(day=1).strftime('%Y-%m-01')

    with db_with_company._get_connection() as conn:
        conn.execute(
            """INSERT INTO reports
               (ticker, report_date, source_type, source_url, raw_text, extraction_status)
               VALUES (?, ?, 'ir_press_release', 'https://example.com/pr', 'some text', 'done')""",
            ('MARA', period),
        )
        report_id = conn.execute(
            "SELECT id FROM reports WHERE ticker='MARA' AND report_date=?", (period,)
        ).fetchone()[0]

        conn.execute(
            """INSERT INTO review_queue
               (ticker, period, metric, raw_value, llm_value, confidence, status, agreement_status)
               VALUES (?, ?, 'production_btc', '', NULL, 0.0, 'PENDING', 'LLM_EMPTY')""",
            ('MARA', period),
        )

    grid = db_with_company.get_coverage_grid(months=1)

    assert 'MARA' in grid, "MARA must appear in the coverage grid"
    assert period in grid['MARA'], f"Period {period} must appear in MARA grid"
    cell = grid['MARA'][period]
    assert cell.get('state') == 'llm_empty', (
        f"Expected cell state 'llm_empty', got {cell.get('state')!r}"
    )


# ---------------------------------------------------------------------------
# 6. /api/coverage/period_trace — no manifest, no report
# ---------------------------------------------------------------------------

def test_period_trace_endpoint_no_manifest(client):
    """Period trace for a period with no manifest and no report returns correct flags."""
    resp = client.get('/api/coverage/period_trace?ticker=MARA&period=2024-01-01')
    assert resp.status_code == 200
    data = resp.get_json()
    assert data['success'] is True
    body = data['data']
    assert body['ticker'] == 'MARA'
    assert body['period'] == '2024-01-01'
    assert body['has_manifest'] is False
    assert body['has_raw_text'] is False


# ---------------------------------------------------------------------------
# 7. keyword_gated derivation
# ---------------------------------------------------------------------------

def test_period_trace_endpoint_keyword_gated(client, app):
    """Report extracted with no data or review items is flagged keyword_gated=True."""
    db = _get_app_db(app)
    with db._get_connection() as conn:
        conn.execute(
            """INSERT INTO reports
               (ticker, report_date, source_type, source_url, raw_text, extraction_status)
               VALUES ('MARA', '2024-02-01', 'ir_press_release',
                       'https://example.com/pr2', 'short text', 'done')"""
        )

    resp = client.get('/api/coverage/period_trace?ticker=MARA&period=2024-02-01')
    assert resp.status_code == 200
    body = resp.get_json()['data']
    assert body['keyword_gated'] is True


# ---------------------------------------------------------------------------
# 8. llm_empty via endpoint
# ---------------------------------------------------------------------------

def test_period_trace_endpoint_llm_empty(client, app):
    """Period with LLM_EMPTY review item returns has_llm_empty_rq=True and cell_state='llm_empty'."""
    db = _get_app_db(app)
    with db._get_connection() as conn:
        conn.execute(
            """INSERT INTO reports
               (ticker, report_date, source_type, source_url, raw_text, extraction_status)
               VALUES ('MARA', '2024-03-01', 'ir_press_release',
                       'https://example.com/pr3', 'bitcoin miner text here', 'done')"""
        )
        conn.execute(
            """INSERT INTO review_queue
               (ticker, period, metric, raw_value, llm_value, confidence, status, agreement_status)
               VALUES ('MARA', '2024-03-01', 'production_btc', '', NULL, 0.0, 'PENDING', 'LLM_EMPTY')"""
        )

    resp = client.get('/api/coverage/period_trace?ticker=MARA&period=2024-03-01')
    assert resp.status_code == 200
    body = resp.get_json()['data']
    assert body['has_llm_empty_rq'] is True
    assert body['cell_state'] == 'llm_empty'


# ---------------------------------------------------------------------------
# 9. has_data_point via endpoint
# ---------------------------------------------------------------------------

def test_period_trace_endpoint_has_data(client, app):
    """Period with a data_point returns has_data_point=True and cell_state='data'."""
    db = _get_app_db(app)
    with db._get_connection() as conn:
        conn.execute(
            """INSERT INTO reports
               (ticker, report_date, source_type, source_url, raw_text, extraction_status)
               VALUES ('MARA', '2024-04-01', 'ir_press_release',
                       'https://example.com/pr4', 'bitcoin mined 500 BTC', 'done')"""
        )
        conn.execute(
            """INSERT INTO data_points
               (ticker, period, metric, value, unit, confidence, extraction_method)
               VALUES ('MARA', '2024-04-01', 'production_btc', 500.0, 'BTC', 0.9, 'llm')"""
        )

    resp = client.get('/api/coverage/period_trace?ticker=MARA&period=2024-04-01')
    assert resp.status_code == 200
    body = resp.get_json()['data']
    assert body['has_data_point'] is True
    assert body['cell_state'] == 'data'


# ---------------------------------------------------------------------------
# 10. keyword_gated requires extraction_status='done'
# ---------------------------------------------------------------------------

def test_period_trace_derives_keyword_gated_correctly(client, app):
    """extraction_status='pending' should NOT be classified as keyword_gated."""
    db = _get_app_db(app)
    with db._get_connection() as conn:
        conn.execute(
            """INSERT INTO reports
               (ticker, report_date, source_type, source_url, raw_text, extraction_status)
               VALUES ('MARA', '2024-05-01', 'ir_press_release',
                       'https://example.com/pr5', 'some text content', 'pending')"""
        )

    resp = client.get('/api/coverage/period_trace?ticker=MARA&period=2024-05-01')
    assert resp.status_code == 200
    body = resp.get_json()['data']
    assert body['keyword_gated'] is False


# ---------------------------------------------------------------------------
# 11. per_metric breakdown
# ---------------------------------------------------------------------------

def test_period_trace_per_metric_breakdown(client, app):
    """per_metric dict contains correct data_point and review_item entries per metric."""
    db = _get_app_db(app)
    with db._get_connection() as conn:
        conn.execute(
            """INSERT INTO reports
               (ticker, report_date, source_type, source_url, raw_text, extraction_status)
               VALUES ('MARA', '2024-06-01', 'ir_press_release',
                       'https://example.com/pr6', 'bitcoin mined 600 BTC held 1200', 'done')"""
        )
        conn.execute(
            """INSERT INTO data_points
               (ticker, period, metric, value, unit, confidence, extraction_method)
               VALUES ('MARA', '2024-06-01', 'production_btc', 600.0, 'BTC', 0.92, 'llm')"""
        )
        conn.execute(
            """INSERT INTO review_queue
               (ticker, period, metric, raw_value, llm_value, confidence, status, agreement_status)
               VALUES ('MARA', '2024-06-01', 'holdings_btc', '', NULL, 0.0, 'PENDING', 'LLM_EMPTY')"""
        )

    resp = client.get(
        '/api/coverage/period_trace?ticker=MARA&period=2024-06-01'
    )
    assert resp.status_code == 200
    body = resp.get_json()['data']

    assert 'per_metric' in body
    per_metric = body['per_metric']

    assert 'production_btc' in per_metric
    assert per_metric['production_btc']['data_point'] is not None
    assert per_metric['production_btc']['review_item'] is None

    assert 'holdings_btc' in per_metric
    assert per_metric['holdings_btc']['data_point'] is None
    rq = per_metric['holdings_btc']['review_item']
    assert rq is not None
    assert rq['agreement_status'] == 'LLM_EMPTY'
    assert rq['status'] == 'PENDING'


# ---------------------------------------------------------------------------
# 12. Backwards compatibility — existing v1 states still work in v2
# ---------------------------------------------------------------------------

def test_coverage_grid_v2_backwards_compat_with_v1_states(db_with_company):
    """Existing v2 states ('data', 'no_document') still function after the llm_empty extension."""
    from coverage_logic import compute_cell_state_v2
    from datetime import date

    # 'data' state
    data_state = compute_cell_state_v2(
        is_analyst_gap=False,
        has_data_point=True,
        has_review_pending=False,
        has_manifest=True,
        has_parse_error=False,
        has_extract_error=False,
        has_scraper_error=False,
        has_llm_empty_rq=False,
    )
    assert data_state == 'data', f"Expected 'data', got {data_state!r}"

    # 'no_document' state
    no_doc_state = compute_cell_state_v2(
        is_analyst_gap=False,
        has_data_point=False,
        has_review_pending=False,
        has_manifest=False,
        has_parse_error=False,
        has_extract_error=False,
        has_scraper_error=False,
        has_llm_empty_rq=False,
    )
    assert no_doc_state == 'no_document', f"Expected 'no_document', got {no_doc_state!r}"

    # 'review_pending' state still unaffected when llm_empty=False
    review_state = compute_cell_state_v2(
        is_analyst_gap=False,
        has_data_point=False,
        has_review_pending=True,
        has_manifest=True,
        has_parse_error=False,
        has_extract_error=False,
        has_scraper_error=False,
        has_llm_empty_rq=False,
    )
    assert review_state == 'review_pending', f"Expected 'review_pending', got {review_state!r}"
