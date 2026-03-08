"""
Operations panel API route tests — TDD.

Tests should FAIL before routes/operations.py is created.
"""
import pytest
import json
from infra.db import MinerDB


@pytest.fixture
def app(tmp_path):
    import sys, os
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))
    import app_globals
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

    import importlib, run_web
    importlib.reload(run_web)
    flask_app = run_web.create_app()
    flask_app.config['TESTING'] = True
    return flask_app


@pytest.fixture
def client(app):
    return app.test_client()


# ── Operations queue ──────────────────────────────────────────────────────────

def test_operations_queue_returns_200(client):
    """GET /api/operations/queue returns 200 with expected structure."""
    resp = client.get('/api/operations/queue')
    assert resp.status_code == 200
    data = resp.get_json()
    assert data['success'] is True
    queue = data['data']
    assert 'pending_extraction' in queue
    assert 'legacy_files' in queue


def test_pipeline_observability_includes_counts_and_config_health(client):
    """GET /api/operations/pipeline_observability returns global + ticker metrics."""
    import app_globals
    db = app_globals.get_db()

    db.update_company_config('MARA', scraper_mode='rss', rss_url=None)
    db.upsert_asset_manifest({
        'ticker': 'MARA',
        'period': '2025-01-01',
        'source_type': 'ir_press_release',
        'file_path': '/tmp/mara_2025_01.html',
        'filename': 'mara_2025_01.html',
        'ingest_state': 'pending',
    })
    report_id = db.insert_report({
        'ticker': 'MARA',
        'report_date': '2025-01-01',
        'published_date': None,
        'source_type': 'ir_press_release',
        'source_url': 'https://example.test/mara',
        'raw_text': 'MARA mined 100 BTC',
        'parsed_at': '2025-02-01T00:00:00',
    })
    db.mark_report_extracted(report_id)
    db.insert_data_point({
        'report_id': report_id,
        'ticker': 'MARA',
        'period': '2025-01-01',
        'metric': 'production_btc',
        'value': 100.0,
        'unit': 'BTC',
        'confidence': 0.9,
        'extraction_method': 'regex',
        'source_snippet': 'mined 100 BTC',
    })
    db.insert_review_item({
        'data_point_id': None,
        'ticker': 'MARA',
        'period': '2025-01-01',
        'metric': 'hodl_btc',
        'raw_value': '5000',
        'confidence': 0.6,
        'source_snippet': 'holdings',
        'status': 'PENDING',
    })

    resp = client.get('/api/operations/pipeline_observability')
    assert resp.status_code == 200
    data = resp.get_json()
    assert data['success'] is True
    snap = data['data']

    assert snap['totals']['companies_total'] >= 1
    assert snap['totals']['manifest_total'] >= 1
    assert snap['totals']['reports_total'] >= 1
    assert snap['totals']['reports_extracted'] >= 1
    assert snap['totals']['data_points_total'] >= 1
    assert snap['totals']['review_pending'] >= 1
    assert 'manifest_ingest_state' in snap['by_state']
    assert 'reports_source_type' in snap['by_state']
    assert snap['scraper_config']['invalid_count'] >= 1
    assert any(r['ticker'] == 'MARA' for r in snap['tickers'])


# ── Operations extract ────────────────────────────────────────────────────────

def test_extract_missing_ticker_runs_all(client):
    """POST /api/operations/interpret with no ticker starts ALL extraction run."""
    resp = client.post('/api/operations/interpret', json={})
    assert resp.status_code in (200, 409)
    if resp.status_code == 200:
        data = resp.get_json()
        assert data['success'] is True
        assert data['data']['ticker'] == 'ALL'
        assert 'task_id' in data['data']


def test_extract_returns_task_id(client, monkeypatch):
    """POST /api/operations/interpret with valid ticker returns task_id."""
    import routes.operations as ops_mod
    # Mock: prevent real extraction thread from starting
    monkeypatch.setattr(ops_mod, '_active_tickers', set())

    resp = client.post('/api/operations/interpret', json={'ticker': 'MARA'})
    # Either 200 (task started) or 409 (already running)
    assert resp.status_code in (200, 409)
    if resp.status_code == 200:
        data = resp.get_json()
        assert data['success'] is True
        assert 'task_id' in data['data']


# ── Operations assign_period ──────────────────────────────────────────────────

def test_assign_period_invalid_format_returns_400(client):
    """POST /api/operations/assign_period with bad period format returns 400."""
    resp = client.post('/api/operations/assign_period', json={
        'manifest_id': 1,
        'period': '2024-01',  # wrong format (missing -01)
    })
    assert resp.status_code == 400
    data = resp.get_json()
    assert data['success'] is False


def test_assign_period_missing_manifest_id_returns_400(client):
    """POST /api/operations/assign_period with no manifest_id returns 400."""
    resp = client.post('/api/operations/assign_period', json={
        'period': '2024-01-01',
    })
    assert resp.status_code == 400


def test_assign_period_valid(client, tmp_path):
    """POST /api/operations/assign_period with valid data updates manifest."""
    import app_globals
    db = app_globals.get_db()
    manifest_id = db.upsert_asset_manifest({
        'ticker': 'MARA',
        'period': None,
        'source_type': 'archive_html',
        'file_path': '/tmp/test_undated.html',
        'filename': 'test_undated.html',
        'ingest_state': 'legacy_undated',
    })
    resp = client.post('/api/operations/assign_period', json={
        'manifest_id': manifest_id,
        'period': '2024-06-01',
    })
    assert resp.status_code == 200
    data = resp.get_json()
    assert data['success'] is True
    assert data['data']['period'] == '2024-06-01'


# ── Operations progress ───────────────────────────────────────────────────────

def test_progress_unknown_task_returns_404(client):
    """GET /api/operations/interpret/<task_id>/progress for unknown task returns 404."""
    resp = client.get('/api/operations/interpret/nonexistent-task-id/progress')
    assert resp.status_code == 404
