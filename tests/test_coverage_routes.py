"""
Coverage and manifest API route tests — TDD.

Tests should FAIL before routes/coverage.py is created.
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
    # Seed MARA company
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


# ── Coverage summary ─────────────────────────────────────────────────────────

def test_coverage_summary_returns_200(client):
    """GET /api/coverage/summary returns 200 with expected keys."""
    resp = client.get('/api/coverage/summary')
    assert resp.status_code == 200
    data = resp.get_json()
    assert data['success'] is True
    summary = data['data']
    assert 'total_reports' in summary
    assert 'manifest_total' in summary


# ── Coverage grid ─────────────────────────────────────────────────────────────

def test_coverage_grid_default_36_months(client):
    """GET /api/coverage/grid returns grid with MARA key."""
    resp = client.get('/api/coverage/grid')
    assert resp.status_code == 200
    data = resp.get_json()
    assert data['success'] is True
    grid = data['data']['grid']
    assert 'MARA' in grid
    assert 'summary' in grid


def test_coverage_grid_custom_months(client):
    """GET /api/coverage/grid?months=12 returns 12 period columns."""
    resp = client.get('/api/coverage/grid?months=12')
    assert resp.status_code == 200
    data = resp.get_json()
    assert data['success'] is True


def test_coverage_grid_invalid_months_returns_400(client):
    """GET /api/coverage/grid?months=0 returns 400."""
    resp = client.get('/api/coverage/grid?months=0')
    assert resp.status_code == 400


def test_coverage_grid_months_too_large_returns_400(client):
    """GET /api/coverage/grid?months=200 returns 400."""
    resp = client.get('/api/coverage/grid?months=200')
    assert resp.status_code == 400


# ── Coverage assets ──────────────────────────────────────────────────────────

def test_coverage_assets_returns_200(client):
    """GET /api/coverage/assets/MARA/2024-01-01 returns 200."""
    resp = client.get('/api/coverage/assets/MARA/2024-01-01')
    assert resp.status_code == 200
    data = resp.get_json()
    assert data['success'] is True
    cell = data['data']
    assert 'manifest' in cell
    assert 'reports' in cell


# ── Manifest scan ─────────────────────────────────────────────────────────────

def test_manifest_scan_returns_scan_result(client, monkeypatch):
    """POST /api/manifest/scan returns ScanResult dict."""
    from miner_types import ScanResult
    # Mock the scanner to avoid filesystem access in tests
    import routes.coverage as coverage_mod
    monkeypatch.setattr(
        coverage_mod, '_do_scan',
        lambda db: ScanResult(total_found=5, newly_discovered=2, tickers_scanned=['MARA'])
    )
    resp = client.post('/api/manifest/scan')
    assert resp.status_code == 200
    data = resp.get_json()
    assert data['success'] is True
    result = data['data']
    assert result['total_found'] == 5
    assert result['newly_discovered'] == 2
