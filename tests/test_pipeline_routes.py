"""Route tests for overnight pipeline orchestration APIs."""

import importlib
import json
import os
import sys

import pytest

from infra.db import MinerDB


@pytest.fixture
def app(tmp_path):
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))
    import app_globals
    import run_web

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

    importlib.reload(run_web)
    flask_app = run_web.create_app()
    flask_app.config['TESTING'] = True
    return flask_app


@pytest.fixture
def client(app):
    return app.test_client()


def test_overnight_start_returns_202_and_run_id(client, monkeypatch):
    import routes.pipeline as pipeline_mod

    class _DummyThread:
        def __init__(self, target=None, args=(), daemon=False, name=None):
            self.target = target
            self.args = args
            self.daemon = daemon
            self.name = name

        def start(self):
            return None

    monkeypatch.setattr(pipeline_mod.threading, 'Thread', _DummyThread)

    resp = client.post('/api/pipeline/overnight/start', json={
        'tickers': ['MARA'],
        'apply_mode_changes': False,
    })
    assert resp.status_code == 202
    payload = resp.get_json()['data']
    assert isinstance(payload['run_id'], int)
    assert payload['status'] == 'queued'


def test_overnight_start_rejects_invalid_tickers_type(client):
    resp = client.post('/api/pipeline/overnight/start', json={'tickers': 'MARA'})
    assert resp.status_code == 400
    assert "'tickers' must be a list" in resp.get_json()['error']['message']


def test_overnight_start_accepts_scout_config(client, monkeypatch):
    import app_globals
    import routes.pipeline as pipeline_mod

    class _DummyThread:
        def __init__(self, target=None, args=(), daemon=False, name=None):
            self.target = target
            self.args = args
            self.daemon = daemon
            self.name = name

        def start(self):
            return None

    monkeypatch.setattr(pipeline_mod.threading, 'Thread', _DummyThread)

    resp = client.post('/api/pipeline/overnight/start', json={
        'tickers': ['MARA'],
        'scout_mode': 'auto',
        'scout_metric': 'production_btc',
        'scout_keywords': ['miner', 'bitcoin', 'production'],
        'scout_max_age_hours': 24,
        'require_scout_success': False,
    })
    assert resp.status_code == 202
    run_id = int(resp.get_json()['data']['run_id'])
    run = app_globals.get_db().get_pipeline_run(run_id)
    cfg_raw = run.get('config_json') or "{}"
    cfg = json.loads(cfg_raw) if isinstance(cfg_raw, str) else cfg_raw
    assert cfg.get('scout_mode') == 'auto'
    assert cfg.get('scout_metric') == 'production_btc'
    assert cfg.get('scout_keywords') == ['miner', 'bitcoin', 'production']
    assert int(cfg.get('scout_max_age_hours')) == 24


def test_overnight_status_404_for_missing_run(client):
    resp = client.get('/api/pipeline/overnight/999999/status')
    assert resp.status_code == 404


def test_overnight_events_404_for_missing_run(client):
    resp = client.get('/api/pipeline/overnight/999999/events')
    assert resp.status_code == 404


def test_overnight_events_rejects_invalid_limit(client):
    import app_globals

    db = app_globals.get_db()
    run = db.create_pipeline_run(triggered_by='test', scope={'tickers': []}, config={})
    run_id = int(run['id'])

    resp = client.get(f'/api/pipeline/overnight/{run_id}/events?limit=bad')
    assert resp.status_code == 400
    assert 'limit must be an integer' in resp.get_json()['error']['message']


def test_get_last_successful_pipeline_run_returns_none_when_no_runs(tmp_path):
    db = MinerDB(str(tmp_path / 'test.db'))
    result = db.get_last_successful_pipeline_run(source='edgar', ticker=None)
    assert result is None


def test_get_last_successful_pipeline_run_returns_completed_run(tmp_path):
    db = MinerDB(str(tmp_path / 'test.db'))
    run = db.create_pipeline_run(triggered_by='test', scope={}, config={})
    db.update_pipeline_run(run['id'], status='complete')
    result = db.get_last_successful_pipeline_run(source='edgar', ticker=None)
    assert result is not None
    assert result['id'] == run['id']


def test_get_last_successful_pipeline_run_ignores_failed_runs(tmp_path):
    db = MinerDB(str(tmp_path / 'test.db'))
    run = db.create_pipeline_run(triggered_by='test', scope={}, config={})
    db.update_pipeline_run(run['id'], status='failed')
    result = db.get_last_successful_pipeline_run(source='edgar', ticker=None)
    assert result is None


def test_get_last_successful_pipeline_run_filters_by_ticker(tmp_path):
    db = MinerDB(str(tmp_path / 'test.db'))
    run = db.create_pipeline_run(triggered_by='test', scope={}, config={})
    db.update_pipeline_run(run['id'], status='complete')
    db.upsert_pipeline_run_ticker(run['id'], 'TICK_A')
    assert db.get_last_successful_pipeline_run(source='edgar', ticker='TICK_A') is not None
    assert db.get_last_successful_pipeline_run(source='edgar', ticker='TICK_B') is None


def test_overnight_latest_returns_most_recent_run(client):
    import app_globals
    db = app_globals.get_db()
    run1 = db.create_pipeline_run(triggered_by='test', scope={'tickers': ['TICK_A']}, config={})
    run2 = db.create_pipeline_run(triggered_by='test', scope={'tickers': ['TICK_B']}, config={})

    resp = client.get('/api/pipeline/overnight/latest')
    assert resp.status_code == 200
    data = resp.get_json()
    assert data['success'] is True
    assert data['data']['run']['id'] == run2['id']


def test_overnight_latest_returns_404_when_no_runs(client):
    resp = client.get('/api/pipeline/overnight/latest')
    assert resp.status_code == 404


def test_overnight_cancel_sets_cancel_requested_flag(client):
    import app_globals

    db = app_globals.get_db()
    run = db.create_pipeline_run(triggered_by='test', scope={'tickers': ['MARA']}, config={})
    run_id = int(run['id'])

    cancel = client.post(f'/api/pipeline/overnight/{run_id}/cancel')
    assert cancel.status_code == 200
    assert cancel.get_json()['data']['cancel_requested'] is True

    status = client.get(f'/api/pipeline/overnight/{run_id}/status')
    assert status.status_code == 200
    assert status.get_json()['data']['cancel_requested'] is True


def test_overnight_start_accepts_force_reextract(client, monkeypatch):
    """force_reextract flag must be stored in the pipeline run config."""
    import app_globals
    import routes.pipeline as pipeline_mod

    class _DummyThread:
        def __init__(self, target=None, args=(), daemon=False, name=None):
            pass
        def start(self):
            return None

    monkeypatch.setattr(pipeline_mod.threading, 'Thread', _DummyThread)

    resp = client.post('/api/pipeline/overnight/start', json={
        'tickers': ['MARA'],
        'force_reextract': True,
    })
    assert resp.status_code == 202
    run_id = int(resp.get_json()['data']['run_id'])
    run = app_globals.get_db().get_pipeline_run(run_id)
    cfg_raw = run.get('config_json') or '{}'
    cfg = json.loads(cfg_raw) if isinstance(cfg_raw, str) else cfg_raw
    assert cfg.get('force_reextract') is True, (
        "force_reextract=True must be stored in the run config"
    )


def test_pipeline_preflight_returns_json(client):
    """GET /api/pipeline/preflight must return 200 with expected fields."""
    resp = client.get('/api/pipeline/preflight')
    assert resp.status_code == 200
    data = resp.get_json()
    assert data['success'] is True
    preflight = data['data']
    assert 'pending_report_count' in preflight
    assert 'already_extracted_count' in preflight
    assert 'llm_available' in preflight
    assert 'keyword_count' in preflight


def test_pipeline_preflight_counts_pending_vs_extracted(client):
    """Preflight counts must correctly reflect pending vs extracted reports."""
    import app_globals
    from infra.db import MinerDB

    db = app_globals.get_db()
    # Insert a pending report
    from helpers import make_report
    pending_id = db.insert_report(make_report(
        raw_text='MARA mined 700 BTC.',
        report_date='2024-09-01',
        source_type='archive_html',
        ticker='MARA',
    ))
    # Insert and mark another as extracted
    extracted_id = db.insert_report(make_report(
        raw_text='MARA mined 800 BTC.',
        report_date='2024-10-01',
        source_type='archive_html',
        ticker='MARA',
    ))
    db.mark_report_extracted(extracted_id)

    resp = client.get('/api/pipeline/preflight')
    assert resp.status_code == 200
    preflight = resp.get_json()['data']
    assert preflight['pending_report_count'] >= 1, "Must count the pending report"
    assert preflight['already_extracted_count'] >= 1, "Must count the extracted report"


def test_overnight_apply_modes_runs_for_run_tickers(client, monkeypatch):
    import app_globals
    import orchestration as orch_mod

    db = app_globals.get_db()
    run = db.create_pipeline_run(triggered_by='test', scope={'tickers': ['MARA']}, config={})
    run_id = int(run['id'])
    db.upsert_pipeline_run_ticker(run_id, 'MARA', targeted=1)

    def _fake_probe(db_obj, ticker, apply_mode, allow_apply_skip, timeout):
        assert ticker == 'MARA'
        assert apply_mode is True
        return {'applied': True, 'recommended_mode': 'rss'}

    monkeypatch.setattr(orch_mod, 'run_bootstrap_probe_for_ticker', _fake_probe)

    resp = client.post(f'/api/pipeline/overnight/{run_id}/apply_modes', json={})
    assert resp.status_code == 200
    data = resp.get_json()['data']
    assert data['targeted'] == 1
    assert data['applied'] == 1
    assert data['failed'] == 0
