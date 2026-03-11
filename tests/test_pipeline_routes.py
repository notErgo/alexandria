"""Route tests for overnight pipeline orchestration APIs."""

import importlib
import json
import os
import sys

import pytest

from infra.db import MinerDB
from helpers import make_report


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


def test_overnight_start_stores_ticker_scope(client, monkeypatch):
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
        'tickers': ['mara', 'riot'],
        'include_ir': True,
    })
    assert resp.status_code == 202
    run_id = int(resp.get_json()['data']['run_id'])
    run = app_globals.get_db().get_pipeline_run(run_id)
    scope_raw = run.get('scope_json') or '{}'
    scope = json.loads(scope_raw) if isinstance(scope_raw, str) else scope_raw
    assert scope.get('tickers') == ['MARA', 'RIOT']


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


def test_execute_overnight_run_passes_scope_to_ingest(monkeypatch, tmp_path):
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))
    import app_globals
    import routes.pipeline as pipeline_mod

    db = MinerDB(str(tmp_path / 'scope.db'))
    for ticker, cik in [('MARA', '0001437491'), ('RIOT', '0001167419')]:
        db.insert_company({
            'ticker': ticker,
            'name': ticker,
            'tier': 1,
            'ir_url': f'https://example.com/{ticker.lower()}',
            'pr_base_url': 'https://example.com',
            'cik': cik,
            'active': 1,
            'scraper_mode': 'rss',
        })
    app_globals._db = db

    run = db.create_pipeline_run(triggered_by='test', scope={'tickers': ['MARA']}, config={})
    run_id = int(run['id'])

    scrape_calls = []

    class _ImmediateFuture:
        def __init__(self, fn, *args, **kwargs):
            self._result = fn(*args, **kwargs)

        def result(self):
            return self._result

    class _ImmediateExecutor:
        def __init__(self, max_workers):
            self.max_workers = max_workers

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def submit(self, fn, *args, **kwargs):
            return _ImmediateFuture(fn, *args, **kwargs)

    def _fake_scrape(**kwargs):
        scrape_calls.append({
            'ticker': kwargs['ticker'],
            'include_ir': kwargs['include_ir'],
            'run_id': kwargs['run_id'],
        })
        return {
            'ticker': kwargs['ticker'],
            'before_reports': 0,
            'after_reports': 0,
            'ingested_delta': 0,
            'failures': [],
        }

    monkeypatch.setattr(pipeline_mod, 'ThreadPoolExecutor', _ImmediateExecutor)
    monkeypatch.setattr(pipeline_mod, 'as_completed', lambda futures: futures)
    monkeypatch.setattr(pipeline_mod, '_build_extraction_batch', lambda *args, **kwargs: [])
    monkeypatch.setattr(pipeline_mod, '_scrape_ticker_for_pipeline', _fake_scrape)

    pipeline_mod._execute_overnight_run(
        run_id,
        {
            'skip_probe': True,
            'include_ir': True,
            'include_crawl': False,
            'warm_model': False,
            'probe_skip_companies': False,
            'force_reextract': False,
            'scout_mode': 'never',
        },
        ['MARA'],
    )

    assert scrape_calls == [{
        'ticker': 'MARA',
        'include_ir': True,
        'run_id': run_id,
    }]


def test_execute_overnight_run_streams_per_ticker_extract(monkeypatch, tmp_path):
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))
    import app_globals
    import routes.pipeline as pipeline_mod

    db = MinerDB(str(tmp_path / 'streaming.db'))
    for ticker, cik in [('MARA', '0001437491'), ('RIOT', '0001167419')]:
        db.insert_company({
            'ticker': ticker,
            'name': ticker,
            'tier': 1,
            'ir_url': f'https://example.com/{ticker.lower()}',
            'pr_base_url': 'https://example.com',
            'cik': cik,
            'active': 1,
            'scraper_mode': 'rss',
        })
    app_globals._db = db

    run = db.create_pipeline_run(triggered_by='test', scope={'tickers': ['MARA', 'RIOT']}, config={})
    run_id = int(run['id'])
    call_order = []

    class _ImmediateFuture:
        def __init__(self, fn, *args, **kwargs):
            self._result = fn(*args, **kwargs)

        def result(self):
            return self._result

    class _ImmediateExecutor:
        def __init__(self, max_workers):
            self.max_workers = max_workers

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def submit(self, fn, *args, **kwargs):
            return _ImmediateFuture(fn, *args, **kwargs)

    def _fake_scrape(**kwargs):
        ticker = kwargs['ticker']
        call_order.append(('scrape', ticker, kwargs['include_ir']))
        return {
            'ticker': ticker,
            'before_reports': 0,
            'after_reports': 1,
            'ingested_delta': 1,
            'failures': [],
        }

    def _fake_extract_reports_for_ticker(db, run_id, ticker, reports, registry, counters, failures, num_workers):
        call_order.append(('extract', ticker, num_workers, len(reports)))

    monkeypatch.setattr(pipeline_mod, 'ThreadPoolExecutor', _ImmediateExecutor)
    monkeypatch.setattr(pipeline_mod, 'as_completed', lambda futures: futures)
    monkeypatch.setattr(pipeline_mod, '_scrape_ticker_for_pipeline', _fake_scrape)
    monkeypatch.setattr(pipeline_mod, '_extract_reports_for_ticker', _fake_extract_reports_for_ticker)
    monkeypatch.setattr(
        pipeline_mod,
        '_build_extraction_batch',
        lambda db_obj, ticker, first_filing, force_reextract=False: [{'id': 1, 'ticker': ticker}],
    )

    pipeline_mod._execute_overnight_run(
        run_id,
        {
            'skip_probe': True,
            'include_ir': True,
            'include_crawl': False,
            'warm_model': False,
            'probe_skip_companies': False,
            'force_reextract': False,
            'extract_workers': 4,
            'scout_mode': 'never',
        },
        ['MARA', 'RIOT'],
    )

    assert call_order == [
        ('scrape', 'MARA', True),
        ('extract', 'MARA', 4, 1),
        ('scrape', 'RIOT', True),
        ('extract', 'RIOT', 4, 1),
    ]


def test_extract_reports_for_ticker_bounds_workers_and_claims_reports(monkeypatch, tmp_path):
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))
    import routes.pipeline as pipeline_mod
    import interpreters.interpret_pipeline as interpret_mod
    from miner_types import ExtractionSummary

    db = MinerDB(str(tmp_path / 'extract-workers.db'))
    db.insert_company({
        'ticker': 'MARA',
        'name': 'MARA',
        'tier': 1,
        'ir_url': 'https://example.com/mara',
        'pr_base_url': 'https://example.com',
        'cik': '0001437491',
        'active': 1,
        'scraper_mode': 'rss',
    })
    report_ids = [
        db.insert_report(make_report(
            ticker='MARA',
            raw_text=f'MARA mined {700 + i} BTC.',
            report_date=f'2024-{i + 1:02d}-01',
            source_url=f'https://example.com/report-{i}',
        ))
        for i in range(3)
    ]
    reports = [db.get_report(rid) for rid in report_ids]
    run = db.create_pipeline_run(triggered_by='test', scope={'tickers': ['MARA']}, config={})
    run_id = int(run['id'])
    extracted = []
    seen_workers = []

    class _ImmediateFuture:
        def __init__(self, fn, *args, **kwargs):
            self._result = fn(*args, **kwargs)

        def result(self):
            return self._result

    class _ImmediateExecutor:
        def __init__(self, max_workers):
            seen_workers.append(max_workers)

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def submit(self, fn, *args, **kwargs):
            return _ImmediateFuture(fn, *args, **kwargs)

    def _fake_extract(report, local_db, registry, **kwargs):
        extracted.append(report['id'])
        local_db.mark_report_extracted(report['id'])
        summary = ExtractionSummary()
        summary.reports_processed = 1
        summary.data_points_extracted = 1
        return summary

    monkeypatch.setattr(interpret_mod, 'extract_report', _fake_extract)
    monkeypatch.setattr(pipeline_mod, 'ThreadPoolExecutor', _ImmediateExecutor)
    monkeypatch.setattr(pipeline_mod, 'as_completed', lambda futures: futures)

    counters = {
        'total_reports': len(reports),
        'report_done_count': 0,
        'processed': 0,
        'data_points': 0,
        'errors': 0,
        'keyword_gated': 0,
        'regex_gated': 0,
    }
    failures = []

    pipeline_mod._extract_reports_for_ticker(
        db=db,
        run_id=run_id,
        ticker='MARA',
        reports=reports,
        registry=object(),
        counters=counters,
        failures=failures,
        num_workers=10,
    )

    assert seen_workers == [3]
    assert sorted(extracted) == sorted(report_ids)
    assert counters['processed'] == 3
    assert counters['data_points'] == 3
    assert counters['errors'] == 0
    assert failures == []
