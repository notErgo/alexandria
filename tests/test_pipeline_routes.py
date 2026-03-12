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


def test_reset_interrupted_pipeline_runs_marks_non_terminal_runs(tmp_path):
    db = MinerDB(str(tmp_path / 'test.db'))
    queued = db.create_pipeline_run(triggered_by='test', scope={'tickers': ['MARA']}, config={})
    running = db.create_pipeline_run(triggered_by='test', scope={'tickers': ['RIOT']}, config={})
    complete = db.create_pipeline_run(triggered_by='test', scope={'tickers': ['CLSK']}, config={})
    db.update_pipeline_run(running['id'], status='running')
    db.update_pipeline_run(complete['id'], status='complete')

    recovered = db.reset_interrupted_pipeline_runs()

    assert recovered == 2
    assert db.get_pipeline_run(queued['id'])['status'] == 'stopped'
    assert db.get_pipeline_run(running['id'])['status'] == 'stopped'
    assert db.get_pipeline_run(complete['id'])['status'] == 'complete'


def test_overnight_start_rejects_when_live_run_thread_exists(client, monkeypatch):
    import app_globals
    import routes.pipeline as pipeline_mod

    db = app_globals.get_db()
    active_run = db.create_pipeline_run(triggered_by='test', scope={'tickers': ['MARA']}, config={})
    db.update_pipeline_run(active_run['id'], status='running')

    class _AliveThread:
        def is_alive(self):
            return True

    class _DummyThread:
        def __init__(self, target=None, args=(), daemon=False, name=None):
            self.target = target
            self.args = args
            self.daemon = daemon
            self.name = name

        def start(self):
            return None

        def is_alive(self):
            return False

    monkeypatch.setattr(pipeline_mod.threading, 'Thread', _DummyThread)
    monkeypatch.setitem(pipeline_mod._run_threads, int(active_run['id']), _AliveThread())

    resp = client.post('/api/pipeline/overnight/start', json={'tickers': ['MARA']})

    assert resp.status_code == 409
    body = resp.get_json()
    assert body['success'] is False
    assert body['error']['code'] == 'ALREADY_RUNNING'
    assert body['data']['active_run_id'] == int(active_run['id'])
    pipeline_mod._run_threads.pop(int(active_run['id']), None)


def test_overnight_start_recovers_stale_running_report_claims(client, monkeypatch):
    import app_globals
    import routes.pipeline as pipeline_mod

    db = app_globals.get_db()
    report_id = db.insert_report(make_report(
        ticker='MARA',
        raw_text='MARA mined 700 BTC.',
        report_date='2024-09-01',
    ))
    db.mark_report_extraction_running(report_id)

    class _DummyThread:
        def __init__(self, target=None, args=(), daemon=False, name=None):
            self.target = target
            self.args = args
            self.daemon = daemon
            self.name = name

        def start(self):
            return None

        def is_alive(self):
            return False

    monkeypatch.setattr(pipeline_mod.threading, 'Thread', _DummyThread)

    resp = client.post('/api/pipeline/overnight/start', json={'tickers': ['MARA']})

    assert resp.status_code == 202
    refreshed = db.get_report(report_id)
    assert refreshed['extraction_status'] == 'pending'
    assert refreshed['extraction_attempts'] == 0


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


def test_overnight_start_accepts_extract_workers(client, monkeypatch):
    """extract_workers must be stored in the pipeline run config."""
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
        'extract_workers': 5,
    })
    assert resp.status_code == 202
    run_id = int(resp.get_json()['data']['run_id'])
    run = app_globals.get_db().get_pipeline_run(run_id)
    cfg_raw = run.get('config_json') or "{}"
    cfg = json.loads(cfg_raw) if isinstance(cfg_raw, str) else cfg_raw
    assert int(cfg.get('extract_workers')) == 5


def test_overnight_start_accepts_ir_workers(client, monkeypatch):
    """ir_workers must be stored in the pipeline run config."""
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
        'ir_workers': 4,
    })
    assert resp.status_code == 202
    run_id = int(resp.get_json()['data']['run_id'])
    run = app_globals.get_db().get_pipeline_run(run_id)
    cfg_raw = run.get('config_json') or "{}"
    cfg = json.loads(cfg_raw) if isinstance(cfg_raw, str) else cfg_raw
    assert int(cfg.get('ir_workers')) == 4


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


def test_scrape_ticker_for_pipeline_runs_archive_ingest_first(monkeypatch, tmp_path):
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))
    import config as config_mod
    import routes.pipeline as pipeline_mod
    from miner_types import IngestSummary

    db = MinerDB(str(tmp_path / 'archive-stage.db'))
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

    call_order = []

    class _FakeArchiveIngestor:
        def __init__(self, archive_dir, db, registry):
            self.archive_dir = archive_dir
            self.db = db
            self.registry = registry

        def ingest_all(self, force=False, progress_callback=None, tickers=None, auto_extract_monthly=True):
            call_order.append(('archive', tuple(tickers or []), auto_extract_monthly))
            summary = IngestSummary()
            summary.reports_ingested = 2
            summary.data_points_extracted = 3
            return summary

    class _FakeIRScraper:
        def __init__(self, db, session):
            pass

        def scrape_company(self, company):
            call_order.append(('ir', company['ticker']))
            return IngestSummary()

    class _FakeEdgarConnector:
        def __init__(self, db, session):
            pass

        def fetch_all_filings(self, cik, ticker, since_date, filing_regime):
            call_order.append(('edgar', ticker))
            return IngestSummary()

    monkeypatch.setattr(pipeline_mod, '_event', lambda *args, **kwargs: None)
    monkeypatch.setattr(pipeline_mod, '_is_cancelled', lambda run_id: False)
    monkeypatch.setattr(pipeline_mod, '_count_reports_for_tickers', lambda db_obj, tickers: 0)
    monkeypatch.setattr(config_mod, 'ARCHIVE_DIR', str(tmp_path))
    monkeypatch.setattr('scrapers.archive_ingestor.ArchiveIngestor', _FakeArchiveIngestor)
    monkeypatch.setattr('scrapers.ir_scraper.IRScraper', _FakeIRScraper)
    monkeypatch.setattr('scrapers.edgar_connector.EdgarConnector', _FakeEdgarConnector)
    monkeypatch.setattr('interpreters.pattern_registry.PatternRegistry.load', lambda config_dir: object())

    result = pipeline_mod._scrape_ticker_for_pipeline(
        db_path=db.db_path,
        run_id=1,
        ticker='MARA',
        include_ir=True,
        ir_semaphore=pipeline_mod.threading.Semaphore(1),
        edgar_semaphore=pipeline_mod.threading.Semaphore(1),
        ir_throttle=None,
        edgar_throttle=None,
        host_backoff_seconds=0,
        max_retries=1,
    )

    assert call_order == [
        ('archive', ('MARA',), False),
        ('ir', 'MARA'),
        ('edgar', 'MARA'),
    ]
    assert result['archive_reports_ingested'] == 2
    assert result['archive_data_points_extracted'] == 3


def test_extract_reports_for_ticker_parallelizes_compute_but_commits_in_order(monkeypatch, tmp_path):
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))
    import time
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

    def _fake_extract(report, local_db, registry, **kwargs):
        delays = {
            report_ids[0]: 0.05,
            report_ids[1]: 0.01,
            report_ids[2]: 0.0,
        }
        time.sleep(delays[report['id']])
        extracted.append(report['id'])
        local_db.mark_report_extracted(report['id'])
        summary = ExtractionSummary()
        summary.reports_processed = 1
        summary.data_points_extracted = 1
        return summary

    monkeypatch.setattr(interpret_mod, 'extract_report', _fake_extract)

    counters = {
        'total_reports': len(reports),
        'report_done_count': 0,
        'processed': 0,
        'data_points': 0,
        'errors': 0,
        'keyword_gated': 0,
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
        num_workers=3,
    )

    assert sorted(extracted) == report_ids
    assert counters['processed'] == 3
    assert counters['data_points'] == 3
    assert counters['errors'] == 0
    assert failures == []
    with db._get_connection() as conn:
        queue_rows = conn.execute(
            "SELECT report_id, status FROM extraction_commit_queue WHERE ticker='MARA' ORDER BY sequence_key"
        ).fetchall()
        done_events = conn.execute(
            """SELECT json_extract(details_json, '$.report_id') AS report_id
                 FROM pipeline_run_events
                WHERE run_id=? AND stage='extract' AND event='report_done' AND ticker='MARA'
                ORDER BY id""",
            (run_id,),
        ).fetchall()
        ticker_start = conn.execute(
            """SELECT details_json FROM pipeline_run_events
                 WHERE run_id=? AND stage='extract' AND event='ticker_start' AND ticker='MARA'""",
            (run_id,),
        ).fetchone()
    assert [(row['report_id'], row['status']) for row in queue_rows] == [
        (report_ids[0], 'committed'),
        (report_ids[1], 'committed'),
        (report_ids[2], 'committed'),
    ]
    assert [int(row['report_id']) for row in done_events] == report_ids
    assert '"workers": 3' in ticker_start['details_json']


def test_extract_reports_for_ticker_limits_claimed_running_rows_to_worker_count(monkeypatch, tmp_path):
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))
    import threading
    import routes.pipeline as pipeline_mod
    import interpreters.interpret_pipeline as interpret_mod
    from miner_types import ExtractionSummary

    db = MinerDB(str(tmp_path / 'bounded-claims.db'))
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
        for i in range(4)
    ]
    reports = [db.get_report(rid) for rid in report_ids]
    run = db.create_pipeline_run(triggered_by='test', scope={'tickers': ['MARA']}, config={})
    run_id = int(run['id'])
    started = 0
    started_lock = threading.Lock()
    all_workers_started = threading.Event()
    release_workers = threading.Event()

    def _fake_extract(report, local_db, registry, **kwargs):
        nonlocal started
        with started_lock:
            started += 1
            if started == 2:
                all_workers_started.set()
        release_workers.wait(timeout=2.0)
        local_db.mark_report_extracted(report['id'])
        summary = ExtractionSummary()
        summary.reports_processed = 1
        return summary

    monkeypatch.setattr(interpret_mod, 'extract_report', _fake_extract)

    counters = {
        'total_reports': len(reports),
        'report_done_count': 0,
        'processed': 0,
        'data_points': 0,
        'errors': 0,
        'keyword_gated': 0,
    }
    failures = []
    worker = threading.Thread(
        target=pipeline_mod._extract_reports_for_ticker,
        kwargs={
            'db': db,
            'run_id': run_id,
            'ticker': 'MARA',
            'reports': reports,
            'registry': object(),
            'counters': counters,
            'failures': failures,
            'num_workers': 2,
        },
    )
    worker.start()
    assert all_workers_started.wait(timeout=2.0)

    with db._get_connection() as conn:
        running_now = conn.execute(
            "SELECT COUNT(*) FROM reports WHERE ticker='MARA' AND extraction_status='running'"
        ).fetchone()[0]
    assert running_now == 2

    release_workers.set()
    worker.join(timeout=5.0)
    assert not worker.is_alive()
    assert failures == []


def test_extract_reports_for_ticker_forwards_run_config(monkeypatch, tmp_path):
    """_extract_reports_for_ticker forwards run_config kwarg to extract_report."""
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))
    import routes.pipeline as pipeline_mod
    import interpreters.interpret_pipeline as interpret_mod
    from miner_types import ExtractionSummary, ExtractionRunConfig

    db = MinerDB(str(tmp_path / 'run-config.db'))
    db.insert_company({'ticker': 'MARA', 'name': 'MARA', 'tier': 1,
                       'ir_url': 'https://example.com/mara', 'pr_base_url': 'https://example.com',
                       'cik': '0001437491', 'active': 1, 'scraper_mode': 'rss'})
    report_id = db.insert_report(make_report(
        ticker='MARA', raw_text='MARA mined 100 BTC.', report_date='2024-01-01',
        source_url='https://example.com/r1',
    ))
    reports = [db.get_report(report_id)]
    run = db.create_pipeline_run(triggered_by='test', scope={}, config={})
    run_id = int(run['id'])

    captured_configs = []

    def _fake_extract(report, local_db, registry, config=None, **kwargs):
        captured_configs.append(config)
        local_db.mark_report_extracted(report['id'])
        s = ExtractionSummary()
        s.reports_processed = 1
        return s

    monkeypatch.setattr(interpret_mod, 'extract_report', _fake_extract)

    run_config = ExtractionRunConfig(expected_granularity='monthly', ticker='MARA')
    counters = {'total_reports': 1, 'report_done_count': 0, 'processed': 0,
                'data_points': 0, 'errors': 0, 'keyword_gated': 0}
    pipeline_mod._extract_reports_for_ticker(
        db=db, run_id=run_id, ticker='MARA', reports=reports,
        registry=object(), counters=counters, failures=[], num_workers=1,
        run_config=run_config,
    )

    assert len(captured_configs) == 1
    assert captured_configs[0] is run_config


def test_extract_reports_for_ticker_force_uses_mark_running_not_claim(monkeypatch, tmp_path):
    """force_reextract=True uses mark_report_extraction_running, not claim_report_for_extraction."""
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))
    import routes.pipeline as pipeline_mod
    import interpreters.interpret_pipeline as interpret_mod
    from miner_types import ExtractionSummary

    db = MinerDB(str(tmp_path / 'force-claim.db'))
    db.insert_company({'ticker': 'MARA', 'name': 'MARA', 'tier': 1,
                       'ir_url': 'https://example.com/mara', 'pr_base_url': 'https://example.com',
                       'cik': '0001437491', 'active': 1, 'scraper_mode': 'rss'})
    report_id = db.insert_report(make_report(
        ticker='MARA', raw_text='MARA mined 100 BTC.', report_date='2024-01-01',
        source_url='https://example.com/r1',
    ))
    # Mark as already extracted so claim_report_for_extraction would skip it
    db.mark_report_extracted(report_id)
    reports = [db.get_report(report_id)]
    run = db.create_pipeline_run(triggered_by='test', scope={}, config={})
    run_id = int(run['id'])

    extracted_ids = []

    def _fake_extract(report, local_db, registry, **kwargs):
        extracted_ids.append(report['id'])
        local_db.mark_report_extracted(report['id'])
        s = ExtractionSummary()
        s.reports_processed = 1
        return s

    monkeypatch.setattr(interpret_mod, 'extract_report', _fake_extract)

    counters = {'total_reports': 1, 'report_done_count': 0, 'processed': 0,
                'data_points': 0, 'errors': 0, 'keyword_gated': 0}
    pipeline_mod._extract_reports_for_ticker(
        db=db, run_id=run_id, ticker='MARA', reports=reports,
        registry=object(), counters=counters, failures=[], num_workers=2,
        force_reextract=True,
    )

    # With force_reextract=True, the already-extracted report should still be processed
    assert extracted_ids == [report_id], (
        "force_reextract=True should process already-extracted reports via mark_running"
    )


def test_operations_extract_delegates_to_shared_worker(monkeypatch, tmp_path):
    """POST /api/operations/interpret delegates extraction to _extract_reports_for_ticker."""
    import importlib
    import app_globals, run_web
    import routes.pipeline as pipeline_mod
    import routes.operations as ops_mod
    from miner_types import ExtractionSummary

    db = MinerDB(str(tmp_path / 'delegate.db'))
    db.insert_company({'ticker': 'MARA', 'name': 'MARA Holdings', 'tier': 1,
                       'ir_url': 'https://example.com', 'pr_base_url': 'https://example.com',
                       'cik': '0001437491', 'active': 1})
    report_id = db.insert_report(make_report(
        ticker='MARA', raw_text='MARA mined 100 BTC.', report_date='2024-01-01',
        source_url='https://example.com/r1',
    ))
    app_globals._db = db
    importlib.reload(run_web)
    flask_app = run_web.create_app()
    flask_app.config['TESTING'] = True
    client = flask_app.test_client()

    delegate_calls = []

    def _fake_extract_for_ticker(db, run_id, ticker, reports, registry, counters,
                                  failures, num_workers, *, run_config=None, force_reextract=False):
        delegate_calls.append({'ticker': ticker, 'reports': reports,
                                'force_reextract': force_reextract})
        # Simulate successful extraction
        counters['processed'] += len(reports)

    monkeypatch.setattr(pipeline_mod, '_extract_reports_for_ticker', _fake_extract_for_ticker)

    class _ImmediateThread:
        def __init__(self, target=None, args=(), daemon=False, name=None):
            self._target = target
        def start(self):
            if self._target:
                self._target()

    monkeypatch.setattr(ops_mod, '_active_tickers', set())
    monkeypatch.setattr(ops_mod.threading, 'Thread', _ImmediateThread)

    resp = client.post('/api/operations/interpret', json={
        'tickers': ['MARA'], 'warm_model': False,
    })
    assert resp.status_code == 200

    assert len(delegate_calls) >= 1
    assert any(c['ticker'] == 'MARA' for c in delegate_calls)
