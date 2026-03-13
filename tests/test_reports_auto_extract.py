"""Tests for ingest auto-extract chaining in reports routes."""

import importlib


class _FakeThread:
    """Thread stub that records constructor args and does not execute target."""

    created = []

    def __init__(self, target=None, args=(), daemon=None, name=None):
        self.target = target
        self.args = args
        self.daemon = daemon
        self.name = name
        self.started = False
        _FakeThread.created.append(self)

    def start(self):
        self.started = True


def _build_app(tmp_path):
    import os
    import sys

    sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

    from infra.db import MinerDB
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

    import run_web
    importlib.reload(run_web)
    flask_app = run_web.create_app()
    flask_app.config['TESTING'] = True
    return flask_app


def test_ingest_ir_passes_auto_extract_flag(tmp_path, monkeypatch):
    app = _build_app(tmp_path)
    client = app.test_client()

    import routes.reports as reports
    _FakeThread.created.clear()
    monkeypatch.setattr(reports.threading, 'Thread', _FakeThread)
    reports._running_tasks.clear()

    resp = client.post('/api/ingest/ir', json={'auto_extract': True})

    assert resp.status_code == 202
    assert _FakeThread.created, 'ingest_ir should create a background thread'
    thread = _FakeThread.created[-1]
    assert thread.target is reports._run_ir_ingest
    assert thread.args[1] is True
    assert thread.started is True


def test_ingest_ir_passes_ticker_scope(tmp_path, monkeypatch):
    app = _build_app(tmp_path)
    client = app.test_client()

    import routes.reports as reports
    _FakeThread.created.clear()
    monkeypatch.setattr(reports.threading, 'Thread', _FakeThread)
    reports._running_tasks.clear()

    resp = client.post('/api/ingest/ir', json={'tickers': ['mara']})

    assert resp.status_code == 202
    assert _FakeThread.created, 'ingest_ir should create a background thread'
    thread = _FakeThread.created[-1]
    assert thread.target is reports._run_ir_ingest
    assert thread.args[3] == ['MARA']
    assert thread.started is True


def test_ingest_edgar_passes_auto_extract_flag(tmp_path, monkeypatch):
    app = _build_app(tmp_path)
    client = app.test_client()

    import routes.reports as reports
    _FakeThread.created.clear()
    monkeypatch.setattr(reports.threading, 'Thread', _FakeThread)
    reports._running_tasks.clear()

    resp = client.post('/api/ingest/edgar', json={'auto_extract': True})

    assert resp.status_code == 202
    assert _FakeThread.created, 'ingest_edgar should create a background thread'
    thread = _FakeThread.created[-1]
    assert thread.target is reports._run_edgar_ingest
    assert thread.args[1] is True
    assert thread.started is True


def test_run_auto_extract_uses_run_extraction_phase_with_monthly_types(tmp_path, monkeypatch):
    """_run_auto_extract delegates to run_extraction_phase with the supplied source_types."""
    import os
    import sys
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))
    import app_globals
    import routes.reports as reports_mod
    import routes.pipeline as pipeline_mod
    from infra.db import MinerDB
    from config import MONTHLY_EXTRACTION_SOURCE_TYPES

    db = MinerDB(str(tmp_path / 'test.db'))
    db.insert_company({
        'ticker': 'MARA', 'name': 'MARA', 'tier': 1,
        'ir_url': 'https://example.com', 'pr_base_url': 'https://example.com',
        'cik': '0001437491', 'active': 1,
    })
    app_globals._db = db

    extraction_calls = []

    def _fake_run_extraction_phase(db, run_id, tickers, **kwargs):
        extraction_calls.append({
            'tickers': list(tickers),
            'source_types': kwargs.get('source_types'),
            'extract_workers': kwargs.get('extract_workers'),
        })
        return {'total_reports': 0, 'processed': 0, 'data_points': 0,
                'errors': 0, 'keyword_gated': 0, 'review_flagged': 0, 'report_done_count': 0}

    monkeypatch.setattr(pipeline_mod, 'run_extraction_phase', _fake_run_extraction_phase)

    reports_mod._run_auto_extract(
        db,
        tickers=['MARA'],
        source_types=list(MONTHLY_EXTRACTION_SOURCE_TYPES),
        triggered_by='auto_extract_ir',
    )

    assert len(extraction_calls) == 1, f"Expected run_extraction_phase called once, got {extraction_calls}"
    call = extraction_calls[0]
    assert call['source_types'] == list(MONTHLY_EXTRACTION_SOURCE_TYPES)
    assert call['tickers'] == ['MARA']
    assert call['extract_workers'] >= 1


def test_run_auto_extract_uses_run_extraction_phase_with_edgar_types(tmp_path, monkeypatch):
    """_run_auto_extract with EDGAR source_types delegates correctly."""
    import os
    import sys
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))
    import app_globals
    import routes.reports as reports_mod
    import routes.pipeline as pipeline_mod
    from infra.db import MinerDB
    from routes.pipeline import _EDGAR_SOURCE_TYPES

    db = MinerDB(str(tmp_path / 'test.db'))
    db.insert_company({
        'ticker': 'MARA', 'name': 'MARA', 'tier': 1,
        'ir_url': 'https://example.com', 'pr_base_url': 'https://example.com',
        'cik': '0001437491', 'active': 1,
    })
    app_globals._db = db

    extraction_calls = []

    def _fake_run_extraction_phase(db, run_id, tickers, **kwargs):
        extraction_calls.append({
            'source_types': kwargs.get('source_types'),
            'extract_workers': kwargs.get('extract_workers'),
        })
        return {'total_reports': 0, 'processed': 0, 'data_points': 0,
                'errors': 0, 'keyword_gated': 0, 'review_flagged': 0, 'report_done_count': 0}

    monkeypatch.setattr(pipeline_mod, 'run_extraction_phase', _fake_run_extraction_phase)

    reports_mod._run_auto_extract(
        db,
        tickers=['MARA'],
        source_types=list(_EDGAR_SOURCE_TYPES),
        triggered_by='auto_extract_edgar',
    )

    assert len(extraction_calls) == 1
    assert extraction_calls[0]['source_types'] == list(_EDGAR_SOURCE_TYPES)
    assert extraction_calls[0]['extract_workers'] >= 1
