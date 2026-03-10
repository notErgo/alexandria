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


def test_extract_pending_reports_aggregates_counts(monkeypatch):
    import routes.reports as reports
    import interpreters.interpret_pipeline as pipeline
    import app_globals

    class _Summary:
        reports_processed = 1
        data_points_extracted = 2
        review_flagged = 1
        errors = 0

    class _FakeDB:
        def get_unextracted_reports(self, ticker=None):
            return [{'id': 1}, {'id': 2}]

    calls = []

    def _fake_extract(report, db, registry):
        calls.append(report['id'])
        return _Summary()

    monkeypatch.setattr(app_globals, 'get_registry', lambda: object())
    monkeypatch.setattr(pipeline, 'extract_report', _fake_extract)

    totals = reports._extract_pending_reports(_FakeDB())

    assert calls == [1, 2]
    assert totals['reports_processed'] == 2
    assert totals['data_points_extracted'] == 4
    assert totals['review_flagged'] == 2
    assert totals['errors'] == 0
