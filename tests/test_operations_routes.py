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


def test_extract_accepts_ticker_scope_and_worker_count(client, monkeypatch):
    """POST /api/operations/interpret accepts tickers[] and extract_workers."""
    import routes.operations as ops_mod
    monkeypatch.setattr(ops_mod, '_active_tickers', set())

    resp = client.post('/api/operations/interpret', json={
        'tickers': ['MARA'],
        'extract_workers': 4,
    })
    assert resp.status_code in (200, 409)
    if resp.status_code == 200:
        data = resp.get_json()
        assert data['success'] is True
        assert data['data']['tickers'] == ['MARA']
        assert data['data']['scope_label'] == 'MARA'
        assert data['data']['extract_workers'] == 4


def test_extract_progress_explains_when_no_stored_reports_match(client, monkeypatch):
    """Empty extraction scope should tell the operator to ingest first."""
    import routes.operations as ops_mod

    class _ImmediateThread:
        def __init__(self, target=None, args=(), daemon=False, name=None):
            self._target = target
            self._args = args

        def start(self):
            if self._target:
                self._target(*self._args)

    monkeypatch.setattr(ops_mod, '_active_tickers', set())
    monkeypatch.setattr(ops_mod.threading, 'Thread', _ImmediateThread)

    resp = client.post('/api/operations/interpret', json={
        'ticker': 'MARA',
        'warm_model': False,
    })
    assert resp.status_code == 200
    task_id = resp.get_json()['data']['task_id']

    progress = client.get(f'/api/operations/interpret/{task_id}/progress')
    assert progress.status_code == 200
    data = progress.get_json()['data']
    assert data['reports_total'] == 0
    assert any('Ingest first' in line for line in data['logs'])


def test_extract_requires_active_metric_keywords(client, monkeypatch):
    """LLM extraction should be blocked when metric_schema.keywords has no active rows."""
    import app_globals
    import routes.operations as ops_mod

    db = app_globals.get_db()
    monkeypatch.setattr(ops_mod, '_active_tickers', set())
    monkeypatch.setattr(db, 'get_all_metric_keywords', lambda active_only=True: [])

    resp = client.post('/api/operations/interpret', json={'ticker': 'MARA'})
    assert resp.status_code == 400
    body = resp.get_json()
    assert body['success'] is False
    assert body['error']['code'] == 'MISSING_METRIC_KEYWORDS'


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


# ── Batch selection policy ────────────────────────────────────────────────────

def test_ops_batch_archive_not_gated_by_btc_first_filing(monkeypatch):
    """archive/IR reports are never date-gated by btc_first_filing_date."""
    import sys, os
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))
    import app_globals, routes.operations as ops_mod
    from infra.db import MinerDB
    import importlib, run_web, tempfile

    with tempfile.TemporaryDirectory() as tmpdir:
        db = MinerDB(os.path.join(tmpdir, 'test.db'))
        db.insert_company({'ticker': 'CLSK', 'name': 'CleanSpark', 'tier': 1,
                            'ir_url': 'https://example.com', 'pr_base_url': 'https://example.com',
                            'cik': '0001517767', 'active': 1})
        db.update_company_config('CLSK', btc_first_filing_date='2022-01-01')
        app_globals._db = db
        importlib.reload(run_web)
        flask_app = run_web.create_app()
        flask_app.config['TESTING'] = True
        client = flask_app.test_client()

        call_args = []

        original_getter = db.get_unextracted_reports
        def _capture_getter(ticker=None, source_types=None, from_period=None, to_period=None):
            call_args.append({'ticker': ticker, 'source_types': list(source_types or []), 'from_period': from_period})
            return []
        monkeypatch.setattr(db, 'get_unextracted_reports', _capture_getter)

        class _ImmediateThread:
            def __init__(self, target=None, args=(), daemon=False, name=None):
                self._target = target
            def start(self):
                if self._target:
                    self._target()

        monkeypatch.setattr(ops_mod, '_active_tickers', set())
        monkeypatch.setattr(ops_mod.threading, 'Thread', _ImmediateThread)

        client.post('/api/operations/interpret', json={
            'tickers': ['CLSK'], 'cadence': 'all', 'warm_model': False,
        })

        # Non-EDGAR call must NOT have from_period set to the btc_first_filing_date anchor
        _EDGAR_TYPES = {'edgar_8k', 'edgar_10k', 'edgar_10q', 'edgar_6k', 'edgar_20f', 'edgar_40f'}
        non_edgar_calls = [c for c in call_args if not set(c['source_types']) & _EDGAR_TYPES]
        assert non_edgar_calls, f"Expected non-EDGAR get_unextracted_reports call, got: {call_args}"
        for c in non_edgar_calls:
            assert c['from_period'] is None, (
                f"Non-EDGAR sources must not be gated by btc_first_filing_date, "
                f"got from_period={c['from_period']!r}"
            )


def test_ops_batch_edgar_gated_by_btc_first_filing(monkeypatch):
    """EDGAR reports ARE date-gated by btc_first_filing_date when no explicit from_period."""
    import sys, os
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))
    import app_globals, routes.operations as ops_mod
    from infra.db import MinerDB
    import importlib, run_web, tempfile

    with tempfile.TemporaryDirectory() as tmpdir:
        db = MinerDB(os.path.join(tmpdir, 'test.db'))
        db.insert_company({'ticker': 'CLSK', 'name': 'CleanSpark', 'tier': 1,
                            'ir_url': 'https://example.com', 'pr_base_url': 'https://example.com',
                            'cik': '0001517767', 'active': 1})
        db.update_company_config('CLSK', btc_first_filing_date='2022-01-01')
        app_globals._db = db
        importlib.reload(run_web)
        flask_app = run_web.create_app()
        flask_app.config['TESTING'] = True
        client = flask_app.test_client()

        call_args = []

        def _capture_getter(ticker=None, source_types=None, from_period=None, to_period=None):
            call_args.append({'ticker': ticker, 'source_types': list(source_types or []), 'from_period': from_period})
            return []
        monkeypatch.setattr(db, 'get_unextracted_reports', _capture_getter)

        class _ImmediateThread:
            def __init__(self, target=None, args=(), daemon=False, name=None):
                self._target = target
            def start(self):
                if self._target:
                    self._target()

        monkeypatch.setattr(ops_mod, '_active_tickers', set())
        monkeypatch.setattr(ops_mod.threading, 'Thread', _ImmediateThread)

        client.post('/api/operations/interpret', json={
            'tickers': ['CLSK'], 'cadence': 'all', 'warm_model': False,
        })

        edgar_types = {'edgar_8k', 'edgar_10k', 'edgar_10q', 'edgar_6k', 'edgar_20f', 'edgar_40f'}
        edgar_calls = [c for c in call_args if set(c['source_types']) <= edgar_types and c['source_types']]
        assert edgar_calls, f"Expected EDGAR get_unextracted_reports call, got: {call_args}"
        for c in edgar_calls:
            assert c['from_period'] == '2022-01-01', (
                f"EDGAR sources must be gated by btc_first_filing_date='2022-01-01', "
                f"got from_period={c['from_period']!r}"
            )


def test_ops_batch_cadence_quarterly_only_edgar_gated(monkeypatch):
    """cadence='quarterly' makes only edgar_10q calls, and those are date-gated."""
    import sys, os
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))
    import app_globals, routes.operations as ops_mod
    from infra.db import MinerDB
    import importlib, run_web, tempfile

    with tempfile.TemporaryDirectory() as tmpdir:
        db = MinerDB(os.path.join(tmpdir, 'test.db'))
        db.insert_company({'ticker': 'MARA', 'name': 'MARA', 'tier': 1,
                            'ir_url': 'https://example.com', 'pr_base_url': 'https://example.com',
                            'cik': '0001437491', 'active': 1})
        db.update_company_config('MARA', btc_first_filing_date='2021-06-01')
        app_globals._db = db
        importlib.reload(run_web)
        flask_app = run_web.create_app()
        flask_app.config['TESTING'] = True
        client = flask_app.test_client()

        call_args = []

        def _capture_getter(ticker=None, source_types=None, from_period=None, to_period=None):
            call_args.append({'source_types': list(source_types or []), 'from_period': from_period})
            return []
        monkeypatch.setattr(db, 'get_unextracted_reports', _capture_getter)

        class _ImmediateThread:
            def __init__(self, target=None, args=(), daemon=False, name=None):
                self._target = target
            def start(self):
                if self._target:
                    self._target()

        monkeypatch.setattr(ops_mod, '_active_tickers', set())
        monkeypatch.setattr(ops_mod.threading, 'Thread', _ImmediateThread)

        client.post('/api/operations/interpret', json={
            'tickers': ['MARA'], 'cadence': 'quarterly', 'warm_model': False,
        })

        # All calls must be edgar_10q and date-gated
        assert call_args, "Expected at least one get_unextracted_reports call"
        for c in call_args:
            assert 'edgar_10q' in c['source_types'], f"quarterly cadence should only call with edgar_10q, got {c}"
            assert c['from_period'] == '2021-06-01', (
                f"EDGAR quarterly must be gated by btc_first_filing_date, got from_period={c['from_period']!r}"
            )


def test_ops_batch_explicit_from_period_overrides_first_filing_for_edgar(monkeypatch):
    """An explicit from_period overrides btc_first_filing_date for EDGAR sources."""
    import sys, os
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))
    import app_globals, routes.operations as ops_mod
    from infra.db import MinerDB
    import importlib, run_web, tempfile

    with tempfile.TemporaryDirectory() as tmpdir:
        db = MinerDB(os.path.join(tmpdir, 'test.db'))
        db.insert_company({'ticker': 'MARA', 'name': 'MARA', 'tier': 1,
                            'ir_url': 'https://example.com', 'pr_base_url': 'https://example.com',
                            'cik': '0001437491', 'active': 1})
        db.update_company_config('MARA', btc_first_filing_date='2021-06-01')
        app_globals._db = db
        importlib.reload(run_web)
        flask_app = run_web.create_app()
        flask_app.config['TESTING'] = True
        client = flask_app.test_client()

        call_args = []

        def _capture_getter(ticker=None, source_types=None, from_period=None, to_period=None):
            call_args.append({'source_types': list(source_types or []), 'from_period': from_period})
            return []
        monkeypatch.setattr(db, 'get_unextracted_reports', _capture_getter)

        class _ImmediateThread:
            def __init__(self, target=None, args=(), daemon=False, name=None):
                self._target = target
            def start(self):
                if self._target:
                    self._target()

        monkeypatch.setattr(ops_mod, '_active_tickers', set())
        monkeypatch.setattr(ops_mod.threading, 'Thread', _ImmediateThread)

        client.post('/api/operations/interpret', json={
            'tickers': ['MARA'], 'cadence': 'all', 'from_period': '2023-01-01', 'warm_model': False,
        })

        edgar_types = {'edgar_8k', 'edgar_10k', 'edgar_10q', 'edgar_6k', 'edgar_20f', 'edgar_40f'}
        edgar_calls = [c for c in call_args if set(c['source_types']) <= edgar_types and c['source_types']]
        assert edgar_calls
        for c in edgar_calls:
            assert c['from_period'] == '2023-01-01', (
                f"Explicit from_period must override btc_first_filing_date for EDGAR, got {c['from_period']!r}"
            )


# ── Manual extract pipeline run persistence ───────────────────────────────────

def test_manual_extract_creates_pipeline_run_row(monkeypatch):
    """POST /api/operations/interpret creates a pipeline_runs DB row."""
    import sys, os
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))
    import app_globals, routes.operations as ops_mod
    from infra.db import MinerDB
    import importlib, run_web, tempfile

    with tempfile.TemporaryDirectory() as tmpdir:
        db = MinerDB(os.path.join(tmpdir, 'test.db'))
        db.insert_company({'ticker': 'MARA', 'name': 'MARA', 'tier': 1,
                            'ir_url': 'https://example.com', 'pr_base_url': 'https://example.com',
                            'cik': '0001437491', 'active': 1})
        app_globals._db = db
        importlib.reload(run_web)
        flask_app = run_web.create_app()
        flask_app.config['TESTING'] = True
        client = flask_app.test_client()

        class _ImmediateThread:
            def __init__(self, target=None, args=(), daemon=False, name=None):
                self._target = target
            def start(self):
                if self._target:
                    self._target()

        monkeypatch.setattr(ops_mod, '_active_tickers', set())
        monkeypatch.setattr(ops_mod.threading, 'Thread', _ImmediateThread)

        client.post('/api/operations/interpret', json={
            'tickers': ['MARA'], 'warm_model': False,
        })

        with db._get_connection() as conn:
            rows = conn.execute(
                "SELECT triggered_by, status FROM pipeline_runs WHERE triggered_by='manual_extract'"
            ).fetchall()
        assert len(rows) >= 1, "Expected at least one pipeline_runs row with triggered_by='manual_extract'"


def test_manual_extract_run_completes_with_status_complete(monkeypatch):
    """After extraction finishes, the pipeline_runs row is updated to status='complete'."""
    import sys, os
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))
    import app_globals, routes.operations as ops_mod, routes.pipeline as pipeline_mod
    from infra.db import MinerDB
    import importlib, run_web, tempfile

    with tempfile.TemporaryDirectory() as tmpdir:
        db = MinerDB(os.path.join(tmpdir, 'test.db'))
        db.insert_company({'ticker': 'MARA', 'name': 'MARA', 'tier': 1,
                            'ir_url': 'https://example.com', 'pr_base_url': 'https://example.com',
                            'cik': '0001437491', 'active': 1})
        app_globals._db = db
        importlib.reload(run_web)
        flask_app = run_web.create_app()
        flask_app.config['TESTING'] = True
        client = flask_app.test_client()

        def _noop_run_extraction_phase(db, run_id, tickers, **kwargs):
            return {'total_reports': 0, 'processed': 0, 'data_points': 0,
                    'errors': 0, 'keyword_gated': 0, 'review_flagged': 0, 'report_done_count': 0}

        monkeypatch.setattr(pipeline_mod, 'run_extraction_phase', _noop_run_extraction_phase)

        class _ImmediateThread:
            def __init__(self, target=None, args=(), daemon=False, name=None):
                self._target = target
            def start(self):
                if self._target:
                    self._target()

        monkeypatch.setattr(ops_mod, '_active_tickers', set())
        monkeypatch.setattr(ops_mod.threading, 'Thread', _ImmediateThread)

        client.post('/api/operations/interpret', json={
            'tickers': ['MARA'], 'warm_model': False,
        })

        with db._get_connection() as conn:
            row = conn.execute(
                "SELECT status FROM pipeline_runs WHERE triggered_by='manual_extract' ORDER BY id DESC LIMIT 1"
            ).fetchone()
        assert row is not None
        assert row['status'] == 'complete', f"Expected status='complete', got {row['status']!r}"


def test_manual_extract_progress_includes_run_id(monkeypatch):
    """GET /api/operations/interpret/<task_id>/progress includes run_id field."""
    import sys, os
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))
    import app_globals, routes.operations as ops_mod, routes.pipeline as pipeline_mod
    from infra.db import MinerDB
    import importlib, run_web, tempfile

    with tempfile.TemporaryDirectory() as tmpdir:
        db = MinerDB(os.path.join(tmpdir, 'test.db'))
        db.insert_company({'ticker': 'MARA', 'name': 'MARA', 'tier': 1,
                            'ir_url': 'https://example.com', 'pr_base_url': 'https://example.com',
                            'cik': '0001437491', 'active': 1})
        app_globals._db = db
        importlib.reload(run_web)
        flask_app = run_web.create_app()
        flask_app.config['TESTING'] = True
        client = flask_app.test_client()

        def _noop_run_extraction_phase(db, run_id, tickers, **kwargs):
            return {'total_reports': 0, 'processed': 0, 'data_points': 0,
                    'errors': 0, 'keyword_gated': 0, 'review_flagged': 0, 'report_done_count': 0}

        monkeypatch.setattr(pipeline_mod, 'run_extraction_phase', _noop_run_extraction_phase)

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
        task_id = resp.get_json()['data']['task_id']

        progress_resp = client.get(f'/api/operations/interpret/{task_id}/progress')
        assert progress_resp.status_code == 200
        data = progress_resp.get_json()['data']
        assert 'run_id' in data, f"Progress should include run_id field, got keys: {list(data.keys())}"
        assert isinstance(data['run_id'], int), f"run_id should be an int, got {data['run_id']!r}"


def test_operations_uses_run_extraction_phase(monkeypatch):
    """POST /api/operations/interpret calls run_extraction_phase (not _extract_reports_for_ticker directly)."""
    import sys, os
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))
    import app_globals, routes.operations as ops_mod, routes.pipeline as pipeline_mod
    from infra.db import MinerDB
    import importlib, run_web, tempfile

    with tempfile.TemporaryDirectory() as tmpdir:
        db = MinerDB(os.path.join(tmpdir, 'test.db'))
        db.insert_company({'ticker': 'MARA', 'name': 'MARA', 'tier': 1,
                            'ir_url': 'https://example.com', 'pr_base_url': 'https://example.com',
                            'cik': '0001437491', 'active': 1})
        app_globals._db = db
        importlib.reload(run_web)
        flask_app = run_web.create_app()
        flask_app.config['TESTING'] = True
        client = flask_app.test_client()

        calls = []

        def _fake_run_extraction_phase(db, run_id, tickers, **kwargs):
            calls.append({'tickers': list(tickers), 'extract_workers': kwargs.get('extract_workers'),
                          'force_reextract': kwargs.get('force_reextract')})
            return {'total_reports': 0, 'processed': 0, 'data_points': 0,
                    'errors': 0, 'keyword_gated': 0, 'review_flagged': 0, 'report_done_count': 0}

        monkeypatch.setattr(pipeline_mod, 'run_extraction_phase', _fake_run_extraction_phase)

        class _ImmediateThread:
            def __init__(self, target=None, args=(), daemon=False, name=None):
                self._target = target
            def start(self):
                if self._target: self._target()

        monkeypatch.setattr(ops_mod, '_active_tickers', set())
        monkeypatch.setattr(ops_mod.threading, 'Thread', _ImmediateThread)

        resp = client.post('/api/operations/interpret', json={
            'tickers': ['MARA'], 'warm_model': False, 'extract_workers': 4,
        })
        assert resp.status_code == 200

        assert len(calls) == 1, f"Expected run_extraction_phase called once, got: {calls}"
        assert calls[0]['extract_workers'] == 4


# ── Requeue Missing ────────────────────────────────────────────────────────────

def test_requeue_missing_resets_reports_and_triggers_extraction(monkeypatch, tmp_path):
    """
    POST /api/operations/requeue-missing:
    - Finds reports with extraction_status='done' and no data_point for given metrics
    - Resets each to 'pending'
    - Triggers extraction (run_extraction_phase)
    - Returns task_id + requeued_count
    """
    import sys
    import os
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))
    import app_globals
    import routes.operations as ops_mod
    import routes.pipeline as pipeline_mod
    from infra.db import MinerDB
    import importlib, run_web, tempfile

    with tempfile.TemporaryDirectory() as tmpdir:
        db = MinerDB(os.path.join(tmpdir, 'test.db'))
        # MARA is pre-seeded via companies.json sync
        app_globals._db = db
        importlib.reload(run_web)
        flask_app = run_web.create_app()
        flask_app.config['TESTING'] = True
        client = flask_app.test_client()

        # Insert a done report with no data_points for production_btc
        report_id = db.insert_report({
            'ticker': 'MARA',
            'report_date': '2025-03-01',
            'published_date': None,
            'source_type': 'archive_html',
            'source_url': None,
            'raw_text': 'MARA bitcoin mined 700 BTC in March 2025.',
            'parsed_at': '2025-03-03T12:00:00',
        })
        db.mark_report_extracted(report_id)

        calls = []

        def _fake_run_extraction_phase(db, run_id, tickers, **kwargs):
            calls.append({'tickers': list(tickers), 'prebuilt_batches': kwargs.get('prebuilt_batches')})
            return {'processed': 0, 'data_points': 0, 'errors': 0}

        monkeypatch.setattr(pipeline_mod, 'run_extraction_phase', _fake_run_extraction_phase)

        class _ImmediateThread:
            def __init__(self, target=None, args=(), daemon=False, name=None):
                self._target = target
            def start(self):
                if self._target: self._target()

        monkeypatch.setattr(ops_mod, '_active_tickers', set())
        monkeypatch.setattr(ops_mod.threading, 'Thread', _ImmediateThread)

        resp = client.post('/api/operations/requeue-missing', json={
            'metrics': ['production_btc'],
            'tickers': ['MARA'],
        })
        assert resp.status_code == 200, resp.data
        body = resp.get_json()
        assert body['success'] is True
        assert body['data']['requeued_count'] >= 1
        assert 'task_id' in body['data']

        # The report should have been reset to pending before extraction
        refreshed = db.get_report(report_id)
        # After the fake extraction, status is still 'pending' (fake doesn't change it)
        assert refreshed['extraction_status'] == 'pending'

        # run_extraction_phase must have been called
        assert len(calls) == 1


def test_requeue_missing_no_metrics_returns_400(client):
    """POST /api/operations/requeue-missing without metrics returns 400."""
    resp = client.post('/api/operations/requeue-missing', json={})
    assert resp.status_code == 400
    body = resp.get_json()
    assert body['success'] is False
