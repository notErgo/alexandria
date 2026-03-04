"""
Tests for re-audit trigger: POST /api/ingest/reaudit.

TDD: tests written before implementation.
"""
import pytest
from unittest.mock import patch, MagicMock
from infra.db import MinerDB


@pytest.fixture
def app(db_with_company, tmp_path):
    import sys, os
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))
    import app_globals
    app_globals._db = db_with_company

    import importlib, run_web
    importlib.reload(run_web)
    flask_app = run_web.create_app()
    flask_app.config['TESTING'] = True
    return flask_app


@pytest.fixture
def client(app):
    return app.test_client()


@pytest.fixture(autouse=True)
def clear_running_tasks():
    """Clear _running_tasks between tests to prevent state leakage."""
    import routes.reports as reports_mod
    with reports_mod._tasks_lock:
        reports_mod._running_tasks.discard('reaudit')
    yield
    with reports_mod._tasks_lock:
        reports_mod._running_tasks.discard('reaudit')


class TestReauditRoute:
    def test_reaudit_returns_202_and_task_id(self, client):
        """POST /api/ingest/reaudit should start a background task and return task_id."""
        with patch('routes.reports._run_reaudit') as mock_run:
            resp = client.post('/api/ingest/reaudit')
        assert resp.status_code == 202
        data = resp.get_json()
        assert data['success'] is True
        assert 'task_id' in data['data']
        assert data['data']['status'] == 'queued'

    def test_reaudit_starts_background_thread(self, client):
        """Re-audit should launch a daemon thread."""
        thread_started = []
        original_start = __import__('threading').Thread.start

        def patched_start(self):
            thread_started.append(self.name)
            # Don't actually start — prevent real ingest
            pass

        with patch('threading.Thread.start', patched_start):
            resp = client.post('/api/ingest/reaudit')
        assert resp.status_code == 202
        assert len(thread_started) > 0

    def test_reaudit_rejects_concurrent_run(self, client):
        """If a reaudit is already running, return 409."""
        # Start first reaudit (patch to prevent actual execution)
        with patch('routes.reports._run_reaudit'):
            client.post('/api/ingest/reaudit')

        # Manually mark reaudit as running
        import routes.reports as reports_mod
        with reports_mod._tasks_lock:
            reports_mod._running_tasks.add('reaudit')

        resp = client.post('/api/ingest/reaudit')
        assert resp.status_code == 409
        data = resp.get_json()
        assert data['success'] is False

        # Cleanup
        with reports_mod._tasks_lock:
            reports_mod._running_tasks.discard('reaudit')

    def test_reaudit_progress_trackable(self, client):
        """Progress endpoint should return state for reaudit task_id."""
        with patch('routes.reports._run_reaudit'):
            resp = client.post('/api/ingest/reaudit')
        task_id = resp.get_json()['data']['task_id']

        # Poll progress (it will show 'queued' since _run_reaudit was mocked)
        resp2 = client.get(f'/api/ingest/{task_id}/progress')
        assert resp2.status_code == 200
        data = resp2.get_json()
        assert data['success'] is True
        assert 'status' in data['data']

    def test_reaudit_scope_ticker_optional(self, client):
        """POST with ticker body param should accept it without error."""
        with patch('routes.reports._run_reaudit'):
            resp = client.post('/api/ingest/reaudit', json={'ticker': 'MARA'})
        assert resp.status_code == 202

    def test_reaudit_scope_without_body(self, client):
        """POST with no body (full re-audit) should also return 202."""
        with patch('routes.reports._run_reaudit'):
            resp = client.post('/api/ingest/reaudit')
        assert resp.status_code == 202
