"""Unit tests for LLM crawl routes (POST /api/crawl/start, GET /api/crawl/status, etc.)."""
import threading
import unittest
from unittest.mock import MagicMock, patch


class TestCrawlStartRoute(unittest.TestCase):
    """POST /api/crawl/start — validate request shape and task creation."""

    def _make_app(self):
        import sys, os
        sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))
        from flask import Flask
        from routes.crawl import bp
        app = Flask(__name__)
        app.register_blueprint(bp)
        app.config['TESTING'] = True
        return app

    def test_start_crawl_returns_task_id(self):
        app = self._make_app()
        mock_progress = MagicMock()
        mock_progress.status = 'pending'
        mock_progress.snapshot.return_value = {
            'ticker': 'MARA', 'status': 'pending',
            'pages_fetched': 0, 'docs_stored': 0,
            'docs_skipped': 0, 'error': None,
            'started_at': None, 'finished_at': None,
            'log': [],
        }
        with patch('scrapers.llm_crawler.start_crawl', return_value={'MARA': mock_progress}) as mock_start:
            with app.test_client() as client:
                resp = client.post('/api/crawl/start', json={'tickers': ['MARA']})
        self.assertEqual(resp.status_code, 202)
        data = resp.get_json()
        self.assertTrue(data['success'])
        self.assertIn('task_id', data['data'])
        self.assertIn('tickers', data['data'])
        mock_start.assert_called_once()

    def test_start_crawl_missing_tickers_returns_400(self):
        app = self._make_app()
        with app.test_client() as client:
            resp = client.post('/api/crawl/start', json={})
        self.assertEqual(resp.status_code, 400)
        data = resp.get_json()
        self.assertFalse(data['success'])

    def test_start_crawl_empty_tickers_returns_400(self):
        app = self._make_app()
        with app.test_client() as client:
            resp = client.post('/api/crawl/start', json={'tickers': []})
        self.assertEqual(resp.status_code, 400)

    def test_start_crawl_invalid_ticker_list_returns_400(self):
        app = self._make_app()
        with app.test_client() as client:
            resp = client.post('/api/crawl/start', json={'tickers': 'MARA'})
        self.assertEqual(resp.status_code, 400)


class TestCrawlStatusRoute(unittest.TestCase):
    """GET /api/crawl/status — returns per-ticker summary for active session."""

    def _make_app(self):
        import sys, os
        sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))
        from flask import Flask
        from routes.crawl import bp
        app = Flask(__name__)
        app.register_blueprint(bp)
        app.config['TESTING'] = True
        return app

    def test_status_returns_list(self):
        app = self._make_app()
        mock_snap = {
            'ticker': 'MARA', 'status': 'running',
            'pages_fetched': 3, 'docs_stored': 1,
            'docs_skipped': 0, 'error': None,
            'started_at': '2026-03-06T00:00:00Z', 'finished_at': None,
            'log': ['[00:00:01] fetched IR listing'],
        }
        with patch('scrapers.llm_crawler.get_crawl_status', return_value=[mock_snap]):
            with app.test_client() as client:
                resp = client.get('/api/crawl/status')
        self.assertEqual(resp.status_code, 200)
        data = resp.get_json()
        self.assertTrue(data['success'])
        self.assertIsInstance(data['data'], list)
        self.assertEqual(data['data'][0]['ticker'], 'MARA')

    def test_status_empty_when_no_session(self):
        app = self._make_app()
        with patch('scrapers.llm_crawler.get_crawl_status', return_value=[]):
            with app.test_client() as client:
                resp = client.get('/api/crawl/status')
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(resp.get_json()['data'], [])


class TestCrawlProgressRoute(unittest.TestCase):
    """GET /api/crawl/<task_id>/progress — per-task log stream."""

    def _make_app(self):
        import sys, os
        sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))
        from flask import Flask
        from routes.crawl import bp
        app = Flask(__name__)
        app.register_blueprint(bp)
        app.config['TESTING'] = True
        return app

    def test_progress_returns_snapshot(self):
        app = self._make_app()
        mock_snap = {
            'ticker': 'RIOT', 'status': 'complete',
            'pages_fetched': 10, 'docs_stored': 5,
            'docs_skipped': 2, 'error': None,
            'started_at': '2026-03-06T00:00:00Z',
            'finished_at': '2026-03-06T00:05:00Z',
            'log': ['[00:05:00] crawl complete'],
        }
        task_state = {'RIOT': mock_snap}
        with patch('scrapers.llm_crawler.get_crawl_task', return_value=task_state):
            with app.test_client() as client:
                resp = client.get('/api/crawl/task-123/progress')
        self.assertEqual(resp.status_code, 200)
        data = resp.get_json()
        self.assertTrue(data['success'])
        tickers = {t['ticker']: t for t in data['data']}
        self.assertIn('RIOT', tickers)
        self.assertEqual(tickers['RIOT']['status'], 'complete')

    def test_progress_unknown_task_returns_404(self):
        app = self._make_app()
        with patch('scrapers.llm_crawler.get_crawl_task', return_value=None):
            with app.test_client() as client:
                resp = client.get('/api/crawl/unknown-task/progress')
        self.assertEqual(resp.status_code, 404)


class TestCrawlProgressModel(unittest.TestCase):
    """CrawlProgress — thread-safe state and snapshot."""

    def test_initial_state(self):
        from scrapers.llm_crawler import CrawlProgress
        p = CrawlProgress('MARA')
        snap = p.snapshot()
        self.assertEqual(snap['ticker'], 'MARA')
        self.assertEqual(snap['status'], 'pending')
        self.assertEqual(snap['pages_fetched'], 0)
        self.assertEqual(snap['docs_stored'], 0)
        self.assertEqual(snap['log'], [])

    def test_add_log_prepends(self):
        from scrapers.llm_crawler import CrawlProgress
        p = CrawlProgress('RIOT')
        p.add_log('first')
        p.add_log('second')
        snap = p.snapshot()
        # Most recent entry is first
        self.assertIn('second', snap['log'][0])
        self.assertIn('first', snap['log'][1])

    def test_log_capped_at_150(self):
        from scrapers.llm_crawler import CrawlProgress
        p = CrawlProgress('MARA')
        for i in range(200):
            p.add_log(f'msg {i}')
        self.assertLessEqual(len(p.snapshot()['log']), 150)

    def test_snapshot_is_copy(self):
        from scrapers.llm_crawler import CrawlProgress
        p = CrawlProgress('CLSK')
        snap1 = p.snapshot()
        p.add_log('new entry')
        snap2 = p.snapshot()
        # snap1 is a frozen copy — does not see new entry
        self.assertEqual(len(snap1['log']), 0)
        self.assertEqual(len(snap2['log']), 1)


class TestStartCrawlFunction(unittest.TestCase):
    """scrapers.llm_crawler.start_crawl() — task registry behavior."""

    def test_start_crawl_returns_progress_per_ticker(self):
        from scrapers.llm_crawler import start_crawl
        import uuid
        task_id = str(uuid.uuid4())
        # Patch thread spawn so we don't actually run the LLM
        with patch('scrapers.llm_crawler._spawn_crawl_thread'):
            result = start_crawl(['MARA', 'RIOT'], task_id=task_id)
        self.assertIn('MARA', result)
        self.assertIn('RIOT', result)

    def test_start_crawl_registers_task(self):
        from scrapers.llm_crawler import start_crawl, get_crawl_task
        import uuid
        task_id = str(uuid.uuid4())
        with patch('scrapers.llm_crawler._spawn_crawl_thread'):
            start_crawl(['BITF'], task_id=task_id)
        task = get_crawl_task(task_id)
        self.assertIsNotNone(task)
        self.assertIn('BITF', task)

    def test_get_crawl_task_unknown_returns_none(self):
        from scrapers.llm_crawler import get_crawl_task
        self.assertIsNone(get_crawl_task('no-such-task'))


if __name__ == '__main__':
    unittest.main()
