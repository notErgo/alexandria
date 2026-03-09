"""TDD tests for EDGAR-only canonical ingestion pivot.

All tests in this file should FAIL before the code changes are applied
and PASS after.
"""
import importlib
import os
import sys

import pytest
from unittest.mock import patch, MagicMock

from infra.db import MinerDB


# ── Shared app fixture ────────────────────────────────────────────────────────

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


# ── TestPipelineDefaultExcludesIR ─────────────────────────────────────────────

class TestPipelineDefaultExcludesIR:

    def _patch_thread(self, monkeypatch):
        import routes.pipeline as pipeline_mod

        class _DummyThread:
            def __init__(self, target=None, args=(), daemon=False, name=None):
                pass

            def start(self):
                return None

        monkeypatch.setattr(pipeline_mod.threading, 'Thread', _DummyThread)

    def test_pipeline_start_default_include_ir_is_false(self, client, monkeypatch):
        """POST with empty body stores include_ir=False."""
        self._patch_thread(monkeypatch)

        resp = client.post('/api/pipeline/overnight/start', json={'tickers': ['MARA']})
        assert resp.status_code == 202
        run_id = resp.get_json()['data']['run_id']

        status_resp = client.get(f'/api/pipeline/overnight/{run_id}/status')
        config = status_resp.get_json()['data']['run']['config']
        assert config['include_ir'] is False

    def test_pipeline_explicit_false_accepted(self, client, monkeypatch):
        """POST with include_ir=False stores False."""
        self._patch_thread(monkeypatch)

        resp = client.post('/api/pipeline/overnight/start', json={
            'tickers': ['MARA'],
            'include_ir': False,
        })
        assert resp.status_code == 202
        run_id = resp.get_json()['data']['run_id']

        status_resp = client.get(f'/api/pipeline/overnight/{run_id}/status')
        config = status_resp.get_json()['data']['run']['config']
        assert config['include_ir'] is False

    def test_pipeline_explicit_true_still_accepted(self, client, monkeypatch):
        """POST with include_ir=True stores True (opt-in still works)."""
        self._patch_thread(monkeypatch)

        resp = client.post('/api/pipeline/overnight/start', json={
            'tickers': ['MARA'],
            'include_ir': True,
        })
        assert resp.status_code == 202
        run_id = resp.get_json()['data']['run_id']

        status_resp = client.get(f'/api/pipeline/overnight/{run_id}/status')
        config = status_resp.get_json()['data']['run']['config']
        assert config['include_ir'] is True


# ── TestScrapeWorkerEDGAROnly ──────────────────────────────────────────────────

class TestScrapeWorkerEDGAROnly:

    def test_execute_scrape_does_not_call_ir_scraper(self, db_with_active_company):
        """_execute_scrape must not construct IRScraper."""
        from scrapers.scrape_worker import ScrapeWorker

        job = db_with_active_company.enqueue_scrape_job('MARA', 'historic')
        worker = ScrapeWorker(db_with_active_company)

        def _ir_scraper_boom(*args, **kwargs):
            raise AssertionError("IRScraper must not be constructed in EDGAR-only mode")

        with patch('scrapers.ir_scraper.IRScraper', side_effect=_ir_scraper_boom):
            with patch('scrapers.edgar_connector.EdgarConnector') as mock_cls:
                mock_edgar = MagicMock()
                mock_cls.return_value = mock_edgar
                mock_edgar.fetch_all_filings.return_value = None
                worker._execute_scrape(job)

        mock_edgar.fetch_all_filings.assert_called_once()

    def test_execute_scrape_domestic_filing_regime(self, db_with_active_company):
        """fetch_all_filings called with filing_regime='domestic'."""
        from scrapers.scrape_worker import ScrapeWorker

        # Patch get_company to return a company with filing_regime='domestic'
        company_data = {
            'ticker': 'MARA', 'cik': '0001437491',
            'filing_regime': 'domestic', 'active': 1,
        }
        job = db_with_active_company.enqueue_scrape_job('MARA', 'historic')
        worker = ScrapeWorker(db_with_active_company)

        with patch.object(worker._db, 'get_company', return_value=company_data):
            with patch('scrapers.edgar_connector.EdgarConnector') as mock_cls:
                mock_edgar = MagicMock()
                mock_cls.return_value = mock_edgar
                mock_edgar.fetch_all_filings.return_value = None
                worker._execute_scrape(job)

        assert mock_edgar.fetch_all_filings.call_args.kwargs.get('filing_regime') == 'domestic'

    def test_execute_scrape_canadian_filing_regime(self, db_with_active_company):
        """fetch_all_filings called with filing_regime='canadian'."""
        from scrapers.scrape_worker import ScrapeWorker

        company_data = {
            'ticker': 'MARA', 'cik': '0001437491',
            'filing_regime': 'canadian', 'active': 1,
        }
        job = db_with_active_company.enqueue_scrape_job('MARA', 'historic')
        worker = ScrapeWorker(db_with_active_company)

        with patch.object(worker._db, 'get_company', return_value=company_data):
            with patch('scrapers.edgar_connector.EdgarConnector') as mock_cls:
                mock_edgar = MagicMock()
                mock_cls.return_value = mock_edgar
                mock_edgar.fetch_all_filings.return_value = None
                worker._execute_scrape(job)

        assert mock_edgar.fetch_all_filings.call_args.kwargs.get('filing_regime') == 'canadian'

    def test_execute_scrape_foreign_filing_regime(self, db_with_active_company):
        """fetch_all_filings called with filing_regime='foreign'."""
        from scrapers.scrape_worker import ScrapeWorker

        company_data = {
            'ticker': 'MARA', 'cik': '0001437491',
            'filing_regime': 'foreign', 'active': 1,
        }
        job = db_with_active_company.enqueue_scrape_job('MARA', 'historic')
        worker = ScrapeWorker(db_with_active_company)

        with patch.object(worker._db, 'get_company', return_value=company_data):
            with patch('scrapers.edgar_connector.EdgarConnector') as mock_cls:
                mock_edgar = MagicMock()
                mock_cls.return_value = mock_edgar
                mock_edgar.fetch_all_filings.return_value = None
                worker._execute_scrape(job)

        assert mock_edgar.fetch_all_filings.call_args.kwargs.get('filing_regime') == 'foreign'

    def test_execute_scrape_no_cik_skips_edgar(self, db_with_active_company):
        """Company with cik=None: fetch_all_filings never called, status set to ok."""
        from scrapers.scrape_worker import ScrapeWorker

        # Set cik=None by patching get_company
        company_data = {
            'ticker': 'MARA', 'cik': None,
            'filing_regime': 'domestic', 'active': 1,
        }
        job = db_with_active_company.enqueue_scrape_job('MARA', 'historic')
        worker = ScrapeWorker(db_with_active_company)

        with patch.object(worker._db, 'get_company', return_value=company_data):
            with patch('scrapers.edgar_connector.EdgarConnector') as mock_cls:
                mock_edgar = MagicMock()
                mock_cls.return_value = mock_edgar
                worker._execute_scrape(job)

        mock_edgar.fetch_all_filings.assert_not_called()
        # scraper_status is set on the real DB
        assert db_with_active_company.get_company('MARA')['scraper_status'] == 'ok'

    def test_execute_scrape_sets_last_scrape_at(self, db_with_active_company):
        """After success, last_scrape_at is set on the company."""
        from scrapers.scrape_worker import ScrapeWorker

        job = db_with_active_company.enqueue_scrape_job('MARA', 'historic')
        worker = ScrapeWorker(db_with_active_company)

        with patch('scrapers.ir_scraper.IRScraper'):
            with patch('scrapers.edgar_connector.EdgarConnector') as mock_cls:
                mock_edgar = MagicMock()
                mock_cls.return_value = mock_edgar
                mock_edgar.fetch_all_filings.return_value = None
                worker._execute_scrape(job)

        assert db_with_active_company.get_company('MARA')['last_scrape_at'] is not None

    def test_execute_scrape_sets_status_error_on_failure(self, db_with_active_company):
        """When fetch_all_filings raises, scraper_status='error' and last_scrape_error set."""
        from scrapers.scrape_worker import ScrapeWorker

        job = db_with_active_company.enqueue_scrape_job('MARA', 'historic')
        worker = ScrapeWorker(db_with_active_company)

        with patch('scrapers.ir_scraper.IRScraper'):
            with patch('scrapers.edgar_connector.EdgarConnector') as mock_cls:
                mock_edgar = MagicMock()
                mock_cls.return_value = mock_edgar
                mock_edgar.fetch_all_filings.side_effect = RuntimeError('EDGAR unavailable')
                with pytest.raises(RuntimeError):
                    worker._execute_scrape(job)

        company = db_with_active_company.get_company('MARA')
        assert company['scraper_status'] == 'error'
        assert 'EDGAR unavailable' in (company['last_scrape_error'] or '')


# ── TestCLIIRDeprecationWarning ───────────────────────────────────────────────

class TestCLIIRDeprecationWarning:

    def _make_args(self, source='ir', ticker=None):
        import types
        return types.SimpleNamespace(source=source, ticker=ticker, force=False)

    def test_cli_ingest_ir_emits_deprecation_warning(self):
        """cmd_ingest with source='ir' calls warnings.warn with DeprecationWarning."""
        import warnings
        import cli

        fake_db = MagicMock()
        mock_scraper_inst = MagicMock()
        mock_scraper_inst.scrape_company.return_value = MagicMock(
            reports_ingested=0, data_points_extracted=0, errors=0
        )

        with patch.object(cli, 'get_db', return_value=fake_db):
            with patch.object(cli, 'get_registry', return_value=MagicMock()):
                with patch('scrapers.ir_scraper.IRScraper', return_value=mock_scraper_inst):
                    with patch('json.load', return_value=[]):
                        with patch('builtins.open', MagicMock(
                            return_value=MagicMock(
                                __enter__=MagicMock(return_value=MagicMock()),
                                __exit__=MagicMock(return_value=False),
                            )
                        )):
                            with warnings.catch_warnings(record=True) as caught:
                                warnings.simplefilter('always')
                                cli.cmd_ingest(self._make_args(source='ir'))

        dep_warnings = [w for w in caught if issubclass(w.category, DeprecationWarning)]
        assert dep_warnings, "Expected DeprecationWarning from cli --source ir"
        assert any('deprecated' in str(w.message).lower() for w in dep_warnings)

    def test_cli_ingest_ir_still_executes(self):
        """cmd_ingest with source='ir' still calls IRScraper.scrape_company (non-breaking)."""
        import warnings
        import cli

        fake_db = MagicMock()
        mock_scraper_inst = MagicMock()
        mock_scraper_inst.scrape_company.return_value = MagicMock(
            reports_ingested=0, data_points_extracted=0, errors=0
        )

        company = {'ticker': 'MARA', 'active': True, 'scrape_mode': 'skip'}

        with patch.object(cli, 'get_db', return_value=fake_db):
            with patch.object(cli, 'get_registry', return_value=MagicMock()):
                with patch('scrapers.ir_scraper.IRScraper', return_value=mock_scraper_inst):
                    with patch('json.load', return_value=[company]):
                        with patch('builtins.open', MagicMock(
                            return_value=MagicMock(
                                __enter__=MagicMock(return_value=MagicMock()),
                                __exit__=MagicMock(return_value=False),
                            )
                        )):
                            with warnings.catch_warnings(record=True):
                                warnings.simplefilter('always')
                                cli.cmd_ingest(self._make_args(source='ir'))

        mock_scraper_inst.scrape_company.assert_called_once()


# ── TestOpsUIDefaults ─────────────────────────────────────────────────────────

class TestOpsUIDefaults:

    OPS_HTML = os.path.join(
        os.path.dirname(__file__), '..', 'templates', 'ops.html'
    )

    def _get_ops_html(self) -> str:
        with open(self.OPS_HTML, encoding='utf-8') as f:
            return f.read()

    def test_pipeline_include_ir_checkbox_unchecked_by_default(self):
        """The pipeline-include-ir checkbox must NOT have the 'checked' attribute."""
        import re
        html = self._get_ops_html()
        match = re.search(r'<input[^>]*id="pipeline-include-ir"[^>]*>', html)
        assert match, "Element id='pipeline-include-ir' not found in ops.html"
        element = match.group(0)
        assert 'checked' not in element, (
            f"pipeline-include-ir should NOT have 'checked', got: {element}"
        )

    def test_ops_html_ir_label_contains_deprecated(self):
        """The label near pipeline-include-ir must contain 'deprecated'."""
        html = self._get_ops_html()
        idx = html.find('id="pipeline-include-ir"')
        assert idx != -1, "Element id='pipeline-include-ir' not found"
        surrounding = html[max(0, idx - 50):idx + 300]
        assert 'deprecated' in surrounding.lower(), (
            f"Expected 'deprecated' near pipeline-include-ir. Got: {surrounding!r}"
        )


# ── TestRunAllIngestIRPhaseLogsDeprecation ────────────────────────────────────

class TestRunAllIngestIRPhaseLogsDeprecation:

    def test_run_all_ingest_ir_phase_logs_deprecation(self, tmp_path):
        """_run_all_ingest emits a deprecation warning log during the IR phase."""
        sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

        db = MinerDB(str(tmp_path / 'test.db'))
        import app_globals
        app_globals._db = db

        zero_summary = MagicMock(reports_ingested=0, data_points_extracted=0,
                                 review_flagged=0, errors=0)
        mock_ingestor = MagicMock()
        mock_ingestor.ingest_all.return_value = zero_summary
        mock_ir_scraper = MagicMock()
        mock_ir_scraper.scrape_company.return_value = zero_summary
        mock_connector = MagicMock()
        mock_connector.fetch_all_filings.return_value = zero_summary

        logged_warnings = []

        import routes.reports as reports_mod

        def capture_warning(msg, *args, **kwargs):
            logged_warnings.append(msg % args if args else msg)

        with patch.object(reports_mod.log, 'warning', side_effect=capture_warning):
            with patch('scrapers.archive_ingestor.ArchiveIngestor',
                       return_value=mock_ingestor):
                with patch('scrapers.ir_scraper.IRScraper',
                           return_value=mock_ir_scraper):
                    with patch('scrapers.edgar_connector.EdgarConnector',
                               return_value=mock_connector):
                        reports_mod._run_all_ingest('test-task-id')

        assert any('deprecated' in w.lower() for w in logged_warnings), (
            f"Expected 'deprecated' in warning logs during IR phase. Got: {logged_warnings}"
        )
