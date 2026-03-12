"""Tests for reports ingest routes — auto_extract wiring via run_extraction_phase."""
import os
import sys
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'tests'))


def test_auto_extract_ir_uses_run_extraction_phase(monkeypatch, tmp_path):
    """_run_ir_ingest with auto_extract=True calls run_extraction_phase, not bare extract_report loop."""
    from infra.db import MinerDB
    import app_globals, routes.pipeline as pipeline_mod

    db = MinerDB(str(tmp_path / 'auto.db'))
    db.insert_company({'ticker': 'MARA', 'name': 'MARA', 'tier': 1,
                       'ir_url': 'https://example.com', 'pr_base_url': 'https://example.com',
                       'cik': '0001437491', 'active': 1, 'scraper_mode': 'skip'})
    app_globals._db = db

    calls = []
    def _fake_run_extraction_phase(db, run_id, tickers, registry, **kwargs):
        calls.append({'tickers': list(tickers), 'source_types': kwargs.get('source_types')})
        return {'total_reports': 0, 'processed': 0, 'data_points': 0,
                'errors': 0, 'keyword_gated': 0, 'review_flagged': 0, 'report_done_count': 0}

    monkeypatch.setattr(pipeline_mod, 'run_extraction_phase', _fake_run_extraction_phase)

    # IRScraper.scrape_company must be patched: insert_company uses INSERT OR IGNORE, so
    # sync_companies_from_config() (run on MinerDB init) sets MARA to scraper_mode='rss'
    # from companies.json before the test's insert_company call, which is then silently ignored.
    # Without this patch, _run_ir_ingest makes real HTTP requests to the MARA RSS feed.
    from scrapers.ir_scraper import IRScraper
    from miner_types import IngestSummary
    monkeypatch.setattr(IRScraper, 'scrape_company', lambda self, company: IngestSummary())

    import uuid
    task_id = str(uuid.uuid4())
    import routes.reports as reports_mod
    reports_mod._run_ir_ingest(task_id, auto_extract=True, warm_model=False, tickers=['MARA'])

    assert calls, "run_extraction_phase must be called when auto_extract=True"
    from config import MONTHLY_EXTRACTION_SOURCE_TYPES
    assert set(calls[0]['source_types']) == set(MONTHLY_EXTRACTION_SOURCE_TYPES), \
        "IR auto_extract must be restricted to MONTHLY_EXTRACTION_SOURCE_TYPES"
