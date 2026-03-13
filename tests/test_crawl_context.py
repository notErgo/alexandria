"""Tests for scrapers.crawl_context — bitcoin lower-bound detection and gap analysis."""
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

import pytest
from unittest.mock import MagicMock


def _make_db(earliest_period=None, covered=None, gaps=None, btc_first_filing_date=None):
    db = MagicMock()
    db.get_earliest_bitcoin_report_period.return_value = earliest_period
    db.get_covered_periods.return_value = covered or []
    db.get_missing_periods.return_value = gaps or []
    db.get_btc_first_filing_date.return_value = btc_first_filing_date
    return db


# ---------------------------------------------------------------------------
# find_bitcoin_lower_bound
# ---------------------------------------------------------------------------
class TestFindBitcoinLowerBound:

    def test_returns_none_when_no_reports(self):
        from scrapers.crawl_context import find_bitcoin_lower_bound
        db = _make_db(earliest_period=None)
        assert find_bitcoin_lower_bound('MARA', db) is None

    def test_returns_earliest_period_from_db(self):
        from scrapers.crawl_context import find_bitcoin_lower_bound
        db = _make_db(earliest_period='2021-06-01')
        assert find_bitcoin_lower_bound('MARA', db) == '2021-06-01'

    def test_passes_ticker_to_db(self):
        from scrapers.crawl_context import find_bitcoin_lower_bound
        db = _make_db(earliest_period='2020-01-01')
        find_bitcoin_lower_bound('RIOT', db)
        db.get_earliest_bitcoin_report_period.assert_called_once_with('RIOT')


# ---------------------------------------------------------------------------
# build_crawl_context
# ---------------------------------------------------------------------------
class TestBuildCrawlContext:

    def test_returns_none_lower_bound_when_no_data(self):
        from scrapers.crawl_context import build_crawl_context
        db = _make_db(earliest_period=None)
        ctx = build_crawl_context('MARA', db)
        assert ctx['lower_bound'] is None
        assert ctx['gaps'] == []
        assert ctx['covered'] == []

    def test_returns_lower_bound_from_db(self):
        from scrapers.crawl_context import build_crawl_context
        db = _make_db(
            earliest_period='2021-06-01',
            covered=['2021-06-01', '2021-07-01'],
            gaps=['2021-08-01'],
        )
        ctx = build_crawl_context('MARA', db)
        assert ctx['lower_bound'] == '2021-06-01'

    def test_includes_gaps_when_lower_bound_exists(self):
        from scrapers.crawl_context import build_crawl_context
        db = _make_db(
            earliest_period='2021-06-01',
            covered=['2021-06-01'],
            gaps=['2021-07-01', '2021-08-01'],
        )
        ctx = build_crawl_context('MARA', db)
        assert '2021-07-01' in ctx['gaps']
        assert '2021-08-01' in ctx['gaps']

    def test_includes_covered_periods(self):
        from scrapers.crawl_context import build_crawl_context
        db = _make_db(
            earliest_period='2022-01-01',
            covered=['2022-01-01', '2022-02-01'],
            gaps=[],
        )
        ctx = build_crawl_context('MARA', db)
        assert '2022-01-01' in ctx['covered']
        assert '2022-02-01' in ctx['covered']

    def test_no_db_calls_for_gaps_when_no_lower_bound(self):
        from scrapers.crawl_context import build_crawl_context
        db = _make_db(earliest_period=None)
        build_crawl_context('MARA', db)
        db.get_covered_periods.assert_not_called()
        db.get_missing_periods.assert_not_called()


# ---------------------------------------------------------------------------
# format_context_block
# ---------------------------------------------------------------------------
class TestFormatContextBlock:

    def test_empty_string_when_no_lower_bound(self):
        from scrapers.crawl_context import format_context_block
        ctx = {'lower_bound': None, 'gaps': [], 'covered': []}
        assert format_context_block('MARA', ctx) == ''

    def test_includes_lower_bound_date(self):
        from scrapers.crawl_context import format_context_block
        ctx = {'lower_bound': '2021-06-01', 'gaps': [], 'covered': []}
        block = format_context_block('MARA', ctx)
        assert '2021-06' in block

    def test_includes_gaps_in_block(self):
        from scrapers.crawl_context import format_context_block
        ctx = {
            'lower_bound': '2021-06-01',
            'gaps': ['2021-08-01', '2021-09-01'],
            'covered': [],
        }
        block = format_context_block('MARA', ctx)
        assert '2021-08' in block
        assert '2021-09' in block

    def test_includes_do_not_search_before_instruction(self):
        from scrapers.crawl_context import format_context_block
        ctx = {'lower_bound': '2021-06-01', 'gaps': [], 'covered': []}
        block = format_context_block('MARA', ctx)
        assert '2021-06' in block
        assert 'before' in block.lower()

    def test_covered_periods_mentioned(self):
        from scrapers.crawl_context import format_context_block
        ctx = {
            'lower_bound': '2022-01-01',
            'gaps': [],
            'covered': ['2022-01-01', '2022-02-01'],
        }
        block = format_context_block('MARA', ctx)
        assert '2022-01' in block or '2022-02' in block

    def test_no_gaps_section_when_fully_covered(self):
        from scrapers.crawl_context import format_context_block
        ctx = {
            'lower_bound': '2022-01-01',
            'gaps': [],
            'covered': ['2022-01-01'],
        }
        block = format_context_block('MARA', ctx)
        assert 'gap' not in block.lower() or '2022-01' in block


# ---------------------------------------------------------------------------
# DB method: get_earliest_bitcoin_report_period (integration-style, uses real DB)
# ---------------------------------------------------------------------------
class TestGetEarliestBitcoinReportPeriod:

    def test_returns_none_when_no_reports(self, tmp_path):
        from infra.db import MinerDB
        db = MinerDB(str(tmp_path / 'test.db'))
        result = db.get_earliest_bitcoin_report_period('MARA')
        assert result is None

    def test_returns_none_when_reports_have_no_bitcoin_keyword(self, tmp_path):
        from infra.db import MinerDB
        db = MinerDB(str(tmp_path / 'test.db'))
        db.insert_report({'ticker': 'MARA', 'source_url': 'http://x.com/1', 'raw_text': 'Quarterly results for Marathon Patent Group.', 'source_type': 'edgar_10k', 'covering_period': '2019-12-01', 'report_date': '2019-12-01', 'published_date': None, 'parsed_at': None})
        result = db.get_earliest_bitcoin_report_period('MARA')
        assert result is None

    def test_returns_earliest_period_with_bitcoin(self, tmp_path):
        from infra.db import MinerDB
        db = MinerDB(str(tmp_path / 'test.db'))
        db.insert_report({'ticker': 'MARA', 'source_url': 'http://x.com/2', 'raw_text': 'Bitcoin mined: 100 BTC in March.', 'source_type': 'edgar_8k', 'covering_period': '2021-03-01', 'report_date': '2021-03-01', 'published_date': None, 'parsed_at': None})
        db.insert_report({'ticker': 'MARA', 'source_url': 'http://x.com/3', 'raw_text': 'BTC mined this quarter: 250.', 'source_type': 'edgar_10q', 'covering_period': '2021-06-01', 'report_date': '2021-06-01', 'published_date': None, 'parsed_at': None})
        result = db.get_earliest_bitcoin_report_period('MARA')
        assert result == '2021-03-01'

    def test_matches_hashrate_keyword(self, tmp_path):
        from infra.db import MinerDB
        db = MinerDB(str(tmp_path / 'test.db'))
        db.insert_report({'ticker': 'RIOT', 'source_url': 'http://x.com/4', 'raw_text': 'Deployed 1.5 exahash of hashrate capacity.', 'source_type': 'edgar_10k', 'covering_period': '2020-12-01', 'report_date': '2020-12-01', 'published_date': None, 'parsed_at': None})
        result = db.get_earliest_bitcoin_report_period('RIOT')
        assert result == '2020-12-01'

    def test_ignores_other_tickers(self, tmp_path):
        from infra.db import MinerDB
        db = MinerDB(str(tmp_path / 'test.db'))
        db.insert_report({'ticker': 'RIOT', 'source_url': 'http://x.com/5', 'raw_text': 'Bitcoin production update.', 'source_type': 'edgar_8k', 'covering_period': '2021-01-01', 'report_date': '2021-01-01', 'published_date': None, 'parsed_at': None})
        result = db.get_earliest_bitcoin_report_period('MARA')
        assert result is None


# ---------------------------------------------------------------------------
# DB method: get_covered_periods
# ---------------------------------------------------------------------------
class TestGetCoveredPeriods:

    def test_returns_empty_when_no_data_points(self, tmp_path):
        from infra.db import MinerDB
        db = MinerDB(str(tmp_path / 'test.db'))
        result = db.get_covered_periods('MARA')
        assert result == []

    def test_returns_distinct_periods_with_data(self, tmp_path):
        from infra.db import MinerDB
        db = MinerDB(str(tmp_path / 'test.db'))
        db.insert_data_point({
            'ticker': 'MARA', 'period': '2022-01-01',
            'metric': 'production_btc', 'value': 100.0,
            'source_type': 'ir_press_release', 'extraction_method': 'regex', 'confidence': 0.9, 'report_id': None, 'unit': 'btc', 'source_snippet': None,
        })
        db.insert_data_point({
            'ticker': 'MARA', 'period': '2022-01-01',
            'metric': 'hodl_btc', 'value': 500.0,
            'source_type': 'ir_press_release', 'extraction_method': 'regex', 'confidence': 0.9, 'report_id': None, 'unit': 'btc', 'source_snippet': None,
        })
        db.insert_data_point({
            'ticker': 'MARA', 'period': '2022-02-01',
            'metric': 'production_btc', 'value': 110.0,
            'source_type': 'ir_press_release', 'extraction_method': 'regex', 'confidence': 0.9, 'report_id': None, 'unit': 'btc', 'source_snippet': None,
        })
        result = db.get_covered_periods('MARA')
        assert sorted(result) == ['2022-01-01', '2022-02-01']

    def test_ignores_other_tickers(self, tmp_path):
        from infra.db import MinerDB
        db = MinerDB(str(tmp_path / 'test.db'))
        db.insert_data_point({
            'ticker': 'RIOT', 'period': '2022-01-01',
            'metric': 'production_btc', 'value': 50.0,
            'source_type': 'ir_press_release', 'extraction_method': 'regex', 'confidence': 0.9, 'report_id': None, 'unit': 'btc', 'source_snippet': None,
        })
        result = db.get_covered_periods('MARA')
        assert result == []
