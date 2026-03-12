"""Tests for review_queue provenance and find_report_for_period ordering (schema v16, issues #5)."""
import os
import tempfile
import unittest


class TestReviewQueueStoresReportId(unittest.TestCase):
    """Issue #5: review_queue must persist report_id for provenance tracking."""

    def setUp(self):
        self.tmp = tempfile.NamedTemporaryFile(suffix='.db', delete=False)
        self.tmp.close()
        self.db_path = self.tmp.name

    def tearDown(self):
        os.unlink(self.db_path)

    def _make_db(self):
        from infra.db import MinerDB
        import sqlite3
        db = MinerDB(self.db_path)
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                "INSERT OR IGNORE INTO companies (ticker, name, tier, ir_url) "
                "VALUES ('MARA', 'MARA Holdings', 1, 'https://ir.mara.com')"
            )
        return db

    def _insert_report(self, db_path, ticker='MARA'):
        """Insert a real report row and return its id."""
        import sqlite3
        with sqlite3.connect(db_path) as conn:
            cur = conn.execute(
                "INSERT INTO reports (ticker, report_date, source_type, raw_text) "
                "VALUES (?, '2024-01-31', 'ir_press_release', 'test text')",
                (ticker,),
            )
            return cur.lastrowid

    def test_review_queue_stores_report_id(self):
        db = self._make_db()
        real_report_id = self._insert_report(self.db_path)
        rid = db.insert_review_item({
            'data_point_id': None,
            'ticker': 'MARA',
            'period': '2024-01-31',
            'metric': 'production_btc',
            'raw_value': '500',
            'confidence': 0.6,
            'source_snippet': 'produced 500 BTC',
            'status': 'PENDING',
            'llm_value': 500.0,
            'regex_value': None,
            'agreement_status': 'LLM_ONLY',
            'report_id': real_report_id,
        })
        row = db.get_review_item(rid)
        self.assertIsNotNone(row)
        self.assertEqual(row['report_id'], real_report_id)

    def test_review_queue_report_id_null_when_not_provided(self):
        db = self._make_db()
        rid = db.insert_review_item({
            'data_point_id': None,
            'ticker': 'MARA',
            'period': '2024-02-28',
            'metric': 'production_btc',
            'raw_value': '600',
            'confidence': 0.6,
            'source_snippet': None,
            'status': 'PENDING',
            'llm_value': None,
            'regex_value': 600.0,
            'agreement_status': 'REGEX_ONLY',
            # No report_id key
        })
        row = db.get_review_item(rid)
        self.assertIsNone(row.get('report_id'))

    def test_review_queue_report_id_explicit_none(self):
        db = self._make_db()
        rid = db.insert_review_item({
            'data_point_id': None,
            'ticker': 'MARA',
            'period': '2024-03-31',
            'metric': 'production_btc',
            'raw_value': '700',
            'confidence': 0.55,
            'source_snippet': None,
            'status': 'PENDING',
            'llm_value': None,
            'regex_value': 700.0,
            'agreement_status': 'REGEX_ONLY',
            'report_id': None,
        })
        row = db.get_review_item(rid)
        self.assertIsNone(row.get('report_id'))


class TestFindReportForPeriodOrdered(unittest.TestCase):
    """Issue #5: find_report_for_period must return latest report_date when multiple exist."""

    def setUp(self):
        self.tmp = tempfile.NamedTemporaryFile(suffix='.db', delete=False)
        self.tmp.close()
        self.db_path = self.tmp.name

    def tearDown(self):
        os.unlink(self.db_path)

    def _make_db_with_two_reports(self):
        from infra.db import MinerDB
        import sqlite3
        db = MinerDB(self.db_path)
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                "INSERT OR IGNORE INTO companies (ticker, name, tier, ir_url) "
                "VALUES ('MARA', 'MARA Holdings', 1, 'https://ir.mara.com')"
            )
            # Older report (inserted first but has earlier date)
            conn.execute(
                """INSERT INTO reports (ticker, report_date, source_type, source_url, raw_text)
                   VALUES ('MARA', '2024-01-10', 'ir_press_release',
                           'https://example.com/old', 'old report text')"""
            )
            # Newer report for same period (YYYY-MM matches 2024-01)
            conn.execute(
                """INSERT INTO reports (ticker, report_date, source_type, source_url, raw_text)
                   VALUES ('MARA', '2024-01-31', 'ir_press_release',
                           'https://example.com/new', 'new report text')"""
            )
        return db

    def test_find_report_for_period_returns_latest(self):
        db = self._make_db_with_two_reports()
        result = db.find_report_for_period('MARA', '2024-01')
        self.assertIsNotNone(result)
        self.assertEqual(result['source_url'], 'https://example.com/new')

    def test_find_report_for_period_returns_none_when_missing(self):
        from infra.db import MinerDB
        import sqlite3
        db = MinerDB(self.db_path)
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                "INSERT OR IGNORE INTO companies (ticker, name, tier, ir_url) "
                "VALUES ('MARA', 'MARA Holdings', 1, 'https://ir.mara.com')"
            )
        result = db.find_report_for_period('MARA', '2024-01')
        self.assertIsNone(result)


class TestReviewDocumentUsesReportId(unittest.TestCase):
    """Issue #2: document lookup must use report_id when set, not find_report_for_period."""

    def _make_review_item(self, report_id):
        return {
            'id': 1, 'ticker': 'MARA', 'period': '2024-01-31',
            'metric': 'production_btc', 'report_id': report_id,
            'source_snippet': 'produced 500 BTC',
            'llm_value': 500.0, 'regex_value': None,
            'agreement_status': 'LLM_ONLY',
        }

    def _call_document_route(self, mock_db):
        from unittest.mock import patch
        from flask import Flask
        from routes.review import bp
        app = Flask(__name__)
        app.register_blueprint(bp)
        with patch('app_globals.get_db', return_value=mock_db):
            with app.test_client() as c:
                return c.get('/api/review/1/document')

    def test_document_lookup_uses_get_report_when_report_id_set(self):
        """When report_id is present, must call db.get_report(), not find_report_for_period."""
        from unittest.mock import MagicMock
        mock_db = MagicMock()
        mock_db.get_review_item.return_value = self._make_review_item(report_id=42)
        mock_db.get_report.return_value = {
            'id': 42, 'ticker': 'MARA', 'source_url': 'https://example.com/pr'
        }
        mock_db.get_report_raw_text.return_value = 'test raw text'

        self._call_document_route(mock_db)

        mock_db.get_report.assert_called_once_with(42)
        mock_db.find_report_for_period.assert_not_called()

    def test_document_lookup_falls_back_to_period_when_no_report_id(self):
        """When report_id is None or missing, must fall back to find_report_for_period."""
        from unittest.mock import MagicMock
        mock_db = MagicMock()
        item = self._make_review_item(report_id=None)
        mock_db.get_review_item.return_value = item
        mock_db.find_report_for_period.return_value = None

        self._call_document_route(mock_db)

        mock_db.find_report_for_period.assert_called_once_with('MARA', '2024-01-31')


class TestReviewItemHasReportId(unittest.TestCase):
    """_apply_llm_result must write report_id to review_queue on all review-routing branches."""

    def _make_report(self, report_id=77):
        return {
            'id': report_id,
            'ticker': 'MARA',
            'report_date': '2024-01-31',
            'source_type': 'ir_press_release',
        }

    def _make_result(self, value=750.0, confidence=0.9, method='llm'):
        from miner_types import ExtractionResult
        return ExtractionResult(
            value=value, unit='BTC', confidence=confidence,
            extraction_method=method, source_snippet='produced 750 BTC',
            metric='production_btc', pattern_id=None,
        )

    def _make_db(self):
        from unittest.mock import MagicMock
        db = MagicMock()
        db.data_point_exists.return_value = False
        db.get_trailing_data_points.return_value = []
        db.insert_review_item.return_value = 1
        return db

    def _make_summary(self):
        from miner_types import ExtractionSummary
        return ExtractionSummary()

    def test_llm_only_low_confidence_includes_report_id(self):
        """LLM_ONLY low-confidence branch must include report_id in insert_review_item."""
        from interpreters.interpret_pipeline import _apply_llm_result
        report = self._make_report(report_id=77)
        db = self._make_db()
        llm_result = self._make_result(value=750.0, confidence=0.5, method='llm')

        _apply_llm_result(
            metric='production_btc',
            llm_result=llm_result,
            db=db,
            report=report,
            confidence_threshold=0.75,
            summary=self._make_summary(),
        )

        db.insert_review_item.assert_called_once()
        call_args = db.insert_review_item.call_args[0][0]
        self.assertIn('report_id', call_args)
        self.assertEqual(call_args['report_id'], 77)

    def test_outlier_flagged_includes_report_id(self):
        """Outlier-flagged LLM result must include report_id in insert_review_item."""
        from interpreters.interpret_pipeline import _apply_llm_result
        report = self._make_report(report_id=88)
        db = self._make_db()
        # Trailing history: recent values around 100 BTC; 750 BTC is a large outlier
        db.get_trailing_data_points.return_value = [
            {'value': 100.0}, {'value': 105.0}, {'value': 98.0},
        ]
        llm_result = self._make_result(value=750.0, confidence=0.95, method='llm')

        _apply_llm_result(
            metric='production_btc',
            llm_result=llm_result,
            db=db,
            report=report,
            confidence_threshold=0.75,
            summary=self._make_summary(),
            llm_interpreter=None,
        )

        db.insert_review_item.assert_called_once()
        call_args = db.insert_review_item.call_args[0][0]
        self.assertIn('report_id', call_args)
        self.assertEqual(call_args['report_id'], 88)


if __name__ == '__main__':
    unittest.main()
