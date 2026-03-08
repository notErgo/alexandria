"""Tests for delete_report() scoped deletion — Fix 1."""
import pytest
from helpers import make_report, make_data_point, make_review_item
from infra.db import MinerDB


@pytest.fixture
def db_mara(tmp_path):
    db = MinerDB(str(tmp_path / 'test.db'))
    db.insert_company({
        'ticker': 'MARA', 'name': 'MARA Holdings', 'tier': 1,
        'ir_url': 'https://example.com', 'pr_base_url': None,
        'cik': '0001437491', 'active': 1,
    })
    return db


class TestDeleteReportScopedDeletion:
    def test_deletes_review_queue_by_report_id(self, db_mara):
        """delete_report removes review_queue rows with matching report_id only."""
        db = db_mara
        r1_id = db.insert_report(make_report(report_date='2024-01-01'))
        r2_id = db.insert_report(make_report(report_date='2024-01-01', source_type='ir_press_release'))

        rq1_id = db.insert_review_item(make_review_item(
            ticker='MARA', period='2024-01-01', report_id=r1_id
        ))
        rq2_id = db.insert_review_item(make_review_item(
            ticker='MARA', period='2024-01-01', report_id=r2_id
        ))

        deleted = db.delete_report('MARA', '2024-01-01', 'archive_pdf')
        assert deleted == 1

        # r2's review_queue item must survive
        remaining = db.get_review_item(rq2_id)
        assert remaining is not None, "review_queue row for sibling report must not be deleted"

        # r1's review_queue item must be gone
        gone = db.get_review_item(rq1_id)
        assert gone is None, "review_queue row for deleted report must be removed"

    def test_deletes_orphan_review_queue_rows(self, db_mara):
        """delete_report also removes NULL-report_id review_queue rows for same ticker+period."""
        db = db_mara
        r1_id = db.insert_report(make_report(report_date='2024-02-01'))
        db.insert_data_point(make_data_point(report_id=r1_id, period='2024-02-01'))

        orphan_id = db.insert_review_item(make_review_item(
            ticker='MARA', period='2024-02-01', report_id=None
        ))

        db.delete_report('MARA', '2024-02-01', 'archive_pdf')

        gone = db.get_review_item(orphan_id)
        assert gone is None, "orphan NULL-report_id review_queue row should be cleaned up"

    def test_does_not_delete_different_period_orphans(self, db_mara):
        """delete_report does not touch NULL-report_id rows for different periods."""
        db = db_mara
        r1_id = db.insert_report(make_report(report_date='2024-03-01'))

        other_orphan_id = db.insert_review_item(make_review_item(
            ticker='MARA', period='2024-04-01', report_id=None
        ))

        db.delete_report('MARA', '2024-03-01', 'archive_pdf')

        still_there = db.get_review_item(other_orphan_id)
        assert still_there is not None, "orphan for different period must survive"

    def test_returns_zero_for_missing_report(self, db_mara):
        result = db_mara.delete_report('MARA', '2099-01-01', 'archive_pdf')
        assert result == 0
