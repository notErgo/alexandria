"""Tests for approve/edit_review_item() report_id propagation — Fix 2."""
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


class TestApproveReviewItemPropagatesReportId:
    def test_approve_propagates_report_id(self, db_mara):
        """approve_review_item carries report_id to the created data_point."""
        db = db_mara
        r_id = db.insert_report(make_report())
        rq_id = db.insert_review_item(make_review_item(report_id=r_id))

        dp = db.approve_review_item(rq_id)

        assert dp['report_id'] == r_id, (
            f"Expected report_id={r_id}, got {dp['report_id']}"
        )

    def test_approve_null_report_id_passes_through(self, db_mara):
        """approve_review_item passes None report_id when review_queue row has NULL."""
        db = db_mara
        rq_id = db.insert_review_item(make_review_item(report_id=None))
        dp = db.approve_review_item(rq_id)
        assert dp['report_id'] is None


class TestEditReviewItemPropagatesReportId:
    def test_edit_propagates_report_id(self, db_mara):
        """edit_review_item carries report_id to the created data_point."""
        db = db_mara
        r_id = db.insert_report(make_report())
        rq_id = db.insert_review_item(make_review_item(report_id=r_id))

        dp = db.edit_review_item(rq_id, corrected_value=750.0, note='manual fix')

        assert dp['report_id'] == r_id, (
            f"Expected report_id={r_id}, got {dp['report_id']}"
        )

    def test_edit_null_report_id_passes_through(self, db_mara):
        rq_id = db_mara.insert_review_item(make_review_item(report_id=None))
        dp = db_mara.edit_review_item(rq_id, corrected_value=600.0, note='')
        assert dp['report_id'] is None
