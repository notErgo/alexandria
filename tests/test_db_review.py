"""Tests for new review-queue DB methods: dedup, batches, targeted delete."""
import pytest
from helpers import make_report, make_review_item
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


class TestInsertReviewItemDedup:
    def test_insert_review_item_dedup_replaces_pending(self, db_mara):
        """Re-inserting same (ticker, period, metric) PENDING replaces the old row."""
        db = db_mara
        first_id = db.insert_review_item(make_review_item(
            ticker='MARA', period='2024-01-01', metric='production_btc',
            raw_value='700.0', confidence=0.65, status='PENDING',
        ))
        second_id = db.insert_review_item(make_review_item(
            ticker='MARA', period='2024-01-01', metric='production_btc',
            raw_value='750.0', confidence=0.70, status='PENDING',
        ))
        rows = db.get_review_items(ticker='MARA', status='PENDING')
        assert len(rows) == 1, f"Expected 1 PENDING row, got {len(rows)}"
        assert rows[0]['raw_value'] == '750.0', "Expected updated raw_value from second insert"

    def test_insert_review_item_dedup_preserves_non_pending(self, db_mara):
        """Dedup only removes PENDING rows; APPROVED rows for same key are untouched."""
        db = db_mara
        first_id = db.insert_review_item(make_review_item(
            ticker='MARA', period='2024-01-01', metric='production_btc',
            raw_value='700.0', confidence=0.65, status='PENDING',
        ))
        # Approve it
        db.approve_review_item(first_id)
        # Now re-insert a new PENDING row for the same key
        db.insert_review_item(make_review_item(
            ticker='MARA', period='2024-01-01', metric='production_btc',
            raw_value='750.0', confidence=0.70, status='PENDING',
        ))
        pending = db.get_review_items(ticker='MARA', status='PENDING')
        approved = db.get_review_items(ticker='MARA', status='APPROVED')
        assert len(pending) == 1
        assert len(approved) == 1

    def test_insert_review_item_dedup_different_metric_both_kept(self, db_mara):
        """Two PENDING rows with different metric for same ticker/period both survive."""
        db = db_mara
        db.insert_review_item(make_review_item(
            ticker='MARA', period='2024-01-01', metric='production_btc',
            raw_value='700.0', status='PENDING',
        ))
        db.insert_review_item(make_review_item(
            ticker='MARA', period='2024-01-01', metric='holdings_btc',
            raw_value='15000.0', status='PENDING',
        ))
        rows = db.get_review_items(ticker='MARA', status='PENDING')
        assert len(rows) == 2


class TestGetReviewBatches:
    def test_get_review_batches_groups_by_date(self, db_mara):
        """get_review_batches() returns groupings by date x ticker with correct counts."""
        db = db_mara
        db.insert_review_item(make_review_item(
            ticker='MARA', period='2024-01-01', metric='production_btc',
            raw_value='700.0', status='PENDING',
        ))
        db.insert_review_item(make_review_item(
            ticker='MARA', period='2024-02-01', metric='production_btc',
            raw_value='750.0', status='PENDING',
        ))
        batches = db.get_review_batches(ticker='MARA', status='PENDING')
        assert len(batches) == 1, f"Expected 1 batch (today), got {len(batches)}"
        assert batches[0]['ticker'] == 'MARA'
        assert batches[0]['item_count'] == 2

    def test_get_review_batches_overlap_final(self, db_mara):
        """overlap_final reflects how many items also exist in final_data_points."""
        db = db_mara
        db.insert_review_item(make_review_item(
            ticker='MARA', period='2024-01-01', metric='production_btc',
            raw_value='700.0', status='PENDING',
        ))
        db.insert_review_item(make_review_item(
            ticker='MARA', period='2024-02-01', metric='production_btc',
            raw_value='750.0', status='PENDING',
        ))
        # Insert one final value that overlaps
        db.upsert_final_data_point(
            ticker='MARA', period='2024-01-01', metric='production_btc',
            value=700.0, unit='BTC',
        )
        batches = db.get_review_batches(ticker='MARA', status='PENDING')
        assert batches[0]['overlap_final'] == 1

    def test_get_review_batches_returns_all_tickers_when_no_filter(self, db_mara):
        """get_review_batches(ticker=None) returns all tickers."""
        db = db_mara
        db.insert_company({
            'ticker': 'RIOT', 'name': 'Riot Platforms', 'tier': 1,
            'ir_url': 'https://riot.com', 'pr_base_url': None,
            'cik': '0001474735', 'active': 1,
        })
        db.insert_review_item(make_review_item(
            ticker='MARA', period='2024-01-01', metric='production_btc',
            raw_value='700.0', status='PENDING',
        ))
        db.insert_review_item(make_review_item(
            ticker='RIOT', period='2024-01-01', metric='production_btc',
            raw_value='500.0', status='PENDING',
        ))
        batches = db.get_review_batches(status='PENDING')
        tickers = {b['ticker'] for b in batches}
        assert 'MARA' in tickers
        assert 'RIOT' in tickers


class TestDeleteReviewItemsByFilter:
    def test_delete_review_items_by_filter_no_report_reset(self, db_mara):
        """delete_review_items_by_filter does NOT touch reports.extraction_status."""
        db = db_mara
        r_id = db.insert_report(make_report())
        db.insert_review_item(make_review_item(
            ticker='MARA', period='2024-01-01', metric='production_btc',
            raw_value='700.0', status='PENDING', report_id=r_id,
        ))
        # Mark the report as done
        db.mark_report_extracted(r_id)
        report_before = db.get_report(r_id)
        assert report_before['extraction_status'] == 'done'

        # Delete via filter
        from datetime import date
        today = date.today().isoformat()
        deleted = db.delete_review_items_by_filter(ticker='MARA', created_date=today, status='PENDING')
        assert deleted == 1

        report_after = db.get_report(r_id)
        assert report_after['extraction_status'] == 'done', \
            "delete_review_items_by_filter must NOT reset extraction_status"

    def test_delete_review_items_by_filter_returns_count(self, db_mara):
        """Returns the number of deleted rows."""
        db = db_mara
        db.insert_review_item(make_review_item(
            ticker='MARA', period='2024-01-01', metric='production_btc',
            raw_value='700.0', status='PENDING',
        ))
        db.insert_review_item(make_review_item(
            ticker='MARA', period='2024-02-01', metric='production_btc',
            raw_value='750.0', status='PENDING',
        ))
        from datetime import date
        today = date.today().isoformat()
        deleted = db.delete_review_items_by_filter(created_date=today, status='PENDING')
        assert deleted == 2

    def test_delete_review_items_by_filter_respects_ticker(self, db_mara):
        """Ticker-scoped delete only removes that ticker's rows."""
        db = db_mara
        db.insert_company({
            'ticker': 'RIOT', 'name': 'Riot Platforms', 'tier': 1,
            'ir_url': 'https://riot.com', 'pr_base_url': None,
            'cik': '0001474735', 'active': 1,
        })
        db.insert_review_item(make_review_item(
            ticker='MARA', period='2024-01-01', metric='production_btc',
            raw_value='700.0', status='PENDING',
        ))
        db.insert_review_item(make_review_item(
            ticker='RIOT', period='2024-01-01', metric='production_btc',
            raw_value='500.0', status='PENDING',
        ))
        from datetime import date
        today = date.today().isoformat()
        deleted = db.delete_review_items_by_filter(ticker='MARA', created_date=today, status='PENDING')
        assert deleted == 1
        riot_rows = db.get_review_items(ticker='RIOT', status='PENDING')
        assert len(riot_rows) == 1
