"""Tests for new review batch API endpoints."""
import pytest
import json
import sys
import os
from helpers import make_report, make_review_item


@pytest.fixture
def app_review_batch(db_with_company, monkeypatch):
    """Flask test app with a seeded DB that has review items."""
    db = db_with_company
    r_id = db.insert_report(make_report())
    db.insert_review_item(make_review_item(
        ticker='MARA', period='2024-01-01', metric='production_btc',
        raw_value='700.0', status='PENDING', report_id=r_id,
    ))
    db.insert_review_item(make_review_item(
        ticker='MARA', period='2024-02-01', metric='production_btc',
        raw_value='750.0', status='PENDING', report_id=r_id,
    ))

    import app_globals
    monkeypatch.setattr(app_globals, 'get_db', lambda: db)

    import importlib
    import run_web
    importlib.reload(run_web)
    app = run_web.create_app()
    app.config['TESTING'] = True
    app._test_db = db
    return app


class TestReviewBatchesEndpoint:
    def test_review_batches_endpoint_returns_schema(self, app_review_batch):
        """GET /api/review/batches returns expected schema."""
        with app_review_batch.test_client() as c:
            resp = c.get('/api/review/batches')
            assert resp.status_code == 200
            data = resp.get_json()
            assert data['success'] is True
            assert 'batches' in data['data']
            batches = data['data']['batches']
            assert isinstance(batches, list)
            if batches:
                b = batches[0]
                assert 'batch_date' in b
                assert 'ticker' in b
                assert 'item_count' in b
                assert 'overlap_final' in b

    def test_review_batches_ticker_filter(self, app_review_batch):
        """GET /api/review/batches?ticker=MARA returns only MARA batches."""
        with app_review_batch.test_client() as c:
            resp = c.get('/api/review/batches?ticker=MARA')
            assert resp.status_code == 200
            data = resp.get_json()
            for b in data['data']['batches']:
                assert b['ticker'] == 'MARA'

    def test_review_batches_shows_today_batch(self, app_review_batch):
        """Batch table has at least one entry with item_count >= 2."""
        with app_review_batch.test_client() as c:
            resp = c.get('/api/review/batches?ticker=MARA')
            data = resp.get_json()
            batches = data['data']['batches']
            assert len(batches) >= 1
            assert batches[0]['item_count'] >= 2


class TestReviewBatchDeleteEndpoint:
    def test_review_batch_delete_dry_run_returns_count(self, app_review_batch):
        """dry_run=true returns count without deleting."""
        from datetime import date
        today = date.today().isoformat()
        with app_review_batch.test_client() as c:
            resp = c.post('/api/review/batch-delete', json={
                'created_date': today,
                'ticker': 'MARA',
                'dry_run': True,
            })
            assert resp.status_code == 200
            data = resp.get_json()
            assert data['success'] is True
            assert data['data']['deleted'] >= 2
            assert data['data'].get('dry_run') is True
            # Verify rows still exist
            rows = app_review_batch._test_db.get_review_items(ticker='MARA', status='PENDING')
            assert len(rows) >= 2

    def test_review_batch_delete_executes(self, app_review_batch):
        """POST /api/review/batch-delete deletes correct rows and returns count."""
        from datetime import date
        today = date.today().isoformat()
        with app_review_batch.test_client() as c:
            resp = c.post('/api/review/batch-delete', json={
                'created_date': today,
                'ticker': 'MARA',
                'dry_run': False,
            })
            assert resp.status_code == 200
            data = resp.get_json()
            assert data['success'] is True
            assert data['data']['deleted'] >= 2
            # Verify rows are gone
            rows = app_review_batch._test_db.get_review_items(ticker='MARA', status='PENDING')
            assert len(rows) == 0

    def test_review_batch_delete_missing_created_date_returns_400(self, app_review_batch):
        """POST /api/review/batch-delete without created_date returns 400."""
        with app_review_batch.test_client() as c:
            resp = c.post('/api/review/batch-delete', json={
                'ticker': 'MARA',
            })
            assert resp.status_code == 400

    def test_review_batch_delete_does_not_reset_extraction_status(self, app_review_batch):
        """Batch-delete does not touch reports.extraction_status."""
        from datetime import date
        today = date.today().isoformat()
        db = app_review_batch._test_db
        # Get the report and mark it done
        with db._get_connection() as conn:
            r_id = conn.execute("SELECT id FROM reports WHERE ticker='MARA' LIMIT 1").fetchone()[0]
        db.mark_report_extracted(r_id)
        assert db.get_report(r_id)['extraction_status'] == 'done'

        with app_review_batch.test_client() as c:
            resp = c.post('/api/review/batch-delete', json={
                'created_date': today,
                'ticker': 'MARA',
                'dry_run': False,
            })
        assert db.get_report(r_id)['extraction_status'] == 'done'
