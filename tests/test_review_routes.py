"""
Review queue route tests — TDD.

Tests use Flask test client with a seeded DB.
Tests should FAIL before routes/review.py is created.
"""
import pytest
import json


@pytest.fixture
def db_with_review(db_with_company):
    """DB with a MARA company, one report, and one pending review item."""
    db = db_with_company
    # Insert a report
    report_id = db.insert_report({
        'ticker': 'MARA',
        'report_date': '2024-01-01',
        'published_date': '2024-01-15',
        'source_type': 'archive_html',
        'source_url': 'https://example.com/mara-jan-2024.html',
        'raw_text': 'MARA Holdings mined 700 BTC in January 2024. Total holdings: 15,000 BTC.',
        'parsed_at': '2024-01-15T10:00:00',
    })
    # Insert a review item with llm_value and regex_value
    db.insert_review_item({
        'data_point_id': None,
        'ticker': 'MARA',
        'period': '2024-01-01',
        'metric': 'production_btc',
        'raw_value': '700.0',
        'confidence': 0.85,
        'source_snippet': 'MARA Holdings mined 700 BTC in January 2024.',
        'status': 'PENDING',
        'llm_value': 710.0,
        'regex_value': 700.0,
        'agreement_status': 'REVIEW_QUEUE',
    })
    db._report_id = report_id
    return db


@pytest.fixture
def app_with_review(db_with_review, monkeypatch):
    """Flask test app with review DB wired in."""
    import sys
    import os
    # Wire the db singleton
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))
    import app_globals
    monkeypatch.setattr(app_globals, 'get_db', lambda: db_with_review)

    import importlib
    import run_web
    importlib.reload(run_web)
    app = run_web.create_app()
    app.config['TESTING'] = True
    return app


class TestReviewListRoute:
    def test_get_review_returns_items(self, app_with_review):
        """GET /api/review → 200 with at least one item."""
        with app_with_review.test_client() as c:
            resp = c.get('/api/review')
            assert resp.status_code == 200
            data = resp.get_json()
            assert data['success'] is True
            assert len(data['data']['items']) >= 1

    def test_get_review_item_has_agreement_fields(self, app_with_review):
        """Review items include llm_value, regex_value, agreement_status."""
        with app_with_review.test_client() as c:
            resp = c.get('/api/review')
            data = resp.get_json()
            item = data['data']['items'][0]
            assert 'llm_value' in item
            assert 'regex_value' in item
            assert 'agreement_status' in item

    def test_get_review_status_filter(self, app_with_review):
        """GET /api/review?status=PENDING returns only PENDING items."""
        with app_with_review.test_client() as c:
            resp = c.get('/api/review?status=PENDING')
            data = resp.get_json()
            for item in data['data']['items']:
                assert item['status'] == 'PENDING'


class TestReviewDocumentRoute:
    def test_get_document_returns_raw_text(self, app_with_review, db_with_review):
        """GET /api/review/<id>/document → 200 with raw_text."""
        with app_with_review.test_client() as c:
            # Get the review item id first
            resp = c.get('/api/review')
            items = resp.get_json()['data']['items']
            item_id = items[0]['id']

            resp = c.get(f'/api/review/{item_id}/document')
            assert resp.status_code == 200
            data = resp.get_json()
            assert data['success'] is True
            assert 'raw_text' in data['data']
            assert len(data['data']['raw_text']) > 0

    def test_get_document_invalid_id_returns_404(self, app_with_review):
        """GET /api/review/99999/document → 404."""
        with app_with_review.test_client() as c:
            resp = c.get('/api/review/99999/document')
            assert resp.status_code == 404


class TestReviewApproveRoute:
    def test_approve_sets_status_approved(self, app_with_review):
        """POST /api/review/<id>/approve → 200; item status becomes APPROVED."""
        with app_with_review.test_client() as c:
            resp = c.get('/api/review')
            item_id = resp.get_json()['data']['items'][0]['id']

            resp = c.post(f'/api/review/{item_id}/approve',
                         json={}, content_type='application/json')
            assert resp.status_code == 200
            data = resp.get_json()
            assert data['success'] is True

    def test_approve_invalid_id_returns_404(self, app_with_review):
        """POST /api/review/99999/approve → 404."""
        with app_with_review.test_client() as c:
            resp = c.post('/api/review/99999/approve',
                         json={}, content_type='application/json')
            assert resp.status_code == 404


class TestReviewRejectRoute:
    def test_reject_with_note_succeeds(self, app_with_review):
        """POST /api/review/<id>/reject with note → 200."""
        with app_with_review.test_client() as c:
            resp = c.get('/api/review')
            item_id = resp.get_json()['data']['items'][0]['id']

            resp = c.post(f'/api/review/{item_id}/reject',
                         json={'note': 'Value is quarterly total, not monthly'},
                         content_type='application/json')
            assert resp.status_code == 200
            data = resp.get_json()
            assert data['success'] is True

    def test_reject_without_note_returns_400(self, app_with_review):
        """POST /api/review/<id>/reject without note → 400 (note required)."""
        with app_with_review.test_client() as c:
            resp = c.get('/api/review')
            item_id = resp.get_json()['data']['items'][0]['id']

            resp = c.post(f'/api/review/{item_id}/reject',
                         json={}, content_type='application/json')
            assert resp.status_code == 400
