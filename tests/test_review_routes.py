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
        'raw_html': '<html><head><meta property="og:title" content="MARA January 2024 Production Update"></head>'
                    '<body><h1>Ignored Header</h1><p>MARA Holdings mined 700 BTC in January 2024.</p></body></html>',
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
            assert data['data']['document_title'] == 'MARA January 2024 Production Update'

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


class TestApproveFinalDataPoints:
    def test_approve_writes_to_final_data_points(self, app_with_review, db_with_review):
        """POST /api/review/<id>/approve writes the value to final_data_points."""
        with app_with_review.test_client() as c:
            resp = c.get('/api/review')
            item_id = resp.get_json()['data']['items'][0]['id']

            resp = c.post(f'/api/review/{item_id}/approve',
                         json={}, content_type='application/json')
            assert resp.status_code == 200

            finals = db_with_review.get_final_data_points('MARA')
            assert len(finals) == 1
            row = finals[0]
            assert row['period'] == '2024-01-01'
            assert row['metric'] == 'production_btc'
            assert row['value'] == 700.0

    def test_edit_writes_to_final_data_points(self, app_with_review, db_with_review):
        """POST /api/review/<id>/approve with value stores corrected value in final_data_points."""
        with app_with_review.test_client() as c:
            resp = c.get('/api/review')
            item_id = resp.get_json()['data']['items'][0]['id']

            resp = c.post(f'/api/review/{item_id}/approve',
                         json={'value': 750}, content_type='application/json')
            assert resp.status_code == 200

            finals = db_with_review.get_final_data_points('MARA')
            assert len(finals) == 1
            assert finals[0]['value'] == 750.0

    def test_batch_finalize_endpoint(self, app_with_review, db_with_review):
        """POST /api/review/batch-finalize finalizes multiple items."""
        db = db_with_review
        # Seed a second review item
        db.insert_review_item({
            'data_point_id': None,
            'ticker': 'MARA',
            'period': '2024-02-01',
            'metric': 'production_btc',
            'raw_value': '620.0',
            'confidence': 0.80,
            'source_snippet': 'MARA mined 620 BTC in February 2024.',
            'status': 'PENDING',
            'llm_value': 620.0,
            'regex_value': 620.0,
            'agreement_status': 'REVIEW_QUEUE',
        })

        with app_with_review.test_client() as c:
            resp = c.get('/api/review')
            items = resp.get_json()['data']['items']
            ids = [item['id'] for item in items[:2]]
            assert len(ids) == 2

            resp = c.post('/api/review/batch-finalize',
                         json={'ids': ids}, content_type='application/json')
            assert resp.status_code == 200
            data = resp.get_json()
            assert data['success'] is True
            assert data['data']['finalized'] == 2

            finals = db.get_final_data_points('MARA')
            assert len(finals) == 2


class TestReviewPurgeRoute:
    def test_purge_review_queue_preserves_reports(self, app_with_review, db_with_review):
        with app_with_review.test_client() as c:
            resp = c.post(
                '/api/delete/review',
                json={'confirm': True, 'ticker': 'MARA', 'targets': ['queue']},
                content_type='application/json',
            )
            assert resp.status_code == 200
            data = resp.get_json()
            assert data['success'] is True
            assert data['data']['reports_preserved'] is True
            assert data['data']['counts']['review_queue_deleted'] == 1
            assert data['data']['counts']['final_data_points_deleted'] == 0
            assert db_with_review.get_report(db_with_review._report_id) is not None
            assert db_with_review.count_review_items(status='PENDING') == 0

    def test_purge_review_queue_resets_report_to_pending(self, app_with_review, db_with_review):
        db_with_review.mark_report_extracted(db_with_review._report_id)

        with app_with_review.test_client() as c:
            resp = c.post(
                '/api/delete/review',
                json={'confirm': True, 'ticker': 'MARA', 'targets': ['queue']},
                content_type='application/json',
            )
            assert resp.status_code == 200

        report = db_with_review.get_report(db_with_review._report_id)
        assert report['extraction_status'] == 'pending'
        assert report['extracted_at'] is None

    def test_purge_final_preserves_reports_and_review_queue(self, app_with_review, db_with_review):
        db_with_review.upsert_final_data_point(
            'MARA',
            '2024-01-01',
            'production_btc',
            700.0,
            'BTC',
            1.0,
            analyst_note='seed',
            source_ref='review_queue:1',
        )

        with app_with_review.test_client() as c:
            resp = c.post(
                '/api/delete/review',
                json={'confirm': True, 'ticker': 'MARA', 'targets': ['final']},
                content_type='application/json',
            )
            assert resp.status_code == 200
            data = resp.get_json()
            assert data['success'] is True
            assert data['data']['counts']['review_queue_deleted'] == 0
            assert data['data']['counts']['final_data_points_deleted'] == 1
            assert db_with_review.get_report(db_with_review._report_id) is not None
            assert len(db_with_review.get_review_items(status='PENDING', limit=50, offset=0)) == 1
            assert db_with_review.get_final_data_points('MARA') == []

    def test_purge_requires_confirm(self, app_with_review):
        with app_with_review.test_client() as c:
            resp = c.post(
                '/api/delete/review',
                json={'ticker': 'MARA', 'targets': ['queue']},
                content_type='application/json',
            )
            assert resp.status_code == 400


class TestReextractTicker:
    """reextract routes pass ticker context to LLM via extract_batch."""

    def test_reextract_uses_extract_batch_with_ticker(self, app_with_review, monkeypatch):
        """POST /api/review/<id>/reextract calls extract_batch with ticker from item."""
        import app_globals
        import interpreters.llm_interpreter as _llm_mod

        captured = {}

        class MockLLM:
            def __init__(self, session, db):
                pass

            def check_connectivity(self):
                return True

            def extract(self, text, metric):
                # Old path — must NOT be called
                raise AssertionError("extract() must not be called; use extract_batch()")

            def extract_batch(self, text, metrics, ticker=None):
                captured['ticker'] = ticker
                captured['metrics'] = list(metrics)
                return {}

        monkeypatch.setattr(_llm_mod, 'LLMInterpreter', MockLLM)

        with app_with_review.test_client() as c:
            resp = c.get('/api/review')
            item_id = resp.get_json()['data']['items'][0]['id']

            resp = c.post(
                f'/api/review/{item_id}/reextract',
                json={'selection': 'MARA mined 700 BTC in January 2024.'},
                content_type='application/json',
            )
            assert resp.status_code == 200, resp.get_json()
            assert captured.get('ticker') == 'MARA', (
                f"extract_batch must receive ticker='MARA', got {captured.get('ticker')!r}"
            )
