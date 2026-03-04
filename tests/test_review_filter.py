"""
Review filter route tests — TDD.

Tests GET /api/review with ticker/period/metric filter params.
Tests MUST FAIL before db.get_review_items() and the route are updated.
"""
import pytest


@pytest.fixture
def db_with_multi_review(db):
    """DB with companies and review items for multiple tickers."""
    for ticker in ('MARA', 'RIOT', 'CLSK'):
        db.insert_company({
            'ticker': ticker,
            'name': ticker + ' Inc.',
            'tier': 1,
            'ir_url': 'https://example.com/' + ticker.lower(),
            'pr_base_url': 'https://example.com/' + ticker.lower(),
            'cik': '000000' + ticker,
            'active': 1,
        })

    # MARA — production_btc, 2024-01
    db.insert_review_item({
        'data_point_id': None,
        'ticker': 'MARA',
        'period': '2024-01-01',
        'metric': 'production_btc',
        'raw_value': '700.0',
        'confidence': 0.85,
        'source_snippet': 'mined 700 BTC',
        'status': 'PENDING',
        'llm_value': 710.0,
        'regex_value': 700.0,
        'agreement_status': 'REVIEW_QUEUE',
    })
    # MARA — hodl_btc, 2024-02
    db.insert_review_item({
        'data_point_id': None,
        'ticker': 'MARA',
        'period': '2024-02-01',
        'metric': 'hodl_btc',
        'raw_value': '15000.0',
        'confidence': 0.80,
        'source_snippet': 'holdings 15000 BTC',
        'status': 'PENDING',
        'llm_value': 15010.0,
        'regex_value': 15000.0,
        'agreement_status': 'REVIEW_QUEUE',
    })
    # RIOT — production_btc, 2024-01
    db.insert_review_item({
        'data_point_id': None,
        'ticker': 'RIOT',
        'period': '2024-01-01',
        'metric': 'production_btc',
        'raw_value': '500.0',
        'confidence': 0.90,
        'source_snippet': 'mined 500 BTC',
        'status': 'PENDING',
        'llm_value': 505.0,
        'regex_value': 500.0,
        'agreement_status': 'REVIEW_QUEUE',
    })
    return db


@pytest.fixture
def app_with_multi_review(db_with_multi_review, monkeypatch):
    """Flask test app wired to multi-review DB."""
    import sys
    import os
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))
    import app_globals
    monkeypatch.setattr(app_globals, 'get_db', lambda: db_with_multi_review)

    import importlib
    import run_web
    importlib.reload(run_web)
    app = run_web.create_app()
    app.config['TESTING'] = True
    return app


class TestReviewTickerPeriodMetricFilter:
    def test_ticker_period_metric_returns_one_item(self, app_with_multi_review):
        """GET /api/review?ticker=MARA&period=2024-01-01&metric=production_btc returns exactly 1."""
        with app_with_multi_review.test_client() as c:
            resp = c.get('/api/review?ticker=MARA&period=2024-01-01&metric=production_btc')
            assert resp.status_code == 200
            data = resp.get_json()
            assert data['success'] is True
            items = data['data']['items']
            assert len(items) == 1
            assert items[0]['ticker'] == 'MARA'
            assert items[0]['metric'] == 'production_btc'
            assert items[0]['period'].startswith('2024-01')

    def test_ticker_filter_returns_all_items_for_ticker(self, app_with_multi_review):
        """GET /api/review?ticker=MARA returns both MARA items."""
        with app_with_multi_review.test_client() as c:
            resp = c.get('/api/review?ticker=MARA')
            assert resp.status_code == 200
            data = resp.get_json()
            items = data['data']['items']
            assert len(items) == 2
            for item in items:
                assert item['ticker'] == 'MARA'

    def test_metric_filter_returns_matching_items(self, app_with_multi_review):
        """GET /api/review?metric=production_btc returns MARA + RIOT production items."""
        with app_with_multi_review.test_client() as c:
            resp = c.get('/api/review?metric=production_btc')
            assert resp.status_code == 200
            data = resp.get_json()
            items = data['data']['items']
            assert len(items) == 2
            for item in items:
                assert item['metric'] == 'production_btc'

    def test_status_filter_still_works_with_new_filters(self, app_with_multi_review):
        """Existing status=PENDING filter still works alongside new filters."""
        with app_with_multi_review.test_client() as c:
            resp = c.get('/api/review?status=PENDING&ticker=RIOT')
            assert resp.status_code == 200
            data = resp.get_json()
            items = data['data']['items']
            assert len(items) == 1
            assert items[0]['ticker'] == 'RIOT'
            assert items[0]['status'] == 'PENDING'

    def test_no_match_returns_empty_list(self, app_with_multi_review):
        """GET /api/review?ticker=CLSK returns empty list (no CLSK items)."""
        with app_with_multi_review.test_client() as c:
            resp = c.get('/api/review?ticker=CLSK')
            assert resp.status_code == 200
            data = resp.get_json()
            items = data['data']['items']
            assert len(items) == 0
