"""
TDD tests for POST /api/patterns/apply.

Written BEFORE implementation — all tests must fail before any production code
is written. Run to confirm failures first.
"""
import pytest


# ── Fixtures ─────────────────────────────────────────────────────────────────

@pytest.fixture
def app(tmp_path, monkeypatch):
    """Flask test client pointing at a temp DB, no pattern files needed."""
    import sys, os
    src_path = os.path.join(os.path.dirname(__file__), '..', 'src')
    if src_path not in sys.path:
        sys.path.insert(0, src_path)

    import app_globals
    from infra.db import MinerDB
    test_db = MinerDB(str(tmp_path / 'test.db'))
    app_globals._db = test_db

    import run_web
    flask_app = run_web.create_app()
    flask_app.config['TESTING'] = True
    return flask_app.test_client()


@pytest.fixture
def app_with_reports(app, tmp_path):
    """Client with two MARA reports in DB — one that matches 'mined 750 BTC'."""
    import app_globals
    db = app_globals._db

    db.insert_company({
        'ticker': 'MARA', 'name': 'MARA Holdings', 'tier': 1,
        'ir_url': 'https://www.marathondh.com/news',
        'pr_base_url': 'https://www.marathondh.com',
        'cik': '0001437491', 'active': 1,
    })

    # Report with matching text, no existing data_point
    db.insert_report({
        'ticker': 'MARA',
        'report_date': '2022-07-01',
        'published_date': '2022-08-03',
        'source_type': 'archive_html',
        'source_url': None,
        'raw_text': 'In July 2022 MARA mined 742 bitcoin during the month.',
        'parsed_at': '2022-08-03T00:00:00',
    })

    # Report with matching text but data_point already exists
    r2_id = db.insert_report({
        'ticker': 'MARA',
        'report_date': '2022-06-01',
        'published_date': '2022-07-05',
        'source_type': 'archive_html',
        'source_url': None,
        'raw_text': 'MARA mined 1100 bitcoin in June 2022.',
        'parsed_at': '2022-07-05T00:00:00',
    })
    db.insert_data_point({
        'report_id': r2_id,
        'ticker': 'MARA', 'period': '2022-06-01', 'metric': 'production_btc',
        'value': 1100.0, 'unit': 'BTC', 'confidence': 0.92,
        'extraction_method': 'prod_btc_0', 'source_snippet': 'mined 1100 bitcoin',
    })

    # Report with no matching text
    db.insert_report({
        'ticker': 'MARA',
        'report_date': '2022-05-01',
        'published_date': '2022-06-02',
        'source_type': 'archive_html',
        'source_url': None,
        'raw_text': 'MARA announces Q2 2022 operations update. Hashrate increased.',
        'parsed_at': '2022-06-02T00:00:00',
    })

    return app


_BTC_REGEX = r'(?i)mined\s+([\d,]+(?:\.\d+)?)\s*(?:bitcoin|btc)'


class TestPatternApply:
    """Tests for POST /api/patterns/apply."""

    def test_creates_data_point_for_matching_report(self, app_with_reports):
        """Matching report with no existing data_point → new data_point created."""
        resp = app_with_reports.post('/api/patterns/apply', json={
            'regex': _BTC_REGEX,
            'metric': 'production_btc',
            'confidence_weight': 0.87,
        })
        assert resp.status_code == 200
        data = resp.get_json()
        assert data['success'] is True
        assert data['data']['created'] >= 1

    def test_skips_existing_data_point(self, app_with_reports):
        """Report whose period already has a data_point → counted as skipped, not overwritten."""
        resp = app_with_reports.post('/api/patterns/apply', json={
            'regex': _BTC_REGEX,
            'metric': 'production_btc',
            'confidence_weight': 0.87,
        })
        data = resp.get_json()
        assert data['data']['skipped_existing'] >= 1

    def test_no_match_counted(self, app_with_reports):
        """Report with no matching text → counted as no_match."""
        resp = app_with_reports.post('/api/patterns/apply', json={
            'regex': _BTC_REGEX,
            'metric': 'production_btc',
            'confidence_weight': 0.87,
        })
        data = resp.get_json()
        assert data['data']['no_match'] >= 1

    def test_low_confidence_routes_to_review_queue(self, app):
        """Pattern with very low weight → result below threshold → goes to review_queue."""
        import app_globals
        db = app_globals._db
        db.insert_company({
            'ticker': 'RIOT', 'name': 'RIOT Platforms', 'tier': 1,
            'ir_url': 'https://ir.riotplatforms.com',
            'pr_base_url': 'https://www.riotplatforms.com',
            'cik': '0001167419', 'active': 1,
        })
        db.insert_report({
            'ticker': 'RIOT', 'report_date': '2023-01-01',
            'published_date': '2023-02-01', 'source_type': 'archive_html',
            'source_url': None,
            'raw_text': 'RIOT mined 500 bitcoin in January.',
            'parsed_at': '2023-02-01T00:00:00',
        })
        # confidence_weight=0.01 → score will be well below 0.75 threshold
        resp = app.post('/api/patterns/apply', json={
            'regex': _BTC_REGEX,
            'metric': 'production_btc',
            'confidence_weight': 0.01,
        })
        data = resp.get_json()
        assert data['success'] is True
        assert data['data']['low_confidence'] >= 1
        assert data['data']['created'] == 0

    def test_invalid_regex_returns_400(self, app):
        resp = app.post('/api/patterns/apply', json={
            'regex': '(?i)mined ([',  # unclosed bracket
            'metric': 'production_btc',
        })
        assert resp.status_code == 400
        data = resp.get_json()
        assert data['success'] is False

    def test_unknown_metric_returns_400(self, app):
        resp = app.post('/api/patterns/apply', json={
            'regex': _BTC_REGEX,
            'metric': 'nonexistent_metric',
        })
        assert resp.status_code == 400

    def test_missing_regex_returns_400(self, app):
        resp = app.post('/api/patterns/apply', json={
            'metric': 'production_btc',
        })
        assert resp.status_code == 400

    def test_response_has_per_ticker_summary(self, app_with_reports):
        resp = app_with_reports.post('/api/patterns/apply', json={
            'regex': _BTC_REGEX,
            'metric': 'production_btc',
            'confidence_weight': 0.87,
        })
        data = resp.get_json()
        assert 'per_ticker' in data['data']
        assert 'MARA' in data['data']['per_ticker']

    def test_response_has_all_count_fields(self, app_with_reports):
        resp = app_with_reports.post('/api/patterns/apply', json={
            'regex': _BTC_REGEX,
            'metric': 'production_btc',
            'confidence_weight': 0.87,
        })
        data = resp.get_json()['data']
        for field in ('applied_to', 'created', 'skipped_existing',
                      'low_confidence', 'no_match'):
            assert field in data, f"Missing field: {field}"

    def test_apply_does_not_overwrite_existing_value(self, app_with_reports):
        """Existing 1100 BTC value must not be changed by the apply operation."""
        import app_globals
        db = app_globals._db
        app_with_reports.post('/api/patterns/apply', json={
            'regex': _BTC_REGEX,
            'metric': 'production_btc',
            'confidence_weight': 0.87,
        })
        rows = db.query_data_points(
            ticker='MARA', metric='production_btc',
            from_period='2022-06-01', to_period='2022-06-01',
        )
        assert len(rows) == 1
        assert rows[0]['value'] == 1100.0  # unchanged

    def test_reports_without_raw_text_skipped(self, app):
        """Reports with NULL raw_text are not processed."""
        import app_globals
        db = app_globals._db
        db.insert_company({
            'ticker': 'CLSK', 'name': 'CleanSpark', 'tier': 1,
            'ir_url': 'https://ir.cleanspark.com',
            'pr_base_url': 'https://ir.cleanspark.com',
            'cik': '0000827054', 'active': 1,
        })
        db.insert_report({
            'ticker': 'CLSK', 'report_date': '2024-01-01',
            'published_date': '2024-02-01', 'source_type': 'archive_html',
            'source_url': None, 'raw_text': None,
            'parsed_at': '2024-02-01T00:00:00',
        })
        resp = app.post('/api/patterns/apply', json={
            'regex': _BTC_REGEX,
            'metric': 'production_btc',
            'confidence_weight': 0.87,
        })
        assert resp.status_code == 200
        data = resp.get_json()
        assert data['data']['applied_to'] == 0  # nothing to apply to
