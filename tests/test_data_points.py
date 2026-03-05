"""Tests for /api/data/lineage endpoint."""
import pytest


# ── Fixtures ─────────────────────────────────────────────────────────────────

@pytest.fixture
def app(db_with_company):
    """Flask test app wired to an in-memory test DB."""
    from flask import Flask
    from routes.data_points import bp

    flask_app = Flask(__name__)
    flask_app.config['TESTING'] = True
    flask_app.register_blueprint(bp)

    # Override get_db() to return the test DB
    import app_globals
    app_globals._db = db_with_company

    with flask_app.app_context():
        yield flask_app

    # Reset singleton after test
    app_globals._db = None


@pytest.fixture
def client(app):
    return app.test_client()


@pytest.fixture
def db_with_datapoint(db_with_company):
    """DB with one MARA production_btc data point for 2024-09."""
    db = db_with_company
    report_id = db.insert_report({
        'ticker': 'MARA',
        'report_date': '2024-09-01',
        'published_date': '2024-10-03',
        'source_type': 'archive_html',
        'source_url': 'https://ir.mara.com/news/2024-09',
        'raw_text': 'Marathon mined 705 BTC in September 2024.',
        'parsed_at': '2024-10-03T12:00:00',
    })
    db.insert_data_point({
        'report_id': report_id,
        'ticker': 'MARA',
        'period': '2024-09-01',
        'metric': 'production_btc',
        'value': 705.0,
        'unit': 'BTC',
        'confidence': 0.95,
        'extraction_method': 'prod_btc_0',
        'source_snippet': 'Marathon mined 705 BTC in September 2024.',
    })
    return db


# ── Tests ─────────────────────────────────────────────────────────────────────

class TestLineageEndpoint:
    def test_lineage_returns_all_provenance_fields(self, client, db_with_datapoint, app):
        import app_globals
        app_globals._db = db_with_datapoint
        resp = client.get('/api/data/lineage?ticker=MARA&metric=production_btc&period=2024-09')
        assert resp.status_code == 200
        body = resp.get_json()
        assert body['success'] is True
        data = body['data']
        assert data['ticker'] == 'MARA'
        assert data['metric'] == 'production_btc'
        assert data['period'] == '2024-09'
        assert abs(data['value'] - 705.0) < 0.1
        assert 'confidence' in data
        assert 'extraction_method' in data
        assert 'source_snippet' in data
        assert 'source_type' in data
        assert 'report_date' in data
        assert 'source_url' in data

    def test_lineage_missing_period_returns_404(self, client, db_with_company, app):
        import app_globals
        app_globals._db = db_with_company
        resp = client.get('/api/data/lineage?ticker=MARA&metric=production_btc&period=1999-01')
        assert resp.status_code == 404

    def test_lineage_missing_params_returns_400(self, client, db_with_company, app):
        import app_globals
        app_globals._db = db_with_company
        resp = client.get('/api/data/lineage?ticker=MARA&metric=production_btc')
        assert resp.status_code == 400

    def test_lineage_invalid_metric_returns_400(self, client, db_with_company, app):
        import app_globals
        app_globals._db = db_with_company
        resp = client.get('/api/data/lineage?ticker=MARA&metric=bad_metric&period=2024-09')
        assert resp.status_code == 400

    def test_lineage_invalid_period_format_returns_400(self, client, db_with_company, app):
        import app_globals
        app_globals._db = db_with_company
        resp = client.get('/api/data/lineage?ticker=MARA&metric=production_btc&period=2024-9')
        assert resp.status_code == 400

    def test_lineage_does_not_expose_raw_text(self, client, db_with_datapoint, app):
        import app_globals
        app_globals._db = db_with_datapoint
        resp = client.get('/api/data/lineage?ticker=MARA&metric=production_btc&period=2024-09')
        assert resp.status_code == 200
        data = resp.get_json()['data']
        assert 'raw_text' not in data


class TestPurgeEndpoint:
    """Tests for POST /api/data/purge."""

    def test_purge_requires_confirm_true(self, client, db_with_company, app):
        import app_globals
        app_globals._db = db_with_company
        resp = client.post('/api/data/purge', json={'confirm': False})
        assert resp.status_code == 400

    def test_purge_missing_confirm_rejected(self, client, db_with_company, app):
        import app_globals
        app_globals._db = db_with_company
        resp = client.post('/api/data/purge', json={})
        assert resp.status_code == 400

    def test_purge_invalid_mode_returns_400(self, client, db_with_company, app):
        import app_globals
        app_globals._db = db_with_company
        resp = client.post('/api/data/purge', json={
            'confirm': True,
            'purge_mode': 'obliterate',
        })
        assert resp.status_code == 400
        body = resp.get_json()
        assert body['error']['code'] == 'INVALID_PURGE_MODE'

    def test_purge_reset_mode_returns_success(self, client, db_with_datapoint, app):
        import app_globals
        app_globals._db = db_with_datapoint
        resp = client.post('/api/data/purge', json={
            'confirm': True,
            'purge_mode': 'reset',
        })
        assert resp.status_code == 200
        body = resp.get_json()
        assert body['success'] is True
        assert body['data']['purge_mode'] == 'reset'
        assert body['data']['counts']['reports'] == 1

    def test_purge_response_includes_auto_sync_status(self, client, db_with_company, app):
        import app_globals
        app_globals._db = db_with_company
        resp = client.post('/api/data/purge', json={
            'confirm': True,
            'purge_mode': 'reset',
        })
        assert resp.status_code == 200
        body = resp.get_json()
        assert 'auto_sync_companies_on_startup' in body['data']

    def test_purge_ticker_scoped_leaves_other_data(self, client, db_with_datapoint, app):
        """Ticker-scoped purge on a company with no data leaves MARA's data untouched."""
        import app_globals
        app_globals._db = db_with_datapoint
        # Register a second company with no data so the route accepts the ticker
        db_with_datapoint.insert_company({
            'ticker': 'RIOT',
            'name': 'Riot Platforms',
            'tier': 1,
            'ir_url': '',
            'pr_base_url': None,
            'cik': None,
            'active': 1,
        })
        resp = client.post('/api/data/purge', json={
            'confirm': True,
            'purge_mode': 'reset',
            'ticker': 'RIOT',
        })
        assert resp.status_code == 200
        body = resp.get_json()
        assert body['data']['ticker'] == 'RIOT'
        assert body['data']['counts']['reports'] == 0
        # MARA data must be untouched
        mara_points = db_with_datapoint.query_data_points(ticker='MARA')
        assert len(mara_points) >= 1
