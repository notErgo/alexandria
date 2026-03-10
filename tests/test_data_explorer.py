"""Tests for Data Explorer API endpoints.

Covers two new endpoints:
  GET /api/data/documents  — document search (Mode B list panel)
  GET /api/data/document/<report_id> — document viewer (Mode B viewer)
"""
import pytest


# ── Fixtures ──────────────────────────────────────────────────────────────────

@pytest.fixture
def app(db_with_company):
    from flask import Flask
    from routes.data_points import bp
    import app_globals
    app_globals._db = db_with_company
    flask_app = Flask(__name__)
    flask_app.config['TESTING'] = True
    flask_app.register_blueprint(bp)
    with flask_app.app_context():
        yield flask_app
    app_globals._db = None


@pytest.fixture
def client(app):
    return app.test_client()


@pytest.fixture
def db_with_report(db_with_company):
    """DB with one MARA report + two data points (two metrics)."""
    db = db_with_company
    report_id = db.insert_report({
        'ticker': 'MARA',
        'report_date': '2024-09-01',
        'published_date': '2024-10-03',
        'source_type': 'archive_html',
        'source_url': 'https://ir.mara.com/news/2024-09',
        'raw_text': 'Marathon mined 705 BTC in September 2024. Hashrate was 36.9 EH/s.',
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
        'source_snippet': 'mined 705 BTC in September 2024',
    })
    db.insert_data_point({
        'report_id': report_id,
        'ticker': 'MARA',
        'period': '2024-09-01',
        'metric': 'hashrate_eh',
        'value': 36.9,
        'unit': 'EH/s',
        'confidence': 0.88,
        'extraction_method': 'hashrate_0',
        'source_snippet': 'Hashrate was 36.9 EH/s',
    })
    return db, report_id


@pytest.fixture
def db_with_two_reports(db_with_company):
    """DB with two MARA reports in different months + one RIOT report."""
    db = db_with_company
    r1 = db.insert_report({
        'ticker': 'MARA',
        'report_date': '2024-08-01',
        'published_date': '2024-09-03',
        'source_type': 'archive_html',
        'source_url': 'https://ir.mara.com/news/2024-08',
        'raw_text': 'Marathon mined 600 BTC in August 2024.',
        'parsed_at': '2024-09-03T12:00:00',
    })
    db.insert_data_point({
        'report_id': r1,
        'ticker': 'MARA',
        'period': '2024-08-01',
        'metric': 'production_btc',
        'value': 600.0,
        'unit': 'BTC',
        'confidence': 0.9,
        'extraction_method': 'prod_btc_0',
        'source_snippet': 'mined 600 BTC',
    })
    r2 = db.insert_report({
        'ticker': 'MARA',
        'report_date': '2024-09-01',
        'published_date': '2024-10-03',
        'source_type': 'edgar_8k',
        'source_url': 'https://sec.gov/cgi-bin/browse-edgar',
        'raw_text': 'Marathon 8-K: mined 705 BTC in September 2024.',
        'parsed_at': '2024-10-03T12:00:00',
    })
    db.insert_company({
        'ticker': 'RIOT',
        'name': 'Riot Platforms',
        'tier': 1,
        'ir_url': '',
        'pr_base_url': None,
        'cik': None,
        'active': 1,
    })
    r3 = db.insert_report({
        'ticker': 'RIOT',
        'report_date': '2024-09-01',
        'published_date': '2024-10-01',
        'source_type': 'archive_html',
        'source_url': 'https://ir.riot.com/news/2024-09',
        'raw_text': 'Riot mined 400 BTC in September 2024.',
        'parsed_at': '2024-10-01T12:00:00',
    })
    return db, r1, r2, r3


# ── TestDocumentsEndpoint ─────────────────────────────────────────────────────

class TestDocumentsEndpoint:
    """GET /api/data/documents — document search for Mode B list panel."""

    def test_returns_list_with_required_shape(self, client, db_with_report, app):
        db, report_id = db_with_report
        import app_globals
        app_globals._db = db
        resp = client.get('/api/data/documents?ticker=MARA')
        assert resp.status_code == 200
        body = resp.get_json()
        assert body['success'] is True
        docs = body['data']
        assert isinstance(docs, list)
        assert len(docs) == 1
        doc = docs[0]
        assert doc['id'] == report_id
        assert doc['ticker'] == 'MARA'
        assert doc['source_type'] == 'archive_html'
        assert 'report_date' in doc
        assert 'source_url' in doc
        assert 'extraction_status' in doc

    def test_includes_data_point_count(self, client, db_with_report, app):
        db, report_id = db_with_report
        import app_globals
        app_globals._db = db
        resp = client.get('/api/data/documents?ticker=MARA')
        assert resp.status_code == 200
        doc = resp.get_json()['data'][0]
        assert doc['data_point_count'] == 2

    def test_filter_by_source_type(self, client, db_with_two_reports, app):
        db, r1, r2, r3 = db_with_two_reports
        import app_globals
        app_globals._db = db
        resp = client.get('/api/data/documents?ticker=MARA&source_type=edgar_8k')
        assert resp.status_code == 200
        docs = resp.get_json()['data']
        assert len(docs) == 1
        assert docs[0]['id'] == r2

    def test_filter_by_ticker_excludes_other_companies(self, client, db_with_two_reports, app):
        db, r1, r2, r3 = db_with_two_reports
        import app_globals
        app_globals._db = db
        resp = client.get('/api/data/documents?ticker=RIOT')
        assert resp.status_code == 200
        docs = resp.get_json()['data']
        assert all(d['ticker'] == 'RIOT' for d in docs)
        assert len(docs) == 1

    def test_filter_by_date_range(self, client, db_with_two_reports, app):
        db, r1, r2, r3 = db_with_two_reports
        import app_globals
        app_globals._db = db
        resp = client.get('/api/data/documents?ticker=MARA&from_date=2024-09&to_date=2024-09')
        assert resp.status_code == 200
        docs = resp.get_json()['data']
        # r1 is 2024-08, should be excluded
        ids = [d['id'] for d in docs]
        assert r1 not in ids
        assert r2 in ids

    def test_no_ticker_returns_all_documents(self, client, db_with_two_reports, app):
        db, r1, r2, r3 = db_with_two_reports
        import app_globals
        app_globals._db = db
        resp = client.get('/api/data/documents')
        assert resp.status_code == 200
        docs = resp.get_json()['data']
        ids = {d['id'] for d in docs}
        assert {r1, r2, r3} == ids

    def test_invalid_ticker_returns_400(self, client, db_with_company, app):
        import app_globals
        app_globals._db = db_with_company
        resp = client.get('/api/data/documents?ticker=NOTREAL')
        assert resp.status_code == 400
        body = resp.get_json()
        assert body['success'] is False
        assert body['error']['code'] == 'INVALID_TICKER'

    def test_invalid_date_format_returns_400(self, client, db_with_company, app):
        import app_globals
        app_globals._db = db_with_company
        resp = client.get('/api/data/documents?from_date=2024-9')
        assert resp.status_code == 400
        body = resp.get_json()
        assert body['error']['code'] == 'INVALID_DATE'

    def test_invalid_source_type_returns_400(self, client, db_with_company, app):
        import app_globals
        app_globals._db = db_with_company
        resp = client.get('/api/data/documents?source_type=not_a_real_type')
        assert resp.status_code == 400
        body = resp.get_json()
        assert body['error']['code'] == 'INVALID_SOURCE_TYPE'

    def test_result_does_not_include_raw_text(self, client, db_with_report, app):
        """List endpoint must not include raw_text — use /api/data/document/<id> for that."""
        db, report_id = db_with_report
        import app_globals
        app_globals._db = db
        resp = client.get('/api/data/documents?ticker=MARA')
        assert resp.status_code == 200
        doc = resp.get_json()['data'][0]
        assert 'raw_text' not in doc

    def test_filter_by_extraction_status(self, client, db_with_report, app):
        db, report_id = db_with_report
        import app_globals
        app_globals._db = db
        resp = client.get('/api/data/documents?ticker=MARA&extraction_status=pending')
        assert resp.status_code == 200
        # report is pending (not extracted); must appear
        docs = resp.get_json()['data']
        assert any(d['id'] == report_id for d in docs)

    def test_invalid_extraction_status_returns_400(self, client, db_with_company, app):
        import app_globals
        app_globals._db = db_with_company
        resp = client.get('/api/data/documents?extraction_status=flying')
        assert resp.status_code == 400
        body = resp.get_json()
        assert body['error']['code'] == 'INVALID_EXTRACTION_STATUS'


# ── TestDocumentViewerEndpoint ────────────────────────────────────────────────

class TestDocumentViewerEndpoint:
    """GET /api/data/document/<report_id> — full doc viewer payload for Mode B."""

    def test_returns_raw_text_and_metadata(self, client, db_with_report, app):
        db, report_id = db_with_report
        import app_globals
        app_globals._db = db
        resp = client.get(f'/api/data/document/{report_id}')
        assert resp.status_code == 200
        body = resp.get_json()
        assert body['success'] is True
        data = body['data']
        assert data['raw_text'] == 'Marathon mined 705 BTC in September 2024. Hashrate was 36.9 EH/s.'
        assert data['ticker'] == 'MARA'
        assert data['source_type'] == 'archive_html'
        assert 'report_date' in data
        assert 'source_url' in data

    def test_returns_data_points_as_matches(self, client, db_with_report, app):
        db, report_id = db_with_report
        import app_globals
        app_globals._db = db
        resp = client.get(f'/api/data/document/{report_id}')
        assert resp.status_code == 200
        matches = resp.get_json()['data']['matches']
        assert isinstance(matches, list)
        assert len(matches) == 2
        metrics = {m['metric'] for m in matches}
        assert 'production_btc' in metrics
        assert 'hashrate_eh' in metrics

    def test_matches_include_required_fields(self, client, db_with_report, app):
        db, report_id = db_with_report
        import app_globals
        app_globals._db = db
        resp = client.get(f'/api/data/document/{report_id}')
        match = resp.get_json()['data']['matches'][0]
        assert 'metric' in match
        assert 'value' in match
        assert 'unit' in match
        assert 'confidence' in match
        assert 'source_snippet' in match

    def test_nonexistent_report_returns_404(self, client, db_with_company, app):
        import app_globals
        app_globals._db = db_with_company
        resp = client.get('/api/data/document/99999')
        assert resp.status_code == 404
        body = resp.get_json()
        assert body['success'] is False
        assert body['error']['code'] == 'NOT_FOUND'

    def test_invalid_report_id_type_returns_404(self, client, db_with_company, app):
        import app_globals
        app_globals._db = db_with_company
        resp = client.get('/api/data/document/notanid')
        assert resp.status_code == 404

    def test_matches_do_not_include_id_or_report_id(self, client, db_with_report, app):
        """Match objects are safe to expose — no internal DB IDs."""
        db, report_id = db_with_report
        import app_globals
        app_globals._db = db
        resp = client.get(f'/api/data/document/{report_id}')
        for match in resp.get_json()['data']['matches']:
            assert 'id' not in match
            assert 'report_id' not in match

    def test_report_with_no_data_points_returns_empty_matches(self, client, db_with_company, app):
        db = db_with_company
        import app_globals
        app_globals._db = db
        report_id = db.insert_report({
            'ticker': 'MARA',
            'report_date': '2024-07-01',
            'published_date': '2024-08-03',
            'source_type': 'ir_press_release',
            'source_url': 'https://ir.mara.com/news/2024-07',
            'raw_text': 'Marathon press release July 2024.',
            'parsed_at': '2024-08-03T12:00:00',
        })
        resp = client.get(f'/api/data/document/{report_id}')
        assert resp.status_code == 200
        data = resp.get_json()['data']
        assert data['matches'] == []
