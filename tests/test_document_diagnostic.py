"""Tests for Track B — Document Diagnostic UI.

Covers:
  - DB method: MinerDB.scan_document_keywords(report_id, phrases) -> list[dict]
  - Extended search_reports() columns: parse_quality, char_count
  - New API endpoint: GET /api/data/documents/<id>/keywords
  - Filter param: search_reports(parse_quality=...) and search_reports(extraction_status=...)

All 14 tests are expected to FAIL before implementation.
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
def report_with_text(db_with_company):
    """Insert a MARA report whose raw_text contains a known phrase."""
    db = db_with_company
    raw = 'Marathon mined 705 bitcoin mined in September 2024. bitcoin mined again later.'
    report_id = db.insert_report({
        'ticker': 'MARA',
        'report_date': '2024-09-01',
        'published_date': '2024-10-01',
        'source_type': 'archive_html',
        'source_url': 'https://ir.mara.com/news/diag-test-1',
        'raw_text': raw,
        'parsed_at': '2024-10-01T00:00:00',
    })
    db.set_report_parse_quality(report_id, 'ok')
    return db, report_id, raw


@pytest.fixture
def report_with_mixed_case(db_with_company):
    """Insert a MARA report whose raw_text uses mixed-case phrase."""
    db = db_with_company
    raw = 'Marathon produced Bitcoin Mined 750 BTC in October 2024.'
    report_id = db.insert_report({
        'ticker': 'MARA',
        'report_date': '2024-10-01',
        'published_date': '2024-11-01',
        'source_type': 'archive_html',
        'source_url': 'https://ir.mara.com/news/diag-test-2',
        'raw_text': raw,
        'parsed_at': '2024-11-01T00:00:00',
    })
    return db, report_id, raw


# ── DB: scan_document_keywords ────────────────────────────────────────────────

class TestScanDocumentKeywordsDB:
    """Unit tests for MinerDB.scan_document_keywords()."""

    def test_scan_document_keywords_finds_phrase(self, report_with_text):
        """Phrase present in raw_text returns found=True with count >= 1."""
        db, report_id, raw = report_with_text
        results = db.scan_document_keywords(report_id, ['bitcoin mined'])
        assert isinstance(results, list)
        assert len(results) == 1
        r = results[0]
        assert r['phrase'] == 'bitcoin mined'
        assert r['found'] is True
        assert r['count'] == 2

    def test_scan_document_keywords_case_insensitive(self, report_with_mixed_case):
        """Search is case-insensitive: 'Bitcoin Mined' found when searching 'bitcoin mined'."""
        db, report_id, raw = report_with_mixed_case
        results = db.scan_document_keywords(report_id, ['bitcoin mined'])
        assert len(results) == 1
        r = results[0]
        assert r['found'] is True
        assert r['count'] >= 1

    def test_scan_document_keywords_returns_offsets(self, report_with_text):
        """Offsets match the actual byte positions in the lowercased raw_text."""
        db, report_id, raw = report_with_text
        results = db.scan_document_keywords(report_id, ['bitcoin mined'])
        r = results[0]
        assert isinstance(r['offsets'], list)
        assert len(r['offsets']) == r['count']
        lowered = raw.lower()
        phrase = 'bitcoin mined'
        for offset in r['offsets']:
            assert lowered[offset:offset + len(phrase)] == phrase

    def test_scan_document_keywords_not_found(self, report_with_text):
        """Phrase absent from raw_text returns found=False, count=0, offsets=[]."""
        db, report_id, raw = report_with_text
        results = db.scan_document_keywords(report_id, ['hashrate exahash'])
        assert len(results) == 1
        r = results[0]
        assert r['phrase'] == 'hashrate exahash'
        assert r['found'] is False
        assert r['count'] == 0
        assert r['offsets'] == []

    def test_scan_document_keywords_multiple_phrases(self, report_with_text):
        """Scanning 3 phrases returns 3 result dicts in the same order."""
        db, report_id, raw = report_with_text
        phrases = ['bitcoin mined', 'september 2024', 'nonexistent phrase xyz']
        results = db.scan_document_keywords(report_id, phrases)
        assert len(results) == 3
        returned_phrases = [r['phrase'] for r in results]
        assert returned_phrases == phrases
        # First two should be found, last should not
        assert results[0]['found'] is True
        assert results[1]['found'] is True
        assert results[2]['found'] is False


# ── DB: search_reports extended columns ───────────────────────────────────────

class TestSearchReportsExtendedColumns:
    """search_reports() must now return parse_quality and char_count."""

    def test_search_reports_returns_parse_quality(self, report_with_text):
        """search_reports() result includes parse_quality field."""
        db, report_id, raw = report_with_text
        rows = db.search_reports(ticker='MARA')
        assert len(rows) >= 1
        row = next(r for r in rows if r['id'] == report_id)
        assert 'parse_quality' in row
        assert row['parse_quality'] == 'ok'

    def test_search_reports_returns_extraction_status(self, report_with_text):
        """search_reports() result includes extraction_status (existing field, still present)."""
        db, report_id, raw = report_with_text
        rows = db.search_reports(ticker='MARA')
        row = next(r for r in rows if r['id'] == report_id)
        assert 'extraction_status' in row

    def test_search_reports_returns_char_count(self, report_with_text):
        """char_count in result matches len(raw_text) for the inserted report."""
        db, report_id, raw = report_with_text
        rows = db.search_reports(ticker='MARA')
        row = next(r for r in rows if r['id'] == report_id)
        assert 'char_count' in row
        assert row['char_count'] == len(raw)


# ── API: GET /api/data/documents (extended columns) ───────────────────────────

class TestDocumentsAPIExtendedColumns:
    """The /api/data/documents list endpoint must include parse_quality and char_count."""

    def test_documents_api_returns_diagnostic_columns(self, client, report_with_text, app):
        """GET /api/data/documents returns items with parse_quality and char_count."""
        db, report_id, raw = report_with_text
        import app_globals
        app_globals._db = db
        resp = client.get('/api/data/documents?ticker=MARA')
        assert resp.status_code == 200
        body = resp.get_json()
        assert body['success'] is True
        docs = body['data']
        assert len(docs) >= 1
        doc = next(d for d in docs if d['id'] == report_id)
        assert 'parse_quality' in doc
        assert 'char_count' in doc
        assert doc['parse_quality'] == 'ok'
        assert doc['char_count'] == len(raw)


# ── API: GET /api/data/documents/<id>/keywords ────────────────────────────────

class TestDocumentKeywordsEndpoint:
    """Tests for the new GET /api/data/documents/<id>/keywords endpoint."""

    def test_documents_keywords_api_valid(self, client, report_with_text, app):
        """Valid request returns 200 with found result for known phrase."""
        db, report_id, raw = report_with_text
        import app_globals
        app_globals._db = db
        resp = client.get(
            f'/api/data/documents/{report_id}/keywords?phrases=bitcoin+mined'
        )
        assert resp.status_code == 200
        body = resp.get_json()
        assert body['success'] is True
        data = body['data']
        assert data['report_id'] == report_id
        assert isinstance(data['results'], list)
        assert len(data['results']) == 1
        r = data['results'][0]
        assert r['phrase'] == 'bitcoin mined'
        assert r['found'] is True
        assert r['count'] >= 1
        assert isinstance(r['offsets'], list)

    def test_documents_keywords_api_empty_phrases(self, client, report_with_text, app):
        """Missing or empty phrases param returns 400."""
        db, report_id, raw = report_with_text
        import app_globals
        app_globals._db = db
        resp = client.get(f'/api/data/documents/{report_id}/keywords')
        assert resp.status_code == 400
        body = resp.get_json()
        assert body['success'] is False

    def test_documents_keywords_api_unknown_report_404(self, client, db_with_company, app):
        """Request for a non-existent report_id returns 404."""
        import app_globals
        app_globals._db = db_with_company
        resp = client.get('/api/data/documents/99999/keywords?phrases=foo')
        assert resp.status_code == 404
        body = resp.get_json()
        assert body['success'] is False


# ── DB: filter params for search_reports ─────────────────────────────────────

class TestSearchReportsFilterParams:
    """search_reports() must support filtering by parse_quality."""

    def test_filter_by_parse_quality(self, db_with_company):
        """search_reports(parse_quality='ok') returns only reports with that quality."""
        db = db_with_company
        r_ok = db.insert_report({
            'ticker': 'MARA',
            'report_date': '2024-09-01',
            'published_date': '2024-10-01',
            'source_type': 'archive_html',
            'source_url': 'https://ir.mara.com/news/pq-ok',
            'raw_text': 'text ok',
            'parsed_at': '2024-10-01T00:00:00',
        })
        db.set_report_parse_quality(r_ok, 'ok')
        r_failed = db.insert_report({
            'ticker': 'MARA',
            'report_date': '2024-10-01',
            'published_date': '2024-11-01',
            'source_type': 'archive_html',
            'source_url': 'https://ir.mara.com/news/pq-failed',
            'raw_text': '',
            'parsed_at': '2024-11-01T00:00:00',
        })
        db.set_report_parse_quality(r_failed, 'parse_failed')
        rows = db.search_reports(ticker='MARA', parse_quality='ok')
        returned_ids = {r['id'] for r in rows}
        assert r_ok in returned_ids
        assert r_failed not in returned_ids

    def test_filter_by_extraction_status(self, db_with_company):
        """search_reports(extraction_status='done') returns only done reports with parse_quality."""
        db = db_with_company
        r_done = db.insert_report({
            'ticker': 'MARA',
            'report_date': '2024-09-01',
            'published_date': '2024-10-01',
            'source_type': 'archive_html',
            'source_url': 'https://ir.mara.com/news/es-done',
            'raw_text': 'done text',
            'parsed_at': '2024-10-01T00:00:00',
        })
        db.set_report_parse_quality(r_done, 'ok')
        db.mark_report_extracted(r_done)
        r_pending = db.insert_report({
            'ticker': 'MARA',
            'report_date': '2024-10-01',
            'published_date': '2024-11-01',
            'source_type': 'archive_html',
            'source_url': 'https://ir.mara.com/news/es-pending',
            'raw_text': 'pending text',
            'parsed_at': '2024-11-01T00:00:00',
        })
        rows = db.search_reports(ticker='MARA', extraction_status='done')
        returned_ids = {r['id'] for r in rows}
        assert r_done in returned_ids
        assert r_pending not in returned_ids
        # Verify parse_quality is present in the result rows
        done_row = next(r for r in rows if r['id'] == r_done)
        assert 'parse_quality' in done_row
