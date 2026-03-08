"""
TDD tests for consolidated miner data API.

Written BEFORE implementation — all tests must fail (ImportError or 404)
before any production code is written. Run to confirm failures first.
"""
import pytest
import json


# ── Fixtures ──────────────────────────────────────────────────────────────────

MARA_COMPANY = {
    'ticker': 'MARA',
    'name': 'MARA Holdings, Inc.',
    'tier': 1,
    'ir_url': 'https://www.marathondh.com/news',
    'pr_base_url': 'https://www.marathondh.com',
    'cik': '0001507605',
    'active': 1,
}

RIOT_COMPANY = {
    'ticker': 'RIOT',
    'name': 'Riot Platforms, Inc.',
    'tier': 1,
    'ir_url': 'https://ir.riotplatforms.com',
    'pr_base_url': 'https://www.riotplatforms.com',
    'cik': '0001167419',
    'active': 1,
}


@pytest.fixture
def app(tmp_path, monkeypatch):
    """Flask test client pointing at a temp DB."""
    import sys
    import os
    src_path = os.path.join(os.path.dirname(__file__), '..', 'src')
    if src_path not in sys.path:
        sys.path.insert(0, src_path)

    db_path = str(tmp_path / 'test.db')
    monkeypatch.setenv('MINERS_DB_PATH', db_path)

    import app_globals
    from infra.db import MinerDB
    test_db = MinerDB(db_path)
    app_globals._db = test_db

    import run_web
    flask_app = run_web.create_app()
    flask_app.config['TESTING'] = True
    return flask_app.test_client()


@pytest.fixture
def app_with_mara(app, tmp_path, monkeypatch):
    """Flask test client with MARA + sparse multi-metric data."""
    import app_globals
    db = app_globals._db

    db.insert_company(MARA_COMPANY)

    # Insert 3 data points with different metrics across 3 periods
    data_points = [
        # period 2022-01: has production_btc
        {
            'report_id': None,
            'ticker': 'MARA',
            'period': '2022-01-01',
            'metric': 'production_btc',
            'value': 742.0,
            'unit': 'BTC',
            'confidence': 0.91,
            'extraction_method': 'prod_btc_3',
            'source_snippet': 'mined 742 bitcoin during January',
        },
        # period 2022-01: also has hodl_btc
        {
            'report_id': None,
            'ticker': 'MARA',
            'period': '2022-01-01',
            'metric': 'hodl_btc',
            'value': 3215.0,
            'unit': 'BTC',
            'confidence': 0.88,
            'extraction_method': 'hodl_btc_1',
            'source_snippet': 'held 3,215 bitcoin',
        },
        # period 2022-03: has production_btc (gap at 2022-02)
        {
            'report_id': None,
            'ticker': 'MARA',
            'period': '2022-03-01',
            'metric': 'production_btc',
            'value': 800.0,
            'unit': 'BTC',
            'confidence': 0.95,
            'extraction_method': 'prod_btc_0',
            'source_snippet': 'mined 800 bitcoin during March',
        },
    ]
    for dp in data_points:
        db.insert_data_point(dp)

    return app


@pytest.fixture
def app_with_report(app, tmp_path, monkeypatch):
    """Flask test client with MARA data + a report with raw_text for 2022-01."""
    import app_globals
    db = app_globals._db

    db.insert_company(MARA_COMPANY)

    # Insert a report with raw_text
    report_id = db.insert_report({
        'ticker': 'MARA',
        'report_date': '2022-01-01',
        'published_date': None,
        'source_type': 'archive_html',
        'source_url': None,
        'raw_text': 'MARA mined 742 bitcoin during January 2022. Holdings of 3215 bitcoin.',
        'parsed_at': '2024-01-01T00:00:00',
    })

    # Data point linked to report
    db.insert_data_point({
        'report_id': report_id,
        'ticker': 'MARA',
        'period': '2022-01-01',
        'metric': 'production_btc',
        'value': 742.0,
        'unit': 'BTC',
        'confidence': 0.91,
        'extraction_method': 'prod_btc_3',
        'source_snippet': 'mined 742 bitcoin during January',
    })

    return app


# ── TestMinerTimeline ─────────────────────────────────────────────────────────

class TestMinerTimeline:
    """Tests for GET /api/miner/<ticker>/timeline."""

    def test_returns_pivoted_rows_for_ticker(self, app_with_mara):
        """3 data points for MARA across different metrics → pivoted row for 2022-01."""
        resp = app_with_mara.get('/api/miner/MARA/timeline')
        assert resp.status_code == 200
        body = json.loads(resp.data)
        assert body['success'] is True
        rows = body['data']['rows']
        # Find the 2022-01 row
        jan_row = next((r for r in rows if r['period_label'] == '2022-01'), None)
        assert jan_row is not None, '2022-01 row missing from timeline'
        assert jan_row['metrics']['production_btc'] is not None
        assert jan_row['metrics']['production_btc']['value'] == 742.0
        assert jan_row['metrics']['hodl_btc'] is not None
        assert jan_row['metrics']['hodl_btc']['value'] == 3215.0

    def test_includes_gap_rows_between_data_points(self, app_with_mara):
        """Data at 2022-01 and 2022-03 → 2022-02 row included, is_gap=True."""
        resp = app_with_mara.get('/api/miner/MARA/timeline')
        assert resp.status_code == 200
        body = json.loads(resp.data)
        rows = body['data']['rows']
        feb_row = next((r for r in rows if r['period_label'] == '2022-02'), None)
        assert feb_row is not None, '2022-02 gap row missing from timeline'
        assert feb_row['is_gap'] is True

    def test_gap_row_all_metrics_null(self, app_with_mara):
        """Gap row: metrics.production_btc == null, is_gap == True."""
        resp = app_with_mara.get('/api/miner/MARA/timeline')
        assert resp.status_code == 200
        body = json.loads(resp.data)
        rows = body['data']['rows']
        feb_row = next((r for r in rows if r['period_label'] == '2022-02'), None)
        assert feb_row is not None
        for metric in ('production_btc', 'hodl_btc', 'sold_btc', 'hashrate_eh', 'realization_rate'):
            assert feb_row['metrics'][metric] is None, f'{metric} should be null in gap row'

    def test_row_includes_report_metadata(self, app_with_report):
        """When report exists for period: row has has_report=True, report_id, source_type."""
        resp = app_with_report.get('/api/miner/MARA/timeline')
        assert resp.status_code == 200
        body = json.loads(resp.data)
        rows = body['data']['rows']
        jan_row = next((r for r in rows if r['period_label'] == '2022-01'), None)
        assert jan_row is not None
        assert jan_row['has_report'] is True
        assert jan_row['report_id'] is not None
        assert jan_row['source_type'] == 'archive_html'

    def test_rows_sorted_descending_by_period(self, app_with_mara):
        """Most recent period first."""
        resp = app_with_mara.get('/api/miner/MARA/timeline')
        assert resp.status_code == 200
        body = json.loads(resp.data)
        rows = body['data']['rows']
        periods = [r['period'] for r in rows]
        assert periods == sorted(periods, reverse=True), 'Rows not sorted descending'

    def test_unknown_ticker_returns_404(self, app):
        """Unknown ticker → 404."""
        resp = app.get('/api/miner/UNKNOWN/timeline')
        assert resp.status_code == 404

    def test_no_data_returns_empty_rows(self, app):
        """Company with no data points → empty rows list."""
        import app_globals
        db = app_globals._db
        db.insert_company(RIOT_COMPANY)

        resp = app.get('/api/miner/RIOT/timeline')
        assert resp.status_code == 200
        body = json.loads(resp.data)
        assert body['success'] is True
        assert body['data']['rows'] == []

    def test_response_includes_company_metadata(self, app_with_mara):
        """Response includes company name, ir_url, CIK."""
        resp = app_with_mara.get('/api/miner/MARA/timeline')
        assert resp.status_code == 200
        body = json.loads(resp.data)
        company = body['data']['company']
        assert company['ticker'] == 'MARA'
        assert company['name'] == 'MARA Holdings, Inc.'
        assert company['cik'] == '0001507605'

    def test_response_includes_stats(self, app_with_mara):
        """Response includes stats: total_periods, gap_periods, first_period, last_period."""
        resp = app_with_mara.get('/api/miner/MARA/timeline')
        assert resp.status_code == 200
        body = json.loads(resp.data)
        stats = body['data']['stats']
        assert 'total_periods' in stats
        assert 'gap_periods' in stats
        assert stats['first_period'] == '2022-01'
        assert stats['last_period'] == '2022-03'

    def test_ticker_lookup_case_insensitive(self, app_with_mara):
        """Ticker lookup is case-insensitive."""
        resp = app_with_mara.get('/api/miner/mara/timeline')
        assert resp.status_code == 200


# ── TestMinerAnalysis ─────────────────────────────────────────────────────────

class TestMinerAnalysis:
    """Tests for GET /api/miner/<ticker>/<period>/analysis."""

    def test_returns_matches_for_report_with_text(self, app_with_report):
        """Report with text matching a pattern → matches list has metric, pattern_id, value."""
        resp = app_with_report.get('/api/miner/MARA/2022-01/analysis')
        assert resp.status_code == 200
        body = json.loads(resp.data)
        assert body['success'] is True
        data = body['data']
        assert data['has_source'] is True
        # At least one match should be found (prod_btc patterns match '742 bitcoin')
        assert isinstance(data['matches'], list)

    def test_no_report_has_source_false(self, app_with_mara):
        """No source document for this period → has_source=False.
        Stored data_points (manually entered or previously extracted) may still appear
        in matches — the endpoint reads from DB, not from a report document."""
        resp = app_with_mara.get('/api/miner/MARA/2022-01/analysis')
        assert resp.status_code == 200
        body = json.loads(resp.data)
        assert body['data']['has_source'] is False
        # data_points exist for 2022-01 (inserted by fixture) so matches is non-empty
        assert isinstance(body['data']['matches'], list)

    def test_report_with_no_matching_text_returns_empty_matches(self, app):
        """Report exists but text has no pattern matches → empty matches."""
        import app_globals
        db = app_globals._db
        db.insert_company(MARA_COMPANY)
        db.insert_report({
            'ticker': 'MARA',
            'report_date': '2022-01-01',
            'published_date': None,
            'source_type': 'archive_html',
            'source_url': None,
            'raw_text': 'No production numbers here. Just random text.',
            'parsed_at': '2024-01-01T00:00:00',
        })

        resp = app.get('/api/miner/MARA/2022-01/analysis')
        assert resp.status_code == 200
        body = json.loads(resp.data)
        assert body['data']['has_source'] is True
        assert body['data']['matches'] == []

    def test_unknown_ticker_returns_404(self, app):
        """Unknown ticker → 404."""
        resp = app.get('/api/miner/UNKNOWN/2022-01/analysis')
        assert resp.status_code == 404

    def test_ym_period_format_accepted(self, app_with_report):
        """Accepts YYYY-MM format (without -DD)."""
        resp = app_with_report.get('/api/miner/MARA/2022-01/analysis')
        assert resp.status_code == 200

    def test_response_shape(self, app_with_report):
        """Response has ticker, period, has_source, matches fields."""
        resp = app_with_report.get('/api/miner/MARA/2022-01/analysis')
        assert resp.status_code == 200
        body = json.loads(resp.data)
        data = body['data']
        assert 'ticker' in data
        assert 'period' in data
        assert 'has_source' in data
        assert 'matches' in data

    def test_match_shape_has_required_fields(self, app_with_report):
        """Each match object has metric, pattern_id, value, confidence, source_snippet."""
        resp = app_with_report.get('/api/miner/MARA/2022-01/analysis')
        assert resp.status_code == 200
        body = json.loads(resp.data)
        matches = body['data']['matches']
        if matches:
            m = matches[0]
            assert 'metric' in m
            assert 'pattern_id' in m
            assert 'value' in m
            assert 'confidence' in m
            assert 'source_snippet' in m


# ── TestMinerRawSource ────────────────────────────────────────────────────────

class TestMinerRawSource:
    """Tests for GET /api/miner/<ticker>/<period>/raw-source."""

    def test_returns_html_for_existing_report(self, app_with_report):
        """Report with raw_text returns 200 with text/html content-type."""
        resp = app_with_report.get('/api/miner/MARA/2022-01/raw-source')
        assert resp.status_code == 200
        assert 'text/html' in resp.content_type

    def test_returns_raw_text_content(self, app_with_report):
        """Response body is the exact raw_text stored in the report."""
        resp = app_with_report.get('/api/miner/MARA/2022-01/raw-source')
        assert b'742 bitcoin' in resp.data

    def test_ym_period_format_accepted(self, app_with_report):
        """YYYY-MM period format (without -DD) is accepted and normalised."""
        resp = app_with_report.get('/api/miner/MARA/2022-01/raw-source')
        assert resp.status_code == 200

    def test_unknown_ticker_returns_404(self, app):
        """Unknown ticker returns 404 JSON error."""
        resp = app.get('/api/miner/ZZZZ/2022-01/raw-source')
        assert resp.status_code == 404
        body = json.loads(resp.data)
        assert body['success'] is False

    def test_no_report_for_period_returns_404(self, app_with_mara):
        """Period with no matching report returns 404 JSON error."""
        resp = app_with_mara.get('/api/miner/MARA/2099-01/raw-source')
        assert resp.status_code == 404
        body = json.loads(resp.data)
        assert body['success'] is False

    def test_ticker_lookup_case_insensitive(self, app_with_report):
        """Ticker lookup is case-insensitive (lowercase ticker works)."""
        resp = app_with_report.get('/api/miner/mara/2022-01/raw-source')
        assert resp.status_code == 200


# ── TestMinerFill ─────────────────────────────────────────────────────────────

class TestMinerFill:
    """Tests for POST /api/miner/<ticker>/<period>/<metric>/fill."""

    def test_fill_inserts_data_point(self, app_with_mara):
        """POST fill inserts data point; subsequent timeline shows the value."""
        resp = app_with_mara.post(
            '/api/miner/MARA/2022-02/production_btc/fill',
            json={'value': 500.0},
        )
        assert resp.status_code == 200
        body = json.loads(resp.data)
        assert body['success'] is True

        # Timeline should now show value for 2022-02 production_btc
        timeline = app_with_mara.get('/api/miner/MARA/timeline')
        tdata = json.loads(timeline.data)['data']
        row = next((r for r in tdata['rows'] if r['period_label'] == '2022-02'), None)
        assert row is not None
        assert row['metrics']['production_btc'] is not None
        assert row['metrics']['production_btc']['value'] == 500.0

    def test_fill_returns_success_shape(self, app_with_mara):
        """Response has success=True, data.ticker, data.period, data.metric, data.value."""
        resp = app_with_mara.post(
            '/api/miner/MARA/2022-02/production_btc/fill',
            json={'value': 500.0, 'note': 'from press release'},
        )
        assert resp.status_code == 200
        body = json.loads(resp.data)
        assert body['success'] is True
        d = body['data']
        assert d['ticker'] == 'MARA'
        assert d['period'] == '2022-02-01'
        assert d['metric'] == 'production_btc'
        assert d['value'] == 500.0

    def test_fill_overwrites_existing_value(self, app_with_mara):
        """2022-01 already has production_btc=742 → fill 600 → value becomes 600."""
        resp = app_with_mara.post(
            '/api/miner/MARA/2022-01/production_btc/fill',
            json={'value': 600.0},
        )
        assert resp.status_code == 200

        timeline = app_with_mara.get('/api/miner/MARA/timeline')
        tdata = json.loads(timeline.data)['data']
        row = next((r for r in tdata['rows'] if r['period_label'] == '2022-01'), None)
        assert row is not None
        assert row['metrics']['production_btc']['value'] == 600.0

    def test_fill_unknown_ticker_returns_404(self, app):
        """Unknown ticker → 404."""
        resp = app.post(
            '/api/miner/ZZZZ/2022-02/production_btc/fill',
            json={'value': 500.0},
        )
        assert resp.status_code == 404

    def test_fill_unknown_metric_returns_400(self, app_with_mara):
        """Unknown metric → 400."""
        resp = app_with_mara.post(
            '/api/miner/MARA/2022-02/not_a_metric/fill',
            json={'value': 500.0},
        )
        assert resp.status_code == 400

    def test_fill_negative_value_returns_400(self, app_with_mara):
        """Non-positive value → 400."""
        resp = app_with_mara.post(
            '/api/miner/MARA/2022-02/production_btc/fill',
            json={'value': -1.0},
        )
        assert resp.status_code == 400

    def test_fill_ym_period_format_accepted(self, app_with_mara):
        """YYYY-MM period format (without -DD) is accepted and normalised."""
        resp = app_with_mara.post(
            '/api/miner/MARA/2022-02/production_btc/fill',
            json={'value': 500.0},
        )
        assert resp.status_code == 200
        body = json.loads(resp.data)
        # Period in response is normalised to YYYY-MM-01
        assert body['data']['period'] == '2022-02-01'
