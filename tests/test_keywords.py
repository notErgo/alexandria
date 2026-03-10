"""
Tests for metric keywords and metric schema SSOT.

search_keywords was retired in v30 (table kept empty, Python access code removed).
Keyword management now lives at /api/metric_schema/<key>/keywords (per-metric, v30+).
"""
import pytest
from infra.db import MinerDB


@pytest.fixture
def app(db_with_company, tmp_path):
    import sys, os
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))
    import app_globals
    app_globals._db = db_with_company

    import importlib, run_web
    importlib.reload(run_web)
    flask_app = run_web.create_app()
    flask_app.config['TESTING'] = True
    return flask_app


@pytest.fixture
def client(app):
    return app.test_client()


# ── Metric schema SSOT tests ───────────────────────────────────────────────────

class TestMetricSchemaSSOT:
    def test_metric_schema_seeds_13_metrics(self, db):
        """Fresh DB must have exactly 13 seeded metrics in metric_schema."""
        rows = db.get_metric_schema('BTC-miners', active_only=False)
        assert len(rows) == 13, (
            f"Expected 13 seeded metric_schema rows, got {len(rows)}: {[r['key'] for r in rows]}"
        )

    def test_metric_schema_includes_all_13_keys(self, db):
        """All 13 expected metric keys must be present in metric_schema."""
        expected_keys = {
            'production_btc', 'holdings_btc', 'sales_btc', 'hashrate_eh', 'realization_rate',
            'ai_hpc_mw', 'encumbered_btc', 'gpu_count', 'restricted_holdings_btc',
            'unrestricted_holdings', 'hpc_revenue_usd', 'mining_mw', 'net_btc_balance_change',
        }
        rows = db.get_metric_schema('BTC-miners', active_only=False)
        actual_keys = {r['key'] for r in rows}
        missing = expected_keys - actual_keys
        assert not missing, f"Missing metric_schema keys: {missing}"

    def test_btc_first_filing_date_crud(self, db_with_company):
        """get/set btc_first_filing_date round-trip for a company."""
        assert db_with_company.get_btc_first_filing_date('MARA') is None
        db_with_company.set_btc_first_filing_date('MARA', '2017-09-15')
        assert db_with_company.get_btc_first_filing_date('MARA') == '2017-09-15'


# ── Per-metric keyword route tests ─────────────────────────────────────────────

class TestMetricKeywordsRoutes:
    """
    Tests for GET/POST/PATCH/DELETE /api/metric_schema/<key>/keywords.
    Keywords are attached to a specific metric_schema key and used as
    EDGAR anchor phrases for first-filing detection and LLM extraction context.
    """

    def test_list_metric_keywords_returns_seeded_rows(self, client):
        """GET /api/metric_schema/production_btc/keywords returns seeded rows."""
        import json
        resp = client.get('/api/metric_schema/production_btc/keywords')
        assert resp.status_code == 200
        body = json.loads(resp.data)
        assert body['success'] is True
        assert 'keywords' in body['data']
        # production_btc is seeded with 7 phrases in v30
        assert len(body['data']['keywords']) == 7

    def test_list_metric_keywords_unknown_key_404(self, client):
        """GET /api/metric_schema/nonexistent/keywords returns 404."""
        import json
        resp = client.get('/api/metric_schema/nonexistent_metric/keywords')
        assert resp.status_code == 404

    def test_add_metric_keyword_201(self, client):
        """POST /api/metric_schema/production_btc/keywords adds a new phrase."""
        import json
        resp = client.post(
            '/api/metric_schema/production_btc/keywords',
            data=json.dumps({'phrase': '"btc mined this quarter"'}),
            content_type='application/json',
        )
        assert resp.status_code == 201
        body = json.loads(resp.data)
        assert body['success'] is True
        assert body['data']['phrase'] == 'btc mined this quarter'
        assert body['data']['metric_key'] == 'production_btc'
        assert 'id' in body['data']

    def test_add_metric_keyword_missing_phrase_400(self, client):
        """POST without phrase field returns 400."""
        import json
        resp = client.post(
            '/api/metric_schema/production_btc/keywords',
            data=json.dumps({'notes': 'no phrase'}),
            content_type='application/json',
        )
        assert resp.status_code == 400
        body = json.loads(resp.data)
        assert body['success'] is False

    def test_add_metric_keyword_bare_phrase_stored_unquoted(self, client):
        """Bare phrase is stored without added quotes."""
        import json
        resp = client.post(
            '/api/metric_schema/hashrate_eh/keywords',
            data=json.dumps({'phrase': 'exahash per second'}),
            content_type='application/json',
        )
        assert resp.status_code == 201
        body = json.loads(resp.data)
        assert body['data']['phrase'] == 'exahash per second'

    def test_add_metric_keyword_duplicate_409(self, client, db):
        """Adding a duplicate phrase for the same metric returns 409."""
        import json
        rows = db.get_metric_keywords('production_btc', active_only=False)
        existing = rows[0]['phrase']
        resp = client.post(
            '/api/metric_schema/production_btc/keywords',
            data=json.dumps({'phrase': existing}),
            content_type='application/json',
        )
        assert resp.status_code == 409

    def test_patch_metric_keyword_active(self, client, db):
        """PATCH /api/metric_schema/<key>/keywords/<id> toggles active flag."""
        import json
        rows = db.get_metric_keywords('production_btc', active_only=True)
        kw_id = rows[0]['id']
        resp = client.patch(
            f'/api/metric_schema/production_btc/keywords/{kw_id}',
            data=json.dumps({'active': 0}),
            content_type='application/json',
        )
        assert resp.status_code == 200
        body = json.loads(resp.data)
        assert body['success'] is True
        updated = db.get_metric_keywords('production_btc', active_only=False)
        row = next(r for r in updated if r['id'] == kw_id)
        assert row['active'] == 0

    def test_patch_metric_keyword_404(self, client):
        """PATCH with non-existent keyword id returns 404."""
        import json
        resp = client.patch(
            '/api/metric_schema/production_btc/keywords/999999',
            data=json.dumps({'active': 0}),
            content_type='application/json',
        )
        assert resp.status_code == 404

    def test_delete_metric_keyword_200(self, client, db):
        """DELETE /api/metric_schema/<key>/keywords/<id> removes the row."""
        import json
        kw_id = db.add_metric_keyword('production_btc', '"delete test phrase"')
        resp = client.delete(f'/api/metric_schema/production_btc/keywords/{kw_id}')
        assert resp.status_code == 200
        body = json.loads(resp.data)
        assert body['success'] is True
        remaining = db.get_metric_keywords('production_btc', active_only=False)
        assert all(r['id'] != kw_id for r in remaining)

    def test_delete_metric_keyword_404(self, client):
        """DELETE with non-existent id returns 404."""
        import json
        resp = client.delete('/api/metric_schema/production_btc/keywords/999999')
        assert resp.status_code == 404

    def test_list_metric_keywords_active_only_filter(self, client, db):
        """?all=1 includes inactive rows; default returns active only."""
        import json
        rows = db.get_metric_keywords('production_btc', active_only=True)
        kw_id = rows[0]['id']
        db.update_metric_keyword(kw_id, active=0)
        resp_active = client.get('/api/metric_schema/production_btc/keywords')
        resp_all = client.get('/api/metric_schema/production_btc/keywords?all=1')
        active_count = json.loads(resp_active.data)['data']['total']
        all_count = json.loads(resp_all.data)['data']['total']
        assert all_count == active_count + 1

    def test_metric_keywords_rows_include_hit_count(self, client):
        """Each keyword row includes hit_count field."""
        import json
        resp = client.get('/api/metric_schema/production_btc/keywords')
        body = json.loads(resp.data)
        assert body['success'] is True
        kws = body['data']['keywords']
        assert len(kws) > 0
        for row in kws:
            assert 'hit_count' in row

    # ── Bulk add (CSV paste) ────────────────────────────────────────────────

    def test_add_bulk_phrases_201(self, client):
        """POST with phrases array adds multiple keywords in one request."""
        import json
        resp = client.post(
            '/api/metric_schema/production_btc/keywords',
            data=json.dumps({'phrases': ['"bulk one"', '"bulk two"', '"bulk three"']}),
            content_type='application/json',
        )
        assert resp.status_code == 201
        body = json.loads(resp.data)
        assert body['success'] is True
        assert body['data']['added'] == 3
        assert body['data']['skipped'] == 0

    def test_add_bulk_phrases_csv_string_parsed(self, client):
        """POST with csv string parses into multiple phrases."""
        import json
        resp = client.post(
            '/api/metric_schema/production_btc/keywords',
            data=json.dumps({'csv': '"csv one", "csv two"'}),
            content_type='application/json',
        )
        assert resp.status_code == 201
        body = json.loads(resp.data)
        assert body['data']['added'] == 2

    def test_add_bulk_phrases_deduplicates_existing(self, client, db):
        """Bulk add skips phrases that already exist; returns skipped count."""
        import json
        existing = db.get_metric_keywords('production_btc', active_only=False)
        existing_phrase = existing[0]['phrase']
        resp = client.post(
            '/api/metric_schema/production_btc/keywords',
            data=json.dumps({'phrases': [existing_phrase, '"brand new bulk"']}),
            content_type='application/json',
        )
        assert resp.status_code == 201
        body = json.loads(resp.data)
        assert body['data']['added'] == 1
        assert body['data']['skipped'] == 1

    def test_add_bulk_empty_phrases_400(self, client):
        """Bulk add with empty list returns 400."""
        import json
        resp = client.post(
            '/api/metric_schema/production_btc/keywords',
            data=json.dumps({'phrases': []}),
            content_type='application/json',
        )
        assert resp.status_code == 400

    # ── Exclude terms ────────────────────────────────────────────────────────

    def test_add_metric_keyword_with_exclude_terms(self, client):
        """POST /api/.../keywords stores exclude_terms alongside phrase."""
        import json
        resp = client.post(
            '/api/metric_schema/holdings_btc/keywords',
            data=json.dumps({
                'phrase': '"holdings"',
                'exclude_terms': 'Digital Holdings,Marathon Holdings',
            }),
            content_type='application/json',
        )
        assert resp.status_code == 201
        body = json.loads(resp.data)
        assert body['data']['exclude_terms'] == 'Digital Holdings,Marathon Holdings'

    def test_patch_metric_keyword_exclude_terms(self, client, db):
        """PATCH /api/.../keywords/<id> can update exclude_terms."""
        import json
        kw_id = db.add_metric_keyword('holdings_btc', '"btc holdings"')
        resp = client.patch(
            f'/api/metric_schema/holdings_btc/keywords/{kw_id}',
            data=json.dumps({'exclude_terms': 'Digital Holdings'}),
            content_type='application/json',
        )
        assert resp.status_code == 200
        rows = db.get_metric_keywords('holdings_btc', active_only=False)
        row = next(r for r in rows if r['id'] == kw_id)
        assert row['exclude_terms'] == 'Digital Holdings'

    def test_get_metric_keywords_returns_exclude_terms(self, client, db):
        """GET /api/.../keywords includes exclude_terms field in each row."""
        import json
        db.add_metric_keyword('holdings_btc', '"total holdings"',
                              exclude_terms='Digital Holdings')
        resp = client.get('/api/metric_schema/holdings_btc/keywords?all=1')
        body = json.loads(resp.data)
        assert body['success'] is True
        kws = body['data']['keywords']
        for row in kws:
            assert 'exclude_terms' in row
        row = next(r for r in kws if r['phrase'] == '"total holdings"')
        assert row['exclude_terms'] == 'Digital Holdings'

    def test_db_add_metric_keyword_with_exclude_terms(self, db):
        """DB-level: add_metric_keyword stores and returns exclude_terms."""
        kw_id = db.add_metric_keyword(
            'holdings_btc', '"bitcoin reserve"', exclude_terms='Digital Holdings'
        )
        rows = db.get_metric_keywords('holdings_btc', active_only=False)
        row = next(r for r in rows if r['id'] == kw_id)
        assert row['exclude_terms'] == 'Digital Holdings'

    def test_db_update_metric_keyword_exclude_terms(self, db):
        """DB-level: update_metric_keyword can set and clear exclude_terms."""
        kw_id = db.add_metric_keyword('holdings_btc', '"reserves"')
        db.update_metric_keyword(kw_id, exclude_terms='Miner Holdings')
        rows = db.get_metric_keywords('holdings_btc', active_only=False)
        row = next(r for r in rows if r['id'] == kw_id)
        assert row['exclude_terms'] == 'Miner Holdings'
        # Clear it
        db.update_metric_keyword(kw_id, exclude_terms='')
        rows = db.get_metric_keywords('holdings_btc', active_only=False)
        row = next(r for r in rows if r['id'] == kw_id)
        assert row['exclude_terms'] == ''
