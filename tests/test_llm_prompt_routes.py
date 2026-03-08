"""
Tests for LLM prompt DB store and editor API routes.

  GET  /api/llm_prompts           — list all prompts (one per metric + default)
  GET  /api/llm_prompts/<metric>  — get active prompt for a metric
  POST /api/llm_prompts/<metric>  — update prompt for a metric

TDD: tests written before implementation.
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


# ── DB-level tests ────────────────────────────────────────────────────────────

class TestLlmPromptDB:
    def test_get_prompt_returns_none_when_no_row(self, db):
        result = db.get_llm_prompt('production_btc')
        assert result is None

    def test_upsert_and_get_round_trip(self, db):
        db.upsert_llm_prompt('production_btc', 'Extract the monthly BTC production figure.', 'test-model')
        result = db.get_llm_prompt('production_btc')
        assert result is not None
        assert result['metric'] == 'production_btc'
        assert 'monthly BTC production' in result['prompt_text']

    def test_upsert_updates_existing(self, db):
        db.upsert_llm_prompt('production_btc', 'Old prompt text.', 'model-1')
        db.upsert_llm_prompt('production_btc', 'New improved prompt.', 'model-2')
        result = db.get_llm_prompt('production_btc')
        assert result['prompt_text'] == 'New improved prompt.'

    def test_list_prompts(self, db):
        db.upsert_llm_prompt('production_btc', 'Prompt A', 'model-1')
        db.upsert_llm_prompt('hodl_btc', 'Prompt B', 'model-1')
        prompts = db.list_llm_prompts()
        assert len(prompts) == 2
        metrics = {p['metric'] for p in prompts}
        assert 'production_btc' in metrics
        assert 'hodl_btc' in metrics


# ── Route-level tests ─────────────────────────────────────────────────────────

class TestLlmPromptRoutes:
    def test_list_prompts_empty(self, client):
        resp = client.get('/api/llm_prompts')
        assert resp.status_code == 200
        data = resp.get_json()
        assert data['success'] is True
        assert isinstance(data['data']['prompts'], list)

    def test_get_prompt_no_override_returns_default(self, client):
        """GET with no DB override returns success=True, prompt=null, default_prompt set."""
        resp = client.get('/api/llm_prompts/production_btc')
        assert resp.status_code == 200
        data = resp.get_json()
        assert data['success'] is True
        assert data['data']['prompt'] is None
        assert data['data']['default_prompt'] is not None
        assert len(data['data']['default_prompt']) > 0

    def test_update_prompt(self, client):
        resp = client.post(
            '/api/llm_prompts/production_btc',
            json={'prompt_text': 'Extract monthly BTC mined.'}
        )
        assert resp.status_code == 200
        assert resp.get_json()['success'] is True

    def test_get_prompt_after_update(self, client):
        client.post('/api/llm_prompts/production_btc',
                    json={'prompt_text': 'New prompt text here.'})
        resp = client.get('/api/llm_prompts/production_btc')
        assert resp.status_code == 200
        data = resp.get_json()
        assert data['success'] is True
        assert data['data']['prompt']['prompt_text'] == 'New prompt text here.'

    def test_update_prompt_empty_text_rejected(self, client):
        resp = client.post('/api/llm_prompts/production_btc', json={'prompt_text': ''})
        assert resp.status_code == 400
        assert resp.get_json()['error']['code'] == 'INVALID_INPUT'

    def test_update_prompt_missing_text_rejected(self, client):
        resp = client.post('/api/llm_prompts/production_btc', json={})
        assert resp.status_code == 400

    def test_update_invalid_metric_rejected(self, client):
        resp = client.post('/api/llm_prompts/nonexistent_metric',
                           json={'prompt_text': 'Some text'})
        assert resp.status_code == 400
        assert resp.get_json()['error']['code'] == 'INVALID_METRIC'

    def test_list_prompts_after_updates(self, client):
        client.post('/api/llm_prompts/production_btc', json={'prompt_text': 'P1'})
        client.post('/api/llm_prompts/hodl_btc', json={'prompt_text': 'P2'})
        resp = client.get('/api/llm_prompts')
        data = resp.get_json()
        assert len(data['data']['prompts']) == 2

    def test_preview_prompt_returns_assembled_text(self, client):
        """GET /api/llm_prompts/preview returns the full assembled prompt string."""
        resp = client.get('/api/llm_prompts/preview')
        assert resp.status_code == 200
        data = resp.get_json()
        assert data['success'] is True
        prompt = data['data']['prompt']
        assert isinstance(prompt, str)
        assert len(prompt) > 0
        # Must contain all major structural sections
        assert '=== METRIC:' in prompt
        assert '=== OUTPUT FORMAT ===' in prompt

    def test_preview_prompt_includes_active_keywords(self, client, db_with_company):
        """Per-metric keywords from metric_keywords table appear in the rendered preview."""
        import app_globals
        db = app_globals.get_db()
        db.add_metric_keyword('hashrate_eh', '"hashrate growth"')
        resp = client.get('/api/llm_prompts/preview')
        assert resp.status_code == 200
        prompt = resp.get_json()['data']['prompt']
        assert '"hashrate growth"' in prompt

    def test_preview_prompt_accepts_ticker_param(self, client):
        """?ticker=X is accepted and does not crash (hint may or may not be set)."""
        resp = client.get('/api/llm_prompts/preview?ticker=MARA')
        assert resp.status_code == 200
        assert resp.get_json()['success'] is True

    def test_preview_default_metrics_exclude_deprecated(self, client):
        """Default preview must not include hashrate_eh (active=0 in schema)."""
        import app_globals
        db = app_globals.get_db()
        with db._get_connection() as conn:
            conn.execute("UPDATE metric_schema SET active = 0 WHERE key = 'hashrate_eh'")

        resp = client.get('/api/llm_prompts/preview')
        assert resp.status_code == 200
        data = resp.get_json()
        assert data['success'] is True
        assert 'hashrate_eh' not in data['data']['metrics'], (
            "Deprecated metric hashrate_eh must not appear in default preview metrics"
        )

    def test_preview_default_metrics_include_core(self, client):
        """Default preview must include production_btc and hodl_btc (active=1)."""
        resp = client.get('/api/llm_prompts/preview')
        assert resp.status_code == 200
        data = resp.get_json()
        assert data['success'] is True
        metrics = data['data']['metrics']
        assert 'production_btc' in metrics, "production_btc must be in default preview metrics"
        assert 'hodl_btc' in metrics, "hodl_btc must be in default preview metrics"

    def test_preview_prompt_includes_target_metrics_block(self, client):
        """Preview prompt must include a TARGET METRICS block with metric labels from metric_schema."""
        resp = client.get('/api/llm_prompts/preview')
        assert resp.status_code == 200
        prompt = resp.get_json()['data']['prompt']
        assert '=== TARGET METRICS ===' in prompt, (
            "Extraction prompt must include TARGET METRICS block sourced from metric_schema"
        )
        # Must contain at least one metric label from the seeded metric_schema
        assert 'BTC Produced' in prompt or 'BTC Holdings' in prompt, (
            "TARGET METRICS block must include metric labels from the DB"
        )
