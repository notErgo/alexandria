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
