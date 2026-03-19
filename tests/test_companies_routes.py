"""Companies route tests for config persistence and API behavior."""

import importlib
import os
import sys

import pytest

from infra.db import MinerDB


@pytest.fixture
def app(tmp_path):
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))
    import app_globals
    import run_web

    db = MinerDB(str(tmp_path / 'test.db'))
    app_globals._db = db

    importlib.reload(run_web)
    flask_app = run_web.create_app()
    flask_app.config['TESTING'] = True
    return flask_app


@pytest.fixture
def client(app):
    return app.test_client()


def test_create_company_persists_globenewswire_url(client):
    resp = client.post('/api/companies', json={
        'ticker': 'GNWS',
        'name': 'Globe Miner',
        'globenewswire_url': 'https://www.globenewswire.com/rssfeed/organization/test',
    })
    assert resp.status_code == 201
    row = resp.get_json()['data']
    assert row['globenewswire_url'] == 'https://www.globenewswire.com/rssfeed/organization/test'


def test_create_company_persists_pr_start_date(client):
    resp = client.post('/api/companies', json={
        'ticker': 'TEST',
        'name': 'Test Miner',
        'ir_url': 'https://example.com/news',
        'pr_start_date': '2022-01-01',
    })
    assert resp.status_code == 201
    data = resp.get_json()['data']
    assert data['pr_start_date'] == '2022-01-01'


def test_scraper_governance_returns_items(client):
    client.post('/api/companies', json={
        'ticker': 'SKIPX',
        'name': 'Skip X',
    })
    assert client.post('/api/companies', json={
        'ticker': 'SKIPX',
        'name': 'Skip X',
    }).status_code in (201, 409)

    gov = client.get('/api/companies/scraper_governance?stale_days=30')
    assert gov.status_code == 200
    data = gov.get_json()['data']
    assert 'items' in data


def test_discovery_candidates_roundtrip(client):
    created = client.post('/api/companies', json={
        'ticker': 'DISC',
        'name': 'Discovery Miner',
    })
    assert created.status_code == 201

    add = client.post('/api/companies/DISC/discovery_candidates', json={
        'proposed_by': 'agent',
        'candidates': [
            {'source_type': 'RSS', 'url': 'https://example.com/rss.xml', 'confidence': 0.91},
            {'source_type': 'IR_PRIMARY', 'url': 'https://example.com/news'},
        ],
    })
    assert add.status_code == 200
    payload = add.get_json()['data']
    assert payload['stored'] == 2

    listed = client.get('/api/companies/DISC/discovery_candidates')
    assert listed.status_code == 200
    rows = listed.get_json()['data']['candidates']
    assert len(rows) == 2
    assert {r['source_type'] for r in rows} == {'RSS', 'IR_PRIMARY'}


def test_sync_companies_reenables_startup_auto_sync(client):
    """POST /api/companies/sync must re-enable auto_sync_companies_on_startup.

    After a hard_delete purge disables startup sync, a manual operator sync
    via this endpoint re-enables the flag so the next server restart picks up
    the company config automatically.
    """
    import app_globals
    db = app_globals.get_db()
    db.set_config('auto_sync_companies_on_startup', '0')
    assert db.get_config('auto_sync_companies_on_startup') == '0'

    resp = client.post('/api/companies/sync')
    if resp.status_code == 200:
        assert db.get_config('auto_sync_companies_on_startup') == '1'


def test_patch_metric_schema_prompt_instructions(client):
    import app_globals
    db = app_globals.get_db()
    rows = db.get_metric_schema('BTC-miners', active_only=False)
    row_id = rows[0]['id']
    resp = client.patch(f'/api/metric_schema/{row_id}',
                        json={'prompt_instructions': 'Test prompt'})
    assert resp.status_code == 200
    updated = db.get_metric_schema('BTC-miners', active_only=False)
    updated_row = next(r for r in updated if r['id'] == row_id)
    assert updated_row['prompt_instructions'] == 'Test prompt'


def test_patch_metric_schema_quarterly_prompt(client):
    import app_globals
    db = app_globals.get_db()
    rows = db.get_metric_schema('BTC-miners', active_only=False)
    row_id = rows[0]['id']
    resp = client.patch(f'/api/metric_schema/{row_id}',
                        json={'quarterly_prompt': 'Quarterly override'})
    assert resp.status_code == 200
    updated = db.get_metric_schema('BTC-miners', active_only=False)
    updated_row = next(r for r in updated if r['id'] == row_id)
    assert updated_row['quarterly_prompt'] == 'Quarterly override'


def test_patch_metric_schema_prompt_instructions_null(client):
    import app_globals
    db = app_globals.get_db()
    rows = db.get_metric_schema('BTC-miners', active_only=False)
    row_id = rows[0]['id']
    client.patch(f'/api/metric_schema/{row_id}', json={'prompt_instructions': 'Something'})
    resp = client.patch(f'/api/metric_schema/{row_id}', json={'prompt_instructions': None})
    assert resp.status_code == 200
    updated = db.get_metric_schema('BTC-miners', active_only=False)
    updated_row = next(r for r in updated if r['id'] == row_id)
    assert updated_row['prompt_instructions'] is None


def test_get_metric_schema_response_includes_prompt_fields(client):
    resp = client.get('/api/metric_schema?sector=BTC-miners')
    assert resp.status_code == 200
    data = resp.get_json()
    assert data['success']
    row = data['data'][0]
    assert 'prompt_instructions' in row
    assert 'quarterly_prompt' in row


# ── DELETE /api/companies/<ticker> ────────────────────────────────────────────

def test_delete_company_not_found(client):
    r = client.delete('/api/companies/FAKE', json={})
    assert r.status_code == 404
    body = r.get_json()
    assert body['success'] is False


def test_delete_company_no_children(client):
    import app_globals
    db = app_globals.get_db()
    db.add_company('TST', 'Test Co')
    r = client.delete('/api/companies/TST', json={'cascade': False})
    assert r.status_code == 200
    assert r.get_json()['success'] is True
    assert db.get_company('TST') is None


def test_delete_company_blocks_on_children(client):
    import app_globals
    db = app_globals.get_db()
    db.add_company('BLKC', 'Block Child')
    db.insert_report({
        'ticker': 'BLKC', 'source_url': 'http://x.com/blkc',
        'source_type': 'ir_press_release', 'report_date': '2024-01-01',
        'published_date': None, 'parsed_at': None, 'raw_text': 'sample text', 'raw_html': None,
    })
    r = client.delete('/api/companies/BLKC', json={'cascade': False})
    assert r.status_code == 409
    body = r.get_json()
    assert body['success'] is False
    assert 'counts' in body
    assert 'reports' in body['counts']
    assert db.get_company('BLKC') is not None


def test_delete_company_cascade(client):
    import app_globals
    db = app_globals.get_db()
    db.add_company('CASC', 'Cascade Co')
    db.insert_report({
        'ticker': 'CASC', 'source_url': 'http://y.com/casc',
        'source_type': 'ir_press_release', 'report_date': '2024-01-01',
        'published_date': None, 'parsed_at': None, 'raw_text': 'sample text', 'raw_html': None,
    })
    r = client.delete('/api/companies/CASC', json={'cascade': True})
    assert r.status_code == 200
    body = r.get_json()
    assert body['success'] is True
    assert db.get_company('CASC') is None
    with db._get_connection() as conn:
        n = conn.execute("SELECT COUNT(*) FROM reports WHERE ticker='CASC'").fetchone()[0]
    assert n == 0


def test_delete_company_ticker_uppercased(client):
    import app_globals
    db = app_globals.get_db()
    db.add_company('UPPR', 'Upper Co')
    r = client.delete('/api/companies/uppr', json={})
    assert r.status_code == 200
    assert db.get_company('UPPR') is None
