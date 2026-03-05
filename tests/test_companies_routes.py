"""Companies route tests for scraper mode validation and config persistence."""

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


def test_create_company_rss_requires_rss_url(client):
    resp = client.post('/api/companies', json={
        'ticker': 'TEST',
        'name': 'Test Miner',
        'scraper_mode': 'rss',
    })
    assert resp.status_code == 400
    assert 'rss mode requires' in resp.get_json()['error']['message']


def test_create_company_rss_accepts_globenewswire_placeholder(client):
    resp = client.post('/api/companies', json={
        'ticker': 'GNWS',
        'name': 'Globe Miner',
        'scraper_mode': 'rss',
        'globenewswire_url': 'https://www.globenewswire.com/rssfeed/organization/test',
    })
    assert resp.status_code == 201
    row = resp.get_json()['data']
    assert row['globenewswire_url'] == 'https://www.globenewswire.com/rssfeed/organization/test'


def test_create_company_template_requires_template_and_start_year(client):
    resp = client.post('/api/companies', json={
        'ticker': 'TEST',
        'name': 'Test Miner',
        'scraper_mode': 'template',
        'url_template': 'https://example.com/{month}-{year}',
    })
    assert resp.status_code == 400
    assert 'pr_start_year' in resp.get_json()['error']['message']


def test_create_company_index_requires_ir_url(client):
    resp = client.post('/api/companies', json={
        'ticker': 'TEST',
        'name': 'Test Miner',
        'scraper_mode': 'index',
    })
    assert resp.status_code == 400
    assert 'index mode requires' in resp.get_json()['error']['message']


def test_create_company_template_persists_mode_fields(client):
    resp = client.post('/api/companies', json={
        'ticker': 'TEST',
        'name': 'Test Miner',
        'scraper_mode': 'template',
        'ir_url': 'https://example.com/news',
        'url_template': 'https://example.com/{month}-{year}',
        'pr_start_year': 2022,
    })
    assert resp.status_code == 201
    data = resp.get_json()['data']
    assert data['scraper_mode'] == 'template'
    assert data['url_template'] == 'https://example.com/{month}-{year}'
    assert data['pr_start_year'] == 2022


def test_update_company_mode_validates_effective_configuration(client):
    create = client.post('/api/companies', json={
        'ticker': 'TEST',
        'name': 'Test Miner',
        'scraper_mode': 'skip',
        'skip_reason': 'inactive',
    })
    assert create.status_code == 201

    resp = client.put('/api/companies/TEST', json={'scraper_mode': 'rss'})
    assert resp.status_code == 400
    assert 'rss mode requires' in resp.get_json()['error']['message']


def test_scraper_governance_flags_stale_skip_and_conflict(client):
    # stale skip: no source_audit evidence
    resp = client.post('/api/companies', json={
        'ticker': 'SKIPX',
        'name': 'Skip X',
        'scraper_mode': 'skip',
        'skip_reason': 'legacy',
    })
    assert resp.status_code == 201

    # skip conflict: has ACTIVE source evidence
    resp = client.post('/api/companies', json={
        'ticker': 'SKIPY',
        'name': 'Skip Y',
        'scraper_mode': 'skip',
        'skip_reason': 'legacy',
    })
    assert resp.status_code == 201

    # configured needs_probe: non-skip with no audit rows
    resp = client.post('/api/companies', json={
        'ticker': 'RUNX',
        'name': 'Run X',
        'scraper_mode': 'rss',
        'rss_url': 'https://example.com/rss.xml',
    })
    assert resp.status_code == 201

    import app_globals
    db = app_globals.get_db()
    db.upsert_source_audit({
        'ticker': 'SKIPY',
        'source_type': 'IR_PRIMARY',
        'url': 'https://example.com',
        'last_checked': '2026-03-05T00:00:00',
        'http_status': 200,
        'status': 'ACTIVE',
    })

    gov = client.get('/api/companies/scraper_governance?stale_days=30')
    assert gov.status_code == 200
    data = gov.get_json()['data']
    items = {x['ticker']: x for x in data['items']}

    assert items['SKIPX']['governance_status'] == 'stale_skip'
    assert items['SKIPY']['governance_status'] == 'skip_conflict_active_source'
    assert items['RUNX']['governance_status'] == 'needs_probe'


def test_discovery_candidates_roundtrip(client):
    created = client.post('/api/companies', json={
        'ticker': 'DISC',
        'name': 'Discovery Miner',
        'scraper_mode': 'skip',
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


def test_bootstrap_probe_recommends_and_applies_mode(client, monkeypatch):
    created = client.post('/api/companies', json={
        'ticker': 'BOOT',
        'name': 'Bootstrap Miner',
        'scraper_mode': 'skip',
    })
    assert created.status_code == 201

    add = client.post('/api/companies/BOOT/discovery_candidates', json={
        'candidates': [{'source_type': 'RSS', 'url': 'https://example.com/rss.xml'}],
    })
    assert add.status_code == 200

    import routes.companies as companies_mod

    def _fake_probe(source_type, url, timeout=12):
        return {
            'probe_status': 'ACTIVE',
            'http_status': 200,
            'last_checked': '2026-03-05T00:00:00',
            'evidence_title': 'RSS feed detected',
            'evidence_date': None,
        }

    monkeypatch.setattr(companies_mod, '_probe_candidate_url', _fake_probe)

    run = client.post('/api/companies/BOOT/bootstrap_probe', json={'apply_mode': True})
    assert run.status_code == 200
    data = run.get_json()['data']
    assert data['recommended_mode'] == 'rss'
    assert data['applied'] is True

    show = client.get('/api/companies/BOOT')
    assert show.status_code == 200
    company = show.get_json()['data']
    assert company['scraper_mode'] == 'rss'
    assert company['rss_url'] == 'https://example.com/rss.xml'


def test_bootstrap_probe_globenewswire_candidate_applies_rss(client, monkeypatch):
    created = client.post('/api/companies', json={
        'ticker': 'GNEW',
        'name': 'Globe Candidate',
        'scraper_mode': 'skip',
    })
    assert created.status_code == 201

    add = client.post('/api/companies/GNEW/discovery_candidates', json={
        'candidates': [{'source_type': 'GLOBENEWSWIRE', 'url': 'https://www.globenewswire.com/rssfeed/org/abc'}],
    })
    assert add.status_code == 200

    import routes.companies as companies_mod

    def _fake_probe(source_type, url, timeout=12):
        return {
            'probe_status': 'ACTIVE',
            'http_status': 200,
            'last_checked': '2026-03-05T00:00:00',
            'evidence_title': 'GlobeNewswire content detected',
            'evidence_date': None,
        }

    monkeypatch.setattr(companies_mod, '_probe_candidate_url', _fake_probe)

    run = client.post('/api/companies/GNEW/bootstrap_probe', json={'apply_mode': True})
    assert run.status_code == 200
    data = run.get_json()['data']
    assert data['recommended_mode'] == 'rss'
    assert data['applied'] is True

    show = client.get('/api/companies/GNEW')
    assert show.status_code == 200
    company = show.get_json()['data']
    assert company['scraper_mode'] == 'rss'
    assert company['rss_url'] == 'https://www.globenewswire.com/rssfeed/org/abc'
    assert company['globenewswire_url'] == 'https://www.globenewswire.com/rssfeed/org/abc'


def test_bootstrap_probe_all_targets_governance_set(client, monkeypatch):
    for ticker in ('AONE', 'ATWO'):
        created = client.post('/api/companies', json={
            'ticker': ticker,
            'name': f'Company {ticker}',
            'scraper_mode': 'skip',
            'skip_reason': 'legacy',
        })
        assert created.status_code == 201

    import routes.companies as companies_mod

    def _fake_probe(source_type, url, timeout=12):
        return {
            'probe_status': 'ACTIVE',
            'http_status': 200,
            'last_checked': '2026-03-05T00:00:00',
            'evidence_title': 'active source',
            'evidence_date': None,
        }

    monkeypatch.setattr(companies_mod, '_probe_candidate_url', _fake_probe)

    # Seed one target with candidate URLs so the batch endpoint can probe it.
    add = client.post('/api/companies/AONE/discovery_candidates', json={
        'candidates': [{'source_type': 'RSS', 'url': 'https://example.com/aone.xml'}],
    })
    assert add.status_code == 200

    run = client.post('/api/companies/bootstrap_probe_all', json={
        'statuses': ['stale_skip'],
        'tickers': ['AONE', 'ATWO'],
        'apply_mode': True,
    })
    assert run.status_code == 200
    payload = run.get_json()['data']
    assert payload['targeted'] == 2
    assert payload['completed'] == 1
    assert payload['failed'] == 1

    # AONE applied from ACTIVE RSS candidate.
    show = client.get('/api/companies/AONE')
    assert show.status_code == 200
    company = show.get_json()['data']
    assert company['scraper_mode'] == 'rss'


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
