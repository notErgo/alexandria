"""Config route tests for keyword dictionary management."""

import pytest
from infra.db import MinerDB


@pytest.fixture
def app(tmp_path):
    import sys
    import os
    import importlib

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


def test_get_keyword_dictionary_default(client):
    resp = client.get('/api/config/keyword_dictionary')
    assert resp.status_code == 200
    data = resp.get_json()
    assert data['success'] is True
    d = data['data']['dictionary']
    assert 'packs' in d
    assert 'btc_activity' in d['packs']


def test_set_keyword_dictionary_rejects_invalid_payload(client):
    resp = client.post('/api/config/keyword_dictionary', json={'dictionary': []})
    assert resp.status_code == 400
    data = resp.get_json()
    assert data['success'] is False


def test_set_keyword_dictionary_and_read_back(client):
    payload = {
        'dictionary': {
            'active_pack': 'btc_activity',
            'packs': {
                'btc_activity': ['Bitcoin', 'BTC', 'hashrate'],
                'ai_hpc_compute': ['GPU', 'cluster'],
            },
        }
    }
    resp = client.post('/api/config/keyword_dictionary', json=payload)
    assert resp.status_code == 200
    data = resp.get_json()
    assert data['success'] is True
    saved = data['data']['dictionary']
    assert saved['active_pack'] == 'btc_activity'
    assert saved['packs']['btc_activity'] == ['bitcoin', 'btc', 'hashrate']

    resp2 = client.get('/api/config/keyword_dictionary')
    assert resp2.status_code == 200
    data2 = resp2.get_json()
    assert data2['success'] is True
    assert data2['data']['dictionary']['packs']['ai_hpc_compute'] == ['gpu', 'cluster']
