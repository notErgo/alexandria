"""Route tests for /api/data/purge mode semantics."""

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


def test_purge_rejects_invalid_mode(client):
    resp = client.post('/api/data/purge', json={'confirm': True, 'purge_mode': 'invalid'})
    assert resp.status_code == 400
    data = resp.get_json()
    assert data['success'] is False
    assert data['error']['code'] == 'INVALID_PURGE_MODE'


def test_purge_defaults_to_archive_mode(client):
    resp = client.post('/api/data/purge', json={'confirm': True})
    assert resp.status_code == 200
    data = resp.get_json()
    assert data['success'] is True
    assert data['data']['purge_mode'] == 'archive'
    assert 'archive_batch_id' in (data['data']['counts'] or {})


def test_hard_delete_full_can_disable_auto_sync(client):
    resp = client.post('/api/data/purge', json={
        'confirm': True,
        'purge_mode': 'hard_delete',
        'suppress_auto_sync': True,
    })
    assert resp.status_code == 200
    data = resp.get_json()
    assert data['success'] is True
    assert data['data']['purge_mode'] == 'hard_delete'
    assert data['data']['auto_sync_companies_on_startup'] == '0'
