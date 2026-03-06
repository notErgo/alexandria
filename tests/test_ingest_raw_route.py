"""Route tests for POST /api/ingest/raw."""
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
    db.insert_company({
        'ticker': 'MARA',
        'name': 'MARA Holdings, Inc.',
        'tier': 1,
        'ir_url': 'https://ir.mara.com/news-events/press-releases',
        'pr_base_url': 'https://ir.mara.com',
        'cik': '0001507605',
        'active': 1,
    })
    app_globals._db = db

    importlib.reload(run_web)
    flask_app = run_web.create_app()
    flask_app.config['TESTING'] = True
    return flask_app


@pytest.fixture
def client(app):
    return app.test_client()


def test_ingest_raw_stores_documents(client, app):
    payload = {
        'documents': [
            {
                'ticker': 'MARA',
                'source_url': 'https://example.com/pr/mara-2024-03',
                'raw_text': 'MARA Holdings produced 890 BTC in March 2024.',
                'source_type': 'wire_press_release',
                'period': '2024-03',
            }
        ]
    }
    resp = client.post('/api/ingest/raw', json=payload)
    assert resp.status_code == 200
    data = resp.get_json()
    assert data['success'] is True
    assert data['data']['ingested'] == 1
    assert data['data']['skipped'] == 0
    assert data['data']['errors'] == []

    import app_globals
    db = app_globals.get_db()
    assert db.report_exists_by_url('MARA', 'https://example.com/pr/mara-2024-03')


def test_ingest_raw_deduplicates_by_url(client, app):
    doc = {
        'ticker': 'MARA',
        'source_url': 'https://example.com/pr/mara-2024-04',
        'raw_text': 'MARA Holdings produced 750 BTC in April 2024.',
        'source_type': 'wire_press_release',
        'period': '2024-04',
    }
    resp1 = client.post('/api/ingest/raw', json={'documents': [doc]})
    assert resp1.status_code == 200
    assert resp1.get_json()['data']['ingested'] == 1

    resp2 = client.post('/api/ingest/raw', json={'documents': [doc]})
    assert resp2.status_code == 200
    data2 = resp2.get_json()
    assert data2['data']['ingested'] == 0
    assert data2['data']['skipped'] == 1


def test_ingest_raw_rejects_missing_required_fields(client):
    resp = client.post('/api/ingest/raw', json={
        'documents': [
            {
                'source_url': 'https://example.com/no-ticker',
                'raw_text': 'Some text',
                'source_type': 'wire_press_release',
            }
        ]
    })
    assert resp.status_code == 400
    data = resp.get_json()
    assert data['success'] is False
    assert data['error']['code'] == 'INVALID_INPUT'


def test_ingest_raw_returns_207_on_partial_failure(client, app):
    docs = [
        {
            'ticker': 'MARA',
            'source_url': 'https://example.com/pr/mara-2024-05',
            'raw_text': 'MARA produced 800 BTC in May 2024.',
            'source_type': 'wire_press_release',
            'period': '2024-05',
        },
        {
            # missing ticker — will fail
            'source_url': 'https://example.com/pr/bad',
            'raw_text': 'Bad doc',
            'source_type': 'wire_press_release',
        },
    ]
    resp = client.post('/api/ingest/raw', json={'documents': docs})
    assert resp.status_code == 207
    data = resp.get_json()
    assert data['success'] is True
    assert data['data']['ingested'] == 1
    assert len(data['data']['errors']) == 1
