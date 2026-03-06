"""
Tests for QC snapshot routes — TDD.

Covers POST /api/qc/snapshot and GET /api/qc/summary.
"""
import pytest
import json
from unittest.mock import MagicMock, patch


def _make_app():
    from flask import Flask
    from routes.qc import bp
    app = Flask(__name__)
    app.register_blueprint(bp)
    app.config['TESTING'] = True
    return app


def _mock_db(snapshots=None):
    db = MagicMock()
    db.upsert_qc_snapshot.return_value = None
    db.get_qc_snapshots.return_value = snapshots or []
    return db


# ── POST /api/qc/snapshot ─────────────────────────────────────────────────────

def test_post_qc_snapshot_stores_and_returns_200():
    """POST /api/qc/snapshot with valid payload calls upsert_qc_snapshot and returns 200."""
    app = _make_app()
    db = _mock_db()

    with app.test_client() as client:
        with patch('routes.qc.get_db', return_value=db):
            resp = client.post('/api/qc/snapshot', json={
                'run_date': '2024-02-01',
                'auto_accepted': 120,
                'review_accepted': 10,
                'review_rejected': 5,
            })

    assert resp.status_code == 200
    body = resp.get_json()
    assert body['success'] is True
    db.upsert_qc_snapshot.assert_called_once()


# ── GET /api/qc/summary ───────────────────────────────────────────────────────

def test_get_qc_summary_returns_snapshots():
    """GET /api/qc/summary returns stored snapshots with precision_est computed."""
    snapshots = [
        {
            'run_date': '2024-02-01',
            'auto_accepted': 100,
            'review_accepted': 8,
            'review_rejected': 4,
            'precision_est': 0.8,
        }
    ]
    app = _make_app()
    db = _mock_db(snapshots=snapshots)

    with app.test_client() as client:
        with patch('routes.qc.get_db', return_value=db):
            resp = client.get('/api/qc/summary')

    assert resp.status_code == 200
    body = resp.get_json()
    assert body['success'] is True
    assert isinstance(body['data'], list)
    assert len(body['data']) == 1
    assert body['data'][0]['precision_est'] == pytest.approx(0.8)
