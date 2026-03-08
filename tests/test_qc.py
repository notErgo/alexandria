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


# ── DB-layer tests ────────────────────────────────────────────────────────────

class TestQCSnapshotDB:
    """upsert_qc_snapshot and get_qc_snapshots must exist on MinerDB."""

    def _make_db(self, tmp_path):
        db_path = str(tmp_path / 'test.db')
        from infra.db import MinerDB
        return MinerDB(db_path)

    def test_upsert_and_get_qc_snapshot(self, tmp_path):
        db = self._make_db(tmp_path)
        snapshot = {
            'run_date': '2024-02-01',
            'ticker': 'MARA',
            'auto_accepted': 120,
            'review_accepted': 10,
            'review_rejected': 5,
            'precision_est': 0.96,
        }
        db.upsert_qc_snapshot(snapshot)
        results = db.get_qc_snapshots()
        assert len(results) == 1
        # snapshot_at is populated from run_date
        assert results[0].get('snapshot_at') == '2024-02-01' or results[0].get('run_date') == '2024-02-01'

    def test_get_qc_snapshots_empty(self, tmp_path):
        db = self._make_db(tmp_path)
        results = db.get_qc_snapshots()
        assert results == []

    def test_get_qc_snapshots_filtered_by_ticker(self, tmp_path):
        db = self._make_db(tmp_path)
        db.upsert_qc_snapshot({'run_date': '2024-01-01', 'ticker': 'MARA', 'precision_est': 0.9})
        db.upsert_qc_snapshot({'run_date': '2024-01-01', 'ticker': 'RIOT', 'precision_est': 0.8})
        mara_results = db.get_qc_snapshots(ticker='MARA')
        assert len(mara_results) == 1
        assert mara_results[0]['ticker'] == 'MARA'

    def test_summary_json_deserialized(self, tmp_path):
        """Fields stored in summary_json must appear directly in the returned dict."""
        db = self._make_db(tmp_path)
        snapshot = {
            'run_date': '2024-03-01',
            'ticker': None,
            'auto_accepted': 50,
            'precision_est': 0.92,
        }
        db.upsert_qc_snapshot(snapshot)
        results = db.get_qc_snapshots()
        assert len(results) == 1
        assert results[0].get('precision_est') == pytest.approx(0.92)
        assert results[0].get('auto_accepted') == 50
