"""
Tests for QC snapshot routes — TDD.

Covers POST /api/qc/snapshot, GET /api/qc/summary,
GET /api/qc/ticker_report, POST /api/qc/reset_orphaned,
GET /api/qc/ticker_history, and DB-layer methods.
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


# ── GET /api/qc/ticker_report ─────────────────────────────────────────────────

def _mock_db_health(company=None, data_points=None, queue_stats=None, reports=None,
                    health_history=None):
    db = MagicMock()
    db.get_company.return_value = company
    db.query_data_points.return_value = data_points or []
    db.get_review_queue_stats.return_value = queue_stats or {'total_pending': 0, 'llm_empty_count': 0}
    db.search_reports.return_value = reports or []
    db.save_health_check.return_value = None
    db.get_health_check_history.return_value = health_history or []
    return db


def test_ticker_report_missing_ticker_param():
    """GET /api/qc/ticker_report with no ticker param returns 400 INVALID_INPUT."""
    app = _make_app()
    db = _mock_db_health()
    with app.test_client() as client:
        with patch('routes.qc.get_db', return_value=db):
            resp = client.get('/api/qc/ticker_report')
    assert resp.status_code == 400
    body = resp.get_json()
    assert body['success'] is False
    assert body['error']['code'] == 'INVALID_INPUT'


def test_ticker_report_unknown_ticker():
    """GET /api/qc/ticker_report with unknown ticker returns 404 TICKER_NOT_FOUND."""
    app = _make_app()
    db = _mock_db_health(company=None)
    with app.test_client() as client:
        with patch('routes.qc.get_db', return_value=db):
            resp = client.get('/api/qc/ticker_report?ticker=ZZZ')
    assert resp.status_code == 404
    body = resp.get_json()
    assert body['success'] is False
    assert body['error']['code'] == 'TICKER_NOT_FOUND'


def test_ticker_report_outlier_detected():
    """Outlier fires when value deviates >2x from trailing average."""
    dps = [
        {'period': '2022-10-01', 'metric': 'holdings_btc', 'value': 5000},
        {'period': '2022-11-01', 'metric': 'holdings_btc', 'value': 5100},
        {'period': '2022-12-01', 'metric': 'holdings_btc', 'value': 4900},
        {'period': '2023-01-01', 'metric': 'holdings_btc', 'value': 47360},  # spike
    ]
    company = {'ticker': 'BITF', 'reporting_cadence': 'monthly'}
    db = _mock_db_health(company=company, data_points=dps)
    app = _make_app()
    with app.test_client() as client:
        with patch('routes.qc.get_db', return_value=db):
            resp = client.get('/api/qc/ticker_report?ticker=BITF')
    assert resp.status_code == 200
    body = resp.get_json()
    outliers = body['data']['checks']['outliers']
    assert len(outliers) >= 1
    assert any(o['metric'] == 'holdings_btc' and o['value'] == 47360 for o in outliers)


def test_ticker_report_no_outliers_flat():
    """No false positives on a flat series."""
    dps = [
        {'period': '2022-10-01', 'metric': 'production_btc', 'value': 100},
        {'period': '2022-11-01', 'metric': 'production_btc', 'value': 102},
        {'period': '2022-12-01', 'metric': 'production_btc', 'value': 98},
        {'period': '2023-01-01', 'metric': 'production_btc', 'value': 101},
    ]
    company = {'ticker': 'MARA', 'reporting_cadence': 'monthly'}
    db = _mock_db_health(company=company, data_points=dps)
    app = _make_app()
    with app.test_client() as client:
        with patch('routes.qc.get_db', return_value=db):
            resp = client.get('/api/qc/ticker_report?ticker=MARA')
    assert resp.status_code == 200
    body = resp.get_json()
    outliers = body['data']['checks']['outliers']
    assert outliers == []


def test_ticker_report_coverage_gaps():
    """missing_periods is correct and gap_ratio is computed."""
    # Provide data for Jan, Feb, Apr only (missing Mar)
    dps = [
        {'period': '2024-01-01', 'metric': 'production_btc', 'value': 100},
        {'period': '2024-02-01', 'metric': 'production_btc', 'value': 110},
        {'period': '2024-04-01', 'metric': 'production_btc', 'value': 120},
    ]
    company = {'ticker': 'TEST', 'reporting_cadence': 'monthly'}
    db = _mock_db_health(company=company, data_points=dps)
    app = _make_app()
    with app.test_client() as client:
        with patch('routes.qc.get_db', return_value=db):
            resp = client.get('/api/qc/ticker_report?ticker=TEST')
    assert resp.status_code == 200
    body = resp.get_json()
    gaps = body['data']['checks']['coverage_gaps']
    assert '2024-03-01' in gaps['missing_periods']
    assert gaps['gap_ratio'] > 0


def test_ticker_report_stuck_queue_flagged():
    """flagged=True when llm_empty_count > 50."""
    company = {'ticker': 'BITF', 'reporting_cadence': 'monthly'}
    db = _mock_db_health(
        company=company,
        queue_stats={'total_pending': 270, 'llm_empty_count': 270},
    )
    app = _make_app()
    with app.test_client() as client:
        with patch('routes.qc.get_db', return_value=db):
            resp = client.get('/api/qc/ticker_report?ticker=BITF')
    assert resp.status_code == 200
    body = resp.get_json()
    sq = body['data']['checks']['stuck_queue']
    assert sq['flagged'] is True
    assert sq['llm_empty_count'] == 270


def test_ticker_report_stuck_queue_clean():
    """flagged=False when queue is empty."""
    company = {'ticker': 'MARA', 'reporting_cadence': 'monthly'}
    db = _mock_db_health(
        company=company,
        queue_stats={'total_pending': 0, 'llm_empty_count': 0},
    )
    app = _make_app()
    with app.test_client() as client:
        with patch('routes.qc.get_db', return_value=db):
            resp = client.get('/api/qc/ticker_report?ticker=MARA')
    assert resp.status_code == 200
    body = resp.get_json()
    assert body['data']['checks']['stuck_queue']['flagged'] is False


def test_ticker_report_extraction_backlog():
    """orphaned_running count reflects running reports."""
    company = {'ticker': 'MARA', 'reporting_cadence': 'monthly'}
    reports = [
        {'extraction_status': 'pending'},
        {'extraction_status': 'pending'},
        {'extraction_status': 'running'},
        {'extraction_status': 'failed'},
        {'extraction_status': 'done'},
    ]
    db = _mock_db_health(company=company, reports=reports)
    app = _make_app()
    with app.test_client() as client:
        with patch('routes.qc.get_db', return_value=db):
            resp = client.get('/api/qc/ticker_report?ticker=MARA')
    assert resp.status_code == 200
    body = resp.get_json()
    backlog = body['data']['checks']['extraction_backlog']
    assert backlog['pending'] == 2
    assert backlog['failed'] == 1
    assert backlog['orphaned_running'] == 1


# ── POST /api/qc/reset_orphaned ───────────────────────────────────────────────

def test_reset_orphaned_route():
    """POST /api/qc/reset_orphaned returns reset_count."""
    app = _make_app()
    db = MagicMock()
    db.reset_orphaned_reports.return_value = 3
    with app.test_client() as client:
        with patch('routes.qc.get_db', return_value=db):
            resp = client.post('/api/qc/reset_orphaned?ticker=BITF')
    assert resp.status_code == 200
    body = resp.get_json()
    assert body['success'] is True
    assert body['data']['reset_count'] == 3
    db.reset_orphaned_reports.assert_called_once_with('BITF')


# ── GET /api/qc/ticker_history ────────────────────────────────────────────────

def test_ticker_history_route():
    """GET /api/qc/ticker_history returns stored health check history."""
    history = [
        {
            'id': 1, 'ticker': 'BITF', 'generated_at': '2026-03-16T10:00:00Z',
            'trigger': 'api', 'months': 24,
            'checks': {'outliers': [], 'coverage_gaps': {}, 'stuck_queue': {}, 'extraction_backlog': {}},
        }
    ]
    app = _make_app()
    db = _mock_db_health(company={'ticker': 'BITF'}, health_history=history)
    with app.test_client() as client:
        with patch('routes.qc.get_db', return_value=db):
            resp = client.get('/api/qc/ticker_history?ticker=BITF')
    assert resp.status_code == 200
    body = resp.get_json()
    assert body['success'] is True
    assert len(body['data']) == 1
    assert body['data'][0]['trigger'] == 'api'


# ── DB-layer tests for new methods ───────────────────────────────────────────

class TestHealthCheckDB:
    """get_review_queue_stats, reset_orphaned_reports, save/get health check."""

    def _make_db(self, tmp_path):
        db_path = str(tmp_path / 'test.db')
        from infra.db import MinerDB
        return MinerDB(db_path)

    def _seed_company(self, db, ticker='MARA'):
        with db._get_connection() as conn:
            conn.execute(
                """INSERT OR IGNORE INTO companies
                   (ticker, name, active, reporting_cadence)
                   VALUES (?, ?, 1, 'monthly')""",
                (ticker, ticker + ' Inc'),
            )

    def _seed_review_queue(self, db, ticker, rows):
        """Insert review_queue rows: list of (period, metric, agreement_status)."""
        with db._get_connection() as conn:
            for period, metric, agreement_status in rows:
                conn.execute(
                    """INSERT INTO review_queue
                       (ticker, period, metric, status, agreement_status, llm_value, raw_value, confidence)
                       VALUES (?, ?, ?, 'PENDING', ?, 0, 0, 0.5)""",
                    (ticker, period, metric, agreement_status),
                )

    def test_get_review_queue_stats_db(self, tmp_path):
        db = self._make_db(tmp_path)
        self._seed_company(db)
        self._seed_review_queue(db, 'MARA', [
            ('2024-01-01', 'production_btc', 'LLM_EMPTY'),
            ('2024-02-01', 'production_btc', 'LLM_EMPTY'),
            ('2024-03-01', 'production_btc', 'AGREE'),
        ])
        stats = db.get_review_queue_stats('MARA')
        assert stats['total_pending'] == 3
        assert stats['llm_empty_count'] == 2

    def test_reset_orphaned_db(self, tmp_path):
        db = self._make_db(tmp_path)
        self._seed_company(db)
        with db._get_connection() as conn:
            conn.execute(
                "INSERT INTO reports (ticker, source_type, extraction_status, report_date) VALUES ('MARA','archive_pdf','running','2024-01-01')"
            )
            conn.execute(
                "INSERT INTO reports (ticker, source_type, extraction_status, report_date) VALUES ('MARA','archive_pdf','running','2024-02-01')"
            )
            conn.execute(
                "INSERT INTO reports (ticker, source_type, extraction_status, report_date) VALUES ('MARA','archive_pdf','pending','2024-03-01')"
            )
        count = db.reset_orphaned_reports('MARA')
        assert count == 2
        reports = db.search_reports(ticker='MARA')
        running = [r for r in reports if r['extraction_status'] == 'running']
        assert running == []

    def test_save_and_get_health_check(self, tmp_path):
        db = self._make_db(tmp_path)
        health_card = {
            'ticker': 'MARA',
            'generated_at': '2026-03-16T10:00:00Z',
            'checks': {
                'outliers': [],
                'coverage_gaps': {'expected_periods': 10, 'actual_periods': 9, 'gap_ratio': 0.1, 'missing_periods': ['2024-01-01']},
                'stuck_queue': {'total_pending': 0, 'llm_empty_count': 0, 'flagged': False},
                'extraction_backlog': {'pending': 0, 'failed': 0, 'orphaned_running': 0},
            },
        }
        db.save_health_check('MARA', health_card, trigger='api', months=24)
        history = db.get_health_check_history('MARA')
        assert len(history) == 1
        assert history[0]['ticker'] == 'MARA'
        assert history[0]['trigger'] == 'api'
        assert history[0]['months'] == 24
        assert isinstance(history[0]['checks'], dict)
        assert history[0]['checks']['coverage_gaps']['gap_ratio'] == pytest.approx(0.1)
