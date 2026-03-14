"""Tests for the Backfill run mode in the extraction pipeline.

Rules under test:
- DB: get_reports_for_backfill() only returns reports whose ticker+period has
  no data_points or PENDING/APPROVED review_queue entries.
- Batch builder: _build_extraction_batch_backfill() applies EDGAR date gating
  and resets extraction_status for returned reports.
- API: POST /api/operations/interpret with run_mode='backfill' returns 200 + task_id
  and uses get_reports_for_backfill, not get_all_reports_for_extraction.
"""
import os
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from infra.db import MinerDB


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------

def _insert_report(db, ticker, report_date, source_type='ir_press_release',
                   raw_text='mined 100 BTC', extraction_status='pending'):
    conn = db._get_connection()
    with conn:
        cur = conn.execute(
            "INSERT INTO reports (ticker, report_date, source_type, raw_text, extraction_status)"
            " VALUES (?, ?, ?, ?, ?)",
            (ticker, report_date, source_type, raw_text, extraction_status),
        )
    return cur.lastrowid


def _insert_data_point(db, ticker, period, metric='production_btc', value=100.0):
    conn = db._get_connection()
    with conn:
        cur = conn.execute(
            """INSERT INTO data_points (ticker, period, metric, value, unit, confidence,
               extraction_method, source_priority)
               VALUES (?, ?, ?, ?, 'BTC', 0.9, 'llm', 2)""",
            (ticker, period, metric, value),
        )
    return cur.lastrowid


def _insert_review_item(db, ticker, period, status='PENDING', metric='production_btc'):
    conn = db._get_connection()
    with conn:
        cur = conn.execute(
            """INSERT INTO review_queue (ticker, period, metric, raw_value, confidence,
               source_snippet, status)
               VALUES (?, ?, ?, '100', 0.5, 'test', ?)""",
            (ticker, period, metric, status),
        )
    return cur.lastrowid


def _ids(reports):
    return {r['id'] for r in reports}


# ---------------------------------------------------------------------------
# fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def db(tmp_path):
    database = MinerDB(str(tmp_path / 'test.db'))
    database.insert_company({
        'ticker': 'MARA',
        'name': 'MARA Holdings',
        'tier': 1,
        'ir_url': 'https://ir.mara.com',
        'pr_base_url': 'https://ir.mara.com',
        'cik': '0001507605',
        'active': 1,
    })
    database.insert_company({
        'ticker': 'RIOT',
        'name': 'Riot Platforms',
        'tier': 1,
        'ir_url': 'https://ir.riotplatforms.com',
        'pr_base_url': 'https://ir.riotplatforms.com',
        'cik': '0001167419',
        'active': 1,
    })
    return database


# ---------------------------------------------------------------------------
# TestGetReportsForBackfill
# ---------------------------------------------------------------------------

class TestGetReportsForBackfill:
    def test_no_data_returns_report(self, db):
        """Report with no data_points is returned."""
        r_id = _insert_report(db, 'MARA', '2024-01-01')
        result = db.get_reports_for_backfill(ticker='MARA')
        assert r_id in _ids(result)

    def test_existing_data_point_excludes_report(self, db):
        """Report whose period already has a data_point is excluded."""
        r_id = _insert_report(db, 'MARA', '2024-01-01')
        _insert_data_point(db, 'MARA', '2024-01-01')
        result = db.get_reports_for_backfill(ticker='MARA')
        assert r_id not in _ids(result)

    def test_pending_review_excludes_report(self, db):
        """Period with PENDING review_queue entry is excluded."""
        r_id = _insert_report(db, 'MARA', '2024-02-01')
        _insert_review_item(db, 'MARA', '2024-02-01', status='PENDING')
        result = db.get_reports_for_backfill(ticker='MARA')
        assert r_id not in _ids(result)

    def test_approved_review_excludes_report(self, db):
        """Period with APPROVED review_queue entry is excluded."""
        r_id = _insert_report(db, 'MARA', '2024-03-01')
        _insert_review_item(db, 'MARA', '2024-03-01', status='APPROVED')
        result = db.get_reports_for_backfill(ticker='MARA')
        assert r_id not in _ids(result)

    def test_rejected_review_includes_report(self, db):
        """Period with only REJECTED review entries is included (rejected = no valid data)."""
        r_id = _insert_report(db, 'MARA', '2024-04-01')
        _insert_review_item(db, 'MARA', '2024-04-01', status='REJECTED')
        result = db.get_reports_for_backfill(ticker='MARA')
        assert r_id in _ids(result)

    def test_other_ticker_data_point_does_not_exclude(self, db):
        """A data_point for RIOT does not exclude MARA's report for the same period."""
        r_id = _insert_report(db, 'MARA', '2024-05-01')
        _insert_data_point(db, 'RIOT', '2024-05-01')
        result = db.get_reports_for_backfill(ticker='MARA')
        assert r_id in _ids(result)

    def test_different_period_data_point_does_not_exclude(self, db):
        """A data_point for a different period does not exclude this report."""
        r_id = _insert_report(db, 'MARA', '2024-06-01')
        _insert_data_point(db, 'MARA', '2024-07-01')
        result = db.get_reports_for_backfill(ticker='MARA')
        assert r_id in _ids(result)

    def test_ticker_filter(self, db):
        """ticker= filter limits results to that company."""
        mara_id = _insert_report(db, 'MARA', '2024-01-01')
        riot_id = _insert_report(db, 'RIOT', '2024-01-01')
        result = db.get_reports_for_backfill(ticker='MARA')
        ids = _ids(result)
        assert mara_id in ids
        assert riot_id not in ids

    def test_source_types_filter(self, db):
        """source_types= filter restricts to those types."""
        ir_id = _insert_report(db, 'MARA', '2024-01-01', source_type='ir_press_release')
        edgar_id = _insert_report(db, 'MARA', '2024-02-01', source_type='edgar_8k')
        result = db.get_reports_for_backfill(ticker='MARA', source_types=['ir_press_release'])
        ids = _ids(result)
        assert ir_id in ids
        assert edgar_id not in ids

    def test_from_period_filter(self, db):
        """from_period= excludes reports before that date."""
        early_id = _insert_report(db, 'MARA', '2023-01-01')
        late_id = _insert_report(db, 'MARA', '2024-06-01')
        result = db.get_reports_for_backfill(ticker='MARA', from_period='2024-01')
        ids = _ids(result)
        assert early_id not in ids
        assert late_id in ids

    def test_to_period_filter(self, db):
        """to_period= excludes reports after that date."""
        early_id = _insert_report(db, 'MARA', '2023-01-01')
        late_id = _insert_report(db, 'MARA', '2025-06-01')
        result = db.get_reports_for_backfill(ticker='MARA', to_period='2024-12')
        ids = _ids(result)
        assert early_id in ids
        assert late_id not in ids

    def test_done_report_with_empty_period_included(self, db):
        """extraction_status='done' report is included — backfill is not status-gated."""
        r_id = _insert_report(db, 'MARA', '2024-08-01', extraction_status='done')
        result = db.get_reports_for_backfill(ticker='MARA')
        assert r_id in _ids(result)

    def test_no_raw_text_excluded(self, db):
        """Report with empty raw_text is excluded (not extractable)."""
        conn = db._get_connection()
        with conn:
            cur = conn.execute(
                "INSERT INTO reports (ticker, report_date, source_type, raw_text, extraction_status)"
                " VALUES ('MARA', '2024-09-01', 'ir_press_release', '', 'pending')"
            )
        r_id = cur.lastrowid
        result = db.get_reports_for_backfill(ticker='MARA')
        assert r_id not in _ids(result)

    def test_no_ticker_filter_returns_all_companies(self, db):
        """Without ticker= filter, reports for all companies are returned."""
        mara_id = _insert_report(db, 'MARA', '2024-01-01')
        riot_id = _insert_report(db, 'RIOT', '2024-01-01')
        result = db.get_reports_for_backfill()
        ids = _ids(result)
        assert mara_id in ids
        assert riot_id in ids


# ---------------------------------------------------------------------------
# TestBuildExtractionBatchBackfill
# ---------------------------------------------------------------------------

class TestBuildExtractionBatchBackfill:
    @pytest.fixture
    def build_batch(self, db):
        from routes.pipeline import _build_extraction_batch_backfill
        return lambda ticker, first_filing, source_types=None: \
            _build_extraction_batch_backfill(db, ticker, first_filing, source_types)

    def test_returns_only_data_empty_reports(self, db, build_batch):
        """Only reports without existing data_points are returned."""
        empty_id = _insert_report(db, 'MARA', '2024-01-01', source_type='ir_press_release')
        filled_id = _insert_report(db, 'MARA', '2024-02-01', source_type='ir_press_release')
        _insert_data_point(db, 'MARA', '2024-02-01')
        batch = build_batch('MARA', first_filing='2023-01-01')
        ids = _ids(batch)
        assert empty_id in ids
        assert filled_id not in ids

    def test_edgar_before_first_filing_excluded(self, db, build_batch):
        """EDGAR reports before btc_first_filing_date are not returned."""
        _insert_report(db, 'MARA', '2021-06-01', source_type='edgar_8k')
        batch = build_batch('MARA', first_filing='2023-05-19')
        assert _ids(batch) == set()

    def test_edgar_after_first_filing_included_if_no_data(self, db, build_batch):
        """EDGAR report after the first filing date is included when period is empty."""
        r_id = _insert_report(db, 'MARA', '2024-01-01', source_type='edgar_8k')
        batch = build_batch('MARA', first_filing='2023-05-19')
        assert r_id in _ids(batch)

    def test_non_edgar_not_date_gated(self, db, build_batch):
        """IR/archive reports are not gated by btc_first_filing_date."""
        r_id = _insert_report(db, 'MARA', '2021-04-01', source_type='ir_press_release')
        batch = build_batch('MARA', first_filing='2023-05-19')
        assert r_id in _ids(batch)

    def test_done_report_gets_status_reset(self, db, build_batch):
        """Reports returned by the batch get extraction_status reset to 'pending'."""
        r_id = _insert_report(db, 'MARA', '2024-01-01', source_type='ir_press_release',
                               extraction_status='done')
        build_batch('MARA', first_filing='2023-01-01')
        conn = db._get_connection()
        row = conn.execute("SELECT extraction_status FROM reports WHERE id = ?", (r_id,)).fetchone()
        assert row['extraction_status'] == 'pending'

    def test_explicit_source_types_no_edgar_date_gating(self, db, build_batch):
        """With explicit source_types, EDGAR date gating is not applied."""
        r_id = _insert_report(db, 'MARA', '2020-01-01', source_type='edgar_8k')
        batch = build_batch('MARA', first_filing='2023-05-19',
                            source_types=['edgar_8k'])
        assert r_id in _ids(batch)

    def test_no_first_filing_all_edgar_included(self, db, build_batch):
        """When first_filing is None, all EDGAR reports are included."""
        r_id = _insert_report(db, 'MARA', '2019-01-01', source_type='edgar_10k')
        batch = build_batch('MARA', first_filing=None)
        assert r_id in _ids(batch)


# ---------------------------------------------------------------------------
# TestOperationsExtractBackfillMode
# ---------------------------------------------------------------------------

@pytest.fixture
def app(tmp_path):
    import importlib, run_web
    import app_globals

    db = MinerDB(str(tmp_path / 'test.db'))
    db.insert_company({
        'ticker': 'MARA',
        'name': 'MARA Holdings',
        'tier': 1,
        'ir_url': 'https://ir.mara.com',
        'pr_base_url': 'https://ir.mara.com',
        'cik': '0001437491',
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


class TestOperationsExtractBackfillMode:
    def test_backfill_mode_returns_200_with_task_id(self, client, monkeypatch):
        """POST /api/operations/interpret with run_mode='backfill' returns 200 + task_id."""
        import routes.operations as ops_mod
        monkeypatch.setattr(ops_mod, '_active_tickers', set())

        resp = client.post('/api/operations/interpret',
                           json={'ticker': 'MARA', 'run_mode': 'backfill'})
        assert resp.status_code == 200
        data = resp.get_json()
        assert data['success'] is True
        assert 'task_id' in data['data']

    def test_absent_run_mode_defaults_to_resume(self, client, monkeypatch):
        """Missing run_mode defaults to resume (force=False) behaviour."""
        import routes.operations as ops_mod
        monkeypatch.setattr(ops_mod, '_active_tickers', set())

        resp = client.post('/api/operations/interpret', json={'ticker': 'MARA'})
        assert resp.status_code in (200, 409)
        if resp.status_code == 200:
            assert resp.get_json()['success'] is True

    def test_legacy_force_true_still_accepted(self, client, monkeypatch):
        """Legacy force=True (no run_mode) is still accepted."""
        import routes.operations as ops_mod
        monkeypatch.setattr(ops_mod, '_active_tickers', set())

        resp = client.post('/api/operations/interpret',
                           json={'ticker': 'MARA', 'force': True})
        assert resp.status_code in (200, 409)
        if resp.status_code == 200:
            assert resp.get_json()['success'] is True

    def test_backfill_mode_calls_get_reports_for_backfill(self, client, monkeypatch):
        """Backfill mode uses get_reports_for_backfill, not get_all_reports_for_extraction."""
        import routes.operations as ops_mod

        called = {'backfill': False, 'all': False}

        original_backfill = MinerDB.get_reports_for_backfill
        original_all = MinerDB.get_all_reports_for_extraction

        def _track_backfill(self_db, **kwargs):
            called['backfill'] = True
            return original_backfill(self_db, **kwargs)

        def _track_all(self_db, **kwargs):
            called['all'] = True
            return original_all(self_db, **kwargs)

        # Use an ImmediateThread so _run() executes synchronously
        class _ImmediateThread:
            def __init__(self, target=None, args=(), daemon=False, name=None):
                self._target = target
            def start(self):
                if self._target:
                    self._target()

        monkeypatch.setattr(ops_mod, '_active_tickers', set())
        monkeypatch.setattr(ops_mod.threading, 'Thread', _ImmediateThread)
        monkeypatch.setattr(MinerDB, 'get_reports_for_backfill', _track_backfill)
        monkeypatch.setattr(MinerDB, 'get_all_reports_for_extraction', _track_all)

        resp = client.post('/api/operations/interpret',
                           json={'ticker': 'MARA', 'run_mode': 'backfill',
                                 'warm_model': False})
        assert resp.status_code == 200
        assert called['backfill'], "get_reports_for_backfill was not called for backfill mode"
        assert not called['all'], "get_all_reports_for_extraction should not be called in backfill mode"

    def test_force_mode_does_not_call_get_reports_for_backfill(self, client, monkeypatch):
        """run_mode='force' uses get_all_reports_for_extraction, not get_reports_for_backfill."""
        import routes.operations as ops_mod

        called = {'backfill': False}

        original_backfill = MinerDB.get_reports_for_backfill

        def _track_backfill(self_db, **kwargs):
            called['backfill'] = True
            return original_backfill(self_db, **kwargs)

        class _ImmediateThread:
            def __init__(self, target=None, args=(), daemon=False, name=None):
                self._target = target
            def start(self):
                if self._target:
                    self._target()

        monkeypatch.setattr(ops_mod, '_active_tickers', set())
        monkeypatch.setattr(ops_mod.threading, 'Thread', _ImmediateThread)
        monkeypatch.setattr(MinerDB, 'get_reports_for_backfill', _track_backfill)

        resp = client.post('/api/operations/interpret',
                           json={'ticker': 'MARA', 'run_mode': 'force',
                                 'warm_model': False})
        assert resp.status_code == 200
        assert not called['backfill'], "get_reports_for_backfill should not be called for force mode"
