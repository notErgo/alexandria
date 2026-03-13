"""
Tests for the Track A report_metric_verdict system.

These tests FAIL before implementation — they document the contract for:
  - DB CRUD: upsert/get/get_all/delete verdict
  - approve_review_item writes 'has_data' verdict
  - edit_review_item writes 'has_data' verdict
  - _apply_llm_result skips when 'no_data' verdict exists
  - _insert_zero_extract_review_items filters already-verdicted metrics
  - get_reports_missing_metric exclude_no_data_acked behaviour
  - POST /api/review/<id>/no_data route
  - 'has_data' verdict survives report requeue
  - Schema v45 table existence
"""
import pytest
from helpers import make_report, make_review_item
from infra.db import MinerDB


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def db(tmp_path):
    return MinerDB(str(tmp_path / 'test.db'))


@pytest.fixture
def db_with_company(db):
    db.insert_company({
        'ticker': 'MARA',
        'name': 'MARA Holdings, Inc.',
        'tier': 1,
        'ir_url': 'https://www.marathondh.com/news',
        'pr_base_url': 'https://www.marathondh.com',
        'cik': '0001437491',
        'active': 1,
    })
    return db


@pytest.fixture
def db_with_report(db_with_company):
    report_id = db_with_company.insert_report(make_report())
    db_with_company._report_id = report_id
    return db_with_company


# ---------------------------------------------------------------------------
# 1. Schema v45: table exists after db init
# ---------------------------------------------------------------------------

class TestSchemaV45:
    def test_report_metric_verdict_table_exists(self, db):
        """report_metric_verdict table must exist after MinerDB initialises."""
        with db._get_connection() as conn:
            row = conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name='report_metric_verdict'"
            ).fetchone()
        assert row is not None, "report_metric_verdict table not found — migration v45 missing"


# ---------------------------------------------------------------------------
# 2. DB CRUD
# ---------------------------------------------------------------------------

class TestUpsertGetVerdict:
    def test_upsert_and_get_verdict(self, db_with_report):
        db = db_with_report
        rid = db._report_id
        db.upsert_report_metric_verdict(rid, 'production_btc', 'no_data')
        result = db.get_report_metric_verdict(rid, 'production_btc')
        assert result == 'no_data'

    def test_upsert_overwrites_existing_verdict(self, db_with_report):
        db = db_with_report
        rid = db._report_id
        db.upsert_report_metric_verdict(rid, 'production_btc', 'no_data')
        db.upsert_report_metric_verdict(rid, 'production_btc', 'has_data')
        result = db.get_report_metric_verdict(rid, 'production_btc')
        assert result == 'has_data'

    def test_get_verdict_returns_none_when_absent(self, db_with_report):
        db = db_with_report
        rid = db._report_id
        result = db.get_report_metric_verdict(rid, 'production_btc')
        assert result is None

    def test_get_report_metric_verdicts_returns_dict(self, db_with_report):
        db = db_with_report
        rid = db._report_id
        db.upsert_report_metric_verdict(rid, 'production_btc', 'no_data')
        db.upsert_report_metric_verdict(rid, 'holdings_btc', 'has_data')
        verdicts = db.get_report_metric_verdicts(rid)
        assert isinstance(verdicts, dict)
        assert verdicts.get('production_btc') == 'no_data'
        assert verdicts.get('holdings_btc') == 'has_data'

    def test_get_report_metric_verdicts_empty_when_none(self, db_with_report):
        db = db_with_report
        rid = db._report_id
        verdicts = db.get_report_metric_verdicts(rid)
        assert verdicts == {}

    def test_delete_verdict_removes_row(self, db_with_report):
        db = db_with_report
        rid = db._report_id
        db.upsert_report_metric_verdict(rid, 'production_btc', 'no_data')
        db.delete_report_metric_verdict(rid, 'production_btc')
        result = db.get_report_metric_verdict(rid, 'production_btc')
        assert result is None

    def test_verdict_check_constraint_rejects_invalid_value(self, db_with_report):
        """Only 'no_data' and 'has_data' are accepted by the CHECK constraint."""
        db = db_with_report
        rid = db._report_id
        import sqlite3
        with pytest.raises((sqlite3.IntegrityError, ValueError)):
            db.upsert_report_metric_verdict(rid, 'production_btc', 'invalid_value')


# ---------------------------------------------------------------------------
# 3. approve_review_item writes 'has_data' verdict
# ---------------------------------------------------------------------------

class TestApproveWritesHasDataVerdict:
    def test_approve_review_item_writes_has_data_verdict(self, db_with_report):
        db = db_with_report
        rid = db._report_id
        rq_id = db.insert_review_item(make_review_item(report_id=rid))
        db.approve_review_item(rq_id)
        verdict = db.get_report_metric_verdict(rid, 'production_btc')
        assert verdict == 'has_data', (
            f"approve_review_item must write 'has_data' verdict; got {verdict!r}"
        )


# ---------------------------------------------------------------------------
# 4. edit_review_item writes 'has_data' verdict
# ---------------------------------------------------------------------------

class TestEditWritesHasDataVerdict:
    def test_edit_review_item_writes_has_data_verdict(self, db_with_report):
        db = db_with_report
        rid = db._report_id
        rq_id = db.insert_review_item(make_review_item(report_id=rid))
        db.edit_review_item(rq_id, corrected_value=750.0, note='manual correction')
        verdict = db.get_report_metric_verdict(rid, 'production_btc')
        assert verdict == 'has_data', (
            f"edit_review_item must write 'has_data' verdict; got {verdict!r}"
        )


# ---------------------------------------------------------------------------
# 5. has_data verdict survives report requeue
# ---------------------------------------------------------------------------

class TestHasDataVerdictSurvivesRequeue:
    def test_has_data_verdict_persists_after_reset_extraction_status(self, db_with_report):
        """Resetting extraction_status must not delete the has_data verdict."""
        db = db_with_report
        rid = db._report_id
        db.upsert_report_metric_verdict(rid, 'production_btc', 'has_data')
        db.mark_report_extracted(rid)
        db.reset_report_extraction_status(rid)
        verdict = db.get_report_metric_verdict(rid, 'production_btc')
        assert verdict == 'has_data', (
            "has_data verdict must survive report requeue (reset_report_extraction_status)"
        )


# ---------------------------------------------------------------------------
# 6. _apply_llm_result skips when no_data verdict exists
# ---------------------------------------------------------------------------

class TestApplyLlmResultSkipsOnNoDataVerdict:
    def test_apply_llm_result_returns_early_for_no_data_verdict(self, db_with_report, monkeypatch):
        """_apply_llm_result must return without inserting when verdict is 'no_data'."""
        from interpreters.result_router import _apply_llm_result
        from miner_types import ExtractionRunConfig, ExtractionResult

        db = db_with_report
        rid = db._report_id

        monkeypatch.setattr(db, 'get_report_metric_verdict',
                            lambda r, m: 'no_data')

        inserted = []
        original_insert = db.insert_data_point
        monkeypatch.setattr(db, 'insert_data_point',
                            lambda *a, **kw: inserted.append(a) or original_insert(*a, **kw))
        original_insert_rq = db.insert_review_item
        monkeypatch.setattr(db, 'insert_review_item',
                            lambda *a, **kw: inserted.append(a) or original_insert_rq(*a, **kw))

        report = db.get_report(rid)
        llm_result = ExtractionResult(
            metric='production_btc',
            value=700.0,
            unit='BTC',
            confidence=0.92,
            extraction_method='llm',
            source_snippet='mined 700 BTC',
            pattern_id='llm_0',
            period_granularity='monthly',
        )
        from miner_types import ExtractionSummary
        summary = ExtractionSummary()
        config = ExtractionRunConfig(expected_granularity='monthly', ticker='MARA')

        _apply_llm_result(
            metric='production_btc',
            llm_result=llm_result,
            db=db,
            report=report,
            confidence_threshold=0.75,
            summary=summary,
            run_config=config,
        )

        assert len(inserted) == 0, (
            "_apply_llm_result must not insert data_point or review_item "
            "when a 'no_data' verdict exists for the metric"
        )


# ---------------------------------------------------------------------------
# 7. _insert_zero_extract_review_items filters verdicted metrics
# ---------------------------------------------------------------------------

class TestZeroExtractFiltersVerdictedMetrics:
    def test_no_data_verdict_blocks_zero_extract_insert(self, db_with_report):
        """_insert_zero_extract_review_items must skip metrics with any existing verdict."""
        from interpreters.interpret_pipeline import _insert_zero_extract_review_items
        from miner_types import ExtractionSummary

        db = db_with_report
        rid = db._report_id
        db.upsert_report_metric_verdict(rid, 'production_btc', 'no_data')

        report = db.get_report(rid)
        summary = ExtractionSummary()
        _insert_zero_extract_review_items(
            db, report, ['production_btc', 'holdings_btc'], summary
        )

        items = db.get_review_items(status='PENDING', limit=50, offset=0)
        metrics_inserted = {i['metric'] for i in items}
        assert 'production_btc' not in metrics_inserted, (
            "_insert_zero_extract_review_items must not insert for metrics "
            "that already have a verdict"
        )
        assert 'holdings_btc' in metrics_inserted, (
            "_insert_zero_extract_review_items must still insert for metrics "
            "without a verdict"
        )


# ---------------------------------------------------------------------------
# 8. get_reports_missing_metric with exclude_no_data_acked
# ---------------------------------------------------------------------------

class TestGetReportsMissingMetricExcludeNoDataAcked:
    def test_excludes_report_when_all_metrics_have_no_data_verdict(self, db_with_report):
        """Report is excluded when ALL requested metrics have a no_data verdict."""
        db = db_with_report
        rid = db._report_id
        db.mark_report_extracted(rid)
        db.upsert_report_metric_verdict(rid, 'production_btc', 'no_data')
        db.upsert_report_metric_verdict(rid, 'holdings_btc', 'no_data')

        results = db.get_reports_missing_metric(
            ['production_btc', 'holdings_btc'],
            exclude_no_data_acked=True,
        )
        report_ids = [r['id'] for r in results]
        assert rid not in report_ids, (
            "Report with all-metrics no_data acked must be excluded when "
            "exclude_no_data_acked=True"
        )

    def test_includes_report_when_exclude_no_data_acked_false(self, db_with_report):
        """Report is included when exclude_no_data_acked=False even if all metrics acked."""
        db = db_with_report
        rid = db._report_id
        db.mark_report_extracted(rid)
        db.upsert_report_metric_verdict(rid, 'production_btc', 'no_data')
        db.upsert_report_metric_verdict(rid, 'holdings_btc', 'no_data')

        results = db.get_reports_missing_metric(
            ['production_btc', 'holdings_btc'],
            exclude_no_data_acked=False,
        )
        report_ids = [r['id'] for r in results]
        assert rid in report_ids, (
            "Report must be included when exclude_no_data_acked=False"
        )

    def test_includes_partially_acked_report(self, db_with_report):
        """Report is included when only SOME (not all) requested metrics are acked."""
        db = db_with_report
        rid = db._report_id
        db.mark_report_extracted(rid)
        db.upsert_report_metric_verdict(rid, 'production_btc', 'no_data')
        # holdings_btc has no verdict — report is still missing that metric

        results = db.get_reports_missing_metric(
            ['production_btc', 'holdings_btc'],
            exclude_no_data_acked=True,
        )
        report_ids = [r['id'] for r in results]
        assert rid in report_ids, (
            "Partially-acked report must still appear in get_reports_missing_metric "
            "because holdings_btc has no verdict yet"
        )


# ---------------------------------------------------------------------------
# 9. POST /api/review/<id>/no_data route
# ---------------------------------------------------------------------------

@pytest.fixture
def flask_app_with_review(db_with_company, monkeypatch):
    """Flask test client wired to db_with_company with one review item."""
    import app_globals
    monkeypatch.setattr(app_globals, 'get_db', lambda: db_with_company)

    rid = db_with_company.insert_report(make_report())
    db_with_company._report_id = rid
    db_with_company._review_id_llm_empty = db_with_company.insert_review_item(
        make_review_item(
            report_id=rid,
            agreement_status='LLM_EMPTY',
        )
    )
    db_with_company._review_id_normal = db_with_company.insert_review_item(
        make_review_item(
            report_id=rid,
            period='2024-10-01',
            agreement_status='REVIEW_QUEUE',
        )
    )

    from flask import Flask
    from routes.review import bp
    flask_app = Flask(__name__)
    flask_app.config['TESTING'] = True
    flask_app.register_blueprint(bp)
    return flask_app, db_with_company


class TestNoDataRoute:
    def test_llm_empty_item_no_confirmation_required(self, flask_app_with_review):
        """LLM_EMPTY items are acked as no_data without requiring confirmed=true."""
        flask_app, db = flask_app_with_review
        item_id = db._review_id_llm_empty
        with flask_app.test_client() as c:
            resp = c.post(
                f'/api/review/{item_id}/no_data',
                json={},
                content_type='application/json',
            )
        assert resp.status_code == 200, (
            f"LLM_EMPTY no_data must return 200 without confirmed; got {resp.status_code}"
        )

    def test_llm_empty_writes_no_data_verdict(self, flask_app_with_review):
        """POST /api/review/<id>/no_data writes 'no_data' verdict for LLM_EMPTY item."""
        flask_app, db = flask_app_with_review
        item_id = db._review_id_llm_empty
        rid = db._report_id
        with flask_app.test_client() as c:
            c.post(
                f'/api/review/{item_id}/no_data',
                json={},
                content_type='application/json',
            )
        verdict = db.get_report_metric_verdict(rid, 'production_btc')
        assert verdict == 'no_data', (
            f"POST no_data must write 'no_data' verdict; got {verdict!r}"
        )

    def test_non_llm_empty_without_confirmed_returns_400(self, flask_app_with_review):
        """Non-LLM_EMPTY items require confirmed=true body; omitting returns 400."""
        flask_app, db = flask_app_with_review
        item_id = db._review_id_normal
        with flask_app.test_client() as c:
            resp = c.post(
                f'/api/review/{item_id}/no_data',
                json={},
                content_type='application/json',
            )
        assert resp.status_code == 400, (
            f"Non-LLM_EMPTY no_data without confirmed must return 400; got {resp.status_code}"
        )

    def test_non_llm_empty_with_confirmed_returns_200(self, flask_app_with_review):
        """Non-LLM_EMPTY items succeed when confirmed=true is provided."""
        flask_app, db = flask_app_with_review
        item_id = db._review_id_normal
        with flask_app.test_client() as c:
            resp = c.post(
                f'/api/review/{item_id}/no_data',
                json={'confirmed': True},
                content_type='application/json',
            )
        assert resp.status_code == 200, (
            f"Non-LLM_EMPTY no_data with confirmed=true must return 200; got {resp.status_code}"
        )

    def test_no_data_rejects_the_review_item(self, flask_app_with_review):
        """POST /api/review/<id>/no_data sets review item status to REJECTED."""
        flask_app, db = flask_app_with_review
        item_id = db._review_id_llm_empty
        with flask_app.test_client() as c:
            c.post(
                f'/api/review/{item_id}/no_data',
                json={},
                content_type='application/json',
            )
        items = db.get_review_items(status='REJECTED', limit=50, offset=0)
        rejected_ids = {i['id'] for i in items}
        assert item_id in rejected_ids, (
            "POST no_data must reject (set status=REJECTED) the review item"
        )
