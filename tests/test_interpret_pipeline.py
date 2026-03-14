"""
Tests for interpreters.interpret_pipeline.extract_report.

Written test-first per TDD requirement. These tests FAIL before
extraction_pipeline.py is created and PASS after implementation.

Monkeypatches LLMInterpreter.check_connectivity → False to force the
regex-only code path, so tests are deterministic (no Ollama dependency).
"""
import pytest
from helpers import make_report, make_data_point


@pytest.fixture
def db_with_company(db):
    """db fixture (from conftest) with a MARA company row pre-inserted."""
    db.insert_company({
        'ticker': 'MARA', 'name': 'MARA Holdings, Inc.',
        'tier': 1, 'ir_url': 'https://www.marathondh.com/news',
        'pr_base_url': 'https://www.marathondh.com',
        'cik': '0001437491', 'active': 1,
    })
    return db




class TestExtractReport:
    def test_extract_report_stores_data_point(self, db_with_company, monkeypatch):
        """LLM batch path: connectivity=True, LLM returns known value → data_point stored."""
        from unittest.mock import MagicMock
        from miner_types import ExtractionResult
        import interpreters.interpret_pipeline as _ep

        mock_llm = MagicMock()
        mock_llm.check_connectivity.return_value = True
        mock_llm.extract_batch.return_value = {
            'production_btc': ExtractionResult(
                metric='production_btc', value=700.0, unit='BTC', confidence=0.95,
                extraction_method='llm_test', source_snippet='MARA mined 700 BTC',
                pattern_id='llm_test',
            ),
        }
        monkeypatch.setattr(_ep, '_get_llm_interpreter', lambda db: mock_llm)

        report_id = db_with_company.insert_report(make_report(
            raw_text='MARA bitcoin mined 700 BTC in September 2024. Hash rate 20 EH/s.',
            report_date='2024-09-01',
            source_type='archive_html',
        ))
        report = db_with_company.get_report(report_id)

        from interpreters.interpret_pipeline import extract_report
        summary = extract_report(report, db_with_company)

        rows = db_with_company.query_data_points(ticker='MARA', metric='production_btc')
        assert len(rows) >= 1
        assert any(abs(r['value'] - 700.0) < 0.01 for r in rows)

    def test_extract_report_analyst_protected(self, db_with_company, monkeypatch):
        """Analyst-protected data point must not be overwritten by the pipeline."""
        from interpreters.llm_interpreter import LLMInterpreter
        monkeypatch.setattr(LLMInterpreter, 'check_connectivity', lambda self: False)

        report_id = db_with_company.insert_report(make_report(
            raw_text='MARA mined 700 BTC in September 2024.',
            report_date='2024-09-01',
            source_type='archive_html',
        ))
        # Insert analyst-protected data point with a sentinel value (999)
        db_with_company.insert_data_point({
            'report_id': report_id, 'ticker': 'MARA', 'period': '2024-09-01',
            'metric': 'production_btc', 'value': 999.0, 'unit': 'BTC',
            'confidence': 1.0, 'extraction_method': 'analyst',
            'source_snippet': 'analyst override',
        })

        report = db_with_company.get_report(report_id)
        from interpreters.interpret_pipeline import extract_report
        extract_report(report, db_with_company)

        rows = db_with_company.query_data_points(ticker='MARA', metric='production_btc')
        assert len(rows) == 1
        assert abs(rows[0]['value'] - 999.0) < 0.01, "Analyst value must not be overwritten"

    def test_extract_report_marks_extracted_at(self, db_with_company, monkeypatch):
        """After extract_report(), the report must no longer appear in get_unextracted_reports()."""
        from interpreters.llm_interpreter import LLMInterpreter
        monkeypatch.setattr(LLMInterpreter, 'check_connectivity', lambda self: False)

        report_id = db_with_company.insert_report(make_report(
            raw_text='MARA mined 700 BTC in September 2024.',
            report_date='2024-09-01',
            source_type='archive_html',
        ))

        # Confirm not yet extracted
        unextracted_before = db_with_company.get_unextracted_reports()
        assert any(r['id'] == report_id for r in unextracted_before)

        report = db_with_company.get_report(report_id)
        from interpreters.interpret_pipeline import extract_report
        extract_report(report, db_with_company)

        # Must no longer appear in unextracted list
        unextracted_after = db_with_company.get_unextracted_reports()
        assert not any(r['id'] == report_id for r in unextracted_after), \
            "Report should be marked as extracted after extract_report() runs"

    def test_extract_report_returns_summary(self, db_with_company, monkeypatch):
        """extract_report must return an ExtractionSummary with reports_processed=1."""
        from interpreters.llm_interpreter import LLMInterpreter
        monkeypatch.setattr(LLMInterpreter, 'check_connectivity', lambda self: False)

        report_id = db_with_company.insert_report(make_report(
            raw_text='MARA mined 700 BTC in September 2024.',
            report_date='2024-09-01',
            source_type='archive_html',
        ))
        report = db_with_company.get_report(report_id)

        from interpreters.interpret_pipeline import extract_report
        from miner_types import ExtractionSummary
        summary = extract_report(report, db_with_company)

        assert isinstance(summary, ExtractionSummary)
        assert summary.reports_processed == 1

    def test_extract_report_empty_text_increments_errors(self, db_with_company, monkeypatch):
        """A report with empty raw_text must increment errors and still mark extracted."""
        from interpreters.llm_interpreter import LLMInterpreter
        monkeypatch.setattr(LLMInterpreter, 'check_connectivity', lambda self: False)

        report_id = db_with_company.insert_report(make_report(
            raw_text='',
            report_date='2024-09-01',
            source_type='archive_html',
        ))
        report = db_with_company.get_report(report_id)

        from interpreters.interpret_pipeline import extract_report
        summary = extract_report(report, db_with_company)

        assert summary.errors == 1
        # Should still be marked extracted (to avoid infinite re-processing)
        unextracted = db_with_company.get_unextracted_reports()
        assert not any(r['id'] == report_id for r in unextracted)


class TestExtractReportBatchPath:
    """extract_report uses extract_batch (1 LLM call) instead of per-metric loop."""

    @pytest.fixture
    def db_with_company(self, db):
        db.insert_company({
            'ticker': 'MARA', 'name': 'MARA Holdings, Inc.',
            'tier': 1, 'ir_url': 'https://www.marathondh.com/news',
            'pr_base_url': 'https://www.marathondh.com',
            'cik': '0001437491', 'active': 1,
        })
        return db

    def test_pipeline_calls_extract_batch_not_per_metric(
        self, db_with_company, monkeypatch
    ):
        """When LLM available, extract_batch called once; per-metric extract() not called."""
        import interpreters.interpret_pipeline as _ep
        from miner_types import ExtractionResult
        from config import LLM_MODEL_ID

        monkeypatch.setattr(_ep, '_get_llm_interpreter', lambda db: _make_mock_llm())

        batch_called = []
        extract_called = []

        def fake_batch(text, metrics, ticker=None, **kwargs):
            batch_called.append(metrics)
            return {
                "production_btc": ExtractionResult(
                    metric="production_btc", value=700.0, unit="BTC",
                    confidence=0.95, extraction_method=f"llm_{LLM_MODEL_ID}",
                    source_snippet="mined 700 BTC", pattern_id=f"llm_{LLM_MODEL_ID}",
                )
            }

        def fake_extract(text, metric):
            extract_called.append(metric)
            return None

        mock_llm = _make_mock_llm()
        mock_llm.extract_batch = fake_batch
        mock_llm.extract = fake_extract
        mock_llm.extract_for_period.return_value = {}
        monkeypatch.setattr(_ep, '_get_llm_interpreter', lambda db: mock_llm)

        report_id = db_with_company.insert_report(make_report(
            raw_text='MARA bitcoin mined 700 BTC in September 2024. Hash rate 20 EH/s.',
            report_date='2024-09-01',
        ))
        report = db_with_company.get_report(report_id)

        from interpreters.interpret_pipeline import extract_report
        extract_report(report, db_with_company)

        assert len(batch_called) == 1, "extract_batch must be called exactly once"
        assert len(extract_called) == 0, "per-metric extract() must not be called"

    def test_pipeline_batch_marks_report_extracted(
        self, db_with_company, monkeypatch
    ):
        """mark_report_extracted fires even when LLM batch is used."""
        import interpreters.interpret_pipeline as _ep

        mock_llm = _make_mock_llm()
        mock_llm.extract_batch = lambda text, metrics, ticker=None, **kw: {}
        mock_llm.extract = lambda text, metric: None
        mock_llm.extract_for_period.return_value = {}
        monkeypatch.setattr(_ep, '_get_llm_interpreter', lambda db: mock_llm)

        report_id = db_with_company.insert_report(make_report(
            raw_text='MARA mined 700 BTC in September 2024.',
            report_date='2024-09-01',
        ))
        report = db_with_company.get_report(report_id)

        from interpreters.interpret_pipeline import extract_report
        extract_report(report, db_with_company)

        unextracted = db_with_company.get_unextracted_reports()
        assert not any(r['id'] == report_id for r in unextracted)


class TestLLMOnlyRouting:
    """LLM_ONLY decision: high confidence → data_points, low → review_queue."""

    @pytest.fixture
    def db_with_company(self, db):
        db.insert_company({
            'ticker': 'MARA', 'name': 'MARA Holdings, Inc.',
            'tier': 1, 'ir_url': 'https://www.marathondh.com/news',
            'pr_base_url': 'https://www.marathondh.com',
            'cik': '0001437491', 'active': 1,
        })
        return db

    def _make_llm_only_batch(self, value, confidence):
        """Build a mock LLM that returns one metric at given confidence; no regex fires."""
        from miner_types import ExtractionResult
        from config import LLM_MODEL_ID

        def fake_batch(text, metrics, ticker=None, **kwargs):
            return {
                "production_btc": ExtractionResult(
                    metric="production_btc", value=value, unit="BTC",
                    confidence=confidence, extraction_method=f"llm_{LLM_MODEL_ID}",
                    source_snippet="LLM found it", pattern_id=f"llm_{LLM_MODEL_ID}",
                )
            }

        mock = _make_mock_llm()
        mock.extract_batch = fake_batch
        mock.extract_for_period.return_value = {}
        return mock

    def test_llm_only_high_confidence_auto_accepts(
        self, db_with_company, monkeypatch
    ):
        """LLM_ONLY with confidence >= threshold → stored directly in data_points."""
        import interpreters.interpret_pipeline as _ep
        from config import CONFIDENCE_REVIEW_THRESHOLD


        mock = self._make_llm_only_batch(value=700.0, confidence=CONFIDENCE_REVIEW_THRESHOLD)
        monkeypatch.setattr(_ep, '_get_llm_interpreter', lambda db: mock)

        report_id = db_with_company.insert_report(make_report(
            raw_text='MARA bitcoin mined 700 BTC in September 2024. Hash rate 20 EH/s.',
            report_date='2024-09-01',
        ))
        report = db_with_company.get_report(report_id)

        from interpreters.interpret_pipeline import extract_report
        summary = extract_report(report, db_with_company)

        rows = db_with_company.query_data_points(ticker='MARA', metric='production_btc')
        assert any(abs(r['value'] - 700.0) < 0.01 for r in rows), \
            "High-confidence LLM_ONLY must land in data_points"
        assert summary.data_points_extracted >= 1

    def test_llm_only_low_confidence_goes_to_review(
        self, db_with_company, monkeypatch
    ):
        """LLM_ONLY with confidence < threshold → review_queue, not data_points."""
        import interpreters.interpret_pipeline as _ep
        from config import CONFIDENCE_REVIEW_THRESHOLD


        low_conf = max(0.0, CONFIDENCE_REVIEW_THRESHOLD - 0.1)
        mock = self._make_llm_only_batch(value=700.0, confidence=low_conf)
        monkeypatch.setattr(_ep, '_get_llm_interpreter', lambda db: mock)

        report_id = db_with_company.insert_report(make_report(
            raw_text='MARA bitcoin mined 700 BTC in September 2024. Hash rate 20 EH/s.',
            report_date='2024-09-01',
        ))
        report = db_with_company.get_report(report_id)

        from interpreters.interpret_pipeline import extract_report
        summary = extract_report(report, db_with_company)

        rows = db_with_company.query_data_points(ticker='MARA', metric='production_btc')
        assert len(rows) == 0, "Low-confidence LLM_ONLY must NOT land in data_points"
        assert summary.review_flagged >= 1


def _make_mock_llm():
    """Return a mock LLM extractor with connectivity=True."""
    from unittest.mock import MagicMock
    mock = MagicMock()
    mock.check_connectivity.return_value = True
    return mock


# ── Boilerplate Stripping ───────────────────────────────────────────────────

class TestBoilerplateStripping:
    """_clean_for_llm strips boilerplate from back 60%+ of document."""

    def test_forward_looking_at_end_stripped(self):
        """FORWARD-LOOKING STATEMENTS in back 60% is stripped."""
        from interpreters.interpret_pipeline import _clean_for_llm
        prefix = "A" * 500 + "MARA mined 700 BTC in January.\n\n"
        suffix = "FORWARD-LOOKING STATEMENTS\nBlah blah legal text."
        text = prefix + suffix
        result = _clean_for_llm(text)
        assert "FORWARD-LOOKING STATEMENTS" not in result
        assert "700 BTC" in result

    def test_sentinel_in_first_40pct_not_stripped(self):
        """FORWARD-LOOKING STATEMENTS in first 40% is NOT stripped."""
        from interpreters.interpret_pipeline import _clean_for_llm
        # Put sentinel at character 10 of a 5000+ char document (well within first 40%)
        preamble = "FORWARD-LOOKING STATEMENTS paragraph at beginning. "
        rest = "A" * 5000 + " MARA mined 700 BTC in January."
        text = preamble + rest
        result = _clean_for_llm(text)
        assert "700 BTC" in result

    def test_no_sentinel_unchanged(self):
        """Document with no boilerplate sentinels is returned unchanged."""
        from interpreters.interpret_pipeline import _clean_for_llm
        text = "MARA mined 700 BTC in January. No legal sections here."
        result = _clean_for_llm(text)
        assert result == text.rstrip()

    def test_multiple_sentinels_strips_at_earliest(self):
        """When multiple sentinels match in back 60%, strips at the earliest one."""
        from interpreters.interpret_pipeline import _clean_for_llm
        prefix = "A" * 600 + " MARA mined 700 BTC.\n\n"
        text = prefix + "SAFE HARBOR STATEMENTS\nsome text\n\nCAUTIONARY STATEMENTS\nmore text"
        result = _clean_for_llm(text)
        assert "700 BTC" in result
        assert "SAFE HARBOR" not in result
        assert "CAUTIONARY STATEMENTS" not in result

    def test_about_company_section_stripped(self):
        """About [Company] section in back 60% is stripped."""
        from interpreters.interpret_pipeline import _clean_for_llm
        prefix = "A" * 600 + " MARA mined 700 BTC.\n\n"
        text = prefix + "About MARA\nWe are a company that mines BTC."
        result = _clean_for_llm(text)
        assert "700 BTC" in result
        assert "About MARA" not in result


# ── Gap Fill ─────────────────────────────────────────────────────────────────

class TestGapFill:
    """_try_gap_fill stores prior-period data when LLM finds it."""

    @pytest.fixture
    def db_with_company(self, db):
        db.insert_company({
            'ticker': 'MARA', 'name': 'MARA Holdings, Inc.',
            'tier': 1, 'ir_url': 'https://www.marathondh.com/news',
            'pr_base_url': 'https://www.marathondh.com',
            'cik': '0001437491', 'active': 1,
        })
        return db

    def test_gap_fill_disabled_when_llm_unavailable(self, db_with_company, monkeypatch):
        """Gap fill must not be attempted when LLM is unavailable."""
        import interpreters.interpret_pipeline as _ep

        # LLM not available → _try_gap_fill should not be called
        monkeypatch.setattr(_ep, '_get_llm_interpreter', lambda db: None)
        gap_fill_called = []
        monkeypatch.setattr(_ep, '_try_gap_fill', lambda *a, **kw: gap_fill_called.append(1))

        report_id = db_with_company.insert_report(make_report(
            raw_text='MARA bitcoin produced 700 BTC in September 2024. In August we produced 650 BTC.',
            report_date='2024-09-01',
        ))
        report = db_with_company.get_report(report_id)

        from interpreters.interpret_pipeline import extract_report
        extract_report(report, db_with_company)
        assert len(gap_fill_called) == 0

    def test_gap_fill_skipped_when_main_pass_yields_zero_data_points(
        self, db_with_company, monkeypatch
    ):
        """Gap fill must not fire when the main extraction pass found no data points.

        A zero-yield report (pre-pivot corporate 8-K that slipped past the keyword
        gate, etc.) cannot contain historical BTC figures for prior periods — skipping
        the gap fill LLM call avoids burning cycles on known-empty documents.

        The keyword gate (no BTC phrases) causes an early return before LLM runs, so
        this test uses text that contains BTC keywords (passes the gate) but where
        the LLM returns nothing — simulating a corporate announcement that mentions
        bitcoin once in boilerplate but has no production figures.
        """
        import interpreters.interpret_pipeline as _ep

        # Stub LLM as available but returning nothing for every call.
        # Must implement the full interface used by _run_llm_batch/_apply_agreement
        # so no AttributeError short-circuits the pipeline before the gap-fill guard.
        class _EmptyLLM:
            _last_call_meta = {}

            def extract_batch(self, *a, **kw):
                return {}

            def extract(self, *a, **kw):
                return None

            def extract_historical_periods(self, *a, **kw):
                return {}

            def check_connectivity(self):
                return True

        monkeypatch.setattr(_ep, '_get_llm_interpreter', lambda db: _EmptyLLM())
        gap_fill_called = []
        monkeypatch.setattr(_ep, '_try_gap_fill', lambda *a, **kw: gap_fill_called.append(1))

        # Report that passes the keyword gate (contains "bitcoin") but has no
        # extractable production figures — LLM will find nothing for current period.
        report_id = db_with_company.insert_report(make_report(
            raw_text=(
                'MARA Holdings announces board changes. Hash rate data was not disclosed. '
                'Bitcoin production figures are unavailable for this filing period.'
            ),
            report_date='2018-06-01',
        ))
        report = db_with_company.get_report(report_id)

        from interpreters.interpret_pipeline import extract_report
        summary = extract_report(report, db_with_company)

        assert summary.data_points_extracted == 0
        assert len(gap_fill_called) == 0, (
            "Gap fill must not be called when main extraction yields 0 data points"
        )

    def test_gap_fill_skips_when_prior_period_fully_populated(
        self, db_with_company, monkeypatch
    ):
        """Gap fill returns early if all metrics already have data for prior period."""
        import interpreters.interpret_pipeline as _ep

        mock_llm = _make_mock_llm()
        mock_llm.extract_batch.return_value = {}
        monkeypatch.setattr(_ep, '_get_llm_interpreter', lambda db: mock_llm)

        # Pre-populate all metrics for prior period (2024-08-01)
        _all_metrics = [r['key'] for r in db_with_company.get_metric_schema('BTC-miners', active_only=True)]
        for metric in _all_metrics:
            db_with_company.insert_data_point({
                'report_id': None, 'ticker': 'MARA', 'period': '2024-08-01',
                'metric': metric, 'value': 1.0, 'unit': 'BTC',
                'confidence': 0.9, 'extraction_method': 'analyst',
                'source_snippet': 'pre-filled',
            })

        extract_for_period_calls = []
        mock_llm.extract_for_period = lambda *a, **kw: extract_for_period_calls.append(1) or {}

        report_id = db_with_company.insert_report(make_report(
            raw_text='MARA mined 700 BTC in September 2024.',
            report_date='2024-09-01',
        ))
        report = db_with_company.get_report(report_id)

        from interpreters.interpret_pipeline import extract_report
        extract_report(report, db_with_company)
        # extract_for_period must not be called — all prior-period slots already filled
        assert len(extract_for_period_calls) == 0

    def test_gap_fill_stores_data_at_prior_period(self, db_with_company, monkeypatch):
        """Gap fill stores LLM result at prior period when slot is empty."""
        import interpreters.interpret_pipeline as _ep
        from miner_types import ExtractionResult

        mock_llm = _make_mock_llm()
        mock_llm.extract_batch.return_value = {
            'production_btc': ExtractionResult(
                metric='production_btc', value=700.0, unit='BTC', confidence=0.95,
                extraction_method='llm_test', source_snippet='September mined 700 BTC',
                pattern_id='llm_test',
            )
        }
        mock_llm.extract.return_value = None  # prevent per-metric fallback side-effects
        prior_result = ExtractionResult(
            metric='production_btc', value=650.0, unit='BTC', confidence=0.90,
            extraction_method='llm_gap_fill', source_snippet='August mined 650 BTC',
            pattern_id='llm_gap_fill',
        )
        mock_llm.extract_historical_periods.return_value = {
            '2024-08-01': {'production_btc': prior_result}
        }
        monkeypatch.setattr(_ep, '_get_llm_interpreter', lambda db: mock_llm)

        report_id = db_with_company.insert_report(make_report(
            raw_text='MARA bitcoin produced 700 BTC in September 2024. In August we produced 650 BTC.',
            report_date='2024-09-01',
        ))
        report = db_with_company.get_report(report_id)

        from interpreters.interpret_pipeline import extract_report
        extract_report(report, db_with_company)

        rows = db_with_company.query_data_points(ticker='MARA', metric='production_btc')
        assert any(
            r['period'] == '2024-08-01' and abs(r['value'] - 650.0) < 0.01
            for r in rows
        ), "Gap fill must store the prior-period value at period=2024-08-01"

    def test_gap_fill_does_not_overwrite_existing_data(
        self, db_with_company, monkeypatch
    ):
        """Gap fill must not overwrite existing data at prior period."""
        import interpreters.interpret_pipeline as _ep
        from miner_types import ExtractionResult

        mock_llm = _make_mock_llm()
        mock_llm.extract_batch.return_value = {}
        mock_llm.extract.return_value = None  # prevent per-metric fallback side-effects
        prior_result = ExtractionResult(
            metric='production_btc', value=650.0, unit='BTC', confidence=0.90,
            extraction_method='llm_gap_fill', source_snippet='August mined 650 BTC',
            pattern_id='llm_gap_fill',
        )
        mock_llm.extract_historical_periods.return_value = {
            '2024-08-01': {'production_btc': prior_result}
        }
        monkeypatch.setattr(_ep, '_get_llm_interpreter', lambda db: mock_llm)

        # Pre-insert prior-period data at a different sentinel value
        db_with_company.insert_data_point({
            'report_id': None, 'ticker': 'MARA', 'period': '2024-08-01',
            'metric': 'production_btc', 'value': 999.0, 'unit': 'BTC',
            'confidence': 0.95, 'extraction_method': 'llm_test',
            'source_snippet': 'existing',
        })

        report_id = db_with_company.insert_report(make_report(
            raw_text='MARA bitcoin produced 700 BTC in September. In August we produced 650 BTC.',
            report_date='2024-09-01',
        ))
        report = db_with_company.get_report(report_id)

        from interpreters.interpret_pipeline import extract_report
        extract_report(report, db_with_company)

        rows = db_with_company.query_data_points(ticker='MARA', metric='production_btc')
        aug_rows = [r for r in rows if r['period'] == '2024-08-01']
        assert len(aug_rows) == 1
        assert abs(aug_rows[0]['value'] - 999.0) < 0.01, "Existing data must not be overwritten"

    def test_prior_period_helper_wraps_january_to_december(self):
        """_prior_period('2024-01-01') → '2023-12-01'; wrap and normal cases correct."""
        from interpreters.interpret_pipeline import _prior_period
        assert _prior_period('2024-01-01') == '2023-12-01'
        assert _prior_period('2024-03-01') == '2024-02-01'


class TestActiveMetricFilter:
    """_active_metric_keys() filters metrics by metric_schema.active flag."""

    @pytest.fixture
    def db_with_company(self, db):
        db.insert_company({
            'ticker': 'MARA', 'name': 'MARA Holdings, Inc.',
            'tier': 1, 'ir_url': 'https://www.marathondh.com/news',
            'pr_base_url': 'https://www.marathondh.com',
            'cik': '0001437491', 'active': 1,
        })
        return db

    def _set_metric_active(self, db, key: str, active: int) -> None:
        with db._get_connection() as conn:
            conn.execute(
                "UPDATE metric_schema SET active = ? WHERE key = ?", (active, key)
            )

    def test_monthly_llm_receives_only_active_metrics(
        self, db_with_company, monkeypatch
    ):
        """LLM extract_batch receives only active metrics; inactive ones are excluded."""
        import interpreters.interpret_pipeline as _ep
        from miner_types import ExtractionResult


        # v22 migration already marks hashrate_eh active=0 in fresh DBs; confirm.
        rows = db_with_company.get_metric_schema('BTC-miners', active_only=False)
        hashrate_row = next((r for r in rows if r['key'] == 'hashrate_eh'), None)
        assert hashrate_row is not None and hashrate_row.get('active', 1) == 0, (
            "Precondition: hashrate_eh must be active=0 in a fresh DB (v22 migration)"
        )

        captured = {}

        class _MockLLM:
            _last_batch_summary = ''
            _last_call_meta = {}

            def check_connectivity(self):
                return True

            def extract_batch(self, text, metrics, ticker=None, **kwargs):
                captured['metrics'] = list(metrics)
                return {
                    'production_btc': ExtractionResult(
                        metric='production_btc', value=700.0, unit='BTC', confidence=0.95,
                        extraction_method='llm_test', source_snippet='mined 700 BTC',
                        pattern_id='llm_test',
                    )
                }

            def extract_for_period(self, text, metrics, period, prior_period):
                return {}

        mock_llm = _MockLLM()
        monkeypatch.setattr(_ep, '_get_llm_interpreter', lambda db: mock_llm)

        report_id = db_with_company.insert_report({
            'ticker': 'MARA', 'report_date': '2024-09-01',
            'published_date': '2024-09-01', 'source_type': 'archive_html',
            'source_url': 'https://example.com/mara-sep-2024.html',
            'raw_text': 'MARA bitcoin mined 700 BTC in September 2024. Hash rate 20 EH/s.',
            'parsed_at': '2024-09-01T10:00:00',
        })
        report = db_with_company.get_report(report_id)

        from interpreters.interpret_pipeline import extract_report
        extract_report(report, db_with_company)

        assert 'metrics' in captured, "extract_batch was not called"
        assert 'hashrate_eh' not in captured['metrics'], (
            "Inactive metric hashrate_eh must not be sent to LLM"
        )
        assert 'production_btc' in captured['metrics'], (
            "Active metric production_btc must be present"
        )



class TestKeywordGate:
    """Tests for 8-K keyword gate and ExtractionSummary.keyword_gated field."""

    @pytest.fixture
    def db_with_company(self, db):
        db.insert_company({
            'ticker': 'MARA', 'name': 'MARA Holdings, Inc.',
            'tier': 1, 'ir_url': 'https://www.marathondh.com/news',
            'pr_base_url': 'https://www.marathondh.com',
            'cik': '0001437491', 'active': 1,
        })
        return db

    def test_8k_keyword_gate_skip_when_keywords_active(self, db_with_company, monkeypatch):
        """8-K with no matching keywords is skipped and keyword_gated=1 in summary."""
        import interpreters.interpret_pipeline as _ep

        # Patch get_all_metric_keywords to return a keyword that won't match the text
        monkeypatch.setattr(
            db_with_company, 'get_all_metric_keywords',
            lambda active_only=True: [{'phrase': 'bitcoin production', 'metric_key': 'production_btc'}],
        )
        # Patch LLM to avoid connectivity check
        from unittest.mock import MagicMock
        mock_llm = MagicMock()
        mock_llm.check_connectivity.return_value = False
        monkeypatch.setattr(_ep, '_get_llm_interpreter', lambda db: mock_llm)

        report_id = db_with_company.insert_report(make_report(
            raw_text='ITEM 1.01 Entry into a Material Definitive Agreement. '
                     'The company signed a loan agreement.',
            report_date='2024-09-01',
            source_type='edgar_8k',
        ))
        report = db_with_company.get_report(report_id)

        from interpreters.interpret_pipeline import extract_report
        from miner_types import ExtractionSummary
        summary = extract_report(report, db_with_company)

        assert isinstance(summary, ExtractionSummary)
        assert summary.keyword_gated == 1, "keyword_gated must be 1 when gate fires"
        assert summary.data_points_extracted == 0, "No data_points should be stored"
        # Report must be marked extracted (so it is not retried forever)
        unextracted = db_with_company.get_unextracted_reports()
        assert not any(r['id'] == report_id for r in unextracted)

    def test_8k_keyword_gate_fails_when_no_db_keywords(self, db_with_company, monkeypatch):
        """Extraction fails when no active metric keywords are configured."""
        import interpreters.interpret_pipeline as _ep

        monkeypatch.setattr(
            db_with_company, 'get_all_metric_keywords',
            lambda active_only=True: [],
        )
        from unittest.mock import MagicMock
        mock_llm = MagicMock()
        mock_llm.check_connectivity.return_value = False
        monkeypatch.setattr(_ep, '_get_llm_interpreter', lambda db: mock_llm)

        report_id = db_with_company.insert_report(make_report(
            raw_text='MARA bitcoin mined 700 BTC in September 2024. Hash rate 20 EH/s.',
            report_date='2024-09-01',
            source_type='edgar_8k',
        ))
        report = db_with_company.get_report(report_id)

        from interpreters.interpret_pipeline import extract_report
        summary = extract_report(report, db_with_company)

        assert summary.keyword_gated == 0
        assert summary.errors == 1
        refreshed = db_with_company.get_report(report_id)
        assert refreshed['extraction_status'] == 'failed'

    def test_keyword_gated_field_exists_on_extraction_summary(self):
        """ExtractionSummary must have a keyword_gated field defaulting to 0."""
        from miner_types import ExtractionSummary
        s = ExtractionSummary()
        assert hasattr(s, 'keyword_gated'), "ExtractionSummary must have keyword_gated field"
        assert s.keyword_gated == 0

    def test_reset_report_extraction_status_makes_report_pending(self, db_with_company):
        """reset_report_extraction_status() resets extracted report back to pending."""
        report_id = db_with_company.insert_report(make_report(
            raw_text='MARA mined 700 BTC in September 2024.',
            report_date='2024-09-01',
            source_type='archive_html',
        ))
        # Mark as extracted first
        db_with_company.mark_report_extracted(report_id)
        # Confirm it's no longer in unextracted
        assert not any(r['id'] == report_id for r in db_with_company.get_unextracted_reports())

        # Now reset
        db_with_company.reset_report_extraction_status(report_id)

        # Must appear in unextracted again
        assert any(r['id'] == report_id for r in db_with_company.get_unextracted_reports()), (
            "reset_report_extraction_status must make the report eligible for extraction again"
        )

    def test_quarterly_keyword_gate_fires_when_no_match(self, db_with_company, monkeypatch):
        """Quarterly 10-Q with no matching keywords is skipped; keyword_gated=1."""
        import interpreters.interpret_pipeline as _ep
        from unittest.mock import MagicMock

        monkeypatch.setattr(
            db_with_company, 'get_all_metric_keywords',
            lambda active_only=True: [{'phrase': 'bitcoin produced', 'metric_key': 'production_btc'}],
        )
        mock_llm = MagicMock()
        mock_llm.check_connectivity.return_value = False
        monkeypatch.setattr(_ep, '_get_llm_interpreter', lambda db: mock_llm)

        report_id = db_with_company.insert_report(make_report(
            raw_text='The company reported strong revenue this quarter.',
            report_date='2024-09-30',
            source_type='edgar_10q',
            covering_period='2024-Q3',
        ))
        report = db_with_company.get_report(report_id)

        from interpreters.interpret_pipeline import extract_report
        from miner_types import ExtractionSummary
        summary = extract_report(report, db_with_company)

        assert isinstance(summary, ExtractionSummary)
        assert summary.keyword_gated == 1, "keyword_gated must be 1 when quarterly gate fires"
        assert summary.data_points_extracted == 0
        unextracted = db_with_company.get_unextracted_reports()
        assert not any(r['id'] == report_id for r in unextracted)

    def test_quarterly_keyword_gate_passes_when_match(self, db_with_company, monkeypatch):
        """Quarterly 10-Q with matching keyword passes gate; LLM is called."""
        import interpreters.interpret_pipeline as _ep
        from unittest.mock import MagicMock
        from miner_types import ExtractionResult

        monkeypatch.setattr(
            db_with_company, 'get_all_metric_keywords',
            lambda active_only=True: [{'phrase': 'bitcoin produced', 'metric_key': 'production_btc'}],
        )
        mock_llm = MagicMock()
        mock_llm.check_connectivity.return_value = True
        mock_llm.extract_quarterly_batch.return_value = {
            'production_btc': ExtractionResult(
                metric='production_btc', value=2100.0, unit='BTC', confidence=0.90,
                extraction_method='llm_test', source_snippet='bitcoin produced 2100',
                pattern_id='llm_test',
            ),
        }
        monkeypatch.setattr(_ep, '_get_llm_interpreter', lambda db: mock_llm)

        report_id = db_with_company.insert_report(make_report(
            raw_text='MARA bitcoin produced 2100 BTC in Q3 2024.',
            report_date='2024-09-30',
            source_type='edgar_10q',
            covering_period='2024-Q3',
        ))
        report = db_with_company.get_report(report_id)

        from interpreters.interpret_pipeline import extract_report
        summary = extract_report(report, db_with_company)

        assert summary.keyword_gated == 0, "gate must not fire when keyword matches"
        mock_llm.extract_quarterly_batch.assert_called_once()

    def test_quarterly_gate_fails_when_no_db_keywords(self, db_with_company, monkeypatch):
        """Quarterly extraction fails when no active metric keywords are configured."""
        import interpreters.interpret_pipeline as _ep
        from unittest.mock import MagicMock

        monkeypatch.setattr(
            db_with_company, 'get_all_metric_keywords',
            lambda active_only=True: [],
        )
        mock_llm = MagicMock()
        mock_llm.check_connectivity.return_value = True
        monkeypatch.setattr(_ep, '_get_llm_interpreter', lambda db: mock_llm)

        report_id = db_with_company.insert_report(make_report(
            raw_text='MARA bitcoin mined 900 BTC in Q3 2024. Hash rate 21 EH/s.',
            report_date='2024-09-30',
            source_type='edgar_10q',
            covering_period='2024-Q3',
        ))
        report = db_with_company.get_report(report_id)

        from interpreters.interpret_pipeline import extract_report
        summary = extract_report(report, db_with_company)

        assert summary.keyword_gated == 0
        assert summary.errors == 1
        mock_llm.extract_quarterly_batch.assert_not_called()

    def test_quarterly_review_is_deferred_while_monthly_reports_are_pending(
        self, db_with_company, monkeypatch
    ):
        """Low-confidence quarterly review stays out of the active queue until monthly docs catch up."""
        import interpreters.interpret_pipeline as _ep
        from unittest.mock import MagicMock
        from miner_types import ExtractionResult

        for month in ('2025-01-01', '2025-02-01', '2025-03-01'):
            db_with_company.insert_report(make_report(
                report_date=month,
                source_type='ir_press_release',
                raw_text='MARA mined bitcoin this month.',
            ))

        monkeypatch.setattr(
            db_with_company, 'get_all_metric_keywords',
            lambda active_only=True: [{'phrase': 'bitcoin produced', 'metric_key': 'production_btc'}],
        )
        mock_llm = MagicMock()
        mock_llm.check_connectivity.return_value = True
        mock_llm.extract_quarterly_batch.return_value = {
            'production_btc': ExtractionResult(
                metric='production_btc', value=2100.0, unit='BTC', confidence=0.60,
                extraction_method='llm_test', source_snippet='bitcoin produced 2100',
                pattern_id='llm_test',
            ),
        }
        monkeypatch.setattr(_ep, '_get_llm_interpreter', lambda db: mock_llm)

        report_id = db_with_company.insert_report(make_report(
            raw_text='MARA bitcoin produced 2100 BTC in Q1 2025.',
            report_date='2025-03-31',
            source_type='edgar_10q',
            covering_period='2025-Q1',
        ))
        report = db_with_company.get_report(report_id)

        from interpreters.interpret_pipeline import extract_report
        summary = extract_report(report, db_with_company)

        assert summary.review_flagged == 1
        assert db_with_company.get_review_items(status='PENDING') == []
        deferred = db_with_company.get_review_items(status='PENDING', include_inactive=True)
        assert len(deferred) == 1
        assert deferred[0]['precedence_state'] == 'deferred'

    def test_quarterly_8k_shareholder_letter_uses_quarterly_path(
        self, db_with_company, monkeypatch
    ):
        """Quarter-style 8-K shareholder letters should not enter the monthly queue."""
        import interpreters.interpret_pipeline as _ep
        from unittest.mock import MagicMock
        from miner_types import ExtractionResult

        for month in ('2025-07-01', '2025-08-01', '2025-09-01'):
            db_with_company.insert_report(make_report(
                report_date=month,
                source_type='ir_press_release',
                raw_text='MARA mined bitcoin this month.',
            ))

        monkeypatch.setattr(
            db_with_company, 'get_all_metric_keywords',
            lambda active_only=True: [
                {'phrase': 'bitcoin produced', 'metric_key': 'production_btc'},
                {'phrase': 'btc produced', 'metric_key': 'production_btc'},
            ],
        )
        mock_llm = MagicMock()
        mock_llm.check_connectivity.return_value = True
        mock_llm.extract_quarterly_batch.return_value = {
            'production_btc': ExtractionResult(
                metric='production_btc', value=1900.0, unit='BTC', confidence=0.60,
                extraction_method='llm_test', source_snippet='BTC Produced | 1,900',
                pattern_id='llm_test',
            ),
        }
        monkeypatch.setattr(_ep, '_get_llm_interpreter', lambda db: mock_llm)

        report_id = db_with_company.insert_report(make_report(
            raw_text='MARA Shareholder Letter Q3 2025. BTC Produced | 1,900',
            report_date='2025-11-04',
            source_type='edgar_8k',
            source_url='https://www.sec.gov/Archives/edgar/data/1507605/000150760525000026/q325shareholderletter.htm',
        ))
        report = db_with_company.get_report(report_id)

        from interpreters.interpret_pipeline import extract_report
        summary = extract_report(report, db_with_company)

        assert summary.review_flagged == 1
        mock_llm.extract_quarterly_batch.assert_called_once()
        active = db_with_company.get_review_items(status='PENDING')
        assert active == []
        deferred = db_with_company.get_review_items(status='PENDING', include_inactive=True)
        assert len(deferred) == 1
        assert deferred[0]['period'] == '2025-Q3'
        assert deferred[0]['time_grain'] == 'quarterly'
        assert deferred[0]['precedence_state'] == 'deferred'

    def test_quarterly_8k_results_press_release_uses_quarterly_path(
        self, db_with_company, monkeypatch
    ):
        """8-K earnings releases with quarter results should bypass the monthly path."""
        import interpreters.interpret_pipeline as _ep
        from unittest.mock import MagicMock
        from miner_types import ExtractionResult

        monkeypatch.setattr(
            db_with_company, 'get_all_metric_keywords',
            lambda active_only=True: [
                {'phrase': 'bitcoin produced', 'metric_key': 'production_btc'},
                {'phrase': 'produced', 'metric_key': 'production_btc'},
            ],
        )
        mock_llm = MagicMock()
        mock_llm.check_connectivity.return_value = True
        mock_llm.extract_quarterly_batch.return_value = {
            'production_btc': ExtractionResult(
                metric='production_btc', value=4242.0, unit='BTC', confidence=0.92,
                extraction_method='llm_test', source_snippet='during which we produced 4,242 bitcoin',
                pattern_id='llm_test',
            ),
        }
        monkeypatch.setattr(_ep, '_get_llm_interpreter', lambda db: mock_llm)

        report_id = db_with_company.insert_report(make_report(
            raw_text=(
                'Marathon Digital Holdings Reports Fourth Quarter and Fiscal Year 2023 Results. '
                'During the fourth quarter of 2023, we produced 4,242 bitcoin.'
            ),
            report_date='2024-02-28',
            source_type='edgar_8k',
            source_url='https://www.sec.gov/Archives/edgar/data/1507605/000149315224008232/ex99-1.htm',
        ))
        report = db_with_company.get_report(report_id)

        from interpreters.interpret_pipeline import extract_report
        summary = extract_report(report, db_with_company)

        assert summary.errors == 0
        mock_llm.extract_quarterly_batch.assert_called_once()
        points = db_with_company.query_data_points(ticker='MARA', metric='production_btc')
        assert any(p['period'] == '2023-Q4' and abs(p['value'] - 4242.0) < 0.01 for p in points)
        assert not any(p['period'] == '2023-12-01' for p in points)

    def test_quarterly_8k_financial_results_with_intervening_word(
        self, db_with_company, monkeypatch
    ):
        """Earnings 8-Ks titled 'Third Quarter YYYY Financial Results' are routed quarterly.

        RIOT Platforms uses 'Third Quarter 2025 Financial Results' (one word between
        year and 'Results'), which previously slipped through to the monthly path.
        """
        import interpreters.interpret_pipeline as _ep
        from unittest.mock import MagicMock
        from miner_types import ExtractionResult

        monkeypatch.setattr(
            db_with_company, 'get_all_metric_keywords',
            lambda active_only=True: [
                {'phrase': 'bitcoin produced', 'metric_key': 'production_btc'},
            ],
        )
        mock_llm = MagicMock()
        mock_llm.check_connectivity.return_value = True
        mock_llm.extract_quarterly_batch.return_value = {
            'production_btc': ExtractionResult(
                metric='production_btc', value=1406.0, unit='BTC', confidence=0.95,
                extraction_method='llm_test', source_snippet='Q3 total 1,406 bitcoin produced',
                pattern_id='llm_test',
            ),
        }
        monkeypatch.setattr(_ep, '_get_llm_interpreter', lambda db: mock_llm)

        report_id = db_with_company.insert_report(make_report(
            raw_text=(
                'Riot Platforms Reports Third Quarter 2025 Financial Results and Strategic Highlights. '
                'Total Q3 bitcoin produced: 1,406 BTC.'
            ),
            report_date='2025-10-30',
            source_type='edgar_8k',
            source_url='https://www.sec.gov/Archives/edgar/data/1167419/000110465925104461/riot-20251030xex99d1.htm',
        ))
        report = db_with_company.get_report(report_id)

        from interpreters.interpret_pipeline import extract_report
        summary = extract_report(report, db_with_company)

        assert summary.errors == 0
        mock_llm.extract_quarterly_batch.assert_called_once()
        points = db_with_company.query_data_points(ticker='MARA', metric='production_btc')
        assert any(p['period'] == '2025-Q3' and abs(p['value'] - 1406.0) < 0.01 for p in points)
        assert not any(p['period'].startswith('2025-10') for p in points)

    def test_quarterly_ir_press_release_fy_quarter_uses_quarterly_path(
        self, db_with_company, monkeypatch
    ):
        """IR quarterly earnings PRs (e.g. 'First Quarter FY2023') are routed quarterly.

        CLSK posts quarterly earnings on their IR site as ir_press_release.  These
        previously slipped through to the monthly path because _infer_quarterly_covering_period
        only inspected edgar_8k source types.
        """
        import interpreters.interpret_pipeline as _ep
        from unittest.mock import MagicMock
        from miner_types import ExtractionResult

        monkeypatch.setattr(
            db_with_company, 'get_all_metric_keywords',
            lambda active_only=True: [
                {'phrase': 'mined', 'metric_key': 'production_btc'},
            ],
        )
        mock_llm = MagicMock()
        mock_llm.check_connectivity.return_value = True
        mock_llm.extract_quarterly_batch.return_value = {
            'production_btc': ExtractionResult(
                metric='production_btc', value=1531.0, unit='BTC', confidence=0.93,
                extraction_method='llm_test', source_snippet='Mined 1,531 Bitcoin',
                pattern_id='llm_test',
            ),
        }
        monkeypatch.setattr(_ep, '_get_llm_interpreter', lambda db: mock_llm)

        report_id = db_with_company.insert_report(make_report(
            raw_text=(
                'CleanSpark Reports First Quarter FY2023 Financial Results '
                'February 9, 2023. Mined 1,531 Bitcoin, a 132% increase over same prior year period.'
            ),
            report_date='2023-02-09',
            source_type='ir_press_release',
            source_url='https://investors.cleanspark.com/news/news-details/2023/CleanSpark-Reports-First-Quarter-FY2023-Financial-Results-02-09-2023/default.aspx',
        ))
        report = db_with_company.get_report(report_id)

        from interpreters.interpret_pipeline import extract_report
        summary = extract_report(report, db_with_company)

        assert summary.errors == 0
        mock_llm.extract_quarterly_batch.assert_called_once()
        points = db_with_company.query_data_points(ticker='MARA', metric='production_btc')
        assert any(p['period'] == '2023-Q1' and abs(p['value'] - 1531.0) < 0.01 for p in points)
        assert not any(p['period'].startswith('2023-02') for p in points)


class TestBoilerplateStrippingBySourceType:
    """Verify extract_report strips IR/archive boilerplate and EDGAR footers before LLM."""

    def _make_llm_mock(self, monkeypatch, captured: dict):
        from unittest.mock import MagicMock
        from miner_types import ExtractionResult
        import interpreters.interpret_pipeline as _ep

        mock_llm = MagicMock()
        mock_llm.check_connectivity.return_value = True

        def _capture_batch(text, *args, **kwargs):
            captured['llm_text'] = text
            return {'production_btc': ExtractionResult(
                metric='production_btc', value=700.0, unit='BTC', confidence=0.9,
                extraction_method='llm_test', source_snippet='700 BTC', pattern_id='llm_test',
            )}

        mock_llm.extract_batch.side_effect = _capture_batch
        mock_llm.extract_quarterly_batch.side_effect = _capture_batch
        monkeypatch.setattr(_ep, '_get_llm_interpreter', lambda db: mock_llm)
        return mock_llm

    def test_archive_html_boilerplate_stripped_before_llm(
        self, db_with_company, monkeypatch
    ):
        """archive_html source type must have PR boilerplate stripped before LLM sees text."""
        captured = {}
        self._make_llm_mock(monkeypatch, captured)

        content = "Bitcoin Produced: 700 BTC\nAvg Hash Rate: 20.0 EH/s\n" * 5
        boilerplate = (
            "About MARA Holdings\n"
            "MARA is a leading digital asset technology company.\n"
            "Forward-Looking Statements\n"
            "This press release contains forward-looking statements.\n"
        )
        report_id = db_with_company.insert_report(make_report(
            raw_text=content + boilerplate,
            report_date='2024-09-01',
            source_type='archive_html',
        ))
        report = db_with_company.get_report(report_id)

        from interpreters.interpret_pipeline import extract_report
        extract_report(report, db_with_company)

        assert 'llm_text' in captured, "LLM must have been called"
        assert "About MARA Holdings" not in captured['llm_text']
        assert "Forward-Looking Statements" not in captured['llm_text']
        assert "700 BTC" in captured['llm_text']

class TestBoilerplateStrippingBySourceType2:
    """edgar_8k boilerplate stripping (moved here to avoid class-level fixture conflict)."""

    @pytest.fixture
    def db_with_company(self, db):
        db.insert_company({
            'ticker': 'MARA', 'name': 'MARA Holdings, Inc.',
            'tier': 1, 'ir_url': 'https://www.marathondh.com/news',
            'pr_base_url': 'https://www.marathondh.com',
            'cik': '0001437491', 'active': 1,
        })
        return db

    def _make_llm_mock(self, monkeypatch, captured: dict):
        from unittest.mock import MagicMock
        from miner_types import ExtractionResult
        import interpreters.interpret_pipeline as _ep

        mock_llm = MagicMock()
        mock_llm.check_connectivity.return_value = True

        def _capture_batch(text, *args, **kwargs):
            captured['llm_text'] = text
            return {'production_btc': ExtractionResult(
                metric='production_btc', value=700.0, unit='BTC', confidence=0.9,
                extraction_method='llm_test', source_snippet='700 BTC', pattern_id='llm_test',
            )}

        mock_llm.extract_batch.side_effect = _capture_batch
        mock_llm.extract_quarterly_batch.side_effect = _capture_batch
        monkeypatch.setattr(_ep, '_get_llm_interpreter', lambda db: mock_llm)
        return mock_llm

    def test_edgar_8k_signatures_stripped_before_llm(
        self, db_with_company, monkeypatch
    ):
        """edgar_8k source type must have SIGNATURES block stripped before LLM sees text."""
        captured = {}
        self._make_llm_mock(monkeypatch, captured)

        content = "Bitcoin Produced: 700 BTC\nHashrate: 20 EH/s\n" * 5
        signatures = (
            "SIGNATURES\n"
            "Pursuant to the requirements of the Securities Exchange Act of 1934, "
            "the registrant has duly caused this report to be signed.\n"
            "By: /s/ Fred Thiel\n"
        )
        report_id = db_with_company.insert_report(make_report(
            raw_text=content + signatures,
            report_date='2024-09-01',
            source_type='edgar_8k',
        ))
        report = db_with_company.get_report(report_id)

        from interpreters.interpret_pipeline import extract_report
        extract_report(report, db_with_company)

        assert 'llm_text' in captured, "LLM must have been called"
        assert "hereunto duly caused" not in captured['llm_text']
        assert "700 BTC" in captured['llm_text']


class TestFindQuarterlyTextWindow:
    """Unit tests for _find_quarterly_text_window."""

    def _call(self, text, budget=500):
        from interpreters.interpret_pipeline import _find_quarterly_text_window
        return _find_quarterly_text_window(text, budget)

    def test_start_fallback_when_no_markers(self):
        text = 'A' * 1000
        window, strategy = self._call(text, budget=200)
        assert strategy == 'start'
        assert window == 'A' * 200

    def test_mda_header_item7(self):
        prefix = 'COVER PAGE\n' * 50            # ~550 chars of front matter
        mda = 'Item 7. Management\'s Discussion\nWe mined 1,200 BTC this quarter.\n'
        text = prefix + mda + 'EXTRA' * 200
        window, strategy = self._call(text, budget=300)
        assert strategy == 'mda_header'
        assert 'Item 7' in window
        assert 'mined 1,200 BTC' in window

    def test_mda_header_item2_10q(self):
        prefix = 'Table of Contents\n' * 30
        mda = 'Item 2. Management\'s Discussion\nHashrate averaged 35 EH/s.\n'
        text = prefix + mda + 'EXTRA' * 200
        window, strategy = self._call(text, budget=300)
        assert strategy == 'mda_header'
        assert 'Item 2' in window

    def test_mda_header_managements_discussion_phrase(self):
        prefix = 'Risk Factors\n' * 40
        mda = "Management\u2019s Discussion and Analysis\nWe produced 900 BTC.\n"
        text = prefix + mda + 'X' * 500
        window, strategy = self._call(text, budget=300)
        assert strategy == 'mda_header'
        assert 'produced 900 BTC' in window

    def test_keyword_seek_when_no_mda_header(self):
        # prefix is short enough that keyword falls within budget after lookback
        prefix = 'General corporate information.\n' * 5   # ~155 chars, no Item 7/2
        data = 'During the quarter we achieved bitcoin production of 1,500 BTC.\n'
        text = prefix + data + 'Z' * 500
        window, strategy = self._call(text, budget=400)
        assert strategy == 'keyword_seek'
        assert 'bitcoin production' in window

    def test_mda_header_wins_over_earlier_keyword(self):
        # keyword appears before MD&A header — MD&A should still win
        prefix = 'hashrate was 10 EH/s in prior period.\n' * 5
        mda = 'Item 7. Management\'s Discussion\nCurrent quarter: 1,100 BTC mined.\n'
        text = prefix + mda + 'X' * 500
        window, strategy = self._call(text, budget=300)
        assert strategy == 'mda_header'
        assert 'Item 7' in window

    def test_window_does_not_exceed_budget(self):
        text = 'Item 7. MD&A\n' + 'data ' * 2000
        window, _ = self._call(text, budget=100)
        assert len(window) <= 100

    def test_lookback_included_in_window(self):
        # 200-char lookback before MD&A header should be included
        prefix = 'PRIOR CONTEXT ' * 20   # 280 chars
        mda = 'Item 7. MD&A\nProduction 800 BTC.\n'
        text = prefix + mda + 'X' * 500
        window, strategy = self._call(text, budget=500)
        assert strategy == 'mda_header'
        # The 200-char lookback means some of the prefix is in the window
        assert 'PRIOR CONTEXT' in window


class TestZeroExtractRouting:
    """
    When an LLM extraction pass succeeds (keyword gate passed, LLM responded)
    but produces zero data_points AND zero review items, the pipeline must
    insert review queue items with agreement_status='LLM_EMPTY' and increment
    summary.zero_extract_misses.
    """

    @pytest.fixture
    def db_with_riot(self, db):
        """db fixture with RIOT pre-seeded."""
        return db

    def test_zero_extract_monthly_routes_to_review_queue(self, db_with_riot, monkeypatch):
        """Monthly report: passes keyword gate, LLM returns all-None → review_queue entries."""
        from unittest.mock import MagicMock
        from miner_types import ExtractionResult
        import interpreters.interpret_pipeline as _ep

        mock_llm = MagicMock()
        mock_llm.check_connectivity.return_value = True
        # LLM returns None for all metrics (zero-extract scenario)
        mock_llm.extract_batch.return_value = {}
        monkeypatch.setattr(_ep, '_get_llm_interpreter', lambda db: mock_llm)

        report_id = db_with_riot.insert_report({
            'ticker': 'RIOT',
            'report_date': '2025-03-01',
            'published_date': None,
            'source_type': 'archive_html',
            'source_url': None,
            # Text passes keyword gate — contains 'bitcoin mined'
            'raw_text': 'RIOT bitcoin mined 0 BTC. No production this month.',
            'parsed_at': '2025-03-03T12:00:00',
        })
        report = db_with_riot.get_report(report_id)

        from interpreters.interpret_pipeline import extract_report
        from miner_types import ExtractionSummary
        summary = extract_report(report, db_with_riot)

        assert isinstance(summary, ExtractionSummary)
        assert summary.zero_extract_misses >= 1, "zero_extract_misses must be incremented"

        review_items = db_with_riot.get_review_items(ticker='RIOT')
        llm_empty_items = [i for i in review_items if i.get('agreement_status') == 'LLM_EMPTY']
        assert len(llm_empty_items) >= 1, "At least one LLM_EMPTY review item expected"

    def test_zero_extract_quarterly_routes_to_review_queue(self, db_with_riot, monkeypatch):
        """Quarterly report: passes keyword gate, LLM returns nothing → review_queue entries."""
        from unittest.mock import MagicMock
        import interpreters.interpret_pipeline as _ep

        mock_llm = MagicMock()
        mock_llm.check_connectivity.return_value = True
        mock_llm.extract_quarterly_batch.return_value = {}
        mock_llm._last_transport_error = False
        monkeypatch.setattr(_ep, '_get_llm_interpreter', lambda db: mock_llm)

        report_id = db_with_riot.insert_report({
            'ticker': 'RIOT',
            'report_date': '2025-01-01',
            'published_date': None,
            'source_type': 'edgar_10q',
            'source_url': None,
            'covering_period': '2025-Q1',
            'raw_text': (
                'RIOT bitcoin mined. During the quarter ended March 31, 2025, '
                'total bitcoin production was significant. Hash rate was 40 EH/s.'
            ),
            'parsed_at': '2025-04-15T12:00:00',
        })
        report = db_with_riot.get_report(report_id)

        from interpreters.interpret_pipeline import extract_report
        from miner_types import ExtractionSummary
        summary = extract_report(report, db_with_riot)

        assert isinstance(summary, ExtractionSummary)
        assert summary.zero_extract_misses >= 1, "zero_extract_misses must be incremented"

        review_items = db_with_riot.get_review_items(ticker='RIOT')
        llm_empty_items = [i for i in review_items if i.get('agreement_status') == 'LLM_EMPTY']
        assert len(llm_empty_items) >= 1, "At least one LLM_EMPTY review item expected"

    def test_zero_extract_not_triggered_when_data_points_stored(self, db_with_riot, monkeypatch):
        """When the LLM produces data_points, zero_extract routing must NOT fire."""
        from unittest.mock import MagicMock
        from miner_types import ExtractionResult
        import interpreters.interpret_pipeline as _ep

        mock_llm = MagicMock()
        mock_llm.check_connectivity.return_value = True
        mock_llm.extract_batch.return_value = {
            'production_btc': ExtractionResult(
                metric='production_btc', value=500.0, unit='BTC', confidence=0.95,
                extraction_method='llm', source_snippet='RIOT mined 500 BTC',
                pattern_id='llm',
            ),
        }
        monkeypatch.setattr(_ep, '_get_llm_interpreter', lambda db: mock_llm)

        report_id = db_with_riot.insert_report({
            'ticker': 'RIOT',
            'report_date': '2025-03-01',
            'published_date': None,
            'source_type': 'archive_html',
            'source_url': None,
            'raw_text': 'RIOT bitcoin mined 500 BTC in March 2025.',
            'parsed_at': '2025-03-03T12:00:00',
        })
        report = db_with_riot.get_report(report_id)

        from interpreters.interpret_pipeline import extract_report
        summary = extract_report(report, db_with_riot)

        assert summary.zero_extract_misses == 0, "zero_extract_misses must be 0 when data extracted"

    def test_zero_extract_not_triggered_when_keyword_gated(self, db_with_riot, monkeypatch):
        """Keyword-gated documents must NOT get LLM_EMPTY review items."""
        from unittest.mock import MagicMock
        import interpreters.interpret_pipeline as _ep

        mock_llm = MagicMock()
        mock_llm.check_connectivity.return_value = True
        mock_llm.extract_batch.return_value = {}
        monkeypatch.setattr(_ep, '_get_llm_interpreter', lambda db: mock_llm)

        report_id = db_with_riot.insert_report({
            'ticker': 'RIOT',
            'report_date': '2025-03-01',
            'published_date': None,
            'source_type': 'archive_html',
            'source_url': None,
            # No mining keywords — should be gated
            'raw_text': 'General corporate announcement. No mining related content here.',
            'parsed_at': '2025-03-03T12:00:00',
        })
        report = db_with_riot.get_report(report_id)

        from interpreters.interpret_pipeline import extract_report
        summary = extract_report(report, db_with_riot)

        assert summary.zero_extract_misses == 0
        assert summary.keyword_gated >= 1
