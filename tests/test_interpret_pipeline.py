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


@pytest.fixture
def registry():
    from interpreters.pattern_registry import PatternRegistry
    from config import CONFIG_DIR
    return PatternRegistry.load(CONFIG_DIR)


class TestExtractReport:
    def test_extract_report_stores_data_point(self, db_with_company, registry, monkeypatch):
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
        summary = extract_report(report, db_with_company, registry)

        rows = db_with_company.query_data_points(ticker='MARA', metric='production_btc')
        assert len(rows) >= 1
        assert any(abs(r['value'] - 700.0) < 0.01 for r in rows)

    def test_extract_report_analyst_protected(self, db_with_company, registry, monkeypatch):
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
        extract_report(report, db_with_company, registry)

        rows = db_with_company.query_data_points(ticker='MARA', metric='production_btc')
        assert len(rows) == 1
        assert abs(rows[0]['value'] - 999.0) < 0.01, "Analyst value must not be overwritten"

    def test_extract_report_marks_extracted_at(self, db_with_company, registry, monkeypatch):
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
        extract_report(report, db_with_company, registry)

        # Must no longer appear in unextracted list
        unextracted_after = db_with_company.get_unextracted_reports()
        assert not any(r['id'] == report_id for r in unextracted_after), \
            "Report should be marked as extracted after extract_report() runs"

    def test_extract_report_returns_summary(self, db_with_company, registry, monkeypatch):
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
        summary = extract_report(report, db_with_company, registry)

        assert isinstance(summary, ExtractionSummary)
        assert summary.reports_processed == 1

    def test_extract_report_empty_text_increments_errors(self, db_with_company, registry, monkeypatch):
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
        summary = extract_report(report, db_with_company, registry)

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
        self, db_with_company, registry, monkeypatch
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
        extract_report(report, db_with_company, registry)

        assert len(batch_called) == 1, "extract_batch must be called exactly once"
        assert len(extract_called) == 0, "per-metric extract() must not be called"

    def test_pipeline_falls_back_to_per_metric_when_batch_empty(
        self, db_with_company, registry, monkeypatch
    ):
        """When extract_batch returns {}, fallback calls extract_batch([metric]) per metric.

        Step 6 changed the per-metric fallback from extract() to extract_batch([metric])
        so that ExtractionRunConfig is forwarded uniformly. extract() must NOT be called.
        """
        import interpreters.interpret_pipeline as _ep

        batch_call_count = []
        extract_called = []

        def fake_batch(text, metrics, ticker=None, **kw):
            batch_call_count.append(len(metrics))
            return {}

        mock_llm = _make_mock_llm()
        mock_llm.extract_batch = fake_batch
        mock_llm.extract = lambda text, metric, **kw: extract_called.append(metric) or None
        mock_llm.extract_for_period.return_value = {}
        monkeypatch.setattr(_ep, '_get_llm_interpreter', lambda db: mock_llm)

        report_id = db_with_company.insert_report(make_report(
            raw_text='MARA bitcoin mined 700 BTC in September 2024. Hash rate 20 EH/s.',
            report_date='2024-09-01',
        ))
        report = db_with_company.get_report(report_id)

        from interpreters.interpret_pipeline import extract_report
        extract_report(report, db_with_company, registry)

        # Per-metric fallback calls extract_batch, NOT extract()
        assert len(extract_called) == 0, (
            "extract() must NOT be called in the fallback path; "
            "extract_batch([metric]) is used instead"
        )
        # 1 batch call + N per-metric fallback calls
        n_metrics = len(registry.metrics)
        assert len(batch_call_count) == 1 + n_metrics, (
            f"Expected 1 batch + {n_metrics} per-metric calls, got {len(batch_call_count)}"
        )

    def test_pipeline_batch_marks_report_extracted(
        self, db_with_company, registry, monkeypatch
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
        extract_report(report, db_with_company, registry)

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
        self, db_with_company, registry, monkeypatch
    ):
        """LLM_ONLY with confidence >= threshold → stored directly in data_points."""
        import interpreters.interpret_pipeline as _ep
        from config import CONFIDENCE_REVIEW_THRESHOLD

        # Empty registry so regex always returns nothing
        from interpreters.pattern_registry import PatternRegistry
        empty_registry = PatternRegistry(metrics={m: [] for m in registry.metrics})

        mock = self._make_llm_only_batch(value=700.0, confidence=CONFIDENCE_REVIEW_THRESHOLD)
        monkeypatch.setattr(_ep, '_get_llm_interpreter', lambda db: mock)

        report_id = db_with_company.insert_report(make_report(
            raw_text='MARA bitcoin mined 700 BTC in September 2024. Hash rate 20 EH/s.',
            report_date='2024-09-01',
        ))
        report = db_with_company.get_report(report_id)

        from interpreters.interpret_pipeline import extract_report
        summary = extract_report(report, db_with_company, empty_registry)

        rows = db_with_company.query_data_points(ticker='MARA', metric='production_btc')
        assert any(abs(r['value'] - 700.0) < 0.01 for r in rows), \
            "High-confidence LLM_ONLY must land in data_points"
        assert summary.data_points_extracted >= 1

    def test_llm_only_low_confidence_goes_to_review(
        self, db_with_company, registry, monkeypatch
    ):
        """LLM_ONLY with confidence < threshold → review_queue, not data_points."""
        import interpreters.interpret_pipeline as _ep
        from config import CONFIDENCE_REVIEW_THRESHOLD

        from interpreters.pattern_registry import PatternRegistry
        empty_registry = PatternRegistry(metrics={m: [] for m in registry.metrics})

        low_conf = max(0.0, CONFIDENCE_REVIEW_THRESHOLD - 0.1)
        mock = self._make_llm_only_batch(value=700.0, confidence=low_conf)
        monkeypatch.setattr(_ep, '_get_llm_interpreter', lambda db: mock)

        report_id = db_with_company.insert_report(make_report(
            raw_text='MARA bitcoin mined 700 BTC in September 2024. Hash rate 20 EH/s.',
            report_date='2024-09-01',
        ))
        report = db_with_company.get_report(report_id)

        from interpreters.interpret_pipeline import extract_report
        summary = extract_report(report, db_with_company, empty_registry)

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

    def test_gap_fill_disabled_when_llm_unavailable(self, db_with_company, registry, monkeypatch):
        """Gap fill must not be attempted when LLM is unavailable."""
        import interpreters.interpret_pipeline as _ep

        # LLM not available → _try_gap_fill should not be called
        monkeypatch.setattr(_ep, '_get_llm_interpreter', lambda db: None)
        gap_fill_called = []
        monkeypatch.setattr(_ep, '_try_gap_fill', lambda *a, **kw: gap_fill_called.append(1))

        report_id = db_with_company.insert_report(make_report(
            raw_text='MARA mined 700 BTC in September 2024. In August we mined 650 BTC.',
            report_date='2024-09-01',
        ))
        report = db_with_company.get_report(report_id)

        from interpreters.interpret_pipeline import extract_report
        extract_report(report, db_with_company, registry)
        assert len(gap_fill_called) == 0

    def test_gap_fill_skipped_when_main_pass_yields_zero_data_points(
        self, db_with_company, registry, monkeypatch
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
        summary = extract_report(report, db_with_company, registry)

        assert summary.data_points_extracted == 0
        assert len(gap_fill_called) == 0, (
            "Gap fill must not be called when main extraction yields 0 data points"
        )

    def test_gap_fill_skips_when_prior_period_fully_populated(
        self, db_with_company, registry, monkeypatch
    ):
        """Gap fill returns early if all metrics already have data for prior period."""
        import interpreters.interpret_pipeline as _ep

        mock_llm = _make_mock_llm()
        mock_llm.extract_batch.return_value = {}
        monkeypatch.setattr(_ep, '_get_llm_interpreter', lambda db: mock_llm)

        # Pre-populate all metrics for prior period (2024-08-01)
        for metric in registry.metrics:
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
        extract_report(report, db_with_company, registry)
        # extract_for_period must not be called — all prior-period slots already filled
        assert len(extract_for_period_calls) == 0

    def test_gap_fill_stores_data_at_prior_period(self, db_with_company, registry, monkeypatch):
        """Gap fill stores LLM result at prior period when slot is empty."""
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
        mock_llm.extract_for_period.return_value = {'production_btc': prior_result}
        monkeypatch.setattr(_ep, '_get_llm_interpreter', lambda db: mock_llm)

        report_id = db_with_company.insert_report(make_report(
            raw_text='MARA mined 700 BTC in September 2024. In August we mined 650 BTC.',
            report_date='2024-09-01',
        ))
        report = db_with_company.get_report(report_id)

        from interpreters.interpret_pipeline import extract_report
        extract_report(report, db_with_company, registry)

        rows = db_with_company.query_data_points(ticker='MARA', metric='production_btc')
        assert any(
            r['period'] == '2024-08-01' and abs(r['value'] - 650.0) < 0.01
            for r in rows
        ), "Gap fill must store the prior-period value at period=2024-08-01"

    def test_gap_fill_does_not_overwrite_existing_data(
        self, db_with_company, registry, monkeypatch
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
        mock_llm.extract_for_period.return_value = {'production_btc': prior_result}
        monkeypatch.setattr(_ep, '_get_llm_interpreter', lambda db: mock_llm)

        # Pre-insert prior-period data at a different sentinel value
        db_with_company.insert_data_point({
            'report_id': None, 'ticker': 'MARA', 'period': '2024-08-01',
            'metric': 'production_btc', 'value': 999.0, 'unit': 'BTC',
            'confidence': 0.95, 'extraction_method': 'llm_test',
            'source_snippet': 'existing',
        })

        report_id = db_with_company.insert_report(make_report(
            raw_text='MARA mined 700 BTC in September. In August we mined 650 BTC.',
            report_date='2024-09-01',
        ))
        report = db_with_company.get_report(report_id)

        from interpreters.interpret_pipeline import extract_report
        extract_report(report, db_with_company, registry)

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
        """LLM extract_batch receives only active metrics; inactive ones are excluded.

        Uses a registry that includes hashrate_eh so the test verifies DB-driven
        filtering, not just the absence of a pattern file.
        """
        import interpreters.interpret_pipeline as _ep
        from interpreters.pattern_registry import PatternRegistry
        from miner_types import ExtractionResult

        # Build a registry that explicitly includes hashrate_eh so it would reach
        # the LLM if DB filtering is not applied.
        fat_registry = PatternRegistry(metrics={
            'production_btc': [],
            'hodl_btc': [],
            'hashrate_eh': [],  # must be filtered out by active=0 in metric_schema
        })

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
        extract_report(report, db_with_company, fat_registry)

        assert 'metrics' in captured, "extract_batch was not called"
        assert 'hashrate_eh' not in captured['metrics'], (
            "Inactive metric hashrate_eh must not be sent to LLM"
        )
        assert 'production_btc' in captured['metrics'], (
            "Active metric production_btc must be present"
        )

    def test_active_metrics_fallback_to_registry_when_schema_empty(
        self, db_with_company, monkeypatch
    ):
        """When metric_schema is empty, _active_metric_keys falls back to registry keys."""
        import interpreters.interpret_pipeline as _ep
        from interpreters.pattern_registry import PatternRegistry

        fat_registry = PatternRegistry(metrics={
            'production_btc': [],
            'hodl_btc': [],
            'hashrate_eh': [],
        })

        # Remove all metric_schema rows to simulate an empty schema
        with db_with_company._get_connection() as conn:
            conn.execute("DELETE FROM metric_schema")

        captured = {}

        class _MockLLM2:
            _last_batch_summary = ''
            _last_call_meta = {}

            def check_connectivity(self):
                return True

            def extract_batch(self, text, metrics, ticker=None, **kwargs):
                captured['metrics'] = list(metrics)
                return {}

            def extract(self, text, metric):
                return None

            def extract_for_period(self, text, metrics, period, prior_period):
                return {}

        mock_llm = _MockLLM2()
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
        extract_report(report, db_with_company, fat_registry)

        assert 'metrics' in captured, "extract_batch was not called"
        assert len(captured['metrics']) > 0, (
            "Fallback must provide a non-empty metrics list from registry"
        )
        # With empty schema, all registry keys should be in the fallback
        assert 'hashrate_eh' in captured['metrics'], (
            "Fallback must include all registry metrics when schema is empty"
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

    @pytest.fixture
    def registry(self):
        from interpreters.pattern_registry import PatternRegistry
        from config import CONFIG_DIR
        return PatternRegistry.load(CONFIG_DIR)

    def test_8k_keyword_gate_skip_when_keywords_active(self, db_with_company, registry, monkeypatch):
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
        summary = extract_report(report, db_with_company, registry)

        assert isinstance(summary, ExtractionSummary)
        assert summary.keyword_gated == 1, "keyword_gated must be 1 when gate fires"
        assert summary.data_points_extracted == 0, "No data_points should be stored"
        # Report must be marked extracted (so it is not retried forever)
        unextracted = db_with_company.get_unextracted_reports()
        assert not any(r['id'] == report_id for r in unextracted)

    def test_8k_keyword_gate_uses_hardcoded_fallback_when_no_db_keywords(self, db_with_company, registry, monkeypatch):
        """Gate fires using hardcoded production phrases even when DB has no metric keywords.

        The gate is never bypassed — it always uses at least _PRODUCTION_GATE_PHRASES.
        A genuine production report must pass regardless of DB keyword state.
        """
        import interpreters.interpret_pipeline as _ep

        # No metric keywords in DB — gate must still fire (and pass for production text)
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
        summary = extract_report(report, db_with_company, registry)

        assert summary.keyword_gated == 0, "production report must pass gate even with no DB keywords"
        assert summary.reports_processed == 1

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

    def test_quarterly_keyword_gate_fires_when_no_match(self, db_with_company, registry, monkeypatch):
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
        summary = extract_report(report, db_with_company, registry)

        assert isinstance(summary, ExtractionSummary)
        assert summary.keyword_gated == 1, "keyword_gated must be 1 when quarterly gate fires"
        assert summary.data_points_extracted == 0
        unextracted = db_with_company.get_unextracted_reports()
        assert not any(r['id'] == report_id for r in unextracted)

    def test_quarterly_keyword_gate_passes_when_match(self, db_with_company, registry, monkeypatch):
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
        summary = extract_report(report, db_with_company, registry)

        assert summary.keyword_gated == 0, "gate must not fire when keyword matches"
        mock_llm.extract_quarterly_batch.assert_called_once()

    def test_quarterly_gate_uses_hardcoded_fallback_when_no_db_keywords(self, db_with_company, registry, monkeypatch):
        """Quarterly gate uses hardcoded production phrases when DB has no metric keywords.

        The gate is never bypassed — production reports with tight phrases must pass.
        """
        import interpreters.interpret_pipeline as _ep
        from unittest.mock import MagicMock
        from miner_types import ExtractionResult

        monkeypatch.setattr(
            db_with_company, 'get_all_metric_keywords',
            lambda active_only=True: [],
        )
        mock_llm = MagicMock()
        mock_llm.check_connectivity.return_value = True
        mock_llm.extract_quarterly_batch.return_value = {
            'production_btc': ExtractionResult(
                metric='production_btc', value=900.0, unit='BTC', confidence=0.85,
                extraction_method='llm_test', source_snippet='bitcoin mined 900 BTC',
                pattern_id='llm_test',
            ),
        }
        monkeypatch.setattr(_ep, '_get_llm_interpreter', lambda db: mock_llm)

        report_id = db_with_company.insert_report(make_report(
            raw_text='MARA bitcoin mined 900 BTC in Q3 2024. Hash rate 21 EH/s.',
            report_date='2024-09-30',
            source_type='edgar_10q',
            covering_period='2024-Q3',
        ))
        report = db_with_company.get_report(report_id)

        from interpreters.interpret_pipeline import extract_report
        summary = extract_report(report, db_with_company, registry)

        assert summary.keyword_gated == 0, "production report must pass gate even with no DB keywords"
        mock_llm.extract_quarterly_batch.assert_called_once()


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
        self, db_with_company, registry, monkeypatch
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
        extract_report(report, db_with_company, registry)

        assert 'llm_text' in captured, "LLM must have been called"
        assert "About MARA Holdings" not in captured['llm_text']
        assert "Forward-Looking Statements" not in captured['llm_text']
        assert "700 BTC" in captured['llm_text']

class TestPerMetricFallbackGate:
    """Per-metric fallback loop is skipped when both LLM batch and regex find nothing."""

    @pytest.fixture
    def db_with_company(self, db):
        db.insert_company({
            'ticker': 'MARA', 'name': 'MARA Holdings, Inc.',
            'tier': 1, 'ir_url': 'https://www.marathondh.com/news',
            'pr_base_url': 'https://www.marathondh.com',
            'cik': '0001437491', 'active': 1,
        })
        return db

    def test_per_metric_fallback_skipped_when_regex_also_empty(
        self, db_with_company, registry, monkeypatch
    ):
        """When LLM batch returns empty AND regex found nothing, per-metric calls must not fire.

        Both LLM and regex failing on the same document means there is nothing to
        extract. Running N individual LLM calls after that is pure waste.
        """
        import interpreters.interpret_pipeline as _ep

        call_log = []

        class _BatchEmptyLLM:
            _last_call_meta = {}
            _last_batch_summary = ''

            def extract_batch(self, text, metrics, **kw):
                call_log.append(('batch', list(metrics)))
                return {}

            def extract(self, *a, **kw):
                return None

            def extract_historical_periods(self, *a, **kw):
                return {}

            def check_connectivity(self):
                return True

        monkeypatch.setattr(_ep, '_get_llm_interpreter', lambda db: _BatchEmptyLLM())

        # Text passes keyword gate (contains "bitcoin") but has no numeric production
        # figures — regex will find zero matches.
        report_id = db_with_company.insert_report(make_report(
            raw_text=(
                'MARA Holdings announces board changes. Hash rate data was not disclosed. '
                'Bitcoin production figures are unavailable for this filing period.'
            ),
            report_date='2018-06-01',
        ))
        report = db_with_company.get_report(report_id)

        from interpreters.interpret_pipeline import extract_report
        extract_report(report, db_with_company, registry)

        # Only the single batch call should have been made; no per-metric calls.
        batch_calls = [c for c in call_log if c[0] == 'batch']
        assert len(batch_calls) == 1, (
            f"Expected exactly 1 batch call (the primary attempt), got {len(batch_calls)}: {batch_calls}"
        )

    def test_per_metric_fallback_runs_when_regex_has_hits(
        self, db_with_company, registry, monkeypatch
    ):
        """When LLM batch returns empty but regex found something, per-metric fallback fires."""
        import interpreters.interpret_pipeline as _ep

        call_log = []

        class _BatchEmptyLLM:
            _last_call_meta = {}
            _last_batch_summary = ''

            def extract_batch(self, text, metrics, **kw):
                call_log.append(('batch', list(metrics)))
                return {}

            def extract(self, *a, **kw):
                return None

            def extract_historical_periods(self, *a, **kw):
                return {}

            def check_connectivity(self):
                return True

        monkeypatch.setattr(_ep, '_get_llm_interpreter', lambda db: _BatchEmptyLLM())

        # Text with a real production figure — regex will match production_btc.
        report_id = db_with_company.insert_report(make_report(
            raw_text='MARA mined 700 BTC in September 2024. Bitcoin production was strong.',
            report_date='2024-09-01',
        ))
        report = db_with_company.get_report(report_id)

        from interpreters.interpret_pipeline import extract_report
        extract_report(report, db_with_company, registry)

        batch_calls = [c for c in call_log if c[0] == 'batch']
        # Batch (1) + per-metric fallback (N metrics) must all have fired.
        assert len(batch_calls) > 1, (
            "Per-metric fallback must run when regex found hits but LLM batch was empty"
        )


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
        self, db_with_company, registry, monkeypatch
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
        extract_report(report, db_with_company, registry)

        assert 'llm_text' in captured, "LLM must have been called"
        assert "hereunto duly caused" not in captured['llm_text']
        assert "700 BTC" in captured['llm_text']
