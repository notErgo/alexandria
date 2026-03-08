"""Tests for shared dataclasses and enums."""
from datetime import date, datetime
from miner_types import ExtractionResult, Metric, ReviewStatus, DataPoint, IngestSummary


def test_extraction_result_has_confidence_field():
    result = ExtractionResult(
        metric='production_btc',
        value=450.0,
        unit='BTC',
        confidence=0.92,
        extraction_method='prod_btc_0',
        source_snippet='mined 450 BTC',
        pattern_id='prod_btc_0',
    )
    assert hasattr(result, 'confidence')
    assert isinstance(result.confidence, float)


def test_metric_enum_has_thirteen_members():
    """Metric enum mirrors metric_schema DB seed — 13 total metrics."""
    assert len(list(Metric)) == 13


def test_review_status_pending_is_default_string():
    assert ReviewStatus.PENDING.name == 'PENDING'


def test_ingest_summary_defaults_to_zero():
    summary = IngestSummary()
    assert summary.reports_ingested == 0
    assert summary.data_points_extracted == 0
    assert summary.review_flagged == 0
    assert summary.errors == 0


# ── New type tests for Phase II ──────────────────────────────────────────────

def test_ingest_state_values():
    """IngestState enum has 5 values and PENDING == 'pending'."""
    from miner_types import IngestState
    assert len(list(IngestState)) == 5
    assert IngestState.PENDING == 'pending'


def test_cell_state_count():
    """CellState enum has 8 values including DATA_QUARTERLY added in Phase 2."""
    from miner_types import CellState
    assert len(list(CellState)) == 8
    assert set(CellState) == {
        CellState.DATA, CellState.DATA_QUARTERLY, CellState.REVIEW_PENDING,
        CellState.PARSE_FAILED, CellState.EXTRACT_FAILED, CellState.NO_DOCUMENT,
        CellState.SCRAPER_ERROR, CellState.ANALYST_GAP,
    }


class TestNewTypes:

    def test_regime_cadence_values(self):
        from miner_types import RegimeCadence
        assert set(v.value for v in RegimeCadence) == {'monthly', 'quarterly'}

    def test_scrape_status_values(self):
        from miner_types import ScrapeStatus
        expected = {'never_run', 'probing', 'probe_ok', 'probe_failed',
                    'js_heavy', 'ok', 'error', 'running'}
        assert set(v.value for v in ScrapeStatus) == expected

    def test_regime_window_dataclass_fields(self):
        from miner_types import RegimeWindow, RegimeCadence
        rw = RegimeWindow(ticker='MARA', cadence=RegimeCadence.MONTHLY,
                          start_date='2020-10-01', end_date=None, notes='')
        assert rw.ticker == 'MARA'
        assert rw.cadence == RegimeCadence.MONTHLY
        assert rw.end_date is None

    def test_scrape_job_dataclass_fields(self):
        from miner_types import ScrapeJob
        job = ScrapeJob(id=1, ticker='MARA', mode='historic',
                        status='pending', created_at='2026-01-01')
        assert job.id == 1
        assert job.ticker == 'MARA'
        assert job.mode == 'historic'
        assert job.started_at is None
        assert job.completed_at is None
        assert job.error_msg is None

    def test_metric_schema_def_dataclass_fields(self):
        from miner_types import MetricSchemaDef
        m = MetricSchemaDef(key='production_btc', label='BTC Produced',
                            unit='BTC', sector='BTC-miners',
                            has_extraction_pattern=True, analyst_defined=False)
        assert m.key == 'production_btc'
        assert m.has_extraction_pattern is True
        assert m.id is None


def test_scan_result_defaults():
    """ScanResult has correct default field values."""
    from miner_types import ScanResult
    sr = ScanResult()
    assert sr.total_found == 0
    assert sr.already_ingested == 0
    assert sr.newly_discovered == 0
    assert sr.legacy_undated == 0
    assert sr.failed == 0
    assert sr.tickers_scanned == []


def test_text_section_fields():
    """TextSection dataclass has the required fields."""
    from miner_types import TextSection
    ts = TextSection(name='full_text', text='hello', char_start=0, char_end=5)
    assert ts.name == 'full_text'
    assert ts.text == 'hello'
    assert ts.char_start == 0
    assert ts.char_end == 5


def test_parse_result_fields():
    """ParseResult dataclass has the required fields."""
    from miner_types import ParseResult, TextSection
    pr = ParseResult(
        text='hello world',
        sections=[TextSection(name='full_text', text='hello world', char_start=0, char_end=11)],
        parse_quality='text_ok',
        parser_used='press_release_html',
        page_count=0,
    )
    assert pr.text == 'hello world'
    assert len(pr.sections) == 1
    assert pr.parse_quality == 'text_ok'
    assert pr.parser_used == 'press_release_html'
    assert pr.page_count == 0
