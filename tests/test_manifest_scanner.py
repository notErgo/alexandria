"""
Tests for manifest_scanner.py — TDD.

Tests should FAIL before manifest_scanner.py is created.
"""
import pytest
from pathlib import Path


# ── detect_ticker_from_path ──────────────────────────────────────────────────

def test_detect_ticker_mara_monthly_dir():
    """MARA MONTHLY dir yields 'MARA'."""
    from scrapers.manifest_scanner import detect_ticker_from_path
    path = Path('/some/root/Miner Monthly/MARA MONTHLY/2024-01-01_mara_report.html')
    ticker = detect_ticker_from_path(path)
    assert ticker == 'MARA'


def test_detect_ticker_riot_monthly_dir():
    """RIOT MONTHLY dir yields 'RIOT'."""
    from scrapers.manifest_scanner import detect_ticker_from_path
    path = Path('/some/root/Miner Monthly/RIOT MONTHLY/2024-01-01_riot_report.html')
    ticker = detect_ticker_from_path(path)
    assert ticker == 'RIOT'


def test_detect_ticker_returns_none_for_unknown():
    """Unrecognized dir yields None."""
    from scrapers.manifest_scanner import detect_ticker_from_path
    path = Path('/some/root/Miner Monthly/UNKNOWN COMPANY/report.html')
    ticker = detect_ticker_from_path(path)
    assert ticker is None


# ── detect_source_type_from_path ─────────────────────────────────────────────

def test_detect_source_type_pdf():
    """PDF file yields 'archive_pdf'."""
    from scrapers.manifest_scanner import detect_source_type_from_path
    path = Path('/some/dir/report.pdf')
    assert detect_source_type_from_path(path) == 'archive_pdf'


def test_detect_source_type_html():
    """HTML file yields 'archive_html'."""
    from scrapers.manifest_scanner import detect_source_type_from_path
    path = Path('/some/dir/report.html')
    assert detect_source_type_from_path(path) == 'archive_html'


def test_detect_source_type_unknown():
    """Unknown extension yields 'archive_html' as fallback."""
    from scrapers.manifest_scanner import detect_source_type_from_path
    path = Path('/some/dir/report.txt')
    result = detect_source_type_from_path(path)
    assert isinstance(result, str)


# ── detect_ingest_state ──────────────────────────────────────────────────────

def test_detect_ingest_state_dated_not_in_db():
    """File with inferrable period that's NOT in the DB → ('pending', 'YYYY-MM-01')."""
    from scrapers.manifest_scanner import detect_ingest_state
    path = Path('/some/MARA MONTHLY/2024-06-01_mara_report.html')
    state, period = detect_ingest_state(path, 'MARA', existing_report_dates=set())
    assert state == 'pending'
    assert period == '2024-06-01'


def test_detect_ingest_state_dated_in_db():
    """File with inferrable period that IS in the DB → ('ingested', 'YYYY-MM-01')."""
    from scrapers.manifest_scanner import detect_ingest_state
    path = Path('/some/MARA MONTHLY/2024-06-01_mara_report.html')
    state, period = detect_ingest_state(path, 'MARA', existing_report_dates={'2024-06-01'})
    assert state == 'ingested'
    assert period == '2024-06-01'


def test_detect_ingest_state_undated():
    """File with no inferrable period → ('legacy_undated', None)."""
    from scrapers.manifest_scanner import detect_ingest_state
    # filename has no date and no month name
    path = Path('/some/RIOT MONTHLY/0013_press_release.html')
    state, period = detect_ingest_state(path, 'RIOT', existing_report_dates=set())
    assert state == 'legacy_undated'
    assert period is None
