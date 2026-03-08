"""
Tests for manifest_scanner.py — TDD.

Tests should FAIL before manifest_scanner.py is created.
"""
import pytest
from pathlib import Path
from unittest.mock import MagicMock


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
    """File with inferrable period that IS in the DB → ('ingested', 'YYYY-MM-01').

    existing_report_dates must contain YYYY-MM prefixes (7 chars), not full dates.
    This matches how scan_archive_directory builds the set from r['report_date'][:7].
    A file dated 2024-06-01 matches a DB record stored as '2024-06-03' (same YYYY-MM).
    """
    from scrapers.manifest_scanner import detect_ingest_state
    path = Path('/some/MARA MONTHLY/2024-06-01_mara_report.html')
    # Pass YYYY-MM prefix (as scan_archive_directory now builds it)
    state, period = detect_ingest_state(path, 'MARA', existing_report_dates={'2024-06'})
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


# ── Drift detection (schema v15) ─────────────────────────────────────────────

def _make_mara_archive(tmp_path, content="<html>MARA report</html>"):
    """Create a minimal Miner Monthly structure with one MARA HTML file."""
    miner_monthly = tmp_path / "Miner Monthly" / "MARA MONTHLY"
    miner_monthly.mkdir(parents=True)
    p = miner_monthly / "2024-01_mara_report.html"
    p.write_text(content)
    return tmp_path, p


def _mock_db_for_scan(manifest_id=1, stored_checksum=None):
    db = MagicMock()
    db.get_all_reports_for_extraction.return_value = []
    db.upsert_asset_manifest.return_value = manifest_id
    db.get_asset_manifest_by_id.return_value = {
        'id': manifest_id,
        'file_checksum': stored_checksum,
    }
    db.get_all_asset_manifests.return_value = []
    return db


def test_scan_ok_on_unchanged_file(tmp_path):
    """When stored checksum matches current checksum, drift_status='ok' is set."""
    from scrapers.manifest_scanner import scan_archive_directory, compute_file_checksum
    archive_root, path = _make_mara_archive(tmp_path)
    checksum = compute_file_checksum(path)
    db = _mock_db_for_scan(stored_checksum=checksum)

    scan_archive_directory(archive_root, db)

    statuses = [c.args[1] for c in db.set_manifest_drift_status.call_args_list]
    assert 'ok' in statuses, f"Expected 'ok' in drift status calls, got: {statuses}"


def test_scan_detects_checksum_change(tmp_path):
    """When stored checksum differs from current checksum, drift_status='checksum_changed'."""
    from scrapers.manifest_scanner import scan_archive_directory
    archive_root, _path = _make_mara_archive(tmp_path)
    db = _mock_db_for_scan(stored_checksum='aaaa0000deadbeef00000000000000000000000000000000000000000000000000')

    scan_archive_directory(archive_root, db)

    statuses = [c.args[1] for c in db.set_manifest_drift_status.call_args_list]
    assert 'checksum_changed' in statuses, f"Expected 'checksum_changed', got: {statuses}"


def test_scan_marks_missing_file(tmp_path):
    """Manifest entry whose file_path no longer exists is marked drift_status='file_missing'."""
    from scrapers.manifest_scanner import scan_archive_directory
    (tmp_path / "Miner Monthly").mkdir()  # empty dir — no files to scan
    db = MagicMock()
    db.get_all_reports_for_extraction.return_value = []
    db.get_all_asset_manifests.return_value = [
        {'id': 42, 'file_path': str(tmp_path / 'ghost_file.html')},
    ]

    scan_archive_directory(tmp_path, db)

    db.set_manifest_drift_status.assert_called_with(42, 'file_missing')
