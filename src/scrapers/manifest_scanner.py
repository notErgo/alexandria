"""
Archive manifest scanner.

Walks the archive directory (OffChain/Miner/Miner Monthly/), upserts every
discovered file into asset_manifest, and returns a ScanResult summary.
"""
import logging
from datetime import date
from pathlib import Path
from typing import Optional

from miner_types import ScanResult

log = logging.getLogger('miners.scrapers.manifest_scanner')

# Known tickers recognized in directory names
_KNOWN_TICKERS = [
    "MARA", "RIOT", "CLSK", "CORZ", "BITF", "BTBT", "CIFR",
    "HIVE", "HUT8", "ARBK", "SDIG", "WULF", "IREN",
]


def detect_ticker_from_path(path: Path) -> Optional[str]:
    """Detect ticker by scanning path parts for '<TICKER> MONTHLY' pattern."""
    path_str = str(path).upper()
    for ticker in _KNOWN_TICKERS:
        if f"{ticker} MONTHLY" in path_str or f"{ticker}_MONTHLY" in path_str:
            return ticker
    return None


def detect_source_type_from_path(path: Path) -> str:
    """Detect source_type from file extension."""
    suffix = path.suffix.lower()
    if suffix == '.pdf':
        return 'archive_pdf'
    return 'archive_html'


def detect_ingest_state(
    path: Path,
    ticker: str,
    existing_report_dates: set,
) -> tuple:
    """Determine ingest_state and period for a file.

    Returns:
        ('ingested', 'YYYY-MM-01')   if period detected AND in existing_report_dates
        ('pending', 'YYYY-MM-01')    if period detected AND NOT in existing_report_dates
        ('legacy_undated', None)     if no period can be inferred
    """
    from scrapers.archive_ingestor import infer_period_from_filename

    period_date: Optional[date] = infer_period_from_filename(str(path), ticker, read_body=False)

    if period_date is None:
        return ('legacy_undated', None)

    period_str = period_date.strftime('%Y-%m-01')
    if period_str in existing_report_dates:
        return ('ingested', period_str)
    return ('pending', period_str)


def scan_archive_directory(archive_root: Path, db) -> ScanResult:
    """Walk archive_root/Miner Monthly/ and upsert manifest entries.

    Args:
        archive_root: Path to the root of the Miner archive (OffChain/Miner/)
        db: MinerDB instance

    Returns:
        ScanResult with counts
    """
    import time
    start = time.time()
    miner_monthly = archive_root / "Miner Monthly"
    if not miner_monthly.exists():
        log.warning("Archive directory not found: %s", miner_monthly)
        return ScanResult()

    log.info("Scanning archive directory: %s", miner_monthly)

    result = ScanResult()
    tickers_seen: set = set()

    # Pre-fetch all existing report dates per ticker for O(1) lookup
    existing_dates_by_ticker: dict = {}

    for path in sorted(miner_monthly.rglob('*')):
        if not path.is_file():
            continue
        if path.name.startswith('.'):
            continue
        if path.suffix.lower() not in ('.pdf', '.html'):
            continue

        result.total_found += 1

        ticker = detect_ticker_from_path(path)
        if ticker is None:
            log.debug("Could not detect ticker for: %s", path)
            result.failed += 1
            continue

        tickers_seen.add(ticker)

        # Lazy-load existing report dates for this ticker
        if ticker not in existing_dates_by_ticker:
            try:
                reports = db.get_all_reports_for_extraction(ticker=ticker)
                existing_dates_by_ticker[ticker] = {
                    r['report_date'] for r in reports
                }
            except Exception as e:
                log.error("Failed to fetch reports for %s: %s", ticker, e)
                existing_dates_by_ticker[ticker] = set()

        source_type = detect_source_type_from_path(path)
        state, period = detect_ingest_state(path, ticker, existing_dates_by_ticker[ticker])

        # Build manifest entry
        entry = {
            'ticker': ticker,
            'period': period,
            'source_type': source_type,
            'file_path': str(path),
            'filename': path.name,
            'ingest_state': state,
        }

        try:
            manifest_id = db.upsert_asset_manifest(entry)

            if state == 'ingested':
                result.already_ingested += 1
                # Try to link to existing report
                if period:
                    report = db.get_report_by_ticker_date(ticker, period)
                    if report:
                        try:
                            db.link_manifest_to_report(manifest_id, report['id'])
                        except Exception as e:
                            log.warning("Failed to link manifest %d to report: %s", manifest_id, e)
            elif state == 'pending':
                result.newly_discovered += 1
            elif state == 'legacy_undated':
                result.legacy_undated += 1

        except Exception as e:
            log.error("Failed to upsert manifest for %s: %s", path, e)
            result.failed += 1

    result.tickers_scanned = sorted(tickers_seen)
    elapsed = time.time() - start
    log.info(
        "Scan complete in %.1fs: total=%d ingested=%d new=%d legacy=%d failed=%d tickers=%s",
        elapsed, result.total_found, result.already_ingested,
        result.newly_discovered, result.legacy_undated,
        result.failed, result.tickers_scanned,
    )
    return result
