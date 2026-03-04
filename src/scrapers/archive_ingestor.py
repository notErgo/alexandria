"""
Archive ingestor: parses historical PDFs and HTMLs from OffChain/Miner/.

Walks the archive directory recursively, identifies production reports by
filename pattern, infers ticker and period from path/filename, extracts text,
runs the extraction pipeline, and writes results to MinerDB.
"""
import re
import logging
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Optional

log = logging.getLogger('miners.scrapers.archive_ingestor')

# Month name → integer (for filename parsing)
MONTH_MAP: dict = {
    "january": 1, "february": 2, "march": 3, "april": 4,
    "may": 5, "june": 6, "july": 7, "august": 8,
    "september": 9, "october": 10, "november": 11, "december": 12,
}

# Tickers recognized in directory names (e.g. "MARA MONTHLY")
_KNOWN_TICKERS = [
    "MARA", "RIOT", "CLSK", "CORZ", "BITF", "BTBT", "CIFR",
    "HIVE", "HUT8", "ARBK", "SDIG", "WULF", "IREN",
]

_ISO_DATE_PATTERN = re.compile(r"^(\d{4})-(\d{2})-\d{2}")
_ISO_DATE_ANYWHERE = re.compile(r"(\d{4})-(\d{2})-\d{2}")
_MONTH_NAME_PATTERN = re.compile(
    r"(january|february|march|april|may|june|july|august|"
    r"september|october|november|december)\s+(\d{4})",
    re.IGNORECASE,
)
_PRODUCTION_KEYWORDS = re.compile(
    r"bitcoin|production|announces|monthly|mining.operations",
    re.IGNORECASE,
)
_QUARTERLY_PATTERN = re.compile(r"(?i)10-[qk]\b")

# Text body patterns for period inference — tried in order, first match wins.
# Only the first 3000 chars of the document are scanned to avoid false positives.
_TEXT_PERIOD_PATTERNS = [
    # "for the month of January 2025" / "during January 2025" / "in January 2025"
    re.compile(
        r"(?i)(?:for\s+the\s+month\s+of|during|in)\s+"
        r"(january|february|march|april|may|june|july|august|"
        r"september|october|november|december)\s+(\d{4})",
    ),
    # "January 2025 production" / "January 2025 bitcoin" / "January 2025 results"
    re.compile(
        r"(?i)(january|february|march|april|may|june|july|august|"
        r"september|october|november|december)\s+(\d{4})\s+"
        r"(?:production|mining|bitcoin|btc|results)",
    ),
    # Generic month+year near the opening of the document
    re.compile(
        r"(?i)(january|february|march|april|may|june|july|august|"
        r"september|october|november|december)\s+(\d{4})",
    ),
]


def infer_period_from_filename(
    path: str, ticker: str = None, *, read_body: bool = False
) -> Optional[date]:
    """
    Infer the reporting period from a file path.

    Four strategies tried in order:
    1. ISO date prefix (anchored on basename): "2024-09-03_..."  → date(2024, 9, 1)
    2. ISO date anywhere in basename: "10-Q 2025-11-04.pdf"     → date(2025, 11, 1)
    3. Month name anywhere in basename: "...September 2024..."  → date(2024, 9, 1)
    4. Body text (HTML only, read_body=True): read first 3000 chars and call
       infer_period_from_text(). Only fires when strategies 1–3 all return None.

    The `ticker` param is accepted for backward compatibility but not used.
    """
    import os
    filename = os.path.basename(path)

    m = _ISO_DATE_PATTERN.match(filename)
    if m:
        year, month = int(m.group(1)), int(m.group(2))
        return date(year, month, 1)

    m = _ISO_DATE_ANYWHERE.search(filename)
    if m:
        year, month = int(m.group(1)), int(m.group(2))
        return date(year, month, 1)

    m = _MONTH_NAME_PATTERN.search(filename)
    if m:
        month_str, year_str = m.group(1).lower(), m.group(2)
        month = MONTH_MAP.get(month_str)
        if month:
            return date(int(year_str), month, 1)

    # Strategy 4: body text (HTML files only, when read_body=True).
    # Parses HTML to extract visible text before sampling 3000 chars — raw HTML
    # markup can push the period reference past 3000 chars even when it appears
    # near the top of the visible document.
    if read_body and path.lower().endswith(".html"):
        try:
            from bs4 import BeautifulSoup
            with open(path, encoding="utf-8", errors="replace") as f:
                soup = BeautifulSoup(f, "lxml")
            parsed_text = soup.get_text(separator=" ", strip=True)
            return infer_period_from_text(parsed_text)
        except OSError as e:
            log.warning("Strategy 4: could not read %s: %s", path, e)
        except Exception as e:
            log.warning("Strategy 4: failed to parse HTML %s: %s", path, e)

    return None


def infer_ticker_from_path(path: str) -> Optional[str]:
    """
    Infer ticker from directory name by looking for '<TICKER> MONTHLY' pattern.
    """
    for ticker in _KNOWN_TICKERS:
        if f"{ticker} MONTHLY" in path or f"{ticker}_MONTHLY" in path.upper():
            return ticker
    return None


def is_production_filename(filename: str) -> bool:
    """Return True if the filename suggests a monthly production report."""
    return bool(_PRODUCTION_KEYWORDS.search(filename))


def is_quarterly_filing(filename: str) -> bool:
    """Return True if the filename indicates a 10-Q or 10-K SEC filing."""
    return bool(_QUARTERLY_PATTERN.search(filename))


def infer_period_from_text(text: str) -> Optional[date]:
    """
    Scan the opening 3000 chars of a document for an explicit month+year reference.

    Tries patterns in order of specificity — "for the month of X YYYY" is most
    reliable; bare "Month YYYY" is the fallback. Returns the first match found,
    or None if no match.
    """
    sample = text[:3000]
    for pattern in _TEXT_PERIOD_PATTERNS:
        m = pattern.search(sample)
        if m:
            month = MONTH_MAP.get(m.group(1).lower())
            if month:
                return date(int(m.group(2)), month, 1)
    return None


def extract_quarterly_months(text: str) -> list:
    """
    Find all month+year references in a quarterly filing's full text.

    Returns a sorted list of unique dates within a window:
    - Lower bound: 24 months ago (excludes old comparative-period references)
    - Upper bound: 6 months from today (excludes bond maturity dates, lease
      terms, and other far-future financial obligations that appear in 10-Q text)
    """
    from datetime import date as date_cls
    today = date_cls.today()
    lower = date_cls(today.year - 2, today.month, 1)
    # 6 months ahead: compute by adding months manually (no timedelta for months)
    upper_month = today.month + 6
    upper_year = today.year + (upper_month - 1) // 12
    upper_month = ((upper_month - 1) % 12) + 1
    upper = date_cls(upper_year, upper_month, 1)

    found: set = set()
    for m in _MONTH_NAME_PATTERN.finditer(text):
        month = MONTH_MAP.get(m.group(1).lower())
        if not month:
            continue
        try:
            d = date_cls(int(m.group(2)), month, 1)
        except ValueError:
            continue
        if lower <= d <= upper:
            found.add(d)
    return sorted(found)


def _extract_quarterly_data_points(text: str, registry, report_id: int, ticker: str, periods: list) -> dict:
    """
    For each period in a quarterly filing, find a text window around that month's
    header and run extraction within that window.

    Returns dict mapping period (date) → list of ExtractionResult.
    """
    from extractors.extractor import extract_all

    results_by_period: dict = {}
    for period_date in periods:
        month_name = period_date.strftime("%B %Y")  # e.g., "September 2025"
        idx = text.lower().find(month_name.lower())
        if idx == -1:
            continue
        # 200 chars before + 1300 chars after the month header
        window = text[max(0, idx - 200): idx + 1300]
        period_results = []
        for metric, patterns in registry.metrics.items():
            period_results.extend(extract_all(window, patterns, metric))
        results_by_period[period_date] = period_results
    return results_by_period


def _parse_pdf(path: str) -> str:
    """Extract all text from a PDF using pdfplumber. Returns '' on failure."""
    try:
        import pdfplumber
        with pdfplumber.open(path) as pdf:
            return "\n".join(page.extract_text() or "" for page in pdf.pages)
    except Exception as e:
        log.error("Failed to parse PDF %s: %s", path, e, exc_info=True)
        return ""


def _parse_html(path: str) -> str:
    """Extract body text from an HTML file using BeautifulSoup. Returns '' on failure."""
    try:
        from bs4 import BeautifulSoup
        with open(path, encoding="utf-8", errors="replace") as f:
            soup = BeautifulSoup(f, "lxml")
        return soup.get_text(separator=" ", strip=True)
    except Exception as e:
        log.error("Failed to parse HTML %s: %s", path, e, exc_info=True)
        return ""


def _build_html_covered(archive_path: Path) -> set:
    """
    Return the set of (ticker, date) pairs that have at least one HTML file.

    Used by ingest_all to skip PDF processing when an HTML already covers the
    same period — HTML is preferred (cleaner text, faster parsing, no PDF
    rendering artifacts). Only considers monthly production reports; quarterly
    filings are excluded because they never have HTML equivalents in the archive.

    Period inference uses filename strategies 1-3 only (no body read) to keep
    this pre-scan fast.
    """
    covered: set = set()
    for html_path in archive_path.rglob("*.html"):
        if not is_production_filename(html_path.name):
            continue
        ticker = infer_ticker_from_path(str(html_path))
        if ticker is None:
            continue
        period = infer_period_from_filename(str(html_path), ticker, read_body=False)
        if period is not None:
            covered.add((ticker, period))
    return covered


@dataclass
class ArchiveIngestor:
    """Ingests historical PDFs and HTMLs from the archive directory."""
    archive_dir: str
    db: object  # MinerDB
    registry: object  # PatternRegistry

    def ingest_all(self, force: bool = False, progress_callback=None):
        """
        Walk archive_dir, parse all production report files, extract metrics,
        write results to DB. Returns IngestSummary.

        force=False (default): skip files that have already been ingested.
            Use this for incremental updates — only new files are processed.
        force=True: delete and re-ingest every matching file, even if already
            in the DB. Use this after logic changes (pattern updates, period
            offset fixes) to reprocess all files with the new logic. The DB
            remains the single source of truth; force simply refreshes it.

        Accepts both monthly production reports (matched by is_production_filename)
        and quarterly SEC filings (matched by is_quarterly_filing). Quarterly
        filings produce one data_point row per month found in the document body.

        Phase 1 — temporal offset correction: if the filename date and the body
        text disagree by ≤2 months, the body text period wins (common pattern:
        a February press release covers January production).
        """
        from miner_types import IngestSummary
        from config import CONFIDENCE_REVIEW_THRESHOLD
        from extractors.extraction_pipeline import extract_report
        from scrapers.manifest_scanner import scan_archive_directory
        from datetime import datetime

        summary = IngestSummary()
        archive_path = Path(self.archive_dir)

        # Phase 0: scan archive to update asset_manifest before processing
        try:
            scan_result = scan_archive_directory(archive_path, self.db)
            log.info(
                "Manifest scan complete: total=%d new=%d legacy=%d",
                scan_result.total_found, scan_result.newly_discovered, scan_result.legacy_undated,
            )
        except Exception as e:
            log.warning("Manifest scan failed (continuing with ingest): %s", e)

        # Pre-scan: find all (ticker, period) pairs that have HTML coverage.
        # Any PDF whose period is in this set will be skipped — HTML is preferred
        # (cleaner text, faster parsing, no PDF rendering artifacts, one fewer
        # LLM call). Undated HTMLs (strategy 4 required) are not in html_covered
        # but they never have corresponding PDFs in the archive anyway.
        html_covered = _build_html_covered(archive_path)
        log.info("Pre-scan: %d (ticker, period) pairs covered by HTML", len(html_covered))

        # Pre-count qualifying candidates so we can report accurate progress
        _all_candidates = [
            fp for fp in sorted(archive_path.rglob("*"))
            if fp.suffix.lower() in (".pdf", ".html")
            and infer_ticker_from_path(str(fp)) is not None
            and (is_production_filename(fp.name) or is_quarterly_filing(fp.name))
        ]
        _total_candidates = len(_all_candidates)
        _processed = 0

        for file_path in _all_candidates:
            suffix = file_path.suffix.lower()
            ticker = infer_ticker_from_path(str(file_path))
            quarterly = is_quarterly_filing(file_path.name)

            _processed += 1
            if progress_callback is not None:
                try:
                    progress_callback(_processed, _total_candidates)
                except Exception:
                    pass  # Never let a progress callback abort the loop

            # Skip PDFs when an HTML covers the same (ticker, period) —
            # avoids double-parsing and a wasted LLM call. Quarterly filings
            # are never in html_covered so they are unaffected.
            if suffix == ".pdf" and not quarterly:
                pdf_period = infer_period_from_filename(str(file_path), ticker, read_body=False)
                if pdf_period is not None and (ticker, pdf_period) in html_covered:
                    log.debug(
                        "Skipping PDF — HTML preferred for %s %s: %s",
                        ticker, pdf_period, file_path.name,
                    )
                    continue

            # Parse text early — needed for period offset correction (Phase 1)
            # and for quarterly month discovery (Phase 2).
            text = _parse_pdf(str(file_path)) if suffix == ".pdf" else _parse_html(str(file_path))
            if not text.strip():
                log.warning("Empty text extracted from %s", file_path)
                summary.errors += 1
                continue

            if quarterly:
                # Phase 2: quarterly filing — extract one period per month in body
                periods = extract_quarterly_months(text)
                if not periods:
                    log.warning("No monthly periods found in quarterly filing: %s", file_path.name)
                    continue
                source_type = "archive_quarterly"
                # Use the filing date from the filename as report_date (e.g.
                # "10-Q 2025-11-04.pdf" → 2025-11-01). This is stable and correct
                # even when body text contains stray future dates (bond maturities,
                # SEC boilerplate) that would corrupt periods[-1].
                filing_date = infer_period_from_filename(str(file_path), ticker)
                period_str = (
                    filing_date.strftime("%Y-%m-%d") if filing_date is not None
                    else periods[-1].strftime("%Y-%m-%d")
                )
            else:
                # Monthly press release — single period.
                # Pass full path and read_body=True for HTML so strategy 4 can fall
                # back to body-text period inference for undated files (e.g. old
                # "Riot Blockchain Announces April Production..." filenames that have
                # no year in the title).
                filename_period = infer_period_from_filename(
                    str(file_path), ticker, read_body=(suffix == ".html")
                )
                if filename_period is None:
                    log.warning("Could not infer period from %s", file_path.name)
                    continue

                # Phase 1: correct temporal offset using body text
                period = filename_period
                text_period = infer_period_from_text(text)
                if text_period is not None and text_period != period:
                    delta_months = abs(
                        (period.year - text_period.year) * 12
                        + (period.month - text_period.month)
                    )
                    if delta_months <= 2:
                        log.info(
                            "Period offset corrected for %s: filename=%s body=%s",
                            file_path.name, filename_period, text_period,
                        )
                        period = text_period

                periods = [period]
                source_type = "archive_pdf" if suffix == ".pdf" else "archive_html"
                period_str = period.strftime("%Y-%m-%d")

                # When force=True and an offset correction shifted the period, also
                # delete any stale record at the uncorrected (filename) period to
                # prevent duplicate rows for the same file.
                if force and period != filename_period:
                    old_period_str = filename_period.strftime("%Y-%m-%d")
                    deleted = self.db.delete_report(ticker, old_period_str, source_type)
                    if deleted:
                        log.info(
                            "Force-reingest: removed stale uncorrected record %s %s %s",
                            ticker, old_period_str, source_type,
                        )

            if self.db.report_exists(ticker, period_str, source_type):
                if not force:
                    log.debug("Report already ingested: %s %s %s", ticker, period_str, source_type)
                    continue
                # force=True: delete existing report + data_points, then re-ingest
                self.db.delete_report(ticker, period_str, source_type)
                log.info("Force-reingest: deleted %s %s %s", ticker, period_str, source_type)

            report = {
                "ticker": ticker,
                "report_date": period_str,
                "published_date": None,
                "source_type": source_type,
                "source_url": str(file_path),
                "raw_text": text[:50000],
                "parsed_at": datetime.utcnow().isoformat(),
            }
            try:
                report_id = self.db.insert_report(report)
                summary.reports_ingested += 1
                # Link manifest entry to the report if one exists
                try:
                    manifest_entry = self.db.get_manifest_by_file_path(str(file_path))
                    if manifest_entry:
                        self.db.link_manifest_to_report(manifest_entry['id'], report_id)
                except Exception as link_err:
                    log.warning("Failed to link manifest for %s: %s", file_path, link_err)
            except Exception as e:
                log.error("Failed to insert report for %s %s: %s", ticker, period_str, e, exc_info=True)
                summary.errors += 1
                continue

            if quarterly:
                # Phase 2: extract data per month in the quarterly filing
                results_by_period = _extract_quarterly_data_points(
                    text, self.registry, report_id, ticker, periods
                )
                for period_date, all_results in results_by_period.items():
                    q_period_str = period_date.strftime("%Y-%m-%d")
                    seen_metrics: set = set()
                    for result in all_results:
                        dp = {
                            "report_id": report_id,
                            "ticker": ticker,
                            "period": q_period_str,
                            "metric": result.metric,
                            "value": result.value,
                            "unit": result.unit,
                            "confidence": result.confidence,
                            "extraction_method": result.extraction_method,
                            "source_snippet": result.source_snippet,
                        }
                        if result.confidence >= CONFIDENCE_REVIEW_THRESHOLD:
                            if result.metric not in seen_metrics:
                                self.db.insert_data_point(dp)
                                summary.data_points_extracted += 1
                                seen_metrics.add(result.metric)
                            else:
                                review_item = {
                                    "data_point_id": None,
                                    "ticker": ticker,
                                    "period": q_period_str,
                                    "metric": result.metric,
                                    "raw_value": str(result.value),
                                    "confidence": result.confidence,
                                    "source_snippet": result.source_snippet,
                                    "status": "PENDING",
                                }
                                self.db.insert_review_item(review_item)
                                summary.review_flagged += 1
                        else:
                            review_item = {
                                "data_point_id": None,
                                "ticker": ticker,
                                "period": q_period_str,
                                "metric": result.metric,
                                "raw_value": str(result.value),
                                "confidence": result.confidence,
                                "source_snippet": result.source_snippet,
                                "status": "PENDING",
                            }
                            self.db.insert_review_item(review_item)
                            summary.review_flagged += 1
            else:
                # Monthly press release — delegate to shared extraction pipeline.
                # Pipeline runs LLM+regex+agreement and marks the report extracted.
                stored_report = self.db.get_report(report_id)
                if stored_report:
                    ext_summary = extract_report(stored_report, self.db, self.registry)
                    summary.data_points_extracted += ext_summary.data_points_extracted
                    summary.review_flagged += ext_summary.review_flagged
                    summary.errors += ext_summary.errors

        return summary


def _create_chunks_from_result(report_id: int, result, db) -> int:
    """Upsert document_chunks from a ParseResult.

    Creates one chunk per ParseResult section. Returns number of chunks created.

    Args:
        report_id: DB report id
        result: ParseResult from a parser
        db: MinerDB instance
    """
    count = 0
    for i, section in enumerate(result.sections):
        try:
            db.upsert_document_chunk({
                'report_id': report_id,
                'chunk_index': i,
                'section': section.name,
                'text': section.text,
                'char_start': section.char_start,
                'char_end': section.char_end,
                'token_count': None,  # estimated at embedding time
            })
            count += 1
        except Exception as e:
            log.warning("Failed to upsert chunk %d for report %d: %s", i, report_id, e)
    return count


