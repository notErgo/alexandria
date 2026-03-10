"""
HTML press release downloader for Bitcoin miner monthly production updates.

Two scraping modes (set per company in config/companies.json):
  template — builds the URL directly from a date-based pattern.
             No listing-page fetch needed; just try month-by-month.
  index    — scrapes the company's IR listing page to discover PR URLs,
             then downloads each matching press release page.

Downloaded files are saved to:
  {ARCHIVE_DIR}/Miner Monthly/{TICKER} MONTHLY/{YYYY}-{MM}-01_{ticker}_production.html

This naming is compatible with archive_ingestor.py:
  - Ticker inferred from parent directory name ("{TICKER} MONTHLY")
  - Period inferred from ISO date prefix ("{YYYY}-{MM}-01_")
  - Filename passes is_production_filename() (contains "production")

URL template substitution tokens:
  {month}  → "january"  (lowercase full month name)
  {Month}  → "January"  (title-case)
  {MONTH}  → "JANUARY"  (uppercase)
  {year}   → "2024"     (4-digit year)

Domains that require sandbox allowlist addition (settings.json networkAllowList):
  ir.mara.com                  (MARA  — index mode)
  investors.cleanspark.com     (CLSK  — template mode)
  investor.bitfarms.com        (BITF  — template mode)
  investors.ciphermining.com   (CIFR  — template mode)
"""
import json
import re
import time
import logging
from dataclasses import dataclass, field
from datetime import date
from pathlib import Path
from typing import Optional

import requests
from bs4 import BeautifulSoup

from config import ARCHIVE_DIR, HTML_DOWNLOAD_DELAY_SECONDS
from scrapers.ir_scraper import (
    candidate_urls_for_period,
    discovery_links_from_html,
    discovery_page_urls_for_company,
)

log = logging.getLogger('miners.scrapers.html_downloader')

# Titles that confirm a press release is a monthly production update
_PRODUCTION_KEYWORDS = (
    "production",
    "operations update",
    "mining operations",
    "bitcoin production",
    "bitcoin mining",
    "monthly bitcoin",
    "monthly update",
    "monthly production",
    "operational update",
    "mining update",
)

# Titles that indicate quarterly/annual reports — must be excluded
_EXCLUSION_KEYWORDS = (
    "financial results",
    "earnings",
    "quarterly results",
    "annual report",
    "10-k",
    "10-q",
    "form 10",
    "q1 ", "q2 ", "q3 ", "q4 ",
)

_MONTH_NAME_PATTERN = re.compile(
    r"(january|february|march|april|may|june|july|august|"
    r"september|october|november|december)\s+(\d{4})",
    re.IGNORECASE,
)

_MONTH_NAMES = [
    "january", "february", "march", "april", "may", "june",
    "july", "august", "september", "october", "november", "december",
]


@dataclass
class DownloadSummary:
    downloaded: int = 0
    skipped_existing: int = 0
    skipped_not_found: int = 0
    errors: int = 0
    companies_processed: list = field(default_factory=list)

    def report_lines(self) -> list[str]:
        lines = [
            f"  downloaded:        {self.downloaded}",
            f"  skipped (exists):  {self.skipped_existing}",
            f"  skipped (404):     {self.skipped_not_found}",
            f"  errors:            {self.errors}",
        ]
        if self.companies_processed:
            lines.append(f"  companies:         {', '.join(self.companies_processed)}")
        return lines


def _is_production_pr(title: str) -> bool:
    lower = title.lower()
    has_production = any(kw in lower for kw in _PRODUCTION_KEYWORDS)
    has_exclusion = any(kw in lower for kw in _EXCLUSION_KEYWORDS)
    return has_production and not has_exclusion


def _infer_period_from_title(title: str) -> Optional[date]:
    """Parse month and year from a press release title or URL slug.
    Hyphens are normalized to spaces so URL slugs like
    'riot-announces-october-2023-...' parse correctly.
    """
    normalized = title.replace('-', ' ')
    m = _MONTH_NAME_PATTERN.search(normalized)
    if not m:
        return None
    month_str, year_str = m.group(1).lower(), m.group(2)
    month = _MONTH_NAMES.index(month_str) + 1 if month_str in _MONTH_NAMES else 0
    if not month:
        return None
    return date(int(year_str), month, 1)


def _build_output_path(archive_dir: str, ticker: str, period: date) -> Path:
    """
    Return the filesystem path to save a downloaded HTML.

    Format: {archive_dir}/Miner Monthly/{TICKER} MONTHLY/{YYYY}-{MM}-01_{ticker}_production.html
    """
    out_dir = Path(archive_dir) / "Miner Monthly" / f"{ticker} MONTHLY"
    out_dir.mkdir(parents=True, exist_ok=True)
    fname = f"{period.strftime('%Y-%m')}-01_{ticker.lower()}_production.html"
    return out_dir / fname


def _fetch(url: str, session: requests.Session) -> Optional[requests.Response]:
    """
    GET url with rate limit. Returns:
      - Response on 200
      - None on 404 (not found, expected for template misses)
      - None on other errors (logged at WARNING/ERROR level)

    Sleep is applied AFTER a successful fetch only — 404 and connection errors
    return immediately without delay so template scanning of non-existent months
    doesn't take minutes of forced sleeping.
    """
    try:
        resp = session.get(url, timeout=20, allow_redirects=True)
        if resp.status_code == 404:
            log.debug("404 for %s", url)
            return None
        if resp.status_code == 400:
            log.debug("400 for %s", url)
            return None
        resp.raise_for_status()
        time.sleep(HTML_DOWNLOAD_DELAY_SECONDS)  # polite delay only on successful fetches
        return resp
    except requests.Timeout:
        log.warning("Timeout fetching %s", url)
        return None
    except requests.ConnectionError as e:
        log.warning("Connection error for %s: %s", url, e)
        return None
    except requests.RequestException as e:
        log.error("HTTP error fetching %s: %s", url, e)
        return None


def _build_template_url(template: str, period: date) -> str:
    """Substitute {month}, {Month}, {MONTH}, {year} in a URL template."""
    month_lower = _MONTH_NAMES[period.month - 1]
    return template.format(
        month=month_lower,
        Month=month_lower.title(),
        MONTH=month_lower.upper(),
        year=str(period.year),
    )


def _months_in_range(start_year: int, end_date: date) -> list[date]:
    """Return list of first-of-month dates from start_year-01-01 through end_date."""
    months = []
    d = date(start_year, 1, 1)
    while d <= end_date:
        months.append(d)
        m = d.month + 1
        y = d.year + (1 if m > 12 else 0)
        m = m if m <= 12 else 1
        d = date(y, m, 1)
    return months


class HTMLDownloader:
    """
    Downloads HTML press releases for each company and saves them to the archive.

    Usage:
        downloader = HTMLDownloader(archive_dir=ARCHIVE_DIR)
        summary = downloader.download_all(companies, since_year=2022)
    """

    def __init__(self, archive_dir: str):
        self.archive_dir = archive_dir
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': (
                'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) '
                'AppleWebKit/537.36 (KHTML, like Gecko) '
                'Chrome/120.0.0.0 Safari/537.36'
            ),
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
        })

    def download_all(
        self,
        companies: list[dict],
        since_year: Optional[int] = None,
        tickers: Optional[list[str]] = None,
    ) -> DownloadSummary:
        """
        Download press releases for all active companies.

        Args:
            companies:  list of company dicts from companies.json
            since_year: only download reports from this year onward.
                        Defaults to each company's pr_start_year.
            tickers:    if set, only process these tickers (case-insensitive).
        """
        summary = DownloadSummary()
        today = date.today()

        # Reports are published 1-5 days after month-end; stop at last full month
        if today.month == 1:
            last_report_month = date(today.year - 1, 12, 1)
        else:
            last_report_month = date(today.year, today.month - 1, 1)

        target_tickers = {t.upper() for t in tickers} if tickers else None

        for company in companies:
            ticker = company['ticker']
            if target_tickers and ticker not in target_tickers:
                continue

            mode = company.get('scraper_mode') or company.get('scrape_mode') or 'skip'
            if mode == 'skip' or not company.get('active', True):
                log.info("Skipping %s (mode=%s, active=%s)", ticker, mode,
                         company.get('active'))
                continue

            start_year = since_year or company.get('pr_start_year') or 2020

            log.info("Processing %s (mode=%s, from %d)", ticker, mode, start_year)

            if mode == 'template':
                sub = self._download_template(
                    company, start_year, last_report_month
                )
            elif mode == 'discovery':
                sub = self._download_discovery(
                    company, start_year, last_report_month
                )
            elif mode == 'index':
                sub = self._download_index(
                    company, start_year, last_report_month
                )
            else:
                log.warning("Unknown scrape_mode '%s' for %s — skipping", mode, ticker)
                continue

            summary.downloaded += sub.downloaded
            summary.skipped_existing += sub.skipped_existing
            summary.skipped_not_found += sub.skipped_not_found
            summary.errors += sub.errors
            summary.companies_processed.append(ticker)

        return summary

    def _download_discovery(
        self,
        company: dict,
        start_year: int,
        end_date: date,
    ) -> DownloadSummary:
        """Discover PR article URLs from archive pages, then download each article."""
        summary = DownloadSummary()
        ticker = company['ticker']
        page_urls = discovery_page_urls_for_company(company)
        seen_urls: set[str] = set()
        consecutive_empty = 0
        found_any = False

        for page_url in page_urls:
            log.info("  Fetching discovery page: %s", page_url)
            resp = _fetch(page_url, self.session)
            if resp is None:
                consecutive_empty += 1
                if found_any and consecutive_empty >= 3:
                    break
                continue

            candidates = discovery_links_from_html(company, resp.text, page_url)
            if not candidates:
                consecutive_empty += 1
                if found_any and consecutive_empty >= 3:
                    break
                continue

            found_any = True
            consecutive_empty = 0
            page_has_in_range_candidate = False

            for title, full_url, period_hint in candidates:
                if full_url in seen_urls:
                    continue
                seen_urls.add(full_url)

                period = period_hint or _infer_period_from_title(f"{title} {full_url}")
                if period is None:
                    continue
                if period.year < start_year or period > end_date:
                    continue

                page_has_in_range_candidate = True
                out_path = _build_output_path(self.archive_dir, ticker, period)
                if out_path.exists():
                    log.debug("Already exists: %s", out_path.name)
                    summary.skipped_existing += 1
                    continue

                log.info("  GET %s  [%s]", full_url, period)
                pr_resp = _fetch(full_url, self.session)
                if pr_resp is None:
                    summary.skipped_not_found += 1
                    continue

                try:
                    out_path.write_bytes(pr_resp.content)
                    log.info("  Saved %s (%d bytes)", out_path.name, len(pr_resp.content))
                    summary.downloaded += 1
                except OSError as e:
                    log.error("Failed to write %s: %s", out_path, e)
                    summary.errors += 1

            if found_any and not page_has_in_range_candidate:
                break

        return summary

    # ------------------------------------------------------------------ #
    #  Template mode                                                       #
    # ------------------------------------------------------------------ #

    def _download_template(
        self,
        company: dict,
        start_year: int,
        end_date: date,
    ) -> DownloadSummary:
        """Generate URLs month-by-month using the url_template, download each."""
        summary = DownloadSummary()
        ticker = company['ticker']
        template = company.get('url_template')
        if not template:
            log.error("%s has scrape_mode=template but no url_template", ticker)
            summary.errors += 1
            return summary

        for period in _months_in_range(start_year, end_date):
            out_path = _build_output_path(self.archive_dir, ticker, period)
            if out_path.exists():
                log.debug("Already exists: %s", out_path.name)
                summary.skipped_existing += 1
                continue

            urls = candidate_urls_for_period(company, period)
            if not urls:
                url = _build_template_url(template, period)
                urls = [url]

            resp = None
            for url in urls:
                log.info("  GET %s", url)
                resp = _fetch(url, self.session)
                if resp is not None:
                    break

            if resp is None:
                summary.skipped_not_found += 1
                continue

            try:
                out_path.write_bytes(resp.content)
                log.info("  Saved %s (%d bytes)", out_path.name, len(resp.content))
                summary.downloaded += 1
            except OSError as e:
                log.error("Failed to write %s: %s", out_path, e)
                summary.errors += 1

        return summary

    # ------------------------------------------------------------------ #
    #  Index mode                                                          #
    # ------------------------------------------------------------------ #

    def _download_index(
        self,
        company: dict,
        start_year: int,
        end_date: date,
    ) -> DownloadSummary:
        """
        Fetch the company's IR listing page, discover production PR links,
        download each matching page.
        """
        summary = DownloadSummary()
        ticker = company['ticker']
        ir_url = company.get('ir_url', '')
        pr_base = company.get('pr_base_url', '')

        log.info("  Fetching index: %s", ir_url)
        resp = _fetch(ir_url, self.session)
        if resp is None:
            log.warning("Could not fetch index page for %s", ticker)
            summary.errors += 1
            return summary

        soup = BeautifulSoup(resp.text, 'lxml')
        links = soup.find_all('a', href=True)

        seen_periods: set[date] = set()

        for link in links:
            title = link.get_text(strip=True)
            href = link['href']

            # Use the link text or slug to determine if it's a production PR
            # Also try inferring from the URL slug itself as a fallback
            slug_as_title = href.replace('-', ' ').replace('/', ' ')
            check_text = title if title else slug_as_title

            if not _is_production_pr(check_text):
                continue

            period = _infer_period_from_title(check_text)
            if period is None:
                log.debug("Could not infer period from: %s", check_text[:80])
                continue

            if period.year < start_year or period > end_date:
                log.debug("Period %s outside range — skip", period)
                continue

            if period in seen_periods:
                continue
            seen_periods.add(period)

            out_path = _build_output_path(self.archive_dir, ticker, period)
            if out_path.exists():
                log.debug("Already exists: %s", out_path.name)
                summary.skipped_existing += 1
                continue

            full_url = href if href.startswith('http') else (pr_base + href)
            log.info("  GET %s  [%s]", full_url, period)
            pr_resp = _fetch(full_url, self.session)
            if pr_resp is None:
                summary.skipped_not_found += 1
                continue

            try:
                out_path.write_bytes(pr_resp.content)
                log.info("  Saved %s (%d bytes)", out_path.name, len(pr_resp.content))
                summary.downloaded += 1
            except OSError as e:
                log.error("Failed to write %s: %s", out_path, e)
                summary.errors += 1

        return summary
