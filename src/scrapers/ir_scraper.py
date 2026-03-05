"""
IR press release scraper: fetches and parses monthly production updates
from company investor relations pages.

Respects IR_REQUEST_DELAY_SECONDS between each request. Handles 400 (unknown
resource) gracefully and raises on 5xx. Uses BeautifulSoup to find press release
links and titles. Supports four scrape modes:
  - "rss"      : fetch RSS feed, filter production PRs, download each HTML
  - "template" : generate URL per month from url_template; try each, skip 404/400
  - "index"    : parse HTML listing page for links (static HTML only)
  - "skip"     : do nothing (site unreachable or company inactive)

Template placeholders (case-sensitive):
  {month}  → lowercase month name  (e.g. "march")
  {Month}  → titlecase month name  (e.g. "March")
  {year}   → 4-digit year          (e.g. "2025")
"""
import re
import time
import logging
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from datetime import date, timedelta
from typing import Optional

import requests
from bs4 import BeautifulSoup

from config import IR_REQUEST_DELAY_SECONDS

log = logging.getLogger('miners.scrapers.ir_scraper')

# Month name map (shared with archive_ingestor)
MONTH_MAP: dict = {
    "january": 1, "february": 2, "march": 3, "april": 4,
    "may": 5, "june": 6, "july": 7, "august": 8,
    "september": 9, "october": 10, "november": 11, "december": 12,
}

_PRODUCTION_KEYWORDS: tuple = (
    "production",
    "operations update",
    "operational update",
    "mining operations",
    "bitcoin production",
    "monthly bitcoin",
)

_EXCLUSION_KEYWORDS: tuple = (
    "financial results",
    "earnings",
    "quarterly results",
    "annual report",
    "10-k",
    "10-q",
    "form 10",
)

_MONTH_NAME_PATTERN = re.compile(
    r"(january|february|march|april|may|june|july|august|"
    r"september|october|november|december)\s+(\d{4})",
    re.IGNORECASE,
)

_MONTH_NAMES = [
    "", "january", "february", "march", "april", "may", "june",
    "july", "august", "september", "october", "november", "december",
]


def expand_url_template(url_template: str, period: date) -> str:
    """
    Expand a URL template for the given month/year.

    Supported placeholders:
      {month}  → lowercase month name  (e.g. "march")
      {Month}  → titlecase month name  (e.g. "March")
      {year}   → 4-digit year          (e.g. "2025")
    """
    month_lower = _MONTH_NAMES[period.month]
    return (url_template
            .replace("{Month}", month_lower.capitalize())
            .replace("{month}", month_lower)
            .replace("{year}", str(period.year)))


def parse_rss_feed(xml_text: str) -> list:
    """
    Parse an Equisolve-format RSS feed.

    Returns a list of dicts with keys: title, link, pub_date.
    Items missing both title and link are skipped. Returns [] on parse error.
    """
    try:
        root = ET.fromstring(xml_text)
    except ET.ParseError:
        return []
    items = []
    for item in root.findall(".//item"):
        title = (item.findtext("title") or "").strip()
        link = (item.findtext("link") or "").strip()
        pub_date = (item.findtext("pubDate") or "").strip()
        if title and link:
            items.append({"title": title, "link": link, "pub_date": pub_date})
    return items


def is_production_pr(title: str) -> bool:
    """Return True if the press release title is a monthly production update."""
    lower = title.lower()
    has_production = any(kw in lower for kw in _PRODUCTION_KEYWORDS)
    has_exclusion = any(kw in lower for kw in _EXCLUSION_KEYWORDS)
    return has_production and not has_exclusion


def infer_period_from_pr_title(title: str) -> Optional[date]:
    """Parse the month and year from a press release title."""
    m = _MONTH_NAME_PATTERN.search(title)
    if not m:
        return None
    month_str, year_str = m.group(1).lower(), m.group(2)
    month = MONTH_MAP.get(month_str)
    if not month:
        return None
    return date(int(year_str), month, 1)


_HEADERS = {
    "User-Agent": (
        "Hermeneutic Research Platform/1.0 "
        "(Bitcoin miner IR data collection; contact@hermeneutic.io)"
    )
}


def _fetch_with_rate_limit(
    url: str, session: requests.Session
) -> Optional[requests.Response]:
    """
    Fetch URL with rate limiting and a User-Agent header.

    Returns None on 400/404 (expected "not found" conditions — log at DEBUG).
    Returns None on timeout or network error (log at WARNING).
    Raises on 5xx (unexpected server errors that should surface).
    """
    time.sleep(IR_REQUEST_DELAY_SECONDS)
    try:
        resp = session.get(url, timeout=15, headers=_HEADERS)
        if resp.status_code in (400, 404):
            # 400: bad request (template month not published in this form)
            # 404: page not found (template month simply doesn't exist)
            # Both are expected during template/index scanning — not errors.
            log.debug("IR page returned %d for %s", resp.status_code, url)
            return None
        resp.raise_for_status()
        return resp
    except requests.Timeout:
        log.warning("Timeout fetching %s", url)
        return None
    except requests.RequestException as e:
        log.error("HTTP error fetching %s: %s", url, e)
        return None


@dataclass
class IRScraper:
    """Fetches and parses live IR press releases for a company."""
    db: object          # MinerDB
    session: requests.Session

    def scrape_company(self, company: dict):
        """
        Dispatch to the appropriate scrape strategy based on scrape_mode.
        Returns IngestSummary.
        """
        from miner_types import IngestSummary
        # Support both legacy config key (scrape_mode) and DB/API key (scraper_mode).
        mode = (company.get("scraper_mode") or company.get("scrape_mode") or "skip").strip().lower()
        if mode == "rss":
            return self._scrape_rss(company)
        elif mode == "template":
            return self._scrape_template(company)
        elif mode == "index":
            return self._scrape_index(company)
        elif mode == "skip":
            log.info("Skipping %s: %s", company["ticker"], company.get("skip_reason", "no reason given"))
            return IngestSummary()
        else:
            log.warning("Unknown scrape_mode '%s' for %s", mode, company["ticker"])
            return IngestSummary()

    def _scrape_rss(self, company: dict):
        """
        Fetch RSS feed, filter production PRs, download each press release HTML.
        Stores raw text only — extraction is handled by the extraction pipeline.
        Returns IngestSummary.
        """
        from miner_types import IngestSummary
        from datetime import datetime

        summary = IngestSummary()
        ticker = company["ticker"]
        rss_url = (
            company.get("rss_url")
            or company.get("globenewswire_url")
            or company.get("prnewswire_url")
        )
        if not rss_url:
            log.error("%s: rss_url/globenewswire_url/prnewswire_url not set", ticker)
            summary.errors += 1
            return summary

        resp = _fetch_with_rate_limit(rss_url, self.session)
        if resp is None:
            summary.errors += 1
            return summary

        items = parse_rss_feed(resp.text)
        for item in items:
            if not is_production_pr(item["title"]):
                continue
            period = infer_period_from_pr_title(item["title"])
            if period is None:
                log.debug("Could not infer period from RSS title: %s", item["title"])
                continue
            period_str = period.strftime("%Y-%m-%d")

            if self.db.report_exists(ticker, period_str, "ir_press_release"):
                log.debug("Already ingested RSS PR: %s %s", ticker, period_str)
                continue

            page = _fetch_with_rate_limit(item["link"], self.session)
            if page is None:
                summary.errors += 1
                continue

            text = BeautifulSoup(page.text, 'lxml').get_text(separator=" ", strip=True)
            report = {
                "ticker": ticker,
                "report_date": period_str,
                "published_date": None,
                "source_type": "ir_press_release",
                "source_url": item["link"],
                "raw_text": text[:50000],
                "parsed_at": datetime.utcnow().isoformat(),
            }
            try:
                self.db.insert_report(report)
                summary.reports_ingested += 1
            except Exception as e:
                log.error("Failed to insert RSS report %s %s: %s", ticker, period_str, e, exc_info=True)
                summary.errors += 1

        return summary

    def _scrape_template(self, company: dict):
        """
        Generate one URL per month from pr_start_year to today using url_template.
        Try each URL; skip on 404/400 (month not published); ingest on 200.
        Stores raw text only — extraction is handled by the extraction pipeline.
        Returns IngestSummary.

        Template placeholders:
          {month}  → lowercase month name  (e.g. "march")
          {Month}  → titlecase month name  (e.g. "March")
          {year}   → 4-digit year          (e.g. "2025")
        """
        from miner_types import IngestSummary
        from datetime import datetime

        summary = IngestSummary()
        ticker = company["ticker"]
        url_template = company.get("url_template")
        start_year = company.get("pr_start_year")

        if not url_template:
            log.error("%s: url_template not set but scrape_mode is 'template'", ticker)
            summary.errors += 1
            return summary
        if not start_year:
            log.error("%s: pr_start_year not set", ticker)
            summary.errors += 1
            return summary

        # Walk months from the LATER OF (pr_start_year-01, latest IR period in DB)
        # up to the most recently COMPLETED month (we don't attempt the current
        # in-progress month — reports publish after month-end).
        today = date.today()
        last_completed = date(today.year, today.month, 1) - timedelta(days=1)
        last_completed = date(last_completed.year, last_completed.month, 1)

        # Fast-forward: if we already have IR reports, start from the month AFTER
        # the latest one already ingested (avoids N×HTTP for known-covered history).
        latest = self.db.latest_ir_period(ticker)
        if latest:
            ly, lm = int(latest[:4]), int(latest[5:7])
            start_from = date(ly, lm + 1, 1) if lm < 12 else date(ly + 1, 1, 1)
            # Never go before pr_start_year — respect the configured floor
            current = max(date(start_year, 1, 1), start_from)
            log.info("%s: fast-forwarding to %s (latest IR: %s)", ticker, current, latest)
        else:
            current = date(start_year, 1, 1)

        def _next_month(d: date) -> date:
            return date(d.year + 1, 1, 1) if d.month == 12 else date(d.year, d.month + 1, 1)

        while current <= last_completed:
            period_str = current.strftime("%Y-%m-%d")
            url = expand_url_template(url_template, current)

            if self.db.report_exists(ticker, period_str, "ir_press_release"):
                log.debug("Already ingested template PR: %s %s", ticker, period_str)
                current = _next_month(current)
                continue

            resp = _fetch_with_rate_limit(url, self.session)
            if resp is None:
                # 400/404 → month not published yet, move on
                current = _next_month(current)
                continue

            text = BeautifulSoup(resp.text, 'lxml').get_text(separator=" ", strip=True)
            report = {
                "ticker": ticker,
                "report_date": period_str,
                "published_date": None,
                "source_type": "ir_press_release",
                "source_url": url,
                "raw_text": text[:50000],
                "parsed_at": datetime.utcnow().isoformat(),
            }
            try:
                self.db.insert_report(report)
                summary.reports_ingested += 1
                log.info("Ingested template PR: %s %s from %s", ticker, period_str, url)
            except Exception as e:
                log.error("Failed to insert template report %s %s: %s", ticker, period_str, e, exc_info=True)
                summary.errors += 1

            current = _next_month(current)

        return summary

    def _scrape_index(self, company: dict):
        """
        Fetch IR index page, find production press releases, store raw text.
        Stores raw text only — extraction is handled by the extraction pipeline.
        Returns IngestSummary.
        """
        from miner_types import IngestSummary
        from datetime import datetime

        summary = IngestSummary()
        ir_url = company.get('ir_url', '')
        pr_base_url = company.get('pr_base_url', '')
        ticker = company['ticker']

        # Paginate through listing pages (?page=N) until we hit a page with no
        # new production PRs. All-already-ingested pages signal we've reached
        # covered history — stop early to avoid traversing the entire archive
        # on every incremental run.
        page = 1
        while True:
            page_url = ir_url if page == 1 else f"{ir_url}?page={page}"
            resp = _fetch_with_rate_limit(page_url, self.session)
            if resp is None:
                if page == 1:
                    summary.errors += 1
                break

            soup = BeautifulSoup(resp.text, 'lxml')
            links = soup.find_all('a', href=True)

            # Identify production PRs on this page
            production_links = []
            for link in links:
                title = link.get_text(separator=" ", strip=True)
                href = link['href']
                if not is_production_pr(title):
                    continue
                period = infer_period_from_pr_title(title)
                if period is None:
                    log.debug("Could not infer period from PR title: %s", title)
                    continue
                full_url = href if href.startswith("http") else (pr_base_url + href)
                production_links.append((title, full_url, period))

            new_count = 0
            for title, full_url, period in production_links:
                period_str = period.strftime("%Y-%m-%d")

                if self.db.report_exists(ticker, period_str, "ir_press_release"):
                    log.debug("Already ingested IR PR: %s %s", ticker, period_str)
                    continue

                new_count += 1
                pr_resp = _fetch_with_rate_limit(full_url, self.session)
                if pr_resp is None:
                    summary.errors += 1
                    continue

                text = BeautifulSoup(pr_resp.text, 'lxml').get_text(separator=" ", strip=True)
                report = {
                    "ticker": ticker,
                    "report_date": period_str,
                    "published_date": None,
                    "source_type": "ir_press_release",
                    "source_url": full_url,
                    "raw_text": text[:50000],
                    "parsed_at": datetime.utcnow().isoformat(),
                }
                try:
                    self.db.insert_report(report)
                    summary.reports_ingested += 1
                    log.info("Ingested index PR: %s %s from %s", ticker, period_str, full_url)
                except Exception as e:
                    log.error("Failed to insert IR report %s %s: %s", ticker, period_str, e, exc_info=True)
                    summary.errors += 1

            if new_count == 0 and production_links:
                # Page had production PRs but all were already ingested — we've
                # caught up to covered history. Stop paginating.
                log.debug("%s page %d: all %d production PRs already ingested, stopping", ticker, page, len(production_links))
                break

            # Check if there's a next page
            has_next = any(
                f"page={page + 1}" in a.get('href', '')
                for a in soup.find_all('a', href=True)
            )
            if not has_next:
                break
            page += 1

        return summary
