"""
DEPRECATED: IR scraping is preserved for historical/manual use only.
EDGAR (edgar_connector.py) is the canonical ingest source as of 2026-03.

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
import hashlib
import re
import time
import logging
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from typing import Optional
from urllib.parse import urljoin, urlparse

from infra.text_utils import make_html_report_fields

import requests
from bs4 import BeautifulSoup

from config import IR_REQUEST_DELAY_SECONDS
from scrapers.dedup import canonical_url, simhash_text
from scrapers.fetch_policy import DEFAULT_RETRY_POLICY, CircuitOpenError

try:
    from playwright.sync_api import sync_playwright
except ImportError:
    sync_playwright = None

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

_DISCOVERY_KEYWORDS: tuple = (
    "production",
    "operations update",
    "operational update",
    "bitcoin production",
    "bitcoin mining",
    "mining update",
    "mining operations",
    "monthly update",
    "monthly production",
    "hashrate",
    "hash rate",
    "exahash",
    "eh/s",
    "miners",
    "energized",
    "energization",
)

_MONTH_NAME_PATTERN = re.compile(
    r"(january|february|march|april|may|june|july|august|"
    r"september|october|november|december)\s+(\d{4})",
    re.IGNORECASE,
)

_PUBLISHED_DATE_PATTERNS: tuple[re.Pattern, ...] = (
    re.compile(
        r"(january|february|march|april|may|june|july|august|"
        r"september|october|november|december)\s+\d{1,2},\s+(\d{4})",
        re.IGNORECASE,
    ),
    re.compile(
        r"(\d{4})-(\d{2})-(\d{2})",
        re.IGNORECASE,
    ),
)

_BOT_CHALLENGE_MARKERS: tuple[str, ...] = (
    "just a moment",
    "enable javascript and cookies to continue",
    "performing security verification",
    "/cdn-cgi/challenge-platform/",
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


def riot_candidate_urls(period: date) -> list[str]:
    """Return historical RIOT press-release URL candidates for one month.

    RIOT changed slug families multiple times across the archive, so a single
    template is not sufficient for historical backfill.
    """
    month = _MONTH_NAMES[period.month]
    year = period.year
    candidates: list[str] = []
    if year >= 2023:
        candidates.append(
            f"https://www.riotplatforms.com/riot-announces-{month}-{year}-production-and-operations-updates/"
        )
    if 2021 <= year <= 2022:
        candidates.append(
            f"https://www.riotplatforms.com/riot-blockchain-announces-{month}-production-and-operations-updates/"
        )
        candidates.append(
            f"https://www.riotplatforms.com/riot-blockchain-announces-{month}-{year}-production-and-operations-updates/"
        )
    if year == 2020:
        candidates.append(
            f"https://www.riotplatforms.com/riot-blockchain-announces-{month}-{year}-production-update/"
        )
        candidates.append(
            f"https://www.riotplatforms.com/riot-blockchain-announces-{month}-production-and-operations-updates/"
        )
    if year <= 2019:
        candidates.append(
            f"https://www.riotplatforms.com/riot-blockchain-releases-{month}-{year}-cryptocurrency-mining-production-yield/"
        )
    return candidates


def cleanspark_candidate_urls(period: date) -> list[str]:
    """Return CleanSpark monthly update URL candidates for one month."""
    month_title = _MONTH_NAMES[period.month].capitalize()
    report_year = period.year
    if period < date(2023, 4, 1):
        return []
    explicit = {
        "2023-04": "https://investors.cleanspark.com/news/news-details/2023/CleanSpark-Releases-April-2023-Bitcoin-Mining-Update-05-03-2023/default.aspx",
        "2023-05": "https://investors.cleanspark.com/news/news-details/2023/CleanSpark-Releases-May-2023-Bitcoin-Mining-Update-06-02-2023/default.aspx",
        "2023-06": "https://investors.cleanspark.com/news/news-details/2023/CleanSpark-Releases-June-2023-Bitcoin-Mining-Update-07-03-2023/default.aspx",
        "2023-07": "https://investors.cleanspark.com/news/news-details/2023/CleanSpark-Releases-July-2023-Bitcoin-Mining-Update-08-02-2023/default.aspx",
        "2023-08": "https://investors.cleanspark.com/news/news-details/2023/CleanSpark-Releases-August-2023-Bitcoin-Mining-Update-09-05-2023/default.aspx",
        "2023-09": "https://investors.cleanspark.com/news/news-details/2023/CleanSpark-Releases-September-2023-Bitcoin-Mining-Update-10-03-2023/default.aspx",
        "2023-10": "https://investors.cleanspark.com/news/news-details/2023/CleanSpark-Releases-October-2023-Bitcoin-Mining-Update-2023-_50Bd5BLR9/default.aspx",
    }
    explicit_key = f"{report_year:04d}-{period.month:02d}"
    candidates: list[str] = []
    if explicit_key in explicit:
        candidates.append(explicit[explicit_key])
    publish_years = [report_year]
    # December monthly updates are typically published in early January of the
    # following year, and CleanSpark's path uses publish year in /news-details/.
    if period.month == 12:
        publish_years.insert(0, report_year + 1)
    for publish_year in publish_years:
        base = f"https://investors.cleanspark.com/news/news-details/{publish_year}/"
        if publish_year >= 2026:
            candidates.append(
                f"{base}CleanSpark-Releases-{month_title}-{report_year}-Operational-Update/default.aspx"
            )
        candidates.append(
            f"{base}CleanSpark-Releases-{month_title}-{report_year}-Bitcoin-Mining-Update/default.aspx"
        )
    return candidates


def candidate_urls_for_period(company: dict, period: date) -> list[str]:
    ticker = (company.get("ticker") or "").upper()
    if ticker == "RIOT":
        return riot_candidate_urls(period)
    if ticker == "CLSK":
        return cleanspark_candidate_urls(period)
    url_template = company.get("url_template")
    if not url_template:
        return []
    return [expand_url_template(url_template, period)]


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


def is_mining_activity_pr(text: str) -> bool:
    """Return True if title/slug text looks like a mining activity press release."""
    lower = (text or "").lower()
    has_activity = any(kw in lower for kw in _DISCOVERY_KEYWORDS)
    has_exclusion = any(kw in lower for kw in _EXCLUSION_KEYWORDS)
    return has_activity and not has_exclusion


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


def infer_period_from_text(text: str) -> Optional[date]:
    """Infer a monthly period from free text or a URL slug."""
    normalized = (text or "").replace('-', ' ').replace('/', ' ')
    return infer_period_from_pr_title(normalized)


def infer_published_date_from_html(html_text: str) -> Optional[str]:
    """Best-effort published-date extractor for discovery-fetched IR pages."""
    soup = BeautifulSoup(html_text, "lxml")
    candidates: list[str] = []

    for tag in soup.find_all("meta"):
        for key in ("content", "value"):
            val = (tag.get(key) or "").strip()
            if val:
                prop = (tag.get("property") or tag.get("name") or "").lower()
                if any(k in prop for k in ("published", "date", "modified_time", "article:published_time")):
                    candidates.append(val)

    for tag in soup.find_all(["time", "span", "p", "div"]):
        text = tag.get_text(" ", strip=True)
        if text and len(text) <= 64:
            classes = " ".join(tag.get("class") or []).lower()
            if tag.name == "time" or any(k in classes for k in ("date", "published", "time")):
                candidates.append(text)

    for candidate in candidates:
        candidate = candidate.strip()
        try:
            if "T" in candidate:
                return datetime.fromisoformat(candidate.replace("Z", "+00:00")).date().isoformat()
        except ValueError:
            pass
        for pattern in _PUBLISHED_DATE_PATTERNS:
            match = pattern.search(candidate)
            if not match:
                continue
            if pattern.pattern.startswith("(\\d{4})"):
                year, month, day = match.groups()
                return f"{int(year):04d}-{int(month):02d}-{int(day):02d}"
            month_str = match.group(1).lower()
            year_str = match.group(2)
            month = MONTH_MAP.get(month_str)
            if month:
                day_match = re.search(r"\b(\d{1,2})\b", candidate)
                day = int(day_match.group(1)) if day_match else 1
                return f"{int(year_str):04d}-{month:02d}-{day:02d}"
    return None


def is_bot_challenge_page(html_text: str, headers: Optional[dict] = None, status_code: Optional[int] = None) -> bool:
    """Detect common anti-bot interstitials that masquerade as normal HTML."""
    lowered = (html_text or "").lower()
    if any(marker in lowered for marker in _BOT_CHALLENGE_MARKERS):
        return True
    headers = {str(k).lower(): str(v).lower() for k, v in (headers or {}).items()}
    if headers.get("cf-mitigated") == "challenge":
        return True
    if status_code == 403 and "cloudflare" in headers.get("server", ""):
        return True
    return False


def discovery_page_urls_for_company(company: dict) -> list[str]:
    """Return archive/listing pages to walk for discovery-first scraping."""
    ticker = (company.get("ticker") or "").upper()
    ir_url = (company.get("ir_url") or "").rstrip("/")
    prnewswire_url = (company.get("prnewswire_url") or "").rstrip("/")

    if ticker == "CLSK":
        # Primary: native IR listing page (more reliable, no bot challenge)
        pages = []
        if ir_url:
            pages.append(ir_url)
            pages.extend(f"{ir_url}?page={page}" for page in range(2, 11))
        # Secondary: prnewswire archive (broader historical coverage, bot-challenged)
        if prnewswire_url:
            pages.append(prnewswire_url)
            pages.extend(f"{prnewswire_url}?page={page}" for page in range(2, 21))
        return pages

    if ticker == "RIOT":
        # The main RIOT IR page is largely shell HTML; the WordPress author
        # archive exposes the full historical article list in stable pagination.
        return [
            f"https://www.riotplatforms.com/author/b2ieverest456dfghbs/page/{page}/"
            for page in range(1, 61)
        ]

    if not ir_url:
        return []

    pages = [ir_url]
    pages.extend(f"{ir_url}?page={page}" for page in range(2, 31))
    return pages


def discovery_links_from_html(company: dict, html_text: str, page_url: str) -> list[tuple[str, str, Optional[date]]]:
    """Extract candidate mining-activity PR links from a discovery/listing page."""
    soup = BeautifulSoup(html_text, "lxml")
    allowed_host = urlparse(page_url).netloc.lower()
    seen: set[str] = set()
    results: list[tuple[str, str, Optional[date]]] = []

    for link in soup.find_all("a", href=True):
        href = (link.get("href") or "").strip()
        if not href or href.startswith("#") or href.lower().startswith("javascript:"):
            continue

        full_url = canonical_url(urljoin(page_url, href))
        parsed = urlparse(full_url)
        if allowed_host and parsed.netloc.lower() != allowed_host:
            continue

        title = link.get_text(" ", strip=True)
        slug_text = full_url.replace("-", " ").replace("/", " ")
        check_text = f"{title} {slug_text}".strip()
        if not is_mining_activity_pr(check_text):
            continue

        if full_url in seen:
            continue
        seen.add(full_url)
        results.append((title, full_url, infer_period_from_text(check_text)))

    return results


# Domains whose IR listing pages are JS-rendered (Equisolve/Q4 widgets).
# requests returns a static shell with no article links; Playwright is required.
_JS_RENDERED_DOMAINS: frozenset = frozenset({
    "investors.cleanspark.com",
    "investors.corescientific.com",  # Core Scientific — Equisolve widget
})

_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection": "keep-alive",
    "Upgrade-Insecure-Requests": "1",
}


# Pagination "Next" button selectors for JS-rendered IR widgets (Equisolve/Q4).
# Tried in order; first visible match is used.
_NEXT_PAGE_SELECTORS: tuple[str, ...] = (
    "a.pager__item--next",
    "li.pager-next a",
    "li.next a",
    "a[rel='next']",
    ".pagination a[aria-label='Next']",
    "[aria-label='Next page']",
    ".listing-pagination a:last-child",
)


def _fetch_with_playwright(url: str) -> Optional[str]:
    """Fallback fetch using a headless Chromium browser for bot-protected pages.

    Returns the page HTML as a string, or None on failure.
    Only called when requests-based fetch gets a bot challenge.
    """
    try:
        from playwright.sync_api import sync_playwright
    except ImportError:
        log.warning("playwright not installed — cannot bypass bot challenge for %s", url)
        return None
    try:
        with sync_playwright() as pw:
            browser = pw.chromium.launch(headless=True)
            context = browser.new_context(
                user_agent=_HEADERS["User-Agent"],
                extra_http_headers={
                    "Accept-Language": _HEADERS["Accept-Language"],
                },
            )
            page = context.new_page()
            page.goto(url, wait_until="networkidle", timeout=30000)
            # Extra settle time for JS widget rendering (Equisolve/Q4) and bot-challenge redirects
            page.wait_for_timeout(2000)
            html = page.content()
            browser.close()
            if is_bot_challenge_page(html):
                log.warning("Playwright also got bot challenge for %s", url)
                return None
            log.info("Playwright fetch OK for %s (%d chars)", url, len(html))
            return html
    except Exception as exc:
        log.warning("Playwright fetch failed for %s: %s", url, exc)
        return None


def _playwright_collect_all_pages(url: str, max_pages: int = 30) -> list[str]:
    """Use a single Playwright session to paginate through a JS-rendered listing.

    Loads the URL, captures the page HTML, then clicks the "Next page" control
    (Equisolve/Q4 widget pagination) and repeats until no next button is found or
    max_pages is reached.  Returns a list of HTML strings, one per page.

    Used for JS-rendered IR listing pages where ?page=N query params are ignored
    by the JavaScript widget (Equisolve, Q4 IR sites).
    """
    try:
        from playwright.sync_api import sync_playwright
    except ImportError:
        log.warning("playwright not installed — cannot paginate JS-rendered listing %s", url)
        return []
    pages_html: list[str] = []
    try:
        with sync_playwright() as pw:
            browser = pw.chromium.launch(headless=True)
            context = browser.new_context(
                user_agent=_HEADERS["User-Agent"],
                extra_http_headers={"Accept-Language": _HEADERS["Accept-Language"]},
            )
            pw_page = context.new_page()
            pw_page.goto(url, wait_until="networkidle", timeout=30000)
            pw_page.wait_for_timeout(2000)

            for page_num in range(1, max_pages + 1):
                html = pw_page.content()
                if is_bot_challenge_page(html):
                    log.warning("Playwright got bot challenge on page %d of %s", page_num, url)
                    break
                log.debug("Playwright paginated fetch: page %d of %s (%d chars)", page_num, url, len(html))
                pages_html.append(html)

                # Try each known "Next" selector; stop paginating if none are visible
                next_clicked = False
                for selector in _NEXT_PAGE_SELECTORS:
                    try:
                        btn = pw_page.locator(selector).first
                        if btn.is_visible(timeout=1000):
                            btn.click()
                            pw_page.wait_for_load_state("networkidle", timeout=15000)
                            pw_page.wait_for_timeout(1500)
                            next_clicked = True
                            break
                    except Exception:
                        continue

                if not next_clicked:
                    log.debug("Playwright pagination: no next button found after page %d", page_num)
                    break

            browser.close()
    except Exception as exc:
        log.warning("Playwright paginated fetch failed for %s: %s", url, exc)
    log.info("Playwright paginated fetch: %d pages collected for %s", len(pages_html), url)
    return pages_html


def _fetch_with_rate_limit(
    url: str, session: requests.Session
) -> Optional[requests.Response]:
    """
    Fetch URL with rate limiting and a browser-like User-Agent.

    Returns None on 400/404 (expected "not found" conditions — log at DEBUG).
    Returns None on timeout or network error (log at WARNING).
    Raises on 5xx (unexpected server errors that should surface).
    For bot-protected domains (prnewswire), falls back to Playwright.
    """
    from urllib.parse import urlparse as _urlparse
    if _urlparse(url).netloc.lower() in _JS_RENDERED_DOMAINS:
        log.debug("JS-rendered domain — using Playwright for %s", url)
        html = _fetch_with_playwright(url)
        if html is None:
            return None
        import requests as _req
        mock = _req.models.Response()
        mock.status_code = 200
        mock._content = html.encode("utf-8", errors="replace")
        mock.encoding = "utf-8"
        return mock

    time.sleep(IR_REQUEST_DELAY_SECONDS)
    try:
        resp = DEFAULT_RETRY_POLICY.execute(session.get, url, timeout=15, headers=_HEADERS)
        if is_bot_challenge_page(resp.text, resp.headers, resp.status_code):
            log.warning("Bot challenge detected for %s (status=%s) — trying Playwright", url, resp.status_code)
            html = _fetch_with_playwright(url)
            if html is None:
                return None
            # Wrap in a mock response-like object so callers get .text
            resp._content = html.encode("utf-8", errors="replace")
            resp.status_code = 200
            return resp
        if resp.status_code in (400, 404):
            # 400: bad request (template month not published in this form)
            # 404: page not found (template month simply doesn't exist)
            # Both are expected during template/index scanning — not errors.
            log.debug("IR page returned %d for %s", resp.status_code, url)
            return None
        resp.raise_for_status()
        return resp
    except CircuitOpenError as e:
        log.warning("Circuit open, skipping fetch for %s: %s", url, e)
        return None
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
    _pipeline_run_id: Optional[int] = None

    def _emit(self, event: str, ticker: str = None, level: str = 'INFO', **details) -> None:
        """Write a pipeline event if this scraper is running inside a pipeline run."""
        if not self._pipeline_run_id:
            return
        try:
            self.db.add_pipeline_run_event(
                self._pipeline_run_id,
                stage='ir_scrape',
                event=event,
                ticker=ticker,
                level=level,
                details=details or None,
            )
        except Exception:
            pass  # never let event logging crash the scraper

    def scrape_company(self, company: dict):
        """
        Dispatch to the appropriate scrape strategy based on scrape_mode.
        Returns IngestSummary.
        """
        from miner_types import IngestSummary
        # Support both legacy config key (scrape_mode) and DB/API key (scraper_mode).
        mode = (company.get("scraper_mode") or company.get("scrape_mode") or "skip").strip().lower()
        ticker = company.get("ticker", "")
        if mode != "skip":
            self._emit('scrape_start', ticker=ticker, mode=mode,
                       ir_url=company.get("ir_url") or company.get("rss_url") or "")
        if mode == "rss":
            return self._scrape_rss(company)
        elif mode == "discovery":
            return self._scrape_discovery(company)
        elif mode == "template":
            return self._scrape_template(company)
        elif mode == "index":
            return self._scrape_index(company)
        elif mode == "playwright":
            return self._scrape_playwright(company)
        elif mode == "drupal_year":
            return self._scrape_drupal_year(company)
        elif mode == "skip":
            log.info("Skipping %s: %s", company["ticker"], company.get("skip_reason", "no reason given"))
            return IngestSummary()
        else:
            log.warning("Unknown scrape_mode '%s' for %s", mode, company["ticker"])
            return IngestSummary()

    def _insert_ir_report(
        self,
        *,
        ticker: str,
        period: date,
        source_url: str,
        html_text: str,
        fetch_strategy: str,
        summary,
        title: str | None = None,
        published_date: str | None = None,
        source_type: str = "ir_press_release",
    ) -> bool:
        """Insert one IR report with shared dedup/content handling."""
        period_str = period.strftime("%Y-%m-%d")
        source_url = canonical_url(source_url)
        url_hash = hashlib.sha256(source_url.encode()).hexdigest()
        if self.db.report_exists_by_url_hash(url_hash, ticker):
            log.debug("Already ingested %s PR by URL: %s %s", fetch_strategy, ticker, source_url)
            self._emit('url_skipped', ticker=ticker, reason='duplicate_url', url=source_url, period=period_str)
            return False

        html_fields = make_html_report_fields(html_text)
        content_hash = simhash_text(html_fields["raw_text"][:5000])
        dupes = self.db.find_near_duplicates(content_hash, ticker)
        if dupes:
            log.warning(
                "Near-duplicate content detected for %s, skipping insert (matched report id=%s)",
                ticker, dupes[0]["id"],
            )
            self._emit(
                'url_skipped',
                ticker=ticker,
                level='WARNING',
                reason='near_duplicate',
                url=source_url,
                period=period_str,
                matched_id=dupes[0]['id'],
            )
            return False

        report = {
            "ticker": ticker,
            "report_date": period_str,
            "published_date": published_date,
            "source_type": source_type,
            "source_url": source_url,
            **html_fields,
            "parsed_at": datetime.now(timezone.utc).isoformat(),
            "content_simhash": content_hash,
            "fetch_strategy": fetch_strategy,
        }
        try:
            self.db.insert_report(report)
            summary.reports_ingested += 1
            self._emit(
                'url_ingested',
                ticker=ticker,
                period=period_str,
                title=title or "",
                fetch_strategy=fetch_strategy,
                text_chars=len(html_fields["raw_text"]),
                url=source_url,
            )
            return True
        except Exception as e:
            log.error("Failed to insert %s report %s %s: %s", fetch_strategy, ticker, period_str, e, exc_info=True)
            self._emit(
                'url_error',
                ticker=ticker,
                level='WARNING',
                period=period_str,
                title=title or "",
                fetch_strategy=fetch_strategy,
                url=source_url,
                error=str(e),
            )
            summary.errors += 1
            return False

    def _scrape_rss(self, company: dict):
        """
        Fetch RSS feed, filter production PRs, download each press release HTML.
        Stores raw text only — extraction is handled by the extraction pipeline.
        Returns IngestSummary.
        """
        from miner_types import IngestSummary

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

            pr_url = canonical_url(item["link"])
            url_hash = hashlib.sha256(pr_url.encode()).hexdigest()
            if self.db.report_exists_by_url_hash(url_hash, ticker):
                log.debug("Already ingested RSS PR by URL: %s %s", ticker, pr_url)
                self._emit('url_skipped', ticker=ticker, reason='duplicate_url', url=pr_url, period=period_str)
                continue

            page = _fetch_with_rate_limit(pr_url, self.session)
            if page is None:
                summary.errors += 1
                continue

            html_fields = make_html_report_fields(page.text)
            content_hash = simhash_text(html_fields["raw_text"][:5000])
            dupes = self.db.find_near_duplicates(content_hash, ticker)
            if dupes:
                log.warning(
                    "Near-duplicate content detected for %s, skipping insert (matched report id=%s)",
                    ticker, dupes[0]["id"],
                )
                self._emit('url_skipped', ticker=ticker, level='WARNING', reason='near_duplicate', url=pr_url, period=period_str, matched_id=dupes[0]['id'])
                continue
            report = {
                "ticker": ticker,
                "report_date": period_str,
                "published_date": None,
                "source_type": "ir_press_release",
                "source_url": pr_url,
                **html_fields,
                "parsed_at": datetime.now(timezone.utc).isoformat(),
                "content_simhash": content_hash,
                "fetch_strategy": "rss",
            }
            try:
                self.db.insert_report(report)
                summary.reports_ingested += 1
                self._emit('url_ingested', ticker=ticker, period=period_str,
                           title=item["title"], pub_date=item.get("pub_date", ""),
                           fetch_strategy='rss', text_chars=len(html_fields["raw_text"]), url=pr_url)
            except Exception as e:
                log.error("Failed to insert RSS report %s %s: %s", ticker, period_str, e, exc_info=True)
                self._emit('url_error', ticker=ticker, level='WARNING', period=period_str,
                           fetch_strategy='rss', url=pr_url, error=str(e))
                summary.errors += 1

        return summary

    def _scrape_discovery(self, company: dict):
        """
        Discovery-first IR scraping.

        Walks archive/listing pages for the ticker, discovers relevant mining
        activity PR links, then fetches the article pages themselves.
        """
        from miner_types import IngestSummary

        summary = IngestSummary()
        ticker = company["ticker"]
        start_year = company.get("pr_start_year")
        if not company.get("ir_url"):
            log.error("%s: ir_url not set for discovery mode", ticker)
            summary.errors += 1
            return summary
        if not start_year:
            log.error("%s: pr_start_year not set for discovery mode", ticker)
            summary.errors += 1
            return summary

        page_urls = discovery_page_urls_for_company(company)
        if not page_urls:
            log.error("%s: no discovery pages configured", ticker)
            summary.errors += 1
            return summary

        ir_url = (company.get("ir_url") or "").rstrip("/")
        ir_host = urlparse(ir_url).netloc.lower() if ir_url else ""
        use_playwright_pagination = ir_host in _JS_RENDERED_DOMAINS

        # For JS-rendered listing pages (Equisolve/Q4 widgets) the ?page=N query
        # parameter is ignored by the JavaScript widget — the server returns the
        # same first-page content regardless.  Use a single Playwright session
        # that clicks through pagination instead.
        if use_playwright_pagination:
            log.info("%s: JS-rendered listing — using Playwright pagination for %s", ticker, ir_url)
            page_htmls_js = _playwright_collect_all_pages(ir_url)
            # Build a synthetic (html, source_url) list mirroring the URL-based loop
            page_sources: list[tuple[str, str]] = [
                (html, ir_url) for html in page_htmls_js
            ]
            # For CLSK the page_urls list also includes prnewswire fallback pages —
            # keep those as a static fallback after the JS pages are exhausted.
            static_fallback_urls = [u for u in page_urls if urlparse(u).netloc.lower() != ir_host]
        else:
            page_sources = []
            static_fallback_urls = page_urls

        seen_urls: set[str] = set()
        consecutive_empty = 0
        found_any = False

        def _process_page(html_text: str, page_url: str, page_idx: int) -> bool:
            """Process one listing page; returns True if at least one recent candidate found."""
            nonlocal consecutive_empty, found_any
            candidates = discovery_links_from_html(company, html_text, page_url)
            if not candidates:
                consecutive_empty += 1
                return False

            consecutive_empty = 0
            found_any = True
            page_has_recent = False

            for title, full_url, hinted_period in candidates:
                if full_url in seen_urls:
                    continue
                seen_urls.add(full_url)

                period_hint = hinted_period or infer_period_from_text(full_url)
                if period_hint and period_hint.year >= start_year:
                    page_has_recent = True

                if period_hint and period_hint.year < start_year:
                    continue

                url_hash = hashlib.sha256(full_url.encode()).hexdigest()
                if self.db.report_exists_by_url_hash(url_hash, ticker):
                    period_str = period_hint.strftime("%Y-%m-%d") if period_hint else ""
                    self._emit('url_skipped', ticker=ticker, reason='duplicate_url', url=full_url, period=period_str)
                    continue

                pr_resp = _fetch_with_rate_limit(full_url, self.session)
                if pr_resp is None:
                    summary.errors += 1
                    continue

                page_period = (
                    period_hint
                    or infer_period_from_text(title)
                    or infer_period_from_text(full_url)
                    or infer_period_from_text(pr_resp.text[:5000])
                )
                published_date = infer_published_date_from_html(pr_resp.text)
                if page_period is None and published_date:
                    pub = datetime.fromisoformat(published_date).date()
                    page_period = date(pub.year, pub.month, 1)

                if page_period is None:
                    log.debug("%s: could not infer period for discovered PR %s", ticker, full_url)
                    continue
                if page_period.year < start_year:
                    continue

                inserted = self._insert_ir_report(
                    ticker=ticker,
                    period=page_period,
                    source_url=full_url,
                    html_text=pr_resp.text,
                    fetch_strategy="discovery",
                    summary=summary,
                    title=title,
                    published_date=published_date,
                    source_type=(
                        "prnewswire_press_release"
                        if "prnewswire.com" in urlparse(full_url).netloc.lower()
                        else "ir_press_release"
                    ),
                )
                if inserted:
                    log.info("Ingested discovery PR: %s %s from %s", ticker, page_period, full_url)

            return page_has_recent

        # Process Playwright-paginated pages first (JS-rendered listing)
        for page_idx, (html_text, page_url) in enumerate(page_sources, start=1):
            self._emit('page_fetch', ticker=ticker, url=page_url, page=page_idx)
            _process_page(html_text, page_url, page_idx)

        # Process static URL pages (regular requests, or prnewswire fallback)
        page_offset = len(page_sources)
        for page_idx, page_url in enumerate(static_fallback_urls, start=page_offset + 1):
            self._emit('page_fetch', ticker=ticker, url=page_url, page=page_idx)
            resp = _fetch_with_rate_limit(page_url, self.session)
            if resp is None:
                consecutive_empty += 1
                if found_any and consecutive_empty >= 3:
                    break
                continue

            has_recent = _process_page(resp.text, page_url, page_idx)
            if found_any and not has_recent:
                break

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

        summary = IngestSummary()
        ticker = company["ticker"]
        url_template = company.get("url_template")
        start_year = company.get("pr_start_year")
        # When True, bypass the fast-forward so all months from pr_start_year are
        # attempted even when the DB already holds recent IR reports.  URL-hash
        # dedup prevents re-inserting already-ingested months.
        backfill_mode = company.get("backfill_mode", False)

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
        # Skipped when backfill_mode=True so pre-existing coverage is traversed.
        latest = self.db.latest_ir_period(ticker)
        if not backfill_mode and latest:
            try:
                ly, lm = int(latest[:4]), int(latest[5:7])
                start_from = date(ly, lm + 1, 1) if lm < 12 else date(ly + 1, 1, 1)
                # Never go before pr_start_year — respect the configured floor
                current = max(date(start_year, 1, 1), start_from)
                log.info("%s: fast-forwarding to %s (latest IR: %s)", ticker, current, latest)
            except (ValueError, IndexError) as e:
                log.warning(
                    "%s: could not parse latest_ir_period %r (%s) — starting from %d",
                    ticker, latest, e, start_year,
                )
                current = date(start_year, 1, 1)
        else:
            current = date(start_year, 1, 1)
            if backfill_mode:
                log.info("%s: backfill_mode=True — starting from %s", ticker, current)

        def _next_month(d: date) -> date:
            return date(d.year + 1, 1, 1) if d.month == 12 else date(d.year, d.month + 1, 1)

        while current <= last_completed:
            period_str = current.strftime("%Y-%m-%d")
            candidates = [canonical_url(u) for u in candidate_urls_for_period(company, current)]
            if not candidates:
                log.debug(
                    "%s: no template candidates for %s; skipping outside known IR boundary",
                    ticker,
                    period_str,
                )
                current = _next_month(current)
                continue

            resp = None
            resolved_url = None
            duplicate_hit = False
            for url in candidates:
                url_hash = hashlib.sha256(url.encode()).hexdigest()
                if self.db.report_exists_by_url_hash(url_hash, ticker):
                    log.debug("Already ingested template PR by URL: %s %s", ticker, url)
                    self._emit('url_skipped', ticker=ticker, reason='duplicate_url', url=url, period=period_str)
                    duplicate_hit = True
                    resolved_url = url
                    break

                candidate_resp = _fetch_with_rate_limit(url, self.session)
                if candidate_resp is None:
                    continue
                resp = candidate_resp
                resolved_url = url
                break

            if duplicate_hit:
                current = _next_month(current)
                continue
            if resp is None or resolved_url is None:
                # 400/404 for all candidates → month not published or slug family unknown
                current = _next_month(current)
                continue

            html_fields = make_html_report_fields(resp.text)
            content_hash = simhash_text(html_fields["raw_text"][:5000])
            dupes = self.db.find_near_duplicates(content_hash, ticker)
            if dupes:
                log.warning(
                    "Near-duplicate content detected for %s, skipping insert (matched report id=%s)",
                    ticker, dupes[0]["id"],
                )
                current = _next_month(current)
                continue
            report = {
                "ticker": ticker,
                "report_date": period_str,
                "published_date": None,
                "source_type": "ir_press_release",
                "source_url": resolved_url,
                **html_fields,
                "parsed_at": datetime.now(timezone.utc).isoformat(),
                "content_simhash": content_hash,
                "fetch_strategy": "template",
            }
            try:
                self.db.insert_report(report)
                summary.reports_ingested += 1
                log.info("Ingested template PR: %s %s from %s", ticker, period_str, resolved_url)
                self._emit('url_ingested', ticker=ticker, period=period_str,
                           fetch_strategy='template', text_chars=len(html_fields["raw_text"]), url=resolved_url)
            except Exception as e:
                log.error("Failed to insert template report %s %s: %s", ticker, period_str, e, exc_info=True)
                self._emit('url_error', ticker=ticker, level='WARNING', period=period_str,
                           fetch_strategy='template', url=resolved_url, error=str(e))
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

        summary = IngestSummary()
        ir_url = company.get('ir_url', '')
        pr_base_url = company.get('pr_base_url', '')
        ticker = company['ticker']
        start_year = company.get('pr_start_year')
        # When False, skip the "all already ingested" early-exit so a backfill
        # run can traverse pages that are fully covered in the DB and reach
        # older history beyond them.
        stop_on_all_seen = company.get('stop_on_all_seen', True)

        # Paginate through listing pages (?page=N) until we hit a page with no
        # new production PRs. All-already-ingested pages signal we've reached
        # covered history — stop early to avoid traversing the entire archive
        # on every incremental run.
        page = 1
        while True:
            page_url = ir_url if page == 1 else f"{ir_url}?page={page}"
            self._emit('page_fetch', ticker=ticker, url=page_url, page=page)
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
                if start_year and period.year < start_year:
                    log.debug("Skipping PR before pr_start_year=%d: %s %s", start_year, ticker, title)
                    continue
                if not href.startswith("http"):
                    if not pr_base_url:
                        log.debug("Skipping relative href with empty pr_base_url: %s", href)
                        continue
                    full_url = pr_base_url + href
                else:
                    full_url = href
                production_links.append((title, full_url, period))

            new_count = 0
            for title, full_url, period in production_links:
                period_str = period.strftime("%Y-%m-%d")

                full_url = canonical_url(full_url)
                url_hash = hashlib.sha256(full_url.encode()).hexdigest()
                if self.db.report_exists_by_url_hash(url_hash, ticker):
                    log.debug("Already ingested IR PR by URL: %s %s", ticker, full_url)
                    self._emit('url_skipped', ticker=ticker, reason='duplicate_url', url=full_url, period=period_str)
                    continue

                new_count += 1
                pr_resp = _fetch_with_rate_limit(full_url, self.session)
                if pr_resp is None:
                    summary.errors += 1
                    continue

                html_fields = make_html_report_fields(pr_resp.text)
                content_hash = simhash_text(html_fields["raw_text"][:5000])
                dupes = self.db.find_near_duplicates(content_hash, ticker)
                if dupes:
                    log.warning(
                        "Near-duplicate content detected for %s, skipping insert (matched report id=%s)",
                        ticker, dupes[0]["id"],
                    )
                    self._emit('url_skipped', ticker=ticker, level='WARNING', reason='near_duplicate', url=full_url, period=period_str, matched_id=dupes[0]['id'])
                    continue
                report = {
                    "ticker": ticker,
                    "report_date": period_str,
                    "published_date": None,
                    "source_type": "ir_press_release",
                    "source_url": full_url,
                    **html_fields,
                    "parsed_at": datetime.now(timezone.utc).isoformat(),
                    "content_simhash": content_hash,
                    "fetch_strategy": "index",
                }
                try:
                    self.db.insert_report(report)
                    summary.reports_ingested += 1
                    log.info("Ingested index PR: %s %s from %s", ticker, period_str, full_url)
                    self._emit('url_ingested', ticker=ticker, period=period_str,
                               title=title, fetch_strategy='index', text_chars=len(html_fields["raw_text"]), url=full_url)
                except Exception as e:
                    log.error("Failed to insert IR report %s %s: %s", ticker, period_str, e, exc_info=True)
                    self._emit('url_error', ticker=ticker, level='WARNING', period=period_str,
                               title=title, fetch_strategy='index', url=full_url, error=str(e))
                    summary.errors += 1

            if stop_on_all_seen and new_count == 0 and production_links:
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

    def _scrape_playwright(self, company: dict):
        """
        Use Playwright (headless Chromium) to render the IR index page, then
        download production PR pages. Intended for JS-heavy IR sites.
        Stores raw text only — extraction is handled by the extraction pipeline.
        Returns IngestSummary.
        """
        from miner_types import IngestSummary

        summary = IngestSummary()
        ticker = company["ticker"]
        ir_url = company.get("ir_url", "")
        pr_base_url = company.get("pr_base_url", "")

        if not ir_url:
            log.error("%s: ir_url not set for playwright scraper", ticker)
            summary.errors += 1
            return summary

        if sync_playwright is None:
            log.error("%s: playwright is not installed; cannot use playwright scraper mode", ticker)
            summary.errors += 1
            return summary

        try:
            with sync_playwright() as pw:
                browser = pw.chromium.launch(headless=True)
                context = browser.new_context(extra_http_headers=_HEADERS)
                page = context.new_page()
                page.goto(ir_url, wait_until="domcontentloaded", timeout=30000)
                html = page.content()
                if is_bot_challenge_page(html):
                    log.warning("%s: bot challenge detected on playwright listing page %s", ticker, ir_url)
                    summary.errors += 1
                    return summary

                soup = BeautifulSoup(html, "lxml")
                links = soup.find_all("a", href=True)

                for link in links:
                    title = link.get_text(separator=" ", strip=True)
                    href = link["href"]
                    if not is_production_pr(title):
                        continue
                    period = infer_period_from_pr_title(title)
                    if period is None:
                        log.debug("Could not infer period from PR title: %s", title)
                        continue

                    if not href.startswith("http"):
                        if not pr_base_url:
                            log.debug("Skipping relative href with empty pr_base_url: %s", href)
                            continue
                        full_url = canonical_url(pr_base_url + href)
                    else:
                        full_url = canonical_url(href)

                    url_hash = hashlib.sha256(full_url.encode()).hexdigest()
                    if self.db.report_exists_by_url_hash(url_hash, ticker):
                        log.debug("Already ingested playwright PR by URL: %s %s", ticker, full_url)
                        self._emit('url_skipped', ticker=ticker, reason='duplicate_url', url=full_url, period=period.strftime("%Y-%m-%d"))
                        continue

                    page.goto(full_url, wait_until="domcontentloaded", timeout=30000)
                    pr_html = page.content()
                    if is_bot_challenge_page(pr_html):
                        log.warning("%s: bot challenge detected on playwright article %s", ticker, full_url)
                        summary.errors += 1
                        continue
                    html_fields = make_html_report_fields(pr_html)
                    period_str = period.strftime("%Y-%m-%d")
                    content_hash = simhash_text(html_fields["raw_text"][:5000])
                    dupes = self.db.find_near_duplicates(content_hash, ticker)
                    if dupes:
                        log.debug(
                            "Near-duplicate content detected for %s, skipping playwright insert "
                            "(matched report id=%s)",
                            ticker, dupes[0]["id"],
                        )
                        self._emit('url_skipped', ticker=ticker, level='WARNING', reason='near_duplicate', url=full_url, period=period_str, matched_id=dupes[0]['id'])
                        continue
                    report = {
                        "ticker": ticker,
                        "report_date": period_str,
                        "published_date": None,
                        "source_type": "ir_press_release",
                        "source_url": full_url,
                        **html_fields,
                        "parsed_at": datetime.now(timezone.utc).isoformat(),
                        "content_simhash": content_hash,
                        "fetch_strategy": "playwright",
                    }
                    try:
                        self.db.insert_report(report)
                        summary.reports_ingested += 1
                        log.info("Ingested playwright PR: %s %s from %s", ticker, period_str, full_url)
                        self._emit('url_ingested', ticker=ticker, period=period_str,
                                   title=title, fetch_strategy='playwright', text_chars=len(html_fields["raw_text"]), url=full_url)
                    except Exception as e:
                        log.error(
                            "Failed to insert playwright report %s %s: %s",
                            ticker, period_str, e, exc_info=True,
                        )
                        self._emit('url_error', ticker=ticker, level='WARNING', period=period_str,
                                   title=title, fetch_strategy='playwright', url=full_url, error=str(e))
                        summary.errors += 1

        except Exception as e:
            log.error("%s: playwright scrape failed: %s", ticker, e, exc_info=True)
            summary.errors += 1

        return summary

    def _scrape_drupal_year(self, company: dict):
        """
        Scrape IR listing pages powered by a Drupal year-filter widget.

        Fetches the base IR page to extract a fresh form_build_id and
        widget_id, then iterates each year from pr_start_year to the
        current year, submitting the year-filter GET request for each.
        Parses each filtered page for production press release links.

        Required company fields:
            ir_url        — base listing page URL
            pr_base_url   — base for resolving relative hrefs
            pr_start_year — first year to scrape
        """
        import re as _re
        from miner_types import IngestSummary
        from urllib.parse import urlencode

        summary = IngestSummary()
        ticker = company['ticker']
        ir_url = company.get('ir_url', '')
        pr_base_url = company.get('pr_base_url', '')
        start_year = company.get('pr_start_year')

        if not ir_url:
            log.error("%s: ir_url not set for drupal_year mode", ticker)
            summary.errors += 1
            return summary
        if not start_year:
            log.error("%s: pr_start_year not set for drupal_year mode", ticker)
            summary.errors += 1
            return summary

        # Fetch base page to extract Drupal form tokens
        self._emit('page_fetch', ticker=ticker, url=ir_url, page=0)
        base_resp = _fetch_with_rate_limit(ir_url, self.session)
        if base_resp is None:
            log.error("%s: could not fetch base IR page %s", ticker, ir_url)
            summary.errors += 1
            return summary

        def _extract_tokens(html_text):
            soup = BeautifulSoup(html_text, 'lxml')
            token_input = soup.find('input', {'name': 'form_build_id'})
            form_build_id = token_input['value'] if token_input else None
            widget_id = None
            for inp in soup.find_all('input', {'type': 'hidden'}):
                if '_widget_id' in (inp.get('name') or ''):
                    widget_id = inp.get('value')
                    break
            if not widget_id:
                for tag in soup.find_all(['select', 'input']):
                    m = _re.match(r'^([a-f0-9]{40,})_year', tag.get('name') or '')
                    if m:
                        widget_id = m.group(1)
                        break
            return form_build_id, widget_id, soup

        form_build_id, widget_id, _ = _extract_tokens(base_resp.text)

        if not widget_id:
            log.error("%s: could not extract drupal widget_id from %s", ticker, ir_url)
            summary.errors += 1
            return summary

        log.info("%s: drupal_year widget_id=%s...", ticker, widget_id[:16])

        current_year = date.today().year

        for year in range(start_year, current_year + 1):
            params = {
                f'{widget_id}_year[value]': str(year),
                'op': 'Filter',
                f'{widget_id}_widget_id': widget_id,
                'form_id': 'widget_form_base',
            }
            if form_build_id:
                params['form_build_id'] = form_build_id

            year_url = f"{ir_url}?{urlencode(params)}"
            self._emit('page_fetch', ticker=ticker, url=year_url, page=year)

            resp = _fetch_with_rate_limit(year_url, self.session)
            if resp is None:
                log.warning("%s: no response for year %d filter", ticker, year)
                continue

            # Refresh token for subsequent year requests
            fresh_build_id, _, year_soup = _extract_tokens(resp.text)
            if fresh_build_id:
                form_build_id = fresh_build_id

            for link in year_soup.find_all('a', href=True):
                title = link.get_text(separator=' ', strip=True)
                href = link['href']
                check_text = f"{title} {href.replace('-', ' ').replace('/', ' ')}"

                if not is_mining_activity_pr(check_text):
                    continue
                period = infer_period_from_text(check_text)
                if period is None:
                    log.debug("Could not infer period from PR title: %s", title)
                    continue

                full_url = href if href.startswith('http') else pr_base_url + href
                full_url = canonical_url(full_url)
                period_str = period.strftime('%Y-%m-%d')

                url_hash = hashlib.sha256(full_url.encode()).hexdigest()
                if self.db.report_exists_by_url_hash(url_hash, ticker):
                    log.debug("Already ingested drupal_year PR by URL: %s %s", ticker, full_url)
                    self._emit('url_skipped', ticker=ticker, reason='duplicate_url',
                               url=full_url, period=period_str)
                    continue

                pr_resp = _fetch_with_rate_limit(full_url, self.session)
                if pr_resp is None:
                    summary.errors += 1
                    continue

                published_date = infer_published_date_from_html(pr_resp.text)
                page_period = period
                if page_period is None:
                    page_period = infer_period_from_text(pr_resp.text[:5000])
                if page_period is None and published_date:
                    pub = datetime.fromisoformat(published_date).date()
                    page_period = date(pub.year, pub.month, 1)
                if page_period is None:
                    log.debug("%s: could not infer period for drupal_year PR %s", ticker, full_url)
                    continue
                period_str = page_period.strftime('%Y-%m-%d')

                inserted = self._insert_ir_report(
                    ticker=ticker,
                    period=page_period,
                    source_url=full_url,
                    html_text=pr_resp.text,
                    fetch_strategy='drupal_year',
                    summary=summary,
                    title=title,
                    published_date=published_date,
                )
                if inserted:
                    log.info("Ingested drupal_year PR: %s %s from %s",
                             ticker, period_str, full_url)

        return summary
