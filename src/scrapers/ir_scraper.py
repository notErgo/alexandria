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
import threading
import xml.etree.ElementTree as ET
from concurrent.futures import ThreadPoolExecutor, as_completed
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
from scrapers.request_throttle import HostThrottle

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

# Markers that appear ONLY in actual bot-challenge interstitial pages.
# Do NOT include "/cdn-cgi/challenge-platform/" here — Cloudflare embeds that
# path as a beacon script on every page it proxies, even successful ones.
# A real challenge page is detected by the explicit user-visible strings below,
# or by the cf-mitigated header / 403+cloudflare checks in is_bot_challenge_page().
_BOT_CHALLENGE_MARKERS: tuple[str, ...] = (
    "just a moment",
    "enable javascript and cookies to continue",
    "performing security verification",
)


def _get_pr_start_date(company: dict):
    """Return the configured press release start date for a company.

    Checks 'pr_start_date' (YYYY-MM-DD string) first.
    Falls back to 'pr_start_year' (int) -> date(year, 1, 1) for backward compat.
    Returns None if neither is configured.
    """
    from datetime import date as _date
    raw = company.get('pr_start_date')
    if raw:
        try:
            return _date.fromisoformat(str(raw)[:10])
        except ValueError:
            pass
    year = company.get('pr_start_year')
    if year:
        try:
            return _date(int(year), 1, 1)
        except (TypeError, ValueError):
            pass
    return None

_MONTH_NAMES = [
    "", "january", "february", "march", "april", "may", "june",
    "july", "august", "september", "october", "november", "december",
]

_DISCOVERY_FETCH_WORKERS = 4
_INFLIGHT_URL_LOCK = threading.Lock()
_INFLIGHT_URLS: set[tuple[str, str]] = set()


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

    if ticker == "CLSK":
        pages = []
        if ir_url:
            pages.append(ir_url)
            pages.extend(f"{ir_url}?page={page}" for page in range(2, 11))
        return pages

    if ticker == "RIOT":
        # The main RIOT IR page is largely shell HTML; the WordPress author
        # archive exposes the full historical article list in stable pagination.
        return [
            f"https://www.riotplatforms.com/author/b2ieverest456dfghbs/page/{page}/"
            for page in range(1, 61)
        ]

    if ticker == "HIVE":
        # hivedigitaltechnologies.com/news/ is a single unpaginated page containing
        # all press releases from 2021 to present. ?page=N returns identical content;
        # only one fetch is needed.
        return [ir_url] if ir_url else []

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

    def _is_official_ir_detail_link(full_url: str) -> bool:
        parsed = urlparse(full_url)
        if not allowed_host or parsed.netloc.lower() != allowed_host:
            return False
        path = (parsed.path or "").lower()
        detail_markers = (
            "/detail/",
            "/news-release-details/",
            "/news/news-details/",
            "/press-releases/detail/",
        )
        return any(marker in path for marker in detail_markers)

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
        lower_check = check_text.lower()
        has_exclusion = any(kw in lower_check for kw in _EXCLUSION_KEYWORDS)
        if has_exclusion:
            continue
        if not (_is_official_ir_detail_link(full_url) or is_mining_activity_pr(check_text)):
            continue

        if full_url in seen:
            continue
        seen.add(full_url)
        results.append((title, full_url, infer_period_from_text(check_text)))

    return results


# Domains whose IR listing pages are JS-rendered (Equisolve/Q4 widgets).
# requests returns a static shell with no article links; Playwright is required.
# NOTE: investors.corescientific.com (CORZ) and investors.terawulf.com (WULF) are
# intentionally NOT here — both listing pages are server-rendered with plain HTTP
# and support ?page=N pagination (confirmed 2026-03-16). Only escalate to Playwright
# if a future site change breaks plain-request listing fetches.
_JS_RENDERED_DOMAINS: frozenset = frozenset({
    "investors.cleanspark.com",
    # www.hivedigitaltechnologies.com is intentionally NOT here: detail page URLs
    # return full server-rendered article HTML with plain requests (~23KB).
    # Only the /news/ listing page is a Vue.js SPA shell; the linked slugs are SSR.
})

# Domains that require curl-cffi Chrome TLS impersonation.
# These sites use Cloudflare bot-protection that blocks both plain requests
# (connection timeout) and headless Playwright (HTTP/2 protocol error).
# curl-cffi mimics the full TLS fingerprint of a real Chrome browser.
_CURL_CFFI_DOMAINS: frozenset = frozenset({
    "ir.bitdeer.com",         # Bitdeer — Cloudflare blocks requests + Playwright; curl-cffi chrome124 works
    "bitdeer.gcs-web.com",    # Bitdeer GCS fallback domain — same Cloudflare protection as ir.bitdeer.com
    "investor.bitfarms.com",  # Bitfarms — Cloudflare embeds /cdn-cgi/ in page, triggering false-positive
                              # bot-challenge detection; curl-cffi Chrome impersonation fetches cleanly
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
    # Equisolve/Q4: numbered pager_button — handled dynamically in Strategy 4
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


def _parse_int_or_none(value: str | None) -> Optional[int]:
    """Best-effort integer parse for pager button labels."""
    if value is None:
        return None
    value = value.strip()
    if not value.isdigit():
        return None
    try:
        return int(value)
    except ValueError:
        return None


def _infer_listing_year_from_html(html_text: str) -> Optional[int]:
    """Infer the dominant listing year from HTML content."""
    candidates = [int(year) for year in re.findall(r"/news-details/(\d{4})/", html_text or "")]
    if not candidates:
        candidates = [int(year) for year in re.findall(r"\b(20\d{2})\b", html_text or "")]
    if not candidates:
        return None
    return max(candidates)


def _playwright_select_year_filter(pw_page, year: int) -> bool:
    """Best-effort switch of a JS-rendered year filter/select control."""
    try:
        result = pw_page.evaluate(
            """(yearStr) => {
                const normalized = String(yearStr).trim();

                for (const select of Array.from(document.querySelectorAll('select'))) {
                    const option = Array.from(select.options || []).find(
                        (opt) => String(opt.textContent || '').trim() === normalized
                              || String(opt.value || '').trim() === normalized
                    );
                    if (!option) continue;
                    if (select.value !== option.value) {
                        select.value = option.value;
                        select.dispatchEvent(new Event('input', { bubbles: true }));
                        select.dispatchEvent(new Event('change', { bubbles: true }));
                    }
                    return true;
                }

                const clickableSelectors = [
                    'button', 'a', '[role=\"button\"]', '[role=\"option\"]',
                    'li', '.dropdown-item', '.filter-item'
                ];
                for (const selector of clickableSelectors) {
                    for (const node of Array.from(document.querySelectorAll(selector))) {
                        const text = String(node.textContent || '').trim();
                        if (text !== normalized) continue;
                        node.dispatchEvent(new MouseEvent('click', { bubbles: true, cancelable: true }));
                        return true;
                    }
                }
                return false;
            }""",
            str(year),
        )
        if not result:
            return False
        pw_page.wait_for_timeout(2500)
        return True
    except Exception:
        return False


def _playwright_collect_all_pages(url: str, max_pages: int = 30, min_year: Optional[int] = None) -> list[str]:
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
    page_hashes: set[str] = set()
    try:
        with sync_playwright() as pw:
            browser = pw.chromium.launch(headless=True)
            context = browser.new_context(
                user_agent=_HEADERS["User-Agent"],
                extra_http_headers={"Accept-Language": _HEADERS["Accept-Language"]},
            )
            pw_page = context.new_page()
            # "domcontentloaded" fires as soon as the HTML is parsed — before scripts
            # run and before any AJAX.  We then wait explicitly for the JS widget to
            # render (signalled by a pager_button or news article appearing).  This
            # avoids the networkidle / load timeouts caused by Equisolve's continuous
            # background AJAX calls.
            pw_page.goto(url, wait_until="domcontentloaded", timeout=60000)
            try:
                pw_page.wait_for_selector(
                    "button.pager_button, article, .press-release-item, .news-item",
                    timeout=15000,
                )
            except Exception:
                pass
            pw_page.wait_for_timeout(1500)

            def _collect_current_listing_pages(expected_year: Optional[int] = None) -> None:
                for page_num in range(1, max_pages + 1):
                    html = pw_page.content()
                    if is_bot_challenge_page(html):
                        log.warning("Playwright got bot challenge on page %d of %s", page_num, url)
                        break
                    page_digest = hashlib.sha256(html.encode("utf-8", errors="replace")).hexdigest()
                    if page_digest in page_hashes:
                        break
                    page_hashes.add(page_digest)
                    listing_year = _infer_listing_year_from_html(html)
                    if expected_year and listing_year and listing_year != expected_year:
                        log.debug(
                            "Playwright year switch mismatch for %s: expected %s got %s",
                            url, expected_year, listing_year,
                        )
                    log.info("Playwright paginated fetch: page %d of %s (%d chars)", page_num, url, len(html))
                    pages_html.append(html)

                    next_clicked = False

                    # Primary strategy: Equisolve pager_button pattern.
                    try:
                        all_pager_btns = pw_page.locator("button.pager_button").all()
                        active_idx = None
                        active_page_num = None
                        for i, b in enumerate(all_pager_btns):
                            try:
                                if b.get_attribute("aria-current", timeout=300) == "true":
                                    active_idx = i
                                    active_page_num = _parse_int_or_none(b.text_content() or "")
                                    break
                            except Exception:
                                continue
                        next_btn = None
                        if active_page_num is not None:
                            next_page_num = active_page_num + 1
                            for b in all_pager_btns:
                                try:
                                    if _parse_int_or_none(b.text_content() or "") != next_page_num:
                                        continue
                                    next_btn = b
                                    break
                                except Exception:
                                    continue
                        if next_btn is None and active_idx is not None:
                            for candidate in all_pager_btns[active_idx + 1:]:
                                try:
                                    if _parse_int_or_none(candidate.text_content() or "") is None:
                                        continue
                                    next_btn = candidate
                                    break
                                except Exception:
                                    continue
                        if next_btn is not None and next_btn.is_visible(timeout=1000):
                            next_btn_text = (next_btn.text_content() or "").strip()
                            next_btn.click()
                            try:
                                pw_page.wait_for_function(
                                    """(txt) => {
                                        const btns = document.querySelectorAll('button.pager_button');
                                        for (const b of btns) {
                                            if (b.getAttribute('aria-current') === 'true'
                                                    && b.textContent.trim() === txt) return true;
                                        }
                                        return false;
                                    }""",
                                    arg=next_btn_text,
                                    timeout=10000,
                                )
                            except Exception:
                                pass
                            pw_page.wait_for_timeout(2000)
                            next_clicked = True
                            log.debug("Playwright: advanced to page button '%s'", next_btn_text)
                    except Exception:
                        pass

                    if not next_clicked:
                        for selector in _NEXT_PAGE_SELECTORS:
                            try:
                                btn = pw_page.locator(selector).first
                                if btn.is_visible(timeout=500):
                                    btn.click()
                                    pw_page.wait_for_timeout(3000)
                                    next_clicked = True
                                    break
                            except Exception:
                                continue

                    if not next_clicked:
                        for next_text in ("Next", ">", "›", "»"):
                            try:
                                btn = pw_page.locator(
                                    f"a:has-text('{next_text}'), button:has-text('{next_text}')"
                                ).first
                                if btn.is_visible(timeout=500):
                                    btn.click()
                                    pw_page.wait_for_timeout(3000)
                                    next_clicked = True
                                    break
                            except Exception:
                                continue

                    if not next_clicked:
                        log.info(
                            "Playwright: no next page control found after page %d of %s — stopping",
                            page_num, url,
                        )
                        break

            current_year = _infer_listing_year_from_html(pw_page.content()) or date.today().year
            _collect_current_listing_pages(expected_year=current_year)
            if min_year is not None:
                for year in range(current_year - 1, min_year - 1, -1):
                    if not _playwright_select_year_filter(pw_page, year):
                        log.debug("Playwright: no year filter control found for %s on %s", year, url)
                        continue
                    log.info("Playwright: switched listing filter to year %s for %s", year, url)
                    _collect_current_listing_pages(expected_year=year)

            browser.close()
    except Exception as exc:
        log.warning("Playwright paginated fetch failed for %s: %s", url, exc)
    log.info("Playwright paginated fetch: %d pages collected for %s", len(pages_html), url)
    return pages_html


def _fetch_with_curl_cffi(url: str) -> Optional[requests.Response]:
    """Fetch using curl-cffi Chrome TLS impersonation for Cloudflare-protected sites.

    Returns a mock requests.Response with .text and .status_code populated,
    or None on failure. Falls back gracefully if curl-cffi is not installed.
    """
    try:
        from curl_cffi import requests as _cffi_req
    except ImportError:
        log.warning("curl_cffi not installed — cannot fetch Cloudflare-protected URL %s", url)
        return None
    try:
        r = _cffi_req.get(url, impersonate="chrome124", timeout=20)
        if r.status_code in (400, 404):
            log.debug("curl-cffi: %d for %s", r.status_code, url)
            return None
        if not (200 <= r.status_code < 300):
            log.warning("curl-cffi: unexpected status %d for %s", r.status_code, url)
            return None
        mock = requests.models.Response()
        mock.status_code = r.status_code
        mock._content = r.content
        mock.encoding = r.encoding or "utf-8"
        return mock
    except Exception as e:
        log.warning("curl-cffi fetch failed for %s: %s", url, e)
        return None


def _fetch_with_rate_limit(
    url: str,
    session: requests.Session,
    *,
    throttle: Optional[HostThrottle] = None,
    cooldown_seconds: float = 0.0,
    max_retries: int = 1,
) -> Optional[requests.Response]:
    """
    Fetch URL with rate limiting and a browser-like User-Agent.

    Returns None on 400/404 (expected "not found" conditions — log at DEBUG).
    Returns None on timeout or network error (log at WARNING).
    Raises on 5xx (unexpected server errors that should surface).
    For bot-protected domains (prnewswire), falls back to Playwright.
    """
    from urllib.parse import urlparse as _urlparse
    host = _urlparse(url).netloc.lower()
    if host in _CURL_CFFI_DOMAINS:
        if throttle is not None:
            throttle.wait(host)
        else:
            time.sleep(IR_REQUEST_DELAY_SECONDS)
        log.debug("Cloudflare-protected domain — using curl-cffi for %s", url)
        return _fetch_with_curl_cffi(url)
    if host in _JS_RENDERED_DOMAINS:
        if throttle is not None:
            throttle.wait(host)
        else:
            time.sleep(IR_REQUEST_DELAY_SECONDS)
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

    attempts = max(1, int(max_retries))
    for attempt in range(attempts):
        if throttle is not None:
            throttle.wait(host)
        else:
            time.sleep(IR_REQUEST_DELAY_SECONDS)
        try:
            resp = DEFAULT_RETRY_POLICY.execute(session.get, url, timeout=15, headers=_HEADERS)
            if is_bot_challenge_page(resp.text, resp.headers, resp.status_code):
                log.warning("Bot challenge detected for %s (status=%s) — trying Playwright", url, resp.status_code)
                cooldown = throttle.penalize(host, cooldown_seconds) if throttle is not None else 0.0
                if cooldown > 0:
                    log.info("IR throttle cooldown host=%s seconds=%.2f", host, cooldown)
                html = _fetch_with_playwright(url)
                if html is None:
                    return None
                # Wrap in a mock response-like object so callers get .text
                resp._content = html.encode("utf-8", errors="replace")
                resp.status_code = 200
                return resp
            if resp.status_code in (400, 404):
                log.debug("IR page returned %d for %s", resp.status_code, url)
                return None
            if resp.status_code in (403, 429):
                cooldown = throttle.penalize(host, cooldown_seconds) if throttle is not None else 0.0
                log.warning("IR rate-limited host=%s status=%s url=%s attempt=%d/%d cooldown=%.2f",
                            host, resp.status_code, url, attempt + 1, attempts, cooldown)
                if attempt + 1 < attempts:
                    continue
                return None
            resp.raise_for_status()
            return resp
        except CircuitOpenError as e:
            log.warning("Circuit open, skipping fetch for %s: %s", url, e)
            return None
        except requests.Timeout:
            cooldown = throttle.penalize(host, cooldown_seconds) if throttle is not None else 0.0
            log.warning("Timeout fetching %s (attempt %d/%d cooldown=%.2f)", url, attempt + 1, attempts, cooldown)
            if attempt + 1 < attempts:
                continue
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
    _request_throttle: Optional[HostThrottle] = None
    _host_backoff_seconds: float = 0.0
    _max_retries: int = 1

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

    def _fetch(self, url: str) -> Optional[requests.Response]:
        return _fetch_with_rate_limit(
            url,
            self.session,
            throttle=self._request_throttle,
            cooldown_seconds=self._host_backoff_seconds,
            max_retries=self._max_retries,
        )

    def _fetch_isolated(self, url: str) -> Optional[requests.Response]:
        """Fetch using a fresh Session for thread-pooled discovery detail pages.

        Detail pages are typically static HTML even on hosts whose listing pages
        require JS rendering.  We always attempt plain requests first so that
        _JS_RENDERED_DOMAINS detail fetches do not incur Playwright overhead
        (and Playwright timeouts on sites like CORZ that block headless browsers
        on detail pages but serve them fine via plain HTTP).

        Falls back to full routing (curl-cffi or Playwright) only when the plain
        request itself fails or returns a bot-challenge interstitial.
        """
        from urllib.parse import urlparse as _up
        host = _up(url).netloc.lower()

        # curl-cffi domains need TLS impersonation even for detail pages.
        if host in _CURL_CFFI_DOMAINS:
            return _fetch_with_curl_cffi(url)

        with requests.Session() as session:
            try:
                resp = DEFAULT_RETRY_POLICY.execute(
                    session.get, url, timeout=15, headers=_HEADERS
                )
                if not is_bot_challenge_page(resp.text, dict(resp.headers), resp.status_code):
                    return resp
                # Bot-challenge on plain request — fall through to full routing
            except Exception:
                pass
            return _fetch_with_rate_limit(
                url,
                session,
                throttle=self._request_throttle,
                cooldown_seconds=self._host_backoff_seconds,
                max_retries=self._max_retries,
            )

    def _claim_url(self, ticker: str, source_url: str) -> tuple[bool, str, str, str]:
        """Claim a canonical URL for fetch/ingest within this Python process."""
        canonical_source_url = canonical_url(source_url)
        if not canonical_source_url:
            return False, "", "", "empty_url"

        url_hash = hashlib.sha256(canonical_source_url.encode()).hexdigest()
        claim_key = (ticker.upper(), url_hash)
        with _INFLIGHT_URL_LOCK:
            if claim_key in _INFLIGHT_URLS:
                return False, canonical_source_url, url_hash, "inflight_duplicate"
            if self.db.report_exists_by_url_hash(url_hash, ticker):
                return False, canonical_source_url, url_hash, "duplicate_url"
            _INFLIGHT_URLS.add(claim_key)
        return True, canonical_source_url, url_hash, "claimed"

    def _release_url(self, ticker: str, url_hash: str) -> None:
        """Release a previously claimed URL."""
        if not url_hash:
            return
        with _INFLIGHT_URL_LOCK:
            _INFLIGHT_URLS.discard((ticker.upper(), url_hash))

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
        url_claimed: bool = False,
        url_hash: str | None = None,
    ) -> bool:
        """Insert one IR report with shared dedup/content handling."""
        period_str = period.strftime("%Y-%m-%d")
        source_url = canonical_url(source_url)
        if url_claimed:
            url_hash = url_hash or hashlib.sha256(source_url.encode()).hexdigest()
        else:
            claimed, source_url, url_hash, reason = self._claim_url(ticker, source_url)
            if not claimed:
                log.debug("Skipping %s PR by URL (%s): %s %s", fetch_strategy, reason, ticker, source_url)
                self._emit('url_skipped', ticker=ticker, reason=reason, url=source_url, period=period_str)
                return False

        try:
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
            import sqlite3 as _sqlite3
            if isinstance(e, _sqlite3.IntegrityError) and "UNIQUE constraint" in str(e):
                # Race condition or hash mismatch between pre-insert check and insert.
                # Treat as a duplicate — not an error.
                log.warning("Skipping duplicate %s report (UNIQUE constraint): %s %s", fetch_strategy, ticker, source_url)
                self._emit('url_skipped', ticker=ticker, reason='duplicate_url', url=source_url, period=period_str)
                return False
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
        finally:
            self._release_url(ticker, url_hash)

    def _scrape_rss(self, company: dict):
        """
        Fetch RSS feed, filter production PRs, download each press release HTML.
        Stores raw text only — extraction is handled by the extraction pipeline.
        Returns IngestSummary.
        """
        from miner_types import IngestSummary

        summary = IngestSummary()
        ticker = company["ticker"]
        rss_url = company.get("rss_url")
        if not rss_url:
            log.error("%s: rss_url not set", ticker)
            summary.errors += 1
            return summary

        resp = self._fetch(rss_url)
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

            claimed, pr_url, url_hash, reason = self._claim_url(ticker, item["link"])
            if not claimed:
                log.debug("Skipping RSS PR by URL (%s): %s %s", reason, ticker, pr_url)
                self._emit('url_skipped', ticker=ticker, reason=reason, url=pr_url, period=period_str)
                continue

            try:
                page = self._fetch(pr_url)
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
            finally:
                self._release_url(ticker, url_hash)

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
        start_date = _get_pr_start_date(company)
        if not company.get("ir_url"):
            log.error("%s: ir_url not set for discovery mode", ticker)
            summary.errors += 1
            return summary
        if not start_date:
            log.error("%s: pr_start_date not set for discovery mode", ticker)
            summary.errors += 1
            return summary
        start_year = start_date.year
        start_month = date(start_date.year, start_date.month, 1)

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
            page_htmls_js = _playwright_collect_all_pages(
                ir_url,
                min_year=start_year,
            )
            page_sources: list[tuple[str, str]] = [
                (html, ir_url) for html in page_htmls_js
            ]
            if page_htmls_js:
                static_fallback_urls: list[str] = []
            else:
                log.warning("%s: Playwright got 0 pages from %s", ticker, ir_url)
                static_fallback_urls = []
        else:
            page_sources = []
            static_fallback_urls = page_urls

        # Probe first available page early so unreachable sites fail visibly
        # rather than completing with 0 reports and scraper_status='ok'.
        if not page_sources and not static_fallback_urls:
            # JS-rendered path got 0 pages and no non-IR fallback URLs exist.
            log.warning("%s: discovery has no pages to fetch (Playwright got 0, no fallback)", ticker)
            summary.errors += 1
            return summary
        if not page_sources and static_fallback_urls:
            _initial_resp = self._fetch(static_fallback_urls[0])
            if _initial_resp is None:
                log.warning(
                    "%s: discovery initial page unreachable: %s",
                    ticker,
                    static_fallback_urls[0],
                )
                summary.errors += 1
                return summary

        seen_urls: set[str] = set()
        consecutive_empty = 0
        found_any = False
        discovered_candidates: list[dict] = []

        def _process_page(html_text: str, page_url: str, page_idx: int) -> bool:
            """Process one listing page; returns True if at least one recent candidate found."""
            nonlocal consecutive_empty, found_any
            candidates = discovery_links_from_html(company, html_text, page_url)
            if not candidates:
                consecutive_empty += 1
                self._emit(
                    'page_fetch_done',
                    ticker=ticker,
                    url=page_url,
                    page=page_idx,
                    candidate_count=0,
                    new_candidates=0,
                    has_recent=0,
                    consecutive_empty=consecutive_empty,
                )
                return False

            consecutive_empty = 0
            found_any = True
            page_has_recent = False
            new_candidates = 0

            for title, full_url, hinted_period in candidates:
                if full_url in seen_urls:
                    continue
                seen_urls.add(full_url)

                period_hint = hinted_period or infer_period_from_text(full_url)
                if period_hint and date(period_hint.year, period_hint.month, 1) >= start_month:
                    page_has_recent = True

                if period_hint and date(period_hint.year, period_hint.month, 1) < start_month:
                    continue

                url_hash = hashlib.sha256(full_url.encode()).hexdigest()
                if self.db.report_exists_by_url_hash(url_hash, ticker):
                    period_str = period_hint.strftime("%Y-%m-%d") if period_hint else ""
                    self._emit('url_skipped', ticker=ticker, reason='duplicate_url', url=full_url, period=period_str)
                    continue

                discovered_candidates.append({
                    "title": title,
                    "full_url": full_url,
                    "period_hint": period_hint,
                    "sequence": len(discovered_candidates),
                })
                new_candidates += 1

            self._emit(
                'page_fetch_done',
                ticker=ticker,
                url=page_url,
                page=page_idx,
                candidate_count=len(candidates),
                new_candidates=new_candidates,
                has_recent=int(page_has_recent),
                consecutive_empty=consecutive_empty,
            )

            return page_has_recent

        def _fetch_candidate(candidate: dict) -> dict:
            sequence = int(candidate["sequence"]) + 1
            total = len(discovered_candidates)
            self._emit(
                'detail_fetch_start',
                ticker=ticker,
                url=candidate["full_url"],
                sequence=sequence,
                total=total,
            )
            claimed, full_url, url_hash, reason = self._claim_url(ticker, candidate["full_url"])
            if not claimed:
                period_hint = candidate.get("period_hint")
                period_str = period_hint.strftime("%Y-%m-%d") if period_hint else ""
                self._emit(
                    'detail_fetch_done',
                    ticker=ticker,
                    url=full_url,
                    sequence=sequence,
                    total=total,
                    fetched=0,
                    reason=reason,
                )
                self._emit('url_skipped', ticker=ticker, reason=reason, url=full_url, period=period_str)
                return {
                    **candidate,
                    "full_url": full_url,
                    "claim_reason": reason,
                    "url_hash": url_hash,
                    "response_text": None,
                }
            pr_resp = self._fetch_isolated(full_url)
            self._emit(
                'detail_fetch_done',
                ticker=ticker,
                url=full_url,
                sequence=sequence,
                total=total,
                fetched=int(pr_resp is not None),
            )
            return {
                **candidate,
                "full_url": full_url,
                "claim_reason": "claimed",
                "url_hash": url_hash,
                "response_text": None if pr_resp is None else pr_resp.text,
            }

        # Process Playwright-paginated pages first (JS-rendered listing)
        for page_idx, (html_text, page_url) in enumerate(page_sources, start=1):
            self._emit('page_fetch', ticker=ticker, url=page_url, page=page_idx)
            _process_page(html_text, page_url, page_idx)

        # Process static URL pages (regular requests, or prnewswire fallback)
        page_offset = len(page_sources)
        for page_idx, page_url in enumerate(static_fallback_urls, start=page_offset + 1):
            self._emit('page_fetch', ticker=ticker, url=page_url, page=page_idx)
            resp = self._fetch(page_url)
            if resp is None:
                consecutive_empty += 1
                if found_any and consecutive_empty >= 3:
                    break
                continue

            has_recent = _process_page(resp.text, page_url, page_idx)
            if found_any and not has_recent:
                break

        if not discovered_candidates:
            self._emit('detail_fetch_stage_done', ticker=ticker, total=0, fetched=0)
            return summary

        fetch_workers = max(1, min(_DISCOVERY_FETCH_WORKERS, len(discovered_candidates)))
        self._emit(
            'detail_fetch_stage_start',
            ticker=ticker,
            total=len(discovered_candidates),
            workers=fetch_workers,
        )
        with ThreadPoolExecutor(max_workers=fetch_workers) as pool:
            futures = [pool.submit(_fetch_candidate, candidate) for candidate in discovered_candidates]
            fetched_candidates = []
            completed = 0
            fetched_ok = 0
            for fut in as_completed(futures):
                candidate = fut.result()
                fetched_candidates.append(candidate)
                completed += 1
                if candidate.get("response_text") is not None:
                    fetched_ok += 1
                self._emit(
                    'detail_fetch_progress',
                    ticker=ticker,
                    completed=completed,
                    total=len(discovered_candidates),
                    fetched=fetched_ok,
                )

        self._emit(
            'detail_fetch_stage_done',
            ticker=ticker,
            total=len(discovered_candidates),
            fetched=sum(1 for c in fetched_candidates if c.get("response_text") is not None),
        )

        for candidate in sorted(fetched_candidates, key=lambda item: item["sequence"]):
            pr_text = candidate.get("response_text")
            url_hash = candidate.get("url_hash") or ""
            claim_reason = candidate.get("claim_reason") or ""
            if pr_text is None:
                if claim_reason == "claimed" and url_hash:
                    self._release_url(ticker, url_hash)
                    summary.errors += 1
                continue

            title = candidate["title"]
            full_url = candidate["full_url"]
            period_hint = candidate["period_hint"]
            page_period = (
                period_hint
                or infer_period_from_text(title)
                or infer_period_from_text(full_url)
                or infer_period_from_text(pr_text[:15000])
            )
            published_date = infer_published_date_from_html(pr_text)
            if page_period is None and published_date:
                pub = datetime.fromisoformat(published_date).date()
                page_period = date(pub.year, pub.month, 1)

            if page_period is None:
                log.debug("%s: could not infer period for discovered PR %s", ticker, full_url)
                self._release_url(ticker, url_hash)
                continue
            if date(page_period.year, page_period.month, 1) < start_month:
                self._release_url(ticker, url_hash)
                continue

            inserted = self._insert_ir_report(
                ticker=ticker,
                period=page_period,
                source_url=full_url,
                html_text=pr_text,
                fetch_strategy="discovery",
                summary=summary,
                title=title,
                published_date=published_date,
                source_type="ir_press_release",
                url_claimed=True,
                url_hash=url_hash,
            )
            if inserted:
                log.info("Ingested discovery PR: %s %s from %s", ticker, page_period, full_url)

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
        start_date = _get_pr_start_date(company)
        # When True, bypass the fast-forward so all months from pr_start_date are
        # attempted even when the DB already holds recent IR reports.  URL-hash
        # dedup prevents re-inserting already-ingested months.
        backfill_mode = company.get("backfill_mode", False)

        if not url_template:
            log.error("%s: url_template not set but scrape_mode is 'template'", ticker)
            summary.errors += 1
            return summary
        if not start_date:
            log.error("%s: pr_start_date not set", ticker)
            summary.errors += 1
            return summary
        start_year = start_date.year
        # Normalize to month-start so a day > 1 (e.g. "2020-12-10") doesn't
        # create a sub-month dead zone — December 2020 must be included when
        # pr_start_date is 2020-12-10.
        start_month = date(start_date.year, start_date.month, 1)

        # Walk months from the LATER OF (pr_start_date month-start, latest IR period in DB)
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
                # Never go before pr_start_date month-start — respect the configured floor
                current = max(start_month, start_from)
                log.info("%s: fast-forwarding to %s (latest IR: %s)", ticker, current, latest)
            except (ValueError, IndexError) as e:
                log.warning(
                    "%s: could not parse latest_ir_period %r (%s) — starting from %s",
                    ticker, latest, e, start_month,
                )
                current = start_month
        else:
            current = start_month
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
                claimed, url, url_hash, reason = self._claim_url(ticker, url)
                if not claimed:
                    log.debug("Skipping template PR by URL (%s): %s %s", reason, ticker, url)
                    self._emit('url_skipped', ticker=ticker, reason=reason, url=url, period=period_str)
                    duplicate_hit = True
                    resolved_url = url
                    break

                candidate_resp = None
                try:
                    candidate_resp = self._fetch(url)
                    if candidate_resp is None:
                        continue
                    resp = candidate_resp
                    resolved_url = url
                    break
                finally:
                    if candidate_resp is None:
                        self._release_url(ticker, url_hash)

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
                if resolved_url:
                    self._release_url(ticker, hashlib.sha256(resolved_url.encode()).hexdigest())
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
            resolved_url_hash = hashlib.sha256(resolved_url.encode()).hexdigest()
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
            finally:
                self._release_url(ticker, resolved_url_hash)

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
        start_date = _get_pr_start_date(company)
        start_year = start_date.year if start_date else None
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
            resp = self._fetch(page_url)
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
                full_url = urljoin(page_url, href)
                period = infer_period_from_pr_title(title) or infer_period_from_text(full_url)
                if period is not None and start_year and period.year < start_year:
                    log.debug("Skipping PR before pr_start_date year=%d: %s %s", start_year, ticker, title)
                    continue
                # period may still be None (e.g. slug has month but no year);
                # keep these links so the detail page body can resolve the year.
                production_links.append((title, full_url, period))

            new_count = 0
            for title, full_url, period in production_links:
                claimed, full_url, url_hash, reason = self._claim_url(ticker, full_url)
                if not claimed:
                    _period_str = period.strftime("%Y-%m-%d") if period else "unknown"
                    log.debug("Skipping index PR by URL (%s): %s %s", reason, ticker, full_url)
                    self._emit('url_skipped', ticker=ticker, reason=reason, url=full_url, period=_period_str)
                    continue

                new_count += 1
                try:
                    pr_resp = self._fetch(full_url)
                    if pr_resp is None:
                        summary.errors += 1
                        continue

                    html_fields = make_html_report_fields(pr_resp.text)

                    # If period could not be inferred from title/URL, try the article body.
                    if period is None:
                        period = infer_period_from_text(html_fields["raw_text"])
                    if period is None:
                        log.debug("Could not infer period for index PR (title+URL+body): %s", title)
                        continue
                    if start_year and period.year < start_year:
                        log.debug("Skipping PR before pr_start_date year=%d: %s %s", start_year, ticker, title)
                        continue

                    period_str = period.strftime("%Y-%m-%d")
                    content_hash = simhash_text(html_fields["raw_text"][:5000])
                    # Near-duplicate check is intentionally skipped in index mode.
                    # URL-based dedup (_claim_url) is sufficient here: each entry on the
                    # listing page is a distinct article at a canonical URL. Simhash
                    # near-dedup causes false positives on sites whose PR detail pages
                    # share large navigation blocks that dominate the first 5000 chars.
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
                    self.db.insert_report(report)
                    summary.reports_ingested += 1
                    log.info("Ingested index PR: %s %s from %s", ticker, period_str, full_url)
                    self._emit('url_ingested', ticker=ticker, period=period_str,
                               title=title, fetch_strategy='index', text_chars=len(html_fields["raw_text"]), url=full_url)
                except Exception as e:
                    _ps = period.strftime("%Y-%m-%d") if period else "unknown"
                    log.error("Failed to insert IR report %s %s: %s", ticker, _ps, e, exc_info=True)
                    self._emit('url_error', ticker=ticker, level='WARNING', period=_ps,
                               title=title, fetch_strategy='index', url=full_url, error=str(e))
                    summary.errors += 1
                finally:
                    self._release_url(ticker, url_hash)

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

                    claimed, full_url, url_hash, reason = self._claim_url(ticker, full_url)
                    if not claimed:
                        log.debug("Skipping playwright PR by URL (%s): %s %s", reason, ticker, full_url)
                        self._emit('url_skipped', ticker=ticker, reason=reason, url=full_url, period=period.strftime("%Y-%m-%d"))
                        continue

                    try:
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
                    finally:
                        self._release_url(ticker, url_hash)

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
        start_date_obj = _get_pr_start_date(company)
        start_year = start_date_obj.year if start_date_obj else None

        if not ir_url:
            log.error("%s: ir_url not set for drupal_year mode", ticker)
            summary.errors += 1
            return summary
        if not start_year:
            log.error("%s: pr_start_date not set for drupal_year mode", ticker)
            summary.errors += 1
            return summary

        # Fetch base page to extract Drupal form tokens
        self._emit('page_fetch', ticker=ticker, url=ir_url, page=0)
        base_resp = self._fetch(ir_url)
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

        form_build_id, widget_id, base_soup = _extract_tokens(base_resp.text)

        if not widget_id:
            # Dump form inputs and select elements to help diagnose widget format
            try:
                from bs4 import BeautifulSoup as _BS
                _soup = _BS(base_resp.text, 'lxml')
                _inputs = [
                    f"{t.name}[name={t.get('name')!r} type={t.get('type')!r}]"
                    for t in _soup.find_all(['input', 'select'])
                    if t.get('name')
                ]
                log.error(
                    "%s: could not extract drupal widget_id from %s — form elements found: %s",
                    ticker, ir_url, _inputs[:30],
                )
            except Exception:
                log.error("%s: could not extract drupal widget_id from %s", ticker, ir_url)
            summary.errors += 1
            return summary

        log.info("%s: drupal_year widget_id=%s...", ticker, widget_id[:16])

        current_year = date.today().year

        for year in range(start_year, current_year + 1):
            # Paginate within each year filter: page=0 is the first page;
            # continue incrementing until a page yields zero new candidates.
            for page_offset in range(50):  # hard cap against runaway pagination
                params = {
                    f'{widget_id}_year[value]': str(year),
                    'op': 'Filter',
                    f'{widget_id}_widget_id': widget_id,
                    'form_id': 'widget_form_base',
                }
                if form_build_id:
                    params['form_build_id'] = form_build_id
                if page_offset > 0:
                    params['page'] = str(page_offset)

                year_url = f"{ir_url}?{urlencode(params)}"
                self._emit('page_fetch', ticker=ticker, url=year_url, page=year)

                resp = self._fetch(year_url)
                if resp is None:
                    log.warning("%s: no response for year %d page %d filter", ticker, year, page_offset)
                    break

                # Refresh token for subsequent requests
                fresh_build_id, _, year_soup = _extract_tokens(resp.text)
                if fresh_build_id:
                    form_build_id = fresh_build_id

                candidates_on_page = 0
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

                    # Count all matching candidates regardless of dedup status so the
                    # pagination break only fires on genuinely empty pages.
                    candidates_on_page += 1

                    full_url = href if href.startswith('http') else urljoin(year_url, href)
                    full_url = canonical_url(full_url)
                    period_str = period.strftime('%Y-%m-%d')

                    claimed, full_url, url_hash, reason = self._claim_url(ticker, full_url)
                    if not claimed:
                        log.debug("Skipping drupal_year PR by URL (%s): %s %s", reason, ticker, full_url)
                        self._emit('url_skipped', ticker=ticker, reason=reason,
                                   url=full_url, period=period_str)
                        continue
                    try:
                        pr_resp = self._fetch(full_url)
                        if pr_resp is None:
                            summary.errors += 1
                            continue

                        published_date = infer_published_date_from_html(pr_resp.text)
                        page_period = period
                        if page_period is None:
                            page_period = infer_period_from_text(pr_resp.text[:15000])
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
                            url_claimed=True,
                            url_hash=url_hash,
                        )
                        if inserted:
                            log.info("Ingested drupal_year PR: %s %s from %s",
                                     ticker, period_str, full_url)
                    finally:
                        self._release_url(ticker, url_hash)

                if candidates_on_page == 0:
                    break  # page has no matching links — end of results for this year

        return summary
