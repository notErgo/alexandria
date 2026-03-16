"""
Diagnostic probe for IR scraper modes — no DB writes.

Probes BITF and BTDR (drupal_year) and WULF and CORZ (discovery).
Reports traversal depth, candidate counts, and actionable recommendations.

Usage:
    venv/bin/python3 scripts/probe_ir_scraper.py BITF BTDR WULF CORZ
    venv/bin/python3 scripts/probe_ir_scraper.py WULF
"""
import json
import os
import sys
import time
from datetime import date, datetime
from pathlib import Path
from typing import Optional
from urllib.parse import urlparse, urlencode

import requests
from bs4 import BeautifulSoup

# Ensure src/ is on the path
ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "src"))

from scrapers.ir_scraper import (
    _JS_RENDERED_DOMAINS,
    _CURL_CFFI_DOMAINS,
    _playwright_collect_all_pages,
    discovery_links_from_html,
    discovery_page_urls_for_company,
    is_mining_activity_pr,
)

COMPANIES_JSON = ROOT / "config" / "companies.json"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
}


def load_company(ticker: str) -> dict:
    with open(COMPANIES_JSON) as f:
        companies = json.load(f)
    for c in companies:
        if c["ticker"] == ticker:
            return c
    raise ValueError(f"Ticker {ticker} not found in companies.json")


def _get(session: requests.Session, url: str, timeout: int = 20) -> Optional[requests.Response]:
    try:
        resp = session.get(url, headers=HEADERS, timeout=timeout, allow_redirects=True)
        return resp
    except Exception as e:
        print(f"    ERROR fetching {url}: {e}")
        return None


def probe_drupal_year(ticker: str, company: dict) -> None:
    print(f"\n{'='*60}")
    print(f"PROBE: {ticker} (drupal_year)")
    print(f"{'='*60}")

    ir_url = company.get("ir_url", "")
    pr_base_url = company.get("pr_base_url", "")
    pr_start_date_str = company.get("pr_start_date", "")

    if not ir_url:
        print("  FAIL: ir_url not set")
        return
    if not pr_start_date_str:
        print("  FAIL: pr_start_date not set")
        return

    start_year = int(pr_start_date_str[:4])
    current_year = date.today().year

    session = requests.Session()
    ir_host = urlparse(ir_url).netloc.lower()
    use_playwright = ir_host in _JS_RENDERED_DOMAINS
    use_curl_cffi  = ir_host in _CURL_CFFI_DOMAINS

    if use_playwright:
        print(f"  NOTE: {ir_host} is JS-rendered — using Playwright for page fetches")
    elif use_curl_cffi:
        print(f"  NOTE: {ir_host} is Cloudflare-protected — using curl-cffi chrome124")

    def _fetch_page(url: str) -> Optional[str]:
        if use_curl_cffi:
            try:
                from curl_cffi import requests as _cffi
                rv = _cffi.get(url, impersonate="chrome124", timeout=20)
                return rv.text if rv.status_code == 200 else None
            except Exception as e:
                print(f"    curl-cffi error: {e}")
                return None
        if use_playwright:
            from scrapers.ir_scraper import _fetch_with_playwright
            return _fetch_with_playwright(url)
        resp = _get(session, url)
        return resp.text if resp and resp.ok else None

    # Fetch base page
    t0 = time.monotonic()
    base_html = _fetch_page(ir_url)
    elapsed = int((time.monotonic() - t0) * 1000)
    if not base_html:
        method = "curl-cffi" if use_curl_cffi else ("Playwright" if use_playwright else "HTTP")
        print(f"  FAIL: {method} returned nothing for base page ({elapsed}ms)")
        return
    print(f"  base page: OK ({elapsed}ms), {len(base_html)} chars")

    # Extract Drupal tokens
    soup = BeautifulSoup(base_html, "lxml")
    token_input = soup.find("input", {"name": "form_build_id"})
    form_build_id = token_input["value"] if token_input else None

    widget_id = None
    for inp in soup.find_all("input", {"type": "hidden"}):
        if "_widget_id" in (inp.get("name") or ""):
            widget_id = inp.get("value")
            break

    if widget_id:
        print(f"  widget_id: {widget_id[:32]}...")
    else:
        print("  WARN: widget_id not found in base page")
    if form_build_id:
        print(f"  form_build_id: {form_build_id[:16]}...")
    else:
        print("  WARN: form_build_id not found in base page")

    # Iterate each year using correct Drupal param format (same as live scraper)
    total_candidates = 0
    years_with_results = 0
    years_with_zero = []

    for year in range(start_year, current_year + 1):
        if not widget_id:
            print(f"  {year}: SKIP (no widget_id)")
            continue
        params = {
            f'{widget_id}_year[value]': str(year),
            'op': 'Filter',
            f'{widget_id}_widget_id': widget_id,
            'form_id': 'widget_form_base',
        }
        if form_build_id:
            params['form_build_id'] = form_build_id

        year_url = f"{ir_url}?{urlencode(params)}"
        t0 = time.monotonic()
        html = _fetch_page(year_url)
        elapsed = int((time.monotonic() - t0) * 1000)

        if html is None:
            print(f"  {year}: FAIL no response ({elapsed}ms)")
            continue

        yr_soup = BeautifulSoup(html, "lxml")

        # Refresh token for next iteration (same as live scraper)
        t = yr_soup.find("input", {"name": "form_build_id"})
        if t:
            form_build_id = t["value"]

        # Use same check_text logic as _scrape_drupal_year (title + URL slug)
        candidates = []
        for a in yr_soup.find_all("a", href=True):
            title = a.get_text(separator=' ', strip=True)
            href = a["href"]
            check_text = f"{title} {href.replace('-', ' ').replace('/', ' ')}"
            if is_mining_activity_pr(check_text) and title not in ("PDF Version", "Read more", ""):
                full_url = href if href.startswith("http") else pr_base_url.rstrip("/") + href
                candidates.append((title, full_url))

        total_candidates += len(candidates)
        sample = [t for t, _ in candidates[:2]]

        if candidates:
            years_with_results += 1
            print(f"  {year}: {len(html)} chars ({elapsed}ms) — {len(candidates)} candidates | {sample}")
        else:
            years_with_zero.append(year)
            print(f"  {year}: {len(html)} chars ({elapsed}ms) — 0 candidates (possible gap)")

        time.sleep(0.3)

    print(f"\n  SUMMARY: years {start_year}-{current_year}, {years_with_results} years with results, {total_candidates} total candidates")
    if years_with_zero:
        print(f"  years with 0 candidates: {years_with_zero}")
    if total_candidates == 0:
        print("  RECOMMENDATION: check if Drupal widget tokens are correct or IR URL has changed")
    else:
        print("  RECOMMENDATION: drupal_year mode looks functional")


def probe_discovery(ticker: str, company: dict) -> None:
    print(f"\n{'='*60}")
    print(f"PROBE: {ticker} (discovery)")
    print(f"{'='*60}")

    ir_url = company.get("ir_url", "")
    pr_start_date_str = company.get("pr_start_date", "")

    if not ir_url:
        print("  FAIL: ir_url not set")
        return

    session = requests.Session()

    # Plain HTTP check
    t0 = time.monotonic()
    resp = _get(session, ir_url)
    elapsed = int((time.monotonic() - t0) * 1000)
    if resp is None:
        print(f"  plain HTTP: no response ({elapsed}ms)")
    elif resp.ok:
        print(f"  plain HTTP: HTTP {resp.status_code} ({elapsed}ms), {len(resp.text)} chars")
    else:
        print(f"  plain HTTP: HTTP {resp.status_code} ({elapsed}ms)")

    ir_host = urlparse(ir_url).netloc.lower()
    use_playwright = ir_host in _JS_RENDERED_DOMAINS

    page_sources = []
    if use_playwright:
        start_year = int(pr_start_date_str[:4]) if pr_start_date_str else 2020
        print(f"  JS-rendered domain — running Playwright pagination (min_year={start_year})...")
        t0 = time.monotonic()
        page_htmls = _playwright_collect_all_pages(ir_url, min_year=start_year)
        elapsed = int((time.monotonic() - t0) * 1000)
        print(f"  Playwright returned {len(page_htmls)} page(s) in {elapsed}ms")
        page_sources = [(html, ir_url) for html in page_htmls]
        if not page_htmls:
            print("  WARN: Playwright got 0 pages")
    else:
        page_urls = discovery_page_urls_for_company(company)
        print(f"  static discovery: {len(page_urls)} page URL(s) configured")
        for i, url in enumerate(page_urls[:5], 1):
            t0 = time.monotonic()
            r = _get(session, url)
            elapsed = int((time.monotonic() - t0) * 1000)
            if r and r.ok:
                page_sources.append((r.text, url))
                print(f"  page {i}: HTTP {r.status_code} ({elapsed}ms) — {url[:80]}")
            else:
                status = r.status_code if r else "no response"
                print(f"  page {i}: FAIL {status} ({elapsed}ms) — {url[:80]}")
            time.sleep(0.3)

    # Count candidates across all pages
    total_candidates = 0
    earliest_period = None
    latest_period = None

    for html, page_url in page_sources:
        links = discovery_links_from_html(company, html, page_url)
        for _url, title, period in links:
            total_candidates += 1
            if period:
                if earliest_period is None or period < earliest_period:
                    earliest_period = period
                if latest_period is None or period > latest_period:
                    latest_period = period
        if links:
            sample = [(t, str(p)) for _u, t, p in links[:2]]
            print(f"  page {page_url[:60]}: {len(links)} candidates | {sample}")
        else:
            print(f"  page {page_url[:60]}: 0 candidates")

    print(f"\n  SUMMARY: {len(page_sources)} page(s) fetched, {total_candidates} total candidates")
    if earliest_period:
        print(f"  period range: {earliest_period} to {latest_period}")

    if total_candidates == 0 and not page_sources:
        # Scan for RSS/feed URLs in the base page
        if resp and resp.ok:
            soup = BeautifulSoup(resp.text, "lxml")
            feed_links = []
            for a in soup.find_all("a", href=True):
                href = a["href"]
                lower = href.lower()
                if any(kw in lower for kw in ["rss", "feed", "globenewswire", "prnewswire"]):
                    feed_links.append(href)
            rss_tags = soup.find_all("link", {"type": "application/rss+xml"})
            for t in rss_tags:
                feed_links.append(t.get("href", ""))
            if feed_links:
                print(f"  RSS/feed URLs found in base page: {feed_links}")
                print("  RECOMMENDATION: consider adding one of these as rss_url for fallback")
            else:
                print("  RECOMMENDATION: no RSS/feed URLs found — needs manual investigation")
        else:
            print("  RECOMMENDATION: IR page unreachable; probe with VPN or check domain change")
    elif total_candidates == 0:
        print("  RECOMMENDATION: pages fetched but 0 mining-activity candidates — check is_mining_activity_pr() patterns")
    else:
        print("  RECOMMENDATION: discovery mode looks functional")


def main() -> None:
    tickers = [t.upper() for t in sys.argv[1:]] if len(sys.argv) > 1 else ["BITF", "BTDR", "WULF", "CORZ"]

    mode_map = {
        "BITF": "drupal_year",
        "BTDR": "drupal_year",
        "WULF": "discovery",
        "CORZ": "discovery",
    }

    for ticker in tickers:
        try:
            company = load_company(ticker)
        except ValueError as e:
            print(f"\n{e}")
            continue

        mode = company.get("scraper_mode", "skip")
        # Allow overriding via mode_map for tickers being probed by plan
        effective_mode = mode_map.get(ticker, mode)

        if effective_mode == "drupal_year":
            probe_drupal_year(ticker, company)
        elif effective_mode == "discovery":
            probe_discovery(ticker, company)
        else:
            print(f"\n{ticker}: mode={mode} — no probe implemented for this mode")


if __name__ == "__main__":
    main()
