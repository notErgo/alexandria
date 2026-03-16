"""Observer + scout orchestration for wire/IR discovery and scraping."""
from __future__ import annotations

import json
import logging
import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional
from urllib.parse import parse_qsl, urlencode, urljoin, urlparse, urlunparse

from infra.text_utils import make_html_report_fields

import requests
from bs4 import BeautifulSoup

from config import DATA_DIR
from infra.db import MinerDB
from scrapers.ir_scraper import (
    IRScraper,
    expand_url_template,
    infer_period_from_pr_title,
    is_production_pr,
    parse_rss_feed,
)
from scrapers.primitive_feedback import run_feedback_loop
from scrapers.primitive_registry import load_active_primitives, match_primitive, materialize_year_filter_source
from scrapers.source_contract import merge_contracts, normalize_contract, validate_contract

log = logging.getLogger("miners.scrapers.observer_swarm")

SOURCE_FAMILIES = ("ir",)
_HEADERS = {
    "User-Agent": (
        "Hermeneutic Research Platform/1.0 "
        "(Observer Scout; contact@hermeneutic.io)"
    )
}


@dataclass
class ScoutConfig:
    max_attempts_per_source: int = 5
    max_consecutive_no_yield: int = 3
    execute_scrape: bool = True
    request_timeout_seconds: int = 15
    run_feedback_loop: bool = True
    apply_validated_primitives: bool = False


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _chunk(values: list[str], n_chunks: int) -> list[list[str]]:
    if n_chunks <= 1:
        return [values]
    out = [[] for _ in range(n_chunks)]
    for i, v in enumerate(values):
        out[i % n_chunks].append(v)
    return [x for x in out if x]


def _count_reports_for_ticker(db: MinerDB, ticker: str) -> int:
    with db._get_connection() as conn:  # noqa: SLF001
        row = conn.execute("SELECT COUNT(*) FROM reports WHERE ticker=?", (ticker,)).fetchone()
    return int(row[0] if row else 0)


def _write_json(path: Path, data: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(data, indent=2))
    tmp.replace(path)


class ScoutWorker:
    def __init__(
        self,
        run_id: str,
        scout_id: str,
        db: Optional[MinerDB],
        session: requests.Session,
        output_dir: Path,
        config: ScoutConfig,
        companies_by_ticker: Optional[dict[str, dict]] = None,
    ) -> None:
        self.run_id = run_id
        self.scout_id = scout_id
        self.db = db
        self.session = session
        self.output_dir = output_dir
        self.config = config
        self.companies_by_ticker = companies_by_ticker or {}
        self.primitives = load_active_primitives()
        self._last_discovery_reason: dict[str, str] = {}
        self._domain_backoff_until: dict[str, float] = {}
        self._primitive_gaps: dict[str, list[dict]] = {}

    def _fetch(self, url: str) -> Optional[requests.Response]:
        tries = 3
        host = urlparse(url).netloc
        now = time.time()
        wait_until = self._domain_backoff_until.get(host, 0.0)
        if wait_until > now:
            time.sleep(min(2.0, wait_until - now))
        for i in range(tries):
            try:
                return self.session.get(
                    url,
                    timeout=self.config.request_timeout_seconds + (i * 5),
                    allow_redirects=True,
                    headers=_HEADERS,
                )
            except requests.RequestException:
                # short per-domain backoff to reduce repeated timeout storms
                self._domain_backoff_until[host] = time.time() + min(6.0, 1.5 * (i + 1))
                if i == tries - 1:
                    return None
        return None

    def _get_company(self, ticker: str) -> Optional[dict]:
        ticker = ticker.upper()
        if ticker in self.companies_by_ticker:
            return dict(self.companies_by_ticker[ticker])
        if self.db is None:
            return None
        return self.db.get_company(ticker)

    def _candidate_urls(self, company: dict, family: str) -> list[str]:
        urls: list[str] = []
        ir_url = (company.get("ir_url") or "").strip()
        rss_url = (company.get("rss_url") or "").strip()
        name = company.get("name") or company.get("ticker")

        if family == "ir":
            if ir_url:
                urls.append(ir_url)
            if rss_url:
                urls.append(rss_url)
            tmpl = (company.get("url_template") or "").strip()
            if tmpl:
                try:
                    urls.append(expand_url_template(tmpl, datetime.now(timezone.utc).date().replace(day=1)))
                except Exception:
                    pass
        return [u for u in urls if u]

    def _discover_source(self, company: dict, family: str) -> Optional[dict]:
        self._last_discovery_reason[family] = "unknown"
        include = ["production", "operations update", "bitcoin production"]
        exclude = ["earnings", "10-q", "10-k", "financial results"]
        candidates = self._candidate_urls(company, family)
        if not candidates:
            self._last_discovery_reason[family] = "no_candidates"
            return None

        # Config-seeded year-filter contract fallback. Useful when IR host is
        # intermittently unreachable but a validated query template is known.
        if family == "ir":
            # Activated data-driven primitive path (validated via feedback loop).
            prim = match_primitive(self.primitives, family=family, entry_url=company.get("ir_url") or "")
            if prim is not None:
                src = materialize_year_filter_source(
                    prim,
                    entry_url=company.get("ir_url") or candidates[0],
                    include=include,
                    exclude=exclude,
                )
                if src is not None:
                    return src

            seeded_template = (company.get("year_filter_template") or "").strip()
            seeded_years = [str(y).strip() for y in (company.get("year_filter_years") or []) if str(y).strip()]
            if seeded_template and seeded_years:
                year_urls = [seeded_template.replace("{year}", y) for y in seeded_years]
                return {
                    "family": family,
                    "entry_url": company.get("ir_url") or year_urls[0],
                    "discovery_method": "year_filter",
                    "url_pattern": seeded_template,
                    "pagination": {"type": "query", "template": "?page={n}", "max_page": 25},
                    "date_extraction": {"strategy": "title_regex", "pattern": "month year"},
                    "filters": {"include": include, "exclude": exclude},
                    "validation": {"http_ok": True, "parse_ok": False, "sample_count": 0},
                    "confidence": 0.65,
                    "evidence_urls": year_urls[:3],
                    "year_filter": {
                        "select_name": "",
                        "years": seeded_years,
                        "year_urls": year_urls,
                        "url_template": seeded_template,
                        "sample_count": 0,
                        "heuristic_only": True,
                        "seeded": True,
                    },
                }

        for entry_url in candidates:
            resp = self._fetch(entry_url)
            if resp is None:
                self._last_discovery_reason[family] = "timeout_or_network"
                continue
            if resp.status_code >= 400:
                self._last_discovery_reason[family] = f"http_{resp.status_code}"
                continue

            text = resp.text or ""
            lower = text.lower()
            is_feed = ("<rss" in lower) or ("<feed" in lower)
            evidence = [entry_url]

            if is_feed:
                items = parse_rss_feed(text)
                sample_count = 0
                for item in items[:20]:
                    title = item.get("title") or ""
                    if is_production_pr(title):
                        sample_count += 1
                        evidence.append(item.get("link") or "")
                return {
                    "family": family,
                    "entry_url": entry_url,
                    "discovery_method": "rss",
                    "url_pattern": "",
                    "pagination": {"type": "none", "template": "", "max_page": 0},
                    "date_extraction": {"strategy": "title_regex", "pattern": "month year"},
                    "filters": {"include": include, "exclude": exclude},
                    "validation": {"http_ok": True, "parse_ok": True, "sample_count": sample_count},
                    "confidence": 0.9 if sample_count > 0 else 0.75,
                    "evidence_urls": [x for x in evidence if x],
                }

            soup = BeautifulSoup(text, "lxml")

            # Some IR sites (e.g., investor.bitfarms.com) expose a year dropdown
            # that changes listing pagination and content globally.
            if family == "ir":
                year_filter = self._discover_year_filter(entry_url, soup, text)
                if year_filter is not None:
                    evidence.extend(year_filter["year_urls"][:3])
                    sample_count = int(year_filter["sample_count"])
                    heuristic_only = bool(year_filter.get("heuristic_only", False))
                    parse_ok = (sample_count > 0) or (not heuristic_only)
                    return {
                        "family": family,
                        "entry_url": entry_url,
                        "discovery_method": "year_filter",
                        "url_pattern": year_filter["url_template"],
                        "pagination": {"type": "query", "template": "?page={n}", "max_page": 25},
                        "date_extraction": {"strategy": "title_regex", "pattern": "month year"},
                        "filters": {"include": include, "exclude": exclude},
                        "validation": {
                            "http_ok": True,
                            "parse_ok": parse_ok,
                            "sample_count": sample_count,
                        },
                        "confidence": 0.9 if sample_count > 0 else (0.7 if not heuristic_only else 0.6),
                        "evidence_urls": [x for x in evidence if x],
                        "year_filter": year_filter,
                    }
                gaps = self._detect_primitive_gaps(entry_url, text, family="ir")
                if gaps:
                    ticker = str(company.get("ticker") or "").upper()
                    self._primitive_gaps.setdefault(ticker, []).extend(gaps)
            links = soup.find_all("a", href=True)
            sample_count = 0
            for a in links[:200]:
                label = (a.get_text(" ", strip=True) or "").lower()
                if not label:
                    continue
                if any(kw in label for kw in include) and not any(kw in label for kw in exclude):
                    sample_count += 1
                    href = a["href"]
                    if href.startswith("http"):
                        evidence.append(href)
            method = "index"
            conf = 0.8 if sample_count > 0 else 0.55
            pagination = {"type": "none", "template": "", "max_page": 0}
            if "?page=" in lower or "page=" in lower:
                pagination = {"type": "query", "template": "?page={n}", "max_page": 10}
            return {
                "family": family,
                "entry_url": entry_url,
                "discovery_method": method,
                "url_pattern": "",
                "pagination": pagination,
                "date_extraction": {"strategy": "title_regex", "pattern": "month year"},
                "filters": {"include": include, "exclude": exclude},
                "validation": {"http_ok": True, "parse_ok": True, "sample_count": sample_count},
                "confidence": conf,
                "evidence_urls": [x for x in evidence if x],
            }
        self._last_discovery_reason[family] = self._last_discovery_reason.get(family, "exhausted")
        return None

    def _detect_primitive_gaps(self, entry_url: str, html_text: str, *, family: str) -> list[dict]:
        text = html_text or ""
        if family != "ir":
            return []

        import re as _re

        select_name = ""
        widget_param = ""
        m = _re.search(r"([A-Za-z0-9_]+_year\[value\])", text)
        if m:
            select_name = m.group(1)
        m2 = _re.search(r"([A-Za-z0-9_]+_widget_id)", text)
        if m2:
            widget_param = m2.group(1)
        years = [y for y in sorted(set(_re.findall(r"\b(20\d{2})\b", text)), reverse=True) if 2010 <= int(y) <= datetime.now(timezone.utc).year + 1]

        if select_name:
            return [{
                "family": "ir",
                "kind": "year_filter_widget",
                "entry_url": entry_url,
                "select_name": select_name,
                "widget_param": widget_param,
                "year_hints": years[:8],
            }]
        return []

    def _scan_wire_search(self, entry_url: str, max_pages: int = 4) -> tuple[int, list[str]]:
        sample_count = 0
        evidence: list[str] = []
        parsed = urlparse(entry_url)
        qs = dict(parse_qsl(parsed.query, keep_blank_values=True))
        page_keys = ("page", "p", "pg")
        for i in range(1, max_pages + 1):
            q = dict(qs)
            if i > 1:
                key = "page"
                for k in page_keys:
                    if k in q:
                        key = k
                        break
                q[key] = str(i)
            url = urlunparse((parsed.scheme, parsed.netloc, parsed.path, "", urlencode(q, doseq=True), ""))
            resp = self._fetch(url)
            if resp is None or resp.status_code >= 400:
                continue
            soup = BeautifulSoup(resp.text or "", "lxml")
            c = self._count_production_links(soup)
            if c > 0:
                sample_count += c
                evidence.append(url)
            # stop early if zero-yield after first page and no next link hints
            if i == 1 and c == 0 and "page" not in (resp.text or "").lower():
                break
        return sample_count, evidence

    def _discover_year_filter(self, entry_url: str, soup: BeautifulSoup, html_text: str) -> Optional[dict]:
        select_name = None
        select_tag = None
        for s in soup.find_all("select"):
            name = (s.get("name") or "").strip()
            if "_year[value]" in name or "year[value]" in name:
                select_name = name
                select_tag = s
                break
        heuristic_only = False

        years: list[str] = []
        if select_tag is not None:
            for opt in select_tag.find_all("option"):
                val = (opt.get("value") or "").strip()
                if len(val) == 4 and val.isdigit():
                    years.append(val)
        years = sorted(set(years), reverse=True)

        form = select_tag.find_parent("form") if select_tag is not None else None
        form_fields: dict[str, str] = {}
        if form is not None:
            for inp in form.find_all("input"):
                name = (inp.get("name") or "").strip()
                if not name:
                    continue
                form_fields[name] = (inp.get("value") or "").strip()

        entry = urlparse(entry_url)
        base_query = dict(parse_qsl(entry.query, keep_blank_values=True))
        for k, v in base_query.items():
            form_fields.setdefault(k, v)

        # Heuristic fallback: some pages render year filter controls in JS blobs.
        if not select_name:
            m = re.search(r"([A-Za-z0-9_]+_year\[value\])", html_text or "")
            if m:
                select_name = m.group(1)
            if not select_name:
                m = re.search(r"([A-Za-z0-9_]+)_year%5Bvalue%5D", html_text or "", re.IGNORECASE)
                if m:
                    select_name = f"{m.group(1)}_year[value]"
            if not select_name:
                return None

        if not years:
            years = sorted(set(re.findall(r"\b(20\d{2})\b", html_text or "")), reverse=True)
            years = [y for y in years if 2010 <= int(y) <= datetime.now(timezone.utc).year + 1]
        if not years:
            now = datetime.now(timezone.utc).year
            years = [str(y) for y in range(now, max(2018, now - 8), -1)]
            heuristic_only = True

        has_widget_key = any(k.endswith("_widget_id") for k in form_fields.keys())
        # Bitfarms-style filter widgets often require widget_form_base even when
        # unrelated forms on the page expose a different form_id.
        if has_widget_key:
            form_fields["form_id"] = "widget_form_base"
        elif "form_id" not in form_fields:
            form_fields["form_id"] = "widget_form_base"
        if "op" not in form_fields:
            form_fields["op"] = "Filter"

        if not any(k.endswith("_widget_id") for k in form_fields.keys()):
            m = re.search(r"([A-Za-z0-9_]+_widget_id)", html_text or "")
            if m:
                form_fields[m.group(1)] = ""

        sample_count = 0
        year_urls: list[str] = []
        checked_years = years[: min(4, len(years))]
        for y in checked_years:
            q = dict(form_fields)
            q[select_name] = y
            year_url = urlunparse((entry.scheme, entry.netloc, entry.path, "", urlencode(q, doseq=True), ""))
            year_urls.append(year_url)
            r = self._fetch(year_url)
            if r is None:
                continue
            if r.status_code >= 400:
                continue
            s = BeautifulSoup(r.text or "", "lxml")
            sample_count += self._count_production_links(s)

        template_query = dict(form_fields)
        template_query[select_name] = "{year}"
        url_template = urlunparse((entry.scheme, entry.netloc, entry.path, "", urlencode(template_query, doseq=True), ""))
        return {
            "select_name": select_name,
            "years": years,
            "year_urls": year_urls,
            "url_template": url_template,
            "sample_count": sample_count,
            "heuristic_only": heuristic_only,
        }

    def _count_production_links(self, soup: BeautifulSoup) -> int:
        include = ("production", "operations update", "operational update", "bitcoin")
        exclude = ("earnings", "10-q", "10-k", "financial results")
        count = 0
        for a in soup.find_all("a", href=True):
            title = (a.get_text(" ", strip=True) or "").lower()
            if not title:
                continue
            if any(k in title for k in include) and not any(k in title for k in exclude):
                count += 1
        return count

    def _parse_listing_links(self, html: str, company: dict) -> list[tuple[str, str]]:
        include = ("production", "operations update", "operational update", "bitcoin")
        exclude = ("earnings", "10-q", "10-k", "financial results")
        base = company.get("pr_base_url") or company.get("ir_url") or ""
        soup = BeautifulSoup(html or "", "lxml")
        out: list[tuple[str, str]] = []
        for a in soup.find_all("a", href=True):
            title = (a.get_text(" ", strip=True) or "").strip()
            lower = title.lower()
            if not title:
                continue
            if not any(k in lower for k in include) or any(k in lower for k in exclude):
                continue
            href = a["href"]
            full = self._normalize_url(base, href)
            out.append((title, full))
        return out

    def _parse_wire_listing_links(self, html: str, entry_url: str) -> list[tuple[str, str]]:
        include = ("production", "operations update", "operational update", "bitcoin")
        exclude = ("earnings", "10-q", "10-k", "financial results")
        soup = BeautifulSoup(html or "", "lxml")
        out: list[tuple[str, str]] = []
        for a in soup.find_all("a", href=True):
            title = (a.get_text(" ", strip=True) or "").strip()
            href = a["href"]
            full = self._normalize_url(entry_url, href)
            low = f"{title} {full}".lower()
            if not low.strip():
                continue
            if any(k in low for k in include) and not any(k in low for k in exclude):
                out.append((title or full, full))
        # keep order but de-dup by URL
        seen: set[str] = set()
        uniq: list[tuple[str, str]] = []
        for title, url in out:
            if url in seen:
                continue
            seen.add(url)
            uniq.append((title, url))
        return uniq

    def _normalize_url(self, base: str, href: str) -> str:
        href = (href or "").strip()
        if href.startswith("http://") or href.startswith("https://"):
            return href
        if href.startswith("www."):
            return "https://" + href
        # Handle malformed "www.domain.comslug" strings (missing slash after host).
        m = re.match(r"^(www\.[A-Za-z0-9.-]+\.[A-Za-z]{2,})([A-Za-z0-9-].+)$", href)
        if m:
            return f"https://{m.group(1)}/{m.group(2)}"
        return urljoin(base, href)

    def _wire_source_type(self, family: str) -> str:
        return "wire_press_release"

    def _source_url_exists(self, url: str) -> bool:
        if self.db is None:
            return False
        with self.db._get_connection() as conn:  # noqa: SLF001
            row = conn.execute(
                "SELECT 1 FROM reports WHERE source_url=? LIMIT 1",
                (url,),
            ).fetchone()
        return bool(row)

    def _execute_wire_scrape(self, company: dict, source: dict) -> int:
        if self.db is None:
            return 0
        entry_url = str(source.get("entry_url") or "").strip()
        if not entry_url:
            return 0
        family = str(source.get("family") or "").strip().lower()
        source_type = self._wire_source_type(family)
        ticker = company["ticker"]
        ingested = 0

        # RSS shortcut for wire feeds.
        resp0 = self._fetch(entry_url)
        if resp0 is not None and resp0.status_code < 400:
            low = (resp0.text or "").lower()
            if "<rss" in low or "<feed" in low:
                for item in parse_rss_feed(resp0.text or ""):
                    title = item.get("title") or ""
                    if not is_production_pr(title):
                        continue
                    link = str(item.get("link") or "").strip()
                    if not link or self._source_url_exists(link):
                        continue
                    page = self._fetch(link)
                    if page is None or page.status_code >= 400:
                        continue
                    html_fields = make_html_report_fields(page.text)
                    soup = BeautifulSoup(page.text or "", "lxml")
                    h1 = soup.find("h1")
                    h1_text = (h1.get_text(" ", strip=True) if h1 else "") or ""
                    period = (
                        infer_period_from_pr_title(title)
                        or self._infer_period_fallback(title, link)
                        or infer_period_from_pr_title(h1_text)
                        or self._infer_period_fallback(h1_text, link)
                        or self._infer_period_from_body_text(html_fields["raw_text"][:4000])
                    )
                    if period is None:
                        continue
                    period_str = period.strftime("%Y-%m-%d")
                    if self.db.report_exists(ticker, period_str, source_type):
                        continue
                    self.db.insert_report({
                        "ticker": ticker,
                        "report_date": period_str,
                        "published_date": None,
                        "source_type": source_type,
                        "source_url": link,
                        **html_fields,
                        "parsed_at": _utc_now(),
                    })
                    ingested += 1
                return ingested

        parsed = urlparse(entry_url)
        base_q = dict(parse_qsl(parsed.query, keep_blank_values=True))
        page_urls: list[tuple[str, int]] = []
        for i in range(1, 5):
            q = dict(base_q)
            if i > 1:
                key = "page"
                for cand in ("page", "p", "pg"):
                    if cand in q:
                        key = cand
                        break
                q[key] = str(i)
            page_urls.append((urlunparse((parsed.scheme, parsed.netloc, parsed.path, "", urlencode(q, doseq=True), "")), i))

        listing_links: list[tuple[str, str]] = []
        for u, page_idx in page_urls:
            r = self._fetch(u)
            if r is None or r.status_code >= 400:
                continue
            links = self._parse_wire_listing_links(r.text or "", u)
            if not links and page_idx > 1:
                break
            listing_links.extend(links)

        seen: set[str] = set()
        for title, full_url in listing_links:
            if full_url in seen:
                continue
            seen.add(full_url)
            if self._source_url_exists(full_url):
                continue
            page = self._fetch(full_url)
            if page is None or page.status_code >= 400:
                continue
            html_fields = make_html_report_fields(page.text)
            soup = BeautifulSoup(page.text or "", "lxml")
            h1 = soup.find("h1")
            h1_text = (h1.get_text(" ", strip=True) if h1 else "") or ""
            period = (
                infer_period_from_pr_title(title)
                or self._infer_period_fallback(title, full_url)
                or infer_period_from_pr_title(h1_text)
                or self._infer_period_fallback(h1_text, full_url)
                or self._infer_period_from_body_text(html_fields["raw_text"][:4000])
            )
            if period is None:
                continue
            period_str = period.strftime("%Y-%m-%d")
            if self.db.report_exists(ticker, period_str, source_type):
                continue
            self.db.insert_report({
                "ticker": ticker,
                "report_date": period_str,
                "published_date": None,
                "source_type": source_type,
                "source_url": full_url,
                **html_fields,
                "parsed_at": _utc_now(),
            })
            ingested += 1
        return ingested

    def _execute_year_filter_scrape(self, company: dict, source: dict) -> int:
        if self.db is None:
            return 0
        yf = source.get("year_filter") or {}
        year_urls = list(yf.get("year_urls") or [])
        if not year_urls:
            return 0
        ingested = 0
        ticker = company["ticker"]
        for yurl in year_urls:
            # Traverse a few paginated pages per year filter URL.
            for page_idx in range(1, 4):
                q = dict(parse_qsl(urlparse(yurl).query, keep_blank_values=True))
                if page_idx > 1:
                    q["page"] = str(page_idx)
                pg_url = urlunparse((urlparse(yurl).scheme, urlparse(yurl).netloc, urlparse(yurl).path, "", urlencode(q, doseq=True), ""))
                resp = self._fetch(pg_url)
                if resp is None:
                    continue
                if resp.status_code >= 400:
                    continue
                links = self._parse_listing_links(resp.text, company)
                if not links and page_idx > 1:
                    break
                for title, full_url in links:
                    period = infer_period_from_pr_title(title) or self._infer_period_fallback(title, full_url)
                    if period is None:
                        continue
                    period_str = period.strftime("%Y-%m-%d")
                    if self.db.report_exists(ticker, period_str, "ir_press_release"):
                        continue
                    try:
                        page = self._fetch(full_url)
                        if page is None:
                            continue
                        if page.status_code >= 400:
                            continue
                        self.db.insert_report({
                            "ticker": ticker,
                            "report_date": period_str,
                            "published_date": None,
                            "source_type": "ir_press_release",
                            "source_url": full_url,
                            **make_html_report_fields(page.text),
                            "parsed_at": _utc_now(),
                        })
                        ingested += 1
                    except Exception:  # noqa: BLE001
                        continue
        return ingested

    def _infer_period_fallback(self, title: str, url: str) -> Optional[datetime.date]:
        d = infer_period_from_pr_title(title)
        if d is not None:
            return d
        text = f"{title} {url}".lower()
        m = re.search(
            r"(january|february|march|april|may|june|july|august|september|october|november|december)[-_/\\s]+(20\\d{2})",
            text,
        )
        if not m:
            return None
        months = {
            "january": 1, "february": 2, "march": 3, "april": 4, "may": 5, "june": 6,
            "july": 7, "august": 8, "september": 9, "october": 10, "november": 11, "december": 12,
        }
        return datetime(int(m.group(2)), months[m.group(1)], 1).date()

    def _infer_period_from_body_text(self, text: str) -> Optional[datetime.date]:
        low = (text or "").lower()
        m = re.search(
            r"(january|february|march|april|may|june|july|august|september|october|november|december)[-_/\\s,]+(20\\d{2})",
            low,
        )
        if not m:
            return None
        months = {
            "january": 1, "february": 2, "march": 3, "april": 4, "may": 5, "june": 6,
            "july": 7, "august": 8, "september": 9, "october": 10, "november": 11, "december": 12,
        }
        return datetime(int(m.group(2)), months[m.group(1)], 1).date()

    def _execute_scrape_for_source(self, company: dict, source: dict) -> int:
        if self.db is None:
            return 0
        mode = source.get("discovery_method", "")
        family = str(source.get("family") or "").strip().lower()
        if mode == "year_filter":
            return self._execute_year_filter_scrape(company, source)

        pre = _count_reports_for_ticker(self.db, company["ticker"])
        if mode not in {"rss", "index", "template"}:
            return 0

        tmp_company = dict(company)
        tmp_company["scraper_mode"] = mode
        if mode == "rss":
            tmp_company["rss_url"] = source.get("entry_url")
        elif mode == "index":
            tmp_company["ir_url"] = source.get("entry_url")

        scraper = IRScraper(db=self.db, session=self.session)
        scraper.scrape_company(tmp_company)
        post = _count_reports_for_ticker(self.db, company["ticker"])
        return max(0, post - pre)

    def run_ticker(self, ticker: str) -> dict:
        ticker = ticker.upper()
        company = self._get_company(ticker)
        if not company:
            out = {
                "ticker": ticker,
                "run_id": self.run_id,
                "sources": [],
                "status": "blocked",
                "blockers": [f"missing_company:{ticker}"],
                "scout_id": self.scout_id,
                "attempts_by_family": {},
                "reports_ingested": 0,
            }
            return out

        sources: list[dict] = []
        blockers: list[str] = []
        attempts_by_family: dict[str, int] = {}
        reports_ingested = 0

        for family in SOURCE_FAMILIES:
            attempts = 0
            no_yield = 0
            before_count = len(sources)
            while attempts < self.config.max_attempts_per_source and no_yield < self.config.max_consecutive_no_yield:
                attempts += 1
                discovered = self._discover_source(company, family)
                if discovered is None:
                    no_yield += 1
                    continue

                sources.append(discovered)
                if self.config.execute_scrape and discovered.get("validation", {}).get("http_ok"):
                    try:
                        ing = self._execute_scrape_for_source(company, discovered)
                        reports_ingested += int(ing)
                    except Exception as exc:  # noqa: BLE001
                        blockers.append(f"scrape_error:{family}:{exc}")
                # New source discovered resets no-yield streak.
                no_yield = 0
                break
            attempts_by_family[family] = attempts
            if len(sources) == before_count:
                reason = self._last_discovery_reason.get(family, "unknown")
                blockers.append(f"exhausted:{family}:{reason}")

        # Coverage gate: block if no IR source was discovered.
        ir_sources = [s for s in sources if s.get("family") == "ir"]
        if not ir_sources:
            blockers.append("coverage_gate:no_ir_source")

        status = "ready_for_scrape"
        if not sources and blockers:
            status = "blocked"
        elif blockers and sources:
            status = "partially_covered"
        elif not sources:
            status = "exhausted"
        if "coverage_gate:no_ir_and_no_wire_signal" in blockers:
            status = "blocked"

        contract = {
            "ticker": ticker,
            "run_id": self.run_id,
            "sources": sources,
            "status": status,
            "blockers": blockers,
            "scout_id": self.scout_id,
            "attempts_by_family": attempts_by_family,
            "reports_ingested": reports_ingested,
            "primitive_gaps": self._primitive_gaps.get(ticker, []),
            "generated_at": _utc_now(),
        }
        errs = validate_contract(contract)
        if errs:
            contract["status"] = "blocked"
            contract["blockers"] = contract.get("blockers", []) + [f"contract_invalid:{';'.join(errs)}"]
            contract["sources"] = []
        return normalize_contract(contract)


def run_scout_batch(
    *,
    run_id: str,
    scout_id: str,
    tickers: list[str],
    output_dir: Path,
    config: ScoutConfig,
    db_path: Optional[str] = None,
    db: Optional[MinerDB] = None,
    session: Optional[requests.Session] = None,
    companies_by_ticker: Optional[dict[str, dict]] = None,
) -> dict:
    if session is None:
        session = requests.Session()
    if db is None and (config.execute_scrape or companies_by_ticker is None):
        db = MinerDB(db_path or str(Path(DATA_DIR) / "minerdata.db"))

    worker = ScoutWorker(
        run_id=run_id,
        scout_id=scout_id,
        db=db,
        session=session,
        output_dir=output_dir,
        config=config,
        companies_by_ticker=companies_by_ticker,
    )

    contracts: list[dict] = []
    for t in tickers:
        contract = worker.run_ticker(t)
        contracts.append(contract)
        _write_json(output_dir / f"source_contract_{t.upper()}.json", contract)

    status_counts: dict[str, int] = {}
    for c in contracts:
        status_counts[c["status"]] = status_counts.get(c["status"], 0) + 1

    result = {
        "run_id": run_id,
        "scout_id": scout_id,
        "tickers": [t.upper() for t in tickers],
        "status_counts": status_counts,
        "contracts_written": len(contracts),
        "completed_at": _utc_now(),
    }
    _write_json(output_dir / f"scout_{scout_id}_summary.json", result)
    return {"summary": result, "contracts": contracts}


def run_observer(
    *,
    run_id: str,
    tickers: list[str],
    scout_count: int,
    output_dir: Path,
    config: ScoutConfig,
    db_path: Optional[str] = None,
    db: Optional[MinerDB] = None,
    companies_by_ticker: Optional[dict[str, dict]] = None,
) -> dict:
    output_dir.mkdir(parents=True, exist_ok=True)
    if db is None and (config.execute_scrape or companies_by_ticker is None):
        db = MinerDB(db_path or str(Path(DATA_DIR) / "minerdata.db"))

    work = _chunk(sorted([t.upper() for t in tickers]), max(1, scout_count))
    all_contracts: list[dict] = []
    scout_summaries: list[dict] = []

    with ThreadPoolExecutor(max_workers=max(1, scout_count)) as ex:
        futs = []
        for i, subset in enumerate(work, start=1):
            scout_id = f"scout-{i}"
            futs.append(
                ex.submit(
                    run_scout_batch,
                    run_id=run_id,
                    scout_id=scout_id,
                    tickers=subset,
                    output_dir=output_dir,
                    config=config,
                    db_path=db_path,
                    db=db,
                    companies_by_ticker=companies_by_ticker,
                )
            )
        for fut in as_completed(futs):
            result = fut.result()
            scout_summaries.append(result["summary"])
            all_contracts.extend(result["contracts"])

    merged = merge_contracts(all_contracts)
    merged_path = output_dir / "merged_source_contracts.json"
    _write_json(merged_path, {"run_id": run_id, "contracts": merged, "generated_at": _utc_now()})

    status_counts: dict[str, int] = {}
    for c in merged:
        status_counts[c["status"]] = status_counts.get(c["status"], 0) + 1

    summary_md = [
        f"# Observer Ops Summary ({run_id})",
        "",
        f"- Generated: {_utc_now()}",
        f"- Tickers: {len(tickers)}",
        f"- Scouts: {len(work)}",
        "",
        "## Status Counts",
    ]
    for k in sorted(status_counts.keys()):
        summary_md.append(f"- {k}: {status_counts[k]}")
    summary_md.extend(["", "## Ticker Outcomes"])
    for c in sorted(merged, key=lambda x: x["ticker"]):
        summary_md.append(f"- {c['ticker']}: {c['status']} (sources={len(c.get('sources', []))}, blockers={len(c.get('blockers', []))})")
    (output_dir / "observer_ops_summary.md").write_text("\n".join(summary_md) + "\n")

    observer_state = {
        "run_id": run_id,
        "tickers": sorted([t.upper() for t in tickers]),
        "scout_count": len(work),
        "status_counts": status_counts,
        "scouts": scout_summaries,
        "artifacts": {
            "merged_source_contracts": str(merged_path),
            "ops_summary": str(output_dir / "observer_ops_summary.md"),
        },
        "completed_at": _utc_now(),
    }

    if config.run_feedback_loop:
        feedback = run_feedback_loop(
            run_id=run_id,
            output_dir=output_dir,
            contracts=merged,
            apply=config.apply_validated_primitives,
        )
        observer_state["feedback_loop"] = {
            "gap_count": feedback.get("gap_count", 0),
            "candidate_count": feedback.get("candidate_count", 0),
            "apply_enabled": bool(config.apply_validated_primitives),
            "artifact": feedback.get("artifact"),
            "applied": (feedback.get("apply_summary") or {}).get("applied", []),
        }
        observer_state["artifacts"]["primitive_feedback"] = str(feedback.get("artifact"))
    _write_json(output_dir / f"observer_run_{run_id}.json", observer_state)
    return observer_state
