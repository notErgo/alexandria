"""
SEC EDGAR connector: Stage-1-only ingestor for 8-K, 10-Q, and 10-K filings.

8-K: full-text search API (production-related only)
10-Q/10-K: Submissions API (https://data.sec.gov/submissions/CIK{cik}.json)

All methods store raw text to reports table ONLY.
No inline extraction — extraction is run separately by interpret_pipeline.interpret_report().
"""
import re
import time
import logging
from dataclasses import dataclass
from datetime import date, datetime, timezone
from typing import Optional

import warnings

import requests
from bs4 import BeautifulSoup, XMLParsedAsHTMLWarning

warnings.filterwarnings("ignore", category=XMLParsedAsHTMLWarning)

from config import (
    EDGAR_BASE_URL,
    EDGAR_SUBMISSIONS_URL,
    EDGAR_REQUEST_DELAY_SECONDS,
    EDGAR_RETRY_BACKOFF_BASE,
)
from scrapers.fetch_policy import DEFAULT_RETRY_POLICY, CircuitOpenError

log = logging.getLogger('miners.scrapers.edgar_connector')

_USER_AGENT = "Hermeneutic Research Platform contact@example.com"
_DATE_PATTERN = re.compile(r"(\d{4})-(\d{2})-(\d{2})")

# Maximum characters of filing text stored (10-Q/10-K can be very long)
_MAX_FILING_TEXT_CHARS = 100_000

# Maximum characters of raw HTML stored for the document viewer
_MAX_RAW_HTML_CHARS = 300_000

_XBRL_VIEWER_MARKER = 'Please enable JavaScript to use the EDGAR Inline XBRL Viewer'
# Inline XBRL (iXBRL) documents declare this namespace; the viewer marker appears
# only in a <noscript> fallback but the full filing content is still present.
_IXBRL_NAMESPACE_MARKER = 'xmlns:ix='


def _is_xbrl_viewer_page(html: str) -> bool:
    """Return True if the page is a pure EDGAR viewer wrapper (no inline filing content).

    Inline XBRL (iXBRL) documents contain the viewer marker in a <noscript> block
    but also carry the full filing text tagged with XBRL namespaces.  Those documents
    are NOT viewer wrappers — BeautifulSoup can extract their text normally.
    """
    if _XBRL_VIEWER_MARKER not in html:
        return False
    # If the document also declares the iXBRL namespace it is the actual filing.
    if _IXBRL_NAMESPACE_MARKER in html:
        return False
    return True

# 8-K full-text search terms (OR-joined). Covers all known production PR phrasings.
_8K_SEARCH_TERMS: list = [
    '"bitcoin production"',
    '"BTC production"',
    '"bitcoin mined"',
    '"BTC mined"',
    '"mining operations update"',
    '"production and operations"',
    '"digital asset production"',
    '"hash rate"',
]


def _build_edgar_query(db=None) -> str:
    """Build the EDGAR full-text search query from metric keywords (active only).

    Delegates to infra.keyword_service.build_edgar_search_query — single read-point for
    all keyword consumers. Falls back to hardcoded _8K_SEARCH_TERMS when db is None.
    """
    from infra.keyword_service import build_edgar_search_query
    return build_edgar_search_query(db)


# ── Module-level helpers (public, used by tests) ──────────────────────────────

def period_of_report_to_covering_period(period_of_report: str, form_type: str) -> str:
    """Convert period_of_report date to covering_period string.

    10-Q: "2025-03-31" -> "2025-Q1", "2025-06-30" -> "2025-Q2", etc.
    10-K: any date -> "{year}-FY"
    """
    parts = period_of_report.split('-')
    year = int(parts[0])
    month = int(parts[1])

    if form_type in ('10-K', '10-k', '20-F', '20-F/A', '40-F', '40-F/A'):
        return f"{year}-FY"

    # Map month to quarter
    if month <= 3:
        quarter = 'Q1'
    elif month <= 6:
        quarter = 'Q2'
    elif month <= 9:
        quarter = 'Q3'
    else:
        quarter = 'Q4'

    return f"{year}-{quarter}"


def parse_submissions_filings(submissions: dict, form_type: str) -> list:
    """Parse the submissions JSON and return a list of filing dicts filtered by form_type.

    Each dict: {form_type, accession_number, filing_date, primary_doc,
                period_of_report, covering_period}
    """
    recent = submissions.get('filings', {}).get('recent', {})
    forms = recent.get('form', [])
    filing_dates = recent.get('filingDate', [])
    accession_numbers = recent.get('accessionNumber', [])
    primary_docs = recent.get('primaryDocument', [])
    periods = recent.get('periodOfReport', [])
    report_dates = recent.get('reportDate', [])

    results = []
    for i, form in enumerate(forms):
        if form != form_type:
            continue
        # SEC submissions payload no longer reliably includes periodOfReport in
        # recent filings. Fall back to reportDate (and finally filingDate) to
        # preserve periodic filing coverage.
        period = (
            (periods[i] if i < len(periods) else '')
            or (report_dates[i] if i < len(report_dates) else '')
            or (filing_dates[i] if i < len(filing_dates) else '')
        )
        if not period:
            continue
        try:
            covering = period_of_report_to_covering_period(period, form_type)
        except Exception:
            log.debug("Could not convert period %s to covering_period", period)
            continue
        results.append({
            'form_type':        form,
            'accession_number': accession_numbers[i] if i < len(accession_numbers) else '',
            'filing_date':      filing_dates[i] if i < len(filing_dates) else '',
            'primary_doc':      primary_docs[i] if i < len(primary_docs) else '',
            'period_of_report': period,
            'covering_period':  covering,
        })
    return results


def parse_8k_exhibit_url(
    index_html: str, cik_numeric: str, acc_no_clean: str
) -> Optional[str]:
    """Parse an 8-K filing index HTML to find the press release exhibit URL.

    Prefers EX-99.1, falls back to EX-99.  Returns None if neither is present.
    The returned URL is absolute (https://www.sec.gov/...).
    """
    soup = BeautifulSoup(index_html, 'lxml')
    base = f"https://www.sec.gov/Archives/edgar/data/{cik_numeric}/{acc_no_clean}/"

    best = None  # EX-99 match
    for row in soup.find_all('tr'):
        cells = row.find_all('td')
        if not cells:
            continue
        row_type = ''
        link_href = None
        for cell in cells:
            text = cell.get_text(separator=' ', strip=True)
            a_tag = cell.find('a', href=True)
            if a_tag and ('.htm' in a_tag['href'].lower() or '.html' in a_tag['href'].lower()):
                link_href = a_tag['href']
            if re.match(r'EX-99', text, re.IGNORECASE):
                row_type = text.upper()

        if not link_href or not row_type.startswith('EX-99'):
            continue

        href = link_href
        if href.startswith('http'):
            pass  # already absolute
        elif href.startswith('/Archives/') or href.startswith('/cgi-bin/'):
            # Absolute EDGAR path — just prepend the host, do NOT re-append filing dir.
            href = 'https://www.sec.gov' + href
        else:
            # Relative filename (e.g. "ex99_1.htm") — append to filing directory.
            href = base + href.lstrip('/')

        if 'EX-99.1' in row_type or 'EX-991' in row_type:
            return href  # Best match
        if best is None:
            best = href

    return best


def _parse_exhibit_url_from_stale_source_url(source_url: str) -> Optional[str]:
    """Reconstruct the actual exhibit URL from a malformed 8-K source_url.

    Stale records stored URLs in the form:
      https://www.sec.gov/Archives/edgar/data/{cik}/{acc_clean}:{doc}/{acc_dashed}:{doc}-index.htm

    We extract cik, acc_clean, and doc_name to build:
      https://www.sec.gov/Archives/edgar/data/{cik}/{acc_clean}/{doc}
    """
    if not source_url:
        return None
    pattern = re.compile(
        r'https://www\.sec\.gov/Archives/edgar/data/(\d+)/([^/:]+):([^/]+)/'
    )
    m = pattern.match(source_url)
    if not m:
        return None
    cik_numeric = m.group(1)
    acc_no_clean = m.group(2)
    doc_name = m.group(3)
    return f"https://www.sec.gov/Archives/edgar/data/{cik_numeric}/{acc_no_clean}/{doc_name}"


def _unwrap_ixbrl_url(url: str) -> str:
    """Strip the EDGAR Inline XBRL viewer wrapper from a URL if present.

    EDGAR wraps iXBRL documents in a viewer URL of the form:
        https://www.sec.gov/ix?doc=/Archives/edgar/data/...
        https://www.sec.gov/ix?doc=https://www.sec.gov/Archives/edgar/data/...

    Fetching the wrapper URL without JavaScript returns only the viewer shell.
    Fetching the underlying /Archives/... URL directly returns the full filing HTML.
    """
    if not url:
        return url
    # Match https://www.sec.gov/ix?doc=<path>
    m = re.match(r'^https?://www\.sec\.gov/ix\?doc=(.+)$', url)
    if not m:
        return url
    inner = m.group(1)
    # inner may be an absolute URL or a root-relative path
    if inner.startswith('https://') or inner.startswith('http://'):
        return inner
    if inner.startswith('/'):
        return 'https://www.sec.gov' + inner
    return 'https://www.sec.gov/' + inner


def parse_filing_index_for_primary_doc(index_html: str) -> Optional[str]:
    """Parse an EDGAR filing index HTML page to find the primary document URL.

    Excludes exhibits (EX-*). Returns the first non-exhibit .htm link, or None.
    The document URL in the table is relative — prepended with https://www.sec.gov.
    """
    soup = BeautifulSoup(index_html, 'lxml')
    for row in soup.find_all('tr'):
        cells = row.find_all('td')
        if not cells:
            continue
        # Type column: look for a cell whose text matches the form type (not an exhibit)
        # Table layout varies; we scan for a non-exhibit cell containing a .htm link
        # Find the type: it's usually in the last or first td
        type_text = ''
        link = None
        for cell in cells:
            text = cell.get_text(separator=' ', strip=True)
            a_tag = cell.find('a', href=True)
            if a_tag and ('.htm' in a_tag['href'].lower() or '.html' in a_tag['href'].lower()):
                link = a_tag
            elif text.startswith('EX-') or text.startswith('ex-'):
                type_text = text
                break
            elif text.upper() in ('10-Q', '10-K', '10-K/A', '10-Q/A'):
                type_text = text

        # Skip exhibit rows
        if type_text.upper().startswith('EX-'):
            continue

        if link:
            href = link['href']
            # Check the row doesn't contain exhibit identifiers in any cell
            row_text = row.get_text(separator=' ', strip=True).upper()
            if 'EX-' in row_text and '10-Q' not in row_text and '10-K' not in row_text:
                continue
            if not href.startswith('http'):
                href = 'https://www.sec.gov' + href if href.startswith('/') else href
            return _unwrap_ixbrl_url(href)

    return None


def _hit_matches_target_entity(source: dict, cik: str) -> bool:
    """Return True when an EDGAR search hit belongs to the target filer CIK.

    efts search may return noisy cross-entity hits even when entity filters are
    present. We enforce filer matching using _source.ciks first, then _source.adsh.
    """
    target_10 = str(cik).zfill(10)
    target_n = str(int(cik)) if str(cik).strip('0') else '0'

    ciks = source.get('ciks') or []
    for c in ciks:
        try:
            if str(c).zfill(10) == target_10:
                return True
        except Exception:
            continue

    adsh = str(source.get('adsh') or '')
    if adsh:
        filer = adsh.split('-', 1)[0]
        if filer == target_n or filer.zfill(10) == target_10:
            return True

    return False


# ── EdgarConnector class ──────────────────────────────────────────────────────

@dataclass
class EdgarConnector:
    """Fetches 8-K, 10-Q, and 10-K filings from SEC EDGAR for a given company.

    Stage-1-only: all methods store raw text to the reports table.
    No extraction is performed — run interpret_pipeline.interpret_report() separately.
    """
    db: object              # MinerDB
    session: requests.Session
    _pipeline_run_id: Optional[int] = None

    def _emit(self, event: str, ticker: str = None, level: str = 'INFO', **details) -> None:
        """Write a pipeline event if running inside a pipeline run."""
        if not self._pipeline_run_id:
            return
        try:
            self.db.add_pipeline_run_event(
                self._pipeline_run_id,
                stage='edgar_ingest',
                event=event,
                ticker=ticker,
                level=level,
                details=details or {},
            )
        except Exception as _e:
            log.debug("pipeline event emit failed (non-fatal): %s", _e)

    def _edgar_request(self, url: str, params: dict = None) -> Optional[dict]:
        """Make a rate-limited GET request to EDGAR. Returns JSON or None.

        Handles 429 with exponential backoff up to 2 retries.
        """
        max_attempts = 3
        for attempt in range(max_attempts):
            time.sleep(EDGAR_REQUEST_DELAY_SECONDS)
            try:
                resp = DEFAULT_RETRY_POLICY.execute(
                    self.session.get,
                    url,
                    params=params,
                    timeout=30,
                    headers={"User-Agent": _USER_AGENT},
                )
                if resp.status_code == 429:
                    backoff = EDGAR_RETRY_BACKOFF_BASE * (2 ** attempt)
                    log.warning(
                        "EDGAR 429 Too Many Requests for %s, backing off %.0fs (attempt %d/%d)",
                        url, backoff, attempt + 1, max_attempts,
                    )
                    time.sleep(backoff)
                    continue
                if resp.status_code == 400:
                    log.debug("EDGAR returned 400 for %s", url)
                    return None
                resp.raise_for_status()
                return resp.json()
            except CircuitOpenError as e:
                log.warning("Circuit open, skipping EDGAR request for %s: %s", url, e)
                return None
            except requests.Timeout:
                log.warning("Timeout fetching EDGAR %s", url)
                return None
            except requests.RequestException as e:
                log.error("EDGAR request error %s: %s", url, e, exc_info=True)
                return None
            except (ValueError, KeyError) as e:
                log.error("Bad EDGAR response from %s: %s", url, e, exc_info=True)
                return None
        log.error("EDGAR request failed after %d attempts: %s", max_attempts, url)
        return None

    def _edgar_get_text(self, url: str) -> str:
        """Fetch an EDGAR document and return rate-limited HTML text, or empty string."""
        time.sleep(EDGAR_REQUEST_DELAY_SECONDS)
        try:
            resp = DEFAULT_RETRY_POLICY.execute(
                self.session.get,
                url,
                timeout=30,
                headers={"User-Agent": _USER_AGENT},
            )
            if resp.status_code == 404:
                log.debug("EDGAR 404 for %s", url)
                return ''
            resp.raise_for_status()
            return resp.text
        except CircuitOpenError as e:
            log.warning("Circuit open, skipping EDGAR document fetch for %s: %s", url, e)
            return ''
        except requests.Timeout:
            log.warning("Timeout fetching EDGAR document %s", url)
            return ''
        except requests.RequestException as e:
            log.error("EDGAR document request error %s: %s", url, e, exc_info=True)
            return ''

    def _get_submissions(self, cik: str) -> Optional[dict]:
        """Fetch the submissions JSON for a CIK from data.sec.gov."""
        cik_padded = cik.lstrip('0').zfill(10)
        url = EDGAR_SUBMISSIONS_URL.format(cik=cik_padded)
        return self._edgar_request(url)

    def _ingest_periodic_filing(
        self, form_type: str, filing: dict, ticker: str, cik: str
    ) -> bool:
        """Fetch and store one 10-Q or 10-K (or foreign annual/periodic) filing.

        Returns True if stored, False if skipped.
        Uses accession number for primary dedup; falls back to (ticker, period, source_type).
        """
        source_type = 'edgar_' + form_type.lower().replace('-', '').replace('/', '')
        period = filing['period_of_report']
        acc_no = filing.get('accession_number', '')

        # Accession-first dedup: if we already have this exact filing, skip
        if acc_no and self.db.report_exists_by_accession(acc_no):
            log.debug("Already ingested %s %s by accession %s", source_type, ticker, acc_no)
            return False

        if self.db.report_exists(ticker, period, source_type):
            log.debug("Already ingested %s %s %s", source_type, ticker, period)
            return False

        # Build the filing index URL
        cik_numeric = cik.lstrip('0') or '0'
        acc_no = filing['accession_number']
        acc_no_clean = acc_no.replace('-', '')
        index_url = (
            f"https://www.sec.gov/Archives/edgar/data/{cik_numeric}/{acc_no_clean}/"
            f"{acc_no}-index.htm"
        )

        # Fetch index page and find primary document
        index_html = self._edgar_get_text(index_url)
        if not index_html:
            log.warning("Empty index page for %s %s %s", ticker, form_type, acc_no)
            return False

        primary_url = parse_filing_index_for_primary_doc(index_html)
        if not primary_url:
            # Fall back to the primary document listed in submissions JSON
            primary_doc = filing.get('primary_doc', '')
            if primary_doc:
                primary_url = (
                    f"https://www.sec.gov/Archives/edgar/data/{cik_numeric}/"
                    f"{acc_no_clean}/{primary_doc}"
                )
            else:
                log.warning("No primary doc found for %s %s %s", ticker, form_type, acc_no)
                return False

        # Strip any EDGAR iXBRL viewer wrapper so we fetch the actual document
        primary_url = _unwrap_ixbrl_url(primary_url)

        # Fetch and parse the primary document
        doc_html = self._edgar_get_text(primary_url)
        if not doc_html:
            log.warning("Empty primary document for %s %s %s", ticker, form_type, acc_no)
            return False

        if _is_xbrl_viewer_page(doc_html):
            log.warning(
                "XBRL viewer page returned for %s %s %s — attempting fallback",
                ticker, form_type, acc_no,
            )
            # Try the primary_doc name from submissions JSON as an alternate URL
            fallback_url = None
            primary_doc = filing.get('primary_doc', '')
            if primary_doc and primary_url and not primary_url.endswith(primary_doc):
                fallback_url = (
                    f"https://www.sec.gov/Archives/edgar/data/{cik_numeric}/"
                    f"{acc_no_clean}/{primary_doc}"
                )
            if fallback_url and fallback_url != primary_url:
                alt_html = self._edgar_get_text(fallback_url)
                if alt_html and not _is_xbrl_viewer_page(alt_html):
                    doc_html = alt_html
                    primary_url = fallback_url
                    log.info("XBRL fallback succeeded for %s %s", ticker, acc_no)
                else:
                    log.warning(
                        "XBRL fallback also failed for %s %s — storing viewer page",
                        ticker, acc_no,
                    )

        soup = BeautifulSoup(doc_html, 'lxml')

        # For 10-Q/10-K, try to extract the MD&A section (Item 2 or Item 7)
        full_text = soup.get_text(separator=' ', strip=True)
        text = _extract_mda_section(full_text, form_type) or full_text
        text = text[:_MAX_FILING_TEXT_CHARS]

        if not text.strip():
            log.warning("No text extracted for %s %s %s", ticker, form_type, acc_no)
            return False

        text_len = len(text.strip())
        if _is_xbrl_viewer_page(doc_html):
            parse_quality = 'xbrl_viewer'
        else:
            parse_quality = 'text_ok' if text_len >= 500 else 'text_sparse'

        report = {
            'ticker':            ticker,
            'report_date':       period,
            'published_date':    filing.get('filing_date', period),
            'source_type':       source_type,
            'source_url':        primary_url,
            'raw_text':          text,
            'raw_html':          doc_html[:300_000],
            'parsed_at':         datetime.now(timezone.utc).isoformat(),
            'covering_period':   filing.get('covering_period'),
            'accession_number':  acc_no or None,
            'form_type':         form_type,
        }
        try:
            report_id = self.db.insert_report(report)
            self.db.set_report_parse_quality(report_id, parse_quality)
            log.info(
                "Stored %s filing: %s %s (covering %s, quality=%s)",
                source_type, ticker, period, filing.get('covering_period'), parse_quality,
            )
            self._emit(
                'url_ingested', ticker=ticker,
                form_type=form_type,
                period=period,
                covering_period=filing.get('covering_period'),
                filing_date=filing.get('filing_date', ''),
                accession=acc_no,
                quality=parse_quality,
                text_chars=text_len,
                url=primary_url,
            )
            return True
        except Exception as e:
            log.error(
                "Failed to insert %s report %s %s: %s",
                source_type, ticker, period, e, exc_info=True,
            )
            self._emit(
                'url_error', ticker=ticker, level='WARNING',
                form_type=form_type, period=period,
                accession=acc_no, url=primary_url, error=str(e),
            )
            return False

    def detect_btc_first_filing_date(self, cik: str, ticker: str) -> Optional[str]:
        """Find the earliest SEC filing date that mentions any active search keyword.

        Uses EDGAR full-text search sorted ascending, takes the first result.
        Stores the result in companies.btc_first_filing_date if a match is found.
        Returns ISO date string YYYY-MM-DD or None if no match found.

        If btc_first_filing_date is already stored for this ticker, returns it
        immediately without hitting EDGAR (idempotent).
        """
        # Return cached value if already detected
        stored = self.db.get_btc_first_filing_date(ticker)
        if stored:
            return stored

        query = _build_edgar_query(self.db)
        if not query.strip():
            log.warning("detect_btc_first_filing_date: no keywords available for %s", ticker)
            return None

        cik_entity = str(cik).lstrip('0') or '0'
        params = {
            'q': query,
            'entity': cik_entity,
            'dateRange': 'custom',
            'startdt': '1993-01-01',
            'forms': '8-K,8-K/A,10-K,10-K/A,10-Q,10-Q/A',
            'hits.hits._source': 'file_date',
            '_source': 'file_date',
            'sort': 'file-date',
            'order': 'asc',
        }
        data = self._edgar_request(EDGAR_BASE_URL, params)
        if data is None:
            log.warning("detect_btc_first_filing_date: no EDGAR response for %s", ticker)
            return None

        hits = data.get('hits', {}).get('hits', [])
        if not hits:
            log.info("detect_btc_first_filing_date: no keyword hits found for %s", ticker)
            return None

        filed_date = hits[0].get('_source', {}).get('file_date', '')
        if not filed_date:
            return None

        # Normalise to YYYY-MM-DD (drop time suffix if present)
        filed_date = filed_date[:10]
        log.info(
            "detect_btc_first_filing_date: earliest BTC filing for %s is %s",
            ticker, filed_date,
        )
        self.db.set_btc_first_filing_date(ticker, filed_date)
        return filed_date

    def fetch_8k_filings(self, cik: str, ticker: str, since_date: date) -> 'IngestSummary':
        """Fetch 8-K filings from the submissions API and store raw exhibit text.

        Uses the submissions API (same approach as fetch_10q_filings / fetch_10k_filings).
        For each 8-K, fetches the filing index page and resolves the EX-99.1 exhibit
        (press release) URL.  Falls back to EX-99 if EX-99.1 is absent.  Filings with
        no exhibit are skipped silently — they are 8-Ks about non-production events.

        Keyword filtering is handled at extraction time by the keyword gate in
        interpret_pipeline — no content-based gating is applied at ingest.

        Stores raw text only — no extraction performed.
        Returns IngestSummary with counts.
        """
        from miner_types import IngestSummary
        summary = IngestSummary()

        submissions = self._get_submissions(cik)
        if submissions is None:
            log.warning("Could not fetch submissions for %s (CIK=%s)", ticker, cik)
            summary.errors += 1
            return summary

        filings = parse_submissions_filings(submissions, form_type='8-K')
        log.info("Found %d 8-K filings for %s", len(filings), ticker)

        cik_numeric = cik.lstrip('0') or '0'

        for filing in filings:
            filing_date = filing.get('filing_date', '')
            if filing_date < since_date.isoformat():
                continue

            acc_no = filing.get('accession_number', '')
            if acc_no and self.db.report_exists_by_accession(acc_no):
                log.debug("Already ingested 8-K by accession: %s %s", ticker, acc_no)
                continue
            if self.db.report_exists(ticker, filing_date, 'edgar_8k'):
                log.debug("Already ingested 8-K %s %s", ticker, filing_date)
                continue

            acc_no_clean = acc_no.replace('-', '')
            index_url = (
                f"https://www.sec.gov/Archives/edgar/data/{cik_numeric}/{acc_no_clean}/"
                f"{acc_no}-index.htm"
            )
            index_html = self._edgar_get_text(index_url)
            if not index_html:
                log.warning("Empty 8-K index page for %s %s", ticker, acc_no)
                summary.errors += 1
                continue

            exhibit_url = parse_8k_exhibit_url(index_html, cik_numeric, acc_no_clean)
            if not exhibit_url:
                log.debug("No EX-99 exhibit for %s 8-K %s; skipping (non-production filing)", ticker, acc_no)
                continue

            doc_html = self._edgar_get_text(exhibit_url)
            if not doc_html:
                log.warning("Empty 8-K exhibit for %s %s", ticker, acc_no)
                summary.errors += 1
                continue

            soup = BeautifulSoup(doc_html, 'lxml')
            text = soup.get_text(separator=' ', strip=True)[:_MAX_FILING_TEXT_CHARS]

            if not text.strip():
                log.warning("No text from 8-K exhibit for %s %s", ticker, acc_no)
                summary.errors += 1
                continue

            text_len = len(text.strip())
            parse_quality = 'text_ok' if text_len >= 500 else 'text_sparse'

            report = {
                'ticker':           ticker,
                'report_date':      filing_date,
                'published_date':   filing_date,
                'source_type':      'edgar_8k',
                'source_url':       exhibit_url,
                'raw_text':         text,
                'raw_html':         doc_html[:_MAX_RAW_HTML_CHARS],
                'parsed_at':        datetime.now(timezone.utc).isoformat(),
                'covering_period':  None,
                'accession_number': acc_no or None,
                'form_type':        '8-K',
            }
            try:
                report_id = self.db.insert_report(report)
                self.db.set_report_parse_quality(report_id, parse_quality)
                summary.reports_ingested += 1
                log.info(
                    "Stored 8-K filing: %s %s (quality=%s, chars=%d)",
                    ticker, filing_date, parse_quality, text_len,
                )
                self._emit(
                    'url_ingested', ticker=ticker,
                    form_type='8-K',
                    period=filing_date,
                    filing_date=filing_date,
                    accession=acc_no,
                    quality=parse_quality,
                    text_chars=text_len,
                    url=exhibit_url,
                )
            except Exception as e:
                log.error(
                    "Failed to insert 8-K report %s %s: %s", ticker, filing_date, e, exc_info=True
                )
                self._emit(
                    'url_error', ticker=ticker, level='WARNING',
                    form_type='8-K', period=filing_date,
                    accession=acc_no, url=exhibit_url, error=str(e),
                )
                summary.errors += 1

        return summary

    def refetch_stale_8k_exhibits(self, ticker: str = None) -> 'IngestSummary':
        """Re-fetch exhibit text for 8-K records that stored the EDGAR index page boilerplate.

        Identifies stale records via get_stale_8k_reports() (raw_text starts with
        'EDGAR Filing Documents').  Reconstructs the exhibit URL from the malformed
        source_url stored during the broken ingest, fetches the exhibit, and updates
        the record in-place via update_report_raw_text().

        Returns IngestSummary where reports_ingested = number of records fixed.
        """
        from miner_types import IngestSummary
        summary = IngestSummary()

        stale = self.db.get_stale_8k_reports(ticker=ticker)
        log.info(
            "Found %d stale 8-K records to refetch (ticker=%s)", len(stale), ticker or 'all'
        )

        for report in stale:
            report_id = report['id']
            source_url = report.get('source_url') or ''

            exhibit_url = _parse_exhibit_url_from_stale_source_url(source_url)
            if not exhibit_url:
                log.warning(
                    "Cannot reconstruct exhibit URL for report %d (source_url=%s)",
                    report_id, source_url,
                )
                summary.errors += 1
                continue

            doc_html = self._edgar_get_text(exhibit_url)
            if not doc_html:
                log.warning("Empty exhibit for stale 8-K report %d: %s", report_id, exhibit_url)
                summary.errors += 1
                continue

            soup = BeautifulSoup(doc_html, 'lxml')
            text = soup.get_text(separator=' ', strip=True)[:50_000]

            if not text.strip() or text.startswith('EDGAR Filing Documents'):
                log.warning(
                    "Still got boilerplate after refetch for report %d: %s",
                    report_id, exhibit_url,
                )
                summary.errors += 1
                continue

            self.db.update_report_raw_text(report_id, text, exhibit_url)
            summary.reports_ingested += 1
            log.info(
                "Refetched exhibit for %s 8-K %s (report %d)",
                report['ticker'], report['report_date'], report_id,
            )

        return summary

    def refetch_xbrl_viewer_reports(self, ticker: str = None) -> 'IngestSummary':
        """Re-fetch 10-Q/10-K reports that were stored as the XBRL viewer stub.

        Finds all reports with parse_quality='xbrl_viewer', strips the ix?doc=
        wrapper from their source_url, fetches the real document, and updates
        raw_text, raw_html, and parse_quality in-place.

        The extraction_status is reset to 'pending' so the extraction pipeline
        will re-process them.

        Returns IngestSummary where reports_ingested = number of records fixed.
        """
        from miner_types import IngestSummary
        summary = IngestSummary()

        stale = self.db.get_xbrl_viewer_reports(ticker=ticker)
        log.info(
            "Found %d xbrl_viewer reports to refetch (ticker=%s)", len(stale), ticker or 'all'
        )

        for report in stale:
            report_id = report['id']
            source_url = report.get('source_url') or ''

            real_url = _unwrap_ixbrl_url(source_url)
            if real_url == source_url and 'ix?doc=' not in source_url:
                # URL is not a viewer wrapper — try fetching directly anyway
                # (it may be a plain /Archives/ URL that just returned bad content)
                pass
            if not real_url:
                log.warning("No resolvable URL for xbrl_viewer report %d", report_id)
                summary.errors += 1
                continue

            doc_html = self._edgar_get_text(real_url)
            if not doc_html:
                log.warning(
                    "Empty response for xbrl_viewer refetch %d: %s", report_id, real_url
                )
                summary.errors += 1
                continue

            if _is_xbrl_viewer_page(doc_html):
                log.warning(
                    "Still got xbrl_viewer page after unwrap for report %d: %s",
                    report_id, real_url,
                )
                summary.errors += 1
                continue

            soup = BeautifulSoup(doc_html, 'lxml')
            full_text = soup.get_text(separator=' ', strip=True)
            form_type = report.get('source_type', '').replace('edgar_', '').upper()
            text = _extract_mda_section(full_text, form_type) or full_text
            text = text[:_MAX_FILING_TEXT_CHARS]

            if not text.strip():
                log.warning("No text after refetch for report %d", report_id)
                summary.errors += 1
                continue

            text_len = len(text.strip())
            new_quality = 'text_ok' if text_len >= 500 else 'text_sparse'

            self.db.update_report_raw_text(
                report_id, text, source_url=real_url, raw_html=doc_html[:_MAX_RAW_HTML_CHARS]
            )
            self.db.set_report_parse_quality(report_id, new_quality)
            # Reset extraction status so pipeline will re-process this report
            with self.db._get_connection() as conn:
                conn.execute(
                    "UPDATE reports SET extraction_status='pending' WHERE id=?",
                    (report_id,),
                )

            summary.reports_ingested += 1
            log.info(
                "Refetched xbrl_viewer report %d: %s %s (quality=%s, chars=%d)",
                report_id, report['ticker'], report['report_date'], new_quality, text_len,
            )

        return summary

    def fetch_10q_filings(self, cik: str, ticker: str, since_date: date) -> 'IngestSummary':
        """Fetch 10-Q filings from the submissions API and store raw text.

        Returns IngestSummary with counts.
        """
        from miner_types import IngestSummary
        summary = IngestSummary()
        submissions = self._get_submissions(cik)
        if submissions is None:
            log.warning("Could not fetch submissions for %s (CIK=%s)", ticker, cik)
            summary.errors += 1
            return summary

        filings = parse_submissions_filings(submissions, form_type='10-Q')
        log.info("Found %d 10-Q filings for %s", len(filings), ticker)

        for filing in filings:
            period = filing.get('period_of_report', '')
            if period < since_date.isoformat():
                continue
            stored = self._ingest_periodic_filing('10-Q', filing, ticker, cik)
            if stored:
                summary.reports_ingested += 1

        return summary

    def fetch_10k_filings(self, cik: str, ticker: str, since_date: date) -> 'IngestSummary':
        """Fetch 10-K filings from the submissions API and store raw text.

        Returns IngestSummary with counts.
        """
        from miner_types import IngestSummary
        summary = IngestSummary()
        submissions = self._get_submissions(cik)
        if submissions is None:
            log.warning("Could not fetch submissions for %s (CIK=%s)", ticker, cik)
            summary.errors += 1
            return summary

        filings = parse_submissions_filings(submissions, form_type='10-K')
        log.info("Found %d 10-K filings for %s", len(filings), ticker)

        for filing in filings:
            period = filing.get('period_of_report', '')
            if period < since_date.isoformat():
                continue
            stored = self._ingest_periodic_filing('10-K', filing, ticker, cik)
            if stored:
                summary.reports_ingested += 1

        return summary

    def fetch_6k_filings(self, cik: str, ticker: str, since_date: date) -> 'IngestSummary':
        """Fetch 6-K filings from the submissions API (foreign companies — current reports).

        Returns IngestSummary with counts.
        """
        from miner_types import IngestSummary
        summary = IngestSummary()
        submissions = self._get_submissions(cik)
        if submissions is None:
            log.warning("Could not fetch submissions for %s (CIK=%s)", ticker, cik)
            summary.errors += 1
            return summary

        filings = parse_submissions_filings(submissions, form_type='6-K')
        log.info("Found %d 6-K filings for %s", len(filings), ticker)

        for filing in filings:
            period = filing.get('period_of_report', '')
            if period < since_date.isoformat():
                continue
            stored = self._ingest_periodic_filing('6-K', filing, ticker, cik)
            if stored:
                summary.reports_ingested += 1

        return summary

    def fetch_20f_filings(self, cik: str, ticker: str, since_date: date) -> 'IngestSummary':
        """Fetch 20-F annual filings from the submissions API (foreign private issuers).

        Returns IngestSummary with counts.
        """
        from miner_types import IngestSummary
        summary = IngestSummary()
        submissions = self._get_submissions(cik)
        if submissions is None:
            log.warning("Could not fetch submissions for %s (CIK=%s)", ticker, cik)
            summary.errors += 1
            return summary

        filings = parse_submissions_filings(submissions, form_type='20-F')
        log.info("Found %d 20-F filings for %s", len(filings), ticker)

        for filing in filings:
            period = filing.get('period_of_report', '')
            if period < since_date.isoformat():
                continue
            stored = self._ingest_periodic_filing('20-F', filing, ticker, cik)
            if stored:
                summary.reports_ingested += 1

        return summary

    def fetch_40f_filings(self, cik: str, ticker: str, since_date: date) -> 'IngestSummary':
        """Fetch 40-F annual filings from the submissions API (Canadian companies).

        Returns IngestSummary with counts.
        """
        from miner_types import IngestSummary
        summary = IngestSummary()
        submissions = self._get_submissions(cik)
        if submissions is None:
            log.warning("Could not fetch submissions for %s (CIK=%s)", ticker, cik)
            summary.errors += 1
            return summary

        filings = parse_submissions_filings(submissions, form_type='40-F')
        log.info("Found %d 40-F filings for %s", len(filings), ticker)

        for filing in filings:
            period = filing.get('period_of_report', '')
            if period < since_date.isoformat():
                continue
            stored = self._ingest_periodic_filing('40-F', filing, ticker, cik)
            if stored:
                summary.reports_ingested += 1

        return summary

    def fetch_all_filings(
        self, cik: str, ticker: str, since_date: date, filing_regime: str = 'domestic'
    ) -> 'IngestSummary':
        """Fetch all filings for a ticker, routed by filing_regime.

        domestic: 8-K + 10-Q + 10-K
        canadian: 6-K + 40-F (no 10-Q/10-K)
        foreign:  6-K + 20-F (no 10-Q/10-K)

        Automatically detects and stores btc_first_filing_date when not yet set,
        then uses max(since_date, btc_first_filing_date) as the effective floor so
        pre-mining-pivot filings are never ingested.

        Updates last_edgar_at on the company row after completion.
        Returns a combined IngestSummary.
        """
        from miner_types import IngestSummary
        combined = IngestSummary()

        # Auto-populate btc_first_filing_date if not yet stored, then gate since_date.
        try:
            btc_pivot = self.detect_btc_first_filing_date(cik=cik, ticker=ticker)
            if btc_pivot:
                from datetime import date as _date_cls
                pivot_date = _date_cls.fromisoformat(btc_pivot[:10])
                if pivot_date > since_date:
                    log.info(
                        "event=edgar_since_gated ticker=%s since_date=%s btc_first_filing=%s",
                        ticker, since_date, btc_pivot,
                    )
                    since_date = pivot_date
        except Exception as _det_err:
            log.warning("event=btc_first_filing_detect_error ticker=%s error=%s", ticker, _det_err)

        regime = (filing_regime or 'domestic').lower()
        if regime == 'domestic':
            fetchers = (self.fetch_8k_filings, self.fetch_10q_filings, self.fetch_10k_filings)
        elif regime == 'canadian':
            fetchers = (self.fetch_6k_filings, self.fetch_40f_filings)
        elif regime == 'foreign':
            fetchers = (self.fetch_6k_filings, self.fetch_20f_filings)
        else:
            log.warning("Unknown filing_regime '%s' for %s, defaulting to domestic", regime, ticker)
            fetchers = (self.fetch_8k_filings, self.fetch_10q_filings, self.fetch_10k_filings)

        for fetcher in fetchers:
            try:
                result = fetcher(cik=cik, ticker=ticker, since_date=since_date)
                combined.reports_ingested += result.reports_ingested
                combined.data_points_extracted += result.data_points_extracted
                combined.review_flagged += result.review_flagged
                combined.errors += result.errors
            except Exception as e:
                log.error("EDGAR fetch failed for %s (%s): %s", ticker, fetcher.__name__, e, exc_info=True)
                combined.errors += 1

        try:
            self.db.update_company_last_edgar(ticker)
        except Exception as e:
            log.warning("Could not update last_edgar_at for %s: %s", ticker, e)

        return combined


# ── Helpers ───────────────────────────────────────────────────────────────────

def _extract_mda_section(full_text: str, form_type: str) -> Optional[str]:
    """Try to extract the MD&A section from a 10-Q or 10-K filing text.

    For 10-Q / 6-K: looks for 'Item 2' (Management Discussion & Analysis).
    For 10-K / 20-F / 40-F: looks for 'Item 7' (Management Discussion & Analysis).

    Returns the section text if found (up to _MAX_FILING_TEXT_CHARS), or None
    to use full_text.

    The search skips table-of-contents occurrences by requiring that the
    candidate section contain at least MIN_SECTION_CHARS characters before the
    next item marker.  This prevents picking up the short TOC stub instead of
    the actual body.
    """
    _annual_forms = ('10-K', '10-k', '20-F', '20-F/A', '40-F', '40-F/A')
    item_label = 'Item 7' if form_type in _annual_forms else 'Item 2'
    next_item = 'Item 8' if form_type in _annual_forms else 'Item 3'

    # Minimum characters between the item marker and the next item before we
    # accept the candidate as the real section body rather than a TOC entry.
    _MIN_SECTION_CHARS = 2000

    # Case-insensitive search
    text_upper = full_text.upper()
    item_upper = item_label.upper()
    next_upper = next_item.upper()

    # Walk all occurrences of item_upper and pick the first one with enough
    # following content (skips short TOC stubs).
    pos = 0
    start = -1
    while True:
        found = text_upper.find(item_upper, pos)
        if found == -1:
            break
        end_candidate = text_upper.find(next_upper, found + 100)
        if end_candidate == -1:
            section_len = len(full_text) - found
        else:
            section_len = end_candidate - found
        if section_len >= _MIN_SECTION_CHARS:
            start = found
            break
        pos = found + 1

    if start == -1:
        return None

    end = text_upper.find(next_upper, start + 100)
    if end == -1 or end - start > _MAX_FILING_TEXT_CHARS:
        return full_text[start:start + _MAX_FILING_TEXT_CHARS]

    return full_text[start:end]


# Keep backward-compat alias for any code that still calls fetch_production_filings
# (removes the old registry argument silently).
def _fetch_production_filings_compat(self, cik, ticker, since_date, **_kwargs):
    return self.fetch_8k_filings(cik=cik, ticker=ticker, since_date=since_date)


EdgarConnector.fetch_production_filings = _fetch_production_filings_compat


# Expose extract_all at module level so tests can patch it
try:
    from interpreters.regex_interpreter import extract_all
except ImportError:
    extract_all = None  # type: ignore
