"""
Shared utilities for HTML-to-plain-text conversion and report field population.

Design contract
---------------
Every scraper that ingests HTML content must store **two** fields:

  raw_html  — the original markup, byte-for-byte as received (truncated to
              MAX_RAW_HTML chars to cap storage). Preserved for the document
              viewer and for future re-processing without re-fetching.

  raw_text  — the extracted plain text used by the extraction pipeline
              (regex patterns and LLM prompts). Always derived from raw_html
              at ingest time, never stored in place of raw_html.

Use ``make_html_report_fields(html)`` to populate both atomically.  The
function makes it structurally impossible to populate one field without
the other, preventing the class of bug where raw_html is silently omitted.

Usage
-----
    from infra.text_utils import make_html_report_fields

    report = {
        "ticker":      ticker,
        "report_date": period_str,
        "source_type": "ir_press_release",
        "source_url":  url,
        **make_html_report_fields(page.text),
        "parsed_at":   ...,
    }
    db.insert_report(report)
"""
from __future__ import annotations
import re as _re

MAX_RAW_HTML: int = 300_000
MAX_RAW_TEXT: int = 50_000

# ---------------------------------------------------------------------------
# IR press-release boilerplate stripping
# ---------------------------------------------------------------------------

# Matches the date/time line that opens an Equisolve/GlobeNewswire-style
# article: "May 03, 2024 8:30 am EDT" or plain "March 5, 2024"
_IR_DATE_LINE_RE = _re.compile(
    r'^(?:Jan(?:uary)?|Feb(?:ruary)?|Mar(?:ch)?|Apr(?:il)?|May|Jun(?:e)?|'
    r'Jul(?:y)?|Aug(?:ust)?|Sep(?:tember)?|Oct(?:ober)?|Nov(?:ember)?|Dec(?:ember)?)'
    r'\s+\d{1,2},?\s+\d{4}',
    _re.IGNORECASE,
)

# Boilerplate section headers — cut everything from the first matching line onward.
# Specific IR markers (Recent Announcements, Investor Notice, Source:) are matched
# unconditionally.  Generic headers (About, Forward-Looking) are only matched at
# or beyond the 40 % mark to avoid false positives in article content.
_PR_FOOTER_UNCONDITIONAL = [
    _re.compile(r'^Recent Announcements\s*$'),
    _re.compile(r'^Investor Notice\s*$'),
    _re.compile(r'^Source:\s+[A-Z]'),
    _re.compile(r'^email\s*$', _re.IGNORECASE),         # Equisolve footer icon label
    _re.compile(r'©\s*\d{4}'),                           # copyright line
    _re.compile(r'^Distributed by\s+\w', _re.IGNORECASE),  # wire attribution (GlobeNewswire etc.)
]
_PR_FOOTER_CONDITIONAL = [
    _re.compile(r'^Forward-Looking Statements?\s*$', _re.IGNORECASE),
    _re.compile(r'^Cautionary Statements?\s*$', _re.IGNORECASE),
    _re.compile(r'^About [A-Z][A-Za-z]'),
    _re.compile(r'^For more information,?\s+visit\s*$', _re.IGNORECASE),
    _re.compile(r'^(?:Investor\s+Relations|Media)\s+Contact\b', _re.IGNORECASE),
    _re.compile(r'^For\s+(?:investor|media)\s+(?:relations\s+)?(?:contact|inquiries|information)\b',
                _re.IGNORECASE),
]

# EDGAR-specific footer sentinels matched at or after 30% of the document.
# The SIGNATURES section and exhibit index are pure boilerplate that appear
# after all substantive content in 8-K, 10-Q, and 10-K filings.
_EDGAR_FOOTER_SENTINELS = [
    _re.compile(r'^\s*SIGNATURES?\s*$', _re.MULTILINE),
    _re.compile(r'Pursuant to the requirements of the Securities Exchange Act'),
    _re.compile(r'^\s*EXHIBIT\s+INDEX\s*$', _re.MULTILINE),
]


def extract_document_title(raw_html: str | None, raw_text: str | None = None) -> str | None:
    """Return a best-effort document title from stored HTML or plain text.

    Priority:
    1. ``og:title`` / ``twitter:title`` meta tags
    2. ``<title>``
    3. first ``<h1>``
    4. first plausible non-boilerplate plain-text line
    """
    def _clean(candidate: str | None) -> str | None:
        if not candidate:
            return None
        text = _re.sub(r'\s+', ' ', str(candidate)).strip(" \t\r\n-|\u2013\u2014")
        if len(text) < 8:
            return None
        if len(text) > 220:
            text = text[:217].rstrip() + "..."
        return text or None

    if raw_html:
        try:
            from bs4 import BeautifulSoup
            soup = BeautifulSoup(raw_html, "lxml")
            for prop in ('og:title', 'twitter:title'):
                node = soup.find('meta', attrs={'property': prop}) or soup.find('meta', attrs={'name': prop})
                if node and node.get('content'):
                    title = _clean(node.get('content'))
                    if title:
                        return title
            if soup.title and soup.title.string:
                title = _clean(soup.title.string)
                if title:
                    return title
            h1 = soup.find('h1')
            if h1:
                title = _clean(h1.get_text(" ", strip=True))
                if title:
                    return title
        except Exception:
            pass

    if raw_text:
        lines = [_clean(line) for line in str(raw_text).splitlines()]
        for line in lines[:20]:
            if not line:
                continue
            lower = line.lower()
            if lower in {'source:', 'email'}:
                continue
            if _IR_DATE_LINE_RE.match(line):
                continue
            if lower.startswith('distributed by '):
                continue
            return line
    return None


def strip_press_release_boilerplate(text: str | None) -> str:
    """Remove IR website navigation and boilerplate footer from plain-text PRs.

    **Header**: identifies the article headline as the last non-empty line
    before the first date line (``Month DD, YYYY ...``) and discards
    everything above it.  If no date line is found the header is untouched.

    **Footer**: truncates at the first occurrence of a recognised boilerplate
    sentinel.  Unconditional sentinels (``Recent Announcements``,
    ``Investor Notice``, ``Source:``, copyright) are matched anywhere past
    the article start.  Generic sentinels (``Forward-Looking Statements``,
    ``About [Company]``) require the match to be past the 40 % mark so they
    do not fire on legitimate in-article references.

    Safe on ``None`` or empty input — returns ``""`` in both cases.
    """
    if not text:
        return ""

    lines = text.split('\n')

    # --- Strip navigation header ---
    start_idx = 0
    for i, line in enumerate(lines):
        if _IR_DATE_LINE_RE.match(line.strip()) and i >= 2:
            # Walk back to find the last non-empty line (the headline)
            for j in range(i - 1, max(i - 8, -1), -1):
                if lines[j].strip():
                    start_idx = j
                    break
            break

    # --- Strip footer boilerplate ---
    end_idx = len(lines)
    body_len = len(lines) - start_idx
    threshold_40pct = start_idx + int(body_len * 0.4)

    for i in range(start_idx, len(lines)):
        s = lines[i].strip()
        if any(p.match(s) for p in _PR_FOOTER_UNCONDITIONAL):
            end_idx = i
            break
        if i >= threshold_40pct and any(p.match(s) for p in _PR_FOOTER_CONDITIONAL):
            end_idx = i
            break

    return '\n'.join(lines[start_idx:end_idx]).strip()


def strip_edgar_boilerplate(text: str | None) -> str:
    """Remove SEC filing boilerplate footer from plain-text EDGAR documents.

    Truncates at the first of:
    - A standalone ``SIGNATURES`` section header
    - ``Pursuant to the requirements of the Securities Exchange Act...``
    - A standalone ``EXHIBIT INDEX`` block

    The match must fall at or after the 30% mark of the document to prevent
    false positives in the main filing body.

    Safe on ``None`` or empty input — returns ``""`` in both cases.
    """
    if not text:
        return ""
    threshold = int(len(text) * 0.30)
    cutoff = len(text)
    for pattern in _EDGAR_FOOTER_SENTINELS:
        m = pattern.search(text)
        if m and m.start() >= threshold:
            cutoff = min(cutoff, m.start())
    return text[:cutoff].rstrip()


def edgar_to_plain(html: str | None) -> str:
    """Convert EDGAR HTML filing to clean plain text.

    Extends ``html_to_plain`` with two EDGAR-specific steps:

    1. Removes the ``<head>`` element before extraction — prevents the
       ``<title>`` (e.g. "10-Q") from landing at the start of the text and
       blocking ``_strip_xbrl_preamble``'s CIK-prefix guard.
    2. Calls ``_strip_xbrl_preamble()`` to discard the iXBRL context block
       (CIK + taxonomy namespace lines) that precedes the SEC cover page in
       modern EDGAR inline XBRL filings.

    Safe on ``None`` or empty input — returns ``""`` in both cases.
    """
    if not html:
        return ""
    from bs4 import BeautifulSoup
    from parsers.annual_report_parser import convert_tables_to_pipe_text, _strip_xbrl_preamble
    soup = BeautifulSoup(html, "lxml")
    if soup.head:
        soup.head.decompose()
    convert_tables_to_pipe_text(soup)
    text = soup.get_text(separator="\n", strip=True)
    return _strip_xbrl_preamble(text)


def html_to_plain(html: str | None, separator: str = "\n") -> str:
    """Strip markup from *html* and return plain text.

    Tables are first converted to pipe-delimited rows
    (``cell1 | cell2 | cell3``) so label-value associations survive
    ``get_text()`` flattening.  This mirrors the behaviour of
    ``PressReleaseParser._parse_html()`` and ``AnnualReportParser.parse_html()``.

    Safe on ``None`` or empty input — returns ``""`` in both cases.
    Uses BeautifulSoup with the lxml parser (same parser used everywhere
    else in the codebase) so behaviour is consistent.
    """
    if not html:
        return ""
    from bs4 import BeautifulSoup
    from parsers.annual_report_parser import convert_tables_to_pipe_text
    soup = BeautifulSoup(html, "lxml")
    convert_tables_to_pipe_text(soup)
    return soup.get_text(separator=separator, strip=True)


def make_html_report_fields(
    html: str | None,
    *,
    max_raw_html: int = MAX_RAW_HTML,
    max_raw_text: int = MAX_RAW_TEXT,
    separator: str = " ",
) -> dict:
    """Return ``{"raw_html": ..., "raw_text": ...}`` for an HTML document.

    *raw_html* preserves the original markup truncated to *max_raw_html*
    characters.  *raw_text* is the BeautifulSoup plain-text extraction
    truncated to *max_raw_text* characters.

    When *html* is ``None`` or empty, ``raw_html`` is ``None`` and
    ``raw_text`` is ``""``.

    The returned dict is intended to be spread (``**``) directly into a
    report dict passed to ``db.insert_report()``.
    """
    if not html:
        return {"raw_html": None, "raw_text": ""}
    plain = html_to_plain(html, separator=separator)
    return {
        "raw_html": html[:max_raw_html],
        "raw_text": plain[:max_raw_text],
    }
