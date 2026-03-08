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

MAX_RAW_HTML: int = 300_000
MAX_RAW_TEXT: int = 50_000


def html_to_plain(html: str | None, separator: str = " ") -> str:
    """Strip markup from *html* and return plain text.

    Safe on ``None`` or empty input — returns ``""`` in both cases.
    Uses BeautifulSoup with the lxml parser (same parser used everywhere
    else in the codebase) so behaviour is consistent.
    """
    if not html:
        return ""
    from bs4 import BeautifulSoup
    return BeautifulSoup(html, "lxml").get_text(separator=separator, strip=True)


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
