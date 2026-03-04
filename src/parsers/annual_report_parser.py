"""
Annual report parser for SEC EDGAR 10-K and 10-Q filings.

Wraps HTML/PDF parsing with EDGAR-specific section detection.
"""
import re
import logging
from pathlib import Path
from typing import Optional

from miner_types import ParseResult, TextSection

log = logging.getLogger('miners.parsers.annual_report_parser')

# Regex to detect EDGAR Item headers (e.g. "ITEM 1. BUSINESS", "Item 7A")
_EDGAR_ITEM_RE = re.compile(r'(?i)\bitem\s+(\d+[a-z]?)\b')


def detect_parse_quality(text: str, page_count: int) -> str:
    """Determine quality of parsed text.

    Returns:
        'parse_failed'  — empty text
        'text_ok'       — HTML (page_count==0) or sufficient chars/page
        'ocr_needed'    — very few chars/page (<50)
        'text_sparse'   — between 50 and 200 chars/page
    """
    stripped = text.strip() if text else ''
    if not stripped:
        return 'parse_failed'
    if page_count == 0:
        return 'text_ok'
    chars_per_page = len(stripped) / page_count
    if chars_per_page < 50:
        return 'ocr_needed'
    if chars_per_page < 200:
        return 'text_sparse'
    return 'text_ok'


class AnnualReportParser:
    """Parser for SEC EDGAR annual/quarterly reports."""

    def parse_html(self, html: str) -> ParseResult:
        """Parse EDGAR HTML, split by Item headers into sections.

        If no Item headers found, returns a single 'full_text' section.
        """
        from bs4 import BeautifulSoup

        soup = BeautifulSoup(html, 'lxml')
        full_text = soup.get_text(separator=' ', strip=True)
        quality = detect_parse_quality(full_text, page_count=0)

        # Find all Item header positions
        sections = []
        matches = list(_EDGAR_ITEM_RE.finditer(full_text))

        if not matches:
            sections.append(TextSection(
                name='full_text',
                text=full_text,
                char_start=0,
                char_end=len(full_text),
            ))
        else:
            for i, m in enumerate(matches):
                section_start = m.start()
                section_end = matches[i + 1].start() if i + 1 < len(matches) else len(full_text)
                section_text = full_text[section_start:section_end].strip()
                item_label = f"item_{m.group(1).lower()}"
                sections.append(TextSection(
                    name=item_label,
                    text=section_text,
                    char_start=section_start,
                    char_end=section_end,
                ))

        return ParseResult(
            text=full_text,
            sections=sections,
            parse_quality=quality,
            parser_used='edgar_html_bs4',
            page_count=0,
        )

    def parse_pdf(self, path: Path) -> ParseResult:
        """Parse EDGAR PDF using pymupdf (fitz). Falls back to parse_failed on import error."""
        try:
            import fitz  # pymupdf
        except ImportError:
            log.error("pymupdf not installed; cannot parse PDF: %s", path)
            return ParseResult(
                text='',
                sections=[],
                parse_quality='parse_failed',
                parser_used='annual_report_pymupdf',
                page_count=0,
            )

        try:
            doc = fitz.open(str(path))
            pages = []
            for page in doc:
                pages.append(page.get_text())
            doc.close()
            full_text = '\n'.join(pages)
            page_count = len(pages)
            quality = detect_parse_quality(full_text, page_count=page_count)
            sections = [TextSection(
                name='full_text',
                text=full_text,
                char_start=0,
                char_end=len(full_text),
            )]
            return ParseResult(
                text=full_text,
                sections=sections,
                parse_quality=quality,
                parser_used='annual_report_pymupdf',
                page_count=page_count,
            )
        except Exception as e:
            log.error("Failed to parse PDF %s: %s", path, e, exc_info=True)
            return ParseResult(
                text='',
                sections=[],
                parse_quality='parse_failed',
                parser_used='annual_report_pymupdf',
                page_count=0,
            )

    def parse(self, path: Path) -> ParseResult:
        """Parse a file based on extension."""
        suffix = path.suffix.lower()
        if suffix == '.pdf':
            return self.parse_pdf(path)
        # Default to HTML parsing
        try:
            html = path.read_text(encoding='utf-8', errors='replace')
        except OSError as e:
            log.error("Cannot read file %s: %s", path, e)
            return ParseResult(
                text='',
                sections=[],
                parse_quality='parse_failed',
                parser_used='edgar_html_bs4',
                page_count=0,
            )
        return self.parse_html(html)
