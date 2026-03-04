"""
Press release parser for monthly production reports (HTML and PDF).

Wraps the existing _parse_pdf / _parse_html logic from archive_ingestor.
Returns a ParseResult with a single 'full_text' section.
"""
import logging
from pathlib import Path

from miner_types import ParseResult, TextSection

log = logging.getLogger('miners.parsers.press_release_parser')


class PressReleaseParser:
    """Parser for monthly production press releases (archive HTML and PDF)."""

    def _parse_html(self, path: Path) -> str:
        """Extract text from HTML using BeautifulSoup."""
        try:
            from bs4 import BeautifulSoup
            with open(str(path), encoding='utf-8', errors='replace') as f:
                soup = BeautifulSoup(f, 'lxml')
            return soup.get_text(separator=' ', strip=True)
        except OSError as e:
            log.error("Cannot read HTML file %s: %s", path, e)
            return ''
        except Exception as e:
            log.error("Failed to parse HTML %s: %s", path, e, exc_info=True)
            return ''

    def _parse_pdf(self, path: Path) -> tuple:
        """Extract text from PDF using pdfplumber. Returns (text, page_count)."""
        try:
            import pdfplumber
            with pdfplumber.open(str(path)) as pdf:
                pages = [page.extract_text() or '' for page in pdf.pages]
            return '\n'.join(pages), len(pages)
        except Exception as e:
            log.error("Failed to parse PDF %s: %s", path, e, exc_info=True)
            return '', 0

    def parse(self, path: Path) -> ParseResult:
        """Parse a press release file (HTML or PDF). Returns ParseResult."""
        suffix = path.suffix.lower()

        if suffix == '.pdf':
            text, page_count = self._parse_pdf(path)
            from parsers.annual_report_parser import detect_parse_quality
            quality = detect_parse_quality(text, page_count=page_count)
            parser_used = 'press_release_pdf'
        else:
            text = self._parse_html(path)
            from parsers.annual_report_parser import detect_parse_quality
            quality = detect_parse_quality(text, page_count=0)
            parser_used = 'press_release_html'

        sections = [TextSection(
            name='full_text',
            text=text,
            char_start=0,
            char_end=len(text),
        )]

        return ParseResult(
            text=text,
            sections=sections,
            parse_quality=quality,
            parser_used=parser_used,
            page_count=0,
        )
