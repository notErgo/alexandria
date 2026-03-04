"""
Tests for document parser abstraction — TDD.

Tests should FAIL before parsers/ module is created.
"""
import pytest
from pathlib import Path


# ── detect_parse_quality ─────────────────────────────────────────────────────

def test_detect_parse_quality_text_ok():
    """Sufficient text per page → 'text_ok'."""
    from parsers.annual_report_parser import detect_parse_quality
    # 1000 chars, 2 pages → 500 chars/page (>200 threshold)
    quality = detect_parse_quality('a' * 1000, page_count=2)
    assert quality == 'text_ok'


def test_detect_parse_quality_ocr_needed():
    """Very few chars per page → 'ocr_needed'."""
    from parsers.annual_report_parser import detect_parse_quality
    # 40 chars, 2 pages → 20 chars/page (<50 threshold)
    quality = detect_parse_quality('a' * 40, page_count=2)
    assert quality == 'ocr_needed'


def test_detect_parse_quality_parse_failed():
    """Empty text → 'parse_failed'."""
    from parsers.annual_report_parser import detect_parse_quality
    quality = detect_parse_quality('', page_count=5)
    assert quality == 'parse_failed'


def test_detect_parse_quality_html_zero_pages():
    """HTML (page_count=0) with text → 'text_ok'."""
    from parsers.annual_report_parser import detect_parse_quality
    quality = detect_parse_quality('hello world ' * 50, page_count=0)
    assert quality == 'text_ok'


def test_detect_parse_quality_text_sparse():
    """Between 50 and 200 chars/page → 'text_sparse'."""
    from parsers.annual_report_parser import detect_parse_quality
    # 100 chars, 1 page → 100 chars/page (between 50 and 200)
    quality = detect_parse_quality('a' * 100, page_count=1)
    assert quality == 'text_sparse'


# ── PressReleaseParser ────────────────────────────────────────────────────────

def test_press_release_html_returns_parse_result(tmp_path):
    """PressReleaseParser returns a ParseResult for an HTML file."""
    from parsers.press_release_parser import PressReleaseParser
    from miner_types import ParseResult

    html_file = tmp_path / 'release.html'
    html_file.write_text(
        '<html><body><p>MARA Holdings mined 700 BTC in January 2024.</p></body></html>',
        encoding='utf-8',
    )
    parser = PressReleaseParser()
    result = parser.parse(html_file)
    assert isinstance(result, ParseResult)
    assert 'MARA' in result.text or '700' in result.text
    assert result.parser_used in ('press_release_html', 'press_release_bs4')


def test_press_release_section_matches_full_text(tmp_path):
    """PressReleaseParser produces a single section named 'full_text'."""
    from parsers.press_release_parser import PressReleaseParser

    html_file = tmp_path / 'release.html'
    html_file.write_text(
        '<html><body><p>MARA Holdings mined 700 BTC in January 2024.</p></body></html>',
        encoding='utf-8',
    )
    parser = PressReleaseParser()
    result = parser.parse(html_file)
    assert len(result.sections) >= 1
    assert result.sections[0].name == 'full_text'


# ── AnnualReportParser ────────────────────────────────────────────────────────

def test_annual_report_parser_detects_edgar_sections():
    """AnnualReportParser splits EDGAR HTML into Item sections."""
    from parsers.annual_report_parser import AnnualReportParser

    # Fake EDGAR HTML with Item headers
    html = """
    <html><body>
    <p>ITEM 1. BUSINESS</p>
    <p>We are a mining company.</p>
    <p>ITEM 7. MANAGEMENT DISCUSSION</p>
    <p>We mined 700 BTC in 2024.</p>
    </body></html>
    """
    parser = AnnualReportParser()
    result = parser.parse_html(html)
    from miner_types import ParseResult
    assert isinstance(result, ParseResult)
    assert result.parse_quality == 'text_ok'
    assert result.parser_used == 'edgar_html_bs4'


def test_annual_report_parser_no_items_single_section():
    """AnnualReportParser returns single 'full_text' section when no Item headers."""
    from parsers.annual_report_parser import AnnualReportParser

    html = '<html><body><p>Some text with no EDGAR headers.</p></body></html>'
    parser = AnnualReportParser()
    result = parser.parse_html(html)
    assert len(result.sections) == 1
    assert result.sections[0].name == 'full_text'
