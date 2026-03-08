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


# ── _strip_xbrl_preamble ─────────────────────────────────────────────────────

class TestStripXbrlPreamble:
    """_strip_xbrl_preamble removes iXBRL context block from EDGAR filing text."""

    def test_strips_cik_prefixed_lines_before_cover_page(self):
        """Text starting with CIK-prefixed XBRL lines is stripped to UNITED STATES."""
        from parsers.annual_report_parser import _strip_xbrl_preamble

        preamble = (
            '0001507605 false --12-31 2021 Q3\n'
            '0001507605 2021-01-01 2021-09-30\n'
            '0001507605 us-gaap:CommonStockMember 2021-06-30\n'
            'iso4217:USD xbrli:shares xbrli:pure MARA:Integer\n'
        )
        body = 'UNITED STATES SECURITIES AND EXCHANGE COMMISSION\nFORM 10-Q\nWe mined 700 BTC.'
        result = _strip_xbrl_preamble(preamble + body)
        assert result.startswith('UNITED STATES'), (
            "Expected result to start with cover page, got: " + repr(result[:60])
        )
        assert 'We mined 700 BTC' in result
        # Preamble noise must be gone
        assert 'us-gaap:CommonStockMember' not in result

    def test_no_op_when_no_xbrl_preamble(self):
        """Text that does not start with a CIK pattern is returned unchanged."""
        from parsers.annual_report_parser import _strip_xbrl_preamble

        text = 'UNITED STATES SECURITIES AND EXCHANGE COMMISSION\nFORM 10-Q\nItem 1. Financials'
        assert _strip_xbrl_preamble(text) == text

    def test_no_op_when_cover_page_not_found(self):
        """If cover page marker is missing, return original text unchanged."""
        from parsers.annual_report_parser import _strip_xbrl_preamble

        text = '0001507605 false --12-31 2021 Q3\nno cover page here'
        assert _strip_xbrl_preamble(text) == text

    def test_parse_html_strips_xbrl_preamble(self):
        """AnnualReportParser.parse_html must not include iXBRL preamble in result text."""
        from parsers.annual_report_parser import AnnualReportParser

        # Simulate an iXBRL filing: XBRL context block followed by real HTML content
        xbrl_block = (
            '0001507605 false --12-31 2021 Q3 0001507605 2021-01-01 2021-09-30 '
            '0001507605 us-gaap:CommonStockMember 2021-06-30 iso4217:USD xbrli:shares '
        )
        html = f'''<html><body>
        <div style="display:none">{xbrl_block}</div>
        <p>UNITED STATES SECURITIES AND EXCHANGE COMMISSION</p>
        <p>FORM 10-Q</p>
        <p>PART I. FINANCIAL INFORMATION</p>
        <p>Item 2. We mined 700 BTC in Q3 2021.</p>
        </body></html>'''

        parser = AnnualReportParser()
        result = parser.parse_html(html)
        assert 'us-gaap:CommonStockMember' not in result.text, (
            "iXBRL preamble must be stripped from parsed text"
        )
        assert 'We mined 700 BTC' in result.text


# ── TOC stub filter (parse_html) ─────────────────────────────────────────────

def test_parse_html_filters_toc_stubs():
    """Sections with <300 chars body are filtered out (TOC stubs)."""
    from parsers.annual_report_parser import AnnualReportParser
    # HTML with a TOC section (short) and a real section (long)
    html = """<html><body>
    <p>Item 1. Business</p>
    <p>Item 2. Risk Factors</p>
    <p>Item 1. Business</p>
    """ + "<p>" + "A" * 400 + "</p>" + """
    <p>Item 2. Risk Factors</p>
    """ + "<p>" + "B" * 400 + "</p>" + """
    </body></html>"""
    parser = AnnualReportParser()
    result = parser.parse_html(html)
    # Should not have stub sections with <300 chars body
    for section in result.sections:
        if section.name == 'full_text':
            continue
        body_lines = section.text.strip().split('\n', 1)
        body = body_lines[1].strip() if len(body_lines) > 1 else ''
        assert len(body) >= 300 or section.name == 'full_text', \
            f"TOC stub section {section.name!r} not filtered: body len={len(body)}"


def test_parse_html_all_stubs_falls_back_to_full_text():
    """If all sections are stubs, returns a single full_text section."""
    from parsers.annual_report_parser import AnnualReportParser
    # HTML where every Item section is tiny (TOC-like)
    html = """<html><body>
    <p>Item 1. Business</p>
    <p>Item 2. Risk Factors</p>
    </body></html>"""
    parser = AnnualReportParser()
    result = parser.parse_html(html)
    # Must have at least one section
    assert len(result.sections) >= 1
    # If all stubs, should fall back to full_text
    names = [s.name for s in result.sections]
    assert 'full_text' in names


def test_parse_html_real_sections_preserved():
    """Sections with substantial content (>=300 chars) are kept."""
    from parsers.annual_report_parser import AnnualReportParser
    long_body = "X" * 500
    html = f"""<html><body><p>Item 1. Business</p><p>{long_body}</p></body></html>"""
    parser = AnnualReportParser()
    result = parser.parse_html(html)
    item_sections = [s for s in result.sections if s.name != 'full_text']
    assert len(item_sections) >= 1
    assert any(len(s.text) > 300 for s in item_sections)
