"""
Tests for edgar_context_builder.py — TDD.

EdgarContextBuilder extracts structured metadata from stored EDGAR reports
to build context objects for LLM crawl prompt generation.
"""
from unittest.mock import MagicMock


def _mock_db(reports=None):
    db = MagicMock()
    db.get_reports_by_channel.return_value = reports or []
    return db


def _make_report(form_type='8-K', period='2024-01', accession='0001507605-24-000001',
                 raw_text='MARA mined 750 BTC in January 2024.', ticker='MARA'):
    return {
        'id': 1,
        'ticker': ticker,
        'form_type': form_type,
        'report_date': '2024-01-31',
        'covering_period': period,
        'accession_number': accession,
        'source_url': f'https://www.sec.gov/Archives/edgar/data/1507605/{accession}.htm',
        'raw_text': raw_text,
        'source_channel': 'edgar',
    }


# ── EdgarContextBuilder ───────────────────────────────────────────────────────

def test_build_context_returns_edgar_context_object():
    """build_context() returns an EdgarContext dataclass with ticker and filings list."""
    from scrapers.edgar_context_builder import EdgarContextBuilder, EdgarContext
    db = _mock_db(reports=[_make_report()])
    builder = EdgarContextBuilder(db)
    ctx = builder.build_context('MARA')
    assert isinstance(ctx, EdgarContext)
    assert ctx.ticker == 'MARA'
    assert len(ctx.filings) >= 1


def test_build_context_extracts_form_type():
    """Each filing in the context includes form_type."""
    from scrapers.edgar_context_builder import EdgarContextBuilder
    db = _mock_db(reports=[_make_report(form_type='10-Q')])
    builder = EdgarContextBuilder(db)
    ctx = builder.build_context('MARA')
    assert ctx.filings[0]['form_type'] == '10-Q'


def test_build_context_empty_when_no_reports():
    """build_context() returns empty filings list when no EDGAR reports exist."""
    from scrapers.edgar_context_builder import EdgarContextBuilder, EdgarContext
    db = _mock_db(reports=[])
    builder = EdgarContextBuilder(db)
    ctx = builder.build_context('RIOT')
    assert isinstance(ctx, EdgarContext)
    assert ctx.filings == []


def test_build_context_truncates_raw_text():
    """Filing raw_text is truncated to CONTEXT_TEXT_LIMIT characters."""
    from scrapers.edgar_context_builder import EdgarContextBuilder, CONTEXT_TEXT_LIMIT
    long_text = 'A' * (CONTEXT_TEXT_LIMIT + 500)
    db = _mock_db(reports=[_make_report(raw_text=long_text)])
    builder = EdgarContextBuilder(db)
    ctx = builder.build_context('MARA')
    assert len(ctx.filings[0]['text_excerpt']) <= CONTEXT_TEXT_LIMIT
