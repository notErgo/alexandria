"""
EdgarContextBuilder — builds structured EDGAR context objects for LLM crawl prompts.

Reads stored EDGAR reports from the DB and packages them into an EdgarContext
that can be serialized into a crawl prompt template.
"""
import logging
from dataclasses import dataclass, field
from typing import List, Optional

log = logging.getLogger('miners.scrapers.edgar_context_builder')

# Maximum characters of raw_text included per filing in the context
CONTEXT_TEXT_LIMIT = 2000


@dataclass
class EdgarContext:
    """Structured EDGAR context for a single ticker."""
    ticker: str
    filings: List[dict] = field(default_factory=list)


class EdgarContextBuilder:
    """Build EdgarContext objects from stored EDGAR reports."""

    def __init__(self, db):
        self._db = db

    def build_context(self, ticker: str) -> EdgarContext:
        """Return an EdgarContext for ticker by reading stored EDGAR reports.

        Args:
            ticker: Company ticker symbol

        Returns:
            EdgarContext with filings list (form_type, period, accession_number,
            source_url, text_excerpt).
        """
        try:
            reports = self._db.get_reports_by_channel(
                ticker=ticker, channel='edgar'
            )
        except Exception:
            log.error("Failed to fetch EDGAR reports for %s", ticker, exc_info=True)
            reports = []

        filings = []
        for r in reports:
            raw_text = r.get('raw_text') or ''
            filings.append({
                'form_type': r.get('form_type'),
                'period': r.get('covering_period') or r.get('report_date'),
                'accession_number': r.get('accession_number'),
                'source_url': r.get('source_url'),
                'text_excerpt': raw_text[:CONTEXT_TEXT_LIMIT],
            })

        return EdgarContext(ticker=ticker, filings=filings)
