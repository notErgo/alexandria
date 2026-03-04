"""
Shared dataclasses and enums used across scrapers, extractors, DB layer, and routes.

Note: This module is named 'types' which shadows the stdlib 'types' module.
Import from this module explicitly: `from types import ExtractionResult`
(works correctly because pytest.ini sets pythonpath=src, placing src/ first).
If import conflicts arise at runtime, rename to miner_types.py and update all imports.
"""
from dataclasses import dataclass, field
from enum import Enum
from datetime import date, datetime
from typing import Optional


class SourceType(Enum):
    ARCHIVE_PDF = "archive_pdf"
    ARCHIVE_HTML = "archive_html"
    IR_PRESS_RELEASE = "ir_press_release"
    EDGAR_8K = "edgar_8k"
    EDGAR_10Q = "edgar_10q"
    EDGAR_10K = "edgar_10k"
    MANUAL = "manual"


class ReviewStatus(Enum):
    PENDING = "PENDING"
    APPROVED = "APPROVED"
    REJECTED = "REJECTED"
    EDITED = "EDITED"


class Metric(Enum):
    PRODUCTION_BTC = "production_btc"
    HODL_BTC = "hodl_btc"
    LIQUIDATION_BTC = "sold_btc"
    HASHRATE_EH = "hashrate_eh"
    REALIZATION_RATE = "realization_rate"


@dataclass
class Company:
    ticker: str
    name: str
    tier: int
    ir_url: str
    pr_base_url: Optional[str]
    cik: Optional[str]
    active: bool = True


@dataclass
class Report:
    ticker: str
    report_date: date
    source_type: SourceType
    source_url: Optional[str]
    raw_text: Optional[str]
    published_date: Optional[date] = None
    parsed_at: Optional[datetime] = None
    id: Optional[int] = None


@dataclass
class DataPoint:
    ticker: str
    period: date
    metric: str
    value: float
    unit: str
    confidence: float
    report_id: Optional[int] = None
    extraction_method: Optional[str] = None
    source_snippet: Optional[str] = None
    created_at: Optional[datetime] = None
    id: Optional[int] = None


@dataclass
class ExtractionResult:
    metric: str
    value: float
    unit: str
    confidence: float
    extraction_method: str
    source_snippet: str
    pattern_id: str


@dataclass
class ReviewItem:
    ticker: str
    period: date
    metric: str
    raw_value: str
    confidence: float
    source_snippet: Optional[str]
    status: ReviewStatus
    reviewer_note: Optional[str] = None
    data_point_id: Optional[int] = None
    id: Optional[int] = None


@dataclass
class IngestSummary:
    reports_ingested: int = 0
    data_points_extracted: int = 0
    review_flagged: int = 0
    errors: int = 0


@dataclass
class ExtractionSummary:
    reports_processed: int = 0
    data_points_extracted: int = 0
    review_flagged: int = 0
    errors: int = 0


class IngestState(str, Enum):
    """Lifecycle state of an asset_manifest entry."""
    PENDING = "pending"
    INGESTED = "ingested"
    FAILED = "failed"
    SKIPPED = "skipped"
    LEGACY_UNDATED = "legacy_undated"


class CellState(str, Enum):
    """Coverage state of a (ticker, period) cell in the coverage grid."""
    NO_SOURCE = "no_source"
    LEGACY_UNDATED = "legacy_undated"
    PENDING_INGEST = "pending_ingest"
    INGESTED_PENDING_EXTRACTION = "ingested_pending_extraction"
    EXTRACTED_IN_REVIEW = "extracted_in_review"
    ACCEPTED = "accepted"


@dataclass
class ScanResult:
    """Result of scanning the archive directory for manifest entries."""
    total_found: int = 0
    already_ingested: int = 0
    newly_discovered: int = 0
    legacy_undated: int = 0
    failed: int = 0
    tickers_scanned: list = None

    def __post_init__(self):
        if self.tickers_scanned is None:
            self.tickers_scanned = []


@dataclass
class TextSection:
    """A named section of text from a parsed document."""
    name: str
    text: str
    char_start: int
    char_end: int


@dataclass
class ParseResult:
    """Result from parsing a document (PDF or HTML)."""
    text: str
    sections: list
    parse_quality: str
    parser_used: str
    page_count: int
