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
    """Coverage state of a (ticker, period) cell in the redesigned coverage grid.

    Priority (highest first when multiple apply):
      DATA > DATA_QUARTERLY > REVIEW_PENDING > PARSE_FAILED > EXTRACT_FAILED >
      SCRAPER_ERROR > ANALYST_GAP > NO_DOCUMENT
    """
    DATA             = 'data'             # accepted data_point exists (monthly source)
    DATA_QUARTERLY   = 'data_quarterly'   # data from 10-Q/10-K carry/inferred
    REVIEW_PENDING   = 'review_pending'   # review_queue item awaiting analyst
    PARSE_FAILED     = 'parse_failed'     # document present, parser failed
    EXTRACT_FAILED   = 'extract_failed'   # document parsed, extraction yielded nothing
    NO_DOCUMENT      = 'no_document'      # no manifest entry and no report
    SCRAPER_ERROR    = 'scraper_error'    # scraper recorded an error for this company
    ANALYST_GAP      = 'analyst_gap'      # analyst explicitly marked no data expected


# Extraction method string constants (stored as TEXT in data_points.extraction_method)
EXTRACTION_METHOD_QUARTERLY_CARRY    = 'quarterly_carry'     # Q/3 or snapshot last-month
EXTRACTION_METHOD_QUARTERLY_INFERRED = 'quarterly_inferred'  # Q - known months
EXTRACTION_METHOD_ANNUAL_CARRY       = 'annual_carry'        # 10-K derived


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


# ── Phase II: new types for platform redesign ─────────────────────────────────

class RegimeCadence(str, Enum):
    """Expected reporting cadence for a company in a given period."""
    MONTHLY   = 'monthly'
    QUARTERLY = 'quarterly'


class ScrapeStatus(str, Enum):
    """Lifecycle status of a company's scraper."""
    NEVER_RUN    = 'never_run'
    PROBING      = 'probing'
    PROBE_OK     = 'probe_ok'
    PROBE_FAILED = 'probe_failed'
    JS_HEAVY     = 'js_heavy'
    OK           = 'ok'
    ERROR        = 'error'
    RUNNING      = 'running'


@dataclass
class RegimeWindow:
    """A time window during which a company reports at a given cadence."""
    ticker:     str
    cadence:    RegimeCadence
    start_date: str            # YYYY-MM-DD
    end_date:   Optional[str]  # YYYY-MM-DD or None (= current regime)
    notes:      str = ''
    id:         Optional[int] = None


@dataclass
class ScrapeJob:
    """A queued or completed scrape job for one company."""
    id:           int
    ticker:       str
    mode:         str            # 'historic' or 'forward'
    status:       str            # ScrapeStatus value
    created_at:   str
    started_at:   Optional[str] = None
    completed_at: Optional[str] = None
    error_msg:    Optional[str] = None


@dataclass
class BridgeSummary:
    """Result counts from a coverage_bridge.bridge_gaps or bridge_all_gaps run."""
    cells_evaluated: int = 0
    cells_filled_carry: int = 0
    cells_filled_inferred: int = 0
    cells_routed_review: int = 0
    cells_skipped_no_quarterly: int = 0


@dataclass
class MetricSchemaDef:
    """Definition of a tracked metric within a sector's global schema."""
    key:                    str
    label:                  str
    unit:                   str
    sector:                 str
    has_extraction_pattern: bool
    analyst_defined:        bool
    id:                     Optional[int] = None
