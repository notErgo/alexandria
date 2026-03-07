"""
Configuration constants for the Bitcoin Miner Data Platform.

Infrastructure probe results (2026-03-01):
  pdfplumber: Works on all MARA PDFs (text extraction confirmed, no replacement chars)
  EDGAR efts.sec.gov: Returns JSON with 'hits' key — requires User-Agent header (403 without)
  EDGAR rate limit: Empirically ~10 req/s acceptable; using 0.1s delay (conservative)
  IR URL status: RIOT=200, HIVE=200; others had 404/timeout/SSL issues — scraper handles gracefully
  RIOT CIK confirmed from EDGAR probe: 0001167419
"""
import json as _json
from pathlib import Path
from typing import List
import os as _os

# --- Extraction thresholds ---
# Extractions below this confidence route to review_queue for analyst approval.
CONFIDENCE_REVIEW_THRESHOLD: float = 0.75

# Reports that fail extraction this many times are promoted to dead_letter and skipped.
MAX_EXTRACTION_ATTEMPTS: int = 5

# --- Paths ---
# Investigation data (sessions, results): backed up, not in project tree
DATA_DIR: str = str(Path(
    _os.environ.get("MINERS_DATA_DIR", str(Path.home() / "Documents/Hermeneutic/data/miners"))
).expanduser())

# Config files (companies.json, patterns/): relative to this file's parent
CONFIG_DIR: str = str(Path(__file__).parent.parent / "config")

def load_companies() -> List[dict]:
    """Return the full company list from config/companies.json.

    This is the single canonical source of all tickers. Never maintain a
    hardcoded ticker list elsewhere — call this function instead.
    """
    path = Path(CONFIG_DIR) / "companies.json"
    return _json.loads(path.read_text())

def get_all_tickers() -> List[str]:
    """Return sorted list of all ticker symbols from config/companies.json."""
    return sorted(c["ticker"] for c in load_companies())


_VALID_SCRAPER_MODES: frozenset = frozenset({
    'rss', 'template', 'index', 'skip', 'playwright',
})

_VALID_FILING_REGIMES: frozenset = frozenset({
    'domestic', 'canadian', 'foreign',
})

_COMPANY_REQUIRED_FIELDS: tuple = (
    'ticker', 'name', 'tier', 'active', 'scraper_mode',
    'filing_regime', 'fiscal_year_end_month',
)


def validate_companies_config(companies: List[dict] = None) -> List[str]:
    """Validate shape and mode contracts for companies.json entries.

    Returns a list of error strings. Empty list means valid.
    Does not raise — caller decides severity.
    """
    if companies is None:
        companies = load_companies()
    errors: List[str] = []
    seen: set = set()
    for i, c in enumerate(companies):
        label = c.get('ticker', f'entry[{i}]')
        if label in seen:
            errors.append(f'{label}: duplicate ticker')
        seen.add(label)

        for field in _COMPANY_REQUIRED_FIELDS:
            if field not in c:
                errors.append(f'{label}: missing required field "{field}"')

        tier = c.get('tier')
        if not isinstance(tier, int) or tier not in (1, 2, 3):
            errors.append(f'{label}: tier must be int 1, 2, or 3, got {tier!r}')

        if not isinstance(c.get('active'), bool):
            errors.append(f'{label}: active must be a boolean')

        fye = c.get('fiscal_year_end_month')
        if not isinstance(fye, int) or isinstance(fye, bool) or not (1 <= fye <= 12):
            errors.append(f'{label}: fiscal_year_end_month must be int 1-12, got {fye!r}')

        mode = c.get('scraper_mode')
        if mode not in _VALID_SCRAPER_MODES:
            errors.append(
                f'{label}: unknown scraper_mode {mode!r}'
                f' (valid: {sorted(_VALID_SCRAPER_MODES)})'
            )
        elif mode == 'rss' and not c.get('rss_url'):
            errors.append(f'{label}: scraper_mode="rss" requires rss_url')
        elif mode == 'template' and not c.get('url_template'):
            errors.append(f'{label}: scraper_mode="template" requires url_template')
        elif mode == 'skip' and not c.get('skip_reason'):
            errors.append(f'{label}: scraper_mode="skip" requires skip_reason')

        regime = c.get('filing_regime')
        if regime not in _VALID_FILING_REGIMES:
            errors.append(
                f'{label}: unknown filing_regime {regime!r}'
                f' (valid: {sorted(_VALID_FILING_REGIMES)})'
            )

    return errors

# Archive of historical PDFs and HTMLs (OffChain/Miner/)
ARCHIVE_DIR: str = str(Path(__file__).parent.parent.parent / "Miner")

# --- Flask ---
# Port 5004 avoids collision: Skopos:5000, EVMlite:5001, polywatch-py:5002, polywatch:5003
# Override with MINERS_PORT env var: MINERS_PORT=5010 ./launch.sh
FLASK_PORT: int = int(_os.environ.get("MINERS_PORT", 5004))
FLASK_HOST: str = _os.environ.get("MINERS_HOST", "127.0.0.1")
FLASK_DEBUG: bool = _os.environ.get("MINERS_DEBUG", "0").strip().lower() in {
    "1", "true", "yes", "on"
}
# Startup config sync gate. Can be overridden at runtime via
# config_settings key: auto_sync_companies_on_startup = "1" | "0".
AUTO_SYNC_COMPANIES_ON_STARTUP: bool = _os.environ.get("MINERS_AUTO_SYNC_COMPANIES", "1").strip().lower() in {
    "1", "true", "yes", "on"
}

# --- EDGAR ---
# Full-text search API — requires User-Agent header, otherwise returns 403
EDGAR_BASE_URL: str = "https://efts.sec.gov/LATEST/search-index"
EDGAR_COMPANY_URL: str = "https://www.sec.gov/cgi-bin/browse-edgar"
# Submissions API — returns all filings for a CIK in one JSON blob
EDGAR_SUBMISSIONS_URL: str = "https://data.sec.gov/submissions/CIK{cik}.json"
# Probed 2026-03-01: 0.1s between requests works without 429; conservative floor
EDGAR_REQUEST_DELAY_SECONDS: float = 0.1
# Base backoff sleep (seconds) when EDGAR returns 429 Too Many Requests
EDGAR_RETRY_BACKOFF_BASE: float = 60.0

# --- IR Scraper ---
# 3.0s between requests: respectful crawl rate for public IR pages
IR_REQUEST_DELAY_SECONDS: float = 3.0

# --- HTML Downloader ---
# 2.0s between requests: company IR pages are lightly trafficked; this is polite
# and well below any rate limit we'd encounter on public press release pages.
HTML_DOWNLOAD_DELAY_SECONDS: float = 2.0

# --- Extraction ---
# Context window around a regex match (chars before + after) for source_snippet
EXTRACTION_CONTEXT_WINDOW: int = 500
# Maximum source_snippet length stored in DB
MAX_SOURCE_SNIPPET_LEN: int = 1000

# --- LLM Interpreter (Ollama) — Stage 2 metric extraction ---
# Used by interpreters/llm_interpreter.py for metric extraction from stored documents.
LLM_BASE_URL: str = _os.environ.get("OLLAMA_BASE_URL", "http://localhost:11434")
LLM_MODEL_ID: str = _os.environ.get("OLLAMA_MODEL", "qwen3.5:27b")
LLM_TIMEOUT_SECONDS: int = 300

# --- Crawl LLM (Ollama or Anthropic) — Stage 1 IR navigation ---
# qwen3.5:9b is used for crawling: fast enough for link navigation, avoids
# the 300s timeout that qwen3.5:27b hits when message history fills with page text.
ANTHROPIC_API_KEY: str = _os.environ.get("ANTHROPIC_API_KEY", "")
CRAWL_MODEL: str = _os.environ.get("CRAWL_MODEL", "claude-haiku-4-5-20251001")
CRAWL_OLLAMA_MODEL: str = _os.environ.get("CRAWL_OLLAMA_MODEL", "qwen3.5:9b")
CRAWL_PROVIDER: str = _os.environ.get("CRAWL_PROVIDER", "ollama")

# Context window budgets for ContextWindowSelector
CONTEXT_CHAR_BUDGET = 8_000          # monthly press releases
CONTEXT_CHAR_BUDGET_QUARTERLY = 24_000  # EDGAR 10-Q / 10-K

# Deprecated: replaced by METRIC_AGREEMENT_THRESHOLDS. Kept for backward compatibility.
LLM_AGREEMENT_THRESHOLD: float = 0.02

# Per-metric LLM/regex agreement tolerance (absolute fractional difference).
# BTC counts are integers — 1% catches rounding. Hashrate rounds to 1 decimal EH/s.
# These are seed values; runtime reads from metric_rules DB table when available.
METRIC_AGREEMENT_THRESHOLDS: dict = {
    'production_btc':          0.01,   # integer BTC counts: 1% tolerance
    'hodl_btc':                0.01,
    'sold_btc':                0.01,
    'hodl_btc_unrestricted':   0.01,
    'hodl_btc_restricted':     0.01,
    'net_btc_balance_change':  0.02,
    'encumbered_btc':          0.01,
    'hashrate_eh':             0.10,   # 1-decimal EH/s: 10% tolerance
    'realization_rate':        0.05,   # ratio: 5%
    'mining_mw':               0.05,   # MW: 5%
    'ai_hpc_mw':               0.05,
    'hpc_revenue_usd':         0.05,   # USD revenue rounds
    'gpu_count':               0.01,   # integer GPU count
}
# Fallback for any metric not in METRIC_AGREEMENT_THRESHOLDS
METRIC_AGREEMENT_THRESHOLD_DEFAULT: float = 0.02

# --- Statistical Outlier Detection ---
# Cross-report outlier thresholds: flag if |candidate - trailing_avg| / avg > threshold.
# These are seed values; runtime reads from metric_rules DB table when available.
OUTLIER_THRESHOLDS: dict = {
    'production_btc':          0.40,   # 40% swing in one month is unusual
    'hodl_btc':                0.30,
    'sold_btc':                1.00,   # sold_btc is volatile; 100% (2x) to flag
    'hodl_btc_unrestricted':   0.30,
    'hodl_btc_restricted':     1.00,
    'hashrate_eh':             0.25,
    'realization_rate':        0.20,
    'net_btc_balance_change':  1.00,
    'encumbered_btc':          1.00,
    'mining_mw':               0.30,
    'ai_hpc_mw':               0.50,
    'hpc_revenue_usd':         0.50,
    'gpu_count':               0.50,
}
OUTLIER_MIN_HISTORY: int = 3   # Minimum prior months needed to flag outliers

# --- Source Types ---
# Canonical source_type values used in reports and asset_manifest tables.
SOURCE_TYPES: dict = {
    'archive_pdf':       'Archived PDF (OffChain/Miner/)',
    'archive_html':      'Archived HTML (OffChain/Miner/)',
    'ir_press_release':  'IR press release (live scrape)',
    'prnewswire_press_release': 'PRNewswire press release (live scrape)',
    'globenewswire_press_release': 'GlobeNewswire press release (live scrape)',
    'edgar_8k':          'SEC EDGAR 8-K filing',
    'edgar_10q':         'SEC EDGAR 10-Q filing',
    'edgar_10k':         'SEC EDGAR 10-K filing',
    'manual':            'Manual entry',
}

# --- Quarterly/Annual Metric Classification ---
# Metrics where quarterly total = sum of monthly values (can infer missing month from remainder)
FLOW_METRICS: frozenset = frozenset({
    'production_btc', 'sold_btc', 'net_btc_balance_change',
})

# Metrics where quarterly value is a point-in-time snapshot (cannot disaggregate across months)
SNAPSHOT_METRICS: frozenset = frozenset({
    'hodl_btc', 'hodl_btc_restricted', 'hodl_btc_unrestricted',
    'hashrate_eh', 'realization_rate', 'encumbered_btc',
    'mining_mw', 'ai_hpc_mw', 'hpc_revenue_usd', 'gpu_count',
})

# --- New metric valid ranges documented here for cross-reference with confidence.py ---
# net_btc_balance_change: signed delta, large range to handle any direction
# encumbered_btc: total btc posted as collateral across all loan facilities
# mining_mw: operational power capacity for bitcoin mining
# ai_hpc_mw: operational power capacity for AI/HPC workloads
# hpc_revenue_usd: revenue from AI/HPC hosting contracts (USD, not millions)
# gpu_count: total GPU units deployed (H100s and equivalents)
