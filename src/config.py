"""
Configuration constants for the Bitcoin Miner Data Platform.

Infrastructure probe results (2026-03-01):
  pdfplumber: Works on all MARA PDFs (text extraction confirmed, no replacement chars)
  EDGAR efts.sec.gov: Returns JSON with 'hits' key — requires User-Agent header (403 without)
  EDGAR rate limit: Empirically ~10 req/s acceptable; using 0.1s delay (conservative)
  IR URL status: RIOT=200, HIVE=200; others had 404/timeout/SSL issues — scraper handles gracefully
  RIOT CIK confirmed from EDGAR probe: 0001167419
"""
from pathlib import Path

# --- Extraction thresholds ---
# Extractions below this confidence route to review_queue for analyst approval.
CONFIDENCE_REVIEW_THRESHOLD: float = 0.75

# --- Paths ---
# Investigation data (sessions, results): backed up, not in project tree
DATA_DIR: str = str(Path.home() / "Documents/Hermeneutic/data/miners")

# Config files (companies.json, patterns/): relative to this file's parent
CONFIG_DIR: str = str(Path(__file__).parent.parent / "config")

# Archive of historical PDFs and HTMLs (OffChain/Miner/)
ARCHIVE_DIR: str = str(Path(__file__).parent.parent.parent / "Miner")

# --- Flask ---
# Port 5004 avoids collision: Skopos:5000, EVMlite:5001, polywatch-py:5002, polywatch:5003
# Override with MINERS_PORT env var: MINERS_PORT=5010 ./launch.sh
import os as _os
FLASK_PORT: int = int(_os.environ.get("MINERS_PORT", 5004))

# --- EDGAR ---
# Full-text search API — requires User-Agent header, otherwise returns 403
EDGAR_BASE_URL: str = "https://efts.sec.gov/LATEST/search-index"
EDGAR_COMPANY_URL: str = "https://www.sec.gov/cgi-bin/browse-edgar"
# Submissions API — returns all filings for a CIK in one JSON blob
EDGAR_SUBMISSIONS_URL: str = "https://data.sec.gov/submissions/CIK{cik}.json"
# Probed 2026-03-01: 0.1s between requests works without 429; conservative floor
EDGAR_REQUEST_DELAY_SECONDS: float = 0.1

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

# --- LLM Extractor (Ollama) ---
# Probed on Apple Silicon 32GB+ with Q4_K_M: ~30–50 tok/s, 262K context.
# Confirm model tag with `ollama list` before running ingest.
# Model: qwen3.5:35b-a3b (confirmed ready 2026-03-03)
LLM_BASE_URL: str = _os.environ.get("OLLAMA_BASE_URL", "http://localhost:11434")
LLM_MODEL_ID: str = _os.environ.get("OLLAMA_MODEL", "qwen3.5:35b-a3b")
LLM_TIMEOUT_SECONDS: int = 300  # 35B @ Q4_K_M: cold-start load can exceed 180s; 300s gives headroom
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
