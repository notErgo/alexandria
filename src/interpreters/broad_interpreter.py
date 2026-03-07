"""
Broad LLM-based extraction: captures ALL numeric values from a miner report.

Unlike the standard extraction pipeline (which targets 13 predefined metrics),
this module asks the LLM to find every numeric metric in the document and
classify it. Known metrics map to standard buckets; novel metrics are stored
with a best-guess category and description so they can be parsed later.

Usage:
    from interpreters.broad_interpreter import BroadInterpreter
    extractor = BroadInterpreter(db)
    count = extractor.extract_report(report)
"""
import json
import logging
import re
import time
from typing import Optional

import requests

from config import LLM_BASE_URL, LLM_MODEL_ID, LLM_TIMEOUT_SECONDS

log = logging.getLogger('miners.interpreters.broad_interpreter')

# Mapping from LLM-returned metric_key synonyms -> canonical standard metric key.
# When the LLM returns one of these keys, the category is set accordingly
# and the row is also a candidate to back-fill the standard data_points table.
_STANDARD_METRIC_MAP: dict[str, str] = {
    # BTC production
    'production_btc':   'production_btc',
    'btc_produced':     'production_btc',
    'bitcoin_produced': 'production_btc',
    'btc_mined':        'production_btc',
    'bitcoin_mined':    'production_btc',
    'self_mined_btc':   'production_btc',
    # BTC treasury / holdings
    'hodl_btc':         'hodl_btc',
    'btc_holdings':     'hodl_btc',
    'bitcoin_holdings': 'hodl_btc',
    'treasury_btc':     'hodl_btc',
    'btc_treasury':     'hodl_btc',
    'total_btc':        'hodl_btc',
    # BTC sold
    'sold_btc':         'sold_btc',
    'btc_sold':         'sold_btc',
    'bitcoin_sold':     'sold_btc',
    # Hashrate
    'hashrate_eh':      'hashrate_eh',
    'hashrate':         'hashrate_eh',
    'hash_rate':        'hashrate_eh',
    'deployed_hash':    'hashrate_eh',
    'operational_hash': 'hashrate_eh',
    # Mining capacity (MW)
    'mining_mw':        'mining_mw',
    'mining_capacity':  'mining_mw',
    'power_capacity':   'mining_mw',
    # AI/HPC capacity
    'ai_hpc_mw':        'ai_hpc_mw',
    'hpc_mw':           'ai_hpc_mw',
    'ai_capacity':      'ai_hpc_mw',
    # Encumbered BTC
    'encumbered_btc':   'encumbered_btc',
    'collateralized_btc': 'encumbered_btc',
    'pledged_btc':      'encumbered_btc',
    # Net BTC balance change
    'net_btc_balance_change': 'net_btc_balance_change',
    'net_btc_change':   'net_btc_balance_change',
    # Realization rate
    'realization_rate': 'realization_rate',
    'btc_realization':  'realization_rate',
}

# Category labels for non-standard metrics
_CATEGORY_MAP: dict[str, str] = {
    'production_btc':           'btc_production',
    'hodl_btc':                 'btc_treasury',
    'sold_btc':                 'btc_sold',
    'hashrate_eh':              'hashrate',
    'mining_mw':                'mining_capacity',
    'ai_hpc_mw':                'ai_hpc',
    'encumbered_btc':           'encumbered_btc',
    'net_btc_balance_change':   'btc_net_change',
    'realization_rate':         'financial',
}

# LLM prompt for comprehensive extraction
_BROAD_PROMPT = """You are a financial data extractor for Bitcoin mining companies.

Extract EVERY numeric metric from the following press release or filing. Include:
- Bitcoin production, holdings, sold, treasury
- Hashrate (EH/s, PH/s, TH/s)
- Power / energy (MW, MWh, GWh)
- Miners deployed, energized, installed (units)
- Revenue, costs, expenses (USD, CAD, AUD)
- Facilities count, locations mentioned with numeric data
- GPU counts, server counts
- Any other quantitative metric explicitly stated

For each metric found, return one JSON object in this array:
[
  {{
    "metric_key": "snake_case_name",
    "category": "one of: btc_production|btc_treasury|btc_sold|hashrate|mining_capacity|ai_hpc|encumbered_btc|btc_net_change|financial|operational|infrastructure|unknown",
    "value": <number or null>,
    "value_text": "<original text of the number, e.g. '1,242' or '4.5'>",
    "unit": "<BTC|EH/s|PH/s|MW|MWh|USD|units|%|other>",
    "description": "<brief description, max 80 chars>",
    "confidence": <0.0-1.0>,
    "source_snippet": "<exact phrase from document, max 120 chars>"
  }}
]

RULES:
1. Do NOT invent values not stated in the document.
2. For YTD vs monthly values: include BOTH but set description to clarify scope.
3. Quarterly values: include with description noting "quarterly" or "Q1/Q2/Q3/Q4".
4. All numbers may use commas as thousands separators (1,242 = 1242).
5. Use null for value if the text is ambiguous or non-numeric.
6. If the document has a table, read row/column headers carefully.
7. Return ONLY the JSON array, no other text.

Document:
{text}"""


class BroadInterpreter:
    """Extracts all numeric values from a report using a broad LLM pass."""

    def __init__(self, db, model: Optional[str] = None):
        self._db = db
        self._model = model or LLM_MODEL_ID
        self._session = requests.Session()

    # ── Public API ────────────────────────────────────────────────────────────

    def extract_report(self, report: dict) -> int:
        """Run broad extraction on one report. Returns count of rows stored."""
        raw_text = report.get('raw_text') or ''
        if not raw_text.strip():
            log.debug("skip report %s — no raw_text", report.get('id'))
            return 0

        # Truncate to avoid LLM timeout; 16k chars covers most press releases
        text = raw_text[:16_000]
        extractions = self._call_llm(text)
        if extractions is None:
            log.warning("LLM returned no extractions for report %s (%s %s)",
                        report.get('id'), report.get('ticker'), report.get('report_date'))
            return 0

        ticker = report.get('ticker', '')
        period = report.get('report_date', '')[:10]
        report_id = report['id']

        stored = 0
        seen_keys: set = set()
        for item in extractions:
            raw_key = (item.get('metric_key') or '').strip().lower()
            if not raw_key:
                continue

            # Deduplicate within a single report (keep first/best per key)
            if raw_key in seen_keys:
                continue
            seen_keys.add(raw_key)

            canonical = _STANDARD_METRIC_MAP.get(raw_key, raw_key)
            category = item.get('category') or _CATEGORY_MAP.get(canonical, 'unknown')
            value_text = str(item.get('value_text') or item.get('value') or '')
            value = self._parse_value(item.get('value'), value_text)

            rex = {
                'report_id':         report_id,
                'ticker':            ticker,
                'period':            period,
                'metric_key':        canonical,
                'category':          category,
                'value':             value,
                'value_text':        value_text,
                'unit':              item.get('unit'),
                'description':       item.get('description'),
                'raw_json':          json.dumps(item),
                'confidence':        float(item.get('confidence') or 0.0),
                'source_snippet':    item.get('source_snippet'),
                'extraction_method': 'llm_broad',
            }
            try:
                self._db.upsert_raw_extraction(rex)
                stored += 1
            except Exception as exc:
                log.error("Failed to store raw_extraction for %s/%s/%s: %s",
                          ticker, period, canonical, exc)

        log.info("broad_extract report %s (%s %s): %d metrics stored",
                 report_id, ticker, period[:7], stored)
        return stored

    def extract_all(self, ticker: Optional[str] = None, force: bool = False) -> dict:
        """Run broad extraction on all reports that haven't been processed yet.

        Args:
            ticker: If set, only process reports for this ticker.
            force:  If True, re-process reports that already have raw_extractions.

        Returns:
            {'reports_processed': N, 'metrics_stored': N, 'errors': N}
        """
        if force:
            reports = self._db.get_all_reports_for_extraction(ticker=ticker)
        else:
            reports = self._db.get_reports_without_broad_extraction(ticker=ticker)

        total_reports = len(reports)
        metrics_stored = 0
        errors = 0
        for i, report in enumerate(reports, 1):
            try:
                count = self.extract_report(report)
                metrics_stored += count
            except Exception as exc:
                log.error("Error in broad_extract for report %s: %s", report.get('id'), exc, exc_info=True)
                errors += 1
            if i % 10 == 0 or i == total_reports:
                log.info("broad_extract progress: %d/%d reports, %d metrics",
                         i, total_reports, metrics_stored)

        return {
            'reports_processed': total_reports,
            'metrics_stored': metrics_stored,
            'errors': errors,
        }

    # ── Internal helpers ──────────────────────────────────────────────────────

    def _call_llm(self, text: str) -> Optional[list]:
        """Call Ollama and return parsed list of extraction dicts, or None on failure."""
        prompt = _BROAD_PROMPT.format(text=text)
        payload = {
            'model': self._model,
            'prompt': prompt,
            'stream': False,
            'options': {'temperature': 0.1, 'num_predict': 4096},
        }
        try:
            resp = self._session.post(
                f"{LLM_BASE_URL}/api/generate",
                json=payload,
                timeout=LLM_TIMEOUT_SECONDS,
            )
            if resp.status_code != 200:
                log.warning("Ollama returned %d for broad extraction", resp.status_code)
                return None
            body = resp.json()
            raw = body.get('response', '')
            return self._parse_json_response(raw)
        except requests.Timeout:
            log.warning("Ollama timeout on broad extraction (limit=%ds)", LLM_TIMEOUT_SECONDS)
            return None
        except requests.RequestException as exc:
            log.error("Ollama request error in broad extraction: %s", exc)
            return None

    def _parse_json_response(self, raw: str) -> Optional[list]:
        """Extract and parse the JSON array from the LLM response string."""
        # Find first [ ... ] block
        m = re.search(r'\[[\s\S]*\]', raw)
        if not m:
            log.debug("No JSON array found in LLM response; len=%d", len(raw))
            return None
        try:
            result = json.loads(m.group(0))
            if isinstance(result, list):
                return result
        except json.JSONDecodeError as exc:
            log.debug("JSON parse error in broad_extractor response: %s", exc)
        return None

    @staticmethod
    def _parse_value(raw_value, value_text: str) -> Optional[float]:
        """Parse a value to float, stripping commas."""
        if raw_value is not None:
            try:
                return float(str(raw_value).replace(',', ''))
            except (ValueError, TypeError):
                pass
        # Fall back to extracting first number from value_text
        m = re.search(r'[\d,]+\.?\d*', str(value_text))
        if m:
            try:
                return float(m.group(0).replace(',', ''))
            except ValueError:
                pass
        return None
