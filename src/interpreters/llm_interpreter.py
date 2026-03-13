"""
LLM-based metric extractor using Ollama (local inference).

Calls Ollama /api/generate with a metric-specific prompt + document text.
Returns an ExtractionResult or None on any failure.

Failure modes handled silently (return None):
  - Network error / timeout
  - HTTP 4xx / 5xx from Ollama
  - Malformed JSON in LLM response
  - Null or out-of-range value in LLM response
  - Missing metric in response JSON
"""
import json
import logging
from typing import Optional

try:
    import json_repair as _json_repair
    _HAS_JSON_REPAIR = True
except ImportError:
    _HAS_JSON_REPAIR = False

import requests

from config import LLM_BACKEND, LLM_BASE_URL, LLM_MODEL_ID, LLM_TIMEOUT_SECONDS
from interpreters.confidence import METRIC_VALID_RANGES
from miner_types import ExtractionResult


def _active_model(db=None) -> str:
    """Return the currently configured Ollama model name.

    Checks the config_settings DB row 'ollama_model' first (set via the UI).
    Falls back to the compile-time constant LLM_MODEL_ID (env var or default).
    """
    if db is not None:
        try:
            val = db.get_config('ollama_model')
            if val:
                return val
        except Exception:
            pass
    return LLM_MODEL_ID

# Valid ranges for quarterly/annual aggregated values (3x the monthly bounds for flow metrics).
# Snapshot metrics (hodl_btc, hashrate_eh, etc.) keep the same bounds since they are
# point-in-time values, not sums.
_QUARTERLY_VALID_RANGES = {
    k: (lo, hi * 3) for k, (lo, hi) in METRIC_VALID_RANGES.items()
}

log = logging.getLogger('miners.interpreters.llm_interpreter')

# Hardcoded default prompts per metric.
# Stored as DB overrides (llm_prompts table) take priority when available.
_DEFAULT_PROMPTS: dict = {
    "production_btc": (
        "You are a financial data extractor. Your task: find the TOTAL number of bitcoin this "
        "company MINED, PRODUCED, EARNED, or SELF-MINED during THIS reporting month.\n\n"
        "Common phrasings to look for:\n"
        "- 'mined X bitcoin', 'produced X BTC', 'earned X BTC', 'X BTC produced', 'X BTC earned'\n"
        "- 'X self-mined bitcoin', 'produced X self-mined bitcoin during [month]'\n"
        "- Table rows like: 'BTC Produced | 713', 'Total BTC earned | 269', 'Bitcoin mined: 450'\n"
        "- 'X bitcoin were mined during the month', 'approximately X BTC produced'\n\n"
        "CRITICAL RULES:\n"
        "1. A single sentence may contain BOTH a monthly figure AND a year-to-date figure. "
        "   Example: 'Produced 1,242 BTC in September 2023 and 8,610 BTC Year-To-Date'\n"
        "   → Extract 1,242 (the monthly figure). IGNORE 8,610 (it is year-to-date).\n"
        "   The monthly value always appears BEFORE 'Year-To-Date' or 'YTD' in such sentences.\n"
        "2. Some tables (Marathon/MARA) show two side-by-side comparison periods like:\n"
        "   'Metric  [current]  [year-ago]  [%]  [current]  [prior-month]  [%]'\n"
        "   The FIRST number after the metric label is always the current period — use that.\n"
        "3. If you see 'Bitcoin Produced - U.S. Only', that is a PARTIAL figure. "
        "   Look instead for total BTC produced including all operations (e.g. in prose: "
        "   'produced 1,202 bitcoin in October. This total includes X bitcoin from our JV').\n"
        "4. REJECT: quarterly (Q1-Q4), annual, year-to-date, cumulative, or network-wide stats.\n"
        "5. ALWAYS reject values preceded by 'year-to-date', 'YTD', 'fiscal year', 'full year', "
        "   'Q1', 'Q2', 'Q3', 'Q4', 'first quarter', 'second quarter', 'third quarter', or "
        "   'fourth quarter'.\n"
        "6. Numbers may contain commas (1,242 = 1242).\n\n"
        "Return ONLY this JSON, no other text:\n"
        '{{"metric":"production_btc","value":<number or null>,"unit":"BTC",'
        '"confidence":<0.0-1.0>,"source_snippet":"<exact phrase you found, max 100 chars>"}}\n\n'
        "Document:\n{text}"
    ),
    "hodl_btc": (
        "You are a financial data extractor. Your task: find the company's TOTAL bitcoin BALANCE "
        "or HOLDINGS at the END of this reporting period (an absolute count, not a change).\n\n"
        "Common phrasings to look for:\n"
        "- 'held X bitcoin', 'holds X BTC', 'X BTC in treasury', 'treasury of X BTC'\n"
        "- 'BTC held in treasury increased to X', 'bringing Treasury to X BTC'\n"
        "- 'X bitcoin on our balance sheet', 'X BTC in self-custody', 'hodl X bitcoin'\n"
        "- 'total bitcoin holdings of X', 'bitcoin balance of X', 'approximately X bitcoin'\n"
        "- Table rows like: 'BTC Holdings | 13,286', 'Total BTC Holdings (in whole numbers) | 13,396'\n\n"
        "CRITICAL RULES:\n"
        "1. Some tables (Marathon/MARA) break holdings into multiple rows:\n"
        "   'Total BTC Holdings (in whole numbers)  13,396  ...'\n"
        "   'Unrestricted BTC Holdings              13,396  ...'\n"
        "   'Restricted BTC Holdings                     0  ...'\n"
        "   Extract the 'Total BTC Holdings' row value (13,396 in this example), NOT unrestricted.\n"
        "2. These tables use a dual-period format: "
        "[current] [year-ago] [%] [current] [prior-month] [%]. "
        "The FIRST number after the label is always the current period.\n"
        "3. Do NOT extract a delta or change ('added 86 BTC' → ignore 86, find the new total).\n"
        "4. Numbers may contain commas (13,286 = 13286).\n"
        "5. The phrase 'total bitcoin holdings to approximately X' means the NEW total is X. "
        "   Use X as the value (do not use any prior-period number mentioned nearby).\n"
        "6. REJECT a value described as 'holdings as of [date]' if that date is in a prior month "
        "   — extract only the current-period total.\n\n"
        "Return ONLY this JSON, no other text:\n"
        '{{"metric":"hodl_btc","value":<number or null>,"unit":"BTC",'
        '"confidence":<0.0-1.0>,"source_snippet":"<exact phrase you found, max 100 chars>"}}\n\n'
        "Document:\n{text}"
    ),
    "sold_btc": (
        "You are a financial data extractor. Your task: find the number of bitcoin SOLD or "
        "LIQUIDATED by this company during THIS reporting month.\n\n"
        "Common phrasings to look for:\n"
        "- 'sold X BTC', 'sold X bitcoin', 'liquidated X BTC'\n"
        "- 'sold X of the Y BTC earned', 'divested X bitcoin'\n"
        "- Table rows like: 'BTC Sold | 245', 'Bitcoin sold: 136'\n\n"
        "REJECT: purchases/buys, network sales, or values from prior months. "
        "If the company did not sell any bitcoin this month, set value to null.\n\n"
        "Return ONLY this JSON, no other text:\n"
        '{{"metric":"sold_btc","value":<number or null>,"unit":"BTC",'
        '"confidence":<0.0-1.0>,"source_snippet":"<exact phrase you found, max 100 chars>"}}\n\n'
        "Document:\n{text}"
    ),
    "hodl_btc_unrestricted": (
        "You are a financial data extractor. Your task: find the company's UNRESTRICTED bitcoin "
        "holdings at the END of this reporting period — bitcoin the company can freely sell or use, "
        "not pledged or encumbered.\n\n"
        "Common phrasings to look for:\n"
        "- 'Unrestricted BTC Holdings  13,726' (Marathon/MARA table row)\n"
        "- 'holds a total of X unrestricted BTC', 'X unrestricted bitcoin'\n"
        "- 'unrestricted bitcoin holdings of X', 'X BTC unrestricted'\n\n"
        "CRITICAL RULES:\n"
        "1. Extract ONLY the unrestricted count — do NOT use 'Total BTC Holdings' or 'Restricted'.\n"
        "2. Marathon tables use dual-period format: [current] [year-ago] [%] [current] [prior] [%]. "
        "The FIRST number after the label is the current period.\n"
        "3. If the document does not distinguish restricted vs unrestricted, set value to null.\n"
        "4. Numbers may contain commas (13,726 = 13726).\n\n"
        "Return ONLY this JSON, no other text:\n"
        '{{"metric":"hodl_btc_unrestricted","value":<number or null>,"unit":"BTC",'
        '"confidence":<0.0-1.0>,"source_snippet":"<exact phrase you found, max 100 chars>"}}\n\n'
        "Document:\n{text}"
    ),
    "hodl_btc_restricted": (
        "You are a financial data extractor. Your task: find the company's RESTRICTED or PLEDGED "
        "bitcoin holdings at the END of this reporting period — bitcoin that is encumbered, "
        "pledged as collateral, or otherwise restricted from free use.\n\n"
        "Common phrasings to look for:\n"
        "- 'Restricted BTC Holdings  0' or 'Restricted BTC Holdings  3,829' (Marathon/MARA table)\n"
        "- 'Pledged BTC Holdings  571' (Marathon/MARA table)\n"
        "- 'X BTC pledged as collateral', 'X BTC were pledged'\n"
        "- 'X bitcoin pledged', 'restricted bitcoin of X'\n\n"
        "CRITICAL RULES:\n"
        "1. A value of 0 is valid — it means no BTC is restricted this period.\n"
        "2. If the table shows BOTH 'Restricted BTC Holdings' and 'Pledged BTC Holdings', "
        "sum them together as the total encumbered/restricted figure.\n"
        "3. Marathon tables use dual-period format: [current] [year-ago] [%] [current] [prior] [%]. "
        "The FIRST number after the label is the current period.\n"
        "4. If the document does not mention restricted or pledged BTC at all, set value to null.\n"
        "5. Numbers may contain commas (3,829 = 3829).\n\n"
        "Return ONLY this JSON, no other text:\n"
        '{{"metric":"hodl_btc_restricted","value":<number or null>,"unit":"BTC",'
        '"confidence":<0.0-1.0>,"source_snippet":"<exact phrase you found, max 100 chars>"}}\n\n'
        "Document:\n{text}"
    ),
    "hashrate_eh": (
        "You are a financial data extractor. Your task: find the company's OPERATIONAL hash rate "
        "at the end of this reporting period, in EH/s.\n\n"
        "Common phrasings to look for:\n"
        "- 'operational hashrate of X EH/s', 'energized hashrate X EH/s', 'X EH/s deployed'\n"
        "- 'hash rate of X EH/s', 'X exahash', 'X PH/s operational' (divide PH/s by 1000)\n"
        "- Table rows like: 'Energized Hashrate (EH/s) | 57.4'\n\n"
        "CRITICAL DISAMBIGUATION RULES:\n"
        "1. REJECT network or industry hashrate figures (e.g., 'Bitcoin network hashrate: 850 EH/s', "
        "   'global hashrate reached 900 EH/s', 'network hashrate increased to 850 EH/s'). "
        "   These are industry metrics, not the company's deployed capacity.\n"
        "2. The company's hashrate is ALWAYS an order of magnitude smaller than the Bitcoin network "
        "   hashrate. If the reported value exceeds 100 EH/s, treat it with high skepticism — "
        "   verify the phrase describes THIS company's operations, not the Bitcoin network.\n"
        "3. REJECT contracted, planned, or total installed capacity that is not yet energized.\n"
        "4. Convert PH/s to EH/s by dividing by 1000. Extract the CURRENT column if side-by-side.\n\n"
        "Return ONLY this JSON, no other text:\n"
        '{{"metric":"hashrate_eh","value":<number in EH/s or null>,"unit":"EH/s",'
        '"confidence":<0.0-1.0>,"source_snippet":"<exact phrase you found, max 100 chars>"}}\n\n'
        "Document:\n{text}"
    ),
    "realization_rate": (
        "You are a financial data extractor. Extract the bitcoin realization rate or mining "
        "revenue per BTC as a ratio (0.0-1.0) from the document below. "
        "Return ONLY valid JSON with no other text:\n"
        '{{"metric":"realization_rate","value":<ratio 0.0-1.0 or null>,"unit":"ratio","confidence":<0.0-1.0>}}\n'
        "Rules: convert percentage to ratio (e.g. 95% → 0.95). If not found, set value to null.\n\n"
        "Document:\n{text}"
    ),
    "net_btc_balance_change": (
        "You are a financial data extractor. Extract the net change in bitcoin holdings "
        "this period (positive = accumulation, negative = reduction) from the document below. "
        "Return ONLY valid JSON with no other text:\n"
        '{{"metric":"net_btc_balance_change","value":<signed number or null>,"unit":"BTC","confidence":<0.0-1.0>}}\n'
        "Rules: include sign. If not found, set value to null.\n\n"
        "Document:\n{text}"
    ),
    "encumbered_btc": (
        "You are a financial data extractor. Extract the total bitcoin pledged as collateral "
        "or encumbered under loan facilities from the document below. "
        "Return ONLY valid JSON with no other text:\n"
        '{{"metric":"encumbered_btc","value":<number or null>,"unit":"BTC","confidence":<0.0-1.0>}}\n'
        "Rules: sum all collateral positions if multiple. If not found, set value to null.\n\n"
        "Document:\n{text}"
    ),
    "mining_mw": (
        "You are a financial data extractor. Extract the total operational power capacity "
        "used for bitcoin mining in megawatts (MW) from the document below. "
        "Return ONLY valid JSON with no other text:\n"
        '{{"metric":"mining_mw","value":<number in MW or null>,"unit":"MW","confidence":<0.0-1.0>}}\n'
        "Rules: operational MW only, not contracted or planned. If not found, set value to null.\n\n"
        "Document:\n{text}"
    ),
    "ai_hpc_mw": (
        "You are a financial data extractor. Extract the total operational power capacity "
        "dedicated to AI or HPC workloads in megawatts (MW) from the document below. "
        "Return ONLY valid JSON with no other text:\n"
        '{{"metric":"ai_hpc_mw","value":<number in MW or null>,"unit":"MW","confidence":<0.0-1.0>}}\n'
        "Rules: AI/HPC MW only, not bitcoin mining. If not found, set value to null.\n\n"
        "Document:\n{text}"
    ),
    "hpc_revenue_usd": (
        "You are a financial data extractor. Extract the revenue from AI/HPC hosting "
        "contracts in USD from the document below. "
        "Return ONLY valid JSON with no other text:\n"
        '{{"metric":"hpc_revenue_usd","value":<number in USD or null>,"unit":"USD","confidence":<0.0-1.0>}}\n'
        "Rules: convert millions to full dollars (e.g. $5.2M → 5200000). "
        "If not found, set value to null.\n\nDocument:\n{text}"
    ),
    "gpu_count": (
        "You are a financial data extractor. Extract the total number of GPU units deployed "
        "(H100s, A100s, or equivalent) from the document below. "
        "Return ONLY valid JSON with no other text:\n"
        '{{"metric":"gpu_count","value":<integer or null>,"unit":"units","confidence":<0.0-1.0>}}\n'
        "Rules: total deployed count only. If not found, set value to null.\n\n"
        "Document:\n{text}"
    ),
    "holdings_btc": (
        "You are a financial data extractor. Your task: find the company's TOTAL bitcoin BALANCE "
        "or HOLDINGS at the END of this reporting period (an absolute count, not a change).\n\n"
        "MARA / Marathon Digital table format (highest priority):\n"
        "- Row label: 'Total BTC Holdings (in whole numbers)' followed by the current-period value.\n"
        "  Example: Total BTC Holdings (in whole numbers)  |  13,396  |  ...\n"
        "  Extract 13,396. Do NOT use year-ago or prior-month values in the same row.\n"
        "- MARA dual-period table: [current] [year-ago] [%] [current] [prior-month] [%].\n"
        "  The FIRST number after the row label is the current period.\n"
        "- If separate 'Unrestricted BTC Holdings' and 'Restricted BTC Holdings' rows are present,\n"
        "  extract the 'Total BTC Holdings' row, not the unrestricted-only row.\n\n"
        "Prose phrasings to look for:\n"
        "- 'held approximately X bitcoin', 'holds X BTC', 'hold X BTC'\n"
        "- 'X BTC in treasury', 'treasury of X BTC', 'bringing Treasury to X BTC'\n"
        "- 'total bitcoin holdings to approximately X', 'total bitcoin holdings of X'\n"
        "- 'bitcoin holdings of X', 'bitcoin balance of X', 'X BTC on our balance sheet'\n"
        "- 'X bitcoin in self-custody', 'ended the month holding X bitcoin'\n\n"
        "CRITICAL RULES:\n"
        "1. Absolute total at period-end — not a delta. ('added 86 BTC' -> find the new total.)\n"
        "2. 'holdings to approximately X' -> X is the NEW total. Use X.\n"
        "3. REJECT 'holdings as of [date]' when that date is a prior month.\n"
        "4. REJECT values labeled YTD, Q1/Q2/Q3/Q4, or fiscal year.\n"
        "5. Numbers may contain commas (13,286 = 13286).\n"
        "6. If no holdings figure is present, return null.\n\n"
        "Return ONLY this JSON, no other text:\n"
        '{{"metric":"holdings_btc","value":<number or null>,"unit":"BTC",'
        '"confidence":<0.0-1.0>,"source_snippet":"<exact phrase you found, max 100 chars>"}}\n\n'
        "Document:\n{text}"
    ),
    "sales_btc": (
        "You are a financial data extractor. Your task: find the number of bitcoin SOLD or "
        "LIQUIDATED by this company during THIS reporting month only.\n\n"
        "MARA / Marathon Digital table format (highest priority):\n"
        "- Row label: 'BTC Sold' or 'Bitcoin Sold' followed by the current-period value.\n"
        "  Example: BTC Sold  |  245  |  ...   -> extract 245\n"
        "  The FIRST number is the current month; 0 or absent row -> return null.\n"
        "  Marathon has been 100% HODL since mid-2024; zero-sale months are normal.\n\n"
        "Prose phrasings to look for:\n"
        "- 'sold X BTC', 'sold X bitcoin', 'sold X of the Y BTC earned'\n"
        "- 'liquidated X BTC', 'liquidated X bitcoin'\n"
        "- 'divested X BTC', 'Bitcoin sold: X', 'proceeds from sale of X BTC'\n\n"
        "REJECT the following:\n"
        "- Purchases or buys of bitcoin.\n"
        "- Network fee outflows, staking losses.\n"
        "- Values described as quarterly, annual, year-to-date, or from a prior month.\n"
        "- Zero-sale disclosure ('did not sell any bitcoin') -> return null, not 0.\n\n"
        "Return ONLY this JSON, no other text:\n"
        '{{"metric":"sales_btc","value":<number or null>,"unit":"BTC",'
        '"confidence":<0.0-1.0>,"source_snippet":"<exact phrase you found, max 100 chars>"}}\n\n'
        "Document:\n{text}"
    ),
    "unrestricted_holdings": (
        "You are a financial data extractor. Your task: find the company's UNRESTRICTED bitcoin "
        "holdings at the END of this reporting period — bitcoin freely available, not pledged.\n\n"
        "MARA table: Row label 'Unrestricted BTC Holdings'; extract the FIRST (current-period) value.\n"
        "Prose: 'unrestricted bitcoin holdings of X', 'X BTC unrestricted', 'unencumbered BTC of X'.\n"
        "If the document does not distinguish restricted vs unrestricted, return null.\n"
        "REJECT beginning-of-period or prior-month values.\n\n"
        "Return ONLY this JSON, no other text:\n"
        '{{"metric":"unrestricted_holdings","value":<number or null>,"unit":"BTC",'
        '"confidence":<0.0-1.0>,"source_snippet":"<exact phrase you found, max 100 chars>"}}\n\n'
        "Document:\n{text}"
    ),
    "restricted_holdings_btc": (
        "You are a financial data extractor. Your task: find the company's RESTRICTED or PLEDGED "
        "bitcoin holdings at the END of this reporting period — bitcoin that is encumbered or "
        "pledged as collateral.\n\n"
        "MARA table: Row label 'Restricted BTC Holdings' or 'Pledged BTC Holdings'; extract the "
        "FIRST (current-period) value.\n"
        "Prose: 'restricted bitcoin holdings of X', 'X BTC pledged as collateral', 'pledged X BTC'.\n"
        "Return 0 (not null) ONLY if the document explicitly states zero restricted holdings.\n"
        "If not mentioned at all, return null.\n\n"
        "Return ONLY this JSON, no other text:\n"
        '{{"metric":"restricted_holdings_btc","value":<number or null>,"unit":"BTC",'
        '"confidence":<0.0-1.0>,"source_snippet":"<exact phrase you found, max 100 chars>"}}\n\n'
        "Document:\n{text}"
    ),
}

# Fallback for unknown metrics
_DEFAULT_FALLBACK_PROMPT = (
    "You are a financial data extractor. Extract the value for metric '{metric}' "
    "from the document below. Return ONLY valid JSON with no other text:\n"
    '{{"metric":"{metric}","value":<number or null>,"unit":"","confidence":<0.0-1.0>}}\n'
    "If not found, set value to null.\n\nDocument:\n{{text}}"
)


# Default preamble for batch extraction. Extracted as a named constant so it can
# be overridden via the config_settings DB key 'llm_batch_preamble'.
_QUARTERLY_BATCH_PREAMBLE = (
    "You are a financial data extractor working on a SEC quarterly report (10-Q) or annual "
    "report (10-K). Extract QUARTERLY TOTALS (for flow metrics) or END-OF-QUARTER VALUES "
    "(for snapshot metrics). Do NOT extract individual monthly figures or year-to-date cumulative "
    "figures unless specifically asked. The target period is stated in the document type.\n\n"
    "IMPORTANT: This is a multi-page filing. Mining operations data appears in the MD&A section "
    "(Management Discussion & Analysis) or in tables labeled 'Mining Operations', "
    "'Bitcoin Production', or similar.\n"
)

_ANNUAL_BATCH_PREAMBLE = (
    "You are a financial data extractor working on a SEC annual report (10-K). "
    "Extract FULL-YEAR TOTALS (flow metrics) or YEAR-END VALUES (snapshot metrics). "
    "Do NOT extract quarterly or monthly sub-period figures.\n\n"
    "Mining operations data appears in Item 1 (Business), Item 7 (MD&A), or exhibit tables.\n"
)

# Per-metric instructions for quarterly context (inverted from monthly prompts).
# These replace the 'REJECT: quarterly' language with 'REJECT: individual month' language.
_QUARTERLY_PROMPTS: dict = {
    "production_btc": (
        "Extract the TOTAL bitcoin MINED, PRODUCED, or EARNED for the QUARTER (or full year). "
        "Common phrasings: 'mined X bitcoin during Q1', 'produced X BTC in fiscal 2024', "
        "'total BTC produced: X', 'bitcoin production for the quarter was X'.\n"
        "REJECT: individual month figures, year-to-date cumulative figures that span multiple quarters."
    ),
    "hodl_btc": (
        "Extract the company's TOTAL bitcoin BALANCE or HOLDINGS at the END of the reported quarter "
        "or fiscal year. This is a point-in-time snapshot.\n"
        "REJECT: beginning-of-period balances, average balances."
    ),
    "sold_btc": (
        "Extract bitcoin SOLD or LIQUIDATED during the quarter or fiscal year (cumulative total).\n"
        "REJECT: individual month figures."
    ),
    "hodl_btc_unrestricted": (
        "Extract UNRESTRICTED bitcoin holdings at quarter-end or year-end.\n"
        "REJECT: beginning-of-period balances."
    ),
    "hodl_btc_restricted": (
        "Extract RESTRICTED or PLEDGED bitcoin holdings at quarter-end or year-end.\n"
        "REJECT: beginning-of-period balances."
    ),
    "hashrate_eh": (
        "Extract OPERATIONAL hash rate in EH/s at the END of the reported quarter or fiscal year.\n"
        "REJECT: planned or contracted capacity that is not yet operational."
    ),
    "realization_rate": (
        "Extract the bitcoin realization rate as a ratio (0.0–1.0) for the reported quarter "
        "or fiscal year. Convert percentages to ratios (e.g. 95% -> 0.95).\n"
        "If not found, set value to null."
    ),
    "net_btc_balance_change": (
        "Extract the net change in bitcoin holdings over the quarter or fiscal year "
        "(positive = accumulation, negative = reduction).\n"
        "REJECT: month-by-month breakdowns."
    ),
    "encumbered_btc": (
        "Extract the total bitcoin pledged as collateral or encumbered at quarter-end or year-end.\n"
        "Sum all collateral positions if multiple facilities are listed."
    ),
    "mining_mw": (
        "Extract total operational power capacity for bitcoin mining in MW at quarter-end or year-end.\n"
        "REJECT: contracted, planned, or not-yet-operational capacity."
    ),
    "ai_hpc_mw": (
        "Extract total operational AI/HPC power capacity in MW at quarter-end or year-end.\n"
        "REJECT: contracted or planned capacity."
    ),
    "hpc_revenue_usd": (
        "Extract AI/HPC hosting revenue in USD for the quarter or fiscal year. "
        "Convert millions to full dollars (e.g. $5.2M -> 5200000).\n"
        "REJECT: individual month figures."
    ),
    "gpu_count": (
        "Extract the total GPU units deployed (H100s, A100s, or equivalents) at quarter-end or year-end.\n"
        "REJECT: planned or ordered units not yet deployed."
    ),
    "holdings_btc": (
        "Extract the company's TOTAL bitcoin BALANCE or HOLDINGS at quarter/year-end "
        "(point-in-time). Use 'Total BTC Holdings (in whole numbers)' row for MARA. "
        "REJECT beginning-of-period balances or averages."
    ),
    "sales_btc": (
        "Extract bitcoin SOLD or LIQUIDATED during the quarter or fiscal year (cumulative "
        "total). Look for 'BTC Sold' rows or prose 'sold X bitcoin'. Return null if no "
        "sales occurred. REJECT individual month figures."
    ),
    "unrestricted_holdings": (
        "Extract UNRESTRICTED bitcoin holdings at quarter-end or year-end. REJECT "
        "beginning-of-period values."
    ),
    "restricted_holdings_btc": (
        "Extract RESTRICTED or PLEDGED bitcoin at quarter-end or year-end. Return null "
        "if the document makes no mention of restricted/pledged BTC."
    ),
}

_BATCH_UNIT_HINTS: dict = {
    "production_btc":          "BTC",
    "hodl_btc":                "BTC",
    "hodl_btc_unrestricted":   "BTC",
    "hodl_btc_restricted":     "BTC",
    "sold_btc":                "BTC",
    "hashrate_eh":             "EH/s",
    "realization_rate":        "ratio",
    "net_btc_balance_change":  "BTC",
    "encumbered_btc":          "BTC",
    "mining_mw":               "MW",
    "ai_hpc_mw":               "MW",
    "hpc_revenue_usd":         "USD",
    "gpu_count":               "units",
    "holdings_btc":            "BTC",
    "sales_btc":               "BTC",
    "unrestricted_holdings":   "BTC",
    "restricted_holdings_btc": "BTC",
}

_DEFAULT_BATCH_PREAMBLE = (
    "You are a financial data extractor. Extract ALL of the following metrics "
    "from the document in a single pass. Follow each metric's instructions carefully.\n\n"
    "IMPORTANT: This document should be a monthly bitcoin mining production report. "
    "If it appears to be general corporate news, a financing announcement, a strategic "
    "update, a legal notice, or any document that does not contain monthly operational "
    "mining statistics, return null for ALL metrics — do not guess or infer values.\n\n"
    "PERIOD SELECTION RULES (apply to every metric):\n"
    "1. Extract figures for THIS SPECIFIC REPORTING MONTH only. "
    "   A press release reporting September 2021 operations contains the monthly figure you want.\n"
    "2. Many reports include both a monthly figure AND a quarter-to-date or year-to-date total "
    "   in the same sentence or table. ALWAYS prefer the single-month figure. "
    "   Example: 'Produced 341 BTC in September and 1,252 BTC in Q3 2021' "
    "   — extract 341 (September), not 1,252 (Q3 total).\n"
    "3. Set period_granularity='monthly' when you extract a single-month figure, "
    "   'quarterly' when you can only find a multi-month total, "
    "   'annual' for a full-year total, or 'unknown' if you cannot determine the period.\n"
    "4. NEVER extract a quarterly, year-to-date, or annual aggregate as the monthly value "
    "   — if no single-month figure exists for a metric, return null for that metric.\n"
)


class LLMInterpreter:
    """
    Calls Ollama to extract a named metric from document text.

    Usage:
        extractor = LLMInterpreter(session=requests.Session(), db=miner_db)
        result = extractor.extract(text, 'production_btc')
        # Returns ExtractionResult or None
    """

    def __init__(self, session: requests.Session, db=None) -> None:
        self._session = session
        self._db = db  # Optional MinerDB for prompt lookup
        self._last_call_meta: dict = {}     # Populated by _call_ollama with timing fields
        self._last_batch_summary: str = ''  # Populated by _parse_batch_response
        self._last_transport_error: bool = False

    @staticmethod
    def get_default_prompt(metric: str) -> str:
        """Return the hardcoded default prompt for a metric (no DB lookup)."""
        if metric in _DEFAULT_PROMPTS:
            return _DEFAULT_PROMPTS[metric]
        return _DEFAULT_FALLBACK_PROMPT.replace('{metric}', metric)

    def check_connectivity(self) -> bool:
        """Return True if the configured LLM backend is reachable and ready."""
        try:
            if LLM_BACKEND == "llamacpp":
                resp = self._session.get(f"{LLM_BASE_URL}/health", timeout=5)
                return resp.status_code == 200
            # Ollama: check server version then verify the model is installed.
            # Without the model check, llm_available stays True even when every
            # /api/generate call gets a 404, routing all regex matches to
            # REGEX_ONLY / review_queue instead of being auto-accepted.
            resp = self._session.get(f"{LLM_BASE_URL}/api/version", timeout=5)
            if resp.status_code != 200:
                return False
            model_id = _active_model(self._db)
            model_resp = self._session.post(
                f"{LLM_BASE_URL}/api/show",
                json={"name": model_id},
                timeout=10,
            )
            if model_resp.status_code == 404:
                log.warning(
                    "Ollama model '%s' not found — LLM disabled (install with: ollama pull %s)",
                    model_id, model_id,
                )
                return False
            return model_resp.status_code == 200
        except Exception:
            return False

    @staticmethod
    def _build_temporal_anchor(expected_granularity: str, period: str = None) -> str:
        """Build a TEMPORAL SCOPE block for LLM prompts.

        Instructs the LLM to extract only figures whose time scope matches
        expected_granularity and to reject figures that belong to a broader or
        narrower period.
        """
        _other_map = {
            'monthly': 'quarterly or annual',
            'quarterly': 'annual',
            'annual': 'N/A',
        }
        other = _other_map.get(expected_granularity, 'other')
        period_line = period if period else 'see document'
        lines = [
            "=== TEMPORAL SCOPE (HARD CONSTRAINT) ===",
            f"Expected granularity: {expected_granularity}",
            f"Target period: {period_line}",
            f"Extract only {expected_granularity} figures. "
            f"If the document contains only a {other} figure for a metric, "
            f"return null for that metric. "
            f"Do NOT decompose {other} totals into estimated {expected_granularity} fractions.",
            "===",
            "",
        ]
        return "\n".join(lines)

    def extract(self, text: str, metric: str, config=None, period: str = None) -> Optional[ExtractionResult]:
        """
        Extract a metric value from document text using the LLM.

        Returns ExtractionResult or None on any failure.
        Never raises exceptions.

        config: Optional ExtractionRunConfig. When supplied, a temporal anchor
            block is prepended to the prompt.
        period: Optional period string forwarded to the temporal anchor.
        """
        try:
            prompt = self._get_prompt(metric).replace('{text}', text)
            if config is not None:
                anchor = self._build_temporal_anchor(config.expected_granularity, period)
                prompt = anchor + prompt
            raw_response = self._call_llm(prompt)
            if raw_response is None:
                return None
            return self._parse_response(raw_response, metric)
        except Exception as e:
            log.error("LLM extraction failed for metric %s: %s", metric, e, exc_info=True)
            return None

    # ------------------------------------------------------------------ #
    #  Batch extraction (1 Ollama call → all metrics)                     #
    # ------------------------------------------------------------------ #

    def extract_batch(
        self,
        text: str,
        metrics: list,
        ticker: str = None,
        expected_granularity: str = 'monthly',
        config=None,
        period: str = None,
    ) -> dict:
        """
        Extract all metrics in a single Ollama call.

        Pays the document prefill cost once instead of once per metric (~13x).
        Returns a dict of {metric: ExtractionResult} for metrics where a valid
        value was found. Returns {} on any failure so the caller can fall back
        to per-metric extract() calls.

        config: Optional ExtractionRunConfig. When supplied, config.expected_granularity
            overrides the expected_granularity param and a temporal anchor block is
            prepended to the prompt.
        expected_granularity: legacy param, used when config is None.
        period: Optional period string forwarded to the temporal anchor.
        """
        # config wins over legacy param
        _eg = config.expected_granularity if config is not None else expected_granularity
        try:
            prompt = self._build_batch_prompt(text, metrics, ticker=ticker, config=config, period=period)
            raw = self._call_llm(prompt)
            if raw is None:
                return {}
            return self._parse_batch_response(raw, metrics)
        except Exception as e:
            log.error("LLM batch extraction failed: %s", e, exc_info=True)
            return {}

    def extract_for_period(
        self,
        text: str,
        metrics: list,
        current_period: str,
        target_period: str,
    ) -> dict:
        """Ask the LLM if text explicitly mentions figures for the PRIOR month.

        Builds a targeted prompt instructing the LLM to extract values only for
        target_period (not current_period). Returns a dict of
        {metric: ExtractionResult} or {} on any failure.
        """
        try:
            prompt = self._build_gap_fill_prompt(text, metrics, current_period, target_period)
            raw = self._call_llm(prompt)
            if raw is None:
                return {}
            return self._parse_batch_response(raw, metrics)
        except Exception as e:
            log.error(
                "LLM gap-fill extraction failed for %s→%s: %s",
                current_period, target_period, e, exc_info=True,
            )
            return {}

    def extract_with_correction(
        self,
        text: str,
        metric: str,
        first_value,
        concern_context: str,
        ticker: str = None,
    ) -> Optional[ExtractionResult]:
        """Run a targeted self-correction pass with explicit concern context.

        Used when the agreement engine routes to REVIEW_QUEUE or OUTLIER_FLAGGED.
        Wraps the standard metric prompt with a preamble explaining the concern
        so the LLM can re-read the document with that specific issue in mind.

        Args:
            text:            Document text (already truncated by caller).
            metric:          Metric being corrected (e.g. 'hashrate_eh').
            first_value:     The value returned in the first extraction pass.
            concern_context: Human-readable explanation of the concern
                             (e.g. disagreement magnitude, outlier vs trailing avg).
            ticker:          Optional ticker for prompt hints.

        Returns:
            ExtractionResult or None on failure.
        """
        try:
            base_instructions = self._get_prompt_instructions(metric)
            unit = _BATCH_UNIT_HINTS.get(metric, '')
            preamble = (
                f"Your first extraction returned {metric} = {first_value} {unit}.\n"
                f"A cross-check raised a concern: {concern_context}\n\n"
                f"Re-read the document carefully with this specific concern in mind "
                f"and extract again.\n\n"
                f"{base_instructions}\n\n"
            )
            # Add ticker hint if available
            if ticker and self._db is not None:
                try:
                    hint_row = self._db.get_ticker_hint(ticker)
                    if hint_row:
                        preamble = f"=== COMPANY CONTEXT: {ticker} ===\n{hint_row}\n\n" + preamble
                except Exception:
                    pass

            output_fmt = (
                f"Return ONLY this JSON, no other text:\n"
                f'{{"metric":"{metric}","value":<number or null>,"unit":"{unit}",'
                f'"confidence":<0.0-1.0>,"source_snippet":"<exact phrase, max 100 chars>"}}\n\n'
            )
            prompt = preamble + output_fmt + "Document:\n" + text

            raw = self._call_llm(prompt)
            if raw is None:
                return None
            result = self._parse_response(raw, metric)
            if result is not None:
                result = result.__class__(
                    value=result.value,
                    unit=result.unit,
                    confidence=result.confidence,
                    extraction_method='llm_correction',
                    source_snippet=result.source_snippet,
                    metric=result.metric,
                    pattern_id=result.pattern_id,
                )
            return result
        except Exception as e:
            log.error(
                "Self-correction extraction failed for %s metric=%s: %s",
                ticker or 'unknown', metric, e, exc_info=True,
            )
            return None

    def _get_prompt_instructions(self, metric: str) -> str:
        """
        Return the task-description block of a metric's prompt — everything
        before the 'Return ONLY this JSON' output-format sentinel (or before
        'Document:' as fallback). DB overrides are checked first.

        Used by _build_batch_prompt to embed per-metric instructions without
        duplicating the output-format boilerplate for each metric.
        """
        full_prompt = self._get_prompt(metric)

        # Strip from the output-format sentinel onward
        for sentinel in ("Return ONLY this JSON", "Return ONLY valid JSON",
                         "Document:\n"):
            idx = full_prompt.find(sentinel)
            if idx != -1:
                return full_prompt[:idx].rstrip()

        # No sentinel found — return whole prompt minus trailing whitespace
        return full_prompt.rstrip()

    def _build_batch_prompt(self, text: str, metrics: list, ticker: str = None,
                            config=None, period: str = None) -> str:
        """
        Build a single prompt that asks the LLM to extract all metrics at once.

        Structure:
          [temporal anchor — when config is supplied]
          [preamble — from DB config_settings or _DEFAULT_BATCH_PREAMBLE]
          [=== COMPANY CONTEXT: {ticker} === if hint is set]
          === METRIC: <name> ===
          [instructions from _get_prompt_instructions]
          ...repeated for each metric...
          === OUTPUT FORMAT ===
          Return ONLY this JSON: { ... }
          Document:
          {text}

        Unit hints are defined in the module-level _BATCH_UNIT_HINTS dict.
        NOTE: when new metrics are added to _DEFAULT_PROMPTS, add their unit
        hint to _BATCH_UNIT_HINTS at module level.
        config: Optional ExtractionRunConfig. When supplied, prepend a TEMPORAL SCOPE block.
        period: Optional period string forwarded to the temporal anchor.
        """
        # Temporal anchor block (prepended before preamble when granularity is set)
        _temporal_prefix = ''
        if config is not None and config.expected_granularity is not None:
            _temporal_prefix = self._build_temporal_anchor(config.expected_granularity, period)

        # Preamble priority: custom_prompt_preamble > DB override > hardcoded constant
        preamble = _DEFAULT_BATCH_PREAMBLE
        if config is not None and config.custom_prompt_preamble:
            preamble = config.custom_prompt_preamble
        elif self._db is not None:
            try:
                db_preamble = self._db.get_config('llm_batch_preamble')
                if db_preamble:
                    preamble = db_preamble
            except Exception as e:
                log.warning("Could not fetch llm_batch_preamble from DB: %s", e)

        lines = [_temporal_prefix + preamble] if _temporal_prefix else [preamble]

        # Per-ticker context hint (injected after preamble if set)
        if ticker and self._db is not None:
            try:
                hint = self._db.get_ticker_hint(ticker)
                if hint:
                    lines.append(f"=== COMPANY CONTEXT: {ticker} ===")
                    lines.append(hint)
                    lines.append("===\n")
            except Exception as e:
                log.warning("Could not fetch ticker hint for %s: %s", ticker, e)

        # Per-metric anchor terms (from metric_keywords v31 SSOT, with exclude hints)
        if self._db is not None:
            try:
                from infra.keyword_service import get_all_active_rows as _get_kw_rows
                kw_rows = _get_kw_rows(self._db)
                if kw_rows:
                    lines.append("=== ANCHOR TERMS ===")
                    lines.append(
                        "Scan the document for these exact phrases and use them as anchor "
                        "points to locate numeric values. When you find a passage containing "
                        "one of these phrases, extract any numeric figures in the surrounding "
                        "sentences before moving on. "
                        "Do not skip a passage just because its phrasing is indirect."
                    )
                    for kw in kw_rows:
                        entry = f"- {kw['phrase']}"
                        excl = (kw.get('exclude_terms') or '').strip()
                        if excl:
                            entry += f" (ignore if surrounded by: {excl})"
                        lines.append(entry)
                    lines.append("===\n")
            except Exception as e:
                log.warning("Could not fetch metric keywords for prompt: %s", e)

        # Target metrics from metric_schema (SSOT — never hardcoded)
        if self._db is not None:
            try:
                metric_rows = self._db.get_metric_schema('BTC-miners', active_only=True)
                if metric_rows:
                    lines.append("=== TARGET METRICS ===")
                    for m in metric_rows:
                        lines.append(f"- {m['label']} ({m['key']}, unit: {m['unit']})")
                    lines.append("Extract a numeric value for each metric if mentioned.")
                    lines.append("===\n")
            except Exception as e:
                log.warning("Could not fetch metric schema for prompt: %s", e)

        for metric in metrics:
            lines.append(f"=== METRIC: {metric} ===")
            lines.append(self._get_prompt_instructions(metric))
            lines.append("")

        # Output format block
        lines.append("=== OUTPUT FORMAT ===")
        lines.append("Return ONLY this JSON object, no other text:")
        lines.append("The top-level JSON value MUST be an object keyed by metric name.")
        lines.append("Do NOT return an array, list, markdown code fence, commentary, or repeated per-metric objects.")
        lines.append("{")
        for metric in metrics:
            unit = _BATCH_UNIT_HINTS.get(metric, "")
            lines.append(
                f'  "{metric}": {{"value": <number or null>, "unit": "{unit}", '
                f'"confidence": <0.0-1.0>, "source_snippet": "<max 100 chars>", '
                f'"period_granularity": "monthly|quarterly|annual|unknown"}},'
            )
        lines.append('  "summary": "<one sentence: document type, company, period, and key figures found — max 150 chars>"')
        lines.append("}")
        lines.append("")
        lines.append("Document:")
        lines.append(text)

        return "\n".join(lines)

    def _build_multi_period_prompt(
        self,
        text: str,
        metrics: list,
        current_period: str,
        target_periods: list,
    ) -> str:
        """Build a prompt to extract historical monthly figures for multiple prior periods.

        Used when a press release contains a trailing table listing the last
        N months of production (e.g. Jan/Feb/Mar at the bottom of an April report).
        Each target_period must be a YYYY-MM-01 string.
        """
        period_list = ', '.join(target_periods)
        lines = [
            f"This document was published reporting figures for {current_period}. "
            f"Your task: extract values EXPLICITLY stated for these prior months: {period_list}. "
            f"Do NOT extract values for {current_period}. "
            f"Only return a value for a period if the document names that specific month explicitly "
            f"(by month name or YYYY-MM date). If a period is not mentioned, set all its values to null.\n",
        ]

        for metric in metrics:
            lines.append(f"=== METRIC: {metric} ===")
            lines.append(self._get_prompt_instructions(metric))
            lines.append("")

        lines.append("=== OUTPUT FORMAT ===")
        lines.append("Return ONLY this JSON object, no other text:")
        lines.append("{")
        for period in target_periods:
            lines.append(f'  "{period}": {{')
            for metric in metrics:
                unit = _BATCH_UNIT_HINTS.get(metric, "")
                lines.append(
                    f'    "{metric}": {{"value": <number or null>, "unit": "{unit}", '
                    f'"confidence": <0.0-1.0>, "source_snippet": "<max 100 chars>"}},'
                )
            lines.append("  },")
        lines.append("}")
        lines.append("")
        lines.append("Document:")
        lines.append(text)

        return "\n".join(lines)

    def _parse_multi_period_response(
        self,
        raw: str,
        metrics: list,
        target_periods: list,
    ) -> dict:
        """Parse a multi-period JSON response into {period: {metric: ExtractionResult}}."""
        start = raw.find('{')
        end = raw.rfind('}') + 1
        if start == -1 or end == 0:
            log.debug("No JSON object in multi-period LLM response")
            return {}

        try:
            data = json.loads(raw[start:end])
        except (json.JSONDecodeError, ValueError) as e:
            if _HAS_JSON_REPAIR:
                try:
                    data = json.loads(_json_repair.repair_json(raw[start:end]))
                    log.debug("Multi-period JSON repaired (original error: %s)", e)
                except Exception:
                    return {}
            else:
                return {}

        _model = _active_model(self._db)
        results = {}

        for period in target_periods:
            period_data = data.get(period)
            if not isinstance(period_data, dict):
                continue

            period_results = {}
            for metric in metrics:
                entry = period_data.get(metric)
                if not isinstance(entry, dict):
                    continue
                value = entry.get('value')
                if value is None:
                    continue
                try:
                    value = float(value)
                except (TypeError, ValueError):
                    continue
                bounds = METRIC_VALID_RANGES.get(metric)
                if bounds is not None:
                    lo, hi = bounds
                    if not (lo <= value <= hi):
                        log.debug(
                            "Multi-period LLM value %.4f out of range for %s %s",
                            value, period, metric,
                        )
                        continue
                unit = str(entry.get('unit', ''))
                confidence = float(entry.get('confidence', 0.5))
                confidence = max(0.0, min(1.0, confidence))
                source_snippet = str(entry.get('source_snippet') or raw[:200])
                period_results[metric] = ExtractionResult(
                    metric=metric,
                    value=value,
                    unit=unit,
                    confidence=confidence,
                    extraction_method=f"llm_{_model}",
                    source_snippet=source_snippet,
                    pattern_id=f"llm_{_model}",
                    period_granularity='monthly',
                )

            if period_results:
                results[period] = period_results

        return results

    def extract_historical_periods(
        self,
        text: str,
        metrics: list,
        current_period: str,
        target_periods: list,
    ) -> dict:
        """Extract monthly values for multiple historical periods in a single LLM call.

        Returns {period: {metric: ExtractionResult}} for periods where values were found.
        """
        try:
            prompt = self._build_multi_period_prompt(text, metrics, current_period, target_periods)
            raw = self._call_llm(prompt)
            if raw is None:
                return {}
            return self._parse_multi_period_response(raw, metrics, target_periods)
        except Exception as e:
            log.error(
                "Multi-period historical extraction failed current=%s targets=%s: %s",
                current_period, target_periods, e, exc_info=True,
            )
            return {}

    def _build_gap_fill_prompt(
        self,
        text: str,
        metrics: list,
        current_period: str,
        target_period: str,
    ) -> str:
        """Build a targeted prompt for prior-period gap fill.

        Instructs the LLM to only extract values explicitly attributed to
        target_period (the prior month), not to current_period.
        """
        lines = [
            f"This document was published reporting figures for {current_period}. "
            f"Your task: find values that are EXPLICITLY stated for the PRIOR month "
            f"({target_period}). Do NOT extract values for {current_period}. "
            f"Only return a value if the text names {target_period} (or the matching "
            f"month name) specifically.\n",
        ]

        for metric in metrics:
            lines.append(f"=== METRIC: {metric} ===")
            lines.append(self._get_prompt_instructions(metric))
            lines.append("")

        lines.append("=== OUTPUT FORMAT ===")
        lines.append("Return ONLY this JSON object, no other text:")
        lines.append("{")
        for metric in metrics:
            unit = _BATCH_UNIT_HINTS.get(metric, "")
            lines.append(
                f'  "{metric}": {{"value": <number or null>, "unit": "{unit}", '
                f'"confidence": <0.0-1.0>, "source_snippet": "<max 100 chars>"}},'
            )
        lines.append("}")
        lines.append("")
        lines.append("Document:")
        lines.append(text)

        return "\n".join(lines)

    def _parse_batch_response(
        self, raw: str, metrics: list
    ) -> dict:
        """
        Parse the LLM's batch JSON response.

        Iterates `metrics` (not data.keys()) to ignore LLM hallucinations.
        Applies the same null/float/range/clamp checks as _parse_response.
        Returns dict of {metric: ExtractionResult} for valid entries only.

        Granularity filtering is NOT performed here — it is the responsibility of
        the write-time validator (validate_period_granularity in interpret_pipeline.py)
        to reject results whose period_granularity does not match the expected
        granularity for the document. This allows the parser to remain neutral and
        the decision to be made at a single authoritative location.
        """
        start = raw.find('{')
        end = raw.rfind('}') + 1
        if start == -1 or end == 0:
            log.debug(
                "No JSON object found in LLM batch response (first 300 chars): %r",
                raw[:300],
            )
            return {}

        try:
            data = json.loads(raw[start:end])
        except (json.JSONDecodeError, ValueError) as e:
            if _HAS_JSON_REPAIR:
                try:
                    data = json.loads(_json_repair.repair_json(raw[start:end]))
                    log.debug("LLM batch JSON repaired (original error: %s)", e)
                except Exception:
                    log.debug("Could not parse LLM batch JSON: %s", e)
                    return {}
            else:
                log.debug("Could not parse LLM batch JSON: %s", e)
                return {}

        self._last_batch_summary = str(data.get('summary') or '').strip()[:200]

        results = {}
        for metric in metrics:
            entry = data.get(metric)
            if not isinstance(entry, dict):
                continue

            value = entry.get('value')
            if value is None:
                continue

            try:
                value = float(value)
            except (TypeError, ValueError):
                log.debug("Batch LLM value not numeric for %s: %r", metric, value)
                continue

            bounds = METRIC_VALID_RANGES.get(metric)
            if bounds is not None:
                lo, hi = bounds
                if not (lo <= value <= hi):
                    log.debug(
                        "Batch LLM value %.4f out of range [%.1f, %.1f] for %s",
                        value, lo, hi, metric,
                    )
                    continue

            unit = str(entry.get('unit', ''))
            confidence = float(entry.get('confidence', 0.5))
            confidence = max(0.0, min(1.0, confidence))
            source_snippet = str(entry.get('source_snippet') or raw[:200])
            period_granularity = str(entry.get('period_granularity') or 'unknown').lower().strip()

            _model = _active_model(self._db)
            results[metric] = ExtractionResult(
                metric=metric,
                value=value,
                unit=unit,
                confidence=confidence,
                extraction_method=f"llm_{_model}",
                source_snippet=source_snippet,
                pattern_id=f"llm_{_model}",
                period_granularity=period_granularity,
            )

        return results

    def _get_prompt(self, metric: str) -> str:
        """Fetch prompt from llm_prompts DB table, or fall back to hardcoded default."""
        if self._db is not None:
            try:
                with self._db._get_connection() as conn:
                    row = conn.execute(
                        "SELECT prompt_text FROM llm_prompts WHERE metric=? AND active=1 "
                        "ORDER BY id DESC LIMIT 1",
                        (metric,)
                    ).fetchone()
                    if row:
                        return row[0]
            except Exception as e:
                log.warning("Could not fetch LLM prompt from DB for %s: %s", metric, e)

        # Fall back to hardcoded defaults
        if metric in _DEFAULT_PROMPTS:
            return _DEFAULT_PROMPTS[metric]

        # Generic fallback for unknown metrics
        return _DEFAULT_FALLBACK_PROMPT.replace('{metric}', metric)

    def _extract_keep_alive(self) -> str:
        """Return the keep_alive value to send with every Ollama call.

        Configurable via 'ollama_keep_alive' in config_settings.
        Fallback: OLLAMA_KEEP_ALIVE constant (default "2h").
        """
        if self._db is not None:
            try:
                v = self._db.get_config('ollama_keep_alive')
                if v:
                    return v
            except Exception:
                pass
        from config import OLLAMA_KEEP_ALIVE
        return OLLAMA_KEEP_ALIVE

    def _extract_num_ctx(self) -> int:
        """Return the num_ctx to use for extraction Ollama calls.

        Extraction prompts are a single document + preamble, typically 3-4k tokens.
        8192 is sufficient and avoids over-allocating VRAM with the model's default
        (often 32768).  Configurable via 'extract_num_ctx' in config_settings.
        """
        default = 8192
        if self._db is not None:
            try:
                v = self._db.get_config('extract_num_ctx')
                if v:
                    return int(v)
            except Exception:
                pass
        return default

    def _call_llm(self, prompt: str) -> Optional[str]:
        """
        POST to the configured LLM backend. Returns the response text or None on failure.

        Supports two backends (controlled by LLM_BACKEND env var):
          "ollama"   — POST /api/generate  (default)
          "llamacpp" — POST /completion    (llama-server)
        """
        import re as _re
        self._last_transport_error = False

        if LLM_BACKEND == "llamacpp":
            url = f"{LLM_BASE_URL}/v1/chat/completions"
            payload = {
                "model": _active_model(self._db),
                "messages": [{"role": "user", "content": prompt}],
                "temperature": 0.0,
                "max_tokens": 768,
                "stream": False,
            }
            error_label = "llama.cpp /v1/chat/completions"
            def _extract_text(data: dict) -> str:
                choices = data.get("choices") or []
                return choices[0].get("message", {}).get("content", "") if choices else ""
            def _extract_meta(data: dict, text: str) -> dict:
                usage = data.get("usage") or {}
                return {
                    'prompt_tokens': usage.get('prompt_tokens', 0) or 0,
                    'response_tokens': usage.get('completion_tokens', 0) or 0,
                    'eval_duration_ms': 0,
                    'total_duration_ms': 0,
                    'response_chars': len(text),
                }
        else:
            url = f"{LLM_BASE_URL}/api/generate"
            payload = {
                "model": _active_model(self._db),
                "prompt": prompt,
                "stream": False,
                "keep_alive": self._extract_keep_alive(),
                "think": False,
                "options": {
                    "temperature": 0.0,
                    "num_predict": 768,
                    "num_ctx": self._extract_num_ctx(),
                },
            }
            error_label = "Ollama /api/generate"
            def _extract_text(data: dict) -> str:
                text = data.get("response", "")
                # Strip <think>...</think> blocks emitted by Qwen3 when the Ollama
                # version does not honour the "think": False API parameter.
                if not data.get("thinking", ""):
                    text = _re.sub(r'<think>.*?</think>', '', text, flags=_re.DOTALL).strip()
                return text
            def _extract_meta(data: dict, text: str) -> dict:
                return {
                    'prompt_tokens': data.get('prompt_eval_count', 0) or 0,
                    'response_tokens': data.get('eval_count', 0) or 0,
                    'eval_duration_ms': (data.get('eval_duration', 0) or 0) / 1e6,
                    'total_duration_ms': (data.get('total_duration', 0) or 0) / 1e6,
                    'response_chars': len(text),
                }

        try:
            resp = self._session.post(url, json=payload, timeout=LLM_TIMEOUT_SECONDS)
            if resp.status_code >= 400:
                log.warning("%s returned HTTP %d", error_label, resp.status_code)
                self._last_transport_error = True
                self._last_call_meta = {}
                return None
            data = resp.json()
            response_text = _extract_text(data)
            self._last_call_meta = _extract_meta(data, response_text)
            return response_text
        except requests.Timeout:
            log.warning("%s timed out after %ds", error_label, LLM_TIMEOUT_SECONDS)
            self._last_transport_error = True
            self._last_call_meta = {}
            return None
        except requests.RequestException as e:
            log.error("%s request failed: %s", error_label, e)
            self._last_transport_error = True
            self._last_call_meta = {}
            return None
        except (ValueError, KeyError) as e:
            log.error("Ollama response malformed: %s", e)
            self._last_call_meta = {}
            return None

    # ------------------------------------------------------------------ #
    #  Quarterly / annual batch extraction                               #
    # ------------------------------------------------------------------ #

    def extract_quarterly_batch(
        self,
        text: str,
        metrics: list,
        ticker: str = None,
        period_type: str = 'quarterly',  # 'quarterly' | 'annual'
    ) -> dict:
        """Like extract_batch() but uses quarterly/annual prompts and preamble.

        Returns dict of {metric: ExtractionResult} for metrics where a valid value
        was found. Returns {} on any failure so caller can handle gracefully.
        """
        try:
            prompt = self._build_quarterly_batch_prompt(
                text, metrics, ticker=ticker, period_type=period_type
            )
            raw = self._call_llm(prompt)
            if raw is None:
                return {}
            return self._parse_quarterly_batch_response(raw, metrics, period_type=period_type)
        except Exception as e:
            log.error("LLM quarterly batch extraction failed: %s", e, exc_info=True)
            return {}

    def _build_quarterly_batch_prompt(
        self,
        text: str,
        metrics: list,
        ticker: str = None,
        period_type: str = 'quarterly',
    ) -> str:
        """Build a prompt for quarterly or annual extraction.

        Uses _QUARTERLY_BATCH_PREAMBLE or _ANNUAL_BATCH_PREAMBLE and
        _QUARTERLY_PROMPTS instructions (which omit 'REJECT: quarterly' language).
        """
        preamble = _ANNUAL_BATCH_PREAMBLE if period_type == 'annual' else _QUARTERLY_BATCH_PREAMBLE
        if self._db is not None:
            db_key = 'llm_annual_batch_preamble' if period_type == 'annual' else 'llm_quarterly_batch_preamble'
            try:
                db_preamble = self._db.get_config(db_key)
                if db_preamble:
                    preamble = db_preamble
            except Exception as e:
                log.warning("Could not fetch %s from DB: %s", db_key, e)

        lines = [preamble]

        # Per-ticker context hint (injected after preamble if set)
        if ticker and self._db is not None:
            try:
                hint = self._db.get_ticker_hint(ticker)
                if hint:
                    lines.append(f"=== COMPANY CONTEXT: {ticker} ===")
                    lines.append(hint)
                    lines.append("===\n")
            except Exception as e:
                log.warning("Could not fetch ticker hint for %s: %s", ticker, e)

        # Per-metric anchor terms (from metric_keywords v31 SSOT, with exclude hints)
        if self._db is not None:
            try:
                from infra.keyword_service import get_all_active_rows as _get_kw_rows
                kw_rows = _get_kw_rows(self._db)
                if kw_rows:
                    lines.append("=== ANCHOR TERMS ===")
                    lines.append(
                        "Scan the document for these exact phrases and use them as anchor "
                        "points to locate numeric values. When you find a passage containing "
                        "one of these phrases, extract any numeric figures in the surrounding "
                        "sentences before moving on. "
                        "Do not skip a passage just because its phrasing is indirect."
                    )
                    for kw in kw_rows:
                        entry = f"- {kw['phrase']}"
                        excl = (kw.get('exclude_terms') or '').strip()
                        if excl:
                            entry += f" (ignore if surrounded by: {excl})"
                        lines.append(entry)
                    lines.append("===\n")
            except Exception as e:
                log.warning("Could not fetch metric keywords for prompt: %s", e)

        # Target metrics from metric_schema (SSOT — never hardcoded)
        if self._db is not None:
            try:
                metric_rows = self._db.get_metric_schema('BTC-miners', active_only=True)
                if metric_rows:
                    lines.append("=== TARGET METRICS ===")
                    for m in metric_rows:
                        lines.append(f"- {m['label']} ({m['key']}, unit: {m['unit']})")
                    lines.append("Extract a numeric value for each metric if mentioned.")
                    lines.append("===\n")
            except Exception as e:
                log.warning("Could not fetch metric schema for prompt: %s", e)

        for metric in metrics:
            lines.append(f"=== METRIC: {metric} ===")
            instructions = _QUARTERLY_PROMPTS.get(metric)
            if instructions is None:
                # Fall back to standard per-metric instructions but strip monthly rejections
                instructions = self._get_prompt_instructions(metric)
            lines.append(instructions)
            lines.append("")

        lines.append("=== OUTPUT FORMAT ===")
        lines.append("Return ONLY this JSON object, no other text:")
        lines.append("{")
        for metric in metrics:
            unit = _BATCH_UNIT_HINTS.get(metric, "")
            lines.append(
                f'  "{metric}": {{"value": <number or null>, "unit": "{unit}", '
                f'"confidence": <0.0-1.0>, "source_snippet": "<max 100 chars>"}},'
            )
        lines.append("}")
        lines.append("")
        lines.append("Document:")
        lines.append(text)

        return "\n".join(lines)

    def _parse_quarterly_batch_response(
        self, raw: str, metrics: list, period_type: str = 'quarterly'
    ) -> dict:
        """Parse LLM batch response for quarterly/annual extraction.

        Like _parse_batch_response but applies 3x range bounds for 'quarterly'
        period_type to accommodate quarterly/annual aggregated values.
        """
        start = raw.find('{')
        end = raw.rfind('}') + 1
        if start == -1 or end == 0:
            log.debug(
                "No JSON object found in LLM quarterly batch response (first 300 chars): %r",
                raw[:300],
            )
            return {}

        try:
            data = json.loads(raw[start:end])
        except (json.JSONDecodeError, ValueError) as e:
            if _HAS_JSON_REPAIR:
                try:
                    data = json.loads(_json_repair.repair_json(raw[start:end]))
                    log.debug("LLM quarterly batch JSON repaired (original error: %s)", e)
                except Exception:
                    log.debug("Could not parse LLM quarterly batch JSON: %s", e)
                    return {}
            else:
                log.debug("Could not parse LLM quarterly batch JSON: %s", e)
                return {}

        if not isinstance(data, dict):
            log.debug(
                "Ignoring LLM quarterly batch response with top-level %s instead of object",
                type(data).__name__,
            )
            return {}

        # Quarterly/annual data gets wider valid ranges for flow metrics
        valid_ranges = _QUARTERLY_VALID_RANGES if period_type in ('quarterly', 'annual') else METRIC_VALID_RANGES

        results = {}
        for metric in metrics:
            entry = data.get(metric)
            if not isinstance(entry, dict):
                continue

            value = entry.get('value')
            if value is None:
                continue

            try:
                value = float(value)
            except (TypeError, ValueError):
                log.debug("Quarterly batch LLM value not numeric for %s: %r", metric, value)
                continue

            bounds = valid_ranges.get(metric)
            if bounds is not None:
                lo, hi = bounds
                if not (lo <= value <= hi):
                    log.debug(
                        "Quarterly batch LLM value %.4f out of range [%.1f, %.1f] for %s",
                        value, lo, hi, metric,
                    )
                    continue

            unit = str(entry.get('unit', ''))
            confidence = float(entry.get('confidence', 0.5))
            confidence = max(0.0, min(1.0, confidence))
            source_snippet = str(entry.get('source_snippet') or raw[:200])

            _model = _active_model(self._db)
            results[metric] = ExtractionResult(
                metric=metric,
                value=value,
                unit=unit,
                confidence=confidence,
                extraction_method=f"llm_{_model}",
                source_snippet=source_snippet,
                pattern_id=f"llm_{_model}",
            )

        return results

    def _parse_response(self, raw: str, metric: str) -> Optional[ExtractionResult]:
        """
        Parse the LLM's response text as JSON.

        Expected format: {"metric": "...", "value": <float|null>, "unit": "...", "confidence": <float>}

        Returns ExtractionResult or None if:
          - JSON cannot be parsed
          - value is null / missing
          - value is outside the metric's valid range
        """
        try:
            # Find the JSON object in the response (LLM may include surrounding text)
            start = raw.find('{')
            end = raw.rfind('}') + 1
            if start == -1 or end == 0:
                log.debug("No JSON object found in LLM response for %s", metric)
                return None
            data = json.loads(raw[start:end])
        except (json.JSONDecodeError, ValueError) as e:
            log.debug("Could not parse LLM JSON for %s: %s", metric, e)
            return None

        value = data.get('value')
        if value is None:
            log.debug("LLM returned null value for %s", metric)
            return None

        try:
            value = float(value)
        except (TypeError, ValueError):
            log.debug("LLM value not numeric for %s: %r", metric, value)
            return None

        # Range check using the same bounds as confidence.py
        bounds = METRIC_VALID_RANGES.get(metric)
        if bounds is not None:
            lo, hi = bounds
            if not (lo <= value <= hi):
                log.debug(
                    "LLM value %.4f out of range [%.1f, %.1f] for %s",
                    value, lo, hi, metric
                )
                return None

        unit = str(data.get('unit', ''))
        confidence = float(data.get('confidence', 0.5))
        confidence = max(0.0, min(1.0, confidence))  # clamp to [0, 1]

        # source_snippet: use the model's self-reported context if available
        source_snippet = str(data.get('source_snippet', raw[:500]))

        _model = _active_model(self._db)
        return ExtractionResult(
            metric=metric,
            value=value,
            unit=unit,
            confidence=confidence,
            extraction_method=f"llm_{_model}",
            source_snippet=source_snippet,
            pattern_id=f"llm_{_model}",
        )
