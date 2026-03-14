"""
LLM prompt constants for the Bitcoin miner extraction pipeline.

This module holds all hardcoded prompt strings and preamble templates used
by LLMInterpreter. Separating them here keeps llm_interpreter.py focused on
HTTP transport and response parsing.

Prompt lookup priority (v46+):
  1. DB row in llm_prompts table (per-metric override, editable via UI)
  2. metric_schema.prompt_instructions (per-metric, seeded by _migrate_v46)
  3. _DEFAULT_PROMPTS[metric] (hardcoded baseline below)
  4. _DEFAULT_FALLBACK_PROMPT (generic template for unknown metrics)
"""

# Migration seed only. Runtime lookup reads metric_schema.prompt_instructions first
# (via LLMInterpreter._get_prompt_instructions). Do not delete until all deployments
# are on v46+ and all rows are seeded.
_DEFAULT_PROMPTS: dict = {
    "production_btc": (
        "Flow metric — total BTC MINED or PRODUCED this period, not held.\n"
        "In mixed monthly+YTD sentences, the monthly figure appears before 'Year-To-Date'.\n"
        "REJECT: U.S.-only partial figures (unless no total exists); Bitcoin network hashrate context."
    ),
    "hodl_btc": (
        "Legacy key — use holdings_btc. Snapshot — absolute BTC at period end, not a delta.\n"
        "REJECT: deltas, beginning-of-period values, values qualified as 'as of [prior date]'."
    ),
    "sold_btc": (
        "Legacy key — use sales_btc. Flow — BTC SOLD this period.\n"
        "Return null if no sales. REJECT: purchases, network fees, prior-period values."
    ),
    "hodl_btc_unrestricted": (
        "Legacy key — use unrestricted_holdings. Snapshot — UNRESTRICTED BTC at period-end.\n"
        "Return null if document does not distinguish restricted vs unrestricted."
    ),
    "hodl_btc_restricted": (
        "Legacy key — use restricted_holdings_btc. Snapshot — RESTRICTED/PLEDGED BTC at period-end.\n"
        "Return 0 if explicitly zero; return null if not mentioned at all."
    ),
    "hashrate_eh": (
        "Snapshot — company's OPERATIONAL EH/s at period end.\n"
        "REJECT: Bitcoin network hashrate (always 10-100x larger); planned/contracted capacity.\n"
        "Convert PH/s to EH/s by dividing by 1000."
    ),
    "realization_rate": (
        "Extract mining revenue per BTC as a ratio (0.0-1.0).\n"
        "Convert percentage to ratio (95% -> 0.95). Return null if not found."
    ),
    "net_btc_balance_change": (
        "Net change in BTC holdings this period (positive = accumulation, negative = reduction).\n"
        "Include sign. Return null if not found."
    ),
    "encumbered_btc": (
        "Total BTC pledged as collateral or encumbered under loan facilities.\n"
        "Sum all positions if multiple. Return null if not found."
    ),
    "mining_mw": (
        "Operational power capacity for BTC mining in MW. Operational only, not contracted.\n"
        "Return null if not found."
    ),
    "ai_hpc_mw": (
        "Operational power capacity for AI/HPC workloads in MW. Not BTC mining.\n"
        "Return null if not found."
    ),
    "hpc_revenue_usd": (
        "Revenue from AI/HPC hosting contracts in USD.\n"
        "Convert millions to full dollars (e.g. $5.2M -> 5200000). Return null if not found."
    ),
    "gpu_count": (
        "Total GPU units deployed (H100s, A100s, or equivalent). Deployed count only.\n"
        "Return null if not found."
    ),
    "holdings_btc": (
        "Snapshot — absolute BTC count at period END, not a delta or change.\n"
        "Use 'Total BTC Holdings' row; do NOT use 'Unrestricted BTC Holdings' row for this metric.\n"
        "REJECT: beginning-of-period balance; any value qualified as 'as of [prior date]'."
    ),
    "sales_btc": (
        "Flow — BTC SOLD this period. Return null if the company made no sales.\n"
        "REJECT: BTC purchases, mining pool fees, prior-period values."
    ),
    "unrestricted_holdings": (
        "Snapshot — UNRESTRICTED BTC at period-end (freely available, not pledged).\n"
        "Use 'Unrestricted BTC Holdings' row; do NOT use the 'Total BTC Holdings' row.\n"
        "Return null if the document does not distinguish restricted vs unrestricted."
    ),
    "restricted_holdings_btc": (
        "Snapshot — RESTRICTED or PLEDGED BTC at period-end.\n"
        "If both 'Restricted' and 'Pledged' rows appear, sum them.\n"
        "Return 0 if document explicitly states zero restricted; return null if not mentioned."
    ),
}

# Fallback for unknown metrics
_DEFAULT_FALLBACK_PROMPT = (
    "You are a financial data extractor. Extract the value for metric '{metric}' "
    "from the document below. Return ONLY valid JSON with no other text:\n"
    '{{"metric":"{metric}","value":<number or null>,"unit":"","confidence":<0.0-1.0>}}\n'
    "If not found, set value to null.\n\nDocument:\n{{text}}"
)


# Shared format rules injected into every preamble once — do NOT repeat per-metric.
_UNIVERSAL_FORMAT_RULES = (
    "\nUNIVERSAL FORMAT RULES (apply to all metrics):\n"
    "- Numbers may contain commas: 1,242 = 1242; 13,286 = 13286.\n"
    "- Parentheses denote reductions: (585) means 585 units were removed; return the absolute value.\n"
    "- Multi-column tables: the first numeric value after the row label is the current reporting\n"
    "  period. Subsequent columns are prior periods, comparisons, or percent changes — ignore them.\n"
)

# Shared note about the GAAP cryptocurrency activity rollforward table.
# Injected once into the quarterly and annual preambles — do NOT repeat per-metric.
_ROLLFORWARD_NOTE = (
    "CRYPTOCURRENCY ACTIVITY ROLLFORWARD TABLE (Notes to Financial Statements):\n"
    "SEC filings include a GAAP note table showing coin flows for the period. "
    "Column order is always: Description | BTC | [other crypto columns] | USD Amount.\n"
    "Use ONLY the BTC column (first numeric column after the label). "
    "NEVER use the USD/dollar column (last column — it is fair value, not coin count).\n"
    "Parentheses denote reductions: (585) = 585 BTC left. Return the absolute value.\n"
    "A dash (-) means zero for that coin in that row.\n"
    "Row-label mappings: "
    "'Revenue recognized from cryptocurrencies mined' -> production_btc; "
    "'Proceeds from sale of cryptocurrencies' or 'Sale of digital assets' -> sales_btc (BTC col, absolute); "
    "'Balance at [period end date]' or 'Ending balance' -> holdings_btc.\n"
    "If the table shows two year-blocks stacked, use ONLY the block whose end-date "
    "matches the filing's reporting period.\n"
)

# Preamble for quarterly (10-Q) batch extraction
_QUARTERLY_BATCH_PREAMBLE = (
    "You are a financial data extractor working on a SEC quarterly report (10-Q) or annual "
    "report (10-K). Extract QUARTERLY TOTALS (for flow metrics) or END-OF-QUARTER VALUES "
    "(for snapshot metrics). Do NOT extract individual monthly figures or year-to-date cumulative "
    "figures unless specifically asked. The target period is stated in the document type.\n\n"
    "IMPORTANT: This is a multi-page filing. Mining operations data appears in the MD&A section "
    "(Management Discussion & Analysis) or in tables labeled 'Mining Operations', "
    "'Bitcoin Production', or similar.\n\n"
    + _ROLLFORWARD_NOTE
    + _UNIVERSAL_FORMAT_RULES
)

# Preamble for annual (10-K) batch extraction
_ANNUAL_BATCH_PREAMBLE = (
    "You are a financial data extractor working on a SEC annual report (10-K). "
    "Extract FULL-YEAR TOTALS (flow metrics) or YEAR-END VALUES (snapshot metrics). "
    "Do NOT extract quarterly or monthly sub-period figures.\n\n"
    "Mining operations data appears in Item 1 (Business), Item 7 (MD&A), or exhibit tables.\n\n"
    + _ROLLFORWARD_NOTE
    + _UNIVERSAL_FORMAT_RULES
)

# Default preamble for monthly batch extraction.
# Can be overridden via the config_settings DB key 'llm_batch_preamble'.
_DEFAULT_BATCH_PREAMBLE = (
    "You are a financial data extractor. Extract ALL of the following metrics "
    "from the document in a single pass. Follow each metric's instructions carefully.\n\n"
    "IMPORTANT: This document should be a monthly bitcoin mining production report. "
    "If it appears to be general corporate news, a financing announcement, a strategic "
    "update, a legal notice, or any document that does not contain monthly operational "
    "mining statistics, return null for ALL metrics \u2014 do not guess or infer values.\n\n"
    "PERIOD SELECTION RULES (apply to every metric):\n"
    "1. Extract figures for THIS SPECIFIC REPORTING MONTH only. "
    "   A press release reporting September 2021 operations contains the monthly figure you want.\n"
    "2. Many reports include both a monthly figure AND a quarter-to-date or year-to-date total "
    "   in the same sentence or table. ALWAYS prefer the single-month figure. "
    "   Example: 'Produced 341 BTC in September and 1,252 BTC in Q3 2021' "
    "   \u2014 extract 341 (September), not 1,252 (Q3 total).\n"
    "3. Set period_granularity='monthly' when you extract a single-month figure, "
    "   'quarterly' when you can only find a multi-month total, "
    "   'annual' for a full-year total, or 'unknown' if you cannot determine the period.\n"
    "4. NEVER extract a quarterly, year-to-date, or annual aggregate as the monthly value "
    "   \u2014 if no single-month figure exists for a metric, return null for that metric.\n"
    + _UNIVERSAL_FORMAT_RULES
)
