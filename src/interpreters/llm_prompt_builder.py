"""
LLM prompt constants for the Bitcoin miner extraction pipeline.

This module holds all hardcoded prompt strings and preamble templates used
by LLMInterpreter. Separating them here keeps llm_interpreter.py focused on
HTTP transport and response parsing.

Prompt lookup priority:
  1. DB row in llm_prompts table (per-metric override, editable via UI)
  2. _DEFAULT_PROMPTS[metric] (hardcoded baseline below)
  3. _DEFAULT_FALLBACK_PROMPT (generic template for unknown metrics)
"""

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
        "   \u2192 Extract 1,242 (the monthly figure). IGNORE 8,610 (it is year-to-date).\n"
        "   The monthly value always appears BEFORE 'Year-To-Date' or 'YTD' in such sentences.\n"
        "2. Comparison tables appear as pipe-delimited rows (MARA, RIOT, and others):\n"
        "   'Bitcoin Produced | 463 | 533 | 375 | -13 % | 23 %'\n"
        "   Column headers may contain trailing footnote numbers (e.g. 'April 2025 1') — these\n"
        "   are footnote references, NOT separate data values. The FIRST numeric value in each\n"
        "   data row (immediately after the metric label) is always the current reporting period.\n"
        "   The second is the prior month. Ignore the rest (prior year, month/month %, year/year %).\n"
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
        "3. Do NOT extract a delta or change ('added 86 BTC' \u2192 ignore 86, find the new total).\n"
        "4. Numbers may contain commas (13,286 = 13286).\n"
        "5. The phrase 'total bitcoin holdings to approximately X' means the NEW total is X. "
        "   Use X as the value (do not use any prior-period number mentioned nearby).\n"
        "6. REJECT a value described as 'holdings as of [date]' if that date is in a prior month "
        "   \u2014 extract only the current-period total.\n\n"
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
        "holdings at the END of this reporting period \u2014 bitcoin the company can freely sell or use, "
        "not pledged or encumbered.\n\n"
        "Common phrasings to look for:\n"
        "- 'Unrestricted BTC Holdings  13,726' (Marathon/MARA table row)\n"
        "- 'holds a total of X unrestricted BTC', 'X unrestricted bitcoin'\n"
        "- 'unrestricted bitcoin holdings of X', 'X BTC unrestricted'\n\n"
        "CRITICAL RULES:\n"
        "1. Extract ONLY the unrestricted count \u2014 do NOT use 'Total BTC Holdings' or 'Restricted'.\n"
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
        "bitcoin holdings at the END of this reporting period \u2014 bitcoin that is encumbered, "
        "pledged as collateral, or otherwise restricted from free use.\n\n"
        "Common phrasings to look for:\n"
        "- 'Restricted BTC Holdings  0' or 'Restricted BTC Holdings  3,829' (Marathon/MARA table)\n"
        "- 'Pledged BTC Holdings  571' (Marathon/MARA table)\n"
        "- 'X BTC pledged as collateral', 'X BTC were pledged'\n"
        "- 'X bitcoin pledged', 'restricted bitcoin of X'\n\n"
        "CRITICAL RULES:\n"
        "1. A value of 0 is valid \u2014 it means no BTC is restricted this period.\n"
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
        "   hashrate. If the reported value exceeds 100 EH/s, treat it with high skepticism \u2014 "
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
        "Rules: convert percentage to ratio (e.g. 95% \u2192 0.95). If not found, set value to null.\n\n"
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
        "Rules: convert millions to full dollars (e.g. $5.2M \u2192 5200000). "
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
        "- Comparison tables appear as pipe-delimited rows (MARA, RIOT, and others):\n"
        "  'Total BTC Holdings | 47,531 | 44,893 | 40,435 | 6 % | 18 %'\n"
        "  Column headers may contain trailing footnote numbers (e.g. 'April 2025 1') — these\n"
        "  are footnote references, NOT separate data values. The FIRST numeric value in each\n"
        "  data row (immediately after the row label) is always the current reporting period.\n"
        "  The second is the prior month. Ignore the rest (prior year, month/month %, year/year %).\n"
        "- If separate 'Unrestricted BTC Holdings' and 'Restricted BTC Holdings' rows are present,\n"
        "  extract the 'Total BTC Holdings' row, not the unrestricted-only row.\n\n"
        "Prose phrasings to look for:\n"
        "- 'held approximately X bitcoin', 'holds X BTC', 'hold X BTC'\n"
        "- 'X BTC in treasury', 'treasury of X BTC', 'bringing Treasury to X BTC'\n"
        "- 'total bitcoin holdings to approximately X', 'total bitcoin holdings of X'\n"
        "- 'bitcoin holdings of X', 'bitcoin balance of X', 'X BTC on our balance sheet'\n"
        "- 'X bitcoin in self-custody', 'ended the month holding X bitcoin'\n\n"
        "CRITICAL RULES:\n"
        "1. Absolute total at period-end \u2014 not a delta. ('added 86 BTC' -> find the new total.)\n"
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
        "holdings at the END of this reporting period \u2014 bitcoin freely available, not pledged.\n\n"
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
        "bitcoin holdings at the END of this reporting period \u2014 bitcoin that is encumbered or "
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


# Preamble for quarterly (10-Q) batch extraction
_QUARTERLY_BATCH_PREAMBLE = (
    "You are a financial data extractor working on a SEC quarterly report (10-Q) or annual "
    "report (10-K). Extract QUARTERLY TOTALS (for flow metrics) or END-OF-QUARTER VALUES "
    "(for snapshot metrics). Do NOT extract individual monthly figures or year-to-date cumulative "
    "figures unless specifically asked. The target period is stated in the document type.\n\n"
    "IMPORTANT: This is a multi-page filing. Mining operations data appears in the MD&A section "
    "(Management Discussion & Analysis) or in tables labeled 'Mining Operations', "
    "'Bitcoin Production', or similar.\n"
)

# Preamble for annual (10-K) batch extraction
_ANNUAL_BATCH_PREAMBLE = (
    "You are a financial data extractor working on a SEC annual report (10-K). "
    "Extract FULL-YEAR TOTALS (flow metrics) or YEAR-END VALUES (snapshot metrics). "
    "Do NOT extract quarterly or monthly sub-period figures.\n\n"
    "Mining operations data appears in Item 1 (Business), Item 7 (MD&A), or exhibit tables.\n"
)

# Per-metric instructions for quarterly context (replaces 'REJECT: quarterly' with
# 'REJECT: individual month' language for each metric).
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
        "Extract the bitcoin realization rate as a ratio (0.0\u20131.0) for the reported quarter "
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

# Unit hint strings shown in the batch prompt for each metric
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
)
