# Codex Research Mission: Bitcoin Miner IR Registry Population

## Your Mission

You are a research agent. Your job is **pure web exploration and schema design** — you will not write Python implementation code. You will produce a structured research artifact that will be used by an engineer to update `config/companies.json` and extend the scraper architecture.

You must **not accept "site is down" or "502" as a final answer** without exhausting every alternative. For each blocked company, search for:
- Alternative IR domains (e.g. company domain rebrands, new investor-relations subdomains)
- GlobeNewswire / BusinessWire / PR Newswire mirror copies of press releases
- SEC EDGAR full-text search (https://efts.sec.gov/LATEST/search-index?q=%22production+update%22&dateRange=custom&startdt=2024-01-01&enddt=2025-12-31&entity=TICKER)
- RSS feeds on alternative domains
- Cached copies (Google Cache, Wayback Machine)
- The company's main corporate website press room (not always the IR subdomain)

---

## System Context (Read Carefully)

This is a Flask/SQLite Bitcoin miner intelligence platform. It ingests monthly production press releases from 13 public mining companies. Two-stage pipeline:

**Stage 1 — Ingest**: Fetch and store raw press release HTML text into `reports` table.
**Stage 2 — Extract**: LLM (Ollama Qwen3.5-35B) + regex pipeline extracts these metrics from each report:

| metric key | description | unit |
|---|---|---|
| `production_btc` | Bitcoin mined during the month | BTC |
| `hodl_btc` | Bitcoin held in treasury at month-end | BTC |
| `sold_btc` | Bitcoin sold/liquidated during the month | BTC |
| `hashrate_eh` | Deployed/operational hashrate | EH/s |
| `realization_rate` | Percentage of block reward realized | % |
| `net_btc_balance_change` | Signed BTC treasury delta | BTC |
| `encumbered_btc` | BTC posted as loan collateral | BTC |
| `mining_mw` | Mining power capacity operational | MW |
| `ai_hpc_mw` | AI/HPC power capacity operational | MW |
| `hpc_revenue_usd` | Revenue from AI/HPC hosting | USD |
| `gpu_count` | Total GPU units deployed | count |

**`companies.json` schema** — every company entry must conform to this JSON object shape:

```json
{
  "ticker": "XXXX",
  "name": "Full Company Name, Inc.",
  "tier": 1,
  "ir_url": "https://domain/news-events/press-releases",
  "pr_base_url": "https://domain",
  "rss_url": null,
  "cik": "0001234567",
  "active": true,
  "scrape_mode": "rss|template|index|skip|js_selenium",
  "url_template": null,
  "pr_start_year": 2021,
  "skip_reason": null,
  "sandbox_note": "Any access notes, URL quirks, or JS rendering requirements"
}
```

**Supported `scrape_mode` values** (existing):
- `"rss"` — company exposes an Equisolve-format RSS feed at `rss_url`; each item has title + link
- `"template"` — press release URLs follow a predictable pattern; `url_template` uses `{month}`, `{Month}`, `{year}` placeholders (e.g. `https://example.com/company-announces-{month}-{year}-production/`)
- `"index"` — fetch the IR listing page and extract `<a>` links matching production PR titles (static HTML only)
- `"skip"` — company is inactive, acquired, or permanently unreachable

**New `scrape_mode` values you may propose** (engineer will implement):
- `"js_selenium"` — page requires JavaScript rendering (propose this with the exact CSS selector or XPath that identifies production PR links on the rendered page)
- `"globenewswire"` — company consistently publishes via GlobeNewswire; provide the GlobeNewswire issuer slug/URL pattern
- `"businesswire"` — company uses Business Wire; provide issuer ID and filter pattern
- `"prnewswire"` — company uses PR Newswire; provide issuer slug

**URL template placeholders** (case-sensitive):
- `{month}` → lowercase (`march`)
- `{Month}` → title case (`March`)
- `{year}` → 4-digit (`2025`)
- Propose new placeholders if needed (e.g. `{MONTH}` for uppercase)

---

## Current Registry — What Needs Fixing

### Companies with Active Problems

#### 1. ARBK — Argo Blockchain plc
- **CIK**: 0001708187
- **Current status**: `scrape_mode: "skip"` — `ir.argo.partners/news` returned 502 at 2026-03
- **Problem**: The `ir.argo.partners` domain is dead/unreachable
- **Required research**:
  - Find the current working IR domain for Argo Blockchain plc. Try: `argoblockchain.com/investors`, `argo.partners`, `www.argoblockchain.com/news`
  - Check GlobeNewswire: search `site:globenewswire.com "Argo Blockchain" "production"`
  - Check BusinessWire: search `site:businesswire.com "Argo Blockchain" "monthly"`
  - Verify: Do they still publish monthly production reports as of 2025? (UK-listed company — LSE:ARB — may have moved to quarterly or stopped)
  - Check SEC EDGAR 8-K filings for ARBK CIK to see if production data appears there
  - If monthly reports stopped, document the last known month and propose `scrape_mode: "skip"` with accurate `skip_reason`
  - If monthly reports continue, provide the working URL and scrape_mode

#### 2. CIFR — Cipher Mining Inc. → Cipher Digital
- **CIK**: 0001838247
- **Current status**: `scrape_mode: "skip"` — acquired by Bitfarms Nov 2024; IR redirects to cipherdigital.com home
- **Required research**:
  - What happened to CIFR? Confirm acquisition details and effective date
  - Does Cipher Digital (post-acquisition entity) publish its own production figures, or are they now folded into Bitfarms (BITF) reports?
  - Search `site:cipherdigital.com "production"` and `site:cipherdigital.com "press"` for any extant press room
  - Check GlobeNewswire for "Cipher Digital" production updates (2025)
  - Check if Bitfarms now reports Cipher Digital's hashrate separately or merged
  - Was CIFR's historical data (2022-2024) ever archived anywhere accessible?
  - Result: either provide a working URL for ongoing Cipher Digital data, or confirm permanent skip with accurate reason

#### 3. HIVE — HIVE Digital Technologies Ltd.
- **CIK**: 0001537808
- **Current status**: `scrape_mode: "index"` but labeled "JS SPA — No Scraper" in extraction diagnostics
- **Problem**: `hivedigitaltechnologies.com/news/` is a JavaScript-rendered SPA; static HTML scraper sees no links
- **Required research**:
  - Does HIVE have an RSS feed? Try: `hivedigitaltechnologies.com/news/rss`, `hivedigitaltechnologies.com/feed`, `hivedigitaltechnologies.com/news-events/press-releases/rss`
  - Does HIVE use GlobeNewswire as primary distribution? Search `site:globenewswire.com/news-release "HIVE Digital" "production"` — if yes, what is the GlobeNewswire issuer URL/slug?
  - Does HIVE use Business Wire? Search `site:businesswire.com "HIVE Digital" "production update"`
  - Find the raw press release HTML URLs for at least 3 recent HIVE monthly production updates (2024). Do they follow a URL template?
  - Example known HIVE PR URLs to compare for template detection:
    - Look for patterns like `hivedigitaltechnologies.com/news/yyyy/mm/title-slug`
  - Inspect GlobeNewswire if used: what is the issuer ID/organization slug?
  - Propose the best scrape_mode with full configuration

#### 4. IREN — IREN Limited (formerly Iris Energy)
- **CIK**: 0001873044
- **Current status**: `scrape_mode: "skip"` — `investors.iren.com/news-releases` returned 502 at 2026-03
- **Required research**:
  - Is `investors.iren.com` now working? (502 may have been transient in March 2026)
  - Try alternative domains: `iren.com/news`, `irenergy.com.au/investors`, `ir.iren.com`
  - Does IREN distribute via GlobeNewswire? Search `site:globenewswire.com "IREN" "production update"` and `site:globenewswire.com "Iris Energy" "production"`
  - Does IREN distribute via Business Wire? Check `businesswire.com` search
  - Find at least 3 recent IREN monthly production PR URLs (2024-2025) and check for URL template pattern
  - IREN acquired Childress in 2024 — do they publish combined hashrate figures?
  - Historical note: before rebranding to IREN, the company was "Iris Energy" — check if old PR archives are accessible
  - Propose updated scrape configuration or confirm skip with accurate reason

#### 5. WULF — TeraWulf Inc.
- **CIK**: 0001855052
- **Current status**: `scrape_mode: "rss"` but `skip_reason: "Monthly production reports discontinued after December 2024"`
- **Required research**:
  - Confirm: did WULF truly stop publishing monthly production updates after December 2024?
  - Search `investors.terawulf.com/news-events/press-releases` — are there any 2025 production PRs?
  - Search GlobeNewswire: `site:globenewswire.com "TeraWulf" "production" 2025`
  - Do they now publish quarterly or semi-annual production figures only?
  - If quarterly only: are the quarterly figures in 8-K or earnings PRs? Can those be harvested via EDGAR?
  - What was the December 2024 production report URL? Verify the RSS feed is still live even if publishing stopped
  - Update `skip_reason` with confirmed facts and the exact last publication date

#### 6. SDIG — Stronghold Digital Mining → acquired by Bitfarms
- **CIK**: 0001830029
- **Current status**: `scrape_mode: "skip"` — `skip_reason: "Acquired by Bitfarms Q1 2025"`
- **Required research**:
  - Confirm acquisition close date (exact month)
  - What was the last standalone monthly production report from Stronghold?
  - Are Stronghold's former facilities now reported in Bitfarms' monthly updates?
  - Is there any historical SDIG production data on GlobeNewswire or SEC EDGAR 8-Ks that could fill gaps?
  - No change expected — but verify and sharpen the `skip_reason` with exact dates

---

### Companies with Partial Coverage Gaps

#### 7. CORZ — Core Scientific, Inc.
- **CIK**: 0001725526
- **Current status**: `scrape_mode: "index"`, 23 data points across 26 months (3 gaps), `sandbox_note` says "IR page requires JS"
- **Required research**:
  - Is `investors.corescientific.com/news-events/press-releases` fully JS-rendered, or does it serve static HTML?
  - Verify by fetching raw HTML of the index page — do production PR `<a>` links appear in the static HTML?
  - Does CORZ have an RSS feed? Try `/news-events/press-releases/rss`, `/rss`, `/feed`
  - Identify the 3 missing months in 2023-2025 coverage. What happened? Were they not published, or scraper miss?
  - Find the URL pattern for CORZ press releases — do they follow a predictable slug?
  - Example CORZ press release URL to inspect for template: `investors.corescientific.com/news-events/press-releases/detail/...`
  - If no template, propose `scrape_mode: "index"` with JS-handling notes, or GlobeNewswire fallback

#### 8. HUT8 — Hut 8 Corp.
- **CIK**: 0001928898
- **Current status**: `scrape_mode: "template"`, 8 data points across 16 months (coverage gap July 2024 onwards suspicious)
- **Required research**:
  - Verify the URL template still works for recent months (2025): `hut8.com/news-insights/press-releases/hut-8-operations-update-for-{month}-{year}`
  - Manually confirm 3-4 recent URLs (e.g. January 2025, February 2025, March 2025) return 200
  - Are there months where HUT8 changed their PR title format (e.g. "operations update" vs "production update" vs "monthly update")?
  - Check for alternative URL structures for the gap months (2024-07 through 2024-12)
  - Does HUT8 also publish via RSS or GlobeNewswire that could supplement the template?
  - Note: Hut 8 merged with USBTC in 2023 — does their PR format differ pre/post merger? Start year should be confirmed

---

## New Miner Candidates to Research

For each candidate below, determine if they publish monthly production reports AND meet the bar for inclusion (publicly traded, publishes `production_btc` and/or `hashrate_eh` monthly). Provide full `companies.json` entry if yes, or a one-line rejection reason if no.

### Tier 1 Candidates (high-volume miners, strong reporting)

#### A. Bitdeer Technologies (BTDR)
- NASDAQ-listed, Singapore HQ
- CIK: find from EDGAR or SEC EDGAR company search
- Likely URLs to try: `ir.bitdeer.com`, `investor.bitdeer.com`, `bitdeer.com/investor-relations`
- Do they publish monthly BTC production figures?
- GlobeNewswire search: `site:globenewswire.com "Bitdeer" "production update"`

#### B. Cipher Digital (post-CIFR)
- What is the current ticker/listing status after acquiring Cipher Mining?
- Does the combined entity publish monthly figures?
- Is it listed on a US exchange?

#### C. GRIID Infrastructure (GRDI)
- NASDAQ-listed
- CIK: search EDGAR
- Try: `ir.griid.com`, `griid.com/investor-relations`, `investors.griid.com`
- Monthly production reports?

#### D. Applied Digital Corporation (APLD)
- NASDAQ-listed (pivoting to AI/HPC but still mining)
- CIK: find from EDGAR
- Try: `ir.applieddigital.com`, `applieddigital.com/investors`
- Still publishing BTC production figures monthly?

#### E. Mawson Infrastructure Group (MIGI)
- NASDAQ-listed (US + Australian operations)
- CIK: find from EDGAR
- Try: `ir.mawsoninc.com`, `mawsoninc.com/investors`
- Monthly production?

#### F. Greenidge Generation (GREE)
- NASDAQ-listed
- CIK: find from EDGAR
- Try: `ir.greenidgegen.com`, GlobeNewswire
- Monthly production reports?

### Tier 2 Candidates (smaller, check if monthly)

#### G. US Bitcoin Corp (USBTC) — merged into Riot
- If merged, confirm it's fully folded into RIOT reports
- If any standalone historical data, document `pr_start_year` and last period

#### H. Northern Data (NB2)
- German exchange (Xetra), not NASDAQ/NYSE
- Monthly production figures in English?
- SEC EDGAR presence?
- Only include if: US-listed OR publishes English-language monthly BTC figures accessible via web

#### I. Phoenix Group (PHX)
- Abu Dhabi Securities Exchange listed
- Monthly English-language production reports?
- Any US regulatory filings?
- Include only if accessible via web scraping

#### J. Rhodium Enterprises
- Was it publicly listed? Current status?

#### K. Any other publicly-traded pure-play Bitcoin miner missing from the list
- Search: `site:globenewswire.com "bitcoin production" "monthly" 2025` — list any company names not in the existing 13
- Search: SEC EDGAR full-text for "bitcoin production" in 8-K filings by companies not in the 13 — identify new filers

---

## Research Deliverable Format

Your output must be a single JSON object with the following structure. Every field must be populated — no "TBD" or empty strings without explicit justification.

```json
{
  "research_date": "YYYY-MM-DD",
  "researcher_notes": "Overall findings summary, 3-5 sentences",

  "updated_companies": [
    {
      "ticker": "ARBK",
      "action": "update|skip_confirmed|new",
      "confidence": "high|medium|low",
      "finding": "One paragraph describing what you found",
      "verified_urls": ["list", "of", "URLs", "you", "confirmed", "200"],
      "failed_urls": ["list", "of", "URLs", "that", "returned", "non-200"],
      "proposed_entry": {
        "ticker": "ARBK",
        "name": "Argo Blockchain plc",
        "tier": 2,
        "ir_url": "...",
        "pr_base_url": "...",
        "rss_url": null,
        "cik": "0001708187",
        "active": true,
        "scrape_mode": "...",
        "url_template": null,
        "pr_start_year": 2021,
        "skip_reason": null,
        "sandbox_note": "..."
      },
      "template_examples": [
        "Concrete URL for Jan 2025 (if template mode)",
        "Concrete URL for Feb 2025",
        "Concrete URL for Mar 2025"
      ],
      "pr_title_patterns": [
        "Exact title string of a recent production PR",
        "Another example"
      ]
    }
  ],

  "new_companies": [
    {
      "ticker": "BTDR",
      "action": "add|reject",
      "confidence": "high|medium|low",
      "finding": "What you found",
      "verified_urls": [],
      "proposed_entry": { }
    }
  ],

  "js_spa_analysis": {
    "companies_requiring_js": ["HIVE", "CORZ"],
    "recommended_approach": "For each JS SPA, provide: (1) the GlobeNewswire/BusinessWire issuer URL if available as a static fallback, (2) the CSS selector that identifies production PR anchor tags on the JS-rendered page, (3) whether Selenium/Playwright headless Chrome would work"
  },

  "globenewswire_issuers": {
    "HIVE": "https://www.globenewswire.com/RssFeed/subjectcode/...",
    "IREN": null
  },

  "coverage_gap_analysis": {
    "CORZ": {
      "missing_periods": ["2023-MM", "2024-MM"],
      "gap_explanation": "Why these months are missing",
      "recovery_strategy": "How to backfill if possible"
    }
  }
}
```

---

## Research Methodology — Mandatory Steps Per Company

For every company (existing 13 + new candidates), execute these steps **in order**. Do not skip a step because an earlier one appeared to succeed.

### Step 1: Direct URL Probing
Try every plausible URL variant for the IR domain:
- `ir.{company}.com/news`, `/press-releases`, `/news-events/press-releases`
- `investors.{company}.com/news-releases`
- `{company}.com/investors/press-releases`
- `investor.{company}.com/news-releases`

### Step 2: RSS Discovery
For every working IR domain, check:
- `{ir_url}/rss`
- `{ir_url}/news-events/press-releases/rss`
- `{company}.com/feed`
- Look for `<link rel="alternate" type="application/rss+xml">` in page HTML

### Step 3: GlobeNewswire Search
Search: `site:globenewswire.com "{Company Name}" "production" "{year}"` for 2024 and 2025.
- If found: extract the GlobeNewswire issuer page URL (e.g. `https://www.globenewswire.com/RssFeed/subjectcode/...` or `https://www.globenewswire.com/NewsRoom/SubjectCode/...`)
- GlobeNewswire RSS feeds are available at: `https://www.globenewswire.com/RssFeed/subjectcode/{issuer_id}`

### Step 4: Business Wire / PR Newswire Search
- `site:businesswire.com "{Company Name}" "production update" 2025`
- `site:prnewswire.com "{Company Name}" "production" 2025`

### Step 5: SEC EDGAR 8-K Search
Search for production-related 8-K filings:
```
https://efts.sec.gov/LATEST/search-index?q=%22production+update%22&dateRange=custom&startdt=2024-01-01&enddt=2025-12-31&entity={COMPANY_NAME_OR_CIK}
```
Note: EDGAR requires `User-Agent` header. Use: `Hermeneutic Research Platform/1.0`

### Step 6: Template Detection
If you find 3+ press release URLs for a company, compare slugs:
- Strip the date/period component
- Identify the invariant prefix and suffix
- Identify what varies (month name lowercase/titlecase, year)
- Write the template using `{month}`, `{Month}`, `{year}` placeholders
- Test the template against 2-3 historical months to verify format consistency

### Step 7: JS Rendering Assessment
If the IR index page returns HTML with no production PR links:
1. Note the page's JavaScript framework (React, Angular, etc.) if identifiable from HTML source
2. Check if GlobeNewswire/BusinessWire is used as primary distribution (Step 3-4) — that eliminates the JS problem entirely
3. If JS is unavoidable, identify: the CSS selector or XPath for production PR anchor tags on the rendered page, and whether the site's `robots.txt` permits scraping

---

## Quality Bar

Your research is complete when, for every company in the registry:

1. **The current `scrape_mode` is either confirmed accurate OR a better mode is proposed with evidence**
2. **Every `scrape_mode: "skip"` entry has a `skip_reason` that is verifiably current** (not just copied from 2026-03 audit)
3. **Every `scrape_mode: "template"` entry has at least 3 verified live URLs** demonstrating the template works
4. **Every `scrape_mode: "rss"` entry has a confirmed live RSS feed URL**
5. **Every JS SPA company has either a static fallback (GlobeNewswire RSS) or a concrete CSS selector**
6. **Every new candidate company has either a full `proposed_entry` or a one-line rejection reason with evidence**

Do not summarize failure as "could not access." Document every URL you tried, its HTTP status, and the redirect chain if applicable.

---

## Architecture Constraints for Your Proposals

Any proposal you make must be compatible with these constraints:

1. **Raw text storage only in Stage 1**: The scraper fetches HTML and stores `soup.get_text(separator=" ", strip=True)[:50000]`. No structured parsing during ingest.
2. **Period inference from PR title**: Each PR must have a title containing a month name + 4-digit year (e.g. "January 2025"). If a company's PR titles don't contain month+year, document this and propose a fallback (e.g. infer from URL slug, publication date).
3. **`ir_press_release` source type**: All live-scraped reports are stored with `source_type = "ir_press_release"`. This is the key that routes them through the extraction pipeline.
4. **No authentication**: Scraper has no login capability. All proposed URLs must be publicly accessible without authentication.
5. **robots.txt compliance**: If a site's `robots.txt` disallows scraping, note this — the engineer will decide. Do not simply exclude based on robots.txt.
6. **Existing patterns**: The regex/LLM pipeline already handles production_btc, hodl_btc, sold_btc, hashrate_eh, and realization_rate. If a new company uses significantly different terminology (e.g. "self-mining" instead of "production"), note it — new patterns may be needed.

---

## Final Output

Produce the JSON deliverable described above, followed by a separate "Implementation Notes" section in Markdown covering:

1. **Scraper Priority Order**: Which companies should be unblocked first (highest data value)
2. **JS SPA Strategy**: Concrete recommendation — GlobeNewswire RSS fallback vs. Selenium, with pros/cons
3. **New Company Priority**: Which new candidates are worth adding immediately vs. deferred
4. **Pattern Gaps**: Any new companies that would require new regex patterns (different BTC terminology)
5. **URL Template Variants Needed**: Any new placeholders beyond `{month}`, `{Month}`, `{year}` needed for specific companies

---

*This prompt was prepared for the Bitcoin Miner Data Platform at OffChain/miners. Architecture reference: CLAUDE.md in that directory. The engineer receiving your output will implement changes and run the full test suite (532 tests) before deployment.*
