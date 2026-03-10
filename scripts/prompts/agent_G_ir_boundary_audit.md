# IR Boundary Audit Prompt

You are auditing investor-relations press release coverage for Bitcoin mining
history. Your task is to verify the true historical boundaries and URL/discovery
patterns for the target ticker, then recommend or implement scraper rules that
cover the full mining-activity history without drifting into irrelevant
corporate/news content.

## Scope
- Tickers: `CLSK`, `RIOT`, `MARA`
- Source family: IR press releases only
- Goal: full history of mining-activity press releases, not generic earnings,
  financing, governance, or unrelated corporate news

## Required Output
For each ticker, produce:
1. Earliest month/year where mining-activity IR coverage exists
2. Latest month/year where monthly mining IR updates exist
3. Whether the IR site is still active for this use case
4. Every distinct URL/discovery pattern used across history
5. Any year/path mismatches between report period and publish path
6. Any irregular historical slugs that cannot be reconstructed from one generic template
7. Recommendation:
   - `rss`
   - `index`
   - `template`
   - `hybrid`
   - `skip`
8. Explicit gaps where EDGAR must remain the fallback source

## Rules
- Bias toward primary-source URLs on the company's own IR domain
- Use exact URLs as evidence for historical pattern changes
- Distinguish:
  - report period year
  - publish year/path year
  - slug family
- Do not assume one template is sufficient unless at least 3 historical samples
  across different years confirm it
- If a ticker uses multiple eras, model it as era-specific rules
- If the listing page is hostile to crawling, prefer deterministic URL families
  or paginated archive pages rather than broad browser automation

## Ticker-Specific Expectations

### CLSK
- Validate whether 2023 monthly mining updates exist on IR
- Check if 2023 uses unstable suffixes or date-stamped tails
- Check if December reports are published under the following year path
- Check whether 2026 switched from `Bitcoin-Mining-Update` to `Operational-Update`

### RIOT
- Validate 2018 mining-yield release pattern
- Validate 2020 `production-update` pattern
- Validate 2021-2022 no-year slug family
- Validate 2023+ current-year slug family
- Confirm end of monthly IR cadence

### MARA
- Validate whether current `index` mode remains sufficient
- Check if RSS is usable for incremental coverage
- Identify earliest IR mining-history month available on the IR domain
- Confirm whether historical archive requires index pagination rather than template rules

## Acceptance Bar
- A ticker is only considered "covered" when the scraper design can explain the
  first known mining IR release, every major slug-era change, and the current
  live monthly/update endpoint.
