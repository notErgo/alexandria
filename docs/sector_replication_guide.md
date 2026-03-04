# Sector Replication Guide: Digital Asset Treasuries

**Status:** Complete
**Last Updated:** 2026-03-04
**Target Sector:** Public companies that accumulate BTC, ETH, or SOL as treasury assets

## Objective
Clone the miner ingestion and extraction pipeline for event-driven treasury disclosures where acquisition and disposition activity replaces mining production cadence.

## 1) Schema Differences From Miners

### Replace primary production metric
- Replace `production_btc` with `treasury_btc_acquired`.
- Keep `hodl_btc`, `sold_btc`, and `encumbered_btc` where disclosed.

### Add treasury-first metrics
- `treasury_btc_total`
- `treasury_btc_acquired`
- `treasury_btc_sold`
- `acquisition_price_usd`
- `nav_per_share`
- `shares_outstanding`
- `premium_to_nav`
- `cost_basis_per_btc`

### Keep compatibility fields
- `treasury_btc_unrestricted`
- `treasury_btc_restricted`
- `treasury_btc_encumbered`

## 2) Primary Data Sources For Treasury Companies
- SEC 8-K filings, often Item 8.01 for purchase disclosures.
- Press releases announcing each buy, sell, and cumulative holdings update.
- Quarterly 10-Q filings for fair value, carrying value, and share count context.
- Annual 10-K filings for full holdings and accounting treatment.

Representative issuers:
- Strategy (MSTR)
- Metaplanet (TYO:3350)
- Semler Scientific (SMLR)
- NEXON (Tokyo)
- Tesla (TSLA)
- Block (SQ)
- Exodus Movement (EXOD)

## 3) Scraping Approach Differences
- Miners: usually monthly cadence.
- Treasuries: event-driven cadence with bursts around financing windows.

Implementation differences:
- Build EDGAR-first polling for 8-K text search on `bitcoin`, `BTC`, `ether`, `ETH`, `solana`, `SOL`.
- Keep IR RSS ingestion, yet expect irregular intervals.
- Add Japan-specific branch for TDnet and Tokyo exchange filings.

## 4) Extraction Differences
- Capture average acquisition price when provided.
- If total holdings appear in multiple locations, select end-of-period value and store earlier value as note.
- Capture Strategy-specific `BTC yield` as custom KPI with explicit formula source.
- Capture impairment, unrealized gain, and unrealized loss separately from coin counts.

## 5) EDGAR Query Patterns
Use SEC EFTS full text query for 8-K:
- `https://efts.sec.gov/LATEST/search-index?q=%22bitcoin%22&dateRange=custom&startdt=2020-01-01&forms=8-K&entity=microstrategy`

Generalized pattern:
- `https://efts.sec.gov/LATEST/search-index?q=%22bitcoin%22+OR+%22BTC%22&dateRange=custom&startdt=YYYY-MM-DD&forms=8-K&entity=<ticker_or_name>`

Use 10-Q and 10-K filters for periodic accounting detail:
- Replace `forms=8-K` with `forms=10-Q` or `forms=10-K`.

## 6) Bootstrap Checklist (10 Steps)
1. Define target ticker universe and exchange mapping.
2. Add schema migration for treasury metrics and NAV-related fields.
3. Build EDGAR poller for 8-K, 10-Q, and 10-K with keyword filters.
4. Add IR RSS collectors for each issuer and normalize publication timestamps.
5. Add country-specific filing adapters (TDnet for Japan).
6. Implement extraction prompt tuned for acquisition/sale language and cumulative holdings.
7. Add anomaly rules for holdings continuity, price plausibility, and share-count consistency.
8. Build deduplication key: `ticker + filing_date + source_type + holdings_total`.
9. Backfill historical events from 2020 forward and run reconciliation against SEC totals.
10. Add dashboard views for holdings timeline, acquisition cadence, NAV premium, and financing linkage.

## Operational Notes
- Treasury issuers can publish a press release and file 8-K on different dates for the same event.
- Use filing acceptance time for legal chronology and press-release date for market communication chronology.
- Store both in the source record.
