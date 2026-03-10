# Agent H — IR Boundary Audit for CORZ, BITF, BTDR, WULF

Audit these four miners using the same discovery/classify/boundary method used for CLSK, RIOT, and MARA.

Scope:
- `CORZ`
- `BITF`
- `BTDR`
- `WULF`

Objectives:
- identify the real IR archive/listing entry points
- confirm which pages are stable crawl surfaces versus JS shells or stale domains
- classify mining-activity PR patterns versus earnings / financing / corporate noise
- determine the earliest defensible IR history boundary for mining activity
- note any gaps where SEC or wire-service fallback is still required

Required outputs per ticker:
1. Active archive/listing URLs
2. Historical slug or year-filter behavior
3. Earliest confirmed mining-activity IR month
4. Latest confirmed monthly/operational update month
5. Boundary risks:
   - publish-year vs report-year mismatch
   - renamed domains or migrated IR vendors
   - bankruptcy / recapitalization discontinuities
   - duplicate wire copies
6. Recommended scraper mode:
   - `discovery`
   - `drupal_year`
   - `rss`
   - `skip`
7. Whether SEC should remain primary or only fallback

Ticker-specific expectations:

### CORZ
- Check post-bankruptcy Core Scientific IR pagination on `investors.corescientific.com`
- Boundary should account for pre-bankruptcy / bankruptcy reporting discontinuities
- Determine whether 2022 monthly-style operating updates are available on the live IR site or only on wire archives

### BITF
- Validate the Drupal year-filter widget on `investor.bitfarms.com`
- Distinguish IR-hosted copies from GlobeNewswire / Canadian wire duplicates
- Confirm whether 2018 is a real mining-history boundary or just a listing-history boundary

### BTDR
- Validate the Bitdeer year-filter/news-release archive
- Separate proprietary mining updates from cloud-hash / hosting / ASIC commercialization pages
- Confirm 2023 start boundary and whether 2022 SPAC/listing-era IR pages should be excluded

### WULF
- Validate TeraWulf IR listing pagination directly rather than assuming RSS is sufficient
- Confirm monthly-history boundary from first full mining month in 2022 through the switch away from monthly reporting
- Identify the final monthly production update month

Rules:
- discovery-first: enumerate archive pages before guessing article URLs
- do not treat a live RSS feed as proof of full-history coverage
- treat stale skip notes as suspect until the live IR site is rechecked
- record exact boundary evidence with concrete dates and example URLs
