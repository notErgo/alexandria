# Codex Research Progress — 2026-03-04

## Completed Groups
- Group 1 (ARBK, CIFR, HIVE, IREN, WULF, SDIG): DONE
- Group 2 (CORZ, HUT8): DONE
- Group 3 (ABTC, BTDR, APLD, GRIID, MIGI, GREE, sweep): DONE

## Key Findings So Far

- ARBK: Still publishing monthly operational updates via accessnewswire.com through at least December 2024. However, Argo hit massive turbulence in 2025: listing suspended from LSE in May 2025, Nasdaq delisting proceedings in July 2025, LSE delisted December 11 2025. In H1 2025 mined only 65 BTC (vs 442 in H1 2024). July 2025 update found: mined 129 BTC at 4.2 BTC/day. Still publishing ops updates but with irregular cadence. URL pattern: accessnewswire.com/newsroom/en/blockchain-and-cryptocurrency/argo-blockchain-plc-announces-{month}-operational-update-{numeric_id}
- CIFR: NOT acquired by Bitfarms — that was SDIG. Cipher Mining rebranded to Cipher Digital (investors.cipherdigital.com) but continues monthly operational updates at same URL pattern. Monthly updates confirmed Jan-Sep 2025. Last confirmed: September 2025 (Oct 7, 2025). GlobeNewswire RSS available. investors.ciphermining.com redirects to investors.cipherdigital.com.
- HIVE: News page has STATIC HTML links (not JS SPA). URL: /news/{slug}/. Production reports via newsfilecorp.com RSS: feeds.newsfilecorp.com/company/5335. Monthly updates confirmed through at least Nov 2025. Does NOT use GlobeNewswire for production reports (only old 2022-2023 PRs).
- IREN: GlobeNewswire RSS at globenewswire.com/rssfeed/organization/82e8_jAApdE1qYPVHkynKQ==. investors.iren.com still ECONNREFUSED. Last monthly update: August 2025 (Sep 8, 2025). Officially announced discontinuation of monthly updates in Oct 2025 press release. Now quarterly only.
- WULF: RSS feed live (investors.terawulf.com/news-events/press-releases/rss). Last monthly production: December 2024 (Jan 3, 2025). Switched to quarterly-only in 2025. No monthly production updates in 2025.
- SDIG: Acquired by Bitfarms, merger closed March 14, 2025. Last standalone production: monthly reports through mid-2024. Now fully folded into BITF monthly production reports.
- CORZ: Static HTML index at investors.corescientific.com with RSS. URL pattern: /news-events/press-releases/detail/{id}/{slug}. Monthly production reports confirmed Jan-Mar 2025. March 2025 (released Apr 7, 2025) appears to be last — no April 2025 found. CORZ pivoting to AI/HPC colocation.
- HUT8: Template https://hut8.com/news-insights/press-releases/hut-8-operations-update-for-{month}-{year} confirmed working for Jan-Mar 2025. March 2025 (Apr 4, 2025) was EXPLICITLY stated as last monthly update. HUT8 announced switch to quarterly. Spun out American Bitcoin (ABTC) Apr 1, 2025.
- ABTC: CIK 1755953, NASDAQ: ABTC. Merged with Gryphon Digital Mining Sep 3, 2025. Quarterly reporting only — no monthly production updates. Founded March 2025, mined 1,654 BTC from Q2-Q4 2025. IR at abtc.com/investors.
- BTDR: Bitdeer Technologies (CIK 1899123). Active monthly production reports at ir.bitdeer.com, URL pattern: /news-releases/news-release-details/{slug}. RSS at ir.bitdeer.com/rss/news-releases.xml. Nov 2025 update confirmed: mined 526 BTC.
- APLD: Applied Digital pivoted to AI/HPC. No longer publishing monthly bitcoin production reports.
- GRIID: Acquired by CleanSpark in Oct 2024. No longer independent. Production folded into CLSK.
- MIGI: Mawson Infrastructure, NASDAQ: MIGI. Monthly financial updates via GlobeNewswire/mawsoninc.com. Oct 2025 update confirmed. Very small BTC production ($0.1M mining revenue in Oct 2025). Mostly energy management/colocation now.
- GREE: Greenidge Generation, NASDAQ: GREE. Quarterly earnings only — no monthly production updates. ~110 BTC per quarter in 2025. Small scale.

## Blockers / Surprises
- CIFR was NOT acquired by Bitfarms. It rebranded as Cipher Digital and continues independently.
- HUT8 explicitly announced end of monthly updates in the March 2025 update itself.
- IREN explicitly announced end of monthly updates in Oct 2025.
- CORZ and WULF also stopped monthly updates (CORZ after March 2025, WULF after Dec 2024).
- HIVE does NOT use GlobeNewswire for production reports — uses Newsfile Corp.
- New companies with monthly reporting: BTDR (Bitdeer) is a strong candidate to add.
- GRIID is gone (acquired by CLSK).
- ABTC, GREE, MIGI, APLD all quarterly or no BTC-specific monthly reporting.
