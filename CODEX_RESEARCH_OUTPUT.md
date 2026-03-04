# Codex Research Output — Bitcoin Miner IR Scraping Research
**Research Date:** 2026-03-04
**Researcher:** Claude Sonnet 4.6 (Codex agent)

---

## Researcher Notes

This research covered all 6 blocked/skip companies, 2 partial-coverage companies, and 6 new company candidates, plus a broad sweep for other monthly-reporting miners. The most significant finding is that four companies (WULF, HUT8, IREN, CORZ) explicitly discontinued or effectively stopped monthly production reporting in 2025 and should be marked as `skip` for scrape_mode. CIFR was confirmed NOT acquired by Bitfarms — it rebranded to Cipher Digital and continues publishing monthly operational updates. HIVE uses Newsfile Corp (not GlobeNewswire) for production reports, with fully static HTML links on its own site. For new companies, Bitdeer (BTDR) is the primary addition candidate with active monthly reporting. GRIID was acquired by CleanSpark in October 2024.

---

```json
{
  "research_date": "2026-03-04",
  "researcher_notes": "Four existing tracked companies (WULF, HUT8, IREN, CORZ) discontinued monthly production reporting in 2025 and should be updated to skip. CIFR was never acquired by Bitfarms — it rebranded to Cipher Digital and continues monthly updates under the same ticker/CIFR name. HIVE has static HTML news links but distributes production reports via Newsfile Corp, not GlobeNewswire. SDIG merger with Bitfarms closed March 14, 2025. BTDR (Bitdeer) is the strongest new add candidate with consistent monthly reporting through Nov 2025.",
  "updated_companies": [
    {
      "ticker": "ARBK",
      "action": "update",
      "confidence": "medium",
      "finding": "Argo Blockchain is in severe financial distress in 2025: LSE suspension requested May 1 2025, Nasdaq delisting proceedings began July 2025, LSE delisted December 11 2025. Despite this, they continued publishing monthly operational updates via accessnewswire.com throughout H1 2025. July 2025 confirmed: 129 BTC mined at 4.2 BTC/day. The company pivoted some operations to AI/HPC. URL pattern on accessnewswire uses numeric IDs with slugs, making template mode not viable. scrape_mode should be 'index' on their accessnewswire page. The argoblockchain.com/investors page is stale (shows 2022 data), so the authoritative source is accessnewswire.com or stocktitan.net/news/ARBK/ as an aggregator.",
      "verified_urls": [
        "https://www.accessnewswire.com/newsroom/en/blockchain-and-cryptocurrency/argo-blockchain-plc-announces-october-operational-update-939078",
        "https://www.accessnewswire.com/newsroom/en/blockchain-and-cryptocurrency/argo-blockchain-plc-november-ops-update-949995",
        "https://www.accessnewswire.com/964700/argo-blockchain-plc-announces-december-operational-update",
        "https://www.stocktitan.net/news/ARBK/argo-blockchain-plc-announces-october-operational-h3lgpm0o2hea.html"
      ],
      "failed_urls": [
        "https://ir.argo.partners/news — 404",
        "https://www.argoblockchain.com/news — 404",
        "https://www.argoblockchain.com/investors — loads but stale (2022 content only)"
      ],
      "proposed_entry": {
        "ticker": "ARBK",
        "name": "Argo Blockchain plc",
        "tier": 3,
        "ir_url": "https://www.accessnewswire.com/newsroom/en/blockchain-and-cryptocurrency",
        "pr_base_url": "https://www.accessnewswire.com",
        "rss_url": null,
        "cik": "0001708187",
        "active": true,
        "scrape_mode": "index",
        "url_template": null,
        "pr_start_year": 2021,
        "skip_reason": null,
        "sandbox_note": "Press releases distributed via accessnewswire.com. URL pattern is /newsroom/en/blockchain-and-cryptocurrency/argo-blockchain-plc-announces-{month}-operational-update-{id} but IDs are non-sequential. Index scrape from /newsroom/en/blockchain-and-cryptocurrency filtered by 'Argo'. Company in severe distress: LSE delisted Dec 11 2025, Nasdaq delisting proceedings. Monthly updates continue but very low BTC volumes (~40-130 BTC/month in late 2024/early 2025). Also check https://www.argoblockchain.com/investors/news/rns for RNS filings."
      },
      "template_examples": [],
      "pr_title_patterns": [
        "Argo Blockchain PLC Announces {Month} Operational Update",
        "Argo Blockchain PLC {Month} Ops Update",
        "Argo Blockchain PLC Announces {Month} Operational Update, TVR and New Shares"
      ]
    },
    {
      "ticker": "CIFR",
      "action": "update",
      "confidence": "high",
      "finding": "Cipher Mining was NOT acquired by Bitfarms. The acquisition confusion was with SDIG (Stronghold Digital Mining). Cipher Mining rebranded to Cipher Digital in late 2025 to reflect its strategic pivot to HPC/AI data center development. The company continues publishing monthly operational updates under the 'Cipher Mining' name via investors.ciphermining.com, which now redirects to investors.cipherdigital.com. Monthly updates confirmed January through September 2025. After September 2025, the company moved to quarterly reporting format ('Q3 2025 Business Update' rather than individual monthly updates). GlobeNewswire is used for syndication. The IR platform is Equisolve-style with RSS feed at /rss/news-releases.xml. Production figures reported in hundreds of BTC monthly (210-251 BTC in Mar-Sep 2025).",
      "verified_urls": [
        "https://www.globenewswire.com/news-release/2025/02/03/3019860/0/en/Cipher-Mining-Announces-January-2025-Operational-Update.html",
        "https://www.globenewswire.com/news-release/2025/03/04/3036954/0/en/cipher-mining-announces-february-2025-operational-update.html",
        "https://www.globenewswire.com/news-release/2025/04/04/3056190/0/en/Cipher-Mining-Announces-March-2025-Operational-Update.html",
        "https://www.globenewswire.com/news-release/2025/10/07/3162944/0/en/Cipher-Mining-Announces-September-2025-Operational-Update.html",
        "https://investors.cipherdigital.com/rss/news-releases.xml"
      ],
      "failed_urls": [
        "https://investors.ciphermining.com — redirects 301 to investors.cipherdigital.com"
      ],
      "proposed_entry": {
        "ticker": "CIFR",
        "name": "Cipher Mining Inc. (now Cipher Digital)",
        "tier": 2,
        "ir_url": "https://investors.cipherdigital.com/news-events/press-releases",
        "pr_base_url": "https://investors.cipherdigital.com",
        "rss_url": "https://investors.cipherdigital.com/rss/news-releases.xml",
        "cik": "0001838247",
        "active": true,
        "scrape_mode": "rss",
        "url_template": null,
        "pr_start_year": 2021,
        "skip_reason": null,
        "sandbox_note": "Rebranded as Cipher Digital but maintains CIFR ticker. investors.ciphermining.com redirects to investors.cipherdigital.com. Monthly updates Jan-Sep 2025 confirmed, then shifted to quarterly 'Business Update' format (Q3 2025 = Oct 2025, Q4 2025 = Feb 2026). RSS feed at investors.cipherdigital.com/rss/news-releases.xml. GlobeNewswire also distributes releases. NOT acquired by Bitfarms."
      },
      "template_examples": [],
      "pr_title_patterns": [
        "Cipher Mining Announces {Month} 2025 Operational Update",
        "Cipher Mining Provides {Quarter} 2025 Business Update"
      ]
    },
    {
      "ticker": "HIVE",
      "action": "update",
      "confidence": "high",
      "finding": "The HIVE Digital news page at hivedigitaltechnologies.com/news/ has STATIC HTML links — it is not a JS SPA. News links follow the slug pattern /news/{slug}/. However, the website does NOT have an RSS feed. Production reports are distributed primarily via Newsfile Corp (newsfilecorp.com/company/5335, RSS at feeds.newsfilecorp.com/company/5335). Monthly production reports confirmed through at least November 2025 (290 BTC mined). GlobeNewswire was used for some older releases (2022-2023) but current production reports go to Newsfile. The company reports under both TSX-V:HIVE and NASDAQ:HIVE.",
      "verified_urls": [
        "https://www.hivedigitaltechnologies.com/news/hive-digital-technologies-provides-august-2025-production-report-with-22-monthly-increase-in-bitcoin-production-and-phase-3-expansion/",
        "https://www.hivedigitaltechnologies.com/news/hive-digital-technologies-tops-15-ehs-and-provides-july-2025-production-report-with-24-monthly-increase-in-production/",
        "https://www.newsfilecorp.com/release/277551/HIVE-Digital-Technologies-Reports-November-Production-of-290-BTC-Achieves-25-EHs-as-Tier-III-AI-Data-Center-Growth-Accelerates-into-2026",
        "https://feeds.newsfilecorp.com/company/5335"
      ],
      "failed_urls": [
        "https://hivedigitaltechnologies.com/feed — not found",
        "https://hivedigitaltechnologies.com/news/rss — not found",
        "https://www.globenewswire.com/rssfeed/organization/VEmGT3UviXAdYJVILnjT3A== — only has releases through Dec 2023"
      ],
      "proposed_entry": {
        "ticker": "HIVE",
        "name": "HIVE Digital Technologies Ltd.",
        "tier": 2,
        "ir_url": "https://www.hivedigitaltechnologies.com/news/",
        "pr_base_url": "https://www.hivedigitaltechnologies.com",
        "rss_url": "https://feeds.newsfilecorp.com/company/5335",
        "cik": "0001537808",
        "active": true,
        "scrape_mode": "rss",
        "url_template": null,
        "pr_start_year": 2021,
        "skip_reason": null,
        "sandbox_note": "News page has static HTML links at /news/{slug}/ pattern. Production reports distributed via Newsfile Corp RSS (feeds.newsfilecorp.com/company/5335). hivedigitaltechnologies.com/news/ has no RSS feed. Newsfile RSS includes RETRANSMISSION duplicates — filter by 'Production Report' or 'Bitcoin Production' in title. Monthly reporting confirmed through Nov 2025 (290 BTC). Fiscal year ends March 31."
      },
      "template_examples": [
        "https://www.hivedigitaltechnologies.com/news/hive-digital-technologies-provides-august-2025-production-report-with-22-monthly-increase-in-bitcoin-production-and-phase-3-expansion/",
        "https://www.hivedigitaltechnologies.com/news/hive-digital-technologies-tops-15-ehs-and-provides-july-2025-production-report-with-24-monthly-increase-in-production/"
      ],
      "pr_title_patterns": [
        "HIVE Digital Technologies Provides {Month} 2025 Production Report...",
        "HIVE Digital Technologies Reports {Month} Production of {N} BTC...",
        "HIVE Digital Technologies Announces {Month} 2025 Production Results..."
      ]
    },
    {
      "ticker": "IREN",
      "action": "skip_updated",
      "confidence": "high",
      "finding": "IREN officially announced the discontinuation of monthly operating updates in October 2025, stating: 'IREN will transition to a standardized reporting process consistent with industry peers and monthly operating updates will be discontinued.' The last monthly update was for August 2025, published September 8, 2025. investors.iren.com returned ECONNREFUSED (still dead). IREN uses GlobeNewswire and the IR is accessible at iren.gcs-web.com or irisenergy.gcs-web.com (legacy domain). GlobeNewswire RSS confirmed active: globenewswire.com/rssfeed/organization/82e8_jAApdE1qYPVHkynKQ==. Company grew dramatically in 2025 (50 EH/s, $501M revenue FY25) and pivoted to AI Cloud (Microsoft $3.6B GPU contract).",
      "verified_urls": [
        "https://www.globenewswire.com/news-release/2025/09/08/3145927/0/en/IREN-August-2025-Monthly-Update.html",
        "https://www.globenewswire.com/rssfeed/organization/82e8_jAApdE1qYPVHkynKQ==",
        "https://irisenergy.gcs-web.com/news-releases/news-release-details/iren-august-2025-monthly-update",
        "https://iren.gcs-web.com/news-releases/news-release-details/iren-secures-new-multi-year-ai-cloud-contracts"
      ],
      "failed_urls": [
        "https://investors.iren.com/news-releases — ECONNREFUSED (confirmed dead)"
      ],
      "proposed_entry": {
        "ticker": "IREN",
        "name": "IREN Limited",
        "tier": 1,
        "ir_url": "https://iren.gcs-web.com/news-releases",
        "pr_base_url": "https://iren.gcs-web.com",
        "rss_url": "https://www.globenewswire.com/rssfeed/organization/82e8_jAApdE1qYPVHkynKQ==",
        "cik": "0001873044",
        "active": false,
        "scrape_mode": "skip",
        "url_template": null,
        "pr_start_year": 2022,
        "skip_reason": "Discontinued monthly production updates October 2025. Last monthly report: August 2025 (published Sep 8, 2025). Now reporting quarterly only. GlobeNewswire RSS still active but no monthly production updates will appear.",
        "sandbox_note": "investors.iren.com dead (ECONNREFUSED). Use irisenergy.gcs-web.com or iren.gcs-web.com as alternate IR URL. GlobeNewswire org ID: 82e8_jAApdE1qYPVHkynKQ==. Monthly updates covered Jan-Aug 2025. Company pivoted to AI Cloud — significant GPU business with Microsoft."
      },
      "template_examples": [],
      "pr_title_patterns": [
        "IREN {Month} 2025 Monthly Update"
      ]
    },
    {
      "ticker": "WULF",
      "action": "skip_updated",
      "confidence": "high",
      "finding": "TeraWulf's RSS feed at investors.terawulf.com/news-events/press-releases/rss is live and active (last updated March 4, 2026). However, the last monthly production update was 'TeraWulf Announces December 2024 Production and Operations Update' published January 3, 2025. No monthly production updates were found for any month in 2025. The company switched to quarterly reporting and Q1 2025 results showed 372 BTC self-mined during the quarter. TeraWulf pivoted heavily to HPC/AI data center infrastructure (signed $12.8B in AI/HPC contracts). The RSS feed is still valuable for quarterly earnings but not for monthly production.",
      "verified_urls": [
        "https://investors.terawulf.com/news-events/press-releases/rss",
        "https://investors.terawulf.com/news-events/press-releases/detail/100/terawulf-announces-december-2024-production-and-operations",
        "https://www.globenewswire.com/news-release/2025/01/03/3004172/0/en/TeraWulf-Announces-December-2024-Production-and-Operations-Update.html"
      ],
      "failed_urls": [],
      "proposed_entry": {
        "ticker": "WULF",
        "name": "TeraWulf Inc.",
        "tier": 2,
        "ir_url": "https://investors.terawulf.com/news-events/press-releases",
        "pr_base_url": "https://investors.terawulf.com",
        "rss_url": "https://investors.terawulf.com/news-events/press-releases/rss",
        "cik": "0001855052",
        "active": false,
        "scrape_mode": "skip",
        "url_template": null,
        "pr_start_year": 2022,
        "skip_reason": "Discontinued monthly production reporting after December 2024. Now reports quarterly only. Last monthly PR: December 2024 (published Jan 3, 2025). Pivoted to HPC/AI data center focus.",
        "sandbox_note": "RSS feed at investors.terawulf.com/news-events/press-releases/rss is live but contains no monthly production updates since Dec 2024. Q1 2025: 372 BTC self-mined. Q4 2025: 168.5M full-year revenue. Company now primarily an HPC/AI infrastructure provider."
      },
      "template_examples": [],
      "pr_title_patterns": [
        "TeraWulf Announces {Month} {Year} Production and Operations Update"
      ]
    },
    {
      "ticker": "SDIG",
      "action": "skip_confirmed",
      "confidence": "high",
      "finding": "Stronghold Digital Mining was acquired by Bitfarms. Stockholders approved the merger on February 27, 2025 with 99.6% approval. The merger closed on March 14, 2025 (confirmed via Bitfarms press release and Nasdaq ECA notice). SDIG no longer exists as an independent public company. The last standalone SDIG monthly production reports were published in 2024 (confirmed: June 2024 reports exist on ir.strongholddigitalmining.com). Bitfarms (BITF) monthly production reports now include the Stronghold capacity — Bitfarms published 'March 2025 Production and Operations Update' on April 1, 2025 which already includes former SDIG sites.",
      "verified_urls": [
        "https://investor.bitfarms.com/news-releases/news-release-details/bitfarms-advances-us-strategy-completion-stronghold-digital",
        "https://www.globenewswire.com/news-release/2025/04/01/3053168/0/en/Bitfarms-Provides-March-2025-Production-and-Operations-Update.html"
      ],
      "failed_urls": [
        "https://ir.strongholddigitalmining.com — no longer active as standalone IR"
      ],
      "proposed_entry": {
        "ticker": "SDIG",
        "name": "Stronghold Digital Mining (acquired by Bitfarms)",
        "tier": 3,
        "ir_url": null,
        "pr_base_url": null,
        "rss_url": null,
        "cik": "0001830029",
        "active": false,
        "scrape_mode": "skip",
        "url_template": null,
        "pr_start_year": 2022,
        "skip_reason": "Acquired by Bitfarms (BITF). Merger closed March 14, 2025. No longer an independent public company. Former SDIG capacity is now included in BITF monthly production reports.",
        "sandbox_note": "Last standalone monthly reports were mid-2024. Q3 2024 was the last quarterly earnings report. Merger closed March 14, 2025."
      },
      "template_examples": [],
      "pr_title_patterns": []
    },
    {
      "ticker": "CORZ",
      "action": "update",
      "confidence": "high",
      "finding": "Core Scientific IR page at investors.corescientific.com has STATIC HTML links in a /detail/{id}/{slug} pattern. RSS feed confirmed at /news-events/press-releases/rss. Monthly production updates published January, February, March 2025 (Jan: 256 BTC, Feb: 215 BTC, Mar: 247 BTC). March 2025 released April 7, 2025. No April 2025 or later monthly updates found — CORZ pivoted to AI/HPC colocation focus (CoreWeave merger negotiations through mid-2025, then terminated Oct 2025). The gap months in coverage were likely due to pre-bankruptcy era (Chapter 11 through January 2024) and the index scraper missing older URLs on the pre-bankruptcy domain ir.core-scientific.com.",
      "verified_urls": [
        "https://investors.corescientific.com/news-events/press-releases/rss",
        "https://investors.corescientific.com/news-events/press-releases/detail/113/core-scientific-announces-march-2025-production-and-operations-updates",
        "https://investors.corescientific.com/news-events/press-releases/detail/112/core-scientific-announces-february-2025-production-and-operations-updates",
        "https://investors.corescientific.com/news-events/press-releases/detail/106/core-scientific-announces-january-2025-production-and-operations-updates"
      ],
      "failed_urls": [],
      "proposed_entry": {
        "ticker": "CORZ",
        "name": "Core Scientific, Inc.",
        "tier": 1,
        "ir_url": "https://investors.corescientific.com/news-events/press-releases",
        "pr_base_url": "https://investors.corescientific.com",
        "rss_url": "https://investors.corescientific.com/news-events/press-releases/rss",
        "cik": "0001725526",
        "active": false,
        "scrape_mode": "skip",
        "url_template": null,
        "pr_start_year": 2022,
        "skip_reason": "Stopped monthly production reporting after March 2025. No April 2025 or later monthly updates found. Company pivoted to AI/HPC colocation. Last monthly report: March 2025 (published Apr 7, 2025). Now quarterly earnings only.",
        "sandbox_note": "Static HTML index with RSS. URL pattern: /news-events/press-releases/detail/{numeric_id}/{slug}. PRE-BANKRUPTCY gap: older PRs (pre-Jan 2024) were on old domain ir.core-scientific.com which may no longer be live. Current domain is investors.corescientific.com. Business Wire also distributes PRs. Monthly updates confirmed Jan-Mar 2025 only; quarterly after."
      },
      "template_examples": [
        "https://investors.corescientific.com/news-events/press-releases/detail/113/core-scientific-announces-march-2025-production-and-operations-updates",
        "https://investors.corescientific.com/news-events/press-releases/detail/112/core-scientific-announces-february-2025-production-and-operations-updates",
        "https://investors.corescientific.com/news-events/press-releases/detail/106/core-scientific-announces-january-2025-production-and-operations-updates"
      ],
      "pr_title_patterns": [
        "Core Scientific Announces {Month} {Year} Production and Operations Updates"
      ]
    },
    {
      "ticker": "HUT8",
      "action": "skip_updated",
      "confidence": "high",
      "finding": "HUT8 template URL (hut8.com/news-insights/press-releases/hut-8-operations-update-for-{month}-{year}) was CONFIRMED working for January 2025, February 2025, and March 2025. March 2025 update (published April 4, 2025) EXPLICITLY stated the company will no longer publish monthly operational updates going forward, replacing them with quarterly reporting. The gaps in coverage (July 2024 through current) are explained by: (1) a URL slug change during that period that differs from the template, and (2) the explicit end of monthly reporting after March 2025. The March 2025 update also announced the launch of American Bitcoin (ABTC) as a majority-owned subsidiary effective April 1, 2025, which took over the mining operations.",
      "verified_urls": [
        "https://www.hut8.com/news-insights/press-releases/hut-8-operations-update-for-january-2025",
        "https://www.hut8.com/news-insights/press-releases/hut-8-operations-update-for-february-2025",
        "https://www.hut8.com/news-insights/press-releases/hut-8-operations-update-for-march-2025",
        "https://www.globenewswire.com/news-release/2025/04/04/3056227/0/en/Hut-8-Operations-Update-for-March-2025.html",
        "https://www.hut8.com/news-insights/press-releases/hut-8-operations-update-for-november-2024"
      ],
      "failed_urls": [
        "https://www.hut8.com/news-insights/press-releases/hut-8-operations-update-for-april-2025 — does not exist",
        "https://www.hut8.com/news-insights/press-releases/hut-8-operations-update-for-july-2024 — not found with standard slug"
      ],
      "proposed_entry": {
        "ticker": "HUT8",
        "name": "Hut 8 Corp.",
        "tier": 1,
        "ir_url": "https://www.hut8.com/news-insights/press-releases",
        "pr_base_url": "https://www.hut8.com",
        "rss_url": null,
        "cik": "0001928898",
        "active": false,
        "scrape_mode": "skip",
        "url_template": "https://www.hut8.com/news-insights/press-releases/hut-8-operations-update-for-{month}-{year}",
        "pr_start_year": 2021,
        "skip_reason": "Discontinued monthly reporting after March 2025. Final monthly update (March 2025, published Apr 4 2025) explicitly stated switch to quarterly reporting. Launched American Bitcoin (ABTC) subsidiary April 1 2025 which absorbed mining operations.",
        "sandbox_note": "Template URL hut8.com/news-insights/press-releases/hut-8-operations-update-for-{month}-{year} works for Jan-Mar 2025 (confirmed). Coverage gap Jul-Dec 2024 has different URL slugs — some use date-based paths (/2024/12/05/hut-8-operations-update-for-november-2024/). GlobeNewswire RSS also distributes (org ID available). Last monthly: March 2025. Template uses lowercase month name."
      },
      "template_examples": [
        "https://www.hut8.com/news-insights/press-releases/hut-8-operations-update-for-january-2025",
        "https://www.hut8.com/news-insights/press-releases/hut-8-operations-update-for-february-2025",
        "https://www.hut8.com/news-insights/press-releases/hut-8-operations-update-for-march-2025"
      ],
      "pr_title_patterns": [
        "Hut 8 Operations Update for {Month} {Year}"
      ]
    }
  ],
  "new_companies": [
    {
      "ticker": "BTDR",
      "action": "add",
      "confidence": "high",
      "finding": "Bitdeer Technologies Group (NASDAQ: BTDR, CIK: 0001899123) is a Singapore-based Bitcoin mining company with operations in the US, Norway, and Bhutan. They publish monthly production and operations updates at ir.bitdeer.com. Confirmed monthly updates from January through November 2025, with 526 BTC mined in November 2025. The IR site has an RSS feed at ir.bitdeer.com/rss/news-releases.xml. URL pattern is slug-based: /news-releases/news-release-details/{slug}. The company is also developing its own SEALMINER ASICs. Strong candidate for addition with consistent monthly reporting.",
      "verified_urls": [
        "https://ir.bitdeer.com/news-releases/news-release-details/bitdeer-announces-january-2025-production-and-operations-update",
        "https://ir.bitdeer.com/news-releases/news-release-details/bitdeer-announces-november-2025-production-and-operations-update",
        "https://ir.bitdeer.com/rss/news-releases.xml"
      ],
      "proposed_entry": {
        "ticker": "BTDR",
        "name": "Bitdeer Technologies Group",
        "tier": 2,
        "ir_url": "https://ir.bitdeer.com/news-releases",
        "pr_base_url": "https://ir.bitdeer.com",
        "rss_url": "https://ir.bitdeer.com/rss/news-releases.xml",
        "cik": "0001899123",
        "active": true,
        "scrape_mode": "rss",
        "url_template": null,
        "pr_start_year": 2023,
        "skip_reason": null,
        "sandbox_note": "Singapore-based, NASDAQ listed. Monthly production reports at ir.bitdeer.com with slug-based URLs: /news-releases/news-release-details/{slug}. RSS feed confirmed at /rss/news-releases.xml. Also has /rss/sec-filings.xml and /rss/events.xml. Monthly updates confirmed Jan-Nov 2025. PR titles: 'Bitdeer Announces {Month} {Year} Production and Operations Update'. Company also files 6-K with SEC (not 8-K). Rapidly growing: 526 BTC/month by Nov 2025, 41.2 EH/s in Oct 2025."
      },
      "template_examples": [],
      "pr_title_patterns": [
        "Bitdeer Announces {Month} {Year} Production and Operations Update"
      ]
    },
    {
      "ticker": "ABTC",
      "action": "reject",
      "confidence": "high",
      "finding": "American Bitcoin Corp (NASDAQ: ABTC, CIK: 0001755953) is a majority-owned subsidiary of Hut 8, launched April 1, 2025 when Hut 8 spun out its mining operations. Co-founded with Eric Trump and Donald Trump Jr. as strategic advisors. Merged with Gryphon Digital Mining (previously GRPN/GRYP) on September 3, 2025. The company reports QUARTERLY only — no monthly production reports. Mined 1,654 BTC from Q2-Q4 2025 (783 BTC in Q4 alone). IR at abtc.com/investors. Not a suitable add for monthly-reporting tracker.",
      "verified_urls": [
        "https://www.abtc.com/investors",
        "https://www.abtc.com/content/american-bitcoin-reports-fourth-quarter-and-full-year-2025-results",
        "https://www.sec.gov/Archives/edgar/data/1755953/000121390025083726/ea0255440-8k_american.htm"
      ],
      "proposed_entry": {
        "ticker": "ABTC",
        "name": "American Bitcoin Corp.",
        "tier": 2,
        "ir_url": "https://www.abtc.com/investors",
        "pr_base_url": "https://www.abtc.com",
        "rss_url": null,
        "cik": "0001755953",
        "active": false,
        "scrape_mode": "skip",
        "url_template": null,
        "pr_start_year": 2025,
        "skip_reason": "Reports quarterly only. No monthly production updates published. Majority-owned subsidiary of HUT8. Merged with Gryphon Digital Mining Sep 3, 2025.",
        "sandbox_note": "Launched April 1 2025 when HUT8 spun out mining ops. Pure-play Bitcoin accumulation platform. 25 EH/s, ~78,000 ASICs. Q2-Q4 2025: 1,654 BTC mined. Quarterly earnings via PRNewswire. CIK 1755953 (formerly Gryphon Digital Mining). Use HUT8 template data through March 2025 for historical coverage."
      }
    },
    {
      "ticker": "APLD",
      "action": "reject",
      "confidence": "high",
      "finding": "Applied Digital Corporation (NASDAQ: APLD) pivoted from Bitcoin mining to AI/HPC data center infrastructure. The company no longer publishes monthly Bitcoin production reports. Revenue is predominantly from AI cloud and data center operations. Not suitable for monthly mining data collection.",
      "verified_urls": [],
      "proposed_entry": {
        "ticker": "APLD",
        "name": "Applied Digital Corporation",
        "tier": 3,
        "ir_url": "https://ir.applieddigital.com",
        "pr_base_url": "https://ir.applieddigital.com",
        "rss_url": null,
        "cik": null,
        "active": false,
        "scrape_mode": "skip",
        "url_template": null,
        "pr_start_year": null,
        "skip_reason": "Pivoted to AI/HPC data center infrastructure. No monthly Bitcoin production reports published.",
        "sandbox_note": "Primarily an AI data center company. Any historical mining data would require quarterly earnings reports, not monthly production updates."
      }
    },
    {
      "ticker": "GRDI",
      "action": "reject",
      "confidence": "high",
      "finding": "GRIID Infrastructure (NASDAQ: GRDI) was acquired by CleanSpark in October 2024. No longer an independent public company. CleanSpark (CLSK) already in the tracked company list.",
      "verified_urls": [],
      "proposed_entry": {
        "ticker": "GRDI",
        "name": "GRIID Infrastructure Inc. (acquired by CleanSpark)",
        "tier": 3,
        "ir_url": null,
        "pr_base_url": null,
        "rss_url": null,
        "cik": null,
        "active": false,
        "scrape_mode": "skip",
        "url_template": null,
        "pr_start_year": null,
        "skip_reason": "Acquired by CleanSpark (CLSK) in October 2024. No longer independent. Production folded into CLSK monthly updates.",
        "sandbox_note": null
      }
    },
    {
      "ticker": "MIGI",
      "action": "reject",
      "confidence": "high",
      "finding": "Mawson Infrastructure Group (NASDAQ: MIGI) publishes monthly financial updates via GlobeNewswire (confirmed Oct 2025). However, Bitcoin self-mining is now a minimal part of their business — only $0.1M in mining revenue in October 2025 (down 55% YoY, down 62% MoM). The company primarily operates energy management services and digital colocation. Not worth tracking for BTC production data — scale is negligible.",
      "verified_urls": [
        "https://www.globenewswire.com/news-release/2025/11/25/3194140/0/en/Mawson-Infrastructure-Group-Inc-Announces-Monthly-Financial-Update-for-October-2025.html",
        "https://www.mawsoninc.com/mawson-infrastructure-group-inc-announces-monthly-financial-update-for-october-2025/"
      ],
      "proposed_entry": {
        "ticker": "MIGI",
        "name": "Mawson Infrastructure Group Inc.",
        "tier": 3,
        "ir_url": "https://www.mawsoninc.com/investors",
        "pr_base_url": "https://www.mawsoninc.com",
        "rss_url": null,
        "cik": null,
        "active": false,
        "scrape_mode": "skip",
        "url_template": null,
        "pr_start_year": null,
        "skip_reason": "Bitcoin mining now negligible (<$0.1M/month revenue). Primarily energy management and digital colocation. Monthly updates published but not Bitcoin-mining-focused.",
        "sandbox_note": "GlobeNewswire distributes monthly financial updates. URL on mawsoninc.com. If BTC mining scale increases significantly, reconsider."
      }
    },
    {
      "ticker": "GREE",
      "action": "reject",
      "confidence": "high",
      "finding": "Greenidge Generation (NASDAQ: GREE) reports quarterly only — no monthly production updates. Produced approximately 110 BTC per quarter in 2025 (very small scale). Sold its Mississippi facility in September 2025. Operating in NY, North Dakota primarily. Not suitable for monthly data collection.",
      "verified_urls": [
        "https://ir.greenidge.com/news-releases/news-release-details/greenidge-generation-reports-financial-and-operating-results-2",
        "https://ir.greenidge.com/news-releases/news-release-details/greenidge-provides-bitcoin-production-update"
      ],
      "proposed_entry": {
        "ticker": "GREE",
        "name": "Greenidge Generation Holdings Inc.",
        "tier": 3,
        "ir_url": "https://ir.greenidge.com/news-releases",
        "pr_base_url": "https://ir.greenidge.com",
        "rss_url": null,
        "cik": null,
        "active": false,
        "scrape_mode": "skip",
        "url_template": null,
        "pr_start_year": null,
        "skip_reason": "Quarterly reporting only. ~110 BTC per quarter in 2025 — too small scale for monthly tracking. No monthly production updates.",
        "sandbox_note": "Still operating but small scale. Uses Business Wire for press releases. IR at ir.greenidge.com. Slug-based URLs."
      }
    }
  ],
  "globenewswire_issuers": {
    "HIVE": "null — HIVE uses Newsfile Corp, not GlobeNewswire. RSS: https://feeds.newsfilecorp.com/company/5335",
    "IREN": "https://www.globenewswire.com/rssfeed/organization/82e8_jAApdE1qYPVHkynKQ== (active but no monthly updates after Aug 2025)",
    "CORZ": "null — CORZ uses Business Wire + own IR site. RSS: https://investors.corescientific.com/news-events/press-releases/rss",
    "CIFR": "https://www.globenewswire.com (syndication only; primary: https://investors.cipherdigital.com/rss/news-releases.xml)",
    "HUT8": "https://www.globenewswire.com (confirmed active for HUT8 updates through Mar 2025; org ID not extracted)"
  },
  "coverage_gap_analysis": {
    "HUT8": {
      "missing_periods": ["2024-07", "2024-08", "2024-09", "2024-10", "2024-11", "2024-12", "2025-01", "2025-02", "2025-03"],
      "gap_explanation": "Gap July-December 2024: Some months used date-based paths (/2024/12/05/hut-8-operations-update-for-november-2024/) rather than the standard template path. The template scraper would miss these. January-March 2025 are confirmed reachable via template. March 2025 was the final monthly update (company explicitly announced end of monthly reporting).",
      "recovery_strategy": "For July-December 2024: search hut8.com news section for 'operations update' dated July-December 2024 using index scrape. Template works for Jan-Mar 2025. After March 2025, ABTC holds the mining data (quarterly only)."
    },
    "CORZ": {
      "missing_periods": ["2022-01 through 2023-12 (pre-bankruptcy era)"],
      "gap_explanation": "CORZ filed Chapter 11 in December 2022 and emerged January 2024. Pre-bankruptcy IR domain was ir.core-scientific.com. Monthly reports from that era would be on the old domain, which may be inaccessible or redirecting. Post-emergence (Jan 2024+) reports are on investors.corescientific.com. The 3 month gaps noted in 26-month coverage likely correspond to specific months where the URL ID sequence was non-continuous or monthly reports were skipped during bankruptcy proceedings.",
      "recovery_strategy": "Fetch the RSS feed (investors.corescientific.com/news-events/press-releases/rss) which should list all available PRs. Use the full press release index to identify any missing monthly production updates. Note: CORZ stopped monthly reporting after March 2025."
    },
    "ARBK": {
      "missing_periods": ["2025-05 through 2025-12 (unverified)"],
      "gap_explanation": "Argo had listing suspension and Nasdaq delisting proceedings in 2025. Monthly updates continued but with less regular cadence. The accessnewswire.com search shows December 2024 as last confirmed month, though July 2025 data was referenced in an AJ Bell article. URL IDs are non-sequential so template is not viable.",
      "recovery_strategy": "Index scrape of accessnewswire.com/newsroom/en/blockchain-and-cryptocurrency filtered for 'Argo Blockchain' — this should surface all 2025 monthly updates. Alternatively use EDGAR 6-K filings (ARBK files 6-K for operational updates) at sec.gov/cgi-bin/browse-edgar?action=getcompany&CIK=0001708187&type=6-K."
    }
  }
}
```

---

## Implementation Notes

### 1. Priority Order (companies to unblock/fix first)

1. **CIFR** (highest priority) — Was incorrectly believed to be acquired by Bitfarms. Active monthly reporting continues via investors.cipherdigital.com RSS feed. Update ir_url, rss_url, and scrape_mode to `rss`. This unblocks ongoing monthly production data capture for a major miner (23.6 EH/s, ~250 BTC/month).

2. **HIVE** (high priority) — Incorrect assumption it was a JS SPA. The website has static HTML links. Primary distribution is Newsfile Corp RSS: feeds.newsfilecorp.com/company/5335. Update scrape_mode to `rss` and set rss_url. Monthly reporting confirmed through Nov 2025.

3. **BTDR** (high priority — new add) — Bitdeer is actively reporting monthly with consistent data through Nov 2025 (526 BTC). RSS feed confirmed. Add as new company with scrape_mode=`rss`.

4. **IREN** (medium priority) — Update to `skip` with skip_reason. Was falsely shown as `skip` due to dead investors.iren.com. The correct IR is via GlobeNewswire (RSS confirmed active). Historical data through Aug 2025 recoverable via GNW RSS. Since monthly updates discontinued Oct 2025, mark active=false.

5. **WULF** (medium priority) — Update to `skip` with skip_reason documenting Dec 2024 as last monthly. RSS is live but no production updates.

6. **HUT8** (medium priority) — Update to `skip`. Template works for Jan-Mar 2025 (confirmed). Backfill Jul-Dec 2024 via index search. Mark active=false with note about ABTC subsidiary.

7. **CORZ** (medium priority) — Update to `skip`. RSS confirmed. Monthly updates Jan-Mar 2025 available but company stopped after March 2025.

8. **SDIG** (low priority) — Already skip. Confirm merged into BITF March 14, 2025.

9. **ARBK** (low priority) — Update ir_url to accessnewswire.com index page. Company in financial distress, very low BTC volumes but still reporting.

### 2. GlobeNewswire Mode Implementation

Proposed `globenewswire` scrape_mode should work as follows:
- RSS URL format: `https://www.globenewswire.com/rssfeed/organization/{org_id_base64}`
- Each RSS item contains title, link, and pubDate
- Filter production PRs by title keywords: `production`, `monthly update`, `operations update`
- Extract period from PR title: match pattern `{Month} {Year}` or `{Month} {4-digit-year}`
- Fetch the linked GlobeNewswire article URL to get full text
- Run normal extraction pipeline on full text

Known GlobeNewswire org IDs:
- IREN: `82e8_jAApdE1qYPVHkynKQ==`
- HIVE: `VEmGT3UviXAdYJVILnjT3A==` (but HIVE switched to Newsfile for production reports)

For BITDR: uses its own IR RSS (`ir.bitdeer.com/rss/news-releases.xml`) — this is an Equisolve-style IR site, not GlobeNewswire. The existing `rss` scrape_mode should handle this if the scraper fetches the RSS and filters by production keywords.

### 3. HUT8/ABTC Relationship in Registry

HUT8 mining data through March 2025 should be tracked under ticker `HUT8`. From April 2025 onward, the mining operations were transferred to ABTC. The platform should handle this as:
- `HUT8` coverage: 2021-01 through 2025-03 (last monthly: March 2025)
- `ABTC` coverage: 2025-04 onward (but ABTC reports quarterly, not monthly)

For the asset_manifest and data_points tables, HUT8 historical entries remain under HUT8. There is no continuation to ABTC in monthly format. HUT8 still exists as a company (manages infrastructure, hosting, AI infrastructure) but no longer self-mines Bitcoin.

### 4. New Company Priority

1. **BTDR (Bitdeer)** — Add immediately. Active monthly reporting, 526 BTC/month, RSS confirmed.
2. **ABTC (American Bitcoin)** — Do NOT add as monthly tracker. Quarterly only. Note in HUT8 entry.
3. All other candidates (APLD, GRDI, MIGI, GREE) — Reject. Either quarterly, negligible BTC, or acquired.

Other monthly-reporting miners found in broad sweep that are NOT in current 13-company list:
- **Canaan Inc.** (CAN) — publishes monthly production updates (86 BTC in Dec 2025). Small scale.
- **Bit Digital** (BTBT is in list) — note: Bit Digital uses BTBT ticker, already tracked.
- **LM Funding America** (LMFA) — publishes monthly, but only 7.5 BTC/month max (record). Negligible.
- **BitFuFu** (FUFU) — publishes monthly updates on GlobeNewswire. Mining platform rather than pure miner.

None of these meet Tier 1/2 scale for addition at this time.

### 5. Template Variants Needed

Current system uses `{month}` (lowercase) and `{Month}` (titlecase) and `{year}`. No additional template placeholders needed for companies researched. HUT8 template confirmed working: `hut8.com/news-insights/press-releases/hut-8-operations-update-for-{month}-{year}` where `{month}` is lowercase (e.g., `january`, `february`).

For HIVE and CIFR and BTDR, the `rss` scrape_mode handles production PR retrieval without templates. The period inference from PR title handles month/year extraction from titles like "HIVE Digital Technologies Provides August 2025 Production Report..." or "Bitdeer Announces November 2025 Production and Operations Update".

---

## Sources Used

- [Argo Blockchain Announces October Operational Update](https://www.stocktitan.net/news/ARBK/argo-blockchain-plc-announces-october-operational-h3lgpm0o2hea.html)
- [Argo on AccessNewswire](https://www.accessnewswire.com/newsroom/en/blockchain-and-cryptocurrency/argo-blockchain-plc-announces-october-operational-update-939078)
- [Cipher Mining January 2025 Operational Update - GlobeNewswire](https://www.globenewswire.com/news-release/2025/02/03/3019860/0/en/Cipher-Mining-Announces-January-2025-Operational-Update.html)
- [Cipher Mining September 2025 Operational Update - GlobeNewswire](https://www.globenewswire.com/news-release/2025/10/07/3162944/0/en/Cipher-Mining-Announces-September-2025-Operational-Update.html)
- [HIVE August 2025 Production Report](https://www.hivedigitaltechnologies.com/news/hive-digital-technologies-provides-august-2025-production-report-with-22-monthly-increase-in-bitcoin-production-and-phase-3-expansion/)
- [HIVE Newsfile RSS](https://feeds.newsfilecorp.com/company/5335)
- [IREN August 2025 Monthly Update - GlobeNewswire](https://www.globenewswire.com/news-release/2025/09/08/3145927/0/en/IREN-August-2025-Monthly-Update.html)
- [IREN discontinuing monthly updates](https://iren.gcs-web.com/news-releases/news-release-details/iren-secures-new-multi-year-ai-cloud-contracts)
- [IREN GlobeNewswire RSS](https://www.globenewswire.com/rssfeed/organization/82e8_jAApdE1qYPVHkynKQ==)
- [TeraWulf December 2024 Production Update](https://investors.terawulf.com/news-events/press-releases/detail/100/terawulf-announces-december-2024-production-and-operations)
- [Bitfarms completes Stronghold acquisition](https://investor.bitfarms.com/news-releases/news-release-details/bitfarms-advances-us-strategy-completion-stronghold-digital)
- [Nasdaq ECA SDIG merger closed](https://www.nasdaqtrader.com/TraderNews.aspx?id=ECA2025-112)
- [Core Scientific March 2025 Production Update](https://investors.corescientific.com/news-events/press-releases/detail/113/core-scientific-announces-march-2025-production-and-operations-updates)
- [Core Scientific RSS](https://investors.corescientific.com/news-events/press-releases/rss)
- [Hut 8 March 2025 Operations Update](https://www.hut8.com/news-insights/press-releases/hut-8-operations-update-for-march-2025)
- [Hut 8 January 2025 Operations Update](https://www.hut8.com/news-insights/press-releases/hut-8-operations-update-for-january-2025)
- [American Bitcoin Q4 2025 Results](https://www.abtc.com/content/american-bitcoin-reports-fourth-quarter-and-full-year-2025-results)
- [Bitdeer November 2025 Production Update](https://ir.bitdeer.com/news-releases/news-release-details/bitdeer-announces-november-2025-production-and-operations-update)
- [Bitdeer RSS Feeds](https://ir.bitdeer.com/rss-feeds)
- [Greenidge Q1 2025 Results](https://ir.greenidge.com/news-releases/news-release-details/greenidge-generation-reports-financial-and-operating-results-2)
- [Mawson October 2025 Monthly Update - GlobeNewswire](https://www.globenewswire.com/news-release/2025/11/25/3194140/0/en/Mawson-Infrastructure-Group-Inc-Announces-Monthly-Financial-Update-for-October-2025.html)
