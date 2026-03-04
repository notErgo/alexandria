# Agent D — Quarterly/Inactive Companies: CORZ + HUT8 + WULF + IREN + ABTC + SDIG

## Environment
- Working dir: /Users/workstation/Documents/Hermeneutic/OffChain/miners
- Python venv: ./venv/bin/python3
- DB: ~/Documents/Hermeneutic/data/miners/minerdata.db
- Progress file: /private/tmp/claude-501/miners_progress/agent_D.json
- OTEL: export OTEL_RESOURCE_ATTRIBUTES="plan=full_ingest_v1,phase=agent_D,tickers=CORZ_HUT8_WULF_IREN_ABTC_SDIG"

## Current State
- CORZ: 26 reports, 23 production_btc months (2023-01 to 2025-03). Stopped monthly reporting Mar 2025. CIK=0001725526.
- HUT8: 16 reports, 8 production_btc months (2024-07 to 2025-03). Stopped monthly reporting Mar 2025. CIK=0001558370 (previously wrong — verified via EDGAR accession prefix).
- WULF: 0 reports. Stopped monthly Dec 2024. CIK=0001083301 (previously wrong 0001855052 — verified via submissions API).
- IREN: 0 reports. Stopped monthly Aug 2025. CIK=0001878848 (previously wrong 0001873044 — verified via submissions API).
- ABTC: 0 reports. Launched Apr 2025. CIK=0001755953. ACTIVE treasury-style company. abtc.com/news
- SDIG: 0 reports. Acquired by BITF Mar 2025. CIK=0001830029. Start 2022. Historical EDGAR only.

## Step 0 — Verify all CIKs before running EDGAR

Before running any EDGAR ingestion, confirm each CIK resolves at the submissions API:
```python
import urllib.request, json
headers = {'User-Agent': 'Hermeneutic Research Platform research@hermeneutic.io', 'Accept': 'application/json'}
for ticker, cik in [('CORZ','0001725526'),('HUT8','0001558370'),('WULF','0001083301'),('IREN','0001878848'),('ABTC','0001755953'),('SDIG','0001830029')]:
    req = urllib.request.Request(f'https://data.sec.gov/submissions/CIK{cik}.json', headers=headers)
    try:
        with urllib.request.urlopen(req, timeout=15) as r:
            d = json.load(r)
            print(f'{ticker} CIK {cik}: OK — {d.get("name")} {d.get("tickers")}')
    except Exception as e:
        print(f'{ticker} CIK {cik}: FAIL — {e} — FIND THE CORRECT CIK BEFORE CONTINUING')
```

If any CIK returns 404: search EDGAR for the correct one using the ticker symbol or company name, update companies.json, and retry. Never proceed with a broken CIK — it will ingest 0 quarterly filings silently.

## Step 1 — OTEL and activate
```bash
export OTEL_RESOURCE_ATTRIBUTES="plan=full_ingest_v1,phase=agent_D,tickers=CORZ_HUT8_WULF_IREN_ABTC_SDIG"
cd /Users/workstation/Documents/Hermeneutic/OffChain/miners
source venv/bin/activate
```

## Step 2 — EDGAR 10-Q/10-K for all six companies
Store as quarterly (source_period_type='quarterly'). Do NOT impute to monthly.
```bash
python3 scripts/run_edgar_all.py --ticker CORZ --since 2022-01 --extract
python3 scripts/run_edgar_all.py --ticker HUT8 --since 2021-01 --extract
python3 scripts/run_edgar_all.py --ticker WULF --since 2022-01 --extract
python3 scripts/run_edgar_all.py --ticker IREN --since 2022-01 --extract
python3 scripts/run_edgar_all.py --ticker ABTC --since 2025-01 --extract
python3 scripts/run_edgar_all.py --ticker SDIG --since 2022-01 --extract
```
Note: BTDR files 6-K (foreign private issuer) — EDGAR may not have standard 10-Q. Attempt anyway.
Note: IREN is also a foreign private issuer (Australian) — files 20-F annually.

## Step 3 — ABTC: Web scrape full press release archive
ABTC has its own news page with treasury-style and operational press releases.
Base URL: https://www.abtc.com/news

Known press releases (fetch ALL of these and extract data):
1. https://www.abtc.com/content/american-bitcoin-expands-mining-capacity-by-an-anticipated-12-in-eh-s-with-11-298-additional-asics (Mar 3, 2026)
2. https://www.abtc.com/content/american-bitcoin-reports-fourth-quarter-and-full-year-2025-results (Feb 26, 2026)
3. https://www.abtc.com/content/american-bitcoin-enters-top-20-publicly-traded-bitcoin-treasury-companies-by-holdings (Dec 16, 2025)
4. https://www.abtc.com/content/american-bitcoin-increases-strategic-reserve-to-4-783-bitcoin (Dec 10, 2025)
5. https://www.abtc.com/content/american-bitcoin-reports-third-quarter-2025-results (Nov 14, 2025)
6. https://www.abtc.com/content/american-bitcoin-adds-139-bitcoin-increasing-strategic-reserve-to-4-004-bitcoin-2 (Nov 7, 2025)
7. https://www.abtc.com/content/american-bitcoin-acquires-1-414-bitcoin-and-increases-strategic-reserve-to-3-865-bitcoin (Oct 27, 2025)
8. https://www.abtc.com/content/american-bitcoin-expands-bitcoin-mining-operations-by-2-4x-from-10-eh-s-to-24-eh-s (Sep 4, 2025)
9. https://www.abtc.com/content/hut-8-and-eric-trump-launch-american-bitcoin-to-set-a-new-standard-in-bitcoin-mining (Mar 31, 2025)

Also check for any additional press releases at https://www.abtc.com/news

## ABTC Quarterly Earnings — PRNewswire (MANDATORY — ingest ALL of these)

ABTC files quarterly earnings as press releases via PRNewswire. These contain detailed financial results (revenue, cost per BTC, hashrate, BTC mined for the quarter).

Search PRNewswire for each quarter:
- Q2 2025 (Aug 2025): `site:prnewswire.com "American Bitcoin" "second quarter" 2025`
- Q3 2025 (Nov 2025): also check PRNewswire for full earnings text beyond abtc.com version
- Q4 + Full Year 2025 (Feb 2026): also check PRNewswire for full earnings text
- Q1 2025 (if exists, May/Jun 2025): `site:prnewswire.com "American Bitcoin" "first quarter" 2025`

For each quarterly earnings report found, store with:
- source_period_type = 'quarterly'
- covering_period = 'YYYY-Q1', 'YYYY-Q2', etc.
- source_type = 'ir_press_release'

Extract from quarterly earnings: btc_mined (quarterly total), mining_revenue_usd, btc_mined_usd_cost_per_btc, hashrate_operational, strategic_reserve_btc, miners_operational/miners_owned.

For ABTC, extract these specific metrics:
- strategic_reserve_btc: total BTC in strategic reserve
- btc_acquired: BTC added/acquired in this announcement
- hashrate_operational: current operational hashrate (EH/s)
- hashrate_total_fleet: total fleet hashrate (EH/s)
- miners_owned: total ASICs owned
- miners_operational: ASICs currently energized/operational
- mining_revenue_usd: quarterly mining revenue
- btc_mined: BTC mined in period (quarterly or monthly)
- btc_mined_usd_cost_per_btc: cost to mine 1 BTC

ABTC is a treasury-focused company — primary KPI is strategic_reserve_btc.
Store announcements with source_period_type='announcement' and period set to the announcement date's YYYY-MM.

To insert ABTC reports:
```bash
python3 -c "
import sys; sys.path.insert(0, 'src')
from infra.db import MinerDB
from config import DATA_DIR
from pathlib import Path
db = MinerDB(str(Path(DATA_DIR) / 'minerdata.db'))
report_id = db.insert_report({
    'ticker': 'ABTC',
    'report_date': '2025-12-10',
    'source_type': 'ir_press_release',
    'source_url': 'https://www.abtc.com/content/...',
    'raw_text': '<full fetched text>',
    'parsed_at': None,
    'covering_period': None,
})
print(report_id)
"
python3 cli.py extract --ticker ABTC
```

## Step 4 — CORZ gap fill (pre-2023)
CORZ has data from 2023-01 but start_year=2022. Try to find 2022 monthly reports:
- Web search: `"Core Scientific" bitcoin production update 2022 [MONTH]`
- Note: Core Scientific filed Chapter 11 in Dec 2022 — operational data may be sparse late 2022
- Check EDGAR for 8-K filings in 2022 with production data
- IR before bankruptcy: ir.core-scientific.com (may be archived)

## Step 5 — HUT8 gap fill (2021-01 to 2024-06)
HUT8 has data from 2024-07 only. Find earlier months:
- Template URL: `https://www.hut8.com/news-insights/press-releases/hut-8-operations-update-for-[month]-[year]`
- Also try date-based path: `https://www.hut8.com/news-insights/press-releases/[YYYY]/[MM]/[DD]/hut-8-operations-update-for-[month]-[year]/`
- Web search: `"Hut 8" operations update [MONTH] [YEAR] bitcoin`
- EDGAR: fetch 10-Q from 2021-2024

## Step 6 — WULF full history (2022-01 to 2024-12)
WULF started reporting in 2022, stopped monthly Dec 2024 (switched to quarterly).
- IR: https://investors.terawulf.com/news-events/press-releases
- RSS was confirmed live (though no recent monthly): investors.terawulf.com/news-events/press-releases/rss
- Web search: `TeraWulf bitcoin production [MONTH] [YEAR] press release`
- EDGAR 10-Q for supplemental quarterly data
- Q1 2025: 372 BTC self-mined (quarterly) — store as quarterly

## Step 7 — IREN full history (2022-01 to 2025-08)
IREN stopped monthly reporting Oct 2025 (last monthly = Aug 2025).
- GlobeNewswire RSS: feeds.globenewswire.com org ID 82e8_jAApdE1qYPVHkynKQ==
- Web search: `"IREN" OR "Iris Energy" bitcoin production [MONTH] [YEAR]`
- Note: Company was "Iris Energy" before rebranding to IREN
- EDGAR 20-F (annual, foreign issuer) + 6-K filings
- IR was at investors.iren.com (ECONNREFUSED) — use iren.gcs-web.com instead
- Monthly coverage: Jan 2022 to Aug 2025 (~44 months)

## Step 8 — SDIG historical (2022-01 to 2024-06)
SDIG was acquired by BITF March 2025. Historical data only.
- Web search: `"Stronghold Digital Mining" monthly production [MONTH] [YEAR]`
- EDGAR: 10-Q filings from 2022-2024
- IR was at ir.strongholddigitalmining.com (now inactive)
- Small operator (~15-40 BTC/month), focused on waste coal energy

## Step 9 — Store quarterly data as-is (no imputation)
For all EDGAR 10-Q data, store with:
- source_period_type = 'quarterly'
- covering_period = 'YYYY-Q1', 'YYYY-Q2', etc.
- period = covering_period (e.g., '2024-Q2-01' or use '2024-04-01' for Q2 start)

## Step 10 — Broad extraction on all new reports
```bash
python3 cli.py broad_extract --ticker CORZ
python3 cli.py broad_extract --ticker HUT8
python3 cli.py broad_extract --ticker WULF
python3 cli.py broad_extract --ticker IREN
python3 cli.py broad_extract --ticker ABTC
python3 cli.py broad_extract --ticker SDIG
```

## Step 11 — Write final progress JSON
Write to /private/tmp/claude-501/miners_progress/agent_D.json:
```json
{
  "agent": "D",
  "tickers": ["CORZ", "HUT8", "WULF", "IREN", "ABTC", "SDIG"],
  "status": "done",
  "per_ticker": {
    "CORZ": {"reports_added": <N>, "prod_months_after": <N>, "source_types": []},
    "HUT8": {"reports_added": <N>, "prod_months_after": <N>, "source_types": []},
    "WULF": {"reports_added": <N>, "prod_months_after": <N>, "source_types": []},
    "IREN": {"reports_added": <N>, "prod_months_after": <N>, "source_types": []},
    "ABTC": {"reports_added": <N>, "treasury_announcements": <N>, "reserve_btc_latest": <N>},
    "SDIG": {"reports_added": <N>, "prod_months_after": <N>, "source_types": []}
  },
  "abtc_note": "Treasury-style company. Strategic reserve is primary KPI, not monthly production.",
  "errors": [],
  "total_raw_extractions_added": <N>
}
```
