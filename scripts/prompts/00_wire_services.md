# Wire Service Search Guide (shared by all agents)

Wire services maintain permanent, searchable archives and are often MORE complete
than a company's own IR page — especially for pre-2022 data where IR sites have
rotated URLs or gone offline. Treat wire services as PRIMARY sources, not fallbacks.

## Per-company primary wire service

| Ticker | Primary Wire       | Secondary Wire      | Notes |
|--------|--------------------|---------------------|-------|
| MARA   | GlobeNewswire      | BusinessWire        | Early 2020 on BusinessWire, shifted to GlobeNewswire mid-2020 |
| RIOT   | BusinessWire       | GlobeNewswire       | Consistent BusinessWire user since 2018 |
| CLSK   | GlobeNewswire      | PR Newswire         | |
| BITF   | Accesswire / GlobeNewswire | newswire.ca (Canada) | Canadian company — also on Marketwired (archived) |
| BTBT   | GlobeNewswire      | PR Newswire         | China-based, some releases on PRN |
| ARBK   | Accesswire         | GlobeNewswire (UK)  | LSE-listed — also on Regulatory News Service (RNS) |
| CIFR   | GlobeNewswire      | —                   | |
| HIVE   | Newsfile Corp      | GlobeNewswire       | Filter Newsfile for "Production Report" or "Bitcoin Production" |
| BTDR   | PR Newswire        | GlobeNewswire       | Singapore HQ |
| CORZ   | BusinessWire       | PR Newswire         | Pre-bankruptcy on BW, post-emergence on PRN |
| HUT8   | GlobeNewswire      | Newsfile Corp       | Canadian company |
| WULF   | GlobeNewswire      | PR Newswire         | |
| IREN   | GlobeNewswire      | —                   | Org ID: 82e8_jAApdE1qYPVHkynKQ== |
| ABTC   | PR Newswire        | GlobeNewswire       | Launched Apr 2025 |
| SDIG   | BusinessWire       | GlobeNewswire       | |

## Search patterns by wire service

### GlobeNewswire
Direct organization RSS (preferred — complete archive, no pagination):
- MARA: https://www.globenewswire.com/RssFeed/company/marathon-digital-holdings (or search by CIK/org)
- Web search: `site:globenewswire.com "[company name]" "[month] [year]" bitcoin production`
- Search page: https://www.globenewswire.com/search/keyword?keyword=[company]&date=[YYYY-MM-DD]..[YYYY-MM-DD]

### BusinessWire
- Search: https://www.businesswire.com/news/home/search/?q=[company]+bitcoin+production+[month]+[year]
- Web search: `site:businesswire.com "[company name]" "[month] [year]" production`
- BusinessWire keeps full text and is Google-indexed going back to 2010+

### PR Newswire
- Web search: `site:prnewswire.com "[company name]" "[month] [year]" production`
- Direct search: https://www.prnewswire.com/news-releases/news-releases-list.html (filter by company)
- PRN archives are permanent and fully text-searchable

### IR year-filter dropdowns (critical for BITF and similar Drupal IR pages)
- Some IR pages expose a year selector where the query payload controls global pagination.
- Detect `select` names like `*_year[value]` plus form fields (`form_id`, `form_build_id`, widget id).
- Build per-year URLs by preserving existing form query params and replacing only the year value.
- Iterate each year (2018..current), then parse each filtered listing for production links.
- Do not hardcode stale `form_build_id`; always refresh from the current page before generating year URLs.
- Treat this as first-class schema in scout output:
  - `discovery_method=year_filter`
  - `year_filter.select_name`
  - `year_filter.years`
  - `year_filter.url_template`
  - `year_filter.year_urls` (sampled validated URLs)

### Accesswire / Access Newswire
- ARBK primary: https://www.accessnewswire.com/newsroom/en/blockchain-and-cryptocurrency (filter "Argo")
- Web search: `site:accessnewswire.com "Argo Blockchain" [month] [year]`

### Newsfile Corp (HIVE primary)
- RSS: https://feeds.newsfilecorp.com/company/5335
- Web: https://www.newsfilecorp.com/company/5335 (HIVE Digital Technologies)
- IMPORTANT: Newsfile RSS includes RETRANSMISSION duplicates — skip any title containing "RETRANSMISSION"

### Canadian wire: newswire.ca / CNW Group
- Used by BITF, HUT8 for Canadian distribution
- Web search: `site:newswire.ca "[company name]" [month] [year]`

### UK Regulatory News Service (RNS) — ARBK only
- ARBK is LSE-listed — some regulatory announcements on RNS
- Web search: `site:londonstockexchange.com "Argo Blockchain" [month] [year]`
- Or: https://www.londonstockexchange.com/stock/ARB/argo-blockchain-plc/company-news

## Search strategy for any missing month

Execute in this order — stop as soon as you find the press release:

1. **Primary wire service** (see table above) — try direct site: search
2. **Secondary wire service** — try direct site: search
3. **Company IR page** — template URL if known, else index page
4. **Broad web search** — `"[Company Name]" "[Month] [Year]" bitcoin production`
5. **EDGAR 8-K** — check EDGAR for 8-K filed in the target month: https://efts.sec.gov/LATEST/search-index?q=%22bitcoin%22+%22production%22&forms=8-K&dateRange=custom&startdt=[YYYY-MM-01]&enddt=[YYYY-MM-31]&entity=[ticker]
6. **Archive.org** — https://web.archive.org/web/[YYYYMM]*/[company-ir-url]/*

## Fetching and storing wire service content

Wire service HTML is clean and text-extractable. Fetch the full article URL and extract body text:

```python
import requests
from bs4 import BeautifulSoup

resp = requests.get(url, headers={'User-Agent': 'Mozilla/5.0'}, timeout=30)
soup = BeautifulSoup(resp.text, 'html.parser')

# GlobeNewswire
text = soup.find('article') or soup.find('div', class_='article-body')

# BusinessWire
text = soup.find('div', class_='bw-release-body') or soup.find('section', class_='bw-release-main')

# PR Newswire
text = soup.find('div', class_='release-body')

# Fallback — get all visible text
raw_text = soup.get_text(separator=' ', strip=True)
```

Then insert to DB and run extraction:
```bash
python3 -c "
import sys; sys.path.insert(0, 'src')
from infra.db import MinerDB
from config import DATA_DIR
from pathlib import Path
db = MinerDB(str(Path(DATA_DIR) / 'minerdata.db'))
rid = db.insert_report({
    'ticker': 'TICKER',
    'report_date': 'YYYY-MM-01',
    'source_type': 'ir_press_release',
    'source_url': 'https://www.globenewswire.com/...',
    'raw_text': raw_text,
    'parsed_at': None,
    'covering_period': None,
})
print('Inserted', rid)
"
python3 cli.py extract --ticker TICKER
```

## Deduplication rule

Before inserting: check if a report for this ticker + period already exists:
```python
conn.execute(
    "SELECT id FROM reports WHERE ticker=? AND report_date=? AND source_type='ir_press_release'",
    (ticker, 'YYYY-MM-01')
).fetchone()
```
If it exists, skip — do not insert a duplicate.

## 502 / rate limit handling on wire services

- GlobeNewswire: rarely 502; if hit, wait 15s and retry
- BusinessWire: occasionally slow; 30s timeout, 3 retries
- PR Newswire: may block aggressive scraping — add 2s delay between requests
- Accesswire: most likely to 502 (ARBK's primary) — use 30s retry, 3 attempts, then fall back to web search + GlobeNewswire

Never crash on a single URL failure. Log it, continue to next month.
