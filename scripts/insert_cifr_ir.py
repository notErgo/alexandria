"""Fetch and insert CIFR (Cipher Digital) monthly IR press releases.

Covers: December 2022 through September 2025.
Source: https://investors.cipherdigital.com (formerly ciphermining.com)

Period derivation
-----------------
Uses the same _correct_period_from_url() logic as fix_ir_period_dates.py.
URLs contain the covered month name (e.g. announces-december-2022), which
is the production month, not the publication month. report_date is set to
the first day of that month (2022-12-01), not the announcement date.

Dedup
-----
insert_report() deduplicates on sha256(source_url). Re-running is safe.

Pre-fetched data
----------------
If scripts/cifr_ir_fetched.json exists (produced by offline WebFetch agent),
the script reads text content from that file instead of making live HTTP
requests. The JSON is a list of {slug, url, status, text} objects.

Usage
-----
    python3 scripts/insert_cifr_ir.py [--dry-run]
"""

import re
import sys
import time
import json
from datetime import date, datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / 'src'))

import requests

from config import DATA_DIR
from infra.db import MinerDB

_FETCHED_CACHE = Path(__file__).parent / 'cifr_ir_fetched.json'

_BASE = 'https://investors.cipherdigital.com/news-releases/news-release-details'

_MONTH_MAP = {
    'january': 1, 'february': 2, 'march': 3, 'april': 4,
    'may': 5, 'june': 6, 'july': 7, 'august': 8,
    'september': 9, 'october': 10, 'november': 11, 'december': 12,
}

_URL_PATTERN = re.compile(r'announces-([a-zA-Z]+)-(\d{4})(?:-|$)', re.IGNORECASE)

# (slug, override_period_or_None)
# override_period is used for slugs that don't match the announces-{month}-{year} pattern
_PRESS_RELEASES = [
    # --- 2022 ---
    ('cipher-mining-announces-december-2022-operational-update',  None),
    # --- 2023 ---
    ('cipher-mining-announces-january-2023-operational-update',   None),
    ('cipher-mining-announces-february-2023-operational-update',  None),
    ('cipher-mining-announces-march-2023-operational-update',     None),
    ('cipher-mining-announces-april-2023-operational-update',     None),
    ('cipher-mining-announces-may-2023-operational-update',       None),
    ('cipher-mining-announces-june-2023-operational-update',      None),
    ('cipher-mining-announces-july-2023-operational-update',      None),
    ('cipher-mining-announces-august-2023-operational-update',    None),
    ('cipher-mining-announces-september-2023-operational-update', None),
    ('cipher-mining-announces-october-2023-operational-update',   None),
    ('cipher-mining-announces-november-2023-operational-update',  None),
    ('cipher-mining-announces-december-2023-operational-update',  None),
    # --- 2024 ---
    ('cipher-mining-announces-january-2024-operational-update',   None),
    ('cipher-mining-announces-february-2024-operational-update',  None),
    ('cipher-mining-announces-march-2024-operational-update',     None),
    ('cipher-mining-announces-april-2024-operational-update',     None),
    ('cipher-mining-announces-may-2024-operational-update',       None),
    ('cipher-mining-announces-june-2024-operational-update',      None),
    ('cipher-mining-announces-july-2024-operational-update',      None),
    ('cipher-mining-announces-august-2024-operational-update',    None),
    ('cipher-mining-announces-september-2024-operational-update', None),
    ('cipher-mining-announces-october-2024-operational-update',   None),
    ('cipher-mining-announces-november-2024-operational-update',  None),
    ('cipher-mining-announces-december-2024-operational-update',  None),
    # --- 2025 ---
    ('cipher-mining-announces-january-2025-operational-update',   None),
    ('cipher-mining-announces-february-2025-operational-update',  None),
    ('cipher-mining-announces-march-2025-operational-update',     None),
    ('cipher-mining-announces-april-2025-operational-update',     None),
    ('cipher-mining-announces-may-2025-operational-update',       None),
    # Jun 2025: non-standard slug — Black Pearl hashrate milestone PR
    ('cipher-mining-surpasses-hashrate-growth-forecasts-black-pearl', '2025-06-01'),
    ('cipher-mining-announces-july-2025-operational-update',      None),
    ('cipher-mining-announces-august-2025-operational-update',    None),
    ('cipher-mining-announces-september-2025-operational-update', None),
    # Oct–Dec 2025 (tentative — try, skip on 404)
    ('cipher-mining-announces-october-2025-operational-update',   None),
    ('cipher-mining-announces-november-2025-operational-update',  None),
    ('cipher-mining-announces-december-2025-operational-update',  None),
]


def _period_from_url(url: str, override: str | None) -> str | None:
    if override:
        return override
    for m in _URL_PATTERN.finditer(url):
        month_name = m.group(1).lower()
        year = int(m.group(2))
        month = _MONTH_MAP.get(month_name)
        if month:
            return date(year, month, 1).strftime('%Y-%m-%d')
    return None


def _load_cache() -> dict[str, dict]:
    """Load pre-fetched content keyed by slug. Returns empty dict if no cache."""
    if not _FETCHED_CACHE.exists():
        return {}
    with open(_FETCHED_CACHE) as f:
        records = json.load(f)
    return {r['slug']: r for r in records}


def _fetch_html(session: requests.Session, url: str) -> tuple[str | None, int]:
    """Fetch URL; return (html, http_status). Returns (None, status) on error."""
    headers = {
        'User-Agent': 'Hermeneutic Research Platform research@hermeneutic.ai',
        'Accept': 'text/html,application/xhtml+xml',
    }
    try:
        resp = session.get(url, headers=headers, timeout=20)
        if resp.status_code == 200:
            return resp.text, 200
        return None, resp.status_code
    except Exception as e:
        print(f"    Fetch error: {e}")
        return None, 0


def run(dry_run: bool = False) -> None:
    db_path = str(Path(DATA_DIR) / 'minerdata.db')
    db = MinerDB(db_path)
    session = requests.Session()
    cache = _load_cache()

    if cache:
        print(f"Using pre-fetched cache: {len(cache)} records from {_FETCHED_CACHE.name}")
    else:
        print("No cache found — will fetch live")
    print()

    now_iso = datetime.now(timezone.utc).isoformat()
    inserted = skipped = errors = not_found = 0

    print(f"{'DRY RUN — ' if dry_run else ''}CIFR IR insert: {len(_PRESS_RELEASES)} slugs to process")
    print()

    for slug, period_override in _PRESS_RELEASES:
        url = f'{_BASE}/{slug}'
        period = _period_from_url(url, period_override)
        if period is None:
            print(f"  SKIP (no period) {slug}")
            errors += 1
            continue

        print(f"  {period}  {slug}")
        if dry_run:
            skipped += 1
            continue

        # Use pre-fetched content when available
        if slug in cache:
            cached = cache[slug]
            status = cached['status']
            raw_text = cached.get('text') or ''
            if status == 404:
                print(f"    404 (cached) — not published yet, skipping")
                not_found += 1
                continue
            if status != 200 or not raw_text:
                print(f"    HTTP {status} (cached) — skipping")
                errors += 1
                continue
            fields = {'raw_text': raw_text, 'raw_html': None}
        else:
            from infra.text_utils import make_html_report_fields
            html, status = _fetch_html(session, url)
            if status == 404:
                print(f"    404 — not published yet, skipping")
                not_found += 1
                time.sleep(1)
                continue
            if status != 200 or not html:
                print(f"    HTTP {status} — skipping")
                errors += 1
                time.sleep(2)
                continue
            fields = make_html_report_fields(html)
            time.sleep(2)

        if not fields.get('raw_text'):
            print(f"    WARNING: empty raw_text after extraction")

        report = {
            'ticker':            'CIFR',
            'report_date':       period,
            'published_date':    period,
            'source_type':       'ir_press_release',
            'source_url':        url,
            'parsed_at':         now_iso,
            'extraction_status': 'pending',
            **fields,
        }

        report_id = db.insert_report(report)
        print(f"    OK  report_id={report_id}  text_len={len(fields.get('raw_text') or '')}")
        inserted += 1

    print()
    print(f"Done: {inserted} inserted, {skipped} dry-run skipped, {not_found} not found (404), {errors} errors")


if __name__ == '__main__':
    dry_run = '--dry-run' in sys.argv
    run(dry_run=dry_run)
