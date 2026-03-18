"""Fix off-by-one report_date for WULF and FUFU IR press release reports.

Problem
-------
IR press release reports for WULF and FUFU were ingested with report_date set
to the RSS/index announcement date (e.g. 2025-01-03) rather than the first day
of the month the report covers (e.g. 2024-12-01 for a December production PR).

The scraper fix in commit 3e8a360 (_apply_body_period_correction) prevents this
for new fetches, but near-duplicate detection blocks re-ingestion of the 38
already-stored reports, so their wrong dates persist.

Effect
------
- data_points.period = announcement date (e.g. 2025-01-03) — phantom rows
- Gap-fill accidentally created correct first-of-month rows alongside them
- Coverage grid has duplicate phantom periods

Fix applied by this script
--------------------------
For each of the 38 off-by-one reports:
  1. Parse the correct YYYY-MM-01 from the URL slug (month name + year).
  2. DELETE data_points where report_id = that report AND period = old wrong date.
  3. UPDATE reports.report_date = correct first-of-month.
  4. UPDATE reports.extraction_status = 'pending' so re-extraction regenerates
     clean data_points at the correct period (existing gap-fill rows at the
     correct period survive and will be refreshed in-place on re-extraction).

Usage
-----
    python3 scripts/fix_ir_period_dates.py [--dry-run]

Flags
-----
  --dry-run   Print planned changes without writing anything to the DB.
"""

import re
import sys
from datetime import date
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / 'src'))

from config import DATA_DIR
from infra.db import MinerDB

_MONTH_MAP = {
    'january': 1, 'february': 2, 'march': 3, 'april': 4,
    'may': 5, 'june': 6, 'july': 7, 'august': 8,
    'september': 9, 'october': 10, 'november': 11, 'december': 12,
}

# Single pattern: match `announces-{month_name}-{year}` where month_name is a
# known English month.  Suffix after the year is not constrained — this handles
# variants like "-production", "-bitcoin-mining", and "-leap-month".
_URL_PATTERN = re.compile(
    r'announces-([a-zA-Z]+)-(\d{4})(?:-|$)',
    re.IGNORECASE,
)


def _correct_period_from_url(url: str) -> str | None:
    """Return 'YYYY-MM-01' inferred from the month name in the URL slug, or None."""
    for m in _URL_PATTERN.finditer(url):
        month_name = m.group(1).lower()
        year = int(m.group(2))
        month = _MONTH_MAP.get(month_name)
        if month:
            return date(year, month, 1).strftime('%Y-%m-%d')
    return None


def _fetch_off_by_one_reports(conn) -> list[dict]:
    """Return WULF and FUFU ir_press_release reports where report_date is not first-of-month."""
    rows = conn.execute(
        """
        SELECT id, ticker, report_date, source_url
        FROM reports
        WHERE ticker IN ('WULF', 'FUFU')
          AND source_type = 'ir_press_release'
          AND report_date NOT LIKE '%-01'
        ORDER BY ticker, report_date
        """
    ).fetchall()
    return [
        {'id': r[0], 'ticker': r[1], 'report_date': r[2], 'source_url': r[3]}
        for r in rows
    ]


def run(dry_run: bool = False) -> None:
    db_path = str(Path(DATA_DIR) / 'minerdata.db')
    db = MinerDB(db_path)

    with db._get_connection() as conn:
        reports = _fetch_off_by_one_reports(conn)

    print(f"Found {len(reports)} off-by-one reports to correct.")
    print()

    corrections = []
    skipped = []
    for r in reports:
        correct = _correct_period_from_url(r['source_url'])
        if correct is None:
            skipped.append(r)
            continue
        if correct == r['report_date']:
            # Already correct — shouldn't happen given the query, but be safe.
            continue
        corrections.append({**r, 'correct_date': correct})

    if skipped:
        print(f"WARNING: could not parse correct period for {len(skipped)} reports:")
        for r in skipped:
            print(f"  [{r['ticker']}] {r['report_date']}  {r['source_url']}")
        print()

    if not corrections:
        print("Nothing to fix.")
        return

    print(f"{'DRY RUN — ' if dry_run else ''}Corrections to apply ({len(corrections)}):")
    print(f"  {'TICKER':<6}  {'OLD DATE':<12}  {'NEW DATE':<12}  URL-SLUG")
    for c in corrections:
        slug = c['source_url'].rsplit('/', 1)[-1][:60]
        print(f"  {c['ticker']:<6}  {c['report_date']:<12}  {c['correct_date']:<12}  {slug}")

    if dry_run:
        print("\nDry run — no changes written.")
        return

    print()
    total_dp_deleted = 0
    total_reports_updated = 0

    with db._get_connection() as conn:
        for c in corrections:
            report_id = c['id']
            old_date = c['report_date']
            new_date = c['correct_date']

            # Count data_points at the wrong date before deleting.
            dp_count = conn.execute(
                "SELECT COUNT(*) FROM data_points WHERE report_id=? AND period=?",
                (report_id, old_date),
            ).fetchone()[0]

            if not dry_run:
                conn.execute(
                    "DELETE FROM data_points WHERE report_id=? AND period=?",
                    (report_id, old_date),
                )
                conn.execute(
                    "UPDATE reports SET report_date=?, extraction_status='pending' WHERE id=?",
                    (new_date, report_id),
                )

            total_dp_deleted += dp_count
            total_reports_updated += 1
            print(
                f"  [{c['ticker']}] report {report_id}: {old_date} -> {new_date}"
                f"  (deleted {dp_count} wrong-period data_points)"
            )

    print()
    print(f"Done. {total_reports_updated} reports corrected, {total_dp_deleted} phantom data_points deleted.")
    print("Run the LLM extract pipeline to regenerate data_points at correct periods.")


if __name__ == '__main__':
    dry_run = '--dry-run' in sys.argv
    run(dry_run=dry_run)
