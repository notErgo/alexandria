"""
Seed metric_schema and metric keywords for the BTC-miners sector.

Run once (or re-run safely — existing rows are skipped):
  cd OffChain/miners
  source venv/bin/activate
  python3 scripts/seed_metrics.py
"""
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from infra.db import MinerDB
from config import DATA_DIR
from pathlib import Path

# Each entry: (key, label, unit, display_order, keywords_pipe_separated)
# Keywords are pipe-separated; empty strings are skipped.
METRICS = [
    (
        'production_btc', 'Production', 'BTC', 10,
        'produced|production|in the month of|self-mined|btc production|btc produced'
    ),
    (
        'hashrate_eh', 'Hashrate', 'EH/s', 20,
        'exahash|EH/s|hashrate|hash rate|energized hashrate'
    ),
    (
        'holdings_btc', 'Holdings', 'BTC', 30,
        'Total Holdings|BTC holdings|total holdings|treasury'
    ),
    (
        'unrestricted_holdings', 'Unrestricted Holdings', 'BTC', 40,
        'Unrestricted BTC|unrestricted holdings|unrestricted bitcoin'
    ),
    (
        'sales_btc', 'Sales', 'BTC', 50,
        'BTC sold|BTC sales|elected to sell|selling|sales|may sell'
    ),
    (
        'restricted_holdings_btc', 'Restricted Holdings', 'BTC', 60,
        'BTC collateral|encumbered BTC|encumbered bitcoin|restricted BTC|restricted bitcoin'
    ),
    (
        'realization_rate', 'Realization Rate', 'ratio', 70,
        'realization rate|realized price|BTC per dollar'
    ),
    (
        'net_btc_balance_change', 'Net BTC Balance Change', 'BTC', 80,
        'net bitcoin|net BTC change|net change in bitcoin'
    ),
]


def main():
    db_path = str(Path(DATA_DIR) / 'minerdata.db')
    db = MinerDB(db_path)

    for key, label, unit, display_order, kw_pipe in METRICS:
        # Add metric row (skip if already exists)
        existing = db.get_metric_schema('BTC-miners', active_only=False)
        if any(r['key'] == key for r in existing):
            print(f'SKIP metric {key}: already exists')
        else:
            row = db.add_analyst_metric(key, label, unit, 'BTC-miners')
            print(f'ADD  metric {key}: id={row["id"]}')

        # Set display_order via direct DB update (add_analyst_metric defaults to 999)
        import sqlite3
        with db._get_connection() as conn:
            conn.execute(
                "UPDATE metric_schema SET display_order = ? WHERE key = ? AND sector = 'BTC-miners'",
                (display_order, key),
            )

        # Add keywords (skip duplicates)
        phrases = [p.strip() for p in kw_pipe.split('|') if p.strip()]
        for phrase in phrases:
            try:
                kw_id = db.add_metric_keyword(key, phrase)
                print(f'  ADD  keyword [{key}] {phrase!r}: id={kw_id}')
            except Exception as e:
                if 'UNIQUE' in str(e) or 'already exists' in str(e):
                    print(f'  SKIP keyword [{key}] {phrase!r}: duplicate')
                else:
                    print(f'  ERR  keyword [{key}] {phrase!r}: {e}')

    print('\nDone. Restart the Flask server to pick up the new schema.')


if __name__ == '__main__':
    main()
