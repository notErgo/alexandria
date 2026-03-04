"""
Hydrate companies DB from config/companies.json.

For companies already in the DB (original 13): PUT /api/companies/<ticker>
For new companies: POST /api/companies

Usage (server must be running at localhost:5004):
    python3 scripts/hydrate_companies.py
    python3 scripts/hydrate_companies.py --dry-run
    python3 scripts/hydrate_companies.py --new-only
"""
import argparse
import json
import sys
from pathlib import Path

try:
    import requests
except ImportError:
    print("ERROR: requests not installed. Run: pip install requests")
    sys.exit(1)

BASE = "http://localhost:5004"

ORIGINAL_13 = {
    "MARA", "RIOT", "CLSK", "CORZ", "BITF", "BTBT", "CIFR",
    "HIVE", "HUT8", "ARBK", "SDIG", "WULF", "IREN",
}


def main():
    parser = argparse.ArgumentParser(description="Hydrate companies DB from companies.json")
    parser.add_argument("--dry-run", action="store_true", help="Print actions without making requests")
    parser.add_argument("--new-only", action="store_true", help="Only POST new companies, skip PUT for existing")
    args = parser.parse_args()

    config_path = Path(__file__).parent.parent / "config" / "companies.json"
    with open(config_path) as f:
        companies = json.load(f)

    print(f"Loaded {len(companies)} companies from {config_path}")

    ok = 0
    failed = 0

    for co in companies:
        ticker = co["ticker"]
        is_existing = ticker in ORIGINAL_13

        if args.new_only and is_existing:
            print(f"  SKIP  {ticker} (--new-only, existing entry)")
            continue

        action = "PUT" if is_existing else "POST"
        url = f"{BASE}/api/companies/{ticker}" if is_existing else f"{BASE}/api/companies"

        print(f"  {action:4s}  {ticker:6s}  {co.get('name', '')[:50]}", end="  ")

        if args.dry_run:
            print("(dry-run)")
            ok += 1
            continue

        try:
            resp = requests.request(action, url, json=co, timeout=10)
            data = resp.json() if resp.content else {}
            if resp.ok:
                print(f"OK ({resp.status_code})")
                ok += 1
            else:
                err_msg = data.get("error", {}).get("message", data.get("error", resp.text[:80]))
                print(f"FAIL ({resp.status_code}) {err_msg}")
                failed += 1
        except requests.RequestException as e:
            print(f"ERROR {e}")
            failed += 1

    print(f"\nDone: {ok} ok, {failed} failed")
    if failed:
        sys.exit(1)


if __name__ == "__main__":
    main()
