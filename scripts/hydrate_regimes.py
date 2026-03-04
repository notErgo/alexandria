"""
Hydrate regime windows from CODEX_REGIME_OUTPUT.json.

POSTs each regime window to POST /api/regime/<ticker>.
Regime windows define the reporting schedule used by the coverage grid to
distinguish "expected gap" from "missing data."

Usage (server must be running at localhost:5004):
    python3 scripts/hydrate_regimes.py
    python3 scripts/hydrate_regimes.py --dry-run
    python3 scripts/hydrate_regimes.py --ticker BTDR   # one ticker only
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


def main():
    parser = argparse.ArgumentParser(description="POST regime windows to running server")
    parser.add_argument("--dry-run", action="store_true", help="Print without making requests")
    parser.add_argument("--ticker", help="Restrict to one ticker")
    args = parser.parse_args()

    regime_path = Path(__file__).parent.parent / "CODEX_REGIME_OUTPUT.json"
    with open(regime_path) as f:
        windows = json.load(f)

    if args.ticker:
        windows = [w for w in windows if w["ticker"] == args.ticker.upper()]
        if not windows:
            print(f"No regime windows found for ticker {args.ticker.upper()}")
            sys.exit(1)

    print(f"Loaded {len(windows)} regime windows from {regime_path}")

    ok = 0
    failed = 0

    for w in windows:
        ticker = w["ticker"]
        payload = {
            "cadence": w["cadence"],
            "start_date": w["start_date"],
            "end_date": w.get("end_date"),
            "notes": w.get("notes", ""),
        }
        end_label = w.get("end_date") or "ongoing"
        print(f"  POST  {ticker:6s}  {w['cadence']:9s}  {w['start_date']} -> {end_label}", end="  ")

        if args.dry_run:
            print("(dry-run)")
            ok += 1
            continue

        try:
            resp = requests.post(f"{BASE}/api/regime/{ticker}", json=payload, timeout=10)
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
