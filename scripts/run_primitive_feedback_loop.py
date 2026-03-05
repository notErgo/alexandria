"""Run primitive feedback loop from merged source contracts artifact."""
from __future__ import annotations

import argparse
import json
from pathlib import Path

import requests

import sys

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from scrapers.primitive_feedback import run_feedback_loop  # noqa: E402


def main() -> None:
    parser = argparse.ArgumentParser(description="Primitive feedback loop runner.")
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--output-dir", default=".data/miners_progress")
    parser.add_argument("--contracts-path", default="", help="Path to merged_source_contracts.json")
    parser.add_argument("--apply", action="store_true", help="Apply validated primitives into config registry")
    args = parser.parse_args()

    out = Path(args.output_dir)
    contracts_path = Path(args.contracts_path) if args.contracts_path else (out / "merged_source_contracts.json")
    payload = json.loads(contracts_path.read_text())
    contracts = payload.get("contracts", []) if isinstance(payload, dict) else []

    session = requests.Session()
    result = run_feedback_loop(
        run_id=args.run_id,
        output_dir=out,
        contracts=contracts,
        apply=bool(args.apply),
        session=session,
    )
    print(json.dumps({
        "run_id": args.run_id,
        "contracts_path": str(contracts_path),
        "gap_count": result.get("gap_count", 0),
        "candidate_count": result.get("candidate_count", 0),
        "artifact": result.get("artifact"),
        "apply_summary": result.get("apply_summary", {}),
    }, indent=2))


if __name__ == "__main__":
    main()
