"""Run a single scout worker over a ticker subset."""
from __future__ import annotations

import argparse
import json
import logging
import os
from datetime import datetime, timezone
from pathlib import Path
from textwrap import dedent

import sys

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from config import CONFIG_DIR  # noqa: E402
from scrapers.observer_swarm import ScoutConfig, run_scout_batch  # noqa: E402

log = logging.getLogger("miners.run_scout_worker")

# Scout runs should not auto-sync config into DB on startup.
os.environ.setdefault("MINERS_AUTO_SYNC_COMPANIES", "0")

_PROMPT_REFERENCES = [
    Path(__file__).parent / "prompts" / "00_wire_services.md",
    Path(__file__).parent / "prompts" / "agent_B_clsk_bitf_btbt.md",
]


def _safe_read(path: Path) -> str:
    try:
        return path.read_text()
    except Exception as exc:  # noqa: BLE001
        return f"[unavailable: {path} :: {exc}]"


def _load_tickers(args) -> list[str]:
    if args.tickers:
        return sorted([x.strip().upper() for x in args.tickers.split(",") if x.strip()])
    cfg = Path(CONFIG_DIR) / "companies.json"
    rows = json.loads(cfg.read_text())
    return sorted([r["ticker"].upper() for r in rows])


def _write_prompt_artifacts(
    *,
    run_id: str,
    output_dir: Path,
    scout_id: str,
    tickers: list[str],
    max_attempts_source: int,
    max_no_yield: int,
    execute_scrape: bool,
) -> dict:
    prompts_dir = output_dir / "prompts"
    prompts_dir.mkdir(parents=True, exist_ok=True)
    refs = {str(p): _safe_read(p) for p in _PROMPT_REFERENCES}

    scout_prompt = dedent(
        f"""
        # Scout Prompt Trace
        run_id: {run_id}
        scout_id: {scout_id}
        assigned_tickers: {", ".join(tickers)}

        ## Deterministic Core
        - Source order: IR -> GlobeNewswire -> PRNewswire
        - Exhaustion gate: max_attempts_per_source={max_attempts_source}, max_consecutive_no_yield={max_no_yield}
        - Coverage gate: block if no IR source and wire sample_count == 0
        - Execute scrape: {execute_scrape}

        ## Prompt References (verbatim)
        """
    ).strip() + "\n"
    for path, body in refs.items():
        scout_prompt += f"\n### {path}\n\n{body}\n"

    scout_prompt_path = prompts_dir / f"{scout_id}_prompt_{run_id}.md"
    scout_prompt_path.write_text(scout_prompt)
    index = {
        "run_id": run_id,
        "scout_id": scout_id,
        "scout_prompt": str(scout_prompt_path),
        "generated_at": datetime.now(timezone.utc).isoformat(),
    }
    index_path = prompts_dir / f"prompt_trace_{scout_id}_{run_id}.json"
    index_path.write_text(json.dumps(index, indent=2))
    return {"index": str(index_path), "scout_prompt": str(scout_prompt_path)}


def _write_decision_trace(*, run_id: str, scout_id: str, output_dir: Path, contracts: list[dict]) -> dict:
    prompts_dir = output_dir / "prompts"
    prompts_dir.mkdir(parents=True, exist_ok=True)
    rows = []
    for c in contracts:
        rows.append(
            {
                "ticker": c.get("ticker"),
                "status": c.get("status"),
                "attempts_by_family": c.get("attempts_by_family", {}),
                "sources": [
                    {
                        "family": s.get("family"),
                        "method": s.get("discovery_method"),
                        "sample_count": (s.get("validation") or {}).get("sample_count", 0),
                        "entry_url": s.get("entry_url"),
                    }
                    for s in c.get("sources", [])
                ],
                "blockers": c.get("blockers", []),
            }
        )
    trace = {
        "run_id": run_id,
        "scout_id": scout_id,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "tickers": sorted(rows, key=lambda r: (r.get("ticker") or "")),
    }
    trace_path = prompts_dir / f"decision_trace_{scout_id}_{run_id}.json"
    trace_path.write_text(json.dumps(trace, indent=2))
    return {"path": str(trace_path)}


def main() -> None:
    parser = argparse.ArgumentParser(description="Run scout discovery/scrape worker.")
    parser.add_argument("--run-id", default=f"observer_{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}")
    parser.add_argument("--scout-id", default="scout-1")
    parser.add_argument("--tickers", help="Comma-separated ticker subset")
    parser.add_argument("--output-dir", default=".data/miners_progress")
    parser.add_argument("--max-attempts-source", type=int, default=5)
    parser.add_argument("--max-no-yield", type=int, default=3)
    parser.add_argument("--no-execute-scrape", action="store_true")
    parser.add_argument("--db-path", help="Optional explicit sqlite DB path")
    args = parser.parse_args()

    tickers = _load_tickers(args)
    rows = json.loads((Path(CONFIG_DIR) / "companies.json").read_text())
    companies_by_ticker = {r["ticker"].upper(): r for r in rows}
    cfg = ScoutConfig(
        max_attempts_per_source=max(1, args.max_attempts_source),
        max_consecutive_no_yield=max(1, args.max_no_yield),
        execute_scrape=not args.no_execute_scrape,
    )
    result = run_scout_batch(
        run_id=args.run_id,
        scout_id=args.scout_id,
        tickers=tickers,
        output_dir=Path(args.output_dir),
        config=cfg,
        db_path=args.db_path,
        companies_by_ticker=companies_by_ticker,
    )
    prompt_artifacts = _write_prompt_artifacts(
        run_id=args.run_id,
        output_dir=Path(args.output_dir),
        scout_id=args.scout_id,
        tickers=tickers,
        max_attempts_source=max(1, args.max_attempts_source),
        max_no_yield=max(1, args.max_no_yield),
        execute_scrape=not args.no_execute_scrape,
    )
    decision_artifact = _write_decision_trace(
        run_id=args.run_id,
        scout_id=args.scout_id,
        output_dir=Path(args.output_dir),
        contracts=result.get("contracts", []),
    )
    result["summary"]["prompt_trace"] = prompt_artifacts
    result["summary"]["decision_trace"] = decision_artifact
    print(json.dumps(result["summary"], indent=2))


if __name__ == "__main__":
    main()
