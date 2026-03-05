"""Run observer-led scout swarm across config tickers."""
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
from scrapers.observer_swarm import ScoutConfig, run_observer  # noqa: E402

log = logging.getLogger("miners.run_observer_swarm")

# Swarm runs should not auto-sync config into DB on startup.
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


def _write_prompt_artifacts(
    *,
    run_id: str,
    output_dir: Path,
    tickers: list[str],
    scout_count: int,
    max_attempts_source: int,
    max_no_yield: int,
    execute_scrape: bool,
    scouts: list[dict],
) -> dict:
    prompts_dir = output_dir / "prompts"
    prompts_dir.mkdir(parents=True, exist_ok=True)

    refs = {str(p): _safe_read(p) for p in _PROMPT_REFERENCES}

    observer_prompt = dedent(
        f"""
        # Observer Prompt Trace
        run_id: {run_id}
        objective: Discover + validate PRNewswire/GlobeNewswire/IR source schema and execute scraping where allowed.
        tickers: {", ".join(tickers)}
        scout_count: {scout_count}

        ## Deterministic Core
        - Source order: IR -> GlobeNewswire -> PRNewswire
        - Exhaustion gate: max_attempts_per_source={max_attempts_source}, max_consecutive_no_yield={max_no_yield}
        - Coverage gate: block if no IR source and wire sample_count == 0
        - Execute scrape: {execute_scrape}

        ## Prompt References (verbatim)
        """
    ).strip() + "\n"
    for path, body in refs.items():
        observer_prompt += f"\n### {path}\n\n{body}\n"

    observer_prompt_path = prompts_dir / f"observer_prompt_{run_id}.md"
    observer_prompt_path.write_text(observer_prompt)

    scout_paths = []
    for scout in scouts:
        scout_id = scout.get("scout_id", "scout-unknown")
        scout_tickers = scout.get("tickers", [])
        scout_prompt = dedent(
            f"""
            # Scout Prompt Trace
            run_id: {run_id}
            scout_id: {scout_id}
            assigned_tickers: {", ".join(scout_tickers)}

            ## Execution Rules
            - Use deterministic source order and contracts.
            - Respect exhaustion + coverage gates.
            - Emit evidence URLs and structured blockers.
            - Do not silently skip a source family.
            """
        ).strip() + "\n"
        for path, body in refs.items():
            scout_prompt += f"\n### {path}\n\n{body}\n"
        path = prompts_dir / f"{scout_id}_prompt_{run_id}.md"
        path.write_text(scout_prompt)
        scout_paths.append(str(path))

    index = {
        "run_id": run_id,
        "observer_prompt": str(observer_prompt_path),
        "scout_prompts": scout_paths,
        "generated_at": datetime.now(timezone.utc).isoformat(),
    }
    index_path = prompts_dir / f"prompt_trace_{run_id}.json"
    index_path.write_text(json.dumps(index, indent=2))
    return {"index": str(index_path), "observer_prompt": str(observer_prompt_path), "scout_prompts": scout_paths}


def _write_decision_trace(*, run_id: str, output_dir: Path, merged_contracts_path: str) -> dict:
    path = Path(merged_contracts_path)
    trace_path = output_dir / "prompts" / f"decision_trace_{run_id}.json"
    try:
        payload = json.loads(path.read_text())
    except Exception as exc:  # noqa: BLE001
        trace = {
            "run_id": run_id,
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "error": f"failed_to_read_merged_contracts:{exc}",
            "merged_contracts_path": str(path),
            "tickers": [],
        }
        trace_path.write_text(json.dumps(trace, indent=2))
        return {"path": str(trace_path)}

    contracts = payload.get("contracts", []) if isinstance(payload, dict) else []
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
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "merged_contracts_path": str(path),
        "tickers": sorted(rows, key=lambda r: (r.get("ticker") or "")),
    }
    trace_path.write_text(json.dumps(trace, indent=2))
    return {"path": str(trace_path)}


def main() -> None:
    parser = argparse.ArgumentParser(description="Observer/scout discovery and scraping run.")
    parser.add_argument("--run-id", default=f"observer_{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}")
    parser.add_argument("--output-dir", default=".data/miners_progress")
    parser.add_argument("--scout-count", type=int, default=4)
    parser.add_argument("--tickers", help="Comma-separated tickers. Defaults to all config tickers.")
    parser.add_argument("--max-attempts-source", type=int, default=5)
    parser.add_argument("--max-no-yield", type=int, default=3)
    parser.add_argument("--no-execute-scrape", action="store_true")
    parser.add_argument("--no-feedback-loop", action="store_true")
    parser.add_argument("--apply-validated-primitives", action="store_true")
    parser.add_argument("--db-path", help="Optional explicit sqlite DB path")
    args = parser.parse_args()

    rows = json.loads((Path(CONFIG_DIR) / "companies.json").read_text())
    companies_by_ticker = {r["ticker"].upper(): r for r in rows}
    if args.tickers:
        tickers = sorted([x.strip().upper() for x in args.tickers.split(",") if x.strip()])
    else:
        tickers = sorted([r["ticker"].upper() for r in rows])

    cfg = ScoutConfig(
        max_attempts_per_source=max(1, args.max_attempts_source),
        max_consecutive_no_yield=max(1, args.max_no_yield),
        execute_scrape=not args.no_execute_scrape,
        run_feedback_loop=not args.no_feedback_loop,
        apply_validated_primitives=bool(args.apply_validated_primitives),
    )
    summary = run_observer(
        run_id=args.run_id,
        tickers=tickers,
        scout_count=max(1, args.scout_count),
        output_dir=Path(args.output_dir),
        config=cfg,
        db_path=args.db_path,
        companies_by_ticker=companies_by_ticker,
    )
    prompt_artifacts = _write_prompt_artifacts(
        run_id=args.run_id,
        output_dir=Path(args.output_dir),
        tickers=tickers,
        scout_count=max(1, args.scout_count),
        max_attempts_source=max(1, args.max_attempts_source),
        max_no_yield=max(1, args.max_no_yield),
        execute_scrape=not args.no_execute_scrape,
        scouts=summary.get("scouts", []),
    )
    decision_artifact = _write_decision_trace(
        run_id=args.run_id,
        output_dir=Path(args.output_dir),
        merged_contracts_path=summary.get("artifacts", {}).get("merged_source_contracts", ""),
    )
    summary["prompt_trace"] = prompt_artifacts
    summary["decision_trace"] = decision_artifact
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
