#!/usr/bin/env python3
"""Summarize remaining ui_spec migration work for canonical components."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from review_maps_common import build_forward_trace


def build_audit() -> dict:
    forward = build_forward_trace()
    components = forward["components"]
    unmapped_canonical = [c for c in components if c["coverage_status"] == "unmapped_canonical"]
    with_endpoints = [c for c in unmapped_canonical if c.get("api_endpoints")]
    without_endpoints = [c for c in unmapped_canonical if not c.get("api_endpoints")]
    return {
        "metadata": forward["metadata"],
        "summary": {
            "display_only": sum(bool(c.get("display_only")) for c in components),
            "operation_traced": sum(c["coverage_status"] == "operation_traced" for c in components),
            "unmapped_canonical": len(unmapped_canonical),
            "unmapped_noncanonical": sum(c["coverage_status"] == "unmapped_noncanonical" for c in components),
        },
        "unmapped_canonical": {
            "with_endpoints": [
                {
                    "id": c["id"],
                    "name": c["name"],
                    "api_endpoints": c.get("api_endpoints", []),
                }
                for c in with_endpoints
            ],
            "without_endpoints": [
                {
                    "id": c["id"],
                    "name": c["name"],
                }
                for c in without_endpoints
            ],
        },
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--write", help="Write JSON output to a file instead of stdout.")
    args = parser.parse_args()

    audit = build_audit()
    payload = json.dumps(audit, indent=2)
    if args.write:
        Path(args.write).write_text(payload + "\n", encoding="utf-8")
        return
    print(payload)


if __name__ == "__main__":
    main()
