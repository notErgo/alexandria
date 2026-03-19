#!/usr/bin/env python3
"""Generate DAG node -> UI/operation/CLI reverse trace mappings."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from review_maps_common import build_reverse_trace


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--write", help="Write JSON output to a file instead of stdout.")
    args = parser.parse_args()

    trace = build_reverse_trace()
    payload = json.dumps(trace, indent=2)
    if args.write:
        out_path = Path(args.write)
        out_path.write_text(payload + "\n", encoding="utf-8")
        return
    print(payload)


if __name__ == "__main__":
    main()
