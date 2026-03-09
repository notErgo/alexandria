#!/usr/bin/env python3
"""
DAG validator for OffChain/miners module dependency graph.

Usage:
    python3 scripts/check_dag.py [path/to/dag.json]
    python3 scripts/check_dag.py --strict       # known violations also count as errors

Checks performed:
    1. Broken references   — edge endpoints not listed in nodes
    2. Cycle detection     — topological sort; any cycle is fatal
    3. Upward dependency   — from.layer < to.layer (lower-level importing higher-level)
    4. Same-layer coupling — from.layer == to.layer == 5 (route importing route)
    5. Skip-layer import   — from.layer - to.layer >= 3 (bypasses abstraction tiers)
    6. Unreachable nodes   — no path from any L5 route (potential dead code)
    7. Leaf nodes          — no outgoing edges beyond L0 (informational)

Violation status:
    Edges with a "notes" field are treated as *known/documented* violations.
    Unknown violations (no notes) always cause exit 1.
    Known violations cause exit 1 only with --strict.

Exit codes:
    0 — pass (no unknown violations)
    1 — one or more unknown violations, or --strict with any violation
    2 — usage error / file not found
"""
import json
import sys
from collections import defaultdict, deque
from pathlib import Path


# ── Colours (suppressed when not a TTY) ──────────────────────────────────────

def _supports_colour():
    return hasattr(sys.stdout, 'isatty') and sys.stdout.isatty()

_RED    = '\033[31m' if _supports_colour() else ''
_YELLOW = '\033[33m' if _supports_colour() else ''
_GREEN  = '\033[32m' if _supports_colour() else ''
_CYAN   = '\033[36m' if _supports_colour() else ''
_DIM    = '\033[2m'  if _supports_colour() else ''
_RESET  = '\033[0m'  if _supports_colour() else ''


# ── Loading ───────────────────────────────────────────────────────────────────

def load_dag(path: Path) -> dict:
    with open(path) as f:
        return json.load(f)


def build_graph(data: dict):
    nodes = {n['id']: n for n in data['nodes']}
    edges = data['edges']
    adj = defaultdict(list)          # node_id → [edge, ...]
    for e in edges:
        adj[e['from']].append(e)
    return nodes, edges, adj


# ── Check 1: Broken references ────────────────────────────────────────────────

def check_broken_refs(nodes: dict, edges: list) -> list:
    findings = []
    for e in edges:
        if e['from'] not in nodes:
            findings.append(f"edge references unknown source: {e['from']!r}")
        if e['to'] not in nodes:
            findings.append(f"edge references unknown target: {e['to']!r}")
    return findings


# ── Check 2: Cycle detection (Kahn's algorithm) ───────────────────────────────

def topological_sort(nodes: dict, adj: dict):
    """Return (sorted_list, cycle_members). cycle_members is [] when no cycle."""
    in_degree = {n: 0 for n in nodes}
    for edges in adj.values():
        for e in edges:
            if e['to'] in in_degree:
                in_degree[e['to']] += 1

    queue = deque(n for n, d in in_degree.items() if d == 0)
    result = []
    while queue:
        node = queue.popleft()
        result.append(node)
        for e in adj.get(node, []):
            to = e['to']
            if to in in_degree:
                in_degree[to] -= 1
                if in_degree[to] == 0:
                    queue.append(to)

    cycle_nodes = sorted(n for n in nodes if n not in set(result))
    return result, cycle_nodes


# ── Check 3-5: Edge violation checks ─────────────────────────────────────────

def check_edges(nodes: dict, edges: list) -> dict:
    upward      = []   # from.layer < to.layer
    same_l5     = []   # from.layer == to.layer == 5
    skip_layer  = []   # from.layer - to.layer >= 3

    for e in edges:
        frm = nodes.get(e['from'])
        to  = nodes.get(e['to'])
        if not frm or not to:
            continue

        fl = frm['layer']
        tl = to['layer']
        is_known = bool(e.get('notes'))

        if fl < tl:
            upward.append({**e, 'from_layer': fl, 'to_layer': tl, 'known': is_known})

        if fl == tl == 5:
            same_l5.append({**e, 'known': is_known})

        # Skip-layer: from.layer - to.layer >= 3, excluding:
        #   - L0 targets (config, miner_types) — pure constants, fine to import from anywhere
        #   - from.layer == 4 (app_globals) — its purpose is to wire L1/L2 into the singleton layer
        gap = fl - tl
        if gap >= 3 and tl > 0 and fl != 4:
            skip_layer.append({**e, 'from_layer': fl, 'to_layer': tl, 'gap': gap, 'known': is_known})

    return {'upward': upward, 'same_l5': same_l5, 'skip_layer': skip_layer}


# ── Check 6: Unreachable nodes ────────────────────────────────────────────────

def find_unreachable(nodes: dict, adj: dict) -> list:
    """Return nodes with no path from any L5 route."""
    l5_nodes = [n for n, info in nodes.items() if info['layer'] == 5]

    visited = set()
    queue = deque(l5_nodes)
    while queue:
        node = queue.popleft()
        if node in visited:
            continue
        visited.add(node)
        for e in adj.get(node, []):
            queue.append(e['to'])

    return sorted(n for n in nodes if n not in visited and nodes[n]['layer'] > 0)


# ── Check 7: Leaf nodes ───────────────────────────────────────────────────────

def find_leaves(nodes: dict, adj: dict) -> list:
    """Return nodes above L0 whose only outgoing edges (if any) go to L0."""
    leaves = []
    for node_id, info in nodes.items():
        if info['layer'] == 0:
            continue
        out_edges = adj.get(node_id, [])
        non_l0_targets = [
            e for e in out_edges
            if nodes.get(e['to'], {}).get('layer', 0) > 0
        ]
        if not non_l0_targets:
            leaves.append(node_id)
    return sorted(leaves)


# ── Formatting helpers ────────────────────────────────────────────────────────

def _edge_str(e: dict) -> str:
    notes = f"  {_DIM}[{e['notes']}]{_RESET}" if e.get('notes') else ''
    return f"  {e['from']} -> {e['to']}{notes}"


def _status(label: str, count: int, unknown: int, colour: str) -> str:
    if count == 0:
        return f"{_GREEN}[PASS]{_RESET} {label}: 0"
    known = count - unknown
    parts = []
    if unknown:
        parts.append(f"{unknown} unknown")
    if known:
        parts.append(f"{known} known")
    detail = ', '.join(parts)
    tag = f"{_RED}[FAIL]{_RESET}" if unknown else f"{_YELLOW}[WARN]{_RESET}"
    return f"{tag} {colour}{label}{_RESET}: {count} ({detail})"


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    strict = '--strict' in sys.argv
    args   = [a for a in sys.argv[1:] if not a.startswith('--')]

    dag_path = Path(args[0]) if args else Path(__file__).parent.parent / 'docs/architecture/dag.json'

    if not dag_path.exists():
        print(f"check_dag: file not found: {dag_path}", file=sys.stderr)
        sys.exit(2)

    data = load_dag(dag_path)
    nodes, edges, adj = build_graph(data)

    print(f"\ncheck_dag: {dag_path}")
    print(f"  {len(nodes)} nodes  |  {len(edges)} edges  |  generated {data['meta'].get('generated','?')}\n")

    errors   = 0
    warnings = 0

    # 1. Broken references
    broken = check_broken_refs(nodes, edges)
    if broken:
        print(f"{_RED}[FAIL]{_RESET} Broken edge references: {len(broken)}")
        for b in broken:
            print(f"  {b}")
        errors += len(broken)
    else:
        print(f"{_GREEN}[PASS]{_RESET} Broken edge references: 0")

    # 2. Cycle detection
    sorted_nodes, cycle_members = topological_sort(nodes, adj)
    if cycle_members:
        print(f"{_RED}[FAIL]{_RESET} Cycles detected — topological sort failed")
        print(f"  Nodes in cycle(s): {', '.join(cycle_members)}")
        errors += 1
    else:
        print(f"{_GREEN}[PASS]{_RESET} Cycle check: no cycles ({len(sorted_nodes)} nodes sorted)")

    # 3-5. Edge violations
    violations = check_edges(nodes, edges)

    up = violations['upward']
    up_unknown = sum(1 for v in up if not v['known'])
    print(_status("Upward dependencies (from.layer < to.layer)", len(up), up_unknown, _RED))
    for v in up:
        flag = '' if v['known'] else f"  {_RED}** NEW **{_RESET}"
        print(f"{_edge_str(v)}  L{v['from_layer']}->L{v['to_layer']}{flag}")
    if up:
        errors += up_unknown
        if strict:
            warnings += len(up) - up_unknown

    sl5 = violations['same_l5']
    sl5_unknown = sum(1 for v in sl5 if not v['known'])
    print(_status("Same-layer L5→L5 coupling", len(sl5), sl5_unknown, _YELLOW))
    for v in sl5:
        flag = f"  {_RED}** NEW **{_RESET}" if not v['known'] else ''
        print(_edge_str(v) + flag)
    if sl5:
        errors += sl5_unknown
        warnings += len(sl5) - sl5_unknown

    skip = violations['skip_layer']
    skip_unknown = sum(1 for v in skip if not v['known'])
    print(_status("Skip-layer imports (gap >= 3 layers)", len(skip), skip_unknown, _YELLOW))
    for v in skip:
        flag = f"  {_RED}** NEW **{_RESET}" if not v['known'] else ''
        print(f"{_edge_str(v)}  L{v['from_layer']}->L{v['to_layer']} (gap={v['gap']}){flag}")
    if skip:
        errors += skip_unknown
        warnings += len(skip) - skip_unknown

    # 6. Unreachable nodes
    unreachable = find_unreachable(nodes, adj)
    tag = f"{_CYAN}[INFO]{_RESET}"
    print(f"{tag} Unreachable from L5 (not imported by any route): {len(unreachable)}")
    for n in unreachable:
        layer = nodes[n]['layer']
        note  = nodes[n].get('notes', '')
        suffix = f"  {_DIM}L{layer}  {note}{_RESET}" if note else f"  {_DIM}L{layer}{_RESET}"
        print(f"  {n}{suffix}")

    # 7. Leaf nodes
    leaves = find_leaves(nodes, adj)
    print(f"{tag} Leaf nodes (no outgoing edges beyond L0): {len(leaves)}")
    for n in leaves:
        layer = nodes[n]['layer']
        print(f"  {n}  {_DIM}L{layer}{_RESET}")

    # Summary
    print()
    if strict and warnings:
        errors += warnings
        warnings = 0

    if errors == 0 and warnings == 0:
        print(f"{_GREEN}PASS{_RESET}  no violations")
        sys.exit(0)
    elif errors == 0:
        print(f"{_YELLOW}PASS{_RESET}  {warnings} known violation(s) — use --strict to fail on these")
        sys.exit(0)
    else:
        unknown_total = errors - (warnings if not strict else 0)
        print(f"{_RED}FAIL{_RESET}  {errors} error(s), {warnings} warning(s)")
        sys.exit(1)


if __name__ == '__main__':
    main()
