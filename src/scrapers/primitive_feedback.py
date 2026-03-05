"""Automated primitive feedback loop with deterministic validation gates."""
from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

import requests
from bs4 import BeautifulSoup

from scrapers.primitive_registry import load_primitives, registry_path, save_primitives


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def collect_primitive_gaps(contracts: list[dict[str, Any]]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for c in contracts:
        ticker = str(c.get("ticker") or "").strip().upper()
        for g in (c.get("primitive_gaps") or []):
            if not isinstance(g, dict):
                continue
            row = dict(g)
            row.setdefault("ticker", ticker)
            out.append(row)
    # de-dup by ticker+family+kind+entry_url
    seen: set[str] = set()
    uniq: list[dict[str, Any]] = []
    for g in out:
        key = "|".join([
            str(g.get("ticker") or ""),
            str(g.get("family") or ""),
            str(g.get("kind") or ""),
            str(g.get("entry_url") or ""),
        ])
        if key in seen:
            continue
        seen.add(key)
        uniq.append(g)
    return uniq


def _slug_host(host: str) -> str:
    return "".join(ch if ch.isalnum() else "_" for ch in (host or "").lower()).strip("_") or "unknown_host"


def propose_candidates(gaps: list[dict[str, Any]]) -> list[dict[str, Any]]:
    candidates: list[dict[str, Any]] = []
    for g in gaps:
        kind = str(g.get("kind") or "").strip().lower()
        family = str(g.get("family") or "").strip().lower()
        entry_url = str(g.get("entry_url") or "").strip()
        if family != "ir" or not entry_url:
            continue
        parsed = urlparse(entry_url)
        host = parsed.netloc.lower()
        path = parsed.path or "/"

        if kind == "year_filter_widget":
            select_name = str(g.get("select_name") or "").strip()
            widget_param = str(g.get("widget_param") or "").strip()
            years = [str(y).strip() for y in (g.get("year_hints") or []) if str(y).strip()]
            if not select_name:
                continue
            if not years:
                now = datetime.now(timezone.utc).year
                years = [str(now), str(now - 1), str(now - 2)]
            base_query = {"form_id": "widget_form_base", "op": "Filter"}
            if widget_param:
                base_query[widget_param] = ""
            candidates.append({
                "id": f"primitive_{_slug_host(host)}_year_filter_query_v1",
                "family": "ir",
                "strategy": "year_filter_query",
                "active": False,
                "match": {"host": host, "path_prefix": path},
                "params": {
                    "select_name": select_name,
                    "years": years,
                    "base_query": base_query,
                },
                "source": {
                    "kind": kind,
                    "ticker": g.get("ticker"),
                    "entry_url": entry_url,
                    "run_id": g.get("run_id"),
                },
                "created_at": _utc_now(),
            })
    # de-dup by id
    uniq: dict[str, dict[str, Any]] = {}
    for c in candidates:
        uniq[c["id"]] = c
    return list(uniq.values())


def _count_production_links(html: str) -> int:
    include = ("production", "operations update", "operational update", "bitcoin")
    exclude = ("earnings", "10-q", "10-k", "financial results")
    soup = BeautifulSoup(html or "", "lxml")
    count = 0
    for a in soup.find_all("a", href=True):
        txt = (a.get_text(" ", strip=True) or "").lower()
        if not txt:
            continue
        if any(k in txt for k in include) and not any(k in txt for k in exclude):
            count += 1
    return count


def validate_candidate(candidate: dict[str, Any], *, session: requests.Session | None = None, timeout: int = 15) -> dict[str, Any]:
    strategy = str(candidate.get("strategy") or "").strip().lower()
    if strategy != "year_filter_query":
        return {"passed": False, "checks": [{"name": "strategy", "passed": False, "detail": "unsupported strategy"}]}

    params = candidate.get("params") or {}
    select_name = str(params.get("select_name") or "").strip()
    years = [str(y).strip() for y in (params.get("years") or []) if str(y).strip()]
    if not select_name or not years:
        return {"passed": False, "checks": [{"name": "schema", "passed": False, "detail": "missing select_name/years"}]}

    src = candidate.get("source") or {}
    entry_url = str(src.get("entry_url") or "").strip()
    if not entry_url:
        return {"passed": False, "checks": [{"name": "source", "passed": False, "detail": "missing entry_url"}]}

    s = session or requests.Session()
    parsed = urlparse(entry_url)
    base_query = dict((params.get("base_query") or {}))
    checks: list[dict[str, Any]] = []
    sample_total = 0

    from urllib.parse import urlencode, urlunparse  # local import keeps module surface small

    for y in years[:2]:
        q = dict(base_query)
        q[select_name] = y
        u = urlunparse((parsed.scheme, parsed.netloc, parsed.path, "", urlencode(q, doseq=True), ""))
        try:
            r = s.get(u, timeout=timeout, allow_redirects=True)
        except requests.RequestException as exc:
            checks.append({"name": f"fetch_{y}", "passed": False, "detail": f"network_error:{exc}"})
            continue
        if r.status_code >= 400:
            checks.append({"name": f"fetch_{y}", "passed": False, "detail": f"http_{r.status_code}"})
            continue
        c = _count_production_links(r.text or "")
        sample_total += c
        checks.append({"name": f"fetch_{y}", "passed": True, "detail": f"status={r.status_code}, samples={c}"})

    passed = any(ch["passed"] for ch in checks)
    if sample_total > 0:
        passed = True
    return {"passed": passed, "checks": checks, "sample_total": sample_total}


def apply_valid_candidates(candidates: list[dict[str, Any]], validations: dict[str, dict[str, Any]]) -> dict[str, Any]:
    existing = load_primitives()
    by_id = {str(x.get("id") or ""): dict(x) for x in existing if isinstance(x, dict)}
    applied: list[str] = []
    skipped: list[str] = []
    for c in candidates:
        cid = str(c.get("id") or "")
        v = validations.get(cid) or {}
        if not v.get("passed", False):
            skipped.append(cid)
            continue
        row = dict(c)
        row["active"] = True
        row["validated_at"] = _utc_now()
        row["validation"] = v
        by_id[cid] = row
        applied.append(cid)
    merged = sorted(by_id.values(), key=lambda x: str(x.get("id") or ""))
    save_primitives(merged)
    return {"applied": applied, "skipped": skipped, "registry_path": str(registry_path())}


def run_feedback_loop(
    *,
    run_id: str,
    output_dir: Path,
    contracts: list[dict[str, Any]],
    apply: bool = False,
    session: requests.Session | None = None,
) -> dict[str, Any]:
    output_dir.mkdir(parents=True, exist_ok=True)
    gaps = collect_primitive_gaps(contracts)
    candidates = propose_candidates(gaps)
    validations: dict[str, dict[str, Any]] = {}
    for c in candidates:
        validations[str(c.get("id") or "")] = validate_candidate(c, session=session)

    apply_summary = {"applied": [], "skipped": [], "registry_path": str(registry_path())}
    if apply:
        apply_summary = apply_valid_candidates(candidates, validations)

    artifact = {
        "run_id": run_id,
        "generated_at": _utc_now(),
        "gap_count": len(gaps),
        "candidate_count": len(candidates),
        "gaps": gaps,
        "candidates": candidates,
        "validations": validations,
        "apply_enabled": apply,
        "apply_summary": apply_summary,
    }
    p = output_dir / f"primitive_feedback_{run_id}.json"
    p.write_text(json.dumps(artifact, indent=2))
    return {"artifact": str(p), **artifact}
