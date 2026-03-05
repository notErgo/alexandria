"""Source contract helpers for observer/scout scraping workflows."""
from __future__ import annotations

from copy import deepcopy
from typing import Any

_FAMILIES = {"ir", "globenewswire", "prnewswire"}
_DISCOVERY_METHODS = {"rss", "index", "template", "search", "year_filter"}
_STATUSES = {"ready_for_scrape", "partially_covered", "exhausted", "blocked"}


def _uniq_keep_order(items: list[str]) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for item in items:
        if not item or item in seen:
            continue
        seen.add(item)
        out.append(item)
    return out


def normalize_source(source: dict[str, Any]) -> dict[str, Any]:
    out = deepcopy(source)
    out["family"] = str(out.get("family", "")).strip().lower()
    out["entry_url"] = str(out.get("entry_url", "")).strip()
    out["discovery_method"] = str(out.get("discovery_method", "")).strip().lower()
    out["url_pattern"] = str(out.get("url_pattern", "")).strip()
    out["confidence"] = float(out.get("confidence", 0.0))
    out["confidence"] = max(0.0, min(1.0, out["confidence"]))

    pagination = out.get("pagination") or {}
    out["pagination"] = {
        "type": str((pagination.get("type") or "none")).strip().lower(),
        "template": str((pagination.get("template") or "")).strip(),
        "max_page": int(pagination.get("max_page") or 0),
    }

    date_extraction = out.get("date_extraction") or {}
    out["date_extraction"] = {
        "strategy": str((date_extraction.get("strategy") or "")).strip().lower(),
        "pattern": str((date_extraction.get("pattern") or "")).strip(),
    }

    filters = out.get("filters") or {}
    include = [str(x).strip().lower() for x in (filters.get("include") or []) if str(x).strip()]
    exclude = [str(x).strip().lower() for x in (filters.get("exclude") or []) if str(x).strip()]
    out["filters"] = {"include": _uniq_keep_order(include), "exclude": _uniq_keep_order(exclude)}

    validation = out.get("validation") or {}
    out["validation"] = {
        "http_ok": bool(validation.get("http_ok", False)),
        "parse_ok": bool(validation.get("parse_ok", False)),
        "sample_count": int(validation.get("sample_count") or 0),
    }

    evidence_urls = [str(x).strip() for x in (out.get("evidence_urls") or []) if str(x).strip()]
    out["evidence_urls"] = _uniq_keep_order(evidence_urls)
    return out


def validate_source(source: dict[str, Any]) -> list[str]:
    errs: list[str] = []
    s = normalize_source(source)
    if s["family"] not in _FAMILIES:
        errs.append(f"invalid family: {s['family']!r}")
    if s["discovery_method"] not in _DISCOVERY_METHODS:
        errs.append(f"invalid discovery_method: {s['discovery_method']!r}")
    if not s["entry_url"]:
        errs.append("entry_url is required")
    if not isinstance(s["validation"]["sample_count"], int) or s["validation"]["sample_count"] < 0:
        errs.append("validation.sample_count must be >= 0")
    if not 0.0 <= s["confidence"] <= 1.0:
        errs.append("confidence must be in [0,1]")
    return errs


def normalize_contract(contract: dict[str, Any]) -> dict[str, Any]:
    out = deepcopy(contract)
    out["ticker"] = str(out.get("ticker", "")).strip().upper()
    out["run_id"] = str(out.get("run_id", "")).strip()
    out["status"] = str(out.get("status", "exhausted")).strip().lower()
    if out["status"] not in _STATUSES:
        out["status"] = "exhausted"
    out["blockers"] = list(out.get("blockers") or [])
    out["sources"] = [normalize_source(x) for x in (out.get("sources") or [])]
    return out


def validate_contract(contract: dict[str, Any]) -> list[str]:
    errs: list[str] = []
    c = normalize_contract(contract)
    if not c["ticker"]:
        errs.append("ticker is required")
    if not c["run_id"]:
        errs.append("run_id is required")
    for i, source in enumerate(c["sources"]):
        for e in validate_source(source):
            errs.append(f"sources[{i}]: {e}")
    return errs


def merge_contracts(contracts: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Merge by ticker and source family, keeping highest-confidence family source."""
    by_ticker: dict[str, dict[str, Any]] = {}
    for raw in contracts:
        c = normalize_contract(raw)
        t = c["ticker"]
        if not t:
            continue
        existing = by_ticker.get(t)
        if existing is None:
            by_ticker[t] = c
            continue

        # Keep stronger status if either is ready/partial.
        rank = {"blocked": 0, "exhausted": 1, "partially_covered": 2, "ready_for_scrape": 3}
        if rank.get(c["status"], 0) > rank.get(existing["status"], 0):
            existing["status"] = c["status"]

        # Merge blockers and sources.
        existing["blockers"].extend(c["blockers"])
        fam_best: dict[str, dict[str, Any]] = {}
        for src in existing.get("sources", []) + c.get("sources", []):
            fam = src.get("family")
            prev = fam_best.get(fam)
            if prev is None or float(src.get("confidence", 0.0)) > float(prev.get("confidence", 0.0)):
                fam_best[fam] = src
            elif prev is not None and float(src.get("confidence", 0.0)) == float(prev.get("confidence", 0.0)):
                prev["evidence_urls"] = _uniq_keep_order((prev.get("evidence_urls") or []) + (src.get("evidence_urls") or []))
        existing["sources"] = sorted(fam_best.values(), key=lambda s: s.get("family", ""))
        existing["blockers"] = _uniq_keep_order([str(x) for x in existing["blockers"] if str(x).strip()])

    return [by_ticker[t] for t in sorted(by_ticker.keys())]
