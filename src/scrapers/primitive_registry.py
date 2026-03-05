"""Data-driven scraper primitive registry."""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any
from urllib.parse import urlencode, urlparse, urlunparse

from config import CONFIG_DIR

_REGISTRY_PATH = Path(CONFIG_DIR) / "scraper_primitives.json"


def registry_path() -> Path:
    return _REGISTRY_PATH


def load_primitives(path: Path | None = None) -> list[dict[str, Any]]:
    p = path or _REGISTRY_PATH
    if not p.exists():
        return []
    try:
        raw = json.loads(p.read_text())
    except Exception:
        return []
    if not isinstance(raw, list):
        return []
    out = []
    for item in raw:
        if isinstance(item, dict):
            out.append(item)
    return out


def save_primitives(primitives: list[dict[str, Any]], path: Path | None = None) -> None:
    p = path or _REGISTRY_PATH
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps(primitives, indent=2) + "\n")


def load_active_primitives(path: Path | None = None) -> list[dict[str, Any]]:
    items = load_primitives(path)
    return [x for x in items if bool(x.get("active", False))]


def _host_matches(host: str, rule: str) -> bool:
    host = (host or "").lower().strip()
    rule = (rule or "").lower().strip()
    if not host or not rule:
        return False
    return host == rule or host.endswith("." + rule)


def _path_matches(path: str, rule: str) -> bool:
    path = (path or "").strip()
    rule = (rule or "").strip()
    if not rule:
        return True
    return path.startswith(rule)


def match_primitive(primitives: list[dict[str, Any]], *, family: str, entry_url: str) -> dict[str, Any] | None:
    parsed = urlparse(entry_url or "")
    for p in primitives:
        if (p.get("family") or "").strip().lower() != family:
            continue
        match = p.get("match") or {}
        host_rule = str(match.get("host") or "").strip()
        path_rule = str(match.get("path_prefix") or "").strip()
        if host_rule and not _host_matches(parsed.netloc, host_rule):
            continue
        if path_rule and not _path_matches(parsed.path, path_rule):
            continue
        return p
    return None


def materialize_year_filter_source(
    primitive: dict[str, Any],
    *,
    entry_url: str,
    include: list[str],
    exclude: list[str],
) -> dict[str, Any] | None:
    strategy = (primitive.get("strategy") or "").strip().lower()
    if strategy != "year_filter_query":
        return None

    params = primitive.get("params") or {}
    select_name = str(params.get("select_name") or "").strip()
    years = [str(y).strip() for y in (params.get("years") or []) if str(y).strip()]
    if not select_name or not years:
        return None

    parsed = urlparse(entry_url)
    base_q: dict[str, str] = {}
    for k, v in (params.get("base_query") or {}).items():
        ks = str(k).strip()
        if ks:
            base_q[ks] = str(v)

    if "op" not in base_q:
        base_q["op"] = "Filter"
    if "form_id" not in base_q:
        base_q["form_id"] = "widget_form_base"

    year_urls: list[str] = []
    for y in years:
        q = dict(base_q)
        q[select_name] = y
        year_urls.append(urlunparse((parsed.scheme, parsed.netloc, parsed.path, "", urlencode(q, doseq=True), "")))

    tq = dict(base_q)
    tq[select_name] = "{year}"
    template = urlunparse((parsed.scheme, parsed.netloc, parsed.path, "", urlencode(tq, doseq=True), ""))

    return {
        "family": "ir",
        "entry_url": entry_url,
        "discovery_method": "year_filter",
        "url_pattern": template,
        "pagination": {"type": "query", "template": "?page={n}", "max_page": 25},
        "date_extraction": {"strategy": "title_regex", "pattern": "month year"},
        "filters": {"include": include, "exclude": exclude},
        "validation": {"http_ok": True, "parse_ok": False, "sample_count": 0},
        "confidence": float(primitive.get("confidence", 0.7)),
        "evidence_urls": year_urls[:3],
        "year_filter": {
            "select_name": select_name,
            "years": years,
            "year_urls": year_urls,
            "url_template": template,
            "sample_count": 0,
            "heuristic_only": True,
            "from_primitive": str(primitive.get("id") or ""),
        },
    }
