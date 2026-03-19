#!/usr/bin/env python3
"""Helpers for reading composed Jinja templates as flattened source."""

from __future__ import annotations

import re
from pathlib import Path


ROOT = Path(__file__).resolve().parent.parent
TEMPLATES_DIR = ROOT / "templates"
INCLUDE_RE = re.compile(r"""{%\s*include\s+['"]([^'"]+)['"]\s*%}""")


def _resolve_template(path: Path, seen: set[Path]) -> str:
    if path in seen:
        raise ValueError(f"recursive template include detected: {path}")
    seen.add(path)
    text = path.read_text(encoding="utf-8")

    def _replace(match: re.Match[str]) -> str:
        include_name = match.group(1)
        include_path = TEMPLATES_DIR / include_name
        if not include_path.exists():
            return match.group(0)
        return _resolve_template(include_path, seen.copy())

    return INCLUDE_RE.sub(_replace, text)


def load_template_source(template_name: str) -> str:
    """Return template source with local Jinja includes expanded."""
    path = TEMPLATES_DIR / template_name
    if not path.exists():
        raise FileNotFoundError(path)
    return _resolve_template(path, set())
