#!/usr/bin/env python3
"""
Static analysis: logging compliance checker for the miners codebase.

Checks:
  1. All 'except Exception' blocks that contain a log call must use exc_info=True.
  2. Files with 5+ log calls and zero event= usage are reported as compliance warnings.

Exit codes:
  0 = clean (no violations)
  1 = violations found

Advisory only — does NOT block CI unless LOGGING_GATE_STRICT=1 is set.
Run: python3 scripts/check_logging_compliance.py [src/]
"""
import ast
import os
import re
import sys
from pathlib import Path


def _find_src_dir(args: list[str]) -> Path:
    if args:
        return Path(args[0]).resolve()
    here = Path(__file__).resolve().parent.parent
    return here / 'src'


def _iter_py_files(root: Path):
    for p in root.rglob('*.py'):
        if 'venv' in p.parts or '__pycache__' in p.parts:
            continue
        yield p


# -- Check 1: except blocks with log but no exc_info=True ------------------


class _ExcInfoChecker(ast.NodeVisitor):
    def __init__(self, filename: str):
        self.filename = filename
        self.violations: list[str] = []

    def visit_ExceptHandler(self, node: ast.ExceptHandler) -> None:
        # Only care about bare 'except Exception' or 'except Exception as e'
        if node.type is None or (
            isinstance(node.type, ast.Name) and node.type.id == 'Exception'
        ):
            has_log_call = False
            has_exc_info = False
            for child in ast.walk(ast.Module(body=node.body, type_ignores=[])):
                if isinstance(child, ast.Call):
                    # log.error(...) / log.warning(...) style
                    if isinstance(child.func, ast.Attribute):
                        if child.func.attr in ('error', 'exception', 'critical', 'warning'):
                            has_log_call = True
                            for kw in child.keywords:
                                if kw.arg == 'exc_info' and (
                                    (isinstance(kw.value, ast.Constant) and kw.value.value) or
                                    (isinstance(kw.value, ast.NameConstant) and kw.value.value)
                                ):
                                    has_exc_info = True
            if has_log_call and not has_exc_info:
                self.violations.append(
                    f"{self.filename}:{node.lineno}: except Exception block has log call without exc_info=True"
                )
        self.generic_visit(node)


def check_exc_info(path: Path) -> list[str]:
    try:
        source = path.read_text(encoding='utf-8', errors='replace')
        tree = ast.parse(source, filename=str(path))
        checker = _ExcInfoChecker(str(path))
        checker.visit(tree)
        return checker.violations
    except SyntaxError:
        return []


# -- Check 2: files with many log calls but no event= usage ----------------

_LOG_CALL_RE = re.compile(r'\blog\.(info|warning|error|debug|critical)\(')
_EVENT_RE = re.compile(r'event=')

_EVENT_EXEMPT_FILES = {
    'logging_config.py',
    'conftest.py',
}


def check_event_usage(path: Path) -> str | None:
    name = path.name
    if name in _EVENT_EXEMPT_FILES:
        return None
    try:
        source = path.read_text(encoding='utf-8', errors='replace')
    except OSError:
        return None
    log_calls = len(_LOG_CALL_RE.findall(source))
    if log_calls < 5:
        return None
    has_event = bool(_EVENT_RE.search(source))
    if not has_event:
        return f"{path}: {log_calls} log calls, zero event= usage (compliance warning)"
    return None


# -- Main ------------------------------------------------------------------

def main(argv: list[str] = None) -> int:
    argv = argv if argv is not None else sys.argv[1:]
    src_dir = _find_src_dir(argv)

    if not src_dir.is_dir():
        print(f"ERROR: source directory not found: {src_dir}", file=sys.stderr)
        return 2

    exc_violations: list[str] = []
    event_warnings: list[str] = []

    for py_file in sorted(_iter_py_files(src_dir)):
        exc_violations.extend(check_exc_info(py_file))
        w = check_event_usage(py_file)
        if w:
            event_warnings.append(w)

    if exc_violations:
        print(f"\n[VIOLATIONS] {len(exc_violations)} except-block logging issues:\n")
        for v in exc_violations:
            print(f"  {v}")

    if event_warnings:
        print(f"\n[WARNINGS] {len(event_warnings)} files with low event= coverage:\n")
        for w in event_warnings:
            print(f"  {w}")

    if not exc_violations and not event_warnings:
        print("Logging compliance: OK")
        return 0

    strict = os.environ.get('LOGGING_GATE_STRICT', '0').strip() == '1'
    if strict:
        print("\nLOGGING_GATE_STRICT=1: treating warnings as errors.", file=sys.stderr)
        return 1

    if exc_violations:
        return 1

    print(f"\n{len(event_warnings)} advisory warning(s) — set LOGGING_GATE_STRICT=1 to treat as errors.")
    return 0


if __name__ == '__main__':
    sys.exit(main())
