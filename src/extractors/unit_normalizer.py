"""
Unit normalization for miner metrics.

All hashrate values are normalized to EH/s regardless of source unit.
All BTC values are normalized to float BTC.
Percentage values are normalized to ratio (0.0–1.0).
"""
import re
from typing import Optional, Tuple

# Conversion multipliers to EH/s (exahashes per second)
_HASHRATE_CONVERSIONS: dict = {
    "EH/S": 1.0,
    "PH/S": 1e-3,
    "TH/S": 1e-6,
    "GH/S": 1e-9,
    "EXAHASH": 1.0,
    "EXAHASHES": 1.0,
}

_HASHRATE_PATTERN = re.compile(
    r"([\d.]+)\s*(EH/s|PH/s|TH/s|GH/s|exahash(?:es)?(?:\s+per\s+second)?)",
    re.IGNORECASE,
)
_BTC_PATTERN = re.compile(
    r"(-?[\d,]+(?:\.\d+)?)\s*(?:BTC|bitcoin)?",
    re.IGNORECASE,
)
_PERCENT_PATTERN = re.compile(r"([\d.]+)\s*%")


def normalize_hashrate(raw: str) -> Optional[Tuple[float, str]]:
    """Parse a hashrate string and return (value_in_EH_per_s, 'EH/s') or None."""
    m = _HASHRATE_PATTERN.search(raw)
    if not m:
        return None
    value_str, unit_str = m.group(1), m.group(2)
    try:
        value = float(value_str)
    except ValueError:
        return None
    # Normalize unit key: strip trailing parts, uppercase
    key = unit_str.upper().split()[0]  # "exahashes per second" → "EXAHASHES"
    multiplier = _HASHRATE_CONVERSIONS.get(key)
    if multiplier is None:
        return None
    return (value * multiplier, "EH/s")


def normalize_btc(raw: str) -> Optional[Tuple[float, str]]:
    """Parse a BTC quantity string and return (value, 'BTC') or None."""
    m = _BTC_PATTERN.search(raw)
    if not m:
        return None
    try:
        value = float(m.group(1).replace(",", ""))
    except ValueError:
        return None
    return (value, "BTC")


def normalize_percent(raw: str) -> Optional[Tuple[float, str]]:
    """Parse a percentage string and return (value/100, 'ratio') or None."""
    m = _PERCENT_PATTERN.search(raw)
    if not m:
        return None
    try:
        value = float(m.group(1)) / 100.0
    except ValueError:
        return None
    return (value, "ratio")


def normalize_value(raw: str, metric: str) -> Optional[Tuple[float, str]]:
    """Dispatch to the correct normalizer based on metric name."""
    if metric == "hashrate_eh":
        return normalize_hashrate(raw)
    elif metric == "realization_rate":
        return normalize_percent(raw)
    else:
        # production_btc, hodl_btc, sold_btc
        return normalize_btc(raw)
