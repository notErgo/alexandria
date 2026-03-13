"""
Outlier detection for LLM extraction results.

Compares a candidate value against the trailing average for the same
ticker+metric to flag suspicious spikes or drops.
"""
from typing import Optional

from config import OUTLIER_MIN_HISTORY


def detect_outlier(
    candidate: float,
    trailing_values: list,
    threshold_pct: float,
    min_history: Optional[int] = None,
) -> tuple:
    """Return (is_outlier, trailing_avg).

    Compares candidate against the mean of trailing_values.
    is_outlier=True if |candidate - avg| / max(avg, 1e-9) > threshold_pct.
    Returns (False, None) if len(trailing_values) < OUTLIER_MIN_HISTORY.

    Args:
        candidate:       The new value to test.
        trailing_values: Historical values (monthly) for the same ticker+metric.
        threshold_pct:   Fractional deviation threshold (e.g. 0.40 = 40%).
        min_history:     Minimum history length required; defaults to OUTLIER_MIN_HISTORY.

    Returns:
        (is_outlier: bool, trailing_avg: float or None)
    """
    _min_history = min_history if min_history is not None else OUTLIER_MIN_HISTORY
    if len(trailing_values) < _min_history:
        return False, None

    avg = sum(trailing_values) / len(trailing_values)
    denominator = max(abs(avg), 1e-9)
    deviation = abs(candidate - avg) / denominator
    is_outlier = deviation > threshold_pct
    return is_outlier, avg
