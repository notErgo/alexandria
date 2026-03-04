"""
Confidence scoring for extraction results.

Score = pattern_weight × distance_factor × range_factor, clamped to [0.0, 1.0].

- pattern_weight: intrinsic weight of the matched regex pattern (0.0–1.0)
- distance_factor: decays linearly as match context distance increases (0 → 1.0, 500 → 0.0)
- range_factor: 1.0 if value is within the metric's valid range; 0.0 if outside
"""
from typing import Optional

# Valid value ranges per metric (inclusive bounds)
METRIC_VALID_RANGES: dict = {
    "production_btc":         (0.0, 5000.0),
    "hodl_btc":               (0.0, 200000.0),
    "hodl_btc_unrestricted":  (0.0, 200000.0),
    "hodl_btc_restricted":    (0.0, 200000.0),
    "sold_btc":               (0.0, 5000.0),
    "hashrate_eh":            (0.0, 100.0),
    "realization_rate":       (0.0, 1.0),
    # v2 metrics
    "net_btc_balance_change": (-50000.0, 50000.0),
    "encumbered_btc":         (0.0, 200000.0),
    "mining_mw":              (0.0, 10000.0),
    "ai_hpc_mw":              (0.0, 5000.0),
    "hpc_revenue_usd":        (0.0, 1_000_000_000.0),
    "gpu_count":              (0.0, 1_000_000.0),
}

# Distance at which distance_factor reaches 0.0
_MAX_DISTANCE: float = 500.0


def score_extraction(
    pattern_weight: float,
    context_distance: int,
    value: float,
    metric: str,
) -> float:
    """
    Compute a confidence score in [0.0, 1.0] for an extraction result.

    Args:
        pattern_weight: Intrinsic weight of the regex pattern that matched.
        context_distance: Characters between the keyword context and numeric match.
        value: The extracted and normalized numeric value.
        metric: Metric name (used to look up valid range).

    Returns:
        Confidence score clamped to [0.0, 1.0].
    """
    # Distance penalty: linear decay, bottoms out at 0 when distance >= MAX_DISTANCE
    distance_factor = max(0.0, 1.0 - context_distance / _MAX_DISTANCE)

    # Range check: value must be within the metric's valid range
    bounds = METRIC_VALID_RANGES.get(metric)
    if bounds is not None:
        lo, hi = bounds
        range_factor = 1.0 if lo <= value <= hi else 0.0
    else:
        # Unknown metric — no range restriction applied
        range_factor = 1.0

    raw = pattern_weight * distance_factor * range_factor
    return max(0.0, min(1.0, raw))
