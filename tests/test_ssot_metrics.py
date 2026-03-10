"""
Phase 1a: SSOT metric consistency tests.
All assertions check that per-module metric sets are consistent with canonical definitions.
These tests FAIL before Phase 2 fixes and PASS after.
"""
import importlib
import sys
import types
import pytest


# ── Canonical 13-metric seed set (matches metric_schema seed in scripts/seed_metrics.py) ──
CANONICAL_METRICS = frozenset({
    'production_btc', 'holdings_btc', 'unrestricted_holdings', 'restricted_holdings_btc',
    'sales_btc', 'hashrate_eh', 'realization_rate',
    'net_btc_balance_change', 'encumbered_btc',
    'mining_mw', 'ai_hpc_mw', 'hpc_revenue_usd', 'gpu_count',
})

# Retired metric names (from before 2026-03-09 rename)
RETIRED_NAMES = frozenset({
    'hodl_btc', 'sold_btc', 'hodl_btc_restricted', 'hodl_btc_unrestricted',
})

# Expected canonical FLOW_METRICS
CANONICAL_FLOW = frozenset({'production_btc', 'sales_btc', 'net_btc_balance_change'})

# Expected canonical SNAPSHOT_METRICS (all snapshot metrics using canonical names)
CANONICAL_SNAPSHOT = frozenset({
    'holdings_btc', 'unrestricted_holdings', 'restricted_holdings_btc',
    'hashrate_eh', 'realization_rate', 'encumbered_btc',
    'mining_mw', 'ai_hpc_mw', 'hpc_revenue_usd', 'gpu_count',
})


def test_config_flow_metrics_uses_canonical_names():
    """config.FLOW_METRICS must not contain retired metric names."""
    from config import FLOW_METRICS
    retired_found = FLOW_METRICS & RETIRED_NAMES
    assert not retired_found, f"config.FLOW_METRICS contains retired names: {retired_found}"


def test_config_snapshot_metrics_uses_canonical_names():
    """config.SNAPSHOT_METRICS must not contain retired metric names."""
    from config import SNAPSHOT_METRICS
    retired_found = SNAPSHOT_METRICS & RETIRED_NAMES
    assert not retired_found, f"config.SNAPSHOT_METRICS contains retired names: {retired_found}"


def test_gap_fill_flow_metrics_matches_config():
    """gap_fill.FLOW_METRICS must equal config.FLOW_METRICS."""
    from interpreters.gap_fill import FLOW_METRICS as gf_flow
    from config import FLOW_METRICS as cfg_flow
    assert gf_flow == cfg_flow, (
        f"gap_fill.FLOW_METRICS {gf_flow} != config.FLOW_METRICS {cfg_flow}"
    )


def test_gap_fill_snapshot_metrics_matches_config():
    """gap_fill.SNAPSHOT_METRICS must equal config.SNAPSHOT_METRICS."""
    from interpreters.gap_fill import SNAPSHOT_METRICS as gf_snap
    from config import SNAPSHOT_METRICS as cfg_snap
    assert gf_snap == cfg_snap, (
        f"gap_fill.SNAPSHOT_METRICS {gf_snap} != config.SNAPSHOT_METRICS {cfg_snap}"
    )


def test_interpret_valid_metrics_fallback_is_full_set():
    """routes.interpret._VALID_METRICS_FALLBACK must cover the full canonical metric set."""
    from routes.interpret import _VALID_METRICS_FALLBACK
    missing = CANONICAL_METRICS - _VALID_METRICS_FALLBACK
    assert not missing, (
        f"routes.interpret._VALID_METRICS_FALLBACK is missing canonical metrics: {missing}"
    )


def test_interpret_fallback_matches_data_points_fallback():
    """routes.interpret._VALID_METRICS_FALLBACK must equal routes.data_points._VALID_METRICS_FALLBACK."""
    from routes.interpret import _VALID_METRICS_FALLBACK as interp_fb
    from routes.data_points import _VALID_METRICS_FALLBACK as dp_fb
    assert interp_fb == dp_fb, (
        f"interpret fallback {interp_fb} != data_points fallback {dp_fb}"
    )


def test_diagnostics_all_metrics_no_retired_names():
    """routes.diagnostics._ALL_METRICS_FALLBACK must not contain retired metric names."""
    from routes.diagnostics import _ALL_METRICS_FALLBACK
    retired_found = set(_ALL_METRICS_FALLBACK) & RETIRED_NAMES
    assert not retired_found, (
        f"routes.diagnostics._ALL_METRICS_FALLBACK contains retired names: {retired_found}"
    )


def test_patterns_metric_order_no_retired_names():
    """routes.patterns._METRIC_ORDER must not contain retired metric names."""
    from routes.patterns import _METRIC_ORDER
    retired_found = set(_METRIC_ORDER) & RETIRED_NAMES
    assert not retired_found, (
        f"routes.patterns._METRIC_ORDER contains retired names: {retired_found}"
    )


def test_config_flow_metrics_equals_canonical():
    """config.FLOW_METRICS must match the expected canonical set."""
    from config import FLOW_METRICS
    assert FLOW_METRICS == CANONICAL_FLOW, (
        f"config.FLOW_METRICS {FLOW_METRICS} != canonical {CANONICAL_FLOW}"
    )


def test_config_snapshot_metrics_equals_canonical():
    """config.SNAPSHOT_METRICS must match the expected canonical set."""
    from config import SNAPSHOT_METRICS
    assert SNAPSHOT_METRICS == CANONICAL_SNAPSHOT, (
        f"config.SNAPSHOT_METRICS {SNAPSHOT_METRICS} != canonical {CANONICAL_SNAPSHOT}"
    )
