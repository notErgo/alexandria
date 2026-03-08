"""Tests for PatternRegistry loading and validation."""
import os
from interpreters.pattern_registry import PatternRegistry

_CONFIG_DIR = os.path.join(os.path.dirname(__file__), '..', 'config')


class TestPatternRegistry:
    def test_load_returns_expected_metrics(self):
        registry = PatternRegistry.load(_CONFIG_DIR)
        expected = {"production_btc", "hodl_btc", "sold_btc"}
        assert set(registry.metrics.keys()) == expected

    def test_patterns_sorted_by_priority(self):
        registry = PatternRegistry.load(_CONFIG_DIR)
        for metric, patterns in registry.metrics.items():
            priorities = [p['priority'] for p in patterns]
            assert priorities == sorted(priorities), f"Metric {metric} patterns not sorted"

    def test_all_patterns_have_required_keys(self):
        registry = PatternRegistry.load(_CONFIG_DIR)
        required = {'id', 'regex', 'confidence_weight', 'priority'}
        for metric, patterns in registry.metrics.items():
            for p in patterns:
                missing = required - set(p.keys())
                assert not missing, f"Pattern in {metric} missing keys: {missing}"

    def test_get_patterns_returns_list(self):
        # Config patterns may be empty in LLM-only mode; test the return type only.
        registry = PatternRegistry.load(_CONFIG_DIR)
        patterns = registry.get_patterns('production_btc')
        assert isinstance(patterns, list)

    def test_get_patterns_raises_for_unknown_metric(self):
        registry = PatternRegistry.load(_CONFIG_DIR)
        try:
            registry.get_patterns('nonexistent_metric')
            assert False, "Expected KeyError"
        except KeyError:
            pass
