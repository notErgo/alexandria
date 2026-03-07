"""
Pattern registry: loads and validates per-metric regex pattern config files.

Config structure (config/patterns/<metric>.json):
  {
    "metric": "production_btc",
    "valid_range": [0, 5000],
    "unit": "BTC",
    "conflict_resolution": "highest_confidence",
    "patterns": [
      {"id": "...", "regex": "...", "confidence_weight": 0.95, "priority": 0},
      ...
    ]
  }
"""
import json
import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List

log = logging.getLogger('miners.interpreters.pattern_registry')

_REQUIRED_PATTERN_KEYS = {'id', 'regex', 'confidence_weight', 'priority'}


@dataclass
class PatternRegistry:
    """Loaded and validated pattern configuration for all metrics."""
    metrics: Dict[str, List[dict]]

    @classmethod
    def load(cls, config_dir: str) -> 'PatternRegistry':
        """
        Load all *.json files from <config_dir>/patterns/, validate structure,
        sort each metric's pattern list by priority ascending, and return registry.
        """
        patterns_dir = Path(config_dir) / 'patterns'
        metrics: Dict[str, List[dict]] = {}
        for json_file in sorted(patterns_dir.glob('*.json')):
            try:
                with open(json_file) as f:
                    data = json.load(f)
                metric = data['metric']
                patterns = data['patterns']
                for p in patterns:
                    missing = _REQUIRED_PATTERN_KEYS - set(p.keys())
                    if missing:
                        log.warning("Pattern in %s missing keys %s — skipping", json_file.name, missing)
                        continue
                sorted_patterns = sorted(patterns, key=lambda p: p['priority'])
                metrics[metric] = sorted_patterns
                log.debug("Loaded %d patterns for metric %s", len(sorted_patterns), metric)
            except (KeyError, json.JSONDecodeError) as e:
                log.error("Failed to load pattern file %s: %s", json_file, e)
        return cls(metrics=metrics)

    def get_patterns(self, metric: str) -> List[dict]:
        """Return pattern list for metric, raising KeyError if not found."""
        if metric not in self.metrics:
            raise KeyError(f"No patterns loaded for metric: {metric!r}")
        return self.metrics[metric]
