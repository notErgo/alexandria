"""Thread-safe host throttling helpers for polite concurrent scraping."""

from __future__ import annotations

import random
import threading
import time


class HostThrottle:
    """Apply per-host spacing and cooldowns across concurrent workers."""

    def __init__(
        self,
        *,
        min_interval_ms: int = 0,
        cooldown_seconds: float = 0.0,
        jitter_ratio: float = 0.1,
        time_fn=None,
        sleep_fn=None,
    ) -> None:
        self._min_interval_seconds = max(0.0, float(min_interval_ms) / 1000.0)
        self._cooldown_seconds = max(0.0, float(cooldown_seconds))
        self._jitter_ratio = max(0.0, float(jitter_ratio))
        self._time_fn = time_fn or time.monotonic
        self._sleep_fn = sleep_fn or time.sleep
        self._lock = threading.Lock()
        self._next_allowed_by_host: dict[str, float] = {}

    def wait(self, host: str) -> float:
        """Block until the host is allowed again. Returns sleep duration."""
        host = (host or '').lower()
        if not host:
            return 0.0
        with self._lock:
            now = self._time_fn()
            next_allowed = self._next_allowed_by_host.get(host, now)
            wait_seconds = max(0.0, next_allowed - now)
            self._next_allowed_by_host[host] = max(next_allowed, now) + self._min_interval_seconds
        if wait_seconds > 0:
            self._sleep_fn(wait_seconds)
        return wait_seconds

    def penalize(self, host: str, seconds: float | None = None) -> float:
        """Push the host's next-allowed time into the future and return cooldown."""
        host = (host or '').lower()
        if not host:
            return 0.0
        cooldown = self._cooldown_seconds if seconds is None else max(0.0, float(seconds))
        if cooldown <= 0:
            return 0.0
        jitter = cooldown * self._jitter_ratio * random.random()
        total = cooldown + jitter
        with self._lock:
            now = self._time_fn()
            next_allowed = self._next_allowed_by_host.get(host, now)
            self._next_allowed_by_host[host] = max(next_allowed, now) + total
        return total
