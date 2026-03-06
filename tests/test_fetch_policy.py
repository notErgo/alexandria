"""Tests for fetch_policy: RetryPolicy and CircuitBreaker.

Written before implementation -- these tests define the expected contract.
"""
import time
import pytest
import requests
from unittest.mock import MagicMock, patch, call


def _fresh_policy(max_attempts=3):
    """Return a RetryPolicy with an isolated DomainCircuitRegistry (no cross-test state)."""
    from scrapers.fetch_policy import RetryPolicy, DomainCircuitRegistry
    return RetryPolicy(max_attempts=max_attempts, registry=DomainCircuitRegistry())


class TestRetryPolicy:
    """Tests for RetryPolicy.execute() retry logic."""

    def test_success_on_first_attempt_no_retry(self):
        """Function succeeds on first call -- called exactly once, no retry."""
        fn = MagicMock(return_value=MagicMock(status_code=200))
        policy = _fresh_policy(max_attempts=3)

        with patch("scrapers.fetch_policy.time.sleep") as mock_sleep:
            result = policy.execute(fn, "https://ir.mara.com/rss", timeout=15)

        fn.assert_called_once_with("https://ir.mara.com/rss", timeout=15)
        mock_sleep.assert_not_called()
        assert result is not None

    def test_retries_on_500_up_to_max(self):
        """Function raises HTTP 500 three times then succeeds; called 4 times total."""
        http_error = requests.exceptions.HTTPError(response=MagicMock(status_code=500))
        success_resp = MagicMock(status_code=200)
        fn = MagicMock(side_effect=[http_error, http_error, http_error, success_resp])
        policy = _fresh_policy(max_attempts=4)

        with patch("scrapers.fetch_policy.time.sleep") as mock_sleep:
            result = policy.execute(fn, "https://ir.mara.com/rss")

        assert fn.call_count == 4
        assert mock_sleep.call_count == 3

    def test_429_uses_longer_backoff(self):
        """After a 429 response, backoff seconds is >= 30."""
        resp_429 = MagicMock(status_code=429)
        resp_429.headers = {}  # no Retry-After header
        http_error = requests.exceptions.HTTPError(response=resp_429)
        success_resp = MagicMock(status_code=200)
        fn = MagicMock(side_effect=[http_error, success_resp])
        policy = _fresh_policy(max_attempts=3)

        with patch("scrapers.fetch_policy.time.sleep") as mock_sleep:
            policy.execute(fn, "https://ir.mara.com/rss")

        assert mock_sleep.call_count == 1
        sleep_seconds = mock_sleep.call_args[0][0]
        assert sleep_seconds >= 30, f"Expected >= 30s backoff for 429, got {sleep_seconds}"

    def test_max_attempts_exceeded_raises(self):
        """Function fails with 503 on every call; after max_attempts, raises last exception."""
        http_error = requests.exceptions.HTTPError(response=MagicMock(status_code=503))
        fn = MagicMock(side_effect=http_error)
        policy = _fresh_policy(max_attempts=3)

        with patch("scrapers.fetch_policy.time.sleep"):
            with pytest.raises(requests.exceptions.HTTPError):
                policy.execute(fn, "https://ir.mara.com/rss")

        assert fn.call_count == 3

    def test_timeout_uses_short_backoff(self):
        """requests.exceptions.Timeout triggers retry with backoff <= 10s."""
        timeout_err = requests.exceptions.Timeout("timed out")
        success_resp = MagicMock(status_code=200)
        fn = MagicMock(side_effect=[timeout_err, success_resp])
        policy = _fresh_policy(max_attempts=3)

        with patch("scrapers.fetch_policy.time.sleep") as mock_sleep:
            policy.execute(fn, "https://ir.mara.com/rss")

        assert mock_sleep.call_count == 1
        sleep_seconds = mock_sleep.call_args[0][0]
        assert sleep_seconds <= 10, f"Expected <= 10s backoff for Timeout, got {sleep_seconds}"

    def test_dns_failure_does_not_retry(self):
        """ConnectionError with 'Name or service not known' raises immediately without retry."""
        dns_error = requests.exceptions.ConnectionError("Name or service not known")
        fn = MagicMock(side_effect=dns_error)
        policy = _fresh_policy(max_attempts=3)

        with patch("scrapers.fetch_policy.time.sleep") as mock_sleep:
            with pytest.raises(requests.exceptions.ConnectionError):
                policy.execute(fn, "https://dead.example.com/rss")

        fn.assert_called_once()
        mock_sleep.assert_not_called()


class TestCircuitBreaker:
    """Tests for CircuitBreaker state machine."""

    def test_closed_state_allows_calls(self):
        """Fresh breaker in CLOSED state; call goes through."""
        from scrapers.fetch_policy import CircuitBreaker

        fn = MagicMock(return_value="ok")
        breaker = CircuitBreaker("ir.mara.com", failure_threshold=3, recovery_timeout=120)

        result = breaker.call(fn, "arg1")

        fn.assert_called_once_with("arg1")
        assert result == "ok"

    def test_opens_after_threshold_failures(self):
        """3 consecutive failures open the circuit; 4th call raises CircuitOpenError."""
        from scrapers.fetch_policy import CircuitBreaker, CircuitOpenError

        fn = MagicMock(side_effect=Exception("server error"))
        breaker = CircuitBreaker("ir.mara.com", failure_threshold=3, recovery_timeout=120)

        for _ in range(3):
            with pytest.raises(Exception):
                breaker.call(fn)

        assert fn.call_count == 3

        with pytest.raises(CircuitOpenError):
            breaker.call(fn)

        # fn not called again on 4th attempt (circuit open)
        assert fn.call_count == 3

    def test_open_circuit_raises_immediately(self):
        """Open circuit raises CircuitOpenError without invoking fn."""
        from scrapers.fetch_policy import CircuitBreaker, CircuitOpenError

        fn = MagicMock(side_effect=Exception("server error"))
        breaker = CircuitBreaker("ir.mara.com", failure_threshold=3, recovery_timeout=120)

        # Trip the breaker
        for _ in range(3):
            with pytest.raises(Exception):
                breaker.call(fn)

        fn.reset_mock()
        fn.side_effect = None

        with pytest.raises(CircuitOpenError):
            breaker.call(fn)

        fn.assert_not_called()

    def test_half_open_probe_on_success_closes_circuit(self):
        """After breaker opens, advance time past recovery_timeout, probe succeeds, circuit closes."""
        from scrapers.fetch_policy import CircuitBreaker, CircuitOpenError

        # Use a fixed base time so patching is stable
        base_time = 1_000_000.0
        fn = MagicMock(side_effect=Exception("server error"))

        with patch("scrapers.fetch_policy.time.time", return_value=base_time):
            breaker = CircuitBreaker("ir.mara.com", failure_threshold=3, recovery_timeout=120)

            # Trip the breaker -- opened_at = base_time
            for _ in range(3):
                with pytest.raises(Exception):
                    breaker.call(fn)

        # Advance time past recovery_timeout
        with patch("scrapers.fetch_policy.time.time", return_value=base_time + 200):
            # Reset fn to succeed
            fn.side_effect = None
            fn.return_value = "recovered"

            # Probe call (half-open) succeeds -> circuit closes
            result = breaker.call(fn)
            assert result == "recovered"

            # Circuit is now closed; subsequent calls go through
            fn.return_value = "normal"
            result2 = breaker.call(fn)
            assert result2 == "normal"
            assert not breaker.is_open()

    def test_half_open_probe_on_failure_reopens(self):
        """Probe call fails; circuit stays open."""
        from scrapers.fetch_policy import CircuitBreaker, CircuitOpenError

        base_time = 1_000_000.0
        fn = MagicMock(side_effect=Exception("server error"))

        with patch("scrapers.fetch_policy.time.time", return_value=base_time):
            breaker = CircuitBreaker("ir.mara.com", failure_threshold=3, recovery_timeout=120)

            # Trip the breaker
            for _ in range(3):
                with pytest.raises(Exception):
                    breaker.call(fn)

        # Advance time past recovery_timeout but use same value so second check
        # does not trigger another half-open transition
        future_time = base_time + 200
        with patch("scrapers.fetch_policy.time.time", return_value=future_time):
            # Probe call fails -> circuit reopens with opened_at = future_time
            with pytest.raises(Exception):
                breaker.call(fn)

        # Now time is at future_time; recovery would need future_time + 120
        # Stay at future_time so circuit is still within its new window
        with patch("scrapers.fetch_policy.time.time", return_value=future_time + 10):
            with pytest.raises(CircuitOpenError):
                breaker.call(fn)

    def test_circuit_keyed_by_domain(self):
        """Failures on domain A do not affect domain B."""
        from scrapers.fetch_policy import CircuitBreaker

        fn_a = MagicMock(side_effect=Exception("dead"))
        breaker_a = CircuitBreaker("dead.example.com", failure_threshold=3, recovery_timeout=120)

        fn_b = MagicMock(return_value="ok")
        breaker_b = CircuitBreaker("ir.mara.com", failure_threshold=3, recovery_timeout=120)

        # Trip breaker_a
        for _ in range(3):
            with pytest.raises(Exception):
                breaker_a.call(fn_a)

        assert breaker_a.is_open()

        # breaker_b unaffected
        result = breaker_b.call(fn_b)
        assert result == "ok"
        assert not breaker_b.is_open()
