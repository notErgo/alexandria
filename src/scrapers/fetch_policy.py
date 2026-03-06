"""Retry policy and circuit breaker for HTTP scraping.

RetryPolicy: configurable per-failure-type backoff, max attempts.
CircuitBreaker: per-domain open/half-open/closed state machine.
DomainCircuitRegistry: global registry, keyed by domain.
"""
import time
import logging
from urllib.parse import urlparse

import requests

log = logging.getLogger('miners.scrapers.fetch_policy')

# Circuit breaker states
_STATE_CLOSED = "CLOSED"
_STATE_OPEN = "OPEN"
_STATE_HALF_OPEN = "HALF_OPEN"


class CircuitOpenError(Exception):
    """Raised when a circuit breaker is open and the call is not allowed."""


class CircuitBreaker:
    """Per-domain open/half-open/closed state machine.

    CLOSED    -> allow calls; on failure_threshold consecutive failures -> OPEN.
    OPEN      -> block calls with CircuitOpenError; after recovery_timeout -> HALF_OPEN probe.
    HALF_OPEN -> allow one probe call; on success -> CLOSED; on failure -> OPEN.
    """

    def __init__(self, domain: str, failure_threshold: int = 3, recovery_timeout: int = 120):
        self.domain = domain
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self._state = _STATE_CLOSED
        self._failure_count = 0
        self._opened_at: float = 0.0

    def is_open(self) -> bool:
        """Return True if the circuit is currently OPEN (blocking calls)."""
        if self._state == _STATE_OPEN:
            if time.time() - self._opened_at >= self.recovery_timeout:
                # Recovery window elapsed -- transition to HALF_OPEN so probe is allowed
                self._state = _STATE_HALF_OPEN
                return False
            return True
        return False

    def call(self, fn, *args, **kwargs):
        """Execute fn if circuit allows; raise CircuitOpenError if open."""
        if self._state == _STATE_OPEN:
            if time.time() - self._opened_at >= self.recovery_timeout:
                self._state = _STATE_HALF_OPEN
                log.info("Circuit breaker for %s transitioning to HALF_OPEN", self.domain)
            else:
                raise CircuitOpenError(
                    f"Circuit breaker open for domain: {self.domain}"
                )

        try:
            result = fn(*args, **kwargs)
            self._on_success()
            return result
        except Exception:
            self._on_failure()
            raise

    def record_success(self):
        """Record a successful outcome without calling a fn (used by RetryPolicy)."""
        self._on_success()

    def record_failure(self):
        """Record a failure without calling a fn (used by RetryPolicy)."""
        self._on_failure()

    def _on_success(self):
        if self._state == _STATE_HALF_OPEN:
            log.info("Circuit breaker for %s closed after successful probe", self.domain)
        self._state = _STATE_CLOSED
        self._failure_count = 0

    def _on_failure(self):
        if self._state == _STATE_HALF_OPEN:
            # Probe failed -- reopen immediately
            self._state = _STATE_OPEN
            self._opened_at = time.time()
            log.warning(
                "Circuit breaker for %s reopened after failed probe", self.domain
            )
        else:
            self._failure_count += 1
            if self._failure_count >= self.failure_threshold:
                self._state = _STATE_OPEN
                self._opened_at = time.time()
                log.warning(
                    "Circuit breaker for %s opened after %d failures",
                    self.domain, self._failure_count,
                )


def extract_domain(url: str) -> str:
    """Strip scheme and path from a URL, returning just the domain.

    Example: 'https://ir.mara.com/news/foo' -> 'ir.mara.com'
    """
    parsed = urlparse(url)
    return parsed.netloc or url


class DomainCircuitRegistry:
    """Registry of CircuitBreaker instances keyed by domain."""

    def __init__(self):
        self._breakers: dict = {}

    def get(self, domain: str) -> CircuitBreaker:
        """Return the CircuitBreaker for domain, creating it on first access."""
        if domain not in self._breakers:
            self._breakers[domain] = CircuitBreaker(domain)
        return self._breakers[domain]


# Module-level singleton used by DEFAULT_RETRY_POLICY
_CIRCUIT_REGISTRY = DomainCircuitRegistry()

# DNS failure indicators -- raise immediately, no retry
_DNS_ERROR_PHRASES = (
    "Name or service not known",
    "nodename nor servname",
)


def _is_dns_error(exc: requests.exceptions.ConnectionError) -> bool:
    msg = str(exc)
    return any(phrase in msg for phrase in _DNS_ERROR_PHRASES)


class RetryPolicy:
    """Configurable retry policy with per-failure-type backoff and circuit breaker integration.

    The circuit breaker is consulted before each attempt (raises CircuitOpenError if open).
    After all retries succeed, a success is recorded on the circuit.
    After all retries fail (max_attempts exhausted), a failure is recorded on the circuit.

    This design separates transient-retry logic (handled here) from persistent-failure
    detection (handled by the circuit breaker across multiple invocations).

    Backoff schedule:
      HTTP 429         -> 60s (or Retry-After header value)
      HTTP 5xx         -> 15s
      Timeout          -> 5s
      ConnectionError  -> 5s (unless DNS failure -- raise immediately)
      CircuitOpenError -> re-raise immediately (no retry)
    """

    def __init__(self, max_attempts: int = 3, registry: DomainCircuitRegistry = None):
        self.max_attempts = max_attempts
        self._registry = registry if registry is not None else _CIRCUIT_REGISTRY

    def execute(self, fn, url: str, *args, **kwargs):
        """Call fn(url, *args, **kwargs) with retry and circuit breaker logic.

        Returns the result of fn on success.
        Raises the last exception when max_attempts is exhausted.
        Raises CircuitOpenError immediately (not counted as a retry).
        Raises ConnectionError immediately for DNS failures.
        """
        domain = extract_domain(url)
        breaker = self._registry.get(domain)

        # Check circuit before starting
        if breaker.is_open():
            log.warning("Circuit open for %s -- aborting fetch before attempt", domain)
            raise CircuitOpenError(f"Circuit breaker open for domain: {domain}")

        for attempt in range(1, self.max_attempts + 1):
            # Check circuit before each attempt (may have opened mid-retry due to
            # external calls or concurrent requests in the same process)
            if breaker._state == _STATE_OPEN:
                if time.time() - breaker._opened_at < breaker.recovery_timeout:
                    log.warning("Circuit open for %s -- aborting fetch (attempt %d)", domain, attempt)
                    raise CircuitOpenError(f"Circuit breaker open for domain: {domain}")

            try:
                result = fn(url, *args, **kwargs)
                breaker.record_success()
                return result

            except CircuitOpenError:
                raise

            except requests.exceptions.HTTPError as exc:
                status = None
                if exc.response is not None:
                    status = exc.response.status_code

                if status == 429:
                    retry_after = None
                    if exc.response is not None:
                        raw = exc.response.headers.get("Retry-After")
                        if raw:
                            try:
                                retry_after = int(raw)
                            except ValueError:
                                pass
                    backoff = retry_after if retry_after is not None else 60
                    log.warning(
                        "HTTP 429 from %s (attempt %d/%d) -- backing off %ds",
                        url, attempt, self.max_attempts, backoff,
                    )
                elif status is not None and status >= 500:
                    backoff = 15
                    log.warning(
                        "HTTP %d from %s (attempt %d/%d) -- backing off %ds",
                        status, url, attempt, self.max_attempts, backoff,
                    )
                else:
                    # Non-retryable HTTP error (4xx other than 429)
                    breaker.record_failure()
                    raise

                if attempt < self.max_attempts:
                    time.sleep(backoff)
                else:
                    breaker.record_failure()
                    raise

            except requests.exceptions.Timeout as exc:
                backoff = 5
                log.warning(
                    "Timeout fetching %s (attempt %d/%d) -- backing off %ds",
                    url, attempt, self.max_attempts, backoff,
                )
                if attempt < self.max_attempts:
                    time.sleep(backoff)
                else:
                    breaker.record_failure()
                    raise

            except requests.exceptions.ConnectionError as exc:
                if _is_dns_error(exc):
                    log.warning("DNS failure fetching %s -- not retrying: %s", url, exc)
                    raise
                backoff = 5
                log.warning(
                    "Connection error fetching %s (attempt %d/%d) -- backing off %ds: %s",
                    url, attempt, self.max_attempts, backoff, exc,
                )
                if attempt < self.max_attempts:
                    time.sleep(backoff)
                else:
                    breaker.record_failure()
                    raise


# Module-level default instance for convenience
DEFAULT_RETRY_POLICY = RetryPolicy(max_attempts=3)
