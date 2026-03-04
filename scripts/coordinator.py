"""
Coordinator state manager for the multi-agent full-ingest pipeline.

Writes/reads coordinator_state.json so sub-agents can share findings
and enforce pre-conditions across concurrent runs.

Enforcement model:
  - Agents call require_clean() before each major step.
  - If a block exists for a ticker/domain, require_clean() raises BlockedError
    with the reason and suggested fix — the agent must resolve it first.
  - Blocks are written by agents when they hit hard failures (bad CIK, Cloudflare,
    502 with no fallback). They are cleared when the agent confirms the fix works.

Usage:
    import sys; sys.path.insert(0, '/Users/workstation/Documents/Hermeneutic/OffChain/miners/scripts')
    from coordinator import CoordinatorState, BlockedError
    coord = CoordinatorState('/private/tmp/claude-501/miners_progress')

    # Pre-step gate (raises BlockedError if ticker is blocked):
    coord.require_clean('MARA')

    # Block a ticker when you hit an unresolvable error:
    coord.block_ticker('HUT8', reason='CIK 0001928898 returns 404 on submissions API',
                       fix='Look up correct CIK on EDGAR and update companies.json')

    # Clear a block after fixing it:
    coord.clear_block('HUT8', resolution='Corrected CIK to 0001558370')

    # Block a domain (Cloudflare, persistent 502):
    coord.block_domain('bit-digital.com', reason='Cloudflare JS challenge, cannot scrape directly',
                       fix='Use GlobeNewswire or PRNewswire instead')

    # Check domain before fetching:
    coord.require_domain_ok('bit-digital.com')

    # Standard update/error/gap methods still work as before.
    coord.update_agent('MARA', status='running', reports_ingested=12, metrics_found=48, gaps_found=3)
    coord.log_error('ARBK', '2023-06', 'HTTP 502 on IR page')
    coord.mark_gap_filled('CLSK', '2022-11', source='globenewswire', value=198.5)
"""
import json
import logging
import os
import threading
from datetime import datetime, timezone
from pathlib import Path

log = logging.getLogger('miners.coordinator')

_LOCK = threading.Lock()


class BlockedError(RuntimeError):
    """Raised by require_clean() / require_domain_ok() when a block is active."""
    def __init__(self, subject: str, reason: str, fix: str):
        self.subject = subject
        self.reason = reason
        self.fix = fix
        super().__init__(f"BLOCKED [{subject}]: {reason} — FIX: {fix}")


class CoordinatorState:
    def __init__(self, progress_dir: str = '/private/tmp/claude-501/miners_progress'):
        self._dir = Path(progress_dir)
        self._dir.mkdir(parents=True, exist_ok=True)
        self._state_path = self._dir / 'coordinator_state.json'
        self._state = self._load()

    # ── Persistence ──────────────────────────────────────────────────────────

    def _load(self) -> dict:
        if self._state_path.exists():
            try:
                with open(self._state_path) as f:
                    return json.load(f)
            except Exception:
                pass
        return {
            'started_at': datetime.now(timezone.utc).isoformat(),
            'agents': {},
            'errors': [],
            'gaps_filled': [],
            'patterns': {},
            'blocks': {},        # ticker -> {reason, fix, blocked_at, resolved?}
            'domain_blocks': {}, # domain -> {reason, fix, blocked_at, resolved?}
            'otel_attributes': {
                'plan': 'full_ingest_v1',
                'phase': 'orchestration',
            },
        }

    def _save(self) -> None:
        # Ensure new keys exist on old state files loaded from disk
        self._state.setdefault('blocks', {})
        self._state.setdefault('domain_blocks', {})
        self._state['updated_at'] = datetime.now(timezone.utc).isoformat()
        tmp = str(self._state_path) + '.tmp'
        with open(tmp, 'w') as f:
            json.dump(self._state, f, indent=2)
        os.replace(tmp, str(self._state_path))

    # ── Enforcement: ticker blocks ────────────────────────────────────────────

    def block_ticker(self, ticker: str, reason: str, fix: str) -> None:
        """Mark a ticker as blocked. require_clean() will raise until cleared."""
        with _LOCK:
            self._state.setdefault('blocks', {})[ticker] = {
                'reason': reason,
                'fix': fix,
                'blocked_at': datetime.now(timezone.utc).isoformat(),
                'resolved': False,
            }
            self._save()
        log.error("[coordinator] BLOCKED %s: %s — FIX: %s", ticker, reason, fix)

    def clear_block(self, ticker: str, resolution: str) -> None:
        """Remove a ticker block after the agent has resolved the issue."""
        with _LOCK:
            blocks = self._state.setdefault('blocks', {})
            if ticker in blocks:
                blocks[ticker]['resolved'] = True
                blocks[ticker]['resolution'] = resolution
                blocks[ticker]['resolved_at'] = datetime.now(timezone.utc).isoformat()
            self._save()
        log.info("[coordinator] UNBLOCKED %s: %s", ticker, resolution)

    def require_clean(self, ticker: str) -> None:
        """
        Call before any major step for a ticker.
        Raises BlockedError if an unresolved block exists.
        Agents must resolve the issue and call clear_block() before retrying.
        """
        with _LOCK:
            block = self._state.get('blocks', {}).get(ticker)
        if block and not block.get('resolved'):
            raise BlockedError(ticker, block['reason'], block['fix'])

    # ── Enforcement: domain blocks ────────────────────────────────────────────

    def block_domain(self, domain: str, reason: str, fix: str) -> None:
        """Mark a domain as unfetchable (Cloudflare, persistent 502, etc.)."""
        with _LOCK:
            self._state.setdefault('domain_blocks', {})[domain] = {
                'reason': reason,
                'fix': fix,
                'blocked_at': datetime.now(timezone.utc).isoformat(),
                'resolved': False,
            }
            self._save()
        log.error("[coordinator] DOMAIN BLOCKED %s: %s — FIX: %s", domain, reason, fix)

    def clear_domain_block(self, domain: str, resolution: str) -> None:
        with _LOCK:
            db = self._state.setdefault('domain_blocks', {})
            if domain in db:
                db[domain]['resolved'] = True
                db[domain]['resolution'] = resolution
                db[domain]['resolved_at'] = datetime.now(timezone.utc).isoformat()
            self._save()
        log.info("[coordinator] DOMAIN UNBLOCKED %s: %s", domain, resolution)

    def require_domain_ok(self, domain: str) -> None:
        """
        Call before fetching from a domain.
        Raises BlockedError if the domain has an unresolved block.
        Use the alternative source specified in the fix field instead.
        """
        with _LOCK:
            block = self._state.get('domain_blocks', {}).get(domain)
        if block and not block.get('resolved'):
            raise BlockedError(domain, block['reason'], block['fix'])

    def get_domain_block(self, domain: str) -> dict:
        """Return block info for a domain, or empty dict if not blocked."""
        with _LOCK:
            return dict(self._state.get('domain_blocks', {}).get(domain, {}))

    # ── Agent tracking ────────────────────────────────────────────────────────

    def update_agent(self, ticker: str, **kwargs) -> None:
        """Update per-agent state: status, findings, patterns, counts."""
        with _LOCK:
            state = self._state
            if ticker not in state['agents']:
                state['agents'][ticker] = {'ticker': ticker, 'created_at': datetime.now(timezone.utc).isoformat()}
            state['agents'][ticker].update(kwargs)
            state['agents'][ticker]['updated_at'] = datetime.now(timezone.utc).isoformat()
            self._save()
        log.info("[coordinator] %s: %s", ticker, kwargs)

    def get_agent(self, ticker: str) -> dict:
        with _LOCK:
            return dict(self._state['agents'].get(ticker, {}))

    # ── Error logging ────────────────────────────────────────────────────────

    def log_error(self, ticker: str, period: str, error: str, url: str = '') -> None:
        with _LOCK:
            self._state['errors'].append({
                'ticker': ticker,
                'period': period,
                'error': error,
                'url': url,
                'at': datetime.now(timezone.utc).isoformat(),
            })
            self._save()
        log.warning("[coordinator] error %s %s: %s", ticker, period, error)

    # ── Gap tracking ─────────────────────────────────────────────────────────

    def mark_gap_filled(self, ticker: str, period: str, source: str, value: float = None) -> None:
        with _LOCK:
            self._state['gaps_filled'].append({
                'ticker': ticker,
                'period': period,
                'source': source,
                'value': value,
                'at': datetime.now(timezone.utc).isoformat(),
            })
            self._save()

    # ── Pattern sharing ───────────────────────────────────────────────────────

    def share_pattern(self, ticker: str, pattern_type: str, pattern: dict) -> None:
        """Relay a pattern finding so other agents can benefit from it."""
        with _LOCK:
            if ticker not in self._state['patterns']:
                self._state['patterns'][ticker] = {}
            self._state['patterns'][ticker][pattern_type] = pattern
            self._save()
        log.info("[coordinator] pattern shared %s/%s: %s", ticker, pattern_type, pattern)

    def get_all_patterns(self) -> dict:
        with _LOCK:
            return dict(self._state['patterns'])

    # ── Summary ───────────────────────────────────────────────────────────────

    def summary(self) -> dict:
        with _LOCK:
            agents = self._state['agents']
            blocks = self._state.get('blocks', {})
            domain_blocks = self._state.get('domain_blocks', {})
            active_blocks = {k: v for k, v in blocks.items() if not v.get('resolved')}
            active_domain_blocks = {k: v for k, v in domain_blocks.items() if not v.get('resolved')}
            return {
                'total_agents': len(agents),
                'running': sum(1 for a in agents.values() if a.get('status') == 'running'),
                'done': sum(1 for a in agents.values() if a.get('status') == 'done'),
                'error': sum(1 for a in agents.values() if a.get('status') == 'error'),
                'total_errors': len(self._state['errors']),
                'gaps_filled': len(self._state['gaps_filled']),
                'patterns_shared': sum(len(v) for v in self._state['patterns'].values()),
                'active_blocks': active_blocks,
                'active_domain_blocks': active_domain_blocks,
                'agents_detail': {
                    t: {k: v for k, v in a.items() if k in
                        ('status', 'reports_ingested', 'metrics_found', 'gaps_found', 'updated_at')}
                    for t, a in agents.items()
                },
            }

    def print_summary(self) -> None:
        s = self.summary()
        print(f"\n{'='*60}")
        print(f"COORDINATOR SUMMARY")
        print(f"{'='*60}")
        print(f"Agents: {s['total_agents']} total  ({s['running']} running, {s['done']} done, {s['error']} error)")
        print(f"Errors logged: {s['total_errors']}")
        print(f"Gaps filled: {s['gaps_filled']}")
        print(f"Patterns shared: {s['patterns_shared']}")
        if s['active_blocks']:
            print(f"\nACTIVE TICKER BLOCKS ({len(s['active_blocks'])}):")
            for ticker, b in s['active_blocks'].items():
                print(f"  {ticker}: {b['reason']}")
                print(f"    FIX: {b['fix']}")
        if s['active_domain_blocks']:
            print(f"\nACTIVE DOMAIN BLOCKS ({len(s['active_domain_blocks'])}):")
            for domain, b in s['active_domain_blocks'].items():
                print(f"  {domain}: {b['reason']}")
                print(f"    FIX: {b['fix']}")
        print(f"\nPer-agent status:")
        for ticker, detail in sorted(s['agents_detail'].items()):
            print(f"  {ticker:<6} {detail.get('status','?'):<10} "
                  f"reports={detail.get('reports_ingested','?')} "
                  f"metrics={detail.get('metrics_found','?')} "
                  f"gaps={detail.get('gaps_found','?')}")
        print(f"{'='*60}\n")
