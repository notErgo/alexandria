"""LLM-driven web crawler for bitcoin miner IR press releases.

Uses the Anthropic SDK with two custom tools:
  fetch_url(url)                           -- HTTP GET + BS4 text extraction
  store_document(ticker, url, text, ...)   -- insert via /api/ingest/raw

The crawler reads a pre-generated crawl prompt from
  scripts/crawl_prompts/{TICKER}_crawl.md
and runs an agentic tool-use loop until the model signals end_turn or the
iteration ceiling is hit.

Module-level registry:
  start_crawl(tickers, task_id?)   -> dict[ticker, CrawlProgress]
  get_crawl_status()               -> list[snapshot_dict]
  get_crawl_task(task_id)          -> dict[ticker, snapshot_dict] | None
"""
import json
import logging
import threading
import urllib.parse as _urllib_parse
import uuid
from collections import deque
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import requests as _requests
from bs4 import BeautifulSoup

log = logging.getLogger('miners.crawl')

_PREFERRED_MODELS = ['qwen3.5:9b', 'qwen3.5:27b']

# ---------------------------------------------------------------------------
# Paths & constants
# ---------------------------------------------------------------------------
_REPO_ROOT = Path(__file__).parent.parent.parent
_CRAWL_PROMPTS_DIR = _REPO_ROOT / 'scripts' / 'crawl_prompts'
_INGEST_RAW_URL = 'http://127.0.0.1:5004/api/ingest/raw'

_MAX_FETCH_CHARS = 12_000   # chars returned to model per page fetch
_MAX_ITERATIONS  = 80       # safety ceiling per ticker
_DEFAULT_NUM_CTX = 32_768   # Ollama context window tokens; override via crawl_num_ctx config
_PRUNE_THRESHOLD_CHARS = 500   # fetch results shorter than this are not worth pruning
_PRUNE_KEEP_RECENT_ROUNDS = 2  # keep the last N assistant turns verbatim; prune older fetch results
_INDEXED_REFRESH_EVERY = 10    # re-inject DB index note every N iterations


def _estimate_ctx(messages: list) -> tuple[int, int]:
    """Return (total_chars, estimated_tokens) for a messages list.

    Counts string content fields plus serialized tool_calls.
    Token estimate uses a 4 chars/token heuristic — good enough for a
    visual sanity check; not a tokenizer count.
    """
    chars = 0
    for m in messages:
        c = m.get('content')
        if isinstance(c, str):
            chars += len(c)
        elif isinstance(c, list):
            for block in c:
                if isinstance(block, dict):
                    v = block.get('content', '')
                    chars += len(v) if isinstance(v, str) else len(str(v))
        tcs = m.get('tool_calls')
        if tcs:
            chars += len(str(tcs))
    return chars, chars // 4

# ---------------------------------------------------------------------------
# Module-level task registry
# ---------------------------------------------------------------------------
# _TASKS: task_id -> dict[ticker -> CrawlProgress]
_TASKS: dict = {}
_TASKS_LOCK = threading.Lock()

# Limit concurrent ticker crawls. For Ollama, set OLLAMA_NUM_PARALLEL on the
# server to match this value so requests are processed simultaneously rather
# than queued. For Anthropic API, keep this low to respect rate limits.
_SEMAPHORE = threading.Semaphore(4)


# ---------------------------------------------------------------------------
# CrawlProgress
# ---------------------------------------------------------------------------
class CrawlProgress:
    """Thread-safe per-ticker crawl progress tracker."""

    def __init__(self, ticker: str) -> None:
        self.ticker = ticker
        self.status: str = 'pending'          # pending | running | complete | failed | stopped
        self.pages_fetched: int = 0
        self.docs_stored: int = 0
        self.docs_skipped: int = 0
        self.error: Optional[str] = None
        self.started_at: Optional[str] = None
        self.finished_at: Optional[str] = None
        self.api_calls: int = 0
        self.ctx_tokens: int = 0
        self.ctx_limit: int = 0
        self._logs: deque = deque(maxlen=150)
        self._lock = threading.Lock()
        self._stop_event = threading.Event()

    def request_stop(self) -> None:
        self._stop_event.set()

    @property
    def stop_requested(self) -> bool:
        return self._stop_event.is_set()

    def add_log(self, msg: str) -> None:
        ts = datetime.now(timezone.utc).strftime('%H:%M:%S')
        with self._lock:
            self._logs.appendleft(f'[{ts}] {msg}')
        log.debug('crawl.%s %s', self.ticker, msg)

    def snapshot(self) -> dict:
        with self._lock:
            return {
                'ticker': self.ticker,
                'status': self.status,
                'pages_fetched': self.pages_fetched,
                'docs_stored': self.docs_stored,
                'docs_skipped': self.docs_skipped,
                'error': self.error,
                'started_at': self.started_at,
                'finished_at': self.finished_at,
                'api_calls': self.api_calls,
                'ctx_tokens': self.ctx_tokens,
                'ctx_limit': self.ctx_limit,
                'log': list(self._logs),
            }


# ---------------------------------------------------------------------------
# Tool definitions (passed to Anthropic SDK)
# ---------------------------------------------------------------------------
_FETCH_URL_TOOL = {
    'name': 'fetch_url',
    'description': (
        'Fetch a URL and return its visible text content. '
        'Use for IR listing pages, pagination pages, and individual press release pages. '
        'Returns up to 12 000 characters of visible text.'
    ),
    'input_schema': {
        'type': 'object',
        'properties': {
            'url': {'type': 'string', 'description': 'The URL to fetch'},
        },
        'required': ['url'],
    },
}

_ERROR_PHRASES = ('404 not found', 'page not found', 'access denied', 'not found')

_STORE_DOCUMENT_TOOL = {
    'name': 'store_document',
    'description': (
        'Store a collected press release or IR document for ingestion into the mining data pipeline. '
        'Call this once per individual article page — not for listing/index pages. '
        'Returns {"status": "ingested" | "skipped" | "error"}.'
    ),
    'input_schema': {
        'type': 'object',
        'properties': {
            'ticker': {
                'type': 'string',
                'description': 'Company ticker symbol, e.g. MARA',
            },
            'url': {
                'type': 'string',
                'description': 'Canonical source URL of the document',
            },
            'text': {
                'type': 'string',
                'description': 'Full visible text of the press release page',
            },
            'source_type': {
                'type': 'string',
                'enum': [
                    'ir_press_release',
                    'prnewswire_press_release',
                    'globenewswire_press_release',
                    'edgar_8k',
                    'wire_press_release',
                ],
                'description': 'Document source channel',
            },
        },
        'required': ['ticker', 'url', 'text', 'source_type'],
    },
}


_WEB_SEARCH_TOOL = {
    'name': 'web_search',
    'description': (
        'Search the web for press releases on GlobeNewswire, PRNewswire, '
        'and other wire services. Returns up to 10 results as title + URL pairs. '
        'Follow up with fetch_url to retrieve individual pages.'
    ),
    'input_schema': {
        'type': 'object',
        'properties': {
            'query': {'type': 'string', 'description': 'Search query string'},
        },
        'required': ['query'],
    },
}

_FETCH_TOOL_OAI = {
    'type': 'function',
    'function': {
        'name': 'fetch_url',
        'description': _FETCH_URL_TOOL['description'],
        'parameters': _FETCH_URL_TOOL['input_schema'],
    },
}
_STORE_TOOL_OAI = {
    'type': 'function',
    'function': {
        'name': 'store_document',
        'description': _STORE_DOCUMENT_TOOL['description'],
        'parameters': _STORE_DOCUMENT_TOOL['input_schema'],
    },
}
_WEB_SEARCH_TOOL_OAI = {
    'type': 'function',
    'function': {
        'name': 'web_search',
        'description': _WEB_SEARCH_TOOL['description'],
        'parameters': _WEB_SEARCH_TOOL['input_schema'],
    },
}

_GET_INDEXED_DOCS_TOOL = {
    'name': 'get_indexed_docs',
    'description': (
        'Query the database for documents already indexed for a ticker. '
        'Call this before crawling a domain to avoid re-fetching stored pages. '
        'Pass domain to filter to a specific site (e.g. "ir.mara.com"). '
        'Returns path fragments, period, and source_type for up to 60 matching docs.'
    ),
    'input_schema': {
        'type': 'object',
        'properties': {
            'ticker': {'type': 'string', 'description': 'Company ticker, e.g. MARA'},
            'domain': {
                'type': 'string',
                'description': 'Optional domain filter, e.g. "ir.mara.com". Omit to see all domains.',
            },
        },
        'required': ['ticker'],
    },
}
_GET_INDEXED_DOCS_TOOL_OAI = {
    'type': 'function',
    'function': {
        'name': 'get_indexed_docs',
        'description': _GET_INDEXED_DOCS_TOOL['description'],
        'parameters': _GET_INDEXED_DOCS_TOOL['input_schema'],
    },
}

_STORE_OBSERVATION_TOOL = {
    'name': 'store_observation',
    'description': (
        'Persist a structured observation about a company\'s IR site or data source. '
        'Use this to record site patterns, pagination behaviour, known dead-end URLs, '
        'coverage gaps, or any other knowledge that should survive across crawl sessions. '
        'Observations are injected back into the prompt on the next crawl so you do not '
        'need to rediscover site behaviour from scratch. '
        'key should be a stable dot-separated identifier, e.g. "ir.mara.com.pagination".'
    ),
    'input_schema': {
        'type': 'object',
        'properties': {
            'ticker': {'type': 'string', 'description': 'Company ticker, e.g. MARA'},
            'key':    {'type': 'string', 'description': 'Stable identifier, e.g. "ir.mara.com.pagination"'},
            'value':  {'type': 'string', 'description': 'Free-text observation to persist'},
        },
        'required': ['ticker', 'key', 'value'],
    },
}
_STORE_OBSERVATION_TOOL_OAI = {
    'type': 'function',
    'function': {
        'name': 'store_observation',
        'description': _STORE_OBSERVATION_TOOL['description'],
        'parameters': _STORE_OBSERVATION_TOOL['input_schema'],
    },
}

_GET_OBSERVATIONS_TOOL = {
    'name': 'get_observations',
    'description': (
        'Retrieve all previously stored observations for a ticker. '
        'Call this at the start of a crawl or when you need to recall site-specific '
        'knowledge from previous sessions.'
    ),
    'input_schema': {
        'type': 'object',
        'properties': {
            'ticker': {'type': 'string', 'description': 'Company ticker, e.g. MARA'},
        },
        'required': ['ticker'],
    },
}
_GET_OBSERVATIONS_TOOL_OAI = {
    'type': 'function',
    'function': {
        'name': 'get_observations',
        'description': _GET_OBSERVATIONS_TOOL['description'],
        'parameters': _GET_OBSERVATIONS_TOOL['input_schema'],
    },
}


# ---------------------------------------------------------------------------
# Ollama lifecycle helpers
# ---------------------------------------------------------------------------

def _ensure_ollama(base_url: str, model: str, progress: 'CrawlProgress') -> None:
    """Restart Ollama with OLLAMA_NUM_PARALLEL set, then warm the target model."""
    import os
    import subprocess
    import time

    tags_url = f'{base_url}/api/tags'
    num_parallel = _SEMAPHORE._value

    def _is_up() -> bool:
        try:
            return _requests.get(tags_url, timeout=3).ok
        except Exception:
            return False

    # Always restart so OLLAMA_NUM_PARALLEL takes effect.
    if _is_up():
        progress.add_log('Stopping existing Ollama server...')
        subprocess.call(
            ['pkill', '-x', 'ollama'],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        for _ in range(10):
            time.sleep(1)
            if not _is_up():
                break

    progress.add_log(f'Starting Ollama server (OLLAMA_NUM_PARALLEL={num_parallel})...')
    env = os.environ.copy()
    env['OLLAMA_NUM_PARALLEL'] = str(num_parallel)
    try:
        subprocess.Popen(
            ['ollama', 'serve'],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            env=env,
        )
    except FileNotFoundError:
        raise RuntimeError('ollama not found on PATH — install Ollama first')
    for _ in range(30):
        time.sleep(1)
        if _is_up():
            progress.add_log(f'Ollama server ready (OLLAMA_NUM_PARALLEL={num_parallel})')
            break
    else:
        raise RuntimeError('Ollama did not start within 30 seconds')

    progress.add_log(f'Warming model {model}...')
    try:
        r = _requests.post(
            f'{base_url}/api/chat',
            json={'model': model, 'messages': [{'role': 'user', 'content': 'hi'}], 'stream': False},
            timeout=120,
        )
        r.raise_for_status()
        progress.add_log(f'Model {model} loaded and ready')
    except Exception as exc:
        progress.add_log(f'Warm-up warning (non-fatal): {exc}')


# ---------------------------------------------------------------------------
# LLMCrawler
# ---------------------------------------------------------------------------
class LLMCrawler:
    """Executes an agentic crawl for a single ticker."""

    def __init__(
        self,
        progress: CrawlProgress,
        api_key: str,
        model: str,
        provider: str = 'ollama',
        prompt_override: Optional[str] = None,
        max_iterations: int = _MAX_ITERATIONS,
        max_fetch_chars: int = _MAX_FETCH_CHARS,
        num_ctx: int = _DEFAULT_NUM_CTX,
        context_block: Optional[str] = None,
        db=None,
    ) -> None:
        self._progress = progress
        self._api_key = api_key
        self._model = model
        self._provider = provider
        self._prompt_override = prompt_override
        self._max_iterations = max_iterations
        self._max_fetch_chars = max_fetch_chars
        self._num_ctx = num_ctx
        self._context_block = context_block
        self._db = db
        self._session = _requests.Session()
        self._seen_urls: set = set()    # URLs passed to store_document (dedup guard)
        self._fetched_urls: set = set() # URLs passed to fetch_url (re-fetch detection)
        self._raw_html_cache: dict = {}  # url → raw HTML for viewer storage
        self._prev_tool_sigs: set = set()  # tool signatures from previous iteration
        self._tool_repeat_streak: int = 0  # consecutive iterations with repeated calls
        self._session.headers.update({
            'User-Agent': (
                'Hermeneutic Research Platform/1.0 '
                '(bitcoin-miner-research; contact: research@hermeneutic.io)'
            )
        })

    # ------------------------------------------------------------------
    # Indexed-docs note (DB ground truth injected into prompt)
    # ------------------------------------------------------------------
    def _build_indexed_note(self, ticker: str) -> Optional[str]:
        """Query the DB for all already-indexed documents for this ticker and
        return a compact note to inject into the conversation.

        Full URLs are replaced with 8-char SHA-256 prefixes (like git short
        hashes) to keep the note small.  The model cannot re-derive the URL
        from the hash, but it does not need to — store_document will dedup on
        the full URL anyway.  The note exists solely to signal 'these are
        already covered; do not hunt for them again'.
        """
        if self._db is None:
            return None
        try:
            docs = self._db.get_indexed_urls_for_ticker(ticker)
        except Exception:
            return None
        if not docs:
            return None
        # Build a compact domain summary — one line per domain.
        # Full per-URL detail is available on demand via the get_indexed_docs tool.
        domain_map: dict = {}  # domain -> {periods, source_types, count}
        for d in docs:
            url = d.get('source_url') or ''
            domain = _urllib_parse.urlparse(url).netloc or '(unknown)'
            entry = domain_map.setdefault(domain, {'periods': [], 'count': 0})
            entry['count'] += 1
            p = d.get('covering_period')
            if p:
                entry['periods'].append(p)
        lines = [
            f'[DB INDEX — {ticker} already has {len(docs)} doc(s) indexed. '
            f'Call get_indexed_docs(ticker, domain) before fetching from any domain to avoid re-work.]'
        ]
        for domain, info in sorted(domain_map.items()):
            periods = sorted(set(info['periods']))
            span = f'{periods[0]}–{periods[-1]}' if len(periods) > 1 else (periods[0] if periods else '?')
            lines.append(f'  {domain}: {info["count"]} docs, {span}')
        lines.append('[End of DB summary]')
        return '\n'.join(lines)

    # ------------------------------------------------------------------
    # Tool handlers
    # ------------------------------------------------------------------
    def _tool_get_indexed_docs(self, ticker: str, domain: Optional[str] = None) -> str:
        """Return path fragments of already-indexed docs for a ticker, optionally filtered by domain.

        Capped at 60 entries to prevent context explosion.
        """
        if self._db is None:
            return '{"error": "DB not available"}'
        try:
            docs = self._db.get_indexed_urls_for_ticker(ticker)
        except Exception as exc:
            return f'{{"error": "{exc}"}}'
        if domain:
            domain_lower = domain.lower().lstrip('www.')
            docs = [
                d for d in docs
                if domain_lower in (_urllib_parse.urlparse(d.get('source_url') or '').netloc or '').lower()
            ]
        cap = 60
        truncated = len(docs) > cap
        results = []
        for d in docs[:cap]:
            url = d.get('source_url') or ''
            parsed = _urllib_parse.urlparse(url)
            path_frag = (parsed.netloc + parsed.path).rstrip('/')[-80:]
            results.append({
                'path': f'...{path_frag}',
                'period': d.get('covering_period') or '?',
                'type': d.get('source_type') or '?',
            })
        out: dict = {'ticker': ticker, 'count': len(docs), 'docs': results}
        if truncated:
            out['note'] = f'Showing first {cap} of {len(docs)} — pass domain= to narrow results'
        self._progress.add_log(
            f'get_indexed_docs: {len(results)} entries returned'
            + (f' (domain={domain})' if domain else '')
        )
        return json.dumps(out)

    def _tool_store_observation(self, ticker: str, key: str, value: str) -> str:
        if self._db is None:
            return '{"status": "error", "reason": "DB not available"}'
        try:
            self._db.upsert_crawl_observation(ticker, key, value)
            self._progress.add_log(f'observation stored: {key}')
            return json.dumps({'status': 'stored', 'key': key})
        except Exception as exc:
            self._progress.add_log(f'store_observation ERROR {key}: {exc}')
            return json.dumps({'status': 'error', 'reason': str(exc)[:200]})

    def _tool_get_observations(self, ticker: str) -> str:
        if self._db is None:
            return '{"observations": []}'
        try:
            rows = self._db.get_crawl_observations(ticker)
            self._progress.add_log(f'get_observations: {len(rows)} entries returned')
            return json.dumps({'ticker': ticker, 'observations': rows})
        except Exception as exc:
            self._progress.add_log(f'get_observations ERROR: {exc}')
            return json.dumps({'observations': [], 'error': str(exc)[:200]})

    def _tool_fetch_url(self, url: str) -> str:
        if url in self._fetched_urls:
            self._progress.add_log(
                f'WARN: re-fetching already-visited URL — possible context degradation: {url}'
            )
        self._fetched_urls.add(url)
        try:
            resp = self._session.get(url, timeout=20)
            resp.raise_for_status()
            self._raw_html_cache[url] = resp.text[:300_000]
            soup = BeautifulSoup(resp.text, 'lxml')
            for tag in soup(['script', 'style', 'nav', 'header', 'footer']):
                tag.decompose()
            text = soup.get_text(separator='\n', strip=True)
            text = '\n'.join(line for line in text.splitlines() if line.strip())
            with self._progress._lock:
                self._progress.pages_fetched += 1
            self._progress.add_log(f'fetch_url OK ({len(text)} chars): {url}')
            # Extract all hrefs before truncation so pagination links survive the char budget.
            all_hrefs = []
            seen_hrefs: set = set()
            for a in soup.find_all('a', href=True):
                h = a['href'].strip()
                if h and h not in seen_hrefs:
                    seen_hrefs.add(h)
                    all_hrefs.append(h)
            if all_hrefs:
                links_section = 'LINKS ON PAGE:\n' + '\n'.join(all_hrefs)
                budget = self._max_fetch_chars - len(links_section) - 1
                if budget > 0:
                    return text[:budget] + '\n' + links_section
                return links_section[:self._max_fetch_chars]
            return text[:self._max_fetch_chars]
        except Exception as exc:
            self._progress.add_log(f'fetch_url ERROR {url}: {exc}')
            return f'ERROR: {exc}'

    def _tool_store_document(
        self, ticker: str, url: str, text: str, source_type: str
    ) -> dict:
        text_lower = text.lower()
        if any(p in text_lower for p in _ERROR_PHRASES):
            with self._progress._lock:
                self._progress.docs_skipped += 1
            self._progress.add_log(f'store_document skipped (error page): {url}')
            return {'status': 'skipped', 'reason': 'error_page'}
        if len(text.split()) < 50:
            with self._progress._lock:
                self._progress.docs_skipped += 1
            self._progress.add_log(
                f'store_document skipped (insufficient_content, {len(text.split())} words): {url}'
            )
            return {'status': 'skipped', 'reason': 'insufficient_content'}
        if url in self._seen_urls:
            with self._progress._lock:
                self._progress.docs_skipped += 1
            self._progress.add_log(f'store_document skipped (intra-crawl duplicate): {url}')
            return {'status': 'skipped'}
        self._seen_urls.add(url)
        try:
            payload = {
                'documents': [{
                    'ticker': ticker.upper(),
                    'source_url': url,
                    'raw_text': text,
                    'raw_html': self._raw_html_cache.get(url),
                    'source_type': source_type,
                }]
            }
            resp = self._session.post(_INGEST_RAW_URL, json=payload, timeout=15)
            body = resp.json()
            if not resp.ok or not body.get('success'):
                raise ValueError(body)
            data = body.get('data', {})
            ingested = data.get('ingested', 0)
            if ingested > 0:
                with self._progress._lock:
                    self._progress.docs_stored += 1
                self._progress.add_log(f'stored: {url}')
                return {'status': 'ingested'}
            else:
                with self._progress._lock:
                    self._progress.docs_skipped += 1
                self._progress.add_log(f'skipped (duplicate): {url}')
                return {'status': 'skipped'}
        except Exception as exc:
            self._progress.add_log(f'store_document ERROR {url}: {exc}')
            return {'status': 'error', 'error': str(exc)[:200]}

    def _tool_web_search(self, query: str) -> str:
        try:
            resp = self._session.get(
                'https://html.duckduckgo.com/html/',
                params={'q': query},
                timeout=15,
            )
            resp.raise_for_status()
            soup = BeautifulSoup(resp.text, 'lxml')
            results = []
            for a in soup.select('a.result__a')[:10]:
                title = a.get_text(strip=True)
                href = a.get('href', '')
                if href:
                    results.append(f'{title}\n{href}')
            if not results:
                return 'No results found for query.'
            self._progress.add_log(f'web_search OK ({len(results)} results): {query}')
            return '\n\n'.join(results)
        except Exception as exc:
            self._progress.add_log(f'web_search ERROR: {exc}')
            return f'ERROR: {exc}'

    # ------------------------------------------------------------------
    # Parallel tool dispatch
    # ------------------------------------------------------------------
    def _dispatch_tool(self, name: str, inp: dict, ticker: str) -> tuple:
        """Execute a single tool call and return (name, primary_arg, content, is_fetch, fetch_url).

        Designed to be called from a thread pool so multiple tool calls within
        one model response execute concurrently (network I/O bound).
        """
        primary_arg = inp.get('url') or inp.get('query') or inp.get('key') or ''
        is_fetch = False
        fetch_url_val = ''

        if name == 'fetch_url':
            url = inp.get('url', '')
            result_content = self._tool_fetch_url(url)
            is_fetch = True
            fetch_url_val = url
        elif name == 'store_document':
            result = self._tool_store_document(
                inp.get('ticker', ticker),
                inp.get('url', ''),
                inp.get('text', ''),
                inp.get('source_type', 'ir_press_release'),
            )
            result_content = json.dumps(result)
        elif name == 'web_search':
            result_content = self._tool_web_search(inp.get('query', ''))
        elif name == 'get_indexed_docs':
            result_content = self._tool_get_indexed_docs(
                inp.get('ticker', ticker), inp.get('domain')
            )
        elif name == 'store_observation':
            result_content = self._tool_store_observation(
                inp.get('ticker', ticker), inp.get('key', ''), inp.get('value', '')
            )
        elif name == 'get_observations':
            result_content = self._tool_get_observations(inp.get('ticker', ticker))
        else:
            result_content = f'Unknown tool: {name}'

        return (name, primary_arg, result_content, is_fetch, fetch_url_val)

    def _run_tools_parallel(
        self,
        tool_calls_parsed: list,
        ticker: str,
    ) -> tuple:
        """Execute a list of parsed tool calls in parallel.

        tool_calls_parsed: list of (name, inp) tuples in call order.
        Returns (ordered_results, cur_tool_sigs, fetch_info) where:
          ordered_results: list of result strings in original call order
          cur_tool_sigs:   set of (name, primary_arg) for repeat detection
          fetch_info:      list of (original_index, url) for fetch_url calls
        """
        n = len(tool_calls_parsed)
        if n == 0:
            return [], set(), []

        # Use a thread per tool call; cap at 8 to avoid overwhelming the server
        workers = min(n, 8)
        ordered_results = [None] * n
        cur_tool_sigs: set = set()
        fetch_info = []  # (original_index, url)

        with ThreadPoolExecutor(max_workers=workers) as pool:
            future_to_idx = {
                pool.submit(self._dispatch_tool, name, inp, ticker): i
                for i, (name, inp) in enumerate(tool_calls_parsed)
            }
            for future in as_completed(future_to_idx):
                i = future_to_idx[future]
                name, primary_arg, result_content, is_fetch, fetch_url_val = future.result()
                ordered_results[i] = result_content
                cur_tool_sigs.add((name, primary_arg))
                if is_fetch:
                    fetch_info.append((i, fetch_url_val))

        return ordered_results, cur_tool_sigs, fetch_info

    # ------------------------------------------------------------------
    # Context management
    # ------------------------------------------------------------------
    def _prune_old_fetches(
        self,
        messages: list,
        fetch_indices: dict,
        keep_recent_rounds: int = _PRUNE_KEEP_RECENT_ROUNDS,
    ) -> None:
        """Replace old fetch_url result content with a compact placeholder.

        Keeps the last `keep_recent_rounds` assistant turns verbatim so the
        model retains recent context. Everything older is collapsed to a
        single-line summary, bounding context growth to O(active_batch)
        rather than O(total_fetched).

        fetch_indices: maps message-list index -> URL for each fetch_url result.
        """
        asst_positions = [
            i for i, m in enumerate(messages)
            if m.get('role') == 'assistant'
        ]
        if len(asst_positions) <= keep_recent_rounds:
            return  # not enough rounds elapsed yet

        # Everything before this message index is old enough to compress
        cutoff = asst_positions[-keep_recent_rounds]

        pruned = 0
        chars_freed = 0
        for idx, url in fetch_indices.items():
            if idx >= cutoff:
                continue  # recent round — leave verbatim
            content = messages[idx].get('content', '')
            if len(content) <= _PRUNE_THRESHOLD_CHARS:
                continue  # already short (pagination page, error, etc.)
            chars_freed += len(content)
            messages[idx]['content'] = f'[fetch_url result pruned — already processed: {url}]'
            pruned += 1

        if pruned:
            self._progress.add_log(
                f'Context pruned: {pruned} old fetch result(s) '
                f'(~{chars_freed // 1000}k chars freed)'
            )

    def _prune_old_fetches_anthropic(
        self,
        messages: list,
        fetch_tool_ids: dict,
        current_round: int,
        keep_recent_rounds: int = _PRUNE_KEEP_RECENT_ROUNDS,
    ) -> None:
        """Prune old fetch_url results from Anthropic-format messages.

        fetch_tool_ids: maps tool_use_id -> round_number for fetch_url calls.
        Replaces content of tool_result blocks that are older than
        keep_recent_rounds with a compact placeholder.
        """
        cutoff_round = current_round - keep_recent_rounds
        if cutoff_round <= 0:
            return

        old_ids = {tid for tid, rnd in fetch_tool_ids.items() if rnd <= cutoff_round}
        if not old_ids:
            return

        pruned = 0
        chars_freed = 0
        for msg in messages:
            if msg.get('role') != 'user':
                continue
            content = msg.get('content')
            if not isinstance(content, list):
                continue
            for block in content:
                if block.get('type') != 'tool_result':
                    continue
                if block.get('tool_use_id') not in old_ids:
                    continue
                old_content = block.get('content', '')
                if len(old_content) <= _PRUNE_THRESHOLD_CHARS:
                    continue
                chars_freed += len(old_content)
                block['content'] = '[fetch_url result pruned — already processed]'
                pruned += 1

        if pruned:
            self._progress.add_log(
                f'Context pruned: {pruned} old fetch result(s) '
                f'(~{chars_freed // 1000}k chars freed)'
            )

    # ------------------------------------------------------------------
    # Main loop
    # ------------------------------------------------------------------
    def run(self) -> None:
        ticker = self._progress.ticker

        # Resolve system prompt: override takes precedence over per-ticker file.
        if self._prompt_override:
            system_prompt = self._prompt_override
        else:
            prompt_path = _CRAWL_PROMPTS_DIR / f'{ticker}_crawl.md'
            if not prompt_path.exists():
                self._progress.status = 'failed'
                self._progress.error = f'No crawl prompt found at {prompt_path}'
                self._progress.add_log(f'ERROR: missing crawl prompt {prompt_path}')
                return
            system_prompt = prompt_path.read_text()

        # Prepend auto-detected coverage context (lower bound + gaps) if available.
        if self._context_block:
            system_prompt = self._context_block + system_prompt
            self._progress.add_log('Coverage context block prepended to system prompt')

        self._progress.status = 'running'
        self._progress.started_at = datetime.now(timezone.utc).isoformat()
        self._progress.add_log(
            f'Starting crawl for {ticker} via {self._provider}'
            + (f' model={self._model}' if self._provider == 'ollama' else '')
        )

        if self._provider == 'ollama':
            self._run_ollama(ticker, system_prompt)
        else:
            self._run_anthropic(ticker, system_prompt)

    def _build_messages_and_tools(self, ticker: str, system_prompt: str):
        messages = [
            {
                'role': 'user',
                'content': (
                    f'Execute the crawl plan for {ticker}. '
                    'Use fetch_url to navigate pages, then store_document for each press release you find. '
                    'When you have collected all available documents, stop.'
                ),
            }
        ]
        tools = [
            _FETCH_URL_TOOL, _STORE_DOCUMENT_TOOL, _WEB_SEARCH_TOOL,
            _GET_INDEXED_DOCS_TOOL, _STORE_OBSERVATION_TOOL, _GET_OBSERVATIONS_TOOL,
        ]
        return messages, tools

    def _run_anthropic(self, ticker: str, system_prompt: str) -> None:
        import anthropic

        client = anthropic.Anthropic(api_key=self._api_key)
        messages, tools = self._build_messages_and_tools(ticker, system_prompt)
        # Maps tool_use_id -> round number for fetch_url calls (Anthropic format).
        fetch_tool_ids: dict = {}

        try:
            for iteration in range(self._max_iterations):
                if self._progress.stop_requested:
                    self._progress.add_log('Stop requested — aborting crawl')
                    self._progress.status = 'stopped'
                    return
                if iteration % _INDEXED_REFRESH_EVERY == 0:
                    note = self._build_indexed_note(ticker)
                    if note:
                        messages.append({'role': 'user', 'content': note})
                        self._progress.add_log(
                            f'DB index injected ({note.count(chr(10))} entries)'
                        )
                if iteration == 0:
                    obs = self._tool_get_observations(ticker)
                    obs_data = json.loads(obs)
                    if obs_data.get('observations'):
                        messages.append({'role': 'user', 'content':
                            f'[Observations from previous crawl sessions for {ticker}:\n{obs}\n'
                            'Use these to skip re-discovery work.]'
                        })
                self._prune_old_fetches_anthropic(messages, fetch_tool_ids, iteration)
                ctx_chars, ctx_tokens = _estimate_ctx(messages)
                ctx_pct = ctx_tokens / self._num_ctx if self._num_ctx else 0
                self._progress.api_calls = iteration + 1
                self._progress.ctx_tokens = ctx_tokens
                self._progress.ctx_limit = self._num_ctx
                self._progress.add_log(
                    f'API call #{iteration + 1} | '
                    f'ctx {ctx_chars // 1000}k chars ~{ctx_tokens:,} tokens'
                )
                if ctx_pct >= 0.85:
                    self._progress.add_log(
                        f'WARN: context at {ctx_pct:.0%} of limit '
                        f'({self._num_ctx - ctx_tokens:,} tokens remaining) — '
                        'model may start losing earlier conversation'
                    )
                response = client.messages.create(
                    model=self._model,
                    max_tokens=4096,
                    system=system_prompt,
                    messages=messages,
                    tools=tools,
                )

                messages.append({'role': 'assistant', 'content': response.content})

                if response.stop_reason == 'end_turn':
                    self._progress.add_log('Model signaled end_turn — crawl complete')
                    break

                # Collect tool_use blocks preserving id and order
                tool_use_blocks = [b for b in response.content if b.type == 'tool_use']
                parsed_calls = [(b.name, b.input) for b in tool_use_blocks]
                block_ids = [b.id for b in tool_use_blocks]

                if len(parsed_calls) > 1:
                    self._progress.add_log(
                        f'Running {len(parsed_calls)} tool calls in parallel'
                    )

                ordered_results, cur_tool_sigs, fetch_info = self._run_tools_parallel(
                    parsed_calls, ticker
                )

                # Record fetch tool ids for context pruning
                for local_idx, _ in fetch_info:
                    fetch_tool_ids[block_ids[local_idx]] = iteration

                tool_results = [
                    {
                        'type': 'tool_result',
                        'tool_use_id': block_ids[i],
                        'content': ordered_results[i],
                    }
                    for i in range(len(ordered_results))
                ]

                if not tool_results:
                    self._progress.add_log('No tool calls in response — stopping')
                    break

                if cur_tool_sigs & self._prev_tool_sigs:
                    self._tool_repeat_streak += 1
                    self._progress.add_log(
                        f'WARN: repeated tool calls from previous iteration '
                        f'(streak {self._tool_repeat_streak}) — '
                        + ('context degradation likely' if self._tool_repeat_streak >= 2 else 'monitoring')
                    )
                else:
                    self._tool_repeat_streak = 0
                self._prev_tool_sigs = cur_tool_sigs

                messages.append({'role': 'user', 'content': tool_results})

            else:
                self._progress.add_log(f'Reached iteration limit ({self._max_iterations})')

            self._progress.status = 'complete'

        except Exception as exc:
            log.error('Crawl failed for %s: %s', ticker, exc, exc_info=True)
            self._progress.status = 'failed'
            self._progress.error = str(exc)[:300]
            self._progress.add_log(f'FATAL: {exc}')
        finally:
            self._progress.finished_at = datetime.now(timezone.utc).isoformat()

    def _run_ollama(self, ticker: str, system_prompt: str) -> None:
        """Run the agentic loop using Ollama's native /api/chat endpoint via requests."""
        import requests as _requests

        from config import LLM_BASE_URL

        _ensure_ollama(LLM_BASE_URL, self._model, self._progress)

        chat_url = f'{LLM_BASE_URL}/api/chat'
        tools = [
            _FETCH_TOOL_OAI, _STORE_TOOL_OAI, _WEB_SEARCH_TOOL_OAI,
            _GET_INDEXED_DOCS_TOOL_OAI, _STORE_OBSERVATION_TOOL_OAI, _GET_OBSERVATIONS_TOOL_OAI,
        ]

        messages: list = [
            {'role': 'system', 'content': system_prompt},
            {
                'role': 'user',
                'content': (
                    f'Execute the crawl plan for {ticker}. '
                    'Do NOT write any text — call fetch_url immediately on the first entry point URL. '
                    'Your very first response must be a fetch_url tool call, not text.'
                ),
            },
        ]

        any_tools_called = False
        no_tool_streak = 0
        _MAX_NO_TOOL_STREAK = 3  # give up if model repeatedly ignores tools
        # Maps message-list index -> URL for every fetch_url tool result appended.
        # Used by _prune_old_fetches to identify and compress stale page content.
        fetch_indices: dict = {}

        try:
            for iteration in range(self._max_iterations):
                if self._progress.stop_requested:
                    self._progress.add_log('Stop requested — aborting crawl')
                    self._progress.status = 'stopped'
                    return
                if iteration % _INDEXED_REFRESH_EVERY == 0:
                    note = self._build_indexed_note(ticker)
                    if note:
                        messages.append({'role': 'user', 'content': note})
                        self._progress.add_log(
                            f'DB index injected ({note.count(chr(10))} entries)'
                        )
                if iteration == 0:
                    obs = self._tool_get_observations(ticker)
                    obs_data = json.loads(obs)
                    if obs_data.get('observations'):
                        messages.append({'role': 'user', 'content':
                            f'[Observations from previous crawl sessions for {ticker}:\n{obs}\n'
                            'Use these to skip re-discovery work.]'
                        })
                self._prune_old_fetches(messages, fetch_indices)
                ctx_chars, ctx_tokens = _estimate_ctx(messages)
                ctx_pct = ctx_tokens / self._num_ctx if self._num_ctx else 0
                self._progress.api_calls = iteration + 1
                self._progress.ctx_tokens = ctx_tokens
                self._progress.ctx_limit = self._num_ctx
                self._progress.add_log(
                    f'Ollama API call #{iteration + 1} | '
                    f'ctx {ctx_chars // 1000}k chars ~{ctx_tokens:,} tokens '
                    f'(limit {self._num_ctx:,})'
                )
                if ctx_pct >= 0.85:
                    self._progress.add_log(
                        f'WARN: context at {ctx_pct:.0%} of limit '
                        f'({self._num_ctx - ctx_tokens:,} tokens remaining) — '
                        'model may start losing earlier conversation'
                    )
                resp = _requests.post(
                    chat_url,
                    json={
                        'model': self._model,
                        'messages': messages,
                        'tools': tools,
                        'stream': False,
                        'think': False,
                        'options': {'num_ctx': self._num_ctx},
                    },
                    timeout=300,
                )
                resp.raise_for_status()
                body = resp.json()
                msg = body.get('message', {})
                messages.append(msg)

                tool_calls = msg.get('tool_calls') or []
                done_reason = body.get('done_reason', '')

                if not tool_calls:
                    if done_reason == 'stop' and any_tools_called:
                        # Model has done real work and explicitly finished
                        self._progress.add_log('Model signaled stop after completing tool work — crawl complete')
                        break
                    # Model returned text without calling any tools — nudge it
                    no_tool_streak += 1
                    content_preview = (msg.get('content') or '')[:120]
                    self._progress.add_log(
                        f'No tool calls (nudge {no_tool_streak}/{_MAX_NO_TOOL_STREAK}): {content_preview}'
                    )
                    if no_tool_streak >= _MAX_NO_TOOL_STREAK:
                        self._progress.add_log('Model repeatedly produced no tool calls — giving up')
                        break
                    messages.append({
                        'role': 'user',
                        'content': 'You must use the tools. Call fetch_url now on the first entry point URL.',
                    })
                    continue

                any_tools_called = True
                no_tool_streak = 0

                base_msg_idx = len(messages)

                # Parse tool call arguments upfront before dispatching in parallel
                parsed_calls = []
                for tc in tool_calls:
                    fn = tc.get('function', {})
                    name = fn.get('name', '')
                    inp = fn.get('arguments') or {}
                    if isinstance(inp, str):
                        try:
                            inp = json.loads(inp)
                        except Exception:
                            inp = {}
                    parsed_calls.append((name, inp))

                if len(parsed_calls) > 1:
                    self._progress.add_log(
                        f'Running {len(parsed_calls)} tool calls in parallel'
                    )

                ordered_results, cur_tool_sigs, fetch_info = self._run_tools_parallel(
                    parsed_calls, ticker
                )

                # Record fetch_url positions for context pruning
                for local_idx, url in fetch_info:
                    fetch_indices[base_msg_idx + local_idx] = url

                tool_results = [
                    {'role': 'tool', 'content': r} for r in ordered_results
                ]
                messages.extend(tool_results)

                if cur_tool_sigs & self._prev_tool_sigs:
                    self._tool_repeat_streak += 1
                    self._progress.add_log(
                        f'WARN: repeated tool calls from previous iteration '
                        f'(streak {self._tool_repeat_streak}) — '
                        + ('context degradation likely' if self._tool_repeat_streak >= 2 else 'monitoring')
                    )
                else:
                    self._tool_repeat_streak = 0
                self._prev_tool_sigs = cur_tool_sigs

            else:
                self._progress.add_log(f'Reached iteration limit ({self._max_iterations})')

            self._progress.status = 'complete'

        except Exception as exc:
            log.error('Ollama crawl failed for %s: %s', ticker, exc, exc_info=True)
            self._progress.status = 'failed'
            self._progress.error = str(exc)[:300]
            self._progress.add_log(f'FATAL: {exc}')
        finally:
            self._progress.finished_at = datetime.now(timezone.utc).isoformat()


# ---------------------------------------------------------------------------
# Module-level API
# ---------------------------------------------------------------------------
def _spawn_crawl_thread(
    progress: CrawlProgress,
    api_key: str,
    model: str,
    provider: str = 'ollama',
    prompt_override: Optional[str] = None,
    max_iterations: int = _MAX_ITERATIONS,
    max_fetch_chars: int = _MAX_FETCH_CHARS,
    num_ctx: int = _DEFAULT_NUM_CTX,
    context_block: Optional[str] = None,
    db=None,
) -> None:
    """Spawn a daemon thread that acquires the semaphore and runs the crawl."""
    def _worker():
        with _SEMAPHORE:
            crawler = LLMCrawler(
                progress, api_key, model,
                provider=provider,
                prompt_override=prompt_override,
                max_iterations=max_iterations,
                max_fetch_chars=max_fetch_chars,
                num_ctx=num_ctx,
                context_block=context_block,
                db=db,
            )
            crawler.run()

    t = threading.Thread(target=_worker, daemon=True, name=f'crawl-{progress.ticker}')
    t.start()


def start_crawl(
    tickers: list,
    task_id: Optional[str] = None,
    provider: Optional[str] = None,
    prompt: Optional[str] = None,
    model: Optional[str] = None,
    db=None,
) -> dict:
    """Create CrawlProgress entries, register the task, and spawn worker threads.

    Returns dict[ticker -> CrawlProgress].

    provider: 'ollama' (default) or 'anthropic'.
    prompt: optional system prompt override; overrides per-ticker file.
    model: optional model override; overrides config/default for this run.
    db: optional MinerDB instance for reading config_settings (crawl_max_iterations, etc.).
    """
    from config import ANTHROPIC_API_KEY, CRAWL_MODEL, CRAWL_OLLAMA_MODEL, CRAWL_PROVIDER

    if provider is None:
        provider = CRAWL_PROVIDER

    # Read configurable limits from DB if available, fall back to module constants.
    max_iterations = _MAX_ITERATIONS
    max_fetch_chars = _MAX_FETCH_CHARS
    num_ctx = _DEFAULT_NUM_CTX
    if db is not None:
        try:
            v = db.get_config('crawl_max_iterations')
            if v:
                max_iterations = int(v)
        except Exception:
            pass
        try:
            v = db.get_config('crawl_max_fetch_chars')
            if v:
                max_fetch_chars = int(v)
        except Exception:
            pass
        try:
            v = db.get_config('crawl_num_ctx')
            if v:
                num_ctx = int(v)
        except Exception:
            pass

    # Model selection: caller override > DB config > CRAWL_OLLAMA_MODEL default.
    if provider == 'ollama':
        api_key = ''
        resolved_model = CRAWL_OLLAMA_MODEL
        if db is not None:
            try:
                m = db.get_config('crawl_ollama_model')
                if m:
                    resolved_model = m
            except Exception:
                pass
        if model:
            resolved_model = model
        model = resolved_model
    else:
        api_key = ANTHROPIC_API_KEY
        model = CRAWL_MODEL

    if task_id is None:
        task_id = str(uuid.uuid4())

    per_ticker: dict = {}
    for ticker in tickers:
        ticker = ticker.upper()
        progress = CrawlProgress(ticker)
        per_ticker[ticker] = progress

        # Build coverage context block from DB data (lower bound + gap list).
        context_block = None
        if db is not None:
            try:
                from scrapers.crawl_context import build_crawl_context, format_context_block
                ctx = build_crawl_context(ticker, db)
                formatted = format_context_block(ticker, ctx)
                context_block = formatted or None
                if context_block:
                    log.info(
                        'event=crawl_context_injected ticker=%s lower_bound=%s gaps=%d covered=%d',
                        ticker, ctx.get('lower_bound'), len(ctx.get('gaps', [])), len(ctx.get('covered', [])),
                    )
            except Exception as exc:
                log.warning('event=crawl_context_build_failed ticker=%s error=%s', ticker, exc)

        _spawn_crawl_thread(
            progress, api_key, model,
            provider=provider,
            prompt_override=prompt,
            max_iterations=max_iterations,
            max_fetch_chars=max_fetch_chars,
            num_ctx=num_ctx,
            context_block=context_block,
            db=db,
        )

    with _TASKS_LOCK:
        _TASKS[task_id] = per_ticker

    log.info(
        'event=crawl_started task_id=%s tickers=%s provider=%s',
        task_id, ','.join(per_ticker), provider,
    )
    return per_ticker


def stop_crawl(task_id: str) -> int:
    """Signal stop on all tickers in a specific task. Returns count signalled."""
    with _TASKS_LOCK:
        per_ticker = _TASKS.get(task_id, {})
    count = 0
    for progress in per_ticker.values():
        if progress.status in ('pending', 'running'):
            progress.request_stop()
            count += 1
    return count


def stop_all_crawls() -> int:
    """Signal stop on every active ticker across all tasks. Returns count signalled."""
    with _TASKS_LOCK:
        tasks = list(_TASKS.values())
    count = 0
    for per_ticker in tasks:
        for progress in per_ticker.values():
            if progress.status in ('pending', 'running'):
                progress.request_stop()
                count += 1
    return count


def get_crawl_status() -> list:
    """Return snapshots for all tickers in the most recent task (latest key in _TASKS)."""
    with _TASKS_LOCK:
        if not _TASKS:
            return []
        latest_id = list(_TASKS.keys())[-1]
        task = _TASKS[latest_id]
    return [p.snapshot() for p in task.values()]


def get_crawl_task(task_id: str) -> Optional[dict]:
    """Return dict[ticker -> snapshot] for a task, or None if not found."""
    with _TASKS_LOCK:
        task = _TASKS.get(task_id)
    if task is None:
        return None
    return {ticker: p.snapshot() for ticker, p in task.items()}
