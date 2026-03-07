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
import uuid
from collections import deque
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import requests as _requests
from bs4 import BeautifulSoup

log = logging.getLogger('miners.crawl')

_PREFERRED_MODELS = ['qwen3.5:27b', 'qwen3.5:9b']

# ---------------------------------------------------------------------------
# Paths & constants
# ---------------------------------------------------------------------------
_REPO_ROOT = Path(__file__).parent.parent.parent
_CRAWL_PROMPTS_DIR = _REPO_ROOT / 'scripts' / 'crawl_prompts'
_INGEST_RAW_URL = 'http://127.0.0.1:5004/api/ingest/raw'

_MAX_FETCH_CHARS = 12_000   # chars returned to model per page fetch
_MAX_ITERATIONS  = 80       # safety ceiling per ticker

# ---------------------------------------------------------------------------
# Module-level task registry
# ---------------------------------------------------------------------------
# _TASKS: task_id -> dict[ticker -> CrawlProgress]
_TASKS: dict = {}
_TASKS_LOCK = threading.Lock()

# Limit concurrent ticker crawls to avoid hammering the Anthropic API
_SEMAPHORE = threading.Semaphore(3)


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


# ---------------------------------------------------------------------------
# Ollama lifecycle helpers
# ---------------------------------------------------------------------------

def _ensure_ollama(base_url: str, model: str, progress: 'CrawlProgress') -> None:
    """Start Ollama if not running, then warm the target model into memory."""
    import subprocess
    import time

    tags_url = f'{base_url}/api/tags'

    def _is_up() -> bool:
        try:
            return _requests.get(tags_url, timeout=3).ok
        except Exception:
            return False

    if not _is_up():
        progress.add_log('Ollama not running — starting server...')
        try:
            subprocess.Popen(
                ['ollama', 'serve'],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
        except FileNotFoundError:
            raise RuntimeError('ollama not found on PATH — install Ollama first')
        for _ in range(30):
            time.sleep(1)
            if _is_up():
                progress.add_log('Ollama server ready')
                break
        else:
            raise RuntimeError('Ollama did not start within 30 seconds')
    else:
        progress.add_log('Ollama already running')

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
    ) -> None:
        self._progress = progress
        self._api_key = api_key
        self._model = model
        self._provider = provider
        self._prompt_override = prompt_override
        self._max_iterations = max_iterations
        self._max_fetch_chars = max_fetch_chars
        self._session = _requests.Session()
        self._seen_urls: set = set()
        self._session.headers.update({
            'User-Agent': (
                'Hermeneutic Research Platform/1.0 '
                '(bitcoin-miner-research; contact: research@hermeneutic.io)'
            )
        })

    # ------------------------------------------------------------------
    # Tool handlers
    # ------------------------------------------------------------------
    def _tool_fetch_url(self, url: str) -> str:
        try:
            resp = self._session.get(url, timeout=20)
            resp.raise_for_status()
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
        tools = [_FETCH_URL_TOOL, _STORE_DOCUMENT_TOOL, _WEB_SEARCH_TOOL]
        return messages, tools

    def _run_anthropic(self, ticker: str, system_prompt: str) -> None:
        import anthropic

        client = anthropic.Anthropic(api_key=self._api_key)
        messages, tools = self._build_messages_and_tools(ticker, system_prompt)

        try:
            for iteration in range(self._max_iterations):
                if self._progress.stop_requested:
                    self._progress.add_log('Stop requested — aborting crawl')
                    self._progress.status = 'stopped'
                    return
                self._progress.add_log(f'API call #{iteration + 1}')
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

                tool_results = []
                for block in response.content:
                    if block.type != 'tool_use':
                        continue
                    name = block.name
                    inp = block.input
                    if name == 'fetch_url':
                        result_content = self._tool_fetch_url(inp.get('url', ''))
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
                    else:
                        result_content = f'Unknown tool: {name}'
                    tool_results.append({
                        'type': 'tool_result',
                        'tool_use_id': block.id,
                        'content': result_content,
                    })

                if not tool_results:
                    self._progress.add_log('No tool calls in response — stopping')
                    break

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
        tools = [_FETCH_TOOL_OAI, _STORE_TOOL_OAI, _WEB_SEARCH_TOOL_OAI]

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

        try:
            for iteration in range(self._max_iterations):
                if self._progress.stop_requested:
                    self._progress.add_log('Stop requested — aborting crawl')
                    self._progress.status = 'stopped'
                    return
                self._progress.add_log(f'Ollama API call #{iteration + 1}')
                resp = _requests.post(
                    chat_url,
                    json={
                        'model': self._model,
                        'messages': messages,
                        'tools': tools,
                        'stream': False,
                        'think': False,
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

                tool_results = []
                for tc in tool_calls:
                    fn = tc.get('function', {})
                    name = fn.get('name', '')
                    inp = fn.get('arguments') or {}
                    if isinstance(inp, str):
                        try:
                            inp = json.loads(inp)
                        except Exception:
                            inp = {}
                    if name == 'fetch_url':
                        result_content = self._tool_fetch_url(inp.get('url', ''))
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
                    else:
                        result_content = f'Unknown tool: {name}'
                    tool_results.append({'role': 'tool', 'content': result_content})

                messages.extend(tool_results)

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
    from config import ANTHROPIC_API_KEY, CRAWL_MODEL, CRAWL_PROVIDER, LLM_MODEL_ID

    if provider is None:
        provider = CRAWL_PROVIDER

    # Read configurable limits from DB if available, fall back to module constants.
    max_iterations = _MAX_ITERATIONS
    max_fetch_chars = _MAX_FETCH_CHARS
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

    # Model selection: caller override > DB config > module default.
    if provider == 'ollama':
        api_key = ''
        resolved_model = LLM_MODEL_ID
        if db is not None:
            try:
                m = db.get_config('ollama_model')
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
        _spawn_crawl_thread(
            progress, api_key, model,
            provider=provider,
            prompt_override=prompt,
            max_iterations=max_iterations,
            max_fetch_chars=max_fetch_chars,
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
