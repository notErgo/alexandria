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

# ---------------------------------------------------------------------------
# Paths & constants
# ---------------------------------------------------------------------------
_REPO_ROOT = Path(__file__).parent.parent.parent
_CRAWL_PROMPTS_DIR = _REPO_ROOT / 'scripts' / 'crawl_prompts'
_INGEST_RAW_URL = 'http://127.0.0.1:5004/api/ingest/raw'

_MAX_FETCH_CHARS = 12_000   # chars returned to Claude per page fetch
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
        self.status: str = 'pending'          # pending | running | complete | failed
        self.pages_fetched: int = 0
        self.docs_stored: int = 0
        self.docs_skipped: int = 0
        self.error: Optional[str] = None
        self.started_at: Optional[str] = None
        self.finished_at: Optional[str] = None
        self._logs: deque = deque(maxlen=150)
        self._lock = threading.Lock()

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


# ---------------------------------------------------------------------------
# LLMCrawler
# ---------------------------------------------------------------------------
class LLMCrawler:
    """Executes an agentic crawl for a single ticker."""

    def __init__(self, progress: CrawlProgress, api_key: str, model: str) -> None:
        self._progress = progress
        self._api_key = api_key
        self._model = model
        self._session = _requests.Session()
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
            return text[:_MAX_FETCH_CHARS]
        except Exception as exc:
            self._progress.add_log(f'fetch_url ERROR {url}: {exc}')
            return f'ERROR: {exc}'

    def _tool_store_document(
        self, ticker: str, url: str, text: str, source_type: str
    ) -> dict:
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

    # ------------------------------------------------------------------
    # Main loop
    # ------------------------------------------------------------------
    def run(self) -> None:
        import anthropic

        ticker = self._progress.ticker
        prompt_path = _CRAWL_PROMPTS_DIR / f'{ticker}_crawl.md'
        if not prompt_path.exists():
            self._progress.status = 'failed'
            self._progress.error = f'No crawl prompt found at {prompt_path}'
            self._progress.add_log(f'ERROR: missing crawl prompt {prompt_path}')
            return

        system_prompt = prompt_path.read_text()
        self._progress.status = 'running'
        self._progress.started_at = datetime.now(timezone.utc).isoformat()
        self._progress.add_log(f'Starting crawl for {ticker}')

        client = anthropic.Anthropic(api_key=self._api_key)
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
        tools = [_FETCH_URL_TOOL, _STORE_DOCUMENT_TOOL]

        try:
            for iteration in range(_MAX_ITERATIONS):
                self._progress.add_log(f'API call #{iteration + 1}')
                response = client.messages.create(
                    model=self._model,
                    max_tokens=4096,
                    system=system_prompt,
                    messages=messages,
                    tools=tools,
                )

                # Append assistant turn
                messages.append({'role': 'assistant', 'content': response.content})

                if response.stop_reason == 'end_turn':
                    self._progress.add_log('Model signaled end_turn — crawl complete')
                    break

                # Collect tool_use blocks and handle them
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
                    else:
                        result_content = f'Unknown tool: {name}'
                    tool_results.append({
                        'type': 'tool_result',
                        'tool_use_id': block.id,
                        'content': result_content,
                    })

                if not tool_results:
                    # No tool calls but not end_turn — treat as done
                    self._progress.add_log('No tool calls in response — stopping')
                    break

                messages.append({'role': 'user', 'content': tool_results})

            else:
                self._progress.add_log(f'Reached iteration limit ({_MAX_ITERATIONS})')

            self._progress.status = 'complete'

        except Exception as exc:
            log.error('Crawl failed for %s: %s', ticker, exc, exc_info=True)
            self._progress.status = 'failed'
            self._progress.error = str(exc)[:300]
            self._progress.add_log(f'FATAL: {exc}')
        finally:
            self._progress.finished_at = datetime.now(timezone.utc).isoformat()


# ---------------------------------------------------------------------------
# Module-level API
# ---------------------------------------------------------------------------
def _spawn_crawl_thread(progress: CrawlProgress, api_key: str, model: str) -> None:
    """Spawn a daemon thread that acquires the semaphore and runs the crawl."""
    def _worker():
        with _SEMAPHORE:
            crawler = LLMCrawler(progress, api_key, model)
            crawler.run()

    t = threading.Thread(target=_worker, daemon=True, name=f'crawl-{progress.ticker}')
    t.start()


def start_crawl(
    tickers: list,
    task_id: Optional[str] = None,
) -> dict:
    """Create CrawlProgress entries, register the task, and spawn worker threads.

    Returns dict[ticker -> CrawlProgress].
    """
    from config import ANTHROPIC_API_KEY, CRAWL_MODEL

    if task_id is None:
        task_id = str(uuid.uuid4())

    per_ticker: dict = {}
    for ticker in tickers:
        ticker = ticker.upper()
        progress = CrawlProgress(ticker)
        per_ticker[ticker] = progress
        _spawn_crawl_thread(progress, ANTHROPIC_API_KEY, CRAWL_MODEL)

    with _TASKS_LOCK:
        _TASKS[task_id] = per_ticker

    log.info(
        'event=crawl_started task_id=%s tickers=%s',
        task_id, ','.join(per_ticker),
    )
    return per_ticker


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
