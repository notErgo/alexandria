"""Unit tests for LLMCrawler bug fixes:
  Step 1 — content validation in _tool_store_document
  Step 2 — intra-crawl URL dedup
  Step 3 — pagination link preservation in _tool_fetch_url
  Step 4 — web_search tool wiring
"""
import json
import sys
import os
import unittest
from unittest.mock import MagicMock, patch, call

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))


def _make_crawler(ticker='TEST'):
    from scrapers.llm_crawler import CrawlProgress, LLMCrawler
    p = CrawlProgress(ticker)
    return LLMCrawler(p, api_key='', model='')


# ---------------------------------------------------------------------------
# Step 1 — content validation in _tool_store_document
# ---------------------------------------------------------------------------
class TestStoreDocumentContentValidation(unittest.TestCase):

    def test_store_document_skips_short_text(self):
        crawler = _make_crawler()
        result = crawler._tool_store_document(
            'TEST', 'https://x.com', 'too short text', 'ir_press_release'
        )
        self.assertEqual(result['status'], 'skipped')
        self.assertEqual(result['reason'], 'insufficient_content')

    def test_store_document_allows_sufficient_text(self):
        crawler = _make_crawler()
        long_text = ' '.join(['word'] * 60)
        mock_resp = MagicMock()
        mock_resp.ok = True
        mock_resp.json.return_value = {'success': True, 'data': {'ingested': 1}}
        crawler._session.post = MagicMock(return_value=mock_resp)
        result = crawler._tool_store_document(
            'TEST', 'https://x.com', long_text, 'ir_press_release'
        )
        crawler._session.post.assert_called_once()
        self.assertIn(result['status'], ('ingested', 'skipped'))

    def test_store_document_skips_error_page(self):
        crawler = _make_crawler()
        long_text = 'page not found ' + ' '.join(['word'] * 60)
        result = crawler._tool_store_document(
            'TEST', 'https://x.com', long_text, 'ir_press_release'
        )
        self.assertEqual(result['status'], 'skipped')
        self.assertEqual(result['reason'], 'error_page')

    def test_store_document_skips_404_not_found(self):
        crawler = _make_crawler()
        text = '404 not found ' + ' '.join(['word'] * 60)
        result = crawler._tool_store_document(
            'TEST', 'https://x.com', text, 'ir_press_release'
        )
        self.assertEqual(result['status'], 'skipped')
        self.assertEqual(result['reason'], 'error_page')

    def test_store_document_skips_access_denied(self):
        crawler = _make_crawler()
        text = 'access denied ' + ' '.join(['word'] * 60)
        result = crawler._tool_store_document(
            'TEST', 'https://x.com', text, 'ir_press_release'
        )
        self.assertEqual(result['status'], 'skipped')
        self.assertEqual(result['reason'], 'error_page')

    def test_docs_skipped_increments_on_content_rejection(self):
        crawler = _make_crawler()
        before = crawler._progress.docs_skipped
        crawler._tool_store_document('TEST', 'https://x.com', 'short', 'ir_press_release')
        self.assertEqual(crawler._progress.docs_skipped, before + 1)

    def test_error_page_check_is_case_insensitive(self):
        crawler = _make_crawler()
        text = 'Page Not Found ' + ' '.join(['word'] * 60)
        result = crawler._tool_store_document(
            'TEST', 'https://x.com', text, 'ir_press_release'
        )
        self.assertEqual(result['reason'], 'error_page')


# ---------------------------------------------------------------------------
# Step 2 — intra-crawl URL dedup
# ---------------------------------------------------------------------------
class TestIntraCrawlURLDedup(unittest.TestCase):

    def test_seen_urls_initialized_empty(self):
        crawler = _make_crawler()
        self.assertEqual(crawler._seen_urls, set())

    def test_store_document_skips_already_seen_url(self):
        crawler = _make_crawler()
        long_text = ' '.join(['word'] * 60)
        mock_resp = MagicMock()
        mock_resp.ok = True
        mock_resp.json.return_value = {'success': True, 'data': {'ingested': 1}}
        crawler._session.post = MagicMock(return_value=mock_resp)

        result1 = crawler._tool_store_document(
            'TEST', 'https://x.com/page1', long_text, 'ir_press_release'
        )
        result2 = crawler._tool_store_document(
            'TEST', 'https://x.com/page1', long_text, 'ir_press_release'
        )

        self.assertEqual(crawler._session.post.call_count, 1)
        self.assertEqual(result2['status'], 'skipped')

    def test_seen_urls_tracks_across_calls(self):
        crawler = _make_crawler()
        long_text = ' '.join(['word'] * 60)
        mock_resp = MagicMock()
        mock_resp.ok = True
        mock_resp.json.return_value = {'success': True, 'data': {'ingested': 1}}
        crawler._session.post = MagicMock(return_value=mock_resp)

        urls = [
            'https://x.com/a',
            'https://x.com/b',
            'https://x.com/c',
        ]
        for url in urls:
            crawler._tool_store_document('TEST', url, long_text, 'ir_press_release')

        self.assertEqual(crawler._session.post.call_count, 3)
        for url in urls:
            self.assertIn(url, crawler._seen_urls)

    def test_docs_skipped_increments_on_url_dedup(self):
        crawler = _make_crawler()
        long_text = ' '.join(['word'] * 60)
        mock_resp = MagicMock()
        mock_resp.ok = True
        mock_resp.json.return_value = {'success': True, 'data': {'ingested': 1}}
        crawler._session.post = MagicMock(return_value=mock_resp)

        crawler._tool_store_document('TEST', 'https://x.com/dup', long_text, 'ir_press_release')
        before = crawler._progress.docs_skipped
        crawler._tool_store_document('TEST', 'https://x.com/dup', long_text, 'ir_press_release')
        self.assertEqual(crawler._progress.docs_skipped, before + 1)


# ---------------------------------------------------------------------------
# Step 3 — pagination link preservation in _tool_fetch_url
# ---------------------------------------------------------------------------
class TestFetchUrlPaginationLinks(unittest.TestCase):

    def _mock_resp(self, html):
        mock_resp = MagicMock()
        mock_resp.text = html
        mock_resp.raise_for_status = MagicMock()
        return mock_resp

    def test_fetch_url_appends_links_section(self):
        crawler = _make_crawler()
        html = '''<html><body>
            <p>Some content</p>
            <a href="/page/2">Next page</a>
            <a href="/page/3">Page 3</a>
        </body></html>'''
        crawler._session.get = MagicMock(return_value=self._mock_resp(html))
        result = crawler._tool_fetch_url('https://x.com/ir')
        self.assertIn('LINKS ON PAGE:', result)
        self.assertIn('/page/2', result)
        self.assertIn('/page/3', result)

    def test_fetch_url_links_survive_truncation(self):
        """Links appended after 12k chars of body still appear in result."""
        crawler = _make_crawler()
        body_text = 'A' * 13000
        html = f'<html><body><p>{body_text}</p><a href="/next-page">Next</a></body></html>'
        crawler._session.get = MagicMock(return_value=self._mock_resp(html))
        result = crawler._tool_fetch_url('https://x.com/ir')
        # Result must not exceed max_fetch_chars
        self.assertLessEqual(len(result), crawler._max_fetch_chars)
        # The pagination link must still be present
        self.assertIn('LINKS ON PAGE:', result)
        self.assertIn('/next-page', result)

    def test_fetch_url_no_links_no_section(self):
        crawler = _make_crawler()
        html = '<html><body><p>No links here at all.</p></body></html>'
        crawler._session.get = MagicMock(return_value=self._mock_resp(html))
        result = crawler._tool_fetch_url('https://x.com/ir')
        self.assertNotIn('LINKS ON PAGE:', result)

    def test_fetch_url_deduplicates_links(self):
        crawler = _make_crawler()
        html = '''<html><body>
            <a href="/dup">Link</a>
            <a href="/dup">Link again</a>
            <a href="/dup">Link third</a>
        </body></html>'''
        crawler._session.get = MagicMock(return_value=self._mock_resp(html))
        result = crawler._tool_fetch_url('https://x.com/ir')
        self.assertEqual(result.count('/dup'), 1)


# ---------------------------------------------------------------------------
# Step 4 — web_search tool
# ---------------------------------------------------------------------------
class TestWebSearchTool(unittest.TestCase):

    def _mock_ddg_html(self, results):
        """Build a minimal DuckDuckGo HTML page with result__a links."""
        items = ''.join(
            f'<a class="result__a" href="{url}">{title}</a>'
            for title, url in results
        )
        return f'<html><body>{items}</body></html>'

    def test_web_search_returns_results(self):
        crawler = _make_crawler()
        html = self._mock_ddg_html([
            ('MARA Production Jan 2024', 'https://ir.mara.com/a'),
            ('MARA BTC Output', 'https://ir.mara.com/b'),
            ('MARA Monthly Update', 'https://ir.mara.com/c'),
        ])
        mock_resp = MagicMock()
        mock_resp.text = html
        mock_resp.raise_for_status = MagicMock()
        crawler._session.get = MagicMock(return_value=mock_resp)

        result = crawler._tool_web_search('MARA production 2024')
        self.assertIn('ir.mara.com', result)
        # At most 10 pairs
        pairs = [p for p in result.split('\n\n') if p.strip()]
        self.assertLessEqual(len(pairs), 10)

    def test_web_search_empty_response(self):
        crawler = _make_crawler()
        mock_resp = MagicMock()
        mock_resp.text = '<html><body><p>No results</p></body></html>'
        mock_resp.raise_for_status = MagicMock()
        crawler._session.get = MagicMock(return_value=mock_resp)
        result = crawler._tool_web_search('nothing found query')
        self.assertEqual(result, 'No results found for query.')

    def test_web_search_request_error(self):
        crawler = _make_crawler()
        crawler._session.get = MagicMock(side_effect=ConnectionError('network down'))
        result = crawler._tool_web_search('test query')
        self.assertTrue(result.startswith('ERROR:'))

    def test_web_search_tool_in_anthropic_tools_list(self):
        from scrapers.llm_crawler import _FETCH_URL_TOOL, _STORE_DOCUMENT_TOOL, _WEB_SEARCH_TOOL
        tools = [_FETCH_URL_TOOL, _STORE_DOCUMENT_TOOL, _WEB_SEARCH_TOOL]
        names = [t['name'] for t in tools]
        self.assertIn('web_search', names)

    def test_web_search_tool_in_ollama_tools_list(self):
        from scrapers.llm_crawler import _WEB_SEARCH_TOOL_OAI
        self.assertEqual(_WEB_SEARCH_TOOL_OAI['type'], 'function')
        self.assertEqual(_WEB_SEARCH_TOOL_OAI['function']['name'], 'web_search')

    def test_anthropic_dispatch_calls_web_search(self):
        """Anthropic loop dispatches web_search tool call to _tool_web_search."""
        from scrapers.llm_crawler import CrawlProgress, LLMCrawler

        p = CrawlProgress('MARA')
        crawler = LLMCrawler(p, api_key='fake-key', model='claude-3-opus-20240229',
                             provider='anthropic')

        # First response: tool_use for web_search
        tool_use_block = MagicMock()
        tool_use_block.type = 'tool_use'
        tool_use_block.name = 'web_search'
        tool_use_block.id = 'tu_001'
        tool_use_block.input = {'query': 'MARA production 2024'}

        resp1 = MagicMock()
        resp1.stop_reason = 'tool_use'
        resp1.content = [tool_use_block]

        # Second response: end_turn
        resp2 = MagicMock()
        resp2.stop_reason = 'end_turn'
        resp2.content = []

        mock_client = MagicMock()
        mock_client.messages.create.side_effect = [resp1, resp2]

        crawler._tool_web_search = MagicMock(return_value='result1\nresult2')

        with patch('anthropic.Anthropic', return_value=mock_client):
            with patch.object(crawler, '_progress') as mock_p:
                mock_p.ticker = 'MARA'
                mock_p._lock = p._lock
                mock_p.stop_requested = False
                # Run but avoid the full run() to bypass file read
                crawler._run_anthropic('MARA', 'system prompt here')

        crawler._tool_web_search.assert_called_once_with('MARA production 2024')

    def test_ollama_dispatch_calls_web_search(self):
        """Ollama loop dispatches web_search tool call to _tool_web_search."""
        from scrapers.llm_crawler import CrawlProgress, LLMCrawler

        p = CrawlProgress('RIOT')
        crawler = LLMCrawler(p, api_key='', model='qwen3.5', provider='ollama')
        crawler._tool_web_search = MagicMock(return_value='search results')

        # First response: web_search tool call
        resp1 = MagicMock()
        resp1.raise_for_status = MagicMock()
        resp1.json.return_value = {
            'message': {
                'role': 'assistant',
                'content': '',
                'tool_calls': [{'function': {'name': 'web_search', 'arguments': {'query': 'RIOT production 2024'}}}],
            },
            'done': False,
            'done_reason': 'tool_calls',
        }

        # Second response: stop
        resp2 = MagicMock()
        resp2.raise_for_status = MagicMock()
        resp2.json.return_value = {
            'message': {'role': 'assistant', 'content': 'Done.'},
            'done': True,
            'done_reason': 'stop',
        }

        with patch('requests.post', side_effect=[resp1, resp2]) as mock_post:
            with patch('config.LLM_BASE_URL', 'http://localhost:11434'):
                with patch('scrapers.llm_crawler._ensure_ollama'):
                    crawler._run_ollama('RIOT', 'system prompt here')

        mock_post.assert_called()
        crawler._tool_web_search.assert_called_once_with('RIOT production 2024')


if __name__ == '__main__':
    unittest.main()
