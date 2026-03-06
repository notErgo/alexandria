"""Offline structural tests for crawl prompt files."""
import os
import unittest
from pathlib import Path

_PROMPTS_DIR = (
    Path(__file__).parent.parent / 'scripts' / 'crawl_prompts'
)

_TICKERS = [
    'ABTC', 'APLD', 'ARBK', 'BITF', 'BTBT', 'BTDR', 'CIFR', 'CLSK',
    'CORZ', 'GRDI', 'GREE', 'HIVE', 'HUT8', 'IREN', 'MARA', 'MIGI',
    'RIOT', 'SDIG', 'WULF',
]


def _read_all() -> dict:
    contents = {}
    for ticker in _TICKERS:
        path = _PROMPTS_DIR / f'{ticker}_crawl.md'
        if path.exists():
            contents[ticker] = path.read_text()
    return contents


class TestCrawlPromptStructure(unittest.TestCase):

    def setUp(self):
        self._prompts = _read_all()

    def test_all_19_prompt_files_exist(self):
        for ticker in _TICKERS:
            path = _PROMPTS_DIR / f'{ticker}_crawl.md'
            self.assertTrue(path.exists(), f'Missing: {path}')

    def test_no_prompt_references_use_websearch(self):
        for ticker, text in self._prompts.items():
            self.assertNotIn(
                'Use WebSearch', text,
                f'{ticker}_crawl.md still contains "Use WebSearch"',
            )

    def test_no_prompt_references_efts_sec_gov(self):
        for ticker, text in self._prompts.items():
            self.assertNotIn(
                'efts.sec.gov', text,
                f'{ticker}_crawl.md still references efts.sec.gov',
            )

    def test_no_prompt_references_results_json(self):
        for ticker, text in self._prompts.items():
            self.assertNotIn(
                'results.json', text,
                f'{ticker}_crawl.md still references results.json',
            )

    def test_all_prompts_reference_web_search_tool(self):
        for ticker, text in self._prompts.items():
            self.assertIn(
                'web_search', text,
                f'{ticker}_crawl.md missing "web_search" tool reference',
            )

    def test_all_prompts_reference_store_document(self):
        for ticker, text in self._prompts.items():
            self.assertIn(
                'store_document', text,
                f'{ticker}_crawl.md missing "store_document" tool reference',
            )


if __name__ == '__main__':
    unittest.main()
