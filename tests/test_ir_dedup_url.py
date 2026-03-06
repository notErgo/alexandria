"""Tests for IR scraper URL-first deduplication (schema v16, issue #6)."""
import unittest
from unittest.mock import MagicMock, patch
from datetime import date


class TestRSSSkipsSameURL(unittest.TestCase):
    """Issue #6: RSS scraper must skip insert when same URL already exists."""

    def _make_scraper(self, db, session=None):
        from scrapers.ir_scraper import IRScraper
        if session is None:
            session = MagicMock()
        return IRScraper(db=db, session=session)

    def _make_rss_company(self):
        return {
            'ticker': 'MARA',
            'ir_url': 'https://ir.mara.com/news-events/press-releases',
            'rss_url': 'https://ir.mara.com/rss',
            'scraper_mode': 'rss',
            'pr_start_year': 2020,
        }

    def test_rss_skips_when_url_already_ingested(self):
        db = MagicMock()
        db.report_exists_by_url_hash.return_value = True
        session = MagicMock()

        rss_xml = """<?xml version="1.0" encoding="UTF-8"?>
        <rss version="2.0">
          <channel>
            <item>
              <title>MARA Holdings Announces January 2024 Production Update</title>
              <link>https://ir.mara.com/news/jan-2024-production</link>
              <pubDate>Mon, 05 Feb 2024 12:00:00 GMT</pubDate>
            </item>
          </channel>
        </rss>"""

        mock_resp = MagicMock()
        mock_resp.text = rss_xml
        mock_resp.status_code = 200

        scraper = self._make_scraper(db, session)

        with patch('scrapers.ir_scraper._fetch_with_rate_limit', return_value=mock_resp):
            scraper._scrape_rss(self._make_rss_company())

        # URL hash check should have been called
        db.report_exists_by_url_hash.assert_called()
        # No new report should be inserted
        db.insert_report.assert_not_called()

    def test_rss_inserts_when_url_is_new(self):
        db = MagicMock()
        db.report_exists_by_url_hash.return_value = False
        db.find_near_duplicates.return_value = []
        db.insert_report.return_value = 99
        session = MagicMock()

        rss_xml = """<?xml version="1.0" encoding="UTF-8"?>
        <rss version="2.0">
          <channel>
            <item>
              <title>MARA Holdings Announces January 2024 Production Update</title>
              <link>https://ir.mara.com/news/jan-2024-production-new</link>
              <pubDate>Mon, 05 Feb 2024 12:00:00 GMT</pubDate>
            </item>
          </channel>
        </rss>"""

        mock_rss_resp = MagicMock()
        mock_rss_resp.text = rss_xml
        mock_rss_resp.status_code = 200

        mock_page_resp = MagicMock()
        mock_page_resp.text = '<html><body>January 2024 production update text</body></html>'
        mock_page_resp.status_code = 200

        scraper = self._make_scraper(db, session)

        def _fetch_side_effect(url, session):
            if 'rss' in url:
                return mock_rss_resp
            return mock_page_resp

        with patch('scrapers.ir_scraper._fetch_with_rate_limit', side_effect=_fetch_side_effect):
            scraper._scrape_rss(self._make_rss_company())

        db.insert_report.assert_called_once()


class TestRSSAllowsSamePeriodDifferentURL(unittest.TestCase):
    """Issue #6: Same period with different URL must be allowed through (URL-first dedup)."""

    def test_different_url_same_period_inserts(self):
        """Even if a report for the same period exists, a different URL should be allowed."""
        db = MagicMock()
        # URL hash check: returns False (new URL not seen before)
        db.report_exists_by_url_hash.return_value = False
        db.find_near_duplicates.return_value = []
        db.insert_report.return_value = 100
        session = MagicMock()

        company = {
            'ticker': 'MARA',
            'ir_url': 'https://ir.mara.com',
            'rss_url': 'https://ir.mara.com/rss',
            'scraper_mode': 'rss',
            'pr_start_year': 2020,
        }

        rss_xml = """<?xml version="1.0" encoding="UTF-8"?>
        <rss version="2.0">
          <channel>
            <item>
              <title>MARA Holdings Announces January 2024 Production Update (Corrected)</title>
              <link>https://ir.mara.com/news/jan-2024-production-corrected</link>
              <pubDate>Mon, 06 Feb 2024 12:00:00 GMT</pubDate>
            </item>
          </channel>
        </rss>"""

        mock_rss = MagicMock()
        mock_rss.text = rss_xml
        mock_rss.status_code = 200

        mock_page = MagicMock()
        mock_page.text = '<html><body>Corrected January 2024 production update</body></html>'
        mock_page.status_code = 200

        from scrapers.ir_scraper import IRScraper
        scraper = IRScraper(db=db, session=session)

        def _fetch_side(url, session):
            if 'rss' in url:
                return mock_rss
            return mock_page

        with patch('scrapers.ir_scraper._fetch_with_rate_limit', side_effect=_fetch_side):
            scraper._scrape_rss(company)

        # Should have inserted because URL is new (even if period might overlap)
        db.insert_report.assert_called_once()


class TestTemplatePathURLDedup(unittest.TestCase):
    """Issue #6: Template scraper path should also use URL-first dedup."""

    def test_template_skips_when_url_already_ingested(self):
        db = MagicMock()
        db.report_exists_by_url_hash.return_value = True
        session = MagicMock()

        company = {
            'ticker': 'RIOT',
            'ir_url': 'https://www.riotplatforms.com',
            'scraper_mode': 'template',
            'url_template': 'https://www.riotplatforms.com/riot-announces-{month}-{year}-production-and-operations-updates/',
            'pr_start_year': 2024,
        }

        from scrapers.ir_scraper import IRScraper
        scraper = IRScraper(db=db, session=session)

        mock_resp = MagicMock()
        mock_resp.text = '<html><body>production update</body></html>'
        mock_resp.status_code = 200

        with patch('scrapers.ir_scraper._fetch_with_rate_limit', return_value=mock_resp):
            scraper._scrape_template(company)

        # URL hash dedup must be checked
        db.report_exists_by_url_hash.assert_called()
        # No new report inserted
        db.insert_report.assert_not_called()


if __name__ == '__main__':
    unittest.main()
