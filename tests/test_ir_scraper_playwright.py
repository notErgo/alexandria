"""Tests for playwright path simhash near-duplicate check — Fix 5."""
from unittest.mock import MagicMock, patch, call


def _make_company(**kwargs):
    base = {
        'ticker': 'MARA',
        'name': 'MARA Holdings',
        'ir_url': 'https://investor.mara.com/press-releases',
        'pr_base_url': 'https://investor.mara.com',
        'scraper_mode': 'playwright',
        'rss_url': None,
        'index_url': None,
        'pr_url_template': None,
        'scrape_mode': None,
        'scraper_issues_log': None,
    }
    base.update(kwargs)
    return base


class TestPlaywrightSimhashGuard:
    def test_skips_near_duplicate_content(self):
        """Playwright path must skip insert when find_near_duplicates returns a match."""
        from scrapers.ir_scraper import IRScraper

        db = MagicMock()
        session = MagicMock()
        scraper = IRScraper(db=db, session=session)

        company = _make_company()

        # Simulate: URL not seen before (url_hash check passes),
        # but content IS a near-duplicate of an existing report.
        db.report_exists_by_url_hash.return_value = False
        db.find_near_duplicates.return_value = [{'id': 42}]

        pr_text = "MARA mined 750 BTC in January 2025. " * 200

        with patch('scrapers.ir_scraper.sync_playwright') as mock_pw:
            pw_ctx = MagicMock()
            mock_pw.return_value.__enter__ = MagicMock(return_value=pw_ctx)
            mock_pw.return_value.__exit__ = MagicMock(return_value=False)

            browser = MagicMock()
            pw_ctx.chromium.launch.return_value = browser
            ctx = MagicMock()
            browser.new_context.return_value = ctx
            page = MagicMock()
            ctx.new_page.return_value = page

            # Listing page returns one production PR link
            listing_html = (
                '<html><body>'
                '<a href="https://investor.mara.com/pr/jan-2025">'
                'MARA Announces Bitcoin Production Update January 2025'
                '</a></body></html>'
            )
            pr_html = f'<html><body>{pr_text}</body></html>'

            def goto_side_effect(url, **kwargs):
                pass
            page.goto.side_effect = goto_side_effect
            page.content.side_effect = [listing_html, pr_html]

            result = scraper._scrape_playwright(company)

        # Should NOT have inserted anything
        db.insert_report.assert_not_called()
        assert result.reports_ingested == 0

    def test_inserts_unique_content(self):
        """Playwright path inserts when no near-duplicate is found."""
        from scrapers.ir_scraper import IRScraper

        db = MagicMock()
        session = MagicMock()
        scraper = IRScraper(db=db, session=session)

        company = _make_company()

        db.report_exists_by_url_hash.return_value = False
        db.find_near_duplicates.return_value = []   # no duplicates
        db.insert_report.return_value = 99

        pr_text = "MARA mined 750 BTC in January 2025. " * 200

        with patch('scrapers.ir_scraper.sync_playwright') as mock_pw:
            pw_ctx = MagicMock()
            mock_pw.return_value.__enter__ = MagicMock(return_value=pw_ctx)
            mock_pw.return_value.__exit__ = MagicMock(return_value=False)

            browser = MagicMock()
            pw_ctx.chromium.launch.return_value = browser
            ctx = MagicMock()
            browser.new_context.return_value = ctx
            page = MagicMock()
            ctx.new_page.return_value = page

            listing_html = (
                '<html><body>'
                '<a href="https://investor.mara.com/pr/jan-2025">'
                'MARA Announces Bitcoin Production Update January 2025'
                '</a></body></html>'
            )
            pr_html = f'<html><body>{pr_text}</body></html>'

            page.goto.side_effect = lambda url, **kwargs: None
            page.content.side_effect = [listing_html, pr_html]

            result = scraper._scrape_playwright(company)

        db.insert_report.assert_called_once()
        assert result.reports_ingested == 1
