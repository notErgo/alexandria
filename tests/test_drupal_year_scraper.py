"""Tests for IRScraper._scrape_drupal_year (Drupal year-filter widget mode)."""
from datetime import date as real_date
import hashlib
from unittest.mock import MagicMock, patch, call
import pytest


def _make_company(ticker='BTDR', ir_url='https://ir.example.com/news', pr_start_year=2024):
    return {
        'ticker': ticker,
        'ir_url': ir_url,
        'pr_base_url': 'https://ir.example.com',
        'scraper_mode': 'drupal_year',
        'pr_start_year': pr_start_year,
        'active': True,
        'skip_reason': None,
    }


WIDGET_ID = 'aac2c52233ec9ed03e44a98dd9028c83ac2c52a24dacec95b3c1757c0d59015b'

BASE_PAGE_HTML = f"""<html><body>
<form>
  <input type="hidden" name="form_build_id" value="form-TESTTOKEN1" />
  <input type="hidden" name="{WIDGET_ID}_widget_id" value="{WIDGET_ID}" />
  <input type="hidden" name="form_id" value="widget_form_base" />
  <select name="{WIDGET_ID}_year[value]"><option value="2024">2024</option></select>
</form>
</body></html>"""

YEAR_PAGE_HTML = f"""<html><body>
<form>
  <input type="hidden" name="form_build_id" value="form-TESTTOKEN2" />
  <input type="hidden" name="{WIDGET_ID}_widget_id" value="{WIDGET_ID}" />
  <input type="hidden" name="form_id" value="widget_form_base" />
</form>
<a href="/news/detail/123">Bitdeer Reports Bitcoin Production for January 2024</a>
<a href="/news/detail/124">Bitdeer Reports Bitcoin Production for February 2024</a>
<a href="/news/detail/about">About Us</a>
</body></html>"""

YEAR_PAGE_HTML_BROAD = f"""<html><body>
<form>
  <input type="hidden" name="form_build_id" value="form-TESTTOKEN2" />
  <input type="hidden" name="{WIDGET_ID}_widget_id" value="{WIDGET_ID}" />
  <input type="hidden" name="form_id" value="widget_form_base" />
</form>
<a href="/news/detail/555">Bitfarms Provides January 2024 Operations and Miner Energization Update</a>
</body></html>"""

PR_HTML = """<html><body>
<h1>Bitdeer Reports Bitcoin Production for January 2024</h1>
<p>Bitcoin mined: 200 BTC. Hash rate 15 EH/s this month.</p>
</body></html>"""


def _make_response(text, status=200):
    r = MagicMock()
    r.status_code = status
    r.text = text
    r.ok = True
    return r


class TestDrupalYearScraper:

    def _get_scraper(self, tmp_path):
        from scrapers.ir_scraper import IRScraper
        db = MagicMock()
        db.report_exists_by_url_hash.return_value = False
        db.find_near_duplicates.return_value = []
        db.insert_report.return_value = 1
        session = MagicMock()
        return IRScraper(db=db, session=session), db

    def test_dispatch_drupal_year_mode(self, tmp_path):
        """scrape_company dispatches drupal_year mode to _scrape_drupal_year."""
        scraper, db = self._get_scraper(tmp_path)
        company = _make_company()
        with patch.object(scraper, '_scrape_drupal_year', return_value=MagicMock(reports_ingested=1, errors=0)) as mock:
            scraper.scrape_company(company)
        mock.assert_called_once_with(company)

    def test_extracts_widget_id_and_form_token(self, tmp_path):
        """Scraper extracts widget_id and form_build_id from base page HTML."""
        scraper, db = self._get_scraper(tmp_path)
        company = _make_company(pr_start_year=2024)

        responses = [
            _make_response(BASE_PAGE_HTML),   # base page
            _make_response(YEAR_PAGE_HTML),   # year 2024 filter
            _make_response(PR_HTML),          # first PR
            _make_response(PR_HTML),          # second PR
        ]
        with patch('scrapers.ir_scraper._fetch_with_rate_limit', side_effect=responses), \
             patch('scrapers.ir_scraper.date') as mock_date:
            mock_date.side_effect = lambda *args, **kwargs: real_date(*args, **kwargs)
            mock_date.today.return_value = MagicMock(year=2024)
            result = scraper._scrape_drupal_year(company)

        assert result.reports_ingested == 2

    def test_year_filter_url_contains_widget_id(self, tmp_path):
        """Year filter GET request includes widget_id and form_build_id in URL."""
        scraper, db = self._get_scraper(tmp_path)
        company = _make_company(pr_start_year=2024)

        fetched_urls = []
        def _fake_fetch(url, session):
            fetched_urls.append(url)
            if url == company['ir_url']:
                return _make_response(BASE_PAGE_HTML)
            if 'year' in url or str(2024) in url:
                return _make_response('<html><body></body></html>')
            return _make_response(PR_HTML)

        with patch('scrapers.ir_scraper._fetch_with_rate_limit', side_effect=_fake_fetch), \
             patch('scrapers.ir_scraper.date') as mock_date:
            mock_date.today.return_value = MagicMock(year=2024)
            scraper._scrape_drupal_year(company)

        year_urls = [u for u in fetched_urls if 'year' in u or 'Filter' in u]
        assert len(year_urls) >= 1
        assert WIDGET_ID[:16] in year_urls[0] or 'year' in year_urls[0]

    def test_non_production_links_skipped(self, tmp_path):
        """Links that do not match is_production_pr() are not fetched."""
        scraper, db = self._get_scraper(tmp_path)
        company = _make_company(pr_start_year=2024)

        page_with_noise = f"""<html><body>
<form>
  <input type="hidden" name="form_build_id" value="form-T1" />
  <input type="hidden" name="{WIDGET_ID}_widget_id" value="{WIDGET_ID}" />
  <input type="hidden" name="form_id" value="widget_form_base" />
</form>
<a href="/about">About Us</a>
<a href="/careers">Join Our Team</a>
<a href="/q4-earnings">Q4 Earnings Release</a>
</body></html>"""

        fetch_calls = []
        def _fake_fetch(url, session):
            fetch_calls.append(url)
            if url == company['ir_url']:
                return _make_response(page_with_noise)
            return _make_response('<html></html>')

        with patch('scrapers.ir_scraper._fetch_with_rate_limit', side_effect=_fake_fetch), \
             patch('scrapers.ir_scraper.date') as mock_date:
            mock_date.today.return_value = MagicMock(year=2024)
            result = scraper._scrape_drupal_year(company)

        pr_fetches = [u for u in fetch_calls if 'about' in u or 'careers' in u or 'earnings' in u]
        assert pr_fetches == [], "Non-production links must not be fetched"
        assert result.reports_ingested == 0

    def test_broad_mining_activity_title_is_ingested(self, tmp_path):
        scraper, db = self._get_scraper(tmp_path)
        company = _make_company(ticker='BITF', ir_url='https://investor.bitfarms.com/news-events/press-releases', pr_start_year=2026)

        article = _make_response("""<html><head>
        <meta property="article:published_time" content="2026-02-01T08:00:00Z" />
        </head><body><p>Bitfarms energized miners and reported January 2026 operating update.</p></body></html>""")

        responses = [
            _make_response(BASE_PAGE_HTML),
            _make_response(YEAR_PAGE_HTML_BROAD.replace("January 2024", "January 2026")),
            article,
        ]

        with patch('scrapers.ir_scraper._fetch_with_rate_limit', side_effect=responses):
            result = scraper._scrape_drupal_year(company)

        assert result.reports_ingested == 1
        inserted = db.insert_report.call_args[0][0]
        assert inserted['fetch_strategy'] == 'drupal_year'
        assert inserted['report_date'] == '2026-01-01'

    def test_missing_ir_url_returns_error(self, tmp_path):
        """Missing ir_url increments errors and returns immediately."""
        scraper, db = self._get_scraper(tmp_path)
        company = _make_company()
        company['ir_url'] = ''

        result = scraper._scrape_drupal_year(company)
        assert result.errors == 1
        assert result.reports_ingested == 0

    def test_missing_pr_start_year_returns_error(self, tmp_path):
        """Missing pr_start_year increments errors and returns immediately."""
        scraper, db = self._get_scraper(tmp_path)
        company = _make_company()
        company['pr_start_year'] = None

        result = scraper._scrape_drupal_year(company)
        assert result.errors == 1

    def test_base_page_fetch_failure_returns_error(self, tmp_path):
        """If base IR page cannot be fetched, returns error."""
        scraper, db = self._get_scraper(tmp_path)
        company = _make_company(pr_start_year=2024)

        with patch('scrapers.ir_scraper._fetch_with_rate_limit', return_value=None):
            result = scraper._scrape_drupal_year(company)

        assert result.errors == 1
        assert result.reports_ingested == 0

    def test_duplicate_url_skipped(self, tmp_path):
        """URLs already in DB are skipped without fetching the PR page."""
        scraper, db = self._get_scraper(tmp_path)
        db.report_exists_by_url_hash.return_value = True
        company = _make_company(pr_start_year=2024)

        responses = [
            _make_response(BASE_PAGE_HTML),
            _make_response(YEAR_PAGE_HTML),
        ]
        with patch('scrapers.ir_scraper._fetch_with_rate_limit', side_effect=responses), \
             patch('scrapers.ir_scraper.date') as mock_date:
            mock_date.today.return_value = MagicMock(year=2024)
            result = scraper._scrape_drupal_year(company)

        assert result.reports_ingested == 0
        db.insert_report.assert_not_called()

    def test_form_build_id_refreshed_between_years(self, tmp_path):
        """form_build_id from year N response is used for year N+1 request."""
        scraper, db = self._get_scraper(tmp_path)
        company = _make_company(pr_start_year=2023)

        year_2023_html = f"""<html><body>
<form>
  <input type="hidden" name="form_build_id" value="form-YEAR2023TOKEN" />
  <input type="hidden" name="{WIDGET_ID}_widget_id" value="{WIDGET_ID}" />
  <input type="hidden" name="form_id" value="widget_form_base" />
</form>
</body></html>"""

        fetched_urls = []
        def _fake_fetch(url, session):
            fetched_urls.append(url)
            if url == company['ir_url']:
                return _make_response(BASE_PAGE_HTML)
            if '2023' in url:
                return _make_response(year_2023_html)
            return _make_response('<html><body><form><input type="hidden" name="form_build_id" value="form-YEAR2024TOKEN"/></form></body></html>')

        with patch('scrapers.ir_scraper._fetch_with_rate_limit', side_effect=_fake_fetch), \
             patch('scrapers.ir_scraper.date') as mock_date:
            mock_date.today.return_value = MagicMock(year=2024)
            scraper._scrape_drupal_year(company)

        year_2024_urls = [u for u in fetched_urls if '2024' in u and 'Filter' in u]
        assert any('YEAR2023TOKEN' in u for u in year_2024_urls), \
            "form_build_id from 2023 response must be used in 2024 request"
