"""Tests for IR press release scraper helper functions."""
from datetime import date
from unittest.mock import MagicMock, patch
from scrapers.ir_scraper import (
    IRScraper,
    is_production_pr,
    infer_period_from_pr_title,
    parse_rss_feed,
    expand_url_template,
)


class TestIsProductionPR:
    def test_mara_monthly_update(self):
        assert is_production_pr(
            "Marathon Digital Holdings Announces October 2024 Production and Operations Update"
        )

    def test_riot_monthly_update(self):
        assert is_production_pr(
            "Riot Platforms Announces October 2024 Production and Operations Update"
        )

    def test_reject_financial_results_title(self):
        assert not is_production_pr(
            "Marathon Digital Holdings Announces Q3 2024 Financial Results"
        )

    def test_reject_earnings_release(self):
        assert not is_production_pr(
            "MARA Reports Fourth Quarter and Full Year 2023 Earnings"
        )

    def test_accept_mining_operations_title(self):
        assert is_production_pr(
            "CleanSpark Announces Monthly Bitcoin Mining Operations Update for August 2024"
        )


_RSS_FEED_XML = """<?xml version="1.0" encoding="UTF-8"?>
<rss version="2.0">
  <channel>
    <title>MARA Holdings Press Releases</title>
    <item>
      <title>MARA Announces Bitcoin Production Updates for September 2025</title>
      <link>https://ir.mara.com/news-events/press-releases/detail/1234/mara-september-2025</link>
      <pubDate>Mon, 06 Oct 2025 07:00:00 -0400</pubDate>
    </item>
    <item>
      <title>MARA Announces Bitcoin Production Updates for August 2025</title>
      <link>https://ir.mara.com/news-events/press-releases/detail/1233/mara-august-2025</link>
      <pubDate>Mon, 08 Sep 2025 07:00:00 -0400</pubDate>
    </item>
  </channel>
</rss>"""


class TestRSSMode:
    def test_parse_rss_returns_title_and_link(self):
        items = parse_rss_feed(_RSS_FEED_XML)
        assert len(items) == 2
        assert items[0]["title"] == "MARA Announces Bitcoin Production Updates for September 2025"
        assert items[0]["link"] == "https://ir.mara.com/news-events/press-releases/detail/1234/mara-september-2025"

    def test_parse_rss_returns_pub_date(self):
        items = parse_rss_feed(_RSS_FEED_XML)
        assert items[0]["pub_date"] == "Mon, 06 Oct 2025 07:00:00 -0400"

    def test_is_production_pr_filters_correctly(self):
        assert is_production_pr("MARA Announces Bitcoin Production Updates for September 2025")
        assert not is_production_pr("MARA Announces Q3 2025 Financial Results")
        assert not is_production_pr("MARA Prices $700 Million Convertible Notes")

    def test_parse_rss_empty_feed(self):
        empty_xml = """<?xml version="1.0"?><rss version="2.0"><channel></channel></rss>"""
        items = parse_rss_feed(empty_xml)
        assert items == []

    def test_parse_rss_malformed_xml_returns_empty(self):
        items = parse_rss_feed("this is not xml <at all")
        assert items == []

    def test_parse_rss_item_missing_title_skipped(self):
        xml = """<?xml version="1.0"?>
<rss version="2.0"><channel>
  <item><link>https://example.com/1</link></item>
  <item><title>Valid Title</title><link>https://example.com/2</link></item>
</channel></rss>"""
        items = parse_rss_feed(xml)
        assert len(items) == 1
        assert items[0]["title"] == "Valid Title"


class TestInferPeriodFromPRTitle:
    def test_october_2024(self):
        result = infer_period_from_pr_title(
            "Riot Announces October 2024 Production and Operations Update"
        )
        assert result == date(2024, 10, 1)

    def test_january_2024(self):
        result = infer_period_from_pr_title(
            "MARA Announces January 2024 Bitcoin Production"
        )
        assert result == date(2024, 1, 1)

    def test_no_month_returns_none(self):
        result = infer_period_from_pr_title("Annual Report 2024")
        assert result is None


class TestExpandUrlTemplate:
    """Verify URL template expansion for all three placeholder types."""

    # HUT8 uses lowercase {month} and {year}
    HUT8_TMPL = "https://www.hut8.com/news-insights/press-releases/hut-8-operations-update-for-{month}-{year}"
    # RIOT uses lowercase {month} and {year}
    RIOT_TMPL = "https://www.riotplatforms.com/riot-announces-{month}-{year}-production-and-operations-updates/"
    # CleanSpark uses titlecase {Month} and {year}
    CLSK_TMPL = "https://investors.cleanspark.com/news/news-details/{year}/CleanSpark-Releases-{Month}-{year}-Bitcoin-Mining-Update/default.aspx"

    def test_hut8_march_2025(self):
        url = expand_url_template(self.HUT8_TMPL, date(2025, 3, 1))
        assert url == "https://www.hut8.com/news-insights/press-releases/hut-8-operations-update-for-march-2025"

    def test_hut8_december_2023(self):
        url = expand_url_template(self.HUT8_TMPL, date(2023, 12, 1))
        assert url == "https://www.hut8.com/news-insights/press-releases/hut-8-operations-update-for-december-2023"

    def test_riot_lowercase_month(self):
        url = expand_url_template(self.RIOT_TMPL, date(2024, 10, 1))
        assert url == "https://www.riotplatforms.com/riot-announces-october-2024-production-and-operations-updates/"

    def test_cleanspark_titlecase_month(self):
        url = expand_url_template(self.CLSK_TMPL, date(2024, 8, 1))
        assert "August" in url
        assert "august" not in url.split("CleanSpark-Releases-")[1].split("-")[0]
        assert "2024" in url

    def test_all_twelve_months_lowercase(self):
        expected = [
            "january", "february", "march", "april", "may", "june",
            "july", "august", "september", "october", "november", "december",
        ]
        tmpl = "/{month}/{year}"
        for i, name in enumerate(expected, start=1):
            url = expand_url_template(tmpl, date(2024, i, 1))
            assert url == f"/{name}/2024", f"month {i} failed: {url!r}"

    def test_all_twelve_months_titlecase(self):
        expected_title = [
            "January", "February", "March", "April", "May", "June",
            "July", "August", "September", "October", "November", "December",
        ]
        tmpl = "/{Month}/{year}"
        for i, name in enumerate(expected_title, start=1):
            url = expand_url_template(tmpl, date(2024, i, 1))
            assert url == f"/{name}/2024", f"month {i} titlecase failed: {url!r}"


class TestTemplateModeBackfill:
    """_scrape_template backfill_mode flag: skips fast-forward so all months from
    pr_start_year are attempted regardless of what is already in the DB."""

    _COMPANY_BASE = {
        "ticker": "RIOT",
        "scraper_mode": "template",
        "url_template": "https://www.riotplatforms.com/riot-announces-{month}-{year}-production-and-operations-updates/",
        "pr_start_year": 2020,
        "pr_base_url": "https://www.riotplatforms.com",
    }
    _PR_HTML = "<html><body><p>January 2020 production: 150 BTC mined.</p></body></html>"

    def _make_scraper(self, latest_ir: str = "2025-12-01"):
        db = MagicMock()
        db.latest_ir_period.return_value = latest_ir
        db.report_exists_by_url_hash.return_value = False
        db.find_near_duplicates.return_value = []
        return IRScraper(db=db, session=MagicMock())

    def test_default_fast_forwards_past_covered_history(self):
        """Without backfill_mode, template scraper starts from latest+1 month
        and never attempts months already covered in the DB."""
        scraper = self._make_scraper(latest_ir="2025-12-01")
        company = dict(self._COMPANY_BASE)  # no backfill_mode override

        with patch("scrapers.ir_scraper._fetch_with_rate_limit") as mock_fetch:
            # All fetches return 404 (Jan 2026 doesn't exist)
            mock_fetch.return_value = None
            scraper._scrape_template(company)

        # First attempted URL should be Jan 2026, not Jan 2020
        if mock_fetch.called:
            first_url = mock_fetch.call_args_list[0][0][0]
            assert "january-2020" not in first_url
            assert "january-2026" in first_url or mock_fetch.call_count == 0

    def test_backfill_mode_starts_from_pr_start_year(self):
        """With backfill_mode=True, template scraper ignores fast-forward and
        starts from pr_start_year even when DB already has recent IR reports."""
        scraper = self._make_scraper(latest_ir="2025-12-01")
        company = {**self._COMPANY_BASE, "backfill_mode": True}

        pr_resp = MagicMock()
        pr_resp.text = self._PR_HTML

        call_urls = []

        def _fetch_side_effect(url, session):
            call_urls.append(url)
            # Only return content for Jan 2020; everything else 404
            if "january-2020" in url:
                return pr_resp
            return None

        with patch("scrapers.ir_scraper._fetch_with_rate_limit", side_effect=_fetch_side_effect):
            result = scraper._scrape_template(company)

        # Jan 2020 URL must have been attempted
        assert any("january-2020" in u for u in call_urls), \
            f"Expected january-2020 in fetched URLs, got: {call_urls[:5]}"
        assert result.reports_ingested == 1
        inserted = scraper.db.insert_report.call_args[0][0]
        assert inserted["report_date"] == "2020-01-01"


class TestIndexModeStopOnAllSeen:
    """_scrape_index early-exit behaviour: stop_on_all_seen flag."""

    _COMPANY_BASE = {
        "ticker": "RIOT",
        "scraper_mode": "index",
        "ir_url": "https://www.riotplatforms.com/press-releases/",
        "pr_base_url": "https://www.riotplatforms.com",
        "pr_start_year": 2020,
    }

    # Two-page listing: page 1 has a 2025 PR (already ingested),
    # page 2 has a 2021 PR (new).
    _PAGE1_HTML = """<html><body>
        <a href="https://www.riotplatforms.com/riot-announces-november-2025-production-and-operations-updates/">
            Riot Announces November 2025 Production Update
        </a>
        <a href="?page=2">2</a>
    </body></html>"""
    _PAGE2_HTML = """<html><body>
        <a href="https://www.riotplatforms.com/riot-announces-january-2021-production-and-operations-updates/">
            Riot Announces January 2021 Production Update
        </a>
    </body></html>"""
    _PR_HTML = "<html><body><p>January 2021 production: 200 BTC mined.</p></body></html>"

    def _make_scraper(self, page1_seen: bool = True):
        db = MagicMock()
        # page1 PR already ingested, page2 PR is new
        db.report_exists_by_url_hash.side_effect = [page1_seen, False]
        db.find_near_duplicates.return_value = []
        return IRScraper(db=db, session=MagicMock())

    def test_default_stops_after_all_seen_page(self):
        """Without stop_on_all_seen=False, scraper halts on the first all-seen page."""
        scraper = self._make_scraper(page1_seen=True)
        company = dict(self._COMPANY_BASE)  # no stop_on_all_seen override

        page1_resp = MagicMock()
        page1_resp.text = self._PAGE1_HTML
        page2_resp = MagicMock()
        page2_resp.text = self._PAGE2_HTML

        with patch("scrapers.ir_scraper._fetch_with_rate_limit",
                   side_effect=[page1_resp, page2_resp]) as mock_fetch:
            result = scraper._scrape_index(company)

        # Only page 1 fetched; early exit triggered before page 2
        assert mock_fetch.call_count == 1
        assert result.reports_ingested == 0

    def test_stop_on_all_seen_false_continues_to_next_page(self):
        """With stop_on_all_seen=False, scraper continues past all-seen pages."""
        scraper = self._make_scraper(page1_seen=True)
        company = {**self._COMPANY_BASE, "stop_on_all_seen": False}

        page1_resp = MagicMock()
        page1_resp.text = self._PAGE1_HTML
        page2_resp = MagicMock()
        page2_resp.text = self._PAGE2_HTML
        pr_resp = MagicMock()
        pr_resp.text = self._PR_HTML

        with patch("scrapers.ir_scraper._fetch_with_rate_limit",
                   side_effect=[page1_resp, page2_resp, pr_resp]):
            result = scraper._scrape_index(company)

        assert result.reports_ingested == 1
        scraper.db.insert_report.assert_called_once()
        inserted = scraper.db.insert_report.call_args[0][0]
        assert inserted["report_date"] == "2021-01-01"
        assert inserted["ticker"] == "RIOT"


class TestScrapeModeDispatch:
    def test_dispatch_uses_scraper_mode_key(self):
        scraper = IRScraper(db=MagicMock(), session=MagicMock())
        company = {"ticker": "MARA", "scraper_mode": "rss"}
        with patch.object(scraper, "_scrape_rss", return_value="ok") as rss:
            assert scraper.scrape_company(company) == "ok"
            rss.assert_called_once_with(company)

    def test_dispatch_falls_back_to_legacy_scrape_mode_key(self):
        scraper = IRScraper(db=MagicMock(), session=MagicMock())
        company = {"ticker": "MARA", "scrape_mode": "index"}
        with patch.object(scraper, "_scrape_index", return_value="ok") as idx:
            assert scraper.scrape_company(company) == "ok"
            idx.assert_called_once_with(company)

    def test_dispatch_playwright_mode(self):
        scraper = IRScraper(db=MagicMock(), session=MagicMock())
        company = {"ticker": "HUT8", "scraper_mode": "playwright", "ir_url": "https://hut8.com/news"}
        with patch.object(scraper, "_scrape_playwright", return_value="ok") as pw:
            assert scraper.scrape_company(company) == "ok"
            pw.assert_called_once_with(company)


class TestPlaywrightScraper:

    def _make_company(self, **kwargs):
        base = {
            "ticker": "HUT8",
            "scraper_mode": "playwright",
            "ir_url": "https://hut8.com/news",
            "pr_base_url": "https://hut8.com",
        }
        base.update(kwargs)
        return base

    def _make_scraper(self):
        db = MagicMock()
        db.report_exists_by_url_hash.return_value = False
        db.find_near_duplicates.return_value = []
        return IRScraper(db=db, session=MagicMock())

    def test_missing_ir_url_returns_error(self):
        """playwright mode with no ir_url returns summary with errors=1."""
        scraper = self._make_scraper()
        company = self._make_company(ir_url="")
        result = scraper._scrape_playwright(company)
        assert result.errors == 1
        assert result.reports_ingested == 0

    def test_playwright_not_installed_returns_error(self):
        """If playwright is not installed, returns summary with errors=1 (no crash)."""
        scraper = self._make_scraper()
        company = self._make_company()
        with patch.dict("sys.modules", {"playwright": None, "playwright.sync_api": None}):
            result = scraper._scrape_playwright(company)
        assert result.errors == 1
        assert result.reports_ingested == 0

    def test_playwright_ingests_production_pr(self):
        """With mocked Playwright, a matching PR link gets ingested."""
        scraper = self._make_scraper()
        company = self._make_company()

        index_html = """<html><body>
            <a href="/news/hut8-announces-march-2025-bitcoin-production-update">
                HUT 8 Announces March 2025 Bitcoin Production Update
            </a>
        </body></html>"""
        pr_html = "<html><body><p>March 2025 production: 500 BTC mined.</p></body></html>"

        mock_page = MagicMock()
        mock_page.content.side_effect = [index_html, pr_html]
        mock_browser = MagicMock()
        mock_browser.new_context.return_value.__enter__ = MagicMock()
        mock_context = MagicMock()
        mock_context.new_page.return_value = mock_page
        mock_browser.new_context.return_value = mock_context

        mock_pw = MagicMock()
        mock_pw.__enter__ = MagicMock(return_value=mock_pw)
        mock_pw.__exit__ = MagicMock(return_value=False)
        mock_pw.chromium.launch.return_value = mock_browser

        with patch("scrapers.ir_scraper.sync_playwright", return_value=mock_pw):
            result = scraper._scrape_playwright(company)

        assert result.reports_ingested == 1
        assert result.errors == 0
        scraper.db.insert_report.assert_called_once()
        call_kwargs = scraper.db.insert_report.call_args[0][0]
        assert call_kwargs["ticker"] == "HUT8"
        assert call_kwargs["source_type"] == "ir_press_release"

    def test_playwright_skips_already_ingested(self):
        """URLs already in DB are skipped without re-inserting."""
        scraper = self._make_scraper()
        scraper.db.report_exists_by_url_hash.return_value = True
        company = self._make_company()

        index_html = """<html><body>
            <a href="https://hut8.com/news/hut8-march-2025-bitcoin-production">
                HUT 8 Announces March 2025 Bitcoin Production Update
            </a>
        </body></html>"""

        mock_page = MagicMock()
        mock_page.content.return_value = index_html
        mock_browser = MagicMock()
        mock_context = MagicMock()
        mock_context.new_page.return_value = mock_page
        mock_browser.new_context.return_value = mock_context

        mock_pw = MagicMock()
        mock_pw.__enter__ = MagicMock(return_value=mock_pw)
        mock_pw.__exit__ = MagicMock(return_value=False)
        mock_pw.chromium.launch.return_value = mock_browser

        with patch("scrapers.ir_scraper.sync_playwright", return_value=mock_pw):
            result = scraper._scrape_playwright(company)

        assert result.reports_ingested == 0
        scraper.db.insert_report.assert_not_called()

    def test_playwright_skips_relative_href_with_empty_base_url(self):
        """Relative hrefs must be skipped (not concatenated) when pr_base_url is empty."""
        scraper = self._make_scraper()
        company = self._make_company(pr_base_url='')

        index_html = """<html><body>
            <a href="/news/hut8-march-2025-bitcoin-production">
                HUT 8 Announces March 2025 Bitcoin Production Update
            </a>
        </body></html>"""

        mock_page = MagicMock()
        mock_page.content.return_value = index_html
        mock_browser = MagicMock()
        mock_context = MagicMock()
        mock_context.new_page.return_value = mock_page
        mock_browser.new_context.return_value = mock_context

        mock_pw = MagicMock()
        mock_pw.__enter__ = MagicMock(return_value=mock_pw)
        mock_pw.__exit__ = MagicMock(return_value=False)
        mock_pw.chromium.launch.return_value = mock_browser

        with patch("scrapers.ir_scraper.sync_playwright", return_value=mock_pw):
            result = scraper._scrape_playwright(company)

        assert result.reports_ingested == 0
        scraper.db.insert_report.assert_not_called()

    def test_playwright_filters_non_production_links(self):
        """Links that are not production PRs are not ingested."""
        scraper = self._make_scraper()
        company = self._make_company()

        index_html = """<html><body>
            <a href="/news/hut8-q3-2025-financial-results">HUT 8 Q3 2025 Financial Results</a>
            <a href="/news/hut8-prices-convertible-notes">HUT 8 Prices Convertible Notes</a>
        </body></html>"""

        mock_page = MagicMock()
        mock_page.content.return_value = index_html
        mock_browser = MagicMock()
        mock_context = MagicMock()
        mock_context.new_page.return_value = mock_page
        mock_browser.new_context.return_value = mock_context

        mock_pw = MagicMock()
        mock_pw.__enter__ = MagicMock(return_value=mock_pw)
        mock_pw.__exit__ = MagicMock(return_value=False)
        mock_pw.chromium.launch.return_value = mock_browser

        with patch("scrapers.ir_scraper.sync_playwright", return_value=mock_pw):
            result = scraper._scrape_playwright(company)

        assert result.reports_ingested == 0
        scraper.db.insert_report.assert_not_called()
