"""Tests for IR press release scraper helper functions."""
from datetime import date
from unittest.mock import MagicMock, patch
from scrapers.ir_scraper import (
    IRScraper,
    _playwright_collect_all_pages,
    candidate_urls_for_period,
    cleanspark_candidate_urls,
    discovery_links_from_html,
    discovery_page_urls_for_company,
    is_bot_challenge_page,
    riot_candidate_urls,
    is_mining_activity_pr,
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


class TestMiningActivityPR:
    def test_accepts_broader_mining_activity(self):
        assert is_mining_activity_pr(
            "CleanSpark Expands Bitcoin Mining Fleet and Announces January 2024 Operational Update"
        )

    def test_rejects_financial_results(self):
        assert not is_mining_activity_pr(
            "Riot Platforms Reports Q1 2024 Financial Results"
        )


class TestBotChallengeDetection:
    def test_detects_cloudflare_interstitial_text(self):
        html = "<html><title>Just a moment...</title><body>Enable JavaScript and cookies to continue</body></html>"
        assert is_bot_challenge_page(html, {"server": "cloudflare"}, 403)

    def test_non_challenge_page_is_not_flagged(self):
        html = "<html><body><a href='/news-events/press-releases/detail/1'>Press release</a></body></html>"
        assert not is_bot_challenge_page(html, {"server": "Apache"}, 200)


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


class TestRiotHistoricalTemplates:
    def test_riot_candidates_for_2018_production_yield(self):
        urls = riot_candidate_urls(date(2018, 4, 1))
        assert urls == [
            "https://www.riotplatforms.com/riot-blockchain-releases-april-2018-cryptocurrency-mining-production-yield/"
        ]

    def test_riot_candidates_for_2020_production_update(self):
        urls = riot_candidate_urls(date(2020, 3, 1))
        assert urls[0] == (
            "https://www.riotplatforms.com/riot-blockchain-announces-march-2020-production-update/"
        )

    def test_riot_candidates_for_2021_operations_update(self):
        urls = riot_candidate_urls(date(2021, 7, 1))
        assert urls[0] == (
            "https://www.riotplatforms.com/riot-blockchain-announces-july-production-and-operations-updates/"
        )

    def test_candidate_urls_for_non_riot_uses_configured_template(self):
        urls = candidate_urls_for_period(
            {"ticker": "MARA", "url_template": "https://example.com/{month}-{year}"},
            date(2024, 9, 1),
        )
        assert urls == ["https://example.com/september-2024"]


class TestDiscoveryHelpers:
    def test_cleanspark_discovery_uses_prnewswire_pages(self):
        urls = discovery_page_urls_for_company(
            {
                "ticker": "CLSK",
                "ir_url": "https://investors.cleanspark.com/news/",
                "prnewswire_url": "https://www.prnewswire.com/news/cleanspark%2C%20inc./",
            }
        )
        assert urls[0] == "https://investors.cleanspark.com/news"
        assert urls[1] == "https://investors.cleanspark.com/news?page=2"
        assert "https://www.prnewswire.com/news/cleanspark%2C%20inc." in urls
        assert "https://www.prnewswire.com/news/cleanspark%2C%20inc.?page=2" in urls

    def test_riot_discovery_uses_archive_pages(self):
        urls = discovery_page_urls_for_company(
            {"ticker": "RIOT", "ir_url": "https://www.riotplatforms.com/overview/news-events/press-releases/"}
        )
        assert urls[0] == "https://www.riotplatforms.com/author/b2ieverest456dfghbs/page/1/"
        assert urls[-1] == "https://www.riotplatforms.com/author/b2ieverest456dfghbs/page/60/"

    def test_mara_discovery_uses_query_pagination(self):
        urls = discovery_page_urls_for_company(
            {"ticker": "MARA", "ir_url": "https://ir.mara.com/news-events/press-releases"}
        )
        assert urls[0] == "https://ir.mara.com/news-events/press-releases"
        assert urls[1] == "https://ir.mara.com/news-events/press-releases?page=2"

    def test_discovery_link_extraction_filters_to_mining_activity(self):
        company = {"ticker": "CLSK", "pr_base_url": "https://investors.cleanspark.com"}
        html = """
        <html><body>
          <a href="/news/news-details/2024/CleanSpark-Releases-January-2024-Bitcoin-Mining-Update/default.aspx">
            CleanSpark Releases January 2024 Bitcoin Mining Update
          </a>
          <a href="/news/news-details/2024/CleanSpark-Reports-Q1-2024-Financial-Results/default.aspx">
            CleanSpark Reports Q1 2024 Financial Results
          </a>
        </body></html>
        """
        links = discovery_links_from_html(company, html, "https://investors.cleanspark.com/news")
        assert len(links) == 1
        assert links[0][1].startswith("https://investors.cleanspark.com/news/news-details/2024/")
        assert links[0][2] == date(2024, 1, 1)

    def test_discovery_link_extraction_handles_prnewswire_articles(self):
        company = {"ticker": "CLSK"}
        html = """
        <html><body>
          <a href="/news-releases/cleanspark-releases-january-2026-operational-update-302678881.html">
            CleanSpark Releases January 2026 Operational Update
          </a>
          <a href="/news-releases/cleanspark-delivers-181-million-in-q1-revenue-302680687.html">
            CleanSpark Delivers $181 Million in Q1 Revenue
          </a>
        </body></html>
        """
        links = discovery_links_from_html(company, html, "https://www.prnewswire.com/news/cleanspark%2C%20inc./")
        assert len(links) == 1
        assert links[0][1] == "https://www.prnewswire.com/news-releases/cleanspark-releases-january-2026-operational-update-302678881.html"
        assert links[0][2] == date(2026, 1, 1)

    def test_discovery_link_extraction_keeps_mara_ir_detail_pages(self):
        company = {"ticker": "MARA", "pr_base_url": "https://ir.mara.com"}
        html = """
        <html><body>
          <a href="/news-events/press-releases/detail/1400/mara-prices-convertible-notes-offering">
            MARA Prices Convertible Notes Offering
          </a>
        </body></html>
        """
        links = discovery_links_from_html(company, html, "https://ir.mara.com/news-events/press-releases")
        assert len(links) == 1
        assert links[0][1] == (
            "https://ir.mara.com/news-events/press-releases/detail/1400/"
            "mara-prices-convertible-notes-offering"
        )


class TestCleanSparkHistoricalTemplates:
    def test_cleanspark_pre_april_2023_returns_no_candidates(self):
        urls = cleanspark_candidate_urls(date(2023, 3, 1))
        assert urls == []

    def test_cleanspark_candidates_for_2024_bitcoin_update(self):
        urls = cleanspark_candidate_urls(date(2024, 1, 1))
        assert urls == [
            "https://investors.cleanspark.com/news/news-details/2024/CleanSpark-Releases-January-2024-Bitcoin-Mining-Update/default.aspx"
        ]

    def test_cleanspark_candidates_for_2026_operational_update(self):
        urls = cleanspark_candidate_urls(date(2026, 1, 1))
        assert urls[0] == (
            "https://investors.cleanspark.com/news/news-details/2026/CleanSpark-Releases-January-2026-Operational-Update/default.aspx"
        )
        assert urls[1] == (
            "https://investors.cleanspark.com/news/news-details/2026/CleanSpark-Releases-January-2026-Bitcoin-Mining-Update/default.aspx"
        )

    def test_cleanspark_december_2024_candidates_try_publish_year_first(self):
        urls = cleanspark_candidate_urls(date(2024, 12, 1))
        assert urls[0] == (
            "https://investors.cleanspark.com/news/news-details/2025/CleanSpark-Releases-December-2024-Bitcoin-Mining-Update/default.aspx"
        )
        assert urls[1] == (
            "https://investors.cleanspark.com/news/news-details/2024/CleanSpark-Releases-December-2024-Bitcoin-Mining-Update/default.aspx"
        )

    def test_cleanspark_2023_october_uses_explicit_historical_override(self):
        urls = cleanspark_candidate_urls(date(2023, 10, 1))
        assert urls[0] == (
            "https://investors.cleanspark.com/news/news-details/2023/CleanSpark-Releases-October-2023-Bitcoin-Mining-Update-2023-_50Bd5BLR9/default.aspx"
        )

    def test_cleanspark_2023_april_uses_publish_date_suffix_override(self):
        urls = cleanspark_candidate_urls(date(2023, 4, 1))
        assert urls[0] == (
            "https://investors.cleanspark.com/news/news-details/2023/CleanSpark-Releases-April-2023-Bitcoin-Mining-Update-05-03-2023/default.aspx"
        )


class TestTemplateModeBackfill:
    """_scrape_template backfill_mode flag: skips fast-forward so all months from
    pr_start_year are attempted regardless of what is already in the DB."""

    _COMPANY_BASE = {
        "ticker": "RIOT",
        "scraper_mode": "template",
        "url_template": "https://www.riotplatforms.com/riot-announces-{month}-{year}-production-and-operations-updates/",
        "pr_start_date": "2020-01-01",
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

        def _fetch_side_effect(url, session, **kwargs):
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

    def test_riot_historical_slug_fallback_uses_working_candidate(self):
        scraper = self._make_scraper(latest_ir=None)
        company = {**self._COMPANY_BASE, "pr_start_date": "2020-01-01", "backfill_mode": True}

        pr_resp = MagicMock()
        pr_resp.text = self._PR_HTML
        call_urls = []

        def _fetch_side_effect(url, session, **kwargs):
            call_urls.append(url)
            if "riot-blockchain-announces-january-2020-production-update" in url:
                return pr_resp
            return None

        with patch("scrapers.ir_scraper._fetch_with_rate_limit", side_effect=_fetch_side_effect):
            result = scraper._scrape_template(company)

        assert result.reports_ingested == 1
        assert any("riot-blockchain-announces-january-2020-production-update" in u for u in call_urls)
        inserted = scraper.db.insert_report.call_args[0][0]
        assert inserted["source_url"].endswith("/riot-blockchain-announces-january-2020-production-update")

    def test_cleanspark_operational_update_fallback_uses_working_candidate(self):
        scraper = self._make_scraper(latest_ir="2025-12-01")
        company = {
            "ticker": "CLSK",
            "scraper_mode": "template",
            "url_template": "https://investors.cleanspark.com/news/news-details/{year}/CleanSpark-Releases-{Month}-{year}-Bitcoin-Mining-Update/default.aspx",
            "pr_start_date": "2024-01-01",
            "pr_base_url": "https://investors.cleanspark.com",
        }

        pr_resp = MagicMock()
        pr_resp.text = "<html><body><p>January 2026 operations update: 626 BTC mined.</p></body></html>"
        call_urls = []

        def _fetch_side_effect(url, session, **kwargs):
            call_urls.append(url)
            if "CleanSpark-Releases-January-2026-Operational-Update" in url:
                return pr_resp
            return None

        with patch("scrapers.ir_scraper._fetch_with_rate_limit", side_effect=_fetch_side_effect):
            result = scraper._scrape_template(company)

        assert result.reports_ingested == 1
        assert any("CleanSpark-Releases-January-2026-Operational-Update" in u for u in call_urls)
        inserted = scraper.db.insert_report.call_args[0][0]
        assert inserted["ticker"] == "CLSK"
        assert inserted["source_url"].endswith("/CleanSpark-Releases-January-2026-Operational-Update/default.aspx")


class TestIndexModeStopOnAllSeen:
    """_scrape_index early-exit behaviour: stop_on_all_seen flag."""

    _COMPANY_BASE = {
        "ticker": "RIOT",
        "scraper_mode": "index",
        "ir_url": "https://www.riotplatforms.com/press-releases/",
        "pr_base_url": "https://www.riotplatforms.com",
        "pr_start_date": "2020-01-01",
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


class TestDiscoveryMode:
    def test_scrape_discovery_ingests_discovered_article(self):
        db = MagicMock()
        db.report_exists_by_url_hash.return_value = False
        db.find_near_duplicates.return_value = []
        scraper = IRScraper(db=db, session=MagicMock())
        company = {
            "ticker": "CLSK",
            "scraper_mode": "discovery",
            "ir_url": "https://investors.cleanspark.com/news",
            "pr_base_url": "https://investors.cleanspark.com",
            "pr_start_date": "2020-01-01",
        }

        listing_resp = MagicMock()
        listing_resp.text = """
        <html><body>
          <a href="/news/news-details/2024/CleanSpark-Releases-January-2024-Bitcoin-Mining-Update/default.aspx">
            CleanSpark Releases January 2024 Bitcoin Mining Update
          </a>
        </body></html>
        """
        article_resp = MagicMock()
        article_resp.text = """
        <html><head><meta property="article:published_time" content="2024-02-05T08:00:00Z" /></head>
        <body><h1>CleanSpark Releases January 2024 Bitcoin Mining Update</h1><p>Mined 626 BTC.</p></body></html>
        """

        def _fake_fetch(url, session, **kwargs):
            if "January-2024-Bitcoin-Mining-Update" in url:
                return article_resp
            return None

        with patch("scrapers.ir_scraper._playwright_collect_all_pages", return_value=[listing_resp.text]), patch(
            "scrapers.ir_scraper._fetch_with_rate_limit",
            side_effect=_fake_fetch,
        ):
            result = scraper._scrape_discovery(company)

        assert result.reports_ingested == 1
        inserted = db.insert_report.call_args[0][0]
        assert inserted["fetch_strategy"] == "discovery"
        assert inserted["report_date"] == "2024-01-01"
        assert inserted["published_date"] == "2024-02-05"

    def test_scrape_discovery_stores_prnewswire_source_type(self):
        db = MagicMock()
        db.report_exists_by_url_hash.return_value = False
        db.find_near_duplicates.return_value = []
        scraper = IRScraper(db=db, session=MagicMock())
        company = {
            "ticker": "CLSK",
            "scraper_mode": "discovery",
            "ir_url": "https://investors.cleanspark.com/news/",
            "prnewswire_url": "https://www.prnewswire.com/news/cleanspark%2C%20inc./",
            "pr_start_date": "2023-01-01",
        }

        listing_resp = MagicMock()
        listing_resp.text = """
        <html><body>
          <a href="/news-releases/cleanspark-releases-january-2026-operational-update-302678881.html">
            CleanSpark Releases January 2026 Operational Update
          </a>
        </body></html>
        """
        article_resp = MagicMock()
        article_resp.text = """
        <html><head><meta property="article:published_time" content="2026-02-05T08:00:00Z" /></head>
        <body><h1>CleanSpark Releases January 2026 Operational Update</h1><p>Mined BTC in January 2026.</p></body></html>
        """

        def _fake_fetch(url, session, **kwargs):
            if "prnewswire.com/news/cleanspark%2C%20inc." in url:
                return listing_resp
            if "operational-update-302678881.html" in url:
                return article_resp
            return None

        with patch("scrapers.ir_scraper._playwright_collect_all_pages", return_value=[]), patch(
            "scrapers.ir_scraper._fetch_with_rate_limit",
            side_effect=_fake_fetch,
        ):
            result = scraper._scrape_discovery(company)

        assert result.reports_ingested == 1
        inserted = db.insert_report.call_args[0][0]
        assert inserted["source_type"] == "prnewswire_press_release"
        assert inserted["report_date"] == "2026-01-01"

    def test_scrape_discovery_keeps_insert_order_deterministic_with_parallel_fetch(self):
        db = MagicMock()
        db.report_exists_by_url_hash.return_value = False
        db.find_near_duplicates.return_value = []
        scraper = IRScraper(db=db, session=MagicMock())
        company = {
            "ticker": "MARA",
            "scraper_mode": "discovery",
            "ir_url": "https://ir.mara.com/news-events/press-releases",
            "pr_base_url": "https://ir.mara.com",
            "pr_start_date": "2024-01-01",
        }

        listing_resp = MagicMock()
        listing_resp.text = """
        <html><body>
          <a href="/news-events/press-releases/detail/1/mara-announces-january-2024-update">
            MARA Announces January 2024 Update
          </a>
          <a href="/news-events/press-releases/detail/2/mara-announces-february-2024-update">
            MARA Announces February 2024 Update
          </a>
        </body></html>
        """
        jan_resp = MagicMock()
        jan_resp.text = """
        <html><head><meta property="article:published_time" content="2024-02-01T08:00:00Z" /></head>
        <body><h1>MARA Announces January 2024 Update</h1><p>January 2024 update.</p></body></html>
        """
        feb_resp = MagicMock()
        feb_resp.text = """
        <html><head><meta property="article:published_time" content="2024-03-01T08:00:00Z" /></head>
        <body><h1>MARA Announces February 2024 Update</h1><p>February 2024 update.</p></body></html>
        """

        def _fake_fetch(url, session, **kwargs):
            if "detail/1/" in url:
                return jan_resp
            if "detail/2/" in url:
                return feb_resp
            return None

        with patch("scrapers.ir_scraper._playwright_collect_all_pages", return_value=[listing_resp.text]), patch(
            "scrapers.ir_scraper._fetch_with_rate_limit",
            side_effect=_fake_fetch,
        ):
            scraper._scrape_discovery(company)

        inserted_rows = [call.args[0] for call in db.insert_report.call_args_list]
        assert [row["report_date"] for row in inserted_rows] == ["2024-01-01", "2024-02-01"]

    def test_scrape_discovery_uses_isolated_sessions_for_parallel_detail_fetches(self):
        db = MagicMock()
        db.report_exists_by_url_hash.return_value = False
        db.find_near_duplicates.return_value = []
        scraper = IRScraper(db=db, session=MagicMock())
        company = {
            "ticker": "MARA",
            "scraper_mode": "discovery",
            "ir_url": "https://ir.mara.com/news-events/press-releases",
            "pr_base_url": "https://ir.mara.com",
            "pr_start_date": "2024-01-01",
        }

        listing_resp = MagicMock()
        listing_resp.text = """
        <html><body>
          <a href="/news-events/press-releases/detail/1/mara-announces-january-2024-update">
            MARA Announces January 2024 Update
          </a>
          <a href="/news-events/press-releases/detail/2/mara-announces-february-2024-update">
            MARA Announces February 2024 Update
          </a>
        </body></html>
        """
        jan_resp = MagicMock()
        jan_resp.text = """
        <html><head><meta property="article:published_time" content="2024-02-01T08:00:00Z" /></head>
        <body><h1>MARA Announces January 2024 Update</h1><p>January 2024 update.</p></body></html>
        """
        feb_resp = MagicMock()
        feb_resp.text = """
        <html><head><meta property="article:published_time" content="2024-03-01T08:00:00Z" /></head>
        <body><h1>MARA Announces February 2024 Update</h1><p>February 2024 update.</p></body></html>
        """

        sessions_created = []

        def _fake_fetch(url, session, **kwargs):
            if "detail/1/" in url:
                return jan_resp
            if "detail/2/" in url:
                return feb_resp
            return None

        class _FakeSession:
            def __enter__(self):
                sessions_created.append(self)
                return self
            def __exit__(self, exc_type, exc, tb):
                return False

        with patch("scrapers.ir_scraper._playwright_collect_all_pages", return_value=[listing_resp.text]), patch(
            "scrapers.ir_scraper._fetch_with_rate_limit",
            side_effect=_fake_fetch,
        ), patch("scrapers.ir_scraper.requests.Session", side_effect=lambda: _FakeSession()):
            scraper._scrape_discovery(company)

        assert len(sessions_created) == 2
        assert db.insert_report.call_count == 2

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

    def test_dispatch_discovery_mode(self):
        scraper = IRScraper(db=MagicMock(), session=MagicMock())
        company = {"ticker": "MARA", "scraper_mode": "discovery", "ir_url": "https://ir.mara.com", "pr_start_date": "2020-01-01"}
        with patch.object(scraper, "_scrape_discovery", return_value="ok") as discovery:
            assert scraper.scrape_company(company) == "ok"
            discovery.assert_called_once_with(company)

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


class TestPlaywrightPagination:
    def test_collect_all_pages_skips_non_numeric_pager_controls(self):
        class FakeButton:
            def __init__(self, page, label):
                self.page = page
                self.label = label

            def get_attribute(self, name, timeout=None):
                if name != "aria-current":
                    return None
                return "true" if self.page.current_page == self.label else None

            def text_content(self):
                return self.label

            def is_visible(self, timeout=None):
                return self.label != "..."

            def click(self):
                if self.label.isdigit():
                    self.page.current_page = self.label

        class FakeLocatorList:
            def __init__(self, page):
                self.page = page

            def all(self):
                labels_by_page = {
                    "1": ["1", "2", "3"],
                    "2": ["1", "2", "...", "3"],
                    "3": ["1", "2", "3"],
                }
                return [FakeButton(self.page, label) for label in labels_by_page[self.page.current_page]]

        class FakeFallbackLocator:
            @property
            def first(self):
                return self

            def is_visible(self, timeout=None):
                return False

        class FakePage:
            def __init__(self):
                self.current_page = "1"
                self.current_year = "2026"

            def goto(self, url, wait_until=None, timeout=None):
                return None

            def wait_for_selector(self, selector, timeout=None):
                return None

            def wait_for_timeout(self, timeout):
                return None

            def wait_for_function(self, script, arg=None, timeout=None):
                return None

            def content(self):
                return f"<html><body>/news-details/{self.current_year}/ page-{self.current_page}</body></html>"

            def evaluate(self, script, arg):
                self.current_year = str(arg)
                self.current_page = "1"
                return True

            def locator(self, selector):
                if selector == "button.pager_button":
                    return FakeLocatorList(self)
                return FakeFallbackLocator()

        class FakeContext:
            def new_page(self):
                return FakePage()

        class FakeBrowser:
            def new_context(self, **kwargs):
                return FakeContext()

            def close(self):
                return None

        class FakePlaywright:
            def __enter__(self):
                self.chromium = MagicMock()
                self.chromium.launch.return_value = FakeBrowser()
                return self

            def __exit__(self, exc_type, exc, tb):
                return False

        fake_sync_playwright = MagicMock(return_value=FakePlaywright())

        with patch("playwright.sync_api.sync_playwright", fake_sync_playwright):
            pages = _playwright_collect_all_pages("https://investors.cleanspark.com/news", max_pages=3)

        assert pages == [
            "<html><body>/news-details/2026/ page-1</body></html>",
            "<html><body>/news-details/2026/ page-2</body></html>",
            "<html><body>/news-details/2026/ page-3</body></html>",
        ]

    def test_collect_all_pages_iterates_year_filters(self):
        class FakeButton:
            def __init__(self, page, label):
                self.page = page
                self.label = label

            def get_attribute(self, name, timeout=None):
                if name != "aria-current":
                    return None
                return "true" if self.page.current_page == self.label else None

            def text_content(self):
                return self.label

            def is_visible(self, timeout=None):
                return True

            def click(self):
                if self.label.isdigit():
                    self.page.current_page = self.label

        class FakeLocatorList:
            def __init__(self, page):
                self.page = page

            def all(self):
                labels_by_state = {
                    ("2026", "1"): ["1", "2"],
                    ("2026", "2"): ["1", "2"],
                    ("2025", "1"): ["1"],
                    ("2020", "1"): ["1"],
                }
                labels = labels_by_state.get((self.page.current_year, self.page.current_page), ["1"])
                return [FakeButton(self.page, label) for label in labels]

        class FakeFallbackLocator:
            @property
            def first(self):
                return self

            def is_visible(self, timeout=None):
                return False

        class FakePage:
            def __init__(self):
                self.current_page = "1"
                self.current_year = "2026"

            def goto(self, url, wait_until=None, timeout=None):
                return None

            def wait_for_selector(self, selector, timeout=None):
                return None

            def wait_for_timeout(self, timeout):
                return None

            def wait_for_function(self, script, arg=None, timeout=None):
                return None

            def content(self):
                return (
                    f"<html><body>/news-details/{self.current_year}/ "
                    f"page-{self.current_page}</body></html>"
                )

            def evaluate(self, script, arg):
                self.current_year = str(arg)
                self.current_page = "1"
                return self.current_year in {"2025", "2020"}

            def locator(self, selector):
                if selector == "button.pager_button":
                    return FakeLocatorList(self)
                return FakeFallbackLocator()

        class FakeContext:
            def new_page(self):
                return FakePage()

        class FakeBrowser:
            def new_context(self, **kwargs):
                return FakeContext()

            def close(self):
                return None

        class FakePlaywright:
            def __enter__(self):
                self.chromium = MagicMock()
                self.chromium.launch.return_value = FakeBrowser()
                return self

            def __exit__(self, exc_type, exc, tb):
                return False

        fake_sync_playwright = MagicMock(return_value=FakePlaywright())

        with patch("playwright.sync_api.sync_playwright", fake_sync_playwright):
            pages = _playwright_collect_all_pages(
                "https://investors.cleanspark.com/news",
                max_pages=3,
                min_year=2020,
            )

        assert pages == [
            "<html><body>/news-details/2026/ page-1</body></html>",
            "<html><body>/news-details/2026/ page-2</body></html>",
            "<html><body>/news-details/2025/ page-1</body></html>",
            "<html><body>/news-details/2020/ page-1</body></html>",
        ]


class TestGetPrStartDate:
    def test_returns_date_from_pr_start_date_string(self):
        from scrapers.ir_scraper import _get_pr_start_date
        from datetime import date
        company = {'pr_start_date': '2020-12-10'}
        assert _get_pr_start_date(company) == date(2020, 12, 10)

    def test_fallback_to_pr_start_year(self):
        from scrapers.ir_scraper import _get_pr_start_date
        from datetime import date
        company = {'pr_start_year': 2020}
        assert _get_pr_start_date(company) == date(2020, 1, 1)

    def test_pr_start_date_takes_priority_over_pr_start_year(self):
        from scrapers.ir_scraper import _get_pr_start_date
        from datetime import date
        company = {'pr_start_date': '2020-12-10', 'pr_start_year': 2018}
        assert _get_pr_start_date(company) == date(2020, 12, 10)

    def test_returns_none_when_neither_set(self):
        from scrapers.ir_scraper import _get_pr_start_date
        assert _get_pr_start_date({}) is None
        assert _get_pr_start_date({'pr_start_date': None, 'pr_start_year': None}) is None

    def test_invalid_date_falls_back_to_year(self):
        from scrapers.ir_scraper import _get_pr_start_date
        from datetime import date
        company = {'pr_start_date': 'not-a-date', 'pr_start_year': 2021}
        assert _get_pr_start_date(company) == date(2021, 1, 1)
