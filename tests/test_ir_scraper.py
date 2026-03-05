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
