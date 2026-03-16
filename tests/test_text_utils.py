"""Unit tests for src/infra/text_utils.py — html_to_plain and make_html_report_fields."""
import sys
import os
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))


class TestHtmlToPlain:
    def test_strips_tags(self):
        from infra.text_utils import html_to_plain
        result = html_to_plain("<p>Hello <b>world</b></p>")
        assert "Hello" in result
        assert "world" in result
        assert "<" not in result

    def test_empty_string_returns_empty(self):
        from infra.text_utils import html_to_plain
        assert html_to_plain("") == ""

    def test_none_returns_empty(self):
        from infra.text_utils import html_to_plain
        assert html_to_plain(None) == ""  # type: ignore[arg-type]

    def test_separator_newline(self):
        from infra.text_utils import html_to_plain
        result = html_to_plain("<p>Line one</p><p>Line two</p>", separator="\n")
        assert "Line one" in result
        assert "Line two" in result

    def test_plain_text_passthrough(self):
        from infra.text_utils import html_to_plain
        result = html_to_plain("Just plain text, no tags")
        assert result == "Just plain text, no tags"

    def test_q4_shell_page_falls_back_to_meta_description(self):
        from infra.text_utils import html_to_plain
        html = """
        <html>
          <head>
            <title>CleanSpark Announces January 2022 Bitcoin Production</title>
            <meta property="og:description" content="Month-to-month bitcoin production increased 35%. January monthly production: 305. Total BTC holdings as of January 31: 471.">
          </head>
          <body>
            <div>Cookie settings Our Cookie Policy Close We use cookies on q4inc.com</div>
            <div>All changes will be saved automatically.</div>
          </body>
        </html>
        """
        result = html_to_plain(html)
        assert "January monthly production: 305" in result
        assert "Cookie settings" not in result

    def test_q4_shell_page_with_article_body_returns_article_content(self):
        """When a Playwright-rendered Equisolve page has both a Q4 cookie banner
        and an actual article body container, html_to_plain should return the
        article body — not just the short og:description meta tag."""
        from infra.text_utils import html_to_plain
        article_body = (
            "CleanSpark Provides Bitcoin Mining Operation Update. "
            "For the month of January 2021, CleanSpark successfully mined 305 BTC. "
            "Deployed hashrate of approximately 1.5 EH/s as of January 31, 2021. "
            "Bitcoin holdings as of January 31, 2021: 471 BTC. "
            "The company continues to expand its mining operations in Georgia. "
            "Management commentary: We believe in taking a big-picture approach to bitcoin mining "
            "and are focused on sustainable and responsible growth."
        )
        html = f"""
        <html>
          <head>
            <meta property="og:description" content="January production: 305 BTC.">
          </head>
          <body>
            <nav>Skip to main content Stock Information Investor Relations Overview</nav>
            <div>Cookie settings Our Cookie Policy Close We use cookies on q4inc.com</div>
            <article>
              {article_body}
            </article>
            <footer>Copyright 2021 CleanSpark Inc.</footer>
          </body>
        </html>
        """
        result = html_to_plain(html)
        assert "1.5 EH/s" in result, "article body metric should be in result"
        assert "471 BTC" in result, "article body BTC holdings should be in result"
        assert "January production: 305 BTC." not in result, "short meta description should be superseded by article body"
        assert "Cookie settings" not in result

    def test_hive_page_extracts_only_article_section(self):
        """HIVE detail pages have nav/footer with links to future articles.

        A 2024-08-01 page that has a sidebar link to a 2026 article must not
        include the 2026 text in raw_text.  The article body lives in
        <section id="news" class="content"> and must be extracted in isolation.
        """
        from infra.text_utils import html_to_plain
        article_body = (
            "HIVE Digital Technologies Provides August 2024 Production Report. "
            "HIVE mined 247.8 BTC in August 2024. "
            "Deployed hashrate reached 6.3 EH/s as of August 31, 2024. "
            "The company continues to expand its renewable-energy-powered data centers. "
            "Total Bitcoin holdings as of August 31, 2024: 2,201 BTC. "
            "Management comments on disciplined capital allocation and growth strategy."
        )
        html = f"""<html>
          <head><title>HIVE Digital Technologies Provides August 2024 Production Report</title></head>
          <body>
            <nav>
              <a href="/news/hive-digital-q1-2026-update/">In 2026 HIVE's renewable energy capacity</a>
              <a href="/news/hive-digital-february-2026-production/">February 2026 Production Report</a>
            </nav>
            <section id="news" class="content">
              <p>{article_body}</p>
            </section>
            <footer>
              <a href="/news/hive-digital-february-2026-production/">See our 2026 reports</a>
              Copyright 2024 HIVE Digital Technologies Ltd.
            </footer>
          </body>
        </html>"""
        result = html_to_plain(html)
        assert "247.8 BTC" in result, "article body metric must be present"
        assert "6.3 EH/s" in result, "article body hashrate must be present"
        assert "In 2026 HIVE's renewable" not in result, "nav bleed from 2026 article must not appear"
        assert "February 2026 Production Report" not in result, "footer bleed must not appear"

    def test_q4_shell_equisolve_aspnet_id_selector(self):
        """Equisolve ASP.NET pages use IDs like 'divPressReleaseBody' — html_to_plain
        should extract the body even when the container has no semantic tag."""
        from infra.text_utils import html_to_plain
        article_body = (
            "CleanSpark Announces July 2021 Bitcoin Production Results. "
            "Bitcoin Mined during July 2021: 436 BTC. "
            "Average Operational Hashrate: 1.8 EH/s. "
            "Total Bitcoin Holdings as of July 31: 1,407 BTC. "
            "The company continues to scale its operations efficiently. "
            "Looking ahead, management expects continued growth in deployed hashrate."
        )
        html = f"""
        <html>
          <head>
            <meta property="og:description" content="July production: 436 BTC.">
          </head>
          <body>
            <div>Cookie settings Our Cookie Policy Close We use cookies on q4inc.com</div>
            <div id="ctl00_ContentPlaceHolder1_divPressReleaseBody">
              {article_body}
            </div>
          </body>
        </html>
        """
        result = html_to_plain(html)
        assert "1.8 EH/s" in result, "hashrate from article body should be present"
        assert "1,407 BTC" in result, "holdings from article body should be present"
        assert "Cookie settings" not in result


class TestMakeHtmlReportFields:
    def test_returns_both_fields(self):
        from infra.text_utils import make_html_report_fields
        fields = make_html_report_fields("<p>BTC produced: 100</p>")
        assert "raw_html" in fields
        assert "raw_text" in fields

    def test_raw_html_preserves_markup(self):
        from infra.text_utils import make_html_report_fields
        html = "<p>BTC produced: <strong>100</strong></p>"
        fields = make_html_report_fields(html)
        assert "<p>" in fields["raw_html"]
        assert "<strong>" in fields["raw_html"]

    def test_raw_text_is_stripped(self):
        from infra.text_utils import make_html_report_fields
        fields = make_html_report_fields("<p>BTC produced: <strong>100</strong></p>")
        assert "<" not in fields["raw_text"]
        assert "BTC produced" in fields["raw_text"]
        assert "100" in fields["raw_text"]

    def test_raw_html_truncated_at_max(self):
        from infra.text_utils import make_html_report_fields
        big_html = "<p>" + "x" * 400_000 + "</p>"
        fields = make_html_report_fields(big_html, max_raw_html=300_000)
        assert len(fields["raw_html"]) == 300_000

    def test_raw_text_truncated_at_max(self):
        from infra.text_utils import make_html_report_fields
        big_html = "A " * 60_000
        fields = make_html_report_fields(big_html, max_raw_text=50_000)
        assert len(fields["raw_text"]) <= 50_000

    def test_none_html_returns_none_raw_html(self):
        from infra.text_utils import make_html_report_fields
        fields = make_html_report_fields(None)  # type: ignore[arg-type]
        assert fields["raw_html"] is None
        assert fields["raw_text"] == ""

    def test_empty_html_returns_none_raw_html(self):
        from infra.text_utils import make_html_report_fields
        fields = make_html_report_fields("")
        assert fields["raw_html"] is None
        assert fields["raw_text"] == ""

    def test_dict_can_be_spread_into_report(self):
        """Result is safe to ** spread into a report dict alongside other fields."""
        from infra.text_utils import make_html_report_fields
        html = "<p>Bitcoin production: 150 BTC</p>"
        report = {
            "ticker": "MARA",
            "report_date": "2024-01-01",
            **make_html_report_fields(html),
        }
        assert report["ticker"] == "MARA"
        assert report["raw_html"] is not None
        assert "150" in report["raw_text"]

    def test_shell_html_uses_meta_text_for_raw_text(self):
        from infra.text_utils import make_html_report_fields
        html = """
        <html>
          <head>
            <meta property="og:title" content="CleanSpark Announces February 2022 Bitcoin Production">
            <meta property="og:description" content="February monthly production: 276. Total BTC holdings: 566.">
          </head>
          <body>
            <div>Cookie settings Our Cookie Policy Close We use cookies on q4inc.com</div>
          </body>
        </html>
        """
        fields = make_html_report_fields(html)
        assert "February monthly production: 276" in fields["raw_text"]
        assert "Cookie settings" not in fields["raw_text"]


class TestObserverSwarmStoresRawHtml:
    """Verify all observer_swarm insert_report calls include raw_html."""

    def _make_worker(self, db):
        from unittest.mock import MagicMock
        from scrapers.observer_swarm import ScoutWorker
        worker = ScoutWorker.__new__(ScoutWorker)
        worker.db = db
        worker.session = MagicMock()
        return worker

    def test_rss_path_stores_raw_html(self):
        from unittest.mock import MagicMock, patch
        from datetime import date

        db = MagicMock()
        db.report_exists.return_value = False
        db.insert_report.return_value = 1

        rss_text = (
            "<?xml version='1.0'?><rss version='2.0'><channel>"
            "<item><title>MARA April 2021 Production</title>"
            "<link>https://example.com/pr/april2021</link></item>"
            "</channel></rss>"
        )
        pr_html = "<html><body><h1>April 2021 Production</h1><p>750 BTC mined</p></body></html>"

        worker = self._make_worker(db)
        fetch_results = iter([
            _mock_resp(200, rss_text),  # entry_url — RSS detected
            _mock_resp(200, pr_html),   # individual PR page
        ])
        worker._fetch = lambda url: next(fetch_results, _mock_resp(404, ""))
        worker._source_url_exists = MagicMock(return_value=False)
        worker._wire_source_type = MagicMock(return_value="wire_press_release")
        worker._infer_period_fallback = MagicMock(return_value=None)
        worker._infer_period_from_body_text = MagicMock(return_value=None)

        with patch("scrapers.observer_swarm.parse_rss_feed") as mock_rss, \
             patch("scrapers.observer_swarm.is_production_pr", return_value=True), \
             patch("scrapers.observer_swarm.infer_period_from_pr_title", return_value=date(2021, 4, 1)):
            mock_rss.return_value = [{"title": "MARA April 2021", "link": "https://example.com/pr/april2021"}]
            worker._execute_wire_scrape(
                {"ticker": "MARA"},
                {"entry_url": "https://rss.example.com/feed", "family": "globenewswire"},
            )

        assert db.insert_report.called, "insert_report was not called"
        call_args = db.insert_report.call_args[0][0]
        assert "raw_html" in call_args, "observer_swarm RSS path must include raw_html key"
        assert call_args["raw_html"] is not None, "raw_html must not be None"

    def test_wire_listing_path_stores_raw_html(self):
        from unittest.mock import MagicMock, patch
        from datetime import date

        db = MagicMock()
        db.report_exists.return_value = False
        db.insert_report.return_value = 1

        plain_html = "<html><body><p>Not RSS</p></body></html>"
        pr_html = "<html><body><h1>May 2021 Production</h1><p>800 BTC mined</p></body></html>"

        worker = self._make_worker(db)
        call_count = [0]

        def _fetch(url):
            call_count[0] += 1
            if call_count[0] == 1:
                return _mock_resp(200, plain_html)   # entry_url — not RSS
            if call_count[0] == 2:
                return _mock_resp(200, plain_html)   # page 1 listing
            return _mock_resp(200, pr_html)           # individual PR

        worker._fetch = _fetch
        worker._source_url_exists = MagicMock(return_value=False)
        worker._wire_source_type = MagicMock(return_value="wire_press_release")
        worker._parse_wire_listing_links = MagicMock(return_value=[
            ("May 2021 Production", "https://example.com/pr/may2021")
        ])
        worker._infer_period_fallback = MagicMock(return_value=None)
        worker._infer_period_from_body_text = MagicMock(return_value=None)

        with patch("scrapers.observer_swarm.is_production_pr", return_value=True), \
             patch("scrapers.observer_swarm.infer_period_from_pr_title", return_value=date(2021, 5, 1)):
            worker._execute_wire_scrape(
                {"ticker": "MARA"},
                {"entry_url": "https://example.com/news", "family": "globenewswire"},
            )

        assert db.insert_report.called, "insert_report was not called"
        call_args = db.insert_report.call_args[0][0]
        assert "raw_html" in call_args, "observer_swarm wire listing path must include raw_html key"
        assert call_args["raw_html"] is not None, "raw_html must not be None"


class TestEdgarToPlain:
    """Tests for edgar_to_plain() — EDGAR HTML → clean plain text."""

    def test_strips_head_title(self):
        from infra.text_utils import edgar_to_plain
        html = "<html><head><title>10-Q filing</title></head><body><p>Content</p></body></html>"
        result = edgar_to_plain(html)
        assert "10-Q filing" not in result
        assert "Content" in result

    def test_strips_xbrl_preamble(self):
        from infra.text_utils import edgar_to_plain
        preamble = "0001507605 us-gaap:CommonStockMember 2024-06-30\n" * 5
        body = "UNITED STATES SECURITIES AND EXCHANGE COMMISSION\nFORM 10-Q\nBTC mined: 700"
        html = f"<html><body><p>{preamble}</p><p>{body}</p></body></html>"
        result = edgar_to_plain(html)
        assert result.startswith("UNITED STATES")
        assert "0001507605" not in result

    def test_strips_head_and_xbrl_together(self):
        from infra.text_utils import edgar_to_plain
        preamble_line = "0001507605 us-gaap:CommonStockMember 2024-06-30"
        html = (
            "<html><head><title>10-Q</title></head>"
            f"<body><p>{preamble_line}</p>"
            "<p>UNITED STATES SECURITIES AND EXCHANGE COMMISSION</p>"
            "<p>BTC mined: 700</p></body></html>"
        )
        result = edgar_to_plain(html)
        assert "10-Q" not in result
        assert "0001507605" not in result
        assert "UNITED STATES" in result
        assert "700" in result

    def test_preserves_pipe_table_structure(self):
        from infra.text_utils import edgar_to_plain
        html = (
            "<html><body>"
            "<table><tr><th>Metric</th><th>Value</th></tr>"
            "<tr><td>BTC Mined</td><td>700</td></tr></table>"
            "</body></html>"
        )
        result = edgar_to_plain(html)
        assert "BTC Mined | 700" in result

    def test_no_op_on_clean_edgar(self):
        """Filing without preamble is returned as-is (after head strip)."""
        from infra.text_utils import edgar_to_plain
        html = (
            "<html><head><title>10-Q</title></head>"
            "<body><p>UNITED STATES SECURITIES AND EXCHANGE COMMISSION</p>"
            "<p>Item 2. BTC mined: 700</p></body></html>"
        )
        result = edgar_to_plain(html)
        assert "UNITED STATES" in result
        assert "700" in result

    def test_empty_returns_empty(self):
        from infra.text_utils import edgar_to_plain
        assert edgar_to_plain("") == ""

    def test_none_returns_empty(self):
        from infra.text_utils import edgar_to_plain
        assert edgar_to_plain(None) == ""  # type: ignore[arg-type]


class TestStripPressReleaseBoilerplate:
    """Tests for strip_press_release_boilerplate() — IR nav and footer removal."""

    # Minimal simulated Equisolve/ir.mara.com style press release plain text
    _NAV_HEADER = (
        "Marathon Digital Holdings Announces Bitcoin Production : MARA (MARA)\n"
        "Skip to main content\n"
        "Skip to section navigation\n"
        "Skip to footer\n"
        "Stock Information\n"
        "Back to Mara.com\n"
        "Investor Relations\n"
        "Overview\n"
        "News & Events\n"
        "Press Releases\n"
        "IR Calendar\n"
        "Email Alerts\n"
        "News & Events\n"
        "Overview\n"
        "Press Releases\n"
        "IR Calendar\n"
        "Email Alerts\n"
    )
    _ARTICLE_HEADLINE = "Marathon Digital Holdings Announces Bitcoin Production for April 2024\n"
    _DATE_LINE = "May 03, 2024 8:30 am EDT\n"
    _CONTENT = (
        "Download as PDF\n"
        "- Bitcoin Produced: 850 BTC\n"
        "- Average Operational Hash Rate: 21.1 EH/s\n"
        "\n"
        "Bitcoin Produced | 850 | 702 | 21 | %\n"
        "Avg Hash Rate | 21.1 | 10.6 | 99 | %\n"
        "\n"
        "As of April 30, 2024 the Company holds 17,631 unrestricted BTC.\n"
    )
    _RECENT_ANNOUNCEMENTS = (
        "Recent Announcements\n"
        "April 25 - Increases 2024 hash rate target to 50 exahash\n"
        "April 24 - Schedules conference call\n"
    )
    _INVESTOR_NOTICE = (
        "Investor Notice\n"
        "Investing in our securities involves a high degree of risk.\n"
    )
    _FWD_LOOKING = (
        "Forward-Looking Statements\n"
        "This press release contains forward-looking statements.\n"
    )
    _ABOUT = (
        "About Marathon Digital Holdings\n"
        "Marathon is a digital asset technology company.\n"
    )
    _FOOTER = (
        "Source: Marathon Digital Holdings Inc.\n"
        "Released May 3, 2024\n"
        "email\n"
        "Email Alerts\n"
        "© 2024 MARA Holdings, Inc. All Rights Reserved.\n"
    )

    def _full_pr(self):
        return (
            self._NAV_HEADER
            + self._ARTICLE_HEADLINE
            + self._DATE_LINE
            + self._CONTENT
            + self._RECENT_ANNOUNCEMENTS
            + self._INVESTOR_NOTICE
            + self._FWD_LOOKING
            + self._ABOUT
            + self._FOOTER
        )

    def test_strips_nav_header(self):
        from infra.text_utils import strip_press_release_boilerplate
        result = strip_press_release_boilerplate(self._full_pr())
        assert "Skip to main content" not in result
        assert "Back to Mara.com" not in result
        assert "Investor Relations" not in result
        assert "Email Alerts" not in result

    def test_preserves_article_headline(self):
        from infra.text_utils import strip_press_release_boilerplate
        result = strip_press_release_boilerplate(self._full_pr())
        assert "Marathon Digital Holdings Announces Bitcoin Production for April 2024" in result

    def test_preserves_content_and_tables(self):
        from infra.text_utils import strip_press_release_boilerplate
        result = strip_press_release_boilerplate(self._full_pr())
        assert "Bitcoin Produced: 850 BTC" in result
        assert "Bitcoin Produced | 850 | 702 | 21 | %" in result
        assert "17,631 unrestricted BTC" in result

    def test_strips_recent_announcements(self):
        from infra.text_utils import strip_press_release_boilerplate
        result = strip_press_release_boilerplate(self._full_pr())
        assert "Recent Announcements" not in result

    def test_strips_investor_notice(self):
        from infra.text_utils import strip_press_release_boilerplate
        result = strip_press_release_boilerplate(self._full_pr())
        assert "Investor Notice" not in result

    def test_strips_forward_looking_statements(self):
        from infra.text_utils import strip_press_release_boilerplate
        result = strip_press_release_boilerplate(self._full_pr())
        assert "Forward-Looking Statements" not in result

    def test_strips_about_section(self):
        from infra.text_utils import strip_press_release_boilerplate
        result = strip_press_release_boilerplate(self._full_pr())
        assert "About Marathon Digital Holdings" not in result

    def test_strips_source_and_footer(self):
        from infra.text_utils import strip_press_release_boilerplate
        result = strip_press_release_boilerplate(self._full_pr())
        assert "Source: Marathon Digital Holdings Inc." not in result
        assert "© 2024" not in result

    def test_no_op_on_clean_text(self):
        """Text without IR nav/footer is returned unchanged (after strip)."""
        from infra.text_utils import strip_press_release_boilerplate
        clean = "Bitcoin Produced | 850 | 702\nHodl BTC | 17631\n"
        assert strip_press_release_boilerplate(clean) == clean.strip()

    def test_no_op_on_empty_string(self):
        from infra.text_utils import strip_press_release_boilerplate
        assert strip_press_release_boilerplate("") == ""

    def test_no_op_on_none(self):
        from infra.text_utils import strip_press_release_boilerplate
        assert strip_press_release_boilerplate(None) == ""  # type: ignore[arg-type]

    def test_strips_wire_attribution_footer(self):
        """'Distributed by GlobeNewswire' at footer should be stripped."""
        from infra.text_utils import strip_press_release_boilerplate
        text = "Bitcoin Produced: 850 BTC\nHodl BTC: 17,631\nDistributed by GlobeNewswire\nNewswire ID: 12345\n"
        result = strip_press_release_boilerplate(text)
        assert "Distributed by GlobeNewswire" not in result
        assert "850 BTC" in result

    def test_strips_ir_contact_block(self):
        """'Investor Relations Contact' section at end should be stripped."""
        from infra.text_utils import strip_press_release_boilerplate
        content = "Bitcoin Produced: 850 BTC\n" * 10
        footer = "Investor Relations Contact\nJohn Smith\njohn@company.com\n+1-555-1234\n"
        result = strip_press_release_boilerplate(content + footer)
        assert "Investor Relations Contact" not in result
        assert "850 BTC" in result

    def test_strips_media_contact_block(self):
        """'Media Contact' section at end should be stripped."""
        from infra.text_utils import strip_press_release_boilerplate
        content = "Bitcoin Produced: 850 BTC\n" * 10
        footer = "Media Contact\nJane Doe\njane@agency.com\n"
        result = strip_press_release_boilerplate(content + footer)
        assert "Media Contact" not in result
        assert "850 BTC" in result


class TestStripEdgarBoilerplate:
    """Tests for strip_edgar_boilerplate() — SEC filing footer removal."""

    _ITEM_CONTENT = (
        "Item 7.01. Regulation FD Disclosure.\n"
        "Attached hereto as Exhibit 99.1 is a press release dated May 3, 2024.\n"
        "Bitcoin Produced: 850 BTC\n"
        "Average Operational Hash Rate: 21.1 EH/s\n"
        "Bitcoin Holdings: 17,631 BTC\n"
    )
    _SIGNATURES_BLOCK = (
        "SIGNATURES\n"
        "Pursuant to the requirements of the Securities Exchange Act of 1934, "
        "the registrant has duly caused this report to be signed on its behalf "
        "by the undersigned, hereunto duly authorized.\n"
        "MARA Holdings, Inc.\n"
        "By: /s/ Fred Thiel\n"
        "Fred Thiel\n"
        "Chief Executive Officer\n"
        "Date: May 3, 2024\n"
    )
    _EXHIBIT_INDEX = (
        "EXHIBIT INDEX\n"
        "Exhibit No.    Description\n"
        "99.1           Press Release dated May 3, 2024\n"
        "104            Cover Page Interactive Data File\n"
    )

    def _full_8k(self):
        return self._ITEM_CONTENT * 3 + self._EXHIBIT_INDEX + self._SIGNATURES_BLOCK

    def test_strips_signatures_section(self):
        from infra.text_utils import strip_edgar_boilerplate
        result = strip_edgar_boilerplate(self._full_8k())
        assert "SIGNATURES" not in result
        assert "hereunto duly authorized" not in result

    def test_strips_pursuant_text(self):
        from infra.text_utils import strip_edgar_boilerplate
        result = strip_edgar_boilerplate(self._full_8k())
        assert "Pursuant to the requirements of the Securities Exchange Act" not in result

    def test_strips_exhibit_index(self):
        from infra.text_utils import strip_edgar_boilerplate
        result = strip_edgar_boilerplate(self._full_8k())
        assert "EXHIBIT INDEX" not in result

    def test_preserves_item_content(self):
        from infra.text_utils import strip_edgar_boilerplate
        result = strip_edgar_boilerplate(self._full_8k())
        assert "Bitcoin Produced: 850 BTC" in result
        assert "21.1 EH/s" in result
        assert "17,631 BTC" in result

    def test_no_op_on_clean_content(self):
        from infra.text_utils import strip_edgar_boilerplate
        clean = "Item 2. BTC mined: 700\nHashrate: 20 EH/s\n"
        result = strip_edgar_boilerplate(clean)
        assert "BTC mined: 700" in result
        assert "20 EH/s" in result

    def test_safe_on_empty(self):
        from infra.text_utils import strip_edgar_boilerplate
        assert strip_edgar_boilerplate("") == ""

    def test_safe_on_none(self):
        from infra.text_utils import strip_edgar_boilerplate
        assert strip_edgar_boilerplate(None) == ""  # type: ignore[arg-type]


def _mock_resp(status_code, text):
    from unittest.mock import MagicMock
    r = MagicMock()
    r.status_code = status_code
    r.text = text
    return r


class TestBackfillRawHtml:
    """DB backfill method for pre-v24 archive_html reports missing raw_html."""

    def _make_db(self, tmp_path):
        os.environ['MINERS_DB_PATH'] = str(tmp_path / 'test.db')
        from infra.db import MinerDB
        return MinerDB(str(tmp_path / 'test.db'))

    def test_backfill_populates_raw_html_from_file(self, tmp_path):
        db = self._make_db(tmp_path)
        html_file = tmp_path / "mara_april_2021.html"
        html_content = "<html><body><h1>April 2021 Production</h1><p>750 BTC mined.</p></body></html>"
        html_file.write_text(html_content, encoding="utf-8")

        # Insert a pre-v24 style report: raw_html NULL, source_url = local file path
        with db._get_connection() as conn:
            conn.execute(
                """INSERT INTO reports
                   (ticker, report_date, source_type, source_url, raw_text, raw_html, parsed_at)
                   VALUES ('MARA', '2021-04-01', 'archive_html', ?, '750 BTC mined.', NULL, datetime('now'))""",
                (str(html_file),),
            )

        result = db.backfill_raw_html_from_disk()
        assert result["backfilled"] >= 1

        with db._get_connection() as conn:
            row = conn.execute(
                "SELECT raw_html FROM reports WHERE ticker='MARA' AND report_date='2021-04-01'"
            ).fetchone()
        assert row["raw_html"] is not None
        assert "<h1>" in row["raw_html"]

    def test_backfill_skips_when_file_missing(self, tmp_path):
        db = self._make_db(tmp_path)
        with db._get_connection() as conn:
            conn.execute(
                """INSERT INTO reports
                   (ticker, report_date, source_type, source_url, raw_text, raw_html, parsed_at)
                   VALUES ('RIOT', '2021-04-01', 'archive_html', '/no/such/file.html', 'text', NULL, datetime('now'))"""
            )
        result = db.backfill_raw_html_from_disk()
        assert result["skipped_missing"] >= 1
        assert result["backfilled"] == 0

    def test_backfill_skips_already_populated(self, tmp_path):
        db = self._make_db(tmp_path)
        html_file = tmp_path / "existing.html"
        html_file.write_text("<p>existing</p>", encoding="utf-8")
        with db._get_connection() as conn:
            conn.execute(
                """INSERT INTO reports
                   (ticker, report_date, source_type, source_url, raw_text, raw_html, parsed_at)
                   VALUES ('MARA', '2021-03-01', 'archive_html', ?, 'existing', '<p>existing</p>', datetime('now'))""",
                (str(html_file),),
            )
        result = db.backfill_raw_html_from_disk()
        assert result["backfilled"] == 0

    def test_backfill_only_targets_archive_html(self, tmp_path):
        """Non-archive_html source types are skipped even if raw_html is NULL."""
        db = self._make_db(tmp_path)
        with db._get_connection() as conn:
            conn.execute(
                """INSERT INTO reports
                   (ticker, report_date, source_type, source_url, raw_text, raw_html, parsed_at)
                   VALUES ('MARA', '2021-02-01', 'ir_press_release', 'https://example.com/pr', 'text', NULL, datetime('now'))"""
            )
        result = db.backfill_raw_html_from_disk()
        assert result["backfilled"] == 0
