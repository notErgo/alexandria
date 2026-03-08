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
