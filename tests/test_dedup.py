"""Tests for URL canonicalization, simhash near-dedup, and DB dedup integration.

Written before implementation -- tests fail until scrapers/dedup.py and v16 migration exist.
"""
import sqlite3
import pytest


class TestCanonicalUrl:

    def test_strips_utm_params(self):
        from scrapers.dedup import canonical_url
        result = canonical_url("https://example.com/pr?utm_source=twitter&utm_medium=social")
        assert result == "https://example.com/pr"

    def test_strips_multiple_tracking_params(self):
        from scrapers.dedup import canonical_url
        url = "https://example.com/pr?utm_source=x&fbclid=abc&gclid=def&ref=home&source=feed"
        result = canonical_url(url)
        assert result == "https://example.com/pr"

    def test_preserves_non_tracking_params(self):
        from scrapers.dedup import canonical_url
        result = canonical_url("https://example.com/pr?page=2&id=123")
        assert "page=2" in result
        assert "id=123" in result

    def test_preserves_trailing_slash(self):
        # Trailing slashes are preserved because some servers (e.g. HIVE) return
        # different content for /slug vs /slug/ — stripping would cause wrong fetches.
        from scrapers.dedup import canonical_url
        result = canonical_url("https://example.com/pr/")
        assert result == "https://example.com/pr/"

    def test_lowercases_scheme_and_host(self):
        from scrapers.dedup import canonical_url
        result = canonical_url("HTTPS://IR.MARA.COM/news")
        assert result == "https://ir.mara.com/news"

    def test_empty_string_returns_empty(self):
        from scrapers.dedup import canonical_url
        assert canonical_url("") == ""


class TestSimhash:

    def test_same_text_same_hash(self):
        from scrapers.dedup import simhash_text
        assert simhash_text("hello world") == simhash_text("hello world")

    def test_different_text_different_hash(self):
        from scrapers.dedup import simhash_text
        h1 = simhash_text("The quick brown fox jumps over the lazy dog")
        h2 = simhash_text("Bitcoin miner produced 700 BTC in March 2024 operations")
        assert h1 != h2

    def test_near_duplicate_detected(self):
        from scrapers.dedup import simhash_text, hamming_distance
        base = (
            "MARA Holdings mined 700 BTC during September 2024 representing its best production "
            "month of the year. The company deployed 8 exahashes per second of hashrate across "
            "its facilities in Nebraska and Ohio. Total bitcoin holdings reached 25000 BTC as of "
            "September 30 2024. The company sold zero BTC consistent with its full hodl strategy. "
            "Revenue from bitcoin mining operations totaled approximately 45 million dollars."
        )
        near = base.replace("700 BTC", "701 BTC")
        h1 = simhash_text(base)
        h2 = simhash_text(near)
        assert hamming_distance(h1, h2) <= 3

    def test_distinct_docs_not_flagged(self):
        from scrapers.dedup import simhash_text, hamming_distance
        h1 = simhash_text("MARA mined 700 BTC in September production update")
        h2 = simhash_text("Federal Reserve raises interest rates amid inflation concerns in Q3")
        assert hamming_distance(h1, h2) >= 10

    def test_empty_text_stable(self):
        from scrapers.dedup import simhash_text
        result = simhash_text("")
        assert isinstance(result, int)

    def test_is_near_duplicate_true(self):
        from scrapers.dedup import simhash_text, is_near_duplicate
        base = (
            "MARA Holdings mined 700 BTC during September 2024 representing its best production "
            "month of the year. The company deployed 8 exahashes per second of hashrate across "
            "its facilities in Nebraska and Ohio. Total bitcoin holdings reached 25000 BTC as of "
            "September 30 2024. The company sold zero BTC consistent with its full hodl strategy. "
            "Revenue from bitcoin mining operations totaled approximately 45 million dollars."
        )
        near = base.replace("700 BTC", "701 BTC")
        h1 = simhash_text(base)
        h2 = simhash_text(near)
        assert is_near_duplicate(h1, h2, threshold=3)

    def test_is_near_duplicate_false(self):
        from scrapers.dedup import simhash_text, is_near_duplicate
        h1 = simhash_text("MARA mined 700 BTC in September production update filing")
        h2 = simhash_text("Federal Reserve raises interest rates amid inflation concerns Q3")
        assert not is_near_duplicate(h1, h2, threshold=3)


class TestDeduplication:

    @pytest.fixture
    def db(self, tmp_path):
        """Fresh MinerDB instance for each test."""
        from infra.db import MinerDB
        instance = MinerDB(str(tmp_path / "test_dedup.db"))
        instance.insert_company({
            "ticker": "MARA",
            "name": "MARA Holdings, Inc.",
            "tier": 1,
            "ir_url": "https://ir.mara.com/news",
            "pr_base_url": "https://ir.mara.com",
            "cik": "0001437491",
            "active": 1,
        })
        return instance

    def test_insert_report_stores_simhash(self, db):
        from scrapers.dedup import simhash_text
        text = "MARA mined 700 BTC in September 2024 production update."
        sh = simhash_text(text)
        report = {
            "ticker": "MARA",
            "report_date": "2024-09-01",
            "published_date": None,
            "source_type": "ir_press_release",
            "source_url": "https://ir.mara.com/news/2024-09",
            "raw_text": text,
            "parsed_at": "2024-09-03T12:00:00",
            "content_simhash": sh,
        }
        report_id = db.insert_report(report)
        conn = sqlite3.connect(db.db_path)
        conn.row_factory = sqlite3.Row
        row = conn.execute(
            "SELECT content_simhash FROM reports WHERE id = ?", (report_id,)
        ).fetchone()
        conn.close()
        assert row is not None
        assert row["content_simhash"] == sh

    def test_find_near_duplicates_returns_match(self, db):
        from scrapers.dedup import simhash_text
        text = "MARA mined 700 BTC in September 2024 production update."
        sh = simhash_text(text)
        db.insert_report({
            "ticker": "MARA",
            "report_date": "2024-09-01",
            "published_date": None,
            "source_type": "ir_press_release",
            "source_url": "https://ir.mara.com/news/2024-09",
            "raw_text": text,
            "parsed_at": "2024-09-03T12:00:00",
            "content_simhash": sh,
        })
        dupes = db.find_near_duplicates(sh, "MARA", threshold=3)
        assert len(dupes) >= 1
        assert dupes[0]["ticker"] == "MARA"

    def test_find_near_duplicates_empty_when_none(self, db):
        from scrapers.dedup import simhash_text
        sh = simhash_text("MARA mined 700 BTC September 2024")
        result = db.find_near_duplicates(sh, "WULF", threshold=3)
        assert result == []

    def test_reports_table_has_fetch_provenance_columns(self, db):
        conn = sqlite3.connect(db.db_path)
        cols = {row[1] for row in conn.execute("PRAGMA table_info(reports)").fetchall()}
        conn.close()
        assert "fetch_strategy" in cols
        assert "render_mode" in cols
        assert "fetch_timing_ms" in cols
