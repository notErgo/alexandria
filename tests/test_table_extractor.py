"""Tests for alias-based HTML table extraction."""
import pytest
from bs4 import BeautifulSoup

from interpreters.table_interpreter import interpret_from_tables

BITF_TABLE_HTML = """<table>
<tr><th>Key Performance Indicators</th><th>April 2024</th><th>March 2024</th></tr>
<tr><td>Total BTC earned</td><td>269</td><td>286</td></tr>
<tr><td>Month End Operating EH/s</td><td>7.0</td><td>6.5</td></tr>
<tr><td>Operating Capacity (MW)</td><td>240</td><td>240</td></tr>
</table>"""

# RIOT table format: label col (0) + empty spacer col (1) + current month col (2)
RIOT_TABLE_HTML = """<table>
<tr><th>Metric</th><th></th><th>May 2023</th><th>April 2023</th><th>May 2022</th><th>Month/Month</th><th>Year/Year</th></tr>
<tr><td>Bitcoin Produced</td><td></td><td>676</td><td>639</td><td>466</td><td>6%</td><td>45%</td></tr>
<tr><td>Bitcoin Held</td><td></td><td>7190</td><td>7112</td><td>6536</td><td>1%</td><td>10%</td></tr>
<tr><td>Bitcoin Sold</td><td></td><td>600</td><td>600</td><td>250</td><td>0%</td><td>140%</td></tr>
</table>"""

CLSK_TABLE_HTML = """<table>
<tr><td colspan="2"><strong>Production Metrics</strong></td></tr>
<tr><td>Bitcoin produced</td><td>587</td></tr>
<tr><td>Operational hashrate</td><td>50.0 EH/s</td></tr>
<tr><td>Bitcoin holdings</td><td>13054</td></tr>
</table>"""


class TestExtractFromTables:
    def test_bitf_btc_earned_extracts_current_month(self):
        soup = BeautifulSoup(BITF_TABLE_HTML, "lxml")
        results = interpret_from_tables(soup)
        prod = [r for r in results if r.metric == "production_btc"]
        assert prod, "Should extract production_btc from 'Total BTC earned'"
        assert prod[0].value == pytest.approx(269.0, abs=0.1)
        assert prod[0].confidence >= 0.87  # 0.90 × 1.0 (exact alias match)

    def test_bitf_hashrate_extracts_eh(self):
        soup = BeautifulSoup(BITF_TABLE_HTML, "lxml")
        results = interpret_from_tables(soup)
        hr = [r for r in results if r.metric == "hashrate_eh"]
        assert hr, "Should extract hashrate_eh from 'Month End Operating EH/s'"
        assert hr[0].value == pytest.approx(7.0, abs=0.1)

    def test_clsk_section_table_three_metrics(self):
        soup = BeautifulSoup(CLSK_TABLE_HTML, "lxml")
        results = interpret_from_tables(soup)
        metrics_found = {r.metric for r in results}
        assert "production_btc" in metrics_found
        assert "hashrate_eh" in metrics_found
        assert "holdings_btc" in metrics_found

    def test_no_table_returns_empty(self):
        soup = BeautifulSoup("<p>No tables here.</p>", "lxml")
        assert interpret_from_tables(soup) == []

    def test_unknown_row_label_ignored(self):
        html = "<table><tr><td>Operating Capacity (MW)</td><td>240</td></tr></table>"
        soup = BeautifulSoup(html, "lxml")
        assert interpret_from_tables(soup) == []

    def test_squished_label_spaces_preserved(self):
        """Table cells with HTML sub-elements (e.g. <span>Bitcoin</span><span>Produced</span>)
        must have a space added between them so label matching works correctly."""
        html = """<table>
<tr><th>Metric</th><th>June 2024</th><th>May 2024</th></tr>
<tr><td><span>Bitcoin</span><span>Produced</span></td><td>255</td><td>215</td></tr>
</table>"""
        soup = BeautifulSoup(html, "lxml")
        results = interpret_from_tables(soup)
        prod = [r for r in results if r.metric == "production_btc"]
        assert prod, "Should extract production_btc even when label spans have no whitespace"
        assert prod[0].value == pytest.approx(255.0, abs=0.1)

    def test_riot_empty_leading_column(self):
        """RIOT format 3: empty col 0, label col 1, empty spacer col 2, data col 3."""
        html = """<table>
<tr><td></td><th>Metric</th><td></td><th>February 2024</th><th>January 2024</th></tr>
<tr><td></td><td>Bitcoin Produced</td><td></td><td>418</td><td>520</td></tr>
<tr><td></td><td>Bitcoin Held</td><td></td><td>8067</td><td>7648</td></tr>
</table>"""
        soup = BeautifulSoup(html, "lxml")
        results = interpret_from_tables(soup)
        prod = [r for r in results if r.metric == "production_btc"]
        assert prod, "Should extract production_btc from format-3 RIOT table"
        assert prod[0].value == pytest.approx(418.0, abs=0.1), (
            "Should extract 418 (col 3), skipping empty col 0 and col 2. Got %s" % prod[0].value
        )

    def test_riot_spacer_column_skipped(self):
        """RIOT tables have an empty spacer column at index 1.
        The extractor must skip it and read the first non-empty data column."""
        soup = BeautifulSoup(RIOT_TABLE_HTML, "lxml")
        results = interpret_from_tables(soup)
        prod = [r for r in results if r.metric == "production_btc"]
        assert prod, "Should extract production_btc from RIOT table despite spacer column"
        assert prod[0].value == pytest.approx(676.0, abs=0.1), (
            "Should extract 676 (col 2), not empty string (col 1). Got %s" % prod[0].value
        )

    def test_current_column_not_prior_month(self):
        """Leftmost data column (current month, index 1) wins over prior month."""
        soup = BeautifulSoup(BITF_TABLE_HTML, "lxml")
        results = interpret_from_tables(soup)
        prod = [r for r in results if r.metric == "production_btc"]
        assert prod, "Should extract production_btc"
        assert prod[0].value == pytest.approx(269.0, abs=0.1), (
            f"Should pick 269 (current, col 1), not 286 (prior, col 2). Got {prod[0].value}"
        )
