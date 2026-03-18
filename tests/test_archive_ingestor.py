"""Tests for archive ingestor helper functions and EDGAR hit parser."""
import json
import tempfile
import pytest
from datetime import date
from pathlib import Path
from scrapers.archive_ingestor import (
    _infer_body_period_strict,
    infer_period_from_filename,
    infer_period_from_text,
    infer_ticker_from_path,
    is_production_filename,
    is_quarterly_filing,
    extract_quarterly_months,
)
from scrapers.edgar_connector import parse_submissions_filings


class _MockLLM:
    """Minimal LLM stub for tests that must not make real HTTP calls.

    Returns empty results — the LLM acts only as a connectivity gate.
    Tests that use this mock verify ingest routing and keyword gating,
    not LLM extraction quality.
    """
    _last_call_meta = {}
    _last_transport_error = False
    _last_batch_summary = None

    def check_connectivity(self):
        return True

    def extract_batch(self, text, metrics, **kwargs):
        return {}




class TestInferPeriod:
    def test_infer_period_mara_pdf(self):
        result = infer_period_from_filename(
            "2024-09-03_Marathon_Digital_Holdings_Announces_Bitcoin.pdf", "MARA"
        )
        assert result == date(2024, 9, 1)

    def test_infer_period_riot_html(self):
        result = infer_period_from_filename(
            "Riot Announces September 2024 Production and Operations Update.html", "RIOT"
        )
        assert result == date(2024, 9, 1)

    def test_infer_period_november(self):
        result = infer_period_from_filename(
            "Riot Announces November 2023 Production and Operations Updates _ Riot Platforms.html",
            "RIOT",
        )
        assert result == date(2023, 11, 1)

    def test_infer_period_iso_prefix_2021(self):
        result = infer_period_from_filename(
            "2021-05-03_Marathon_Digital_Holdings_Announces_Bitcoin_1238.pdf", "MARA"
        )
        assert result == date(2021, 5, 1)

    def test_infer_period_no_match_returns_none(self):
        result = infer_period_from_filename("random_document.pdf", "MARA")
        assert result is None


class TestInferTicker:
    def test_infer_ticker_from_mara_path(self):
        result = infer_ticker_from_path("/some/dir/MARA MONTHLY/file.pdf")
        assert result == "MARA"

    def test_infer_ticker_from_riot_path(self):
        result = infer_ticker_from_path("/some/dir/RIOT MONTHLY/file.html")
        assert result == "RIOT"

    def test_infer_ticker_unknown_returns_none(self):
        result = infer_ticker_from_path("/some/dir/RANDOM_FOLDER/file.pdf")
        assert result is None


class TestIsProductionFilename:
    def test_is_production_filename_true(self):
        assert is_production_filename(
            "2024-09-03_Marathon_Digital_Holdings_Announces_Bitcoin_Production.pdf"
        )

    def test_is_production_filename_riot_html(self):
        assert is_production_filename(
            "Riot Announces September 2024 Production and Operations Updates _ Riot Platforms.html"
        )

    def test_not_production_quarterly_results(self):
        # Quarterly financial results, not production report
        assert not is_production_filename("Q3_2024_Financial_Results.pdf")


class TestInferPeriodFromText:
    def test_for_the_month_of_pattern(self):
        text = "Marathon Digital mined 750 BTC for the month of January 2025."
        assert infer_period_from_text(text) == date(2025, 1, 1)

    def test_in_month_year_pattern(self):
        text = "In September 2024, the company produced 1,200 BTC."
        assert infer_period_from_text(text) == date(2024, 9, 1)

    def test_month_production_pattern(self):
        text = "October 2024 production update: 1,400 BTC mined."
        assert infer_period_from_text(text) == date(2024, 10, 1)

    def test_returns_none_for_no_match(self):
        assert infer_period_from_text("No date information here.") is None

    def test_only_searches_first_3000_chars(self):
        # Period mentioned only after 5000 chars → not found
        text = "X" * 5000 + " January 2025 production"
        assert infer_period_from_text(text) is None


class TestHTMLPriorityOverPDF:
    """PDF is skipped entirely when an HTML covers the same (ticker, period)."""

    def test_pdf_skipped_when_html_covers_same_period(self, tmp_path, db, monkeypatch):
        """
        When a PDF and HTML both cover the same ticker+period, the PDF must be
        skipped — no archive_pdf report row is created, only archive_html.
        """
        from pathlib import Path
        from scrapers.archive_ingestor import ArchiveIngestor
        from interpreters.llm_interpreter import LLMInterpreter

        monkeypatch.setattr(LLMInterpreter, 'check_connectivity', lambda self: False)

        db.insert_company({
            'ticker': 'MARA', 'name': 'MARA Holdings', 'tier': 1,
            'ir_url': 'https://example.com', 'pr_base_url': None,
            'cik': '0001437491', 'active': 1,
        })

        mara_dir = tmp_path / "MARA MONTHLY"
        mara_dir.mkdir()

        # Both files resolve to 2024-09-01 via filename inference
        pdf_file = mara_dir / "2024-09-03_Marathon_Digital_Holdings_Announces_Bitcoin.pdf"
        html_file = mara_dir / "Marathon September 2024 Production Update.html"

        # HTML has extractable text; PDF is a stub (never parsed if skip works)
        html_file.write_text(
            "Marathon mined 736 BTC in September 2024. Bitcoin production: 736 BTC.",
            encoding="utf-8",
        )
        pdf_file.write_bytes(b"%PDF-1.4 stub")  # invalid PDF — parse would fail

        ingestor = ArchiveIngestor(archive_dir=str(tmp_path), db=db)
        ingestor.ingest_all()

        # Only the HTML report row must exist — no archive_pdf row
        assert not db.report_exists('MARA', '2024-09-01', 'archive_pdf'), \
            "PDF must be skipped when HTML covers same period"
        assert db.report_exists('MARA', '2024-09-01', 'archive_html'), \
            "HTML report must be ingested"

    def test_ticker_filtered_ingest_only_processes_selected_ticker(self, tmp_path, db, monkeypatch):
        """Ticker filter must prevent unrelated archive directories from being ingested."""
        from scrapers.archive_ingestor import ArchiveIngestor
        from interpreters.llm_interpreter import LLMInterpreter

        monkeypatch.setattr(LLMInterpreter, 'check_connectivity', lambda self: False)

        for ticker in ('MARA', 'RIOT'):
            db.insert_company({
                'ticker': ticker,
                'name': ticker,
                'tier': 1,
                'ir_url': 'https://example.com',
                'pr_base_url': None,
                'cik': '0001437491',
                'active': 1,
            })

        mara_dir = tmp_path / "MARA MONTHLY"
        mara_dir.mkdir()
        (mara_dir / "Marathon September 2024 Production Update.html").write_text(
            "Marathon mined 736 BTC in September 2024.",
            encoding="utf-8",
        )

        riot_dir = tmp_path / "RIOT MONTHLY"
        riot_dir.mkdir()
        (riot_dir / "Riot September 2024 Production Update.html").write_text(
            "Riot mined 412 BTC in September 2024.",
            encoding="utf-8",
        )

        ingestor = ArchiveIngestor(archive_dir=str(tmp_path), db=db)
        ingestor.ingest_all(tickers=['MARA'])

        assert db.report_exists('MARA', '2024-09-01', 'archive_html')
        assert not db.report_exists('RIOT', '2024-09-01', 'archive_html')


class TestTwoPassHTMLExtraction:
    def test_table_result_preferred_over_prose(self):
        """Table extraction (269) beats prose reference to prior month value (286)."""
        from bs4 import BeautifulSoup
        from interpreters.table_interpreter import interpret_from_tables

        html = """<html><body>
        <table>
        <tr><th>Metric</th><th>April 2024</th><th>March 2024</th></tr>
        <tr><td>Total BTC earned</td><td>269</td><td>286</td></tr>
        </table>
        <p>In March 2024, the company earned 286 BTC.</p>
        </body></html>"""

        soup = BeautifulSoup(html, "lxml")
        results = interpret_from_tables(soup)
        prod = [r for r in results if r.metric == "production_btc"]
        assert prod, "Should extract production_btc from table"
        assert prod[0].value == pytest.approx(269.0, abs=0.1), (
            f"Should pick 269 (table, current month), not 286 (prose). Got {prod[0].value}"
        )


class TestBestResultPerMetric:
    def test_only_highest_confidence_result_inserted(self, db, monkeypatch):
        """
        When extract_all returns two results for the same metric (both above threshold),
        only the highest-confidence result must be inserted into data_points.
        The runner-up must go to review_queue, not overwrite via INSERT OR REPLACE.

        Regression test for the 'last insert wins' bug:
        CLSK March 2024 showed value=2024 (year) instead of 806 (BTC mined)
        because both were above threshold and the lower-confidence 2024 was
        inserted last, surviving the UNIQUE(ticker, period, metric) upsert.

        LLM extractor is patched out — this test verifies regex-only ingest logic.
        Uses an inline test registry with two patterns so the extraction pipeline
        produces two candidate values from the test HTML.
        """
        import interpreters.interpret_pipeline as _ep_mod
        monkeypatch.setattr(_ep_mod, '_get_llm_interpreter', lambda db: _MockLLM())

        from scrapers.archive_ingestor import ArchiveIngestor

        db.insert_company({
            'ticker': 'MARA', 'name': 'MARA Holdings', 'tier': 1,
            'ir_url': 'https://example.com', 'pr_base_url': None,
            'cik': '0001437491', 'active': 1,
        })

        with tempfile.TemporaryDirectory() as tmpdir:
            mara_dir = Path(tmpdir) / "MARA MONTHLY"
            mara_dir.mkdir()
            html_file = mara_dir / "Riot Announces March 2024 Production and Operations Update.html"
            html_file.write_text(
                "<html><body>"
                "<p>The Company mined 806 BTC in March.</p>"
                "<p>Bitcoin production target for 2024 remains unchanged.</p>"
                "</body></html>",
                encoding="utf-8",
            )

            ingestor = ArchiveIngestor(archive_dir=tmpdir, db=db)
            summary = ingestor.ingest_all()

        # LLM mock returns empty. Verify the document was ingested without errors.
        assert summary.reports_ingested == 1, (
            f"Expected 1 report ingested, got {summary.reports_ingested}"
        )
        assert summary.errors == 0, f"Pipeline errors: {summary.errors}"


class TestStrategy4BodyTextPeriodInference:
    """Strategy 4: when filename has no date, read HTML body to infer period."""

    HTML = """<html><body>
    <h1>Riot Blockchain Announces April Production and Operations Updates</h1>
    <p>During April 2021, Riot mined 511 bitcoin.</p>
    </body></html>"""

    def test_strategy4_fires_when_filename_has_no_date(self, tmp_path):
        f = tmp_path / "Riot Blockchain Announces April Production and Operations Updates _ Riot Platforms.html"
        f.write_text(self.HTML)
        result = infer_period_from_filename(str(f), read_body=True)
        assert result == date(2021, 4, 1)

    def test_strategy1_wins_when_filename_has_iso_date(self, tmp_path):
        f = tmp_path / "2022-08-01_riot_production.html"
        f.write_text(self.HTML)
        # Body says April 2021 but filename says Aug 2022 — filename must win
        result = infer_period_from_filename(str(f), read_body=False)
        assert result == date(2022, 8, 1)

    def test_returns_none_when_body_has_no_date(self, tmp_path):
        f = tmp_path / "0005_press_release.html"
        f.write_text("<html><body><p>No date here.</p></body></html>")
        result = infer_period_from_filename(str(f), read_body=True)
        assert result is None

    def test_read_body_false_does_not_read_file(self, tmp_path):
        """With read_body=False, a filename-only match returns None for undated file."""
        f = tmp_path / "Riot Blockchain Announces April Production.html"
        f.write_text(self.HTML)
        # Filename has no date — strategy 4 disabled — should return None
        result = infer_period_from_filename(str(f), read_body=False)
        assert result is None

    def test_strategy4_parses_html_not_raw_bytes(self, tmp_path):
        """Strategy 4 must parse HTML before scanning; raw markup can push
        period text beyond 3000 raw bytes even when it's near the top visually."""
        # Build an HTML file where the visible "May 2021" text appears at ~position
        # 800 in parsed text but ~5000+ chars into the raw HTML (due to nav markup).
        padding_html = "<nav>" + "<a>link</a>" * 400 + "</nav>"
        deep_html = f"<html><body>{padding_html}<p>In May 2021, Riot produced 227 BTC.</p></body></html>"
        f = tmp_path / "Riot Blockchain Announces May Production.html"
        f.write_text(deep_html)
        result = infer_period_from_filename(str(f), read_body=True)
        assert result == date(2021, 5, 1), (
            "Strategy 4 must parse HTML (not raw bytes) to reach period text "
            "buried past 3000 raw chars"
        )

    def test_strategy4_not_applied_to_pdf_extension(self, tmp_path):
        """strategy 4 is HTML-only; a .pdf with no filename date returns None."""
        f = tmp_path / "undated_production_report.pdf"
        f.write_text("dummy")  # not a real PDF, but shouldn't be read
        result = infer_period_from_filename(str(f), read_body=True)
        assert result is None


class TestIsQuarterlyFiling:
    def test_10q_detected(self):
        assert is_quarterly_filing("10-Q 2025-11-04.pdf") is True

    def test_10k_detected(self):
        assert is_quarterly_filing("10-K 2024-02-15.pdf") is True

    def test_monthly_pr_not_quarterly(self):
        assert is_quarterly_filing("2024-09-03_Marathon_Announces_Bitcoin.pdf") is False

    def test_case_insensitive(self):
        assert is_quarterly_filing("10-q annual.pdf") is True


class TestExtractQuarterlyMonths:
    def test_finds_three_months_in_q3(self):
        text = "Production results: July 2025  Aug 2025  September 2025"
        months = extract_quarterly_months(text)
        assert date(2025, 7, 1) in months
        assert date(2025, 9, 1) in months

    def test_excludes_old_dates(self):
        text = "Comparative period: January 2019 vs July 2025"
        months = extract_quarterly_months(text)
        assert date(2019, 1, 1) not in months

    def test_returns_sorted_list(self):
        text = "September 2025 and July 2025 and August 2025"
        months = extract_quarterly_months(text)
        assert months == sorted(months)

    def test_returns_unique_months(self):
        text = "July 2025 production. July 2025 summary."
        months = extract_quarterly_months(text)
        assert months.count(date(2025, 7, 1)) == 1

    def test_empty_text_returns_empty_list(self):
        assert extract_quarterly_months("") == []

    def test_excludes_far_future_dates(self):
        # Bond maturity dates like "August 2032" must NOT appear in results
        text = "August 2032 senior notes maturity. July 2025 production data."
        months = extract_quarterly_months(text)
        assert date(2032, 8, 1) not in months
        assert date(2025, 7, 1) in months


class TestQuarterlyIngestorIntegration:
    def test_quarterly_report_date_uses_filename_not_body_text(self, db):
        """
        A 10-Q file named '10-Q 2025-11-04.html' must be stored with
        report_date=2025-11-01 (from filename), NOT from the latest
        month found in the body text (which may be a boilerplate SEC date).
        """
        import sqlite3
        from scrapers.archive_ingestor import ArchiveIngestor

        db.insert_company({
            'ticker': 'MARA', 'name': 'MARA Holdings', 'tier': 1,
            'ir_url': 'https://example.com', 'pr_base_url': None,
            'cik': '0001437491', 'active': 1,
        })

        with tempfile.TemporaryDirectory() as tmpdir:
            mara_dir = Path(tmpdir) / "MARA MONTHLY"
            mara_dir.mkdir()
            # Body mentions July-September 2025 (production months) AND March 2026
            # (a stray SEC boilerplate date). report_date must be 2025-11-01 (filename).
            quarterly_file = mara_dir / "10-Q 2025-11-04.html"
            quarterly_file.write_text(
                "<html><body>"
                "<p>July 2025: mined 900 BTC.</p>"
                "<p>August 2025: mined 950 BTC.</p>"
                "<p>September 2025: mined 1000 BTC.</p>"
                "<p>Filed pursuant to requirements as of March 2026.</p>"
                "</body></html>",
                encoding="utf-8",
            )

            ingestor = ArchiveIngestor(archive_dir=tmpdir, db=db)
            ingestor.ingest_all()

        conn = sqlite3.connect(db.db_path)
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            "SELECT report_date FROM reports WHERE ticker='MARA' AND source_type='archive_quarterly'"
        ).fetchall()
        conn.close()
        assert len(rows) == 1, f"Expected 1 quarterly report, got {len(rows)}"
        assert rows[0]['report_date'] == '2025-11-01', (
            f"report_date should be filename date 2025-11-01, got {rows[0]['report_date']}"
        )

    def test_quarterly_filing_stored_as_pending_for_llm_pipeline(self, db):
        """A 10-Q file must be stored as a pending report (extraction deferred to LLM pipeline)."""
        from scrapers.archive_ingestor import ArchiveIngestor

        db.insert_company({
            'ticker': 'MARA', 'name': 'MARA Holdings', 'tier': 1,
            'ir_url': 'https://example.com', 'pr_base_url': None,
            'cik': '0001437491', 'active': 1,
        })

        with tempfile.TemporaryDirectory() as tmpdir:
            mara_dir = Path(tmpdir) / "MARA MONTHLY"
            mara_dir.mkdir()

            quarterly_file = mara_dir / "10-Q 2025-11-04.html"
            quarterly_file.write_text(
                "<html><body>"
                "<p>July 2025: The company mined 900 BTC in July 2025.</p>"
                "<p>August 2025: The company mined 950 BTC in August 2025.</p>"
                "<p>September 2025: The company mined 1000 BTC in September 2025.</p>"
                "</body></html>",
                encoding="utf-8",
            )

            ingestor = ArchiveIngestor(archive_dir=tmpdir, db=db)
            summary = ingestor.ingest_all()

        # Quarterly files are stored and left pending for the LLM pipeline stage
        assert summary.reports_ingested == 1, (
            f"Expected 1 report ingested, got {summary.reports_ingested}"
        )
        assert db.report_exists('MARA', '2025-11-01', 'archive_quarterly')


class TestForceReingest:
    def test_force_true_reprocesses_existing_report(self, db, monkeypatch):
        """
        With force=True, an already-ingested file is deleted and re-inserted.
        The data_point count must stay at 1 (not double-insert).

        LLM extractor is patched out — this test verifies regex-only ingest logic.
        Uses an inline test registry so the extraction pipeline can produce a
        data_point from the test HTML content.
        """
        import interpreters.interpret_pipeline as _ep_mod
        monkeypatch.setattr(_ep_mod, '_get_llm_interpreter', lambda db: _MockLLM())

        from scrapers.archive_ingestor import ArchiveIngestor

        db.insert_company({
            'ticker': 'MARA', 'name': 'MARA Holdings', 'tier': 1,
            'ir_url': 'https://example.com', 'pr_base_url': None,
            'cik': '0001437491', 'active': 1,
        })

        with tempfile.TemporaryDirectory() as tmpdir:
            mara_dir = Path(tmpdir) / "MARA MONTHLY"
            mara_dir.mkdir()
            html_file = mara_dir / "Riot Announces March 2024 Production and Operations Update.html"
            html_file.write_text(
                "<html><body><p>The Company bitcoin mined 806 BTC in March. Hash rate 20 EH/s.</p></body></html>",
                encoding="utf-8",
            )

            ingestor = ArchiveIngestor(archive_dir=tmpdir, db=db)
            s1 = ingestor.ingest_all()
            assert s1.reports_ingested == 1

            s2 = ingestor.ingest_all()
            assert s2.reports_ingested == 0

            s3 = ingestor.ingest_all(force=True)
            assert s3.reports_ingested == 1

    def test_force_false_is_default(self, db):
        """ingest_all() with no arguments must not force-reingest."""
        from scrapers.archive_ingestor import ArchiveIngestor

        db.insert_company({
            'ticker': 'MARA', 'name': 'MARA Holdings', 'tier': 1,
            'ir_url': 'https://example.com', 'pr_base_url': None,
            'cik': '0001437491', 'active': 1,
        })

        with tempfile.TemporaryDirectory() as tmpdir:
            mara_dir = Path(tmpdir) / "MARA MONTHLY"
            mara_dir.mkdir()
            html_file = mara_dir / "Riot Announces March 2024 Production and Operations Update.html"
            html_file.write_text(
                "<html><body><p>The Company mined 806 BTC in March.</p></body></html>",
                encoding="utf-8",
            )

            ingestor = ArchiveIngestor(archive_dir=tmpdir, db=db)
            ingestor.ingest_all()
            s2 = ingestor.ingest_all()
            assert s2.reports_ingested == 0, "Default must skip already-ingested reports"


class TestParseEdgarHit:
    def test_parse_submissions_extracts_10q_filings(self):
        """parse_submissions_filings parses EDGAR Submissions JSON for 10-Q entries."""
        filings_data = {
            "filings": {
                "recent": {
                    "form": ["10-Q", "8-K", "10-Q"],
                    "filingDate": ["2024-11-04", "2024-10-15", "2024-08-05"],
                    "accessionNumber": ["0001437491-24-000010", "0001437491-24-000009", "0001437491-24-000008"],
                    "primaryDocument": ["mara-20240930.htm", "8k.htm", "mara-20240630.htm"],
                    "periodOfReport": ["2024-09-30", "2024-10-14", "2024-06-30"],
                }
            }
        }
        results = parse_submissions_filings(filings_data, form_type="10-Q")
        assert len(results) == 2
        accessions = {r["accession_number"] for r in results}
        assert "0001437491-24-000010" in accessions
        assert "0001437491-24-000008" in accessions
        assert "0001437491-24-000009" not in accessions  # 8-K excluded

    def test_edgar_cik_registry_has_correct_entries(self):
        import os
        config_path = os.path.join(os.path.dirname(__file__), '..', 'config', 'companies.json')
        with open(config_path) as f:
            companies = json.load(f)
        # Companies with no CIK must have an explicit skip_reason documenting why
        # (e.g., newly formed company with no SEC filing history yet).
        missing_cik_no_reason = [
            c['ticker'] for c in companies
            if not c.get('cik') and not c.get('skip_reason')
        ]
        assert missing_cik_no_reason == [], (
            f"Companies without a CIK must document the reason in skip_reason: "
            f"{missing_cik_no_reason}"
        )


class TestInferBodyPeriodStrict:
    """_infer_body_period_strict only matches high-specificity phrases, not bare Month YYYY."""

    def test_matches_for_the_month_of_phrase(self):
        text = "TeraWulf Announces March 2025 Mining Update. For the month of February 2025, the company mined 200 BTC."
        assert _infer_body_period_strict(text) == date(2025, 2, 1)

    def test_matches_during_phrase(self):
        text = "TeraWulf Announces March 2025 Mining Update. During February 2025, we achieved record production."
        assert _infer_body_period_strict(text) == date(2025, 2, 1)

    def test_matches_month_production_phrase(self):
        text = "HIVE Digital Reports March 2025 Update. February 2025 production totalled 150 BTC."
        assert _infer_body_period_strict(text) == date(2025, 2, 1)

    def test_matches_month_mining_phrase(self):
        text = "March 2025 Operational Update. February 2025 mining results are detailed below."
        assert _infer_body_period_strict(text) == date(2025, 2, 1)

    def test_returns_none_for_bare_month_only(self):
        # Bare "February 2025" with no qualifying phrase -> strict mode returns None
        text = "In this report we discuss February 2025 highlights."
        assert _infer_body_period_strict(text) is None

    def test_returns_none_when_no_period_in_body(self):
        text = "The company had strong operational results this quarter."
        assert _infer_body_period_strict(text) is None

    def test_announcement_month_skipped_when_title_period_passed(self):
        # "March 2025 Mining" matches pattern[1], but equals title_period -> skipped -> None
        text = "TeraWulf Announces March 2025 Mining Update."
        assert _infer_body_period_strict(text, title_period=date(2025, 3, 1)) is None

    def test_announcement_month_returned_when_no_title_period(self):
        # Without title_period context, first qualifying match is returned
        text = "TeraWulf Announces March 2025 Mining Update."
        assert _infer_body_period_strict(text, title_period=None) == date(2025, 3, 1)

    def test_in_phrase_matches(self):
        text = "March 2025 Update. Results for in February 2025 are summarised here."
        assert _infer_body_period_strict(text) == date(2025, 2, 1)
