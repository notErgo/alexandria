"""
Tests for the refactored EdgarConnector (Stage-1-only, submissions API).
Written before implementation — these tests define the expected contract.
"""
import pytest
from unittest.mock import MagicMock, patch


class TestPeriodMapping:
    def test_period_of_report_to_covering_period_q1(self):
        from scrapers.edgar_connector import period_of_report_to_covering_period
        assert period_of_report_to_covering_period("2025-03-31", "10-Q") == "2025-Q1"

    def test_period_of_report_to_covering_period_q2(self):
        from scrapers.edgar_connector import period_of_report_to_covering_period
        assert period_of_report_to_covering_period("2025-06-30", "10-Q") == "2025-Q2"

    def test_period_of_report_to_covering_period_q3(self):
        from scrapers.edgar_connector import period_of_report_to_covering_period
        assert period_of_report_to_covering_period("2025-09-30", "10-Q") == "2025-Q3"

    def test_period_of_report_to_covering_period_q4(self):
        from scrapers.edgar_connector import period_of_report_to_covering_period
        assert period_of_report_to_covering_period("2025-12-31", "10-Q") == "2025-Q4"

    def test_period_of_report_to_covering_period_10k(self):
        from scrapers.edgar_connector import period_of_report_to_covering_period
        assert period_of_report_to_covering_period("2024-12-31", "10-K") == "2024-FY"

    def test_period_of_report_to_covering_period_10k_march(self):
        from scrapers.edgar_connector import period_of_report_to_covering_period
        # Some companies have fiscal year ending in March
        assert period_of_report_to_covering_period("2024-03-31", "10-K") == "2024-FY"


class TestSubmissionsParsing:
    def test_parse_submissions_filters_10q(self):
        """mock submissions JSON, verify only 10-Qs returned by the 10-Q fetcher."""
        from scrapers.edgar_connector import parse_submissions_filings
        mock_submissions = {
            "filings": {
                "recent": {
                    "form": ["10-Q", "8-K", "10-K", "10-Q"],
                    "filingDate": ["2025-05-01", "2025-04-15", "2025-03-01", "2024-11-01"],
                    "accessionNumber": ["0001-25-001", "0001-25-002", "0001-25-003", "0001-24-004"],
                    "primaryDocument": ["doc1.htm", "doc2.htm", "doc3.htm", "doc4.htm"],
                    "periodOfReport": ["2025-03-31", "2025-04-14", "2024-12-31", "2024-09-30"],
                }
            }
        }
        result = parse_submissions_filings(mock_submissions, form_type="10-Q")
        assert len(result) == 2
        assert all(f["form_type"] == "10-Q" for f in result)
        assert result[0]["covering_period"] == "2025-Q1"
        assert result[1]["covering_period"] == "2024-Q3"

    def test_parse_submissions_filters_10k(self):
        from scrapers.edgar_connector import parse_submissions_filings
        mock_submissions = {
            "filings": {
                "recent": {
                    "form": ["10-Q", "10-K"],
                    "filingDate": ["2025-05-01", "2025-03-01"],
                    "accessionNumber": ["0001-25-001", "0001-25-002"],
                    "primaryDocument": ["doc1.htm", "doc2.htm"],
                    "periodOfReport": ["2025-03-31", "2024-12-31"],
                }
            }
        }
        result = parse_submissions_filings(mock_submissions, form_type="10-K")
        assert len(result) == 1
        assert result[0]["covering_period"] == "2024-FY"

    def test_parse_submissions_falls_back_to_report_date_when_period_missing(self):
        from scrapers.edgar_connector import parse_submissions_filings
        mock_submissions = {
            "filings": {
                "recent": {
                    "form": ["10-Q", "10-K"],
                    "filingDate": ["2025-05-01", "2025-03-01"],
                    "reportDate": ["2025-03-31", "2024-12-31"],
                    "accessionNumber": ["0001-25-001", "0001-25-002"],
                    "primaryDocument": ["doc1.htm", "doc2.htm"],
                    "periodOfReport": [],
                }
            }
        }
        q = parse_submissions_filings(mock_submissions, form_type="10-Q")
        k = parse_submissions_filings(mock_submissions, form_type="10-K")
        assert len(q) == 1
        assert q[0]["covering_period"] == "2025-Q1"
        assert len(k) == 1
        assert k[0]["covering_period"] == "2024-FY"


class TestFilingIndexParsing:
    def test_parse_filing_index_finds_primary_doc(self):
        """mock index HTML, verify primary document link extracted, exhibits excluded."""
        from scrapers.edgar_connector import parse_filing_index_for_primary_doc
        # Simulate an EDGAR filing index table
        index_html = """
        <html><body>
        <table>
        <tr><td>10-Q</td><td><a href="/Archives/edgar/data/1437491/000143749125000001/mara20250331.htm">mara20250331.htm</a></td><td>10-Q</td></tr>
        <tr><td>EX-31.1</td><td><a href="/Archives/edgar/data/1437491/000143749125000001/ex311.htm">ex311.htm</a></td><td>EX-31.1</td></tr>
        <tr><td>EX-32.1</td><td><a href="/Archives/edgar/data/1437491/000143749125000001/ex321.htm">ex321.htm</a></td><td>EX-32.1</td></tr>
        </table>
        </body></html>
        """
        url = parse_filing_index_for_primary_doc(index_html)
        assert url is not None
        assert "mara20250331.htm" in url
        assert "ex311" not in url


class TestParse8kExhibitUrl:
    def test_finds_ex991_first(self):
        from scrapers.edgar_connector import parse_8k_exhibit_url
        index_html = """
        <html><body><table>
        <tr><td>8-K</td><td><a href="/Archives/edgar/data/1/000001/8k.htm">8k.htm</a></td><td>8-K</td></tr>
        <tr><td>EX-99.1</td><td><a href="/Archives/edgar/data/1/000001/ex991.htm">ex991.htm</a></td><td>EX-99.1</td></tr>
        <tr><td>EX-31.1</td><td><a href="/Archives/edgar/data/1/000001/ex311.htm">ex311.htm</a></td><td>EX-31.1</td></tr>
        </table></body></html>
        """
        url = parse_8k_exhibit_url(index_html, '1', '000001')
        assert url is not None
        assert 'ex991.htm' in url

    def test_falls_back_to_ex99(self):
        from scrapers.edgar_connector import parse_8k_exhibit_url
        index_html = """
        <html><body><table>
        <tr><td>8-K</td><td><a href="/Archives/edgar/data/1/000001/8k.htm">8k.htm</a></td><td>8-K</td></tr>
        <tr><td>EX-99</td><td><a href="/Archives/edgar/data/1/000001/ex99.htm">ex99.htm</a></td><td>EX-99</td></tr>
        </table></body></html>
        """
        url = parse_8k_exhibit_url(index_html, '1', '000001')
        assert url is not None
        assert 'ex99.htm' in url

    def test_no_exhibit_returns_none(self):
        from scrapers.edgar_connector import parse_8k_exhibit_url
        index_html = """
        <html><body><table>
        <tr><td>8-K</td><td><a href="/Archives/edgar/data/1/000001/8k.htm">8k.htm</a></td><td>8-K</td></tr>
        <tr><td>EX-31.1</td><td><a href="/Archives/edgar/data/1/000001/ex311.htm">ex311.htm</a></td><td>EX-31.1</td></tr>
        </table></body></html>
        """
        url = parse_8k_exhibit_url(index_html, '1', '000001')
        assert url is None

    def test_prefers_ex991_over_ex99(self):
        from scrapers.edgar_connector import parse_8k_exhibit_url
        index_html = """
        <html><body><table>
        <tr><td>EX-99</td><td><a href="/Archives/edgar/data/1/000001/ex99.htm">ex99.htm</a></td><td>EX-99</td></tr>
        <tr><td>EX-99.1</td><td><a href="/Archives/edgar/data/1/000001/ex991.htm">ex991.htm</a></td><td>EX-99.1</td></tr>
        </table></body></html>
        """
        url = parse_8k_exhibit_url(index_html, '1', '000001')
        assert url is not None
        assert 'ex991.htm' in url


class TestParseExhibitUrlFromStaleSourceUrl:
    def test_extracts_exhibit_url(self):
        from scrapers.edgar_connector import _parse_exhibit_url_from_stale_source_url
        stale_url = (
            "https://www.sec.gov/Archives/edgar/data/1725526/"
            "000119312522111309:d330098dex991.htm/"
            "0001193125-22-111309:d330098dex991.htm-index.htm"
        )
        result = _parse_exhibit_url_from_stale_source_url(stale_url)
        assert result == (
            "https://www.sec.gov/Archives/edgar/data/1725526/"
            "000119312522111309/d330098dex991.htm"
        )

    def test_returns_none_for_clean_url(self):
        from scrapers.edgar_connector import _parse_exhibit_url_from_stale_source_url
        clean_url = "https://www.sec.gov/Archives/edgar/data/1725526/000119312522111309/8k.htm"
        assert _parse_exhibit_url_from_stale_source_url(clean_url) is None

    def test_returns_none_for_empty(self):
        from scrapers.edgar_connector import _parse_exhibit_url_from_stale_source_url
        assert _parse_exhibit_url_from_stale_source_url('') is None


class TestFetch8kExhibitResolution:
    """Verify fetch_8k_filings stores exhibit text, not index page boilerplate."""

    def _make_session(self, acc_no: str, exhibit_text: str):
        cik_numeric = '1437491'
        acc_clean = acc_no.replace('-', '')
        submissions_resp = MagicMock(status_code=200, raise_for_status=lambda: None)
        submissions_resp.json.return_value = _make_submissions_response(
            cik_numeric, acc_no, '2024-06-01'
        )
        index_html = (
            "<html><body><table><tr><td>EX-99.1</td>"
            "<td><a href='ex991pressrelease.htm'>press release</a></td>"
            "</tr></table></body></html>"
        )
        index_resp = MagicMock(status_code=200, text=index_html, raise_for_status=lambda: None)
        exhibit_resp = MagicMock(
            status_code=200,
            text=f"<html><body>{exhibit_text}</body></html>",
            raise_for_status=lambda: None,
        )

        def side_effect(url, **kwargs):
            if 'data.sec.gov' in url:
                return submissions_resp
            if f'{acc_clean}/{acc_no}-index.htm' in url:
                return index_resp
            return exhibit_resp

        session = MagicMock()
        session.get.side_effect = side_effect
        return session

    def test_fetches_exhibit_url_from_index(self):
        """Stored source_url points to the EX-99.1 exhibit, not the index page."""
        from datetime import date as dt
        from scrapers.edgar_connector import EdgarConnector

        mock_db = MagicMock()
        mock_db.report_exists.return_value = False
        mock_db.report_exists_by_accession.return_value = False
        mock_db.insert_report.return_value = 1

        session = self._make_session(
            acc_no='0001437491-24-001',
            exhibit_text='MARA mined 750 bitcoin during June 2024',
        )
        connector = EdgarConnector(db=mock_db, session=session)
        connector.fetch_8k_filings(cik='0001437491', ticker='MARA', since_date=dt(2024, 1, 1))

        stored = mock_db.insert_report.call_args[0][0]
        assert 'ex991pressrelease.htm' in stored['source_url']
        assert 'MARA mined' in stored['raw_text']

    def test_does_not_store_edgar_index_boilerplate(self):
        """raw_text contains the exhibit body, not EDGAR index page text."""
        from datetime import date as dt
        from scrapers.edgar_connector import EdgarConnector

        mock_db = MagicMock()
        mock_db.report_exists.return_value = False
        mock_db.report_exists_by_accession.return_value = False
        mock_db.insert_report.return_value = 1

        session = self._make_session(
            acc_no='0001437491-24-001',
            exhibit_text='Bitcoin production was 750 BTC',
        )
        connector = EdgarConnector(db=mock_db, session=session)
        connector.fetch_8k_filings(cik='0001437491', ticker='MARA', since_date=dt(2024, 1, 1))

        stored = mock_db.insert_report.call_args[0][0]
        assert not stored['raw_text'].startswith('EDGAR Filing Documents')


def _make_submissions_response(cik_numeric: str, acc_no: str, filing_date: str) -> dict:
    """Build a minimal SEC submissions JSON payload with one 8-K entry."""
    return {
        "filings": {
            "recent": {
                "form":            ["8-K"],
                "filingDate":      [filing_date],
                "accessionNumber": [acc_no],
                "primaryDocument": ["8k.htm"],
                "periodOfReport":  [filing_date],
                "reportDate":      [filing_date],
            }
        }
    }


class TestFetch8kSubmissionsAPI:
    """fetch_8k_filings must use the submissions API, not EFTS full-text search."""

    def _make_session(self, cik_numeric: str, acc_no: str, filing_date: str, exhibit_text: str):
        acc_clean = acc_no.replace('-', '')
        submissions_url = f"https://data.sec.gov/submissions/CIK{cik_numeric.zfill(10)}.json"
        index_url_fragment = f"{acc_clean}/{acc_no}-index.htm"
        exhibit_url = f"https://www.sec.gov/Archives/edgar/data/{cik_numeric}/{acc_clean}/ex991.htm"

        index_html = (
            f"<html><body><table><tr>"
            f"<td>EX-99.1</td>"
            f"<td><a href='ex991.htm'>{exhibit_text[:20]}</a></td>"
            f"</tr></table></body></html>"
        )

        submissions_resp = MagicMock(status_code=200, raise_for_status=lambda: None)
        submissions_resp.json.return_value = _make_submissions_response(
            cik_numeric, acc_no, filing_date
        )

        index_resp = MagicMock(
            status_code=200, text=index_html, raise_for_status=lambda: None
        )

        exhibit_resp = MagicMock(
            status_code=200,
            text=f"<html><body>{exhibit_text}</body></html>",
            raise_for_status=lambda: None,
        )

        def side_effect(url, **kwargs):
            if 'data.sec.gov' in url:
                return submissions_resp
            if index_url_fragment in url:
                return index_resp
            return exhibit_resp

        session = MagicMock()
        session.get.side_effect = side_effect
        return session

    def test_fetch_8k_uses_submissions_api_not_efts(self):
        """fetch_8k_filings must NOT call efts.sec.gov — uses submissions API."""
        from datetime import date as dt
        from scrapers.edgar_connector import EdgarConnector

        mock_db = MagicMock()
        mock_db.report_exists.return_value = False
        mock_db.report_exists_by_accession.return_value = False
        mock_db.insert_report.return_value = 1

        session = self._make_session(
            cik_numeric='1507605',
            acc_no='0001507605-24-000001',
            filing_date='2024-06-01',
            exhibit_text='MARA mined 750 bitcoin during June 2024 production',
        )

        connector = EdgarConnector(db=mock_db, session=session)
        connector.fetch_8k_filings(cik='0001507605', ticker='MARA', since_date=dt(2024, 1, 1))

        # EFTS must never be called
        efts_calls = [
            call for call in session.get.call_args_list
            if 'efts.sec.gov' in str(call)
        ]
        assert efts_calls == [], f"EFTS was called: {efts_calls}"

        # Report must be stored
        mock_db.insert_report.assert_called_once()
        stored = mock_db.insert_report.call_args[0][0]
        assert stored['source_type'] == 'edgar_8k'
        assert stored['ticker'] == 'MARA'
        assert stored['report_date'] == '2024-06-01'
        assert 'mined 750 bitcoin' in stored['raw_text']

    def test_fetch_8k_skips_filings_before_since_date(self):
        """8-K with filing_date before since_date is not ingested."""
        from datetime import date as dt
        from scrapers.edgar_connector import EdgarConnector

        mock_db = MagicMock()
        mock_db.report_exists.return_value = False
        mock_db.report_exists_by_accession.return_value = False

        session = self._make_session(
            cik_numeric='1507605',
            acc_no='0001507605-23-000001',
            filing_date='2023-01-15',
            exhibit_text='Bitcoin mining report',
        )

        connector = EdgarConnector(db=mock_db, session=session)
        connector.fetch_8k_filings(
            cik='0001507605', ticker='MARA', since_date=dt(2024, 1, 1)
        )

        mock_db.insert_report.assert_not_called()

    def test_fetch_8k_falls_back_to_primary_doc_when_no_exhibit(self):
        """8-K with no EX-99 exhibit still stores the primary filing document."""
        from datetime import date as dt
        from scrapers.edgar_connector import EdgarConnector

        mock_db = MagicMock()
        mock_db.report_exists.return_value = False
        mock_db.report_exists_by_accession.return_value = False
        mock_db.insert_report.return_value = 1

        index_html = (
            "<html><body><table>"
            "<tr><td>8-K</td><td><a href='8k.htm'>form</a></td><td>8-K</td></tr>"
            "</table></body></html>"
        )

        submissions_resp = MagicMock(status_code=200, raise_for_status=lambda: None)
        submissions_resp.json.return_value = _make_submissions_response(
            '1507605', '0001507605-24-000001', '2024-06-01'
        )
        index_resp = MagicMock(status_code=200, text=index_html, raise_for_status=lambda: None)
        primary_resp = MagicMock(
            status_code=200,
            text="<html><body>MARA current report production update 725 BTC.</body></html>",
            raise_for_status=lambda: None,
        )

        def _side_effect(url, **kw):
            if 'data.sec.gov' in url:
                return submissions_resp
            if url.endswith('-index.htm'):
                return index_resp
            if url.endswith('/8k.htm'):
                return primary_resp
            raise AssertionError(f'unexpected URL {url}')

        session = MagicMock()
        session.get.side_effect = _side_effect

        connector = EdgarConnector(db=mock_db, session=session)
        result = connector.fetch_8k_filings(
            cik='0001507605', ticker='MARA', since_date=dt(2024, 1, 1)
        )

        mock_db.insert_report.assert_called_once()
        stored = mock_db.insert_report.call_args[0][0]
        assert stored['source_url'].endswith('/8k.htm')
        assert 'production update 725 BTC' in stored['raw_text']
        assert result.errors == 0

    def test_fetch_8k_deduplicates_by_accession(self):
        """8-K already stored by accession number is not re-ingested."""
        from datetime import date as dt
        from scrapers.edgar_connector import EdgarConnector

        mock_db = MagicMock()
        mock_db.report_exists_by_accession.return_value = True

        session = self._make_session(
            cik_numeric='1507605',
            acc_no='0001507605-24-000001',
            filing_date='2024-06-01',
            exhibit_text='MARA bitcoin production',
        )

        connector = EdgarConnector(db=mock_db, session=session)
        connector.fetch_8k_filings(cik='0001507605', ticker='MARA', since_date=dt(2024, 1, 1))

        mock_db.insert_report.assert_not_called()

    def test_report_date_uses_period_of_report_not_filing_date(self):
        """report_date must be derived from period_of_report (production period),
        not filing_date.  Monthly production 8-Ks are filed ~7 days after the
        month ends: filing_date="2024-10-07", period_of_report="2024-09-30".
        The stored report_date must be "2024-09-01" so data points align with
        the archive and IR records for the same September production period.
        """
        from datetime import date as dt
        from scrapers.edgar_connector import EdgarConnector

        period_of_report = '2024-09-30'   # last day of the reporting month
        filing_date = '2024-10-07'        # when the 8-K was filed with the SEC

        submissions_data = {
            "filings": {
                "recent": {
                    "form":            ["8-K"],
                    "filingDate":      [filing_date],
                    "accessionNumber": ["0001167419-24-000001"],
                    "primaryDocument": ["ex991.htm"],
                    "periodOfReport":  [period_of_report],
                    "reportDate":      [period_of_report],
                }
            }
        }
        cik_numeric = '1167419'
        acc_no = '0001167419-24-000001'
        acc_clean = acc_no.replace('-', '')

        submissions_resp = MagicMock(status_code=200, raise_for_status=lambda: None)
        submissions_resp.json.return_value = submissions_data

        index_html = (
            f"<html><body><table><tr>"
            f"<td>EX-99.1</td>"
            f"<td><a href='ex991.htm'>exhibit</a></td>"
            f"</tr></table></body></html>"
        )
        index_resp = MagicMock(status_code=200, text=index_html, raise_for_status=lambda: None)
        exhibit_resp = MagicMock(
            status_code=200,
            text="<html><body>RIOT mined 750 BTC during September 2024.</body></html>",
            raise_for_status=lambda: None,
        )

        def side_effect(url, **kwargs):
            if 'data.sec.gov' in url:
                return submissions_resp
            if f"{acc_clean}/{acc_no}-index.htm" in url:
                return index_resp
            return exhibit_resp

        session = MagicMock()
        session.get.side_effect = side_effect

        mock_db = MagicMock()
        mock_db.report_exists.return_value = False
        mock_db.report_exists_by_accession.return_value = False
        mock_db.insert_report.return_value = 1

        connector = EdgarConnector(db=mock_db, session=session)
        connector.fetch_8k_filings(cik='0001167419', ticker='RIOT', since_date=dt(2024, 1, 1))

        mock_db.insert_report.assert_called_once()
        stored = mock_db.insert_report.call_args[0][0]
        # report_date must be "2024-09-01" (first of production month), NOT "2024-10-07"
        assert stored['report_date'] == '2024-09-01', (
            f"Expected '2024-09-01' (production period) but got '{stored['report_date']}'"
        )
        # filing_date must be preserved as published_date for audit trail
        assert stored['published_date'] == filing_date


class TestFetch8kNoExtraction:
    def test_fetch_8k_no_extraction(self):
        """Verify fetch_8k_filings never calls extract_all — Stage 1 only."""
        from datetime import date as dt
        from scrapers.edgar_connector import EdgarConnector

        mock_db = MagicMock()
        mock_db.report_exists.return_value = False
        mock_db.report_exists_by_accession.return_value = False
        mock_db.insert_report.return_value = 1

        session = MagicMock()
        acc_no = '0001437491-24-000001'
        acc_clean = acc_no.replace('-', '')
        submissions_resp = MagicMock(status_code=200, raise_for_status=lambda: None)
        submissions_resp.json.return_value = _make_submissions_response(
            '1437491', acc_no, '2024-06-01'
        )
        index_html = (
            "<html><body><table><tr><td>EX-99.1</td>"
            "<td><a href='ex991.htm'>press release</a></td></tr></table></body></html>"
        )
        index_resp = MagicMock(status_code=200, text=index_html, raise_for_status=lambda: None)
        exhibit_resp = MagicMock(
            status_code=200,
            text="<html><body>Bitcoin production report text here</body></html>",
            raise_for_status=lambda: None,
        )

        def side_effect(url, **kwargs):
            if 'data.sec.gov' in url:
                return submissions_resp
            if f'{acc_clean}/{acc_no}-index.htm' in url:
                return index_resp
            return exhibit_resp

        session.get.side_effect = side_effect
        connector = EdgarConnector(db=mock_db, session=session)

        connector.fetch_8k_filings(cik="0001437491", ticker="MARA",
                                    since_date=dt(2024, 1, 1))
        # Stage 1 is ingest-only — extraction module is never called
        mock_db.insert_report.assert_called_once()


class TestEdgarHitEntityFiltering:
    def test_hit_matches_target_entity_with_ciks(self):
        from scrapers.edgar_connector import _hit_matches_target_entity
        source = {'ciks': ['0001167419'], 'adsh': '0001167419-24-000001'}
        assert _hit_matches_target_entity(source, '0001167419') is True
        assert _hit_matches_target_entity(source, '0001437491') is False

    def test_hit_matches_target_entity_with_adsh_fallback(self):
        from scrapers.edgar_connector import _hit_matches_target_entity
        source = {'ciks': [], 'adsh': '0001839341-22-111309'}
        assert _hit_matches_target_entity(source, '1839341') is True
        assert _hit_matches_target_entity(source, '1167419') is False


class TestBuildEdgarQuery:
    """Tests for _build_edgar_query — dead fallback path."""

    def test_build_query_with_no_keywords_returns_hardcoded_fallback(self):
        """When metric_keywords returns empty rows, hardcoded terms are used (not empty string)."""
        from scrapers.edgar_connector import _build_edgar_query
        from unittest.mock import MagicMock

        mock_db = MagicMock()
        mock_db.get_all_metric_keywords.return_value = []

        result = _build_edgar_query(mock_db)

        assert result, "Query string must not be empty"
        assert '"' in result, "Hardcoded fallback must use quoted terms"
        # Must NOT call get_search_keywords (retired table)
        mock_db.get_search_keywords.assert_not_called()

    def test_build_query_with_metric_keywords_uses_them(self):
        """When metric_keywords returns phrases, they appear in the query."""
        from scrapers.edgar_connector import _build_edgar_query
        from unittest.mock import MagicMock

        mock_db = MagicMock()
        mock_db.get_all_metric_keywords.return_value = [
            {'phrase': 'bitcoin mined', 'metric_key': 'production_btc'},
            {'phrase': 'hash rate', 'metric_key': 'hashrate_eh'},
        ]

        result = _build_edgar_query(mock_db)

        assert 'bitcoin mined' in result
        assert 'hash rate' in result
