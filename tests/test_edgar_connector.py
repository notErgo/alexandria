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
    """Verify fetch_8k_filings fetches the exhibit, not the index page."""

    def _make_session(self, search_id, exhibit_text):
        session = MagicMock()

        search_resp = MagicMock(status_code=200, raise_for_status=lambda: None)
        search_resp.json.return_value = {
            "hits": {"hits": [{"_id": search_id, "_source": {"file_date": "2024-06-01"}}]}
        }

        exhibit_resp = MagicMock(
            status_code=200,
            text=f"<html><body>{exhibit_text}</body></html>",
            raise_for_status=lambda: None,
        )

        def side_effect(url, **kwargs):
            if kwargs.get('params'):
                return search_resp
            return exhibit_resp

        session.get.side_effect = side_effect
        return session

    def test_fetches_exhibit_when_id_contains_doc_name(self):
        from datetime import date as dt
        from scrapers.edgar_connector import EdgarConnector

        mock_db = MagicMock()
        mock_db.report_exists.return_value = False

        session = self._make_session(
            search_id='0001437491-24-001:ex991pressrelease.htm',
            exhibit_text='MARA mined 750 bitcoin during June 2024',
        )
        connector = EdgarConnector(db=mock_db, session=session)
        connector.fetch_8k_filings(cik='0001437491', ticker='MARA', since_date=dt(2024, 1, 1))

        stored = mock_db.insert_report.call_args[0][0]
        assert 'ex991pressrelease.htm' in stored['source_url']
        assert 'MARA mined' in stored['raw_text']

    def test_does_not_store_edgar_index_boilerplate(self):
        from datetime import date as dt
        from scrapers.edgar_connector import EdgarConnector

        mock_db = MagicMock()
        mock_db.report_exists.return_value = False

        session = self._make_session(
            search_id='0001437491-24-001:ex991.htm',
            exhibit_text='Bitcoin production was 750 BTC',
        )
        connector = EdgarConnector(db=mock_db, session=session)
        connector.fetch_8k_filings(cik='0001437491', ticker='MARA', since_date=dt(2024, 1, 1))

        stored = mock_db.insert_report.call_args[0][0]
        assert not stored['raw_text'].startswith('EDGAR Filing Documents')


class TestFetch8kNoExtraction:
    def test_fetch_8k_no_extraction(self):
        """Verify fetch_8k_filings never calls extract_all — Stage 1 only."""
        import requests as req
        from scrapers.edgar_connector import EdgarConnector

        mock_db = MagicMock()
        mock_db.report_exists.return_value = False
        mock_db.insert_report.return_value = 1

        session = MagicMock()
        # Simulate EDGAR full-text search response
        session.get.return_value = MagicMock(
            status_code=200,
            json=lambda: {
                "hits": {
                    "hits": [
                        {
                            "_id": "0001-24-001",
                            "_source": {
                                "file_date": "2024-06-01",
                                "entity_name": "MARA Holdings",
                                "period_ending": "2024-06-01",
                            }
                        }
                    ]
                }
            },
            text="<html><body>Bitcoin production report text here</body></html>",
            raise_for_status=lambda: None,
        )

        connector = EdgarConnector(db=mock_db, session=session)

        with patch('scrapers.edgar_connector.extract_all') as mock_extract:
            connector.fetch_8k_filings(cik="0001437491", ticker="MARA",
                                        since_date=__import__('datetime').date(2024, 1, 1))
            mock_extract.assert_not_called()
