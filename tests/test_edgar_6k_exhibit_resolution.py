"""Regression tests for 6-K exhibit resolution.

6-K filings from foreign issuers often attach the actual operations update as
EX-99.1 / EX-99 while the primary filing document is only a SEC wrapper page.
The ingestor must prefer the exhibit when available.
"""
from datetime import date as dt
from unittest.mock import MagicMock


def _make_6k_submissions(acc_no: str, filing_date: str, period_of_report: str) -> dict:
    return {
        "filings": {
            "recent": {
                "form": ["6-K"],
                "filingDate": [filing_date],
                "accessionNumber": [acc_no],
                "primaryDocument": ["sixk.htm"],
                "periodOfReport": [period_of_report],
                "reportDate": [period_of_report],
            }
        }
    }


class TestFetch6kExhibitResolution:
    def _make_session(self, cik_numeric: str, acc_no: str, *, exhibit_text: str, primary_text: str):
        acc_clean = acc_no.replace('-', '')
        submissions_resp = MagicMock(status_code=200, raise_for_status=lambda: None)
        submissions_resp.json.return_value = _make_6k_submissions(
            acc_no=acc_no,
            filing_date='2026-02-26',
            period_of_report='2025-12-31',
        )

        index_html = (
            "<html><body><table>"
            "<tr><td>6-K</td><td><a href='sixk.htm'>sixk.htm</a></td><td>6-K</td></tr>"
            "<tr><td>EX-99.1</td><td><a href='operations-update.htm'>operations update</a></td><td>EX-99.1</td></tr>"
            "</table></body></html>"
        )
        index_resp = MagicMock(status_code=200, text=index_html, raise_for_status=lambda: None)
        exhibit_resp = MagicMock(
            status_code=200,
            text=f"<html><body>{exhibit_text}</body></html>",
            raise_for_status=lambda: None,
        )
        primary_resp = MagicMock(
            status_code=200,
            text=f"<html><body>{primary_text}</body></html>",
            raise_for_status=lambda: None,
        )

        def side_effect(url, **kwargs):
            if 'data.sec.gov' in url:
                return submissions_resp
            if f'{acc_clean}/{acc_no}-index.htm' in url:
                return index_resp
            if url.endswith('/operations-update.htm'):
                return exhibit_resp
            return primary_resp

        session = MagicMock()
        session.get.side_effect = side_effect
        return session

    def test_fetch_6k_prefers_ex99_exhibit_over_primary_doc(self):
        from scrapers.edgar_connector import EdgarConnector

        mock_db = MagicMock()
        mock_db.report_exists.return_value = False
        mock_db.report_exists_by_accession.return_value = False
        mock_db.insert_report.return_value = 1

        session = self._make_session(
            cik_numeric='1812477',
            acc_no='0001213900-26-020827',
            exhibit_text='Bitdeer produced 126 BTC during the quarter and reached hash rate of 9.4 EH/s.',
            primary_text='REPORT OF FOREIGN PRIVATE ISSUER FORM 6-K cover page only',
        )

        connector = EdgarConnector(db=mock_db, session=session)
        connector.fetch_6k_filings(cik='0001812477', ticker='BITF', since_date=dt(2020, 1, 1))

        stored = mock_db.insert_report.call_args[0][0]
        assert stored['source_type'] == 'edgar_6k'
        assert 'operations-update.htm' in stored['source_url']
        assert 'produced 126 BTC' in stored['raw_text']
        assert 'cover page only' not in stored['raw_text']

    def test_parse_current_report_exhibit_url_supports_6k(self):
        from scrapers.edgar_connector import parse_current_report_exhibit_url

        index_html = """
        <html><body><table>
        <tr><td>6-K</td><td><a href="/Archives/edgar/data/1/000001/sixk.htm">sixk.htm</a></td><td>6-K</td></tr>
        <tr><td>EX-99.1</td><td><a href="/Archives/edgar/data/1/000001/ex991.htm">ex991.htm</a></td><td>EX-99.1</td></tr>
        </table></body></html>
        """
        url = parse_current_report_exhibit_url(index_html, '1', '000001', '6-K')
        assert url is not None
        assert 'ex991.htm' in url
