"""
Tests for CSV export v2: new provenance columns (source_url, llm_value,
regex_value, agreement_status).

TDD: tests written before implementation.
"""
import csv
import io
import pytest
from infra.db import MinerDB


def _seed_report_and_dp(db, ticker='MARA', source_url='https://ir.mara.com/jan2023.html'):
    """Insert a report and data_point; return (report_id, dp_id)."""
    report_id = db.insert_report({
        'ticker': ticker,
        'report_date': '2023-01-01',
        'published_date': '2023-01-05',
        'source_type': 'archive',
        'source_url': source_url,
        'raw_text': 'Bitcoin produced 692 BTC',
        'parsed_at': '2023-01-05T00:00:00',
    })
    dp_id = db.insert_data_point({
        'report_id': report_id,
        'ticker': ticker,
        'period': '2023-01-01',
        'metric': 'production_btc',
        'value': 692.0,
        'unit': 'BTC',
        'confidence': 0.95,
        'extraction_method': 'regex',
        'source_snippet': 'Bitcoin produced 692 BTC',
    })
    return report_id, dp_id


@pytest.fixture
def app_with_data(db_with_company, tmp_path):
    """Flask test app with MARA company + one data point seeded."""
    import sys, os
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))
    import app_globals
    app_globals._db = db_with_company

    import importlib, run_web
    importlib.reload(run_web)
    flask_app = run_web.create_app()
    flask_app.config['TESTING'] = True
    return flask_app, db_with_company


class TestCsvExportV2:
    def test_csv_has_provenance_columns(self, app_with_data):
        """CSV must include source_url, llm_value, regex_value, agreement_status."""
        flask_app, db = app_with_data
        _seed_report_and_dp(db)

        with flask_app.test_client() as client:
            resp = client.get('/api/export.csv')
        assert resp.status_code == 200
        assert 'text/csv' in resp.content_type

        reader = csv.DictReader(io.StringIO(resp.data.decode('utf-8')))
        fieldnames = reader.fieldnames
        assert 'source_url' in fieldnames, f"source_url missing; fields: {fieldnames}"
        assert 'llm_value' in fieldnames, f"llm_value missing"
        assert 'regex_value' in fieldnames, f"regex_value missing"
        assert 'agreement_status' in fieldnames, f"agreement_status missing"

    def test_csv_core_fields_present(self, app_with_data):
        """Core provenance fields from plan must all be present."""
        flask_app, db = app_with_data
        _seed_report_and_dp(db)

        with flask_app.test_client() as client:
            resp = client.get('/api/export.csv')
        reader = csv.DictReader(io.StringIO(resp.data.decode('utf-8')))
        required = {'ticker', 'period', 'metric', 'value', 'unit', 'confidence',
                    'extraction_method', 'source_url'}
        assert required.issubset(set(reader.fieldnames)), \
            f"Missing required fields: {required - set(reader.fieldnames)}"

    def test_source_url_populated_from_report(self, app_with_data):
        """source_url in CSV row should match the report's source_url."""
        flask_app, db = app_with_data
        _seed_report_and_dp(db)

        with flask_app.test_client() as client:
            resp = client.get('/api/export.csv')
        reader = csv.DictReader(io.StringIO(resp.data.decode('utf-8')))
        rows = list(reader)
        assert len(rows) >= 1
        assert rows[0]['source_url'] == 'https://ir.mara.com/jan2023.html'

    def test_llm_regex_values_empty_when_no_review_item(self, app_with_data):
        """When there's no review_queue row, llm_value/regex_value should be empty."""
        flask_app, db = app_with_data
        _seed_report_and_dp(db)

        with flask_app.test_client() as client:
            resp = client.get('/api/export.csv')
        reader = csv.DictReader(io.StringIO(resp.data.decode('utf-8')))
        row = list(reader)[0]
        assert row['llm_value'] == ''
        assert row['regex_value'] == ''
        assert row['agreement_status'] == ''

    def test_period_iso_format(self, app_with_data):
        """Period should be in YYYY-MM-DD format."""
        flask_app, db = app_with_data
        _seed_report_and_dp(db)

        with flask_app.test_client() as client:
            resp = client.get('/api/export.csv')
        reader = csv.DictReader(io.StringIO(resp.data.decode('utf-8')))
        row = list(reader)[0]
        period = row['period']
        import re
        assert re.match(r'^\d{4}-\d{2}-\d{2}$', period), f"Period not ISO: {period}"

    def test_csv_filter_by_ticker(self, app_with_data):
        """Ticker filter applied to /api/export.csv."""
        flask_app, db = app_with_data
        _seed_report_and_dp(db)  # MARA

        # Insert RIOT company + data point
        db.insert_company({
            'ticker': 'RIOT', 'name': 'Riot Platforms', 'tier': 1,
            'ir_url': 'https://www.riotplatforms.com/news',
            'pr_base_url': 'https://www.riotplatforms.com',
            'cik': '0001167419', 'active': 1,
        })
        _seed_report_and_dp(db, ticker='RIOT', source_url=None)

        with flask_app.test_client() as client:
            resp = client.get('/api/export.csv?ticker=MARA')
        reader = csv.DictReader(io.StringIO(resp.data.decode('utf-8')))
        rows = list(reader)
        assert all(r['ticker'] == 'MARA' for r in rows), "Filter failed: RIOT rows present"
        assert len(rows) == 1
