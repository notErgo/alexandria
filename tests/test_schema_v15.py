"""
Tests for schema v15 DB migrations and new DB methods.

Finding 1 (deduplication), Finding 3 (extraction backlog states),
Finding 4 (provenance), Finding 6 (source normalization).
"""
import hashlib
import sqlite3
import tempfile
from pathlib import Path

import pytest


@pytest.fixture()
def db(tmp_path):
    """Fresh MinerDB in a temp directory."""
    from infra.db import MinerDB
    db_path = str(tmp_path / "test.db")
    return MinerDB(db_path)


@pytest.fixture()
def db_with_company(db):
    db.insert_company({
        'ticker': 'MARA', 'name': 'MARA Holdings', 'tier': 1,
        'ir_url': 'http://example.com', 'pr_base_url': None,
        'cik': '0001507605', 'active': 1,
    })
    return db


# ── Finding 1: Deduplication ─────────────────────────────────────────────────

class TestDeduplication:

    def test_accession_number_column_exists(self, db):
        """reports table must have accession_number column after v15 migration."""
        with db._get_connection() as conn:
            cols = {r[1] for r in conn.execute("PRAGMA table_info(reports)").fetchall()}
        assert 'accession_number' in cols

    def test_source_url_hash_column_exists(self, db):
        """reports table must have source_url_hash column after v15 migration."""
        with db._get_connection() as conn:
            cols = {r[1] for r in conn.execute("PRAGMA table_info(reports)").fetchall()}
        assert 'source_url_hash' in cols

    def test_accession_number_unique_constraint(self, db_with_company):
        """Two inserts with the same accession_number must raise IntegrityError."""
        report_base = {
            'ticker': 'MARA',
            'report_date': '2024-01-15',
            'published_date': '2024-01-15',
            'source_type': 'edgar_8k',
            'source_url': 'https://www.sec.gov/Archives/1',
            'raw_text': 'some text',
            'parsed_at': None,
            'covering_period': None,
            'accession_number': '0001507605-24-000001',
        }
        db_with_company.insert_report(report_base)
        # Second insert with same accession_number must fail
        with pytest.raises(Exception):
            db_with_company.insert_report({
                **report_base,
                'report_date': '2024-01-16',
                'source_url': 'https://www.sec.gov/Archives/2',
            })

    def test_url_hash_dedup_prevents_duplicate_insert(self, db_with_company):
        """insert_report must compute and store source_url_hash for URL dedup."""
        url = 'https://ir.mara.com/press-release/jan-2024'
        report = {
            'ticker': 'MARA',
            'report_date': '2024-01-15',
            'published_date': '2024-01-15',
            'source_type': 'ir_press_release',
            'source_url': url,
            'raw_text': 'press release text',
            'parsed_at': None,
            'covering_period': None,
        }
        db_with_company.insert_report(report)
        expected_hash = hashlib.sha256(url.encode()).hexdigest()
        with db_with_company._get_connection() as conn:
            row = conn.execute(
                "SELECT source_url_hash FROM reports WHERE ticker='MARA' AND source_type='ir_press_release'"
            ).fetchone()
        assert row is not None
        assert row[0] == expected_hash

    def test_insert_report_idempotent_on_accession_conflict(self, db_with_company):
        """report_exists_by_accession returns True after first insert."""
        acc = '0001507605-24-000999'
        report = {
            'ticker': 'MARA',
            'report_date': '2024-03-15',
            'published_date': '2024-03-15',
            'source_type': 'edgar_8k',
            'source_url': 'https://www.sec.gov/Archives/999',
            'raw_text': 'some text',
            'parsed_at': None,
            'covering_period': None,
            'accession_number': acc,
        }
        db_with_company.insert_report(report)
        assert db_with_company.report_exists_by_accession(acc) is True

    def test_report_exists_by_accession_false_on_miss(self, db_with_company):
        assert db_with_company.report_exists_by_accession('0000-00-000000') is False


# ── Finding 3: Extraction backlog states ─────────────────────────────────────

class TestExtractionStatus:

    def _insert_report(self, db):
        db.insert_company({
            'ticker': 'RIOT', 'name': 'Riot', 'tier': 1,
            'ir_url': 'http://example.com', 'pr_base_url': None,
            'cik': None, 'active': 1,
        })
        return db.insert_report({
            'ticker': 'RIOT',
            'report_date': '2024-02-01',
            'published_date': '2024-02-01',
            'source_type': 'archive_html',
            'source_url': None,
            'raw_text': 'test text',
            'parsed_at': None,
            'covering_period': None,
        })

    def test_extraction_status_defaults_to_pending(self, db):
        report_id = self._insert_report(db)
        with db._get_connection() as conn:
            row = conn.execute(
                "SELECT extraction_status, extraction_attempts FROM reports WHERE id=?",
                (report_id,),
            ).fetchone()
        assert row['extraction_status'] == 'pending'
        assert row['extraction_attempts'] == 0

    def test_extraction_status_column_exists(self, db):
        with db._get_connection() as conn:
            cols = {r[1] for r in conn.execute("PRAGMA table_info(reports)").fetchall()}
        assert 'extraction_status' in cols
        assert 'extraction_error' in cols
        assert 'extraction_attempts' in cols

    def test_mark_report_extraction_failed_sets_error_and_increments_attempts(self, db):
        report_id = self._insert_report(db)
        db.mark_report_extraction_failed(report_id, "parse error: bad PDF")
        with db._get_connection() as conn:
            row = conn.execute(
                "SELECT extraction_status, extraction_error, extraction_attempts FROM reports WHERE id=?",
                (report_id,),
            ).fetchone()
        assert row['extraction_status'] == 'failed'
        assert 'parse error' in row['extraction_error']
        assert row['extraction_attempts'] == 1

    def test_dead_letter_after_max_attempts(self, db):
        from config import MAX_EXTRACTION_ATTEMPTS
        report_id = self._insert_report(db)
        for _ in range(MAX_EXTRACTION_ATTEMPTS):
            db.mark_report_extraction_failed(report_id, "persistent error")
        with db._get_connection() as conn:
            row = conn.execute(
                "SELECT extraction_status, extraction_attempts FROM reports WHERE id=?",
                (report_id,),
            ).fetchone()
        assert row['extraction_status'] == 'dead_letter'
        assert row['extraction_attempts'] == MAX_EXTRACTION_ATTEMPTS

    def test_get_unextracted_excludes_dead_letter(self, db):
        report_id = self._insert_report(db)
        from config import MAX_EXTRACTION_ATTEMPTS
        for _ in range(MAX_EXTRACTION_ATTEMPTS):
            db.mark_report_extraction_failed(report_id, "error")
        unextracted = db.get_unextracted_reports(ticker='RIOT')
        ids = [r['id'] for r in unextracted]
        assert report_id not in ids


# ── Finding 4: Provenance ─────────────────────────────────────────────────────

class TestProvenance:

    def _setup(self, db):
        db.insert_company({
            'ticker': 'CLSK', 'name': 'CleanSpark', 'tier': 1,
            'ir_url': 'http://example.com', 'pr_base_url': None,
            'cik': None, 'active': 1,
        })
        report_id = db.insert_report({
            'ticker': 'CLSK',
            'report_date': '2024-01-01',
            'published_date': '2024-01-01',
            'source_type': 'archive_html',
            'source_url': None,
            'raw_text': 'mined 500 BTC',
            'parsed_at': None,
            'covering_period': None,
        })
        return report_id

    def test_data_points_provenance_columns_exist(self, db):
        with db._get_connection() as conn:
            cols = {r[1] for r in conn.execute("PRAGMA table_info(data_points)").fetchall()}
        assert 'run_id' in cols
        assert 'model_name' in cols
        assert 'extractor_version' in cols
        assert 'prompt_version' in cols

    def test_data_points_run_id_stored(self, db):
        report_id = self._setup(db)
        db.insert_data_point({
            'report_id': report_id,
            'ticker': 'CLSK',
            'period': '2024-01-01',
            'metric': 'production_btc',
            'value': 500.0,
            'unit': 'BTC',
            'confidence': 0.95,
            'extraction_method': 'regex',
            'source_snippet': 'mined 500 BTC',
            'run_id': 'run-abc-123',
        })
        with db._get_connection() as conn:
            row = conn.execute(
                "SELECT run_id FROM data_points WHERE ticker='CLSK' AND metric='production_btc'"
            ).fetchone()
        assert row['run_id'] == 'run-abc-123'

    def test_data_points_model_name_stored(self, db):
        report_id = self._setup(db)
        db.insert_data_point({
            'report_id': report_id,
            'ticker': 'CLSK',
            'period': '2024-02-01',
            'metric': 'hodl_btc',
            'value': 1000.0,
            'unit': 'BTC',
            'confidence': 0.90,
            'extraction_method': 'llm',
            'source_snippet': None,
            'run_id': 'run-xyz',
            'model_name': 'qwen3.5:9b',
            'extractor_version': '1.0',
            'prompt_version': '1.0',
        })
        with db._get_connection() as conn:
            row = conn.execute(
                "SELECT model_name, extractor_version FROM data_points WHERE ticker='CLSK' AND metric='hodl_btc'"
            ).fetchone()
        assert row['model_name'] == 'qwen3.5:9b'
        assert row['extractor_version'] == '1.0'

    def test_data_points_extractor_version_stored(self, db):
        report_id = self._setup(db)
        db.insert_data_point({
            'report_id': report_id,
            'ticker': 'CLSK',
            'period': '2024-03-01',
            'metric': 'hashrate_eh',
            'value': 25.0,
            'unit': 'EH/s',
            'confidence': 0.88,
            'extraction_method': 'llm',
            'source_snippet': None,
            'extractor_version': '2.0',
            'prompt_version': '1.1',
        })
        with db._get_connection() as conn:
            row = conn.execute(
                "SELECT extractor_version, prompt_version FROM data_points WHERE ticker='CLSK' AND metric='hashrate_eh'"
            ).fetchone()
        assert row['extractor_version'] == '2.0'
        assert row['prompt_version'] == '1.1'


# ── Finding 6: source_channel + form_type ────────────────────────────────────

class TestSourceNormalization:

    def test_source_channel_form_type_columns_exist(self, db):
        with db._get_connection() as conn:
            cols = {r[1] for r in conn.execute("PRAGMA table_info(reports)").fetchall()}
        assert 'source_channel' in cols
        assert 'form_type' in cols

    def test_source_channel_set_on_edgar_insert(self, db):
        db.insert_company({
            'ticker': 'BITF', 'name': 'Bitfarms', 'tier': 1,
            'ir_url': 'http://example.com', 'pr_base_url': None,
            'cik': None, 'active': 1,
        })
        db.insert_report({
            'ticker': 'BITF',
            'report_date': '2024-06-30',
            'published_date': '2024-06-30',
            'source_type': 'edgar_40f',
            'source_url': 'https://www.sec.gov/Archives/edgar/data/1812477/000181247724000001',
            'raw_text': 'annual report text',
            'parsed_at': None,
            'covering_period': '2024-FY',
            'source_channel': 'edgar',
            'form_type': '40-F',
        })
        with db._get_connection() as conn:
            row = conn.execute(
                "SELECT source_channel, form_type FROM reports WHERE ticker='BITF'"
            ).fetchone()
        assert row['source_channel'] == 'edgar'
        assert row['form_type'] == '40-F'

    def test_form_type_set_on_10q_insert(self, db):
        db.insert_company({
            'ticker': 'CORZ', 'name': 'Core Scientific', 'tier': 1,
            'ir_url': 'http://example.com', 'pr_base_url': None,
            'cik': None, 'active': 1,
        })
        db.insert_report({
            'ticker': 'CORZ',
            'report_date': '2024-03-31',
            'published_date': '2024-05-15',
            'source_type': 'edgar_10q',
            'source_url': 'https://www.sec.gov/Archives/edgar/1',
            'raw_text': 'quarterly report',
            'parsed_at': None,
            'covering_period': '2024-Q1',
            'source_channel': 'edgar',
            'form_type': '10-Q',
        })
        with db._get_connection() as conn:
            row = conn.execute(
                "SELECT form_type FROM reports WHERE ticker='CORZ'"
            ).fetchone()
        assert row['form_type'] == '10-Q'

    def test_get_reports_by_channel_filters_correctly(self, db):
        db.insert_company({
            'ticker': 'WULF', 'name': 'TeraWulf', 'tier': 2,
            'ir_url': 'http://example.com', 'pr_base_url': None,
            'cik': None, 'active': 1,
        })
        # Insert one edgar report and one IR report
        db.insert_report({
            'ticker': 'WULF',
            'report_date': '2024-01-15',
            'published_date': '2024-01-15',
            'source_type': 'edgar_8k',
            'source_url': 'https://www.sec.gov/Archives/1',
            'raw_text': 'edgar text',
            'parsed_at': None,
            'covering_period': None,
            'source_channel': 'edgar',
            'form_type': '8-K',
        })
        db.insert_report({
            'ticker': 'WULF',
            'report_date': '2024-01-20',
            'published_date': '2024-01-20',
            'source_type': 'ir_press_release',
            'source_url': 'https://investors.terawulf.com/pr/1',
            'raw_text': 'pr text',
            'parsed_at': None,
            'covering_period': None,
            'source_channel': 'ir',
            'form_type': 'press_release',
        })
        edgar_reports = db.get_reports_by_channel('WULF', channel='edgar')
        assert len(edgar_reports) == 1
        assert edgar_reports[0]['source_channel'] == 'edgar'
