"""Tests for schema v18 — UNIQUE index on (ticker, source_url_hash) — Fix 4."""
import sqlite3
import pytest
from infra.db import MinerDB


@pytest.fixture
def db_mara(tmp_path):
    db = MinerDB(str(tmp_path / 'test.db'))
    db.insert_company({
        'ticker': 'MARA', 'name': 'MARA Holdings', 'tier': 1,
        'ir_url': 'https://example.com', 'pr_base_url': None,
        'cik': '0001437491', 'active': 1,
    })
    return db


class TestMigrateV18:
    def test_unique_index_exists(self, db_mara):
        """After v18, idx_reports_url_hash must be a UNIQUE index."""
        with db_mara._get_connection() as conn:
            indexes = conn.execute(
                "SELECT name, \"unique\" FROM pragma_index_list('reports')"
            ).fetchall()
        url_hash_indexes = [r for r in indexes if r[0] == 'idx_reports_url_hash']
        assert url_hash_indexes, "idx_reports_url_hash index must exist on reports"
        assert url_hash_indexes[0][1] == 1, "idx_reports_url_hash must be UNIQUE"

    def test_schema_version_is_18(self, db_mara):
        with db_mara._get_connection() as conn:
            ver = conn.execute("PRAGMA user_version").fetchone()[0]
        assert ver == 18

    def test_duplicate_url_hash_rejected(self, db_mara):
        """Inserting two reports with same (ticker, source_url_hash) must raise IntegrityError."""
        url_hash = 'a' * 64
        from helpers import make_report
        db_mara.insert_report(make_report(
            source_url='https://example.com/pr1',
            source_type='ir_press_release',
        ))
        with db_mara._get_connection() as conn:
            conn.execute(
                "UPDATE reports SET source_url_hash=? WHERE ticker='MARA'", (url_hash,)
            )
        with pytest.raises(sqlite3.IntegrityError):
            with db_mara._get_connection() as conn:
                conn.execute(
                    """INSERT INTO reports(ticker, report_date, source_type, source_url_hash)
                       VALUES('MARA', '2024-02-01', 'ir_press_release', ?)""",
                    (url_hash,),
                )

    def test_null_url_hash_allows_multiple(self, db_mara):
        """NULL source_url_hash is excluded from the UNIQUE constraint."""
        from helpers import make_report
        # Two reports with NULL hash — must not raise
        db_mara.insert_report(make_report(report_date='2024-01-01'))
        db_mara.insert_report(make_report(report_date='2024-02-01'))
        # If we get here without IntegrityError, the partial index is correct.
