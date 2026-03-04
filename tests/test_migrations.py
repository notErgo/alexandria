"""
Migration tests — verify schema version 2 tables and columns exist.

Tests are written first (TDD). They should FAIL before the migration is applied
and PASS after db.py is updated to include the v2 migration.
"""
import pytest
import sqlite3


@pytest.fixture
def fresh_db(tmp_path):
    """Fresh MinerDB on a new temp path — always migrates from version 0."""
    from infra.db import MinerDB
    return MinerDB(str(tmp_path / 'migration_test.db'))


@pytest.fixture
def raw_conn(fresh_db):
    """Raw sqlite3 connection to the fresh DB, for PRAGMA inspection."""
    conn = sqlite3.connect(fresh_db.db_path)
    yield conn
    conn.close()


def _table_columns(conn, table_name):
    """Return set of column names for a table."""
    rows = conn.execute(f"PRAGMA table_info({table_name})").fetchall()
    return {row[1] for row in rows}


class TestSchemaVersion:
    def test_user_version_is_current(self, raw_conn):
        """After init, PRAGMA user_version should be >= 3 (v4 adds config/hints tables)."""
        version = raw_conn.execute("PRAGMA user_version").fetchone()[0]
        assert version >= 3


class TestSourceAuditTable:
    def test_source_audit_table_exists(self, raw_conn):
        cols = _table_columns(raw_conn, 'source_audit')
        assert len(cols) > 0, "source_audit table does not exist"

    def test_source_audit_has_required_columns(self, raw_conn):
        cols = _table_columns(raw_conn, 'source_audit')
        required = {'id', 'ticker', 'source_type', 'url', 'last_checked',
                    'http_status', 'status', 'notes'}
        assert required.issubset(cols), f"Missing columns: {required - cols}"


class TestBtcLoansTable:
    def test_btc_loans_table_exists(self, raw_conn):
        cols = _table_columns(raw_conn, 'btc_loans')
        assert len(cols) > 0, "btc_loans table does not exist"

    def test_btc_loans_has_required_columns(self, raw_conn):
        cols = _table_columns(raw_conn, 'btc_loans')
        required = {'id', 'ticker', 'counterparty', 'total_btc_encumbered', 'as_of_date', 'source_snippet'}
        assert required.issubset(cols), f"Missing columns: {required - cols}"


class TestFacilitiesTable:
    def test_facilities_table_exists(self, raw_conn):
        cols = _table_columns(raw_conn, 'facilities')
        assert len(cols) > 0, "facilities table does not exist"

    def test_facilities_has_required_columns(self, raw_conn):
        cols = _table_columns(raw_conn, 'facilities')
        required = {'id', 'ticker', 'name', 'address', 'city', 'state', 'lat', 'lon',
                    'purpose', 'size_mw', 'operational_since'}
        assert required.issubset(cols), f"Missing columns: {required - cols}"


class TestLlmPromptsTable:
    def test_llm_prompts_table_exists(self, raw_conn):
        cols = _table_columns(raw_conn, 'llm_prompts')
        assert len(cols) > 0, "llm_prompts table does not exist"

    def test_llm_prompts_has_required_columns(self, raw_conn):
        cols = _table_columns(raw_conn, 'llm_prompts')
        required = {'id', 'metric', 'prompt_text', 'model', 'active', 'created_at', 'updated_at'}
        assert required.issubset(cols), f"Missing columns: {required - cols}"


class TestReviewQueueNewColumns:
    def test_review_queue_has_llm_value(self, raw_conn):
        cols = _table_columns(raw_conn, 'review_queue')
        assert 'llm_value' in cols, "review_queue missing llm_value column"

    def test_review_queue_has_agreement_status(self, raw_conn):
        cols = _table_columns(raw_conn, 'review_queue')
        assert 'agreement_status' in cols, "review_queue missing agreement_status column"

    def test_review_queue_has_regex_value(self, raw_conn):
        cols = _table_columns(raw_conn, 'review_queue')
        assert 'regex_value' in cols, "review_queue missing regex_value column"


class TestMigrationIdempotent:
    def test_init_twice_does_not_error(self, tmp_path):
        """Calling MinerDB init on an already-migrated DB must not raise."""
        from infra.db import MinerDB
        db_path = str(tmp_path / 'idempotent_test.db')
        db1 = MinerDB(db_path)
        # Second init on same path should be a no-op
        db2 = MinerDB(db_path)
        assert db2 is not None


class TestSchemaV5:
    """Tests for schema migration version 5 (asset_manifest, document_chunks, new columns)."""

    def test_v5_schema_version_is_at_least_5(self, raw_conn):
        """PRAGMA user_version must be >= 5 after fresh DB init."""
        version = raw_conn.execute("PRAGMA user_version").fetchone()[0]
        assert version >= 5

    def test_v5_asset_manifest_table_exists(self, raw_conn):
        """asset_manifest table must exist with the v5 columns (plus any later additions)."""
        cols = _table_columns(raw_conn, 'asset_manifest')
        # v5 required columns; later migrations may add more
        expected = {
            'id', 'ticker', 'period', 'source_type', 'file_path',
            'filename', 'discovered_at', 'ingest_state', 'report_id',
            'ingest_error', 'notes',
        }
        assert expected.issubset(cols), f"Missing v5 cols: {expected - cols}"

    def test_v5_document_chunks_table_exists(self, raw_conn):
        """document_chunks table must exist with correct columns."""
        cols = _table_columns(raw_conn, 'document_chunks')
        expected = {
            'id', 'report_id', 'chunk_index', 'section', 'text',
            'char_start', 'char_end', 'token_count', 'embedding', 'embedded_at',
        }
        assert expected.issubset(cols), f"Missing cols: {expected - cols}"

    def test_v5_data_points_has_chunk_id(self, raw_conn):
        """data_points table must have chunk_id column."""
        cols = _table_columns(raw_conn, 'data_points')
        assert 'chunk_id' in cols, f"chunk_id not in data_points cols: {cols}"

    def test_v5_reports_has_parse_quality(self, raw_conn):
        """reports table must have parse_quality column."""
        cols = _table_columns(raw_conn, 'reports')
        assert 'parse_quality' in cols, f"parse_quality not in reports cols: {cols}"


class TestSchemaV6:
    """Tests for schema migration version 6 (llm_benchmark_runs)."""

    def test_v6_schema_version_is_6(self, raw_conn):
        """PRAGMA user_version must be at least 6 after fresh DB init."""
        version = raw_conn.execute("PRAGMA user_version").fetchone()[0]
        assert version >= 6

    def test_v6_benchmark_runs_table_exists(self, raw_conn):
        """llm_benchmark_runs table must exist with correct columns."""
        cols = _table_columns(raw_conn, 'llm_benchmark_runs')
        expected = {
            'id', 'created_at', 'model', 'call_type', 'ticker', 'period',
            'report_id', 'prompt_chars', 'response_chars', 'prompt_tokens',
            'response_tokens', 'total_duration_ms', 'eval_duration_ms',
            'metrics_requested', 'metrics_extracted',
            'hits_90', 'hits_80', 'hits_75',
        }
        assert expected.issubset(cols), f"Missing cols: {expected - cols}"
