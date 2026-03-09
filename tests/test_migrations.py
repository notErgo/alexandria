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


class TestSchemaV28:
    """Tests for schema migration version 28 (embedding_model on document_chunks)."""

    def test_v28_schema_version_is_28(self, raw_conn):
        """PRAGMA user_version must be at least 28 after fresh DB init."""
        version = raw_conn.execute("PRAGMA user_version").fetchone()[0]
        assert version >= 28

    def test_v28_document_chunks_has_embedding_columns(self, raw_conn):
        """document_chunks must have all three embedding columns."""
        cols = _table_columns(raw_conn, 'document_chunks')
        required = {'embedding', 'embedding_model', 'embedded_at'}
        assert required.issubset(cols), f"Missing cols: {required - cols}"

    def test_v28_embedding_model_defaults_to_null(self, raw_conn):
        """embedding_model column must be nullable (no default constraint)."""
        rows = raw_conn.execute(
            "PRAGMA table_info(document_chunks)"
        ).fetchall()
        col_info = {row[1]: row for row in rows}
        assert 'embedding_model' in col_info
        # dflt_value (index 4) should be None — no default
        assert col_info['embedding_model'][4] is None


class TestV30MetricKeywords:
    """v30 → v32: seed keywords for production_btc/hashrate_eh migrated to metric_schema.keywords JSON."""

    def test_schema_version_is_at_least_30(self, raw_conn):
        version = raw_conn.execute("PRAGMA user_version").fetchone()[0]
        assert version >= 30

    def test_metric_schema_has_keywords_column(self, raw_conn):
        """v30 introduced metric_keywords table; v32 folded it into metric_schema.keywords."""
        cols = _table_columns(raw_conn, 'metric_schema')
        assert 'keywords' in cols, f"metric_schema missing keywords column, got: {cols}"

    def test_production_btc_seeded_with_7_phrases(self, raw_conn):
        import json
        row = raw_conn.execute(
            "SELECT keywords FROM metric_schema WHERE key = 'production_btc'"
        ).fetchone()
        assert row is not None
        kws = json.loads(row[0] or '[]')
        assert len(kws) == 7, f"Expected 7 keywords for production_btc, got {len(kws)}"

    def test_hashrate_eh_seeded_with_1_phrase(self, raw_conn):
        import json
        row = raw_conn.execute(
            "SELECT keywords FROM metric_schema WHERE key = 'hashrate_eh'"
        ).fetchone()
        assert row is not None
        kws = json.loads(row[0] or '[]')
        assert len(kws) == 1, f"Expected 1 keyword for hashrate_eh, got {len(kws)}"

    def test_search_keywords_is_empty_after_v30(self, raw_conn):
        """v30 empties search_keywords (table kept but rows deleted)."""
        count = raw_conn.execute("SELECT COUNT(*) FROM search_keywords").fetchone()[0]
        assert count == 0, f"search_keywords should be empty after v30, got {count} rows"


class TestV31MetricKeywordsExclude:
    """v31 → v32: exclude_terms field present in keyword JSON objects."""

    def test_schema_version_is_at_least_31(self, raw_conn):
        version = raw_conn.execute("PRAGMA user_version").fetchone()[0]
        assert version >= 31

    def test_keyword_json_objects_have_exclude_terms_field(self, raw_conn):
        import json
        row = raw_conn.execute(
            "SELECT keywords FROM metric_schema WHERE key = 'production_btc'"
        ).fetchone()
        kws = json.loads(row[0] or '[]')
        assert len(kws) > 0
        for k in kws:
            assert 'exclude_terms' in k, f"keyword object missing exclude_terms: {k}"

    def test_seeded_exclude_terms_are_empty_string(self, raw_conn):
        import json
        rows = raw_conn.execute("SELECT keywords FROM metric_schema").fetchall()
        for row in rows:
            for k in json.loads(row[0] or '[]'):
                assert k.get('exclude_terms') == '', \
                    f"Seeded keywords should have empty exclude_terms, got: {k!r}"


class TestV32KeywordsOnMetricSchema:
    """v32: metric_keywords table dropped; keywords JSON column on metric_schema."""

    def test_schema_version_is_at_least_32(self, raw_conn):
        version = raw_conn.execute("PRAGMA user_version").fetchone()[0]
        assert version >= 32

    def test_metric_keywords_table_dropped(self, raw_conn):
        tables = {r[0] for r in raw_conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        ).fetchall()}
        assert 'metric_keywords' not in tables, "metric_keywords table should be dropped at v32"

    def test_keywords_json_objects_have_required_fields(self, raw_conn):
        import json
        row = raw_conn.execute(
            "SELECT keywords FROM metric_schema WHERE key = 'production_btc'"
        ).fetchone()
        kws = json.loads(row[0] or '[]')
        assert len(kws) > 0
        first = kws[0]
        assert {'id', 'phrase', 'exclude_terms', 'active', 'hit_count'}.issubset(first.keys())

    def test_other_metrics_default_to_empty_keywords(self, raw_conn):
        import json
        row = raw_conn.execute(
            "SELECT keywords FROM metric_schema WHERE key = 'sales_btc'"
        ).fetchone()
        assert row is not None
        kws = json.loads(row[0] or '[]')
        assert kws == [], f"sales_btc should have no seeded keywords, got: {kws}"


class TestSchemaV33:
    """v33 migration: reviewed_periods table."""

    def test_user_version_is_33(self, raw_conn):
        version = raw_conn.execute("PRAGMA user_version").fetchone()[0]
        assert version >= 33

    def test_reviewed_periods_table_exists(self, raw_conn):
        cols = _table_columns(raw_conn, 'reviewed_periods')
        assert len(cols) > 0, "reviewed_periods table does not exist"

    def test_reviewed_periods_columns(self, raw_conn):
        cols = _table_columns(raw_conn, 'reviewed_periods')
        required = {'id', 'ticker', 'period', 'reviewed_at'}
        assert required.issubset(cols), f"Missing columns: {required - cols}"

    def test_reviewed_periods_unique_constraint(self, fresh_db):
        """Cannot insert duplicate (ticker, period) pair."""
        fresh_db.set_reviewed_periods('MARA', ['2024-01-01'])
        count = fresh_db.set_reviewed_periods('MARA', ['2024-01-01'])  # INSERT OR IGNORE
        assert count == 0  # second insert is a no-op

    def test_get_reviewed_periods_empty(self, fresh_db):
        result = fresh_db.get_reviewed_periods('MARA')
        assert isinstance(result, set)
        assert len(result) == 0

    def test_set_and_get_reviewed_periods(self, fresh_db):
        fresh_db.set_reviewed_periods('MARA', ['2024-01-01', '2024-02-01'])
        result = fresh_db.get_reviewed_periods('MARA')
        assert '2024-01-01' in result
        assert '2024-02-01' in result

    def test_unset_reviewed_period(self, fresh_db):
        fresh_db.set_reviewed_periods('MARA', ['2024-01-01', '2024-02-01'])
        deleted = fresh_db.unset_reviewed_period('MARA', '2024-01-01')
        assert deleted == 1
        result = fresh_db.get_reviewed_periods('MARA')
        assert '2024-01-01' not in result
        assert '2024-02-01' in result

    def test_unset_all_reviewed(self, fresh_db):
        fresh_db.set_reviewed_periods('MARA', ['2024-01-01', '2024-02-01'])
        fresh_db.set_reviewed_periods('RIOT', ['2024-01-01'])
        deleted = fresh_db.unset_all_reviewed('MARA')
        assert deleted == 2
        assert len(fresh_db.get_reviewed_periods('MARA')) == 0
        assert len(fresh_db.get_reviewed_periods('RIOT')) == 1
