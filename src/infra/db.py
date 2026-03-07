"""
SQLite persistence layer for the Bitcoin Miner Data Platform.

Uses WAL mode for concurrent reads during Flask operation.
Schema version tracked via PRAGMA user_version (current: 1).
All writes go through context managers (auto-commit on __exit__).
"""
import hashlib
import sqlite3
import threading
import logging
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

log = logging.getLogger('miners.infra.db')


class MinerDB:
    """SQLite store for all miner data."""

    def __init__(self, db_path: str) -> None:
        self.db_path = db_path
        Path(db_path).parent.mkdir(parents=True, exist_ok=True)
        self._init_lock = threading.Lock()
        self._init_db()

    def _get_connection(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path, timeout=30)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys = ON")
        return conn

    def _archive_db_path(self) -> str:
        return str(Path(self.db_path).with_name('purge_archive.db'))

    def _get_archive_connection(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self._archive_db_path(), timeout=30)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys = ON")
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS purge_batches (
                id             INTEGER PRIMARY KEY AUTOINCREMENT,
                created_at     TEXT    DEFAULT (datetime('now')),
                mode           TEXT    NOT NULL,
                ticker_scope   TEXT,
                reason         TEXT,
                source_db_path TEXT    NOT NULL
            );

            CREATE TABLE IF NOT EXISTS purge_rows (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                batch_id    INTEGER NOT NULL REFERENCES purge_batches(id) ON DELETE CASCADE,
                table_name  TEXT    NOT NULL,
                row_data    TEXT    NOT NULL
            );

            CREATE INDEX IF NOT EXISTS idx_purge_rows_batch
                ON purge_rows(batch_id);
        """)
        return conn

    def _init_db(self) -> None:
        with self._init_lock:
            with self._get_connection() as conn:
                version = conn.execute("PRAGMA user_version").fetchone()[0]
                if version == 0:
                    conn.execute("PRAGMA journal_mode=WAL")
                    conn.executescript("""
                        CREATE TABLE IF NOT EXISTS companies (
                            ticker      TEXT PRIMARY KEY,
                            name        TEXT NOT NULL,
                            tier        INTEGER NOT NULL,
                            ir_url      TEXT NOT NULL,
                            pr_base_url TEXT,
                            cik         TEXT,
                            active      INTEGER NOT NULL DEFAULT 1
                        );

                        CREATE TABLE IF NOT EXISTS reports (
                            id             INTEGER PRIMARY KEY AUTOINCREMENT,
                            ticker         TEXT NOT NULL REFERENCES companies(ticker),
                            report_date    TEXT NOT NULL,
                            published_date TEXT,
                            source_type    TEXT NOT NULL,
                            source_url     TEXT,
                            raw_text       TEXT,
                            parsed_at      TEXT
                        );

                        CREATE TABLE IF NOT EXISTS data_points (
                            id                INTEGER PRIMARY KEY AUTOINCREMENT,
                            report_id         INTEGER REFERENCES reports(id),
                            ticker            TEXT NOT NULL,
                            period            TEXT NOT NULL,
                            metric            TEXT NOT NULL,
                            value             REAL NOT NULL,
                            unit              TEXT NOT NULL,
                            confidence        REAL NOT NULL,
                            extraction_method TEXT,
                            source_snippet    TEXT,
                            created_at        TEXT DEFAULT (datetime('now')),
                            UNIQUE(ticker, period, metric)
                        );

                        CREATE TABLE IF NOT EXISTS patterns (
                            id               INTEGER PRIMARY KEY AUTOINCREMENT,
                            metric           TEXT NOT NULL,
                            ticker_scope     TEXT,
                            pattern_text     TEXT NOT NULL,
                            priority         INTEGER NOT NULL,
                            confidence_weight REAL NOT NULL
                        );

                        CREATE TABLE IF NOT EXISTS review_queue (
                            id             INTEGER PRIMARY KEY AUTOINCREMENT,
                            data_point_id  INTEGER REFERENCES data_points(id),
                            ticker         TEXT NOT NULL,
                            period         TEXT NOT NULL,
                            metric         TEXT NOT NULL,
                            raw_value      TEXT NOT NULL,
                            confidence     REAL NOT NULL,
                            source_snippet TEXT,
                            status         TEXT NOT NULL DEFAULT 'PENDING',
                            reviewer_note  TEXT,
                            created_at     TEXT DEFAULT (datetime('now')),
                            reviewed_at    TEXT
                        );

                        CREATE INDEX IF NOT EXISTS idx_dp_ticker_period
                            ON data_points(ticker, period);

                        CREATE INDEX IF NOT EXISTS idx_dp_metric_period
                            ON data_points(metric, period);

                        CREATE INDEX IF NOT EXISTS idx_rq_status
                            ON review_queue(status);
                    """)
                    conn.execute("PRAGMA user_version = 1")
                    version = 1

                if version < 2:
                    self._migrate_v2(conn)
                    conn.execute("PRAGMA user_version = 2")
                    version = 2

                if version < 3:
                    self._migrate_v3(conn)
                    conn.execute("PRAGMA user_version = 3")
                    version = 3

                if version < 4:
                    self._migrate_v4(conn)
                    conn.execute("PRAGMA user_version = 4")
                    version = 4

                if version < 5:
                    self._migrate_v5(conn)
                    conn.execute("PRAGMA user_version = 5")
                    version = 5

                if version < 6:
                    self._migrate_v6(conn)
                    conn.execute("PRAGMA user_version = 6")
                    version = 6

                if version < 7:
                    self._migrate_v7(conn)
                    conn.execute("PRAGMA user_version = 7")
                    version = 7

                if version < 8:
                    self._migrate_v8(conn)
                    conn.execute("PRAGMA user_version = 8")
                    version = 8

                if version < 9:
                    self._migrate_v9(conn)
                    conn.execute("PRAGMA user_version = 9")
                    version = 9

                if version < 10:
                    self._migrate_v10(conn)
                    conn.execute("PRAGMA user_version = 10")
                    version = 10

                if version < 11:
                    self._migrate_v11(conn)
                    conn.execute("PRAGMA user_version = 11")
                    version = 11

                if version < 12:
                    self._migrate_v12(conn)
                    conn.execute("PRAGMA user_version = 12")
                    version = 12

                if version < 13:
                    self._migrate_v13(conn)
                    conn.execute("PRAGMA user_version = 13")
                    version = 13

                if version < 14:
                    self._migrate_v14(conn)
                    conn.execute("PRAGMA user_version = 14")
                    version = 14

                if version < 15:
                    self._migrate_v15(conn)
                    conn.execute("PRAGMA user_version = 15")
                    version = 15

                if version < 16:
                    self._migrate_v16(conn)
                    conn.execute("PRAGMA user_version = 16")
                    version = 16

                if version < 17:
                    self._migrate_v17(conn)
                    conn.execute("PRAGMA user_version = 17")
                    version = 17

                if version < 18:
                    self._migrate_v18(conn)
                    conn.execute("PRAGMA user_version = 18")
                    version = 18

                if version < 19:
                    self._migrate_v19(conn)
                    conn.execute("PRAGMA user_version = 19")
                    version = 19

                if version < 20:
                    self._migrate_v20(conn)
                    conn.execute("PRAGMA user_version = 20")
                    version = 20

                if version < 21:
                    self._migrate_v21(conn)
                    conn.execute("PRAGMA user_version = 21")
                    version = 21

        # Sync company config from companies.json on startup only if enabled.
        # Runtime config key "auto_sync_companies_on_startup" (0/1) overrides
        # the env-backed default in config.AUTO_SYNC_COMPANIES_ON_STARTUP.
        from config import AUTO_SYNC_COMPANIES_ON_STARTUP
        auto_sync_enabled = bool(AUTO_SYNC_COMPANIES_ON_STARTUP)
        cfg_override = self.get_config('auto_sync_companies_on_startup', default=None)
        if cfg_override is not None:
            auto_sync_enabled = str(cfg_override).strip().lower() in {'1', 'true', 'yes', 'on'}
        if auto_sync_enabled:
            config_path = Path(__file__).parent.parent.parent / 'config' / 'companies.json'
            if config_path.exists():
                self.sync_companies_from_config(str(config_path))

    def _migrate_v2(self, conn: sqlite3.Connection) -> None:
        """Schema migration from version 1 to version 2.

        Adds:
        - review_queue columns: llm_value, regex_value, agreement_status
        - New tables: source_audit, btc_loans, facilities, llm_prompts
        """
        # Add new columns to review_queue (ALTER TABLE is additive — safe to run)
        existing_rq_cols = {row[1] for row in conn.execute("PRAGMA table_info(review_queue)").fetchall()}
        if 'llm_value' not in existing_rq_cols:
            conn.execute("ALTER TABLE review_queue ADD COLUMN llm_value REAL")
        if 'regex_value' not in existing_rq_cols:
            conn.execute("ALTER TABLE review_queue ADD COLUMN regex_value REAL")
        if 'agreement_status' not in existing_rq_cols:
            conn.execute("ALTER TABLE review_queue ADD COLUMN agreement_status TEXT")

        conn.executescript("""
            CREATE TABLE IF NOT EXISTS source_audit (
                id            INTEGER PRIMARY KEY AUTOINCREMENT,
                ticker        TEXT NOT NULL,
                source_type   TEXT NOT NULL,
                url           TEXT,
                last_checked  TEXT,
                http_status   INTEGER,
                status        TEXT NOT NULL DEFAULT 'NOT_TRIED',
                notes         TEXT,
                UNIQUE(ticker, source_type)
            );

            CREATE TABLE IF NOT EXISTS btc_loans (
                id                    INTEGER PRIMARY KEY AUTOINCREMENT,
                ticker                TEXT NOT NULL,
                counterparty          TEXT,
                total_btc_encumbered  REAL NOT NULL,
                as_of_date            TEXT,
                source_snippet        TEXT,
                created_at            TEXT DEFAULT (datetime('now'))
            );

            CREATE TABLE IF NOT EXISTS facilities (
                id                INTEGER PRIMARY KEY AUTOINCREMENT,
                ticker            TEXT NOT NULL,
                name              TEXT NOT NULL,
                address           TEXT,
                city              TEXT,
                state             TEXT,
                lat               REAL,
                lon               REAL,
                purpose           TEXT NOT NULL DEFAULT 'MINING',
                size_mw           REAL,
                operational_since TEXT,
                notes             TEXT,
                created_at        TEXT DEFAULT (datetime('now')),
                UNIQUE(ticker, name)
            );

            CREATE TABLE IF NOT EXISTS llm_prompts (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                metric      TEXT NOT NULL UNIQUE,
                prompt_text TEXT NOT NULL,
                model       TEXT NOT NULL DEFAULT 'unsloth/Qwen3.5-35B-A3B-GGUF:Q4_K_M',
                active      INTEGER NOT NULL DEFAULT 1,
                created_at  TEXT DEFAULT (datetime('now')),
                updated_at  TEXT DEFAULT (datetime('now'))
            );

            CREATE INDEX IF NOT EXISTS idx_sa_ticker ON source_audit(ticker);
            CREATE INDEX IF NOT EXISTS idx_loans_ticker ON btc_loans(ticker);
            CREATE INDEX IF NOT EXISTS idx_fac_ticker ON facilities(ticker);
            CREATE INDEX IF NOT EXISTS idx_llm_metric ON llm_prompts(metric);
        """)

    def _migrate_v3(self, conn: sqlite3.Connection) -> None:
        """Schema migration from version 2 to version 3.

        Adds extracted_at column to reports table to support the two-stage
        pipeline (ingest = fetch+store; extract = LLM+regex+agreement on stored).
        """
        existing_cols = {row[1] for row in conn.execute("PRAGMA table_info(reports)").fetchall()}
        if 'extracted_at' not in existing_cols:
            conn.execute("ALTER TABLE reports ADD COLUMN extracted_at TEXT")

    def _migrate_v4(self, conn: sqlite3.Connection) -> None:
        """Schema migration from version 3 to version 4.

        Adds:
        - config_settings: global key-value store (e.g. llm_batch_preamble)
        - llm_ticker_hints: per-ticker context hints injected into LLM prompts
        """
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS config_settings (
                key        TEXT PRIMARY KEY,
                value      TEXT NOT NULL,
                updated_at TEXT DEFAULT (datetime('now'))
            );

            CREATE TABLE IF NOT EXISTS llm_ticker_hints (
                ticker     TEXT PRIMARY KEY,
                hint       TEXT NOT NULL,
                active     INTEGER NOT NULL DEFAULT 1,
                updated_at TEXT DEFAULT (datetime('now'))
            );
        """)

    def _migrate_v5(self, conn: sqlite3.Connection) -> None:
        """Schema migration from version 4 to version 5.

        Adds:
        - asset_manifest: tracks every discovered archive file with lifecycle state
        - document_chunks: chunked text from parsed documents for embedding
        - data_points.chunk_id: links a data point back to the source chunk
        - reports.parse_quality: quality signal from document parser
        """
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS asset_manifest (
                id            INTEGER PRIMARY KEY AUTOINCREMENT,
                ticker        TEXT NOT NULL REFERENCES companies(ticker),
                period        TEXT,
                source_type   TEXT NOT NULL,
                file_path     TEXT NOT NULL,
                filename      TEXT NOT NULL,
                discovered_at TEXT DEFAULT (datetime('now')),
                ingest_state  TEXT NOT NULL DEFAULT 'pending',
                report_id     INTEGER REFERENCES reports(id),
                ingest_error  TEXT,
                notes         TEXT,
                UNIQUE(file_path)
            );

            CREATE INDEX IF NOT EXISTS idx_am_ticker_period
                ON asset_manifest(ticker, period);

            CREATE INDEX IF NOT EXISTS idx_am_ingest_state
                ON asset_manifest(ingest_state);

            CREATE TABLE IF NOT EXISTS document_chunks (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                report_id   INTEGER NOT NULL REFERENCES reports(id),
                chunk_index INTEGER NOT NULL,
                section     TEXT,
                text        TEXT NOT NULL,
                char_start  INTEGER,
                char_end    INTEGER,
                token_count INTEGER,
                embedding   BLOB,
                embedded_at TEXT,
                UNIQUE(report_id, chunk_index)
            );

            CREATE INDEX IF NOT EXISTS idx_chunks_report
                ON document_chunks(report_id);

            CREATE INDEX IF NOT EXISTS idx_chunks_unembedded
                ON document_chunks(embedded_at) WHERE embedded_at IS NULL;
        """)

        # Add chunk_id to data_points (safe — check first)
        existing_dp_cols = {row[1] for row in conn.execute("PRAGMA table_info(data_points)").fetchall()}
        if 'chunk_id' not in existing_dp_cols:
            conn.execute("ALTER TABLE data_points ADD COLUMN chunk_id INTEGER REFERENCES document_chunks(id)")

        # Add parse_quality to reports (safe — check first)
        existing_rpt_cols = {row[1] for row in conn.execute("PRAGMA table_info(reports)").fetchall()}
        if 'parse_quality' not in existing_rpt_cols:
            conn.execute("ALTER TABLE reports ADD COLUMN parse_quality TEXT")

    def _migrate_v6(self, conn: sqlite3.Connection) -> None:
        """Schema migration from version 5 to version 6.

        Adds:
        - llm_benchmark_runs: per-Ollama-call timing + hit-rate metrics
        """
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS llm_benchmark_runs (
                id                INTEGER PRIMARY KEY AUTOINCREMENT,
                created_at        TEXT    NOT NULL,
                model             TEXT    NOT NULL,
                call_type         TEXT    NOT NULL,
                ticker            TEXT,
                period            TEXT,
                report_id         INTEGER,
                prompt_chars      INTEGER,
                response_chars    INTEGER,
                prompt_tokens     INTEGER,
                response_tokens   INTEGER,
                total_duration_ms REAL,
                eval_duration_ms  REAL,
                metrics_requested INTEGER,
                metrics_extracted INTEGER,
                hits_90           INTEGER,
                hits_80           INTEGER,
                hits_75           INTEGER
            );

            CREATE INDEX IF NOT EXISTS idx_bench_model
                ON llm_benchmark_runs(model);

            CREATE INDEX IF NOT EXISTS idx_bench_ticker
                ON llm_benchmark_runs(ticker);
        """)

    def _seed_metric_schema(self, conn: sqlite3.Connection) -> None:
        """Seed metric_schema with the 13 known BTC-miners extraction metrics.

        Uses INSERT OR IGNORE so re-runs on an already-seeded DB are no-ops.
        Ordered to match the 13 pattern JSON files in config/patterns/.
        """
        metrics = [
            ('production_btc',          'BTC Produced',                 'BTC'),
            ('hodl_btc',                'BTC Holdings (Total)',          'BTC'),
            ('sold_btc',                'BTC Sold',                      'BTC'),
            ('hashrate_eh',             'Hashrate',                      'EH/s'),
            ('realization_rate',        'BTC Realization Rate',          '%'),
            ('ai_hpc_mw',               'AI/HPC Capacity',               'MW'),
            ('encumbered_btc',          'Encumbered BTC',                'BTC'),
            ('gpu_count',               'GPU Count',                     'units'),
            ('hodl_btc_restricted',     'BTC Holdings (Restricted)',     'BTC'),
            ('hodl_btc_unrestricted',   'BTC Holdings (Unrestricted)',   'BTC'),
            ('hpc_revenue_usd',         'HPC Revenue',                   'USD'),
            ('mining_mw',               'Mining Capacity',               'MW'),
            ('net_btc_balance_change',  'Net BTC Balance Change',        'BTC'),
        ]
        conn.executemany(
            """INSERT OR IGNORE INTO metric_schema
               (key, label, unit, sector, has_extraction_pattern, analyst_defined)
               VALUES (?, ?, ?, 'BTC-miners', 1, 0)""",
            metrics,
        )

    def _migrate_v7(self, conn: sqlite3.Connection) -> None:
        """Schema migration from version 6 to version 7.

        Adds:
          - companies: sector, scraper_mode, scraper_issues_log, scraper_status,
                       last_scrape_at, last_scrape_error, probe_completed_at
          - asset_manifest: mutation_log
          - NEW TABLE: regime_config
          - NEW TABLE: metric_schema
          - NEW TABLE: scrape_queue
        """
        # ALTER TABLE does not support multiple columns per statement in SQLite;
        # each column requires its own ALTER TABLE call.
        alterations = [
            "ALTER TABLE companies ADD COLUMN sector TEXT NOT NULL DEFAULT 'BTC-miners'",
            "ALTER TABLE companies ADD COLUMN scraper_mode TEXT NOT NULL DEFAULT 'skip'",
            "ALTER TABLE companies ADD COLUMN scraper_issues_log TEXT NOT NULL DEFAULT ''",
            "ALTER TABLE companies ADD COLUMN scraper_status TEXT NOT NULL DEFAULT 'never_run'",
            "ALTER TABLE companies ADD COLUMN last_scrape_at TEXT",
            "ALTER TABLE companies ADD COLUMN last_scrape_error TEXT",
            "ALTER TABLE companies ADD COLUMN probe_completed_at TEXT",
            "ALTER TABLE asset_manifest ADD COLUMN mutation_log TEXT",
        ]
        for sql in alterations:
            conn.execute(sql)

        conn.executescript("""
            CREATE TABLE IF NOT EXISTS regime_config (
                id         INTEGER PRIMARY KEY AUTOINCREMENT,
                ticker     TEXT NOT NULL REFERENCES companies(ticker),
                cadence    TEXT NOT NULL CHECK(cadence IN ('monthly','quarterly')),
                start_date TEXT NOT NULL,
                end_date   TEXT,
                notes      TEXT NOT NULL DEFAULT '',
                created_at TEXT DEFAULT (datetime('now'))
            );

            CREATE TABLE IF NOT EXISTS metric_schema (
                id                    INTEGER PRIMARY KEY AUTOINCREMENT,
                key                   TEXT NOT NULL,
                label                 TEXT NOT NULL,
                unit                  TEXT NOT NULL DEFAULT '',
                sector                TEXT NOT NULL DEFAULT 'BTC-miners',
                has_extraction_pattern INTEGER NOT NULL DEFAULT 0,
                analyst_defined       INTEGER NOT NULL DEFAULT 0,
                created_at            TEXT DEFAULT (datetime('now')),
                UNIQUE(key, sector)
            );

            CREATE TABLE IF NOT EXISTS scrape_queue (
                id           INTEGER PRIMARY KEY AUTOINCREMENT,
                ticker       TEXT NOT NULL REFERENCES companies(ticker),
                mode         TEXT NOT NULL DEFAULT 'historic'
                                 CHECK(mode IN ('historic','forward')),
                status       TEXT NOT NULL DEFAULT 'pending'
                                 CHECK(status IN ('pending','running','done','error')),
                created_at   TEXT DEFAULT (datetime('now')),
                started_at   TEXT,
                completed_at TEXT,
                error_msg    TEXT
            );
        """)

        self._seed_metric_schema(conn)

    def _migrate_v8(self, conn: sqlite3.Connection) -> None:
        """Schema migration from version 7 to version 8.

        Adds config fields to companies table that are present in companies.json
        but were never persisted to the DB:
          - rss_url: RSS feed URL for scraping
          - url_template: template URL pattern (e.g. with {month}/{year})
          - pr_start_year: year when press release archive begins
          - skip_reason: human-readable reason why scraper_mode is 'skip'
          - sandbox_note: notes on IR site behaviour, URL patterns, quirks
        """
        existing = {row[1] for row in conn.execute("PRAGMA table_info(companies)").fetchall()}
        for col, typedef in [
            ('rss_url',      'TEXT'),
            ('url_template', 'TEXT'),
            ('pr_start_year','INTEGER'),
            ('skip_reason',  'TEXT'),
            ('sandbox_note', 'TEXT'),
        ]:
            if col not in existing:
                conn.execute(f"ALTER TABLE companies ADD COLUMN {col} {typedef}")

    def _migrate_v9(self, conn: sqlite3.Connection) -> None:
        """Schema migration from version 8 to version 9.

        Adds provenance and period-type columns to support quarterly/annual filing ingestion:
          - data_points: source_period_type, covering_report_id, covering_period
          - reports: covering_period
          - llm_prompts: document_type
          - companies: last_edgar_at
        """
        existing_dp = {row[1] for row in conn.execute("PRAGMA table_info(data_points)").fetchall()}
        for col, typedef in [
            ('source_period_type', "TEXT NOT NULL DEFAULT 'monthly'"),
            ('covering_report_id', 'INTEGER REFERENCES reports(id)'),
            ('covering_period',    'TEXT'),
        ]:
            if col not in existing_dp:
                conn.execute(f"ALTER TABLE data_points ADD COLUMN {col} {typedef}")

        existing_rpt = {row[1] for row in conn.execute("PRAGMA table_info(reports)").fetchall()}
        if 'covering_period' not in existing_rpt:
            conn.execute("ALTER TABLE reports ADD COLUMN covering_period TEXT")

        existing_lp = {row[1] for row in conn.execute("PRAGMA table_info(llm_prompts)").fetchall()}
        if 'document_type' not in existing_lp:
            conn.execute(
                "ALTER TABLE llm_prompts ADD COLUMN document_type TEXT NOT NULL DEFAULT 'monthly'"
            )

        existing_co = {row[1] for row in conn.execute("PRAGMA table_info(companies)").fetchall()}
        if 'last_edgar_at' not in existing_co:
            conn.execute("ALTER TABLE companies ADD COLUMN last_edgar_at TEXT")

    def _migrate_v10(self, conn: sqlite3.Connection) -> None:
        """Schema migration from version 9 to version 10.

        Adds raw_extractions table: stores ALL numeric values extracted from every
        report via the broad LLM extraction pass. Unlike data_points (which tracks
        only the 13 standard metrics), raw_extractions captures everything the LLM
        finds — miners deployed, facility MWs, GPU counts, revenue, energy cost,
        custom KPIs, etc. Unknown metrics are stored with best-guess classification.
        """
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS raw_extractions (
                id                INTEGER PRIMARY KEY AUTOINCREMENT,
                report_id         INTEGER NOT NULL REFERENCES reports(id),
                ticker            TEXT NOT NULL,
                period            TEXT NOT NULL,
                metric_key        TEXT NOT NULL,
                category          TEXT NOT NULL DEFAULT 'unknown',
                value             REAL,
                value_text        TEXT NOT NULL,
                unit              TEXT,
                description       TEXT,
                raw_json          TEXT,
                confidence        REAL NOT NULL DEFAULT 0.0,
                source_snippet    TEXT,
                extraction_method TEXT NOT NULL DEFAULT 'llm_broad',
                created_at        TEXT DEFAULT (datetime('now')),
                UNIQUE(report_id, metric_key)
            );

            CREATE INDEX IF NOT EXISTS idx_rex_ticker_period
                ON raw_extractions(ticker, period);

            CREATE INDEX IF NOT EXISTS idx_rex_metric_key
                ON raw_extractions(metric_key);

            CREATE INDEX IF NOT EXISTS idx_rex_category
                ON raw_extractions(category);
        """)

    def _migrate_v11(self, conn: sqlite3.Connection) -> None:
        """Schema migration from version 10 to version 11.

        Adds metric_rules table: stores per-metric agreement and outlier thresholds
        that can be edited from the Ops UI without a code deploy.
        Seeded from config.py METRIC_AGREEMENT_THRESHOLDS and OUTLIER_THRESHOLDS.
        """
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS metric_rules (
                metric                TEXT PRIMARY KEY,
                agreement_threshold   REAL NOT NULL DEFAULT 0.02,
                outlier_threshold     REAL NOT NULL DEFAULT 0.40,
                outlier_min_history   INTEGER NOT NULL DEFAULT 3,
                enabled               INTEGER NOT NULL DEFAULT 1,
                notes                 TEXT,
                updated_at            TEXT DEFAULT (datetime('now'))
            );

            CREATE INDEX IF NOT EXISTS idx_mr_metric ON metric_rules(metric);
        """)
        self._seed_metric_rules(conn)

    def _migrate_v12(self, conn: sqlite3.Connection) -> None:
        """Schema migration from version 11 to version 12.

        Adds scraper_discovery_candidates for agent-proposed source discovery.
        """
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS scraper_discovery_candidates (
                id              INTEGER PRIMARY KEY AUTOINCREMENT,
                ticker          TEXT NOT NULL REFERENCES companies(ticker),
                source_type     TEXT NOT NULL,
                url             TEXT NOT NULL,
                pr_start_year   INTEGER,
                confidence      REAL,
                rationale       TEXT,
                proposed_by     TEXT NOT NULL DEFAULT 'agent',
                proposed_at     TEXT DEFAULT (datetime('now')),
                last_checked    TEXT,
                http_status     INTEGER,
                probe_status    TEXT,
                evidence_title  TEXT,
                evidence_date   TEXT,
                verified        INTEGER NOT NULL DEFAULT 0,
                UNIQUE(ticker, source_type, url)
            );

            CREATE INDEX IF NOT EXISTS idx_sdc_ticker
                ON scraper_discovery_candidates(ticker);

            CREATE INDEX IF NOT EXISTS idx_sdc_verified
                ON scraper_discovery_candidates(verified);
        """)

    def _migrate_v13(self, conn: sqlite3.Connection) -> None:
        """Schema migration from version 12 to version 13.

        Adds explicit aggregator placeholders on companies.
        """
        existing = {row[1] for row in conn.execute("PRAGMA table_info(companies)").fetchall()}
        for col in ('prnewswire_url', 'globenewswire_url'):
            if col not in existing:
                conn.execute(f"ALTER TABLE companies ADD COLUMN {col} TEXT")

    def _migrate_v14(self, conn: sqlite3.Connection) -> None:
        """Schema migration from version 13 to version 14.

        Adds run/event tracking for overnight pipeline orchestration.
        """
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS pipeline_runs (
                id           INTEGER PRIMARY KEY AUTOINCREMENT,
                started_at   TEXT NOT NULL DEFAULT (datetime('now')),
                ended_at     TEXT,
                status       TEXT NOT NULL DEFAULT 'queued',
                triggered_by TEXT,
                scope_json   TEXT,
                config_json  TEXT,
                summary_json TEXT,
                error        TEXT
            );

            CREATE TABLE IF NOT EXISTS pipeline_run_events (
                id           INTEGER PRIMARY KEY AUTOINCREMENT,
                run_id       INTEGER NOT NULL REFERENCES pipeline_runs(id) ON DELETE CASCADE,
                ts           TEXT NOT NULL DEFAULT (datetime('now')),
                stage        TEXT NOT NULL,
                event        TEXT NOT NULL,
                ticker       TEXT,
                level        TEXT NOT NULL DEFAULT 'INFO',
                details_json TEXT
            );

            CREATE TABLE IF NOT EXISTS pipeline_run_tickers (
                id           INTEGER PRIMARY KEY AUTOINCREMENT,
                run_id       INTEGER NOT NULL REFERENCES pipeline_runs(id) ON DELETE CASCADE,
                ticker       TEXT NOT NULL,
                targeted     INTEGER NOT NULL DEFAULT 1,
                probed       INTEGER NOT NULL DEFAULT 0,
                mode_applied INTEGER NOT NULL DEFAULT 0,
                scraped      INTEGER NOT NULL DEFAULT 0,
                ingested     INTEGER NOT NULL DEFAULT 0,
                extracted    INTEGER NOT NULL DEFAULT 0,
                failed_reason TEXT,
                UNIQUE(run_id, ticker)
            );

            CREATE INDEX IF NOT EXISTS idx_pipeline_runs_status
                ON pipeline_runs(status);

            CREATE INDEX IF NOT EXISTS idx_pipeline_run_events_run
                ON pipeline_run_events(run_id, id);

            CREATE INDEX IF NOT EXISTS idx_pipeline_run_tickers_run
                ON pipeline_run_tickers(run_id, ticker);
        """)


    def _migrate_v15(self, conn: sqlite3.Connection) -> None:
        """Schema v15: accession dedup, extraction backlog, provenance, source normalization.

        Reports additions:
          accession_number, source_url_hash (dedup)
          extraction_status, extraction_error, extraction_attempts (backlog)
          source_channel, form_type (normalization)
          amends_accession_number (EDGAR amendment tracking)

        Data points additions:
          run_id, model_name, extractor_version, prompt_version (provenance)
          chunk_id (links back to document_chunks)

        New table: qc_snapshots
        """
        existing_r = {row[1] for row in conn.execute("PRAGMA table_info(reports)").fetchall()}
        for col, typedef in [
            ('accession_number',        'TEXT'),
            ('source_url_hash',         'TEXT'),
            ('source_channel',          'TEXT'),
            ('form_type',               'TEXT'),
            ('extraction_status',       "TEXT NOT NULL DEFAULT 'pending'"),
            ('extraction_error',        'TEXT'),
            ('extraction_attempts',     'INTEGER NOT NULL DEFAULT 0'),
            ('amends_accession_number', 'TEXT'),
        ]:
            if col not in existing_r:
                conn.execute(f"ALTER TABLE reports ADD COLUMN {col} {typedef}")

        conn.execute(
            """CREATE UNIQUE INDEX IF NOT EXISTS idx_reports_accession
               ON reports(accession_number) WHERE accession_number IS NOT NULL"""
        )

        existing_dp = {row[1] for row in conn.execute("PRAGMA table_info(data_points)").fetchall()}
        for col, typedef in [
            ('run_id',           'TEXT'),
            ('model_name',       'TEXT'),
            ('extractor_version', 'TEXT'),
            ('prompt_version',   'TEXT'),
            ('chunk_id',         'INTEGER'),
        ]:
            if col not in existing_dp:
                conn.execute(f"ALTER TABLE data_points ADD COLUMN {col} {typedef}")

        existing_rq = {row[1] for row in conn.execute("PRAGMA table_info(review_queue)").fetchall()}
        if 'report_id' not in existing_rq:
            conn.execute("ALTER TABLE review_queue ADD COLUMN report_id INTEGER REFERENCES reports(id)")

        conn.execute("""
            CREATE TABLE IF NOT EXISTS qc_snapshots (
                id           INTEGER PRIMARY KEY AUTOINCREMENT,
                snapshot_at  TEXT NOT NULL DEFAULT (datetime('now')),
                ticker       TEXT,
                summary_json TEXT
            )
        """)

    def _migrate_v16(self, conn: sqlite3.Connection) -> None:
        """Schema migration from version 15 to version 16.

        Adds fetch provenance and content near-dedup columns to reports:
          - fetch_strategy: how the document was fetched (e.g. rss, template, index)
          - render_mode: rendering approach used (e.g. requests, playwright)
          - fetch_timing_ms: time taken to fetch the document in milliseconds
          - content_simhash: 64-bit simhash fingerprint for near-duplicate detection
        """
        existing = {row[1] for row in conn.execute("PRAGMA table_info(reports)").fetchall()}
        for col, typedef in [
            ('fetch_strategy',  'TEXT'),
            ('render_mode',     'TEXT'),
            ('fetch_timing_ms', 'INTEGER'),
            ('content_simhash', 'INTEGER'),
        ]:
            if col not in existing:
                conn.execute(f"ALTER TABLE reports ADD COLUMN {col} {typedef}")

    def _migrate_v17(self, conn: sqlite3.Connection) -> None:
        """Schema migration from version 16 to version 17.

        Adds EDGAR filing regime and fiscal year config to companies:
          - filing_regime: 'domestic' | 'canadian' | 'foreign'
          - fiscal_year_end_month: 1-12 (default 12 = December)
        """
        existing = {row[1] for row in conn.execute("PRAGMA table_info(companies)").fetchall()}
        for col, typedef in [
            ('filing_regime',         "TEXT NOT NULL DEFAULT 'domestic'"),
            ('fiscal_year_end_month', 'INTEGER NOT NULL DEFAULT 12'),
        ]:
            if col not in existing:
                conn.execute(f"ALTER TABLE companies ADD COLUMN {col} {typedef}")
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_reports_url_hash "
            "ON reports(source_url_hash) WHERE source_url_hash IS NOT NULL"
        )

    def _migrate_v18(self, conn: sqlite3.Connection) -> None:
        """Schema migration from version 17 to version 18.

        Upgrades the non-unique idx_reports_url_hash to a UNIQUE partial index
        on (ticker, source_url_hash) WHERE source_url_hash IS NOT NULL.

        Before creating the index, deduplicates any existing rows that share
        the same (ticker, source_url_hash) by keeping the highest-id row and
        deleting all dependent data for the discarded duplicates.
        """
        # Deduplicate existing rows grouped by (ticker, source_url_hash)
        dup_rows = conn.execute(
            """SELECT ticker, source_url_hash, MAX(id) AS keep_id
               FROM reports
               WHERE source_url_hash IS NOT NULL
               GROUP BY ticker, source_url_hash
               HAVING COUNT(*) > 1"""
        ).fetchall()
        for ticker, url_hash, keep_id in dup_rows:
            victims = conn.execute(
                """SELECT id FROM reports
                   WHERE ticker=? AND source_url_hash=? AND id != ?""",
                (ticker, url_hash, keep_id),
            ).fetchall()
            for (victim_id,) in victims:
                conn.execute("DELETE FROM review_queue WHERE report_id=?", (victim_id,))
                conn.execute("DELETE FROM raw_extractions WHERE report_id=?", (victim_id,))
                conn.execute("DELETE FROM data_points WHERE report_id=?", (victim_id,))
                conn.execute("DELETE FROM document_chunks WHERE report_id=?", (victim_id,))
                conn.execute("UPDATE asset_manifest SET report_id=NULL WHERE report_id=?", (victim_id,))
                conn.execute("DELETE FROM reports WHERE id=?", (victim_id,))

        # Drop the old non-unique index (may have been created under different name or scope)
        conn.execute("DROP INDEX IF EXISTS idx_reports_url_hash")

        # Create a UNIQUE partial index scoped to (ticker, source_url_hash)
        conn.execute(
            "CREATE UNIQUE INDEX idx_reports_url_hash "
            "ON reports(ticker, source_url_hash) WHERE source_url_hash IS NOT NULL"
        )

    def _migrate_v19(self, conn: sqlite3.Connection) -> None:
        """Schema migration from version 18 to version 19.

        Backfill: ensures fetch-provenance columns exist on the reports table.
        These were introduced in _migrate_v16 but that migration was added to the
        codebase after some DBs had already advanced past user_version 16, leaving
        the columns absent. This migration is idempotent and safe to re-run.
        """
        existing = {row[1] for row in conn.execute("PRAGMA table_info(reports)").fetchall()}
        for col, typedef in [
            ('fetch_strategy',  'TEXT'),
            ('render_mode',     'TEXT'),
            ('fetch_timing_ms', 'INTEGER'),
            ('content_simhash', 'INTEGER'),
        ]:
            if col not in existing:
                conn.execute(f"ALTER TABLE reports ADD COLUMN {col} {typedef}")

    def _migrate_v20(self, conn: sqlite3.Connection) -> None:
        """Schema migration v19 → v20: crawl_observations table.

        Stores structured knowledge the LLM crawler learns about each company's
        IR sites (pagination patterns, URL structures, known dead ends, etc.).
        Observations survive across crawl sessions and are injected back into the
        prompt so the model doesn't re-discover site behaviour from scratch.
        """
        conn.execute("""
            CREATE TABLE IF NOT EXISTS crawl_observations (
                id         INTEGER PRIMARY KEY AUTOINCREMENT,
                ticker     TEXT    NOT NULL,
                key        TEXT    NOT NULL,
                value      TEXT    NOT NULL,
                created_at TEXT    NOT NULL DEFAULT (datetime('now')),
                updated_at TEXT    NOT NULL DEFAULT (datetime('now')),
                UNIQUE(ticker, key)
            )
        """)
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_crawl_obs_ticker ON crawl_observations(ticker)"
        )

    def upsert_crawl_observation(self, ticker: str, key: str, value: str) -> None:
        """Insert or update a crawl observation for a ticker.

        On conflict (same ticker+key) the value and updated_at are refreshed.
        """
        from datetime import datetime, timezone
        now = datetime.now(timezone.utc).isoformat()
        with self._get_connection() as conn:
            conn.execute(
                """INSERT INTO crawl_observations (ticker, key, value, created_at, updated_at)
                   VALUES (?, ?, ?, ?, ?)
                   ON CONFLICT(ticker, key) DO UPDATE SET
                       value      = excluded.value,
                       updated_at = excluded.updated_at""",
                (ticker.upper(), key, value, now, now),
            )

    def get_crawl_observations(self, ticker: str) -> list:
        """Return all crawl observations for a ticker, ordered by key."""
        with self._get_connection() as conn:
            rows = conn.execute(
                """SELECT key, value, updated_at
                   FROM crawl_observations
                   WHERE ticker = ?
                   ORDER BY key""",
                (ticker.upper(),),
            ).fetchall()
            return [dict(r) for r in rows]

    def _migrate_v21(self, conn: sqlite3.Connection) -> None:
        """Schema migration v20 → v21: add llm_summary column to reports.

        Stores the one-sentence summary returned by the LLM during batch
        extraction so documents are searchable without re-running the model.
        """
        existing = {row[1] for row in conn.execute("PRAGMA table_info(reports)").fetchall()}
        if 'llm_summary' not in existing:
            conn.execute("ALTER TABLE reports ADD COLUMN llm_summary TEXT")

    def update_report_summary(self, report_id: int, summary: str) -> None:
        """Write the LLM-generated summary for a report."""
        with self._get_connection() as conn:
            conn.execute(
                "UPDATE reports SET llm_summary = ? WHERE id = ?",
                (summary.strip()[:500] if summary else None, report_id),
            )

    def _seed_metric_rules(self, conn: sqlite3.Connection) -> None:
        """Seed metric_rules with config.py threshold values.

        Uses INSERT OR IGNORE so re-runs on an already-seeded DB are no-ops.
        """
        from config import METRIC_AGREEMENT_THRESHOLDS, OUTLIER_THRESHOLDS, OUTLIER_MIN_HISTORY
        all_metrics = set(METRIC_AGREEMENT_THRESHOLDS) | set(OUTLIER_THRESHOLDS)
        rows = []
        for m in sorted(all_metrics):
            rows.append((
                m,
                METRIC_AGREEMENT_THRESHOLDS.get(m, 0.02),
                OUTLIER_THRESHOLDS.get(m, 0.40),
                OUTLIER_MIN_HISTORY,
            ))
        conn.executemany(
            """INSERT OR IGNORE INTO metric_rules
               (metric, agreement_threshold, outlier_threshold, outlier_min_history)
               VALUES (?, ?, ?, ?)""",
            rows,
        )

    # ── Company CRUD ─────────────────────────────────────────────────────────

    def sync_companies_from_config(self, config_path: str) -> dict:
        """Upsert all companies from a companies.json config file into the DB.

        Config fields (name, URLs, scraper settings) are always updated from JSON.
        Operational fields (scraper_status, last_scrape_at, last_scrape_error,
        probe_completed_at, scraper_issues_log) are preserved from the existing row.

        Returns {'added': N, 'updated': N}.
        """
        with open(config_path) as f:
            companies = json.load(f)

        added = updated = 0
        with self._get_connection() as conn:
            for c in companies:
                ticker = c.get('ticker', '').strip().upper()
                if not ticker:
                    continue
                existing = conn.execute(
                    "SELECT ticker, scraper_mode FROM companies WHERE ticker = ?",
                    (ticker,),
                ).fetchone()

                canonical_mode = c.get('scraper_mode')
                legacy_mode = c.get('scrape_mode')

                if existing is None:
                    # New company — insert with all fields
                    conn.execute(
                        """INSERT INTO companies
                           (ticker, name, tier, ir_url, pr_base_url, cik, active,
                            rss_url, url_template, pr_start_year, skip_reason, sandbox_note,
                            scraper_mode, sector, scraper_issues_log, scraper_status,
                            prnewswire_url, globenewswire_url,
                            filing_regime, fiscal_year_end_month)
                           VALUES
                           (:ticker,:name,:tier,:ir_url,:pr_base_url,:cik,:active,
                            :rss_url,:url_template,:pr_start_year,:skip_reason,:sandbox_note,
                            :scraper_mode,:sector,'','never_run',
                            :prnewswire_url,:globenewswire_url,
                            :filing_regime,:fiscal_year_end_month)""",
                        {
                            'ticker':               ticker,
                            'name':                 c.get('name', ticker),
                            'tier':                 int(c.get('tier', 2)),
                            'ir_url':               c.get('ir_url') or '',
                            'pr_base_url':          c.get('pr_base_url'),
                            'cik':                  c.get('cik'),
                            'active':               1 if c.get('active', True) else 0,
                            'rss_url':              c.get('rss_url'),
                            'url_template':         c.get('url_template'),
                            'pr_start_year':        c.get('pr_start_year'),
                            'skip_reason':          c.get('skip_reason'),
                            'sandbox_note':         c.get('sandbox_note'),
                            'scraper_mode':         canonical_mode or legacy_mode or 'skip',
                            'sector':               c.get('sector', 'BTC-miners'),
                            'prnewswire_url':       c.get('prnewswire_url'),
                            'globenewswire_url':    c.get('globenewswire_url'),
                            'filing_regime':        c.get('filing_regime', 'domestic'),
                            'fiscal_year_end_month': int(c.get('fiscal_year_end_month', 12)),
                        },
                    )
                    added += 1
                else:
                    # Existing company — update config fields only, preserve operational state
                    conn.execute(
                        """UPDATE companies SET
                           name=:name, tier=:tier, ir_url=:ir_url, pr_base_url=:pr_base_url,
                           cik=:cik, active=:active,
                           rss_url=:rss_url, url_template=:url_template,
                           pr_start_year=:pr_start_year, skip_reason=:skip_reason,
                           sandbox_note=:sandbox_note,
                           scraper_mode=:scraper_mode, sector=:sector,
                           prnewswire_url=:prnewswire_url, globenewswire_url=:globenewswire_url,
                           filing_regime=:filing_regime,
                           fiscal_year_end_month=:fiscal_year_end_month
                           WHERE ticker=:ticker""",
                        {
                            'ticker':               ticker,
                            'name':                 c.get('name', ticker),
                            'tier':                 int(c.get('tier', 2)),
                            'ir_url':               c.get('ir_url') or '',
                            'pr_base_url':          c.get('pr_base_url'),
                            'cik':                  c.get('cik'),
                            'active':               1 if c.get('active', True) else 0,
                            'rss_url':              c.get('rss_url'),
                            'url_template':         c.get('url_template'),
                            'pr_start_year':        c.get('pr_start_year'),
                            'skip_reason':          c.get('skip_reason'),
                            'sandbox_note':         c.get('sandbox_note'),
                            # For existing rows, only canonical "scraper_mode" may overwrite.
                            # Legacy "scrape_mode" is treated as seed-only to avoid reverting
                            # analyst-updated modes from old config files.
                            'scraper_mode':         (canonical_mode if canonical_mode is not None else existing['scraper_mode']) or 'skip',
                            'sector':               c.get('sector', 'BTC-miners'),
                            'prnewswire_url':       c.get('prnewswire_url'),
                            'globenewswire_url':    c.get('globenewswire_url'),
                            'filing_regime':        c.get('filing_regime', 'domestic'),
                            'fiscal_year_end_month': int(c.get('fiscal_year_end_month', 12)),
                        },
                    )
                    updated += 1

        log.info("sync_companies_from_config: %d added, %d updated from %s", added, updated, config_path)
        return {'added': added, 'updated': updated}

    def insert_company(self, company: dict) -> None:
        with self._get_connection() as conn:
            conn.execute(
                """INSERT OR IGNORE INTO companies
                   (ticker, name, tier, ir_url, pr_base_url, cik, active)
                   VALUES (:ticker, :name, :tier, :ir_url, :pr_base_url, :cik, :active)""",
                company,
            )

    def get_company(self, ticker: str) -> Optional[dict]:
        with self._get_connection() as conn:
            row = conn.execute(
                "SELECT * FROM companies WHERE ticker = ?", (ticker,)
            ).fetchone()
            return dict(row) if row else None

    def get_companies(self, active_only: bool = True) -> list:
        with self._get_connection() as conn:
            if active_only:
                rows = conn.execute(
                    "SELECT * FROM companies WHERE active = 1 ORDER BY ticker"
                ).fetchall()
            else:
                rows = conn.execute(
                    "SELECT * FROM companies ORDER BY ticker"
                ).fetchall()
            return [dict(r) for r in rows]

    def seed_companies(self, companies_json_path: str) -> int:
        with open(companies_json_path) as f:
            companies = json.load(f)
        count = 0
        for c in companies:
            c.setdefault('active', 1)
            self.insert_company(c)
            count += 1
        log.info("Seeded %d companies from %s", count, companies_json_path)
        return count

    # ── Report CRUD ──────────────────────────────────────────────────────────

    def insert_report(self, report: dict) -> int:
        source_url = report.get('source_url')
        url_hash = hashlib.sha256(source_url.encode()).hexdigest() if source_url else None
        with self._get_connection() as conn:
            cursor = conn.execute(
                """INSERT INTO reports
                   (ticker, report_date, published_date, source_type, source_url, raw_text,
                    parsed_at, covering_period,
                    accession_number, source_url_hash, source_channel, form_type,
                    amends_accession_number,
                    fetch_strategy, render_mode, fetch_timing_ms, content_simhash)
                   VALUES (:ticker, :report_date, :published_date, :source_type,
                           :source_url, :raw_text, :parsed_at, :covering_period,
                           :accession_number, :source_url_hash, :source_channel, :form_type,
                           :amends_accession_number,
                           :fetch_strategy, :render_mode, :fetch_timing_ms, :content_simhash)""",
                {
                    **report,
                    'covering_period':         report.get('covering_period'),
                    'accession_number':        report.get('accession_number'),
                    'source_url_hash':         report.get('source_url_hash', url_hash),
                    'source_channel':          report.get('source_channel'),
                    'form_type':               report.get('form_type'),
                    'amends_accession_number': report.get('amends_accession_number'),
                    'fetch_strategy':          report.get('fetch_strategy'),
                    'render_mode':             report.get('render_mode'),
                    'fetch_timing_ms':         report.get('fetch_timing_ms'),
                    'content_simhash':         report.get('content_simhash'),
                },
            )
            return cursor.lastrowid

    def find_near_duplicates(self, simhash: int, ticker: str, threshold: int = 3) -> list:
        """Return reports for this ticker whose content_simhash is within hamming threshold.

        Fetches all non-null simhash rows for the ticker and filters in Python,
        since SQLite has no built-in hamming distance function.
        """
        with self._get_connection() as conn:
            rows = conn.execute(
                "SELECT id, ticker, source_url, content_simhash FROM reports "
                "WHERE ticker=? AND content_simhash IS NOT NULL",
                (ticker,),
            ).fetchall()
        return [
            dict(r) for r in rows
            if bin(r['content_simhash'] ^ simhash).count('1') <= threshold
        ]

    def get_reports_with_text(self) -> list:
        """Return all reports that have non-empty raw_text, for pattern application."""
        with self._get_connection() as conn:
            rows = conn.execute(
                """SELECT id, ticker, report_date, source_type
                   FROM reports
                   WHERE raw_text IS NOT NULL AND raw_text != ''
                   ORDER BY ticker, report_date"""
            ).fetchall()
            return [dict(r) for r in rows]

    def get_report_raw_text(self, report_id: int) -> Optional[str]:
        """Return only the raw_text for a report."""
        with self._get_connection() as conn:
            row = conn.execute(
                "SELECT raw_text FROM reports WHERE id = ?", (report_id,)
            ).fetchone()
            return row[0] if row else None

    def data_point_exists(self, ticker: str, period: str, metric: str) -> bool:
        """Return True if a data_point already exists for the given ticker/period/metric."""
        with self._get_connection() as conn:
            row = conn.execute(
                "SELECT 1 FROM data_points WHERE ticker=? AND period=? AND metric=?",
                (ticker, period, metric),
            ).fetchone()
            return row is not None

    def get_report(self, report_id: int) -> Optional[dict]:
        """Return a single report row by id, or None if not found."""
        with self._get_connection() as conn:
            row = conn.execute(
                "SELECT * FROM reports WHERE id = ?", (report_id,)
            ).fetchone()
            return dict(row) if row else None

    def mark_report_extraction_running(self, report_id: int) -> None:
        """Set extraction_status='running'. Called at pipeline entry to prevent double-processing."""
        with self._get_connection() as conn:
            conn.execute(
                "UPDATE reports SET extraction_status = 'running' WHERE id = ?",
                (report_id,),
            )

    def mark_report_extracted(self, report_id: int) -> None:
        """Set extraction_status='done', increment extraction_attempts, set extracted_at."""
        with self._get_connection() as conn:
            conn.execute(
                """UPDATE reports
                   SET extracted_at = ?,
                       extraction_status = 'done',
                       extraction_attempts = extraction_attempts + 1
                   WHERE id = ?""",
                (datetime.now(timezone.utc).isoformat(), report_id),
            )

    def mark_report_extraction_failed(self, report_id: int, error: str) -> None:
        """Record extraction failure, increment attempts. Promotes to dead_letter at MAX."""
        from config import MAX_EXTRACTION_ATTEMPTS
        with self._get_connection() as conn:
            conn.execute(
                """UPDATE reports
                   SET extraction_error = ?,
                       extraction_attempts = extraction_attempts + 1,
                       extraction_status = CASE
                           WHEN extraction_attempts + 1 >= ? THEN 'dead_letter'
                           ELSE 'failed'
                       END
                   WHERE id = ?""",
                (str(error)[:500], MAX_EXTRACTION_ATTEMPTS, report_id),
            )

    def reset_report_to_pending(self, report_id: int) -> None:
        """Reset extraction_status from 'running' back to 'pending' for transient failures.

        Called when a transient failure (e.g. LLM temporarily unavailable) occurs mid-pipeline
        so the report is picked up again in the same process rather than waiting for next boot.
        """
        with self._get_connection() as conn:
            conn.execute(
                "UPDATE reports SET extraction_status = 'pending' WHERE id = ?",
                (report_id,),
            )

    def report_exists_by_accession(self, accession_number: str) -> bool:
        """Return True if a report with this accession_number already exists."""
        with self._get_connection() as conn:
            row = conn.execute(
                "SELECT 1 FROM reports WHERE accession_number = ?",
                (accession_number,),
            ).fetchone()
        return row is not None

    def get_reports_by_channel(self, ticker: str, channel: str) -> list:
        """Return reports for a ticker filtered by source_channel."""
        with self._get_connection() as conn:
            rows = conn.execute(
                "SELECT * FROM reports WHERE ticker = ? AND source_channel = ? ORDER BY report_date",
                (ticker, channel),
            ).fetchall()
        return [dict(r) for r in rows]

    def get_unextracted_reports(
        self,
        ticker: Optional[str] = None,
        source_type: Optional[str] = None,
    ) -> list:
        """Return reports eligible for extraction.

        Eligible means: raw_text present AND extraction_status IN ('pending','failed')
        AND extraction_attempts < MAX_EXTRACTION_ATTEMPTS (not dead_letter).

        Optionally filter by ticker and/or source_type (e.g. 'edgar_10q').
        """
        from config import MAX_EXTRACTION_ATTEMPTS
        clauses = [
            "raw_text IS NOT NULL",
            "raw_text != ''",
            "extraction_status IN ('pending', 'failed')",
            "extraction_attempts < ?",
        ]
        params: list = [MAX_EXTRACTION_ATTEMPTS]
        if ticker:
            clauses.append("ticker = ?")
            params.append(ticker)
        if source_type:
            clauses.append("source_type = ?")
            params.append(source_type)
        where = "WHERE " + " AND ".join(clauses)
        with self._get_connection() as conn:
            rows = conn.execute(
                f"SELECT * FROM reports {where} ORDER BY ticker, report_date",
                params,
            ).fetchall()
            return [dict(r) for r in rows]

    def get_quarterly_data_point(
        self, ticker: str, covering_period: str, metric: str
    ) -> Optional[dict]:
        """Return a data_point with source_period_type IN ('quarterly','annual') for the
        given covering_period (e.g. '2025-Q1' or '2024-FY'), or None if not found."""
        with self._get_connection() as conn:
            row = conn.execute(
                """SELECT * FROM data_points
                   WHERE ticker = ? AND period = ? AND metric = ?
                   AND source_period_type IN ('quarterly', 'annual')
                   LIMIT 1""",
                (ticker, covering_period, metric),
            ).fetchone()
            return dict(row) if row else None

    def upsert_data_point_quarterly(self, dp: dict) -> int:
        """Insert or replace a quarterly/annual data_point.

        dp must include source_period_type ('quarterly'|'annual'),
        covering_period (e.g. '2025-Q1'), and covering_report_id.
        The UNIQUE key is (ticker, period, metric) — quarterly rows use
        covering_period as the period value (e.g. '2025-Q1').
        """
        with self._get_connection() as conn:
            cursor = conn.execute(
                """INSERT OR REPLACE INTO data_points
                   (report_id, ticker, period, metric, value, unit, confidence,
                    extraction_method, source_snippet,
                    source_period_type, covering_report_id, covering_period)
                   VALUES (:report_id, :ticker, :period, :metric, :value, :unit,
                           :confidence, :extraction_method, :source_snippet,
                           :source_period_type, :covering_report_id, :covering_period)""",
                {
                    'report_id':         dp.get('report_id'),
                    'ticker':            dp['ticker'],
                    'period':            dp['period'],
                    'metric':            dp['metric'],
                    'value':             dp['value'],
                    'unit':              dp.get('unit', ''),
                    'confidence':        dp['confidence'],
                    'extraction_method': dp.get('extraction_method'),
                    'source_snippet':    dp.get('source_snippet'),
                    'source_period_type': dp.get('source_period_type', 'quarterly'),
                    'covering_report_id': dp.get('covering_report_id'),
                    'covering_period':   dp.get('covering_period'),
                },
            )
            return cursor.lastrowid

    def get_reports_by_source_type(self, ticker: str, source_type: str) -> list:
        """Return all reports for a ticker with the given source_type, ordered by report_date."""
        with self._get_connection() as conn:
            rows = conn.execute(
                """SELECT * FROM reports
                   WHERE ticker = ? AND source_type = ?
                   ORDER BY report_date ASC""",
                (ticker, source_type),
            ).fetchall()
            return [dict(r) for r in rows]

    def get_indexed_urls_for_ticker(self, ticker: str) -> list:
        """Return compact doc index for a ticker: source_url, covering_period, source_type.

        Used by the LLM crawler to inject an 'already indexed' block into the prompt
        so the model does not re-fetch or re-store documents already in the DB.
        """
        with self._get_connection() as conn:
            rows = conn.execute(
                """SELECT source_url, covering_period, source_type
                   FROM reports
                   WHERE ticker = ? AND source_url IS NOT NULL
                   ORDER BY report_date ASC""",
                (ticker.upper(),),
            ).fetchall()
            return [dict(r) for r in rows]

    def update_company_last_edgar(self, ticker: str) -> None:
        """Set last_edgar_at to current UTC time for a company."""
        with self._get_connection() as conn:
            conn.execute(
                "UPDATE companies SET last_edgar_at = ? WHERE ticker = ?",
                (datetime.now(timezone.utc).isoformat(), ticker),
            )

    def get_data_point_value(self, ticker: str, period: str, metric: str) -> Optional[float]:
        """Return the value of a data_point for the given ticker/period/metric, or None."""
        with self._get_connection() as conn:
            row = conn.execute(
                "SELECT value FROM data_points WHERE ticker=? AND period=? AND metric=?",
                (ticker, period, metric),
            ).fetchone()
            return float(row[0]) if row else None

    def get_data_point_by_key(self, ticker: str, period: str, metric: str) -> Optional[dict]:
        """Return the full data_point row for the given ticker/period/metric, or None."""
        with self._get_connection() as conn:
            row = conn.execute(
                "SELECT * FROM data_points WHERE ticker=? AND period=? AND metric=?",
                (ticker, period, metric),
            ).fetchone()
            return dict(row) if row else None

    def get_all_quarterly_data_points(self, ticker: Optional[str] = None) -> list:
        """Return all data_points with source_period_type != 'monthly'."""
        clauses = ["source_period_type != 'monthly'"]
        params: list = []
        if ticker:
            clauses.append("ticker = ?")
            params.append(ticker)
        where = "WHERE " + " AND ".join(clauses)
        with self._get_connection() as conn:
            rows = conn.execute(
                f"SELECT * FROM data_points {where} ORDER BY ticker, period, metric",
                params,
            ).fetchall()
            return [dict(r) for r in rows]

    def get_regime_cadence_for_period(self, ticker: str, period: str) -> str:
        """Return the cadence ('monthly' or 'quarterly') for a ticker at a given period.

        Looks up regime_config for the ticker where start_date <= period and
        (end_date IS NULL or end_date >= period). Returns 'monthly' if no match.
        """
        with self._get_connection() as conn:
            row = conn.execute(
                """SELECT cadence FROM regime_config
                   WHERE ticker = ?
                   AND start_date <= ?
                   AND (end_date IS NULL OR end_date >= ?)
                   ORDER BY start_date DESC
                   LIMIT 1""",
                (ticker, period, period),
            ).fetchone()
            return row[0] if row else 'monthly'

    def get_all_reports_for_extraction(self, ticker: Optional[str] = None) -> list:
        """Return all reports with raw_text regardless of extracted_at (for --force re-extraction)."""
        clauses = ["raw_text IS NOT NULL", "raw_text != ''"]
        params: list = []
        if ticker:
            clauses.append("ticker = ?")
            params.append(ticker)
        where = "WHERE " + " AND ".join(clauses)
        with self._get_connection() as conn:
            rows = conn.execute(
                f"SELECT * FROM reports {where} ORDER BY ticker, report_date",
                params,
            ).fetchall()
            return [dict(r) for r in rows]

    def latest_ir_period(self, ticker: str) -> Optional[str]:
        """
        Return the YYYY-MM-DD of the most recently ingested ir_press_release report
        for this ticker, or None if no IR reports exist yet.
        Used by the template scraper to fast-forward past already-covered months.
        """
        with self._get_connection() as conn:
            row = conn.execute(
                """SELECT MAX(report_date) FROM reports
                   WHERE ticker=? AND source_type='ir_press_release'""",
                (ticker,),
            ).fetchone()
            return row[0] if row and row[0] else None

    def report_exists(self, ticker: str, report_date: str, source_type: str) -> bool:
        with self._get_connection() as conn:
            row = conn.execute(
                "SELECT 1 FROM reports WHERE ticker=? AND report_date=? AND source_type=?",
                (ticker, report_date, source_type),
            ).fetchone()
            return row is not None

    def report_exists_by_url(self, ticker: str, source_url: str) -> bool:
        """Return True if a report with this (ticker, source_url) already exists."""
        with self._get_connection() as conn:
            row = conn.execute(
                "SELECT 1 FROM reports WHERE ticker=? AND source_url=?",
                (ticker, source_url),
            ).fetchone()
            return row is not None

    def report_exists_by_url_hash(self, url_hash: str, ticker: str = None) -> bool:
        """Return True if a report with this source_url_hash already exists.

        When ticker is provided, the check is scoped to that ticker only,
        preventing cross-ticker URL collision from suppressing legitimate ingestion.
        """
        with self._get_connection() as conn:
            if ticker:
                row = conn.execute(
                    "SELECT 1 FROM reports WHERE source_url_hash=? AND ticker=?",
                    (url_hash, ticker),
                ).fetchone()
            else:
                row = conn.execute(
                    "SELECT 1 FROM reports WHERE source_url_hash=?",
                    (url_hash,),
                ).fetchone()
            return row is not None

    def update_report_raw_text(
        self, report_id: int, raw_text: str, source_url: str = None
    ) -> None:
        """Update raw_text (and optionally source_url) for an existing report."""
        with self._get_connection() as conn:
            if source_url is not None:
                conn.execute(
                    "UPDATE reports SET raw_text=?, source_url=?, parsed_at=? WHERE id=?",
                    (raw_text, source_url, datetime.now(timezone.utc).isoformat(), report_id),
                )
            else:
                conn.execute(
                    "UPDATE reports SET raw_text=?, parsed_at=? WHERE id=?",
                    (raw_text, datetime.now(timezone.utc).isoformat(), report_id),
                )

    def get_stale_8k_reports(self, ticker: str = None) -> list:
        """Return edgar_8k reports where raw_text is the EDGAR index page boilerplate."""
        with self._get_connection() as conn:
            if ticker:
                rows = conn.execute(
                    "SELECT id, ticker, report_date, source_url FROM reports "
                    "WHERE source_type='edgar_8k' AND ticker=? "
                    "AND raw_text LIKE 'EDGAR Filing Documents%'",
                    (ticker,),
                ).fetchall()
            else:
                rows = conn.execute(
                    "SELECT id, ticker, report_date, source_url FROM reports "
                    "WHERE source_type='edgar_8k' "
                    "AND raw_text LIKE 'EDGAR Filing Documents%'"
                ).fetchall()
            return [dict(r) for r in rows]

    def delete_report(self, ticker: str, report_date: str, source_type: str) -> int:
        """
        Delete a report and all dependent rows that reference it.
        Returns the number of reports deleted (0 or 1).
        Used by force-reingest to clear stale records before re-processing.
        """
        with self._get_connection() as conn:
            row = conn.execute(
                "SELECT id FROM reports WHERE ticker=? AND report_date=? AND source_type=?",
                (ticker, report_date, source_type),
            ).fetchone()
            if row is None:
                return 0
            report_id = row[0]
            # Primary scoped delete: remove review_queue rows that reference this report directly.
            conn.execute(
                "DELETE FROM review_queue WHERE report_id=?", (report_id,)
            )
            # Fallback for pre-migration orphans with NULL report_id: clean up by ticker+period
            # but only where report_id IS NULL to avoid touching other reports' rows.
            period_rows = conn.execute(
                "SELECT DISTINCT period FROM data_points WHERE report_id=?", (report_id,)
            ).fetchall()
            periods = [r[0] for r in period_rows] or [report_date]
            for p in periods:
                conn.execute(
                    "DELETE FROM review_queue WHERE report_id IS NULL AND ticker=? AND period=?",
                    (ticker, p),
                )
            conn.execute("UPDATE asset_manifest SET report_id=NULL WHERE report_id=?", (report_id,))
            conn.execute("DELETE FROM raw_extractions WHERE report_id=?", (report_id,))
            conn.execute("DELETE FROM data_points WHERE report_id=?", (report_id,))
            conn.execute("DELETE FROM document_chunks WHERE report_id=?", (report_id,))
            conn.execute("DELETE FROM reports WHERE id=?", (report_id,))
            return 1

    # ── DataPoint CRUD ───────────────────────────────────────────────────────

    def insert_data_point(self, dp: dict) -> int:
        with self._get_connection() as conn:
            cursor = conn.execute(
                """INSERT OR REPLACE INTO data_points
                   (report_id, ticker, period, metric, value, unit, confidence,
                    extraction_method, source_snippet,
                    run_id, model_name, extractor_version, prompt_version, chunk_id)
                   VALUES (:report_id, :ticker, :period, :metric, :value, :unit,
                           :confidence, :extraction_method, :source_snippet,
                           :run_id, :model_name, :extractor_version, :prompt_version, :chunk_id)""",
                {
                    **dp,
                    'run_id':            dp.get('run_id'),
                    'model_name':        dp.get('model_name'),
                    'extractor_version': dp.get('extractor_version'),
                    'prompt_version':    dp.get('prompt_version'),
                    'chunk_id':          dp.get('chunk_id'),
                },
            )
            return cursor.lastrowid

    def query_data_points(
        self,
        ticker: Optional[str] = None,
        tickers: Optional[list] = None,
        metric: Optional[str] = None,
        from_period: Optional[str] = None,
        to_period: Optional[str] = None,
        min_confidence: Optional[float] = None,
        limit: int = 1000,
        offset: int = 0,
    ) -> list:
        clauses = []
        params = []
        # Support both single ticker (legacy) and list of tickers
        _tickers = tickers if tickers else ([ticker] if ticker else None)
        if _tickers:
            if len(_tickers) == 1:
                clauses.append("ticker = ?")
                params.append(_tickers[0])
            else:
                placeholders = ','.join('?' * len(_tickers))
                clauses.append(f"ticker IN ({placeholders})")
                params.extend(_tickers)
        if metric:
            clauses.append("metric = ?")
            params.append(metric)
        if from_period:
            clauses.append("period >= ?")
            params.append(from_period)
        if to_period:
            clauses.append("period <= ?")
            params.append(to_period)
        if min_confidence is not None:
            clauses.append("confidence >= ?")
            params.append(min_confidence)
        where = ("WHERE " + " AND ".join(clauses)) if clauses else ""
        params.extend([limit, offset])
        with self._get_connection() as conn:
            rows = conn.execute(
                f"SELECT * FROM data_points {where} ORDER BY ticker, period, metric LIMIT ? OFFSET ?",
                params,
            ).fetchall()
            return [dict(r) for r in rows]

    def count_data_points(self) -> int:
        with self._get_connection() as conn:
            return conn.execute("SELECT COUNT(*) FROM data_points").fetchone()[0]

    def purge_data_points(self, ticker: Optional[str] = None) -> int:
        """Delete all data_points (or for one ticker). Returns row count deleted.

        Also resets reports.extracted_at = NULL for the affected ticker(s) so
        reports can be re-extracted without the --force flag.
        """
        with self._get_connection() as conn:
            if ticker:
                count = conn.execute(
                    "SELECT COUNT(*) FROM data_points WHERE ticker = ?", (ticker,)
                ).fetchone()[0]
                conn.execute("DELETE FROM data_points WHERE ticker = ?", (ticker,))
                conn.execute(
                    "UPDATE reports SET extracted_at = NULL WHERE ticker = ?", (ticker,)
                )
            else:
                count = conn.execute("SELECT COUNT(*) FROM data_points").fetchone()[0]
                conn.execute("DELETE FROM data_points")
                conn.execute("UPDATE reports SET extracted_at = NULL")
        log.info("purge_data_points: deleted %d rows (ticker=%s)", count, ticker or 'ALL')
        return count

    def purge_review_queue(self, ticker: Optional[str] = None) -> int:
        """Delete all review_queue rows (or for one ticker). Returns row count deleted."""
        with self._get_connection() as conn:
            if ticker:
                count = conn.execute(
                    "SELECT COUNT(*) FROM review_queue WHERE ticker = ?", (ticker,)
                ).fetchone()[0]
                conn.execute("DELETE FROM review_queue WHERE ticker = ?", (ticker,))
            else:
                count = conn.execute("SELECT COUNT(*) FROM review_queue").fetchone()[0]
                conn.execute("DELETE FROM review_queue")
        log.info("purge_review_queue: deleted %d rows (ticker=%s)", count, ticker or 'ALL')
        return count

    def _create_purge_archive_batch(self, mode: str, ticker_scope: Optional[str], reason: Optional[str]) -> int:
        with self._get_archive_connection() as conn:
            cur = conn.execute(
                """INSERT INTO purge_batches (mode, ticker_scope, reason, source_db_path)
                   VALUES (?, ?, ?, ?)""",
                (mode, ticker_scope, reason, self.db_path),
            )
            return int(cur.lastrowid)

    def _archive_table_rows(
        self,
        archive_conn: sqlite3.Connection,
        batch_id: int,
        data_conn: sqlite3.Connection,
        table: str,
        where: str = '',
        params: tuple = (),
    ) -> int:
        sql = f"SELECT * FROM {table}"
        if where:
            sql += f" WHERE {where}"
        rows = data_conn.execute(sql, params).fetchall()
        if not rows:
            return 0
        archive_conn.executemany(
            "INSERT INTO purge_rows (batch_id, table_name, row_data) VALUES (?, ?, ?)",
            [(batch_id, table, json.dumps(dict(r), default=str)) for r in rows],
        )
        return len(rows)

    def purge_all(
        self,
        ticker: Optional[str] = None,
        purge_mode: str = 'hard_delete',
        reason: Optional[str] = None,
        suppress_auto_sync: bool = False,
    ) -> dict:
        """Delete operational data with explicit purge semantics.

        FK-safe deletion order (enforced because PRAGMA foreign_keys = ON):
          review_queue      — child of data_points (must precede data_points)
          raw_extractions   — child of reports
          data_points       — child of reports AND document_chunks
                              (must precede document_chunks)
          document_chunks   — child of reports (must follow data_points)
          asset_manifest    — child of reports and companies
          reports           — child of companies
          scrape_queue      — child of companies
          btc_loans, facilities, source_audit, llm_benchmark_runs — no FK deps

        Modes:
          - reset: delete data tables only; keep companies + regime_config.
          - archive: same as reset, but first writes deleted rows to purge_archive.db.
          - hard_delete: delete data tables; full-scope also deletes companies + regime_config.

        Args:
            ticker: if given, scope the purge to that ticker only.
            purge_mode: one of reset|archive|hard_delete.
            reason: optional operator-provided reason for archival metadata.
            suppress_auto_sync: if True on full hard_delete, writes
                config_settings.auto_sync_companies_on_startup='0'.

        Returns:
            dict with row counts deleted per table and optional archive_batch_id.
        """
        valid_modes = {'reset', 'archive', 'hard_delete'}
        mode = str(purge_mode or '').strip().lower()
        if mode not in valid_modes:
            raise ValueError(f"purge_mode must be one of {sorted(valid_modes)}")

        counts: dict = {}
        archive_batch_id = None
        archive_conn = None
        if mode == 'archive':
            archive_batch_id = self._create_purge_archive_batch(mode=mode, ticker_scope=ticker or 'ALL', reason=reason)
            archive_conn = self._get_archive_connection()

        try:
            with self._get_connection() as conn:
                if archive_conn is not None:
                    archive_conn.execute('BEGIN')
                def _archive(table: str, where: str = '', params: tuple = ()) -> int:
                    if archive_conn is None or archive_batch_id is None:
                        return 0
                    return self._archive_table_rows(
                        archive_conn=archive_conn,
                        batch_id=archive_batch_id,
                        data_conn=conn,
                        table=table,
                        where=where,
                        params=params,
                    )

                def _del(table: str, where: str = '', params: tuple = ()) -> int:
                    sql = f"SELECT COUNT(*) FROM {table}"
                    if where:
                        sql += f" WHERE {where}"
                    n = conn.execute(sql, params).fetchone()[0]
                    sql = f"DELETE FROM {table}"
                    if where:
                        sql += f" WHERE {where}"
                    conn.execute(sql, params)
                    return n

                if ticker:
                    t = ticker
                    # review_queue before data_points (FK: review_queue.data_point_id → data_points)
                    _archive('review_queue', 'ticker = ?', (t,))
                    counts['review_queue'] = _del('review_queue', 'ticker = ?', (t,))
                    # raw_extractions before reports
                    _archive('raw_extractions', 'ticker = ?', (t,))
                    counts['raw_extractions'] = _del('raw_extractions', 'ticker = ?', (t,))
                    # data_points before document_chunks (FK: data_points.chunk_id → document_chunks)
                    _archive('data_points', 'ticker = ?', (t,))
                    counts['data_points'] = _del('data_points', 'ticker = ?', (t,))
                    # document_chunks after data_points, before reports
                    _archive(
                        'document_chunks',
                        "report_id IN (SELECT id FROM reports WHERE ticker = ?)",
                        (t,),
                    )
                    counts['document_chunks'] = conn.execute(
                        "SELECT COUNT(*) FROM document_chunks WHERE report_id IN "
                        "(SELECT id FROM reports WHERE ticker = ?)", (t,)
                    ).fetchone()[0]
                    conn.execute(
                        "DELETE FROM document_chunks WHERE report_id IN "
                        "(SELECT id FROM reports WHERE ticker = ?)", (t,)
                    )
                    # asset_manifest before reports
                    _archive('asset_manifest', 'ticker = ?', (t,))
                    counts['asset_manifest'] = _del('asset_manifest', 'ticker = ?', (t,))
                    _archive('reports', 'ticker = ?', (t,))
                    counts['reports'] = _del('reports', 'ticker = ?', (t,))
                    _archive('scrape_queue', 'ticker = ?', (t,))
                    counts['scrape_queue'] = _del('scrape_queue', 'ticker = ?', (t,))
                    _archive('btc_loans', 'ticker = ?', (t,))
                    counts['btc_loans'] = _del('btc_loans', 'ticker = ?', (t,))
                    _archive('facilities', 'ticker = ?', (t,))
                    counts['facilities'] = _del('facilities', 'ticker = ?', (t,))
                    _archive('source_audit', 'ticker = ?', (t,))
                    counts['source_audit'] = _del('source_audit', 'ticker = ?', (t,))
                    _archive('llm_benchmark_runs', 'ticker = ?', (t,))
                    counts['llm_benchmark_runs'] = _del('llm_benchmark_runs', 'ticker = ?', (t,))
                    # Reset operational fields on the company row
                    conn.execute(
                        """UPDATE companies
                           SET scraper_status = 'never_run',
                               last_scrape_at = NULL,
                               last_scrape_error = NULL,
                               probe_completed_at = NULL,
                               scraper_issues_log = ''
                           WHERE ticker = ?""",
                        (t,),
                    )
                else:
                    # review_queue before data_points
                    _archive('review_queue')
                    counts['review_queue'] = _del('review_queue')
                    # raw_extractions before reports
                    _archive('raw_extractions')
                    counts['raw_extractions'] = _del('raw_extractions')
                    # data_points before document_chunks
                    _archive('data_points')
                    counts['data_points'] = _del('data_points')
                    # document_chunks after data_points, before reports
                    _archive('document_chunks')
                    counts['document_chunks'] = _del('document_chunks')
                    _archive('asset_manifest')
                    counts['asset_manifest'] = _del('asset_manifest')
                    _archive('reports')
                    counts['reports'] = _del('reports')
                    _archive('scrape_queue')
                    counts['scrape_queue'] = _del('scrape_queue')
                    _archive('btc_loans')
                    counts['btc_loans'] = _del('btc_loans')
                    _archive('facilities')
                    counts['facilities'] = _del('facilities')
                    _archive('source_audit')
                    counts['source_audit'] = _del('source_audit')
                    _archive('llm_benchmark_runs')
                    counts['llm_benchmark_runs'] = _del('llm_benchmark_runs')
                    if mode in {'reset', 'archive'}:
                        # Full reset keeps config rows and only resets company operational fields.
                        conn.execute(
                            """UPDATE companies
                               SET scraper_status = 'never_run',
                                   last_scrape_at = NULL,
                                   last_scrape_error = NULL,
                                   probe_completed_at = NULL,
                                   scraper_issues_log = ''"""
                        )
                    else:
                        # For FULL hard-delete, clear regime windows and company catalog rows.
                        # regime_config and scraper_discovery_candidates before companies
                        # (FK: both reference companies.ticker)
                        _archive('regime_config')
                        counts['regime_config'] = _del('regime_config')
                        _archive('scraper_discovery_candidates')
                        counts['scraper_discovery_candidates'] = _del('scraper_discovery_candidates')
                        _archive('companies')
                        counts['companies'] = _del('companies')
                        if suppress_auto_sync:
                            conn.execute(
                                """INSERT OR REPLACE INTO config_settings (key, value, updated_at)
                                   VALUES ('auto_sync_companies_on_startup', '0', datetime('now'))"""
                            )

                if archive_conn is not None:
                    archive_conn.commit()
        except Exception:
            if archive_conn is not None:
                archive_conn.rollback()
            raise
        finally:
            if archive_conn is not None:
                archive_conn.close()

        if archive_batch_id is not None:
            counts['archive_batch_id'] = archive_batch_id

        log.info(
            "purge_all: mode=%s counts=%s (ticker=%s suppress_auto_sync=%s)",
            mode,
            counts,
            ticker or 'ALL',
            suppress_auto_sync,
        )
        return counts

    def get_trailing_data_points(
        self, ticker: str, metric: str, before_period: str, limit: int = 3
    ) -> list:
        """Return up to `limit` monthly data_points for (ticker, metric) with
        period < before_period, ordered DESC by period.

        Used by outlier detection to compute a trailing average.
        Only returns source_period_type='monthly' rows.
        """
        with self._get_connection() as conn:
            rows = conn.execute(
                """SELECT period, value, source_period_type FROM data_points
                   WHERE ticker = ? AND metric = ? AND period < ?
                   AND (source_period_type IS NULL OR source_period_type = 'monthly')
                   ORDER BY period DESC
                   LIMIT ?""",
                (ticker, metric, before_period, limit),
            ).fetchall()
            return [dict(r) for r in rows]

    def query_data_points_for_export(
        self,
        ticker: Optional[str] = None,
        tickers: Optional[list] = None,
        metric: Optional[str] = None,
        from_period: Optional[str] = None,
        to_period: Optional[str] = None,
        min_confidence: Optional[float] = None,
        limit: int = 10000,
        offset: int = 0,
    ) -> list:
        """Like query_data_points but LEFT JOINs reports (source_url) and
        review_queue (llm_value, regex_value, agreement_status) for provenance."""
        clauses = []
        params = []
        _tickers = tickers if tickers else ([ticker] if ticker else None)
        if _tickers:
            if len(_tickers) == 1:
                clauses.append("dp.ticker = ?")
                params.append(_tickers[0])
            else:
                placeholders = ','.join('?' * len(_tickers))
                clauses.append(f"dp.ticker IN ({placeholders})")
                params.extend(_tickers)
        if metric:
            clauses.append("dp.metric = ?")
            params.append(metric)
        if from_period:
            clauses.append("dp.period >= ?")
            params.append(from_period)
        if to_period:
            clauses.append("dp.period <= ?")
            params.append(to_period)
        if min_confidence is not None:
            clauses.append("dp.confidence >= ?")
            params.append(min_confidence)
        where = ("WHERE " + " AND ".join(clauses)) if clauses else ""
        params.extend([limit, offset])
        sql = f"""
            SELECT dp.id, dp.ticker, dp.period, dp.metric, dp.value, dp.unit,
                   dp.confidence, dp.extraction_method, dp.source_snippet, dp.created_at,
                   r.source_url,
                   rq.llm_value, rq.regex_value, rq.agreement_status
            FROM data_points dp
            LEFT JOIN reports r ON r.id = dp.report_id
            LEFT JOIN review_queue rq
                ON  rq.ticker = dp.ticker
                AND rq.period = dp.period
                AND rq.metric = dp.metric
            {where}
            ORDER BY dp.ticker, dp.period, dp.metric
            LIMIT ? OFFSET ?
        """
        with self._get_connection() as conn:
            rows = conn.execute(sql, params).fetchall()
            return [dict(r) for r in rows]

    # ── Review Queue CRUD ────────────────────────────────────────────────────

    def insert_review_item(self, item: dict) -> int:
        with self._get_connection() as conn:
            cursor = conn.execute(
                """INSERT INTO review_queue
                   (data_point_id, ticker, period, metric, raw_value, confidence,
                    source_snippet, status, llm_value, regex_value, agreement_status, report_id)
                   VALUES (:data_point_id, :ticker, :period, :metric, :raw_value,
                           :confidence, :source_snippet, :status,
                           :llm_value, :regex_value, :agreement_status, :report_id)""",
                {
                    'data_point_id': item.get('data_point_id'),
                    'ticker': item['ticker'],
                    'period': item['period'],
                    'metric': item['metric'],
                    'raw_value': item['raw_value'],
                    'confidence': item['confidence'],
                    'source_snippet': item.get('source_snippet'),
                    'status': item.get('status', 'PENDING'),
                    'llm_value': item.get('llm_value'),
                    'regex_value': item.get('regex_value'),
                    'agreement_status': item.get('agreement_status'),
                    'report_id': item.get('report_id'),
                },
            )
            return cursor.lastrowid

    def get_review_item(self, id: int) -> Optional[dict]:
        """Return a single review_queue row by id, or None if not found."""
        with self._get_connection() as conn:
            row = conn.execute(
                "SELECT * FROM review_queue WHERE id = ?", (id,)
            ).fetchone()
            return dict(row) if row else None

    def get_review_items(
        self,
        status: Optional[str] = None,
        limit: int = 50,
        offset: int = 0,
        ticker: Optional[str] = None,
        period: Optional[str] = None,
        metric: Optional[str] = None,
    ) -> list:
        conditions = []
        params: list = []
        if status:
            conditions.append("status = ?")
            params.append(status)
        if ticker:
            conditions.append("ticker = ?")
            params.append(ticker)
        if period:
            # Accept YYYY-MM or YYYY-MM-DD; match by year-month prefix
            conditions.append("period LIKE ?")
            params.append(period[:7] + '%')
        if metric:
            conditions.append("metric = ?")
            params.append(metric)
        where = ("WHERE " + " AND ".join(conditions)) if conditions else ""
        sql = f"SELECT * FROM review_queue {where} ORDER BY created_at DESC LIMIT ? OFFSET ?"
        params += [limit, offset]
        with self._get_connection() as conn:
            rows = conn.execute(sql, params).fetchall()
            return [dict(r) for r in rows]

    def find_report_for_period(self, ticker: str, period: str) -> Optional[dict]:
        """Return {id, source_type, source_url} for the latest report matching ticker + YYYY-MM."""
        period_ym = period[:7]
        with self._get_connection() as conn:
            rows = conn.execute(
                """SELECT id, source_type, source_url FROM reports
                   WHERE ticker = ? AND report_date LIKE ?
                   ORDER BY report_date DESC, id DESC
                   LIMIT 1""",
                (ticker, period_ym + '%'),
            ).fetchall()
            return dict(rows[0]) if rows else None

    def get_nearby_reports(self, ticker: str, period_ym: str, window_days: int = 90) -> list:
        """Return reports for ticker whose report_date is within window_days of period_ym-01."""
        period_date = period_ym + '-01'
        with self._get_connection() as conn:
            rows = conn.execute(
                """SELECT id, ticker, report_date, source_type, source_url
                   FROM reports
                   WHERE ticker = ?
                   AND ABS(julianday(report_date) - julianday(?)) <= ?
                   ORDER BY ABS(julianday(report_date) - julianday(?))""",
                [ticker, period_date, window_days, period_date],
            ).fetchall()
            return [dict(r) for r in rows]

    def get_review_items_for_period(self, ticker: str, period: str, metric: str) -> list:
        """Return review_queue items for a specific ticker+period+metric."""
        with self._get_connection() as conn:
            rows = conn.execute(
                """SELECT id, raw_value, confidence, status, source_snippet,
                          reviewer_note, created_at
                   FROM review_queue
                   WHERE ticker = ? AND period = ? AND metric = ?
                   ORDER BY created_at DESC""",
                [ticker, period, metric],
            ).fetchall()
            return [dict(r) for r in rows]

    def count_review_items(self, status: Optional[str] = "PENDING") -> int:
        with self._get_connection() as conn:
            if status:
                return conn.execute(
                    "SELECT COUNT(*) FROM review_queue WHERE status = ?", (status,)
                ).fetchone()[0]
            else:
                return conn.execute(
                    "SELECT COUNT(*) FROM review_queue"
                ).fetchone()[0]

    def approve_review_item(self, id: int) -> dict:
        with self._get_connection() as conn:
            row = conn.execute(
                "SELECT * FROM review_queue WHERE id = ?", (id,)
            ).fetchone()
            if not row:
                raise ValueError(f"Review item {id} not found")
            item = dict(row)
            dp = {
                "report_id": item.get("report_id"),
                "ticker": item["ticker"],
                "period": item["period"],
                "metric": item["metric"],
                "value": float(item["raw_value"]),
                "unit": _metric_unit(item["metric"]),
                "confidence": item["confidence"],
                "extraction_method": "review_approved",
                "source_snippet": item["source_snippet"],
            }
            cursor = conn.execute(
                """INSERT OR REPLACE INTO data_points
                   (report_id, ticker, period, metric, value, unit, confidence,
                    extraction_method, source_snippet)
                   VALUES (:report_id, :ticker, :period, :metric, :value, :unit,
                           :confidence, :extraction_method, :source_snippet)""",
                dp,
            )
            dp_id = cursor.lastrowid
            conn.execute(
                """UPDATE review_queue SET status='APPROVED', reviewed_at=?, data_point_id=?
                   WHERE id=?""",
                (datetime.now(timezone.utc).isoformat(), dp_id, id),
            )
            dp["id"] = dp_id
            return dp

    def reject_review_item(self, id: int, note: str) -> None:
        with self._get_connection() as conn:
            conn.execute(
                """UPDATE review_queue SET status='REJECTED', reviewer_note=?, reviewed_at=?
                   WHERE id=?""",
                (note, datetime.now(timezone.utc).isoformat(), id),
            )

    def edit_review_item(self, id: int, corrected_value: float, note: str) -> dict:
        with self._get_connection() as conn:
            row = conn.execute(
                "SELECT * FROM review_queue WHERE id = ?", (id,)
            ).fetchone()
            if not row:
                raise ValueError(f"Review item {id} not found")
            item = dict(row)
            dp = {
                "report_id": item.get("report_id"),
                "ticker": item["ticker"],
                "period": item["period"],
                "metric": item["metric"],
                "value": corrected_value,
                "unit": _metric_unit(item["metric"]),
                "confidence": 1.0,
                "extraction_method": "review_edited",
                "source_snippet": item["source_snippet"],
            }
            cursor = conn.execute(
                """INSERT OR REPLACE INTO data_points
                   (report_id, ticker, period, metric, value, unit, confidence,
                    extraction_method, source_snippet)
                   VALUES (:report_id, :ticker, :period, :metric, :value, :unit,
                           :confidence, :extraction_method, :source_snippet)""",
                dp,
            )
            dp_id = cursor.lastrowid
            conn.execute(
                """UPDATE review_queue
                   SET status='EDITED', raw_value=?, reviewer_note=?, reviewed_at=?,
                       data_point_id=?
                   WHERE id=?""",
                (str(corrected_value), note, datetime.now(timezone.utc).isoformat(), dp_id, id),
            )
            dp["id"] = dp_id
            return dp


    # ── Diagnostics queries ──────────────────────────────────────────────────

    def get_pattern_usage(self) -> list:
        """Return extraction method counts and avg confidence, grouped by method and metric."""
        with self._get_connection() as conn:
            rows = conn.execute(
                """SELECT extraction_method, metric, COUNT(*) as count,
                          ROUND(AVG(confidence), 3) as avg_conf
                   FROM data_points
                   GROUP BY extraction_method, metric
                   ORDER BY metric, count DESC"""
            ).fetchall()
            return [dict(r) for r in rows]

    def get_metric_coverage(self) -> list:
        """Return period count per ticker+metric combination."""
        with self._get_connection() as conn:
            rows = conn.execute(
                """SELECT ticker, metric, COUNT(*) as period_count
                   FROM data_points
                   GROUP BY ticker, metric"""
            ).fetchall()
            return [dict(r) for r in rows]

    def get_confidence_buckets(self) -> list:
        """Return count per confidence bucket for data_points and review_queue."""
        buckets = [
            ('0.95–1.00', 0.95, 1.01),
            ('0.90–0.94', 0.90, 0.95),
            ('0.85–0.89', 0.85, 0.90),
            ('0.80–0.84', 0.80, 0.85),
            ('0.75–0.79', 0.75, 0.80),
        ]
        with self._get_connection() as conn:
            result = []
            for label, lo, hi in buckets:
                count = conn.execute(
                    "SELECT COUNT(*) FROM data_points WHERE confidence >= ? AND confidence < ?",
                    (lo, hi),
                ).fetchone()[0]
                result.append({'bucket': label, 'count': count, 'source': 'accepted'})
            pending = conn.execute(
                "SELECT COUNT(*) FROM review_queue WHERE status = 'PENDING'"
            ).fetchone()[0]
            result.append({'bucket': '<0.75 (review)', 'count': pending, 'source': 'review'})
            return result

    def get_company_status(self) -> list:
        """Return per-ticker aggregated stats for all tickers that have at least one report.

        Returns list of dicts with keys:
          ticker, report_count, data_point_count, prod_months,
          first_period, last_period, avg_confidence
        """
        with self._get_connection() as conn:
            rows = conn.execute(
                """SELECT
                       r.ticker,
                       COUNT(DISTINCT r.id) AS report_count,
                       COUNT(dp.id) AS data_point_count,
                       COUNT(DISTINCT CASE WHEN dp.metric = 'production_btc'
                                          THEN dp.period END) AS prod_months,
                       MIN(CASE WHEN dp.metric = 'production_btc'
                                THEN dp.period END) AS first_period,
                       MAX(CASE WHEN dp.metric = 'production_btc'
                                THEN dp.period END) AS last_period,
                       ROUND(AVG(dp.confidence), 3) AS avg_confidence
                   FROM reports r
                   LEFT JOIN data_points dp ON dp.report_id = r.id
                   GROUP BY r.ticker
                   ORDER BY r.ticker"""
            ).fetchall()
            return [dict(r) for r in rows]

    def get_missing_periods(self, ticker: str, metric: str = 'production_btc') -> list:
        """Return YYYY-MM-DD strings for months between min and max data_point period
        where no data_point exists for ticker+metric.

        Returns an empty list if the ticker has no data_points for the metric
        (no spine to build) or if there are no gaps.
        """
        with self._get_connection() as conn:
            row = conn.execute(
                "SELECT MIN(period), MAX(period) FROM data_points WHERE ticker=? AND metric=?",
                (ticker, metric),
            ).fetchone()
            if not row or row[0] is None:
                return []
            min_period, max_period = row[0], row[1]

            existing = {
                r[0] for r in conn.execute(
                    "SELECT period FROM data_points WHERE ticker=? AND metric=?",
                    (ticker, metric),
                ).fetchall()
            }

        # Build monthly spine from min to max inclusive
        min_y, min_m = int(min_period[:4]), int(min_period[5:7])
        max_y, max_m = int(max_period[:4]), int(max_period[5:7])
        gaps = []
        y, m = min_y, min_m
        while (y, m) <= (max_y, max_m):
            period_str = f"{y:04d}-{m:02d}-01"
            if period_str not in existing:
                gaps.append(period_str)
            m += 1
            if m > 12:
                m = 1
                y += 1
        return gaps

    _DEFAULT_BITCOIN_MINING_KEYWORDS = [
        '%bitcoin%', '%btc%', '%hash rate%', '%hashrate%',
        '%exahash%', '%petahash%', '%mining operations%',
    ]

    def get_earliest_bitcoin_report_period(self, ticker: str) -> Optional[str]:
        """Return the earliest covering_period for reports that mention bitcoin mining.

        Scans raw_text for LIKE-pattern keywords indicating active mining operations.
        Keywords are read from config_settings key 'bitcoin_mining_keywords'
        (comma-separated, e.g. '%bitcoin%,%btc%') and fall back to hardcoded defaults.
        Returns None if no such report exists for the ticker.
        """
        raw = self.get_config('bitcoin_mining_keywords')
        if raw:
            keywords = [k.strip() for k in raw.split(',') if k.strip()]
        else:
            keywords = list(self._DEFAULT_BITCOIN_MINING_KEYWORDS)
        clauses = ' OR '.join('LOWER(raw_text) LIKE ?' for _ in keywords)
        params = [ticker] + [k.lower() for k in keywords]
        with self._get_connection() as conn:
            row = conn.execute(
                f"SELECT MIN(covering_period) FROM reports WHERE ticker=? AND ({clauses})",
                params,
            ).fetchone()
        return row[0] if row and row[0] else None

    def get_covered_periods(self, ticker: str) -> list:
        """Return distinct covering_period values that have at least one data_point."""
        with self._get_connection() as conn:
            rows = conn.execute(
                "SELECT DISTINCT period FROM data_points WHERE ticker=? ORDER BY period",
                (ticker,),
            ).fetchall()
        return [r[0] for r in rows]

    # ── Metric Rules CRUD ─────────────────────────────────────────────────────

    def get_metric_rules(self, metric: Optional[str] = None) -> list:
        """Return all metric_rules rows, or a single row if metric is specified."""
        with self._get_connection() as conn:
            if metric:
                row = conn.execute(
                    "SELECT * FROM metric_rules WHERE metric = ?", (metric,)
                ).fetchone()
                return [dict(row)] if row else []
            rows = conn.execute(
                "SELECT * FROM metric_rules ORDER BY metric"
            ).fetchall()
            return [dict(r) for r in rows]

    def upsert_metric_rule(
        self,
        metric: str,
        agreement_threshold: float,
        outlier_threshold: float,
        outlier_min_history: int,
        enabled: int = 1,
        notes: Optional[str] = None,
    ) -> dict:
        """Insert or replace a metric_rules row. Returns the updated row."""
        with self._get_connection() as conn:
            conn.execute(
                """INSERT OR REPLACE INTO metric_rules
                   (metric, agreement_threshold, outlier_threshold,
                    outlier_min_history, enabled, notes, updated_at)
                   VALUES (?, ?, ?, ?, ?, ?, datetime('now'))""",
                (metric, agreement_threshold, outlier_threshold,
                 outlier_min_history, enabled, notes),
            )
            row = conn.execute(
                "SELECT * FROM metric_rules WHERE metric = ?", (metric,)
            ).fetchone()
            return dict(row)

    def delete_metric_rule(self, metric: str) -> None:
        """Delete a metric_rules row by metric key. No-op if it does not exist."""
        with self._get_connection() as conn:
            conn.execute("DELETE FROM metric_rules WHERE metric = ?", (metric,))

    def upsert_qc_snapshot(self, snapshot: dict) -> None:
        """Insert a QC precision snapshot. snapshot_at is set from snapshot['run_date']."""
        import json
        with self._get_connection() as conn:
            conn.execute(
                """INSERT OR REPLACE INTO qc_snapshots (snapshot_at, ticker, summary_json)
                   VALUES (?, ?, ?)""",
                (
                    snapshot.get('run_date'),
                    snapshot.get('ticker'),
                    json.dumps(snapshot),
                ),
            )

    def get_qc_snapshots(self, ticker: str = None) -> list:
        """Return QC snapshots ordered by snapshot_at DESC."""
        import json
        with self._get_connection() as conn:
            if ticker:
                rows = conn.execute(
                    "SELECT * FROM qc_snapshots WHERE ticker=? ORDER BY snapshot_at DESC",
                    (ticker,),
                ).fetchall()
            else:
                rows = conn.execute(
                    "SELECT * FROM qc_snapshots ORDER BY snapshot_at DESC"
                ).fetchall()
            result = []
            for row in rows:
                d = dict(row)
                try:
                    d.update(json.loads(d.get('summary_json') or '{}'))
                except Exception:
                    pass
                result.append(d)
            return result

    def get_snippets(self, limit: int = 2000) -> list:
        """Return source snippets from data_points and review_queue for keyword analysis."""
        with self._get_connection() as conn:
            dp_rows = conn.execute(
                "SELECT source_snippet FROM data_points WHERE source_snippet IS NOT NULL LIMIT ?",
                (limit,),
            ).fetchall()
            rq_rows = conn.execute(
                "SELECT source_snippet FROM review_queue WHERE source_snippet IS NOT NULL LIMIT ?",
                (limit // 4,),
            ).fetchall()
            return [r[0] for r in dp_rows] + [r[0] for r in rq_rows]


    # ── LLM Prompts ───────────────────────────────────────────────────────────

    def get_llm_prompt(self, metric: str) -> Optional[dict]:
        """Return the active prompt for a metric, or None if not set."""
        with self._get_connection() as conn:
            row = conn.execute(
                "SELECT * FROM llm_prompts WHERE metric = ? AND active = 1",
                (metric,)
            ).fetchone()
            return dict(row) if row else None

    def upsert_llm_prompt(self, metric: str, prompt_text: str, model: str = None) -> None:
        """Insert or replace the prompt for a metric."""
        from config import LLM_MODEL_ID
        with self._get_connection() as conn:
            conn.execute(
                """INSERT OR REPLACE INTO llm_prompts (metric, prompt_text, model, active, updated_at)
                   VALUES (?, ?, ?, 1, datetime('now'))""",
                (metric, prompt_text, model or LLM_MODEL_ID),
            )

    def list_llm_prompts(self) -> list:
        """Return all active prompts ordered by metric."""
        with self._get_connection() as conn:
            rows = conn.execute(
                "SELECT * FROM llm_prompts WHERE active = 1 ORDER BY metric"
            ).fetchall()
            return [dict(r) for r in rows]

    # ── Facilities ───────────────────────────────────────────────────────────

    def insert_facility(self, record: dict) -> int:
        """Insert a facility row. Raises IntegrityError on duplicate ticker+name."""
        with self._get_connection() as conn:
            cursor = conn.execute(
                """INSERT INTO facilities
                   (ticker, name, address, city, state, lat, lon, purpose, size_mw, operational_since)
                   VALUES (:ticker, :name, :address, :city, :state, :lat, :lon, :purpose, :size_mw, :operational_since)""",
                {
                    'ticker': record.get('ticker'),
                    'name': record.get('name'),
                    'address': record.get('address'),
                    'city': record.get('city'),
                    'state': record.get('state'),
                    'lat': record.get('lat'),
                    'lon': record.get('lon'),
                    'purpose': record.get('purpose'),
                    'size_mw': record.get('size_mw'),
                    'operational_since': record.get('operational_since'),
                },
            )
            return cursor.lastrowid

    def get_facilities(self, ticker: str = None) -> list:
        """Return facilities, optionally filtered by ticker."""
        with self._get_connection() as conn:
            if ticker is not None:
                rows = conn.execute(
                    "SELECT * FROM facilities WHERE ticker = ? ORDER BY name", (ticker,)
                ).fetchall()
            else:
                rows = conn.execute(
                    "SELECT * FROM facilities ORDER BY ticker, name"
                ).fetchall()
            return [dict(r) for r in rows]

    # ── BTC Loans ─────────────────────────────────────────────────────────────

    def insert_btc_loan(self, record: dict) -> int:
        """Insert a BTC loan row."""
        with self._get_connection() as conn:
            cursor = conn.execute(
                """INSERT INTO btc_loans
                   (ticker, counterparty, total_btc_encumbered, as_of_date)
                   VALUES (:ticker, :counterparty, :total_btc_encumbered, :as_of_date)""",
                {
                    'ticker': record.get('ticker'),
                    'counterparty': record.get('counterparty'),
                    'total_btc_encumbered': record.get('total_btc_encumbered'),
                    'as_of_date': record.get('as_of_date'),
                },
            )
            return cursor.lastrowid

    def get_btc_loans(self, ticker: str) -> list:
        """Return BTC loans for a ticker, ordered by as_of_date desc."""
        with self._get_connection() as conn:
            rows = conn.execute(
                "SELECT * FROM btc_loans WHERE ticker = ? ORDER BY as_of_date DESC",
                (ticker,)
            ).fetchall()
            return [dict(r) for r in rows]

    # ── Source Audit ──────────────────────────────────────────────────────────

    def upsert_source_audit(self, record: dict) -> None:
        """Insert or replace a source_audit row (UNIQUE on ticker, source_type)."""
        with self._get_connection() as conn:
            conn.execute(
                """INSERT OR REPLACE INTO source_audit
                   (ticker, source_type, url, last_checked, http_status, status, notes)
                   VALUES (:ticker, :source_type, :url, :last_checked, :http_status, :status, :notes)""",
                {
                    'ticker': record.get('ticker'),
                    'source_type': record.get('source_type'),
                    'url': record.get('url'),
                    'last_checked': record.get('last_checked'),
                    'http_status': record.get('http_status'),
                    'status': record.get('status', 'NOT_TRIED'),
                    'notes': record.get('notes'),
                },
            )

    def get_source_audit(self, ticker: str) -> list:
        """Return source_audit rows for a ticker."""
        with self._get_connection() as conn:
            rows = conn.execute(
                "SELECT * FROM source_audit WHERE ticker = ? ORDER BY source_type",
                (ticker,)
            ).fetchall()
            return [dict(r) for r in rows]

    # ── Scraper Discovery Candidates ────────────────────────────────────────

    def upsert_discovery_candidate(self, candidate: dict) -> int:
        """Insert or replace a scraper_discovery_candidates row."""
        with self._get_connection() as conn:
            cursor = conn.execute(
                """INSERT OR REPLACE INTO scraper_discovery_candidates
                   (ticker, source_type, url, pr_start_year, confidence, rationale,
                    proposed_by, last_checked, http_status, probe_status,
                    evidence_title, evidence_date, verified)
                   VALUES (:ticker, :source_type, :url, :pr_start_year, :confidence, :rationale,
                           :proposed_by, :last_checked, :http_status, :probe_status,
                           :evidence_title, :evidence_date, :verified)""",
                {
                    'ticker': candidate['ticker'],
                    'source_type': candidate['source_type'],
                    'url': candidate['url'],
                    'pr_start_year': candidate.get('pr_start_year'),
                    'confidence': candidate.get('confidence'),
                    'rationale': candidate.get('rationale'),
                    'proposed_by': candidate.get('proposed_by', 'agent'),
                    'last_checked': candidate.get('last_checked'),
                    'http_status': candidate.get('http_status'),
                    'probe_status': candidate.get('probe_status'),
                    'evidence_title': candidate.get('evidence_title'),
                    'evidence_date': candidate.get('evidence_date'),
                    'verified': candidate.get('verified', 0),
                },
            )
            return cursor.lastrowid

    def list_discovery_candidates(self, ticker: str, verified_only: bool = False) -> list:
        """Return discovery candidates for a ticker."""
        with self._get_connection() as conn:
            if verified_only:
                rows = conn.execute(
                    """SELECT * FROM scraper_discovery_candidates
                       WHERE ticker = ? AND verified = 1
                       ORDER BY source_type, confidence DESC, id DESC""",
                    (ticker,),
                ).fetchall()
            else:
                rows = conn.execute(
                    """SELECT * FROM scraper_discovery_candidates
                       WHERE ticker = ?
                       ORDER BY source_type, confidence DESC, id DESC""",
                    (ticker,),
                ).fetchall()
            return [dict(r) for r in rows]

    # ── Config Settings ───────────────────────────────────────────────────────

    def get_config(self, key: str, default=None) -> Optional[str]:
        """Return the value for a config_settings key, or default if not set."""
        try:
            with self._get_connection() as conn:
                row = conn.execute(
                    "SELECT value FROM config_settings WHERE key = ?", (key,)
                ).fetchone()
                return row[0] if row else default
        except Exception as e:
            log.warning("get_config(%r) failed: %s", key, e)
            return default

    def set_config(self, key: str, value: str) -> None:
        """Insert or replace a config_settings key."""
        with self._get_connection() as conn:
            conn.execute(
                """INSERT OR REPLACE INTO config_settings (key, value, updated_at)
                   VALUES (?, ?, datetime('now'))""",
                (key, value),
            )

    def list_config(self) -> list:
        """Return all config_settings rows as dicts."""
        with self._get_connection() as conn:
            rows = conn.execute(
                "SELECT key, value, updated_at FROM config_settings ORDER BY key"
            ).fetchall()
            return [dict(r) for r in rows]

    # ── Asset Manifest CRUD ───────────────────────────────────────────────────

    def upsert_asset_manifest(self, entry: dict) -> int:
        """Insert or replace an asset_manifest row by file_path. Returns lastrowid."""
        with self._get_connection() as conn:
            cursor = conn.execute(
                """INSERT OR REPLACE INTO asset_manifest
                   (ticker, period, source_type, file_path, filename,
                    ingest_state, report_id, ingest_error, notes)
                   VALUES (:ticker, :period, :source_type, :file_path, :filename,
                           :ingest_state, :report_id, :ingest_error, :notes)""",
                {
                    'ticker': entry['ticker'],
                    'period': entry.get('period'),
                    'source_type': entry['source_type'],
                    'file_path': entry['file_path'],
                    'filename': entry['filename'],
                    'ingest_state': entry.get('ingest_state', 'pending'),
                    'report_id': entry.get('report_id'),
                    'ingest_error': entry.get('ingest_error'),
                    'notes': entry.get('notes'),
                },
            )
            return cursor.lastrowid

    def get_manifest_by_ticker(self, ticker: str) -> list:
        """Return all asset_manifest rows for a ticker, ordered by period."""
        with self._get_connection() as conn:
            rows = conn.execute(
                "SELECT * FROM asset_manifest WHERE ticker = ? ORDER BY period",
                (ticker,),
            ).fetchall()
            return [dict(r) for r in rows]

    def get_uningested_assets(self, ticker: Optional[str] = None) -> list:
        """Return manifest rows with ingest_state='pending', optionally filtered by ticker."""
        if ticker:
            with self._get_connection() as conn:
                rows = conn.execute(
                    "SELECT * FROM asset_manifest WHERE ingest_state='pending' AND ticker=? ORDER BY ticker, period",
                    (ticker,),
                ).fetchall()
        else:
            with self._get_connection() as conn:
                rows = conn.execute(
                    "SELECT * FROM asset_manifest WHERE ingest_state='pending' ORDER BY ticker, period",
                ).fetchall()
        return [dict(r) for r in rows]

    def link_manifest_to_report(self, manifest_id: int, report_id: int) -> None:
        """Set ingest_state='ingested' and report_id on a manifest entry."""
        with self._get_connection() as conn:
            conn.execute(
                "UPDATE asset_manifest SET ingest_state='ingested', report_id=? WHERE id=?",
                (report_id, manifest_id),
            )

    def get_manifest_by_id(self, manifest_id: int) -> Optional[dict]:
        """Return a single asset_manifest row by primary key, or None if not found."""
        with self._get_connection() as conn:
            row = conn.execute(
                "SELECT * FROM asset_manifest WHERE id = ?",
                (manifest_id,),
            ).fetchone()
            return dict(row) if row else None

    def get_manifest_by_file_path(self, file_path: str) -> Optional[dict]:
        """Return a single manifest row by file_path, or None if not found."""
        with self._get_connection() as conn:
            row = conn.execute(
                "SELECT * FROM asset_manifest WHERE file_path = ?",
                (file_path,),
            ).fetchone()
            return dict(row) if row else None

    def update_manifest_period(self, manifest_id: int, period: str) -> None:
        """Assign a period to a legacy_undated manifest entry and reset state to pending."""
        with self._get_connection() as conn:
            conn.execute(
                "UPDATE asset_manifest SET period=?, ingest_state='pending' WHERE id=?",
                (period, manifest_id),
            )

    def get_report_by_ticker_date(self, ticker: str, period: str) -> Optional[dict]:
        """Return the first report matching ticker+period (YYYY-MM-01 exact match)."""
        with self._get_connection() as conn:
            row = conn.execute(
                "SELECT * FROM reports WHERE ticker=? AND report_date=? LIMIT 1",
                (ticker, period),
            ).fetchone()
            return dict(row) if row else None

    def get_operations_queue(self) -> dict:
        """Return pending extraction queue grouped by ticker + legacy undated files.

        Returns:
            {
              'pending_extraction': {ticker: {'count': int, 'report_ids': [...]}},
              'legacy_files': [manifest rows with ingest_state='legacy_undated'],
            }
        """
        with self._get_connection() as conn:
            # Reports that have raw_text but no extracted_at
            pending_rows = conn.execute(
                """SELECT ticker, COUNT(*) as cnt, MIN(report_date) as earliest,
                          MAX(report_date) as latest
                   FROM reports
                   WHERE raw_text IS NOT NULL AND raw_text != '' AND extracted_at IS NULL
                   GROUP BY ticker
                   ORDER BY ticker"""
            ).fetchall()
            pending_extraction = {}
            for r in pending_rows:
                pending_extraction[r['ticker']] = {
                    'count': r['cnt'],
                    'earliest': r['earliest'],
                    'latest': r['latest'],
                }

            # Legacy undated manifest files
            legacy_rows = conn.execute(
                "SELECT * FROM asset_manifest WHERE ingest_state='legacy_undated' ORDER BY ticker, filename"
            ).fetchall()
            legacy_files = [dict(r) for r in legacy_rows]

        return {
            'pending_extraction': pending_extraction,
            'legacy_files': legacy_files,
        }

    def get_pipeline_observability(self) -> dict:
        """Return end-to-end pipeline counts and scraper config health.

        Tracks how much data is discovered (asset_manifest), ingested (reports),
        parsed (reports.parsed_at), extracted (reports.extracted_at), and routed
        to output tables (data_points/review_queue), both globally and per ticker.
        """
        with self._get_connection() as conn:
            companies_total = conn.execute("SELECT COUNT(*) FROM companies").fetchone()[0]
            companies_active = conn.execute(
                "SELECT COUNT(*) FROM companies WHERE active = 1"
            ).fetchone()[0]

            manifest_total = conn.execute(
                "SELECT COUNT(*) FROM asset_manifest"
            ).fetchone()[0]
            reports_total = conn.execute("SELECT COUNT(*) FROM reports").fetchone()[0]
            reports_with_text = conn.execute(
                "SELECT COUNT(*) FROM reports WHERE raw_text IS NOT NULL AND raw_text != ''"
            ).fetchone()[0]
            reports_parsed = conn.execute(
                "SELECT COUNT(*) FROM reports WHERE parsed_at IS NOT NULL"
            ).fetchone()[0]
            reports_extracted = conn.execute(
                "SELECT COUNT(*) FROM reports WHERE extracted_at IS NOT NULL"
            ).fetchone()[0]
            reports_unextracted = conn.execute(
                """SELECT COUNT(*) FROM reports
                   WHERE raw_text IS NOT NULL AND raw_text != '' AND extracted_at IS NULL"""
            ).fetchone()[0]
            data_points_total = conn.execute(
                "SELECT COUNT(*) FROM data_points"
            ).fetchone()[0]
            review_total = conn.execute("SELECT COUNT(*) FROM review_queue").fetchone()[0]
            review_pending = conn.execute(
                "SELECT COUNT(*) FROM review_queue WHERE status='PENDING'"
            ).fetchone()[0]

            manifest_state_rows = conn.execute(
                """SELECT ingest_state, COUNT(*) AS count
                   FROM asset_manifest
                   GROUP BY ingest_state
                   ORDER BY ingest_state"""
            ).fetchall()
            reports_source_rows = conn.execute(
                """SELECT source_type, COUNT(*) AS count
                   FROM reports
                   GROUP BY source_type
                   ORDER BY source_type"""
            ).fetchall()

            ticker_rows = conn.execute(
                """
                WITH
                  m AS (
                    SELECT
                      ticker,
                      COUNT(*) AS manifest_total,
                      SUM(CASE WHEN ingest_state='pending' THEN 1 ELSE 0 END) AS manifest_pending,
                      SUM(CASE WHEN ingest_state='legacy_undated' THEN 1 ELSE 0 END) AS manifest_legacy_undated,
                      SUM(CASE WHEN ingest_state='ingested' THEN 1 ELSE 0 END) AS manifest_ingested
                    FROM asset_manifest
                    GROUP BY ticker
                  ),
                  r AS (
                    SELECT
                      ticker,
                      COUNT(*) AS reports_total,
                      SUM(CASE WHEN raw_text IS NOT NULL AND raw_text != '' THEN 1 ELSE 0 END) AS reports_with_text,
                      SUM(CASE WHEN parsed_at IS NOT NULL THEN 1 ELSE 0 END) AS reports_parsed,
                      SUM(CASE WHEN extracted_at IS NOT NULL THEN 1 ELSE 0 END) AS reports_extracted,
                      SUM(
                        CASE
                          WHEN raw_text IS NOT NULL AND raw_text != '' AND extracted_at IS NULL THEN 1
                          ELSE 0
                        END
                      ) AS reports_unextracted
                    FROM reports
                    GROUP BY ticker
                  ),
                  dp AS (
                    SELECT ticker, COUNT(*) AS data_points_total
                    FROM data_points
                    GROUP BY ticker
                  ),
                  rq AS (
                    SELECT
                      ticker,
                      COUNT(*) AS review_total,
                      SUM(CASE WHEN status='PENDING' THEN 1 ELSE 0 END) AS review_pending
                    FROM review_queue
                    GROUP BY ticker
                  )
                SELECT
                  c.ticker,
                  c.active,
                  c.scraper_mode,
                  c.ir_url,
                  c.rss_url,
                  c.url_template,
                  c.pr_start_year,
                  COALESCE(m.manifest_total, 0) AS manifest_total,
                  COALESCE(m.manifest_pending, 0) AS manifest_pending,
                  COALESCE(m.manifest_legacy_undated, 0) AS manifest_legacy_undated,
                  COALESCE(m.manifest_ingested, 0) AS manifest_ingested,
                  COALESCE(r.reports_total, 0) AS reports_total,
                  COALESCE(r.reports_with_text, 0) AS reports_with_text,
                  COALESCE(r.reports_parsed, 0) AS reports_parsed,
                  COALESCE(r.reports_extracted, 0) AS reports_extracted,
                  COALESCE(r.reports_unextracted, 0) AS reports_unextracted,
                  COALESCE(dp.data_points_total, 0) AS data_points_total,
                  COALESCE(rq.review_total, 0) AS review_total,
                  COALESCE(rq.review_pending, 0) AS review_pending
                FROM companies c
                LEFT JOIN m  ON m.ticker = c.ticker
                LEFT JOIN r  ON r.ticker = c.ticker
                LEFT JOIN dp ON dp.ticker = c.ticker
                LEFT JOIN rq ON rq.ticker = c.ticker
                ORDER BY c.ticker
                """
            ).fetchall()

        def _scraper_mode_issue(row: dict) -> str | None:
            mode = (row.get('scraper_mode') or 'skip').strip().lower()
            if mode == 'rss':
                if not (row.get('rss_url') or '').strip():
                    return 'rss mode missing rss_url'
            elif mode == 'index':
                if not (row.get('ir_url') or '').strip():
                    return 'index mode missing ir_url'
            elif mode == 'template':
                if not (row.get('url_template') or '').strip():
                    return 'template mode missing url_template'
                if not row.get('pr_start_year'):
                    return 'template mode missing pr_start_year'
            elif mode == 'skip':
                return None
            else:
                return f"unknown scraper_mode '{mode}'"
            return None

        ticker_summaries = []
        invalid_tickers = []
        for raw in ticker_rows:
            row = dict(raw)
            issue = _scraper_mode_issue(row)
            if issue:
                invalid_tickers.append({'ticker': row['ticker'], 'issue': issue})
            ticker_summaries.append({
                'ticker': row['ticker'],
                'active': bool(row['active']),
                'scraper_mode': row.get('scraper_mode') or 'skip',
                'scraper_config_valid': issue is None,
                'scraper_config_issue': issue,
                'manifest_total': row['manifest_total'],
                'manifest_pending': row['manifest_pending'],
                'manifest_legacy_undated': row['manifest_legacy_undated'],
                'manifest_ingested': row['manifest_ingested'],
                'reports_total': row['reports_total'],
                'reports_with_text': row['reports_with_text'],
                'reports_parsed': row['reports_parsed'],
                'reports_extracted': row['reports_extracted'],
                'reports_unextracted': row['reports_unextracted'],
                'data_points_total': row['data_points_total'],
                'review_total': row['review_total'],
                'review_pending': row['review_pending'],
            })

        return {
            'generated_at': datetime.now(timezone.utc).isoformat(),
            'totals': {
                'companies_total': companies_total,
                'companies_active': companies_active,
                'manifest_total': manifest_total,
                'reports_total': reports_total,
                'reports_with_text': reports_with_text,
                'reports_parsed': reports_parsed,
                'reports_extracted': reports_extracted,
                'reports_unextracted': reports_unextracted,
                'data_points_total': data_points_total,
                'review_total': review_total,
                'review_pending': review_pending,
            },
            'by_state': {
                'manifest_ingest_state': {r['ingest_state']: r['count'] for r in manifest_state_rows},
                'reports_source_type': {r['source_type']: r['count'] for r in reports_source_rows},
            },
            'scraper_config': {
                'valid_count': len(ticker_summaries) - len(invalid_tickers),
                'invalid_count': len(invalid_tickers),
                'invalid_tickers': invalid_tickers,
            },
            'tickers': ticker_summaries,
        }

    def get_coverage_grid(self, months: int = 36) -> dict:
        """Return coverage grid: {ticker: {period: {state, report_id, manifest_id}}, summary: {...}}.

        Period states (priority order, highest first):
          accepted > extracted_in_review > ingested_pending_extraction > pending_ingest > legacy_undated > no_source
        """
        from coverage_logic import compute_cell_state, generate_month_range, summarize_grid

        periods = generate_month_range(months)
        if not periods:
            return {'summary': {}}

        period_start = periods[0]
        period_end = periods[-1]

        with self._get_connection() as conn:
            tickers = [r[0] for r in conn.execute(
                "SELECT ticker FROM companies WHERE active=1 ORDER BY ticker"
            ).fetchall()]

            # Bulk-fetch all manifest rows in period range
            manifest_rows = conn.execute(
                """SELECT * FROM asset_manifest
                   WHERE period >= ? AND period <= ?""",
                (period_start, period_end),
            ).fetchall()

            # Bulk-fetch all reports in period range
            report_rows = conn.execute(
                """SELECT id, ticker, report_date, extracted_at FROM reports
                   WHERE report_date >= ? AND report_date <= ?""",
                (period_start, period_end),
            ).fetchall()

            # Bulk-fetch all data_points in period range
            dp_rows = conn.execute(
                """SELECT ticker, period FROM data_points
                   WHERE period >= ? AND period <= ?""",
                (period_start, period_end),
            ).fetchall()

            # Bulk-fetch review_queue PENDING items in period range
            rq_rows = conn.execute(
                """SELECT ticker, period FROM review_queue
                   WHERE period >= ? AND period <= ? AND status='PENDING'""",
                (period_start, period_end),
            ).fetchall()

        # Index by (ticker, period) for O(1) lookups
        manifests_by_tp: dict = {}
        for r in manifest_rows:
            key = (r['ticker'], r['period'])
            manifests_by_tp.setdefault(key, []).append(dict(r))

        reports_by_tp: dict = {}
        for r in report_rows:
            key = (r['ticker'], r['report_date'])
            reports_by_tp.setdefault(key, []).append(dict(r))

        dp_set: set = {(r['ticker'], r['period']) for r in dp_rows}
        rq_set: set = {(r['ticker'], r['period']) for r in rq_rows}

        grid: dict = {}
        for ticker in tickers:
            grid[ticker] = {}
            for period in periods:
                tp = (ticker, period)
                m_list = manifests_by_tp.get(tp, [])
                r_list = reports_by_tp.get(tp, [])
                has_dp = tp in dp_set
                has_rq = tp in rq_set

                state = compute_cell_state(m_list, r_list, has_dp, has_rq)
                cell: dict = {'state': state}

                # Add report_id if available
                if r_list:
                    cell['report_id'] = r_list[0].get('id')
                # Add manifest_id if available
                if m_list:
                    cell['manifest_id'] = m_list[0].get('id')

                grid[ticker][period] = cell

        grid['summary'] = summarize_grid(grid)
        return grid

    # ── Document Chunks ───────────────────────────────────────────────────────

    def upsert_document_chunk(self, entry: dict) -> int:
        """Insert or replace a document chunk. Returns lastrowid."""
        with self._get_connection() as conn:
            cursor = conn.execute(
                """INSERT OR REPLACE INTO document_chunks
                   (report_id, chunk_index, section, text, char_start, char_end, token_count)
                   VALUES (:report_id, :chunk_index, :section, :text,
                           :char_start, :char_end, :token_count)""",
                {
                    'report_id': entry['report_id'],
                    'chunk_index': entry['chunk_index'],
                    'section': entry.get('section'),
                    'text': entry['text'],
                    'char_start': entry.get('char_start'),
                    'char_end': entry.get('char_end'),
                    'token_count': entry.get('token_count'),
                },
            )
            return cursor.lastrowid

    def get_chunks_for_report(self, report_id: int) -> list:
        """Return all chunks for a report, ordered by chunk_index."""
        with self._get_connection() as conn:
            rows = conn.execute(
                "SELECT * FROM document_chunks WHERE report_id=? ORDER BY chunk_index",
                (report_id,),
            ).fetchall()
            return [dict(r) for r in rows]

    def get_unembedded_chunks(self, limit: int = 100) -> list:
        """Return chunks that have not yet been embedded."""
        with self._get_connection() as conn:
            rows = conn.execute(
                "SELECT * FROM document_chunks WHERE embedded_at IS NULL LIMIT ?",
                (limit,),
            ).fetchall()
            return [dict(r) for r in rows]

    def set_report_parse_quality(self, report_id: int, quality: str) -> None:
        """Set the parse_quality field on a report."""
        with self._get_connection() as conn:
            conn.execute(
                "UPDATE reports SET parse_quality=? WHERE id=?",
                (quality, report_id),
            )

    # ── Ticker Hints ──────────────────────────────────────────────────────────

    def get_ticker_hint(self, ticker: str) -> Optional[str]:
        """Return the active hint for a ticker, or None if not set."""
        with self._get_connection() as conn:
            row = conn.execute(
                "SELECT hint FROM llm_ticker_hints WHERE ticker = ? AND active = 1",
                (ticker,)
            ).fetchone()
            return row[0] if row else None

    def upsert_ticker_hint(self, ticker: str, hint: str) -> None:
        """Insert or replace the hint for a ticker."""
        with self._get_connection() as conn:
            conn.execute(
                """INSERT OR REPLACE INTO llm_ticker_hints (ticker, hint, active, updated_at)
                   VALUES (?, ?, 1, datetime('now'))""",
                (ticker, hint),
            )

    def list_ticker_hints(self) -> list:
        """Return all active ticker hints ordered by ticker."""
        with self._get_connection() as conn:
            rows = conn.execute(
                "SELECT ticker, hint, active, updated_at FROM llm_ticker_hints ORDER BY ticker"
            ).fetchall()
            return [dict(r) for r in rows]


    # ── LLM Benchmark ─────────────────────────────────────────────────────────

    def insert_benchmark_run(self, data: dict) -> int:
        """Insert one row into llm_benchmark_runs. Returns lastrowid."""
        with self._get_connection() as conn:
            cursor = conn.execute(
                """INSERT INTO llm_benchmark_runs
                   (created_at, model, call_type, ticker, period, report_id,
                    prompt_chars, response_chars, prompt_tokens, response_tokens,
                    total_duration_ms, eval_duration_ms,
                    metrics_requested, metrics_extracted,
                    hits_90, hits_80, hits_75)
                   VALUES (:created_at, :model, :call_type, :ticker, :period, :report_id,
                           :prompt_chars, :response_chars, :prompt_tokens, :response_tokens,
                           :total_duration_ms, :eval_duration_ms,
                           :metrics_requested, :metrics_extracted,
                           :hits_90, :hits_80, :hits_75)""",
                {
                    'created_at': data.get('created_at', datetime.now(timezone.utc).isoformat()),
                    'model': data.get('model', ''),
                    'call_type': data.get('call_type', 'batch'),
                    'ticker': data.get('ticker'),
                    'period': data.get('period'),
                    'report_id': data.get('report_id'),
                    'prompt_chars': data.get('prompt_chars', 0),
                    'response_chars': data.get('response_chars', 0),
                    'prompt_tokens': data.get('prompt_tokens', 0),
                    'response_tokens': data.get('response_tokens', 0),
                    'total_duration_ms': data.get('total_duration_ms', 0),
                    'eval_duration_ms': data.get('eval_duration_ms', 0),
                    'metrics_requested': data.get('metrics_requested', 0),
                    'metrics_extracted': data.get('metrics_extracted', 0),
                    'hits_90': data.get('hits_90', 0),
                    'hits_80': data.get('hits_80', 0),
                    'hits_75': data.get('hits_75', 0),
                },
            )
            return cursor.lastrowid

    def get_benchmark_runs(
        self,
        model: Optional[str] = None,
        ticker: Optional[str] = None,
        limit: int = 100,
    ) -> list:
        """Return recent llm_benchmark_runs rows, newest first."""
        clauses = []
        params = []
        if model:
            clauses.append("model = ?")
            params.append(model)
        if ticker:
            clauses.append("ticker = ?")
            params.append(ticker)
        where = ("WHERE " + " AND ".join(clauses)) if clauses else ""
        params.append(limit)
        with self._get_connection() as conn:
            rows = conn.execute(
                f"SELECT * FROM llm_benchmark_runs {where} ORDER BY id DESC LIMIT ?",
                params,
            ).fetchall()
            return [dict(r) for r in rows]

    def get_benchmark_summary(self) -> list:
        """Return per-model aggregate stats from llm_benchmark_runs."""
        with self._get_connection() as conn:
            rows = conn.execute(
                """SELECT
                       model,
                       call_type,
                       COUNT(*) AS run_count,
                       ROUND(AVG(prompt_tokens), 1) AS avg_prompt_tokens,
                       ROUND(AVG(response_tokens), 1) AS avg_response_tokens,
                       ROUND(AVG(total_duration_ms), 1) AS avg_total_ms,
                       ROUND(AVG(eval_duration_ms), 1) AS avg_eval_ms,
                       ROUND(AVG(CASE WHEN metrics_requested > 0
                           THEN CAST(hits_90 AS REAL) / metrics_requested END), 3) AS avg_hit_rate_90,
                       ROUND(AVG(CASE WHEN metrics_requested > 0
                           THEN CAST(hits_80 AS REAL) / metrics_requested END), 3) AS avg_hit_rate_80,
                       ROUND(AVG(CASE WHEN metrics_requested > 0
                           THEN CAST(hits_75 AS REAL) / metrics_requested END), 3) AS avg_hit_rate_75,
                       MIN(created_at) AS first_run,
                       MAX(created_at) AS last_run
                   FROM llm_benchmark_runs
                   GROUP BY model, call_type
                   ORDER BY model, call_type"""
            ).fetchall()
            return [dict(r) for r in rows]


    # ── Phase III: regime_config CRUD ────────────────────────────────────────

    def upsert_regime_window(
        self, ticker: str, cadence: str, start_date: str,
        end_date: Optional[str], notes: str
    ) -> dict:
        """Insert a new regime window for a company. Returns the new row as a dict."""
        with self._get_connection() as conn:
            cursor = conn.execute(
                """INSERT INTO regime_config (ticker, cadence, start_date, end_date, notes)
                   VALUES (?, ?, ?, ?, ?)""",
                (ticker, cadence, start_date, end_date, notes),
            )
            row = conn.execute(
                "SELECT * FROM regime_config WHERE id = ?", (cursor.lastrowid,)
            ).fetchone()
            return dict(row)

    def get_regime_windows(self, ticker: str) -> list:
        """Return all regime windows for a company, ordered by start_date ascending."""
        with self._get_connection() as conn:
            rows = conn.execute(
                "SELECT * FROM regime_config WHERE ticker = ? ORDER BY start_date ASC",
                (ticker,),
            ).fetchall()
            return [dict(r) for r in rows]

    def delete_regime_window(self, window_id: int) -> None:
        """Delete a regime window by id."""
        with self._get_connection() as conn:
            conn.execute("DELETE FROM regime_config WHERE id = ?", (window_id,))

    # ── Phase III: scrape_queue CRUD ──────────────────────────────────────────

    def enqueue_scrape_job(self, ticker: str, mode: str) -> dict:
        """Enqueue a scrape job. Raises ValueError if company scraper_mode is 'skip'."""
        company = self.get_company(ticker)
        if company and company.get('scraper_mode', 'skip') == 'skip':
            raise ValueError(
                f"Scrape skipped — scraper_mode is 'skip' for {ticker}. "
                f"Update scraper_mode before triggering a scrape."
            )
        with self._get_connection() as conn:
            cursor = conn.execute(
                "INSERT INTO scrape_queue (ticker, mode) VALUES (?, ?)",
                (ticker, mode),
            )
            row = conn.execute(
                "SELECT * FROM scrape_queue WHERE id = ?", (cursor.lastrowid,)
            ).fetchone()
            return dict(row)

    def get_pending_scrape_jobs(self) -> list:
        """Return all pending scrape jobs in FIFO order."""
        with self._get_connection() as conn:
            rows = conn.execute(
                "SELECT * FROM scrape_queue WHERE status = 'pending' ORDER BY created_at ASC"
            ).fetchall()
            return [dict(r) for r in rows]

    def claim_scrape_job(self, job_id: int) -> None:
        """Mark a job as running with the current timestamp."""
        with self._get_connection() as conn:
            conn.execute(
                "UPDATE scrape_queue SET status='running', started_at=datetime('now') "
                "WHERE id = ? AND status = 'pending'",
                (job_id,),
            )

    def complete_scrape_job(self, job_id: int) -> None:
        """Mark a job as done."""
        with self._get_connection() as conn:
            conn.execute(
                "UPDATE scrape_queue SET status='done', completed_at=datetime('now') WHERE id = ?",
                (job_id,),
            )

    def fail_scrape_job(self, job_id: int, error_msg: str) -> None:
        """Mark a job as failed with an error message."""
        with self._get_connection() as conn:
            conn.execute(
                "UPDATE scrape_queue SET status='error', completed_at=datetime('now'), "
                "error_msg=? WHERE id = ?",
                (error_msg, job_id),
            )

    def reset_interrupted_scrape_jobs(self) -> int:
        """Reset any jobs left in 'running' state back to 'pending'. Returns count reset.

        Called on server startup to handle jobs orphaned by a previous process crash
        (Anti-pattern #28 mitigation).
        """
        with self._get_connection() as conn:
            cursor = conn.execute(
                "UPDATE scrape_queue SET status='pending', started_at=NULL WHERE status='running'"
            )
            return cursor.rowcount

    def get_scrape_queue_status(self) -> list:
        """Return recent scrape queue rows joined with company scraper_status. Limit 50."""
        with self._get_connection() as conn:
            rows = conn.execute(
                """SELECT sq.*, c.scraper_status
                   FROM scrape_queue sq
                   JOIN companies c ON sq.ticker = c.ticker
                   ORDER BY sq.created_at DESC LIMIT 50"""
            ).fetchall()
            return [dict(r) for r in rows]

    # ── Phase IV: overnight pipeline run tracking ────────────────────────────

    def create_pipeline_run(
        self,
        triggered_by: str = 'ops_ui',
        scope: Optional[dict] = None,
        config: Optional[dict] = None,
    ) -> dict:
        """Create a pipeline run row and return it."""
        with self._get_connection() as conn:
            cursor = conn.execute(
                """INSERT INTO pipeline_runs (triggered_by, status, scope_json, config_json)
                   VALUES (?, 'queued', ?, ?)""",
                (
                    triggered_by,
                    json.dumps(scope or {}, ensure_ascii=False),
                    json.dumps(config or {}, ensure_ascii=False),
                ),
            )
            row = conn.execute(
                "SELECT * FROM pipeline_runs WHERE id = ?",
                (cursor.lastrowid,),
            ).fetchone()
            return self._deserialize_pipeline_run_row(dict(row))

    def update_pipeline_run(
        self,
        run_id: int,
        *,
        status: Optional[str] = None,
        ended_at: Optional[str] = None,
        summary: Optional[dict] = None,
        error: Optional[str] = None,
    ) -> Optional[dict]:
        """Update pipeline_runs fields and return the latest row."""
        updates = []
        values = []
        if status is not None:
            updates.append("status = ?")
            values.append(status)
        if ended_at is not None:
            updates.append("ended_at = ?")
            values.append(ended_at)
        if summary is not None:
            updates.append("summary_json = ?")
            values.append(json.dumps(summary, ensure_ascii=False))
        if error is not None:
            updates.append("error = ?")
            values.append(error)
        if updates:
            values.append(run_id)
            with self._get_connection() as conn:
                conn.execute(
                    f"UPDATE pipeline_runs SET {', '.join(updates)} WHERE id = ?",
                    values,
                )
        return self.get_pipeline_run(run_id)

    def get_pipeline_run(self, run_id: int) -> Optional[dict]:
        """Return one pipeline run row with decoded JSON payloads."""
        with self._get_connection() as conn:
            row = conn.execute(
                "SELECT * FROM pipeline_runs WHERE id = ?",
                (run_id,),
            ).fetchone()
            if row is None:
                return None
            return self._deserialize_pipeline_run_row(dict(row))

    def add_pipeline_run_event(
        self,
        run_id: int,
        stage: str,
        event: str,
        *,
        ticker: Optional[str] = None,
        level: str = 'INFO',
        details: Optional[dict] = None,
    ) -> int:
        """Append one structured event row and return event id."""
        with self._get_connection() as conn:
            cursor = conn.execute(
                """INSERT INTO pipeline_run_events
                   (run_id, stage, event, ticker, level, details_json)
                   VALUES (?, ?, ?, ?, ?, ?)""",
                (
                    run_id,
                    stage,
                    event,
                    ticker,
                    level,
                    json.dumps(details or {}, ensure_ascii=False),
                ),
            )
            return int(cursor.lastrowid)

    def list_pipeline_run_events(self, run_id: int, limit: int = 500) -> list:
        """Return run events in ascending insertion order."""
        limit = max(1, min(int(limit), 5000))
        with self._get_connection() as conn:
            rows = conn.execute(
                """SELECT * FROM pipeline_run_events
                   WHERE run_id = ?
                   ORDER BY id ASC
                   LIMIT ?""",
                (run_id, limit),
            ).fetchall()
            events = []
            for row in rows:
                d = dict(row)
                try:
                    d['details'] = json.loads(d.get('details_json') or '{}')
                except Exception:
                    d['details'] = {}
                events.append(d)
            return events

    def upsert_pipeline_run_ticker(self, run_id: int, ticker: str, **fields) -> None:
        """Insert or update per-ticker run status fields."""
        allowed = {
            'targeted', 'probed', 'mode_applied', 'scraped',
            'ingested', 'extracted', 'failed_reason',
        }
        clean = {k: v for k, v in fields.items() if k in allowed}
        with self._get_connection() as conn:
            conn.execute(
                "INSERT OR IGNORE INTO pipeline_run_tickers (run_id, ticker) VALUES (?, ?)",
                (run_id, ticker),
            )
            if clean:
                set_clause = ", ".join(f"{k}=?" for k in clean.keys())
                values = list(clean.values()) + [run_id, ticker]
                conn.execute(
                    f"UPDATE pipeline_run_tickers SET {set_clause} WHERE run_id=? AND ticker=?",
                    values,
                )

    def list_pipeline_run_tickers(self, run_id: int) -> list:
        """Return per-ticker run records."""
        with self._get_connection() as conn:
            rows = conn.execute(
                "SELECT * FROM pipeline_run_tickers WHERE run_id = ? ORDER BY ticker",
                (run_id,),
            ).fetchall()
            return [dict(r) for r in rows]

    @staticmethod
    def _deserialize_pipeline_run_row(row: dict) -> dict:
        """Decode JSON columns on a pipeline_runs row."""
        for col, key in (
            ('scope_json', 'scope'),
            ('config_json', 'config'),
            ('summary_json', 'summary'),
        ):
            raw = row.get(col)
            try:
                row[key] = json.loads(raw) if raw else {}
            except Exception:
                row[key] = {}
        return row

    # ── Phase III: metric_schema CRUD ─────────────────────────────────────────

    def get_metric_schema(self, sector: str) -> list:
        """Return all metric schema rows for a sector, ordered by key."""
        with self._get_connection() as conn:
            rows = conn.execute(
                "SELECT * FROM metric_schema WHERE sector = ? ORDER BY key ASC",
                (sector,),
            ).fetchall()
            return [dict(r) for r in rows]

    def add_analyst_metric(self, key: str, label: str, unit: str, sector: str) -> dict:
        """Add an analyst-defined metric to the schema. Raises IntegrityError on duplicate key+sector."""
        with self._get_connection() as conn:
            cursor = conn.execute(
                """INSERT INTO metric_schema (key, label, unit, sector, has_extraction_pattern, analyst_defined)
                   VALUES (?, ?, ?, ?, 0, 1)""",
                (key, label, unit, sector),
            )
            row = conn.execute(
                "SELECT * FROM metric_schema WHERE id = ?", (cursor.lastrowid,)
            ).fetchone()
            return dict(row)

    # ── Phase III: extended company CRUD ─────────────────────────────────────

    def update_company_scraper_fields(
        self,
        ticker: str,
        scraper_status: str = None,
        scraper_mode: str = None,
        last_scrape_at: str = None,
        last_scrape_error: str = None,
        probe_completed_at: str = None,
    ) -> None:
        """Update one or more scraper-related fields on a company row.

        Only non-None arguments are included in the UPDATE statement.
        """
        field_map = {
            'scraper_status': scraper_status,
            'scraper_mode': scraper_mode,
            'last_scrape_at': last_scrape_at,
            'last_scrape_error': last_scrape_error,
            'probe_completed_at': probe_completed_at,
        }
        updates = {k: v for k, v in field_map.items() if v is not None}
        if not updates:
            return
        set_clause = ', '.join(f"{k} = ?" for k in updates)
        values = list(updates.values()) + [ticker]
        with self._get_connection() as conn:
            conn.execute(
                f"UPDATE companies SET {set_clause} WHERE ticker = ?", values
            )

    def update_company_config(self, ticker: str, **kwargs) -> dict:
        """Update editable company fields. Returns updated company dict."""
        allowed = {
            'name', 'ir_url', 'pr_base_url', 'scraper_mode', 'scraper_issues_log', 'cik', 'sector',
            'rss_url', 'url_template', 'pr_start_year', 'skip_reason', 'sandbox_note',
            'prnewswire_url', 'globenewswire_url',
        }
        updates = {k: v for k, v in kwargs.items() if k in allowed}
        if updates:
            set_clause = ', '.join(f"{k} = ?" for k in updates)
            values = list(updates.values()) + [ticker]
            with self._get_connection() as conn:
                conn.execute(
                    f"UPDATE companies SET {set_clause} WHERE ticker = ?", values
                )
        return self.get_company(ticker)

    def get_scraper_governance_snapshot(self, stale_days: int = 30) -> dict:
        """Return scraper governance status by ticker.

        A ticker is considered stale when no source_audit check has been recorded
        in the last `stale_days`.
        """
        stale_days = max(1, int(stale_days))
        with self._get_connection() as conn:
            rows = conn.execute(
                """
                SELECT
                  c.ticker,
                  c.active,
                  c.scraper_mode,
                  c.skip_reason,
                  c.ir_url,
                  c.rss_url,
                  c.url_template,
                  c.pr_start_year,
                  c.probe_completed_at,
                  MAX(sa.last_checked) AS last_audit_checked,
                  COUNT(sa.id) AS source_audit_rows,
                  SUM(CASE WHEN sa.status = 'ACTIVE' THEN 1 ELSE 0 END) AS active_source_rows
                FROM companies c
                LEFT JOIN source_audit sa ON sa.ticker = c.ticker
                GROUP BY c.ticker
                ORDER BY c.ticker
                """
            ).fetchall()

        def _is_stale(last_checked: Optional[str]) -> bool:
            if not last_checked:
                return True
            # Accept ISO strings; lexical compare works for YYYY-MM-DDTHH:MM:SS.
            cutoff = datetime.now(timezone.utc).timestamp() - (stale_days * 86400)
            try:
                ts = datetime.fromisoformat(str(last_checked).replace('Z', '+00:00')).timestamp()
                return ts < cutoff
            except Exception:
                return True

        items = []
        for raw in rows:
            r = dict(raw)
            mode = (r.get('scraper_mode') or 'skip').strip().lower()
            active_sources = int(r.get('active_source_rows') or 0)
            stale = _is_stale(r.get('last_audit_checked'))
            is_active = bool(r.get('active'))

            if mode == 'skip':
                if active_sources > 0:
                    governance = 'skip_conflict_active_source'
                elif stale:
                    governance = 'stale_skip'
                else:
                    governance = 'skip_verified'
            else:
                governance = 'needs_probe' if stale else 'configured'

            items.append({
                'ticker': r['ticker'],
                'active': is_active,
                'scraper_mode': mode,
                'skip_reason': r.get('skip_reason'),
                'last_audit_checked': r.get('last_audit_checked'),
                'source_audit_rows': int(r.get('source_audit_rows') or 0),
                'active_source_rows': active_sources,
                'stale': stale,
                'governance_status': governance,
            })

        return {
            'stale_days': stale_days,
            'total': len(items),
            'needs_probe': sum(1 for x in items if x['governance_status'] == 'needs_probe'),
            'stale_skip': sum(1 for x in items if x['governance_status'] == 'stale_skip'),
            'skip_conflict_active_source': sum(1 for x in items if x['governance_status'] == 'skip_conflict_active_source'),
            'items': items,
        }

    def add_company(
        self, ticker: str, name: str, tier: int = 2, ir_url: str = '',
        sector: str = 'BTC-miners', scraper_mode: str = 'skip',
        pr_base_url: str = None, cik: str = None,
        scraper_issues_log: str = '', active: int = 1,
        rss_url: str = None, url_template: str = None,
        pr_start_year: int = None, skip_reason: str = None,
        sandbox_note: str = None,
        prnewswire_url: str = None, globenewswire_url: str = None,
    ) -> dict:
        """Add a new company. Returns the new company row as a dict."""
        with self._get_connection() as conn:
            conn.execute(
                """INSERT INTO companies
                   (ticker, name, tier, ir_url, pr_base_url, cik, active, sector, scraper_mode, scraper_issues_log,
                    rss_url, url_template, pr_start_year, skip_reason, sandbox_note,
                    prnewswire_url, globenewswire_url)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (ticker, name, tier, ir_url, pr_base_url, cik, active,
                 sector, scraper_mode, scraper_issues_log,
                 rss_url, url_template, pr_start_year, skip_reason, sandbox_note,
                 prnewswire_url, globenewswire_url),
            )
        return self.get_company(ticker)

    # ── raw_extractions CRUD ──────────────────────────────────────────────────

    def upsert_raw_extraction(self, rex: dict) -> int:
        """Insert or replace a raw_extraction row.

        rex keys: report_id, ticker, period, metric_key, category, value,
                  value_text, unit, description, raw_json, confidence,
                  source_snippet, extraction_method.
        UNIQUE constraint on (report_id, metric_key) — last writer wins.
        """
        with self._get_connection() as conn:
            cursor = conn.execute(
                """INSERT OR REPLACE INTO raw_extractions
                   (report_id, ticker, period, metric_key, category, value,
                    value_text, unit, description, raw_json, confidence,
                    source_snippet, extraction_method)
                   VALUES (:report_id, :ticker, :period, :metric_key, :category,
                           :value, :value_text, :unit, :description, :raw_json,
                           :confidence, :source_snippet, :extraction_method)""",
                {
                    'report_id':         rex['report_id'],
                    'ticker':            rex['ticker'],
                    'period':            rex['period'],
                    'metric_key':        rex['metric_key'],
                    'category':          rex.get('category', 'unknown'),
                    'value':             rex.get('value'),
                    'value_text':        rex.get('value_text', str(rex.get('value', ''))),
                    'unit':              rex.get('unit'),
                    'description':       rex.get('description'),
                    'raw_json':          rex.get('raw_json'),
                    'confidence':        rex.get('confidence', 0.0),
                    'source_snippet':    rex.get('source_snippet'),
                    'extraction_method': rex.get('extraction_method', 'llm_broad'),
                },
            )
            return cursor.lastrowid

    def get_raw_extractions(
        self,
        ticker: Optional[str] = None,
        period: Optional[str] = None,
        category: Optional[str] = None,
        metric_key: Optional[str] = None,
    ) -> list:
        """Return raw_extraction rows with optional filters."""
        clauses, params = [], []
        if ticker:
            clauses.append("ticker = ?"); params.append(ticker)
        if period:
            clauses.append("period = ?"); params.append(period)
        if category:
            clauses.append("category = ?"); params.append(category)
        if metric_key:
            clauses.append("metric_key = ?"); params.append(metric_key)
        where = ("WHERE " + " AND ".join(clauses)) if clauses else ""
        with self._get_connection() as conn:
            rows = conn.execute(
                f"SELECT * FROM raw_extractions {where} ORDER BY ticker, period, metric_key",
                params,
            ).fetchall()
            return [dict(r) for r in rows]

    def get_raw_extraction_count(self, ticker: Optional[str] = None) -> int:
        """Return count of raw_extraction rows, optionally filtered by ticker."""
        params = []
        where = ""
        if ticker:
            where = "WHERE ticker = ?"
            params.append(ticker)
        with self._get_connection() as conn:
            row = conn.execute(
                f"SELECT COUNT(*) FROM raw_extractions {where}", params
            ).fetchone()
            return row[0]

    def get_reports_without_broad_extraction(self, ticker: Optional[str] = None) -> list:
        """Return reports that have raw_text but no rows in raw_extractions yet."""
        clauses = ["r.raw_text IS NOT NULL", "r.raw_text != ''"]
        params = []
        if ticker:
            clauses.append("r.ticker = ?")
            params.append(ticker)
        where = "WHERE " + " AND ".join(clauses)
        with self._get_connection() as conn:
            rows = conn.execute(
                f"""SELECT r.* FROM reports r
                    LEFT JOIN raw_extractions rex ON rex.report_id = r.id
                    {where}
                    AND rex.id IS NULL
                    ORDER BY r.ticker, r.report_date""",
                params,
            ).fetchall()
            return [dict(r) for r in rows]


def _metric_unit(metric: str) -> str:
    """Return canonical unit string for a metric name."""
    units = {
        "production_btc":         "BTC",
        "hodl_btc":               "BTC",
        "sold_btc":               "BTC",
        "hashrate_eh":            "EH/s",
        "realization_rate":       "ratio",
        # v2 metrics
        "net_btc_balance_change": "BTC",
        "encumbered_btc":         "BTC",
        "mining_mw":              "MW",
        "ai_hpc_mw":              "MW",
        "hpc_revenue_usd":        "USD",
        "gpu_count":              "units",
    }
    return units.get(metric, "")
