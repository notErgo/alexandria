"""
SQLite persistence layer for the Bitcoin Miner Data Platform.

Uses WAL mode for concurrent reads during Flask operation.
Schema version tracked via PRAGMA user_version (current: 1).
All writes go through context managers (auto-commit on __exit__).
"""
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

    # ── Company CRUD ─────────────────────────────────────────────────────────

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
        with self._get_connection() as conn:
            cursor = conn.execute(
                """INSERT INTO reports
                   (ticker, report_date, published_date, source_type, source_url, raw_text, parsed_at)
                   VALUES (:ticker, :report_date, :published_date, :source_type,
                           :source_url, :raw_text, :parsed_at)""",
                report,
            )
            return cursor.lastrowid

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

    def mark_report_extracted(self, report_id: int) -> None:
        """Set extracted_at timestamp on a report after the extraction pipeline runs."""
        with self._get_connection() as conn:
            conn.execute(
                "UPDATE reports SET extracted_at = ? WHERE id = ?",
                (datetime.utcnow().isoformat(), report_id),
            )

    def get_unextracted_reports(self, ticker: Optional[str] = None) -> list:
        """Return reports with raw_text that have not yet been through the extraction pipeline."""
        clauses = ["raw_text IS NOT NULL", "raw_text != ''", "extracted_at IS NULL"]
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

    def delete_report(self, ticker: str, report_date: str, source_type: str) -> int:
        """
        Delete a report and all its associated data_points and review_queue items.
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
            # Collect all periods covered by this report's data_points BEFORE deleting them.
            # review_queue has no report_id column; items are linked by ticker+period.
            period_rows = conn.execute(
                "SELECT DISTINCT period FROM data_points WHERE report_id=?", (report_id,)
            ).fetchall()
            periods = [r[0] for r in period_rows] or [report_date]
            for p in periods:
                conn.execute(
                    "DELETE FROM review_queue WHERE ticker=? AND period=?", (ticker, p)
                )
            conn.execute("DELETE FROM data_points WHERE report_id=?", (report_id,))
            conn.execute("DELETE FROM reports WHERE id=?", (report_id,))
            return 1

    # ── DataPoint CRUD ───────────────────────────────────────────────────────

    def insert_data_point(self, dp: dict) -> int:
        with self._get_connection() as conn:
            cursor = conn.execute(
                """INSERT OR REPLACE INTO data_points
                   (report_id, ticker, period, metric, value, unit, confidence,
                    extraction_method, source_snippet)
                   VALUES (:report_id, :ticker, :period, :metric, :value, :unit,
                           :confidence, :extraction_method, :source_snippet)""",
                dp,
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
                    source_snippet, status, llm_value, regex_value, agreement_status)
                   VALUES (:data_point_id, :ticker, :period, :metric, :raw_value,
                           :confidence, :source_snippet, :status,
                           :llm_value, :regex_value, :agreement_status)""",
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
        self, status: Optional[str] = None, limit: int = 50, offset: int = 0
    ) -> list:
        if status:
            sql = "SELECT * FROM review_queue WHERE status = ? ORDER BY created_at DESC LIMIT ? OFFSET ?"
            params = [status, limit, offset]
        else:
            sql = "SELECT * FROM review_queue ORDER BY created_at DESC LIMIT ? OFFSET ?"
            params = [limit, offset]
        with self._get_connection() as conn:
            rows = conn.execute(sql, params).fetchall()
            return [dict(r) for r in rows]

    def find_report_for_period(self, ticker: str, period: str) -> Optional[dict]:
        """Return {id, source_type, source_url} for the first report matching ticker + YYYY-MM."""
        period_ym = period[:7]
        with self._get_connection() as conn:
            rows = conn.execute(
                """SELECT id, source_type, source_url FROM reports
                   WHERE ticker = ? AND report_date LIKE ?
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
                "report_id": None,
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
                "report_id": None,
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
                    'created_at': data.get('created_at', datetime.utcnow().isoformat()),
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
