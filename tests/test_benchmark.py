"""Unit tests for llm_benchmark_runs CRUD (insert_benchmark_run, get_benchmark_runs,
get_benchmark_summary). Uses a fresh tmp_path DB via the conftest `db` fixture."""
import pytest


# ── Helpers ──────────────────────────────────────────────────────────────────

def _make_run(**overrides):
    """Build a benchmark run dict with sensible defaults + overrides."""
    defaults = {
        'model': 'test-model',
        'call_type': 'batch',
        'ticker': 'MARA',
        'period': '2024-01-01',
        'report_id': 1,
        'prompt_chars': 5000,
        'response_chars': 300,
        'prompt_tokens': 1200,
        'response_tokens': 75,
        'total_duration_ms': 8000.0,
        'eval_duration_ms': 7500.0,
        'metrics_requested': 6,
        'metrics_extracted': 5,
        'hits_90': 4,
        'hits_80': 5,
        'hits_75': 5,
    }
    defaults.update(overrides)
    return defaults


# ── insert_benchmark_run ─────────────────────────────────────────────────────

class TestInsertBenchmarkRun:
    def test_returns_row_id(self, db):
        row_id = db.insert_benchmark_run(_make_run())
        assert isinstance(row_id, int)
        assert row_id > 0

    def test_inserts_row_retrievable(self, db):
        db.insert_benchmark_run(_make_run(ticker='RIOT', model='model-A'))
        rows = db.get_benchmark_runs()
        assert len(rows) == 1
        assert rows[0]['ticker'] == 'RIOT'
        assert rows[0]['model'] == 'model-A'

    def test_inserts_all_fields(self, db):
        db.insert_benchmark_run(_make_run(
            prompt_tokens=999,
            response_tokens=42,
            total_duration_ms=1234.5,
            eval_duration_ms=1100.0,
            hits_90=3,
            hits_80=4,
            hits_75=5,
        ))
        row = db.get_benchmark_runs()[0]
        assert row['prompt_tokens'] == 999
        assert row['response_tokens'] == 42
        assert abs(row['total_duration_ms'] - 1234.5) < 0.01
        assert row['hits_90'] == 3
        assert row['hits_80'] == 4
        assert row['hits_75'] == 5

    def test_defaults_zero_for_missing_numeric_fields(self, db):
        """Minimal dict — numeric fields default to 0."""
        db.insert_benchmark_run({'model': 'x', 'call_type': 'batch'})
        row = db.get_benchmark_runs()[0]
        assert row['prompt_tokens'] == 0
        assert row['response_tokens'] == 0
        assert row['hits_90'] == 0

    def test_multiple_rows_inserted_independently(self, db):
        db.insert_benchmark_run(_make_run(ticker='MARA'))
        db.insert_benchmark_run(_make_run(ticker='RIOT'))
        db.insert_benchmark_run(_make_run(ticker='CLSK', call_type='gap_fill'))
        rows = db.get_benchmark_runs()
        assert len(rows) == 3

    def test_call_type_gap_fill_stored(self, db):
        db.insert_benchmark_run(_make_run(call_type='gap_fill', period='2023-12-01'))
        row = db.get_benchmark_runs()[0]
        assert row['call_type'] == 'gap_fill'
        assert row['period'] == '2023-12-01'


# ── get_benchmark_runs ────────────────────────────────────────────────────────

class TestGetBenchmarkRuns:
    def test_empty_db_returns_empty_list(self, db):
        assert db.get_benchmark_runs() == []

    def test_filter_by_ticker(self, db):
        db.insert_benchmark_run(_make_run(ticker='MARA'))
        db.insert_benchmark_run(_make_run(ticker='RIOT'))
        rows = db.get_benchmark_runs(ticker='MARA')
        assert len(rows) == 1
        assert rows[0]['ticker'] == 'MARA'

    def test_filter_by_model(self, db):
        db.insert_benchmark_run(_make_run(model='model-A'))
        db.insert_benchmark_run(_make_run(model='model-B'))
        rows = db.get_benchmark_runs(model='model-A')
        assert len(rows) == 1
        assert rows[0]['model'] == 'model-A'

    def test_filter_by_ticker_and_model(self, db):
        db.insert_benchmark_run(_make_run(ticker='MARA', model='model-A'))
        db.insert_benchmark_run(_make_run(ticker='RIOT', model='model-A'))
        db.insert_benchmark_run(_make_run(ticker='MARA', model='model-B'))
        rows = db.get_benchmark_runs(ticker='MARA', model='model-A')
        assert len(rows) == 1

    def test_limit_applied(self, db):
        for i in range(10):
            db.insert_benchmark_run(_make_run(period=f'2024-{i+1:02d}-01'))
        rows = db.get_benchmark_runs(limit=3)
        assert len(rows) == 3

    def test_returns_newest_first(self, db):
        db.insert_benchmark_run(_make_run(ticker='MARA'))
        db.insert_benchmark_run(_make_run(ticker='RIOT'))
        rows = db.get_benchmark_runs()
        # Newest inserted has highest id → should appear first
        assert rows[0]['ticker'] == 'RIOT'

    def test_no_filter_returns_all(self, db):
        db.insert_benchmark_run(_make_run(ticker='MARA'))
        db.insert_benchmark_run(_make_run(ticker='RIOT'))
        assert len(db.get_benchmark_runs()) == 2


# ── get_benchmark_summary ─────────────────────────────────────────────────────

class TestGetBenchmarkSummary:
    def test_empty_db_returns_empty_list(self, db):
        assert db.get_benchmark_summary() == []

    def test_single_model_aggregated(self, db):
        db.insert_benchmark_run(_make_run(
            model='model-A', call_type='batch',
            metrics_requested=6, hits_90=4, hits_80=5, hits_75=6,
            prompt_tokens=1000, response_tokens=80,
            total_duration_ms=8000.0,
        ))
        rows = db.get_benchmark_summary()
        assert len(rows) == 1
        row = rows[0]
        assert row['model'] == 'model-A'
        assert row['run_count'] == 1
        assert row['avg_prompt_tokens'] == 1000.0
        assert row['avg_response_tokens'] == 80.0
        assert abs(row['avg_hit_rate_90'] - (4 / 6)) < 0.001
        assert abs(row['avg_hit_rate_80'] - (5 / 6)) < 0.001
        assert abs(row['avg_hit_rate_75'] - (6 / 6)) < 0.001

    def test_multiple_models_separate_rows(self, db):
        db.insert_benchmark_run(_make_run(model='model-A', call_type='batch'))
        db.insert_benchmark_run(_make_run(model='model-B', call_type='batch'))
        rows = db.get_benchmark_summary()
        models = {r['model'] for r in rows}
        assert 'model-A' in models
        assert 'model-B' in models

    def test_same_model_different_call_types_separate_rows(self, db):
        db.insert_benchmark_run(_make_run(model='X', call_type='batch'))
        db.insert_benchmark_run(_make_run(model='X', call_type='gap_fill'))
        rows = db.get_benchmark_summary()
        assert len(rows) == 2
        call_types = {r['call_type'] for r in rows}
        assert call_types == {'batch', 'gap_fill'}

    def test_run_count_aggregated(self, db):
        for _ in range(5):
            db.insert_benchmark_run(_make_run(model='model-A', call_type='batch'))
        rows = db.get_benchmark_summary()
        assert rows[0]['run_count'] == 5

    def test_hit_rate_averaged_across_runs(self, db):
        db.insert_benchmark_run(_make_run(
            model='M', call_type='batch', metrics_requested=4, hits_90=2
        ))
        db.insert_benchmark_run(_make_run(
            model='M', call_type='batch', metrics_requested=4, hits_90=4
        ))
        rows = db.get_benchmark_summary()
        # avg hit_rate_90 = (2/4 + 4/4) / 2 = (0.5 + 1.0) / 2 = 0.75
        assert abs(rows[0]['avg_hit_rate_90'] - 0.75) < 0.001
