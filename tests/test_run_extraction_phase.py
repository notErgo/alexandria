"""Tests for run_extraction_phase canonical function."""
import os
import sys
import pytest
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from infra.db import MinerDB
from helpers import make_report


def _make_db(tmp_path, tickers=('MARA',)):
    db = MinerDB(str(tmp_path / 'test.db'))
    for ticker in tickers:
        db.insert_company({
            'ticker': ticker, 'name': ticker, 'tier': 1,
            'ir_url': 'https://example.com', 'pr_base_url': 'https://example.com',
            'cik': '0001437491', 'active': 1,
        })
    return db


def test_run_extraction_phase_delegates_to_extract_reports_for_ticker(monkeypatch, tmp_path):
    import routes.pipeline as pipeline_mod
    db = _make_db(tmp_path)
    run = db.create_pipeline_run(triggered_by='test', scope={}, config={})
    run_id = int(run['id'])
    calls = []

    def _fake_worker(db, run_id, ticker, reports, counters, failures,
                     num_workers, *, run_config=None, force_reextract=False):
        calls.append({'ticker': ticker, 'num_workers': num_workers,
                      'force_reextract': force_reextract})
        counters['processed'] += len(reports)

    monkeypatch.setattr(pipeline_mod, '_extract_reports_for_ticker', _fake_worker)
    monkeypatch.setattr(pipeline_mod, '_build_extraction_batch',
                        lambda db, ticker, first_filing, force_reextract=False: [{'id': 1}])
    monkeypatch.setattr(pipeline_mod, 'prepare_extraction_runtime', lambda *a, **kw: None)

    pipeline_mod.run_extraction_phase(
        db, run_id, ['MARA'],
        extract_workers=3, force_reextract=True,
    )

    assert len(calls) == 1
    assert calls[0]['ticker'] == 'MARA'
    assert calls[0]['num_workers'] == 3
    assert calls[0]['force_reextract'] is True


def test_run_extraction_phase_emits_stage_start_and_end(monkeypatch, tmp_path):
    import routes.pipeline as pipeline_mod
    db = _make_db(tmp_path, tickers=['MARA', 'RIOT'])
    db.insert_company({
        'ticker': 'RIOT', 'name': 'RIOT', 'tier': 1,
        'ir_url': 'https://example.com', 'pr_base_url': 'https://example.com',
        'cik': '0001167419', 'active': 1,
    })
    run = db.create_pipeline_run(triggered_by='test', scope={}, config={})
    run_id = int(run['id'])

    monkeypatch.setattr(pipeline_mod, '_extract_reports_for_ticker',
                        lambda *a, **kw: None)
    monkeypatch.setattr(pipeline_mod, '_build_extraction_batch',
                        lambda db, ticker, first_filing, force_reextract=False: [])

    pipeline_mod.run_extraction_phase(db, run_id, ['MARA', 'RIOT'])

    with db._get_connection() as conn:
        events = conn.execute(
            "SELECT event FROM pipeline_run_events WHERE run_id=? AND stage='extract' ORDER BY id",
            (run_id,)
        ).fetchall()
    event_names = [e['event'] for e in events]
    assert event_names[0] == 'stage_start'
    assert event_names[-1] == 'stage_end'


def test_run_extraction_phase_emits_ticker_preflight_per_ticker(monkeypatch, tmp_path):
    import routes.pipeline as pipeline_mod
    db = _make_db(tmp_path, tickers=['MARA'])
    db.insert_company({'ticker': 'RIOT', 'name': 'RIOT', 'tier': 1,
                       'ir_url': 'https://example.com', 'pr_base_url': 'https://example.com',
                       'cik': '0001167419', 'active': 1})
    run = db.create_pipeline_run(triggered_by='test', scope={}, config={})
    run_id = int(run['id'])

    monkeypatch.setattr(pipeline_mod, '_extract_reports_for_ticker',
                        lambda *a, **kw: None)
    monkeypatch.setattr(pipeline_mod, '_build_extraction_batch',
                        lambda db, ticker, first_filing, force_reextract=False: [])

    pipeline_mod.run_extraction_phase(db, run_id, ['MARA', 'RIOT'])

    with db._get_connection() as conn:
        rows = conn.execute(
            "SELECT ticker FROM pipeline_run_events WHERE run_id=? AND stage='extract' AND event='ticker_preflight' ORDER BY id",
            (run_id,)
        ).fetchall()
    assert [r['ticker'] for r in rows] == ['MARA', 'RIOT']


def test_run_extraction_phase_warms_ollama_once(monkeypatch, tmp_path):
    import routes.pipeline as pipeline_mod
    db = _make_db(tmp_path)
    db.insert_company({'ticker': 'RIOT', 'name': 'RIOT', 'tier': 1,
                       'ir_url': 'https://example.com', 'pr_base_url': 'https://example.com',
                       'cik': '0001167419', 'active': 1})
    run = db.create_pipeline_run(triggered_by='test', scope={}, config={})
    run_id = int(run['id'])
    warmup_calls = []

    monkeypatch.setattr(pipeline_mod, 'prepare_extraction_runtime',
                        lambda db, **kw: warmup_calls.append(kw))
    monkeypatch.setattr(pipeline_mod, '_extract_reports_for_ticker',
                        lambda *a, **kw: None)
    monkeypatch.setattr(pipeline_mod, '_build_extraction_batch',
                        lambda db, ticker, ff, force_reextract=False: [{'id': 1}])

    pipeline_mod.run_extraction_phase(db, run_id, ['MARA', 'RIOT'], warm_model=True)

    assert len(warmup_calls) == 1, f"Expected exactly 1 warmup call, got {len(warmup_calls)}"


def test_run_extraction_phase_skips_warmup_when_warm_model_false(monkeypatch, tmp_path):
    import routes.pipeline as pipeline_mod
    db = _make_db(tmp_path)
    run = db.create_pipeline_run(triggered_by='test', scope={}, config={})
    run_id = int(run['id'])

    def _fail_warmup(*a, **kw):
        raise AssertionError("prepare_extraction_runtime must not be called when warm_model=False")

    monkeypatch.setattr(pipeline_mod, 'prepare_extraction_runtime', _fail_warmup)
    monkeypatch.setattr(pipeline_mod, '_extract_reports_for_ticker',
                        lambda *a, **kw: None)
    monkeypatch.setattr(pipeline_mod, '_build_extraction_batch',
                        lambda db, ticker, ff, force_reextract=False: [{'id': 1}])

    pipeline_mod.run_extraction_phase(db, run_id, ['MARA'], warm_model=False)


def test_run_extraction_phase_empty_tickers_returns_zero_counters(monkeypatch, tmp_path):
    import routes.pipeline as pipeline_mod
    db = _make_db(tmp_path)
    run = db.create_pipeline_run(triggered_by='test', scope={}, config={})
    run_id = int(run['id'])

    def _fail_warmup(*a, **kw):
        raise AssertionError("No warmup for empty ticker list")

    monkeypatch.setattr(pipeline_mod, 'prepare_extraction_runtime', _fail_warmup)

    result = pipeline_mod.run_extraction_phase(db, run_id, [])

    assert result['total_reports'] == 0
    assert result['processed'] == 0
    assert result['errors'] == 0


def test_run_extraction_phase_cancel_check_stops_early(monkeypatch, tmp_path):
    import routes.pipeline as pipeline_mod
    db = _make_db(tmp_path)
    db.insert_company({'ticker': 'RIOT', 'name': 'RIOT', 'tier': 1,
                       'ir_url': 'https://example.com', 'pr_base_url': 'https://example.com',
                       'cik': '0001167419', 'active': 1})
    run = db.create_pipeline_run(triggered_by='test', scope={}, config={})
    run_id = int(run['id'])
    extracted = []

    def _fake_worker(db, run_id, ticker, reports, counters, failures,
                     num_workers, *, run_config=None, force_reextract=False):
        extracted.append(ticker)
        counters['processed'] += 1

    monkeypatch.setattr(pipeline_mod, '_extract_reports_for_ticker', _fake_worker)
    monkeypatch.setattr(pipeline_mod, '_build_extraction_batch',
                        lambda db, ticker, ff, force_reextract=False: [{'id': 1}])
    monkeypatch.setattr(pipeline_mod, 'prepare_extraction_runtime', lambda *a, **kw: None)

    call_count = [0]
    def _cancel_after_first():
        call_count[0] += 1
        return call_count[0] > 1  # cancel after first ticker

    pipeline_mod.run_extraction_phase(
        db, run_id, ['MARA', 'RIOT'],
        cancel_check=_cancel_after_first,
    )

    assert extracted == ['MARA'], f"Expected only MARA to be extracted, got {extracted}"


def test_run_extraction_phase_progress_callback_called_per_ticker(monkeypatch, tmp_path):
    import routes.pipeline as pipeline_mod
    db = _make_db(tmp_path)
    db.insert_company({'ticker': 'RIOT', 'name': 'RIOT', 'tier': 1,
                       'ir_url': 'https://example.com', 'pr_base_url': 'https://example.com',
                       'cik': '0001167419', 'active': 1})
    run = db.create_pipeline_run(triggered_by='test', scope={}, config={})
    run_id = int(run['id'])
    snapshots = []

    def _fake_worker(db, run_id, ticker, reports, counters, failures,
                     num_workers, *, run_config=None, force_reextract=False):
        counters['processed'] += 1

    monkeypatch.setattr(pipeline_mod, '_extract_reports_for_ticker', _fake_worker)
    monkeypatch.setattr(pipeline_mod, '_build_extraction_batch',
                        lambda db, ticker, ff, force_reextract=False: [{'id': 1}])
    monkeypatch.setattr(pipeline_mod, 'prepare_extraction_runtime', lambda *a, **kw: None)

    pipeline_mod.run_extraction_phase(
        db, run_id, ['MARA', 'RIOT'],
        progress_callback=lambda c: snapshots.append(c['processed']),
    )

    assert snapshots == [1, 2], f"Expected [1, 2] accumulated processed, got {snapshots}"


def test_run_extraction_phase_run_config_factory_called_per_ticker(monkeypatch, tmp_path):
    import routes.pipeline as pipeline_mod
    db = _make_db(tmp_path)
    db.insert_company({'ticker': 'RIOT', 'name': 'RIOT', 'tier': 1,
                       'ir_url': 'https://example.com', 'pr_base_url': 'https://example.com',
                       'cik': '0001167419', 'active': 1})
    run = db.create_pipeline_run(triggered_by='test', scope={}, config={})
    run_id = int(run['id'])
    factory_calls = []
    received_configs = []

    def _factory(ticker):
        factory_calls.append(ticker)
        return f'config_for_{ticker}'

    def _fake_worker(db, run_id, ticker, reports, counters, failures,
                     num_workers, *, run_config=None, force_reextract=False):
        received_configs.append(run_config)

    monkeypatch.setattr(pipeline_mod, '_extract_reports_for_ticker', _fake_worker)
    monkeypatch.setattr(pipeline_mod, '_build_extraction_batch',
                        lambda db, ticker, ff, force_reextract=False: [{'id': 1}])
    monkeypatch.setattr(pipeline_mod, 'prepare_extraction_runtime', lambda *a, **kw: None)

    pipeline_mod.run_extraction_phase(
        db, run_id, ['MARA', 'RIOT'],
        run_config_factory=_factory,
    )

    assert factory_calls == ['MARA', 'RIOT']
    assert received_configs == ['config_for_MARA', 'config_for_RIOT']


def test_run_extraction_phase_source_types_explicit_skips_edgar_date_gate(monkeypatch, tmp_path):
    """When source_types is explicit, use _build_extraction_batch_for_source_types, not _build_extraction_batch."""
    import routes.pipeline as pipeline_mod
    from config import MONTHLY_EXTRACTION_SOURCE_TYPES
    db = _make_db(tmp_path)
    run = db.create_pipeline_run(triggered_by='test', scope={}, config={})
    run_id = int(run['id'])
    build_batch_calls = []
    build_for_source_calls = []

    monkeypatch.setattr(pipeline_mod, '_build_extraction_batch',
                        lambda db, ticker, ff, force_reextract=False:
                            build_batch_calls.append(ticker) or [])
    monkeypatch.setattr(pipeline_mod, '_build_extraction_batch_for_source_types',
                        lambda db, ticker, source_types, force_reextract=False:
                            build_for_source_calls.append((ticker, source_types)) or [])
    monkeypatch.setattr(pipeline_mod, '_extract_reports_for_ticker',
                        lambda *a, **kw: None)

    pipeline_mod.run_extraction_phase(
        db, run_id, ['MARA'],
        source_types=list(MONTHLY_EXTRACTION_SOURCE_TYPES),
    )

    assert build_batch_calls == [], "Should not call _build_extraction_batch when source_types is explicit"
    assert len(build_for_source_calls) == 1
    assert build_for_source_calls[0][0] == 'MARA'


def test_run_extraction_phase_prebuilt_batches_skips_batch_building(monkeypatch, tmp_path):
    import routes.pipeline as pipeline_mod
    db = _make_db(tmp_path)
    run_id = int(db.create_pipeline_run(triggered_by='test', scope={}, config={})['id'])
    build_calls = []
    worker_reports = []

    monkeypatch.setattr(pipeline_mod, '_build_extraction_batch',
                        lambda *a, **kw: build_calls.append(a) or [])

    def _worker(db, run_id, ticker, reports, counters, failures,
                num_workers, *, run_config=None, force_reextract=False):
        worker_reports.extend(reports)

    monkeypatch.setattr(pipeline_mod, '_extract_reports_for_ticker', _worker)
    monkeypatch.setattr(pipeline_mod, 'prepare_extraction_runtime', lambda *a, **kw: None)

    pm_report = {'id': 99, 'ticker': 'MARA'}
    pipeline_mod.run_extraction_phase(db, run_id, ['MARA'],
                                      prebuilt_batches={'MARA': [pm_report]})

    assert build_calls == [], "Must not call _build_extraction_batch when prebuilt_batches supplied"
    assert worker_reports == [pm_report]


def test_run_extraction_phase_default_extract_workers_is_positive(monkeypatch, tmp_path):
    import routes.pipeline as pipeline_mod
    db = _make_db(tmp_path)
    run_id = int(db.create_pipeline_run(triggered_by='test', scope={}, config={})['id'])
    worker_calls = []

    def _worker(db, run_id, ticker, reports, counters, failures,
                num_workers, *, run_config=None, force_reextract=False):
        worker_calls.append(num_workers)

    monkeypatch.setattr(pipeline_mod, '_extract_reports_for_ticker', _worker)
    monkeypatch.setattr(pipeline_mod, '_build_extraction_batch',
                        lambda db, t, ff, force_reextract=False: [{'id': 1}])
    monkeypatch.setattr(pipeline_mod, 'prepare_extraction_runtime', lambda *a, **kw: None)

    pipeline_mod.run_extraction_phase(db, run_id, ['MARA'])

    assert len(worker_calls) == 1, f"Expected exactly 1 worker call, got {worker_calls}"
    assert worker_calls[0] >= 1, f"extract_workers must be >= 1, got {worker_calls[0]}"
