"""
Tests for orchestration.py — TDD.

Covers check_edgar_complete(): EDGAR-first guardrail for IR/archive ingest.
"""
from unittest.mock import MagicMock
from datetime import date


def _mock_db(last_run=None):
    db = MagicMock()
    db.get_last_successful_pipeline_run.return_value = last_run
    return db


# ── check_edgar_complete ──────────────────────────────────────────────────────

def test_check_edgar_complete_returns_true_when_run_exists():
    """Returns True (complete=True) when a successful EDGAR run exists for the ticker."""
    from orchestration import check_edgar_complete
    db = _mock_db(last_run={
        'id': 1,
        'ticker': 'MARA',
        'source': 'edgar',
        'status': 'done',
        'completed_at': '2024-02-01T12:00:00',
    })
    result = check_edgar_complete(db, ticker='MARA')
    assert result.complete is True
    assert result.ticker == 'MARA'


def test_check_edgar_complete_returns_false_when_no_run():
    """Returns False (complete=False) when no successful EDGAR run exists for the ticker."""
    from orchestration import check_edgar_complete
    db = _mock_db(last_run=None)
    result = check_edgar_complete(db, ticker='RIOT')
    assert result.complete is False
    assert result.ticker == 'RIOT'
    assert result.warning is not None  # warning message provided


def test_check_edgar_complete_all_tickers_passes_none():
    """When ticker=None, queries for any EDGAR run (cross-ticker check)."""
    from orchestration import check_edgar_complete
    db = _mock_db(last_run={
        'id': 2,
        'ticker': None,
        'source': 'edgar',
        'status': 'done',
        'completed_at': '2024-01-15T08:00:00',
    })
    result = check_edgar_complete(db, ticker=None)
    db.get_last_successful_pipeline_run.assert_called_with(source='edgar', ticker=None)
    assert result.complete is True
