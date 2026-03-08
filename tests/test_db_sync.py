"""T3 — sync_companies_from_config respects the active flag in companies.json."""
import json
import os
import pytest
from pathlib import Path


def _write_companies_json(tmp_path: Path, companies: list) -> Path:
    p = tmp_path / 'companies.json'
    p.write_text(json.dumps(companies))
    return p


@pytest.fixture
def db_no_sync(tmp_path):
    """Fresh MinerDB with auto-sync disabled so tests control the companies file."""
    os.environ['MINERS_AUTO_SYNC_COMPANIES'] = '0'
    from infra.db import MinerDB
    yield MinerDB(str(tmp_path / 'test.db'))
    os.environ.pop('MINERS_AUTO_SYNC_COMPANIES', None)


def test_active_company_synced(db_no_sync, tmp_path):
    """Active company in JSON appears in DB after sync."""
    companies_json = _write_companies_json(tmp_path, [
        {'ticker': 'MARA', 'name': 'MARA Holdings', 'active': True, 'cik': '0001437491'},
    ])
    db_no_sync.sync_companies_from_config(str(companies_json))
    rows = db_no_sync.get_companies(active_only=False)
    assert any(r['ticker'] == 'MARA' for r in rows)


def test_inactive_company_not_active_in_db(db_no_sync, tmp_path):
    """Company with active=false in JSON has active=0 in DB after sync."""
    companies_json = _write_companies_json(tmp_path, [
        {'ticker': 'MARA', 'name': 'MARA Holdings', 'active': False, 'cik': '0001437491'},
    ])
    db_no_sync.sync_companies_from_config(str(companies_json))
    rows = db_no_sync.get_companies(active_only=False)
    mara = next((r for r in rows if r['ticker'] == 'MARA'), None)
    assert mara is not None, 'MARA row should exist in DB even when inactive'
    assert mara.get('active') in (0, False), 'inactive JSON entry must produce active=0 in DB'


def test_inactive_company_excluded_from_active_only(db_no_sync, tmp_path):
    """get_companies(active_only=True) excludes companies with active=false."""
    companies_json = _write_companies_json(tmp_path, [
        {'ticker': 'MARA', 'name': 'MARA Holdings', 'active': False, 'cik': '0001437491'},
        {'ticker': 'RIOT', 'name': 'Riot Platforms', 'active': True, 'cik': '0001167419'},
    ])
    db_no_sync.sync_companies_from_config(str(companies_json))
    active_rows = db_no_sync.get_companies(active_only=True)
    active_tickers = [r['ticker'] for r in active_rows]
    assert 'RIOT' in active_tickers
    assert 'MARA' not in active_tickers


def test_reactivation_restores_company(db_no_sync, tmp_path):
    """Setting active back to true on a previously inactive company makes it active."""
    companies_json = _write_companies_json(tmp_path, [
        {'ticker': 'MARA', 'name': 'MARA Holdings', 'active': False, 'cik': '0001437491'},
    ])
    db_no_sync.sync_companies_from_config(str(companies_json))
    # Confirm inactive first
    assert not any(r['ticker'] == 'MARA' for r in db_no_sync.get_companies(active_only=True))
    # Re-activate
    companies_json.write_text(json.dumps([
        {'ticker': 'MARA', 'name': 'MARA Holdings', 'active': True, 'cik': '0001437491'},
    ]))
    db_no_sync.sync_companies_from_config(str(companies_json))
    active_rows = db_no_sync.get_companies(active_only=True)
    assert any(r['ticker'] == 'MARA' for r in active_rows)
