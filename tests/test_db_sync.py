"""T3 — sync_companies_from_config respects the active flag in companies.json
and auto-purges stale EDGAR reports when a company CIK changes."""
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


# ---------------------------------------------------------------------------
# CIK change: auto-purge stale EDGAR reports
# ---------------------------------------------------------------------------

def _insert_report(db, ticker, source_type, cik=None):
    """Insert a minimal report row; return its id."""
    return db.insert_report({
        'ticker': ticker,
        'source_type': source_type,
        'source_url': f'https://example.com/{ticker}/{source_type}',
        'raw_text': 'bitcoin mined 100 BTC hash rate 5 EH/s',
        'report_date': '2022-01-01',
        'published_date': None,
        'parsed_at': None,
    })


class TestCIKChangePurgesEdgarReports:

    def test_cik_change_purges_edgar_reports(self, db_no_sync, tmp_path):
        """When CIK changes, all edgar_* reports for that ticker are deleted."""
        cfg = _write_companies_json(tmp_path, [
            {'ticker': 'CORZ', 'name': 'Core Scientific', 'active': True, 'cik': '0001836012'},
        ])
        db_no_sync.sync_companies_from_config(str(cfg))
        _insert_report(db_no_sync, 'CORZ', 'edgar_8k')
        _insert_report(db_no_sync, 'CORZ', 'edgar_10q')

        with db_no_sync._get_connection() as conn:
            before = conn.execute("SELECT COUNT(*) FROM reports WHERE ticker='CORZ'").fetchone()[0]
        assert before == 2

        cfg.write_text(json.dumps([
            {'ticker': 'CORZ', 'name': 'Core Scientific', 'active': True, 'cik': '0001839341'},
        ]))
        db_no_sync.sync_companies_from_config(str(cfg))

        with db_no_sync._get_connection() as conn:
            after = conn.execute("SELECT COUNT(*) FROM reports WHERE ticker='CORZ'").fetchone()[0]
        assert after == 0, f"Expected 0 EDGAR reports after CIK change, got {after}"

    def test_cik_change_resets_btc_first_filing_date(self, db_no_sync, tmp_path):
        """When CIK changes, btc_first_filing_date is reset to NULL."""
        cfg = _write_companies_json(tmp_path, [
            {'ticker': 'CORZ', 'name': 'Core Scientific', 'active': True, 'cik': '0001836012'},
        ])
        db_no_sync.sync_companies_from_config(str(cfg))
        with db_no_sync._get_connection() as conn:
            conn.execute("UPDATE companies SET btc_first_filing_date='2019-01-15' WHERE ticker='CORZ'")

        cfg.write_text(json.dumps([
            {'ticker': 'CORZ', 'name': 'Core Scientific', 'active': True, 'cik': '0001839341'},
        ]))
        db_no_sync.sync_companies_from_config(str(cfg))

        with db_no_sync._get_connection() as conn:
            row = conn.execute("SELECT btc_first_filing_date FROM companies WHERE ticker='CORZ'").fetchone()
        assert row['btc_first_filing_date'] is None, "btc_first_filing_date must be NULL after CIK change"

    def test_cik_unchanged_preserves_edgar_reports(self, db_no_sync, tmp_path):
        """When CIK is unchanged, existing EDGAR reports are not deleted."""
        cfg = _write_companies_json(tmp_path, [
            {'ticker': 'MARA', 'name': 'MARA Holdings', 'active': True, 'cik': '0001507605'},
        ])
        db_no_sync.sync_companies_from_config(str(cfg))
        _insert_report(db_no_sync, 'MARA', 'edgar_8k')

        db_no_sync.sync_companies_from_config(str(cfg))

        with db_no_sync._get_connection() as conn:
            count = conn.execute("SELECT COUNT(*) FROM reports WHERE ticker='MARA'").fetchone()[0]
        assert count == 1, "EDGAR reports must be preserved when CIK is unchanged"

    def test_cik_change_preserves_non_edgar_reports(self, db_no_sync, tmp_path):
        """CIK change only deletes edgar_* reports; IR and archive reports survive."""
        cfg = _write_companies_json(tmp_path, [
            {'ticker': 'CORZ', 'name': 'Core Scientific', 'active': True, 'cik': '0001836012'},
        ])
        db_no_sync.sync_companies_from_config(str(cfg))
        _insert_report(db_no_sync, 'CORZ', 'edgar_8k')
        _insert_report(db_no_sync, 'CORZ', 'ir_press_release')
        _insert_report(db_no_sync, 'CORZ', 'archive_pdf')

        cfg.write_text(json.dumps([
            {'ticker': 'CORZ', 'name': 'Core Scientific', 'active': True, 'cik': '0001839341'},
        ]))
        db_no_sync.sync_companies_from_config(str(cfg))

        with db_no_sync._get_connection() as conn:
            rows = conn.execute(
                "SELECT source_type FROM reports WHERE ticker='CORZ'"
            ).fetchall()
        surviving = {r['source_type'] for r in rows}
        assert 'edgar_8k' not in surviving, "EDGAR report must be purged"
        assert 'ir_press_release' in surviving, "IR report must survive CIK change"
        assert 'archive_pdf' in surviving, "Archive report must survive CIK change"

    def test_null_cik_to_real_cik_does_not_purge(self, db_no_sync, tmp_path):
        """Setting a CIK for the first time (null → value) does not purge reports."""
        cfg = _write_companies_json(tmp_path, [
            {'ticker': 'ABTC', 'name': 'American Bitcoin', 'active': True, 'cik': None},
        ])
        db_no_sync.sync_companies_from_config(str(cfg))
        _insert_report(db_no_sync, 'ABTC', 'ir_press_release')

        cfg.write_text(json.dumps([
            {'ticker': 'ABTC', 'name': 'American Bitcoin', 'active': True, 'cik': '0001234567'},
        ]))
        db_no_sync.sync_companies_from_config(str(cfg))

        with db_no_sync._get_connection() as conn:
            count = conn.execute("SELECT COUNT(*) FROM reports WHERE ticker='ABTC'").fetchone()[0]
        assert count == 1, "Reports must not be purged when CIK is set for the first time"
