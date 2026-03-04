"""
Unit tests for DB CRUD: facilities, btc_loans, source_audit.

TDD: tests written before implementation. Each test should fail until
the corresponding DB method is added to MinerDB.
"""
import pytest
from infra.db import MinerDB


@pytest.fixture
def db(tmp_path):
    return MinerDB(str(tmp_path / 'test.db'))


# ── Facilities ────────────────────────────────────────────────────────────────

class TestFacilitiesDB:
    def test_insert_and_get_round_trip(self, db):
        rec = {
            'ticker': 'MARA',
            'name': 'Garden City',
            'address': '100 Main St',
            'city': 'Garden City',
            'state': 'TX',
            'lat': 31.8,
            'lon': -101.5,
            'purpose': 'MINING',
            'size_mw': 200.0,
            'operational_since': '2021-01-01',
        }
        fid = db.insert_facility(rec)
        assert isinstance(fid, int) and fid > 0
        facilities = db.get_facilities('MARA')
        assert len(facilities) == 1
        f = facilities[0]
        assert f['ticker'] == 'MARA'
        assert f['name'] == 'Garden City'
        assert f['purpose'] == 'MINING'
        assert abs(f['size_mw'] - 200.0) < 0.01

    def test_unique_constraint_ticker_name(self, db):
        rec = {'ticker': 'MARA', 'name': 'Garden City', 'purpose': 'MINING', 'size_mw': 200.0}
        db.insert_facility(rec)
        # Inserting same ticker+name should raise or silently ignore
        import sqlite3
        with pytest.raises(sqlite3.IntegrityError):
            db.insert_facility(rec)

    def test_get_facilities_by_ticker(self, db):
        db.insert_facility({'ticker': 'MARA', 'name': 'Site A', 'purpose': 'MINING', 'size_mw': 100.0})
        db.insert_facility({'ticker': 'MARA', 'name': 'Site B', 'purpose': 'AI_HPC', 'size_mw': 50.0})
        db.insert_facility({'ticker': 'RIOT', 'name': 'Rockdale', 'purpose': 'MINING', 'size_mw': 700.0})
        mara = db.get_facilities('MARA')
        assert len(mara) == 2
        riot = db.get_facilities('RIOT')
        assert len(riot) == 1

    def test_get_facilities_unknown_ticker(self, db):
        result = db.get_facilities('ZZYZ')
        assert result == []

    def test_get_all_facilities_no_ticker(self, db):
        db.insert_facility({'ticker': 'MARA', 'name': 'Site A', 'purpose': 'MINING', 'size_mw': 100.0})
        db.insert_facility({'ticker': 'RIOT', 'name': 'Rockdale', 'purpose': 'MINING', 'size_mw': 700.0})
        all_facilities = db.get_facilities()
        assert len(all_facilities) == 2

    def test_optional_fields_default_to_none(self, db):
        db.insert_facility({'ticker': 'WULF', 'name': 'Nautilus', 'purpose': 'MINING', 'size_mw': 300.0})
        f = db.get_facilities('WULF')[0]
        assert f['address'] is None
        assert f['lat'] is None
        assert f['lon'] is None
        assert f['operational_since'] is None


# ── BTC Loans ─────────────────────────────────────────────────────────────────

class TestBtcLoansDB:
    def test_insert_and_get_round_trip(self, db):
        rec = {
            'ticker': 'MARA',
            'counterparty': 'Silvergate',
            'total_btc_encumbered': 3000.0,
            'as_of_date': '2023-06-30',
        }
        lid = db.insert_btc_loan(rec)
        assert isinstance(lid, int) and lid > 0
        loans = db.get_btc_loans('MARA')
        assert len(loans) == 1
        loan = loans[0]
        assert loan['ticker'] == 'MARA'
        assert loan['counterparty'] == 'Silvergate'
        assert abs(loan['total_btc_encumbered'] - 3000.0) < 0.01

    def test_get_btc_loans_by_ticker(self, db):
        db.insert_btc_loan({'ticker': 'MARA', 'counterparty': 'A', 'total_btc_encumbered': 1000.0, 'as_of_date': '2023-01-01'})
        db.insert_btc_loan({'ticker': 'MARA', 'counterparty': 'B', 'total_btc_encumbered': 2000.0, 'as_of_date': '2023-01-01'})
        db.insert_btc_loan({'ticker': 'RIOT', 'counterparty': 'C', 'total_btc_encumbered': 500.0, 'as_of_date': '2023-01-01'})
        mara = db.get_btc_loans('MARA')
        assert len(mara) == 2
        riot = db.get_btc_loans('RIOT')
        assert len(riot) == 1

    def test_get_btc_loans_empty(self, db):
        assert db.get_btc_loans('ZZYZ') == []

    def test_multiple_loans_same_counterparty_different_dates(self, db):
        """Different as_of_date makes distinct rows."""
        db.insert_btc_loan({'ticker': 'MARA', 'counterparty': 'Silvergate', 'total_btc_encumbered': 3000.0, 'as_of_date': '2023-01-01'})
        db.insert_btc_loan({'ticker': 'MARA', 'counterparty': 'Silvergate', 'total_btc_encumbered': 2500.0, 'as_of_date': '2023-06-30'})
        loans = db.get_btc_loans('MARA')
        assert len(loans) == 2


# ── Source Audit ──────────────────────────────────────────────────────────────

class TestSourceAuditDB:
    def test_upsert_and_get_round_trip(self, db):
        rec = {
            'ticker': 'CORZ',
            'source_type': 'IR_PRIMARY',
            'url': 'https://ir.core-scientific.com/rss',
            'http_status': 502,
            'status': 'DEAD',
            'notes': '502 Bad Gateway since 2024-11',
        }
        db.upsert_source_audit(rec)
        rows = db.get_source_audit('CORZ')
        assert len(rows) == 1
        row = rows[0]
        assert row['ticker'] == 'CORZ'
        assert row['source_type'] == 'IR_PRIMARY'
        assert row['http_status'] == 502
        assert row['status'] == 'DEAD'

    def test_upsert_updates_existing_row(self, db):
        """UNIQUE(ticker, source_type) → second upsert replaces the first."""
        rec = {'ticker': 'CORZ', 'source_type': 'IR_PRIMARY', 'http_status': 502, 'status': 'DEAD'}
        db.upsert_source_audit(rec)
        rec2 = {'ticker': 'CORZ', 'source_type': 'IR_PRIMARY', 'http_status': 200, 'status': 'ACTIVE', 'url': 'https://new-url.com'}
        db.upsert_source_audit(rec2)
        rows = db.get_source_audit('CORZ')
        assert len(rows) == 1
        assert rows[0]['http_status'] == 200
        assert rows[0]['status'] == 'ACTIVE'

    def test_get_source_audit_by_ticker(self, db):
        db.upsert_source_audit({'ticker': 'CORZ', 'source_type': 'IR_PRIMARY', 'status': 'DEAD'})
        db.upsert_source_audit({'ticker': 'CORZ', 'source_type': 'GLOBENEWSWIRE', 'status': 'ACTIVE'})
        db.upsert_source_audit({'ticker': 'IREN', 'source_type': 'IR_PRIMARY', 'status': 'DEAD'})
        corz = db.get_source_audit('CORZ')
        assert len(corz) == 2
        iren = db.get_source_audit('IREN')
        assert len(iren) == 1

    def test_get_source_audit_unknown_ticker(self, db):
        assert db.get_source_audit('ZZYZ') == []

    def test_last_checked_stored(self, db):
        rec = {'ticker': 'ARBK', 'source_type': 'EDGAR', 'status': 'ACTIVE', 'last_checked': '2026-03-02T12:00:00'}
        db.upsert_source_audit(rec)
        rows = db.get_source_audit('ARBK')
        assert rows[0]['last_checked'] == '2026-03-02T12:00:00'
