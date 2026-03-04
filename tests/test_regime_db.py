"""Tests for MinerDB regime_config CRUD methods."""
import pytest


class TestRegimeDB:

    def test_upsert_regime_window_inserts_new(self, db_with_company):
        db_with_company.upsert_regime_window('MARA', 'monthly', '2020-10-01', None, '')
        windows = db_with_company.get_regime_windows('MARA')
        assert len(windows) == 1
        assert windows[0]['cadence'] == 'monthly'
        assert windows[0]['ticker'] == 'MARA'

    def test_get_regime_windows_empty_for_unknown_ticker(self, db):
        assert db.get_regime_windows('UNKNOWN') == []

    def test_delete_regime_window(self, db_with_company):
        db_with_company.upsert_regime_window('MARA', 'monthly', '2020-10-01', None, '')
        windows = db_with_company.get_regime_windows('MARA')
        assert len(windows) == 1
        db_with_company.delete_regime_window(windows[0]['id'])
        assert db_with_company.get_regime_windows('MARA') == []

    def test_get_regime_windows_ordered_by_start_date(self, db_with_company):
        db_with_company.upsert_regime_window('MARA', 'quarterly', '2022-01-01', '2023-12-31', '')
        db_with_company.upsert_regime_window('MARA', 'monthly', '2020-10-01', '2021-12-31', '')
        windows = db_with_company.get_regime_windows('MARA')
        assert windows[0]['start_date'] == '2020-10-01'
        assert windows[1]['start_date'] == '2022-01-01'
