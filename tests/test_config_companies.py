"""Tests for companies.json schema validation and canonical accessors."""
import pytest
from config import load_companies, get_all_tickers, validate_companies_config


def _valid_entry(**kwargs):
    base = {
        'ticker': 'TEST',
        'name': 'Test Corp',
        'tier': 1,
        'active': True,
        'filing_regime': 'domestic',
        'fiscal_year_end_month': 12,
    }
    base.update(kwargs)
    return base


class TestValidateCompaniesConfig:
    def test_valid_entry_passes(self):
        assert validate_companies_config([_valid_entry()]) == []

    def test_missing_required_field(self):
        entry = _valid_entry()
        del entry['tier']
        errors = validate_companies_config([entry])
        assert any('tier' in e for e in errors)

    def test_invalid_tier(self):
        errors = validate_companies_config([_valid_entry(tier=5)])
        assert any('tier' in e for e in errors)

    def test_active_not_bool(self):
        errors = validate_companies_config([_valid_entry(active='yes')])
        assert any('active' in e for e in errors)

    def test_fiscal_year_end_month_out_of_range(self):
        errors = validate_companies_config([_valid_entry(fiscal_year_end_month=13)])
        assert any('fiscal_year_end_month' in e for e in errors)

    def test_fiscal_year_end_month_bool_rejected(self):
        # bool is a subclass of int in Python; True == 1 passes range check without explicit guard
        errors = validate_companies_config([_valid_entry(fiscal_year_end_month=True)])
        assert any('fiscal_year_end_month' in e for e in errors)

    def test_unknown_filing_regime(self):
        errors = validate_companies_config([_valid_entry(filing_regime='alien')])
        assert any('filing_regime' in e for e in errors)

    def test_duplicate_ticker(self):
        errors = validate_companies_config([_valid_entry(), _valid_entry()])
        assert any('duplicate' in e for e in errors)

    def test_production_config_is_valid(self):
        errors = validate_companies_config()
        assert errors == [], 'companies.json validation errors:\n' + '\n'.join(errors)


class TestAccessors:
    def test_load_companies_returns_nonempty_list(self):
        companies = load_companies()
        assert isinstance(companies, list)
        assert len(companies) > 0

    def test_get_all_tickers_is_sorted(self):
        tickers = get_all_tickers()
        assert tickers == sorted(tickers)

    def test_get_all_tickers_all_strings(self):
        assert all(isinstance(t, str) for t in get_all_tickers())

    def test_get_all_tickers_matches_load_companies(self):
        expected = sorted(c['ticker'] for c in load_companies())
        assert get_all_tickers() == expected
