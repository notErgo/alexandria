"""
Tests for registry expansion: new tickers in KNOWN_TICKERS and scraper coverage.
Written before implementation (test-first). Tests will fail until Phase II changes
are applied to archive_ingestor.py.
"""
from datetime import date
import pytest


class TestKnownTickersExpansion:
    """KNOWN_TICKERS must contain all tickers in companies.json."""

    def test_known_tickers_contains_original_13(self):
        from scrapers.archive_ingestor import KNOWN_TICKERS
        for t in ['MARA', 'RIOT', 'CLSK', 'CORZ', 'BITF', 'BTBT', 'CIFR',
                  'HIVE', 'HUT8', 'ARBK', 'SDIG', 'WULF', 'IREN']:
            assert t in KNOWN_TICKERS, f"{t} missing from KNOWN_TICKERS"

    def test_known_tickers_contains_new_entries(self):
        from scrapers.archive_ingestor import KNOWN_TICKERS
        for t in ['BTDR', 'ABTC', 'APLD', 'GRDI', 'MIGI', 'GREE']:
            assert t in KNOWN_TICKERS, f"{t} missing from KNOWN_TICKERS"

    def test_known_tickers_is_list(self):
        from scrapers.archive_ingestor import KNOWN_TICKERS
        assert isinstance(KNOWN_TICKERS, list)

    def test_known_tickers_no_duplicates(self):
        from scrapers.archive_ingestor import KNOWN_TICKERS
        assert len(KNOWN_TICKERS) == len(set(KNOWN_TICKERS)), "Duplicate tickers in KNOWN_TICKERS"


class TestInferTickerFromPath:
    """infer_ticker_from_path must recognize all new tickers."""

    def test_infer_ticker_btdr_monthly(self):
        from scrapers.archive_ingestor import infer_ticker_from_path
        assert infer_ticker_from_path('/archive/BTDR MONTHLY/report.html') == 'BTDR'

    def test_infer_ticker_abtc_monthly(self):
        from scrapers.archive_ingestor import infer_ticker_from_path
        assert infer_ticker_from_path('/archive/ABTC MONTHLY/report.html') == 'ABTC'

    def test_infer_ticker_migi_monthly(self):
        from scrapers.archive_ingestor import infer_ticker_from_path
        assert infer_ticker_from_path('/archive/MIGI MONTHLY/update.html') == 'MIGI'

    def test_infer_ticker_gree_monthly(self):
        from scrapers.archive_ingestor import infer_ticker_from_path
        assert infer_ticker_from_path('/archive/GREE MONTHLY/report.html') == 'GREE'

    def test_infer_ticker_grdi_monthly(self):
        from scrapers.archive_ingestor import infer_ticker_from_path
        assert infer_ticker_from_path('/archive/GRDI MONTHLY/report.html') == 'GRDI'

    def test_infer_ticker_original_still_works(self):
        from scrapers.archive_ingestor import infer_ticker_from_path
        assert infer_ticker_from_path('/archive/MARA MONTHLY/report.pdf') == 'MARA'
        assert infer_ticker_from_path('/archive/RIOT MONTHLY/report.html') == 'RIOT'


class TestIsProductionPRNewCompanies:
    """is_production_pr must accept new company PR title formats."""

    def test_bitdeer_production_update(self):
        from scrapers.ir_scraper import is_production_pr
        assert is_production_pr(
            "Bitdeer Announces November 2025 Production and Operations Update"
        )

    def test_hive_production_report(self):
        from scrapers.ir_scraper import is_production_pr
        assert is_production_pr(
            "HIVE Digital Technologies Provides August 2025 Production Report "
            "with 22% Monthly Increase in Bitcoin Production"
        )

    def test_cipher_mining_operational_update(self):
        from scrapers.ir_scraper import is_production_pr
        assert is_production_pr(
            "Cipher Mining Announces January 2025 Operational Update"
        )

    def test_argo_operational_update(self):
        from scrapers.ir_scraper import is_production_pr
        assert is_production_pr(
            "Argo Blockchain PLC Announces October Operational Update"
        )


class TestInferPeriodFromPRTitleNewCompanies:
    """Period inference must work for new company PR title formats."""

    def test_bitdeer_title(self):
        from scrapers.ir_scraper import infer_period_from_pr_title
        result = infer_period_from_pr_title(
            "Bitdeer Announces November 2025 Production and Operations Update"
        )
        assert result == date(2025, 11, 1)

    def test_hive_title(self):
        from scrapers.ir_scraper import infer_period_from_pr_title
        result = infer_period_from_pr_title(
            "HIVE Digital Technologies Provides August 2025 Production Report"
        )
        assert result == date(2025, 8, 1)

    def test_cipher_title(self):
        from scrapers.ir_scraper import infer_period_from_pr_title
        result = infer_period_from_pr_title(
            "Cipher Mining Announces January 2025 Operational Update"
        )
        assert result == date(2025, 1, 1)

    def test_iren_title(self):
        from scrapers.ir_scraper import infer_period_from_pr_title
        result = infer_period_from_pr_title("IREN August 2025 Monthly Update")
        assert result == date(2025, 8, 1)
