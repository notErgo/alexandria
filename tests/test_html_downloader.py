"""
Unit tests for HTMLDownloader helper functions.

All tests are pure (no network, no filesystem side effects except tmp_path).
"""
from datetime import date
from pathlib import Path

import pytest

from scrapers.html_downloader import (
    HTMLDownloader,
    _is_production_pr,
    _infer_period_from_title,
    _build_output_path,
    _build_template_url,
    _months_in_range,
)


# ── _is_production_pr ──────────────────────────────────────────────────────

class TestIsProductionPR:
    def test_riot_monthly_update(self):
        assert _is_production_pr(
            "Riot Announces January 2024 Production and Operations Updates"
        )

    def test_clsk_bitcoin_mining_update(self):
        assert _is_production_pr(
            "CleanSpark Releases January 2024 Bitcoin Mining Update"
        )

    def test_bitf_production_update(self):
        assert _is_production_pr(
            "Bitfarms Provides January 2024 Production and Operations Update"
        )

    def test_btbt_monthly_production(self):
        assert _is_production_pr(
            "Bit Digital Inc Announces Monthly Production Update for January 2024"
        )

    def test_cifr_operational_update(self):
        assert _is_production_pr(
            "Cipher Mining Announces January 2024 Operational Update"
        )

    def test_iren_monthly_update(self):
        assert _is_production_pr("IREN January 2024 Monthly Update")

    def test_reject_financial_results(self):
        assert not _is_production_pr(
            "Marathon Reports Q3 2024 Financial Results"
        )

    def test_reject_earnings_release(self):
        assert not _is_production_pr(
            "MARA Reports Fourth Quarter and Full Year 2023 Earnings"
        )

    def test_reject_10q_filing(self):
        assert not _is_production_pr(
            "Riot Platforms Files 10-Q for Q3 2024"
        )

    def test_reject_annual_report(self):
        assert not _is_production_pr(
            "Hut 8 Releases Annual Report 2023"
        )


# ── _infer_period_from_title ───────────────────────────────────────────────

class TestInferPeriodFromTitle:
    def test_title_case_month(self):
        assert _infer_period_from_title(
            "Riot Announces January 2024 Production and Operations Updates"
        ) == date(2024, 1, 1)

    def test_lowercase_month_in_slug(self):
        assert _infer_period_from_title(
            "riot-announces-october-2023-production-and-operations-updates"
        ) == date(2023, 10, 1)

    def test_december(self):
        assert _infer_period_from_title(
            "IREN December 2023 Monthly Update"
        ) == date(2023, 12, 1)

    def test_returns_none_when_no_month(self):
        assert _infer_period_from_title("CleanSpark Announces Record Revenue") is None

    def test_returns_none_for_empty_string(self):
        assert _infer_period_from_title("") is None


# ── _build_template_url ────────────────────────────────────────────────────

class TestBuildTemplateUrl:
    def test_riot_lowercase_month(self):
        template = "https://www.riotplatforms.com/riot-announces-{month}-{year}-production-and-operations-updates/"
        url = _build_template_url(template, date(2024, 1, 1))
        assert url == "https://www.riotplatforms.com/riot-announces-january-2024-production-and-operations-updates/"

    def test_clsk_title_case_month(self):
        template = "https://investors.cleanspark.com/news/news-details/{year}/CleanSpark-Releases-{Month}-{year}-Bitcoin-Mining-Update/default.aspx"
        url = _build_template_url(template, date(2024, 3, 1))
        assert url == "https://investors.cleanspark.com/news/news-details/2024/CleanSpark-Releases-March-2024-Bitcoin-Mining-Update/default.aspx"

    def test_december(self):
        template = "https://example.com/{month}-{year}-update/"
        url = _build_template_url(template, date(2023, 12, 1))
        assert url == "https://example.com/december-2023-update/"

    def test_uppercase_month_token(self):
        template = "https://example.com/{MONTH}-{year}/"
        url = _build_template_url(template, date(2024, 6, 1))
        assert url == "https://example.com/JUNE-2024/"


# ── _months_in_range ───────────────────────────────────────────────────────

class TestMonthsInRange:
    def test_single_month(self):
        months = _months_in_range(2024, date(2024, 1, 1))
        assert months == [date(2024, 1, 1)]

    def test_three_months(self):
        months = _months_in_range(2024, date(2024, 3, 1))
        assert months == [
            date(2024, 1, 1),
            date(2024, 2, 1),
            date(2024, 3, 1),
        ]

    def test_year_boundary(self):
        months = _months_in_range(2023, date(2024, 2, 1))
        assert months[0] == date(2023, 1, 1)
        assert months[-1] == date(2024, 2, 1)
        assert len(months) == 14  # 12 months 2023 + Jan+Feb 2024

    def test_empty_when_start_after_end(self):
        months = _months_in_range(2025, date(2024, 1, 1))
        assert months == []


# ── _build_output_path ─────────────────────────────────────────────────────

class TestBuildOutputPath:
    def test_path_structure(self, tmp_path):
        out = _build_output_path(str(tmp_path), "RIOT", date(2024, 1, 1))
        assert out.parent.name == "RIOT MONTHLY"
        assert out.parent.parent.name == "Miner Monthly"

    def test_filename_iso_prefix(self, tmp_path):
        out = _build_output_path(str(tmp_path), "RIOT", date(2024, 1, 1))
        assert out.name == "2024-01-01_riot_production.html"

    def test_filename_december(self, tmp_path):
        out = _build_output_path(str(tmp_path), "CLSK", date(2023, 12, 1))
        assert out.name == "2023-12-01_clsk_production.html"

    def test_directory_created(self, tmp_path):
        out = _build_output_path(str(tmp_path), "MARA", date(2024, 6, 1))
        assert out.parent.exists()

    def test_filename_passes_production_keyword(self, tmp_path):
        """Filename must contain 'production' for archive_ingestor.is_production_filename."""
        from scrapers.archive_ingestor import is_production_filename
        out = _build_output_path(str(tmp_path), "BITF", date(2024, 5, 1))
        assert is_production_filename(out.name)


class TestDownloadAllModeSelection:
    def test_uses_scraper_mode_key_for_discovery(self, tmp_path):
        downloader = HTMLDownloader(str(tmp_path))
        company = {
            "ticker": "CLSK",
            "active": True,
            "scraper_mode": "discovery",
            "ir_url": "https://investors.cleanspark.com/news",
            "pr_start_date": "2020-01-01",
        }
        with pytest.MonkeyPatch.context() as mp:
            mp.setattr(downloader, "_download_discovery", lambda *args, **kwargs: type("S", (), {
                "downloaded": 0, "skipped_existing": 0, "skipped_not_found": 0, "errors": 0
            })())
            summary = downloader.download_all([company])
        assert summary.companies_processed == ["CLSK"]
