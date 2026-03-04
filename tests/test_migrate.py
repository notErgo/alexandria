"""Tests for migrate_to_miner_monthly helper functions."""
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'historic_parsing'))
from migrate_to_miner_monthly import mara_filename_to_period, riot_filename_to_period


class TestMaraFilenametoPeriod:
    def test_standard_hyphen_format(self):
        fn = "1247_marathon-digital-holdings-announces-bitcoin-production-and-mining-operation-updates-for-june-2021.html"
        assert mara_filename_to_period(fn) == (2021, 6)

    def test_no_space_after_for(self):
        """forjanuary-2025 (no hyphen between 'for' and month name)"""
        fn = "1386_mara-announces-bitcoin-production-and-mining-operation-updates-forjanuary-2025.html"
        assert mara_filename_to_period(fn) == (2025, 1)

    def test_no_space_december(self):
        fn = "1385_mara-announces-bitcoin-production-and-mining-operation-updates-fordecember-2024.html"
        assert mara_filename_to_period(fn) == (2024, 12)

    def test_november_no_hyphen(self):
        fn = "1381_mara-announces-bitcoin-production-and-mining-operation-updates-fornovember-2024.html"
        assert mara_filename_to_period(fn) == (2024, 11)

    def test_february_no_hyphen(self):
        fn = "1391_mara-announces-bitcoin-production-and-mining-operation-updates-forfebruary-2025.html"
        assert mara_filename_to_period(fn) == (2025, 2)

    def test_september_no_hyphen(self):
        fn = "1410_mara-announces-bitcoin-production-and-mining-operation-updates-forseptember-2025.html"
        assert mara_filename_to_period(fn) == (2025, 9)

    def test_returns_none_for_non_production(self):
        fn = "1328_marathon-digital-holdings-schedules-conference-call-for-fourth-quarter-and-fiscal-year-2023-financial-results.html"
        assert mara_filename_to_period(fn) is None

    def test_may_2021(self):
        fn = "1245_marathon-digital-holdings-announces-bitcoin-production-and-mining-operation-updates-for-may-2021.html"
        assert mara_filename_to_period(fn) == (2021, 5)

    def test_march_2025(self):
        fn = "1392_mara-announces-bitcoin-production-and-mining-operation-updates-for-march-2025.html"
        assert mara_filename_to_period(fn) == (2025, 3)


class TestRiotFilenametoPeriod:
    def test_dated_format(self):
        fn = "0001_2025-12_press_release.html"
        assert riot_filename_to_period(fn) == (2025, 12)

    def test_undated_returns_none(self):
        fn = "0002_press_release.html"
        assert riot_filename_to_period(fn) is None

    def test_early_period(self):
        fn = "0099_2022-08_press_release.html"
        assert riot_filename_to_period(fn) == (2022, 8)

    def test_2020_period(self):
        fn = "0151_2020-05_press_release.html"
        assert riot_filename_to_period(fn) == (2020, 5)

    def test_2018_period(self):
        fn = "0176_2018-04_press_release.html"
        assert riot_filename_to_period(fn) == (2018, 4)

    def test_undated_variant(self):
        """Files like 0013_press_release.html with no date."""
        fn = "0013_press_release.html"
        assert riot_filename_to_period(fn) is None
