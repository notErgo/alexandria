"""Tests for unit normalization functions."""
from interpreters.unit_normalizer import normalize_hashrate, normalize_btc, normalize_percent, normalize_value


def test_normalize_hashrate_eh():
    assert normalize_hashrate("21.3 EH/s") == (21.3, "EH/s")


def test_normalize_hashrate_ph_converts_to_eh():
    result = normalize_hashrate("3400 PH/s")
    assert result is not None
    assert abs(result[0] - 3.4) < 0.001
    assert result[1] == "EH/s"


def test_normalize_hashrate_th_converts_to_eh():
    result = normalize_hashrate("3400000 TH/s")
    assert result is not None
    assert result[1] == "EH/s"
    assert abs(result[0] - 3.4) < 0.001


def test_normalize_btc_integer_with_comma():
    assert normalize_btc("1,234") == (1234.0, "BTC")


def test_normalize_btc_negative():
    assert normalize_btc("-450 BTC") == (-450.0, "BTC")


def test_normalize_percent_to_ratio():
    result = normalize_percent("94.2%")
    assert result is not None
    assert abs(result[0] - 0.942) < 0.0001
    assert result[1] == "ratio"


def test_normalize_unknown_returns_none():
    assert normalize_hashrate("unknown value") is None


def test_normalize_value_dispatch_hashrate():
    result = normalize_value("3.4 EH/s", "hashrate_eh")
    assert result is not None
    assert result == (3.4, "EH/s")


def test_normalize_btc_plain_number():
    result = normalize_btc("700")
    assert result == (700.0, "BTC")
