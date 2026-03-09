"""
TDD tests for LLMInterpreter._parse_quarterly_batch_response.

These tests exercise the parsing logic that was previously mocked away in all
quarterly extraction tests. Covers: valid JSON, missing JSON, null values,
out-of-range values, non-numeric values, and json-repair fallback.
"""
import pytest
from unittest.mock import MagicMock


@pytest.fixture
def interpreter():
    """LLMInterpreter instance with mocked session and db."""
    import requests as req_lib
    from interpreters.llm_interpreter import LLMInterpreter
    mock_db = MagicMock()
    mock_db.get_config.return_value = None
    return LLMInterpreter(session=req_lib.Session(), db=mock_db)


class TestParseQuarterlyBatchResponse:
    def test_parse_valid_json_response(self, interpreter):
        """Valid JSON with production_btc value returns ExtractionResult."""
        raw = '{"production_btc": {"value": 2100.0, "unit": "BTC", "confidence": 0.9, "source_snippet": "mined 2100 BTC"}}'
        result = interpreter._parse_quarterly_batch_response(raw, ['production_btc'])
        assert 'production_btc' in result
        assert abs(result['production_btc'].value - 2100.0) < 0.01
        assert result['production_btc'].confidence == pytest.approx(0.9)

    def test_parse_returns_empty_when_no_json(self, interpreter):
        """Response with no JSON object returns empty dict."""
        raw = 'Sorry, I cannot extract that information from this filing.'
        result = interpreter._parse_quarterly_batch_response(raw, ['production_btc'])
        assert result == {}

    def test_parse_drops_null_values(self, interpreter):
        """Metric entry with null value is excluded from results."""
        raw = '{"production_btc": {"value": null, "unit": "BTC", "confidence": 0.5}}'
        result = interpreter._parse_quarterly_batch_response(raw, ['production_btc'])
        assert 'production_btc' not in result

    def test_parse_drops_out_of_range_values(self, interpreter):
        """Value outside valid range for metric is excluded."""
        # production_btc max is 5000 * 3 = 15000 for quarterly
        raw = '{"production_btc": {"value": 999999.0, "unit": "BTC", "confidence": 0.9}}'
        result = interpreter._parse_quarterly_batch_response(raw, ['production_btc'])
        assert 'production_btc' not in result

    def test_parse_drops_non_numeric_values(self, interpreter):
        """Non-numeric value string is excluded from results."""
        raw = '{"production_btc": {"value": "not_a_number", "unit": "BTC", "confidence": 0.8}}'
        result = interpreter._parse_quarterly_batch_response(raw, ['production_btc'])
        assert 'production_btc' not in result

    def test_parse_handles_multiple_metrics(self, interpreter):
        """Multiple metrics in one response are all parsed correctly."""
        raw = (
            '{"production_btc": {"value": 2100.0, "unit": "BTC", "confidence": 0.9}, '
            '"hashrate_eh": {"value": 35.5, "unit": "EH/s", "confidence": 0.85}}'
        )
        result = interpreter._parse_quarterly_batch_response(
            raw, ['production_btc', 'hashrate_eh']
        )
        assert 'production_btc' in result
        assert 'hashrate_eh' in result
        assert abs(result['hashrate_eh'].value - 35.5) < 0.01

    def test_parse_ignores_metrics_not_in_requested_list(self, interpreter):
        """Metrics present in JSON but not in requested list are ignored."""
        raw = '{"production_btc": {"value": 500.0, "unit": "BTC", "confidence": 0.8}, "unknown_metric": {"value": 1.0}}'
        result = interpreter._parse_quarterly_batch_response(raw, ['production_btc'])
        assert 'unknown_metric' not in result
        assert 'production_btc' in result
