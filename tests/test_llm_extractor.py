"""
LLM extractor unit tests — TDD.

All tests use mock HTTP sessions; no network calls permitted.
Tests should FAIL before llm_extractor.py is implemented.
"""
import pytest
from unittest.mock import MagicMock
import json


class FakeResponse:
    """Simulate a requests.Response object."""
    def __init__(self, status_code=200, json_data=None):
        self.status_code = status_code
        self._json = json_data

    def json(self):
        if self._json is None:
            raise ValueError("No JSON")
        return self._json

    def raise_for_status(self):
        if self.status_code >= 400:
            import requests
            raise requests.HTTPError(response=self)


def _make_extractor(session=None, db=None):
    from extractors.llm_extractor import LLMExtractor
    if session is None:
        session = MagicMock()
    return LLMExtractor(session=session, db=db)


def _ollama_ok_response(metric, value, unit="BTC", confidence=0.92):
    """Build a realistic Ollama /api/generate response."""
    payload = json.dumps({
        "metric": metric,
        "value": value,
        "unit": unit,
        "confidence": confidence,
    })
    return FakeResponse(
        status_code=200,
        json_data={"response": payload, "done": True},
    )


class TestLLMExtractorParsing:
    def test_returns_extraction_result_on_valid_json(self):
        """LLM returns valid JSON → ExtractionResult with correct value and confidence."""
        from miner_types import ExtractionResult
        session = MagicMock()
        session.post.return_value = _ollama_ok_response("production_btc", 700)
        extractor = _make_extractor(session=session)
        result = extractor.extract("MARA mined 700 BTC in January", "production_btc")
        assert result is not None
        assert isinstance(result, ExtractionResult)
        assert result.value == 700.0
        assert result.confidence == 0.92

    def test_returns_none_on_malformed_json(self):
        """LLM returns non-JSON text → None, no exception raised."""
        session = MagicMock()
        session.post.return_value = FakeResponse(
            status_code=200,
            json_data={"response": "Sorry, I cannot extract that.", "done": True},
        )
        extractor = _make_extractor(session=session)
        result = extractor.extract("some text", "production_btc")
        assert result is None

    def test_returns_none_on_timeout(self):
        """requests.Timeout raised → None, no exception propagated."""
        import requests
        session = MagicMock()
        session.post.side_effect = requests.Timeout("timed out")
        extractor = _make_extractor(session=session)
        result = extractor.extract("some text", "production_btc")
        assert result is None

    def test_returns_none_when_value_out_of_range(self):
        """LLM returns 99999 BTC for production_btc (exceeds 5000 max) → None."""
        session = MagicMock()
        session.post.return_value = _ollama_ok_response("production_btc", 99999)
        extractor = _make_extractor(session=session)
        result = extractor.extract("some text", "production_btc")
        assert result is None

    def test_uses_correct_model_from_config(self):
        """Payload sent to Ollama must contain the configured LLM_MODEL_ID."""
        from config import LLM_MODEL_ID
        session = MagicMock()
        session.post.return_value = _ollama_ok_response("production_btc", 700)
        extractor = _make_extractor(session=session)
        extractor.extract("MARA mined 700 BTC", "production_btc")
        call_kwargs = session.post.call_args[1]
        payload = call_kwargs.get('json', {})
        assert payload.get('model') == LLM_MODEL_ID

    def test_prompt_contains_document_text(self):
        """Ollama payload must include the document text passed to extract()."""
        doc_text = "MARA Holdings mined 700 BTC in January 2024"
        session = MagicMock()
        session.post.return_value = _ollama_ok_response("production_btc", 700)
        extractor = _make_extractor(session=session)
        extractor.extract(doc_text, "production_btc")
        call_kwargs = session.post.call_args[1]
        payload = call_kwargs.get('json', {})
        assert doc_text in payload.get('prompt', '')

    def test_confidence_clamped_to_1(self):
        """LLM returns confidence > 1.0 → clamped to 1.0 in result."""
        session = MagicMock()
        session.post.return_value = _ollama_ok_response("production_btc", 700, confidence=1.5)
        extractor = _make_extractor(session=session)
        result = extractor.extract("MARA mined 700 BTC", "production_btc")
        assert result is not None
        assert result.confidence <= 1.0

    def test_returns_none_on_http_500(self):
        """Ollama returns HTTP 500 → None, no exception propagated."""
        session = MagicMock()
        session.post.return_value = FakeResponse(status_code=500)
        extractor = _make_extractor(session=session)
        result = extractor.extract("some text", "production_btc")
        assert result is None

    def test_returns_none_when_llm_value_is_null(self):
        """LLM returns {"value": null} → None (metric not extractable)."""
        session = MagicMock()
        session.post.return_value = FakeResponse(
            status_code=200,
            json_data={"response": '{"metric":"production_btc","value":null}', "done": True},
        )
        extractor = _make_extractor(session=session)
        result = extractor.extract("some text", "production_btc")
        assert result is None

    def test_extraction_method_includes_llm_prefix(self):
        """ExtractionResult.extraction_method starts with 'llm_'."""
        session = MagicMock()
        session.post.return_value = _ollama_ok_response("production_btc", 700)
        extractor = _make_extractor(session=session)
        result = extractor.extract("MARA mined 700 BTC", "production_btc")
        assert result is not None
        assert result.extraction_method.startswith('llm_')


class TestLLMExtractorPromptDB:
    def test_uses_db_prompt_when_available(self, db):
        """If DB has active prompt for metric, it is included in the Ollama call."""
        with db._get_connection() as conn:
            conn.execute(
                "INSERT INTO llm_prompts (metric, prompt_text, model, active) VALUES (?, ?, ?, 1)",
                ('production_btc', 'CUSTOM PROMPT TEXT', 'test-model')
            )
        session = MagicMock()
        session.post.return_value = _ollama_ok_response("production_btc", 700)
        extractor = _make_extractor(session=session, db=db)
        extractor.extract("MARA mined 700 BTC", "production_btc")
        call_kwargs = session.post.call_args[1]
        payload = call_kwargs.get('json', {})
        assert 'CUSTOM PROMPT TEXT' in payload.get('prompt', '')

    def test_falls_back_to_hardcoded_prompt_when_db_empty(self, db):
        """If DB has no prompt for metric, hardcoded default is used (no crash)."""
        session = MagicMock()
        session.post.return_value = _ollama_ok_response("production_btc", 700)
        extractor = _make_extractor(session=session, db=db)
        result = extractor.extract("MARA mined 700 BTC", "production_btc")
        assert result is not None


class TestBatchExtraction:
    """extract_batch() sends one prompt and returns a dict of ExtractionResults."""

    def _batch_response(self, payload: dict):
        """Wrap a dict as a realistic Ollama /api/generate batch response."""
        return FakeResponse(
            status_code=200,
            json_data={"response": json.dumps(payload), "done": True},
        )

    def test_extract_batch_returns_dict_of_results(self):
        """Valid batch JSON → dict keyed by metric; null-value metrics absent."""
        from miner_types import ExtractionResult
        session = MagicMock()
        session.post.return_value = self._batch_response({
            "production_btc": {"value": 700, "unit": "BTC", "confidence": 0.95,
                               "source_snippet": "mined 700 BTC"},
            "hodl_btc": {"value": None, "unit": "BTC", "confidence": 0.0,
                         "source_snippet": ""},
        })
        extractor = _make_extractor(session=session)
        result = extractor.extract_batch("MARA mined 700 BTC", ["production_btc", "hodl_btc"])
        assert "production_btc" in result
        assert isinstance(result["production_btc"], ExtractionResult)
        assert result["production_btc"].value == 700.0
        assert "hodl_btc" not in result, "Null-value metrics must be absent"

    def test_extract_batch_prompt_contains_document_once(self):
        """Document text appears exactly once in the Ollama prompt."""
        session = MagicMock()
        session.post.return_value = self._batch_response({
            "production_btc": {"value": 700, "unit": "BTC", "confidence": 0.9,
                               "source_snippet": "mined 700 BTC"},
        })
        doc = "MARA mined 700 BTC in January"
        extractor = _make_extractor(session=session)
        extractor.extract_batch(doc, ["production_btc"])
        prompt = session.post.call_args[1]["json"]["prompt"]
        assert prompt.count(doc) == 1, "Document must appear exactly once"

    def test_extract_batch_prompt_contains_all_metric_names(self):
        """All requested metric names appear in the batch prompt."""
        session = MagicMock()
        session.post.return_value = self._batch_response({
            "production_btc": {"value": 700, "unit": "BTC", "confidence": 0.9,
                               "source_snippet": ""},
            "hodl_btc": {"value": 13000, "unit": "BTC", "confidence": 0.9,
                         "source_snippet": ""},
        })
        extractor = _make_extractor(session=session)
        extractor.extract_batch("some text", ["production_btc", "hodl_btc"])
        prompt = session.post.call_args[1]["json"]["prompt"]
        assert "production_btc" in prompt
        assert "hodl_btc" in prompt

    def test_extract_batch_returns_empty_dict_on_malformed_json(self):
        """LLM returns non-JSON text → empty dict, no exception."""
        session = MagicMock()
        session.post.return_value = FakeResponse(
            status_code=200,
            json_data={"response": "Sorry, I cannot extract that.", "done": True},
        )
        extractor = _make_extractor(session=session)
        result = extractor.extract_batch("some text", ["production_btc"])
        assert result == {}

    def test_extract_batch_returns_empty_dict_on_http_500(self):
        """Ollama returns HTTP 500 → empty dict, no exception."""
        session = MagicMock()
        session.post.return_value = FakeResponse(status_code=500)
        extractor = _make_extractor(session=session)
        result = extractor.extract_batch("some text", ["production_btc"])
        assert result == {}

    def test_extract_batch_skips_out_of_range_values(self):
        """production_btc value of 99999 exceeds valid range → key absent from result."""
        session = MagicMock()
        session.post.return_value = self._batch_response({
            "production_btc": {"value": 99999, "unit": "BTC", "confidence": 0.9,
                               "source_snippet": ""},
        })
        extractor = _make_extractor(session=session)
        result = extractor.extract_batch("some text", ["production_btc"])
        assert "production_btc" not in result

    def test_extract_batch_uses_db_prompt_for_overridden_metric(self, db):
        """DB prompt override text appears in the batch prompt."""
        with db._get_connection() as conn:
            conn.execute(
                "INSERT INTO llm_prompts (metric, prompt_text, model, active) VALUES (?, ?, ?, 1)",
                ("production_btc", "CUSTOM BATCH INSTRUCTIONS HERE", "test-model"),
            )
        session = MagicMock()
        session.post.return_value = self._batch_response({
            "production_btc": {"value": 700, "unit": "BTC", "confidence": 0.9,
                               "source_snippet": ""},
        })
        extractor = _make_extractor(session=session, db=db)
        extractor.extract_batch("MARA mined 700 BTC", ["production_btc"])
        prompt = session.post.call_args[1]["json"]["prompt"]
        assert "CUSTOM BATCH INSTRUCTIONS HERE" in prompt

    def test_extract_batch_result_has_llm_extraction_method(self):
        """ExtractionResult.extraction_method starts with 'llm_'."""
        session = MagicMock()
        session.post.return_value = self._batch_response({
            "production_btc": {"value": 700, "unit": "BTC", "confidence": 0.9,
                               "source_snippet": "mined 700 BTC"},
        })
        extractor = _make_extractor(session=session)
        result = extractor.extract_batch("MARA mined 700 BTC", ["production_btc"])
        assert result["production_btc"].extraction_method.startswith("llm_")


class TestCheckConnectivity:
    def test_returns_true_when_version_ok_and_model_exists(self):
        """/api/version 200 + /api/show 200 → True."""
        session = MagicMock()
        session.get.return_value = FakeResponse(status_code=200, json_data={"version": "0.6.0"})
        session.post.return_value = FakeResponse(status_code=200, json_data={"modelfile": "..."})
        extractor = _make_extractor(session=session)
        assert extractor.check_connectivity() is True

    def test_returns_false_when_version_fails(self):
        """/api/version 500 → False without calling /api/show."""
        session = MagicMock()
        session.get.return_value = FakeResponse(status_code=500)
        extractor = _make_extractor(session=session)
        assert extractor.check_connectivity() is False
        session.post.assert_not_called()

    def test_returns_false_when_model_not_found(self):
        """/api/version 200 but /api/show 404 → False (model not installed)."""
        session = MagicMock()
        session.get.return_value = FakeResponse(status_code=200, json_data={"version": "0.6.0"})
        session.post.return_value = FakeResponse(status_code=404)
        extractor = _make_extractor(session=session)
        assert extractor.check_connectivity() is False

    def test_returns_false_on_connection_error(self):
        """Network error → False, no exception propagated."""
        import requests
        session = MagicMock()
        session.get.side_effect = requests.ConnectionError("refused")
        extractor = _make_extractor(session=session)
        assert extractor.check_connectivity() is False


class TestExtractForPeriod:
    """extract_for_period sends a targeted prompt for the prior period."""

    def _batch_response(self, payload: dict):
        return FakeResponse(
            status_code=200,
            json_data={"response": json.dumps(payload), "done": True},
        )

    def test_extract_for_period_prompt_mentions_target_period(self):
        """extract_for_period builds a prompt mentioning the target (prior) period."""
        session = MagicMock()
        session.post.return_value = self._batch_response({
            "production_btc": {"value": 650, "unit": "BTC", "confidence": 0.88,
                               "source_snippet": "August mined 650 BTC"},
        })
        extractor = _make_extractor(session=session)
        extractor.extract_for_period(
            "document text",
            ["production_btc"],
            current_period="2024-09-01",
            target_period="2024-08-01",
        )
        prompt = session.post.call_args[1]["json"]["prompt"]
        assert "2024-08-01" in prompt or "August" in prompt

    def test_extract_for_period_prompt_mentions_current_period_exclusion(self):
        """extract_for_period prompt tells LLM NOT to extract current-period values."""
        session = MagicMock()
        session.post.return_value = self._batch_response({
            "production_btc": {"value": 650, "unit": "BTC", "confidence": 0.88,
                               "source_snippet": ""},
        })
        extractor = _make_extractor(session=session)
        extractor.extract_for_period(
            "doc",
            ["production_btc"],
            current_period="2024-09-01",
            target_period="2024-08-01",
        )
        prompt = session.post.call_args[1]["json"]["prompt"]
        # Prompt should reference current period to instruct LLM to exclude it
        assert "2024-09-01" in prompt or "September" in prompt

    def test_extract_for_period_returns_empty_on_http_error(self):
        """extract_for_period returns {} on HTTP error, no exception."""
        session = MagicMock()
        session.post.return_value = FakeResponse(status_code=500)
        extractor = _make_extractor(session=session)
        result = extractor.extract_for_period(
            "doc",
            ["production_btc"],
            current_period="2024-09-01",
            target_period="2024-08-01",
        )
        assert result == {}
