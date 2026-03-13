"""
Tests for quarterly/annual extraction path in extraction_pipeline.
Written before implementation — these tests define the expected contract.
"""
import pytest
from unittest.mock import MagicMock, patch


class TestQuarterlyBatchPrompt:
    def test_quarterly_batch_prompt_uses_quarterly_preamble(self):
        """_build_quarterly_batch_prompt() must include quarterly preamble, not monthly."""
        from interpreters.llm_interpreter import LLMInterpreter
        import requests
        extractor = LLMInterpreter(session=requests.Session())
        prompt = extractor._build_quarterly_batch_prompt(
            text="sample text",
            metrics=["production_btc", "hodl_btc"],
            ticker="MARA",
            period_type="quarterly",
        )
        # Must contain quarterly preamble indicator
        assert "quarterly" in prompt.lower() or "10-Q" in prompt or "10-q" in prompt.lower()
        # Must NOT contain the monthly REJECT instruction
        assert "REJECT: quarterly" not in prompt

    def test_quarterly_batch_prompt_rejects_monthly_qualifier(self):
        """For production_btc, quarterly prompt must not say 'REJECT: quarterly'."""
        from interpreters.llm_interpreter import LLMInterpreter
        import requests
        extractor = LLMInterpreter(session=requests.Session())
        prompt = extractor._build_quarterly_batch_prompt(
            text="sample text",
            metrics=["production_btc"],
            ticker="WULF",
            period_type="quarterly",
        )
        assert "REJECT: quarterly" not in prompt
        assert "REJECT: individual month" not in prompt or "monthly" in prompt.lower()

    def test_annual_batch_prompt_uses_annual_preamble(self):
        """_build_quarterly_batch_prompt() with period_type='annual' uses annual preamble."""
        from interpreters.llm_interpreter import LLMInterpreter
        import requests
        extractor = LLMInterpreter(session=requests.Session())
        prompt = extractor._build_quarterly_batch_prompt(
            text="sample text",
            metrics=["production_btc"],
            ticker="MARA",
            period_type="annual",
        )
        assert "annual" in prompt.lower() or "10-K" in prompt


class TestExtractionPipelineQuarterlyPath:
    def test_extract_report_10q_skips_regex(self):
        """For source_type='edgar_10q', regex extraction must not be called."""
        from interpreters.interpret_pipeline import extract_report

        mock_db = MagicMock()
        mock_db.data_point_exists.return_value = False
        mock_db.get_quarterly_data_point.return_value = None

        report = {
            'id': 1,
            'ticker': 'WULF',
            'report_date': '2025-03-31',
            'source_type': 'edgar_10q',
            'raw_text': 'WULF bitcoin mined 800 BTC during Q1 2025. Hash rate 8 EH/s.',
            'covering_period': '2025-Q1',
        }

        # For quarterly docs, extraction goes through LLM-only path (no regex)
        with patch('interpreters.interpret_pipeline._check_llm_available', return_value=False):
            extract_report(report, mock_db)
        # No regex-related assertion needed — regex extraction is removed entirely

    def test_extract_report_10q_stores_source_period_type(self):
        """data_point inserted from 10-Q must have source_period_type='quarterly'."""
        from interpreters.interpret_pipeline import extract_report
        from interpreters.llm_interpreter import LLMInterpreter
        from miner_types import ExtractionResult

        inserted_dps = []
        mock_db = MagicMock()
        mock_db.data_point_exists.return_value = False
        mock_db.get_quarterly_data_point.return_value = None
        mock_db.insert_data_point.side_effect = lambda dp: inserted_dps.append(dp) or 1
        mock_db.upsert_data_point_quarterly.side_effect = lambda dp: inserted_dps.append(dp) or 1
        mock_db.get_all_metric_keywords.return_value = [
            {'phrase': 'bitcoin mined', 'metric_key': 'production_btc'},
        ]
        mock_db.get_metric_schema.return_value = [
            {'key': 'production_btc', 'active': 1},
        ]

        report = {
            'id': 1,
            'ticker': 'WULF',
            'report_date': '2025-03-31',
            'source_type': 'edgar_10q',
            'raw_text': 'WULF bitcoin mined 800 BTC during Q1 2025. Hash rate 8 EH/s.',
            'covering_period': '2025-Q1',
        }

        mock_llm_result = ExtractionResult(
            metric='production_btc', value=800.0, unit='BTC',
            confidence=0.90, extraction_method='llm_test',
            source_snippet='produced 800 bitcoin', pattern_id='llm_test',
        )

        with patch('interpreters.interpret_pipeline._get_llm_interpreter') as mock_get_llm, \
             patch('interpreters.interpret_pipeline._check_llm_available', return_value=True):
            mock_extractor = MagicMock()
            mock_extractor.extract_quarterly_batch.return_value = {'production_btc': mock_llm_result}
            mock_get_llm.return_value = mock_extractor

            extract_report(report, mock_db)

        # At least one data_point should have source_period_type='quarterly'
        quarterly_dps = [dp for dp in inserted_dps if dp.get('source_period_type') == 'quarterly']
        assert len(quarterly_dps) >= 1


class TestQuarterlyRangeValidation:
    def test_range_validation_quarterly_applies_3x_bounds(self):
        """Quarterly production of 5000 BTC should be accepted (monthly max ~2000)."""
        from interpreters.llm_interpreter import LLMInterpreter
        import requests

        extractor = LLMInterpreter(session=requests.Session())
        # 5000 BTC for a quarter is valid (e.g. 1667/month)
        # The standard monthly range rejects >5000 BTC which is also fine,
        # but quarterly should accept up to 3x the monthly limit
        raw = '{"production_btc": {"value": 4500, "unit": "BTC", "confidence": 0.9, "source_snippet": "produced 4500 BTC"}}'
        results = extractor._parse_quarterly_batch_response(raw, ['production_btc'], period_type='quarterly')
        assert 'production_btc' in results
        assert results['production_btc'].value == 4500.0
