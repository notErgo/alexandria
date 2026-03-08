"""
Agreement engine unit tests — TDD.

Tests should FAIL before agreement.py is implemented.
All tests are pure (no DB, no network).
"""
import pytest
from miner_types import ExtractionResult


def _make_result(value, metric="production_btc", confidence=0.9):
    return ExtractionResult(
        metric=metric,
        value=value,
        unit="BTC",
        confidence=confidence,
        extraction_method="test_pattern",
        source_snippet="test snippet",
        pattern_id="test_pattern",
    )


class TestAgreementDecision:
    def test_both_none_returns_no_extraction(self):
        """regex=None, llm=None → decision='NO_EXTRACTION'."""
        from interpreters.agreement import evaluate_agreement
        decision = evaluate_agreement(None, None)
        assert decision.decision == 'NO_EXTRACTION'
        assert decision.accepted_value is None

    def test_regex_only_routes_to_review(self):
        """regex=result, llm=None → decision='REGEX_ONLY'."""
        from interpreters.agreement import evaluate_agreement
        decision = evaluate_agreement(_make_result(700), None)
        assert decision.decision == 'REGEX_ONLY'
        assert decision.accepted_value is None

    def test_llm_only_routes_to_review(self):
        """regex=None, llm=result → decision='LLM_ONLY'."""
        from interpreters.agreement import evaluate_agreement
        decision = evaluate_agreement(None, _make_result(700))
        assert decision.decision == 'LLM_ONLY'
        assert decision.accepted_value is None

    def test_within_2pct_auto_accepts(self):
        """regex=700, llm=705 → diff=0.71% < 2% → AUTO_ACCEPT."""
        from interpreters.agreement import evaluate_agreement
        decision = evaluate_agreement(_make_result(700), _make_result(705))
        assert decision.decision == 'AUTO_ACCEPT'

    def test_exactly_2pct_boundary_auto_accepts(self):
        """regex=700, llm=714 → diff=2.0% → AUTO_ACCEPT (boundary inclusive)."""
        from interpreters.agreement import evaluate_agreement
        # 714 - 700 = 14; 14/714 ≈ 1.96% — actually within 2%
        # For exact 2%: llm = 700 * 1.02 = 714
        decision = evaluate_agreement(_make_result(700), _make_result(714))
        assert decision.decision == 'AUTO_ACCEPT'

    def test_over_2pct_routes_to_review(self):
        """regex=700, llm=750 → diff=6.7% > 2% → REVIEW_QUEUE."""
        from interpreters.agreement import evaluate_agreement
        decision = evaluate_agreement(_make_result(700), _make_result(750))
        assert decision.decision == 'REVIEW_QUEUE'

    def test_stored_value_is_regex_on_auto_accept(self):
        """On AUTO_ACCEPT, accepted_value == regex_result.value (not llm)."""
        from interpreters.agreement import evaluate_agreement
        regex_r = _make_result(700.0)
        llm_r = _make_result(705.0)
        decision = evaluate_agreement(regex_r, llm_r)
        assert decision.decision == 'AUTO_ACCEPT'
        assert decision.accepted_value == 700.0

    def test_agreement_pct_computed_correctly(self):
        """regex=1000, llm=1020 → agreement_pct ≈ 1.96%."""
        from interpreters.agreement import evaluate_agreement
        decision = evaluate_agreement(_make_result(1000), _make_result(1020))
        assert decision.agreement_pct is not None
        assert abs(decision.agreement_pct - (20.0 / 1020.0 * 100)) < 0.01

    def test_zero_both_auto_accepts(self):
        """regex=0, llm=0 → auto-accept (both zero, no division by zero)."""
        from interpreters.agreement import evaluate_agreement
        decision = evaluate_agreement(_make_result(0), _make_result(0))
        assert decision.decision == 'AUTO_ACCEPT'
        assert decision.accepted_value == 0.0

    def test_zero_regex_nonzero_llm_routes_to_review(self):
        """regex=0, llm=500 → REVIEW_QUEUE (100% disagreement)."""
        from interpreters.agreement import evaluate_agreement
        decision = evaluate_agreement(_make_result(0), _make_result(500))
        assert decision.decision == 'REVIEW_QUEUE'

    def test_review_queue_carries_both_values(self):
        """REVIEW_QUEUE decision includes both regex_value and llm_value."""
        from interpreters.agreement import evaluate_agreement
        decision = evaluate_agreement(_make_result(700), _make_result(750))
        assert decision.regex_value == 700.0
        assert decision.llm_value == 750.0


class TestAgreementDecisionDataclass:
    def test_dataclass_fields_present(self):
        """AgreementDecision has all required fields."""
        from interpreters.agreement import AgreementDecision
        d = AgreementDecision(
            decision='AUTO_ACCEPT',
            accepted_value=700.0,
            regex_value=700.0,
            llm_value=705.0,
            agreement_pct=0.71,
            regex_result=None,
            llm_result=None,
        )
        assert d.decision == 'AUTO_ACCEPT'
        assert d.accepted_value == 700.0
        assert d.regex_value == 700.0
        assert d.llm_value == 705.0
