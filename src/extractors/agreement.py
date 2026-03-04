"""
Agreement engine: compare LLM and regex extraction results.

Implements per-metric tolerance rules:
  abs(llm - regex) / max(llm, regex) <= threshold → AUTO_ACCEPT (regex value stored)
  Otherwise → REVIEW_QUEUE

Decision codes:
  AUTO_ACCEPT   — both sources agree within threshold; regex value accepted
  REVIEW_QUEUE  — both sources found values but disagree > threshold
  LLM_ONLY      — only LLM found a value (regex found nothing)
  REGEX_ONLY    — only regex found a value (LLM found nothing)
  NO_EXTRACTION — neither source found a value (period gap)

agreement_status values for review_queue:
  REVIEW_QUEUE       — LLM/regex disagreement beyond per-metric threshold
  LLM_ONLY           — only LLM found a value
  REGEX_ONLY         — only regex found a value
  OUTLIER_FLAGGED    — agreed but value deviates > outlier threshold vs trailing avg
  CORRECTION_FAILED  — self-correction pass ran but candidate still disagreed/outlier
"""
from dataclasses import dataclass, field
from typing import Optional

from config import (
    METRIC_AGREEMENT_THRESHOLDS,
    METRIC_AGREEMENT_THRESHOLD_DEFAULT,
    OUTLIER_MIN_HISTORY,
)
from miner_types import ExtractionResult


@dataclass
class AgreementDecision:
    """Result of comparing regex and LLM extraction candidates."""
    decision: str                       # AUTO_ACCEPT | REVIEW_QUEUE | LLM_ONLY | REGEX_ONLY | NO_EXTRACTION
    accepted_value: Optional[float]     # The regex value to store in data_points (only on AUTO_ACCEPT)
    regex_value: Optional[float]
    llm_value: Optional[float]
    agreement_pct: Optional[float]      # Absolute percentage difference (e.g. 1.96 means 1.96%)
    regex_result: Optional[ExtractionResult]
    llm_result: Optional[ExtractionResult]
    outlier_flag: bool = False
    outlier_trailing_avg: Optional[float] = None


def evaluate_agreement(
    regex_result: Optional[ExtractionResult],
    llm_result: Optional[ExtractionResult],
    metric: Optional[str] = None,
) -> AgreementDecision:
    """
    Compare regex and LLM extraction results for the same document and metric.

    Args:
        regex_result: Best result from the regex extractor, or None.
        llm_result:   Result from the LLM extractor, or None.
        metric:       Metric name (e.g. 'hashrate_eh') for per-metric threshold
                      lookup. If None, uses METRIC_AGREEMENT_THRESHOLD_DEFAULT.

    Returns:
        AgreementDecision with routing decision and candidate values.
    """
    rv = regex_result.value if regex_result is not None else None
    lv = llm_result.value if llm_result is not None else None

    # Neither found anything → gap
    if rv is None and lv is None:
        return AgreementDecision(
            decision='NO_EXTRACTION',
            accepted_value=None,
            regex_value=None,
            llm_value=None,
            agreement_pct=None,
            regex_result=None,
            llm_result=None,
        )

    # Only one source produced a value → review queue
    if rv is None:
        return AgreementDecision(
            decision='LLM_ONLY',
            accepted_value=None,
            regex_value=None,
            llm_value=lv,
            agreement_pct=None,
            regex_result=regex_result,
            llm_result=llm_result,
        )

    if lv is None:
        return AgreementDecision(
            decision='REGEX_ONLY',
            accepted_value=None,
            regex_value=rv,
            llm_value=None,
            agreement_pct=None,
            regex_result=regex_result,
            llm_result=llm_result,
        )

    # Both found values — compute agreement percentage
    # Special case: both zero → auto-accept (no division)
    if rv == 0.0 and lv == 0.0:
        return AgreementDecision(
            decision='AUTO_ACCEPT',
            accepted_value=0.0,
            regex_value=0.0,
            llm_value=0.0,
            agreement_pct=0.0,
            regex_result=regex_result,
            llm_result=llm_result,
        )

    denominator = max(abs(rv), abs(lv))
    if denominator == 0.0:
        # One is zero, other is not — treat as 100% disagreement
        agreement_pct = 100.0
    else:
        agreement_pct = abs(rv - lv) / denominator * 100.0

    # Per-metric threshold lookup
    threshold = METRIC_AGREEMENT_THRESHOLDS.get(metric, METRIC_AGREEMENT_THRESHOLD_DEFAULT)
    threshold_pct = threshold * 100.0

    if agreement_pct <= threshold_pct:
        # Within tolerance → auto-accept, store regex value
        return AgreementDecision(
            decision='AUTO_ACCEPT',
            accepted_value=rv,
            regex_value=rv,
            llm_value=lv,
            agreement_pct=agreement_pct,
            regex_result=regex_result,
            llm_result=llm_result,
        )
    else:
        # Outside tolerance → send to review queue
        return AgreementDecision(
            decision='REVIEW_QUEUE',
            accepted_value=None,
            regex_value=rv,
            llm_value=lv,
            agreement_pct=agreement_pct,
            regex_result=regex_result,
            llm_result=llm_result,
        )


def detect_outlier(
    candidate: float,
    trailing_values: list,
    threshold_pct: float,
) -> tuple:
    """Return (is_outlier, trailing_avg).

    Compares candidate against the mean of trailing_values.
    is_outlier=True if |candidate - avg| / max(avg, 1e-9) > threshold_pct.
    Returns (False, None) if len(trailing_values) < OUTLIER_MIN_HISTORY.

    Args:
        candidate:      The new value to test.
        trailing_values: Historical values (monthly) for the same ticker+metric.
        threshold_pct:  Fractional deviation threshold (e.g. 0.40 = 40%).

    Returns:
        (is_outlier: bool, trailing_avg: float or None)
    """
    if len(trailing_values) < OUTLIER_MIN_HISTORY:
        return False, None

    avg = sum(trailing_values) / len(trailing_values)
    denominator = max(abs(avg), 1e-9)
    deviation = abs(candidate - avg) / denominator
    is_outlier = deviation > threshold_pct
    return is_outlier, avg
