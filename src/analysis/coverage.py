"""
Coverage matrix helpers for the 'cli.py diagnose' command.

Pure functions only — no Flask, no database calls.
Callers are responsible for fetching data_points, reports, and review_queue
from the DB and passing them in.
"""
from dataclasses import dataclass, field
from enum import Enum
from typing import Optional


class GapReason(Enum):
    """Reason a reporting period has no production_btc data point."""
    NO_FILE = "no_file"
    NO_EXTRACTION = "no_extraction"
    LOW_CONFIDENCE = "low_confidence"
    OK = "ok"


@dataclass
class CoverageRow:
    """Coverage status for one ticker + period combination."""
    ticker: str
    period: str
    reason: GapReason
    max_confidence: Optional[float] = None
    values: dict = field(default_factory=dict)


def build_coverage_row(
    ticker: str,
    period: str,
    data_points: list,
    reports: list,
    review_queue: list,
) -> CoverageRow:
    """
    Determine coverage status for a single ticker + period.

    Args:
        ticker:       Company ticker (e.g. "MARA").
        period:       ISO date string "YYYY-MM-DD".
        data_points:  List of data_point dicts (keys: metric, value) for this
                      ticker + period. May be empty.
        reports:      List of report dicts (key: period) for this ticker + period.
                      May be empty.
        review_queue: List of review_queue dicts (key: confidence) for this
                      ticker + period. May be empty.

    Returns:
        CoverageRow with:
          - reason=OK if production_btc is in data_points
          - reason=LOW_CONFIDENCE if reports exist, data_points empty,
            review_queue non-empty (max_confidence set to highest confidence)
          - reason=NO_EXTRACTION if reports exist, data_points empty,
            review_queue empty
          - reason=NO_FILE if no reports exist
    """
    # Build values dict from data_points
    values = {dp["metric"]: dp["value"] for dp in data_points}

    # OK: production_btc is present
    if "production_btc" in values:
        return CoverageRow(
            ticker=ticker,
            period=period,
            reason=GapReason.OK,
            values=values,
        )

    # No report at all
    if not reports:
        return CoverageRow(ticker=ticker, period=period, reason=GapReason.NO_FILE)

    # Report exists but no data_points
    if review_queue:
        max_conf = max(item["confidence"] for item in review_queue)
        return CoverageRow(
            ticker=ticker,
            period=period,
            reason=GapReason.LOW_CONFIDENCE,
            max_confidence=max_conf,
            values=values,
        )

    return CoverageRow(ticker=ticker, period=period, reason=GapReason.NO_EXTRACTION)
