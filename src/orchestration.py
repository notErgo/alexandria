"""
Orchestration guardrails for the ingest pipeline.

EDGAR-first policy: IR and archive extraction should only run after EDGAR
has been fetched, so that cross-source agreement scoring has full context.
"""
import logging
from dataclasses import dataclass, field
from typing import Optional

log = logging.getLogger('miners.orchestration')


@dataclass
class EdgarCheckResult:
    """Result of an EDGAR prerequisite check."""
    complete: bool
    ticker: Optional[str]
    last_run: Optional[dict] = None
    warning: Optional[str] = None


def check_edgar_complete(db, ticker: Optional[str] = None) -> EdgarCheckResult:
    """Check whether a successful EDGAR pipeline run exists for ticker (or any ticker).

    Args:
        db: MinerDB instance
        ticker: Ticker symbol to check, or None for a global check

    Returns:
        EdgarCheckResult with complete=True if a successful run is found.
    """
    last_run = db.get_last_successful_pipeline_run(source='edgar', ticker=ticker)
    if last_run:
        return EdgarCheckResult(
            complete=True,
            ticker=ticker,
            last_run=last_run,
        )
    scope = ticker or 'any'
    warning = (
        f"No successful EDGAR run found for {scope}. "
        "Run POST /api/ingest/edgar before IR or archive extraction "
        "to ensure cross-source agreement has full EDGAR context."
    )
    log.warning("event=edgar_prereq_missing ticker=%s", scope)
    return EdgarCheckResult(
        complete=False,
        ticker=ticker,
        last_run=None,
        warning=warning,
    )
