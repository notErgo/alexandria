"""
EDGAR-first coverage scout runner.

Builds per-ticker coverage state and scrape directives, then writes:
  /private/tmp/claude-501/miners_progress/coverage_scout_<ticker>.json

Default pilot tickers are DB-aligned:
  RIOT, CLSK, WULF, ABTC
"""
from __future__ import annotations

import argparse
import calendar
import json
import logging
import os
import statistics
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Optional

import requests

import sys

# Coverage scout is read-mostly and should work against read-only DB mounts.
# Disable startup config sync unless explicitly overridden by caller.
os.environ.setdefault("MINERS_AUTO_SYNC_COMPANIES", "0")

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from config import DATA_DIR  # noqa: E402
from infra.db import MinerDB  # noqa: E402

log = logging.getLogger("miners.coverage_scout")

# canonical-sources: noqa — intentional CLI default subset (4 pilot tickers), not a copy of all tickers
DEFAULT_TICKERS = ["RIOT", "CLSK", "WULF", "ABTC"]
DEFAULT_OUTPUT_DIR = Path("/private/tmp/claude-501/miners_progress")
ANALYST_METHODS = {"analyst", "analyst_approved", "review_approved", "review_edited"}


@dataclass
class EdgarFiling:
    form: str
    filing_date: str
    period_of_report: str
    accession_number: str
    primary_doc: str


def _month_start(d: date) -> date:
    return date(d.year, d.month, 1)


def _parse_iso_date(s: Optional[str]) -> Optional[date]:
    if not s:
        return None
    try:
        return date.fromisoformat(s[:10])
    except Exception:
        return None


def _month_iter(start_month: date, end_month: date) -> list[str]:
    out: list[str] = []
    cur = date(start_month.year, start_month.month, 1)
    end = date(end_month.year, end_month.month, 1)
    while cur <= end:
        out.append(cur.strftime("%Y-%m-01"))
        if cur.month == 12:
            cur = date(cur.year + 1, 1, 1)
        else:
            cur = date(cur.year, cur.month + 1, 1)
    return out


def _quarter_months(covering_period: str) -> list[str]:
    # covering_period example: "2025-Q2"
    year = int(covering_period[:4])
    q = covering_period[5:]
    first = {"Q1": 1, "Q2": 4, "Q3": 7, "Q4": 10}.get(q)
    if first is None:
        return []
    return [
        f"{year}-{first:02d}-01",
        f"{year}-{first + 1:02d}-01",
        f"{year}-{first + 2:02d}-01",
    ]


def _fetch_edgar_submissions(cik: str, session: requests.Session) -> list[EdgarFiling]:
    cik_10 = str(cik).zfill(10)
    url = f"https://data.sec.gov/submissions/CIK{cik_10}.json"
    resp = session.get(url, timeout=30)
    resp.raise_for_status()
    payload = resp.json()
    recent = payload.get("filings", {}).get("recent", {})

    forms = recent.get("form", [])
    dates = recent.get("filingDate", [])
    periods = recent.get("periodOfReport", [])
    accessions = recent.get("accessionNumber", [])
    docs = recent.get("primaryDocument", [])

    filings: list[EdgarFiling] = []
    for i, form in enumerate(forms):
        if form not in {"10-Q", "10-K", "8-K"}:
            continue
        filings.append(
            EdgarFiling(
                form=form,
                filing_date=dates[i] if i < len(dates) else "",
                period_of_report=periods[i] if i < len(periods) else "",
                accession_number=accessions[i] if i < len(accessions) else "",
                primary_doc=docs[i] if i < len(docs) else "",
            )
        )
    return filings


def _infer_cadence_windows(
    ticker: str,
    company: dict,
    report_dates: list[str],
    edgar_filings: list[EdgarFiling],
    as_of: date,
) -> list[dict]:
    # Prefer explicit regime windows if present elsewhere in DB; this function is fallback-only.
    evidence_dates = [_parse_iso_date(x) for x in report_dates]
    evidence_dates = [d for d in evidence_dates if d is not None]
    evidence_dates.sort()

    # ABTC and other event-driven profiles should default to announcement cadence.
    if ticker == "ABTC" or ((company.get("scrape_mode") or "").lower() == "index" and not report_dates):
        start = date(company.get("pr_start_year") or as_of.year, 1, 1)
        return [{
            "ticker": ticker,
            "cadence": "announcement",
            "start_date": start.isoformat(),
            "end_date": None,
            "confidence": 0.9,
            "evidence": ["index_mode_event_driven_profile"],
        }]

    # If we have enough dated evidence, infer cadence from median interval.
    if len(evidence_dates) >= 4:
        deltas = [(evidence_dates[i] - evidence_dates[i - 1]).days for i in range(1, len(evidence_dates))]
        median_days = statistics.median(deltas)
        start = date(company.get("pr_start_year") or evidence_dates[0].year, 1, 1)
        if median_days <= 45:
            return [{
                "ticker": ticker,
                "cadence": "monthly",
                "start_date": start.isoformat(),
                "end_date": None,
                "confidence": 0.85,
                "evidence": [f"median_interval_days={median_days:.1f}"],
            }]
        if median_days >= 70:
            return [{
                "ticker": ticker,
                "cadence": "quarterly",
                "start_date": start.isoformat(),
                "end_date": None,
                "confidence": 0.85,
                "evidence": [f"median_interval_days={median_days:.1f}"],
            }]

    # Fallback using EDGAR forms.
    if any(f.form in {"10-Q", "10-K"} for f in edgar_filings):
        start = date(company.get("pr_start_year") or as_of.year, 1, 1)
        return [{
            "ticker": ticker,
            "cadence": "quarterly",
            "start_date": start.isoformat(),
            "end_date": None,
            "confidence": 0.7,
            "evidence": ["edgar_quarterly_forms_present"],
        }]

    start = date(company.get("pr_start_year") or as_of.year, 1, 1)
    return [{
        "ticker": ticker,
        "cadence": "monthly",
        "start_date": start.isoformat(),
        "end_date": None,
        "confidence": 0.6,
        "evidence": ["fallback_default_monthly"],
    }]


def _expected_periods(
    company: dict,
    cadence_windows: list[dict],
    report_dates: list[str],
    edgar_filings: list[EdgarFiling],
    as_of: date,
    inactive_end_month: Optional[date],
) -> list[str]:
    end_month = inactive_end_month if inactive_end_month else _month_start(as_of)
    if company.get("active", 1):
        end_month = _month_start(as_of)

    expected: set[str] = set()
    for window in cadence_windows:
        cadence = window.get("cadence", "monthly")
        start = _parse_iso_date(window.get("start_date")) or date(company.get("pr_start_year") or as_of.year, 1, 1)
        end = _parse_iso_date(window.get("end_date")) or end_month
        start = _month_start(start)
        end = _month_start(min(end, end_month))
        if end < start:
            continue

        if cadence == "monthly":
            expected.update(_month_iter(start, end))
        elif cadence == "quarterly":
            for month in _month_iter(start, end):
                if month[5:7] in {"01", "04", "07", "10"}:
                    expected.add(month)
        elif cadence == "announcement":
            # Announcement cadence expects disclosure months only.
            for rd in report_dates:
                p = _parse_iso_date(rd)
                if p:
                    expected.add(p.strftime("%Y-%m-01"))
            for filing in edgar_filings:
                p = _parse_iso_date(filing.filing_date)
                if p:
                    expected.add(p.strftime("%Y-%m-01"))

    if not expected:
        start = date(company.get("pr_start_year") or as_of.year, 1, 1)
        expected.update(_month_iter(_month_start(start), end_month))
    return sorted(expected)


def _build_source_hints(ticker: str, period: str, company: dict, has_cik: bool) -> list[str]:
    y = period[:4]
    m = period[5:7]
    last_day = calendar.monthrange(int(y), int(m))[1]
    hints: list[str] = []
    if has_cik:
        hints.append(
            f"https://efts.sec.gov/LATEST/search-index?q=%22{ticker}%22%20%22bitcoin%22&forms=8-K&dateRange=custom&startdt={y}-{m}-01&enddt={y}-{m}-{last_day:02d}"
        )
    hints.append(f"site:globenewswire.com \"{company.get('name', ticker)}\" \"{period[:7]}\" bitcoin production")
    hints.append(f"site:prnewswire.com \"{company.get('name', ticker)}\" \"{period[:7]}\" bitcoin")
    if company.get("ir_url"):
        hints.append(company["ir_url"])
    return hints


def _pick_state_and_directive(
    ticker: str,
    period: str,
    metric: str,
    conn,
    sec_candidate_periods: set[str],
    monthly_window_periods: set[str],
    company: dict,
    keywords: list[str],
) -> tuple[dict, Optional[dict]]:
    period_ym = period[:7]
    dp_monthly = conn.execute(
        """SELECT extraction_method FROM data_points
           WHERE ticker=? AND period=? AND metric=? AND source_period_type='monthly' LIMIT 1""",
        (ticker, period, metric),
    ).fetchone()
    if dp_monthly:
        return {"period": period, "state": "data", "priority": "none", "reason": "monthly_data_present"}, None

    # Quarterly bridge evidence: a quarterly/annual point covering this month.
    q_rows = conn.execute(
        """SELECT covering_period FROM data_points
           WHERE ticker=? AND metric=? AND source_period_type IN ('quarterly','annual')""",
        (ticker, metric),
    ).fetchall()
    for row in q_rows:
        cp = row["covering_period"] or ""
        if cp.endswith("-Q1") or cp.endswith("-Q2") or cp.endswith("-Q3") or cp.endswith("-Q4"):
            if period in _quarter_months(cp):
                return {
                    "period": period,
                    "state": "data_quarterly",
                    "priority": "none",
                    "reason": f"covered_by_{cp}",
                }, None

    rq = conn.execute(
        """SELECT id FROM review_queue
           WHERE ticker=? AND period=? AND metric=? AND status='PENDING' LIMIT 1""",
        (ticker, period, metric),
    ).fetchone()
    if rq:
        state = {"period": period, "state": "review_pending", "priority": "medium", "reason": "pending_review_queue_item"}
        directive = {
            "period": period,
            "priority": "medium",
            "state": "review_pending",
            "strategy": "analyst_review_required",
            "source_hints": [],
            "timeout_seconds": 0,
            "max_retries": 0,
        }
        return state, directive

    report_parse_failed = conn.execute(
        """SELECT id FROM reports
           WHERE ticker=? AND substr(report_date,1,7)=? AND parse_quality='parse_failed' LIMIT 1""",
        (ticker, period_ym),
    ).fetchone()
    if report_parse_failed:
        priority = "high" if period in sec_candidate_periods or period in monthly_window_periods else "medium"
        state = {"period": period, "state": "parse_failed", "priority": priority, "reason": "report_parse_failed"}
        directive = {
            "period": period,
            "priority": priority,
            "state": "parse_failed",
            "strategy": "reparse_then_reextract_then_edgar_refresh",
            "source_hints": _build_source_hints(ticker, period, company, bool(company.get("cik"))),
            "timeout_seconds": 30,
            "max_retries": 2,
        }
        if keywords:
            directive["keywords"] = keywords
        return state, directive

    report_ingested_not_extracted = conn.execute(
        """SELECT id FROM reports
           WHERE ticker=? AND substr(report_date,1,7)=? AND coalesce(extracted_at, '') = '' LIMIT 1""",
        (ticker, period_ym),
    ).fetchone()
    if report_ingested_not_extracted:
        priority = "high" if period in sec_candidate_periods or period in monthly_window_periods else "medium"
        state = {"period": period, "state": "extract_failed", "priority": priority, "reason": "report_ingested_pending_extraction"}
        directive = {
            "period": period,
            "priority": priority,
            "state": "extract_failed",
            "strategy": "extract_ingested_report_then_reconcile",
            "source_hints": [],
            "timeout_seconds": 15,
            "max_retries": 1,
        }
        return state, directive

    report_extracted_no_dp = conn.execute(
        """SELECT id FROM reports
           WHERE ticker=? AND substr(report_date,1,7)=? AND extracted_at IS NOT NULL LIMIT 1""",
        (ticker, period_ym),
    ).fetchone()
    if report_extracted_no_dp:
        priority = "high" if period in sec_candidate_periods or period in monthly_window_periods else "medium"
        state = {"period": period, "state": "extract_failed", "priority": priority, "reason": "report_extracted_without_metric"}
        directive = {
            "period": period,
            "priority": priority,
            "state": "extract_failed",
            "strategy": "reextract_existing_report_then_edgar_refresh",
            "source_hints": _build_source_hints(ticker, period, company, bool(company.get("cik"))),
            "timeout_seconds": 30,
            "max_retries": 2,
        }
        if keywords:
            directive["keywords"] = keywords
        return state, directive

    manifest_failed = conn.execute(
        """SELECT id FROM asset_manifest
           WHERE ticker=? AND period=? AND (ingest_state='failed' OR ingest_error IS NOT NULL)
           LIMIT 1""",
        (ticker, period),
    ).fetchone()
    if manifest_failed:
        priority = "high" if period in sec_candidate_periods or period in monthly_window_periods else "medium"
        state = {"period": period, "state": "scraper_error", "priority": priority, "reason": "manifest_ingest_failed"}
        directive = {
            "period": period,
            "priority": priority,
            "state": "scraper_error",
            "strategy": "edgar_then_wire_then_ir",
            "source_hints": _build_source_hints(ticker, period, company, bool(company.get("cik"))),
            "timeout_seconds": 30,
            "max_retries": 3,
        }
        if keywords:
            directive["keywords"] = keywords
        return state, directive

    # Analyst-approved intentional gap marker (best-effort heuristic).
    analyst_gap = conn.execute(
        """SELECT id FROM review_queue
           WHERE ticker=? AND period=? AND metric=?
             AND status IN ('APPROVED','EDITED')
             AND lower(coalesce(reviewer_note, '')) LIKE '%intentional%'
           LIMIT 1""",
        (ticker, period, metric),
    ).fetchone()
    if analyst_gap:
        return {"period": period, "state": "analyst_gap", "priority": "low", "reason": "analyst_marked_intentional_gap"}, None

    priority = "high" if period in sec_candidate_periods or period in monthly_window_periods else "medium"
    state = {"period": period, "state": "no_document", "priority": priority, "reason": "expected_period_missing_source"}
    directive = {
        "period": period,
        "priority": priority,
        "state": "no_document",
        "strategy": "edgar_then_wire_then_ir",
        "source_hints": _build_source_hints(ticker, period, company, bool(company.get("cik"))),
        "timeout_seconds": 30,
        "max_retries": 3,
    }
    if keywords:
        directive["keywords"] = keywords
    return state, directive


def build_coverage_scout_for_ticker(
    db: MinerDB,
    ticker: str,
    as_of: date,
    metric: str,
    session: requests.Session,
    keywords: Optional[list[str]] = None,
) -> dict:
    keywords = keywords or []
    company = db.get_company(ticker)
    if not company:
        return {
            "ticker": ticker,
            "run_id": f"coverage_scout_{as_of.isoformat()}",
            "as_of_date": as_of.isoformat(),
            "error": "ticker_not_found",
        }

    with db._get_connection() as conn:
        rows = conn.execute(
            "SELECT report_date FROM reports WHERE ticker=? ORDER BY report_date",
            (ticker,),
        ).fetchall()
        report_dates = [r["report_date"] for r in rows]

        last_report = _parse_iso_date(report_dates[-1]) if report_dates else None
        last_dp = conn.execute(
            "SELECT max(period) p FROM data_points WHERE ticker=?",
            (ticker,),
        ).fetchone()["p"]
        last_dp_date = _parse_iso_date(last_dp)

    edgar_filings: list[EdgarFiling] = []
    edgar_error = None
    cik = (company.get("cik") or "").strip()
    if cik:
        try:
            edgar_filings = _fetch_edgar_submissions(cik, session)
        except Exception as exc:
            edgar_error = str(exc)
            log.warning("EDGAR submissions fetch failed for %s: %s", ticker, exc)

    with db._get_connection() as conn:
        db_windows = db.get_regime_windows(ticker)
    if db_windows:
        cadence_windows = []
        for w in db_windows:
            cadence_windows.append({
                "ticker": ticker,
                "cadence": w["cadence"],
                "start_date": w["start_date"],
                "end_date": w.get("end_date"),
                "confidence": 0.95,
                "evidence": ["regime_config_db"],
            })
    else:
        cadence_windows = _infer_cadence_windows(ticker, company, report_dates, edgar_filings, as_of)

    inactive_end = None
    if not company.get("active", 1):
        candidates = [d for d in [last_report, last_dp_date] if d is not None]
        filing_months = [_parse_iso_date(f.filing_date) for f in edgar_filings]
        filing_months = [d for d in filing_months if d is not None]
        if filing_months:
            candidates.append(max(filing_months))
        inactive_end = _month_start(max(candidates)) if candidates else _month_start(as_of)

    expected = _expected_periods(
        company=company,
        cadence_windows=cadence_windows,
        report_dates=report_dates,
        edgar_filings=edgar_filings,
        as_of=as_of,
        inactive_end_month=inactive_end,
    )

    sec_candidate_periods: set[str] = set()
    for filing in edgar_filings:
        fd = _parse_iso_date(filing.filing_date)
        if fd:
            sec_candidate_periods.add(fd.strftime("%Y-%m-01"))
        pd = _parse_iso_date(filing.period_of_report)
        if pd:
            sec_candidate_periods.add(pd.strftime("%Y-%m-01"))

    monthly_window_periods: set[str] = set()
    for w in cadence_windows:
        if w.get("cadence") != "monthly":
            continue
        start = _parse_iso_date(w.get("start_date"))
        end = _parse_iso_date(w.get("end_date")) or _month_start(as_of)
        if not start:
            continue
        monthly_window_periods.update(_month_iter(_month_start(start), _month_start(end)))

    missing_periods: list[dict] = []
    directives: list[dict] = []
    seen_directives: set[tuple[str, str]] = set()

    with db._get_connection() as conn:
        for period in expected:
            state, directive = _pick_state_and_directive(
                ticker=ticker,
                period=period,
                metric=metric,
                conn=conn,
                sec_candidate_periods=sec_candidate_periods,
                monthly_window_periods=monthly_window_periods,
                company=company,
                keywords=keywords,
            )
            if state["state"] in {"data", "data_quarterly", "analyst_gap"}:
                continue
            missing_periods.append(state)
            if directive:
                key = (directive["period"], directive["strategy"])
                if key not in seen_directives:
                    seen_directives.add(key)
                    directives.append(directive)

    directives.sort(key=lambda d: (0 if d["priority"] == "high" else 1, d["period"]))
    missing_periods.sort(key=lambda m: m["period"])

    resolved_states = {"data", "data_quarterly", "analyst_gap"}
    unresolved_count = len(missing_periods)
    total_expected = len(expected)
    covered_count = max(total_expected - unresolved_count, 0)
    coverage_ratio = (covered_count / total_expected) if total_expected else 0.0
    high_priority_gaps = sum(1 for m in missing_periods if m.get("priority") == "high")

    out = {
        "ticker": ticker,
        "run_id": f"coverage_scout_{as_of.isoformat()}",
        "as_of_date": as_of.isoformat(),
        "cadence_windows": cadence_windows,
        "expected_periods": expected,
        "missing_periods": missing_periods,
        "directives": directives,
        "finish_gate": {
            "target_coverage_ratio": 0.90,
            "max_high_priority_gaps": 2,
        },
        "summary": {
            "expected_period_count": total_expected,
            "covered_count": covered_count,
            "unresolved_count": unresolved_count,
            "high_priority_gaps": high_priority_gaps,
            "coverage_ratio": round(coverage_ratio, 4),
            "needs_analyst_confirm": any(float(w.get("confidence", 0)) < 0.8 for w in cadence_windows),
            "edgar_filings_considered": len(edgar_filings),
            "sec_candidate_period_count": len(sec_candidate_periods),
            "edgar_error": edgar_error,
            "keywords": keywords,
            "resolved_states": sorted(resolved_states),
        },
    }
    return out


def main() -> int:
    parser = argparse.ArgumentParser(description="Run EDGAR-first coverage scout and emit directives")
    parser.add_argument(
        "--tickers",
        default=",".join(DEFAULT_TICKERS),
        help=f"Comma-separated tickers (default: {','.join(DEFAULT_TICKERS)})",
    )
    parser.add_argument("--as-of", default=date.today().isoformat(), help="As-of date YYYY-MM-DD")
    parser.add_argument("--metric", default="production_btc", help="Metric used for gap state")
    parser.add_argument(
        "--output-dir",
        default=str(DEFAULT_OUTPUT_DIR),
        help=f"Output directory (default: {DEFAULT_OUTPUT_DIR})",
    )
    parser.add_argument("--pretty", action="store_true", help="Pretty-print JSON output")
    parser.add_argument(
        "--keywords",
        default="",
        help="Optional comma-separated search keywords added to directives",
    )
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")

    as_of = date.fromisoformat(args.as_of)
    tickers = [t.strip().upper() for t in args.tickers.split(",") if t.strip()]
    keywords = [k.strip().lower() for k in str(args.keywords or "").split(",") if k.strip()]
    out_dir = Path(args.output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    db = MinerDB(str(Path(DATA_DIR) / "minerdata.db"))
    session = requests.Session()
    session.headers["User-Agent"] = os.environ.get(
        "EDGAR_USER_AGENT", "Hermeneutic Research Platform contact@example.com"
    )
    session.headers["Accept"] = "application/json"

    all_summaries = []
    for ticker in tickers:
        result = build_coverage_scout_for_ticker(
            db=db,
            ticker=ticker,
            as_of=as_of,
            metric=args.metric,
            session=session,
            keywords=keywords,
        )
        path = out_dir / f"coverage_scout_{ticker}.json"
        with open(path, "w") as f:
            json.dump(result, f, indent=2 if args.pretty else None, sort_keys=False)
        log.info("Wrote %s", path)
        all_summaries.append(
            {
                "ticker": ticker,
                "coverage_ratio": result.get("summary", {}).get("coverage_ratio"),
                "high_priority_gaps": result.get("summary", {}).get("high_priority_gaps"),
                "expected_period_count": result.get("summary", {}).get("expected_period_count"),
            }
        )

    summary_path = out_dir / "coverage_scout_summary.json"
    with open(summary_path, "w") as f:
        json.dump(
            {
                "as_of_date": as_of.isoformat(),
                "tickers": tickers,
                "metric": args.metric,
                "results": all_summaries,
            },
            f,
            indent=2 if args.pretty else None,
        )
    log.info("Wrote %s", summary_path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
