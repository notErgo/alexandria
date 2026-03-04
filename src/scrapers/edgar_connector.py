"""
SEC EDGAR connector: queries full-text search API for 8-K filings containing
bitcoin production data, fetches filing text, and runs extraction pipeline.

API: https://efts.sec.gov/LATEST/search-index
Requires User-Agent header (403 without).
Rate limit: 0.1s between requests (empirically safe).
"""
import re
import time
import logging
from dataclasses import dataclass
from datetime import date, datetime
from typing import Optional

import requests
from bs4 import BeautifulSoup

from config import EDGAR_BASE_URL, EDGAR_REQUEST_DELAY_SECONDS

log = logging.getLogger('miners.scrapers.edgar_connector')

_USER_AGENT = "Hermeneutic Research Platform contact@example.com"
_DATE_PATTERN = re.compile(r"(\d{4})-(\d{2})-(\d{2})")


def _parse_edgar_hit(hit: dict) -> dict:
    """
    Extract structured fields from an EDGAR full-text search hit.

    Returns dict with: filed_date (date), accession_number (str), entity_name (str).
    """
    source = hit.get("_source", {})
    file_date_str = source.get("file_date", "")
    m = _DATE_PATTERN.match(file_date_str)
    filed_date = date(int(m.group(1)), int(m.group(2)), int(m.group(3))) if m else None
    return {
        "filed_date": filed_date,
        "accession_number": hit.get("_id", ""),
        "entity_name": source.get("entity_name", ""),
        "period_ending": source.get("period_ending") or source.get("period_of_report"),
    }


def _build_filing_url(accession_number: str, cik: str) -> str:
    """Construct EDGAR filing index URL from accession number and CIK."""
    cik_clean = cik.lstrip("0")
    acc_clean = accession_number.replace("-", "")
    return (
        f"https://www.sec.gov/Archives/edgar/{cik_clean}/{acc_clean}/"
        f"{accession_number}-index.htm"
    )


@dataclass
class EdgarConnector:
    """Fetches 8-K filings from SEC EDGAR for a given company."""
    db: object          # MinerDB
    registry: object    # PatternRegistry
    session: requests.Session

    def _edgar_request(self, url: str, params: dict = None) -> Optional[dict]:
        """Make a rate-limited GET request to EDGAR. Returns JSON or None."""
        time.sleep(EDGAR_REQUEST_DELAY_SECONDS)
        try:
            resp = self.session.get(
                url,
                params=params,
                timeout=15,
                headers={"User-Agent": _USER_AGENT},
            )
            if resp.status_code == 400:
                log.debug("EDGAR returned 400 for %s", url)
                return None
            resp.raise_for_status()
            return resp.json()
        except requests.Timeout:
            log.warning("Timeout fetching EDGAR %s", url)
            return None
        except requests.RequestException as e:
            log.error("EDGAR request error %s: %s", url, e, exc_info=True)
            return None
        except (ValueError, KeyError) as e:
            log.error("Bad EDGAR response from %s: %s", url, e, exc_info=True)
            return None

    def _fetch_filing_text(self, accession_number: str, cik: str) -> str:
        """Fetch and parse the plain-text body of an EDGAR filing."""
        index_url = _build_filing_url(accession_number, cik)
        time.sleep(EDGAR_REQUEST_DELAY_SECONDS)
        try:
            resp = self.session.get(
                index_url,
                timeout=15,
                headers={"User-Agent": _USER_AGENT},
            )
            resp.raise_for_status()
            soup = BeautifulSoup(resp.text, 'lxml')
            return soup.get_text(separator=" ", strip=True)
        except Exception as e:
            log.error("Failed to fetch filing %s: %s", accession_number, e, exc_info=True)
            return ""

    def fetch_production_filings(
        self, cik: str, ticker: str, since_date: date
    ):
        """
        Search EDGAR for 8-K filings mentioning 'bitcoin production' since since_date.
        Extract data from each new filing and write to DB. Returns IngestSummary.
        """
        from miner_types import IngestSummary
        from config import CONFIDENCE_REVIEW_THRESHOLD
        from extractors.extractor import extract_all

        summary = IngestSummary()
        params = {
            "q": '"bitcoin production"',
            "forms": "8-K",
            "dateRange": "custom",
            "startdt": since_date.isoformat(),
            "entity": ticker,
        }
        data = self._edgar_request(EDGAR_BASE_URL, params)
        if data is None:
            summary.errors += 1
            return summary

        hits = data.get("hits", {}).get("hits", [])
        log.info("EDGAR returned %d hits for %s since %s", len(hits), ticker, since_date)

        for hit in hits:
            parsed = _parse_edgar_hit(hit)
            if parsed["filed_date"] is None:
                continue

            period_str = parsed["filed_date"].strftime("%Y-%m-%d")
            if self.db.report_exists(ticker, period_str, "edgar_8k"):
                log.debug("Already ingested EDGAR filing: %s %s", ticker, period_str)
                continue

            text = self._fetch_filing_text(parsed["accession_number"], cik)
            if not text.strip():
                summary.errors += 1
                continue

            report = {
                "ticker": ticker,
                "report_date": period_str,
                "published_date": period_str,
                "source_type": "edgar_8k",
                "source_url": _build_filing_url(parsed["accession_number"], cik),
                "raw_text": text[:50000],
                "parsed_at": datetime.utcnow().isoformat(),
            }
            try:
                report_id = self.db.insert_report(report)
                summary.reports_ingested += 1
            except Exception as e:
                log.error("Failed to insert EDGAR report %s %s: %s", ticker, period_str, e, exc_info=True)
                summary.errors += 1
                continue

            for metric, patterns in self.registry.metrics.items():
                results = extract_all(text, patterns, metric)
                for result in results:
                    dp = {
                        "report_id": report_id,
                        "ticker": ticker,
                        "period": period_str,
                        "metric": result.metric,
                        "value": result.value,
                        "unit": result.unit,
                        "confidence": result.confidence,
                        "extraction_method": result.extraction_method,
                        "source_snippet": result.source_snippet,
                    }
                    if result.confidence >= CONFIDENCE_REVIEW_THRESHOLD:
                        self.db.insert_data_point(dp)
                        summary.data_points_extracted += 1
                    else:
                        self.db.insert_review_item({
                            "data_point_id": None,
                            "ticker": ticker,
                            "period": period_str,
                            "metric": result.metric,
                            "raw_value": str(result.value),
                            "confidence": result.confidence,
                            "source_snippet": result.source_snippet,
                            "status": "PENDING",
                        })
                        summary.review_flagged += 1

        return summary
