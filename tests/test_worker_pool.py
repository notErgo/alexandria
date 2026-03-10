"""
Tests for parallel extraction worker pool.

Tests are written first (TDD). They FAIL before implementation and PASS after.

Coverage:
  - db.claim_report_for_extraction: atomic claim semantics
  - cmd_extract worker pool: N workers process M reports exactly once each
  - No double-processing under concurrent claim attempts
"""
import queue
import threading
import pytest
from helpers import make_report


# ── DB claim tests ────────────────────────────────────────────────────────────

class TestClaimReportForExtraction:

    @pytest.fixture
    def db_with_report(self, db):
        db.insert_company({
            'ticker': 'MARA', 'name': 'MARA Holdings, Inc.',
            'tier': 1, 'ir_url': 'https://www.marathondh.com/news',
            'pr_base_url': 'https://www.marathondh.com',
            'cik': '0001437491', 'active': 1,
        })
        report_id = db.insert_report(make_report(
            raw_text='MARA mined 700 BTC.',
            report_date='2024-09-01',
        ))
        return db, report_id

    def test_claim_returns_true_when_pending(self, db_with_report):
        """First claim on a pending report must return True."""
        db, report_id = db_with_report
        assert db.claim_report_for_extraction(report_id) is True

    def test_claim_sets_status_to_running(self, db_with_report):
        """Claiming a report must set extraction_status to 'running'."""
        db, report_id = db_with_report
        db.claim_report_for_extraction(report_id)
        report = db.get_report(report_id)
        assert report['extraction_status'] == 'running'

    def test_claim_returns_false_when_already_running(self, db_with_report):
        """Second claim on the same report must return False."""
        db, report_id = db_with_report
        assert db.claim_report_for_extraction(report_id) is True
        assert db.claim_report_for_extraction(report_id) is False

    def test_claim_returns_false_when_done(self, db_with_report):
        """Claim on an already-done report must return False."""
        db, report_id = db_with_report
        db.mark_report_extracted(report_id)
        assert db.claim_report_for_extraction(report_id) is False

    def test_concurrent_claims_only_one_wins(self, db_with_report):
        """Under concurrent access only one thread may claim a given report."""
        db, report_id = db_with_report
        results = []
        barrier = threading.Barrier(4)

        def try_claim():
            barrier.wait()  # all threads attempt simultaneously
            won = db.claim_report_for_extraction(report_id)
            results.append(won)

        threads = [threading.Thread(target=try_claim) for _ in range(4)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert results.count(True) == 1, (
            f"Exactly one thread must win the claim, got: {results}"
        )
        assert results.count(False) == 3


# ── Worker pool integration tests ─────────────────────────────────────────────

class TestWorkerPool:

    @pytest.fixture
    def db_with_company(self, db):
        db.insert_company({
            'ticker': 'MARA', 'name': 'MARA Holdings, Inc.',
            'tier': 1, 'ir_url': 'https://www.marathondh.com/news',
            'pr_base_url': 'https://www.marathondh.com',
            'cik': '0001437491', 'active': 1,
        })
        return db

    def _make_reports(self, db, n: int) -> list:
        ids = []
        for i in range(n):
            rid = db.insert_report(make_report(
                raw_text=f'MARA mined {700 + i} BTC.',
                report_date=f'2024-{(i % 12) + 1:02d}-01',
                source_url=f'https://example.com/report-{i}',
            ))
            ids.append(rid)
        return ids

    def test_all_reports_extracted_with_two_workers(self, db_with_company, monkeypatch):
        """With 2 workers and 6 reports, all 6 must be extracted exactly once."""
        import interpreters.interpret_pipeline as _ep
        from infra.db import MinerDB

        report_ids = self._make_reports(db_with_company, 6)
        extracted = []
        lock = threading.Lock()

        def fake_extract(report, db, registry, **kw):
            with lock:
                extracted.append(report['id'])
            db.mark_report_extracted(report['id'])
            from miner_types import ExtractionSummary
            s = ExtractionSummary()
            s.reports_processed = 1
            return s

        monkeypatch.setattr(_ep, 'extract_report', fake_extract)

        from cli import _run_worker_pool
        from interpreters.pattern_registry import PatternRegistry
        from config import CONFIG_DIR
        registry = PatternRegistry.load(CONFIG_DIR)

        _run_worker_pool(
            db_path=db_with_company.db_path,
            report_ids=report_ids,
            registry=registry,
            num_workers=2,
        )

        assert sorted(extracted) == sorted(report_ids), (
            f"Expected all {len(report_ids)} reports extracted once each.\n"
            f"Extracted: {sorted(extracted)}\nExpected: {sorted(report_ids)}"
        )

    def test_no_report_extracted_twice(self, db_with_company, monkeypatch):
        """No report ID must appear more than once in extracted list."""
        import interpreters.interpret_pipeline as _ep

        report_ids = self._make_reports(db_with_company, 10)
        extracted = []
        lock = threading.Lock()

        def fake_extract(report, db, registry, **kw):
            with lock:
                extracted.append(report['id'])
            db.mark_report_extracted(report['id'])
            from miner_types import ExtractionSummary
            s = ExtractionSummary()
            s.reports_processed = 1
            return s

        monkeypatch.setattr(_ep, 'extract_report', fake_extract)

        from cli import _run_worker_pool
        from interpreters.pattern_registry import PatternRegistry
        from config import CONFIG_DIR
        registry = PatternRegistry.load(CONFIG_DIR)

        _run_worker_pool(
            db_path=db_with_company.db_path,
            report_ids=report_ids,
            registry=registry,
            num_workers=4,
        )

        duplicates = [rid for rid in set(extracted) if extracted.count(rid) > 1]
        assert not duplicates, f"Reports extracted more than once: {duplicates}"

    def test_single_worker_behaves_like_serial(self, db_with_company, monkeypatch):
        """num_workers=1 must process all reports (regression guard for serial path)."""
        import interpreters.interpret_pipeline as _ep

        report_ids = self._make_reports(db_with_company, 4)
        extracted = []

        def fake_extract(report, db, registry, **kw):
            extracted.append(report['id'])
            db.mark_report_extracted(report['id'])
            from miner_types import ExtractionSummary
            s = ExtractionSummary()
            s.reports_processed = 1
            return s

        monkeypatch.setattr(_ep, 'extract_report', fake_extract)

        from cli import _run_worker_pool
        from interpreters.pattern_registry import PatternRegistry
        from config import CONFIG_DIR
        registry = PatternRegistry.load(CONFIG_DIR)

        _run_worker_pool(
            db_path=db_with_company.db_path,
            report_ids=report_ids,
            registry=registry,
            num_workers=1,
        )

        assert sorted(extracted) == sorted(report_ids)

    def test_already_running_reports_skipped(self, db_with_company, monkeypatch):
        """Reports already marked 'running' by another worker must not be re-extracted."""
        import interpreters.interpret_pipeline as _ep

        report_ids = self._make_reports(db_with_company, 3)
        # Simulate one report already claimed by another worker
        db_with_company.mark_report_extraction_running(report_ids[1])

        extracted = []

        def fake_extract(report, db, registry, **kw):
            extracted.append(report['id'])
            db.mark_report_extracted(report['id'])
            from miner_types import ExtractionSummary
            s = ExtractionSummary()
            s.reports_processed = 1
            return s

        monkeypatch.setattr(_ep, 'extract_report', fake_extract)

        from cli import _run_worker_pool
        from interpreters.pattern_registry import PatternRegistry
        from config import CONFIG_DIR
        registry = PatternRegistry.load(CONFIG_DIR)

        _run_worker_pool(
            db_path=db_with_company.db_path,
            report_ids=report_ids,
            registry=registry,
            num_workers=2,
        )

        assert report_ids[1] not in extracted, (
            "Already-running report must not be re-extracted"
        )
        assert report_ids[0] in extracted
        assert report_ids[2] in extracted
