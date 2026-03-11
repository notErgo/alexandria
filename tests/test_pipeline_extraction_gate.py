"""Tests for extraction-batch assembly: EDGAR date gate vs monthly-source passthrough.

Rules under test:
- EDGAR source types (edgar_8k, edgar_10k, edgar_10q, edgar_6k, edgar_20f, edgar_40f)
  are gated by btc_first_filing_date.  Reports before that date are excluded.
- Monthly miner source types (IR / archive / wire) are NOT date-gated.
- force_reextract=True path must apply the same per-source-type logic.
"""
import os
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from infra.db import MinerDB


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------

def _insert_report(db, ticker, report_date, source_type, raw_text='data'):
    """Insert a minimal pending report and return its id."""
    conn = db._get_connection()
    with conn:
        cur = conn.execute(
            "INSERT INTO reports (ticker, report_date, source_type, raw_text, extraction_status)"
            " VALUES (?, ?, ?, ?, 'pending')",
            (ticker, report_date, source_type, raw_text),
        )
    return cur.lastrowid


def _ids(reports):
    return {r['id'] for r in reports}


# ---------------------------------------------------------------------------
# fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def db(tmp_path):
    database = MinerDB(str(tmp_path / 'test.db'))
    database.insert_company({
        'ticker': 'MARA',
        'name': 'MARA Holdings',
        'tier': 1,
        'ir_url': 'https://ir.mara.com',
        'pr_base_url': 'https://ir.mara.com',
        'cik': '0001507605',
        'active': 1,
    })
    return database


# ---------------------------------------------------------------------------
# import the helper under test after sys.path is set
# ---------------------------------------------------------------------------

@pytest.fixture
def build_batch(db):
    """Return the _build_extraction_batch helper bound to the test db."""
    from routes.pipeline import _build_extraction_batch
    return lambda ticker, first_filing, force=False: _build_extraction_batch(
        db, ticker, first_filing, force_reextract=force
    )


# ---------------------------------------------------------------------------
# normal path (force_reextract=False)
# ---------------------------------------------------------------------------

class TestNormalPath:
    def test_edgar_before_gate_excluded(self, db, build_batch):
        """EDGAR report dated before btc_first_filing_date is not returned."""
        _insert_report(db, 'MARA', '2021-06-01', 'edgar_8k')
        batch = build_batch('MARA', first_filing='2023-05-19')
        assert _ids(batch) == set()

    def test_edgar_on_gate_date_included(self, db, build_batch):
        """EDGAR report on the gate date is included."""
        _insert_report(db, 'MARA', '2023-05-19', 'edgar_8k')
        batch = build_batch('MARA', first_filing='2023-05-19')
        assert len(batch) == 1

    def test_edgar_after_gate_included(self, db, build_batch):
        """EDGAR report after btc_first_filing_date is included."""
        _insert_report(db, 'MARA', '2023-06-01', 'edgar_8k')
        batch = build_batch('MARA', first_filing='2023-05-19')
        assert len(batch) == 1

    def test_ir_before_gate_included(self, db, build_batch):
        """IR press release before btc_first_filing_date is NOT gated out."""
        r_id = _insert_report(db, 'MARA', '2021-04-01', 'ir_press_release')
        batch = build_batch('MARA', first_filing='2023-05-19')
        assert r_id in _ids(batch)

    def test_archive_html_before_gate_included(self, db, build_batch):
        r_id = _insert_report(db, 'MARA', '2021-01-01', 'archive_html')
        batch = build_batch('MARA', first_filing='2023-05-19')
        assert r_id in _ids(batch)

    def test_archive_pdf_before_gate_included(self, db, build_batch):
        r_id = _insert_report(db, 'MARA', '2020-06-01', 'archive_pdf')
        batch = build_batch('MARA', first_filing='2023-05-19')
        assert r_id in _ids(batch)

    def test_prnewswire_before_gate_included(self, db, build_batch):
        r_id = _insert_report(db, 'MARA', '2021-04-01', 'prnewswire_press_release')
        batch = build_batch('MARA', first_filing='2023-05-19')
        assert r_id in _ids(batch)

    def test_globenewswire_before_gate_included(self, db, build_batch):
        r_id = _insert_report(db, 'MARA', '2021-04-01', 'globenewswire_press_release')
        batch = build_batch('MARA', first_filing='2023-05-19')
        assert r_id in _ids(batch)

    def test_wire_before_gate_included(self, db, build_batch):
        r_id = _insert_report(db, 'MARA', '2021-04-01', 'wire_press_release')
        batch = build_batch('MARA', first_filing='2023-05-19')
        assert r_id in _ids(batch)

    def test_no_gate_date_all_edgar_included(self, db, build_batch):
        """When btc_first_filing_date is None, all EDGAR reports are included."""
        r_id = _insert_report(db, 'MARA', '2019-01-01', 'edgar_10k')
        batch = build_batch('MARA', first_filing=None)
        assert r_id in _ids(batch)

    def test_mixed_sources_split_correctly(self, db, build_batch):
        """IR before gate + EDGAR before gate + EDGAR after gate."""
        ir_id = _insert_report(db, 'MARA', '2021-04-01', 'ir_press_release')
        edgar_old = _insert_report(db, 'MARA', '2021-06-01', 'edgar_8k')
        edgar_new = _insert_report(db, 'MARA', '2024-01-01', 'edgar_8k')
        batch = build_batch('MARA', first_filing='2023-05-19')
        batch_ids = _ids(batch)
        assert ir_id in batch_ids
        assert edgar_old not in batch_ids
        assert edgar_new in batch_ids

    def test_mixed_sources_are_globally_sorted_oldest_first(self, db, build_batch):
        """Chronology is enforced across merged IR and EDGAR queues."""
        ir_old = _insert_report(db, 'MARA', '2021-04-01', 'ir_press_release')
        edgar_mid = _insert_report(db, 'MARA', '2024-01-01', 'edgar_8k')
        ir_new = _insert_report(db, 'MARA', '2025-02-01', 'ir_press_release')

        batch = build_batch('MARA', first_filing='2023-05-19')

        assert [row['id'] for row in batch] == [ir_old, edgar_mid, ir_new]

    def test_all_edgar_types_gated(self, db, build_batch):
        """All six EDGAR source types respect the date gate."""
        edgar_types = [
            'edgar_8k', 'edgar_10k', 'edgar_10q',
            'edgar_6k', 'edgar_20f', 'edgar_40f',
        ]
        for st in edgar_types:
            _insert_report(db, 'MARA', '2021-01-01', st)
        batch = build_batch('MARA', first_filing='2023-05-19')
        assert _ids(batch) == set(), f"Expected empty but got: {batch}"


# ---------------------------------------------------------------------------
# force_reextract=True path
# ---------------------------------------------------------------------------

class TestForceReextractPath:
    def test_ir_before_gate_included_on_force(self, db, build_batch):
        r_id = _insert_report(db, 'MARA', '2021-04-01', 'ir_press_release')
        batch = build_batch('MARA', first_filing='2023-05-19', force=True)
        assert r_id in _ids(batch)

    def test_wire_before_gate_included_on_force(self, db, build_batch):
        r_id = _insert_report(db, 'MARA', '2021-04-01', 'wire_press_release')
        batch = build_batch('MARA', first_filing='2023-05-19', force=True)
        assert r_id in _ids(batch)

    def test_edgar_before_gate_excluded_on_force(self, db, build_batch):
        _insert_report(db, 'MARA', '2021-06-01', 'edgar_8k')
        batch = build_batch('MARA', first_filing='2023-05-19', force=True)
        assert _ids(batch) == set()

    def test_force_no_gate_includes_all(self, db, build_batch):
        ir_id = _insert_report(db, 'MARA', '2021-04-01', 'ir_press_release')
        edgar_id = _insert_report(db, 'MARA', '2021-06-01', 'edgar_8k')
        batch = build_batch('MARA', first_filing=None, force=True)
        assert ir_id in _ids(batch)
        assert edgar_id in _ids(batch)
