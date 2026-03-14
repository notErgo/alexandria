"""Tests for source-type-first extraction ordering and source priority rules."""

import pytest

from infra.db import MinerDB, _SOURCE_PRIORITY, _DEFAULT_SOURCE_PRIORITY
from helpers import make_report, make_data_point


@pytest.fixture
def db(tmp_path):
    d = MinerDB(str(tmp_path / 'test.db'))
    d.insert_company({
        'ticker': 'MARA',
        'name': 'MARA Holdings',
        'tier': 1,
        'ir_url': 'https://ir.mara.com',
        'pr_base_url': 'https://ir.mara.com',
        'cik': '0001437491',
        'active': 1,
    })
    return d


# ── Source priority mapping ──────────────────────────────────────────────────

def test_monthly_pr_types_have_priority_1():
    for st in ('ir_press_release', 'archive_pdf', 'archive_html',
               'prnewswire_press_release', 'globenewswire_press_release',
               'wire_press_release'):
        assert _SOURCE_PRIORITY[st] == 1, f"{st} should be priority 1"


def test_edgar_8k_has_priority_2():
    assert _SOURCE_PRIORITY['edgar_8k'] == 2
    assert _SOURCE_PRIORITY['edgar_8ka'] == 2


def test_edgar_quarterly_has_priority_3():
    for st in ('edgar_10q', 'edgar_6k', 'edgar_6ka'):
        assert _SOURCE_PRIORITY[st] == 3, f"{st} should be priority 3"


def test_edgar_annual_has_priority_4():
    for st in ('edgar_10k', 'edgar_20f', 'edgar_20fa', 'edgar_40f', 'edgar_40fa'):
        assert _SOURCE_PRIORITY[st] == 4, f"{st} should be priority 4"


def test_default_source_priority_is_4():
    assert _DEFAULT_SOURCE_PRIORITY == 4


# ── Upsert conflict resolution ───────────────────────────────────────────────

def test_source_priority_monthly_beats_edgar(db):
    """IR value already exists; EDGAR upsert must not overwrite it."""
    ir_report_id = db.insert_report(make_report(source_type='ir_press_release', ticker='MARA'))
    db.insert_data_point(make_data_point(
        report_id=ir_report_id,
        ticker='MARA',
        period='2024-09-01',
        metric='production_btc',
        value=700.0,
    ))

    edgar_report_id = db.insert_report(make_report(source_type='edgar_10q', ticker='MARA'))
    db.insert_data_point(make_data_point(
        report_id=edgar_report_id,
        ticker='MARA',
        period='2024-09-01',
        metric='production_btc',
        value=999.0,
    ))

    rows = db.query_data_points(ticker='MARA', metric='production_btc')
    assert len(rows) == 1
    assert rows[0]['value'] == 700.0, "IR value should survive EDGAR upsert attempt"
    assert rows[0]['source_priority'] == 1


def test_source_priority_edgar_fills_gap(db):
    """No existing row — EDGAR data must be written (gap fill)."""
    edgar_report_id = db.insert_report(make_report(source_type='edgar_10q', ticker='MARA'))
    db.insert_data_point(make_data_point(
        report_id=edgar_report_id,
        ticker='MARA',
        period='2024-08-01',
        metric='production_btc',
        value=650.0,
    ))

    rows = db.query_data_points(ticker='MARA', metric='production_btc')
    assert len(rows) == 1
    assert rows[0]['value'] == 650.0
    assert rows[0]['source_priority'] == 3


def test_source_priority_analyst_beats_all(db):
    """Analyst row must survive an IR upsert attempt."""
    ir_report_id = db.insert_report(make_report(source_type='ir_press_release', ticker='MARA'))
    db.insert_data_point(make_data_point(
        report_id=ir_report_id,
        ticker='MARA',
        period='2024-07-01',
        metric='production_btc',
        value=500.0,
        extraction_method='analyst',
    ))

    ir_report_id2 = db.insert_report(make_report(source_type='ir_press_release', ticker='MARA'))
    db.insert_data_point(make_data_point(
        report_id=ir_report_id2,
        ticker='MARA',
        period='2024-07-01',
        metric='production_btc',
        value=999.0,
        extraction_method='llm_batch',
    ))

    rows = db.query_data_points(ticker='MARA', metric='production_btc')
    assert len(rows) == 1
    assert rows[0]['value'] == 500.0, "Analyst value should survive IR upsert"
    assert rows[0]['source_priority'] == 0


# ── Chronology key ordering ──────────────────────────────────────────────────

def test_chronology_key_source_first():
    """All monthly sources must sort before 8-Ks, which sort before 10-Qs."""
    from routes.pipeline import _report_chronology_key

    reports = [
        {'source_type': 'edgar_10q',        'report_date': '2024-01-01', 'id': 1},
        {'source_type': 'ir_press_release',  'report_date': '2024-06-01', 'id': 2},
        {'source_type': 'edgar_8k',          'report_date': '2024-03-01', 'id': 3},
        {'source_type': 'ir_press_release',  'report_date': '2024-01-01', 'id': 4},
        {'source_type': 'edgar_10q',         'report_date': '2024-06-01', 'id': 5},
    ]
    sorted_reports = sorted(reports, key=_report_chronology_key)
    source_types = [r['source_type'] for r in sorted_reports]

    # All IR before 8-K before 10-Q
    last_ir_pos = max(i for i, r in enumerate(sorted_reports) if r['source_type'] == 'ir_press_release')
    first_8k_pos = min(i for i, r in enumerate(sorted_reports) if r['source_type'] == 'edgar_8k')
    first_10q_pos = min(i for i, r in enumerate(sorted_reports) if r['source_type'] == 'edgar_10q')

    assert last_ir_pos < first_8k_pos, "All IR reports must come before 8-K reports"
    assert first_8k_pos < first_10q_pos, "8-K reports must come before 10-Q reports"

    # Within same source type, sorted by date
    ir_dates = [r['report_date'] for r in sorted_reports if r['source_type'] == 'ir_press_release']
    assert ir_dates == sorted(ir_dates)

    tq_dates = [r['report_date'] for r in sorted_reports if r['source_type'] == 'edgar_10q']
    assert tq_dates == sorted(tq_dates)


def test_chronology_key_archive_is_rank0():
    """archive_html and archive_pdf must sort at rank 0 alongside IR press releases."""
    from routes.pipeline import _report_chronology_key

    reports = [
        {'source_type': 'edgar_8k',    'report_date': '2024-01-01', 'id': 1},
        {'source_type': 'archive_html', 'report_date': '2024-06-01', 'id': 2},
        {'source_type': 'archive_pdf',  'report_date': '2024-03-01', 'id': 3},
    ]
    sorted_reports = sorted(reports, key=_report_chronology_key)

    # archive docs must appear before 8-K
    last_archive_pos = max(
        i for i, r in enumerate(sorted_reports)
        if r['source_type'] in ('archive_html', 'archive_pdf')
    )
    first_8k_pos = min(
        i for i, r in enumerate(sorted_reports) if r['source_type'] == 'edgar_8k'
    )
    assert last_archive_pos < first_8k_pos, "Archive docs must come before 8-K in extraction order"


def test_chronology_key_annual_after_quarterly():
    """10-K reports must sort after 10-Q reports."""
    from routes.pipeline import _report_chronology_key

    reports = [
        {'source_type': 'edgar_10k', 'report_date': '2024-03-01', 'id': 1},
        {'source_type': 'edgar_10q', 'report_date': '2024-12-01', 'id': 2},
    ]
    sorted_reports = sorted(reports, key=_report_chronology_key)
    assert sorted_reports[0]['source_type'] == 'edgar_10q'
    assert sorted_reports[1]['source_type'] == 'edgar_10k'
