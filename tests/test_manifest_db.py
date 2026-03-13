"""
Tests for asset_manifest DB methods — TDD.

Tests should FAIL before the methods are added to db.py.
"""
import pytest


@pytest.fixture
def db_with_mara(db):
    """DB with MARA company seeded."""
    db.insert_company({
        'ticker': 'MARA',
        'name': 'MARA Holdings, Inc.',
        'tier': 1,
        'ir_url': 'https://www.marathondh.com/news',
        'pr_base_url': 'https://www.marathondh.com',
        'cik': '0001437491',
        'active': 1,
    })
    return db


# ── upsert_asset_manifest ───────────────────────────────────────────────────

def test_upsert_creates_manifest_entry(db_with_mara):
    """upsert_asset_manifest creates a row and returns its id."""
    db = db_with_mara
    manifest_id = db.upsert_asset_manifest({
        'ticker': 'MARA',
        'period': '2024-01-01',
        'source_type': 'archive_html',
        'file_path': '/some/path/mara_jan_2024.html',
        'filename': 'mara_jan_2024.html',
        'ingest_state': 'pending',
    })
    assert isinstance(manifest_id, int)
    assert manifest_id > 0


def test_upsert_is_idempotent(db_with_mara):
    """Same file_path inserted twice yields exactly 1 row (UNIQUE constraint)."""
    db = db_with_mara
    entry = {
        'ticker': 'MARA',
        'period': '2024-01-01',
        'source_type': 'archive_html',
        'file_path': '/some/path/mara_jan_2024.html',
        'filename': 'mara_jan_2024.html',
        'ingest_state': 'pending',
    }
    db.upsert_asset_manifest(entry)
    db.upsert_asset_manifest(entry)  # second call — should not create duplicate
    rows = db.get_manifest_by_ticker('MARA')
    assert len(rows) == 1


# ── get_uningested_assets ───────────────────────────────────────────────────

def test_get_uningested_assets_returns_pending_only(db_with_mara):
    """get_uningested_assets returns only rows with ingest_state='pending'."""
    db = db_with_mara
    db.upsert_asset_manifest({
        'ticker': 'MARA',
        'period': '2024-01-01',
        'source_type': 'archive_html',
        'file_path': '/path/a.html',
        'filename': 'a.html',
        'ingest_state': 'pending',
    })
    db.upsert_asset_manifest({
        'ticker': 'MARA',
        'period': '2024-02-01',
        'source_type': 'archive_html',
        'file_path': '/path/b.html',
        'filename': 'b.html',
        'ingest_state': 'ingested',
    })
    pending = db.get_uningested_assets()
    assert len(pending) == 1
    assert pending[0]['filename'] == 'a.html'


# ── link_manifest_to_report ─────────────────────────────────────────────────

def test_link_manifest_to_report(db_with_mara):
    """link_manifest_to_report sets ingest_state='ingested' and report_id."""
    db = db_with_mara
    manifest_id = db.upsert_asset_manifest({
        'ticker': 'MARA',
        'period': '2024-01-01',
        'source_type': 'archive_html',
        'file_path': '/path/mara_2024_01.html',
        'filename': 'mara_2024_01.html',
        'ingest_state': 'pending',
    })
    report_id = db.insert_report({
        'ticker': 'MARA',
        'report_date': '2024-01-01',
        'published_date': None,
        'source_type': 'archive_html',
        'source_url': None,
        'raw_text': 'test text',
        'parsed_at': None,
    })
    db.link_manifest_to_report(manifest_id, report_id)
    rows = db.get_manifest_by_ticker('MARA')
    assert len(rows) == 1
    assert rows[0]['ingest_state'] == 'ingested'
    assert rows[0]['report_id'] == report_id


# ── get_coverage_grid ───────────────────────────────────────────────────────

def test_get_coverage_grid_returns_nested_dict(db_with_mara):
    """get_coverage_grid returns a dict with ticker keys."""
    db = db_with_mara
    grid = db.get_coverage_grid(months=3)
    assert isinstance(grid, dict)
    assert 'MARA' in grid
    assert 'summary' in grid


# ── get_operations_queue ────────────────────────────────────────────────────

def test_get_operations_queue_structure(db_with_mara):
    """get_operations_queue returns dict with pending_extraction and legacy_files keys."""
    db = db_with_mara
    queue = db.get_operations_queue()
    assert isinstance(queue, dict)
    assert 'pending_extraction' in queue
    assert 'legacy_files' in queue


# ── get_report_by_ticker_date ───────────────────────────────────────────────

def test_get_report_by_ticker_date_found(db_with_mara):
    """get_report_by_ticker_date returns report for matching ticker+period."""
    db = db_with_mara
    db.insert_report({
        'ticker': 'MARA',
        'report_date': '2024-03-01',
        'published_date': None,
        'source_type': 'archive_html',
        'source_url': None,
        'raw_text': 'MARA mined 600 BTC',
        'parsed_at': None,
    })
    row = db.get_report_by_ticker_date('MARA', '2024-03-01')
    assert row is not None
    assert row['ticker'] == 'MARA'


def test_get_report_by_ticker_date_not_found(db_with_mara):
    """get_report_by_ticker_date returns None when no match."""
    db = db_with_mara
    row = db.get_report_by_ticker_date('MARA', '2099-01-01')
    assert row is None


# ── get_manifest_by_file_path ───────────────────────────────────────────────

def test_get_manifest_by_file_path_found(db_with_mara):
    """get_manifest_by_file_path returns entry by file_path."""
    db = db_with_mara
    db.upsert_asset_manifest({
        'ticker': 'MARA',
        'period': '2024-05-01',
        'source_type': 'archive_html',
        'file_path': '/unique/path/file.html',
        'filename': 'file.html',
        'ingest_state': 'pending',
    })
    row = db.get_manifest_by_file_path('/unique/path/file.html')
    assert row is not None
    assert row['filename'] == 'file.html'


def test_get_manifest_by_file_path_not_found(db_with_mara):
    """get_manifest_by_file_path returns None for unknown path."""
    db = db_with_mara
    row = db.get_manifest_by_file_path('/nonexistent/path.html')
    assert row is None


# ── update_manifest_period ──────────────────────────────────────────────────

def test_update_manifest_period(db_with_mara):
    """update_manifest_period sets period and resets state to pending."""
    db = db_with_mara
    manifest_id = db.upsert_asset_manifest({
        'ticker': 'MARA',
        'period': None,
        'source_type': 'archive_html',
        'file_path': '/path/undated.html',
        'filename': 'undated.html',
        'ingest_state': 'legacy_undated',
    })
    db.update_manifest_period(manifest_id, '2024-06-01')
    rows = db.get_manifest_by_ticker('MARA')
    assert len(rows) == 1
    assert rows[0]['period'] == '2024-06-01'
    assert rows[0]['ingest_state'] == 'pending'


# ── Extra coverage grid tests (Phase III.2) ─────────────────────────────────

def test_coverage_grid_accepted_cell(db_with_mara):
    """A period with a data_point shows as 'accepted' in coverage grid."""
    from coverage_logic import generate_month_range
    db = db_with_mara
    # Use the most recent period in the grid range so it's always in-range
    periods = generate_month_range(3)
    target_period = periods[-1]  # most recent month

    report_id = db.insert_report({
        'ticker': 'MARA',
        'report_date': target_period,
        'published_date': None,
        'source_type': 'archive_html',
        'source_url': None,
        'raw_text': 'MARA mined 700 BTC',
        'parsed_at': None,
    })
    db.insert_data_point({
        'report_id': report_id,
        'ticker': 'MARA',
        'period': target_period,
        'metric': 'production_btc',
        'value': 700.0,
        'unit': 'BTC',
        'confidence': 0.95,
        'extraction_method': 'regex',
        'source_snippet': 'mined 700 BTC',
    })
    grid = db.get_coverage_grid(months=3)
    mara_cells = grid.get('MARA', {})
    target_cell = mara_cells.get(target_period, {})
    assert target_cell.get('state') in ('accepted', 'data', 'data_quarterly'), f"Expected accepted/data for {target_period}, got {target_cell}"


def test_coverage_grid_no_source_for_gap(db_with_mara):
    """A period with no manifest entry and no data_point shows as 'no_source'."""
    db = db_with_mara
    grid = db.get_coverage_grid(months=2)
    mara_cells = grid.get('MARA', {})
    # All cells should be no_source (no data inserted)
    for period, cell in mara_cells.items():
        assert cell.get('state') in ('no_source', 'no_document'), f"Expected no_source/no_document for {period}"


def test_coverage_grid_summary_key_present(db_with_mara):
    """get_coverage_grid result must include a 'summary' key."""
    db = db_with_mara
    grid = db.get_coverage_grid(months=2)
    assert 'summary' in grid
    summary = grid['summary']
    assert isinstance(summary, dict)
