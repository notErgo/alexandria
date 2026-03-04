"""
Tests for document_chunks DB methods — TDD.

Tests should FAIL before the methods are added to db.py.
"""
import pytest


@pytest.fixture
def db_with_report(db):
    """DB with MARA company and one report."""
    db.insert_company({
        'ticker': 'MARA',
        'name': 'MARA Holdings, Inc.',
        'tier': 1,
        'ir_url': 'https://www.marathondh.com/news',
        'pr_base_url': 'https://www.marathondh.com',
        'cik': '0001437491',
        'active': 1,
    })
    report_id = db.insert_report({
        'ticker': 'MARA',
        'report_date': '2024-01-01',
        'published_date': None,
        'source_type': 'archive_html',
        'source_url': None,
        'raw_text': 'MARA mined 700 BTC in January 2024.',
        'parsed_at': None,
    })
    db._report_id = report_id
    return db


def test_upsert_chunk_creates_row(db_with_report):
    """upsert_document_chunk creates a row and returns its id."""
    db = db_with_report
    chunk_id = db.upsert_document_chunk({
        'report_id': db._report_id,
        'chunk_index': 0,
        'section': 'full_text',
        'text': 'MARA mined 700 BTC in January 2024.',
        'char_start': 0,
        'char_end': 36,
        'token_count': 9,
    })
    assert isinstance(chunk_id, int)
    assert chunk_id > 0

    chunks = db.get_chunks_for_report(db._report_id)
    assert len(chunks) == 1
    assert chunks[0]['section'] == 'full_text'


def test_upsert_chunk_is_idempotent(db_with_report):
    """Same report_id + chunk_index yields exactly 1 row (UNIQUE constraint)."""
    db = db_with_report
    entry = {
        'report_id': db._report_id,
        'chunk_index': 0,
        'section': 'full_text',
        'text': 'MARA mined 700 BTC in January 2024.',
        'char_start': 0,
        'char_end': 36,
        'token_count': 9,
    }
    db.upsert_document_chunk(entry)
    db.upsert_document_chunk(entry)  # second call — no duplicate
    chunks = db.get_chunks_for_report(db._report_id)
    assert len(chunks) == 1


def test_get_unembedded_chunks_excludes_embedded(db_with_report):
    """get_unembedded_chunks only returns chunks where embedded_at IS NULL."""
    db = db_with_report
    # Insert two chunks
    db.upsert_document_chunk({
        'report_id': db._report_id,
        'chunk_index': 0,
        'section': 'full_text',
        'text': 'chunk 0',
        'char_start': 0,
        'char_end': 7,
        'token_count': 2,
    })
    db.upsert_document_chunk({
        'report_id': db._report_id,
        'chunk_index': 1,
        'section': 'full_text',
        'text': 'chunk 1',
        'char_start': 8,
        'char_end': 15,
        'token_count': 2,
    })
    # Mark chunk 0 as embedded
    with db._get_connection() as conn:
        conn.execute(
            "UPDATE document_chunks SET embedded_at=datetime('now') WHERE report_id=? AND chunk_index=0",
            (db._report_id,)
        )
    unembedded = db.get_unembedded_chunks(limit=100)
    assert len(unembedded) == 1
    assert unembedded[0]['chunk_index'] == 1


def test_set_report_parse_quality(db_with_report):
    """set_report_parse_quality updates the parse_quality column on the report."""
    db = db_with_report
    db.set_report_parse_quality(db._report_id, 'text_ok')
    report = db.get_report(db._report_id)
    assert report['parse_quality'] == 'text_ok'
