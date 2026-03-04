"""Shared test fixtures for Bitcoin Miner Data Platform unit tests."""
import pytest
from pathlib import Path


@pytest.fixture
def db(tmp_path):
    """Fresh MinerDB instance using temp directory."""
    from infra.db import MinerDB
    return MinerDB(str(tmp_path / 'test.db'))


@pytest.fixture
def db_with_company(db):
    """MinerDB with one MARA company row pre-inserted."""
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
