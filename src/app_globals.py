"""
Module-level singletons for MinerDB and ScrapeWorker.
All route modules import from here to avoid circular imports.
"""
import threading
import logging
from pathlib import Path

log = logging.getLogger('miners.app_globals')

_db = None
_lock = threading.Lock()


def get_db():
    global _db
    if _db is None:
        with _lock:
            if _db is None:
                from infra.db import MinerDB
                from config import DATA_DIR
                db_path = str(Path(DATA_DIR) / 'minerdata.db')
                log.info("Initializing MinerDB at %s", db_path)
                _db = MinerDB(db_path)
    return _db


_scrape_worker = None
_scrape_worker_lock = threading.Lock()


def get_scrape_worker():
    """Return the singleton ScrapeWorker (creates it if not yet instantiated)."""
    global _scrape_worker
    if _scrape_worker is None:
        with _scrape_worker_lock:
            if _scrape_worker is None:
                from scrapers.scrape_worker import ScrapeWorker
                _scrape_worker = ScrapeWorker(get_db())
    return _scrape_worker
