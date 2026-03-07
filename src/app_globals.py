"""
Module-level singletons for MinerDB and PatternRegistry.
All route modules import from here to avoid circular imports.
"""
import threading
import logging
from pathlib import Path

log = logging.getLogger('miners.app_globals')

_db = None
_registry = None
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


def get_registry():
    global _registry
    if _registry is None:
        with _lock:
            if _registry is None:
                from interpreters.pattern_registry import PatternRegistry
                from config import CONFIG_DIR
                log.info("Loading PatternRegistry from %s", CONFIG_DIR)
                _registry = PatternRegistry.load(CONFIG_DIR)
    return _registry


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


def reload_registry():
    """Force-reload PatternRegistry from disk. Called after pattern edits."""
    global _registry
    with _lock:
        from interpreters.pattern_registry import PatternRegistry
        from config import CONFIG_DIR
        _registry = PatternRegistry.load(CONFIG_DIR)
        log.info("PatternRegistry reloaded from %s", CONFIG_DIR)
    return _registry
