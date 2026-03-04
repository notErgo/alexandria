"""Logging setup for the Bitcoin Miner Data Platform."""
import logging
import sys


def setup_logging(level: int = logging.DEBUG) -> logging.Logger:
    """Configure the 'miners' logger hierarchy. Call once before create_app()."""
    logger = logging.getLogger('miners')
    if logger.handlers:
        return logger
    logger.setLevel(level)
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(logging.Formatter('[%(levelname)s %(name)s] %(message)s'))
    logger.addHandler(handler)
    return logger
