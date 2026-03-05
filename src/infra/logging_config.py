"""Logging setup for the Bitcoin Miner Data Platform."""
import logging
import os
import sys
from logging.handlers import RotatingFileHandler
from pathlib import Path


def _resolve_log_file_path() -> Path:
    """Return the target log file path from env/config defaults."""
    # Explicit override takes precedence.
    env_file = os.environ.get('MINERS_LOG_FILE', '').strip()
    if env_file:
        return Path(env_file).expanduser()

    # Directory override fallback.
    env_dir = os.environ.get('MINERS_LOG_DIR', '').strip()
    if env_dir:
        return Path(env_dir).expanduser() / 'miners.log'

    # Default under persistent data dir.
    from config import DATA_DIR
    return Path(DATA_DIR) / 'logs' / 'miners.log'


def setup_logging(level: int = logging.DEBUG) -> logging.Logger:
    """Configure the 'miners' logger hierarchy. Call once before create_app()."""
    logger = logging.getLogger('miners')
    if logger.handlers:
        return logger
    logger.setLevel(level)
    fmt = logging.Formatter(
        '%(asctime)s [%(levelname)s %(name)s] %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
    )

    # Always keep stdout logs for terminal visibility.
    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setFormatter(fmt)
    logger.addHandler(stream_handler)

    # Add rotating file logs for persistent diagnostics.
    log_file = _resolve_log_file_path()
    max_bytes = int(os.environ.get('MINERS_LOG_MAX_BYTES', '5242880'))  # 5MB
    backup_count = int(os.environ.get('MINERS_LOG_BACKUP_COUNT', '5'))
    try:
        log_file.parent.mkdir(parents=True, exist_ok=True)
        file_handler = RotatingFileHandler(
            filename=str(log_file),
            maxBytes=max_bytes,
            backupCount=backup_count,
            encoding='utf-8',
        )
        file_handler.setFormatter(fmt)
        logger.addHandler(file_handler)
        logger.info("File logging enabled at %s", log_file)
    except Exception as e:
        logger.warning("File logging disabled (path=%s): %s", log_file, e)

    return logger
