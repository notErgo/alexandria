"""Best-effort Ollama model warmup for extraction flows."""
from __future__ import annotations

import logging
import os
import threading
import time
from typing import Optional

import requests

from config import LLM_BASE_URL, LLM_MODEL_ID, LLM_TIMEOUT_SECONDS

log = logging.getLogger('miners.infra.ollama_warmup')

_last_warm_at_by_model: dict[str, float] = {}
_warm_lock = threading.Lock()


def _active_model_name(db=None) -> str:
    """Resolve active model from runtime config, falling back to static config."""
    try:
        if db is not None and hasattr(db, 'get_config'):
            model = (db.get_config('ollama_model') or '').strip()
            if model:
                return model
    except Exception:
        pass
    return LLM_MODEL_ID


def warm_ollama_for_extraction(
    db=None,
    *,
    reason: str = '',
    force: bool = False,
    min_ttl_seconds: Optional[int] = None,
) -> dict:
    """Warm the active Ollama model once per TTL window.

    Returns:
      {
        'attempted': bool,
        'warmed': bool,
        'skipped': bool,
        'model': str,
        'reason': str,
      }
    """
    model = _active_model_name(db)
    ttl = int(os.environ.get('MINERS_OLLAMA_WARM_TTL_SECONDS', '1800'))
    if min_ttl_seconds is not None:
        ttl = max(ttl, int(min_ttl_seconds))
    now = time.time()

    with _warm_lock:
        last = _last_warm_at_by_model.get(model)
        if (not force) and last and ((now - last) < max(1, ttl)):
            return {
                'attempted': False,
                'warmed': True,
                'skipped': True,
                'model': model,
                'reason': 'ttl_cache',
            }

    try:
        log.info(
            "event=ollama_warmup_start model=%s reason=%s ttl_seconds=%s",
            model, reason or 'n/a', ttl,
        )
        resp = requests.post(
            f"{LLM_BASE_URL}/api/generate",
            json={
                'model': model,
                'prompt': 'ping',
                'stream': False,
                'keep_alive': '2h',
                'options': {'temperature': 0.0},
            },
            timeout=LLM_TIMEOUT_SECONDS,
        )
        ok = resp.status_code == 200
        if ok:
            with _warm_lock:
                _last_warm_at_by_model[model] = time.time()
            log.info(
                "event=ollama_warmup_end model=%s warmed=1 reason=%s",
                model, reason or 'n/a',
            )
            return {
                'attempted': True,
                'warmed': True,
                'skipped': False,
                'model': model,
                'reason': 'ok',
            }
        log.warning(
            "event=ollama_warmup_end model=%s warmed=0 reason=http_%s trigger=%s",
            model, resp.status_code, reason or 'n/a',
        )
        return {
            'attempted': True,
            'warmed': False,
            'skipped': False,
            'model': model,
            'reason': f'http_{resp.status_code}',
        }
    except Exception as e:
        log.warning(
            "event=ollama_warmup_end model=%s warmed=0 reason=exception trigger=%s error=%s",
            model, reason or 'n/a', e,
        )
        return {
            'attempted': True,
            'warmed': False,
            'skipped': False,
            'model': model,
            'reason': 'exception',
        }
