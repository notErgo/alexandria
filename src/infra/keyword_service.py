"""
Keyword pipeline facade — single read-point for all metric keyword consumers.

Replaces 5 scattered calls to db.get_all_metric_keywords() across pipeline modules
with a single service boundary. Callers receive db via dependency injection.

Layer: L1 (infrastructure IO — reads from DB, no higher-layer imports).
"""
import logging

log = logging.getLogger('miners.infra.keyword_service')

# Hardcoded fallback — used when DB has no keywords configured.
# Keep in sync with edgar_connector._8K_SEARCH_TERMS.
_8K_SEARCH_TERMS: list = [
    '"bitcoin production"',
    '"BTC production"',
    '"bitcoin mined"',
    '"BTC mined"',
    '"mining operations update"',
    '"production and operations"',
    '"digital asset production"',
    '"hash rate"',
    '"bitcoin mining"',
    '"BTC mining"',
    '"exahash"',
    '"petahash"',
    '"mining capacity"',
    '"mining operations"',
    '"bitcoin holdings"',
]

def get_all_active_rows(db) -> list:
    """Single read-point for metric keywords.

    Replaces direct db.get_all_metric_keywords(active_only=True) calls in pipeline modules.
    Returns list of dicts with keys: id, metric_key, phrase, active, exclude_terms, hit_count.
    """
    try:
        return db.get_all_metric_keywords(active_only=True)
    except Exception as exc:
        log.warning("event=keyword_read_error error=%s", exc)
        return []


def build_edgar_search_query(db) -> str:
    """Build OR-joined EDGAR full-text search query string from active keyword phrases.

    Reads from metric_schema.keywords (DB SSOT).
    Falls back to _8K_SEARCH_TERMS when db is None or has no active keywords.
    """
    if db is not None:
        try:
            rows = db.get_all_metric_keywords(active_only=True)
            if rows:
                return ' OR '.join(f'"{r["phrase"]}"' for r in rows)
        except Exception as exc:
            log.warning("event=edgar_query_build_error error=%s", exc)
    return ' OR '.join(_8K_SEARCH_TERMS)


def get_mining_detection_phrases(db) -> list:
    """Return phrases used to gate documents before LLM extraction.

    Uses only active phrases from metric_schema.keywords (SSOT).
    Callers should treat an empty result as a configuration error and block
    extraction instead of silently falling back to hardcoded phrases.
    """
    if db is None:
        return []
    try:
        rows = db.get_all_metric_keywords(active_only=True)
    except Exception as exc:
        log.warning("event=mining_phrase_read_error error=%s", exc)
        return []
    phrases: list[str] = []
    seen: set[str] = set()
    for row in rows:
        phrase = str(row.get('phrase') or '').strip()
        if not phrase:
            continue
        lowered = phrase.lower()
        if lowered in seen:
            continue
        phrases.append(phrase)
        seen.add(lowered)
    return phrases
