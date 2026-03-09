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
]

# Default LIKE-pattern phrases for BTC mining detection (used by get_earliest_bitcoin_report_period).
_DEFAULT_MINING_DETECTION_PHRASES: list = [
    '%bitcoin%', '%btc%', '%hash rate%', '%hashrate%',
    '%exahash%', '%petahash%', '%mining operations%',
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
    """Return LIKE-pattern phrases for BTC mining detection in report text.

    Baseline: _DEFAULT_MINING_DETECTION_PHRASES (broad patterns always included).
    ADDITIVE: metric_schema.keywords converted to LIKE patterns.
    ADDITIVE: config_settings.bitcoin_mining_keywords (comma-separated).
    The defaults are always present so broad detection never regresses even when
    metric_schema is seeded with specific anchor phrases.
    """
    # Always start with broad defaults — never replace them.
    phrases = list(_DEFAULT_MINING_DETECTION_PHRASES)

    # Supplement with metric_schema keyword phrases as more specific patterns.
    if db is not None:
        try:
            rows = db.get_all_metric_keywords(active_only=True)
            for r in rows:
                p = r.get('phrase', '').strip()
                if p:
                    pattern = f'%{p}%'
                    if pattern not in phrases:
                        phrases.append(pattern)
        except Exception as exc:
            log.warning("event=mining_phrase_read_error error=%s", exc)

    # Additive supplement from config_settings override.
    if db is not None:
        try:
            raw = db.get_config('bitcoin_mining_keywords')
            if raw:
                for k in raw.split(','):
                    k = k.strip()
                    if k and k not in phrases:
                        phrases.append(k)
        except Exception as exc:
            log.warning("event=mining_phrase_config_error error=%s", exc)

    return phrases
