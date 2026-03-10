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

# Tight production-reporting phrases used as the gate before LLM extraction.
# These only appear when a company is REPORTING operational mining figures —
# not in generic corporate announcements, investor letters, or blockchain-era
# filings that mention "bitcoin" in a non-production context (e.g. RIOT 2018).
# Single words like 'bitcoin', 'btc', 'treasury' are deliberately excluded.
_PRODUCTION_GATE_PHRASES: list = [
    'bitcoin mined', 'btc mined', 'bitcoin produced', 'btc produced',
    'self-mined', 'hash rate', 'hashrate', 'exahash', 'petahash',
    'btc production', 'bitcoin production', 'digital asset production',
    'mining operations update', 'production and operations',
    'in the month of',  # common MARA/RIOT monthly PR opener
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

    Returns only production-specific phrases — NOT broad terms like 'bitcoin'
    or 'btc' that appear in generic corporate announcements and would cause
    every blockchain-era filing to pass the gate.

    Sources (all additive, all production-specific):
      1. _PRODUCTION_GATE_PHRASES — hardcoded tight fallback (always present).
      2. metric_schema.keywords — only phrases with 2+ words (single-word phrases
         like 'bitcoin', 'treasury', 'production' are too broad to gate on).
      3. config_settings.bitcoin_mining_keywords — operator-supplied overrides.

    Both the monthly (8-K/IR) and quarterly (10-Q/10-K) gate paths call this
    function — there is no separate gate phrase set for each path.
    """
    phrases = list(_PRODUCTION_GATE_PHRASES)
    seen = {p.lower() for p in phrases}

    # Add multi-word metric_schema keyword phrases (skip single-word entries).
    if db is not None:
        try:
            rows = db.get_all_metric_keywords(active_only=True)
            for r in rows:
                p = r.get('phrase', '').strip()
                if p and len(p.split()) >= 2 and p.lower() not in seen:
                    phrases.append(p)
                    seen.add(p.lower())
        except Exception as exc:
            log.warning("event=mining_phrase_read_error error=%s", exc)

    # Operator-supplied overrides from config_settings.
    if db is not None:
        try:
            raw = db.get_config('bitcoin_mining_keywords')
            if raw:
                for k in raw.split(','):
                    k = k.strip()
                    if k and k.lower() not in seen:
                        phrases.append(k)
                        seen.add(k.lower())
        except Exception as exc:
            log.warning("event=mining_phrase_config_error error=%s", exc)

    return phrases
