"""
Phase 2b: Keyword pipeline coherence tests.
Verifies that keyword_service provides a single read-point for all keyword consumers.
These tests FAIL before keyword_service.py is created and PASS after.
"""
import pytest
from unittest.mock import MagicMock, patch


def _make_db_with_keywords(phrases=None):
    """Return a mock db whose get_all_metric_keywords returns the given phrases."""
    db = MagicMock()
    if phrases is None:
        phrases = ['bitcoin production', 'BTC mined', 'hash rate']
    kw_rows = [
        {'id': i, 'metric_key': 'production_btc', 'phrase': p, 'active': 1, 'exclude_terms': '', 'hit_count': 0}
        for i, p in enumerate(phrases)
    ]
    db.get_all_metric_keywords.return_value = kw_rows
    db.get_config.return_value = None
    return db


def _make_empty_db():
    """Return a mock db that has no keywords configured."""
    db = MagicMock()
    db.get_all_metric_keywords.return_value = []
    db.get_config.return_value = None
    return db


class TestKeywordServiceGetAllActiveRows:
    def test_returns_phrases_from_db(self):
        """get_all_active_rows returns rows from db.get_all_metric_keywords."""
        from infra.keyword_service import get_all_active_rows
        db = _make_db_with_keywords(['bitcoin production', 'BTC mined'])
        rows = get_all_active_rows(db)
        phrases = [r['phrase'] for r in rows]
        assert 'bitcoin production' in phrases
        assert 'BTC mined' in phrases

    def test_delegates_to_db(self):
        """get_all_active_rows calls db.get_all_metric_keywords(active_only=True)."""
        from infra.keyword_service import get_all_active_rows
        db = _make_db_with_keywords()
        get_all_active_rows(db)
        db.get_all_metric_keywords.assert_called_once_with(active_only=True)


class TestKeywordServiceBuildEdgarSearchQuery:
    def test_returns_or_joined_quoted_string(self):
        """build_edgar_search_query returns correctly quoted OR-joined string."""
        from infra.keyword_service import build_edgar_search_query
        db = _make_db_with_keywords(['bitcoin production', 'BTC mined'])
        query = build_edgar_search_query(db)
        assert '"bitcoin production"' in query
        assert '"BTC mined"' in query
        assert ' OR ' in query

    def test_falls_back_to_hardcoded_when_empty(self):
        """When db has no keywords, build_edgar_search_query uses _8K_SEARCH_TERMS fallback."""
        from infra.keyword_service import build_edgar_search_query, _8K_SEARCH_TERMS
        db = _make_empty_db()
        query = build_edgar_search_query(db)
        assert query, "Query must not be empty string when falling back"
        # Fallback must include at least one term from the hardcoded list
        assert any(term.strip('"') in query for term in _8K_SEARCH_TERMS)

    def test_falls_back_when_db_is_none(self):
        """build_edgar_search_query works with db=None (uses hardcoded fallback)."""
        from infra.keyword_service import build_edgar_search_query
        query = build_edgar_search_query(None)
        assert query, "Query must not be empty string"


class TestKeywordServiceMiningDetectionPhrases:
    def test_returns_metric_schema_phrases(self):
        """get_mining_detection_phrases returns LIKE-formatted phrases from metric_schema."""
        from infra.keyword_service import get_mining_detection_phrases
        db = _make_db_with_keywords(['bitcoin production', 'hash rate'])
        phrases = get_mining_detection_phrases(db)
        assert any('bitcoin' in p.lower() for p in phrases)

    def test_does_not_read_config_overrides(self):
        """Gate phrases come only from metric_schema.keywords."""
        from infra.keyword_service import get_mining_detection_phrases
        db = _make_db_with_keywords(['bitcoin production'])
        db.get_config.return_value = '%custom_keyword%'
        phrases = get_mining_detection_phrases(db)
        assert '%custom_keyword%' not in phrases
        assert any('bitcoin' in p.lower() for p in phrases)

    def test_returns_empty_when_no_active_keywords(self):
        """No active metric keywords means the gate has no phrases."""
        from infra.keyword_service import get_mining_detection_phrases
        db = _make_empty_db()
        phrases = get_mining_detection_phrases(db)
        assert phrases == []
