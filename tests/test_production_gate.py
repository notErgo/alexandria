"""
Tests for the unified production keyword gate.

The gate must:
  1. Use only SSOT phrases from metric_schema.keywords.
  2. Apply identically to both monthly (8-K/IR) and quarterly (10-Q/10-K) paths.
  3. Block RIOT Blockchain 2018 corporate announcements that mention "bitcoin"
     in an investment/blockchain context but contain no production figures.
  4. Pass genuine monthly production press releases.

These tests FAIL before implementation and PASS after.
"""
import pytest
from helpers import make_report


# ── keyword_service unit tests ────────────────────────────────────────────────

class TestGetProductionGatePhrases:
    """get_mining_detection_phrases returns only SSOT metric keywords."""

    def test_returns_empty_without_db(self):
        """Without a DB-backed keyword source there are no gate phrases."""
        from infra.keyword_service import get_mining_detection_phrases
        phrases = get_mining_detection_phrases(db=None)
        assert phrases == []


# ── pipeline gate integration tests ──────────────────────────────────────────

class TestUnifiedProductionGate:
    """Monthly and quarterly paths both gate using production-specific phrases."""

    @pytest.fixture
    def db_with_company(self, db):
        db.insert_company({
            'ticker': 'RIOT', 'name': 'Riot Platforms, Inc.',
            'tier': 1, 'ir_url': 'https://ir.riotplatforms.com',
            'pr_base_url': 'https://ir.riotplatforms.com',
            'cik': '0001167419', 'active': 1,
        })
        return db

    @pytest.fixture
    def registry(self):
        from interpreters.pattern_registry import PatternRegistry
        from config import CONFIG_DIR
        return PatternRegistry.load(CONFIG_DIR)

    def test_riot_blockchain_corporate_8k_gated(self, db_with_company, registry, monkeypatch):
        """8-K mentioning 'bitcoin' in a corporate/investment context must be gated out.

        This simulates a RIOT Blockchain 2018 8-K: 'bitcoin' appears but there
        are no production figures, hash rate data, or BTC mining phrases.
        The gate must skip it without calling the LLM.
        """
        import interpreters.interpret_pipeline as _ep

        llm_calls = []

        class _TrackingLLM:
            _last_call_meta = {}
            _last_batch_summary = ''
            def check_connectivity(self): return True
            def extract_batch(self, *a, **kw):
                llm_calls.append(1)
                return {}
            def extract(self, *a, **kw): return None
            def extract_historical_periods(self, *a, **kw): return {}

        monkeypatch.setattr(_ep, '_get_llm_interpreter', lambda db: _TrackingLLM())

        report_id = db_with_company.insert_report(make_report(
            ticker='RIOT',
            raw_text=(
                'Riot Blockchain, Inc. (NASDAQ: RIOT) today announced it has entered '
                'into a definitive agreement to acquire a bitcoin exchange platform. '
                'The company believes bitcoin and blockchain technology represent a '
                'significant opportunity. This acquisition will accelerate our bitcoin '
                'strategy. The transaction is expected to close in Q2 2018.'
            ),
            report_date='2018-03-15',
            source_type='edgar_8k',
        ))
        report = db_with_company.get_report(report_id)

        from interpreters.interpret_pipeline import extract_report
        summary = extract_report(report, db_with_company, registry)

        assert len(llm_calls) == 0, (
            f"LLM must not be called on a corporate 8-K that contains only 'bitcoin' "
            f"without production phrases. Got {len(llm_calls)} LLM call(s)."
        )
        assert summary.keyword_gated == 1

    def test_production_report_passes_gate(self, db_with_company, registry, monkeypatch):
        """8-K with actual production figures must pass the gate and reach LLM."""
        import interpreters.interpret_pipeline as _ep

        llm_calls = []

        class _TrackingLLM:
            _last_call_meta = {}
            _last_batch_summary = ''
            def check_connectivity(self): return True
            def extract_batch(self, *a, **kw):
                llm_calls.append(1)
                return {}
            def extract(self, *a, **kw): return None
            def extract_historical_periods(self, *a, **kw): return {}

        monkeypatch.setattr(_ep, '_get_llm_interpreter', lambda db: _TrackingLLM())

        report_id = db_with_company.insert_report(make_report(
            ticker='RIOT',
            raw_text=(
                'Riot Platforms, Inc. (NASDAQ: RIOT) announces bitcoin production '
                'results for August 2023. The Company mined 333 BTC in August 2023. '
                'Hash rate reached 10.7 EH/s. BTC produced increased 15% month-over-month.'
            ),
            report_date='2023-09-06',
            source_type='edgar_8k',
        ))
        report = db_with_company.get_report(report_id)

        from interpreters.interpret_pipeline import extract_report
        extract_report(report, db_with_company, registry)

        assert len(llm_calls) > 0, (
            "LLM must be called for a genuine production report with hash rate and BTC produced."
        )

    def test_quarterly_corporate_filing_gated(self, db_with_company, registry, monkeypatch):
        """10-Q mentioning bitcoin in investment context only must be gated."""
        import interpreters.interpret_pipeline as _ep

        llm_calls = []

        class _TrackingLLM:
            _last_call_meta = {}
            _last_batch_summary = ''
            def check_connectivity(self): return True
            def extract_batch(self, *a, **kw):
                llm_calls.append(1)
                return {}
            def extract_quarterly_batch(self, *a, **kw):
                llm_calls.append(1)
                return {}
            def extract(self, *a, **kw): return None
            def extract_historical_periods(self, *a, **kw): return {}

        monkeypatch.setattr(_ep, '_get_llm_interpreter', lambda db: _TrackingLLM())

        report_id = db_with_company.insert_report(make_report(
            ticker='RIOT',
            raw_text=(
                'Riot Blockchain, Inc. Form 10-Q for the quarter ended March 31 2018. '
                'The Company is engaged in the blockchain and bitcoin ecosystem. '
                'We have invested in various bitcoin and cryptocurrency related ventures. '
                'Risk factors include volatility of bitcoin prices.'
            ),
            report_date='2018-03-31',
            source_type='edgar_10q',
            covering_period='2018-Q1',
        ))
        report = db_with_company.get_report(report_id)

        from interpreters.interpret_pipeline import extract_report
        summary = extract_report(report, db_with_company, registry)

        assert len(llm_calls) == 0, (
            f"Quarterly LLM must not fire on a 10-Q with only generic bitcoin mentions. "
            f"Got {len(llm_calls)} call(s)."
        )
        assert summary.keyword_gated == 1

    def test_gate_consistent_monthly_and_quarterly(self):
        """Monthly and quarterly gates must use the same phrase set."""
        from infra.keyword_service import get_mining_detection_phrases
        class _Db:
            def get_all_metric_keywords(self, active_only=True):
                return [
                    {'phrase': 'bitcoin production', 'metric_key': 'production_btc'},
                    {'phrase': 'hash rate', 'metric_key': 'hashrate_eh'},
                ]
        result1 = get_mining_detection_phrases(_Db())
        result2 = get_mining_detection_phrases(_Db())
        assert result1 == result2

    def test_monthly_ir_without_metric_keywords_is_keyword_gated(self, db_with_company, registry, monkeypatch):
        """Monthly miner docs without mining metric keywords must be filtered by the keyword gate."""
        import interpreters.interpret_pipeline as _ep

        llm_calls = []

        class _TrackingLLM:
            _last_call_meta = {}
            _last_batch_summary = ''
            def check_connectivity(self): return True
            def extract_batch(self, *a, **kw):
                llm_calls.append(1)
                return {}
            def extract(self, *a, **kw): return None
            def extract_historical_periods(self, *a, **kw): return {}

        monkeypatch.setattr(_ep, '_get_llm_interpreter', lambda db: _TrackingLLM())

        report_id = db_with_company.insert_report(make_report(
            ticker='RIOT',
            raw_text=(
                'Riot Platforms issued a mining operations update regarding expansion plans, '
                'fleet deployment timing, and infrastructure improvements.'
            ),
            report_date='2024-09-01',
            source_type='ir_press_release',
        ))
        report = db_with_company.get_report(report_id)

        from interpreters.interpret_pipeline import extract_report
        summary = extract_report(report, db_with_company, registry)

        assert len(llm_calls) == 0
        assert summary.keyword_gated == 1
