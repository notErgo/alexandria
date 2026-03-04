"""Tests for multi-stage extraction pipeline."""
import json
import os
import pytest
from extractors.extractor import extract_all
from miner_types import ExtractionResult

# Tests that depend on production regex patterns loaded from config/patterns/ are
# skipped when config patterns are empty (LLM-only mode).  Re-enable by removing
# the skip decorator when patterns are re-added to config.
_PATTERNS_EMPTY = "Config patterns emptied for LLM-only mode — re-enable when regex patterns are re-added"

# Load real production_btc patterns for tests
_CONFIG_DIR = os.path.join(os.path.dirname(__file__), '..', 'config', 'patterns')

def _load_patterns(metric: str) -> list:
    path = os.path.join(_CONFIG_DIR, f"{metric}.json")
    with open(path) as f:
        return json.load(f)['patterns']


@pytest.mark.skip(reason=_PATTERNS_EMPTY)
class TestExtractAll:
    def test_extract_clear_match_high_confidence(self):
        patterns = _load_patterns('production_btc')
        results = extract_all("Marathon mined 345 BTC in October", patterns, "production_btc")
        assert len(results) >= 1
        assert results[0].value == 345.0
        assert results[0].confidence >= 0.85

    def test_extract_no_match_returns_empty_list(self):
        patterns = _load_patterns('production_btc')
        results = extract_all("Company announced quarterly earnings of $1.2M", patterns, "production_btc")
        assert results == []

    def test_extract_ph_converted_in_result(self):
        patterns = _load_patterns('hashrate_eh')
        results = extract_all("achieved hashrate of 3400 PH/s this month", patterns, "hashrate_eh")
        assert len(results) >= 1
        assert abs(results[0].value - 3.4) < 0.001
        assert results[0].unit == "EH/s"

    def test_extract_multiple_matches_sorted_by_confidence(self):
        patterns = _load_patterns('production_btc')
        # Two matches: one with high-priority pattern, one with low-priority
        text = "Company mined 700 BTC. Bitcoin production figures: 700"
        results = extract_all(text, patterns, "production_btc")
        if len(results) >= 2:
            assert results[0].confidence >= results[1].confidence

    def test_extract_context_snippet_max_1000_chars(self):
        patterns = _load_patterns('production_btc')
        # Build long text with match in the middle
        prefix = "A" * 1500
        suffix = "B" * 1500
        text = prefix + " mined 500 BTC " + suffix
        results = extract_all(text, patterns, "production_btc")
        if results:
            assert len(results[0].source_snippet) <= 1000

    def test_extract_conflict_same_number_keeps_highest_confidence(self):
        patterns = _load_patterns('production_btc')
        # Text that matches multiple patterns for the same value
        text = "Company mined 700 BTC and bitcoin production showed 700 BTC"
        results = extract_all(text, patterns, "production_btc")
        # After conflict resolution, there should be at most one result per unique value
        values = [round(r.value) for r in results]
        assert len(values) == len(set(values)), "Duplicate values survived conflict resolution"

    def test_result_has_pattern_id(self):
        patterns = _load_patterns('production_btc')
        results = extract_all("Company mined 600 BTC last month", patterns, "production_btc")
        assert results
        assert results[0].pattern_id.startswith("prod_btc_")


@pytest.mark.skip(reason=_PATTERNS_EMPTY)
class TestPatternJSONFixes:
    def test_percent_contaminated_production_not_extracted(self):
        """prod_btc_3 must not fire on 'bitcoin production increased 12%'."""
        patterns = _load_patterns('production_btc')
        results = extract_all(
            "Monthly bitcoin production increased 12%; treasury grew.",
            patterns,
            "production_btc",
        )
        assert all(r.value != 12.0 for r in results)

    def test_colon_pattern_extracts_production(self):
        """'mined in March: 806' colon format should yield 806 BTC."""
        patterns = _load_patterns('production_btc')
        results = extract_all(
            "Production metrics: mined in March: 806",
            patterns,
            "production_btc",
        )
        assert any(abs(r.value - 806.0) < 0.1 for r in results)


@pytest.mark.skip(reason=_PATTERNS_EMPTY)
class TestPatternJSONFixesPDFFootnote:
    def test_pdf_footnote_marker_between_keyword_and_value(self):
        """MARA PDFs inject footnote superscripts: 'BTC Produced 2 750 865 (13)%'
        The 2 is a footnote marker, 750 is the actual value. prod_btc_7 must
        extract 750, not 2."""
        patterns = _load_patterns('production_btc')
        results = extract_all(
            "BTC Produced 2 750 865 (13)%",
            patterns,
            'production_btc',
        )
        assert any(abs(r.value - 750.0) < 0.1 for r in results), (
            f"Should extract 750 (not 2) from footnote-injected PDF text. Got {[r.value for r in results]}"
        )
        assert all(r.value != 2.0 for r in results), (
            "Must not extract the footnote marker (2) as the BTC value"
        )


@pytest.mark.skip(reason=_PATTERNS_EMPTY)
class TestFinditerContextScoring:
    def test_finditer_picks_company_not_global_hashrate(self):
        """When global hashrate appears before company hashrate, company value wins."""
        import pytest
        text = (
            "Global hashrate increased to an average of 949 EH/s. "
            "Company operational hashrate reached 50.0 EH/s."
        )
        patterns = _load_patterns('hashrate_eh')
        results = extract_all(text, patterns, "hashrate_eh")
        assert results, "Should find at least one hashrate"
        assert results[0].value == pytest.approx(50.0, abs=1.0)

    def test_percent_immediately_after_number_excluded(self):
        """Number followed immediately by % must be excluded; valid BTC kept."""
        patterns = _load_patterns('production_btc')
        text = (
            "Monthly bitcoin production increased 12%; "
            "The Company mined 806 BTC in March."
        )
        results = extract_all(text, patterns, "production_btc")
        values = [r.value for r in results]
        assert 12.0 not in values
        assert 806.0 in values

    def test_finditer_context_score_beats_first_match(self):
        """
        Discriminating test: first regex match has a disqualifying pre-context;
        second match (>150 chars later) has clean context and should win.

        With re.search (old code): hash_eh_0 returns 85.0 at full pattern_weight
        0.95 — no context penalty applied — so 85.0 wins over 50.0 (0.80 from
        hash_eh_3).

        With finditer + _score_match_context (new code): 85.0 gets
        context_score=0.5 (global hashrate in pre) → effective weight 0.475;
        50.0 gets context_score=1.0 → effective weight 0.95 → 50.0 wins.

        The 200-char padding ensures 50.0's 150-char pre-window does NOT reach
        the 'global hashrate' phrase, so it receives an unpenalised score.
        """
        import pytest
        padding = "X" * 200
        text = (
            "The global hashrate stands at 85.0 EH/s. "
            + padding
            + "Our operational hashrate: 50.0 EH/s."
        )
        patterns = _load_patterns("hashrate_eh")
        results = extract_all(text, patterns, "hashrate_eh")
        assert results, "Should find at least one hashrate"
        assert results[0].value == pytest.approx(50.0, abs=1.0), (
            f"Context scoring must prefer company hashrate (50.0) over "
            f"global hashrate (85.0). Got {results[0].value}"
        )


@pytest.mark.skip(reason=_PATTERNS_EMPTY)
class TestPatternGapFormats:
    """Tests for production_btc patterns covering 3 MARA PDF text formats
    that caused 21 months of zero-extraction gaps.

    Format A (2021 early): 'Produced X new minted/self-mined bitcoins'
    Format B (2023):        'Produced/Producing a Record X BTC'
    Format C (2024-2025):   'BTC production ... to/of X BTC'
    """

    # ── Format A ──────────────────────────────────────────────────────────
    def test_format_a_new_minted_bitcoins(self):
        """2021 MARA PDFs: 'Produced 162.1 new minted bitcoins during April 2021'"""
        patterns = _load_patterns('production_btc')
        results = extract_all(
            "Produced 162.1 new minted bitcoins during April 2021",
            patterns,
            'production_btc',
        )
        assert any(abs(r.value - 162.1) < 0.1 for r in results), (
            f"Format A (new minted) must extract 162.1. Got {[r.value for r in results]}"
        )

    def test_format_a_self_mined_bitcoins(self):
        """2021 MARA PDFs: 'Produced 417.7 self-mined bitcoins during October 2021'"""
        patterns = _load_patterns('production_btc')
        results = extract_all(
            "Produced 417.7 self-mined bitcoins during October 2021",
            patterns,
            'production_btc',
        )
        assert any(abs(r.value - 417.7) < 0.1 for r in results), (
            f"Format A (self-mined) must extract 417.7. Got {[r.value for r in results]}"
        )

    def test_format_a_picks_first_match_when_no_context_signal(self):
        """_apply_pattern returns the single best match per pattern; on equal
        context scores, the first match in text wins. When a report contains
        a quarterly total before a monthly figure in Format A, the quarterly
        value is returned. This is a known limitation: context scoring does not
        treat 'Q3' as a quarterly signal vs 'September' as monthly.

        The correct fix for ambiguous multi-value reports is to order the document
        so the monthly figure appears before the quarterly total, or to add Q-signal
        penalty to context scoring in a future iteration."""
        patterns = _load_patterns('production_btc')
        text = (
            "Produced 1,252.4 new minted bitcoins during Q3 2021. "
            "Produced 340.6 new minted bitcoins during September 2021."
        )
        results = extract_all(text, patterns, 'production_btc')
        assert results, "Should extract at least one value"
        # prod_btc_8 returns the first match (Q3 total) when context scores tie
        assert any(r.extraction_method == 'prod_btc_8' for r in results), (
            "Format A pattern (prod_btc_8) must fire on this text"
        )

    # ── Format B ──────────────────────────────────────────────────────────
    def test_format_b_produced_a_record_btc(self):
        """2023 MARA PDFs: 'Produced a Record 687 BTC in January 2023'"""
        patterns = _load_patterns('production_btc')
        results = extract_all(
            "Produced a Record 687 BTC in January 2023",
            patterns,
            'production_btc',
        )
        assert any(abs(r.value - 687.0) < 0.1 for r in results), (
            f"Format B must extract 687. Got {[r.value for r in results]}"
        )

    def test_format_b_producing_present_participle(self):
        """PR title format: 'Producing a Record 825 BTC in March 2023'"""
        patterns = _load_patterns('production_btc')
        results = extract_all(
            "Producing a Record 825 BTC in March 2023",
            patterns,
            'production_btc',
        )
        assert any(abs(r.value - 825.0) < 0.1 for r in results), (
            f"Format B (Producing) must extract 825. Got {[r.value for r in results]}"
        )

    # ── Format C ──────────────────────────────────────────────────────────
    def test_format_c_btc_production_grew_to(self):
        """2024-2025 MARA PDFs: 'BTC production grew 5% to 705 BTC'"""
        patterns = _load_patterns('production_btc')
        results = extract_all(
            "BTC production grew 5% to 705 BTC.",
            patterns,
            'production_btc',
        )
        assert any(abs(r.value - 705.0) < 0.1 for r in results), (
            f"Format C must extract 705. Got {[r.value for r in results]}"
        )

    def test_format_c_btc_production_of(self):
        """2024 MARA PDFs: 'BTC Production of 692 BTC, 17% Increase M/M'"""
        patterns = _load_patterns('production_btc')
        results = extract_all(
            "BTC Production of 692 BTC, 17% Increase M/M",
            patterns,
            'production_btc',
        )
        assert any(abs(r.value - 692.0) < 0.1 for r in results), (
            f"Format C must extract 692. Got {[r.value for r in results]}"
        )

    def test_format_c_btc_production_declined_to(self):
        """2025 MARA PDFs: 'BTC production declined 2% to 890 BTC'"""
        patterns = _load_patterns('production_btc')
        results = extract_all(
            "BTC production declined 2% to 890 BTC",
            patterns,
            'production_btc',
        )
        assert any(abs(r.value - 890.0) < 0.1 for r in results), (
            f"Format C must extract 890. Got {[r.value for r in results]}"
        )

    def test_format_c_month_over_month_to(self):
        """2024 MARA PDFs: 'Our BTC production grew 26% month-over-month to 907 BTC'"""
        patterns = _load_patterns('production_btc')
        results = extract_all(
            "Our BTC production grew 26% month-over-month to 907 BTC",
            patterns,
            'production_btc',
        )
        assert any(abs(r.value - 907.0) < 0.1 for r in results), (
            f"Format C must extract 907. Got {[r.value for r in results]}"
        )


@pytest.mark.skip(reason=_PATTERNS_EMPTY)
class TestHodlBtcProseExtraction:
    """Tests for MARA 2021 prose format: 'total bitcoin holdings to approximately 5,518'.

    hodl_btc_4 had [^\\d]{0,10} — the phrase 'to approximately ' is 18 chars,
    so the pattern never fired. Fix: widen to {0,50}.
    """

    MARA_2021_MAY = (
        "Produced 226.6 new minted bitcoins during May 2021, increasing total "
        "bitcoin holdings to approximately 5,518"
    )

    def test_hodl_btc_4_matches_to_approximately(self):
        patterns = _load_patterns('hodl_btc')
        results = extract_all(self.MARA_2021_MAY, patterns, 'hodl_btc')
        values = [r.value for r in results if r.confidence >= 0.75]
        assert 5518.0 in values, (
            f"hodl_btc_4 must match 'total bitcoin holdings to approximately 5,518'. "
            f"Got confident values: {values}"
        )

    def test_production_value_not_extracted_as_hodl(self):
        patterns = _load_patterns('hodl_btc')
        results = extract_all(self.MARA_2021_MAY, patterns, 'hodl_btc')
        values = [r.value for r in results]
        assert 226.6 not in values, (
            "Production value (226.6) must not be extracted as hodl_btc"
        )

    def test_hodl_btc_approximate_thousands(self):
        """Variant with comma-formatted number: 'holdings to approximately 15,232'"""
        patterns = _load_patterns('hodl_btc')
        results = extract_all(
            "increasing total bitcoin holdings to approximately 15,232",
            patterns, 'hodl_btc',
        )
        assert any(abs(r.value - 15232.0) < 0.1 for r in results), (
            f"hodl_btc_4 must match 15,232. Got {[r.value for r in results]}"
        )


@pytest.mark.skip(reason=_PATTERNS_EMPTY)
class TestSoldBtcPatterns:
    """Verification tests for sold_btc patterns.

    Phase 4 investigation: DB shows 15 non-zero MARA sold_btc months (2023-01
    through 2024-05). Source snippet analysis confirmed these are REAL sales —
    MARA explicitly 'opted to sell' BTC to cover operating expenses during this
    period before returning to 100% HODL. The extractions are correct.
    """

    def test_opted_to_sell_btc_extracted_correctly(self):
        """Real MARA 2023 prose: 'Marathon opted to sell 1,500 BTC during January 2023'"""
        patterns = _load_patterns('sold_btc')
        results = extract_all(
            "Marathon opted to sell 1,500 BTC during January 2023.",
            patterns, 'sold_btc',
        )
        assert any(abs(r.value - 1500.0) < 0.1 for r in results), (
            f"Should extract 1500. Got {[r.value for r in results]}"
        )

    def test_sell_bitcoin_future_tense_extracted(self):
        """'The Company opted to sell 650 bitcoin this month'"""
        patterns = _load_patterns('sold_btc')
        results = extract_all(
            "The Company opted to sell 650 bitcoin this month to offset expenses.",
            patterns, 'sold_btc',
        )
        assert any(abs(r.value - 650.0) < 0.1 for r in results), (
            f"Should extract 650. Got {[r.value for r in results]}"
        )

    def test_share_sale_does_not_extract_as_sold_btc(self):
        """'Marathon sold 3.5 million shares' must NOT fire sold_btc patterns."""
        patterns = _load_patterns('sold_btc')
        results = extract_all(
            "Marathon sold 3.5 million shares at $15 per share in a secondary offering.",
            patterns, 'sold_btc',
        )
        # No value should be confidently extracted — shares ≠ BTC
        confident = [r for r in results if r.confidence >= 0.75]
        assert len(confident) == 0, (
            f"Share sale must not produce confident sold_btc extraction. Got {[(r.value, r.confidence) for r in confident]}"
        )


@pytest.mark.skip(reason=_PATTERNS_EMPTY)
class TestCLSKTableFormat:
    """Tests for CLSK 'as of Month DD: value' table format.

    CLSK press releases use a structured table with entries like:
      'Total bitcoin holdings as of January 31: 10,556'
      'Month-end operating hashrate: 40.1 EH/s'

    The old hodl_btc_4 pattern ([^\\d]{0,10} window) failed because
    'as of January ' is 15 non-digit characters before hitting '31'.
    hash_eh_0 picked the rounded headline '40 EH/s' over the precise
    table entry '40.1 EH/s'.
    """

    def test_hodl_btc_colon_format_extracts_holdings(self):
        """'Total bitcoin holdings as of January 31: 10,556' must yield 10556."""
        patterns = _load_patterns('hodl_btc')
        results = extract_all(
            "Total bitcoin holdings as of January 31: 10,556",
            patterns,
            'hodl_btc',
        )
        assert any(abs(r.value - 10556.0) < 0.1 for r in results), (
            f"hodl_btc_6 must extract 10556. Got {[r.value for r in results]}"
        )

    def test_hodl_btc_colon_format_does_not_capture_date(self):
        """Must not capture '31' (the day) as the holdings value."""
        patterns = _load_patterns('hodl_btc')
        results = extract_all(
            "Total bitcoin holdings as of January 31: 10,556",
            patterns,
            'hodl_btc',
        )
        assert all(r.value != 31.0 for r in results), (
            "Must not extract the day-of-month (31) as holdings"
        )

    def test_hodl_btc_colon_format_february(self):
        """Variant with February date: 'bitcoin holdings as of February 28: 9,500'"""
        patterns = _load_patterns('hodl_btc')
        results = extract_all(
            "Total bitcoin holdings as of February 28: 9,500",
            patterns,
            'hodl_btc',
        )
        assert any(abs(r.value - 9500.0) < 0.1 for r in results), (
            f"hodl_btc_6 must handle February variant. Got {[r.value for r in results]}"
        )

    def test_hash_eh_month_end_operating_hashrate(self):
        """'Month-end operating hashrate: 40.1 EH/s' must yield 40.1."""
        patterns = _load_patterns('hashrate_eh')
        results = extract_all(
            "Month-end operating hashrate: 40.1 EH/s",
            patterns,
            'hashrate_eh',
        )
        assert any(abs(r.value - 40.1) < 0.01 for r in results), (
            f"hash_eh_7 must extract 40.1. Got {[r.value for r in results]}"
        )

    def test_hash_eh_month_end_wins_over_rounded_headline(self):
        """When headline has '40 EH/s' and table has '40.1 EH/s', table value wins."""
        import pytest
        patterns = _load_patterns('hashrate_eh')
        text = (
            "CleanSpark surpassed 40 EH/s in operating hashrate during January. "
            "Month-end operating hashrate: 40.1 EH/s"
        )
        results = extract_all(text, patterns, 'hashrate_eh')
        assert results, "Should find at least one hashrate"
        best = max(results, key=lambda r: r.confidence)
        assert best.value == pytest.approx(40.1, abs=0.01), (
            f"Month-end table entry (40.1) must beat rounded headline (40.0). "
            f"Got {best.value}"
        )


class TestTemporalScoping:
    """Patterns with valid_from/valid_to are filtered by report_date."""

    _PATTERN = {
        "id": "prod_btc_0",
        "regex": r"(?i)mined\s+([\d,]+(?:\.\d+)?)\s*(?:bitcoin|btc)",
        "confidence_weight": 0.95,
        "priority": 0,
    }

    def _scoped(self, valid_from=None, valid_to=None):
        p = dict(self._PATTERN)
        if valid_from:
            p['valid_from'] = valid_from
        if valid_to:
            p['valid_to'] = valid_to
        return p

    def test_pattern_without_scope_always_applies(self):
        """Pattern with no valid_from/valid_to fires regardless of report_date."""
        results = extract_all(
            "MARA mined 700 BTC", [self._PATTERN], "production_btc",
            report_date="2020-06-01",
        )
        assert len(results) == 1

    def test_valid_from_before_report_date_applies(self):
        """valid_from='2020-01' with report_date='2021-03-01' → pattern applies."""
        results = extract_all(
            "MARA mined 700 BTC", [self._scoped(valid_from="2020-01")],
            "production_btc", report_date="2021-03-01",
        )
        assert len(results) == 1

    def test_valid_from_after_report_date_skipped(self):
        """valid_from='2022-01' with report_date='2021-03-01' → pattern skipped."""
        results = extract_all(
            "MARA mined 700 BTC", [self._scoped(valid_from="2022-01")],
            "production_btc", report_date="2021-03-01",
        )
        assert len(results) == 0

    def test_valid_to_after_report_date_applies(self):
        """valid_to='2022-12' with report_date='2021-03-01' → pattern applies."""
        results = extract_all(
            "MARA mined 700 BTC", [self._scoped(valid_to="2022-12")],
            "production_btc", report_date="2021-03-01",
        )
        assert len(results) == 1

    def test_valid_to_before_report_date_skipped(self):
        """valid_to='2021-12' with report_date='2022-06-01' → pattern skipped."""
        results = extract_all(
            "MARA mined 700 BTC", [self._scoped(valid_to="2021-12")],
            "production_btc", report_date="2022-06-01",
        )
        assert len(results) == 0

    def test_out_of_window_pattern_blocked_in_window_pattern_fires(self):
        """Two patterns: one scoped to 2020–2021, one global. Report date 2023-06.
        Only the global pattern should fire."""
        old_pattern = self._scoped(valid_from="2020-01", valid_to="2021-12")
        new_pattern = {
            "id": "prod_btc_1",
            "regex": r"(?i)produced\s+([\d,]+(?:\.\d+)?)\s*(?:bitcoin|btc)",
            "confidence_weight": 0.90,
            "priority": 1,
        }
        text = "MARA mined 700 BTC and produced 700 BTC"
        results = extract_all(text, [old_pattern, new_pattern], "production_btc",
                              report_date="2023-06-01")
        ids = [r.pattern_id for r in results]
        assert "prod_btc_0" not in ids, "Scoped-out pattern must not fire"
        assert "prod_btc_1" in ids, "In-scope global pattern must fire"

    def test_no_report_date_uses_all_patterns(self):
        """When report_date=None, scoped patterns still apply (no filtering)."""
        results = extract_all(
            "MARA mined 700 BTC",
            [self._scoped(valid_from="2022-01", valid_to="2022-12")],
            "production_btc",
            report_date=None,
        )
        assert len(results) == 1
