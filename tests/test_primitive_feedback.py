from pathlib import Path

from scrapers.primitive_feedback import collect_primitive_gaps, propose_candidates, run_feedback_loop, validate_candidate


def test_collect_gaps_and_propose_year_filter_candidate():
    contracts = [{
        "ticker": "BITF",
        "primitive_gaps": [{
            "family": "ir",
            "kind": "year_filter_widget",
            "entry_url": "https://investor.bitfarms.com/news-events/press-releases",
            "select_name": "aac_year[value]",
            "widget_param": "aac_widget_id",
            "year_hints": ["2026", "2025", "2024"],
        }],
    }]
    gaps = collect_primitive_gaps(contracts)
    assert len(gaps) == 1
    candidates = propose_candidates(gaps)
    assert len(candidates) == 1
    c = candidates[0]
    assert c["strategy"] == "year_filter_query"
    assert c["params"]["select_name"] == "aac_year[value]"
    assert "2026" in c["params"]["years"]


def test_validate_candidate_rejects_missing_schema():
    result = validate_candidate({"strategy": "year_filter_query", "params": {}, "source": {}})
    assert not result["passed"]


def test_run_feedback_loop_writes_artifact(tmp_path):
    contracts = [{
        "ticker": "BTDR",
        "primitive_gaps": [{
            "family": "ir",
            "kind": "year_filter_widget",
            "entry_url": "https://ir.bitdeer.com/news-events/news-releases",
            "select_name": "aac_year[value]",
            "widget_param": "aac_widget_id",
            "year_hints": ["2026", "2025"],
        }],
    }]
    out = run_feedback_loop(
        run_id="feedback_test",
        output_dir=Path(tmp_path),
        contracts=contracts,
        apply=False,
    )
    assert out["gap_count"] == 1
    assert out["candidate_count"] == 1
    assert Path(out["artifact"]).exists()
