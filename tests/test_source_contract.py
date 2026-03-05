from scrapers.source_contract import merge_contracts, normalize_contract, validate_contract


def test_validate_contract_rejects_missing_fields():
    errs = validate_contract({"ticker": "MARA", "sources": []})
    assert any("run_id is required" in e for e in errs)


def test_merge_contracts_prefers_higher_confidence_per_family():
    merged = merge_contracts([
        {
            "ticker": "MARA",
            "run_id": "r1",
            "status": "partially_covered",
            "sources": [{
                "family": "ir",
                "entry_url": "https://a",
                "discovery_method": "index",
                "confidence": 0.7,
                "pagination": {"type": "none", "template": "", "max_page": 0},
                "date_extraction": {"strategy": "title_regex", "pattern": "month year"},
                "filters": {"include": [], "exclude": []},
                "validation": {"http_ok": True, "parse_ok": True, "sample_count": 1},
                "evidence_urls": ["https://a/1"],
            }],
            "blockers": [],
        },
        {
            "ticker": "MARA",
            "run_id": "r1",
            "status": "ready_for_scrape",
            "sources": [{
                "family": "ir",
                "entry_url": "https://b",
                "discovery_method": "index",
                "confidence": 0.9,
                "pagination": {"type": "none", "template": "", "max_page": 0},
                "date_extraction": {"strategy": "title_regex", "pattern": "month year"},
                "filters": {"include": [], "exclude": []},
                "validation": {"http_ok": True, "parse_ok": True, "sample_count": 2},
                "evidence_urls": ["https://b/1"],
            }],
            "blockers": [],
        },
    ])
    assert len(merged) == 1
    c = normalize_contract(merged[0])
    assert c["status"] == "ready_for_scrape"
    assert len(c["sources"]) == 1
    assert c["sources"][0]["entry_url"] == "https://b"


def test_validate_contract_accepts_year_filter_discovery_method():
    errs = validate_contract({
        "ticker": "BITF",
        "run_id": "run1",
        "status": "ready_for_scrape",
        "sources": [{
            "family": "ir",
            "entry_url": "https://investor.bitfarms.com/news-events/press-releases",
            "discovery_method": "year_filter",
            "url_pattern": "https://investor.bitfarms.com/news-events/press-releases?...year={year}",
            "pagination": {"type": "query", "template": "?page={n}", "max_page": 20},
            "date_extraction": {"strategy": "title_regex", "pattern": "month year"},
            "filters": {"include": ["production"], "exclude": ["earnings"]},
            "validation": {"http_ok": True, "parse_ok": True, "sample_count": 2},
            "confidence": 0.9,
            "evidence_urls": ["https://investor.bitfarms.com/news-events/press-releases?...year=2025"],
        }],
        "blockers": [],
    })
    assert errs == []
