from scrapers.primitive_registry import materialize_year_filter_source, match_primitive


def test_match_primitive_by_host_and_path():
    primitives = [{
        "id": "p1",
        "family": "ir",
        "active": True,
        "match": {"host": "investor.bitfarms.com", "path_prefix": "/news-events"},
        "strategy": "year_filter_query",
        "params": {"select_name": "aac_year[value]", "years": ["2026"], "base_query": {}},
    }]
    m = match_primitive(primitives, family="ir", entry_url="https://investor.bitfarms.com/news-events/press-releases")
    assert m is not None
    assert m["id"] == "p1"


def test_materialize_year_filter_source():
    p = {
        "id": "p1",
        "strategy": "year_filter_query",
        "confidence": 0.8,
        "params": {
            "select_name": "aac_year[value]",
            "years": ["2026", "2025"],
            "base_query": {"form_id": "widget_form_base", "op": "Filter"},
        },
    }
    src = materialize_year_filter_source(
        p,
        entry_url="https://investor.bitfarms.com/news-events/press-releases",
        include=["production"],
        exclude=["10-q"],
    )
    assert src is not None
    assert src["discovery_method"] == "year_filter"
    assert "{year}" in src["url_pattern"] or "%7Byear%7D" in src["url_pattern"]
    assert len(src["year_filter"]["year_urls"]) == 2
