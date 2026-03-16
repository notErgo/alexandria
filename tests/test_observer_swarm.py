from pathlib import Path

from bs4 import BeautifulSoup

from scrapers.observer_swarm import ScoutConfig, ScoutWorker, run_observer


def test_scout_exhaustion_gate_marks_blocked_when_no_sources(tmp_path):
    worker = ScoutWorker(
        run_id="run1",
        scout_id="scout-1",
        db=None,
        session=None,
        output_dir=Path(tmp_path),
        config=ScoutConfig(max_attempts_per_source=2, max_consecutive_no_yield=2, execute_scrape=False),
        companies_by_ticker={
            "MARA": {
                "ticker": "MARA",
                "name": "MARA Holdings, Inc.",
                "ir_url": "https://example.com",
            }
        },
    )

    # Force no discovery yield.
    worker._discover_source = lambda *_args, **_kwargs: None  # noqa: SLF001
    contract = worker.run_ticker("MARA")
    assert contract["status"] == "blocked"
    assert contract["sources"] == []
    assert any(b.startswith("exhausted:ir:") for b in contract["blockers"])
    assert "coverage_gate:no_ir_source" in contract["blockers"]
    assert contract["attempts_by_family"]["ir"] == 2


def test_observer_writes_artifacts_from_scout_outputs(tmp_path, monkeypatch):
    def fake_run_scout_batch(**kwargs):
        out_dir = kwargs["output_dir"]
        out_dir.mkdir(parents=True, exist_ok=True)
        contracts = []
        for t in kwargs["tickers"]:
            contracts.append({
                "ticker": t,
                "run_id": kwargs["run_id"],
                "status": "ready_for_scrape",
                "sources": [{
                    "family": "ir",
                    "entry_url": f"https://{t.lower()}.example.com",
                    "discovery_method": "index",
                    "url_pattern": "",
                    "pagination": {"type": "none", "template": "", "max_page": 0},
                    "date_extraction": {"strategy": "title_regex", "pattern": "month year"},
                    "filters": {"include": [], "exclude": []},
                    "validation": {"http_ok": True, "parse_ok": True, "sample_count": 1},
                    "confidence": 0.8,
                    "evidence_urls": [f"https://{t.lower()}.example.com/pr"],
                }],
                "blockers": [],
            })
        return {
            "summary": {
                "run_id": kwargs["run_id"],
                "scout_id": kwargs["scout_id"],
                "tickers": kwargs["tickers"],
                "status_counts": {"ready_for_scrape": len(kwargs["tickers"])},
                "contracts_written": len(kwargs["tickers"]),
            },
            "contracts": contracts,
        }

    monkeypatch.setattr("scrapers.observer_swarm.run_scout_batch", fake_run_scout_batch)
    summary = run_observer(
        run_id="obs1",
        tickers=["MARA", "RIOT", "CLSK"],
        scout_count=2,
        output_dir=Path(tmp_path),
        config=ScoutConfig(execute_scrape=False),
        companies_by_ticker={},
    )
    assert summary["status_counts"]["ready_for_scrape"] == 3
    assert (Path(tmp_path) / "merged_source_contracts.json").exists()
    assert (Path(tmp_path) / "observer_ops_summary.md").exists()


def test_discover_year_filter_extracts_year_selector_and_urls(tmp_path):
    class _Resp:
        def __init__(self, text):
            self.status_code = 200
            self.text = text

    class _Session:
        def get(self, *_args, **_kwargs):
            return _Resp('<html><body><a href="/x">January 2025 Production Update</a></body></html>')

    html = """
    <html><body>
      <form>
        <input type="hidden" name="form_id" value="widget_form_base"/>
        <input type="hidden" name="aac_widget_id" value="w1"/>
        <select name="aac_year[value]">
          <option value="">- Any -</option>
          <option value="2026">2026</option>
          <option value="2025">2025</option>
          <option value="2024">2024</option>
        </select>
      </form>
    </body></html>
    """
    worker = ScoutWorker(
        run_id="run1",
        scout_id="scout-1",
        db=None,
        session=_Session(),
        output_dir=Path(tmp_path),
        config=ScoutConfig(max_attempts_per_source=1, max_consecutive_no_yield=1, execute_scrape=False),
        companies_by_ticker={},
    )
    soup = BeautifulSoup(html, "lxml")
    y = worker._discover_year_filter("https://investor.bitfarms.com/news-events/press-releases", soup, html)  # noqa: SLF001
    assert y is not None
    assert y["select_name"] == "aac_year[value]"
    assert y["years"][:2] == ["2026", "2025"]
    assert len(y["year_urls"]) >= 1
    assert "%7Byear%7D" in y["url_template"]


def test_discover_year_filter_heuristic_from_script_like_markup(tmp_path):
    class _Resp:
        def __init__(self, text):
            self.status_code = 200
            self.text = text

    class _Session:
        def get(self, *_args, **_kwargs):
            return _Resp("<html><body><a href='/x'>no production links</a></body></html>")

    html = """
    <html><body>
      <script>
      var key='aac2c52233ec9ed03e44a98dd9028c83ac2c52a24dacec95b3c1757c0d59015b_year[value]';
      var wid='aac2c52233ec9ed03e44a98dd9028c83ac2c52a24dacec95b3c1757c0d59015b_widget_id';
      var years=['2026','2025','2024','2023'];
      </script>
    </body></html>
    """
    worker = ScoutWorker(
        run_id="run1",
        scout_id="scout-1",
        db=None,
        session=_Session(),
        output_dir=Path(tmp_path),
        config=ScoutConfig(max_attempts_per_source=1, max_consecutive_no_yield=1, execute_scrape=False),
        companies_by_ticker={},
    )
    soup = BeautifulSoup(html, "lxml")
    y = worker._discover_year_filter("https://investor.bitfarms.com/news-events/press-releases", soup, html)  # noqa: SLF001
    assert y is not None
    assert y["select_name"].endswith("_year[value]")
    assert "2026" in y["years"]


