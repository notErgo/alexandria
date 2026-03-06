"""Generate per-company LLM crawl prompts from source_contract files.

Usage:
    python3 scripts/gen_crawl_prompts.py --all
    python3 scripts/gen_crawl_prompts.py --ticker BTBT

Reads:
    scripts/prompts/llm_crawl_prompt.md   (master template, git-committed)
    .data/miners_progress/source_contract_{TICKER}.json
    config/companies.json

Writes:
    scripts/crawl_prompts/{TICKER}_crawl.md   (gitignored, derived)
"""
import argparse
import json
import sys
from pathlib import Path

# Canonical directory locations (overridable by tests)
_REPO_ROOT = Path(__file__).parent.parent
PROMPTS_DIR = _REPO_ROOT / 'scripts' / 'prompts'
OUTPUT_DIR = _REPO_ROOT / 'scripts' / 'crawl_prompts'
_CONTRACTS_DIR = _REPO_ROOT / '.data' / 'miners_progress'
_COMPANIES_JSON = _REPO_ROOT / 'config' / 'companies.json'
_DATA_RESULTS_DIR = _REPO_ROOT / '.data' / 'crawl_results'


def _load_template() -> str:
    template_path = PROMPTS_DIR / 'llm_crawl_prompt.md'
    if not template_path.exists():
        raise FileNotFoundError(f"Template not found: {template_path}")
    return template_path.read_text()


def _load_companies() -> dict:
    """Return companies.json as a dict keyed by ticker."""
    with open(_COMPANIES_JSON) as f:
        companies = json.load(f)
    return {c['ticker']: c for c in companies}


def _build_year_filter_instructions(company: dict) -> str:
    """Return Drupal two-step instructions if company has a year_filter_template, else ''."""
    template = company.get('year_filter_template', '')
    years = company.get('year_filter_years', [])
    if not template or not years:
        return ''
    lines = [
        "**Drupal year filter (two-step)**\n",
        "This site uses a Drupal form with a CSRF token that changes on every page load.\n",
        "Do NOT use the year_filter_template URL directly — the form_build_id will be stale.\n\n",
        "Instead:\n",
        f"1. Fetch the base IR listing page: `{company.get('ir_url', '')}`\n",
        "2. In the returned HTML, find the hidden input: `<input type='hidden' name='form_build_id' value='...'>`\n",
        "3. Extract the current `form_build_id` value.\n",
        f"4. For each year in {years}, construct the filter URL by replacing the stale\n",
        "   `form_build_id` in the template below with the live value you extracted:\n\n",
        f"   Template: `{template}`\n\n",
        "5. Fetch each year-filtered page and collect article links.\n\n",
    ]
    return ''.join(lines)


def _pick_entry_urls(sources: list) -> list:
    """Return entry_urls from sources with confidence > 0.6, or all sources if none qualify."""
    high = [s['entry_url'] for s in sources if s.get('confidence', 0) > 0.6]
    return high if high else [s['entry_url'] for s in sources]


def _pick_evidence_urls(sources: list) -> list:
    """Return all evidence_urls from all source families."""
    urls = []
    for s in sources:
        urls.extend(s.get('evidence_urls', []))
    return list(dict.fromkeys(urls))  # deduplicate, preserve order


def generate_prompt(ticker: str, contract: dict, company: dict, contracts_dir: Path) -> None:
    """Fill template vars for ticker and write to OUTPUT_DIR/{ticker}_crawl.md."""
    template = _load_template()

    sources = contract.get('sources', [])
    entry_urls = _pick_entry_urls(sources)
    evidence_urls = _pick_evidence_urls(sources)
    year_filter = _build_year_filter_instructions(company)
    pr_start_year = company.get('pr_start_year') or 2021
    output_path = str(_DATA_RESULTS_DIR / ticker / 'results.json')

    filled = (
        template
        .replace('{TICKER}', ticker)
        .replace('{company_name}', company.get('name', ticker))
        .replace('{pr_start_year}', str(pr_start_year))
        .replace('{entry_urls_json}', json.dumps(entry_urls, indent=2))
        .replace('{evidence_urls_json}', json.dumps(evidence_urls, indent=2))
        .replace('{year_filter_instructions}', year_filter)
        .replace('{output_path}', output_path)
    )

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    out_path = OUTPUT_DIR / f'{ticker}_crawl.md'
    out_path.write_text(filled)
    print(f"Wrote {out_path}")


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description='Generate LLM crawl prompts per company.')
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument('--all', action='store_true', help='Generate for all companies with contracts')
    group.add_argument('--ticker', metavar='TICKER', help='Generate for one ticker')
    args = parser.parse_args(argv)

    companies = _load_companies()

    if args.ticker:
        tickers = [args.ticker.upper()]
    else:
        tickers = sorted(
            p.stem.replace('source_contract_', '')
            for p in _CONTRACTS_DIR.glob('source_contract_*.json')
        )

    errors = 0
    for ticker in tickers:
        contract_path = _CONTRACTS_DIR / f'source_contract_{ticker}.json'
        if not contract_path.exists():
            print(f"[SKIP] No source contract for {ticker}", file=sys.stderr)
            continue
        if ticker not in companies:
            print(f"[SKIP] {ticker} not in companies.json", file=sys.stderr)
            continue
        try:
            contract = json.loads(contract_path.read_text())
            generate_prompt(ticker, contract, companies[ticker], _CONTRACTS_DIR)
        except Exception as exc:
            print(f"[ERROR] {ticker}: {exc}", file=sys.stderr)
            errors += 1

    return 1 if errors else 0


if __name__ == '__main__':
    sys.exit(main())
