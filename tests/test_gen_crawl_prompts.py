"""Tests for scripts/gen_crawl_prompts.py — file I/O only, no web calls."""
import importlib.util
import json
import os
import sys
from pathlib import Path

import pytest


def _load_gen_module():
    scripts_dir = Path(__file__).parent.parent / 'scripts'
    spec = importlib.util.spec_from_file_location(
        'gen_crawl_prompts',
        scripts_dir / 'gen_crawl_prompts.py',
    )
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


@pytest.fixture
def gen(tmp_path):
    """Return the gen module with PROMPTS_DIR and OUTPUT_DIR patched to tmp_path."""
    mod = _load_gen_module()
    mod.PROMPTS_DIR = tmp_path / 'prompts'
    mod.OUTPUT_DIR = tmp_path / 'crawl_prompts'
    mod.PROMPTS_DIR.mkdir()
    mod.OUTPUT_DIR.mkdir()

    # Write a minimal template
    template_text = (
        "# Crawl prompt for {TICKER} — {company_name}\n"
        "Start year: {pr_start_year}\n"
        "Entry URLs: {entry_urls_json}\n"
        "Evidence URLs: {evidence_urls_json}\n"
        "{year_filter_instructions}"
        "Write output to: {output_path}\n"
    )
    (mod.PROMPTS_DIR / 'llm_crawl_prompt.md').write_text(template_text)
    return mod


def _make_contract(tmp_path, ticker, sources=None):
    contract = {
        'ticker': ticker,
        'sources': sources or [
            {
                'family': 'ir',
                'entry_url': f'https://ir.example.com/{ticker.lower()}',
                'confidence': 0.8,
                'evidence_urls': [
                    f'https://ir.example.com/{ticker.lower()}/pr-1',
                    f'https://ir.example.com/{ticker.lower()}/pr-2',
                ],
            }
        ],
    }
    contract_path = tmp_path / f'source_contract_{ticker}.json'
    contract_path.write_text(json.dumps(contract))
    return contract_path


def _make_company(ticker, name='Test Corp', pr_start_year=2021, year_filter_template=None):
    c = {
        'ticker': ticker,
        'name': name,
        'pr_start_year': pr_start_year,
        'ir_url': f'https://ir.example.com/{ticker.lower()}',
        'active': True,
    }
    if year_filter_template:
        c['year_filter_template'] = year_filter_template
    return c


def test_gen_prompt_includes_entry_urls(gen, tmp_path):
    contract_path = _make_contract(tmp_path, 'MARA')
    company = _make_company('MARA', 'MARA Holdings, Inc.', 2020)
    contract = json.loads(contract_path.read_text())

    gen.generate_prompt('MARA', contract, company, tmp_path)

    out_file = gen.OUTPUT_DIR / 'MARA_crawl.md'
    assert out_file.exists()
    content = out_file.read_text()
    assert 'ir.example.com/mara' in content


def test_gen_prompt_includes_evidence_urls(gen, tmp_path):
    contract_path = _make_contract(tmp_path, 'RIOT')
    company = _make_company('RIOT', 'Riot Platforms, Inc.', 2020)
    contract = json.loads(contract_path.read_text())

    gen.generate_prompt('RIOT', contract, company, tmp_path)

    out_file = gen.OUTPUT_DIR / 'RIOT_crawl.md'
    assert out_file.exists()
    content = out_file.read_text()
    assert 'pr-1' in content
    assert 'pr-2' in content


def test_gen_prompt_writes_to_output_file(gen, tmp_path):
    contract_path = _make_contract(tmp_path, 'CLSK')
    company = _make_company('CLSK', 'CleanSpark, Inc.', 2021)
    contract = json.loads(contract_path.read_text())

    gen.generate_prompt('CLSK', contract, company, tmp_path)

    out_file = gen.OUTPUT_DIR / 'CLSK_crawl.md'
    assert out_file.exists()
    content = out_file.read_text()
    assert 'CLSK' in content
    assert '2021' in content
    assert 'crawl_results/CLSK' in content
