"""
Persist LLM prompts and metric_schema.prompt_instructions to the DB.

Run once after deploying llm_prompt_builder.py changes:
  cd OffChain/miners
  source venv/bin/activate
  python3 scripts/upsert_prompts.py
"""
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from infra.db import MinerDB
from interpreters.llm_prompt_builder import _DEFAULT_PROMPTS
from config import DATA_DIR
from pathlib import Path

# Keys to upsert into llm_prompts table (DB override tier 1)
UPSERT_KEYS = ['production_btc', 'holdings_btc', 'sales_btc', 'unrestricted_holdings', 'restricted_holdings_btc']

def main():
    db_path = str(Path(DATA_DIR) / 'minerdata.db')
    db = MinerDB(db_path)

    # Seed metric_schema.prompt_instructions (tier 2) with slim content for all keys
    print('Seeding metric_schema.prompt_instructions...')
    try:
        with db._get_connection() as conn:
            for key, instr in _DEFAULT_PROMPTS.items():
                conn.execute(
                    "UPDATE metric_schema SET prompt_instructions = ? WHERE key = ? AND (prompt_instructions IS NULL OR prompt_instructions = '')",
                    (instr, key),
                )
        print(f'  Seeded {len(_DEFAULT_PROMPTS)} metric keys (skipped existing non-empty rows)')
    except Exception as e:
        print(f'  ERROR seeding metric_schema: {e}')

    # Upsert into llm_prompts table (tier 1 override) for active core metrics
    print('Upserting llm_prompts overrides...')
    for key in UPSERT_KEYS:
        prompt = _DEFAULT_PROMPTS.get(key)
        if not prompt:
            print(f'  SKIP {key}: not found in _DEFAULT_PROMPTS')
            continue
        db.upsert_llm_prompt(key, prompt)
        print(f'  OK   {key}: upserted ({len(prompt)} chars)')

    print('Done.')

if __name__ == '__main__':
    main()
