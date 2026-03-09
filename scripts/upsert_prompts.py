"""
Persist LLM prompts for new metric keys to the llm_prompts DB table.

Run once after deploying llm_interpreter.py changes:
  cd OffChain/miners
  source venv/bin/activate
  python3 scripts/upsert_prompts.py
"""
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from infra.db import MinerDB
from interpreters.llm_interpreter import _DEFAULT_PROMPTS
from config import DATA_DIR
from pathlib import Path

NEW_KEYS = ['holdings_btc', 'sales_btc', 'unrestricted_holdings', 'restricted_holdings_btc']

def main():
    db_path = str(Path(DATA_DIR) / 'minerdata.db')
    db = MinerDB(db_path)

    for key in NEW_KEYS:
        prompt = _DEFAULT_PROMPTS.get(key)
        if not prompt:
            print(f'SKIP {key}: not found in _DEFAULT_PROMPTS')
            continue
        db.upsert_llm_prompt(key, prompt)
        print(f'OK   {key}: upserted ({len(prompt)} chars)')

    print('Done.')

if __name__ == '__main__':
    main()
