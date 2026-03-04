#!/usr/bin/env bash
set -euo pipefail

cd /Users/workstation/Documents/Hermeneutic/OffChain/miners
python3 -m venv .venv-fresh
source .venv-fresh/bin/activate
pip install --upgrade pip
pip install -r requirements.txt
pytest -q tests/test_ui_spec.py
pytest -q tests
echo "Fresh-session verification complete."
