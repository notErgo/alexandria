VENV := venv
PYTHON := $(VENV)/bin/python
PIP := $(VENV)/bin/pip
PYTEST := $(VENV)/bin/pytest

.PHONY: venv install test test-ui test-arch test-fast review-maps

venv:
	python3 -m venv $(VENV)

install:
	$(PIP) install -r requirements.txt

test:
	$(PYTEST) -q

test-ui:
	$(PYTEST) -q tests/test_ui_spec.py tests/test_architecture_contracts.py

test-arch:
	python3 scripts/check_dag.py
	$(PYTEST) -q tests/test_architecture_contracts.py

test-fast:
	$(PYTEST) -q tests/test_ui_spec.py tests/test_architecture_contracts.py tests/test_operations_routes.py tests/test_pipeline_routes.py

review-maps:
	python3 scripts/ui_dag_trace.py --write docs/review/ui_dag_trace_map.json
	python3 scripts/dag_ui_reverse_trace.py --write docs/review/dag_ui_reverse_trace.json
	python3 scripts/ui_spec_migration_audit.py --write docs/review/ui_spec_migration_audit.json
