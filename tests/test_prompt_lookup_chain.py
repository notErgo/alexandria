"""Tests for the three-tier prompt lookup chain in LLMInterpreter."""
import pytest
import requests


def test_get_prompt_instructions_falls_back_to_metric_schema(db):
    """When no llm_prompts row, metric_schema.prompt_instructions is used."""
    from interpreters.llm_interpreter import LLMInterpreter
    session = requests.Session()
    interp = LLMInterpreter(session=session, db=db)
    # Seed metric_schema with a custom prompt_instructions
    rows = db.get_metric_schema('BTC-miners', active_only=False)
    row_id = next(r['id'] for r in rows if r['key'] == 'production_btc')
    db.update_metric_schema(row_id, prompt_instructions="Custom DB instructions for testing")
    # Ensure no llm_prompts override
    result = interp._get_prompt_instructions('production_btc')
    assert "Custom DB instructions for testing" in result


def test_llm_prompts_override_wins_over_metric_schema(db):
    """Active llm_prompts row beats metric_schema.prompt_instructions."""
    from interpreters.llm_interpreter import LLMInterpreter
    session = requests.Session()
    interp = LLMInterpreter(session=session, db=db)
    rows = db.get_metric_schema('BTC-miners', active_only=False)
    row_id = next(r['id'] for r in rows if r['key'] == 'production_btc')
    db.update_metric_schema(row_id, prompt_instructions="Schema instructions")
    # Add an active llm_prompts row
    db.upsert_llm_prompt('production_btc', 'Override prompt instructions here\n\nDocument:\n{text}')
    result = interp._get_prompt_instructions('production_btc')
    assert "Override prompt" in result


def test_generic_fallback_when_both_empty(db):
    """Falls back to _DEFAULT_FALLBACK_PROMPT when neither source has a value."""
    from interpreters.llm_interpreter import LLMInterpreter
    session = requests.Session()
    interp = LLMInterpreter(session=session, db=db)
    # Use a metric that has no entry in either source
    result = interp._get_prompt_instructions('nonexistent_metric_xyz')
    assert result  # non-empty


def test_quarterly_prompt_from_metric_schema(db):
    """Quarterly path uses metric_schema.quarterly_prompt when set."""
    from interpreters.llm_interpreter import LLMInterpreter
    session = requests.Session()
    interp = LLMInterpreter(session=session, db=db)
    rows = db.get_metric_schema('BTC-miners', active_only=False)
    row_id = next(r['id'] for r in rows if r['key'] == 'production_btc')
    db.update_metric_schema(row_id, quarterly_prompt="Quarterly instructions from DB")
    result = interp._get_quarterly_prompt_instructions('production_btc')
    assert "Quarterly instructions from DB" in result
