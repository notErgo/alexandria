"""
LLM-based metric extractor using Ollama (local inference).

Calls Ollama /api/generate with a metric-specific prompt + document text.
Returns an ExtractionResult or None on any failure.

Failure modes handled silently (return None):
  - Network error / timeout
  - HTTP 4xx / 5xx from Ollama
  - Malformed JSON in LLM response
  - Null or out-of-range value in LLM response
  - Missing metric in response JSON
"""
import json
import logging
from typing import Optional

try:
    import json_repair as _json_repair
    _HAS_JSON_REPAIR = True
except ImportError:
    _HAS_JSON_REPAIR = False

import requests

from config import LLM_BACKEND, LLM_BASE_URL, LLM_MODEL_ID, LLM_TIMEOUT_SECONDS
from interpreters.confidence import METRIC_VALID_RANGES
from interpreters.llm_prompt_builder import (
    _DEFAULT_PROMPTS,
    _DEFAULT_FALLBACK_PROMPT,
    _QUARTERLY_BATCH_PREAMBLE,
    _ANNUAL_BATCH_PREAMBLE,
    _DEFAULT_BATCH_PREAMBLE,
)
from miner_types import ExtractionResult


def _active_model(db=None) -> str:
    """Return the currently configured Ollama model name.

    Checks the config_settings DB row 'ollama_model' first (set via the UI).
    Falls back to the compile-time constant LLM_MODEL_ID (env var or default).
    """
    if db is not None:
        try:
            val = db.get_config('ollama_model')
            if val:
                return val
        except Exception:
            pass
    return LLM_MODEL_ID

# Valid ranges for quarterly/annual aggregated values (3x the monthly bounds for flow metrics).
# Snapshot metrics (hodl_btc, hashrate_eh, etc.) keep the same bounds since they are
# point-in-time values, not sums.
_QUARTERLY_VALID_RANGES = {
    k: (lo, hi * 3) for k, (lo, hi) in METRIC_VALID_RANGES.items()
}

log = logging.getLogger('miners.interpreters.llm_interpreter')


class LLMInterpreter:
    """
    Calls Ollama to extract a named metric from document text.

    Usage:
        extractor = LLMInterpreter(session=requests.Session(), db=miner_db)
        result = extractor.extract(text, 'production_btc')
        # Returns ExtractionResult or None
    """

    def __init__(self, session: requests.Session, db=None) -> None:
        self._session = session
        self._db = db  # Optional MinerDB for prompt lookup
        self._last_call_meta: dict = {}     # Populated by _call_ollama with timing fields
        self._last_batch_summary: str = ''  # Populated by _parse_batch_response
        self._last_transport_error: bool = False

    @staticmethod
    def get_default_prompt(metric: str) -> str:
        """Return the hardcoded default prompt for a metric (no DB lookup)."""
        if metric in _DEFAULT_PROMPTS:
            return _DEFAULT_PROMPTS[metric]
        return _DEFAULT_FALLBACK_PROMPT.replace('{metric}', metric)

    def check_connectivity(self) -> bool:
        """Return True if the configured LLM backend is reachable and ready."""
        try:
            if LLM_BACKEND == "llamacpp":
                resp = self._session.get(f"{LLM_BASE_URL}/health", timeout=5)
                return resp.status_code == 200
            # Ollama: check server version then verify the model is installed.
            # Without the model check, llm_available stays True even when every
            # /api/generate call gets a 404, routing all regex matches to
            # REGEX_ONLY / review_queue instead of being auto-accepted.
            resp = self._session.get(f"{LLM_BASE_URL}/api/version", timeout=5)
            if resp.status_code != 200:
                return False
            model_id = _active_model(self._db)
            model_resp = self._session.post(
                f"{LLM_BASE_URL}/api/show",
                json={"name": model_id},
                timeout=10,
            )
            if model_resp.status_code == 404:
                log.warning(
                    "Ollama model '%s' not found — LLM disabled (install with: ollama pull %s)",
                    model_id, model_id,
                )
                return False
            return model_resp.status_code == 200
        except Exception:
            return False

    @staticmethod
    def _build_temporal_anchor(expected_granularity: str, period: str = None) -> str:
        """Build a TEMPORAL SCOPE block for LLM prompts.

        Instructs the LLM to extract only figures whose time scope matches
        expected_granularity and to reject figures that belong to a broader or
        narrower period.
        """
        _other_map = {
            'monthly': 'quarterly or annual',
            'quarterly': 'annual',
            'annual': 'N/A',
        }
        other = _other_map.get(expected_granularity, 'other')
        period_line = period if period else 'see document'
        lines = [
            "=== TEMPORAL SCOPE (HARD CONSTRAINT) ===",
            f"Expected granularity: {expected_granularity}",
            f"Target period: {period_line}",
            f"Extract only {expected_granularity} figures. "
            f"If the document contains only a {other} figure for a metric, "
            f"return null for that metric. "
            f"Do NOT decompose {other} totals into estimated {expected_granularity} fractions.",
            "===",
            "",
        ]
        return "\n".join(lines)

    def extract(self, text: str, metric: str, config=None, period: str = None) -> Optional[ExtractionResult]:
        """
        Extract a metric value from document text using the LLM.

        Returns ExtractionResult or None on any failure.
        Never raises exceptions.

        config: Optional ExtractionRunConfig. When supplied, a temporal anchor
            block is prepended to the prompt.
        period: Optional period string forwarded to the temporal anchor.
        """
        try:
            prompt = self._get_prompt(metric).replace('{text}', text)
            if config is not None:
                anchor = self._build_temporal_anchor(config.expected_granularity, period)
                prompt = anchor + prompt
            raw_response = self._call_llm(prompt)
            if raw_response is None:
                return None
            return self._parse_response(raw_response, metric)
        except Exception as e:
            log.error("LLM extraction failed for metric %s: %s", metric, e, exc_info=True)
            return None

    # ------------------------------------------------------------------ #
    #  Batch extraction (1 Ollama call → all metrics)                     #
    # ------------------------------------------------------------------ #

    def extract_batch(
        self,
        text: str,
        metrics: list,
        ticker: str = None,
        expected_granularity: str = 'monthly',
        config=None,
        period: str = None,
    ) -> dict:
        """
        Extract all metrics in a single Ollama call.

        Pays the document prefill cost once instead of once per metric (~13x).
        Returns a dict of {metric: ExtractionResult} for metrics where a valid
        value was found. Returns {} on any failure so the caller can fall back
        to per-metric extract() calls.

        config: Optional ExtractionRunConfig. When supplied, config.expected_granularity
            overrides the expected_granularity param and a temporal anchor block is
            prepended to the prompt.
        expected_granularity: legacy param, used when config is None.
        period: Optional period string forwarded to the temporal anchor.
        """
        # config wins over legacy param
        _eg = config.expected_granularity if config is not None else expected_granularity
        _model = (config.model if config is not None else None) or None
        try:
            prompt = self._build_batch_prompt(text, metrics, ticker=ticker, config=config, period=period)
            raw = self._call_llm(prompt, model=_model)
            if raw is None:
                return {}
            return self._parse_batch_response(raw, metrics, model=_model)
        except Exception as e:
            log.error("LLM batch extraction failed: %s", e, exc_info=True)
            return {}

    def extract_for_period(
        self,
        text: str,
        metrics: list,
        current_period: str,
        target_period: str,
    ) -> dict:
        """Ask the LLM if text explicitly mentions figures for the PRIOR month.

        Builds a targeted prompt instructing the LLM to extract values only for
        target_period (not current_period). Returns a dict of
        {metric: ExtractionResult} or {} on any failure.
        """
        try:
            prompt = self._build_gap_fill_prompt(text, metrics, current_period, target_period)
            raw = self._call_llm(prompt)
            if raw is None:
                return {}
            return self._parse_batch_response(raw, metrics)
        except Exception as e:
            log.error(
                "LLM gap-fill extraction failed for %s→%s: %s",
                current_period, target_period, e, exc_info=True,
            )
            return {}

    def extract_with_correction(
        self,
        text: str,
        metric: str,
        first_value,
        concern_context: str,
        ticker: str = None,
    ) -> Optional[ExtractionResult]:
        """Run a targeted self-correction pass with explicit concern context.

        Used when the agreement engine routes to REVIEW_QUEUE or OUTLIER_FLAGGED.
        Wraps the standard metric prompt with a preamble explaining the concern
        so the LLM can re-read the document with that specific issue in mind.

        Args:
            text:            Document text (already truncated by caller).
            metric:          Metric being corrected (e.g. 'hashrate_eh').
            first_value:     The value returned in the first extraction pass.
            concern_context: Human-readable explanation of the concern
                             (e.g. disagreement magnitude, outlier vs trailing avg).
            ticker:          Optional ticker for prompt hints.

        Returns:
            ExtractionResult or None on failure.
        """
        try:
            base_instructions = self._get_prompt_instructions(metric)
            unit = ''
            if self._db is not None:
                try:
                    rows = self._db.get_metric_schema('BTC-miners', active_only=False)
                    schema_row = next((r for r in rows if r['key'] == metric), None)
                    if schema_row and schema_row.get('unit'):
                        unit = schema_row['unit']
                except Exception:
                    pass
            preamble = (
                f"Your first extraction returned {metric} = {first_value} {unit}.\n"
                f"A cross-check raised a concern: {concern_context}\n\n"
                f"Re-read the document carefully with this specific concern in mind "
                f"and extract again.\n\n"
                f"{base_instructions}\n\n"
            )
            # Add ticker hint if available
            if ticker and self._db is not None:
                try:
                    hint_row = self._db.get_ticker_hint(ticker)
                    if hint_row:
                        preamble = f"=== COMPANY CONTEXT: {ticker} ===\n{hint_row}\n\n" + preamble
                except Exception:
                    pass

            output_fmt = (
                f"Return ONLY this JSON, no other text:\n"
                f'{{"metric":"{metric}","value":<number or null>,"unit":"{unit}",'
                f'"confidence":<0.0-1.0>,"source_snippet":"<exact phrase, max 100 chars>"}}\n\n'
            )
            prompt = preamble + output_fmt + "Document:\n" + text

            raw = self._call_llm(prompt)
            if raw is None:
                return None
            result = self._parse_response(raw, metric)
            if result is not None:
                result = result.__class__(
                    value=result.value,
                    unit=result.unit,
                    confidence=result.confidence,
                    extraction_method='llm_correction',
                    source_snippet=result.source_snippet,
                    metric=result.metric,
                    pattern_id=result.pattern_id,
                )
            return result
        except Exception as e:
            log.error(
                "Self-correction extraction failed for %s metric=%s: %s",
                ticker or 'unknown', metric, e, exc_info=True,
            )
            return None

    def _strip_output_format(self, full_prompt: str) -> str:
        """Strip output-format boilerplate from a full prompt string."""
        for sentinel in ("Return ONLY this JSON", "Return ONLY valid JSON", "Document:\n"):
            idx = full_prompt.find(sentinel)
            if idx != -1:
                return full_prompt[:idx].rstrip()
        return full_prompt.rstrip()

    def _get_prompt_instructions(self, metric: str) -> str:
        """Return the task-description block of a metric's prompt.

        Lookup order:
        1. llm_prompts table (active=1 DB override)
        2. metric_schema.prompt_instructions
        3. _DEFAULT_PROMPTS[metric] (hardcoded baseline, boilerplate stripped)
        4. _DEFAULT_FALLBACK_PROMPT (generic template)

        Used by _build_batch_prompt to embed per-metric instructions without
        duplicating the output-format boilerplate for each metric.
        """
        # Tier 1: Check llm_prompts DB override
        if self._db is not None:
            try:
                with self._db._get_connection() as conn:
                    row = conn.execute(
                        "SELECT prompt_text FROM llm_prompts WHERE metric=? AND active=1 "
                        "ORDER BY id DESC LIMIT 1",
                        (metric,)
                    ).fetchone()
                    if row and row[0]:
                        return self._strip_output_format(row[0])
            except Exception as e:
                log.warning("Could not fetch LLM prompt from DB for %s: %s", metric, e)

        # Tier 2: metric_schema.prompt_instructions (already stripped of boilerplate)
        if self._db is not None:
            try:
                rows = self._db.get_metric_schema('BTC-miners', active_only=False)
                schema_row = next((r for r in rows if r['key'] == metric), None)
                if schema_row and schema_row.get('prompt_instructions'):
                    return schema_row['prompt_instructions']
            except Exception as e:
                log.warning("Could not fetch metric_schema.prompt_instructions for %s: %s", metric, e)

        # Tier 3: hardcoded _DEFAULT_PROMPTS
        if metric in _DEFAULT_PROMPTS:
            return self._strip_output_format(_DEFAULT_PROMPTS[metric])

        # Tier 4: generic fallback
        return _DEFAULT_FALLBACK_PROMPT.replace('{metric}', metric)

    def _get_quarterly_prompt_instructions(self, metric: str) -> str:
        """Return quarterly extraction instructions for a metric.

        Lookup order:
        1. metric_schema.quarterly_prompt
        2. _get_prompt_instructions(metric) (monthly instructions as fallback — period scoping
           is handled by the quarterly preamble, per-metric disambiguation is period-agnostic)
        """
        # Tier 1: metric_schema.quarterly_prompt
        if self._db is not None:
            try:
                rows = self._db.get_metric_schema('BTC-miners', active_only=False)
                schema_row = next((r for r in rows if r['key'] == metric), None)
                if schema_row and schema_row.get('quarterly_prompt'):
                    return schema_row['quarterly_prompt']
            except Exception as e:
                log.warning("Could not fetch metric_schema.quarterly_prompt for %s: %s", metric, e)

        # Tier 2: fall back to monthly instructions (period scoping handled by preamble)
        return self._get_prompt_instructions(metric)

    def _fetch_unit_map(self) -> dict:
        """Return {metric_key: unit} from metric_schema. Empty dict on failure."""
        if self._db is None:
            return {}
        try:
            rows = self._db.get_metric_schema('BTC-miners', active_only=False)
            return {r['key']: (r.get('unit') or '') for r in rows}
        except Exception:
            return {}

    def _append_examples_block(self, lines: list, metrics: list, ticker: str = None) -> None:
        """Inject === EXAMPLE PATTERNS === block into a prompt lines list.

        Uses a single bulk DB query for all metrics to avoid N connections.
        Exceptions are swallowed so a DB failure never crashes prompt building.
        """
        if self._db is None:
            return
        try:
            examples_by_metric = self._db.get_bulk_active_examples_for_prompt(metrics, ticker=ticker)
            all_examples = []
            for metric in metrics:
                for snippet in examples_by_metric.get(metric, []):
                    entry = f"  [{metric}] {snippet}"
                    if entry not in all_examples:
                        all_examples.append(entry)
            if all_examples:
                lines.append("=== EXAMPLE PATTERNS ===")
                lines.append(
                    "These are real snippets from past successful extractions. "
                    "Use them as recognition templates only — do not copy these values. "
                    "Extract only from the document below."
                )
                lines.extend(all_examples)
                lines.append("===\n")
        except Exception as e:
            log.warning("metric_examples fetch failed, skipping: %s", e)

    def _build_batch_prompt(self, text: str, metrics: list, ticker: str = None,
                            config=None, period: str = None) -> str:
        """
        Build a single prompt that asks the LLM to extract all metrics at once.

        Structure:
          [temporal anchor — when config is supplied]
          [preamble — from DB config_settings or _DEFAULT_BATCH_PREAMBLE]
          [=== COMPANY CONTEXT: {ticker} === if hint is set]
          === METRIC: <name> ===
          [instructions from _get_prompt_instructions]
          ...repeated for each metric...
          === OUTPUT FORMAT ===
          Return ONLY this JSON: { ... }
          Document:
          {text}

        Unit values are read from metric_schema at build time.
        config: Optional ExtractionRunConfig. When supplied, prepend a TEMPORAL SCOPE block.
        period: Optional period string forwarded to the temporal anchor.
        """
        # Temporal anchor block (prepended before preamble when granularity is set)
        _temporal_prefix = ''
        if config is not None and config.expected_granularity is not None:
            _temporal_prefix = self._build_temporal_anchor(config.expected_granularity, period)

        # Preamble priority: custom_prompt_preamble > DB override > hardcoded constant
        preamble = _DEFAULT_BATCH_PREAMBLE
        if config is not None and config.custom_prompt_preamble:
            preamble = config.custom_prompt_preamble
        elif self._db is not None:
            try:
                db_preamble = self._db.get_config('llm_batch_preamble')
                if db_preamble:
                    preamble = db_preamble
            except Exception as e:
                log.warning("Could not fetch llm_batch_preamble from DB: %s", e)

        lines = [_temporal_prefix + preamble] if _temporal_prefix else [preamble]

        # Per-ticker context hint (injected after preamble if set)
        if ticker and self._db is not None:
            try:
                hint = self._db.get_ticker_hint(ticker)
                if hint:
                    lines.append(f"=== COMPANY CONTEXT: {ticker} ===")
                    lines.append(hint)
                    lines.append("===\n")
            except Exception as e:
                log.warning("Could not fetch ticker hint for %s: %s", ticker, e)

        # Per-metric anchor terms (from metric_keywords v31 SSOT, with exclude hints)
        if self._db is not None:
            try:
                from infra.keyword_service import get_all_active_rows as _get_kw_rows
                kw_rows = _get_kw_rows(self._db)
                if kw_rows:
                    lines.append("=== ANCHOR TERMS ===")
                    lines.append(
                        "Scan the document for these exact phrases as anchor points. "
                        "When you find an anchor phrase, the associated numeric value is almost always in the same "
                        "table row or the same sentence. Read the ~100 characters surrounding the phrase to locate it. "
                        "Your source_snippet must be the verbatim text containing both the phrase and value (max 100 chars). "
                        "If the value cannot be found within one table row or sentence of the phrase, return null — do not guess."
                    )
                    for kw in kw_rows:
                        entry = f"- {kw['phrase']}"
                        excl = (kw.get('exclude_terms') or '').strip()
                        if excl:
                            entry += f" (ignore if surrounded by: {excl})"
                        lines.append(entry)
                    lines.append("===\n")
            except Exception as e:
                log.warning("Could not fetch metric keywords for prompt: %s", e)

        # Unconditional ticker line — helps model orient to the correct company
        if ticker:
            lines.append(f"Company: {ticker}\n")

        self._append_examples_block(lines, metrics, ticker=ticker)

        # Target metrics from metric_schema (SSOT — never hardcoded)
        # When config.target_metrics is set, restrict to those keys only.
        _target_set = set(getattr(config, 'target_metrics', None) or [])
        unit_map: dict = {}
        if self._db is not None:
            try:
                metric_rows = self._db.get_metric_schema('BTC-miners', active_only=True)
                if _target_set:
                    metric_rows = [r for r in metric_rows if r['key'] in _target_set]
                if metric_rows:
                    unit_map = {m['key']: (m.get('unit') or '') for m in metric_rows}
                    lines.append("=== TARGET METRICS ===")
                    for m in metric_rows:
                        lines.append(f"- {m['label']} ({m['key']}, unit: {m['unit']})")
                    lines.append("Extract a numeric value for each metric if mentioned.")
                    lines.append("===\n")
            except Exception as e:
                log.warning("Could not fetch metric schema for prompt: %s", e)
        if not unit_map:
            unit_map = self._fetch_unit_map()

        _metrics_for_prompt = [m for m in metrics if not _target_set or m in _target_set]
        for metric in _metrics_for_prompt:
            lines.append(f"=== METRIC: {metric} ===")
            lines.append(self._get_prompt_instructions(metric))
            lines.append("")

        # Output format block
        lines.append("=== OUTPUT FORMAT ===")
        lines.append("Return ONLY this JSON object, no other text:")
        lines.append("The top-level JSON value MUST be an object keyed by metric name.")
        lines.append("Do NOT return an array, list, markdown code fence, commentary, or repeated per-metric objects.")
        lines.append("{")
        for metric in _metrics_for_prompt:
            unit = unit_map.get(metric, "")
            lines.append(
                f'  "{metric}": {{"value": <number or null>, "unit": "{unit}", '
                f'"confidence": <0.0-1.0>, "source_snippet": "<max 100 chars>", '
                f'"period_granularity": "monthly|quarterly|annual|unknown"}},'
            )
        lines.append('  "summary": "<one sentence: document type, company, period, and key figures found — max 150 chars>"')
        lines.append("}")
        lines.append("")
        lines.append("Document:")
        lines.append(text)

        return "\n".join(lines)

    def _build_multi_period_prompt(
        self,
        text: str,
        metrics: list,
        current_period: str,
        target_periods: list,
    ) -> str:
        """Build a prompt to extract historical monthly figures for multiple prior periods.

        Used when a press release contains a trailing table listing the last
        N months of production (e.g. Jan/Feb/Mar at the bottom of an April report).
        Each target_period must be a YYYY-MM-01 string.
        """
        period_list = ', '.join(target_periods)
        lines = [
            f"This document was published reporting figures for {current_period}. "
            f"Your task: extract values EXPLICITLY stated for these prior months: {period_list}. "
            f"Do NOT extract values for {current_period}. "
            f"Only return a value for a period if the document names that specific month explicitly "
            f"(by month name or YYYY-MM date). If a period is not mentioned, set all its values to null.\n",
        ]

        _unit_map = self._fetch_unit_map()

        for metric in metrics:
            lines.append(f"=== METRIC: {metric} ===")
            lines.append(self._get_prompt_instructions(metric))
            lines.append("")

        lines.append("=== OUTPUT FORMAT ===")
        lines.append("Return ONLY this JSON object, no other text:")
        lines.append("{")
        for period in target_periods:
            lines.append(f'  "{period}": {{')
            for metric in metrics:
                unit = _unit_map.get(metric, "")
                lines.append(
                    f'    "{metric}": {{"value": <number or null>, "unit": "{unit}", '
                    f'"confidence": <0.0-1.0>, "source_snippet": "<max 100 chars>"}},'
                )
            lines.append("  },")
        lines.append("}")
        lines.append("")
        lines.append("Document:")
        lines.append(text)

        return "\n".join(lines)

    def _parse_multi_period_response(
        self,
        raw: str,
        metrics: list,
        target_periods: list,
    ) -> dict:
        """Parse a multi-period JSON response into {period: {metric: ExtractionResult}}."""
        start = raw.find('{')
        end = raw.rfind('}') + 1
        if start == -1 or end == 0:
            log.debug("No JSON object in multi-period LLM response")
            return {}

        try:
            data = json.loads(raw[start:end])
        except (json.JSONDecodeError, ValueError) as e:
            if _HAS_JSON_REPAIR:
                try:
                    data = json.loads(_json_repair.repair_json(raw[start:end]))
                    log.debug("Multi-period JSON repaired (original error: %s)", e)
                except Exception:
                    return {}
            else:
                return {}

        _model = _active_model(self._db)
        results = {}

        for period in target_periods:
            period_data = data.get(period)
            if not isinstance(period_data, dict):
                continue

            period_results = {}
            for metric in metrics:
                entry = period_data.get(metric)
                if not isinstance(entry, dict):
                    continue
                value = entry.get('value')
                if value is None:
                    continue
                try:
                    value = float(value)
                except (TypeError, ValueError):
                    continue
                bounds = METRIC_VALID_RANGES.get(metric)
                if bounds is not None:
                    lo, hi = bounds
                    if not (lo <= value <= hi):
                        log.debug(
                            "Multi-period LLM value %.4f out of range for %s %s",
                            value, period, metric,
                        )
                        continue
                unit = str(entry.get('unit', ''))
                confidence = float(entry.get('confidence', 0.5))
                confidence = max(0.0, min(1.0, confidence))
                source_snippet = str(entry.get('source_snippet') or raw[:200])
                period_results[metric] = ExtractionResult(
                    metric=metric,
                    value=value,
                    unit=unit,
                    confidence=confidence,
                    extraction_method=f"llm_{_model}",
                    source_snippet=source_snippet,
                    pattern_id=f"llm_{_model}",
                    period_granularity='monthly',
                )

            if period_results:
                results[period] = period_results

        return results

    def extract_historical_periods(
        self,
        text: str,
        metrics: list,
        current_period: str,
        target_periods: list,
    ) -> dict:
        """Extract monthly values for multiple historical periods in a single LLM call.

        Returns {period: {metric: ExtractionResult}} for periods where values were found.
        """
        try:
            prompt = self._build_multi_period_prompt(text, metrics, current_period, target_periods)
            raw = self._call_llm(prompt)
            if raw is None:
                return {}
            return self._parse_multi_period_response(raw, metrics, target_periods)
        except Exception as e:
            log.error(
                "Multi-period historical extraction failed current=%s targets=%s: %s",
                current_period, target_periods, e, exc_info=True,
            )
            return {}

    def _build_gap_fill_prompt(
        self,
        text: str,
        metrics: list,
        current_period: str,
        target_period: str,
    ) -> str:
        """Build a targeted prompt for prior-period gap fill.

        Instructs the LLM to only extract values explicitly attributed to
        target_period (the prior month), not to current_period.
        """
        lines = [
            f"This document was published reporting figures for {current_period}. "
            f"Your task: find values that are EXPLICITLY stated for the PRIOR month "
            f"({target_period}). Do NOT extract values for {current_period}. "
            f"Only return a value if the text names {target_period} (or the matching "
            f"month name) specifically.\n",
        ]

        for metric in metrics:
            lines.append(f"=== METRIC: {metric} ===")
            lines.append(self._get_prompt_instructions(metric))
            lines.append("")

        _unit_map = self._fetch_unit_map()

        lines.append("=== OUTPUT FORMAT ===")
        lines.append("Return ONLY this JSON object, no other text:")
        lines.append("{")
        for metric in metrics:
            unit = _unit_map.get(metric, "")
            lines.append(
                f'  "{metric}": {{"value": <number or null>, "unit": "{unit}", '
                f'"confidence": <0.0-1.0>, "source_snippet": "<max 100 chars>"}},'
            )
        lines.append("}")
        lines.append("")
        lines.append("Document:")
        lines.append(text)

        return "\n".join(lines)

    def _parse_batch_response(
        self, raw: str, metrics: list, model: Optional[str] = None
    ) -> dict:
        """
        Parse the LLM's batch JSON response.

        Iterates `metrics` (not data.keys()) to ignore LLM hallucinations.
        Applies the same null/float/range/clamp checks as _parse_response.
        Returns dict of {metric: ExtractionResult} for valid entries only.

        Granularity filtering is NOT performed here — it is the responsibility of
        the write-time validator (validate_period_granularity in interpret_pipeline.py)
        to reject results whose period_granularity does not match the expected
        granularity for the document. This allows the parser to remain neutral and
        the decision to be made at a single authoritative location.
        """
        start = raw.find('{')
        end = raw.rfind('}') + 1
        if start == -1 or end == 0:
            log.debug(
                "No JSON object found in LLM batch response (first 300 chars): %r",
                raw[:300],
            )
            return {}

        try:
            data = json.loads(raw[start:end])
        except (json.JSONDecodeError, ValueError) as e:
            if _HAS_JSON_REPAIR:
                try:
                    data = json.loads(_json_repair.repair_json(raw[start:end]))
                    log.debug("LLM batch JSON repaired (original error: %s)", e)
                except Exception:
                    log.debug("Could not parse LLM batch JSON: %s", e)
                    return {}
            else:
                log.debug("Could not parse LLM batch JSON: %s", e)
                return {}

        self._last_batch_summary = str(data.get('summary') or '').strip()[:200]

        results = {}
        for metric in metrics:
            entry = data.get(metric)
            if not isinstance(entry, dict):
                continue

            value = entry.get('value')
            if value is None:
                continue

            try:
                value = float(value)
            except (TypeError, ValueError):
                log.debug("Batch LLM value not numeric for %s: %r", metric, value)
                continue

            bounds = METRIC_VALID_RANGES.get(metric)
            if bounds is not None:
                lo, hi = bounds
                if not (lo <= value <= hi):
                    log.debug(
                        "Batch LLM value %.4f out of range [%.1f, %.1f] for %s",
                        value, lo, hi, metric,
                    )
                    continue

            unit = str(entry.get('unit', ''))
            confidence = float(entry.get('confidence', 0.5))
            confidence = max(0.0, min(1.0, confidence))
            source_snippet = str(entry.get('source_snippet') or raw[:200])
            period_granularity = str(entry.get('period_granularity') or 'unknown').lower().strip()

            _model = model or _active_model(self._db)
            results[metric] = ExtractionResult(
                metric=metric,
                value=value,
                unit=unit,
                confidence=confidence,
                extraction_method=f"llm_{_model}",
                source_snippet=source_snippet,
                pattern_id=f"llm_{_model}",
                period_granularity=period_granularity,
            )

        return results

    def _get_prompt(self, metric: str) -> str:
        """Fetch prompt from llm_prompts DB table, or fall back to hardcoded default.

        Ensures the returned string contains '{text}' so extract() can substitute
        the document. If the stored or default prompt is instructions-only (no
        document placeholder), appends 'Document:\\n{text}' automatically.
        """
        raw = None
        if self._db is not None:
            try:
                with self._db._get_connection() as conn:
                    row = conn.execute(
                        "SELECT prompt_text FROM llm_prompts WHERE metric=? AND active=1 "
                        "ORDER BY id DESC LIMIT 1",
                        (metric,)
                    ).fetchone()
                    if row:
                        raw = row[0]
            except Exception as e:
                log.warning("Could not fetch LLM prompt from DB for %s: %s", metric, e)

        if raw is None:
            # Fall back to hardcoded defaults
            if metric in _DEFAULT_PROMPTS:
                raw = _DEFAULT_PROMPTS[metric]
            else:
                # Generic fallback for unknown metrics
                raw = _DEFAULT_FALLBACK_PROMPT.replace('{metric}', metric)

        # Ensure document placeholder is present (slim prompts omit it)
        if '{text}' not in raw:
            raw = raw + '\n\nDocument:\n{text}'
        return raw

    def _extract_keep_alive(self) -> str:
        """Return the keep_alive value to send with every Ollama call.

        Configurable via 'ollama_keep_alive' in config_settings.
        Fallback: OLLAMA_KEEP_ALIVE constant (default "2h").
        """
        if self._db is not None:
            try:
                v = self._db.get_config('ollama_keep_alive')
                if v:
                    return v
            except Exception:
                pass
        from config import OLLAMA_KEEP_ALIVE
        return OLLAMA_KEEP_ALIVE

    def _extract_num_ctx(self) -> int:
        """Return the num_ctx to use for extraction Ollama calls.

        Extraction prompts are a single document + preamble, typically 3-4k tokens.
        8192 is sufficient and avoids over-allocating VRAM with the model's default
        (often 32768).  Configurable via 'extract_num_ctx' in config_settings.
        """
        default = 8192
        if self._db is not None:
            try:
                v = self._db.get_config('extract_num_ctx')
                if v:
                    return int(v)
            except Exception:
                pass
        return default

    def _call_llm(self, prompt: str, model: Optional[str] = None) -> Optional[str]:
        """
        POST to the configured LLM backend. Returns the response text or None on failure.

        model: optional per-call override; falls back to _active_model(db) when None.

        Supports two backends (controlled by LLM_BACKEND env var):
          "ollama"   — POST /api/generate  (default)
          "llamacpp" — POST /completion    (llama-server)
        """
        import re as _re
        self._last_transport_error = False
        _model = model or _active_model(self._db)

        if LLM_BACKEND == "llamacpp":
            url = f"{LLM_BASE_URL}/v1/chat/completions"
            payload = {
                "model": _model,
                "messages": [{"role": "user", "content": prompt}],
                "temperature": 0.0,
                "max_tokens": 768,
                "stream": False,
            }
            error_label = "llama.cpp /v1/chat/completions"
            def _extract_text(data: dict) -> str:
                choices = data.get("choices") or []
                return choices[0].get("message", {}).get("content", "") if choices else ""
            def _extract_meta(data: dict, text: str) -> dict:
                usage = data.get("usage") or {}
                return {
                    'prompt_tokens': usage.get('prompt_tokens', 0) or 0,
                    'response_tokens': usage.get('completion_tokens', 0) or 0,
                    'eval_duration_ms': 0,
                    'total_duration_ms': 0,
                    'response_chars': len(text),
                }
        else:
            url = f"{LLM_BASE_URL}/api/generate"
            payload = {
                "model": _model,
                "prompt": prompt,
                "stream": False,
                "keep_alive": self._extract_keep_alive(),
                "think": False,
                "options": {
                    "temperature": 0.0,
                    "num_predict": 768,
                    "num_ctx": self._extract_num_ctx(),
                },
            }
            error_label = "Ollama /api/generate"
            def _extract_text(data: dict) -> str:
                text = data.get("response", "")
                # Strip <think>...</think> blocks emitted by Qwen3 when the Ollama
                # version does not honour the "think": False API parameter.
                if not data.get("thinking", ""):
                    text = _re.sub(r'<think>.*?</think>', '', text, flags=_re.DOTALL).strip()
                return text
            def _extract_meta(data: dict, text: str) -> dict:
                return {
                    'prompt_tokens': data.get('prompt_eval_count', 0) or 0,
                    'response_tokens': data.get('eval_count', 0) or 0,
                    'eval_duration_ms': (data.get('eval_duration', 0) or 0) / 1e6,
                    'total_duration_ms': (data.get('total_duration', 0) or 0) / 1e6,
                    'response_chars': len(text),
                }

        try:
            resp = self._session.post(url, json=payload, timeout=LLM_TIMEOUT_SECONDS)
            if resp.status_code >= 400:
                log.warning("%s returned HTTP %d", error_label, resp.status_code)
                self._last_transport_error = True
                self._last_call_meta = {}
                return None
            data = resp.json()
            response_text = _extract_text(data)
            self._last_call_meta = _extract_meta(data, response_text)
            return response_text
        except requests.Timeout:
            log.warning("%s timed out after %ds", error_label, LLM_TIMEOUT_SECONDS)
            self._last_transport_error = True
            self._last_call_meta = {}
            return None
        except requests.RequestException as e:
            log.error("%s request failed: %s", error_label, e)
            self._last_transport_error = True
            self._last_call_meta = {}
            return None
        except (ValueError, KeyError) as e:
            log.error("Ollama response malformed: %s", e)
            self._last_call_meta = {}
            return None

    # ------------------------------------------------------------------ #
    #  Quarterly / annual batch extraction                               #
    # ------------------------------------------------------------------ #

    def extract_quarterly_batch(
        self,
        text: str,
        metrics: list,
        ticker: str = None,
        period_type: str = 'quarterly',  # 'quarterly' | 'annual'
        config=None,
    ) -> dict:
        """Like extract_batch() but uses quarterly/annual prompts and preamble.

        config: Optional ExtractionRunConfig. When supplied, config.model overrides
            the global model setting for this call.
        Returns dict of {metric: ExtractionResult} for metrics where a valid value
        was found. Returns {} on any failure so caller can handle gracefully.
        """
        _model = (config.model if config is not None else None) or None
        try:
            prompt = self._build_quarterly_batch_prompt(
                text, metrics, ticker=ticker, period_type=period_type, config=config
            )
            raw = self._call_llm(prompt, model=_model)
            if raw is None:
                return {}
            return self._parse_quarterly_batch_response(raw, metrics, period_type=period_type, model=_model)
        except Exception as e:
            log.error("LLM quarterly batch extraction failed: %s", e, exc_info=True)
            return {}

    def _build_quarterly_batch_prompt(
        self,
        text: str,
        metrics: list,
        ticker: str = None,
        period_type: str = 'quarterly',
        config=None,
    ) -> str:
        """Build a prompt for quarterly or annual extraction.

        Uses _QUARTERLY_BATCH_PREAMBLE or _ANNUAL_BATCH_PREAMBLE and
        per-metric instructions from _get_quarterly_prompt_instructions (period
        scoping is handled by the preamble; per-metric disambiguation is period-agnostic).
        """
        preamble = _ANNUAL_BATCH_PREAMBLE if period_type == 'annual' else _QUARTERLY_BATCH_PREAMBLE
        if self._db is not None:
            db_key = 'llm_annual_batch_preamble' if period_type == 'annual' else 'llm_quarterly_batch_preamble'
            try:
                db_preamble = self._db.get_config(db_key)
                if db_preamble:
                    preamble = db_preamble
            except Exception as e:
                log.warning("Could not fetch %s from DB: %s", db_key, e)

        lines = [preamble]

        # Per-ticker context hint (injected after preamble if set)
        if ticker and self._db is not None:
            try:
                hint = self._db.get_ticker_hint(ticker)
                if hint:
                    lines.append(f"=== COMPANY CONTEXT: {ticker} ===")
                    lines.append(hint)
                    lines.append("===\n")
            except Exception as e:
                log.warning("Could not fetch ticker hint for %s: %s", ticker, e)

        # Per-metric anchor terms (from metric_keywords v31 SSOT, with exclude hints)
        if self._db is not None:
            try:
                from infra.keyword_service import get_all_active_rows as _get_kw_rows
                kw_rows = _get_kw_rows(self._db)
                if kw_rows:
                    lines.append("=== ANCHOR TERMS ===")
                    lines.append(
                        "Scan the document for these exact phrases as anchor points. "
                        "When you find an anchor phrase, the associated numeric value is almost always in the same "
                        "table row or the same sentence. Read the ~100 characters surrounding the phrase to locate it. "
                        "Your source_snippet must be the verbatim text containing both the phrase and value (max 100 chars). "
                        "If the value cannot be found within one table row or sentence of the phrase, return null — do not guess."
                    )
                    for kw in kw_rows:
                        entry = f"- {kw['phrase']}"
                        excl = (kw.get('exclude_terms') or '').strip()
                        if excl:
                            entry += f" (ignore if surrounded by: {excl})"
                        lines.append(entry)
                    lines.append("===\n")
            except Exception as e:
                log.warning("Could not fetch metric keywords for prompt: %s", e)

        # Unconditional ticker line — helps model orient to the correct company
        if ticker:
            lines.append(f"Company: {ticker}\n")

        self._append_examples_block(lines, metrics, ticker=ticker)

        # Target metrics from metric_schema (SSOT — never hardcoded)
        # When config.target_metrics is set, restrict to those keys only.
        _target_set = set(getattr(config, 'target_metrics', None) or [])
        unit_map: dict = {}
        if self._db is not None:
            try:
                metric_rows = self._db.get_metric_schema('BTC-miners', active_only=True)
                if _target_set:
                    metric_rows = [r for r in metric_rows if r['key'] in _target_set]
                if metric_rows:
                    unit_map = {m['key']: (m.get('unit') or '') for m in metric_rows}
                    lines.append("=== TARGET METRICS ===")
                    for m in metric_rows:
                        lines.append(f"- {m['label']} ({m['key']}, unit: {m['unit']})")
                    lines.append("Extract a numeric value for each metric if mentioned.")
                    lines.append("===\n")
            except Exception as e:
                log.warning("Could not fetch metric schema for prompt: %s", e)
        if not unit_map:
            unit_map = self._fetch_unit_map()

        _metrics_for_prompt = [m for m in metrics if not _target_set or m in _target_set]
        for metric in _metrics_for_prompt:
            lines.append(f"=== METRIC: {metric} ===")
            lines.append(self._get_quarterly_prompt_instructions(metric))
            lines.append("")

        lines.append("=== OUTPUT FORMAT ===")
        lines.append("Return ONLY this JSON object, no other text:")
        lines.append("{")
        for metric in _metrics_for_prompt:
            unit = unit_map.get(metric, "")
            lines.append(
                f'  "{metric}": {{"value": <number or null>, "unit": "{unit}", '
                f'"confidence": <0.0-1.0>, "source_snippet": "<max 100 chars>"}},'
            )
        lines.append("}")
        lines.append("")
        lines.append("Document:")
        lines.append(text)

        return "\n".join(lines)

    def _parse_quarterly_batch_response(
        self, raw: str, metrics: list, period_type: str = 'quarterly', model: Optional[str] = None
    ) -> dict:
        """Parse LLM batch response for quarterly/annual extraction.

        Like _parse_batch_response but applies 3x range bounds for 'quarterly'
        period_type to accommodate quarterly/annual aggregated values.
        """
        start = raw.find('{')
        end = raw.rfind('}') + 1
        if start == -1 or end == 0:
            log.debug(
                "No JSON object found in LLM quarterly batch response (first 300 chars): %r",
                raw[:300],
            )
            return {}

        try:
            data = json.loads(raw[start:end])
        except (json.JSONDecodeError, ValueError) as e:
            if _HAS_JSON_REPAIR:
                try:
                    data = json.loads(_json_repair.repair_json(raw[start:end]))
                    log.debug("LLM quarterly batch JSON repaired (original error: %s)", e)
                except Exception:
                    log.debug("Could not parse LLM quarterly batch JSON: %s", e)
                    return {}
            else:
                log.debug("Could not parse LLM quarterly batch JSON: %s", e)
                return {}

        if not isinstance(data, dict):
            log.debug(
                "Ignoring LLM quarterly batch response with top-level %s instead of object",
                type(data).__name__,
            )
            return {}

        # Quarterly/annual data gets wider valid ranges for flow metrics
        valid_ranges = _QUARTERLY_VALID_RANGES if period_type in ('quarterly', 'annual') else METRIC_VALID_RANGES

        results = {}
        for metric in metrics:
            entry = data.get(metric)
            if not isinstance(entry, dict):
                continue

            value = entry.get('value')
            if value is None:
                continue

            try:
                value = float(value)
            except (TypeError, ValueError):
                log.debug("Quarterly batch LLM value not numeric for %s: %r", metric, value)
                continue

            bounds = valid_ranges.get(metric)
            if bounds is not None:
                lo, hi = bounds
                if not (lo <= value <= hi):
                    log.debug(
                        "Quarterly batch LLM value %.4f out of range [%.1f, %.1f] for %s",
                        value, lo, hi, metric,
                    )
                    continue

            unit = str(entry.get('unit', ''))
            confidence = float(entry.get('confidence', 0.5))
            confidence = max(0.0, min(1.0, confidence))
            source_snippet = str(entry.get('source_snippet') or raw[:200])

            _model = model or _active_model(self._db)
            results[metric] = ExtractionResult(
                metric=metric,
                value=value,
                unit=unit,
                confidence=confidence,
                extraction_method=f"llm_{_model}",
                source_snippet=source_snippet,
                pattern_id=f"llm_{_model}",
            )

        return results

    def _parse_response(self, raw: str, metric: str) -> Optional[ExtractionResult]:
        """
        Parse the LLM's response text as JSON.

        Expected format: {"metric": "...", "value": <float|null>, "unit": "...", "confidence": <float>}

        Returns ExtractionResult or None if:
          - JSON cannot be parsed
          - value is null / missing
          - value is outside the metric's valid range
        """
        try:
            # Find the JSON object in the response (LLM may include surrounding text)
            start = raw.find('{')
            end = raw.rfind('}') + 1
            if start == -1 or end == 0:
                log.debug("No JSON object found in LLM response for %s", metric)
                return None
            data = json.loads(raw[start:end])
        except (json.JSONDecodeError, ValueError) as e:
            log.debug("Could not parse LLM JSON for %s: %s", metric, e)
            return None

        value = data.get('value')
        if value is None:
            log.debug("LLM returned null value for %s", metric)
            return None

        try:
            value = float(value)
        except (TypeError, ValueError):
            log.debug("LLM value not numeric for %s: %r", metric, value)
            return None

        # Range check using the same bounds as confidence.py
        bounds = METRIC_VALID_RANGES.get(metric)
        if bounds is not None:
            lo, hi = bounds
            if not (lo <= value <= hi):
                log.debug(
                    "LLM value %.4f out of range [%.1f, %.1f] for %s",
                    value, lo, hi, metric
                )
                return None

        unit = str(data.get('unit', ''))
        confidence = float(data.get('confidence', 0.5))
        confidence = max(0.0, min(1.0, confidence))  # clamp to [0, 1]

        # source_snippet: use the model's self-reported context if available
        source_snippet = str(data.get('source_snippet', raw[:500]))

        _model = _active_model(self._db)
        return ExtractionResult(
            metric=metric,
            value=value,
            unit=unit,
            confidence=confidence,
            extraction_method=f"llm_{_model}",
            source_snippet=source_snippet,
            pattern_id=f"llm_{_model}",
        )
