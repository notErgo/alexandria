"""Token-budgeted context window selector for LLM extraction.

Selects the best text window(s) from a document for a given metric.
Uses document_chunks when available (chunk path), falls back to
sliding character windows (sliding path) when chunks are absent.
"""
from config import CONTEXT_CHAR_BUDGET, CONTEXT_CHAR_BUDGET_QUARTERLY

_METRIC_KEYWORDS = {
    'production_btc':   ['produced', 'mined', 'mining', 'btc', 'bitcoin', 'production'],
    'hodl_btc':         ['held', 'holdings', 'hodl', 'treasury', 'balance', 'btc'],
    'sold_btc':         ['sold', 'sale', 'proceeds', 'liquidated', 'btc'],
    'hashrate_eh':      ['hashrate', 'hash rate', 'eh/s', 'exahash', 'deployed', 'energized'],
    'realization_rate': ['realized', 'realization', 'revenue', 'price', 'usd'],
}

_DEFAULT_KEYWORDS = ['produced', 'mined', 'btc', 'bitcoin']

_QUARTERLY_SOURCES = {'edgar_10q', 'edgar_10k', 'edgar_6k', 'edgar_20f', 'edgar_40f'}

_MAX_WINDOWS = 3


def _score_chunk(text_lower: str, keywords: list) -> int:
    """Count total keyword occurrences in lowercased chunk text."""
    return sum(text_lower.count(kw) for kw in keywords)


class ContextWindowSelector:
    """Select the best text window(s) from a document for a given metric."""

    def __init__(self, doc_type: str = 'ir_press_release'):
        if doc_type in _QUARTERLY_SOURCES:
            self.char_budget = CONTEXT_CHAR_BUDGET_QUARTERLY
        else:
            self.char_budget = CONTEXT_CHAR_BUDGET

    def select_windows(self, report_id: int, raw_text: str, metric: str, db) -> list:
        """Return up to 3 window dicts, each with keys: text, source, window_index.

        Chunk path: db.get_chunks_for_report returns non-empty list.
        Sliding path: no chunks available — use overlapping character windows.
        """
        chunks = []
        try:
            chunks = db.get_chunks_for_report(report_id) or []
        except Exception:
            chunks = []

        if chunks:
            return self._chunk_windows(chunks, metric)
        return self._sliding_windows(raw_text)

    def _chunk_windows(self, chunks: list, metric: str) -> list:
        """Build windows from document chunks, scored by keyword density."""
        keywords = _METRIC_KEYWORDS.get(metric, _DEFAULT_KEYWORDS)
        budget = self.char_budget

        scored = sorted(
            chunks,
            key=lambda c: _score_chunk(c.get('text', '').lower(), keywords),
            reverse=True,
        )

        windows = []
        used_indices = set()

        # Window 0: greedily accumulate highest-scoring chunks up to budget
        primary_parts = []
        primary_len = 0
        for chunk in scored:
            chunk_text = chunk.get('text', '')
            if primary_len + len(chunk_text) <= budget:
                primary_parts.append(chunk_text)
                primary_len += len(chunk_text)
                used_indices.add(id(chunk))
            else:
                # Add as much of this chunk as fits
                remaining = budget - primary_len
                if remaining > 0:
                    primary_parts.append(chunk_text[:remaining])
                    used_indices.add(id(chunk))
                break

        if primary_parts:
            primary_text = '\n\n'.join(primary_parts)[:budget]
            windows.append({
                'text': primary_text,
                'source': 'chunk',
                'window_index': 0,
            })

        # Fallback windows: remaining chunks not consumed in primary
        remaining_chunks = [c for c in scored if id(c) not in used_indices]
        fallback_parts = []
        fallback_len = 0
        for chunk in remaining_chunks:
            if len(windows) >= _MAX_WINDOWS:
                break
            chunk_text = chunk.get('text', '')
            if fallback_len + len(chunk_text) <= budget:
                fallback_parts.append(chunk_text)
                fallback_len += len(chunk_text)
            else:
                remaining = budget - fallback_len
                if fallback_parts or remaining > 0:
                    if remaining > 0:
                        fallback_parts.append(chunk_text[:remaining])
                    fb_text = '\n\n'.join(fallback_parts)[:budget]
                    windows.append({
                        'text': fb_text,
                        'source': 'chunk',
                        'window_index': len(windows),
                    })
                fallback_parts = []
                fallback_len = 0

        if fallback_parts and len(windows) < _MAX_WINDOWS:
            fb_text = '\n\n'.join(fallback_parts)[:budget]
            windows.append({
                'text': fb_text,
                'source': 'chunk',
                'window_index': len(windows),
            })

        return windows

    def _sliding_windows(self, raw_text: str) -> list:
        """Build overlapping sliding character windows over raw_text."""
        budget = self.char_budget
        text_len = len(raw_text)
        windows = []

        if text_len == 0:
            return []

        # Window 0: start of document
        windows.append({
            'text': raw_text[:budget],
            'source': 'sliding',
            'window_index': 0,
        })

        if text_len <= budget:
            return windows

        # Window 1: 25% overlap with window 0
        overlap_start = budget - budget // 4
        window1_end = overlap_start + budget
        if overlap_start < text_len:
            windows.append({
                'text': raw_text[overlap_start:window1_end],
                'source': 'sliding',
                'window_index': 1,
            })

        if len(windows) >= _MAX_WINDOWS:
            return windows

        # Window 2: next non-overlapping segment after window 1
        window2_start = window1_end
        window2_end = window2_start + budget
        if window2_start < text_len:
            windows.append({
                'text': raw_text[window2_start:window2_end],
                'source': 'sliding',
                'window_index': 2,
            })

        return windows

    def needs_fallback(self, llm_result) -> bool:
        """Return True if the LLM result is absent or low-confidence."""
        if llm_result is None:
            return True
        return llm_result.confidence < 0.5
