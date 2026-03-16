"""T3 — SOURCE_TYPE_DISPLAY covers all known source_type values."""
import pytest


def test_source_type_display_exists():
    from config import SOURCE_TYPE_DISPLAY
    assert isinstance(SOURCE_TYPE_DISPLAY, dict)
    assert len(SOURCE_TYPE_DISPLAY) > 0


def test_source_type_display_known_keys():
    from config import SOURCE_TYPE_DISPLAY
    required = [
        'edgar_8k', 'edgar_10q', 'edgar_10k', 'edgar_20f',
        'ir_press_release', 'archive_html', 'archive_pdf',
    ]
    for key in required:
        assert key in SOURCE_TYPE_DISPLAY, f'SOURCE_TYPE_DISPLAY missing key: {key}'
        assert SOURCE_TYPE_DISPLAY[key], f'SOURCE_TYPE_DISPLAY[{key!r}] is empty'


def test_source_type_display_no_empty_values():
    from config import SOURCE_TYPE_DISPLAY
    for key, val in SOURCE_TYPE_DISPLAY.items():
        assert val, f'SOURCE_TYPE_DISPLAY[{key!r}] is empty string'


def test_source_type_display_edgar_values_start_with_sec():
    from config import SOURCE_TYPE_DISPLAY
    edgar_keys = [k for k in SOURCE_TYPE_DISPLAY if k.startswith('edgar_')]
    for key in edgar_keys:
        assert SOURCE_TYPE_DISPLAY[key].startswith('SEC'), (
            f'Edgar key {key!r} display should start with "SEC", got: {SOURCE_TYPE_DISPLAY[key]!r}'
        )
