"""T3 — metric_schema active filter excludes deprecated metrics."""
import pytest


ACTIVE_METRICS = {'production_btc', 'hodl_btc', 'sold_btc'}
DEPRECATED_METRICS = {
    'hashrate_eh', 'realization_rate', 'ai_hpc_mw', 'gpu_count',
    'hpc_revenue_usd', 'mining_mw', 'encumbered_btc',
    'hodl_btc_restricted', 'hodl_btc_unrestricted', 'net_btc_balance_change',
}


@pytest.fixture
def db(tmp_path):
    from infra.db import MinerDB
    return MinerDB(str(tmp_path / 'test.db'))


def test_active_filter_excludes_deprecated(db):
    """GET /api/metric_schema?active=true excludes deprecated metric keys."""
    rows = db.get_metric_schema('BTC-miners', active_only=True)
    keys = {r['key'] for r in rows}
    for dep in DEPRECATED_METRICS:
        if dep in keys:
            pytest.fail(f'Deprecated metric {dep!r} appears in active_only=True results')


def test_active_filter_includes_core(db):
    """Active metrics (production_btc, hodl_btc, sold_btc) appear when active_only=True."""
    rows = db.get_metric_schema('BTC-miners', active_only=True)
    keys = {r['key'] for r in rows}
    for m in ACTIVE_METRICS:
        assert m in keys, f'Core metric {m!r} missing from active_only results'


def test_inactive_filter_shows_deprecated(db):
    """active_only=False returns all rows including deprecated."""
    all_rows = db.get_metric_schema('BTC-miners', active_only=False)
    all_keys = {r['key'] for r in all_rows}
    # At least one deprecated metric must be present (they exist in DB but inactive)
    found_deprecated = DEPRECATED_METRICS & all_keys
    assert found_deprecated, 'Expected at least one deprecated metric in all rows'


def test_deprecated_rows_have_active_zero(db):
    """Deprecated metric rows have active=0 after v22 migration."""
    all_rows = db.get_metric_schema('BTC-miners', active_only=False)
    by_key = {r['key']: r for r in all_rows}
    for dep in DEPRECATED_METRICS:
        if dep in by_key:
            row = by_key[dep]
            assert row.get('active') in (0, False, None), (
                f'Deprecated metric {dep!r} should have active=0, got: {row.get("active")}'
            )
