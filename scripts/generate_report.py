"""
Completeness report generator for the Bitcoin Miner Data Platform.

Reads all data from the minerdata.db and produces:
  1. Coverage heatmap (companies x months) for production_btc
  2. Data completeness bar chart per company
  3. Metric distribution chart (which metrics exist for which tickers)
  4. Source type breakdown (archive vs IR vs EDGAR)
  5. raw_extractions category breakdown (if broad extraction has been run)
  6. Gap analysis table with recommendations

Usage:
    cd OffChain/miners
    python3 scripts/generate_report.py [--out-dir reports/] [--since 2020-01] [--no-show]
"""
import argparse
import json
import logging
import os
import sqlite3
import sys
from datetime import datetime, date
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / 'src'))

from infra.logging_config import setup_logging
setup_logging()

from config import DATA_DIR, get_all_tickers as _get_all_tickers

log = logging.getLogger('miners.generate_report')

try:
    import matplotlib
    matplotlib.use('Agg')  # non-interactive backend for server-side rendering
    import matplotlib.pyplot as plt
    import matplotlib.colors as mcolors
    import matplotlib.patches as mpatches
    MATPLOTLIB_AVAILABLE = True
except ImportError:
    MATPLOTLIB_AVAILABLE = False
    log.warning("matplotlib not available — charts will be skipped. Install with: pip install matplotlib")

try:
    import pandas as pd
    PANDAS_AVAILABLE = True
except ImportError:
    PANDAS_AVAILABLE = False

# Color palette (matches Hermeneutic design system)
COLOR_PRESENT   = '#22c55e'   # green
COLOR_REVIEW    = '#f97316'   # orange
COLOR_MISSING   = '#374151'   # dark gray
COLOR_INACTIVE  = '#1e1e1e'   # near-black
COLOR_ACCENT    = '#3b82f6'   # blue
COLOR_DANGER    = '#ef4444'   # red

# All tickers in display order — derived from config/companies.json (AP-043)
DISPLAY_ORDER = _get_all_tickers()

STANDARD_METRICS = [
    'production_btc', 'hodl_btc', 'sold_btc', 'hashrate_eh',
    'mining_mw', 'ai_hpc_mw', 'encumbered_btc', 'net_btc_balance_change',
]


def get_db_connection(db_path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn


def fetch_coverage_matrix(conn: sqlite3.Connection, since: str, until: str) -> dict:
    """Return {ticker: {period: value}} for production_btc."""
    rows = conn.execute(
        """SELECT ticker, substr(period,1,7) as period, value
           FROM data_points
           WHERE metric = 'production_btc'
           AND period >= ? AND period <= ?
           ORDER BY ticker, period""",
        (since + '-01', until + '-31'),
    ).fetchall()
    coverage: dict = {}
    for r in rows:
        ticker = r['ticker']
        period = r['period']
        if ticker not in coverage:
            coverage[ticker] = {}
        coverage[ticker][period] = r['value']
    return coverage


def generate_month_range(start: str, end: str) -> list:
    """Generate list of YYYY-MM strings between start and end inclusive."""
    months = []
    y, m = int(start[:4]), int(start[5:7])
    ey, em = int(end[:4]), int(end[5:7])
    while (y, m) <= (ey, em):
        months.append(f"{y:04d}-{m:02d}")
        m += 1
        if m > 12:
            m = 1
            y += 1
    return months


def fetch_report_counts(conn: sqlite3.Connection) -> dict:
    """Return {ticker: {'total': N, 'extracted': N, 'by_source': {source_type: N}}}."""
    rows = conn.execute(
        """SELECT ticker, source_type, COUNT(*) as n,
           SUM(CASE WHEN extracted_at IS NOT NULL THEN 1 ELSE 0 END) as extracted
           FROM reports GROUP BY ticker, source_type"""
    ).fetchall()
    result: dict = {}
    for r in rows:
        t = r['ticker']
        if t not in result:
            result[t] = {'total': 0, 'extracted': 0, 'by_source': {}}
        result[t]['total'] += r['n']
        result[t]['extracted'] += r['extracted']
        result[t]['by_source'][r['source_type']] = r['n']
    return result


def fetch_metric_matrix(conn: sqlite3.Connection) -> dict:
    """Return {ticker: {metric: count}} for all standard metrics."""
    rows = conn.execute(
        """SELECT ticker, metric, COUNT(*) as n FROM data_points
           GROUP BY ticker, metric ORDER BY ticker, metric"""
    ).fetchall()
    result: dict = {}
    for r in rows:
        t = r['ticker']
        if t not in result:
            result[t] = {}
        result[t][r['metric']] = r['n']
    return result


def fetch_raw_extraction_summary(conn: sqlite3.Connection) -> dict:
    """Return category counts from raw_extractions table (if it exists)."""
    try:
        rows = conn.execute(
            """SELECT ticker, category, COUNT(*) as n, COUNT(DISTINCT metric_key) as keys
               FROM raw_extractions GROUP BY ticker, category"""
        ).fetchall()
        result: dict = {}
        for r in rows:
            t = r['ticker']
            if t not in result:
                result[t] = {}
            result[t][r['category']] = {'count': r['n'], 'keys': r['keys']}
        return result
    except sqlite3.OperationalError:
        return {}  # table doesn't exist yet


def compute_completeness_pct(coverage: dict, months: list, ticker: str) -> float:
    """Return fraction of months covered for a ticker."""
    tc = coverage.get(ticker, {})
    if not months:
        return 0.0
    covered = sum(1 for m in months if m in tc)
    return 100.0 * covered / len(months)


def plot_coverage_heatmap(coverage: dict, months: list, tickers: list, out_path: str) -> None:
    if not MATPLOTLIB_AVAILABLE:
        return
    fig_w = max(16, len(months) * 0.22)
    fig_h = max(4, len(tickers) * 0.45)
    fig, ax = plt.subplots(figsize=(fig_w, fig_h))
    fig.patch.set_facecolor('#121212')
    ax.set_facecolor('#1e1e1e')

    for row_idx, ticker in enumerate(tickers):
        tc = coverage.get(ticker, {})
        for col_idx, month in enumerate(months):
            val = tc.get(month)
            if val is not None:
                color = COLOR_PRESENT
                label = f'{val:.0f}'
            else:
                color = COLOR_MISSING
                label = ''
            rect = mpatches.FancyBboxPatch(
                (col_idx + 0.05, row_idx + 0.05), 0.9, 0.9,
                boxstyle='round,pad=0.02',
                facecolor=color, edgecolor='none', linewidth=0,
            )
            ax.add_patch(rect)
            if val is not None and len(months) <= 60:
                ax.text(col_idx + 0.5, row_idx + 0.5, label,
                        ha='center', va='center', fontsize=5.5,
                        color='white', fontweight='bold')

    # Axes
    ax.set_xlim(0, len(months))
    ax.set_ylim(0, len(tickers))
    ax.set_yticks([i + 0.5 for i in range(len(tickers))])
    ax.set_yticklabels(tickers, color='#e8e8e8', fontsize=9)
    ax.set_xticks([i + 0.5 for i in range(0, len(months), max(1, len(months)//24))])
    ax.set_xticklabels(
        [months[i] for i in range(0, len(months), max(1, len(months)//24))],
        color='#a0a0a0', fontsize=7, rotation=45, ha='right',
    )
    ax.tick_params(left=False, bottom=False)
    for spine in ax.spines.values():
        spine.set_visible(False)

    ax.set_title('Bitcoin Production Coverage by Company (green = data, gray = gap)',
                 color='#e8e8e8', fontsize=12, pad=12)

    legend_patches = [
        mpatches.Patch(color=COLOR_PRESENT, label='Data present'),
        mpatches.Patch(color=COLOR_MISSING,  label='Gap / no data'),
    ]
    ax.legend(handles=legend_patches, loc='lower left', bbox_to_anchor=(0, -0.15),
              ncol=2, facecolor='#1e1e1e', edgecolor='none', labelcolor='#e8e8e8', fontsize=9)

    fig.tight_layout(rect=[0, 0.05, 1, 1])
    fig.savefig(out_path, dpi=150, bbox_inches='tight', facecolor='#121212')
    plt.close(fig)
    log.info("Saved coverage heatmap: %s", out_path)


def plot_completeness_bar(completeness: dict, tickers: list, months: list, out_path: str) -> None:
    if not MATPLOTLIB_AVAILABLE:
        return
    fig, ax = plt.subplots(figsize=(max(10, len(tickers) * 0.7), 5))
    fig.patch.set_facecolor('#121212')
    ax.set_facecolor('#1e1e1e')

    pcts = [completeness.get(t, 0.0) for t in tickers]
    colors = [COLOR_PRESENT if p >= 80 else COLOR_REVIEW if p >= 40 else COLOR_DANGER for p in pcts]
    bars = ax.bar(range(len(tickers)), pcts, color=colors, width=0.6)

    for bar, pct in zip(bars, pcts):
        ax.text(bar.get_x() + bar.get_width() / 2,
                bar.get_height() + 1.5,
                f'{pct:.0f}%', ha='center', va='bottom',
                color='#e8e8e8', fontsize=8, fontweight='bold')

    ax.set_xticks(range(len(tickers)))
    ax.set_xticklabels(tickers, color='#e8e8e8', fontsize=10)
    ax.set_ylim(0, 115)
    ax.set_ylabel('Completeness %', color='#a0a0a0')
    ax.set_title(f'Data Completeness: production_btc ({months[0]} — {months[-1]})',
                 color='#e8e8e8', fontsize=12, pad=10)
    ax.tick_params(colors='#a0a0a0')
    for spine in ax.spines.values():
        spine.set_color('#374151')
    ax.set_facecolor('#1e1e1e')
    ax.axhline(80, color=COLOR_PRESENT, linestyle='--', linewidth=0.8, alpha=0.5, label='80% threshold')
    ax.axhline(40, color=COLOR_REVIEW, linestyle='--', linewidth=0.8, alpha=0.5, label='40% threshold')
    ax.legend(facecolor='#1e1e1e', edgecolor='none', labelcolor='#e8e8e8', fontsize=8)

    fig.tight_layout()
    fig.savefig(out_path, dpi=150, bbox_inches='tight', facecolor='#121212')
    plt.close(fig)
    log.info("Saved completeness bar chart: %s", out_path)


def plot_metric_matrix(metric_matrix: dict, tickers: list, out_path: str) -> None:
    if not MATPLOTLIB_AVAILABLE:
        return
    all_metrics = STANDARD_METRICS + sorted(
        m for m in
        set(m for t in metric_matrix.values() for m in t.keys())
        if m not in STANDARD_METRICS
    )

    fig_w = max(12, len(all_metrics) * 0.9)
    fig_h = max(4, len(tickers) * 0.5)
    fig, ax = plt.subplots(figsize=(fig_w, fig_h))
    fig.patch.set_facecolor('#121212')
    ax.set_facecolor('#1e1e1e')

    for row_idx, ticker in enumerate(tickers):
        tm = metric_matrix.get(ticker, {})
        for col_idx, metric in enumerate(all_metrics):
            count = tm.get(metric, 0)
            color = COLOR_PRESENT if count > 0 else COLOR_MISSING
            rect = mpatches.FancyBboxPatch(
                (col_idx + 0.05, row_idx + 0.05), 0.9, 0.9,
                boxstyle='round,pad=0.02', facecolor=color,
                edgecolor='none', linewidth=0,
            )
            ax.add_patch(rect)
            if count > 0:
                ax.text(col_idx + 0.5, row_idx + 0.5, str(count),
                        ha='center', va='center', fontsize=7, color='white')

    ax.set_xlim(0, len(all_metrics))
    ax.set_ylim(0, len(tickers))
    ax.set_yticks([i + 0.5 for i in range(len(tickers))])
    ax.set_yticklabels(tickers, color='#e8e8e8', fontsize=9)
    ax.set_xticks([i + 0.5 for i in range(len(all_metrics))])
    ax.set_xticklabels(
        [m.replace('_', '\n') for m in all_metrics],
        color='#a0a0a0', fontsize=7, rotation=0, ha='center',
    )
    ax.tick_params(left=False, bottom=False)
    for spine in ax.spines.values():
        spine.set_visible(False)
    ax.set_title('Metric Coverage Matrix (number = data points; dark = no data)',
                 color='#e8e8e8', fontsize=12, pad=12)

    fig.tight_layout()
    fig.savefig(out_path, dpi=150, bbox_inches='tight', facecolor='#121212')
    plt.close(fig)
    log.info("Saved metric matrix: %s", out_path)


def plot_source_breakdown(report_counts: dict, tickers: list, out_path: str) -> None:
    if not MATPLOTLIB_AVAILABLE:
        return
    source_types = ['archive_html', 'archive_pdf', 'ir_press_release', 'edgar_10q', 'edgar_10k', 'edgar_8k']
    source_colors = [COLOR_ACCENT, '#60a5fa', COLOR_PRESENT, '#f97316', '#fb923c', '#fbbf24']

    totals = {t: report_counts.get(t, {}).get('total', 0) for t in tickers}
    display_tickers = [t for t in tickers if totals.get(t, 0) > 0]

    if not display_tickers:
        return

    fig, ax = plt.subplots(figsize=(max(10, len(display_tickers) * 0.8), 5))
    fig.patch.set_facecolor('#121212')
    ax.set_facecolor('#1e1e1e')

    bottoms = [0.0] * len(display_tickers)
    for stype, color in zip(source_types, source_colors):
        vals = [report_counts.get(t, {}).get('by_source', {}).get(stype, 0) for t in display_tickers]
        if any(vals):
            ax.bar(range(len(display_tickers)), vals, bottom=bottoms,
                   color=color, label=stype, width=0.6)
            bottoms = [b + v for b, v in zip(bottoms, vals)]

    ax.set_xticks(range(len(display_tickers)))
    ax.set_xticklabels(display_tickers, color='#e8e8e8', fontsize=10)
    ax.set_ylabel('Report Count', color='#a0a0a0')
    ax.tick_params(colors='#a0a0a0')
    for spine in ax.spines.values():
        spine.set_color('#374151')
    ax.set_title('Reports by Source Type per Company', color='#e8e8e8', fontsize=12, pad=10)
    ax.legend(facecolor='#1e1e1e', edgecolor='none', labelcolor='#e8e8e8', fontsize=8, loc='upper right')

    fig.tight_layout()
    fig.savefig(out_path, dpi=150, bbox_inches='tight', facecolor='#121212')
    plt.close(fig)
    log.info("Saved source breakdown: %s", out_path)


def build_gap_analysis(coverage: dict, months: list, metric_matrix: dict, report_counts: dict) -> list:
    """Return list of gap records with recommendations."""
    gaps = []
    for ticker in DISPLAY_ORDER:
        tc = coverage.get(ticker, {})
        tm = metric_matrix.get(ticker, {})
        rc = report_counts.get(ticker, {})
        total_reports = rc.get('total', 0)
        edgar_reports = sum(v for k, v in rc.get('by_source', {}).items() if 'edgar' in k)
        prod_months = sum(1 for m in months if m in tc)
        missing_months = [m for m in months if m not in tc]
        pct = 100.0 * prod_months / len(months) if months else 0.0

        # Recommendations
        recs = []
        if prod_months == 0:
            recs.append("No production data at all — run IR scrape and EDGAR fetch")
        elif pct < 50:
            recs.append(f"Only {pct:.0f}% coverage — check archive for older files")
        if edgar_reports == 0:
            recs.append("No EDGAR filings ingested — run run_edgar_all.py for this ticker")
        if 'hodl_btc' not in tm and prod_months > 0:
            recs.append("Missing hodl_btc — check press releases for treasury data")
        if 'hashrate_eh' not in tm and prod_months > 0:
            recs.append("Missing hashrate_eh — check press releases for operational data")
        if missing_months and len(missing_months) <= 24:
            recs.append(f"Missing periods: {', '.join(missing_months[:12])}"
                        + (" ..." if len(missing_months) > 12 else ""))

        gaps.append({
            'ticker': ticker,
            'total_reports': total_reports,
            'edgar_reports': edgar_reports,
            'production_months': prod_months,
            'missing_months': len(missing_months),
            'completeness_pct': round(pct, 1),
            'metrics_present': sorted(tm.keys()),
            'recommendations': recs,
        })

    return sorted(gaps, key=lambda g: g['completeness_pct'])


def print_gap_report(gaps: list, months: list) -> None:
    print(f"\n{'='*80}")
    print(f"DATA COMPLETENESS REPORT")
    print(f"Period: {months[0]} to {months[-1]} ({len(months)} months)")
    print(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M UTC')}")
    print(f"{'='*80}\n")

    print(f"{'Ticker':<6} {'Reports':>7} {'EDGAR':>5} {'Prod Mo':>7} {'Missing':>7} {'Pct':>6}  Metrics")
    print(f"{'-'*6} {'-'*7} {'-'*5} {'-'*7} {'-'*7} {'-'*6}  {'-'*20}")
    for g in sorted(gaps, key=lambda x: -x['completeness_pct']):
        metrics_short = len(g['metrics_present'])
        print(f"{g['ticker']:<6} {g['total_reports']:>7} {g['edgar_reports']:>5} "
              f"{g['production_months']:>7} {g['missing_months']:>7} {g['completeness_pct']:>5.1f}%  "
              f"{metrics_short} metrics")

    print(f"\n{'='*80}")
    print(f"GAPS AND RECOMMENDATIONS (worst coverage first):")
    print(f"{'='*80}")
    for g in gaps:
        if g['recommendations']:
            print(f"\n{g['ticker']} ({g['completeness_pct']:.0f}% complete):")
            for rec in g['recommendations']:
                print(f"  - {rec}")


def main():
    parser = argparse.ArgumentParser(description='Generate miner data completeness report')
    parser.add_argument('--out-dir', default='reports', help='Output directory for charts')
    parser.add_argument('--since', default='2020-01', metavar='YYYY-MM',
                        help='Start of coverage window (default: 2020-01)')
    parser.add_argument('--until', default=None, metavar='YYYY-MM',
                        help='End of coverage window (default: current month)')
    parser.add_argument('--no-show', action='store_true', help='Do not attempt to display charts')
    args = parser.parse_args()

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    until = args.until or datetime.now().strftime('%Y-%m')
    months = generate_month_range(args.since, until)

    db_path = str(Path(DATA_DIR) / 'minerdata.db')
    conn = get_db_connection(db_path)

    log.info("Loading data from %s", db_path)
    coverage     = fetch_coverage_matrix(conn, args.since, until)
    report_counts = fetch_report_counts(conn)
    metric_matrix = fetch_metric_matrix(conn)
    raw_summary   = fetch_raw_extraction_summary(conn)
    conn.close()

    # Determine which tickers to show (those in display order + any others)
    all_tickers = sorted(set(list(coverage.keys()) + list(report_counts.keys())))
    tickers = [t for t in DISPLAY_ORDER if t in all_tickers] + \
              [t for t in all_tickers if t not in DISPLAY_ORDER]

    # Completeness per ticker
    completeness = {t: compute_completeness_pct(coverage, months, t) for t in tickers}

    # Gap analysis
    gaps = build_gap_analysis(coverage, months, metric_matrix, report_counts)

    # Print text report
    print_gap_report(gaps, months)

    if MATPLOTLIB_AVAILABLE:
        log.info("Generating charts in %s ...", out_dir)

        # 1. Coverage heatmap
        plot_coverage_heatmap(
            coverage, months, tickers,
            str(out_dir / '01_coverage_heatmap.png'),
        )
        # 2. Completeness bar
        plot_completeness_bar(
            completeness, tickers, months,
            str(out_dir / '02_completeness_bar.png'),
        )
        # 3. Metric matrix
        plot_metric_matrix(
            metric_matrix, tickers,
            str(out_dir / '03_metric_matrix.png'),
        )
        # 4. Source breakdown
        plot_source_breakdown(
            report_counts, tickers,
            str(out_dir / '04_source_breakdown.png'),
        )

        print(f"\nCharts saved to {out_dir}/")
        for f in sorted(out_dir.glob('*.png')):
            print(f"  {f.name}")
    else:
        print("\nWARNING: matplotlib not installed — no charts generated.")
        print("Install with: pip install matplotlib pandas")

    # Write JSON summary for coordinator
    summary_path = Path('/private/tmp/claude-501/miners_progress/report_summary.json')
    with open(summary_path, 'w') as f:
        json.dump({
            'generated_at': datetime.now().isoformat(),
            'period': {'since': args.since, 'until': until, 'months': len(months)},
            'completeness': completeness,
            'gaps': gaps,
            'raw_extraction_summary': raw_summary,
        }, f, indent=2, default=str)
    log.info("JSON summary written to %s", summary_path)


if __name__ == '__main__':
    main()
