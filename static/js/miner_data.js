// miner_data.js — Analyst timeline view. Extracted from miner_data.html.
// Boot is deferred: call boot() on first activation of Review > Review sub-tab.
let _minerDataBooted = false;

// ── State ─────────────────────────────────────────────────────────────────
let _ticker = null;               // currently selected company ticker
let _rows = [];                   // full unfiltered row list from API
let _selectedPeriod = null;       // period of the currently open doc panel
let _currentDocText = '';         // raw text for pattern generator (set when doc is loaded)

// Metric highlight colours — 5 known colours; extras assigned from a palette.
const _METRIC_COLORS_KNOWN = {
  production_btc:   '#3b82f6',
  holdings_btc:     '#8b5cf6',
  sales_btc:        '#f59e0b',
  hashrate_eh:      '#10b981',
  realization_rate: '#f97316',
};
const _METRIC_PALETTE = [
  '#06b6d4', '#ec4899', '#84cc16', '#a78bfa', '#fb923c',
  '#14b8a6', '#f43f5e', '#facc15', '#60a5fa', '#34d399',
];
let METRIC_COLORS = Object.assign({}, _METRIC_COLORS_KNOWN);

function _assignMetricColor(key) {
  if (!METRIC_COLORS[key]) {
    const idx = Object.keys(METRIC_COLORS).length % _METRIC_PALETTE.length;
    METRIC_COLORS[key] = _METRIC_PALETTE[idx];
  }
  return METRIC_COLORS[key];
}

// ── Boot ──────────────────────────────────────────────────────────────────

// Event delegation for row clicks — wired once at page load (anti-pattern #26 compliance).
// Placing inside renderTable() would accumulate a listener on every filter change.
document.addEventListener('DOMContentLoaded', function() {
  document.getElementById('timeline-tbody').addEventListener('click', function(e) {
    const row = e.target.closest('tr[data-period]');
    if (!row) return;
    selectPeriod(row.getAttribute('data-period'));
  });

  // ── ReviewPanel init ────────────────────────────────────────────────────
  ReviewPanel.init('miner-review-panel');
  ReviewPanel.setOnFilled(function(e) {
    showToast('Submitted to review queue');
    selectCompany(_ticker);
  });
  ReviewPanel.setOnWritten(function(e) {
    showToast('Written to timeline: ' + e.metric + ' = ' + e.value);
    selectCompany(_ticker);
  });
  ReviewPanel.setOnApproved(function(e) {
    showToast('Approved');
    selectCompany(_ticker);
  });
  ReviewPanel.setOnRejected(function(e) {
    showToast('Rejected');
  });

  // ── Right-click context menu ────────────────────────────────────────────
  // "Add as Rule" option removed — pattern generator disabled in LLM-only mode.

  // ── Column visibility init ──────────────────────────────────────────────
  // Metric checkboxes are built dynamically by buildTableHeaders(), so we use
  // event delegation on #col-vis-menu (wired once here) rather than per-checkbox
  // listeners. localStorage restore happens in buildTableHeaders() too.
  document.getElementById('col-vis-menu').addEventListener('change', function(e) {
    if (!e.target.classList.contains('col-vis-cb')) return;
    const state = {};
    document.querySelectorAll('.col-vis-cb').forEach(function(c) {
      state[c.getAttribute('data-col')] = c.checked;
    });
    localStorage.setItem(_COL_VIS_KEY, JSON.stringify(state));
    applyColVisibility();
  });

  // Close dropdown when clicking outside
  document.addEventListener('click', function(e) {
    if (!document.getElementById('col-vis-wrap').contains(e.target)) {
      const menu = document.getElementById('col-vis-menu');
      if (menu) menu.style.display = 'none';
    }
  });

  // Manual fill is now handled by ReviewPanel.setOnFilled (wired above).
});

// ── Coverage summary ───────────────────────────────────────────────────────
async function loadCoverageSummary(ticker) {
  const bar = document.getElementById('coverage-summary-bar');
  if (!bar) return;
  bar.innerHTML = '<span style="color:var(--theme-text-muted)">Loading...</span>';
  try {
    const resp = await fetch(`/api/miner/${encodeURIComponent(ticker)}/coverage_summary`);
    if (!resp.ok) { bar.innerHTML = ''; return; }
    const body = await resp.json();
    if (!body.success) { bar.innerHTML = ''; return; }
    const d = body.data;
    const rows = [
      ['Monthly (PR/IR)', d.monthly.total_reports, d.monthly.extracted, d.monthly.earliest || '—', d.monthly.latest || '—'],
      ['10-Q / 10-K', (d.sec.by_source['edgar_10q'] || 0) + (d.sec.by_source['edgar_10k'] || 0),
        null, null, null],
      ['8-K', d.sec.by_source['edgar_8k'] || 0, null, null, null],
    ];
    // For SEC row, aggregate
    rows[1][2] = d.sec.extracted;
    rows[1][3] = d.sec.earliest || '—';
    rows[1][4] = d.sec.latest || '—';
    rows[2][2] = '—';
    rows[2][3] = '—';
    rows[2][4] = '—';

    bar.innerHTML = `<table>
      <thead><tr><th>Source</th><th>Count</th><th>Extracted</th><th>Earliest</th><th>Latest</th></tr></thead>
      <tbody>
        ${rows.map(function(r) {
          return `<tr><td>${escapeHtml(String(r[0]))}</td><td>${escapeHtml(String(r[1]))}</td><td>${escapeHtml(String(r[2]))}</td><td>${escapeHtml(String(r[3]))}</td><td>${escapeHtml(String(r[4]))}</td></tr>`;
        }).join('')}
      </tbody>
    </table>`;
  } catch (e) {
    bar.innerHTML = '';
  }
}

// ── XBRL preamble strip ────────────────────────────────────────────────────
function stripXbrlPreamble(text) {
  if (!text) return text;
  const idx = text.search(/UNITED\s+STATES/i);
  if (idx > 200 && /^\d{9,10}\s/.test(text)) {
    return text.slice(idx);
  }
  return text;
}

// ── Reviewed period checkboxes ─────────────────────────────────────────────
async function onReviewedChange(cb) {
  if (!_ticker) return;
  const period = cb.getAttribute('data-period');
  const tr = cb.closest('tr');
  try {
    if (cb.checked) {
      const resp = await fetch(`/api/interpret/${encodeURIComponent(_ticker)}/reviewed`, {
        method: 'POST',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify({periods: [period]}),
      });
      if (resp.ok) {
        if (tr) tr.classList.add('is-reviewed');
      } else {
        cb.checked = false;
      }
    } else {
      const resp = await fetch(`/api/interpret/${encodeURIComponent(_ticker)}/reviewed`, {
        method: 'DELETE',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify({period: period}),
      });
      if (resp.ok) {
        if (tr) tr.classList.remove('is-reviewed');
      } else {
        cb.checked = true;
      }
    }
  } catch (e) {
    // revert on error
    cb.checked = !cb.checked;
  }
}

async function toggleMarkAllReviewed(checked) {
  if (!_ticker) return;
  const cbs = document.querySelectorAll('.period-reviewed-cb');
  if (checked) {
    const periods = Array.from(cbs).map(function(cb) { return cb.getAttribute('data-period'); });
    try {
      const resp = await fetch(`/api/interpret/${encodeURIComponent(_ticker)}/reviewed`, {
        method: 'POST',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify({periods: periods}),
      });
      if (resp.ok) {
        cbs.forEach(function(cb) {
          cb.checked = true;
          const tr = cb.closest('tr');
          if (tr) tr.classList.add('is-reviewed');
        });
      }
    } catch (e) {}
  } else {
    try {
      const resp = await fetch(`/api/interpret/${encodeURIComponent(_ticker)}/reviewed/all`, {
        method: 'DELETE',
      });
      if (resp.ok) {
        cbs.forEach(function(cb) {
          cb.checked = false;
          const tr = cb.closest('tr');
          if (tr) tr.classList.remove('is-reviewed');
        });
      }
    } catch (e) {}
  }
}

// ── Inline cell editing ────────────────────────────────────────────────────
let _editingCell = null;

function inlineEditCell(event, period, metric) {
  event.stopPropagation();
  if (_editingCell) return; // one edit at a time
  const td = event.currentTarget;
  const row = _rows.find(function(r) { return r.period === period; });
  const m = row && row.metrics && row.metrics[metric];
  const currentVal = m ? m.value : null;

  _editingCell = {td: td, period: period, metric: metric, original: td.innerHTML};
  td.innerHTML = `<input type="number" step="any" style="width:80px;font-size:0.82rem;background:var(--theme-bg-input);border:1px solid var(--theme-accent);border-radius:3px;color:var(--theme-text-primary);padding:1px 3px" value="${currentVal != null ? currentVal : ''}" id="inline-edit-input">` +
    `<button style="margin-left:2px;font-size:0.7rem;padding:1px 4px;background:var(--theme-accent);color:#fff;border:none;border-radius:2px;cursor:pointer" onclick="confirmInlineEdit(event,'${escapeHtml(period)}','${escapeHtml(metric)}')">OK</button>`;
  const input = td.querySelector('#inline-edit-input');
  if (input) { input.focus(); input.select(); }

  input.addEventListener('keydown', function(e) {
    if (e.key === 'Enter') confirmInlineEdit(e, period, metric);
    if (e.key === 'Escape') cancelInlineEdit();
  });
}

async function confirmInlineEdit(event, period, metric) {
  event.stopPropagation();
  if (!_editingCell) return;
  const td = _editingCell.td;
  const input = td.querySelector('input');
  const val = input ? parseFloat(input.value) : NaN;
  if (isNaN(val) || val < 0) { cancelInlineEdit(); return; }

  const original = _editingCell.original;
  _editingCell = null;

  try {
    const resp = await fetch(`/api/interpret/${encodeURIComponent(_ticker)}/finalize`, {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({values: [{period: period, metric: metric, value: val}]}),
    });
    const body = await resp.json();
    if (body.success) {
      // Update _rows cache
      const row = _rows.find(function(r) { return r.period === period; });
      if (row) {
        if (!row.metrics[metric]) row.metrics[metric] = {};
        row.metrics[metric].value = val;
        row.metrics[metric].is_finalized = true;
      }
      // Re-render just this cell
      const formatted = val.toLocaleString(undefined, {minimumFractionDigits: 1, maximumFractionDigits: 1});
      td.innerHTML = `${escapeHtml(formatted)}<span class="badge-final">F</span>`;
      showToast('Value finalized');
      // Reload finalized list
      if (_currentView === 'interpret') loadInterpretData(_ticker);
    } else {
      td.innerHTML = original;
      showToast((body.error && body.error.message) || 'Finalize failed', true);
    }
  } catch (e) {
    td.innerHTML = original;
    showToast('Finalize failed', true);
  }
}

function cancelInlineEdit() {
  if (!_editingCell) return;
  _editingCell.td.innerHTML = _editingCell.original;
  _editingCell = null;
}

async function boot() {
  // Load metric schema from SSOT — populates #pattern-metric and METRICS_ORDER fallback
  try {
    const mresp = await fetch('/api/metric_schema?sector=BTC-miners');
    if (mresp.ok) {
      const mbody = await mresp.json();
      if (mbody.success && mbody.data && mbody.data.metrics) {
        const metrics = mbody.data.metrics;
        METRICS_ORDER = metrics.map(function(m) { return m.key; });
        metrics.forEach(function(m) { _assignMetricColor(m.key); });
        const sel = document.getElementById('pattern-metric');
        sel.innerHTML = '';
        const repromptSel = document.getElementById('reprompt-metrics-filter');
        if (repromptSel) repromptSel.innerHTML = '';
        metrics.forEach(function(m) {
          const opt = document.createElement('option');
          opt.value = m.key;
          opt.textContent = m.label || m.key;
          sel.appendChild(opt);
          if (repromptSel) {
            const opt2 = document.createElement('option');
            opt2.value = m.key;
            opt2.textContent = m.label || m.key;
            repromptSel.appendChild(opt2);
          }
        });
      }
    }
  } catch (e) {
    // Non-fatal: pattern-metric retains Loading placeholder
  }

  // Load companies to build tab strip
  try {
    const resp = await fetch('/api/companies');
    if (!resp.ok) throw new Error(`HTTP ${resp.status}`);
    const body = await resp.json();
    if (!body.success) throw new Error(body.error?.message || 'Failed');
    renderCompanyTabs(body.data);

    // Restore last-selected ticker from localStorage
    const saved = localStorage.getItem('miner-data-ticker');
    const tickers = body.data.map(c => c.ticker);
    const initial = (saved && tickers.includes(saved)) ? saved : (tickers[0] || null);
    if (initial) selectCompany(initial);
  } catch (err) {
    document.getElementById('company-tabs').innerHTML =
      `<span style="color:var(--theme-danger);font-size:0.82rem">Failed to load companies: ${escapeHtml(err.message)}</span>`;
  }
}

function renderCompanyTabs(companies) {
  // Only show companies that have data (tier 1 archivable set)
  const tabs = document.getElementById('company-tabs');
  tabs.innerHTML = '';
  for (const c of companies) {
    const btn = document.createElement('button');
    btn.className = 'md-tab';
    btn.textContent = c.ticker;
    btn.setAttribute('data-ticker', c.ticker);
    btn.addEventListener('click', function() { selectCompany(c.ticker); });
    tabs.appendChild(btn);
  }
}

// ── Select company ────────────────────────────────────────────────────────
async function selectCompany(ticker) {
  _ticker = ticker;
  localStorage.setItem('miner-data-ticker', ticker);
  closeDocPanel();

  // Reset cached SEC + finalized data on company switch
  _secRows = [];
  _finalizedValues = [];
  _stagedValues = [];

  // Highlight active tab
  document.querySelectorAll('.md-tab').forEach(function(btn) {
    btn.classList.toggle('active', btn.getAttribute('data-ticker') === ticker);
  });

  // Show loading state (colspan 20 — actual column count not yet known)
  document.getElementById('timeline-tbody').innerHTML =
    `<tr><td colspan="20" class="md-table-empty"><span class="spin"></span> Loading…</td></tr>`;
  document.getElementById('company-info').innerHTML =
    `<div class="md-info-placeholder"><span class="spin"></span> Loading…</div>`;

  try {
    const resp = await fetch(`/api/miner/${encodeURIComponent(ticker)}/timeline?source=monthly`);
    if (!resp.ok) {
      const err = await resp.json().catch(() => ({}));
      throw new Error(err.error?.message || `HTTP ${resp.status}`);
    }
    const body = await resp.json();
    if (!body.success) throw new Error(body.error?.message || 'Failed');

    const data = body.data;
    _rows = data.rows;

    // Update dynamic metric list and rebuild table headers + col-vis checkboxes
    // Fall back to METRICS_ORDER populated by boot() (from /api/metric_schema) if not in response.
    METRICS_ORDER = data.metric_keys || METRICS_ORDER;
    buildTableHeaders(METRICS_ORDER, data.metric_labels || {}, data.metric_units || {});

    renderInfoCard(data.company, data.stats);
    loadCoverageSummary(ticker);
    renderTable(_rows);
    makeSortable('timeline-table');

    // If another view is active, also refresh it
    if (_currentView === 'sec') loadSecData(ticker);
    if (_currentView === 'interpret') {
      await loadSecData(ticker);
      loadInterpretData(ticker);
    }

  } catch (err) {
    document.getElementById('timeline-tbody').innerHTML =
      `<tr><td colspan="20" class="md-table-empty" style="color:var(--theme-danger)">Error: ${escapeHtml(err.message)}</td></tr>`;
    document.getElementById('company-info').innerHTML =
      `<div class="md-info-placeholder" style="color:var(--theme-danger)">Load failed.</div>`;
  }
}

// ── Company info card ─────────────────────────────────────────────────────
function renderInfoCard(company, stats) {
  const el = document.getElementById('company-info');
  const irLink = company.ir_url
    ? `<a href="${escapeHtml(company.ir_url)}" target="_blank" rel="noopener">${escapeHtml(company.ir_url)}</a>`
    : '—';
  const prLink = company.pr_base_url
    ? `<a href="${escapeHtml(company.pr_base_url)}" target="_blank" rel="noopener">${escapeHtml(company.pr_base_url)}</a>`
    : '—';

  el.innerHTML = `
    <div class="md-info-name">${escapeHtml(company.name)}</div>
    <div class="md-info-row">
      <span class="md-info-label">Ticker</span>
      <span class="md-info-value">${escapeHtml(company.ticker)}</span>
    </div>
    <div class="md-info-row">
      <span class="md-info-label">CIK</span>
      <span class="md-info-value">${escapeHtml(company.cik || '—')}</span>
    </div>
    <div class="md-info-row">
      <span class="md-info-label">IR URL</span>
      <span class="md-info-value">${irLink}</span>
    </div>
    <div class="md-info-row">
      <span class="md-info-label">PR Base</span>
      <span class="md-info-value">${prLink}</span>
    </div>
    ${stats.total_periods > 0 ? `
    <div class="md-info-row">
      <span class="md-info-label">Data range</span>
      <span class="md-info-value">${escapeHtml(stats.first_period)} → ${escapeHtml(stats.last_period)}</span>
    </div>
    <div class="md-info-row">
      <span class="md-info-label">Periods</span>
      <span class="md-info-value">${stats.total_periods} months</span>
    </div>
    <div class="md-info-row">
      <span class="md-info-label">Gaps</span>
      <span class="md-info-value" style="color:${stats.gap_periods > 0 ? 'var(--theme-warning)' : 'var(--theme-success)'}">
        ${stats.gap_periods}
      </span>
    </div>` : `<div class="md-info-placeholder">No data points ingested.</div>`}
  `;
}

// ── Filter helpers ─────────────────────────────────────────────────────────
function applyFilters(rows) {
  const gapsOnly = document.getElementById('filter-gaps-only').checked;
  const incompleteOnly = document.getElementById('filter-show-incomplete').checked;
  const fromVal = document.getElementById('filter-from').value;  // YYYY-MM
  const toVal = document.getElementById('filter-to').value;       // YYYY-MM

  return rows.filter(function(row) {
    if (gapsOnly && !row.is_gap) return false;
    if (incompleteOnly) {
      // incomplete = has at least one metric null but not all (i.e. not a full gap)
      const vals = Object.values(row.metrics);
      const nullCount = vals.filter(v => v === null).length;
      if (nullCount === 0 || nullCount === vals.length) return false;
    }
    if (fromVal && row.period_label < fromVal) return false;
    if (toVal && row.period_label > toVal) return false;
    return true;
  });
}

// ── Render table ──────────────────────────────────────────────────────────
// METRICS_ORDER is populated from GET /api/metric_schema in boot(), then updated by
// buildTableHeaders() when company data arrives. Never hardcoded here.
let METRICS_ORDER = [];
let _metricLabelsCache = {};

function fmtValue(m) {
  // m is a metric cell object or null
  if (!m || m.value == null) return null;
  const v = m.value;
  if (m.unit === '%') return v.toFixed(2) + '%';
  if (m.unit === 'EH/s') return v.toFixed(2) + ' EH/s';
  return v.toLocaleString(undefined, {minimumFractionDigits: 1, maximumFractionDigits: 1});
}

function renderTable(allRows) {
  const rows = applyFilters(allRows);
  const tbody = document.getElementById('timeline-tbody');

  document.getElementById('table-row-count').textContent =
    `${rows.length} row${rows.length === 1 ? '' : 's'}`;

  if (rows.length === 0) {
    tbody.innerHTML =
      `<tr><td colspan="${METRICS_ORDER.length + 2}" class="md-table-empty">No rows match current filters.</td></tr>`;
    return;
  }

  const parts = [];
  for (const row of rows) {
    const isGap = row.is_gap;
    const rowClass = isGap ? 'row-gap' : '';
    const isSelected = row.period === _selectedPeriod;
    const selClass = isSelected ? ' selected' : '';

    // Build metric cells
    const metricCells = METRICS_ORDER.map(function(metric) {
      const m = row.metrics[metric];
      if (isGap) {
        return `<td class="td-gap-label">—</td>`;
      }
      if (m && m.value != null) {
        const formatted = escapeHtml(fmtValue(m) || '');
        if (m.is_pending) {
          return `<td class="td-pending">${formatted}<span class="badge-pending">P</span></td>`;
        }
        // Method badge: R=regex, L=LLM, A=analyst, F=finalized, D=inferred_delta, S=inferred_snapshot, P=inferred_prorated
        let methodChar = '';
        if (m.is_finalized) methodChar = 'F';
        else if (m.extraction_method === 'analyst' || m.extraction_method === 'analyst_approved') methodChar = 'A';
        else if (m.extraction_method === 'manual') methodChar = 'M';
        else if (m.extraction_method && m.extraction_method.startsWith('llm')) methodChar = 'L';
        else if (m.extraction_method === 'regex') methodChar = 'R';
        else if (m.extraction_method === 'inferred_delta') methodChar = 'D';
        else if (m.extraction_method === 'inferred_snapshot') methodChar = 'S';
        else if (m.extraction_method === 'inferred_prorated') methodChar = 'P';
        const badge = methodChar
          ? `<span class="badge-method badge-method-${methodChar.toLowerCase()}">${methodChar}</span>`
          : '';
        const finalBadge = m.is_finalized ? `<span class="badge-final">F</span>` : '';
        // Build tooltip: prefer inference_notes for inferred rows, else source_snippet.
        let tooltipText = '';
        if (m.extraction_method && m.extraction_method.startsWith('inferred_') && m.inference_notes) {
          try {
            const notes = typeof m.inference_notes === 'string' ? JSON.parse(m.inference_notes) : m.inference_notes;
            const parts = [`method: ${notes.method || m.extraction_method}`];
            if (notes.quarterly_period) parts.push(`from: ${notes.quarterly_period}`);
            if (notes.quarterly_value != null) parts.push(`q_total: ${notes.quarterly_value}`);
            if (notes.computed_value != null) parts.push(`computed: ${notes.computed_value}`);
            tooltipText = parts.join(' | ');
          } catch (e) {
            tooltipText = m.inference_notes;
          }
        } else {
          tooltipText = m.source_snippet ? m.source_snippet.slice(0, 200) : '';
        }
        const tooltip = escapeHtml(tooltipText);
        return `<td class="td-value" title="${tooltip}" ondblclick="inlineEditCell(event,'${escapeHtml(row.period)}','${escapeHtml(metric)}')">${formatted}${m.is_finalized ? finalBadge : badge}</td>`;
      }
      // No value — check if has report (could fill)
      if (row.has_report) {
        return `<td class="td-empty" ondblclick="inlineEditCell(event,'${escapeHtml(row.period)}','${escapeHtml(metric)}')">—</td>`;
      }
      return `<td class="td-nodoc">—</td>`;
    });

    // Type and Date columns
    const typeLabel = srcTypeLabel(row.source_type);
    const altDocs = row.alt_docs || [];
    const altTip = altDocs.length > 0
      ? escapeHtml(row.source_type + ' (priority ' + (row.doc_priority || '?') + '/3 — selected)\n'
          + 'Also available:\n'
          + altDocs.map(function(a) { return '  ' + a.source_type + ' ' + (a.report_date || '') + ' (p' + a.priority + '/3)'; }).join('\n')
          + '\nClick row to view — use doc switcher to compare.')
      : escapeHtml(row.source_type || '');
    const altBadge = altDocs.length > 0
      ? `<span class="badge-alt-docs" title="${altTip}">+${altDocs.length}</span>`
      : '';
    const typeCell = `<td class="td-type-col" title="${altTip}">${escapeHtml(typeLabel)}${altBadge}</td>`;
    const dateVal = row.report_date ? row.report_date.slice(0, 10) : '—';
    const dateCell = `<td class="td-date-col">${escapeHtml(dateVal)}</td>`;

    const reviewedClass = row.is_reviewed ? ' is-reviewed' : '';
    const reviewedChecked = row.is_reviewed ? ' checked' : '';
    const reviewedCb = `<td style="text-align:center"><input type="checkbox" class="period-reviewed-cb" data-period="${escapeHtml(row.period)}"${reviewedChecked} onchange="onReviewedChange(this)"></td>`;
    parts.push(`
      <tr class="${rowClass}${selClass}${reviewedClass}" data-period="${escapeHtml(row.period)}">
        ${reviewedCb}
        <td class="td-period">${escapeHtml(row.period_label)}</td>
        ${metricCells.join('')}
        ${typeCell}${dateCell}
      </tr>`);
  }
  tbody.innerHTML = parts.join('');
  applyColVisibility();
}

// ── Source type → display label ────────────────────────────────────────────
const _SOURCE_TYPE_LABELS = {
  archive_pdf:      'PR',
  archive_html:     'PR',
  ir_press_release: 'IR',
  edgar_8k:         '8-K',
  edgar_10k:        '10-K',
  edgar_10q:        '10-Q',
  edgar_20f:        '20-F',
  edgar_40f:        '40-F',
  edgar_6k:         '6-K',
};
function srcTypeLabel(srcType) {
  if (!srcType) return '—';
  return _SOURCE_TYPE_LABELS[srcType] || srcType.replace(/^edgar_/, '').toUpperCase();
}

// ── Dynamic table headers + col-vis ───────────────────────────────────────
// Core 5 metrics shown by default; non-core default to hidden unless saved state
// says otherwise.
const _CORE_5 = new Set(['production_btc', 'holdings_btc', 'sales_btc', 'hashrate_eh', 'realization_rate']);
const _COL_VIS_KEY = 'miners-visible-cols';
const _COL_VIS_VERSION = 2;
const _COL_VIS_VER_KEY = 'miners-col-vis-version';

function buildTableHeaders(metricKeys, metricLabels, metricUnits) {
  _metricLabelsCache = metricLabels || {};

  // ── Rebuild <thead> metric columns ─────────────────────────────────────
  const theadRow = document.getElementById('timeline-thead-row');
  // Remove old metric ths (all those with data-col except the fixed Type/Date headers)
  const fixedCols = new Set(['doc-type', 'doc-date']);
  Array.from(theadRow.querySelectorAll('th[data-col]')).forEach(function(th) {
    if (!fixedCols.has(th.getAttribute('data-col'))) th.remove();
  });
  const typeTh = theadRow.querySelector('th[data-col="doc-type"]');
  metricKeys.forEach(function(key) {
    const label = (metricLabels && metricLabels[key]) || key;
    const th = document.createElement('th');
    th.setAttribute('data-sort', key);
    th.setAttribute('data-sort-type', 'num');
    th.setAttribute('data-col', key);
    // ⚙ link to prompt editor — stopPropagation prevents sort trigger
    const gearLink = `<a href="/patterns?metric=${encodeURIComponent(key)}" ` +
      `title="Edit extraction prompt" ` +
      `style="color:var(--theme-text-muted);font-size:0.7em;text-decoration:none;` +
      `opacity:0.55;margin-left:3px;vertical-align:middle" ` +
      `onclick="event.stopPropagation()">⚙</a>`;
    th.innerHTML = escapeHtml(label) + gearLink;
    theadRow.insertBefore(th, typeTh);
  });

  // ── Rebuild col-vis metric checkboxes ──────────────────────────────────
  const metricsContainer = document.getElementById('col-vis-metrics');
  let savedState = {};
  // Clear stale column visibility state when schema version changes
  const savedVersion = parseInt(localStorage.getItem(_COL_VIS_VER_KEY) || '0', 10);
  if (savedVersion !== _COL_VIS_VERSION) {
    localStorage.removeItem(_COL_VIS_KEY);
    localStorage.setItem(_COL_VIS_VER_KEY, String(_COL_VIS_VERSION));
  }
  const saved = localStorage.getItem(_COL_VIS_KEY);
  if (saved) { try { savedState = JSON.parse(saved); } catch(e) {} }

  metricsContainer.innerHTML = metricKeys.map(function(key) {
    const label = (metricLabels && metricLabels[key]) || key;
    const checked = (key in savedState) ? savedState[key] : _CORE_5.has(key);
    return `<label style="display:flex;align-items:center;gap:.4rem;font-size:0.8rem;cursor:pointer;padding:.15rem 0">` +
      `<input type="checkbox" class="col-vis-cb" data-col="${escapeHtml(key)}"${checked ? ' checked' : ''}> ` +
      `${escapeHtml(label)}</label>`;
  }).join('');

  applyColVisibility();
}

// ── Column visibility ──────────────────────────────────────────────────────
// Change listeners use event delegation on #col-vis-menu (wired once in
// DOMContentLoaded) so they survive dynamic checkbox rebuilds.

function toggleColMenu() {
  const menu = document.getElementById('col-vis-menu');
  menu.style.display = menu.style.display === 'none' ? 'block' : 'none';
}

function applyColVisibility() {
  const table = document.getElementById('timeline-table');
  if (!table) return;

  // Build col-key → column-index map from thead <th data-col="...">
  const headers = Array.from(table.querySelectorAll('thead th'));
  const colIndexByKey = {};
  headers.forEach(function(th, idx) {
    const key = th.getAttribute('data-col');
    if (key) colIndexByKey[key] = idx;
  });

  // Read current checkbox state
  const visible = {};
  document.querySelectorAll('.col-vis-cb').forEach(function(cb) {
    visible[cb.getAttribute('data-col')] = cb.checked;
  });

  // Apply to header cells
  headers.forEach(function(th) {
    const key = th.getAttribute('data-col');
    if (key) th.style.display = visible[key] ? '' : 'none';
  });

  // Apply to all body rows
  Array.from(table.querySelectorAll('tbody tr')).forEach(function(tr) {
    Array.from(tr.cells).forEach(function(td, idx) {
      // Find which col key this index maps to
      for (const key in colIndexByKey) {
        if (colIndexByKey[key] === idx) {
          td.style.display = visible[key] ? '' : 'none';
          break;
        }
      }
    });
  });
}

// Re-render on filter changes
['filter-gaps-only', 'filter-show-incomplete'].forEach(function(id) {
  document.getElementById(id).addEventListener('change', function() {
    renderTable(_rows);
  });
});
['filter-from', 'filter-to'].forEach(function(id) {
  document.getElementById(id).addEventListener('input', function() {
    renderTable(_rows);
  });
});

// ── Select period → open doc panel ────────────────────────────────────────
function selectPeriod(period) {
  if (!_ticker) return;
  _selectedPeriod = period;

  // Highlight selected row
  document.querySelectorAll('#timeline-tbody tr[data-period]').forEach(function(r) {
    r.classList.toggle('selected', r.getAttribute('data-period') === period);
  });

  const periodLabel = period.slice(0, 7);

  // Find the row to get source info and compute null metrics
  const row = _rows.find(r => r.period === period);
  const nullMetrics = row
    ? METRICS_ORDER.filter(function(m) { return !row.metrics[m] || row.metrics[m].value == null; })
    : METRICS_ORDER.slice();

  // Show doc panel
  const panel = document.getElementById('doc-panel');
  panel.classList.add('visible');
  document.getElementById('doc-panel-title-text').textContent =
    `${_ticker} · ${periodLabel}${row && row.source_type ? ' · ' + row.source_type : ''}`;

  // Reset pattern panel state (pattern generator is separate from ReviewPanel)
  document.getElementById('pattern-panel').style.display = 'none';
  document.getElementById('pattern-save-status').textContent = '';
  document.getElementById('apply-result').style.display = 'none';

  // Open ReviewPanel for this cell (no specific metric — shows all analysis)
  ReviewPanel.openCell(_ticker, period, null, {nullMetrics: nullMetrics});

  // Scroll panel into view
  panel.scrollIntoView({ behavior: 'smooth', block: 'nearest' });
}



// ── Close doc panel ────────────────────────────────────────────────────────
function closeDocPanel() {
  _selectedPeriod = null;
  const panel = document.getElementById('doc-panel');
  panel.classList.remove('visible');
  document.getElementById('pattern-panel').style.display = 'none';
  ReviewPanel.close();
  // Clear row selection
  document.querySelectorAll('#timeline-tbody tr.selected').forEach(function(r) {
    r.classList.remove('selected');
  });
}

// Escape key closes doc panel
document.addEventListener('keydown', function(e) {
  if (e.key === 'Escape' && _selectedPeriod) closeDocPanel();
});

// ── Text selection → pattern generation (wired in DOMContentLoaded) ───────

function showPatternPanel(selectedText) {
  // Pattern generator disabled in LLM-only mode (no-op).
}

async function generatePattern(selectedText) {
  if (!selectedText) return;
  const metric = document.getElementById('pattern-metric').value;
  try {
    const resp = await fetch('/api/patterns/generate', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({
        selected_text: selectedText,
        metric: metric,
        doc_text: _currentDocText.slice(0, 50000),
      }),
    });
    if (!resp.ok) {
      const err = await resp.json().catch(() => ({}));
      showToast(err.error?.message || 'Pattern generation failed', true);
      return;
    }
    const data = await resp.json();
    if (!data.success) { showToast(data.error?.message || 'Failed', true); return; }
    document.getElementById('pattern-regex-input').value = data.data.regex;
    updateMatchCount(data.data.match_count);
  } catch (err) {
    showToast(`Pattern error: ${err.message}`, true);
  }
}

function updateMatchCount(count) {
  const el = document.getElementById('match-count-display');
  if (count === 0) {
    el.textContent = '0 matches';
    el.className = 'pattern-match-count zero';
  } else {
    el.textContent = `${count} match${count === 1 ? '' : 'es'} ✓`;
    el.className = 'pattern-match-count good';
  }
}

async function testGeneratedPattern() {
  const regex = document.getElementById('pattern-regex-input').value.trim();
  if (!regex) return;
  const metric = document.getElementById('pattern-metric').value;
  if (!_currentDocText) { showToast('No document loaded', true); return; }
  try {
    const resp = await fetch('/api/patterns/test', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({ regex, text: _currentDocText.slice(0, 50000), metric }),
    });
    const data = await resp.json();
    if (!data.success) { showToast(data.error?.message || 'Test failed', true); return; }
    updateMatchCount(data.data.matches.length);
  } catch (err) {
    showToast(`Test error: ${err.message}`, true);
  }
}

async function saveGeneratedPattern() {
  const regex = document.getElementById('pattern-regex-input').value.trim();
  if (!regex) { showToast('No regex to save', true); return; }
  const metric = document.getElementById('pattern-metric').value;
  try {
    const resp = await fetch(`/api/patterns/${encodeURIComponent(metric)}`, {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({ regex, confidence_weight: 0.87 }),
    });
    const data = await resp.json();
    if (!data.success) { showToast(data.error?.message || 'Save failed', true); return; }
    const newId = data.data?.id || '';
    document.getElementById('pattern-save-status').textContent = `Saved as ${newId}`;
    showToast(`Pattern saved as ${newId}`);
  } catch (err) {
    showToast(`Save error: ${err.message}`, true);
  }
}

// ── Open raw source in new tab ─────────────────────────────────────────────
function openInNewTab() {
  if (!_ticker || !_selectedPeriod) return;
  const row = _rows.find(function(r) { return r.period === _selectedPeriod; });
  const url = row && row.source_url;
  if (url) {
    window.open(url, '_blank', 'noopener');
  } else {
    showToast('No source URL for this report', true);
  }
}


// ── Source view switching ─────────────────────────────────────────────────
let _currentView = 'monthly';
let _secRows = [];
let _stagedValues = [];   // [{period, metric, value, unit, analyst_note}]
let _finalizedValues = [];

async function switchView(view) {
  _currentView = view;
  document.querySelectorAll('.source-toggle-btn').forEach(function(btn) {
    btn.classList.toggle('active', btn.getAttribute('data-view') === view);
  });
  document.getElementById('view-monthly').style.display = view === 'monthly' ? '' : 'none';
  document.getElementById('view-sec').style.display = view === 'sec' ? '' : 'none';
  document.getElementById('view-interpret').style.display = view === 'interpret' ? '' : 'none';

  if (view === 'sec' && _ticker && _secRows.length === 0) {
    loadSecData(_ticker);
  }
  if (view === 'interpret' && _ticker) {
    if (_secRows.length === 0) await loadSecData(_ticker);
    loadInterpretData(_ticker);
  }
}

// ── SEC view ──────────────────────────────────────────────────────────────
let _secMetricKeys = [];

async function loadSecData(ticker) {
  document.getElementById('sec-timeline-tbody').innerHTML =
    `<tr><td colspan="10" class="md-table-empty"><span class="spin"></span> Loading…</td></tr>`;
  try {
    const resp = await fetch(`/api/miner/${encodeURIComponent(ticker)}/timeline?source=sec`);
    if (!resp.ok) throw new Error(`HTTP ${resp.status}`);
    const body = await resp.json();
    if (!body.success) throw new Error(body.error?.message || 'Failed');
    const data = body.data;
    _secRows = data.rows || [];
    _secMetricKeys = data.metric_keys || [];
    buildSecHeaders(_secMetricKeys, data.metric_labels || {});
    renderSecTable(_secRows);
    // Update info card
    document.getElementById('sec-info-card').innerHTML =
      `<div class="md-info-name">${escapeHtml(data.company.name)}</div>` +
      `<div class="md-info-row"><span class="md-info-label">Periods</span><span class="md-info-value">${data.stats.total_periods}</span></div>`;
  } catch (err) {
    document.getElementById('sec-timeline-tbody').innerHTML =
      `<tr><td colspan="10" class="md-table-empty" style="color:var(--theme-danger)">Error: ${escapeHtml(err.message)}</td></tr>`;
  }
}

function buildSecHeaders(metricKeys, metricLabels) {
  const theadRow = document.getElementById('sec-thead-row');
  const fixedSecCols = new Set(['sec-doc-type', 'sec-doc-date']);
  Array.from(theadRow.querySelectorAll('th[data-col]')).forEach(function(th) {
    if (!fixedSecCols.has(th.getAttribute('data-col'))) th.remove();
  });
  const typeTh = theadRow.querySelector('th[data-col="sec-doc-type"]');
  metricKeys.forEach(function(key) {
    const label = (metricLabels && metricLabels[key]) || key;
    const th = document.createElement('th');
    th.setAttribute('data-col', 'sec-' + key);
    th.textContent = label;
    theadRow.insertBefore(th, typeTh);
  });
}

function renderSecTable(rows) {
  const tbody = document.getElementById('sec-timeline-tbody');
  if (!rows || rows.length === 0) {
    tbody.innerHTML = `<tr><td colspan="${_secMetricKeys.length + 2}" class="md-table-empty">No SEC data found.</td></tr>`;
    return;
  }
  const parts = [];
  for (const row of rows) {
    const metricCells = _secMetricKeys.map(function(metric) {
      const m = row.metrics[metric];
      if (!m || m.value == null) return `<td class="td-empty">—</td>`;
      if (m.is_pending) {
        return `<td class="td-pending">${escapeHtml(fmtValue(m) || '')}<span class="badge-pending">pending</span></td>`;
      }
      const finalBadge = m.is_finalized ? `<span class="badge-final">F</span>` : '';
      return `<td class="td-value">${escapeHtml(fmtValue(m) || '')}${finalBadge}</td>`;
    });
    const srcType = row.source_type || '';
    const srcUrl  = row.source_url  || '';
    const typeLabel = srcTypeLabel(srcType);
    const typeCellSec = srcUrl
      ? `<td class="td-type-col"><a href="${escapeHtml(srcUrl)}" target="_blank" rel="noopener" title="${escapeHtml(srcType)}" style="color:var(--theme-accent);text-decoration:none">${escapeHtml(typeLabel)}</a></td>`
      : `<td class="td-type-col">${escapeHtml(typeLabel)}</td>`;
    const secDateVal = row.report_date ? row.report_date.slice(0, 10) : '—';
    const dateCellSec = `<td class="td-date-col">${escapeHtml(secDateVal)}</td>`;
    parts.push(`<tr>
      <td class="td-period">${escapeHtml(row.period_label)}</td>
      ${metricCells.join('')}${typeCellSec}${dateCellSec}
    </tr>`);
  }
  tbody.innerHTML = parts.join('');
}

// ── Interpret tab ─────────────────────────────────────────────────────────
async function loadInterpretData(ticker) {
  // Load finalized values
  try {
    const resp = await fetch(`/api/interpret/${encodeURIComponent(ticker)}/final`);
    if (resp.ok) {
      const body = await resp.json();
      if (body.success) {
        _finalizedValues = body.data || [];
        renderFinalizedTable();
        renderReconcileTable();
      }
    }
  } catch(e) {}
  // Update danger zone confirmation word
  const word = `CLEAR_FINAL_${ticker}`;
  const label = document.getElementById('dz-ticker-label');
  const confWord = document.getElementById('dz-confirm-word');
  if (label) label.textContent = ticker;
  if (confWord) confWord.textContent = word;
  // Restore commentary from localStorage
  const key = `interp-commentary-${ticker}`;
  const saved = localStorage.getItem(key);
  const ta = document.getElementById('interp-commentary');
  if (ta && saved !== null) ta.value = saved;
  if (ta) {
    ta.oninput = function() { localStorage.setItem(key, ta.value); };
  }
}

function renderReconcileTable() {
  const tbody = document.getElementById('interp-reconcile-tbody');
  // Build a merged map: {period_metric: {period, metric, monthly_val, sec_val, final_val}}
  const map = {};
  function key(p, m) { return p + '|' + m; }
  // Normalize a period to a stable map key:
  // monthly "2024-07-01" -> "2024-07"; quarterly/annual "2024-Q3"/"2024-FY" kept as-is.
  function periodKey(p) {
    if (!p) return '';
    return /^\d{4}-Q\d$|^\d{4}-FY$/.test(p) ? p : p.slice(0, 7);
  }
  (_rows || []).forEach(function(row) {
    Object.keys(row.metrics || {}).forEach(function(metric) {
      const c = row.metrics[metric];
      if (!c || c.value == null) return;
      const k = key(periodKey(row.period), metric);
      if (!map[k]) map[k] = {period: periodKey(row.period), period_label: row.period_label, metric, monthly: null, sec: null, final: null};
      map[k].monthly = c.value;
    });
  });
  (_secRows || []).forEach(function(row) {
    Object.keys(row.metrics || {}).forEach(function(metric) {
      const c = row.metrics[metric];
      if (!c || c.value == null) return;
      const k = key(periodKey(row.period), metric);
      if (!map[k]) map[k] = {period: periodKey(row.period), period_label: row.period_label, metric, monthly: null, sec: null, final: null};
      map[k].sec = c.value;
    });
  });
  (_finalizedValues || []).forEach(function(f) {
    const p = periodKey(f.period);
    const k = key(p, f.metric);
    if (!map[k]) map[k] = {period: p, period_label: p, metric: f.metric, monthly: null, sec: null, final: null};
    map[k].final = f.value;
  });
  const entries = Object.values(map).sort(function(a, b) {
    return b.period.localeCompare(a.period) || a.metric.localeCompare(b.metric);
  });
  if (entries.length === 0) {
    tbody.innerHTML = `<tr><td colspan="5" class="md-table-empty">No data to reconcile.</td></tr>`;
    return;
  }
  tbody.innerHTML = entries.map(function(e) {
    const finalCell = e.final != null
      ? `<span class="badge-final">F</span> ${e.final.toLocaleString()}`
      : '—';
    return `<tr>
      <td>${escapeHtml(e.period_label || e.period)}</td>
      <td>${escapeHtml(e.metric)}</td>
      <td>${e.monthly != null ? e.monthly.toLocaleString() : '—'}</td>
      <td>${e.sec != null ? e.sec.toLocaleString() : '—'}</td>
      <td>${finalCell}</td>
    </tr>`;
  }).join('');
}

function renderFinalizedTable() {
  const tbody = document.getElementById('finalized-tbody');
  if (!_finalizedValues || _finalizedValues.length === 0) {
    tbody.innerHTML = `<tr><td colspan="7" class="md-table-empty">No finalized values.</td></tr>`;
    return;
  }
  tbody.innerHTML = _finalizedValues.map(function(f, i) {
    const updated = (f.updated_at || f.created_at || '').slice(0, 10);
    return `<tr>
      <td>${escapeHtml(f.period ? f.period.slice(0,7) : '')}</td>
      <td>${escapeHtml(f.metric)}</td>
      <td>${f.value != null ? f.value.toLocaleString() : ''}</td>
      <td>${escapeHtml(f.unit || '')}</td>
      <td style="max-width:200px;overflow:hidden;text-overflow:ellipsis">${escapeHtml(f.analyst_note || '')}</td>
      <td>${escapeHtml(updated)}</td>
      <td><button class="btn btn-secondary btn-sm" style="font-size:0.72rem;padding:0.15rem 0.4rem"
        onclick="unfinalize(${i})">Revise</button></td>
    </tr>`;
  }).join('');
}

async function doReprompt() {
  if (!_ticker) { showToast('Select a company first', true); return; }
  const commentary = document.getElementById('interp-commentary').value.trim();
  const btn = document.getElementById('reprompt-btn');
  const statusEl = document.getElementById('reprompt-status');
  btn.disabled = true;
  statusEl.textContent = 'Calling LLM…';
  document.getElementById('suggestions-area').style.display = 'none';
  try {
    const resp = await fetch(`/api/interpret/${encodeURIComponent(_ticker)}/reprompt`, {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({
      commentary,
      metrics: Array.from(document.getElementById('reprompt-metrics-filter').selectedOptions).map(function(o) { return o.value; }),
    }),
    });
    if (!resp.ok) throw new Error(`HTTP ${resp.status}`);
    const body = await resp.json();
    if (!body.success) throw new Error(body.error?.message || 'Failed');
    const suggestions = body.data.suggestions || [];
    if (suggestions.length === 0) {
      statusEl.textContent = 'No suggestions returned.';
    } else {
      statusEl.textContent = `${suggestions.length} suggestion${suggestions.length === 1 ? '' : 's'}`;
      renderSuggestions(suggestions);
      document.getElementById('suggestions-area').style.display = '';
    }
  } catch (err) {
    statusEl.textContent = `Error: ${err.message}`;
    showToast(`Reprompt failed: ${err.message}`, true);
  } finally {
    btn.disabled = false;
  }
}

async function doRerunSec() {
  if (!_ticker) { showToast('Select a company first', true); return; }
  const btn = document.getElementById('rerun-sec-btn');
  const statusEl = document.getElementById('rerun-sec-status');
  btn.disabled = true;
  statusEl.textContent = 'Running…';
  try {
    const resp = await fetch(`/api/interpret/${encodeURIComponent(_ticker)}/rerun-sec`, {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
    });
    if (!resp.ok) throw new Error(`HTTP ${resp.status}`);
    const body = await resp.json();
    if (!body.success) throw new Error(body.error?.message || 'Failed');
    const d = body.data;
    statusEl.textContent =
      `Done: ${d.reports_processed} reports, ${d.data_points_extracted} data points` +
      (d.errors > 0 ? `, ${d.errors} errors` : '');
    // Refresh SEC and reconcile views with updated data
    _secRows = [];
    await loadSecData(_ticker);
    loadInterpretData(_ticker);
  } catch (err) {
    statusEl.textContent = `Error: ${err.message}`;
    showToast(`Re-interpret failed: ${err.message}`, true);
  } finally {
    btn.disabled = false;
  }
}

function renderSuggestions(suggestions) {
  const container = document.getElementById('suggestion-cards');
  container.innerHTML = suggestions.map(function(s, i) {
    const periodLabel = s.period ? s.period.slice(0, 7) : s.period;
    return `<div class="suggestion-card" id="sug-card-${i}">
      <div class="sug-header">
        <span>${escapeHtml(s.metric)}</span>
        <span style="color:var(--theme-text-muted)">${escapeHtml(periodLabel)}</span>
        <span style="color:var(--theme-accent);font-weight:700">${(s.value || 0).toLocaleString()}</span>
        <span style="color:var(--theme-text-muted);font-size:0.75rem">(conf: ${(s.confidence || 0).toFixed(2)})</span>
      </div>
      <div class="sug-rationale">${escapeHtml(s.rationale || '')}</div>
      <div class="sug-actions">
        <button class="btn btn-primary btn-sm" style="font-size:0.75rem;padding:0.2rem 0.5rem"
          onclick="acceptSuggestion(${i})">Accept</button>
        <button class="btn btn-secondary btn-sm" style="font-size:0.75rem;padding:0.2rem 0.5rem"
          onclick="document.getElementById('sug-card-${i}').style.opacity='0.4'">Skip</button>
      </div>
    </div>`;
  }).join('');
  // Store suggestions for accept callbacks
  container._suggestions = suggestions;
}

function acceptSuggestion(idx) {
  const container = document.getElementById('suggestion-cards');
  const suggestions = container._suggestions || [];
  const s = suggestions[idx];
  if (!s) return;
  _stagedValues.push({
    period: s.period,
    metric: s.metric,
    value:  s.value,
    unit:   '',
    analyst_note: '',
    source_ref: '',
  });
  document.getElementById(`sug-card-${idx}`).style.opacity = '0.4';
  renderStagingTable();
  document.getElementById('staging-area').style.display = '';
}

function renderStagingTable() {
  const tbody = document.getElementById('staging-tbody');
  if (_stagedValues.length === 0) {
    tbody.innerHTML = '';
    document.getElementById('staging-area').style.display = 'none';
    return;
  }
  tbody.innerHTML = _stagedValues.map(function(v, i) {
    return `<tr>
      <td>${escapeHtml(v.period ? v.period.slice(0,7) : '')}</td>
      <td>${escapeHtml(v.metric)}</td>
      <td><input type="number" value="${v.value}" step="any"
        style="width:90px;padding:0.15rem 0.3rem;background:var(--theme-bg-input);border:1px solid var(--theme-border);border-radius:3px;color:var(--theme-text-primary);font-size:0.8rem"
        onchange="_stagedValues[${i}].value = parseFloat(this.value)"></td>
      <td><input type="text" value="${escapeHtml(v.unit)}" placeholder="BTC"
        style="width:55px;padding:0.15rem 0.3rem;background:var(--theme-bg-input);border:1px solid var(--theme-border);border-radius:3px;color:var(--theme-text-primary);font-size:0.8rem"
        onchange="_stagedValues[${i}].unit = this.value"></td>
      <td><input type="text" value="${escapeHtml(v.analyst_note)}" placeholder="Note"
        style="width:160px;padding:0.15rem 0.3rem;background:var(--theme-bg-input);border:1px solid var(--theme-border);border-radius:3px;color:var(--theme-text-primary);font-size:0.8rem"
        onchange="_stagedValues[${i}].analyst_note = this.value"></td>
      <td><button class="btn btn-secondary btn-sm" style="font-size:0.72rem;padding:0.15rem 0.4rem"
        onclick="removeStaged(${i})">Remove</button></td>
    </tr>`;
  }).join('');
}

function removeStaged(i) {
  _stagedValues.splice(i, 1);
  renderStagingTable();
}

async function finalizeSelected() {
  if (!_ticker || _stagedValues.length === 0) return;
  const btn = document.querySelector('#staging-area button.btn-primary');
  const statusEl = document.getElementById('finalize-status');
  if (btn) btn.disabled = true;
  statusEl.textContent = 'Saving…';
  try {
    const resp = await fetch(`/api/interpret/${encodeURIComponent(_ticker)}/finalize`, {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({values: _stagedValues}),
    });
    if (!resp.ok) throw new Error(`HTTP ${resp.status}`);
    const body = await resp.json();
    if (!body.success) throw new Error(body.error?.message || 'Failed');
    const count = body.data.count;
    showToast(`Finalized ${count} value${count !== 1 ? 's' : ''}`);
    statusEl.textContent = `${count} saved`;
    _stagedValues = [];
    renderStagingTable();
    loadInterpretData(_ticker);
  } catch (err) {
    statusEl.textContent = `Error: ${err.message}`;
    showToast(`Finalize failed: ${err.message}`, true);
  } finally {
    if (btn) btn.disabled = false;
  }
}

function unfinalize(idx) {
  const f = _finalizedValues[idx];
  if (!f) return;
  // Move into staged values for editing
  _stagedValues.push({
    period: f.period,
    metric: f.metric,
    value:  f.value,
    unit:   f.unit || '',
    analyst_note: f.analyst_note || '',
    source_ref: f.source_ref || '',
  });
  renderStagingTable();
  document.getElementById('staging-area').style.display = '';
}

async function clearFinalTicker() {
  if (!_ticker) return;
  const expectedWord = `CLEAR_FINAL_${_ticker}`;
  const input = document.getElementById('dz-ticker-confirm-input').value.trim();
  const statusEl = document.getElementById('dz-ticker-status');
  if (input !== expectedWord) {
    statusEl.textContent = `Type "${expectedWord}" to confirm`;
    statusEl.style.color = 'var(--theme-danger)';
    return;
  }
  statusEl.textContent = 'Clearing…';
  statusEl.style.color = '';
  try {
    const resp = await fetch(`/api/interpret/${encodeURIComponent(_ticker)}/final`, {
      method: 'DELETE',
    });
    if (!resp.ok) throw new Error(`HTTP ${resp.status}`);
    const body = await resp.json();
    if (!body.success) throw new Error(body.error?.message || 'Failed');
    const n = body.data.deleted;
    statusEl.textContent = `Cleared ${n} rows`;
    statusEl.style.color = 'var(--theme-success)';
    document.getElementById('dz-ticker-confirm-input').value = '';
    _finalizedValues = [];
    renderFinalizedTable();
    renderReconcileTable();
    showToast(`Cleared ${n} finalized values for ${_ticker}`);
  } catch (err) {
    statusEl.textContent = `Error: ${err.message}`;
    statusEl.style.color = 'var(--theme-danger)';
  }
}

async function purgeAllFinal() {
  const input = document.getElementById('dz-global-confirm-input').value.trim();
  const statusEl = document.getElementById('dz-global-status');
  if (input !== 'CLEAR_FINAL_ALL') {
    statusEl.textContent = 'Type CLEAR_FINAL_ALL to confirm';
    statusEl.style.color = 'var(--theme-danger)';
    return;
  }
  const mode = document.getElementById('dz-purge-mode').value;
  statusEl.textContent = 'Purging…';
  statusEl.style.color = '';
  try {
    const resp = await fetch('/api/interpret/final/purge', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({confirm: true, mode}),
    });
    if (!resp.ok) throw new Error(`HTTP ${resp.status}`);
    const body = await resp.json();
    if (!body.success) throw new Error(body.error?.message || 'Failed');
    const n = body.data.deleted;
    statusEl.textContent = `Purged ${n} rows`;
    statusEl.style.color = 'var(--theme-success)';
    document.getElementById('dz-global-confirm-input').value = '';
    _finalizedValues = [];
    renderFinalizedTable();
    renderReconcileTable();
    showToast(`Purged ${n} finalized values`);
  } catch (err) {
    statusEl.textContent = `Error: ${err.message}`;
    statusEl.style.color = 'var(--theme-danger)';
  }
}

async function applyPatternToArchive() {
  const regex = document.getElementById('pattern-regex-input').value.trim();
  if (!regex) { showToast('No regex to apply', true); return; }
  const metric = document.getElementById('pattern-metric').value;
  const btn = document.getElementById('apply-archive-btn');
  const resultEl = document.getElementById('apply-result');
  btn.disabled = true;
  btn.textContent = 'Applying…';
  resultEl.style.display = 'none';

  try {
    const resp = await fetch('/api/patterns/apply', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({ regex, metric, confidence_weight: 0.87 }),
    });
    if (!resp.ok) {
      const err = await resp.json().catch(() => ({}));
      throw new Error(err?.error?.message || `HTTP ${resp.status}`);
    }
    const data = await resp.json();
    if (!data.success) throw new Error(data.error?.message || 'Apply failed');

    const d = data.data;
    const parts = [];
    if (d.created > 0) parts.push(`<span style="color:var(--theme-success)">${d.created} created</span>`);
    if (d.skipped_existing > 0) parts.push(`${d.skipped_existing} skipped`);
    if (d.low_confidence > 0) parts.push(`${d.low_confidence} → review queue`);
    if (d.no_match > 0) parts.push(`${d.no_match} no match`);
    resultEl.innerHTML = `Applied to ${d.applied_to} reports — ${parts.join(', ') || 'no changes'}.`;
    resultEl.style.display = '';

    if (d.created > 0) {
      showToast(`Applied: ${d.created} new data point${d.created !== 1 ? 's' : ''} created`);
      // Refresh table for current company
      selectCompany(_ticker);
    } else {
      showToast('Pattern applied — no new data points created');
    }
  } catch (err) {
    resultEl.innerHTML = `<span style="color:var(--theme-danger)">Apply error: ${escapeHtml(err.message)}</span>`;
    resultEl.style.display = '';
    showToast(`Apply error: ${err.message}`, true);
  } finally {
    btn.disabled = false;
    btn.textContent = 'Apply to Archive';
  }
}
