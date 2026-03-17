// miner_data.js — Analyst timeline view. Extracted from miner_data.html.
// Boot is deferred: call boot() on first activation of Review > Review sub-tab.
let _minerDataBooted = false;

// ── State ─────────────────────────────────────────────────────────────────
let _ticker = null;               // currently selected company ticker
let _rows = [];                   // full unfiltered row list from API
let _selectedPeriod = null;       // period of the currently open doc panel
let _currentDocText = '';         // raw text for pattern generator (set when doc is loaded)
let _lastSelectedRowIdx = -1;     // row index of the last selected row (for Shift+click range)
let _tableMode = 'view';
let _expandedDocsPeriod = null;
let _selectedReportIds = {};

// ── Projection / forward-fill state ───────────────────────────────────────
let _projEnabled = false;         // whether forward-fill projection is on
let _projWindow  = 6;             // moving-average window in months
let _projPanelOpen = false;       // dropdown panel visibility

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
  const _tbody = document.getElementById('timeline-tbody');
  // doc-panel is already in its correct DOM position — no relocation needed.

  // Prevent text selection on Shift+click (standard multi-select UX)
  _tbody.addEventListener('mousedown', function(e) {
    if (e.shiftKey) e.preventDefault();
  });

  _tbody.addEventListener('click', function(e) {
    const row = e.target.closest('tr[data-period]');
    if (!row) return;

    // Accept button handled by its own onclick
    if (e.target.closest('button')) return;

    const period = row.getAttribute('data-period');
    if (_tableMode === 'view' && !e.target.classList.contains('row-select-cb')) {
      // Expand the doc chips sub-row so all available docs for this period are visible,
      // then open the doc panel for the period.
      toggleRowDocs(period);
      selectPeriod(period);
      return;
    }
    const metricTd = e.target.closest('td[data-metric]');
    if (_tableMode === 'edit' && metricTd) {
      _beginInlineEdit(metricTd, period, metricTd.getAttribute('data-metric'));
      return;
    }

    // Checkbox-only selection for batch accept
    const rows = Array.from(_tbody.querySelectorAll('tr[data-period]'));
    const idx = rows.indexOf(row);
    const isCbTarget = e.target.classList && e.target.classList.contains('row-select-cb');
    if (!isCbTarget) return;

    if (e.shiftKey && _lastSelectedRowIdx >= 0) {
      e.preventDefault();
      const lo = Math.min(idx, _lastSelectedRowIdx);
      const hi = Math.max(idx, _lastSelectedRowIdx);
      rows.forEach(function(r, i) {
        if (r.classList.contains('row-gap')) return;
        const cb = r.querySelector('.row-select-cb');
        if (cb && i >= lo && i <= hi) cb.checked = true;
      });
      _lastSelectedRowIdx = idx;
      _updateBatchAcceptBtn();
      _syncSelectAllHeader();
    } else {
      _lastSelectedRowIdx = idx;
      Promise.resolve().then(function() {
        _updateBatchAcceptBtn();
        _syncSelectAllHeader();
      });
    }
  });

  // ── SEC table row click → open doc panel ────────────────────────────────
  const _secTbody = document.getElementById('sec-timeline-tbody');
  if (_secTbody) {
    _secTbody.addEventListener('click', function(e) {
      if (e.target.closest('a')) return;  // let type-col links open normally
      const row = e.target.closest('tr[data-period]');
      if (!row) return;
      const period = row.getAttribute('data-period');
      const secRow = _secRows.find(function(r) { return r.period === period; });
      const reportId = secRow ? secRow.report_id : null;
      const nullMetrics = secRow
        ? _secMetricKeys.filter(function(m) { return !secRow.metrics[m] || secRow.metrics[m].value == null; })
        : _secMetricKeys.slice();

      // Highlight selected row
      _secTbody.querySelectorAll('tr[data-period]').forEach(function(r) {
        r.classList.toggle('selected', r.getAttribute('data-period') === period);
      });

      // Show doc panel
      const panel = document.getElementById('doc-panel');
      if (panel) {
        panel.classList.add('visible');
        panel.style.display = 'flex';
        const titleEl = document.getElementById('doc-panel-title-text');
        if (titleEl) titleEl.textContent = `${_ticker} · ${period}` + (secRow && secRow.source_type ? ` · ${secRow.source_type}` : '');
      }

      ReviewPanel.openCell(_ticker, period, null, {
        nullMetrics: nullMetrics,
        reportId: reportId != null ? reportId : null,
      });
    });
  }

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
  setMinerTableMode('view');
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

// ── Row selection — batch accept helpers ───────────────────────────────────

function toggleSelectAll(checked) {
  document.querySelectorAll('#timeline-tbody .row-select-cb').forEach(function(cb) {
    cb.checked = checked;
  });
  _lastSelectedRowIdx = -1;
  _updateBatchAcceptBtn();
  _syncSelectAllHeader();
}

function _updateBatchAcceptBtn() {
  const n = document.querySelectorAll('#timeline-tbody .row-select-cb:checked').length;
  const btn = document.getElementById('batch-accept-btn');
  if (!btn) return;
  btn.disabled = n === 0;
  btn.textContent = n > 0 ? ('Accept ' + n + ' row' + (n === 1 ? '' : 's')) : 'Batch Accept';
}

function _syncSelectAllHeader() {
  const cbs = Array.from(document.querySelectorAll('#timeline-tbody .row-select-cb'));
  if (!cbs.length) return;
  const allChecked = cbs.every(function(cb) { return cb.checked; });
  const anyChecked = cbs.some(function(cb) { return cb.checked; });
  const headerCb = document.getElementById('select-all-rows-cb');
  if (!headerCb) return;
  headerCb.checked = allChecked;
  headerCb.indeterminate = !allChecked && anyChecked;
}

async function batchAcceptSelected() {
  if (!_ticker) return;
  const periods = Array.from(document.querySelectorAll('#timeline-tbody .row-select-cb:checked'))
    .map(function(cb) { return cb.getAttribute('data-period'); })
    .filter(Boolean);
  if (!periods.length) return;

  const values = [];
  periods.forEach(function(period) {
    const row = _rows && _rows.find(function(r) { return r.period === period; });
    if (!row) return;
    METRICS_ORDER.forEach(function(metric) {
      const m = row.metrics && row.metrics[metric];
      if (m && m.value != null) {
        values.push({
          period: period,
          metric: metric,
          value: m.value,
          unit: m.unit || '',
          confidence: m.confidence != null ? m.confidence : 1.0,
          analyst_note: 'review_approved',
        });
      }
    });
  });

  if (!values.length) { showToast('No values to accept', true); return; }

  try {
    const resp = await fetch(`/api/interpret/${encodeURIComponent(_ticker)}/finalize`, {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({values: values}),
    });
    const body = await resp.json();
    if (body.success) {
      periods.forEach(function(period) {
        const row = _rows && _rows.find(function(r) { return r.period === period; });
        if (!row) return;
        METRICS_ORDER.forEach(function(m) {
          if (row.metrics[m] && row.metrics[m].value != null) row.metrics[m].is_finalized = true;
        });
      });
      document.querySelectorAll('#timeline-tbody .row-select-cb:checked')
        .forEach(function(cb) { cb.checked = false; });
      _lastSelectedRowIdx = -1;
      _updateBatchAcceptBtn();
      _syncSelectAllHeader();
      renderTable(_rows);
      const nRows = periods.length;
      showToast((body.data && body.data.count || 0) + ' values accepted across ' + nRows + ' period' + (nRows === 1 ? '' : 's'));
      if (_currentView === 'interpret') loadInterpretData(_ticker);
    } else {
      showToast((body.error && body.error.message) || 'Batch accept failed', true);
    }
  } catch (e) {
    showToast('Batch accept failed', true);
  }
}

// ── Inline cell editing ────────────────────────────────────────────────────
let _editingCell = null;

function _beginInlineEdit(td, period, metric) {
  if (!td || _editingCell) return;
  const row = _rows.find(function(r) { return r.period === period; });
  const m = row && row.metrics && row.metrics[metric];
  const currentVal = m ? m.value : null;

  _editingCell = {td: td, period: period, metric: metric, original: td.innerHTML};
  td.innerHTML = `<input type="number" step="any" style="width:80px;font-size:0.82rem;background:var(--theme-bg-input);border:1px solid var(--theme-accent);border-radius:3px;color:var(--theme-text-primary);padding:1px 3px" value="${currentVal != null ? currentVal : ''}" id="inline-edit-input">`
    + `<button style="margin-left:2px;font-size:0.7rem;padding:1px 4px;background:var(--theme-accent);color:#fff;border:none;border-radius:2px;cursor:pointer" onclick="confirmInlineEdit(event,'${escapeHtml(period)}','${escapeHtml(metric)}')">OK</button>`;
  const input = td.querySelector('#inline-edit-input');
  if (input) { input.focus(); input.select(); }
  input.addEventListener('keydown', function(e) {
    if (e.key === 'Enter') confirmInlineEdit(e, period, metric);
    if (e.key === 'Escape') cancelInlineEdit();
  });
}

function inlineEditCell(event, period, metric) {
  event.stopPropagation();
  if (_tableMode !== 'edit') return;
  _beginInlineEdit(event.currentTarget, period, metric);
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

async function flushInlineEdit() {
  if (!_editingCell) return;
  const input = _editingCell.td.querySelector('input');
  const val = input ? parseFloat(input.value) : NaN;
  if (isNaN(val) || val < 0) {
    cancelInlineEdit();
    return;
  }
  const { period, metric } = _editingCell;
  _editingCell = null;
  try {
    const resp = await fetch(`/api/interpret/${encodeURIComponent(_ticker)}/finalize`, {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({values: [{period, metric, value: val}]}),
    });
    const body = await resp.json();
    if (body.success) {
      const row = _rows.find(function(r) { return r.period === period; });
      if (row) {
        if (!row.metrics[metric]) row.metrics[metric] = {};
        row.metrics[metric].value = val;
        row.metrics[metric].is_finalized = true;
      }
      showToast('Value finalized');
    } else {
      showToast((body.error && body.error.message) || 'Finalize failed', true);
    }
  } catch (e) {
    showToast('Finalize failed', true);
  }
}

async function syncEdits() {
  await flushInlineEdit();
  showToast('All edits saved to database');
}

async function acceptRow(period) {
  const row = _rows && _rows.find(function(r) { return r.period === period; });
  if (!row) return;

  // Collect all non-null metric values for this period
  const values = [];
  METRICS_ORDER.forEach(function(metric) {
    const m = row.metrics && row.metrics[metric];
    if (m && m.value != null) {
      values.push({
        period: period,
        metric: metric,
        value: m.value,
        unit: m.unit || '',
        confidence: m.confidence != null ? m.confidence : 1.0,
        analyst_note: 'review_approved',
      });
    }
  });
  if (!values.length) return;

  try {
    const resp = await fetch(`/api/interpret/${encodeURIComponent(_ticker)}/finalize`, {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({values: values}),
    });
    const body = await resp.json();
    if (body.success) {
      // Mark all cells as finalized in the cache
      values.forEach(function(v) {
        if (row.metrics[v.metric]) row.metrics[v.metric].is_finalized = true;
      });
      renderTable(_rows);
      showToast(values.length + ' value' + (values.length === 1 ? '' : 's') + ' accepted for ' + period.slice(0, 7));
      if (_currentView === 'interpret') loadInterpretData(_ticker);
    } else {
      showToast((body.error && body.error.message) || 'Accept failed', true);
    }
  } catch (e) {
    showToast('Accept failed', true);
  }
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
        if (typeof populateChartMetricSelect === 'function') populateChartMetricSelect();
        const sel = document.getElementById('pattern-metric');
        sel.innerHTML = '';
        metrics.forEach(function(m) {
          const opt = document.createElement('option');
          opt.value = m.key;
          opt.textContent = m.label || m.key;
          sel.appendChild(opt);
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
  // Populate purge-final ticker dropdown if present (ops.html review tab)
  const pfSel = document.getElementById('mn-pf-ticker');
  if (pfSel && pfSel.options.length <= 1) {
    companies.forEach(function(c) {
      const opt = document.createElement('option');
      opt.value = c.ticker;
      opt.textContent = c.ticker;
      pfSel.appendChild(opt);
    });
  }
}

// ── Select company ────────────────────────────────────────────────────────
async function selectCompany(ticker) {
  _ticker = ticker;
  localStorage.setItem('miner-data-ticker', ticker);
  closeDocPanel();
  _expandedDocsPeriod = null;
  _selectedReportIds = {};

  // Reset cached SEC + finalized data on company switch
  _secRows = [];
  _finalizedValues = [];
  _stagedValues = [];
  const addRowStatus = document.getElementById('add-row-status');
  if (addRowStatus) addRowStatus.textContent = '';
  const addRowPanel = document.getElementById('add-row-panel');
  if (addRowPanel && _tableMode !== 'edit') addRowPanel.style.display = 'none';

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

    // Refresh chart if panel is visible
    if (typeof renderMinerChart === 'function') {
      const chartPanel = document.getElementById('chart-panel');
      if (chartPanel && chartPanel.style.display !== 'none') renderMinerChart();
    }

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

// ── Forward-fill period helpers ────────────────────────────────────────────

function _ffAddMonths(yyyymm, n) {
  let year = parseInt(yyyymm.slice(0, 4), 10);
  let month = parseInt(yyyymm.slice(5, 7), 10);
  month += n;
  while (month > 12) { month -= 12; year++; }
  while (month < 1)  { month += 12; year--; }
  return `${year}-${String(month).padStart(2, '0')}`;
}

function _ffPeriodToNorm(period) {
  // 'YYYY-MM-01' or 'YYYY-MM' → 'YYYY-MM'
  if (!period) return null;
  const m = period.match(/^(\d{4})-(\d{2})/);
  return m ? `${m[1]}-${m[2]}` : null;
}

function _ffQuarterToMonths(period) {
  // 'YYYY-Qn' → ['YYYY-MM', 'YYYY-MM', 'YYYY-MM']
  const m = period.match(/^(\d{4})-Q([1-4])$/);
  if (!m) return [];
  const year = parseInt(m[1], 10);
  const start = (parseInt(m[2], 10) - 1) * 3 + 1;
  return [0, 1, 2].map(function(i) {
    return `${year}-${String(start + i).padStart(2, '0')}`;
  });
}

function _ffCurrentMonth() {
  const d = new Date();
  return `${d.getFullYear()}-${String(d.getMonth() + 1).padStart(2, '0')}`;
}

// Returns a map:  period (YYYY-MM) → { metricKey → projectedValue }
// Only covers periods not already present in confirmedRows.
function _computeProjections(confirmedRows) {
  if (!_projEnabled || _projWindow < 1) return {};

  const currentMonth = _ffCurrentMonth();

  // Per-metric: collect (period, value) from confirmed rows
  const metricSeries = {};  // metricKey → [ {period:'YYYY-MM', value:float} ]

  for (const row of confirmedRows) {
    if (row.is_projected) continue;
    const rowPeriod = row.period;
    for (const metric of Object.keys(row.metrics || {})) {
      const m = row.metrics[metric];
      if (!m || m.value == null) continue;
      const v = parseFloat(m.value);
      if (isNaN(v)) continue;

      if (!metricSeries[metric]) metricSeries[metric] = {};

      // Handle quarterly period rows in the timeline
      const qMatch = rowPeriod && rowPeriod.match(/^(\d{4})-Q([1-4])$/);
      if (qMatch) {
        for (const mp of _ffQuarterToMonths(rowPeriod)) {
          if (!metricSeries[metric][mp]) metricSeries[metric][mp] = v;
        }
      } else {
        const mp = _ffPeriodToNorm(rowPeriod);
        if (mp && !metricSeries[metric][mp]) metricSeries[metric][mp] = v;
      }
    }
  }

  // Build set of confirmed periods (normalised) so we don't project over real data
  const confirmedPeriodSet = new Set(
    confirmedRows
      .filter(function(r) { return !r.is_projected; })
      .map(function(r) { return _ffPeriodToNorm(r.period); })
      .filter(Boolean)
  );

  // For each metric, compute MA and generate projected values
  const projMap = {};  // period → { metricKey → value }

  for (const metric of Object.keys(metricSeries)) {
    const sorted = Object.keys(metricSeries[metric]).sort();
    if (!sorted.length) continue;
    const lastPeriod = sorted[sorted.length - 1];
    if (lastPeriod >= currentMonth) continue;

    // MA base: last _projWindow confirmed values
    const base = sorted.slice(-_projWindow);
    const maVal = base.reduce(function(acc, p) {
      return acc + metricSeries[metric][p];
    }, 0) / base.length;

    let fp = _ffAddMonths(lastPeriod, 1);
    while (fp <= currentMonth) {
      if (!confirmedPeriodSet.has(fp)) {
        if (!projMap[fp]) projMap[fp] = {};
        projMap[fp][metric] = maVal;
      }
      fp = _ffAddMonths(fp, 1);
    }
  }

  return projMap;
}

// Build synthetic row objects for projected periods not in confirmedRows,
// and augment existing confirmed rows with projected values for missing metrics.
function _mergeProjections(confirmedRows, projMap) {
  if (!Object.keys(projMap).length) return confirmedRows;

  const confirmedByPeriod = {};
  for (const row of confirmedRows) {
    const np = _ffPeriodToNorm(row.period);
    if (np) confirmedByPeriod[np] = row;
  }

  // Augment confirmed rows that are missing some metrics
  for (const [period, metricVals] of Object.entries(projMap)) {
    if (confirmedByPeriod[period]) {
      const row = confirmedByPeriod[period];
      for (const [metric, val] of Object.entries(metricVals)) {
        if (!row.metrics[metric] || row.metrics[metric].value == null) {
          if (!row._projectedMetrics) row._projectedMetrics = {};
          row._projectedMetrics[metric] = val;
        }
      }
    }
  }

  // Create new synthetic rows for periods with no confirmed data
  const syntheticRows = [];
  for (const [period, metricVals] of Object.entries(projMap)) {
    if (confirmedByPeriod[period]) continue;
    const metrics = {};
    for (const metric of Object.keys(metricVals)) {
      metrics[metric] = {
        value: metricVals[metric],
        unit: '',
        is_projected: true,
        extraction_method: 'projected',
      };
    }
    syntheticRows.push({
      period:        period + '-01',
      period_label:  period,
      is_gap:        false,
      is_reviewed:   false,
      is_projected:  true,
      has_report:    false,
      source_type:   null,
      report_date:   null,
      report_id:     null,
      doc_priority:  null,
      alt_docs:      [],
      metrics:       metrics,
    });
  }

  const merged = confirmedRows.concat(syntheticRows);
  merged.sort(function(a, b) {
    return (a.period || '').localeCompare(b.period || '');
  });
  return merged;
}

function renderTable(allRows) {
  // Inject forward-fill projections before filtering so projected rows are visible
  const baseRows = _projEnabled ? _mergeProjections(allRows, _computeProjections(allRows)) : allRows;
  const rows = applyFilters(baseRows);
  const tbody = document.getElementById('timeline-tbody');

  document.getElementById('table-row-count').textContent =
    `${rows.length} row${rows.length === 1 ? '' : 's'}`;

  if (rows.length === 0) {
    tbody.innerHTML =
      `<tr><td colspan="${METRICS_ORDER.length + 5}" class="md-table-empty">No rows match current filters.</td></tr>`;
    return;
  }

  const parts = [];
  for (const row of rows) {
    const isGap = row.is_gap;
    const isProjRow = !!row.is_projected;
    const rowClass = isGap ? 'row-gap' : (isProjRow ? 'row-projected' : '');
    const isSelected = row.period === _selectedPeriod;
    const selClass = isSelected ? ' selected' : '';

    // Build metric cells
    const metricCells = METRICS_ORDER.map(function(metric) {
      // Check if this specific metric is a projection (either full projected row
      // or a projected metric injected into a confirmed row via _projectedMetrics).
      const isProjMetric = isProjRow ||
        (row._projectedMetrics && row._projectedMetrics[metric] != null);
      const metricColor = _assignMetricColor(metric);

      let m = row.metrics[metric];

      // Overlay projected value for confirmed rows that have a projected metric
      if (!isProjRow && row._projectedMetrics && row._projectedMetrics[metric] != null
          && (!m || m.value == null)) {
        m = {
          value: row._projectedMetrics[metric],
          unit: '',
          extraction_method: 'projected',
          is_projected: true,
        };
      }

      if (isGap) {
        return `<td class="td-gap-label" data-metric="${escapeHtml(metric)}" onclick="if(_tableMode==='edit'){event.stopPropagation();cancelInlineEdit();_beginInlineEdit(this,'${escapeHtml(row.period)}','${escapeHtml(metric)}')}" ondblclick="if(_tableMode==='edit'){event.stopPropagation();cancelInlineEdit();_beginInlineEdit(this,'${escapeHtml(row.period)}','${escapeHtml(metric)}');}">—</td>`;
      }
      if (m && m.value != null) {
        const formatted = escapeHtml(fmtValue(m) || '');

        // Projected cell: faded tint of series color, ~ prefix, no interaction
        if (m.extraction_method === 'projected' || m.is_projected) {
          const r = parseInt(metricColor.slice(1, 3), 16);
          const g = parseInt(metricColor.slice(3, 5), 16);
          const b = parseInt(metricColor.slice(5, 7), 16);
          const projBg = `rgba(${r},${g},${b},0.12)`;
          return `<td class="td-projected" data-metric="${escapeHtml(metric)}"
            style="background:${projBg};font-style:italic;opacity:0.6"
            title="Forward-fill projection (${_projWindow}-month MA)">~${formatted}<span class="badge-method badge-method-proj" style="font-size:0.6rem;margin-left:2px">~</span></td>`;
        }

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
        return `<td class="td-value" data-metric="${escapeHtml(metric)}" title="${tooltip}" ondblclick="inlineEditCell(event,'${escapeHtml(row.period)}','${escapeHtml(metric)}')">${formatted}${m.is_finalized ? finalBadge : badge}</td>`;
      }
      // No value — check if has report (could fill)
      if (row.has_report) {
        return `<td class="td-empty" data-metric="${escapeHtml(metric)}" onclick="if(_tableMode==='edit'){event.stopPropagation();cancelInlineEdit();_beginInlineEdit(this,'${escapeHtml(row.period)}','${escapeHtml(metric)}')}" ondblclick="if(_tableMode==='edit'){event.stopPropagation();cancelInlineEdit();_beginInlineEdit(this,'${escapeHtml(row.period)}','${escapeHtml(metric)}');}">—</td>`;
      }
      return `<td class="td-nodoc" data-metric="${escapeHtml(metric)}" onclick="if(_tableMode==='edit'){event.stopPropagation();cancelInlineEdit();_beginInlineEdit(this,'${escapeHtml(row.period)}','${escapeHtml(metric)}')}" ondblclick="if(_tableMode==='edit'){event.stopPropagation();cancelInlineEdit();_beginInlineEdit(this,'${escapeHtml(row.period)}','${escapeHtml(metric)}');}">—</td>`;
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
    const typeCell = `<td class="td-type-col" title="${altTip}">${escapeHtml(typeLabel)}${altBadge}${altDocs.length > 0 ? '<span class="timeline-type-expand">docs</span>' : ''}</td>`;
    const dateVal = row.report_date ? row.report_date.slice(0, 10) : '—';
    const dateCell = `<td class="td-date-col">${escapeHtml(dateVal)}</td>`;

    const reviewedClass = row.is_reviewed ? ' is-reviewed' : '';
    // Projected rows: no checkbox, no accept button
    const reviewedCb = isProjRow
      ? `<td></td>`
      : `<td style="text-align:center"><input type="checkbox" class="row-select-cb" data-period="${escapeHtml(row.period)}"></td>`;

    // Accept button: finalize all non-null metric values for this period
    const allFinalized = METRICS_ORDER.every(function(m) {
      return !row.metrics[m] || row.metrics[m].value == null || row.metrics[m].is_finalized;
    });
    const hasValues = METRICS_ORDER.some(function(m) {
      return row.metrics[m] && row.metrics[m].value != null;
    });
    const acceptBtn = (!isGap && !isProjRow && hasValues)
      ? `<td style="text-align:center"><button class="btn btn-xs ${allFinalized ? 'btn-secondary' : 'btn-primary'}" title="${allFinalized ? 'All values finalized' : 'Accept all values for this period'}" onclick="acceptRow('${escapeHtml(row.period)}')">${allFinalized ? 'F' : 'Accept'}</button></td>`
      : `<td></td>`;

    parts.push(`
      <tr class="${rowClass}${selClass}${reviewedClass}" data-period="${escapeHtml(row.period)}">
        ${reviewedCb}
        <td class="td-period">${escapeHtml(row.period_label)}</td>
        ${metricCells.join('')}
        ${typeCell}${dateCell}${acceptBtn}
      </tr>`);
    if (_expandedDocsPeriod === row.period) {
      const selectedDocId = _selectedReportIds[row.period] != null ? _selectedReportIds[row.period] : row.report_id;
      const docs = [{
        id: row.report_id,
        source_type: row.source_type,
        source_url: row.source_url,
        document_title: row.document_title,
        report_date: row.report_date,
        priority: row.doc_priority,
        selected: selectedDocId === row.report_id,
      }].concat(altDocs);
      parts.push(`<tr class="timeline-docs-subrow"><td colspan="${METRICS_ORDER.length + 5}">`
        + `<div style="display:flex;align-items:center;gap:0.5rem;flex-wrap:wrap"><strong style="font-size:0.78rem">Documents for ${escapeHtml(row.period_label)}</strong>`
        + `<span style="font-size:0.72rem;color:var(--theme-text-muted)">Click a document below to open the review panel for that source.</span></div>`
        + `<div style="margin-top:0.4rem">`
        + docs.map(function(doc) {
            const cls = doc.selected ? 'timeline-doc-chip selected' : 'timeline-doc-chip';
            const pr = doc.priority != null ? 'p' + doc.priority + '/3' : '';
            const docId = doc.id != null ? String(doc.id) : '';
            const label = doc.document_title || `${srcTypeLabel(doc.source_type || '')}${doc.report_date ? ' · ' + String(doc.report_date).slice(0, 10) : ''}`;
            return `<button type="button" class="${cls}" onclick="event.stopPropagation();openDocumentFromRow('${escapeHtml(row.period)}','${escapeHtml(docId)}')"><span>${escapeHtml(label)}</span><span style="font-size:0.7rem;color:inherit">${escapeHtml(pr)}</span></button>`;
          }).join('')
        + `</div></td></tr>`);
    }
  }
  tbody.innerHTML = parts.join('');
  applyColVisibility();
}

function toggleRowDocs(period) {
  _expandedDocsPeriod = _expandedDocsPeriod === period ? null : period;
  renderTable(_rows);
}

function openDocumentFromRow(period, reportId) {
  if (!_ticker) return;
  const row = _rows.find(function(r) { return r.period === period; });
  const numericReportId = reportId ? parseInt(reportId, 10) : null;
  if (numericReportId != null) _selectedReportIds[period] = numericReportId;
  _selectedPeriod = period;
  document.querySelectorAll('#timeline-tbody tr[data-period]').forEach(function(tr) {
    tr.classList.toggle('selected', tr.getAttribute('data-period') === period);
  });
  if (_expandedDocsPeriod === period) renderTable(_rows);
  const panel = document.getElementById('doc-panel');
  if (panel) {
    panel.classList.add('visible');
    panel.style.display = 'flex';
  }
  const selectedDoc = row ? ([{
    id: row.report_id,
    source_type: row.source_type,
    source_url: row.source_url,
    document_title: row.document_title,
    report_date: row.report_date,
  }].concat(row.alt_docs || [])).find(function(doc) {
    return doc.id === numericReportId;
  }) : null;
  document.getElementById('doc-panel-title-text').textContent =
    `${_ticker} · ${period.slice(0, 7)}${selectedDoc && selectedDoc.document_title ? ' · ' + selectedDoc.document_title : (selectedDoc && selectedDoc.source_type ? ' · ' + selectedDoc.source_type : (row && row.source_type ? ' · ' + row.source_type : ''))}`;
  const _pp = document.getElementById('pattern-panel'); if (_pp) _pp.style.display = 'none';
  document.getElementById('pattern-save-status').textContent = '';
  document.getElementById('apply-result').style.display = 'none';
  const nullMetrics = row
    ? METRICS_ORDER.filter(function(m) { return !row.metrics[m] || row.metrics[m].value == null; })
    : METRICS_ORDER.slice();
  ReviewPanel.openCell(_ticker, period, null, {
    nullMetrics: nullMetrics,
    reportId: numericReportId,
  });
}

async function setMinerTableMode(mode) {
  if (mode !== 'edit' && _editingCell) {
    await flushInlineEdit();
  }
  _tableMode = mode === 'edit' ? 'edit' : 'view';
  const table = document.getElementById('timeline-table');
  if (table) table.classList.toggle('edit-mode', _tableMode === 'edit');
  const viewBtn = document.getElementById('table-mode-view-btn');
  const editBtn = document.getElementById('table-mode-edit-btn');
  const addRowBtn = document.getElementById('add-row-btn');
  const addRowPanel = document.getElementById('add-row-panel');
  const banner = document.getElementById('table-mode-banner');
  const syncBtn = document.getElementById('sync-btn');
  if (viewBtn) viewBtn.classList.toggle('active', _tableMode === 'view');
  if (editBtn) editBtn.classList.toggle('active', _tableMode === 'edit');
  if (addRowBtn) addRowBtn.style.display = _tableMode === 'edit' ? '' : 'none';
  if (addRowPanel && _tableMode === 'view') addRowPanel.style.display = 'none';
  if (syncBtn) syncBtn.style.display = _tableMode === 'edit' ? '' : 'none';
  if (banner) {
    banner.textContent = _tableMode === 'edit'
      ? 'Edit mode: click any metric cell to change it or fill an empty value. Use Add Row for a manual month.'
      : 'View mode: click a row to open its document. Use the checkbox to select rows for batch accept.';
  }
}

function toggleAddRowPanel() {
  if (_tableMode !== 'edit') return;
  const panel = document.getElementById('add-row-panel');
  if (!panel) return;
  panel.style.display = panel.style.display === 'none' || !panel.style.display ? 'block' : 'none';
}

function addManualRow() {
  if (_tableMode !== 'edit') return;
  const input = document.getElementById('add-row-period');
  const status = document.getElementById('add-row-status');
  const ym = input ? input.value : '';
  if (!ym) {
    if (status) { status.textContent = 'Choose a month first.'; status.style.color = 'var(--theme-danger)'; }
    return;
  }
  const period = ym + '-01';
  if (_rows.some(function(r) { return r.period === period; })) {
    if (status) { status.textContent = 'That period already exists.'; status.style.color = 'var(--theme-danger)'; }
    return;
  }
  const metrics = {};
  METRICS_ORDER.forEach(function(metric) { metrics[metric] = null; });
  _rows.unshift({
    period: period,
    period_label: ym,
    metrics: metrics,
    has_report: false,
    is_gap: false,
    source_type: '',
    report_date: '',
    alt_docs: [],
    doc_priority: null,
    is_reviewed: false,
  });
  _rows.sort(function(a, b) { return String(b.period).localeCompare(String(a.period)); });
  if (status) { status.textContent = 'Row created. Click a metric cell to enter a value.'; status.style.color = 'var(--theme-success)'; }
  renderTable(_rows);
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
  if (row && _selectedReportIds[period] == null && row.report_id != null) {
    _selectedReportIds[period] = row.report_id;
  }
  const selectedId = _selectedReportIds[period];
  const selectedDoc = row ? ([{
    id: row.report_id,
    source_type: row.source_type,
    source_url: row.source_url,
    document_title: row.document_title,
    report_date: row.report_date,
  }].concat(row.alt_docs || [])).find(function(doc) {
    return doc.id === selectedId;
  }) : null;
  const nullMetrics = row
    ? METRICS_ORDER.filter(function(m) { return !row.metrics[m] || row.metrics[m].value == null; })
    : METRICS_ORDER.slice();

  // Show doc panel
  const panel = document.getElementById('doc-panel');
  panel.classList.add('visible');
  panel.style.display = 'flex';
  document.getElementById('doc-panel-title-text').textContent =
    `${_ticker} · ${periodLabel}${selectedDoc && selectedDoc.document_title ? ' · ' + selectedDoc.document_title : (selectedDoc && selectedDoc.source_type ? ' · ' + selectedDoc.source_type : (row && row.source_type ? ' · ' + row.source_type : ''))}`;

  // Reset pattern panel state (pattern generator is separate from ReviewPanel)
  const _pp = document.getElementById('pattern-panel'); if (_pp) _pp.style.display = 'none';
  const _ps = document.getElementById('pattern-save-status'); if (_ps) _ps.textContent = '';
  const _ar = document.getElementById('apply-result'); if (_ar) _ar.style.display = 'none';

  // Open ReviewPanel for this cell (no specific metric — shows all analysis)
  ReviewPanel.openCell(_ticker, period, null, {
    nullMetrics: nullMetrics,
    reportId: selectedId != null ? selectedId : null,
  });

  // Intentionally no scrollIntoView — doc-panel is position:fixed at viewport bottom.
}



// ── Close doc panel ────────────────────────────────────────────────────────
function closeDocPanel() {
  _selectedPeriod = null;
  _expandedDocsPeriod = null;
  const panel = document.getElementById('doc-panel');
  panel.classList.remove('visible');
  panel.style.display = 'none';
  _docSearchClose();
  const _pp = document.getElementById('pattern-panel'); if (_pp) _pp.style.display = 'none';
  ReviewPanel.close();
  // Clear row selection
  document.querySelectorAll('#timeline-tbody tr.selected').forEach(function(r) {
    r.classList.remove('selected');
  });
  document.querySelectorAll('#review-tbody tr.selected').forEach(function(r) {
    r.classList.remove('selected');
  });
  if (typeof _reviewIdx !== 'undefined') _reviewIdx = -1;
}

// ── Panel 5.2 in-panel search (Ctrl+F) ────────────────────────────────────
var _searchMatches = [];
var _searchIdx = 0;

function _docSearchGetText() {
  // rp-doc-text is a <pre> inside #miner-review-panel
  return document.querySelector('#miner-review-panel .rp-doc-text') || null;
}

function _docSearchOpen() {
  const bar = document.getElementById('doc-search-bar');
  if (!bar) return;
  bar.classList.add('visible');
  const input = document.getElementById('doc-search-input');
  if (input) { input.value = ''; input.focus(); }
  _searchMatches = [];
  _searchIdx = 0;
  _docSearchUpdateCount();
}

function _docSearchClose() {
  const bar = document.getElementById('doc-search-bar');
  if (bar) bar.classList.remove('visible');
  _docSearchClearHighlights();
  _searchMatches = [];
  _searchIdx = 0;
  _docSearchUpdateCount();
}

function _docSearchClearHighlights() {
  const el = _docSearchGetText();
  if (!el) return;
  // Replace <mark> wrappers with their text content
  el.querySelectorAll('mark.doc-search-match').forEach(function(m) {
    m.replaceWith(document.createTextNode(m.textContent));
  });
  el.normalize();
}

function _docSearchUpdateCount() {
  const el = document.getElementById('doc-search-count');
  if (!el) return;
  el.textContent = _searchMatches.length
    ? (_searchIdx + 1) + ' / ' + _searchMatches.length
    : (document.getElementById('doc-search-input') && document.getElementById('doc-search-input').value ? '0' : '');
}

function _docSearchRun() {
  _docSearchClearHighlights();
  _searchMatches = [];
  _searchIdx = 0;
  const input = document.getElementById('doc-search-input');
  const query = input ? input.value : '';
  if (!query) { _docSearchUpdateCount(); return; }

  const el = _docSearchGetText();
  if (!el) return;

  // Walk text nodes only — preserve existing HTML spans
  const walker = document.createTreeWalker(el, NodeFilter.SHOW_TEXT);
  const nodes = [];
  let n;
  while ((n = walker.nextNode())) nodes.push(n);

  const re = new RegExp(query.replace(/[.*+?^${}()|[\]\\]/g, '\\$&'), 'gi');
  nodes.forEach(function(node) {
    const text = node.nodeValue;
    const parts = [];
    let last = 0, m2;
    re.lastIndex = 0;
    while ((m2 = re.exec(text)) !== null) {
      if (m2.index > last) parts.push(document.createTextNode(text.slice(last, m2.index)));
      const mark = document.createElement('mark');
      mark.className = 'doc-search-match';
      mark.textContent = m2[0];
      parts.push(mark);
      _searchMatches.push(mark);
      last = re.lastIndex;
    }
    if (parts.length) {
      if (last < text.length) parts.push(document.createTextNode(text.slice(last)));
      const frag = document.createDocumentFragment();
      parts.forEach(function(p) { frag.appendChild(p); });
      node.parentNode.replaceChild(frag, node);
    }
  });

  if (_searchMatches.length) _docSearchScroll(0);
  _docSearchUpdateCount();
}

function _docSearchScroll(idx) {
  if (!_searchMatches.length) return;
  _searchMatches.forEach(function(m) { m.classList.remove('current'); });
  _searchIdx = ((idx % _searchMatches.length) + _searchMatches.length) % _searchMatches.length;
  const cur = _searchMatches[_searchIdx];
  cur.classList.add('current');
  cur.scrollIntoView({ behavior: 'smooth', block: 'nearest' });
  _docSearchUpdateCount();
}

document.addEventListener('DOMContentLoaded', function() {
  const input = document.getElementById('doc-search-input');
  if (input) {
    input.addEventListener('input', _docSearchRun);
    input.addEventListener('keydown', function(e) {
      if (e.key === 'Enter') {
        e.preventDefault();
        if (_searchMatches.length) _docSearchScroll(e.shiftKey ? _searchIdx - 1 : _searchIdx + 1);
      } else if (e.key === 'Escape') {
        _docSearchClose();
      }
    });
  }
  const prevBtn = document.getElementById('doc-search-prev');
  if (prevBtn) prevBtn.addEventListener('click', function() { _docSearchScroll(_searchIdx - 1); });
  const nextBtn = document.getElementById('doc-search-next');
  if (nextBtn) nextBtn.addEventListener('click', function() { _docSearchScroll(_searchIdx + 1); });
  const closeBtn = document.getElementById('doc-search-close');
  if (closeBtn) closeBtn.addEventListener('click', _docSearchClose);
});

// Escape key closes doc panel (or search bar if open)
document.addEventListener('keydown', function(e) {
  if (e.key === 'Escape') {
    const bar = document.getElementById('doc-search-bar');
    if (bar && bar.classList.contains('visible')) { _docSearchClose(); return; }
    if (_selectedPeriod) closeDocPanel();
  }
  // Ctrl+F opens in-panel search when panel is visible
  if ((e.ctrlKey || e.metaKey) && e.key === 'f') {
    const panel = document.getElementById('doc-panel');
    if (panel && panel.classList.contains('visible')) {
      e.preventDefault();
      _docSearchOpen();
    }
  }
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
  const selectedId = _selectedReportIds[_selectedPeriod];
  const selectedDoc = row ? ([{
    id: row.report_id,
    source_url: row.source_url,
  }].concat(row.alt_docs || [])).find(function(doc) { return doc.id === selectedId; }) : null;
  const url = (selectedDoc && selectedDoc.source_url) || (row && row.source_url);
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
    loadExtractionSuggestions(_ticker, null);
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
    parts.push(`<tr data-period="${escapeHtml(row.period)}" data-report-id="${row.report_id != null ? row.report_id : ''}">
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

async function doExtract() {
  if (!_ticker) { showToast('Select a company first', true); return; }
  const scopeEl = document.getElementById('extract-scope');
  const scope = scopeEl ? scopeEl.value : 'both';
  const customPromptEl = document.getElementById('custom-prompt-input');
  const customPromptPanel = document.getElementById('custom-prompt-panel');
  const customPrompt = (customPromptEl && customPromptPanel && customPromptPanel.style.display !== 'none')
    ? customPromptEl.value.trim() : '';
  const btn = document.getElementById('extract-btn');
  const statusEl = document.getElementById('extract-status');
  btn.disabled = true;
  statusEl.textContent = 'Starting…';
  try {
    const payload = {ticker: _ticker, force: true, source_scope: scope};
    if (customPrompt) payload.custom_prompt = customPrompt;
    const resp = await fetch('/api/operations/interpret', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify(payload),
    });
    if (!resp.ok) throw new Error(`HTTP ${resp.status}`);
    const body = await resp.json();
    if (!body.success) throw new Error(body.error?.message || 'Failed');
    const taskId = body.data.task_id;
    await _pollExtractionProgress(taskId, statusEl);
    await selectCompany(_ticker);
    if (_currentView === 'sec' || _currentView === 'interpret') {
      _secRows = [];
      await loadSecData(_ticker);
    }
    if (_currentView === 'interpret') loadInterpretData(_ticker);
  } catch (err) {
    statusEl.textContent = `Error: ${err.message}`;
    showToast(`Extraction failed: ${err.message}`, true);
  } finally {
    btn.disabled = false;
  }
}

async function _pollExtractionProgress(taskId, statusEl) {
  for (let i = 0; i < 600; i++) {
    await new Promise(r => setTimeout(r, 2000));
    try {
      const resp = await fetch(`/api/operations/interpret/${encodeURIComponent(taskId)}/progress`);
      if (!resp.ok) break;
      const body = await resp.json();
      const prog = body.data || {};
      const status = prog.status || 'running';
      statusEl.textContent = `${prog.reports_processed || 0}/${prog.reports_total || '?'} reports, ${prog.data_points || 0} data points`;
      if (status === 'complete') { statusEl.textContent += ' — done'; if (_ticker) loadExtractionSuggestions(_ticker, prog.run_id || null); break; }
      if (status === 'error') { statusEl.textContent = `Error: ${prog.error_message || 'unknown'}`; break; }
    } catch (_) { break; }
  }
}

// ── Pattern Suggestion Panel ──────────────────────────────────────────────────

async function loadExtractionSuggestions(ticker, runId) {
  if (!ticker) return;
  const panel = document.getElementById('suggestion-panel');
  const list  = document.getElementById('suggestion-list');
  const badge = document.getElementById('suggestion-count-badge');
  if (!panel || !list) return;
  try {
    const url = '/api/suggestions/' + encodeURIComponent(ticker) + (runId ? '?run_id=' + runId : '');
    const resp = await fetch(url, { cache: 'no-store' });
    if (!resp.ok) return;
    const d = await resp.json();
    const sugs = (d.data && d.data.suggestions) || [];
    if (sugs.length === 0) { panel.style.display = 'none'; return; }
    if (badge) badge.textContent = sugs.length;
    list.innerHTML = sugs.map(s => _renderSuggestionCard(s, ticker)).join('');
    panel.style.display = '';
  } catch (_e) {}
}

function _renderSuggestionCard(sug, ticker) {
  const signalClass  = sug.signal === 'found' ? 'sug-found' : 'sug-missed';
  const hintAreaId   = 'sug-hint-area-'   + sug.id;
  const promptAreaId = 'sug-prompt-area-' + sug.id;
  return `<div class="sug-card ${signalClass}" id="sug-card-${sug.id}">`
    + `<div style="display:flex;gap:0.5rem;align-items:center;flex-wrap:wrap">`
    + `<span class="badge">${sug.signal}</span>`
    + `<span class="badge" style="opacity:.7">${escapeHtml(sug.pattern_type || '')}</span>`
    + `<span style="font-size:0.78rem;font-weight:600">${escapeHtml(sug.metric || '')}</span>`
    + `<span style="font-size:0.72rem;color:var(--theme-text-muted)">freq=${sug.frequency || 1} reports=${sug.report_count || 1}</span>`
    + `</div>`
    + `<div class="sug-pattern" style="margin-top:0.3rem">${escapeHtml((sug.text_window || '').slice(0, 100))}</div>`
    + `<div class="sug-pattern" style="color:var(--theme-text-muted)">${escapeHtml((sug.normalized_pattern || '').slice(0, 120))}</div>`
    + `<div style="display:flex;gap:0.4rem;margin-top:0.35rem;flex-wrap:wrap">`
    + `<button class="btn btn-xs btn-secondary" onclick="openSugApply('${sug.id}','hint')">+ Ticker hint</button>`
    + `<button class="btn btn-xs btn-secondary" onclick="openSugApply('${sug.id}','prompt')">+ Metric prompt</button>`
    + `</div>`
    + `<div class="sug-apply-area" id="${hintAreaId}">`
    + `<textarea id="ta-hint-${sug.id}">${escapeHtml(sug.suggested_hint_addition || '')}</textarea>`
    + `<div style="display:flex;gap:0.4rem;margin-top:0.3rem">`
    + `<button class="btn btn-xs btn-primary" onclick="saveSugApply('${ticker}','ticker_hint','','ta-hint-${sug.id}','${sug.id}')">Save to ticker hint</button>`
    + `<button class="btn btn-xs btn-secondary" onclick="document.getElementById('${hintAreaId}').style.display='none'">Cancel</button>`
    + `</div></div>`
    + `<div class="sug-apply-area" id="${promptAreaId}">`
    + `<textarea id="ta-prompt-${sug.id}">${escapeHtml(sug.suggested_prompt_addition || '')}</textarea>`
    + `<div style="display:flex;gap:0.4rem;margin-top:0.3rem">`
    + `<button class="btn btn-xs btn-primary" onclick="saveSugApply('${ticker}','metric_prompt','${escapeHtml(sug.metric || '')}','ta-prompt-${sug.id}','${sug.id}')">Save to metric prompt</button>`
    + `<button class="btn btn-xs btn-secondary" onclick="document.getElementById('${promptAreaId}').style.display='none'">Cancel</button>`
    + `</div></div>`
    + `</div>`;
}

function openSugApply(sugId, areaType) {
  const hintArea   = document.getElementById('sug-hint-area-'   + sugId);
  const promptArea = document.getElementById('sug-prompt-area-' + sugId);
  if (areaType === 'hint') {
    if (hintArea)   hintArea.style.display   = hintArea.style.display   === 'none' ? '' : 'none';
    if (promptArea) promptArea.style.display = 'none';
  } else {
    if (promptArea) promptArea.style.display = promptArea.style.display === 'none' ? '' : 'none';
    if (hintArea)   hintArea.style.display   = 'none';
  }
}

async function saveSugApply(ticker, target, metric, taId, sugId) {
  const ta = document.getElementById(taId);
  if (!ta) return;
  const appendText = ta.value.trim();
  if (!appendText) return;
  const body = { target, append_text: appendText };
  if (metric) body.metric = metric;
  const card = document.getElementById('sug-card-' + sugId);
  try {
    const resp = await fetch('/api/suggestions/' + encodeURIComponent(ticker) + '/apply', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(body),
    });
    const d = await resp.json();
    if (d.success) {
      if (card) {
        const ok = document.createElement('div');
        ok.style.cssText = 'font-size:0.72rem;color:var(--theme-success,#16a34a);margin-top:0.3rem';
        ok.textContent = 'Saved to ' + target.replace('_', ' ');
        card.appendChild(ok);
      }
      const areaEl = document.getElementById('sug-' + (target === 'ticker_hint' ? 'hint' : 'prompt') + '-area-' + sugId);
      if (areaEl) areaEl.style.display = 'none';
    } else {
      _sugApplyError(card, (d.error && d.error.message) || 'Save failed');
    }
  } catch (_err) {
    _sugApplyError(card, 'Network error');
  }
}

function _sugApplyError(card, msg) {
  if (!card) return;
  const el = document.createElement('div');
  el.style.cssText = 'font-size:0.72rem;color:var(--theme-danger,#ef4444);margin-top:0.3rem';
  el.textContent = msg;
  card.appendChild(el);
}

function closeSuggestionPanel() {
  const panel = document.getElementById('suggestion-panel');
  if (panel) panel.style.display = 'none';
}

// ── End Pattern Suggestion Panel ──────────────────────────────────────────────

function toggleCustomPrompt() {
  const panel = document.getElementById('custom-prompt-panel');
  const btn = document.getElementById('custom-prompt-toggle');
  if (!panel) return;
  const visible = panel.style.display !== 'none';
  panel.style.display = visible ? 'none' : '';
  if (btn) btn.textContent = visible ? 'Custom prompt' : 'Hide custom prompt';
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

async function previewSalesBtcDerive() {
  const statusEl = document.getElementById('sales-derive-status');
  const previewEl = document.getElementById('sales-derive-preview');
  const applyBtn = document.getElementById('apply-sales-derive-btn');
  statusEl.textContent = 'Loading preview...';
  previewEl.style.display = 'none';
  applyBtn.style.display = 'none';

  try {
    const res = await fetch('/api/operations/derive-sales-btc', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({ticker: _ticker, dry_run: true}),
    });
    const data = await res.json();
    const tbody = document.getElementById('sales-derive-tbody');
    tbody.innerHTML = '';
    data.rows.forEach(r => {
      const tr = document.createElement('tr');
      const isDerived = r.status === 'would_derive';
      tr.innerHTML = `
        <td>${r.period || ''}</td>
        <td>${r.prev_holdings != null ? r.prev_holdings.toFixed(4) : '&mdash;'}</td>
        <td>${r.production != null ? r.production.toFixed(4) : '&mdash;'}</td>
        <td>${r.curr_holdings != null ? r.curr_holdings.toFixed(4) : '&mdash;'}</td>
        <td>${isDerived ? r.value.toFixed(4) : '&mdash;'}</td>
        <td>${r.status}${r.reason ? ` (${r.reason})` : ''}</td>
      `;
      tbody.appendChild(tr);
    });
    const wouldDerive = data.rows.filter(r => r.status === 'would_derive').length;
    statusEl.textContent = `${wouldDerive} period(s) would be derived, ${data.skipped} skipped.`;
    previewEl.style.display = wouldDerive > 0 ? '' : 'none';
    applyBtn.style.display = wouldDerive > 0 ? '' : 'none';
  } catch (e) {
    statusEl.textContent = 'Preview failed.';
  }
}

async function applySalesBtcDerive() {
  const statusEl = document.getElementById('sales-derive-status');
  const applyBtn = document.getElementById('apply-sales-derive-btn');
  statusEl.textContent = 'Applying...';
  applyBtn.disabled = true;

  try {
    const res = await fetch('/api/operations/derive-sales-btc', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({ticker: _ticker, dry_run: false}),
    });
    const data = await res.json();
    statusEl.textContent = `Done: ${data.derived} derived, ${data.skipped} skipped.`;
    applyBtn.style.display = 'none';
    document.getElementById('sales-derive-preview').style.display = 'none';
    loadInterpretData(_ticker);
  } catch (e) {
    statusEl.textContent = 'Apply failed.';
    applyBtn.disabled = false;
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
