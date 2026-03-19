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
  if (!_editingCell) return true;
  const input = _editingCell.td.querySelector('input');
  const val = input ? parseFloat(input.value) : NaN;
  if (isNaN(val) || val < 0) {
    cancelInlineEdit();
    return true;
  }
  const { period, metric, td, original } = _editingCell;
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
      return true;
    } else {
      showToast((body.error && body.error.message) || 'Save failed', true);
      return false;
    }
  } catch (e) {
    showToast('Save failed', true);
    return false;
  }
}

async function syncEdits() {
  const ok = await flushInlineEdit();
  if (ok) {
    showToast('All edits saved to database');
  }
}

// ── CSV Paste (2.5.2.2.1) ─────────────────────────────────────────────────
let _csvPasteValues = [];

function _ensureCsvPasteModals() {
  if (document.getElementById('csv-paste-modal-step1')) return;
  const html = `
  <div id="csv-paste-modal-step1" class="csv-paste-modal-overlay" style="display:none">
    <div class="csv-paste-modal">
      <h3>Paste CSV \u2014 Step 1: Input</h3>
      <div style="font-size:0.78rem;color:var(--theme-text-muted);margin-bottom:0.5rem">
        Paste tab-separated or comma-separated data. First column is period (<code>YYYY-MM</code>).
        Remaining column headers must be exact metric keys (e.g. <code>production_btc</code>, <code>holdings_btc</code>).
        Empty cells are skipped.
      </div>
      <textarea id="csv-paste-textarea" rows="10" placeholder="Month\tproduction_btc\tholdings_btc\n2024-01\t751.2\t15220.0\n2024-02\t832.5\t"></textarea>
      <div id="csv-paste-step1-error" class="csv-paste-error" style="display:none"></div>
      <div style="display:flex;gap:0.5rem;margin-top:0.75rem">
        <button class="btn btn-primary btn-sm" onclick="previewCsvPaste()">Preview</button>
        <button class="btn btn-secondary btn-sm" onclick="closeCsvPasteModal()">Cancel</button>
      </div>
    </div>
  </div>
  <div id="csv-paste-modal-step2" class="csv-paste-modal-overlay" style="display:none">
    <div class="csv-paste-modal">
      <h3>Paste CSV \u2014 Step 2: Preview</h3>
      <div id="csv-paste-preview-summary" style="font-size:0.8rem;color:var(--theme-text-muted);margin-bottom:0.5rem"></div>
      <div style="overflow-x:auto">
        <table class="csv-paste-preview-table" id="csv-paste-preview-table">
          <thead><tr id="csv-paste-preview-thead"></tr></thead>
          <tbody id="csv-paste-preview-tbody"></tbody>
        </table>
      </div>
      <div id="csv-paste-step2-error" class="csv-paste-error" style="display:none"></div>
      <div style="display:flex;gap:0.5rem;margin-top:0.75rem;align-items:center">
        <button class="btn btn-primary btn-sm" id="csv-paste-commit-btn" onclick="commitCsvPaste()">Commit 0 values</button>
        <button class="btn btn-secondary btn-sm" onclick="backCsvPaste()">Back</button>
        <button class="btn btn-secondary btn-sm" onclick="closeCsvPasteModal()">Cancel</button>
      </div>
    </div>
  </div>`;
  const wrapper = document.createElement('div');
  wrapper.innerHTML = html;
  Array.from(wrapper.children).forEach(function(el) { document.body.appendChild(el); });
}

function openCsvPasteModal() {
  if (!_ticker) { showToast('Select a company first', true); return; }
  _ensureCsvPasteModals();
  const step1 = document.getElementById('csv-paste-modal-step1');
  const step2 = document.getElementById('csv-paste-modal-step2');
  const ta    = document.getElementById('csv-paste-textarea');
  const errEl = document.getElementById('csv-paste-step1-error');
  if (ta)    ta.value = '';
  if (errEl) { errEl.textContent = ''; errEl.style.display = 'none'; }
  if (step1) step1.style.display = '';
  if (step2) step2.style.display = 'none';
  _csvPasteValues = [];
  if (ta) ta.focus();
}

function closeCsvPasteModal() {
  const step1 = document.getElementById('csv-paste-modal-step1');
  const step2 = document.getElementById('csv-paste-modal-step2');
  if (step1) step1.style.display = 'none';
  if (step2) step2.style.display = 'none';
  _csvPasteValues = [];
}

function backCsvPaste() {
  const step1 = document.getElementById('csv-paste-modal-step1');
  const step2 = document.getElementById('csv-paste-modal-step2');
  if (step1) step1.style.display = '';
  if (step2) step2.style.display = 'none';
}

function _parseCsvPaste(text) {
  const lines = text.split('\n').map(function(l) { return l.trimEnd(); }).filter(function(l) { return l.trim() !== ''; });
  if (lines.length < 2) {
    return {values: [], warnings: [], errors: ['Paste at least a header row and one data row.']};
  }
  const sep = lines[0].includes('\t') ? '\t' : ',';
  const headers = lines[0].split(sep).map(function(h) { return h.trim(); });
  if (headers.length < 2) {
    return {values: [], warnings: [], errors: ['Need at least two columns: period + one metric key.']};
  }
  const metricCols = headers.slice(1);
  const unknownMetrics = metricCols.filter(function(k) { return !METRICS_ORDER.includes(k); });
  if (unknownMetrics.length > 0) {
    return {values: [], warnings: [], errors: ['Unknown metric key' + (unknownMetrics.length > 1 ? 's' : '') + ': ' + unknownMetrics.join(', ') + '. Valid: ' + METRICS_ORDER.join(', ')]};
  }
  const values = [];
  const warnings = [];
  for (let i = 1; i < lines.length; i++) {
    const cells = lines[i].split(sep);
    const rawPeriod = cells[0] ? cells[0].trim() : '';
    const periodMatch = rawPeriod.match(/^(\d{4})-(\d{2})(-\d{2})?$/);
    if (!periodMatch) {
      warnings.push('Row ' + (i + 1) + ': invalid period "' + rawPeriod + '", skipped.');
      continue;
    }
    const period = periodMatch[1] + '-' + periodMatch[2] + '-01';
    for (let j = 0; j < metricCols.length; j++) {
      const rawVal = (cells[j + 1] || '').trim();
      if (!rawVal) continue;
      const val = parseFloat(rawVal.replace(/,/g, ''));
      if (isNaN(val)) {
        warnings.push('Row ' + (i + 1) + ', ' + metricCols[j] + ': "' + rawVal + '" is not a number, skipped.');
        continue;
      }
      values.push({period: period, metric: metricCols[j], value: val});
    }
  }
  return {values: values, warnings: warnings, errors: []};
}

function previewCsvPaste() {
  const ta    = document.getElementById('csv-paste-textarea');
  const errEl = document.getElementById('csv-paste-step1-error');
  const text  = ta ? ta.value.trim() : '';
  if (!text) {
    if (errEl) { errEl.textContent = 'Paste some data first.'; errEl.style.display = ''; }
    return;
  }
  const result = _parseCsvPaste(text);
  if (result.errors.length > 0) {
    if (errEl) { errEl.textContent = result.errors.join(' '); errEl.style.display = ''; }
    return;
  }
  if (errEl) { errEl.textContent = ''; errEl.style.display = 'none'; }
  _csvPasteValues = result.values;

  const periodsSet = new Set(_csvPasteValues.map(function(v) { return v.period; }));
  const metricsSet = new Set(_csvPasteValues.map(function(v) { return v.metric; }));
  const periods    = Array.from(periodsSet).sort().reverse();
  const metrics    = Array.from(metricsSet);
  const nValues    = _csvPasteValues.length;
  const nPeriods   = periodsSet.size;

  const summaryEl = document.getElementById('csv-paste-preview-summary');
  const commitBtn = document.getElementById('csv-paste-commit-btn');
  const thead     = document.getElementById('csv-paste-preview-thead');
  const tbody     = document.getElementById('csv-paste-preview-tbody');

  if (summaryEl) {
    let txt = nValues + ' value' + (nValues === 1 ? '' : 's') + ' across ' + nPeriods + ' period' + (nPeriods === 1 ? '' : 's');
    if (result.warnings.length > 0) {
      txt += ' (' + result.warnings.length + ' skipped \u2014 ' + result.warnings.slice(0, 2).join('; ') + (result.warnings.length > 2 ? '\u2026' : '') + ')';
    }
    summaryEl.textContent = txt;
  }
  if (commitBtn) {
    commitBtn.textContent = 'Commit ' + nValues + ' value' + (nValues === 1 ? '' : 's');
    commitBtn.disabled = nValues === 0;
  }

  const valMap = {};
  _csvPasteValues.forEach(function(v) { valMap[v.period + '|' + v.metric] = v.value; });

  if (thead) {
    thead.innerHTML = '<th>Period</th>' + metrics.map(function(m) { return '<th>' + escapeHtml(m) + '</th>'; }).join('');
  }
  if (tbody) {
    tbody.innerHTML = periods.map(function(period) {
      const cells = metrics.map(function(m) {
        const v = valMap[period + '|' + m];
        return '<td>' + (v != null ? v.toLocaleString() : '\u2014') + '</td>';
      }).join('');
      return '<tr><td>' + escapeHtml(period.slice(0, 7)) + '</td>' + cells + '</tr>';
    }).join('');
  }

  document.getElementById('csv-paste-modal-step1').style.display = 'none';
  document.getElementById('csv-paste-modal-step2').style.display = '';
}

async function commitCsvPaste() {
  if (!_ticker || _csvPasteValues.length === 0) return;
  const commitBtn = document.getElementById('csv-paste-commit-btn');
  const errEl     = document.getElementById('csv-paste-step2-error');
  if (commitBtn) commitBtn.disabled = true;
  if (errEl) { errEl.textContent = ''; errEl.style.display = 'none'; }
  try {
    const resp = await fetch('/api/interpret/' + encodeURIComponent(_ticker) + '/finalize', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({values: _csvPasteValues}),
    });
    const body = await resp.json();
    if (body.success) {
      const count = body.data.count;
      closeCsvPasteModal();
      showToast(count + ' value' + (count === 1 ? '' : 's') + ' finalized');
      selectCompany(_ticker);
    } else {
      const msg = (body.error && body.error.message) || 'Commit failed';
      if (errEl) { errEl.textContent = msg; errEl.style.display = ''; }
      else showToast(msg, true);
    }
  } catch (e) {
    showToast('Commit failed', true);
  } finally {
    if (commitBtn) commitBtn.disabled = false;
  }
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

function renderTable(allRows) {
  const rows = applyFilters(allRows);
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
    const rowClass = isGap ? 'row-gap' : '';
    const isSelected = row.period === _selectedPeriod;
    const selClass = isSelected ? ' selected' : '';

    // Build metric cells
    const metricCells = METRICS_ORDER.map(function(metric) {
      let m = row.metrics[metric];

      if (isGap) {
        return `<td class="td-gap-label" data-metric="${escapeHtml(metric)}" onclick="if(_tableMode==='edit'){event.stopPropagation();cancelInlineEdit();_beginInlineEdit(this,'${escapeHtml(row.period)}','${escapeHtml(metric)}')}" ondblclick="if(_tableMode==='edit'){event.stopPropagation();cancelInlineEdit();_beginInlineEdit(this,'${escapeHtml(row.period)}','${escapeHtml(metric)}');}">—</td>`;
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
  const viewBtn    = document.getElementById('table-mode-view-btn');
  const editBtn    = document.getElementById('table-mode-edit-btn');
  const addRowBtn  = document.getElementById('add-row-btn');
  const addRowPanel = document.getElementById('add-row-panel');
  const banner     = document.getElementById('table-mode-banner');
  const syncBtn    = document.getElementById('sync-btn');
  const csvPasteBtn = document.getElementById('csv-paste-btn');
  if (viewBtn)    viewBtn.classList.toggle('active', _tableMode === 'view');
  if (editBtn)    editBtn.classList.toggle('active', _tableMode === 'edit');
  if (addRowBtn)  addRowBtn.style.display  = _tableMode === 'edit' ? '' : 'none';
  if (addRowPanel && _tableMode === 'view') addRowPanel.style.display = 'none';
  if (syncBtn)    syncBtn.style.display    = _tableMode === 'edit' ? '' : 'none';
  if (csvPasteBtn) csvPasteBtn.style.display = _tableMode === 'edit' ? '' : 'none';
  if (banner) {
    banner.textContent = _tableMode === 'edit'
      ? 'Edit mode: click any metric cell to change it or fill an empty value. Use Add Row for a manual month. Use Paste CSV to bulk-enter values.'
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
    + `<div class="sug-pattern" style="margin-top:0.3rem">${escapeHtml(sug.text_window || '')}</div>`
    + `<div class="sug-pattern sug-pattern-dim">${escapeHtml(sug.normalized_pattern || '')}</div>`
    + _renderSugExamples(sug)
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

function _renderSugExamples(sug) {
  const examples = Array.isArray(sug.examples) ? sug.examples : [];
  const extras = examples.filter(e => e !== sug.text_window);
  if (!extras.length) return '';
  const listId   = 'sug-examples-' + sug.id;
  const toggleId = 'sug-examples-toggle-' + sug.id;
  const rows = extras.map(e =>
    `<div class="sug-pattern sug-example-row">${escapeHtml(e)}</div>`
  ).join('');
  const n = extras.length;
  return `<div>`
    + `<button class="btn btn-xs sug-examples-toggle" id="${toggleId}" `
    + `onclick="toggleSugExamples('${sug.id}')">`
    + `show ${n} more example${n > 1 ? 's' : ''}`
    + `</button>`
    + `<div class="sug-examples-list" id="${listId}">${rows}</div>`
    + `</div>`;
}

function toggleSugExamples(sugId) {
  const list = document.getElementById('sug-examples-' + sugId);
  const btn  = document.getElementById('sug-examples-toggle-' + sugId);
  if (!list || !btn) return;
  const open = list.style.display === 'block';
  list.style.display = open ? 'none' : 'block';
  const count = list.querySelectorAll('.sug-example-row').length;
  btn.textContent = open
    ? `show ${count} more example${count > 1 ? 's' : ''}`
    : `hide examples`;
}

function openSugApply(sugId, areaType) {
  const hintArea   = document.getElementById('sug-hint-area-'   + sugId);
  const promptArea = document.getElementById('sug-prompt-area-' + sugId);
  if (areaType === 'hint') {
    if (hintArea)   hintArea.style.display   = hintArea.style.display   === 'block' ? 'none' : 'block';
    if (promptArea) promptArea.style.display = 'none';
  } else {
    if (promptArea) promptArea.style.display = promptArea.style.display === 'block' ? 'none' : 'block';
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
      const areaEl = document.getElementById(
        'sug-' + (target === 'ticker_hint' ? 'hint' : 'prompt') + '-area-' + sugId
      );
      if (areaEl) areaEl.style.display = 'none';
      if (card) {
        const feedback = document.createElement('div');
        feedback.className = 'sug-save-feedback';
        const preview     = d.data && d.data.new_prompt_preview;
        const savedMetric = d.data && d.data.metric;
        let html = 'Saved to ' + target.replace('_', ' ');
        if (preview) {
          html += '<div class="sug-prompt-preview">'
            + escapeHtml(preview) + (preview.length >= 300 ? '...' : '')
            + '</div>';
        }
        if (target === 'metric_prompt' && savedMetric) {
          html += ' <a class="sug-edit-link" href="/ops?tab=prompts&metric='
            + encodeURIComponent(savedMetric) + '" target="_blank">Edit in prompt editor</a>';
        }
        feedback.innerHTML = html;
        card.appendChild(feedback);
      }
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
