// ── Review tab ────────────────────────────────────────────────────────────────
let _reviewLoaded = false;
let _reviewItems = [];
let _reviewIdx = -1;
let _reviewExpandedIdx = -1;
let _reviewDocsCache = {};

async function promoteDataPointsToReview() {
  const btn = document.getElementById('rv-promote-btn');
  const statusEl = document.getElementById('rv-promote-status');
  const ticker = (document.getElementById('rv-ticker')?.value || '').trim() || null;
  if (btn) btn.disabled = true;
  if (statusEl) statusEl.textContent = 'Promoting...';
  try {
    const body = {};
    if (ticker) body.ticker = ticker;
    const resp = await fetch('/api/operations/promote-to-review', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify(body),
    });
    const data = await resp.json();
    if (!resp.ok || !data.success) throw new Error(data.error?.message || ('HTTP ' + resp.status));
    const n = data.data.promoted;
    if (statusEl) statusEl.textContent = n + ' row' + (n === 1 ? '' : 's') + ' queued';
    loadReview();
  } catch (err) {
    if (statusEl) statusEl.textContent = 'Error: ' + String(err);
  } finally {
    if (btn) btn.disabled = false;
  }
}

async function loadReview() {
  _reviewLoaded = true;
  const statusEl = document.getElementById('rv-status');
  const tickerEl = document.getElementById('rv-ticker');
  const metricEl = document.getElementById('rv-metric');
  const sourceEl = document.getElementById('rv-source-type');
  const params = new URLSearchParams();
  if (statusEl && statusEl.value) params.set('status', statusEl.value);
  if (tickerEl && tickerEl.value.trim()) params.set('ticker', tickerEl.value.trim().toUpperCase());
  if (metricEl && metricEl.value) params.set('metric', metricEl.value);
  if (sourceEl && sourceEl.value) params.set('source_type', sourceEl.value);
  const resp = await fetch('/api/review?' + params.toString());
  if (!resp.ok) { showToast('Failed to load review queue', true); return; }
  const data = await resp.json();
  _reviewItems = data.data?.items || [];
  const countEl = document.getElementById('rv-count');
  if (countEl) countEl.textContent = _reviewItems.length + ' items';
  renderReview();
  _rvPfPopulateTickers();
}

function _rvPfPopulateTickers() {
  const sel = document.getElementById('rv-pf-ticker');
  if (!sel || sel.options.length > 1) return; // already populated
  fetch('/api/companies').then(r => r.ok ? r.json() : null).then(data => {
    if (!data || !data.success) return;
    data.data.forEach(c => {
      const opt = document.createElement('option');
      opt.value = c.ticker;
      opt.textContent = c.ticker;
      sel.appendChild(opt);
    });
  }).catch(() => {});
}

function _sourceTypeBadge(sourceType) {
  if (!sourceType) return '';
  const label = (window._SOURCE_TYPES && window._SOURCE_TYPES[sourceType]) || sourceType;
  const tier = sourceType.startsWith('edgar') ? 'sec'
    : (sourceType === 'ir_press_release' || sourceType.startsWith('archive')) ? 'ir'
    : 'wire';
  return '<span class="source-tier-badge source-tier-' + escapeAttr(tier) + '">' + escapeHtml(label) + '</span>';
}

function _statusBadge(status) {
  if (status === 'APPROVED' || status === 'EDITED') return '<span class="badge-status-final">Finalized</span>';
  if (status === 'REJECTED') return '<span class="badge-status-rejected">Rejected</span>';
  return '<span class="badge-status-pending">Review Pending</span>';
}

function renderReview() {
  const tbody = document.getElementById('review-tbody');
  if (!tbody) return;
  if (!_reviewItems.length) {
    tbody.innerHTML = '<tr><td colspan="11" style="text-align:center;color:var(--theme-text-muted)">Queue is empty.</td></tr>';
    return;
  }
  tbody.innerHTML = _reviewItems.map(function(item, idx) {
    const docsKey = (item.ticker || '') + '|' + (item.period || '');
    const docsState = _reviewDocsCache[docsKey];
    const isExpanded = idx === _reviewExpandedIdx;
    let docsHtml = '';
    if (isExpanded) {
      if (!docsState || docsState.loading) {
        docsHtml = '<div style="color:var(--theme-text-muted)">Loading documents...</div>';
      } else if (docsState.error) {
        docsHtml = '<div style="color:var(--theme-danger)">Failed to load documents: ' + escapeHtml(docsState.error) + '</div>';
      } else {
        const docs = [];
        if (docsState.selected) docs.push(Object.assign({is_selected: true}, docsState.selected));
        (docsState.alternatives || []).forEach(function(doc) { docs.push(doc); });
        docsHtml = docs.length
          ? docs.map(function(doc) {
              const badge = doc.is_selected
                ? '<span style="font-size:0.7rem;color:#22c55e;background:#22c55e1a;border-radius:10px;padding:1px 6px;margin-left:6px">priority</span>'
                : '';
              return '<button class="btn btn-sm btn-secondary" style="text-align:left;justify-content:flex-start"'
                + ' onclick="event.stopPropagation();openReviewItemByIndex(' + idx + ')">'
                + escapeHtml(doc.source_type || 'document')
                + (doc.report_date ? ' · ' + escapeHtml(doc.report_date) : '')
                + badge
                + '</button>';
            }).join(' ')
          : '<div style="color:var(--theme-text-muted)">No documents found for this period.</div>';
      }
      docsHtml = '<tr class="review-docs-row">'
        + '<td colspan="11" style="background:var(--theme-bg-primary);padding:0.75rem 0.9rem;border-bottom:1px solid var(--theme-border)">'
        + '<div style="display:flex;align-items:center;gap:0.5rem;flex-wrap:wrap;margin-bottom:0.35rem">'
        + '<strong style="font-size:0.78rem">Documents</strong>'
        + '<span style="font-size:0.72rem;color:var(--theme-text-muted)">Open the drawer from here if row click is unreliable.</span>'
        + '</div>'
        + '<div style="display:flex;gap:0.4rem;flex-wrap:wrap">' + docsHtml + '</div>'
        + '</td></tr>';
    }
    return '<tr data-idx="' + idx + '" onclick="openReviewItemByIndex(' + idx + ')">'
      + '<td style="width:1%;white-space:nowrap" onclick="event.stopPropagation()">'
      + '<button class="btn btn-xs btn-secondary" onclick="event.stopPropagation();toggleReviewDocs(' + idx + ')">' + (isExpanded ? 'Hide' : 'Docs') + '</button>'
      + '</td>'
      + '<td onclick="event.stopPropagation()"><input type="checkbox" class="rv-cb" data-id="' + escapeAttr(String(item.id)) + '"></td>'
      + '<td style="font-weight:600;cursor:pointer">' + escapeHtml(item.ticker || '') + '</td>'
      + '<td style="cursor:pointer">' + escapeHtml(item.period || '') + '</td>'
      + '<td style="font-size:0.75rem;cursor:pointer">' + escapeHtml(item.metric || '') + '</td>'
      + '<td style="cursor:pointer">' + _sourceTypeBadge(item.source_type) + '</td>'
      + '<td style="text-align:right;cursor:pointer">' + escapeHtml(item.llm_value != null ? String(item.llm_value) : String(item.raw_value || '—')) + '</td>'
      + '<td style="font-size:0.75rem;cursor:pointer">' + escapeHtml(item.agreement_status || '') + '</td>'
      + '<td style="cursor:pointer">' + _statusBadge(item.status) + '</td>'
      + '<td style="display:flex;gap:4px;flex-wrap:wrap" onclick="event.stopPropagation()">'
        + '<button class="btn btn-sm btn-secondary" data-id="' + escapeAttr(String(item.id)) + '" onclick="event.stopPropagation();acceptItem(this.getAttribute(\'data-id\'))">Accept</button>'
        + '<button class="btn btn-sm btn-secondary" data-id="' + escapeAttr(String(item.id)) + '" onclick="event.stopPropagation();rejectItem(this.getAttribute(\'data-id\'))">R</button>'
        + '<button class="btn btn-sm btn-muted no-data-btn" data-item-id="' + escapeAttr(String(item.id)) + '" data-agreement-status="' + escapeAttr(item.agreement_status || '') + '">No data</button>'
      + '</td>'
      + '</tr>'
      + docsHtml;
  }).join('');
}

async function toggleReviewDocs(idx) {
  if (_reviewExpandedIdx === idx) {
    _reviewExpandedIdx = -1;
    renderReview();
    return;
  }
  _reviewExpandedIdx = idx;
  renderReview();
  const item = _reviewItems[idx];
  if (!item || !item.ticker || !item.period) return;
  const docsKey = (item.ticker || '') + '|' + (item.period || '');
  if (_reviewDocsCache[docsKey] && !_reviewDocsCache[docsKey].loading) {
    renderReview();
    return;
  }
  _reviewDocsCache[docsKey] = {loading: true};
  renderReview();
  try {
    const period = String(item.period).slice(0, 7);
    const resp = await fetch('/api/miner/' + encodeURIComponent(item.ticker) + '/' + encodeURIComponent(period) + '/reports');
    const data = await resp.json().catch(function() { return {}; });
    if (!resp.ok || !data.success) throw new Error((data.error && data.error.message) || ('HTTP ' + resp.status));
    _reviewDocsCache[docsKey] = data.data || {selected: null, alternatives: []};
  } catch (err) {
    _reviewDocsCache[docsKey] = {error: err.message || String(err)};
  }
  renderReview();
}

function openReviewItemByIndex(idx) {
  const item = _reviewItems[idx];
  if (!item) return;
  _reviewIdx = idx;
  document.querySelectorAll('#review-tbody tr[data-idx]').forEach(function(row, rowIdx) {
    row.classList.toggle('selected', rowIdx === idx);
  });
  const panel = document.getElementById('doc-panel');
  if (panel) panel.classList.add('visible');
  const titleEl = document.getElementById('doc-panel-title-text');
  if (titleEl) {
    titleEl.textContent = (item.ticker || '')
      + (item.period ? ' · ' + item.period : '')
      + (item.metric ? ' · ' + item.metric : '')
      + (item.source_type ? ' · ' + item.source_type : '');
  }
  ReviewPanel.openItem(item.id);
}

function clearReviewSelection() {
  _reviewIdx = -1;
  document.querySelectorAll('#review-tbody tr.selected').forEach(function(row) {
    row.classList.remove('selected');
  });
}

function _isReviewQueueContext() {
  const pane = document.getElementById('spane-review-queue');
  const panel = document.getElementById('doc-panel');
  return !!(pane && pane.classList.contains('active') && panel && panel.classList.contains('visible') && _reviewIdx >= 0);
}

function _configureSharedReviewPanelCallbacks() {
  ReviewPanel.setOnFilled(function(e) {
    showToast('Submitted to review queue');
    selectCompany(_ticker);
  });
  ReviewPanel.setOnWritten(function(e) {
    showToast('Written to timeline: ' + e.metric + ' = ' + e.value);
    selectCompany(_ticker);
  });
  ReviewPanel.setOnApproved(function() {
    if (_isReviewQueueContext()) {
      showToast('Approved');
      clearReviewSelection();
      closeDocPanel();
      loadReview();
      return;
    }
    showToast('Approved');
    selectCompany(_ticker);
  });
  ReviewPanel.setOnRejected(function() {
    if (_isReviewQueueContext()) {
      showToast('Rejected');
      clearReviewSelection();
      closeDocPanel();
      loadReview();
      return;
    }
    showToast('Rejected');
  });
}

function toggleSelectAll() {
  const checked = document.getElementById('rv-select-all').checked;
  document.querySelectorAll('.rv-cb').forEach(function(cb) { cb.checked = checked; });
  document.getElementById('rv-bulk-finalize').disabled = !checked;
}

// Event delegation for review tbody — wired once
document.addEventListener('DOMContentLoaded', function() {
  const reviewTbody = document.getElementById('review-tbody');
  if (!reviewTbody) return;
  _configureSharedReviewPanelCallbacks();
  reviewTbody.addEventListener('change', function(e) {
    if (!e.target.classList.contains('rv-cb')) return;
    const anyChecked = Array.from(document.querySelectorAll('.rv-cb')).some(function(cb) { return cb.checked; });
    document.getElementById('rv-bulk-finalize').disabled = !anyChecked;
  });
});

async function acceptItem(id) {
  const resp = await fetch('/api/review/' + encodeURIComponent(id) + '/approve', { method: 'POST', headers: {'Content-Type': 'application/json'}, body: '{}' });
  const data = await resp.json();
  if (!data.success) { showToast(data.error?.message || 'Error', true); return; }
  showToast('Finalized');
  const active = _reviewItems[_reviewIdx];
  if (active && String(active.id) === String(id)) closeDocPanel();
  loadReview();
}

async function rejectItem(id) {
  const note = prompt('Rejection note (optional):') || '';
  const resp = await fetch('/api/review/' + encodeURIComponent(id) + '/reject', {
    method: 'POST',
    headers: {'Content-Type': 'application/json'},
    body: JSON.stringify({ note }),
  });
  const data = await resp.json();
  if (!data.success) { showToast(data.error?.message || 'Error', true); return; }
  showToast('Rejected');
  const active = _reviewItems[_reviewIdx];
  if (active && String(active.id) === String(id)) closeDocPanel();
  loadReview();
}

document.addEventListener('click', function(e) {
  const btn = e.target.closest('.no-data-btn');
  if (!btn) return;
  e.stopPropagation();
  const itemId = btn.dataset.itemId;
  const status = btn.dataset.agreementStatus;

  function doNoData() {
    const body = status !== 'LLM_EMPTY' ? JSON.stringify({confirmed: true}) : null;
    fetch('/api/review/' + itemId + '/no_data', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: body,
    })
    .then(function(r) { return r.json(); })
    .then(function(data) {
      if (data.success) {
        loadReview();
      } else {
        showToast((data.error && data.error.message) ? data.error.message : 'Error', true);
      }
    });
  }

  if (status !== 'LLM_EMPTY') {
    if (!confirm('This item has data. Mark as no_data anyway?')) return;
  }
  doNoData();
});

async function bulkFinalize() {
  const ids = Array.from(document.querySelectorAll('.rv-cb:checked'))
    .map(function(cb) { return parseInt(cb.getAttribute('data-id'), 10); });
  if (!ids.length) return;
  const resp = await fetch('/api/review/batch-finalize', {
    method: 'POST',
    headers: {'Content-Type': 'application/json'},
    body: JSON.stringify({ids: ids}),
  });
  const data = await resp.json();
  if (!data.success) { showToast('Batch finalize failed', true); return; }
  const n = data.data && data.data.finalized || 0;
  const failed = data.data && data.data.failed || 0;
  if (failed) showToast(n + ' finalized, ' + failed + ' failed', true);
  else showToast(n + ' items finalized');
  loadReview();
  // Navigate to miner chart view after finalizing
  activatePipelineSubTab('review', 'miner');
  setTimeout(function() { showChartView(); }, 200);
}

// Keyboard navigation — J/K/A/R (Anti-pattern #33)
document.addEventListener('keydown', function(e) {
  if (_activeTab !== 'review') return;
  const rows = document.querySelectorAll('#review-tbody tr[data-idx]');
  if (!rows.length) return;
  if (e.key === 'j' || e.key === 'J') {
    _reviewIdx = Math.min(_reviewIdx + 1, rows.length - 1);
    openReviewItemByIndex(_reviewIdx);
  } else if (e.key === 'k' || e.key === 'K') {
    _reviewIdx = Math.max(_reviewIdx - 1, 0);
    openReviewItemByIndex(_reviewIdx);
  } else if ((e.key === 'a' || e.key === 'A') && _reviewIdx >= 0) {
    const row = rows[_reviewIdx];
    const btn = row ? row.querySelector('[data-id]') : null;
    if (btn) acceptItem(btn.getAttribute('data-id'));
  } else if ((e.key === 'r' || e.key === 'R') && _reviewIdx >= 0) {
    const row = rows[_reviewIdx];
    const btn = row ? row.querySelectorAll('[data-id]')[1] : null;
    if (btn) rejectItem(btn.getAttribute('data-id'));
  }
});

function highlightReviewRow(rows) {
  rows.forEach(function(r, i) { r.style.background = i === _reviewIdx ? 'var(--theme-bg-tertiary)' : ''; });
  if (rows[_reviewIdx]) rows[_reviewIdx].scrollIntoView({ block: 'nearest' });
}

// ── QC / Pipeline tab ─────────────────────────────────────────────────────────
let _pipelinePollTimer = null;

async function captureQCSnapshot() {
  document.getElementById('qc-msg').textContent = 'Capturing…';
  try {
    const resp = await fetch('/api/qc/snapshot', {method: 'POST'});
    const data = await resp.json();
    if (!resp.ok || !data.success) throw new Error(data.error?.message || 'Failed');
    document.getElementById('qc-msg').textContent = 'Snapshot captured.';
    await _loadQCTable();
  } catch (err) {
    document.getElementById('qc-msg').textContent = 'Error: ' + String(err);
  }
}

async function _loadQCTable() {
  try {
    const resp = await fetch('/api/qc/summary');
    if (!resp.ok) return;
    const data = await resp.json();
    const rows = data.data || [];
    const tbody = document.getElementById('qc-table-body');
    if (!tbody) return;
    if (!rows.length) {
      tbody.innerHTML = '<tr><td colspan="7" style="padding:0.5rem;color:var(--theme-text-muted)">No snapshots yet. Click "Capture Snapshot" to create the first one.</td></tr>';
      return;
    }
    tbody.innerHTML = rows.map(function(r) {
      return '<tr>' +
        '<td style="padding:0.2rem 0.5rem">' + escapeHtml(r.run_date || '') + '</td>' +
        '<td style="text-align:right;padding:0.2rem 0.5rem">' + (r.auto_accepted ?? '') + '</td>' +
        '<td style="text-align:right;padding:0.2rem 0.5rem">' + (r.review_approved ?? '') + '</td>' +
        '<td style="text-align:right;padding:0.2rem 0.5rem">' + (r.review_rejected ?? '') + '</td>' +
        '<td style="text-align:right;padding:0.2rem 0.5rem">' + (r.review_edited ?? '') + '</td>' +
        '<td style="text-align:right;padding:0.2rem 0.5rem">' + (r.precision_est != null ? (r.precision_est * 100).toFixed(1) + '%' : '') + '</td>' +
        '<td style="text-align:right;padding:0.2rem 0.5rem">' + (r.recall_est != null ? (r.recall_est * 100).toFixed(1) + '%' : '') + '</td>' +
        '</tr>';
    }).join('');
  } catch (_e) {}
}

let _pipelineRunId = null;
let _pipelineEventOffset = 0;

function _setPipelineRunId(id) {
  _pipelineRunId = id;
  try { localStorage.setItem('pipeline_run_id', String(id)); } catch (_e) {}
}

async function _restorePipelineRun() {
  // Try localStorage first, then fall back to /api/pipeline/overnight/latest.
  let runId = null;
  try { runId = parseInt(localStorage.getItem('pipeline_run_id'), 10) || null; } catch (_e) {}
  if (!runId) {
    try {
      const resp = await fetch('/api/pipeline/overnight/latest');
      if (resp.ok) {
        const d = await resp.json();
        runId = d.data?.run?.id || null;
      }
    } catch (_e) {}
  }
  if (!runId) return;
  try {
    const resp = await fetch('/api/pipeline/overnight/' + runId + '/status');
    if (!resp.ok) return;
    const data = await resp.json();
    const run = data.data?.run || {};
    const msgEl = document.getElementById('pipeline-msg');
    const statusEl = document.getElementById('pipeline-status');
    const cancelBtn = document.getElementById('pipeline-cancel-btn');
    const terminalStatuses = new Set(['complete', 'partial_complete', 'failed', 'failed_preflight', 'cancelled']);
    if (terminalStatuses.has(run.status)) {
      // Terminal run — show status only, do NOT replay log. Log is blank so user
      // sees a clean slate ready for the next run.
      if (msgEl) msgEl.textContent = 'Last run: ' + run.status + ' (run ' + runId + ')';
      if (statusEl) statusEl.textContent = 'Status: ' + run.status;
      if (cancelBtn) cancelBtn.style.display = 'none';
    } else {
      // Run is still active — restore run id, replay events, resume polling.
      _pipelineRunId = runId;
      _pipelineEventOffset = 0;
      await _fetchPipelineEvents();
      if (msgEl) msgEl.textContent = 'Resumed run ' + runId + '…';
      if (cancelBtn) cancelBtn.style.display = '';
      _startPipelinePoll();
    }
  } catch (_e) {}
}

function _appendPipelineLog(text, warn) {
  const el = document.getElementById('pipeline-log');
  if (!el) return;
  el.style.display = '';
  const row = document.createElement('div');
  row.className = 'acq-log-row' + (warn ? ' acq-log-warn' : '');
  row.textContent = text;
  el.appendChild(row);
  el.scrollTop = el.scrollHeight;
  while (el.children.length > 200) el.removeChild(el.firstChild);
}

function _clearPipelineLog() {
  const el = document.getElementById('pipeline-log');
  if (el) { el.innerHTML = ''; el.style.display = 'none'; }
}

function _shortUrl(url, max) {
  if (!url) return '';
  max = max || 90;
  return url.length > max ? url.slice(0, max) + '…' : url;
}

async function _fetchPipelineEvents() {
  if (!_pipelineRunId) return;
  try {
    const resp = await fetch('/api/pipeline/overnight/' + _pipelineRunId + '/events?limit=500');
    if (!resp.ok) return;
    const data = await resp.json();
    const rows = (data.data || []);
    if (rows.length <= _pipelineEventOffset) return;
    const newRows = rows.slice(_pipelineEventOffset);
    _pipelineEventOffset = rows.length;
    newRows.forEach(function(row) {
      const ts = (row.created_at || '').slice(11, 19);
      const stage = row.stage || '';
      const event = row.event || '';
      const d = row.details || {};
      const ticker = d.ticker || row.ticker || '';
      const warn = row.level === 'WARNING';

      // ── IR scrape URL-level events ────────────────────────────────────────
      if (stage === 'ir_scrape') {
        const period = d.period ? ' ' + d.period.slice(0, 7) : '';
        const url = _shortUrl(d.url);
        if (event === 'scrape_start') {
          _appendPipelineLog(ts + ' [IR] ' + ticker + ' starting ' + (d.mode || '') + ' — ' + _shortUrl(d.ir_url || ''), false);
        } else if (event === 'url_ingested') {
          _appendPipelineLog(ts + ' [IR] ' + ticker + period + ' stored — ' + url, false);
        } else if (event === 'url_skipped') {
          _appendPipelineLog(ts + ' [IR] ' + ticker + period + ' skip:' + (d.reason || '') + ' — ' + url, false);
        } else if (event === 'url_error') {
          _appendPipelineLog(ts + ' [IR] ' + ticker + period + ' ERROR: ' + (d.error || '') + ' — ' + url, true);
        } else if (event === 'page_fetch') {
          _appendPipelineLog(ts + ' [IR] ' + ticker + ' page ' + (d.page || 1) + ' — ' + url, false);
        } else if (event === 'page_fetch_done') {
          _appendPipelineLog(
            ts + ' [IR] ' + ticker
            + ' page ' + (d.page || 1)
            + ' done'
            + ' candidates=' + String(d.candidate_count || 0)
            + ' new=' + String(d.new_candidates || 0),
            false
          );
        } else if (event === 'detail_fetch_stage_start') {
          _appendPipelineLog(
            ts + ' [IR] ' + ticker
            + ' fetching ' + String(d.total || 0)
            + ' article(s) with ' + String(d.workers || 0) + ' worker(s)',
            false
          );
        } else if (event === 'detail_fetch_start') {
          _appendPipelineLog(
            ts + ' [IR] ' + ticker
            + ' article ' + String(d.sequence || 0) + '/' + String(d.total || 0)
            + ' — ' + url,
            false
          );
        } else if (event === 'detail_fetch_done') {
          const suffix = d.fetched ? 'fetched' : ('skip:' + (d.reason || 'no_response'));
          _appendPipelineLog(
            ts + ' [IR] ' + ticker
            + ' article ' + String(d.sequence || 0) + '/' + String(d.total || 0)
            + ' ' + suffix + ' — ' + url,
            false
          );
        } else if (event === 'detail_fetch_progress') {
          _appendPipelineLog(
            ts + ' [IR] ' + ticker
            + ' article progress ' + String(d.completed || 0) + '/' + String(d.total || 0)
            + ' fetched=' + String(d.fetched || 0),
            false
          );
        } else if (event === 'detail_fetch_stage_done') {
          _appendPipelineLog(
            ts + ' [IR] ' + ticker
            + ' article fetch complete fetched=' + String(d.fetched || 0)
            + '/' + String(d.total || 0),
            false
          );
        }
        return;
      }

      // ── EDGAR ingest URL-level events ─────────────────────────────────────
      if (stage === 'edgar_ingest') {
        const form = d.form_type || '';
        const period = d.covering_period || d.period || '';
        const url = _shortUrl(d.url);
        const acc = d.accession ? d.accession.slice(-12) : '';
        if (event === 'url_ingested') {
          const quality = d.quality ? ' [' + d.quality + ']' : '';
          _appendPipelineLog(ts + ' [EDGAR] ' + ticker + ' ' + form + ' ' + period.slice(0, 7) + quality + ' — ' + url, false);
        } else if (event === 'url_error') {
          _appendPipelineLog(ts + ' [EDGAR] ' + ticker + ' ' + form + ' ' + acc + ' ERROR: ' + (d.error || '') + ' — ' + url, true);
        } else if (event === 'search_result') {
          _appendPipelineLog(ts + ' [EDGAR] ' + ticker + ' ' + form + ' found ' + (d.hits || 0) + ' hit(s)', false);
        }
        return;
      }

      // ── Ollama lifecycle ──────────────────────────────────────────────────
      if (event === 'ollama_status') {
        _appendPipelineLog(ts + ' [Ollama] ' + (d.message || ''), warn);
        return;
      }
      if (event === 'ollama_warmup') {
        const status = d.warmed ? 'model ready' : 'warmup failed (' + (d.reason || '') + ')';
        _appendPipelineLog(ts + ' [Ollama] ' + (d.model || '') + ' — ' + status, !d.warmed);
        return;
      }

      // ── Extraction per-report events ──────────────────────────────────────
      if (stage === 'extract' && event === 'stage_start') {
        _appendPipelineLog(ts + ' [Extract] ' + (d.total_reports || 0) + ' reports queued for extraction');
        return;
      }
      if (stage === 'extract' && event === 'report_done') {
        const pct = d.total > 0 ? Math.round(d.progress / d.total * 100) : 0;
        const pts = d.data_points > 0 ? ' +' + d.data_points + 'dp' : '';
        const rv = d.review_flagged > 0 ? ' +' + d.review_flagged + 'rv' : '';
        const src = d.source_type ? ' [' + d.source_type.replace('ir_press_release', 'IR').replace('edgar_', 'SEC/') + ']' : '';
        const tok = (d.prompt_tokens || d.response_tokens) ? ' (' + (d.prompt_tokens || 0) + '+' + (d.response_tokens || 0) + 'tok)' : '';
        _appendPipelineLog(
          ts + ' [Extract] ' + d.progress + '/' + d.total + ' (' + pct + '%) ' +
          ticker + ' ' + (d.period || '').slice(0, 7) + src + pts + rv + tok +
          ' — ' + d.running_total_dp + ' pts total'
        );
        return;
      }
      if (stage === 'extract' && event === 'report_extract_failed') {
        _appendPipelineLog(ts + ' [Extract] FAILED ' + ticker + ' ' + (d.period || '') + ' — ' + (d.error || ''), true);
        return;
      }
      if (stage === 'extract' && event === 'stage_end') {
        _appendPipelineLog(ts + ' [Extract] done — ' + (d.reports_processed || 0) + ' reports, ' + (d.data_points || 0) + ' pts, ' + (d.errors || 0) + ' errors');
        return;
      }

      // ── High-level pipeline stage transitions ─────────────────────────────
      if (event === 'pipeline_run_start') {
        const tickers = (d.config?.tickers || []).join(', ') || 'all';
        _appendPipelineLog(ts + ' [Pipeline] started — ' + (d.requested_count || '?') + ' companies: ' + tickers);
        return;
      }
      if (event === 'pipeline_run_end') {
        _appendPipelineLog(ts + ' [Pipeline] ' + (d.status || 'done') + (d.failures ? ' (' + d.failures + ' failures)' : ''), warn || d.status === 'failed');
        return;
      }
      if (event === 'pipeline_run_cancelled') {
        _appendPipelineLog(ts + ' [Pipeline] cancelled', true);
        return;
      }
      if (event === 'pipeline_run_failed') {
        _appendPipelineLog(ts + ' [Pipeline] FAILED — ' + (d.error || ''), true);
        return;
      }
      if (stage === 'ingest' && event === 'stage_end') {
        _appendPipelineLog(ts + ' [Ingest] done — ' + (d.ingested_delta || 0) + ' new docs (' + (d.before_reports || 0) + ' → ' + (d.after_reports || 0) + ' total)');
        return;
      }
      if (stage === 'ingest' && event === 'ir_skipped') {
        _appendPipelineLog(ts + ' [Ingest] IR skipped (' + (d.reason || '') + ')', false);
        return;
      }
      if (stage === 'crawl' && event === 'stage_start') {
        _appendPipelineLog(ts + ' [Crawl] starting ' + (d.tickers || 0) + ' companies via ' + (d.provider || 'ollama'));
        return;
      }
      if (stage === 'crawl' && event === 'stage_end') {
        _appendPipelineLog(ts + ' [Crawl] done — ' + (d.stored || 0) + ' docs stored, ' + (d.failed || 0) + ' failed');
        return;
      }
      if (stage === 'crawl' && event === 'stage_skipped') {
        _appendPipelineLog(ts + ' [Crawl] skipped (' + (d.reason || '') + ')');
        return;
      }
      if (event === 'stage_skipped') {
        _appendPipelineLog(ts + ' [' + stage + '] skipped — ' + (d.reason || ''));
        return;
      }
      if (event === 'cancel_requested') {
        _appendPipelineLog(ts + ' [Pipeline] cancel requested', true);
        return;
      }

      // Fallback — generic key=value pairs
      const parts = [];
      if (ticker) parts.push(ticker);
      if (d.recommended_mode) parts.push('mode=' + d.recommended_mode);
      if (d.targeted != null) parts.push('targeted=' + d.targeted);
      if (d.ingested_delta != null) parts.push('new_docs=' + d.ingested_delta);
      if (d.reports_processed != null) parts.push('reports=' + d.reports_processed);
      if (d.data_points != null) parts.push('pts=' + d.data_points);
      if (d.errors) parts.push('errors=' + d.errors);
      if (d.reason) parts.push('reason=' + d.reason);
      if (d.error) parts.push('err=' + d.error);
      const detail = parts.length ? ' — ' + parts.join(', ') : '';
      _appendPipelineLog(ts + ' [' + stage + '] ' + event + detail, warn);
    });
  } catch (_e) {}
}

async function runOvernightScout() {
  document.getElementById('pipeline-msg').textContent = 'Starting scout…';
  try {
    const resp = await fetch('/api/pipeline/overnight/start', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({scout_mode: 'force'}),
    });
    const data = await resp.json();
    if (!resp.ok || !data.success) throw new Error(data.error?.message || 'Failed');
    _setPipelineRunId(data.data?.run_id);
    document.getElementById('pipeline-msg').textContent = 'Scout started (run ' + _pipelineRunId + ').';
    _startPipelinePoll();
  } catch (err) {
    document.getElementById('pipeline-msg').textContent = 'Error: ' + String(err);
  }
}

async function loadPipelinePreflight() {
  var panel = document.getElementById('pipeline-preflight-panel');
  if (panel) panel.style.display = '';
  try {
    const resp = await fetch('/api/pipeline/preflight');
    if (!resp.ok) return;
    const d = (await resp.json()).data || {};
    var pendingEl = document.getElementById('pipeline-preflight-pending');
    var extractedEl = document.getElementById('pipeline-preflight-extracted');
    var llmEl = document.getElementById('pipeline-preflight-llm');
    var kwEl = document.getElementById('pipeline-preflight-keywords');
    if (pendingEl) {
      var pendingStyle = d.pending_report_count > 0 ? 'color:#4caf50' : 'color:#ff9800';
      pendingEl.innerHTML = '<span style="' + pendingStyle + '">' + d.pending_report_count + ' pending</span> &nbsp;';
    }
    if (extractedEl) extractedEl.innerHTML = d.already_extracted_count + ' already extracted &nbsp;';
    if (llmEl) {
      var llmStyle = d.llm_available ? 'color:#4caf50' : 'color:#f44336';
      var llmBackend = d.llm_backend || 'ollama';
      var llmLabel = llmBackend === 'llamacpp' ? 'llama-server' : 'Ollama';
      var llmText = d.llm_available ? (llmLabel + ' ready (' + (d.ollama_model || 'unknown') + ')') : (llmLabel + ' unreachable');
      llmEl.innerHTML = '<span style="' + llmStyle + '">' + llmText + '</span> &nbsp;';
    }
    if (kwEl) kwEl.innerHTML = d.keyword_count + ' keywords configured';
    if (d.pending_report_count === 0 && panel) {
      panel.innerHTML += '<div style="color:#ff9800;margin-top:0.25rem">0 pending reports — check Force re-extract to re-run LLM on already-extracted reports.</div>';
    }
    var dlEl = document.getElementById('pipeline-preflight-dead-letter');
    var dlMsg = document.getElementById('pipeline-preflight-dead-letter-msg');
    if (dlEl && dlMsg && d.dead_letter_count > 0) {
      dlMsg.textContent = d.dead_letter_count + ' report' + (d.dead_letter_count > 1 ? 's' : '') + ' blocked by attempt cap (max 5 failures) — invisible to normal extraction runs.';
      dlEl.style.display = '';
    }
  } catch (_e) {}
}

async function resetExtractionAttempts(ticker) {
  const label = ticker || 'all companies';
  const body = ticker ? {ticker} : {};
  try {
    const resp = await fetch('/api/pipeline/reset-attempts', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify(body),
    });
    const d = await resp.json();
    if (d.success) {
      const dlEl = document.getElementById('pipeline-preflight-dead-letter');
      if (dlEl) dlEl.style.display = 'none';
      const msg = document.getElementById('pipeline-msg');
      if (msg) msg.textContent = 'Reset ' + (d.data.rows_updated || 0) + ' attempt counters for ' + label + '. Re-run Check Readiness.';
    }
  } catch (_e) {}
}

async function runFullPipeline() {
  _clearPipelineLog();
  _pipelineEventOffset = 0;
  document.getElementById('pipeline-msg').textContent = 'Starting pipeline…';
  document.getElementById('pipeline-status').textContent = '';
  var cancelBtn = document.getElementById('pipeline-cancel-btn');
  if (cancelBtn) cancelBtn.style.display = '';
  var includeIr = document.getElementById('pipeline-include-ir')?.checked !== false;
  var includeCrawl = document.getElementById('pipeline-include-crawl')?.checked === true;
  var probeSkip = document.getElementById('pipeline-probe-skip')?.checked === true;
  var forceReextract = document.getElementById('pipeline-force-reextract')?.checked === true;
  var irWorkers = Math.max(1, parseInt(document.getElementById('pipeline-ir-workers')?.value || '2', 10) || 2);
  var extractWorkers = Math.max(1, parseInt(document.getElementById('pipeline-extract-workers')?.value || '8', 10) || 8);
  var tickers = _getSharedSelectedTickers();
  try {
    const resp = await fetch('/api/pipeline/overnight/start', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({skip_probe: true, require_probe_success: false, require_non_skip_recommendation: false, include_ir: includeIr, include_crawl: includeCrawl, probe_skip_companies: probeSkip, force_reextract: forceReextract, ir_workers: irWorkers, extract_workers: extractWorkers, tickers: tickers}),
    });
    const data = await resp.json();
    if (!resp.ok || !data.success) throw new Error(data.error?.message || 'Failed');
    _setPipelineRunId(data.data?.run_id);
    document.getElementById('pipeline-msg').textContent = 'Running (run ' + _pipelineRunId + ')…';
    _startPipelinePoll();
  } catch (err) {
    document.getElementById('pipeline-msg').textContent = 'Error: ' + String(err);
    if (cancelBtn) cancelBtn.style.display = 'none';
  }
}

async function cancelPipeline() {
  if (!_pipelineRunId) return;
  try {
    await fetch('/api/pipeline/overnight/' + _pipelineRunId + '/cancel', {method: 'POST'});
    document.getElementById('pipeline-msg').textContent = 'Cancellation requested.';
  } catch (_e) {}
}

function _startPipelinePoll() {
  if (_pipelinePollTimer) clearInterval(_pipelinePollTimer);
  _pipelinePollTimer = setInterval(_pollPipelineStatus, 2000);
}

async function _pollPipelineStatus() {
  if (!_pipelineRunId) return;
  _fetchPipelineEvents();
  try {
    const resp = await fetch('/api/pipeline/overnight/' + _pipelineRunId + '/status');
    if (!resp.ok) return;
    const data = await resp.json();
    const status = data.data?.status || '';
    const msgEl = document.getElementById('pipeline-msg');
    const statusEl = document.getElementById('pipeline-status');
    const cancelBtn = document.getElementById('pipeline-cancel-btn');
    if (statusEl) statusEl.textContent = status ? ('Status: ' + status) : '';
    if (msgEl && (status === 'queued' || status === 'running')) {
      msgEl.textContent = status === 'queued' ? 'Queued — waiting for thread…' : 'Running (run ' + _pipelineRunId + ')…';
    }
    const _terminalStatuses = new Set(['complete', 'partial_complete', 'failed', 'failed_preflight', 'cancelled']);
    if (status && _terminalStatuses.has(status)) {
      clearInterval(_pipelinePollTimer);
      _pipelinePollTimer = null;
      if (cancelBtn) cancelBtn.style.display = 'none';
      if (msgEl) msgEl.textContent = 'Done — ' + status + '.';
      // Final event flush before stopping
      await _fetchPipelineEvents();
      // Refresh crawl prompt — EDGAR ingest may have updated bitcoin start date context
      const tickers = _getCrawlSelectedTickers();
      const promptTicker = tickers.length === 1 ? tickers[0] : (_companies.find(function(c) { return c.active !== false; }) || {}).ticker;
      if (promptTicker) loadCrawlPrompt(promptTicker);
      loadPipelineObservability();
    }
  } catch (_e) {}
}

// ── Miner chart (ECharts 5.4.3) ─────────────────────────────────────────────
let _minerChartInstance = null;

function populateChartMetricSelect() {
  const sel = document.getElementById('chart-metric-select');
  if (!sel) return;
  if (sel.options.length > 0) return; // already populated
  const metrics = (typeof METRICS_ORDER !== 'undefined' ? METRICS_ORDER : []);
  const labels = (typeof _metricLabelsCache !== 'undefined' ? _metricLabelsCache : {});
  metrics.forEach(function(key) {
    const opt = document.createElement('option');
    opt.value = key;
    opt.textContent = labels[key] || key;
    sel.appendChild(opt);
  });
  if (sel.options.length > 0) sel.options[0].selected = true;
}

function showChartView() {
  const panel = document.getElementById('chart-panel');
  if (panel) panel.style.display = '';
  populateChartMetricSelect();
  renderMinerChart();
}

function hideChartView() {
  const panel = document.getElementById('chart-panel');
  if (panel) panel.style.display = 'none';
}

function renderMinerChart() {
  const container = document.getElementById('miner-echarts');
  if (!container) return;

  const sel = document.getElementById('chart-metric-select');
  const selectedMetrics = sel
    ? Array.from(sel.options).filter(function(o) { return o.selected; }).map(function(o) { return o.value; })
    : [];
  if (!selectedMetrics.length) return;

  const rows = (typeof _rows !== 'undefined' ? _rows : []);
  const finalizedOnly = document.getElementById('chart-finalized-only') && document.getElementById('chart-finalized-only').checked;
  const labels = (typeof _metricLabelsCache !== 'undefined' ? _metricLabelsCache : {});

  // Build period list (x-axis)
  const periods = rows.map(function(r) { return r.period || ''; }).filter(Boolean);

  // Build one series per selected metric
  const series = selectedMetrics.map(function(metric) {
    const dataPoints = rows.map(function(r) {
      const cell = r.metrics && r.metrics[metric];
      if (!cell) return null;
      if (finalizedOnly && !cell.is_finalized) return null;
      return cell.value != null ? cell.value : null;
    });
    return {
      name: labels[metric] || metric,
      type: 'line',
      connectNulls: false,
      data: dataPoints,
    };
  });

  // Determine ECharts theme from localStorage (same key as dashboard.html)
  const theme = (localStorage.getItem('hermeneutic-chart-theme') === 'dark') ? 'dark' : null;

  // Init or reuse chart instance
  if (!_minerChartInstance || _minerChartInstance.isDisposed()) {
    _minerChartInstance = typeof echarts !== 'undefined'
      ? echarts.init(container, theme)
      : null;
  }
  if (!_minerChartInstance) return;

  _minerChartInstance.setOption({
    tooltip: { trigger: 'axis' },
    legend: { type: 'scroll', bottom: 0 },
    grid: { top: 30, left: 60, right: 20, bottom: 50 },
    xAxis: { type: 'category', data: periods, axisLabel: { rotate: 45, fontSize: 10 } },
    yAxis: { type: 'value' },
    series: series,
  }, true);
}

// Hook renderMinerChart into selectCompany: after _rows is populated, refresh chart if visible
(function() {
  const _origSelectCompany = typeof selectCompany === 'function' ? selectCompany : null;
  if (!_origSelectCompany) return;
  window._origSelectCompanyForChart = _origSelectCompany;
  // Patch is applied after DOMContentLoaded so selectCompany is defined
  document.addEventListener('DOMContentLoaded', function() {
    // No-op: selectCompany is in miner_data.js and already updates _rows.
    // renderMinerChart reads _rows directly; callers invoke showChartView which calls renderMinerChart.
  });
})();

// ── Review tab (miner sub-pane): Purge Final Data ─────────────────────────
function mnPfOnTickerInput() {
  const ticker = (document.getElementById('mn-pf-ticker').value || '').trim().toUpperCase();
  const expected = ticker ? 'CLEAR_FINAL_' + ticker : '';
  const typed = (document.getElementById('mn-pf-ticker-confirm').value || '').trim();
  document.getElementById('mn-pf-ticker-btn').disabled = !expected || typed !== expected;
}

function mnPfOnGlobalInput() {
  const typed = (document.getElementById('mn-pf-global-confirm').value || '').trim();
  document.getElementById('mn-pf-global-btn').disabled = typed !== 'CLEAR_FINAL_ALL';
}

async function mnPfClearTicker() {
  const ticker = (document.getElementById('mn-pf-ticker').value || '').trim().toUpperCase();
  if (!ticker) return;
  const statusEl = document.getElementById('mn-pf-ticker-status');
  statusEl.textContent = 'Clearing…';
  statusEl.style.color = '';
  try {
    const resp = await fetch('/api/interpret/' + encodeURIComponent(ticker) + '/final', {method: 'DELETE'});
    if (!resp.ok) throw new Error('HTTP ' + resp.status);
    const data = await resp.json();
    if (!data.success) throw new Error(data.error && data.error.message || 'Failed');
    const n = data.data.deleted;
    statusEl.textContent = 'Cleared ' + n + ' rows';
    statusEl.style.color = 'var(--theme-success)';
    document.getElementById('mn-pf-ticker-confirm').value = '';
    document.getElementById('mn-pf-ticker-btn').disabled = true;
    if (ticker === _ticker) { _finalizedValues = []; renderFinalizedTable(); renderReconcileTable(); }
    showToast('Cleared ' + n + ' finalized values for ' + ticker);
  } catch (err) {
    statusEl.textContent = 'Error: ' + err.message;
    statusEl.style.color = 'var(--theme-danger)';
  }
}

async function mnPfPurgeAll() {
  const mode = (document.getElementById('mn-pf-mode').value || 'clear');
  const statusEl = document.getElementById('mn-pf-global-status');
  statusEl.textContent = 'Purging…';
  statusEl.style.color = '';
  try {
    const resp = await fetch('/api/delete/final', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({confirm: true, mode: mode}),
    });
    if (!resp.ok) throw new Error('HTTP ' + resp.status);
    const data = await resp.json();
    if (!data.success) throw new Error(data.error && data.error.message || 'Failed');
    const n = data.data.deleted;
    statusEl.textContent = 'Purged ' + n + ' rows';
    statusEl.style.color = 'var(--theme-success)';
    document.getElementById('mn-pf-global-confirm').value = '';
    document.getElementById('mn-pf-global-btn').disabled = true;
    _finalizedValues = []; renderFinalizedTable(); renderReconcileTable();
    showToast('Purged ' + n + ' finalized values');
  } catch (err) {
    statusEl.textContent = 'Error: ' + err.message;
    statusEl.style.color = 'var(--theme-danger)';
  }
}

// ── Review tab (queue sub-pane): Stage FINAL ─────────────────────────
function rvPfOnTickerInput() {
  const ticker = (document.getElementById('rv-pf-ticker').value || '').trim().toUpperCase();
  const expected = ticker ? 'CLEAR_FINAL_' + ticker : '';
  const typed = (document.getElementById('rv-pf-ticker-confirm').value || '').trim();
  document.getElementById('rv-pf-ticker-btn').disabled = !expected || typed !== expected;
}

function rvPfOnGlobalInput() {
  const typed = (document.getElementById('rv-pf-global-confirm').value || '').trim();
  document.getElementById('rv-pf-global-btn').disabled = typed !== 'CLEAR_FINAL_ALL';
}

async function rvPfClearTicker() {
  const ticker = (document.getElementById('rv-pf-ticker').value || '').trim().toUpperCase();
  if (!ticker) return;
  const statusEl = document.getElementById('rv-pf-ticker-status');
  statusEl.textContent = 'Clearing…';
  statusEl.style.color = '';
  try {
    const resp = await fetch('/api/interpret/' + encodeURIComponent(ticker) + '/final', {method: 'DELETE'});
    if (!resp.ok) throw new Error('HTTP ' + resp.status);
    const data = await resp.json();
    if (!data.success) throw new Error(data.error && data.error.message || 'Failed');
    const n = data.data.deleted;
    statusEl.textContent = 'Cleared ' + n + ' rows';
    statusEl.style.color = 'var(--theme-success)';
    document.getElementById('rv-pf-ticker-confirm').value = '';
    document.getElementById('rv-pf-ticker-btn').disabled = true;
    showToast('Cleared ' + n + ' finalized values for ' + ticker);
  } catch (err) {
    statusEl.textContent = 'Error: ' + err.message;
    statusEl.style.color = 'var(--theme-danger)';
  }
}

async function rvPfPurgeAll() {
  const mode = (document.getElementById('rv-pf-mode').value || 'clear');
  const statusEl = document.getElementById('rv-pf-global-status');
  statusEl.textContent = 'Purging…';
  statusEl.style.color = '';
  try {
    const resp = await fetch('/api/delete/final', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({confirm: true, mode: mode}),
    });
    if (!resp.ok) throw new Error('HTTP ' + resp.status);
    const data = await resp.json();
    if (!data.success) throw new Error(data.error && data.error.message || 'Failed');
    const n = data.data.deleted;
    statusEl.textContent = 'Purged ' + n + ' rows';
    statusEl.style.color = 'var(--theme-success)';
    document.getElementById('rv-pf-global-confirm').value = '';
    document.getElementById('rv-pf-global-btn').disabled = true;
    showToast('Purged ' + n + ' finalized values');
  } catch (err) {
    statusEl.textContent = 'Error: ' + err.message;
    statusEl.style.color = 'var(--theme-danger)';
  }
}

// rvReviewPurgeArtifacts / dzReviewPurge / dbFinalStage removed — superseded by Data Management panel (1.6)

function allPurgeOnInput() {
  const typed = (document.getElementById('all-purge-confirm-input').value || '').trim().toUpperCase();
  const btn = document.getElementById('all-purge-btn');
  if (btn) btn.disabled = typed !== 'DELETE_ALL_GLOBAL';
}

async function executeAllPurge() {
  const typed = (document.getElementById('all-purge-confirm-input').value || '').trim().toUpperCase();
  if (typed !== 'DELETE_ALL_GLOBAL') return;
  const reason = (document.getElementById('all-purge-reason').value || '').trim();
  const msgEl = document.getElementById('all-purge-msg');
  msgEl.style.color = 'var(--theme-text-muted)';
  msgEl.textContent = 'Deleting ALL stage…';
  document.getElementById('all-purge-btn').disabled = true;
  document.getElementById('all-purge-confirm-input').value = '';
  allPurgeOnInput();
  try {
    const body = { confirm: true, purge_mode: 'hard_delete', suppress_auto_sync: true };
    if (reason) body.reason = reason;
    const resp = await fetch('/api/delete/all', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(body),
    });
    const json = await resp.json();
    if (!resp.ok || !json.success) throw new Error((json.error && json.error.message) || 'Delete ALL failed');
    const counts = (json.data && json.data.counts) || {};
    const total = Object.values(counts).reduce(function(s, v) { return s + v; }, 0);
    msgEl.style.color = 'var(--theme-success)';
    msgEl.textContent = 'Deleted ALL stage: ' + total + ' rows.';
  } catch (err) {
    msgEl.style.color = 'var(--theme-danger)';
    msgEl.textContent = 'Error: ' + String(err);
  }
}

rvReviewPurgeOnInput();
dzReviewPurgeOnInput();
dbFinalOnInput();
allPurgeOnInput();

// Restore log panels from previous session on page load.
_extractPanel.restore({
  onProgress: function(p) {
    const state = p.status || 'running';
    const processed = p.reports_processed || 0;
    const total = p.reports_total || 0;
    _extractTaskId = _extractTaskId || 'restored';
    _setExtractEnabled(false);
    _setExtractStatus('Extraction (restored): ' + state + ' (' + processed + '/' + total + ')');
  },
  onComplete: function(p) {
    _extractTaskId = null;
    _setExtractEnabled(true);
    const processed = p.reports_processed || 0;
    const points = p.data_points || 0;
    const errors = p.errors || 0;
    _setExtractStatus('(restored) complete  reports=' + processed + '  pts=' + points + (errors ? '  errors=' + errors : ''));
  },
  onError: function(p) {
    _extractTaskId = null;
    _setExtractEnabled(true);
    _setExtractStatus('(restored) error: ' + (p.error_message || 'extraction failed'), true);
  },
  onFetchError: function() {
    _extractTaskId = null;
    _setExtractEnabled(true);
  },
});

_gapReextractPanel.restore({
  onProgress: function(p) {
    const statusEl = document.getElementById('gap-reextract-status');
    if (statusEl) {
      const state = p.status || 'running';
      const processed = p.reports_processed || 0;
      const total = p.reports_total || 0;
      statusEl.textContent = 'Gap re-extract (restored): ' + state + ' (' + processed + '/' + total + ')';
    }
  },
  onComplete: function(p) {
    const statusEl = document.getElementById('gap-reextract-status');
    const btn = document.getElementById('gap-reextract-btn');
    if (btn) btn.disabled = false;
    if (statusEl) {
      const processed = p.reports_processed || 0;
      const points = p.data_points || 0;
      statusEl.textContent = '(restored) complete  reports=' + processed + '  pts=' + points;
    }
  },
  onError: function(p) {
    const statusEl = document.getElementById('gap-reextract-status');
    const btn = document.getElementById('gap-reextract-btn');
    if (btn) btn.disabled = false;
    if (statusEl) statusEl.textContent = '(restored) error: ' + (p.error_message || 'failed');
  },
});

