// ── Registry tab ─────────────────────────────────────────────────────────────
let _registryLoaded = false;

async function loadRegistry() {
  _registryLoaded = true;
  const ticker   = document.getElementById('reg-ticker').value.trim().toUpperCase() || '';
  const period   = document.getElementById('reg-period').value || '';
  const docType  = document.getElementById('reg-doc-type').value || '';
  const exStatus = document.getElementById('reg-extraction-status').value || '';
  const params = new URLSearchParams();
  if (ticker)   params.set('ticker', ticker);
  if (period)   params.set('period', period.slice(0, 7)); // YYYY-MM
  if (docType)  params.set('doc_type', docType);
  if (exStatus) params.set('extraction_status', exStatus);
  const resp = await fetch('/api/registry?' + params.toString());
  if (!resp.ok) { showToast('Failed to load registry', true); return; }
  const data = await resp.json();
  const items = data.data?.items || [];
  document.getElementById('reg-count').textContent = items.length + ' items';
  const tbody = document.getElementById('registry-tbody');
  if (!items.length) {
    tbody.innerHTML = '<tr><td colspan="11" style="text-align:center;color:var(--theme-text-muted)">No records.</td></tr>';
    return;
  }
  tbody.innerHTML = items.map(function(item) {
    const parseQual = item.parse_quality || '';
    const pqBadge = parseQual === 'ok' ? '<span class="badge-parse-ok">ok</span>'
      : parseQual === 'parse_failed' ? '<span class="badge-parse-failed">fail</span>'
      : parseQual ? '<span class="badge-empty">' + escapeHtml(parseQual) + '</span>' : '—';

    const es = item.extraction_status || '';
    const esBadge = es === 'done' ? '<span class="badge-done">done</span>'
      : es === 'pending' ? '<span class="badge-pending">pending</span>'
      : es === 'keyword_gated' ? '<span class="badge-keyword-gated">gated</span>'
      : es ? '<span class="badge-empty">' + escapeHtml(es) + '</span>' : '—';

    const chars = item.char_count != null ? Number(item.char_count).toLocaleString() : '—';

    const reportId = item.report_id || item.report_id_join || '';
    const scanBtn = reportId
      ? '<button class="btn btn-sm btn-muted scan-kw-btn" data-report-id="' + escapeAttr(String(reportId)) + '">Scan</button>'
      : '';

    return '<tr>'
      + '<td style="font-weight:600">' + escapeHtml(item.ticker || '') + '</td>'
      + '<td>' + escapeHtml(item.period || '') + '</td>'
      + '<td style="font-size:0.75rem">' + escapeHtml(item.source_type || '') + '</td>'
      + '<td style="font-size:0.75rem">' + escapeHtml(item.ingest_state || '') + '</td>'
      + '<td>' + pqBadge + '</td>'
      + '<td>' + esBadge + '</td>'
      + '<td style="text-align:right;font-size:0.75rem">' + chars + '</td>'
      + '<td style="text-align:right">' + escapeHtml(String(item.metrics_found || 0)) + '</td>'
      + '<td style="font-size:0.72rem;color:var(--theme-text-muted)">' + escapeHtml((item.extracted_at || '—').slice(0, 16)) + '</td>'
      + '<td style="font-size:0.72rem;max-width:180px;overflow:hidden;text-overflow:ellipsis" title="' + escapeAttr(item.source_path || '') + '">' + escapeHtml((item.source_path || item.source_url || '').split('/').slice(-1)[0]) + '</td>'
      + '<td>' + scanBtn + '</td>'
      + '</tr>';
  }).join('');
}

async function backfillRawHtml() {
  const msg = document.getElementById('backfill-msg');
  if (msg) msg.textContent = 'Running backfill…';
  try {
    const resp = await fetch('/api/ingest/backfill_raw_html', {method: 'POST'});
    const data = await resp.json();
    if (!resp.ok || !data.success) throw new Error(data.error?.message || 'Failed');
    const r = data.data || {};
    if (msg) msg.textContent = 'Done — backfilled: ' + (r.backfilled || 0)
      + ', skipped (missing): ' + (r.skipped_missing || 0)
      + ', errors: ' + (r.errors || 0);
  } catch(err) {
    if (msg) msg.textContent = 'Error: ' + String(err);
  }
}

// ── Document Explorer ─────────────────────────────────────────────────────────
async function loadDocuments() {
  const ticker     = (document.getElementById('doc-ex-ticker')?.value || '').trim().toUpperCase();
  const sourceType = document.getElementById('doc-ex-source')?.value || '';
  const extracted  = document.getElementById('doc-ex-extracted')?.value || '';
  const params = new URLSearchParams();
  if (ticker)     params.set('ticker', ticker);
  if (sourceType) params.set('source_type', sourceType);
  if (extracted)  params.set('extracted', extracted);
  params.set('limit', '300');

  const tbody = document.getElementById('doc-ex-tbody');
  const countEl = document.getElementById('doc-ex-count');
  if (tbody) tbody.innerHTML = '<tr><td colspan="20" style="text-align:center;padding:1rem;color:var(--theme-text-muted)">Loading…</td></tr>';

  try {
    const resp = await fetch('/api/documents?' + params.toString());
    if (!resp.ok) throw new Error('HTTP ' + resp.status);
    const body = await resp.json();
    if (!body.success) throw new Error(body.error?.message || 'Failed');
    const reports = body.data.reports || [];
    const metricKeys = body.data.metric_keys || [];

    if (countEl) countEl.textContent = reports.length + ' document' + (reports.length === 1 ? '' : 's');

    // Rebuild metric columns in thead
    const theadRow = document.getElementById('doc-ex-thead-row');
    if (theadRow) {
      // Remove old metric ths
      Array.from(theadRow.querySelectorAll('th[data-doc-metric]')).forEach(th => th.remove());
      metricKeys.forEach(function(key) {
        const th = document.createElement('th');
        th.setAttribute('data-doc-metric', key);
        th.style.cssText = 'text-align:right;padding:4px 6px;border-bottom:1px solid var(--theme-border);white-space:nowrap';
        th.textContent = key;
        theadRow.appendChild(th);
      });
    }

    if (!reports.length) {
      if (tbody) tbody.innerHTML = '<tr><td colspan="' + (4 + metricKeys.length) + '" style="text-align:center;color:var(--theme-text-muted);padding:1rem">No documents found.</td></tr>';
      return;
    }

    const _srcShort = {'ir_press_release': 'IR', 'archive_html': 'Arch-HTML', 'archive_pdf': 'Arch-PDF',
      'edgar_8k': '8-K', 'edgar_10q': '10-Q', 'edgar_10k': '10-K'};

    if (tbody) tbody.innerHTML = reports.map(function(r) {
      const period = r.covering_period || (r.report_date ? r.report_date.slice(0, 7) : '—');
      const src = _srcShort[r.source_type] || (r.source_type || '—');
      const hasData = Object.keys(r.metrics || {}).length > 0;
      const rowStyle = hasData ? '' : 'opacity:0.55';
      const metricCells = metricKeys.map(function(key) {
        const m = r.metrics && r.metrics[key];
        if (!m || m.value == null) return '<td style="text-align:right;padding:4px 6px;border-bottom:1px solid var(--theme-border);color:var(--theme-text-muted)">—</td>';
        const methodBadge = m.method ? ' <span style="font-size:0.65rem;opacity:0.65">' + escapeHtml(m.method.slice(0,1).toUpperCase()) + '</span>' : '';
        return '<td style="text-align:right;padding:4px 6px;border-bottom:1px solid var(--theme-border)">' + escapeHtml(m.value.toLocaleString()) + methodBadge + '</td>';
      }).join('');
      return '<tr style="' + rowStyle + '">'
        + '<td style="font-weight:600;padding:4px 6px;border-bottom:1px solid var(--theme-border)">' + escapeHtml(r.ticker || '') + '</td>'
        + '<td style="padding:4px 6px;border-bottom:1px solid var(--theme-border);font-size:0.75rem">' + escapeHtml(src) + '</td>'
        + '<td style="padding:4px 6px;border-bottom:1px solid var(--theme-border);font-size:0.75rem;white-space:nowrap">' + escapeHtml((r.report_date || '').slice(0, 10)) + '</td>'
        + '<td style="padding:4px 6px;border-bottom:1px solid var(--theme-border);font-size:0.75rem;white-space:nowrap">' + escapeHtml(period) + '</td>'
        + metricCells
        + '</tr>';
    }).join('');
  } catch(err) {
    if (tbody) tbody.innerHTML = '<tr><td colspan="20" style="color:var(--theme-danger);padding:1rem">' + escapeHtml('Error: ' + err.message) + '</td></tr>';
  }
}

// ── Explorer tab ─────────────────────────────────────────────────────────────
let _explorerLoaded = false;
let _metricsLoaded = false;
let _explorerGrid = [];
let _selectedCell = null;
let _keywordDictionary = null;

function _escapeRegExp(s) {
  return String(s).replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
}

function _getSelectedKeywordPack() {
  const sel = document.getElementById('ex-keyword-pack');
  const selected = sel ? sel.value : '';
  if (selected) return selected;
  try {
    return localStorage.getItem('keyword_pack') || '';
  } catch (_e) {
    return '';
  }
}

function _getKeywordTerms() {
  const dict = _keywordDictionary || { packs: {} };
  const selected = _getSelectedKeywordPack() || dict.active_pack;
  const packTerms = (dict.packs && dict.packs[selected]) || [];
  return Array.isArray(packTerms) ? packTerms : [];
}

// ── HTML document viewer with snippet highlighting ────────────────────────────
function _renderHtmlDocViewer(container, rawHtml, matches) {
  const parser = new DOMParser();
  const doc = parser.parseFromString(rawHtml, 'text/html');

  // Strip elements that should not render in the viewer
  ['script', 'style', 'link', 'meta', 'iframe', 'object', 'embed',
   'nav', 'header', 'footer'].forEach(function(tag) {
    doc.querySelectorAll(tag).forEach(function(el) { el.remove(); });
  });

  // Collect snippets to highlight (source_snippet or text field, normalise whitespace)
  const snippets = [];
  (matches || []).forEach(function(m) {
    const raw = (m.source_snippet || m.text || '').replace(/\s+/g, ' ').trim();
    if (raw.length >= 6) snippets.push(raw);
  });

  // Walk text nodes in the parsed document body and wrap matches with <mark>
  snippets.forEach(function(snippet) {
    const needle = snippet.slice(0, 120).toLowerCase();
    const walker = document.createTreeWalker(doc.body, NodeFilter.SHOW_TEXT, null);
    const toProcess = [];
    let node;
    while ((node = walker.nextNode())) toProcess.push(node);
    toProcess.forEach(function(textNode) {
      const val = textNode.nodeValue;
      const idx = val.toLowerCase().indexOf(needle);
      if (idx < 0) return;
      const mark = doc.createElement('mark');
      mark.className = 'doc-snippet-hl';
      mark.textContent = val.slice(idx, idx + snippet.length);
      const parent = textNode.parentNode;
      if (!parent) return;
      const before = val.slice(0, idx);
      const after  = val.slice(idx + snippet.length);
      if (before) parent.insertBefore(doc.createTextNode(before), textNode);
      parent.insertBefore(mark, textNode);
      if (after)  parent.insertBefore(doc.createTextNode(after), textNode);
      parent.removeChild(textNode);
    });
  });

  // Render into an isolated wrapper div
  const wrapper = document.createElement('div');
  wrapper.className = 'html-doc-viewer';
  wrapper.innerHTML = doc.body.innerHTML;

  container.innerHTML = '';
  container.appendChild(wrapper);

  // Scroll first highlight into view
  const firstMark = container.querySelector('mark.doc-snippet-hl');
  if (firstMark) firstMark.scrollIntoView({ block: 'nearest', behavior: 'smooth' });
}

function _buildExplorerHighlightedSource(rawText, matches) {
  if (!rawText) return '';
  const regions = [];
  (matches || []).forEach(function(m) {
    const needleRaw = (m.source_snippet || m.text || '').replace(/\s+/g, ' ').trim();
    const needle = needleRaw.slice(0, 80);
    if (needle.length < 6) return;
    const idx = rawText.toLowerCase().indexOf(needle.toLowerCase());
    if (idx < 0) return;
    regions.push({
      start: idx,
      end: idx + needle.length,
      color: '#60a5fa',
      label: m.metric || 'match',
    });
  });

  const terms = _getKeywordTerms();
  terms.forEach(function(termRaw) {
    const term = String(termRaw || '').trim();
    if (term.length < 2) return;
    const rx = new RegExp('\\b' + _escapeRegExp(term) + '\\b', 'gi');
    let mrx;
    while ((mrx = rx.exec(rawText)) !== null) {
      regions.push({
        start: mrx.index,
        end: mrx.index + mrx[0].length,
        color: '#fde047',
        label: 'keyword: ' + term,
      });
      if (mrx.index === rx.lastIndex) rx.lastIndex++;
    }
  });

  regions.sort(function(a, b) {
    if (a.start !== b.start) return a.start - b.start;
    const alen = a.end - a.start;
    const blen = b.end - b.start;
    if (alen !== blen) return blen - alen;
    return String(a.label || '').localeCompare(String(b.label || ''));
  });

  let html = '';
  let pos = 0;
  for (let i = 0; i < regions.length; i++) {
    const r = regions[i];
    if (r.start < pos) continue;
    html += escapeHtml(rawText.slice(pos, r.start));
    html += '<span class="doc-hl" style="background:' + r.color + '28;border-bottom:2px solid ' + r.color
      + '" title="' + escapeHtml(r.label) + '">' + escapeHtml(rawText.slice(r.start, r.end)) + '</span>';
    pos = r.end;
  }
  html += escapeHtml(rawText.slice(pos));
  return html;
}

async function loadKeywordDictionaryOptions() {
  const sel = document.getElementById('ex-keyword-pack');
  if (!sel) return;
  try {
    const resp = await fetch('/api/config/keyword_dictionary');
    if (!resp.ok) return;
    const data = await resp.json();
    if (!data.success || !data.data || !data.data.dictionary) return;
    _keywordDictionary = data.data.dictionary;
    const packs = _keywordDictionary.packs || {};
    const remembered = _getSelectedKeywordPack();
    sel.innerHTML = '';
    Object.keys(packs).forEach(function(k) {
      const opt = document.createElement('option');
      opt.value = k;
      opt.textContent = 'keywords: ' + k;
      if ((remembered && remembered === k) || (!remembered && _keywordDictionary.active_pack === k)) {
        opt.selected = true;
      }
      sel.appendChild(opt);
    });
    sel.addEventListener('change', function() {
      try { localStorage.setItem('keyword_pack', sel.value); } catch (_e) {}
      renderKeywordEditor();
      if (_selectedCell) {
        loadCellDetail(_selectedCell.ticker, _selectedCell.period, _selectedCell.metric);
      }
    }, { once: true });
    renderKeywordEditor();
  } catch (_e) {
    // non-fatal
  }
}

function renderKeywordEditor() {
  const dict = _keywordDictionary || { packs: {} };
  const editorSelect = document.getElementById('kw-pack-select');
  const editorTerms = document.getElementById('kw-pack-terms');
  if (!editorSelect || !editorTerms) return;
  const current = _getSelectedKeywordPack() || dict.active_pack || '';
  editorSelect.innerHTML = '';
  Object.keys(dict.packs || {}).forEach(function(k) {
    const opt = document.createElement('option');
    opt.value = k;
    opt.textContent = k;
    if (k === current) opt.selected = true;
    editorSelect.appendChild(opt);
  });
  const selected = editorSelect.value || current;
  const terms = (dict.packs && dict.packs[selected]) || [];
  editorTerms.value = terms.join('\n');

  if (!editorSelect.dataset.bound) {
    editorSelect.dataset.bound = '1';
    editorSelect.addEventListener('change', function() {
      const exSel = document.getElementById('ex-keyword-pack');
      if (exSel) exSel.value = editorSelect.value;
      try { localStorage.setItem('keyword_pack', editorSelect.value); } catch (_e) {}
      renderKeywordEditor();
      if (_selectedCell) {
        loadCellDetail(_selectedCell.ticker, _selectedCell.period, _selectedCell.metric);
      }
    });
  }
}

function addKeywordPack() {
  if (!_keywordDictionary) return;
  const name = prompt('New pack name (snake_case recommended):', 'custom_pack');
  if (!name) return;
  const key = name.trim();
  if (!key) return;
  _keywordDictionary.packs = _keywordDictionary.packs || {};
  if (!_keywordDictionary.packs[key]) {
    _keywordDictionary.packs[key] = [];
  }
  _keywordDictionary.active_pack = key;
  try { localStorage.setItem('keyword_pack', key); } catch (_e) {}
  const exSel = document.getElementById('ex-keyword-pack');
  if (exSel) exSel.value = key;
  renderKeywordEditor();
  saveKeywordDictionary();
}

function deleteKeywordPack() {
  if (!_keywordDictionary) return;
  const current = _getSelectedKeywordPack();
  if (!current) return;
  if (!confirm('Delete pack "' + current + '"?')) return;
  const packs = _keywordDictionary.packs || {};
  delete packs[current];
  const keys = Object.keys(packs);
  if (!keys.length) {
    packs.btc_activity = ['bitcoin', 'btc'];
  }
  _keywordDictionary.active_pack = Object.keys(packs)[0];
  try { localStorage.setItem('keyword_pack', _keywordDictionary.active_pack); } catch (_e) {}
  renderKeywordEditor();
  saveKeywordDictionary();
}

async function saveKeywordDictionary() {
  const msg = document.getElementById('kw-dict-msg');
  if (!_keywordDictionary) return;
  const pack = document.getElementById('kw-pack-select')?.value;
  const raw = document.getElementById('kw-pack-terms')?.value || '';
  if (!pack) return;
  _keywordDictionary.packs = _keywordDictionary.packs || {};
  _keywordDictionary.packs[pack] = raw
    .split('\n')
    .map(function(x) { return x.trim().toLowerCase(); })
    .filter(function(x, i, arr) { return x && arr.indexOf(x) === i; });
  _keywordDictionary.active_pack = pack;

  msg.style.color = 'var(--theme-text-muted)';
  msg.textContent = 'Saving…';
  try {
    const resp = await fetch('/api/config/keyword_dictionary', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({ dictionary: _keywordDictionary }),
    });
    const data = await resp.json();
    if (!resp.ok || !data.success) throw new Error(data.error?.message || 'Save failed');
    _keywordDictionary = data.data.dictionary;
    try { localStorage.setItem('keyword_pack', _keywordDictionary.active_pack); } catch (_e) {}
    msg.style.color = 'var(--theme-success)';
    msg.textContent = 'Saved';
    loadKeywordDictionaryOptions();
  } catch (err) {
    msg.style.color = 'var(--theme-danger)';
    msg.textContent = String(err);
  }
}

async function loadExplorerMetrics() {
  _metricsLoaded = true;
  const resp = await fetch('/api/metric_schema');
  if (!resp.ok) return;
  const data = await resp.json();
  const metrics = data.data || [];
  const sel = document.getElementById('ex-metric');
  metrics.forEach(function(m) {
    const opt = document.createElement('option');
    opt.value = m.key;
    opt.textContent = m.label || m.key;
    sel.appendChild(opt);
  });
}

async function loadExplorer() {
  _explorerLoaded = true;
  const ticker  = document.getElementById('ex-ticker').value.trim().toUpperCase() || '';
  const state   = document.getElementById('ex-state').value || '';
  const metric  = document.getElementById('ex-metric').value || '';
  const months  = document.getElementById('ex-months').value || '36';
  const params = new URLSearchParams({ months });
  if (ticker) params.set('ticker', ticker);
  if (state)  params.set('state', state);
  if (metric) params.set('metric', metric);
  const resp = await fetch('/api/explorer/grid?' + params.toString());
  if (!resp.ok) { showToast('Failed to load explorer grid', true); return; }
  const data = await resp.json();
  _explorerGrid = data.data?.grid || [];
  document.getElementById('ex-count').textContent = _explorerGrid.length + ' cells';
  renderExplorerGrid(_explorerGrid);
}

function renderExplorerGrid(grid) {
  const container = document.getElementById('explorer-grid');
  if (!grid.length) {
    container.innerHTML = '<p style="color:var(--theme-text-muted);font-size:0.85rem">No cells match the current filters.</p>';
    return;
  }
  // Group by ticker, then period (rows = tickers, cols = period×metric)
  const byTicker = {};
  const periodsSet = new Set();
  const metricsSet = new Set();
  grid.forEach(function(cell) {
    if (!byTicker[cell.ticker]) byTicker[cell.ticker] = {};
    const key = cell.period + '|' + cell.metric;
    byTicker[cell.ticker][key] = cell;
    periodsSet.add(cell.period);
    metricsSet.add(cell.metric);
  });
  const periods = Array.from(periodsSet).sort().reverse();
  const metrics = Array.from(metricsSet).sort();
  const tickers = Object.keys(byTicker).sort();

  let html = '<table style="border-spacing:2px;border-collapse:separate"><thead><tr>'
    + '<th style="font-size:0.7rem;text-align:right;padding-right:6px;white-space:nowrap;color:var(--theme-text-secondary)">Ticker</th>';
  periods.forEach(function(p) {
    metrics.forEach(function(m) {
      html += '<th style="font-size:0.55rem;writing-mode:vertical-lr;padding:2px;color:var(--theme-text-muted);width:22px" title="' + escapeAttr(p + ' / ' + m) + '">' + escapeHtml(p.slice(5) + '/' + m.slice(0, 4)) + '</th>';
    });
  });
  html += '</tr></thead><tbody>';
  tickers.forEach(function(ticker) {
    html += '<tr><td style="font-size:0.72rem;font-weight:600;text-align:right;padding-right:6px;color:var(--theme-text-secondary)">' + escapeHtml(ticker) + '</td>';
    periods.forEach(function(p) {
      metrics.forEach(function(m) {
        const cell = byTicker[ticker][p + '|' + m];
        const state = cell ? cell.state : 'no_document';
        const val   = cell && cell.value != null ? cell.value : '';
        html += '<td><span class="explorer-cell cell-state-' + escapeHtml(state) + '"'
          + ' data-ticker="' + escapeAttr(ticker) + '"'
          + ' data-period="' + escapeAttr(p) + '"'
          + ' data-metric="' + escapeAttr(m) + '"'
          + ' title="' + escapeAttr(ticker + ' ' + p + ' ' + m + ': ' + state + (val !== '' ? ' = ' + val : '')) + '"'
          + '></span></td>';
      });
    });
    html += '</tr>';
  });
  html += '</tbody></table>';
  container.innerHTML = html;
}

// Explorer cell click — event delegation wired once (Anti-pattern #25)
document.addEventListener('DOMContentLoaded', function() {
  document.getElementById('explorer-grid').addEventListener('click', function(e) {
    const span = e.target.closest('.explorer-cell');
    if (!span) return;
    const ticker = span.getAttribute('data-ticker');
    const period = span.getAttribute('data-period');
    const metric = span.getAttribute('data-metric');
    loadCellDetail(ticker, period, metric);
  });
});

async function loadCellDetail(ticker, period, metric) {
  _selectedCell = { ticker, period, metric };
  const panel = document.getElementById('cell-detail-panel');
  panel.style.display = '';
  document.getElementById('cell-detail-header-text').textContent = ticker + ' · ' + period + ' · ' + metric;
  document.getElementById('cell-detail-rows').innerHTML = '<p style="color:var(--theme-text-muted);font-size:0.8rem">Loading…</p>';
  document.getElementById('cell-actions').innerHTML = '';
  document.getElementById('cell-save-form').style.display = 'none';
  document.getElementById('cell-reextract-panel').style.display = 'none';
  document.getElementById('cell-snippet').style.display = 'none';

  const url = '/api/explorer/cell/' + encodeURIComponent(ticker) + '/' + encodeURIComponent(period) + '/' + encodeURIComponent(metric);
  const resp = await fetch(url);
  if (!resp.ok) { document.getElementById('cell-detail-rows').innerHTML = '<p style="color:var(--theme-danger)">Error loading cell</p>'; return; }
  const data = await resp.json();
  const d = data.data;

  const rows = [
    ['State', d.state || '—'],
    ['Value', d.value != null ? d.value : '—'],
    ['Confidence', d.confidence != null ? d.confidence.toFixed(3) : '—'],
    ['Method', d.extraction_method || '—'],
  ];
  document.getElementById('cell-detail-rows').innerHTML = rows.map(function(r) {
    return '<div class="cell-detail-row"><span class="cell-detail-label">' + escapeHtml(r[0]) + '</span><span class="cell-detail-value">' + escapeHtml(String(r[1])) + '</span></div>';
  }).join('');

  // Actions
  const actEl = document.getElementById('cell-actions');
  actEl.innerHTML = '';
  const editBtn = document.createElement('button');
  editBtn.className = 'btn btn-sm btn-secondary';
  editBtn.textContent = 'Edit Value';
  editBtn.setAttribute('data-ui-id', '5.1.4');
  editBtn.onclick = function() {
    document.getElementById('cell-save-value').value = d.value != null ? d.value : '';
    document.getElementById('cell-save-form').style.display = '';
  };
  actEl.appendChild(editBtn);

  const gapBtn = document.createElement('button');
  gapBtn.className = 'btn btn-sm btn-secondary';
  gapBtn.textContent = 'Mark Gap';
  gapBtn.setAttribute('data-ui-id', '5.1.5');
  gapBtn.onclick = function() { submitMarkGap(); };
  actEl.appendChild(gapBtn);

  const reexBtn = document.createElement('button');
  reexBtn.className = 'btn btn-sm btn-secondary';
  reexBtn.textContent = 'Re-extract';
  reexBtn.setAttribute('data-ui-id', '5.1.6');
  reexBtn.onclick = function() { document.getElementById('cell-reextract-panel').style.display = ''; };
  actEl.appendChild(reexBtn);

  // Source view — prefer raw HTML renderer, fall back to plain-text highlighter
  const snippetEl = document.getElementById('cell-snippet');
  if (d.raw_html) {
    _renderHtmlDocViewer(snippetEl, d.raw_html, d.matches || []);
    snippetEl.style.display = '';
  } else if (d.raw_text) {
    snippetEl.innerHTML = _buildExplorerHighlightedSource(d.raw_text, d.matches || []);
    snippetEl.style.display = '';
  } else if (d.source_snippet) {
    snippetEl.innerHTML = _buildExplorerHighlightedSource(d.source_snippet, d.matches || []);
    snippetEl.style.display = '';
  } else {
    snippetEl.style.display = 'none';
  }
}

async function submitSaveCell() {
  if (!_selectedCell) return;
  const value = parseFloat(document.getElementById('cell-save-value').value);
  const note  = document.getElementById('cell-save-note').value;
  document.getElementById('cell-save-error').textContent = '';
  if (isNaN(value)) { document.getElementById('cell-save-error').textContent = 'Value must be numeric'; return; }
  const { ticker, period, metric } = _selectedCell;
  const resp = await fetch('/api/explorer/cell/' + encodeURIComponent(ticker) + '/' + encodeURIComponent(period) + '/' + encodeURIComponent(metric) + '/save', {
    method: 'POST',
    headers: {'Content-Type': 'application/json'},
    body: JSON.stringify({ value, note }),
  });
  const data = await resp.json();
  if (!data.success) {
    const msg = data.error?.message || 'Error';
    // 409 = analyst-protected — show clearly but not as a crash
    document.getElementById('cell-save-error').textContent = msg;
    if (resp.status !== 409) showToast(msg, true);
    return;
  }
  document.getElementById('cell-save-form').style.display = 'none';
  showToast('Value saved');
  loadCellDetail(ticker, period, metric);
}

async function submitMarkGap() {
  if (!_selectedCell) return;
  const { ticker, period, metric } = _selectedCell;
  const resp = await fetch('/api/explorer/cell/' + encodeURIComponent(ticker) + '/' + encodeURIComponent(period) + '/' + encodeURIComponent(metric) + '/gap', { method: 'POST' });
  const data = await resp.json();
  if (!data.success) { showToast(data.error?.message || 'Error', true); return; }
  showToast('Marked as gap');
  loadCellDetail(ticker, period, metric);
}

async function submitReextract() {
  if (!_selectedCell) return;
  const { ticker, period } = _selectedCell;
  const selection = document.getElementById('cell-reextract-text').value.trim();
  if (!selection) { showToast('Paste document text first', true); return; }
  const resp = await fetch('/api/explorer/reextract', {
    method: 'POST',
    headers: {'Content-Type': 'application/json'},
    body: JSON.stringify({ selection, ticker, period }),
  });
  const data = await resp.json();
  const candidates = data.data?.candidates || [];
  const el = document.getElementById('reextract-results');
  if (!candidates.length) { el.innerHTML = '<p style="font-size:0.75rem;color:var(--theme-text-muted)">No matches found.</p>'; return; }
  el.innerHTML = candidates.map(function(c) {
    return '<div class="reextract-candidate" title="' + escapeAttr(c.snippet || '') + '">'
      + '<span style="font-weight:600">' + escapeHtml(c.metric) + '</span>'
      + '<span style="color:var(--theme-text-muted)">' + escapeHtml(String(c.value)) + '</span>'
      + '<span style="font-size:0.72rem;color:var(--theme-text-muted)">' + escapeHtml((c.confidence || 0).toFixed(2)) + '</span>'
      + '</div>';
  }).join('');
}

