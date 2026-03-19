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

// ── Keyword dictionary (used by review pane highlight editor) ─────────────────
let _keywordDictionary = null;

function _getSelectedKeywordPack() {
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

async function loadKeywordDictionaryOptions() {
  try {
    const resp = await fetch('/api/config/keyword_dictionary');
    if (!resp.ok) return;
    const data = await resp.json();
    if (!data.success || !data.data || !data.data.dictionary) return;
    _keywordDictionary = data.data.dictionary;
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
      try { localStorage.setItem('keyword_pack', editorSelect.value); } catch (_e) {}
      renderKeywordEditor();
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

  if (msg) { msg.style.color = 'var(--theme-text-muted)'; msg.textContent = 'Saving…'; }
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
    if (msg) { msg.style.color = 'var(--theme-success)'; msg.textContent = 'Saved'; }
    loadKeywordDictionaryOptions();
  } catch (err) {
    if (msg) { msg.style.color = 'var(--theme-danger)'; msg.textContent = String(err); }
  }
}

// ── Data Points tab (DE4.2–4.5) ──────────────────────────────────────────────

async function deInitCompanySelect() {
  const sel = document.getElementById('de-f-ticker');
  if (!sel) return;
  try {
    const resp = await fetch('/api/companies');
    if (!resp.ok) return;
    const data = await resp.json();
    const companies = data.data || [];
    sel.innerHTML = companies
      .sort(function(a, b) { return a.ticker.localeCompare(b.ticker); })
      .map(function(c) {
        return '<option value="' + escapeAttr(c.ticker) + '">' + escapeHtml(c.ticker) + ' — ' + escapeHtml(c.name || '') + '</option>';
      }).join('');
  } catch (_e) {}
}

async function deInitMetricSelect() {
  const sel = document.getElementById('de-f-metric');
  if (!sel) return;
  try {
    const resp = await fetch('/api/metric_schema');
    if (!resp.ok) return;
    const data = await resp.json();
    const metrics = data.data || [];
    sel.innerHTML = '<option value="">All metrics</option>' + metrics.map(function(m) {
      return '<option value="' + escapeAttr(m.key) + '">' + escapeHtml(m.label || m.key) + '</option>';
    }).join('');
  } catch (_e) {}
}

function deCurrentParams() {
  const tickers = Array.from(document.getElementById('de-f-ticker').selectedOptions).map(function(o) { return o.value; });
  const params = new URLSearchParams();
  tickers.forEach(function(t) { params.append('ticker', t); });
  const metric = document.getElementById('de-f-metric').value;
  if (metric) params.set('metric', metric);
  const from = document.getElementById('de-f-from').value;
  if (from) params.set('from_period', from);
  const to = document.getElementById('de-f-to').value;
  if (to) params.set('to_period', to);
  const conf = parseFloat(document.getElementById('de-f-confidence').value);
  if (conf > 0) params.set('min_confidence', conf.toFixed(2));
  return params;
}

async function deFetchData() {
  const banner = document.getElementById('de-error-banner');
  if (banner) banner.hidden = true;
  try {
    const resp = await fetch('/api/data?' + deCurrentParams().toString());
    if (!resp.ok) {
      const err = await resp.json().catch(function() { return {}; });
      throw new Error(err?.error?.message || 'HTTP ' + resp.status);
    }
    const json = await resp.json();
    deRenderTable(json.data);
  } catch(e) {
    if (banner) {
      banner.textContent = 'Error: ' + e.message;
      banner.hidden = false;
    }
  }
}

function _deConfidenceBadge(conf) {
  const cls = conf >= 0.90 ? 'badge-success' : conf >= 0.75 ? 'badge-warning' : 'badge-danger';
  return '<span class="badge ' + cls + '">' + escapeHtml(conf.toFixed(3)) + '</span>';
}

function deRenderTable(rows) {
  const tbody = document.getElementById('de-results-body');
  const countEl = document.getElementById('de-results-count');
  if (!rows || rows.length === 0) {
    tbody.innerHTML = '<tr><td colspan="9" style="text-align:center;color:var(--theme-text-muted);padding:1rem">No data found.</td></tr>';
    if (countEl) countEl.textContent = '';
    return;
  }
  if (countEl) countEl.textContent = rows.length + ' row' + (rows.length === 1 ? '' : 's') + (rows.length === 1000 ? ' (limit)' : '');
  tbody.innerHTML = rows.map(function(r) {
    return '<tr'
      + ' data-ticker="' + escapeAttr(r.ticker) + '"'
      + ' data-metric="' + escapeAttr(r.metric) + '"'
      + ' data-period="' + escapeAttr((r.period || '').slice(0, 7)) + '"'
      + ' data-report-id="' + escapeAttr(String(r.report_id || '')) + '"'
      + '>'
      + '<td style="font-weight:600;padding:4px 6px;border-bottom:1px solid var(--theme-border)">' + escapeHtml(r.ticker) + '</td>'
      + '<td style="padding:4px 6px;border-bottom:1px solid var(--theme-border)">' + escapeHtml(r.period ? r.period.slice(0, 7) : '') + '</td>'
      + '<td style="padding:4px 6px;border-bottom:1px solid var(--theme-border)">' + escapeHtml(r.metric) + '</td>'
      + '<td style="text-align:right;padding:4px 6px;border-bottom:1px solid var(--theme-border)">' + escapeHtml(r.value != null ? r.value.toLocaleString() : '') + '</td>'
      + '<td style="padding:4px 6px;border-bottom:1px solid var(--theme-border)">' + escapeHtml(r.unit || '') + '</td>'
      + '<td style="text-align:right;padding:4px 6px;border-bottom:1px solid var(--theme-border)">' + _deConfidenceBadge(r.confidence || 0) + '</td>'
      + '<td style="padding:4px 6px;border-bottom:1px solid var(--theme-border)">' + escapeHtml(r.extraction_method || '') + '</td>'
      + '<td style="max-width:200px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;padding:4px 6px;border-bottom:1px solid var(--theme-border)" title="' + escapeAttr(r.source_snippet || '') + '">' + escapeHtml((r.source_snippet || '').slice(0, 80)) + '</td>'
      + '<td style="padding:4px 6px;border-bottom:1px solid var(--theme-border)"><button class="btn btn-secondary" style="padding:0.1rem 0.45rem;font-size:0.72rem" onclick="deOpenDocForRow(this)" data-report-id="' + escapeAttr(String(r.report_id || '')) + '" data-snippet="' + escapeAttr(r.source_snippet || '') + '">Doc</button></td>'
      + '</tr>';
  }).join('');

  // Wire row clicks for lineage
  const rows2 = document.getElementById('de-results-body').querySelectorAll('tr');
  rows2.forEach(function(row) {
    row.addEventListener('click', function(e) {
      if (e.target.tagName === 'BUTTON') return;
      const ticker = row.getAttribute('data-ticker');
      const metric = row.getAttribute('data-metric');
      const period = row.getAttribute('data-period');
      deLoadLineageForRow(ticker, metric, period);
    });
  });
}

async function deLoadLineageForRow(ticker, metric, period) {
  const panel = document.getElementById('de-lineage-panel');
  if (!panel) return;
  if (!ticker || !metric || !period) {
    panel.textContent = 'Lineage unavailable for this row.';
    return;
  }
  panel.textContent = 'Loading lineage...';
  try {
    const qs = new URLSearchParams({ticker: ticker, metric: metric, period: period});
    const resp = await fetch('/api/data/lineage?' + qs.toString());
    const body = await resp.json().catch(function() { return {}; });
    if (!resp.ok || !body.success) throw new Error(body?.error?.message || 'HTTP ' + resp.status);
    const d = body.data || {};
    panel.innerHTML = ''
      + '<div><strong>Ticker:</strong> ' + escapeHtml(d.ticker || '') + '</div>'
      + '<div><strong>Metric:</strong> ' + escapeHtml(d.metric || '') + '</div>'
      + '<div><strong>Period:</strong> ' + escapeHtml(d.period || '') + '</div>'
      + '<div><strong>Method:</strong> ' + escapeHtml(d.extraction_method || '') + '</div>'
      + '<div><strong>Confidence:</strong> ' + escapeHtml(d.confidence != null ? Number(d.confidence).toFixed(3) : '—') + '</div>'
      + '<div><strong>Report Date:</strong> ' + escapeHtml(d.report_date || '—') + '</div>'
      + '<div><strong>Source Type:</strong> ' + escapeHtml(d.source_type || '—') + '</div>'
      + '<div><strong>Source URL:</strong> ' + (d.source_url ? '<a href="' + escapeAttr(d.source_url) + '" target="_blank" rel="noopener">' + escapeHtml(d.source_url) + '</a>' : '—') + '</div>'
      + '<div><strong>Snippet:</strong> ' + escapeHtml(d.source_snippet || '—') + '</div>';
  } catch (e) {
    panel.textContent = 'Lineage error: ' + e.message;
  }
}

async function deOpenDocForRow(btn) {
  const reportId = btn.getAttribute('data-report-id');
  if (!reportId) return;
  const snippet = btn.getAttribute('data-snippet') || '';
  try {
    const resp = await fetch('/api/data/document/' + encodeURIComponent(reportId));
    if (!resp.ok) { showToast('Failed to load document', true); return; }
    const body = await resp.json();
    if (!body.success) { showToast(body.error?.message || 'Error', true); return; }
    const doc = body.data || {};
    const title = (doc.ticker || '') + ' — ' + (doc.source_type || '') + ' — ' + (doc.report_date || '').slice(0, 10);
    DocPanel.open(title, doc.raw_text || doc.text || '', snippet ? [{ source_snippet: snippet }] : []);
  } catch (e) {
    showToast('Error loading document: ' + e.message, true);
  }
}

function deExportCsv() {
  window.location = '/api/export.csv?' + deCurrentParams().toString();
}
