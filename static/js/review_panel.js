/**
 * review_panel.js — Single source of truth for the review + document-viewer + LLM reprompt
 * workflow. Used by review.html, miner_data.html, and dashboard.html.
 *
 * Public API:
 *   ReviewPanel.init(containerId)
 *     Inject HTML skeleton + wire event handlers. Call once on page load.
 *
 *   ReviewPanel.openItem(itemId)
 *     Load a review-queue item by ID. Shows doc text, value cards, approve/reject.
 *
 *   ReviewPanel.openCell(ticker, period, metric, opts)
 *     Open for a grid cell. If a PENDING review item exists, delegates to openItem().
 *     Otherwise shows analysis + raw source + fill form.
 *     opts.nullMetrics — array of metric keys to populate the fill-metric dropdown.
 *
 *   ReviewPanel.close()
 *     Clear state and show placeholder.
 *
 *   ReviewPanel.setOnApproved(fn)   fn({itemId, value})
 *   ReviewPanel.setOnRejected(fn)   fn({itemId})
 *   ReviewPanel.setOnFilled(fn)     fn({ticker, period, metric, value})
 *
 * Requires escapeHtml() from base.html and escapeAttr() if available.
 */
const ReviewPanel = (function () {
  'use strict';

  // ── Metric highlight colours ────────────────────────────────────────────────
  const _COLORS = {
    production_btc:   '#3b82f6',
    hodl_btc:         '#8b5cf6',
    sold_btc:         '#f59e0b',
    hashrate_eh:      '#10b981',
    realization_rate: '#f97316',
  };
  const _KEYWORD_COLOR = '#fde047';

  // ── Module state ────────────────────────────────────────────────────────────
  let _container   = null;
  let _currentItemId = null;
  let _currentTicker = null;
  let _currentPeriod = null;
  let _currentMetric = null;
  let _onApproved  = null;
  let _onRejected  = null;
  let _onFilled    = null;
  let _keywordDictionary = {
    active_pack: 'btc_activity',
    packs: { btc_activity: ['bitcoin', 'btc'] },
  };

  // ── HTML skeleton ───────────────────────────────────────────────────────────
  const _HTML = `
<div class="rp-layout">
  <div class="rp-source-col">
    <div class="rp-source-header">
      <span class="rp-source-meta"></span>
    </div>
    <div class="rp-doc-placeholder">Select a row to view the document.</div>
    <iframe class="rp-doc-iframe" src="about:blank" style="display:none;width:100%;min-height:500px;border:none;background:#fff"></iframe>
    <pre class="rp-doc-text" style="display:none"></pre>
    <div class="rp-reprompt-bar" style="display:none">
      <span class="rp-hint">Highlight a value in the text above, select which metric it is, then click Re-extract to run regex + LLM on it:</span>
      <select class="rp-reprompt-metric">
        <option value="production_btc">production_btc</option>
        <option value="hodl_btc">hodl_btc</option>
        <option value="sold_btc">sold_btc</option>
        <option value="hashrate_eh">hashrate_eh</option>
        <option value="realization_rate">realization_rate</option>
      </select>
      <button class="rp-btn rp-btn-reprompt" data-spec-id="3.6" disabled>Re-extract / LLM Reprompt</button>
    </div>
  </div>
  <div class="rp-sidebar">
    <div class="rp-value-cards" style="display:none"></div>
    <div class="rp-candidates" style="display:none"></div>
    <div class="rp-actions" style="display:none">
      <input type="number" class="rp-corrected" placeholder="Override value (optional)" step="any">
      <input type="text" class="rp-reject-note" placeholder="Rejection note (required)">
      <button class="rp-btn rp-btn-approve" data-spec-id="3.4">Approve</button>
      <button class="rp-btn rp-btn-reject" data-spec-id="3.5">Reject</button>
    </div>
    <div class="rp-fill-form" style="display:none">
      <label class="rp-fill-label">Metric</label>
      <select class="rp-fill-metric"></select>
      <label class="rp-fill-label">Value</label>
      <input type="number" class="rp-fill-value" placeholder="Value" step="any" min="0">
      <label class="rp-fill-label">Note (optional)</label>
      <input type="text" class="rp-fill-note" placeholder="Note">
      <button class="rp-btn rp-btn-submit-fill">Submit to Review Queue</button>
    </div>
    <div class="rp-status"></div>
  </div>
</div>`;

  // ── Helpers ─────────────────────────────────────────────────────────────────

  function _esc(s) {
    // Use page-global escapeHtml if available, otherwise a simple fallback
    if (typeof escapeHtml === 'function') return escapeHtml(String(s == null ? '' : s));
    const d = document.createElement('div');
    d.textContent = String(s == null ? '' : s);
    return d.innerHTML;
  }

  function _fmtNum(v) {
    if (v == null) return '—';
    return Number(v).toLocaleString(undefined, {maximumFractionDigits: 4});
  }

  function _el(cls) {
    return _container ? _container.querySelector('.' + cls) : null;
  }

  function _getSelectedPackKey() {
    try {
      var k = localStorage.getItem('keyword_pack');
      return k || (_keywordDictionary && _keywordDictionary.active_pack) || 'btc_activity';
    } catch (_e) {
      return (_keywordDictionary && _keywordDictionary.active_pack) || 'btc_activity';
    }
  }

  function _getActiveTerms() {
    var pack = _getSelectedPackKey();
    var packs = (_keywordDictionary && _keywordDictionary.packs) || {};
    var terms = packs[pack] || [];
    return Array.isArray(terms) ? terms : [];
  }

  function _escapeRegExp(s) {
    return String(s).replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
  }

  async function _loadKeywordDictionary() {
    try {
      var resp = await fetch('/api/config/keyword_dictionary');
      if (!resp.ok) return;
      var data = await resp.json();
      if (!data.success || !data.data || !data.data.dictionary) return;
      _keywordDictionary = data.data.dictionary;
    } catch (_e) {
      // Non-fatal: retain fallback dictionary
    }
  }

  function _setStatus(msg, isError) {
    const el = _el('rp-status');
    if (!el) return;
    el.textContent = msg;
    el.style.color = isError ? 'var(--theme-danger)' : 'var(--theme-success)';
  }

  // ── buildHighlightedSource (canonical copy — supersedes doc_panel.js) ───────

  /**
   * Build an HTML string from rawText with coloured <span> wrappers around
   * source_snippet matches. Each match: { metric, source_snippet, metric_label?, pattern_id? }
   */
  function buildHighlightedSource(rawText, matches) {
    var regions = [];
    for (var j = 0; j < (matches || []).length; j++) {
      var m = matches[j];
      if (!m.source_snippet) continue;
      var needle = m.source_snippet.replace(/\s+/g, ' ').trim().slice(0, 60);
      if (needle.length < 6) continue;
      var idx = rawText.toLowerCase().indexOf(needle.toLowerCase());
      if (idx < 0) continue;
      var color = _COLORS[m.metric] || '#9ca3af';
      var label = (m.metric_label || m.metric || '');
      var patternId = m.pattern_id ? ' — ' + m.pattern_id : '';
      regions.push({
        start: idx,
        end:   idx + needle.length,
        color: color,
        label: label + patternId,
      });
    }

    // Add global keyword regions from selected pack
    var terms = _getActiveTerms();
    for (var t = 0; t < terms.length; t++) {
      var term = String(terms[t] || '').trim();
      if (term.length < 2) continue;
      var rx = new RegExp('\\b' + _escapeRegExp(term) + '\\b', 'gi');
      var mrx;
      while ((mrx = rx.exec(rawText)) !== null) {
        regions.push({
          start: mrx.index,
          end: mrx.index + mrx[0].length,
          color: _KEYWORD_COLOR,
          label: 'keyword: ' + term,
        });
        if (mrx.index === rx.lastIndex) rx.lastIndex++; // guard zero-length loops
      }
    }

    // Deterministic region order:
    // 1) earlier start index first
    // 2) for same start, longer region first (keeps full snippet over tiny keyword)
    // 3) for same start/length, lexicographic label
    regions.sort(function (a, b) {
      if (a.start !== b.start) return a.start - b.start;
      var alen = a.end - a.start;
      var blen = b.end - b.start;
      if (alen !== blen) return blen - alen;
      return String(a.label || '').localeCompare(String(b.label || ''));
    });

    var html = '';
    var pos  = 0;
    for (var i = 0; i < regions.length; i++) {
      var r = regions[i];
      if (r.start < pos) continue;
      html += _esc(rawText.slice(pos, r.start));
      var titleAttr = _esc(r.label);
      html += '<span class="doc-hl" style="background:' + r.color
        + '28;border-bottom:2px solid ' + r.color + '" title="' + titleAttr + '">'
        + _esc(rawText.slice(r.start, r.end)) + '</span>';
      pos = r.end;
    }
    html += _esc(rawText.slice(pos));
    return html;
  }

  // ── Value cards ─────────────────────────────────────────────────────────────

  function _renderValueCards(llmValue, regexValue, agreementStatus) {
    const agreementMap = {
      'REVIEW_QUEUE': ['badge-disagree', 'Disagree'],
      'LLM_ONLY':     ['badge-llm-only', 'LLM only'],
      'REGEX_ONLY':   ['badge-regex-only', 'Regex only'],
      'AUTO_ACCEPT':  ['badge-agree', 'Agree'],
    };
    const [cls, label] = agreementMap[agreementStatus] || ['badge-pending', agreementStatus || '—'];
    return `
      <div class="rp-vc regex-card">
        <div class="rp-vc-label">Regex Value</div>
        <div class="rp-vc-value">${_fmtNum(regexValue)}</div>
      </div>
      <div class="rp-vc llm-card">
        <div class="rp-vc-label">LLM Value</div>
        <div class="rp-vc-value">${_fmtNum(llmValue)}</div>
      </div>
      <div class="rp-vc">
        <div class="rp-vc-label">Agreement</div>
        <div class="rp-vc-value" style="font-size:0.82rem"><span class="${_esc(cls)}">${_esc(label)}</span></div>
      </div>`;
  }

  // ── Candidate cards (from re-extract) ───────────────────────────────────────

  function _renderCandidates(candidates) {
    const el = _el('rp-candidates');
    if (!el) return;
    if (!candidates || candidates.length === 0) {
      el.innerHTML = '<div class="rp-candidate-empty">No value found in selected text.</div>';
      el.style.display = 'block';
      return;
    }
    el.innerHTML = candidates.map(function (c, idx) {
      const conf = c.confidence != null ? (c.confidence * 100).toFixed(0) + '%' : '—';
      return `<div class="rp-candidate-card" data-candidate-index="${idx}">
        <span class="rp-cand-source">${_esc(c.source)}</span>
        <span class="rp-cand-value">${_fmtNum(c.value)} ${_esc(c.unit || '')}</span>
        <span class="rp-cand-conf">conf ${_esc(conf)}</span>
        <button class="rp-btn rp-btn-use-candidate" data-value="${_esc(String(c.value))}">Use this value</button>
      </div>`;
    }).join('');
    el.style.display = 'block';
  }

  // ── API calls ───────────────────────────────────────────────────────────────

  async function _fetchDocument(itemId) {
    const resp = await fetch('/api/review/' + itemId + '/document');
    if (!resp.ok) {
      const err = await resp.json().catch(function () { return {}; });
      throw new Error(err.error?.message || 'HTTP ' + resp.status);
    }
    const body = await resp.json();
    if (!body.success) throw new Error(body.error?.message || 'Failed');
    return body.data;
  }

  async function _fetchAnalysis(ticker, period) {
    const p = period.slice(0, 7);
    const resp = await fetch('/api/miner/' + encodeURIComponent(ticker) + '/' + encodeURIComponent(p) + '/analysis');
    if (!resp.ok) {
      const err = await resp.json().catch(function () { return {}; });
      throw new Error(err.error?.message || 'HTTP ' + resp.status);
    }
    const body = await resp.json();
    if (!body.success) throw new Error(body.error?.message || 'Failed');
    return body.data.matches || [];
  }

  async function _fetchRawSource(ticker, period) {
    const p = period.slice(0, 7);
    // /raw-text returns clean plain text (extracted from raw_html when available)
    // for the highlight panel; /raw-source serves the full HTML for the iframe.
    const resp = await fetch('/api/miner/' + encodeURIComponent(ticker) + '/' + encodeURIComponent(p) + '/raw-text');
    if (!resp.ok) return '';
    return await resp.text();
  }

  function _loadIframe(ticker, period) {
    const iframe = _el('rp-doc-iframe');
    if (!iframe) return;
    const p = period.slice(0, 7);
    const src = '/api/miner/' + encodeURIComponent(ticker) + '/' + encodeURIComponent(p) + '/raw-source';
    iframe.src = src;
    iframe.style.display = '';
  }

  function _unloadIframe() {
    const iframe = _el('rp-doc-iframe');
    if (!iframe) return;
    iframe.src = 'about:blank';
    iframe.style.display = 'none';
  }

  // ── Render doc text ─────────────────────────────────────────────────────────

  function _renderDocText(rawText, matches) {
    const el = _el('rp-doc-text');
    if (!el) return;
    // Strip XBRL/EDGAR boilerplate preamble if the page-global helper is available
    if (typeof stripXbrlPreamble === 'function') rawText = stripXbrlPreamble(rawText);
    if (rawText) {
      el.innerHTML = buildHighlightedSource(rawText, matches || []);
      el.style.display = '';
      var firstHl = el.querySelector('.doc-hl');
      if (firstHl) firstHl.scrollIntoView({behavior: 'smooth', block: 'center'});
    } else {
      el.innerHTML = '<em style="color:var(--theme-text-muted)">No document text available.</em>';
      el.style.display = '';
    }
    const bar = _el('rp-reprompt-bar');
    if (bar) bar.style.display = '';
  }

  // ── Internal actions ────────────────────────────────────────────────────────

  async function _doReprompt() {
    const sel = window.getSelection()?.toString().trim();
    if (!sel) return;

    const btn = _el('rp-btn-reprompt');
    if (btn) { btn.disabled = true; btn.textContent = 'Extracting...'; }

    try {
      let url, body;
      if (_currentItemId != null) {
        url  = '/api/review/' + _currentItemId + '/reextract';
        body = {selection: sel};
      } else {
        const metricSel = _el('rp-reprompt-metric');
        const metric = (metricSel && metricSel.value) || _currentMetric;
        if (!metric) { _setStatus('Choose a metric first.', true); return; }
        url  = '/api/review/reextract_selection';
        body = {metric: metric, selection: sel};
      }
      const resp = await fetch(url, {
        method: 'POST',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify(body),
      });
      if (!resp.ok) {
        const err = await resp.json().catch(function () { return {}; });
        throw new Error(err.error?.message || 'HTTP ' + resp.status);
      }
      const data = await resp.json();
      if (!data.success) throw new Error(data.error?.message || 'Failed');
      _renderCandidates(data.data.candidates);
      _setStatus('', false);
    } catch (e) {
      _setStatus('Re-extract failed: ' + e.message, true);
    } finally {
      if (btn) { btn.disabled = false; btn.textContent = 'Re-extract / LLM Reprompt'; }
    }
  }

  async function _doApprove() {
    if (_currentItemId == null) return;
    const valEl = _el('rp-corrected');
    const valRaw = valEl ? valEl.value.trim() : '';
    const body = valRaw !== '' ? {value: parseFloat(valRaw)} : {};

    const btn = _el('rp-btn-approve');
    if (btn) btn.disabled = true;

    try {
      const resp = await fetch('/api/review/' + _currentItemId + '/approve', {
        method: 'POST',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify(body),
      });
      if (!resp.ok) {
        const err = await resp.json().catch(function () { return {}; });
        throw new Error(err.error?.message || 'HTTP ' + resp.status);
      }
      const data = await resp.json();
      if (!data.success) throw new Error(data.error?.message || 'Failed');
      const itemId = _currentItemId;
      const value  = valRaw !== '' ? parseFloat(valRaw) : null;
      if (_onApproved) _onApproved({itemId: itemId, value: value});
    } catch (e) {
      _setStatus('Approve failed: ' + e.message, true);
      if (btn) btn.disabled = false;
    }
  }

  async function _doReject() {
    if (_currentItemId == null) return;
    const noteEl = _el('rp-reject-note');
    const note = noteEl ? noteEl.value.trim() : '';
    if (!note) {
      _setStatus('A rejection note is required.', true);
      if (noteEl) noteEl.focus();
      return;
    }

    const btn = _el('rp-btn-reject');
    if (btn) btn.disabled = true;

    try {
      const resp = await fetch('/api/review/' + _currentItemId + '/reject', {
        method: 'POST',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify({note: note}),
      });
      if (!resp.ok) {
        const err = await resp.json().catch(function () { return {}; });
        throw new Error(err.error?.message || 'HTTP ' + resp.status);
      }
      const data = await resp.json();
      if (!data.success) throw new Error(data.error?.message || 'Failed');
      const itemId = _currentItemId;
      if (_onRejected) _onRejected({itemId: itemId});
    } catch (e) {
      _setStatus('Reject failed: ' + e.message, true);
      if (btn) btn.disabled = false;
    }
  }

  async function _doFill() {
    const metricEl = _el('rp-fill-metric');
    const valueEl  = _el('rp-fill-value');
    const noteEl   = _el('rp-fill-note');
    const metric   = metricEl ? metricEl.value : (_currentMetric || '');
    const value    = valueEl  ? parseFloat(valueEl.value) : NaN;
    const note     = noteEl   ? noteEl.value.trim() : '';

    if (!metric) { _setStatus('Select a metric.', true); return; }
    if (!isFinite(value) || value < 0) {
      _setStatus('Enter a valid non-negative value.', true);
      return;
    }

    const btn = _el('rp-btn-submit-fill');
    if (btn) btn.disabled = true;

    try {
      const resp = await fetch('/api/timeseries/fill', {
        method: 'POST',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify({
          ticker: _currentTicker,
          metric: metric,
          period: _currentPeriod,
          value:  value,
          note:   note,
        }),
      });
      const data = await resp.json().catch(function () { return {}; });
      if (resp.status === 409) {
        _setStatus('Data already exists for this period.', true);
        if (btn) btn.disabled = false;
        return;
      }
      if (!resp.ok || !data.success) {
        throw new Error(data.error?.message || 'HTTP ' + resp.status);
      }
      if (_onFilled) _onFilled({ticker: _currentTicker, period: _currentPeriod, metric: metric, value: value});
    } catch (e) {
      _setStatus('Submit failed: ' + e.message, true);
      if (btn) btn.disabled = false;
    }
  }

  // ── Wire event handlers (called ONCE from init) ─────────────────────────────

  function _wireHandlers() {
    // Text selection on doc text → enable reprompt button
    const docEl = _el('rp-doc-text');
    if (docEl) {
      docEl.addEventListener('mouseup', _updateRepromptBtn);
      docEl.addEventListener('keyup', _updateRepromptBtn);
    }

    // Reprompt button
    const repromptBtn = _el('rp-btn-reprompt');
    if (repromptBtn) repromptBtn.addEventListener('click', _doReprompt);

    // Approve / Reject
    const approveBtn = _el('rp-btn-approve');
    if (approveBtn) approveBtn.addEventListener('click', _doApprove);

    const rejectBtn = _el('rp-btn-reject');
    if (rejectBtn) rejectBtn.addEventListener('click', _doReject);

    // Submit fill
    const fillBtn = _el('rp-btn-submit-fill');
    if (fillBtn) fillBtn.addEventListener('click', _doFill);

    // "Use this value" — delegated on rp-candidates (persistent parent)
    const candidatesEl = _el('rp-candidates');
    if (candidatesEl) {
      candidatesEl.addEventListener('click', function (e) {
        const btn = e.target.closest('.rp-btn-use-candidate');
        if (!btn) return;
        const val = btn.getAttribute('data-value');
        const correctedEl = _el('rp-corrected');
        if (correctedEl) correctedEl.value = val;
      });
    }
  }

  function _updateRepromptBtn() {
    const btn = _el('rp-btn-reprompt');
    if (!btn) return;
    const sel = window.getSelection();
    btn.disabled = !(sel && sel.toString().trim().length > 0);
  }

  // ── Public API ──────────────────────────────────────────────────────────────

  /**
   * Inject HTML skeleton into container and wire event handlers once.
   * @param {string} containerId  id of the target container element
   */
  function init(containerId) {
    _container = document.getElementById(containerId);
    if (!_container) return;
    _container.innerHTML = _HTML;
    _loadKeywordDictionary();
    _wireHandlers();
  }

  /**
   * Open a review-queue item by ID.
   * Fetches document text + value comparison; shows approve/reject controls.
   */
  async function openItem(itemId) {
    if (!_container) return;
    _currentItemId = itemId;

    // Reset sidebar state
    const candidatesEl = _el('rp-candidates');
    if (candidatesEl) { candidatesEl.innerHTML = ''; candidatesEl.style.display = 'none'; }
    const fillEl = _el('rp-fill-form');
    if (fillEl) fillEl.style.display = 'none';
    const actionsEl = _el('rp-actions');
    if (actionsEl) actionsEl.style.display = 'none';
    const cardsEl = _el('rp-value-cards');
    if (cardsEl) cardsEl.style.display = 'none';
    _setStatus('', false);

    // Show placeholder while loading; clear iframe (review items use text only)
    _unloadIframe();
    const placeholder = _el('rp-doc-placeholder');
    if (placeholder) { placeholder.textContent = 'Loading...'; placeholder.style.display = ''; }
    const docEl = _el('rp-doc-text');
    if (docEl) docEl.style.display = 'none';
    const repromptBar = _el('rp-reprompt-bar');
    if (repromptBar) repromptBar.style.display = 'none';

    // Reset action buttons
    const approveBtn = _el('rp-btn-approve');
    if (approveBtn) approveBtn.disabled = false;
    const rejectBtn = _el('rp-btn-reject');
    if (rejectBtn) rejectBtn.disabled = false;
    const correctedEl = _el('rp-corrected');
    if (correctedEl) correctedEl.value = '';
    const rejectNoteEl = _el('rp-reject-note');
    if (rejectNoteEl) rejectNoteEl.value = '';

    try {
      const doc = await _fetchDocument(itemId);

      // Update meta header
      const metaEl = _el('rp-source-meta');
      if (metaEl) {
        let metaHtml = '';
        if (doc.source_url) {
          if (doc.source_url.startsWith('http')) {
            metaHtml = '<a href="' + _esc(doc.source_url) + '" target="_blank" rel="noopener" style="color:var(--theme-accent)">'
              + _esc(doc.source_url.slice(0, 60)) + (doc.source_url.length > 60 ? '…' : '') + '</a>';
          } else {
            metaHtml = _esc(doc.source_url);
          }
        }
        metaEl.innerHTML = metaHtml;
      }

      // Value cards
      if (cardsEl) {
        cardsEl.innerHTML = _renderValueCards(doc.llm_value, doc.regex_value, doc.agreement_status);
        cardsEl.style.display = 'flex';
      }

      // Pre-fill corrected value
      if (correctedEl) {
        if (doc.regex_value != null) correctedEl.value = doc.regex_value;
      }

      // Pre-select metric in reprompt dropdown
      if (doc.metric) {
        const metricSel = _el('rp-reprompt-metric');
        if (metricSel) metricSel.value = doc.metric;
      }

      // Render doc
      if (placeholder) placeholder.style.display = 'none';
      _renderDocText(doc.raw_text, doc.source_snippet
        ? [{metric: '', source_snippet: doc.source_snippet, metric_label: 'Source'}]
        : []);

      // Show actions
      if (actionsEl) actionsEl.style.display = 'flex';

    } catch (e) {
      if (placeholder) { placeholder.textContent = 'Error loading document: ' + e.message; placeholder.style.display = ''; }
    }
  }

  /**
   * Open for a grid cell (ticker + period + metric).
   * If a PENDING review item exists, delegates to openItem().
   * Otherwise fetches analysis + raw-source and shows the fill form.
   *
   * @param {string} ticker
   * @param {string} period  YYYY-MM or YYYY-MM-DD
   * @param {string} metric
   * @param {object} opts
   *   opts.nullMetrics {string[]} — metrics to include in fill dropdown
   */
  async function openCell(ticker, period, metric, opts) {
    if (!_container) return;
    _currentTicker = ticker;
    _currentPeriod = period;
    _currentMetric = metric;
    _currentItemId = null;
    const nullMetrics = (opts && opts.nullMetrics) || (metric ? [metric] : []);

    // Reset sidebar
    const candidatesEl = _el('rp-candidates');
    if (candidatesEl) { candidatesEl.innerHTML = ''; candidatesEl.style.display = 'none'; }
    const cardsEl = _el('rp-value-cards');
    if (cardsEl) { cardsEl.innerHTML = ''; cardsEl.style.display = 'none'; }
    const actionsEl = _el('rp-actions');
    if (actionsEl) actionsEl.style.display = 'none';
    const fillEl = _el('rp-fill-form');
    if (fillEl) fillEl.style.display = 'none';
    _setStatus('', false);

    // Show loading state
    const placeholder = _el('rp-doc-placeholder');
    if (placeholder) { placeholder.textContent = 'Loading...'; placeholder.style.display = ''; }
    const docEl = _el('rp-doc-text');
    if (docEl) docEl.style.display = 'none';
    const repromptBar = _el('rp-reprompt-bar');
    if (repromptBar) repromptBar.style.display = 'none';

    try {
      // Look for a PENDING review item
      let qs = '?ticker=' + encodeURIComponent(ticker)
        + '&period=' + encodeURIComponent(period)
        + '&status=PENDING&limit=1';
      if (metric) qs += '&metric=' + encodeURIComponent(metric);
      const rResp = await fetch('/api/review' + qs);
      if (rResp.ok) {
        const rData = await rResp.json();
        if (rData.success && rData.data.items.length > 0) {
          // Delegate to openItem — sets _currentItemId
          await openItem(rData.data.items[0].id);
          return;
        }
      }

      // No pending item — show analysis + raw source + fill form
      const [rawText, matches] = await Promise.all([
        _fetchRawSource(ticker, period),
        _fetchAnalysis(ticker, period).catch(function () { return []; }),
      ]);

      if (placeholder) placeholder.style.display = 'none';
      // Load rendered HTML in iframe (raw-source serves raw_html when available)
      _loadIframe(ticker, period);
      _renderDocText(rawText, matches);

      // Populate fill dropdown
      if (fillEl) {
        const metricSel = fillEl.querySelector('.rp-fill-metric');
        if (metricSel) {
          metricSel.innerHTML = nullMetrics.map(function (m) {
            return '<option value="' + _esc(m) + '"' + (m === metric ? ' selected' : '') + '>' + _esc(m) + '</option>';
          }).join('');
          if (nullMetrics.length === 0) {
            metricSel.innerHTML = '<option value="' + _esc(metric || '') + '">' + _esc(metric || '') + '</option>';
          }
        }
        // Clear previous values
        const valEl = fillEl.querySelector('.rp-fill-value');
        if (valEl) valEl.value = '';
        const noteEl = fillEl.querySelector('.rp-fill-note');
        if (noteEl) noteEl.value = '';
        fillEl.style.display = 'flex';
      }

    } catch (e) {
      if (placeholder) { placeholder.textContent = 'Error: ' + e.message; placeholder.style.display = ''; }
    }
  }

  /** Clear state and return to placeholder. */
  function close() {
    _currentItemId = null;
    _currentTicker = null;
    _currentPeriod = null;
    _currentMetric = null;

    const placeholder = _el('rp-doc-placeholder');
    if (placeholder) { placeholder.textContent = 'Select a row to view the document.'; placeholder.style.display = ''; }

    const docEl = _el('rp-doc-text');
    if (docEl) { docEl.innerHTML = ''; docEl.style.display = 'none'; }
    _unloadIframe();

    const repromptBar = _el('rp-reprompt-bar');
    if (repromptBar) repromptBar.style.display = 'none';

    const cardsEl = _el('rp-value-cards');
    if (cardsEl) { cardsEl.innerHTML = ''; cardsEl.style.display = 'none'; }

    const candidatesEl = _el('rp-candidates');
    if (candidatesEl) { candidatesEl.innerHTML = ''; candidatesEl.style.display = 'none'; }

    const actionsEl = _el('rp-actions');
    if (actionsEl) actionsEl.style.display = 'none';

    const fillEl = _el('rp-fill-form');
    if (fillEl) fillEl.style.display = 'none';

    _setStatus('', false);
  }

  function setOnApproved(fn)  { _onApproved = fn; }
  function setOnRejected(fn)  { _onRejected = fn; }
  function setOnFilled(fn)    { _onFilled   = fn; }

  return {
    init,
    openItem,
    openCell,
    close,
    setOnApproved,
    setOnRejected,
    setOnFilled,
    buildHighlightedSource,
  };
})();
