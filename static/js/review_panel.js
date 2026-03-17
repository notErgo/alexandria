/**
 * review_panel.js — Shared review/document viewer used by review.html,
 * miner_data.js, and dashboard contexts.
 */
const ReviewPanel = (function () {
  'use strict';

  const _KEYWORD_COLOR = '#9ca3af';
  const _EXTRACTED_DEFAULT = '#facc15';
  const _KEYWORD_RANK_COLORS = {
    1: '#f97316',
    2: '#3b82f6',
    3: '#22c55e',
  };

  let _container = null;
  let _currentItemId = null;
  let _currentTicker = null;
  let _currentPeriod = null;
  let _currentMetric = null;
  let _currentReportId = null;
  let _currentRawText = '';
  let _currentMatches = [];
  let _showGenericKeywords = true;
  let _keywordDictionary = {
    active_pack: 'btc_activity',
    packs: { btc_activity: ['bitcoin', 'btc'] },
  };
  let _onApproved = null;
  let _onRejected = null;
  let _onFilled = null;
  let _onWritten = null;

  const _HTML = `
<div class="rp-layout">
  <details class="rp-controls-details">
    <summary class="rp-controls-summary">Controls</summary>
    <div class="rp-controls-top">
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
        <input type="number" class="rp-fill-value" placeholder="Value" step="any">
        <label class="rp-fill-label">Note (optional)</label>
        <input type="text" class="rp-fill-note" placeholder="Note">
        <button class="rp-btn rp-btn-submit-fill">Submit to Review Queue</button>
        <button class="rp-btn rp-btn-write-cell">Write to Cell</button>
      </div>
      <div class="rp-reprompt-bar" style="display:none">
        <span class="rp-hint">Highlight a value in the text below, select metric, click Re-extract:</span>
        <select class="rp-reprompt-metric"></select>
        <button class="rp-btn rp-btn-reprompt" data-spec-id="3.6" disabled>Re-extract / LLM Reprompt</button>
      </div>
      <label class="rp-toggle-row">
        <input type="checkbox" class="rp-keyword-toggle" checked>
        <span>Show generic keyword highlights</span>
      </label>
      <div class="rp-status"></div>
    </div>
  </details>
  <div class="rp-source-col">
    <div class="rp-source-header">
      <div class="rp-source-title-wrap">
        <div class="rp-source-title">Document Viewer</div>
        <div class="rp-source-meta"></div>
      </div>
    </div>
    <div class="rp-doc-placeholder">Select a row to view the document.</div>
    <div class="rp-evidence" style="display:none">
      <div class="rp-evidence-header">Extracted Evidence</div>
      <div class="rp-evidence-list"></div>
    </div>
    <div class="rp-doc-scroll" style="display:none">
      <div class="rp-doc-text"></div>
    </div>
  </div>
</div>`;

  function _esc(value) {
    if (typeof escapeHtml === 'function') return escapeHtml(String(value == null ? '' : value));
    const div = document.createElement('div');
    div.textContent = String(value == null ? '' : value);
    return div.innerHTML;
  }

  function _fmtNum(value) {
    if (value == null || value === '') return '—';
    const num = Number(value);
    return Number.isFinite(num)
      ? num.toLocaleString(undefined, {maximumFractionDigits: 4})
      : _esc(value);
  }

  function _el(className) {
    return _container ? _container.querySelector('.' + className) : null;
  }

  function _sourceLabel(doc) {
    const bits = [];
    if (doc.source_type) bits.push(doc.source_type);
    if (doc.report_date) bits.push(String(doc.report_date).slice(0, 10));
    return bits.join(' · ');
  }

  function _getSelectedPackKey() {
    try {
      return localStorage.getItem('keyword_pack') || _keywordDictionary.active_pack || 'btc_activity';
    } catch (_err) {
      return _keywordDictionary.active_pack || 'btc_activity';
    }
  }

  function _getActiveTerms() {
    const packKey = _getSelectedPackKey();
    const terms = (_keywordDictionary.packs || {})[packKey] || [];
    return Array.isArray(terms) ? terms : [];
  }

  function _escapeRegExp(text) {
    return String(text).replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
  }

  function _highlightColor(match) {
    const rank = Number(match && match.keyword_rank);
    if (rank && _KEYWORD_RANK_COLORS[rank]) return _KEYWORD_RANK_COLORS[rank];
    if (rank && rank > 3) return _EXTRACTED_DEFAULT;
    if (match && match.keyword_color_key === 'yellow') return _EXTRACTED_DEFAULT;
    return _EXTRACTED_DEFAULT;
  }

  async function _loadKeywordDictionary() {
    try {
      const resp = await fetch('/api/config/keyword_dictionary');
      if (!resp.ok) return;
      const body = await resp.json();
      if (body.success && body.data && body.data.dictionary) {
        _keywordDictionary = body.data.dictionary;
      }
    } catch (_err) {
      // Non-fatal.
    }
  }

  function _setStatus(msg, isError) {
    const statusEl = _el('rp-status');
    if (!statusEl) return;
    statusEl.textContent = msg || '';
    statusEl.style.color = isError ? 'var(--theme-danger)' : 'var(--theme-success)';
  }

  function _buildHighlightState(rawText, matches, includeGenericKeywords) {
    const regions = [];
    const evidenceAnchors = {};

    for (let idx = 0; idx < (matches || []).length; idx++) {
      const match = matches[idx];
      if (!match || !match.source_snippet) continue;
      const needle = String(match.source_snippet).replace(/\s+/g, ' ').trim().slice(0, 120);
      if (needle.length < 4) continue;
      const pos = rawText.toLowerCase().indexOf(needle.toLowerCase());
      if (pos < 0) continue;
      regions.push({
        start: pos,
        end: pos + needle.length,
        color: _highlightColor(match),
        label: (match.metric_label || match.metric || 'Extracted evidence')
          + (match.matched_keyword ? ' · ' + match.matched_keyword : '')
          + (match.pattern_id ? ' · ' + match.pattern_id : ''),
        evidenceIndex: idx,
        cssClass: 'doc-hl-evidence',
      });
      evidenceAnchors[idx] = true;
    }

    if (includeGenericKeywords) {
      const terms = _getActiveTerms();
      for (let i = 0; i < terms.length; i++) {
        const term = String(terms[i] || '').trim();
        if (term.length < 2) continue;
        const re = new RegExp('\\b' + _escapeRegExp(term) + '\\b', 'gi');
        let hit;
        while ((hit = re.exec(rawText)) !== null) {
          regions.push({
            start: hit.index,
            end: hit.index + hit[0].length,
            color: _KEYWORD_COLOR,
            label: 'keyword: ' + term,
            evidenceIndex: null,
            cssClass: 'doc-hl-keyword',
          });
          if (hit.index === re.lastIndex) re.lastIndex += 1;
        }
      }
    }

    regions.sort(function(a, b) {
      if (a.start !== b.start) return a.start - b.start;
      const aLen = a.end - a.start;
      const bLen = b.end - b.start;
      if (aLen !== bLen) return bLen - aLen;
      return String(a.label || '').localeCompare(String(b.label || ''));
    });

    let html = '';
    let pos = 0;
    for (let i = 0; i < regions.length; i++) {
      const region = regions[i];
      if (region.start < pos) continue;
      html += _esc(rawText.slice(pos, region.start));
      const attrs = [
        'class="doc-hl ' + region.cssClass + '"',
        'style="background:' + region.color + '40;border-bottom:2px solid ' + region.color + '"',
        'title="' + _esc(region.label) + '"',
      ];
      if (region.evidenceIndex != null) attrs.push('data-evidence-index="' + region.evidenceIndex + '"');
      html += '<span ' + attrs.join(' ') + '>' + _esc(rawText.slice(region.start, region.end)) + '</span>';
      pos = region.end;
    }
    html += _esc(rawText.slice(pos));
    return {html: html, evidenceAnchors: evidenceAnchors};
  }

  function buildHighlightedSource(rawText, matches) {
    return _buildHighlightState(rawText || '', matches || [], true).html;
  }

  function _renderValueCards(candidateValue, reviewReason) {
    const badgeMap = {
      OUTLIER_FLAGGED: ['badge-disagree', 'Outlier'],
      LLM_ONLY: ['badge-llm-only', 'LLM only'],
      REGEX_ONLY: ['badge-regex-only', 'Regex only'],
    };
    const badge = badgeMap[reviewReason] || ['badge-pending', reviewReason || '—'];
    return `
      <div class="rp-vc llm-card">
        <div class="rp-vc-label">Candidate Value</div>
        <div class="rp-vc-value">${_fmtNum(candidateValue)}</div>
      </div>
      <div class="rp-vc">
        <div class="rp-vc-label">Reason</div>
        <div class="rp-vc-value" style="font-size:0.82rem"><span class="${_esc(badge[0])}">${_esc(badge[1])}</span></div>
      </div>`;
  }

  function _renderCandidates(candidates) {
    const el = _el('rp-candidates');
    if (!el) return;
    if (!candidates || !candidates.length) {
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

  function _renderSourceHeader(selectionData, currentReportId, currentDoc) {
    const titleEl = _el('rp-source-title');
    const metaEl = _el('rp-source-meta');
    if (!titleEl || !metaEl) return;

    const docTitle = currentDoc && currentDoc.document_title
      ? currentDoc.document_title
      : 'Document Viewer';
    titleEl.textContent = docTitle;

    if (!selectionData || !selectionData.selected) {
      metaEl.innerHTML = currentDoc && currentDoc.source_url
        ? '<a href="' + _esc(currentDoc.source_url) + '" target="_blank" rel="noopener" style="color:var(--theme-accent)">' + _esc(currentDoc.source_url) + '</a>'
        : '<span style="color:var(--theme-text-muted)">No document metadata available.</span>';
      return;
    }

    const selected = selectionData.selected;
    const docs = [selected].concat(selectionData.alternatives || []);
    const current = currentReportId != null
      ? docs.find(function (doc) { return doc.id === currentReportId; }) || selected
      : selected;

    let html = '<span class="rp-doc-sel">';
    html += '<span class="rp-doc-sel-type">' + _esc(current.source_type || '') + '</span>';
    if (current.report_date) html += '<span class="rp-doc-sel-date">' + _esc(String(current.report_date).slice(0, 10)) + '</span>';
    if (current.priority != null) html += '<span class="rp-doc-sel-prio">p' + _esc(String(current.priority)) + '/3</span>';
    if (docs.length > 1) {
      html += '<span class="rp-doc-sel-alts"><select class="rp-doc-alt-select">';
      html += docs.map(function (doc) {
        const label = (doc.document_title || _sourceLabel(doc) || 'Document').slice(0, 90);
        return '<option value="' + _esc(String(doc.id)) + '"' + (doc.id === current.id ? ' selected' : '') + '>'
          + _esc(label) + '</option>';
      }).join('');
      html += '</select></span>';
    }
    if (current.source_url) {
      html += '<a href="' + _esc(current.source_url) + '" target="_blank" rel="noopener" style="color:var(--theme-accent)">source</a>';
    }
    html += '</span>';
    metaEl.innerHTML = html;

    const altSelect = metaEl.querySelector('.rp-doc-alt-select');
    if (altSelect) {
      altSelect.addEventListener('change', function () {
        const nextId = altSelect.value ? parseInt(altSelect.value, 10) : null;
        if (Number.isFinite(nextId)) _loadSpecificReport(nextId);
      });
    }
  }

  function _renderEvidence(matches, anchors) {
    const wrap = _el('rp-evidence');
    const list = _el('rp-evidence-list');
    if (!wrap || !list) return;
    const items = [];
    (matches || []).forEach(function (match, idx) {
      if (match && match.source_snippet && anchors[idx]) items.push({match: match, index: idx});
    });
    if (!items.length) {
      list.innerHTML = '';
      wrap.style.display = 'none';
      return;
    }
    let html = '<table class="rp-evidence-table"><thead><tr>'
      + '<th>Metric</th><th>Amount</th><th>Extracted text</th>'
      + '</tr></thead><tbody>';
    items.forEach(function (entry) {
      const match = entry.match;
      const rank = Number(match.keyword_rank);
      const badgeColor = rank && _KEYWORD_RANK_COLORS[rank]
        ? _KEYWORD_RANK_COLORS[rank]
        : (match.keyword_color_key === 'yellow' ? _EXTRACTED_DEFAULT : '#64748b');
      const keyLabel = match.matched_keyword || match.pattern_id || '';
      const metricCell = _esc(match.metric_label || match.metric || 'Match')
        + (keyLabel ? '<br><span class="rp-evidence-key">' + _esc(keyLabel) + '</span>' : '');
      const amountCell = match.value != null
        ? '<span class="rp-evidence-value" style="color:' + badgeColor + '">'
            + _fmtNum(match.value) + (match.unit ? ' ' + _esc(match.unit) : '') + '</span>'
        : '<span class="rp-evidence-value-empty">\u2014</span>';
      html += '<tr class="rp-evidence-row" data-evidence-index="' + _esc(String(entry.index)) + '">'
        + '<td class="rp-ev-metric">' + metricCell + '</td>'
        + '<td class="rp-ev-amount">' + amountCell + '</td>'
        + '<td class="rp-ev-snippet">' + _esc(match.source_snippet) + '</td>'
        + '</tr>';
    });
    html += '</tbody></table>';
    list.innerHTML = html;
    wrap.style.display = 'block';
  }

  function _renderDocText(rawText, matches) {
    _currentRawText = rawText || '';
    _currentMatches = matches || [];

    const placeholder = _el('rp-doc-placeholder');
    const scroll = _el('rp-doc-scroll');
    const textEl = _el('rp-doc-text');
    if (!textEl || !scroll) return;

    if (typeof stripXbrlPreamble === 'function') _currentRawText = stripXbrlPreamble(_currentRawText);
    if (!_currentRawText) {
      if (placeholder) {
        placeholder.textContent = 'No document text available.';
        placeholder.style.display = '';
      }
      scroll.style.display = 'none';
      _renderEvidence([], {});
      return;
    }

    const result = _buildHighlightState(_currentRawText, _currentMatches, _showGenericKeywords);
    textEl.innerHTML = result.html;
    scroll.style.display = '';
    if (placeholder) placeholder.style.display = 'none';
    _renderEvidence(_currentMatches, result.evidenceAnchors);

    const firstHighlight = textEl.querySelector('.doc-hl-evidence');
    if (firstHighlight) firstHighlight.scrollIntoView({behavior: 'smooth', block: 'center'});

    const repromptBar = _el('rp-reprompt-bar');
    if (repromptBar) repromptBar.style.display = '';
  }

  function _scrollToEvidence(index) {
    const textEl = _el('rp-doc-text');
    if (!textEl) return;
    const target = textEl.querySelector('.doc-hl-evidence[data-evidence-index="' + index + '"]');
    if (target) target.scrollIntoView({behavior: 'smooth', block: 'center'});
  }

  async function _fetchJson(url, options) {
    const resp = await fetch(url, options);
    const body = await resp.json().catch(function () { return {}; });
    if (!resp.ok || !body.success) {
      throw new Error((body.error && body.error.message) || ('HTTP ' + resp.status));
    }
    return body.data;
  }

  function _fetchDocument(itemId) {
    return _fetchJson('/api/review/' + encodeURIComponent(itemId) + '/document');
  }

  function _fetchPeriodReports(ticker, period) {
    const p = String(period || '').slice(0, 7);
    return _fetchJson('/api/miner/' + encodeURIComponent(ticker) + '/' + encodeURIComponent(p) + '/reports');
  }

  function _fetchAnalysis(ticker, period, reportId) {
    let url = '/api/miner/' + encodeURIComponent(ticker) + '/' + encodeURIComponent(String(period || '').slice(0, 7)) + '/analysis';
    if (reportId != null) url += '?report_id=' + encodeURIComponent(reportId);
    return _fetchJson(url).then(function (data) { return data.matches || []; });
  }

  async function _fetchRawSource(ticker, period, reportId) {
    let url = '/api/miner/' + encodeURIComponent(ticker) + '/' + encodeURIComponent(String(period || '').slice(0, 7)) + '/raw-text';
    if (reportId != null) url += '?report_id=' + encodeURIComponent(reportId);
    const resp = await fetch(url);
    if (!resp.ok) return '';
    return resp.text();
  }

  async function _loadSpecificReport(reportId) {
    if (!_currentTicker || !_currentPeriod) return;
    _currentReportId = reportId;
    try {
      const [selectionData, rawText, matches] = await Promise.all([
        _fetchPeriodReports(_currentTicker, _currentPeriod),
        _fetchRawSource(_currentTicker, _currentPeriod, reportId),
        _fetchAnalysis(_currentTicker, _currentPeriod, reportId).catch(function () { return []; }),
      ]);
      const docs = selectionData && selectionData.selected
        ? [selectionData.selected].concat(selectionData.alternatives || [])
        : [];
      const currentDoc = docs.find(function (doc) { return doc.id === reportId; }) || null;
      _renderSourceHeader(selectionData, reportId, currentDoc);
      _renderDocText(rawText, matches);
    } catch (err) {
      _setStatus('Document switch failed: ' + err.message, true);
    }
  }

  function _resetPanels() {
    const cards = _el('rp-value-cards');
    const candidates = _el('rp-candidates');
    const actions = _el('rp-actions');
    const fill = _el('rp-fill-form');
    const evidence = _el('rp-evidence');
    const placeholder = _el('rp-doc-placeholder');
    const scroll = _el('rp-doc-scroll');
    const corrected = _el('rp-corrected');
    const reject = _el('rp-reject-note');
    const reprompt = _el('rp-reprompt-bar');
    if (cards) { cards.innerHTML = ''; cards.style.display = 'none'; }
    if (candidates) { candidates.innerHTML = ''; candidates.style.display = 'none'; }
    if (actions) actions.style.display = 'none';
    if (fill) fill.style.display = 'none';
    if (evidence) evidence.style.display = 'none';
    if (placeholder) { placeholder.textContent = 'Loading...'; placeholder.style.display = ''; }
    if (scroll) scroll.style.display = 'none';
    if (corrected) corrected.value = '';
    if (reject) reject.value = '';
    if (reprompt) reprompt.style.display = 'none';
    _setStatus('', false);
  }

  async function _doReprompt() {
    const selection = window.getSelection() && window.getSelection().toString().trim();
    if (!selection) return;
    const button = _el('rp-btn-reprompt');
    if (button) { button.disabled = true; button.textContent = 'Extracting...'; }
    try {
      let url;
      let body;
      if (_currentItemId != null) {
        url = '/api/review/' + encodeURIComponent(_currentItemId) + '/reextract';
        body = {selection: selection};
      } else {
        const metricSelect = _el('rp-reprompt-metric');
        const metric = metricSelect ? metricSelect.value : _currentMetric;
        if (!metric) throw new Error('Choose a metric first.');
        url = '/api/review/reextract_selection';
        body = {metric: metric, selection: selection};
      }
      const data = await _fetchJson(url, {
        method: 'POST',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify(body),
      });
      _renderCandidates(data.candidates || []);
      _setStatus('', false);
    } catch (err) {
      _setStatus('Re-extract failed: ' + err.message, true);
    } finally {
      if (button) { button.disabled = false; button.textContent = 'Re-extract / LLM Reprompt'; }
    }
  }

  async function _doApprove() {
    if (_currentItemId == null) return;
    const corrected = _el('rp-corrected');
    const rawValue = corrected ? corrected.value.trim() : '';
    const payload = rawValue !== '' ? {value: parseFloat(rawValue)} : {};
    try {
      await _fetchJson('/api/review/' + encodeURIComponent(_currentItemId) + '/approve', {
        method: 'POST',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify(payload),
      });
      if (_onApproved) _onApproved({itemId: _currentItemId, value: rawValue !== '' ? parseFloat(rawValue) : null});
    } catch (err) {
      _setStatus('Approve failed: ' + err.message, true);
    }
  }

  async function _doReject() {
    if (_currentItemId == null) return;
    const noteInput = _el('rp-reject-note');
    const note = noteInput ? noteInput.value.trim() : '';
    if (!note) {
      _setStatus('A rejection note is required.', true);
      if (noteInput) noteInput.focus();
      return;
    }
    try {
      await _fetchJson('/api/review/' + encodeURIComponent(_currentItemId) + '/reject', {
        method: 'POST',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify({note: note}),
      });
      if (_onRejected) _onRejected({itemId: _currentItemId});
    } catch (err) {
      _setStatus('Reject failed: ' + err.message, true);
    }
  }

  async function _doFill() {
    const metricSelect = _el('rp-fill-metric');
    const valueInput = _el('rp-fill-value');
    const noteInput = _el('rp-fill-note');
    const metric = metricSelect ? metricSelect.value : _currentMetric;
    const value = valueInput ? parseFloat(valueInput.value) : NaN;
    const note = noteInput ? noteInput.value.trim() : '';
    if (!metric) return _setStatus('Select a metric.', true);
    if (!Number.isFinite(value)) return _setStatus('Enter a valid value.', true);
    try {
      await _fetchJson('/api/timeseries/fill', {
        method: 'POST',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify({
          ticker: _currentTicker,
          metric: metric,
          period: _currentPeriod,
          value: value,
          note: note,
        }),
      });
      if (_onFilled) _onFilled({ticker: _currentTicker, period: _currentPeriod, metric: metric, value: value});
    } catch (err) {
      _setStatus('Submit failed: ' + err.message, true);
    }
  }

  async function _doWriteToCell() {
    const metricSelect = _el('rp-fill-metric');
    const valueInput = _el('rp-fill-value');
    const noteInput = _el('rp-fill-note');
    const metric = metricSelect ? metricSelect.value : _currentMetric;
    const value = valueInput ? parseFloat(valueInput.value) : NaN;
    const note = noteInput ? noteInput.value.trim() : '';
    if (!_currentTicker || !_currentPeriod) return _setStatus('No cell selected.', true);
    if (!metric) return _setStatus('Select a metric.', true);
    if (!Number.isFinite(value)) return _setStatus('Enter a valid value.', true);
    // Normalize to YYYY-MM-01 for monthly periods so the stored period matches
    // the data_points/timeline spine format. SEC periods (YYYY-Qn, YYYY-FY)
    // and full YYYY-MM-DD dates pass through unchanged.
    let period = String(_currentPeriod || '');
    if (/^\d{4}-\d{2}$/.test(period)) period = period + '-01';
    const url = '/api/explorer/cell/' + encodeURIComponent(_currentTicker) + '/'
      + encodeURIComponent(period) + '/' + encodeURIComponent(metric) + '/save';
    try {
      await _fetchJson(url, {
        method: 'POST',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify({value: value, note: note, manual: true}),
      });
      _setStatus('Written to cell (' + metric + ' = ' + value + ')', false);
      if (_onWritten) _onWritten({ticker: _currentTicker, period: _currentPeriod, metric: metric, value: value});
    } catch (err) {
      _setStatus('Write failed: ' + err.message, true);
    }
  }

  function _updateRepromptButton() {
    const btn = _el('rp-btn-reprompt');
    if (!btn) return;
    const selection = window.getSelection();
    btn.disabled = !(selection && selection.toString().trim().length > 0);
  }

  function _wireHandlers() {
    const docText = _el('rp-doc-text');
    if (docText) {
      docText.addEventListener('mouseup', _updateRepromptButton);
      docText.addEventListener('keyup', _updateRepromptButton);
      docText.addEventListener('click', function (event) {
        const button = event.target.closest('.rp-evidence-item');
        if (button) event.preventDefault();
      });
    }

    const reprompt = _el('rp-btn-reprompt');
    if (reprompt) reprompt.addEventListener('click', _doReprompt);
    const approve = _el('rp-btn-approve');
    if (approve) approve.addEventListener('click', _doApprove);
    const reject = _el('rp-btn-reject');
    if (reject) reject.addEventListener('click', _doReject);
    const fill = _el('rp-btn-submit-fill');
    if (fill) fill.addEventListener('click', _doFill);
    const write = _el('rp-btn-write-cell');
    if (write) write.addEventListener('click', _doWriteToCell);

    const candidates = _el('rp-candidates');
    if (candidates) {
      candidates.addEventListener('click', function (event) {
        const button = event.target.closest('.rp-btn-use-candidate');
        if (!button) return;
        const input = _el('rp-corrected');
        if (input) input.value = button.getAttribute('data-value') || '';
      });
    }

    const evidenceList = _el('rp-evidence-list');
    if (evidenceList) {
      evidenceList.addEventListener('click', function (event) {
        const item = event.target.closest('.rp-evidence-row');
        if (!item) return;
        _scrollToEvidence(item.getAttribute('data-evidence-index'));
      });
    }

    const keywordToggle = _el('rp-keyword-toggle');
    if (keywordToggle) {
      keywordToggle.addEventListener('change', function () {
        _showGenericKeywords = !!keywordToggle.checked;
        _renderDocText(_currentRawText, _currentMatches);
      });
    }
  }

  function init(containerId) {
    _container = document.getElementById(containerId);
    if (!_container) return;
    _container.innerHTML = _HTML;
    _wireHandlers();
    _loadKeywordDictionary();
    fetch('/api/metric_schema?sector=BTC-miners')
      .then(function (resp) { return resp.ok ? resp.json() : null; })
      .then(function (body) {
        if (!body || !body.success) return;
        const select = _el('rp-reprompt-metric');
        if (!select) return;
        select.innerHTML = (body.data.metrics || []).map(function (row) {
          return '<option value="' + _esc(row.key) + '">' + _esc(row.label || row.key) + '</option>';
        }).join('');
      })
      .catch(function () {});
  }

  async function openItem(itemId) {
    if (!_container) return;
    _currentItemId = itemId;
    _resetPanels();

    try {
      const doc = await _fetchDocument(itemId);
      _currentTicker = doc.ticker || _currentTicker;
      _currentPeriod = doc.period || _currentPeriod;
      _currentMetric = doc.metric || _currentMetric;
      _currentReportId = doc.report_id || null;

      const selectionData = (_currentTicker && _currentPeriod)
        ? await _fetchPeriodReports(_currentTicker, _currentPeriod).catch(function () { return null; })
        : null;
      const docs = selectionData && selectionData.selected
        ? [selectionData.selected].concat(selectionData.alternatives || [])
        : [];
      const currentDoc = docs.find(function (row) { return row.id === _currentReportId; }) || {
        document_title: doc.document_title,
        source_url: doc.source_url,
        source_type: doc.source_type,
        report_date: doc.period,
        id: doc.report_id,
      };
      _renderSourceHeader(selectionData, _currentReportId, currentDoc);

      const cards = _el('rp-value-cards');
      if (cards) {
        cards.innerHTML = _renderValueCards(doc.candidate_value, doc.review_reason);
        cards.style.display = 'flex';
      }

      const corrected = _el('rp-corrected');
      if (corrected && doc.candidate_value != null) corrected.value = doc.candidate_value;
      const metricSelect = _el('rp-reprompt-metric');
      if (metricSelect && doc.metric) metricSelect.value = doc.metric;

      _renderDocText(doc.raw_text || '', doc.source_snippet ? [{
        metric: doc.metric || '',
        metric_label: doc.metric || 'Source',
        source_snippet: doc.source_snippet,
        value: doc.candidate_value,
      }] : []);

      const actions = _el('rp-actions');
      if (actions) actions.style.display = 'flex';
    } catch (err) {
      const placeholder = _el('rp-doc-placeholder');
      if (placeholder) {
        placeholder.textContent = 'Error loading document: ' + err.message;
        placeholder.style.display = '';
      }
    }
  }

  async function openCell(ticker, period, metric, opts) {
    if (!_container) return;
    _currentTicker = ticker;
    _currentPeriod = period;
    _currentMetric = metric;
    _currentItemId = null;
    _currentReportId = opts && opts.reportId != null ? opts.reportId : null;
    const nullMetrics = (opts && opts.nullMetrics) || (metric ? [metric] : []);
    const bypassPendingReview = _currentReportId != null;
    _resetPanels();

    try {
      if (!bypassPendingReview) {
        let query = '?ticker=' + encodeURIComponent(ticker)
          + '&period=' + encodeURIComponent(period)
          + '&status=PENDING&limit=1';
        if (metric) query += '&metric=' + encodeURIComponent(metric);
        const pending = await _fetchJson('/api/review' + query).catch(function () { return null; });
        if (pending && pending.items && pending.items.length) {
          await openItem(pending.items[0].id);
          return;
        }
      }

      const [selectionData, rawText, matches] = await Promise.all([
        _fetchPeriodReports(ticker, period).catch(function () { return null; }),
        _fetchRawSource(ticker, period, _currentReportId),
        _fetchAnalysis(ticker, period, _currentReportId).catch(function () { return []; }),
      ]);

      const docs = selectionData && selectionData.selected
        ? [selectionData.selected].concat(selectionData.alternatives || [])
        : [];
      const currentDoc = (_currentReportId != null
        ? docs.find(function (row) { return row.id === _currentReportId; })
        : (selectionData && selectionData.selected)) || null;
      if (currentDoc && currentDoc.id != null) _currentReportId = currentDoc.id;
      _renderSourceHeader(selectionData, _currentReportId, currentDoc);
      _renderDocText(rawText, matches);

      const fill = _el('rp-fill-form');
      if (fill) {
        const metricSelect = fill.querySelector('.rp-fill-metric');
        if (metricSelect) {
          const options = (nullMetrics && nullMetrics.length ? nullMetrics : [metric || '']).filter(Boolean);
          metricSelect.innerHTML = options.map(function (entry) {
            return '<option value="' + _esc(entry) + '"' + (entry === metric ? ' selected' : '') + '>' + _esc(entry) + '</option>';
          }).join('');
        }
        const valueInput = fill.querySelector('.rp-fill-value');
        const noteInput = fill.querySelector('.rp-fill-note');
        if (valueInput) valueInput.value = '';
        if (noteInput) noteInput.value = '';
        fill.style.display = 'flex';
      }
    } catch (err) {
      const placeholder = _el('rp-doc-placeholder');
      if (placeholder) {
        placeholder.textContent = 'Error loading document: ' + err.message;
        placeholder.style.display = '';
      }
    }
  }

  function close() {
    _currentItemId = null;
    _currentTicker = null;
    _currentPeriod = null;
    _currentMetric = null;
    _currentReportId = null;
    _currentRawText = '';
    _currentMatches = [];
    const placeholder = _el('rp-doc-placeholder');
    const scroll = _el('rp-doc-scroll');
    const evidence = _el('rp-evidence');
    const title = _el('rp-source-title');
    const meta = _el('rp-source-meta');
    _resetPanels();
    if (placeholder) { placeholder.textContent = 'Select a row to view the document.'; placeholder.style.display = ''; }
    if (scroll) scroll.style.display = 'none';
    if (evidence) evidence.style.display = 'none';
    if (title) title.textContent = 'Document Viewer';
    if (meta) meta.innerHTML = '';
  }

  function setOnApproved(fn) { _onApproved = fn; }
  function setOnRejected(fn) { _onRejected = fn; }
  function setOnFilled(fn) { _onFilled = fn; }
  function setOnWritten(fn) { _onWritten = fn; }

  return {
    init: init,
    openItem: openItem,
    openCell: openCell,
    close: close,
    setOnApproved: setOnApproved,
    setOnRejected: setOnRejected,
    setOnFilled: setOnFilled,
    setOnWritten: setOnWritten,
    buildHighlightedSource: buildHighlightedSource,
  };
})();
