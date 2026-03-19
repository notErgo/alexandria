// ── Companies tab ───────────────────────────────────────────────────────────
let _companiesLoaded = false;
let _companies = [];
let _governanceByTicker = {};
let _regimeTicker = null;
let _acqTaskId = null;
let _acqTaskTimer = null;
let _extractTaskId = null;
let _extractTimer = null;

function _acqStateLabel(raw) {
  if (raw === 'complete') return 'complete';
  if (raw === 'error') return 'failed';
  if (raw === 'running') return 'running';
  return 'queued';
}

function _appendAcqLog(line) {
  const el = document.getElementById('acq-log');
  if (!el) return;
  el.style.display = '';
  const row = document.createElement('div');
  row.className = 'acq-log-row';
  row.textContent = line;
  el.prepend(row);
  el.style.display = '';
  while (el.children.length > 20) {
    el.removeChild(el.lastChild);
  }
}

function _setAcqStatus(text, isError) {
  const el = document.getElementById('acq-status');
  if (!el) return;
  el.textContent = text;
  el.style.color = isError ? 'var(--theme-danger)' : 'var(--theme-text-muted)';
}

function _setAcquireButtonsEnabled(enabled) {
  document.querySelectorAll('[data-acq-source]').forEach(function(btn) {
    btn.disabled = !enabled;
  });
}

async function _pollAcquireTask(taskId, source) {
  if (_acqTaskTimer) clearInterval(_acqTaskTimer);
  _acqTaskTimer = setInterval(async function() {
    try {
      const resp = await fetch('/api/ingest/' + encodeURIComponent(taskId) + '/progress');
      if (!resp.ok) throw new Error('HTTP ' + resp.status);
      const data = await resp.json();
      const p = data.data || {};
      const state = _acqStateLabel(p.status || '');
      const phase = p.phase ? (' (' + p.phase + ')') : '';
      const srcLabel = source === 'all' ? 'Scrape All' : ('Acquire ' + source.toUpperCase());
      _setAcqStatus(srcLabel + ': ' + state + phase, state === 'failed');
      if (state === 'running') {
        return;
      }
      _acqTaskId = null;
      clearInterval(_acqTaskTimer);
      _acqTaskTimer = null;
      _setAcquireButtonsEnabled(true);
      if (state === 'complete') {
        const detail = [];
        if (source === 'all') {
          // Per-phase counters for scrape-all
          if (typeof p.archive_reports === 'number') detail.push('archive=' + p.archive_reports + ' reports, ' + (p.archive_points || 0) + ' pts');
          if (typeof p.ir_reports === 'number') detail.push('ir=' + p.ir_reports + ' reports');
          if (typeof p.edgar_reports === 'number') detail.push('edgar=' + p.edgar_reports + ' reports');
        } else {
          if (typeof p.reports_ingested === 'number') detail.push('reports=' + p.reports_ingested);
          if (typeof p.data_points_extracted === 'number') detail.push('data_points=' + p.data_points_extracted);
          if (typeof p.review_flagged === 'number') detail.push('review=' + p.review_flagged);
          if (typeof p.reports_extracted === 'number') detail.push('extracted_reports=' + p.reports_extracted);
          if (typeof p.extraction_data_points === 'number') detail.push('extract_points=' + p.extraction_data_points);
          if (typeof p.extraction_review_flagged === 'number') detail.push('extract_review=' + p.extraction_review_flagged);
          if (typeof p.extraction_errors === 'number') detail.push('extract_errors=' + p.extraction_errors);
        }
        if (typeof p.errors === 'number' && p.errors > 0) detail.push('errors=' + p.errors);
        _appendAcqLog(new Date().toISOString().slice(11, 19) + ' complete ' + source + (detail.length ? ' (' + detail.join(', ') + ')' : ''));
        showToast((source === 'all' ? 'Scrape All' : 'Acquire ' + source.toUpperCase()) + ' complete');
        loadPipelineObservability();
        if (_registryLoaded) loadRegistry();
        if (_explorerLoaded) loadExplorer();
        if (_reviewLoaded) loadReview();
      } else {
        const msg = p.message || 'Task failed';
        _appendAcqLog(new Date().toISOString().slice(11, 19) + ' failed ' + source + ' (' + msg + ')');
        showToast('Acquire ' + source.toUpperCase() + ' failed: ' + msg, true);
      }
    } catch (err) {
      _acqTaskId = null;
      clearInterval(_acqTaskTimer);
      _acqTaskTimer = null;
      _setAcquireButtonsEnabled(true);
      _setAcqStatus('Acquire ' + source.toUpperCase() + ': failed', true);
      _appendAcqLog(new Date().toISOString().slice(11, 19) + ' failed ' + source + ' (' + String(err) + ')');
    }
  }, 1000);
}

async function scanManifest() {
  if (_acqTaskId) return;
  _setAcquireButtonsEnabled(false);
  _setAcqStatus('Scanning archive directory for files…', false);
  _appendAcqLog(new Date().toISOString().slice(11, 19) + ' manifest scan started');
  try {
    const resp = await fetch('/api/manifest/scan', { method: 'POST', headers: {'Content-Type': 'application/json'} });
    const data = await resp.json();
    if (!resp.ok || !data.success) throw new Error(data.error?.message || ('HTTP ' + resp.status));
    const s = data.data || {};
    const msg = 'Manifest scan complete — ' + (s.total_found || 0) + ' files found, ' + (s.newly_discovered || 0) + ' new, ' + (s.already_ingested || 0) + ' already ingested';
    _setAcqStatus(msg, false);
    _appendAcqLog(new Date().toISOString().slice(11, 19) + ' ' + msg);
    const counts = s.ticker_counts || {};
    const tickerLines = Object.keys(counts).sort().map(function(t) { return t + ': ' + counts[t]; });
    if (tickerLines.length) _appendAcqLog('  ' + tickerLines.join('  |  '));
    loadIngest();
  } catch (err) {
    _setAcqStatus('Manifest scan failed: ' + String(err), true);
    _appendAcqLog(new Date().toISOString().slice(11, 19) + ' manifest scan failed (' + String(err) + ')');
  } finally {
    _setAcquireButtonsEnabled(true);
  }
}

async function triggerAcquire(source, tickers) {
  if (!source || _acqTaskId) return;
  const autoExtractEnabled = !!document.getElementById('acq-auto-extract')?.checked;
  const autoExtract = autoExtractEnabled && (source === 'ir' || source === 'edgar');
  const scopeLabel = (tickers && tickers.length) ? ' [' + tickers.join(', ') + ']' : '';
  _setAcquireButtonsEnabled(false);
  _acqTaskId = 'starting';
  _setAcqStatus('Acquire ' + source.toUpperCase() + scopeLabel + ': queued' + (autoExtract ? ' (with extraction)' : ''), false);
  _appendAcqLog(new Date().toISOString().slice(11, 19) + ' queued ' + source + scopeLabel + (autoExtract ? ' + extract' : ''));
  const body = { auto_extract: autoExtract };
  if (tickers && tickers.length) body.tickers = tickers;
  try {
    const resp = await fetch('/api/ingest/' + encodeURIComponent(source), {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify(body),
    });
    const data = await resp.json();
    if (!resp.ok || !data.success) {
      throw new Error((data.error && data.error.message) || ('HTTP ' + resp.status));
    }
    _acqTaskId = data.data.task_id;
    _setAcqStatus('Acquire ' + source.toUpperCase() + scopeLabel + ': running' + (autoExtract ? ' (with extraction)' : ''), false);
    _appendAcqLog(new Date().toISOString().slice(11, 19) + ' running ' + source + scopeLabel + ' (task ' + _acqTaskId + ')' + (autoExtract ? ' + extract' : ''));
    _pollAcquireTask(_acqTaskId, source);
  } catch (err) {
    _acqTaskId = null;
    _setAcquireButtonsEnabled(true);
    _setAcqStatus('Acquire ' + source.toUpperCase() + ': failed', true);
    _appendAcqLog(new Date().toISOString().slice(11, 19) + ' failed ' + source + ' (' + String(err) + ')');
    showToast('Failed to start Acquire ' + source.toUpperCase(), true);
  }
}

const _extractPanel = new LogPanel('extract-log', { maxLines: 30, storageKey: 'logpanel_extract' });
const _gapReextractPanel = new LogPanel('gap-reextract-log', { maxLines: 30, storageKey: 'logpanel_gap_reextract' });

function _appendExtractLog(line, level) { _extractPanel.append(line, level); }

function _setExtractStatus(text, isError) {
  const el = document.getElementById('extract-status');
  if (!el) return;
  el.textContent = text;
  el.style.color = isError ? 'var(--theme-danger)' : 'var(--theme-text-muted)';
}

function _setExtractEnabled(enabled) {
  const btn = document.getElementById('extract-start-btn');
  if (btn) btn.disabled = !enabled;
}

async function _pollExtractionRun(taskId, ticker, panelOverride) {
  if (_extractTimer) { clearInterval(_extractTimer); _extractTimer = null; }
  const panel = panelOverride || _extractPanel;
  panel.stopPolling();
  panel.startPolling(
    taskId,
    '/api/operations/interpret/' + encodeURIComponent(taskId) + '/progress',
    {
      onProgress: function(p) {
        const state     = p.status || 'running';
        const processed = p.reports_processed || 0;
        const total     = p.reports_total || 0;
        const points    = p.data_points || 0;
        const errors    = p.errors || 0;
        _setExtractStatus(
          'Extraction ' + (ticker || 'ALL') + ': ' + state
          + ' (' + processed + '/' + total + ', points=' + points + ', errors=' + errors + ')',
          state === 'error'
        );
      },
      onComplete: async function(p) {
        _extractTaskId = null;
        _setExtractEnabled(true);
        const processed = p.reports_processed || 0;
        const points    = p.data_points || 0;
        const errors    = p.errors || 0;
        panel.append(new Date().toISOString().slice(11, 19) + ' complete ' + (ticker || 'ALL')
          + '  reports=' + processed + '  pts=' + points + '  errors=' + errors);
        showToast('Extraction complete (' + (ticker || 'ALL') + ')');
        if (window._extractAutoGapFill) {
          window._extractAutoGapFill = false;
          await _runGapFillAfterExtraction(window._extractTicker || '');
        }
        loadPipelineObservability();
        if (_explorerLoaded) loadExplorer();
        if (_reviewLoaded) loadReview();
        if (window._minerDataBooted && typeof selectCompany === 'function' && typeof _ticker !== 'undefined' && _ticker) selectCompany(_ticker);
      },
      onError: function(p) {
        _extractTaskId = null;
        _setExtractEnabled(true);
        panel.append(new Date().toISOString().slice(11, 19) + ' failed ' + (ticker || 'ALL'), 'error');
        _setExtractStatus('Extraction failed: ' + (p.error_message || 'unknown error'), true);
        showToast('Extraction failed (' + (ticker || 'ALL') + ')', true);
      },
      onFetchError: function(err) {
        _extractTaskId = null;
        _setExtractEnabled(true);
        _setExtractStatus('Extraction failed: ' + String(err), true);
        panel.append(new Date().toISOString().slice(11, 19) + ' network error (' + String(err) + ')', 'error');
      },
    }
  );
}


// Shared model loader — reads ollama_model config key as the SSOT default.
// Sorts: saved config model first, then preferred fallbacks, then alpha.
async function populateOllamaModelSelect(selId) {
  const sel = document.getElementById(selId);
  if (!sel) return;
  try {
    let models = [];
    try {
      const resp = await fetch('/api/ollama/models', { cache: 'no-store' });
      const d = await resp.json();
      models = (d.data?.models || []).map(function(m) { return m.name; });
    } catch (_) {}
    let active = '';
    try {
      const cfgResp = await fetch('/api/config', { cache: 'no-store' });
      const cfgData = await cfgResp.json();
      const row = (cfgData.data?.config || []).find(function(r) { return r.key === 'ollama_model'; });
      active = row?.value || '';
    } catch (_) {}
    if (!models.length) {
      sel.innerHTML = '<option value="">No models found</option>';
      return;
    }
    const preferred = ['qwen2.5:7b'];
    models.sort(function(a, b) {
      if (a === active) return -1;
      if (b === active) return 1;
      const ai = preferred.indexOf(a), bi = preferred.indexOf(b);
      if (ai !== -1 && bi !== -1) return ai - bi;
      if (ai !== -1) return -1;
      if (bi !== -1) return 1;
      return a.localeCompare(b);
    });
    sel.innerHTML = models.map(function(n) {
      return '<option value="' + escapeAttr(n) + '"' + (n === active ? ' selected' : '') + '>' + escapeHtml(n) + '</option>';
    }).join('');
  } catch (_err) {
    sel.innerHTML = '<option value="">Could not load models</option>';
  }
}

async function loadOllamaModels() {
  await populateOllamaModelSelect('extract-model');
}

async function saveOllamaModel(modelName) {
  if (!modelName) return;
  try {
    const resp = await fetch('/api/config/ollama_model', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({ value: modelName }),
    });
    const data = await resp.json();
    if (!resp.ok || !data.success) throw new Error(data.error?.message || ('HTTP ' + resp.status));
    showToast('Model set to ' + modelName);
  } catch (err) {
    showToast('Failed to save model: ' + String(err), true);
  }
}

function toggleSampleMode() {
  const enabled = document.getElementById('extract-sample-enabled')?.checked;
  const controls = document.getElementById('extract-sample-controls');
  if (controls) controls.style.display = enabled ? 'flex' : 'none';
}

function onSourceDocsChange() {
  const cadence = document.getElementById('extract-cadence')?.value || 'all';
  const gapfillRow = document.getElementById('extract-gapfill-row');
  if (gapfillRow) {
    // Show gap-fill option when processing 10-Q or 10-K — those don't produce monthly rows directly
    gapfillRow.style.display = (cadence === 'quarterly' || cadence === 'annual' || cadence === 'sec' || cadence === 'all') ? 'flex' : 'none';
  }
  // Auto-sync granularity selector to match cadence selection
  // 'sec' covers both 10-Q and 10-K so no single granularity is auto-selected
  const granMap = { monthly: 'gran-monthly', quarterly: 'gran-quarterly', annual: 'gran-annual' };
  const granId = granMap[cadence];
  if (granId) {
    const el = document.getElementById(granId);
    if (el) el.checked = true;
  }
}

function onExtractModeChange() {
  const mode = document.getElementById('extract-mode')?.value || 'resume';
  const hint = document.getElementById('extract-mode-hint');
  const warning = document.getElementById('extract-reset-warning');
  if (hint) {
    if (mode === 'resume') hint.textContent = 'Processes only reports not yet extracted. Analyst-approved rows are never overwritten.';
    else if (mode === 'backfill') hint.textContent = 'Extracts only reports whose period has no existing data or pending review entries. Resets those reports to pending before running. Does not touch periods that already have results.';
    else if (mode === 'force') hint.textContent = 'Re-runs all matched reports and overwrites existing data_points. Analyst-approved rows are never overwritten. Review queue entries are appended (not cleared).';
    else hint.textContent = '';
  }
  if (warning) warning.style.display = mode === 'reset' ? '' : 'none';
}

async function startExtractionRun() {
  if (_extractTaskId) return;
  const tickers = _getSharedSelectedTickers();
  const ticker = tickers.length === 1 ? tickers[0] : '';
  const scopeLabel = tickers.length ? tickers.join(',') : 'ALL';
  const mode = document.getElementById('extract-mode')?.value || 'resume';
  const force = mode === 'force' || mode === 'reset';

  if (mode === 'reset') {
    if (!tickers.length) {
      _setExtractStatus('Full Reset requires a ticker selection — select at least one ticker first.', true);
      return;
    }
    // Purge extraction outputs only (data_points, review_queue, final_data_points).
    // Source documents (reports table) are preserved — they are re-extracted with force=true.
    _setExtractEnabled(false);
    _extractTaskId = 'starting';
    _setExtractStatus('Full Reset: clearing extraction outputs for ' + scopeLabel + '…', false);
    try {
      for (const t of tickers) {
        const pr = await fetch('/api/operations/purge_ticker', {
          method: 'POST',
          headers: {'Content-Type': 'application/json'},
          body: JSON.stringify({ticker: t}),
        });
        if (!pr.ok) { const pd = await pr.json(); throw new Error('purge: ' + (pd.error || pr.status)); }
      }
      _appendExtractLog(new Date().toISOString().slice(11, 19) + ' cleared extraction outputs for ' + scopeLabel);
    } catch (err) {
      _extractTaskId = null;
      _setExtractEnabled(true);
      _setExtractStatus('Purge failed: ' + String(err), true);
      return;
    }
    _extractTaskId = null;
  }
  const cadence = document.getElementById('extract-cadence')?.value || 'all';
  const fromPeriod = (document.getElementById('extract-from')?.value || '').trim() || null;
  const toPeriod = (document.getElementById('extract-to')?.value || '').trim() || null;
  const sampleEnabled = !!document.getElementById('extract-sample-enabled')?.checked;
  const sampleN = sampleEnabled ? parseInt(document.getElementById('extract-sample-n')?.value || '5', 10) : 0;
  const extractWorkers = Math.max(1, parseInt(document.getElementById('interpret-extract-workers')?.value || '8', 10) || 8);
  const autoGapFill = !!(document.getElementById('extract-auto-gapfill')?.checked) && cadence !== 'monthly';

  if (fromPeriod && toPeriod && fromPeriod > toPeriod) {
    _setExtractStatus('Interpret window invalid: From must be on or before To', true);
    return;
  }

  const filters = [];
  if (cadence !== 'all') filters.push(cadence);
  if (fromPeriod) filters.push('date >= ' + fromPeriod);
  if (toPeriod) filters.push('date <= ' + toPeriod);
  if (extractWorkers > 1) filters.push('workers=' + extractWorkers);
  if (sampleN > 0) filters.push('sample=' + sampleN);
  if (autoGapFill) filters.push('gap-fill after');
  const filterLabel = filters.length ? ' [' + filters.join(', ') + ']' : '';

  _setExtractEnabled(false);
  _extractTaskId = 'starting';
  // Store auto-gap-fill intent for the completion handler
  window._extractAutoGapFill = autoGapFill;
  window._extractTicker = ticker;
  _setExtractStatus('Extraction ' + scopeLabel + filterLabel + ': queued', false);
  _appendExtractLog(new Date().toISOString().slice(11, 19) + ' queued ' + scopeLabel + (force ? ' force' : '') + filterLabel);
  try {
    const _granEl = document.querySelector('input[name="expected_granularity"]:checked');
    const expectedGranularity = _granEl ? _granEl.value : 'monthly';
    const forceReview = !!document.getElementById('extract-force-review')?.checked;
    const resp = await fetch('/api/operations/interpret', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({
        ticker: ticker || null,
        tickers: tickers,
        run_mode: mode,
        force,
        cadence,
        from_period: fromPeriod,
        to_period: toPeriod,
        extract_workers: extractWorkers,
        sample: sampleN || null,
        expected_granularity: expectedGranularity,
        force_review: forceReview,
      }),
    });
    const data = await resp.json();
    if (!resp.ok || !data.success) throw new Error(data.error?.message || ('HTTP ' + resp.status));
    _extractTaskId = data.data.task_id;
    _setExtractStatus('Extraction ' + scopeLabel + filterLabel + ': running', false);
    _appendExtractLog(new Date().toISOString().slice(11, 19) + ' running task ' + _extractTaskId);
    _pollExtractionRun(_extractTaskId, scopeLabel);
  } catch (err) {
    _extractTaskId = null;
    _setExtractEnabled(true);
    _setExtractStatus('Extraction failed: ' + String(err), true);
    _appendExtractLog(new Date().toISOString().slice(11, 19) + ' failed to start (' + String(err) + ')');
    showToast('Failed to start extraction', true);
  }
}

// ── 3.1.2  Re-extract gap months ────────────────────────────────────────────

const _selectedGapTickers = new Set();

function _renderGapTickerBar() {
  const bar = document.getElementById('gap-ticker-bar');
  if (!bar) return;
  const actives = _companies.filter(function(c) { return c.active !== false; });
  bar.innerHTML = '';
  if (!actives.length) { bar.textContent = 'No companies configured.'; return; }
  actives.forEach(function(c) {
    const btn = document.createElement('button');
    btn.className = 'btn btn-sm btn-secondary crawl-ticker-btn' + (_selectedGapTickers.has(c.ticker) ? ' active' : '');
    btn.textContent = c.ticker;
    btn.dataset.ticker = c.ticker;
    btn.onclick = function() {
      if (_selectedGapTickers.has(c.ticker)) { _selectedGapTickers.delete(c.ticker); }
      else { _selectedGapTickers.add(c.ticker); }
      _renderGapTickerBar();
    };
    bar.appendChild(btn);
  });
}

async function loadInterpretMetricToggles(force) {
  const wrap = document.getElementById('interpret-metric-toggles');
  if (!wrap) return;
  if (!force && wrap.dataset.loaded === '1') return;
  try {
    const resp = await fetch('/api/metric_schema');
    const body = await resp.json();
    if (!body.success) throw new Error(body.error?.message || 'Failed');
    const metrics = (body.data || []).slice().sort(function(a, b) {
      return (a.display_order || 999) - (b.display_order || 999);
    });

    const groupKeys = [];
    const groups = {};
    metrics.forEach(function(m) {
      const g = m.metric_group || 'other';
      if (!groups[g]) { groups[g] = []; groupKeys.push(g); }
      groups[g].push(m);
    });

    wrap.innerHTML = '';
    wrap.dataset.loaded = '1';

    groupKeys.forEach(function(grp) {
      const lbl = document.createElement('span');
      lbl.textContent = grp + ':';
      lbl.style.cssText = 'font-size:0.7rem;color:var(--theme-text-muted);align-self:center;white-space:nowrap';
      wrap.appendChild(lbl);

      groups[grp].forEach(function(m) {
        const btn = document.createElement('button');
        const isActive = !!m.active;
        btn.textContent = (isActive ? '\u25cf ' : '\u25cb ') + m.label;
        btn.title = m.key + (isActive ? ' \u2014 click to deactivate' : ' \u2014 click to activate');
        btn.style.cssText = isActive
          ? 'font-size:0.75rem;padding:2px 9px;border-radius:12px;border:1px solid var(--theme-accent,#3b82f6);background:var(--theme-accent,#3b82f6);color:#fff;cursor:pointer'
          : 'font-size:0.75rem;padding:2px 9px;border-radius:12px;border:1px solid var(--theme-border);background:transparent;color:var(--theme-text-muted);cursor:pointer;opacity:0.65';
        btn.onclick = async function() {
          btn.disabled = true;
          await toggleMetricSchemaActive(m.id, !isActive);
          loadInterpretMetricToggles(true);
        };
        wrap.appendChild(btn);
      });
    });
  } catch (err) {
    if (wrap) wrap.textContent = 'Failed to load metrics';
  }
}

function _loadGapMetrics() {
  const sel = document.getElementById('gap-metric-list');
  if (!sel || sel.options.length > 0) return;
  fetch('/api/metric_schema')
    .then(function(r) { return r.json(); })
    .then(function(data) {
      if (!data.success) return;
      (data.data || []).forEach(function(m) {
        const opt = document.createElement('option');
        opt.value = m.key;
        opt.textContent = m.label || m.key;
        sel.appendChild(opt);
      });
    })
    .catch(function() {});
}

async function runRequeueMissing() {
  const sel = document.getElementById('gap-metric-list');
  const metrics = sel ? Array.from(sel.selectedOptions).map(function(o) { return o.value; }) : [];
  if (!metrics.length) { showToast('Select at least one metric', true); return; }
  const tickers = Array.from(_selectedGapTickers);
  const btn = document.getElementById('gap-reextract-btn');
  const statusEl = document.getElementById('gap-reextract-status');
  const logEl = document.getElementById('gap-reextract-log');
  if (btn) btn.disabled = true;
  if (statusEl) statusEl.textContent = 'Queuing...';
  _gapReextractPanel.clear();
  try {
    const resp = await fetch('/api/operations/requeue-missing', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({
        metrics: metrics,
        target_metrics: metrics,
        tickers: tickers.length ? tickers : undefined,
      }),
    });
    if (!resp.ok) throw new Error('HTTP ' + resp.status);
    const body = await resp.json();
    if (!body.success) throw new Error(body.error?.message || 'Request failed');
    const d = body.data || {};
    if (d.requeued_count === 0) {
      if (statusEl) statusEl.textContent = 'No gap months found — all selected metrics already have data.';
      if (btn) btn.disabled = false;
      return;
    }
    const scopeLabel = tickers.length ? tickers.join(',') : 'ALL';
    if (statusEl) statusEl.textContent = d.requeued_count + ' reports queued (' + scopeLabel + '). Running...';
    _gapReextractPanel.append(new Date().toISOString().slice(11, 19) + ' task ' + (d.task_id || '?')
      + '  requeued=' + d.requeued_count + '  scope=' + scopeLabel);
    if (d.task_id) _pollExtractionRun(d.task_id, scopeLabel, _gapReextractPanel);
  } catch (err) {
    if (statusEl) statusEl.textContent = 'Error: ' + String(err);
    showToast('Re-extract failed: ' + err.message, true);
    if (btn) btn.disabled = false;
  }
}

async function runGapDiagnosis() {
  const sel = document.getElementById('gap-metric-list');
  const metrics = sel ? Array.from(sel.selectedOptions).map(function(o) { return o.value; }) : [];
  if (!metrics.length) { showToast('Select at least one metric first', true); return; }
  const tickers = Array.from(_selectedGapTickers);
  const out = document.getElementById('gap-diagnosis-out');
  if (out) { out.style.display = ''; out.textContent = 'Running diagnosis...'; }
  try {
    const qs = 'metrics=' + encodeURIComponent(metrics.join(','))
      + (tickers.length ? '&tickers=' + encodeURIComponent(tickers.join(',')) : '');
    const resp = await fetch('/api/operations/gap-diagnosis?' + qs);
    const body = await resp.json();
    if (!body.success) throw new Error(body.error?.message || 'Request failed');
    const d = body.data || {};
    let txt = '=== Gap Diagnosis ===\n';
    txt += 'Metrics:   ' + (Array.isArray(d.metrics) ? d.metrics.join(', ') : d.metrics) + '\n';
    txt += 'Tickers:   ' + d.tickers + '\n\n';
    txt += 'Total reports in scope:         ' + d.total_reports + '\n';
    txt += '  have raw_text:                ' + d.has_raw_text + '\n';
    txt += '  extraction_status = done:     ' + d.is_done + '\n';
    txt += '  done AND have raw_text:       ' + d.done_with_text + '\n';
    txt += '  missing data by report_id:    ' + d.missing_by_report_id + '\n';
    txt += '  excluded by no_data verdict:  ' + d.excluded_by_no_data_verdict + '\n';
    txt += '  => final gap count:           ' + d.final_gap_count + '\n\n';
    txt += 'Pending review items for metric: ' + d.pending_review_items + '\n';
    txt += 'Existing data_points for metric: ' + d.data_points_for_metric + '\n';
    if (d.pending_sample && d.pending_sample.length) {
      txt += '\nSample pending review items (up to 20):\n';
      txt += 'ticker  period   metric            agreement_status  extract_status  dp_for_report\n';
      d.pending_sample.forEach(function(r) {
        txt += (r.ticker||'').padEnd(8)
          + (r.period||'').padEnd(9)
          + (r.metric||'').padEnd(18)
          + (r.agreement_status||'').padEnd(18)
          + (r.extraction_status||'').padEnd(16)
          + r.dp_for_this_report + '\n';
      });
    }
    if (out) out.textContent = txt;
  } catch (err) {
    if (out) out.textContent = 'Diagnosis error: ' + String(err);
    showToast('Diagnosis failed: ' + err.message, true);
  }
}

async function _runGapFillAfterExtraction(ticker) {
  if (!ticker) {
    _appendExtractLog(new Date().toISOString().slice(11, 19) + ' gap-fill skipped (no single ticker selected)');
    return;
  }
  _appendExtractLog(new Date().toISOString().slice(11, 19) + ' running gap-fill for ' + ticker + '…');
  try {
    const resp = await fetch('/api/operations/gap-fill', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({ticker}),
    });
    const data = await resp.json();
    if (resp.ok) {
      _appendExtractLog(new Date().toISOString().slice(11, 19)
        + ' gap-fill done: ' + (data.filled || 0) + ' filled, '
        + (data.skipped || 0) + ' skipped, '
        + (data.errors || 0) + ' errors');
    } else {
      _appendExtractLog(new Date().toISOString().slice(11, 19) + ' gap-fill error: ' + (data.error || 'unknown'));
    }
  } catch (err) {
    _appendExtractLog(new Date().toISOString().slice(11, 19) + ' gap-fill failed: ' + String(err));
  }
}

async function loadPipelineObservability() {
  const summaryEl = document.getElementById('pipe-summary');
  const healthEl = document.getElementById('pipe-scraper-health');
  const msgEl = document.getElementById('pipe-msg');
  const tbody = document.getElementById('pipeline-tbody');
  if (!summaryEl || !healthEl || !msgEl || !tbody) return;
  msgEl.textContent = 'Loading…';
  try {
    const resp = await fetch('/api/operations/pipeline_observability', { cache: 'no-store' });
    const data = await resp.json();
    if (!resp.ok || !data.success) {
      throw new Error(data?.error?.message || ('HTTP ' + resp.status));
    }

    const snap = data.data || {};
    const t = snap.totals || {};
    const sc = snap.scraper_config || {};
    summaryEl.textContent =
      'Companies ' + (t.companies_active ?? 0) + '/' + (t.companies_total ?? 0)
      + ' | Manifest ' + (t.manifest_total ?? 0)
      + ' | Reports ' + (t.reports_total ?? 0)
      + ' | Parsed ' + (t.reports_parsed ?? 0)
      + ' | Extracted ' + (t.reports_extracted ?? 0)
      + ' | Unextracted ' + (t.reports_unextracted ?? 0)
      + ' | Data Points ' + (t.data_points_total ?? 0)
      + ' | Review Pending ' + (t.review_pending ?? 0);

    const invalidCount = sc.invalid_count || 0;
    const invalidList = (sc.invalid_tickers || []).map(function(i) {
      return i.ticker + ': ' + i.issue;
    });
    if (invalidCount > 0) {
      healthEl.style.color = 'var(--theme-danger)';
      healthEl.textContent = 'Scraper config issues (' + invalidCount + '): ' + invalidList.join(' | ');
    } else {
      healthEl.style.color = 'var(--theme-success)';
      healthEl.textContent = 'Scraper config health: all ticker modes have required fields.';
    }

    const rows = snap.tickers || [];
    if (!rows.length) {
      tbody.innerHTML = '<tr><td colspan="8" style="text-align:center;color:var(--theme-text-muted)">No ticker rows.</td></tr>';
    } else {
      tbody.innerHTML = rows.map(function(r) {
        const cfg = r.scraper_config_valid
          ? '<span class="scraper-badge scraper-badge-ok">ok</span>'
          : '<span class="scraper-badge scraper-badge-error" title="' + escapeAttr(r.scraper_config_issue || '') + '">invalid</span>';
        return '<tr>'
          + '<td>' + escapeHtml(r.ticker) + '</td>'
          + '<td>' + escapeHtml(r.scraper_mode || 'skip') + '</td>'
          + '<td>' + escapeHtml(String(r.manifest_total || 0)) + '</td>'
          + '<td>' + escapeHtml(String(r.reports_total || 0)) + '</td>'
          + '<td>' + escapeHtml(String(r.reports_unextracted || 0)) + '</td>'
          + '<td>' + escapeHtml(String(r.data_points_total || 0)) + '</td>'
          + '<td>' + escapeHtml(String(r.review_pending || 0)) + '</td>'
          + '<td>' + cfg + '</td>'
          + '</tr>';
      }).join('');
    }

    const ts = (snap.generated_at || '').slice(11, 19);
    msgEl.textContent = ts ? ('updated ' + ts) : 'updated';
  } catch (err) {
    msgEl.textContent = 'failed';
    summaryEl.textContent = 'Failed to load pipeline observability.';
    healthEl.textContent = String(err);
    healthEl.style.color = 'var(--theme-danger)';
    tbody.innerHTML = '<tr><td colspan="8" style="text-align:center;color:var(--theme-danger)">Failed to load.</td></tr>';
  }
}

async function loadDocumentOverview() {
  const tbody = document.getElementById('doc-overview-tbody');
  const msgEl = document.getElementById('doc-overview-msg');
  if (!tbody) return;
  if (msgEl) msgEl.textContent = 'Loading…';
  try {
    const resp = await fetch('/api/operations/pipeline_observability', { cache: 'no-store' });
    const data = await resp.json();
    if (!resp.ok || !data.success) {
      throw new Error(data?.error?.message || ('HTTP ' + resp.status));
    }
    const rows = (data.data || {}).tickers || [];
    if (!rows.length) {
      tbody.innerHTML = '<tr><td colspan="11" style="text-align:center;color:var(--theme-text-muted)">No data.</td></tr>';
    } else {
      tbody.innerHTML = rows.map(function(r) {
        return '<tr>'
          + '<td>' + escapeHtml(r.ticker) + '</td>'
          + '<td>' + (r.reports_total || 0) + '</td>'
          + '<td>' + (r.reports_done || 0) + '</td>'
          + '<td>' + (r.reports_pending || 0) + '</td>'
          + '<td>' + (r.reports_keyword_gated || 0) + '</td>'
          + '<td>' + (r.reports_failed || 0) + '</td>'
          + '<td>' + (r.reports_ir || 0) + '</td>'
          + '<td>' + (r.reports_edgar || 0) + '</td>'
          + '<td>' + (r.reports_archive || 0) + '</td>'
          + '<td>' + (r.data_points_total || 0) + '</td>'
          + '<td>' + (r.review_pending || 0) + '</td>'
          + '</tr>';
      }).join('');
    }
    const ts = ((data.data || {}).generated_at || '').slice(11, 19);
    if (msgEl) msgEl.textContent = ts ? ('updated ' + ts) : 'updated';
  } catch (err) {
    if (msgEl) msgEl.textContent = 'failed';
    tbody.innerHTML = '<tr><td colspan="11" style="text-align:center;color:var(--theme-danger)">Failed to load.</td></tr>';
  }
}

async function loadPurgeInventory() {
  // Legacy: delegate to new management inventory loader
  await loadManagementInventory();
}

// ── Data Management panel ──────────────────────────────────────────────────

function _dmSelectedTicker() {
  const el = document.getElementById('dm-scope');
  return el ? el.value.trim() : '';
}

function _dmSelectedStage() {
  const el = document.getElementById('dm-stage');
  return el ? el.value : 'review_queue';
}

async function loadManagementInventory() {
  const tbody = document.getElementById('dm-inventory-tbody');
  const msgEl = document.getElementById('dm-inventory-msg');
  if (!tbody) return;
  if (msgEl) msgEl.textContent = 'Loading…';
  const ticker = _dmSelectedTicker();
  const url = '/api/data/management-inventory' + (ticker ? ('?ticker=' + encodeURIComponent(ticker)) : '');
  try {
    const resp = await fetch(url, { cache: 'no-store' });
    const data = await resp.json();
    if (!resp.ok || !data.success) throw new Error(data?.error?.message || ('HTTP ' + resp.status));
    const rows = data.data || [];
    const stage = _dmSelectedStage();
    // Populate scope dropdown on first load (all-companies fetch)
    if (!ticker) {
      const scopeEl = document.getElementById('dm-scope');
      if (scopeEl && scopeEl.options.length <= 1) {
        rows.forEach(function(r) {
          const opt = document.createElement('option');
          opt.value = r.ticker;
          opt.textContent = r.ticker;
          scopeEl.appendChild(opt);
        });
      }
    }
    if (!rows.length) {
      tbody.innerHTML = '<tr><td colspan="6" style="text-align:center;color:var(--theme-text-muted)">No data.</td></tr>';
    } else {
      tbody.innerHTML = rows.map(function(r) {
        const deleteBtn = (stage === 'review_queue' || stage === 'review_and_final' || stage === 'final' || stage === 'scrape')
          ? '<button class="btn btn-sm btn-danger" style="padding:1px 7px;font-size:0.74rem"'
            + ' onclick="managementPurgeRow(\'' + escapeAttr(r.ticker) + '\', \'' + escapeAttr(stage) + '\')">'
            + 'Delete</button>'
          : '';
        return '<tr>'
          + '<td>' + escapeHtml(r.ticker) + '</td>'
          + '<td>' + r.reports + '</td>'
          + '<td>' + r.data_points + '</td>'
          + '<td>' + r.review_pending + '</td>'
          + '<td>' + r.final_values + '</td>'
          + '<td>' + deleteBtn + '</td>'
          + '</tr>';
      }).join('');
    }
    if (msgEl) msgEl.textContent = 'updated';
    // Load batch section if review_queue stage
    const batchSection = document.getElementById('dm-batch-section');
    if (batchSection) {
      if (stage === 'review_queue') {
        batchSection.style.display = '';
        await loadReviewBatches();
      } else {
        batchSection.style.display = 'none';
      }
    }
  } catch (err) {
    if (msgEl) msgEl.textContent = 'failed';
    tbody.innerHTML = '<tr><td colspan="6" style="text-align:center;color:var(--theme-danger)">Failed to load.</td></tr>';
  }
}

async function loadReviewBatches() {
  const tbody = document.getElementById('dm-batch-tbody');
  const globalRow = document.getElementById('dm-batch-global-row');
  if (!tbody) return;
  tbody.innerHTML = '<tr><td colspan="5" style="text-align:center;color:var(--theme-text-muted)">Loading…</td></tr>';
  const ticker = _dmSelectedTicker();
  const url = '/api/review/batches' + (ticker ? ('?ticker=' + encodeURIComponent(ticker)) : '');
  try {
    const resp = await fetch(url, { cache: 'no-store' });
    const data = await resp.json();
    if (!resp.ok || !data.success) throw new Error(data?.error?.message || ('HTTP ' + resp.status));
    const batches = (data.data || {}).batches || [];
    if (!batches.length) {
      tbody.innerHTML = '<tr><td colspan="5" style="text-align:center;color:var(--theme-text-muted)">No pending batches.</td></tr>';
      if (globalRow) globalRow.style.display = 'none';
      return;
    }
    tbody.innerHTML = batches.map(function(b) {
      const label = escapeHtml(b.batch_date) + ' / ' + escapeHtml(b.ticker);
      return '<tr>'
        + '<td>' + escapeHtml(b.batch_date) + '</td>'
        + '<td>' + escapeHtml(b.ticker) + '</td>'
        + '<td>' + b.item_count + '</td>'
        + '<td>' + b.overlap_final + ' overlap</td>'
        + '<td><button class="btn btn-sm btn-danger" style="padding:1px 7px;font-size:0.74rem"'
          + ' onclick="deleteReviewBatch(\'' + escapeAttr(b.batch_date) + '\', \'' + escapeAttr(b.ticker) + '\', ' + b.item_count + ')">'
          + 'Delete</button></td>'
        + '</tr>';
    }).join('');
    // Group by date for global-date delete button
    const byDate = {};
    batches.forEach(function(b) {
      byDate[b.batch_date] = (byDate[b.batch_date] || 0) + b.item_count;
    });
    const dates = Object.keys(byDate).sort().reverse();
    if (globalRow) {
      globalRow.style.display = '';
      globalRow.innerHTML = dates.map(function(d) {
        return '<button class="btn btn-sm btn-danger" style="margin-right:0.5rem;font-size:0.78rem"'
          + ' onclick="deleteReviewBatch(\'' + escapeAttr(d) + '\', null, ' + byDate[d] + ')">'
          + 'Delete ALL on ' + escapeHtml(d) + ' (' + byDate[d] + ' items)'
          + '</button>';
      }).join('');
    }
  } catch (err) {
    tbody.innerHTML = '<tr><td colspan="5" style="text-align:center;color:var(--theme-danger)">Failed: ' + escapeHtml(String(err.message)) + '</td></tr>';
  }
}

async function deleteReviewBatch(date, ticker, count) {
  const who = ticker ? (count + ' PENDING items for ' + ticker + ' on ' + date)
                      : (count + ' PENDING items across all tickers on ' + date);
  if (!confirm('Delete ' + who + '?\n\nThis removes review_queue rows only — scraped reports are preserved.')) return;
  try {
    const body = { created_date: date, dry_run: false };
    if (ticker) body.ticker = ticker;
    const resp = await fetch('/api/review/batch-delete', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(body),
    });
    const data = await resp.json();
    if (!resp.ok || !data.success) throw new Error(data?.error?.message || ('HTTP ' + resp.status));
    showToast('Deleted ' + data.data.deleted + ' review items. Reports preserved.', false);
    await loadManagementInventory();
  } catch (err) {
    showToast('Error: ' + err.message, true);
  }
}

async function managementPurgeRow(ticker, stage) {
  if (!ticker) return;
  const stageLabels = {
    review_queue: 'review queue (PENDING rows only)',
    final: 'final values',
    review_and_final: 'review queue + final values',
    scrape: 'scraped sources + everything downstream',
  };
  const label = stageLabels[stage] || stage;
  if (!confirm('Delete ' + label + ' for ' + ticker + '?\nThis cannot be undone.')) return;
  try {
    let url, body;
    if (stage === 'review_queue') {
      // Use batch-delete for today + any date to wipe all PENDING for this ticker
      const resp = await fetch('/api/delete/review', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ confirm: true, ticker: ticker, targets: ['queue'] }),
      });
      const data = await resp.json();
      if (!resp.ok || !data.success) throw new Error(data?.error?.message || ('HTTP ' + resp.status));
      showToast('Deleted review queue for ' + ticker + '.', false);
    } else if (stage === 'final') {
      const resp = await fetch('/api/delete/final', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ confirm: true, ticker: ticker }),
      });
      const data = await resp.json();
      if (!resp.ok || !data.success) throw new Error(data?.error?.message || ('HTTP ' + resp.status));
      showToast('Deleted final values for ' + ticker + '.', false);
    } else if (stage === 'review_and_final') {
      const resp = await fetch('/api/delete/review', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ confirm: true, ticker: ticker, targets: ['queue', 'final'] }),
      });
      const data = await resp.json();
      if (!resp.ok || !data.success) throw new Error(data?.error?.message || ('HTTP ' + resp.status));
      showToast('Deleted review + final for ' + ticker + '.', false);
    } else if (stage === 'scrape') {
      const resp = await fetch('/api/delete/scrape', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ confirm: true, ticker: ticker, purge_mode: 'reset' }),
      });
      const data = await resp.json();
      if (!resp.ok || !data.success) throw new Error(data?.error?.message || ('HTTP ' + resp.status));
      showToast('Deleted scraped sources for ' + ticker + '.', false);
    }
    await loadManagementInventory();
  } catch (err) {
    showToast('Error: ' + err.message, true);
  }
}

function onManagementScopeChange() {
  loadManagementInventory();
}

function onManagementStageChange() {
  loadManagementInventory();
}

function _parseCommaOrNewlineTerms(raw) {
  const text = String(raw || '');
  const parts = text.split(/[\n,]/g).map(function(p) { return p.trim().toLowerCase(); }).filter(Boolean);
  const seen = new Set();
  const out = [];
  parts.forEach(function(p) {
    if (!seen.has(p)) {
      seen.add(p);
      out.push(p);
    }
  });
  return out;
}

async function loadCompanies() {
  const tbody = document.getElementById('companies-tbody');
  try {
    const resp = await fetch('/api/companies', { cache: 'no-store' });
    if (!resp.ok) {
      _companiesLoaded = false;
      if (tbody) {
        tbody.innerHTML = '<tr><td colspan="7" style="text-align:center;color:var(--theme-danger)">Failed to load companies.</td></tr>';
      }
      showToast('Failed to load companies', true);
      return;
    }
    const data = await resp.json();
    _companies = data.data || [];
    await loadScraperGovernance();
    _companiesLoaded = true;
    renderCompanies();
    _renderInterpretTickerBar();
    loadScrapeQueue();
    loadPipelineObservability();
    loadDocumentOverview();
  } catch (_err) {
    _companiesLoaded = false;
    if (tbody) {
      tbody.innerHTML = '<tr><td colspan="7" style="text-align:center;color:var(--theme-danger)">Failed to load companies.</td></tr>';
    }
    showToast('Failed to load companies', true);
  }
}

function renderCompanies() {
  const tbody = document.getElementById('companies-tbody');
  if (!_companies.length) {
    tbody.innerHTML = '<tr><td colspan="8" style="text-align:center;color:var(--theme-text-muted)">No companies.</td></tr>';
    return;
  }
  const rows = [];
  _companies.forEach(function(c) {
    const mode = c.scraper_mode || 'skip';
    const modeBadge = '<span class="scraper-badge scraper-badge-' + escapeHtml(mode) + '">' + escapeHtml(mode) + '</span>';
    const activeBadge = c.active
      ? '<span class="scraper-badge scraper-badge-ok" style="margin-left:4px">active</span>'
      : '<span class="scraper-badge scraper-badge-skip" style="margin-left:4px">inactive</span>';
    const statusBadge = c.scraper_status && c.scraper_status !== 'never_run'
      ? '<span class="scraper-badge scraper-badge-' + escapeHtml(c.scraper_status) + '">' + escapeHtml(c.scraper_status) + '</span>'
      : '<span style="color:var(--theme-text-muted);font-size:0.75rem">never run</span>';
    const lastScrape = c.last_scrape_at ? c.last_scrape_at.slice(0, 10) : '—';
    const prStartValue = c.pr_start_date || '';

    // URL column: show ir_url link, rss_url link if present, url_template if present
    let urlParts = [];
    if (c.ir_url) {
      urlParts.push('<a href="' + escapeAttr(c.ir_url) + '" target="_blank" rel="noopener" style="font-size:0.75rem;color:var(--theme-accent)" title="IR page">IR</a>');
    }
    if (c.rss_url) {
      urlParts.push('<a href="' + escapeAttr(c.rss_url) + '" target="_blank" rel="noopener" style="font-size:0.75rem;color:var(--theme-highlight)" title="' + escapeAttr(c.rss_url) + '">RSS</a>');
    }
    if (c.prnewswire_url) {
      urlParts.push('<a href="' + escapeAttr(c.prnewswire_url) + '" target="_blank" rel="noopener" style="font-size:0.75rem;color:#64748b" title="' + escapeAttr(c.prnewswire_url) + '">PRN</a>');
    }
    if (c.globenewswire_url) {
      urlParts.push('<a href="' + escapeAttr(c.globenewswire_url) + '" target="_blank" rel="noopener" style="font-size:0.75rem;color:#2563eb" title="' + escapeAttr(c.globenewswire_url) + '">GNW</a>');
    }
    if (c.url_template) {
      urlParts.push('<span style="font-size:0.7rem;color:var(--theme-text-muted)" title="' + escapeAttr(c.url_template) + '">template</span>');
    }
    const urlCell = urlParts.length ? urlParts.join(' &middot; ') : '<span style="color:var(--theme-text-muted);font-size:0.75rem">—</span>';

    const detailId = 'cdetail-' + escapeHtml(c.ticker);

    // Skip reason shown inline (brief); sandbox_note in expandable detail
    let nameExtra = '';
    if (c.skip_reason) {
      nameExtra = '<div style="font-size:0.7rem;color:var(--theme-text-muted);margin-top:2px;max-width:320px;white-space:normal">' + escapeHtml(c.skip_reason.slice(0, 120)) + (c.skip_reason.length > 120 ? '…' : '') + '</div>';
    }

    const governance = _governanceByTicker[c.ticker];
    const governanceBadge = governance
      ? '<span class="scraper-badge scraper-badge-governance-' + escapeHtml(governance) + '"'
        + ' title="scraper governance: ' + escapeAttr(governance) + '" style="margin-left:4px">'
        + escapeHtml(governance) + '</span>'
      : '';

    const hasDetail = true;  // cadence + BTC anchor always shown
    const expandBtn = hasDetail
      ? '<button class="btn btn-sm btn-secondary" style="padding:1px 6px;font-size:0.7rem" data-ticker="' + escapeAttr(c.ticker) + '" onclick="toggleCompanyDetail(this.getAttribute(\'data-ticker\'))">+</button>'
      : '';

    rows.push(
      '<tr id="crow-' + escapeHtml(c.ticker) + '">'
      + '<td style="white-space:nowrap">'
          + '<a href="/company/' + escapeHtml(c.ticker) + '" style="color:var(--theme-accent);font-weight:600">' + escapeHtml(c.ticker) + '</a>'
          + activeBadge
        + '</td>'
      + '<td>' + escapeHtml(c.name) + nameExtra + '</td>'
      + '<td>' + modeBadge + (c.pr_start_date ? '<span style="font-size:0.7rem;color:var(--theme-text-muted);margin-left:4px" title="Crawler backfill floor, not verified monthly production coverage start">crawl from ' + escapeHtml(c.pr_start_date || '') + '</span>' : '') + '</td>'
      + '<td style="white-space:nowrap">'
          + '<input id="pr-start-date-input-' + escapeAttr(c.ticker) + '" type="date"'
            + ' value="' + escapeAttr(prStartValue) + '" style="width:140px;font-size:0.78rem">'
          + '<button class="btn btn-sm btn-secondary" style="margin-left:4px" data-ticker="' + escapeAttr(c.ticker) + '"'
            + ' onclick="savePrStartYear(this.getAttribute(\'data-ticker\'))">Save</button>'
          + '<div id="pr-start-date-msg-' + escapeAttr(c.ticker) + '" style="font-size:0.68rem;color:var(--theme-text-muted);margin-top:2px">'
            + (prStartValue ? 'Current ' + escapeHtml(prStartValue) : 'Required for discovery/template')
            + '</div>'
        + '</td>'
      + '<td style="font-size:0.8rem">' + urlCell + '</td>'
      + '<td>' + statusBadge + governanceBadge + '</td>'
      + '<td style="font-size:0.78rem;color:var(--theme-text-muted)">' + escapeHtml(lastScrape) + '</td>'
      + '<td style="display:flex;gap:4px;flex-wrap:wrap">'
          + expandBtn
          + '<button class="btn btn-sm btn-secondary" data-ticker="' + escapeAttr(c.ticker) + '" onclick="runTickerBootstrapProbe(this.getAttribute(\'data-ticker\'), false)">Probe</button>'
          + '<button class="btn btn-sm btn-secondary" data-ticker="' + escapeAttr(c.ticker) + '" onclick="runTickerBootstrapProbe(this.getAttribute(\'data-ticker\'), true)">Probe+Apply</button>'
          + (mode !== 'skip' ? '<button class="btn btn-sm btn-primary" data-ticker="' + escapeAttr(c.ticker) + '" onclick="triggerScrape(this.getAttribute(\'data-ticker\'))">Scrape</button>' : '')
          + '<button class="btn btn-sm btn-danger" style="font-size:0.7rem;padding:1px 6px" data-ticker="' + escapeAttr(c.ticker) + '" data-name="' + escapeAttr(c.name) + '" onclick="confirmDeleteCompany(this.getAttribute(\'data-ticker\'), this.getAttribute(\'data-name\'))">Delete</button>'
        + '</td>'
      + '</tr>'
    );

    // Expandable detail row (hidden by default)
    if (hasDetail) {
      const detailParts = [];

      // Scrape plan — explains what the scraper will do for this company
      {
        const plansByMode = {
          index: 'Paginates the IR press releases listing page (?page=1, 2, …) in order. '
            + 'On each page, identifies production announcements by title keyword match. '
            + 'Fetches and stores each matching press release as raw text. '
            + 'Stops automatically when a full page of production PRs is already ingested — '
            + 'so incremental runs only fetch what is new. '
            + (c.pr_start_date ? 'Crawler backfill floor is ' + escapeHtml(c.pr_start_date || '') + ' onward; this is not a guarantee of complete monthly mining coverage from that year.' : ''),
          rss: 'Fetches the RSS feed URL, which provides a rolling window of the most recent '
            + '~10 press releases. Suitable for staying current but does not backfill history. '
            + 'For full historical coverage, switch to index mode.',
          template: 'Generates one URL per month from the configured URL template and pr_start_date. '
            + 'Fetches each month\'s press release in sequence until the current month. '
            + 'Suitable for IR sites with predictable URL patterns.',
          playwright: 'Uses a headless browser to render JavaScript-heavy IR pages before extracting links. '
            + 'Falls back to index-style pagination once the page is rendered.',
          skip: 'Scraping is disabled for this company. '
            + (c.skip_reason ? 'Reason: ' + escapeHtml(c.skip_reason) : 'No reason recorded.'),
        };
        const planText = plansByMode[mode] || ('No scrape plan defined for mode: ' + escapeHtml(mode));
        const planColor = mode === 'skip' ? 'var(--theme-text-muted)' : 'var(--theme-text)';
        detailParts.push(
          '<div style="margin-bottom:8px;padding:6px 8px;background:var(--theme-bg-secondary);border-radius:4px;border-left:3px solid var(--theme-accent)">'
          + '<div style="font-size:0.7rem;font-weight:600;color:var(--theme-accent);margin-bottom:3px;text-transform:uppercase;letter-spacing:0.04em">Scrape plan — ' + escapeHtml(mode) + ' mode</div>'
          + '<div style="font-size:0.75rem;color:' + planColor + ';line-height:1.5">' + planText + '</div>'
          + '</div>'
        );
      }

      if (c.url_template) {
        detailParts.push('<div><span style="color:var(--theme-text-muted);font-size:0.72rem">Template: </span><code style="font-size:0.72rem">' + escapeHtml(c.url_template) + '</code></div>');
      }
      if (c.skip_reason && c.skip_reason.length > 120) {
        detailParts.push('<div style="margin-top:4px"><span style="color:var(--theme-text-muted);font-size:0.72rem">Skip reason: </span><span style="font-size:0.75rem">' + escapeHtml(c.skip_reason) + '</span></div>');
      }
      if (c.sandbox_note) {
        detailParts.push('<div style="margin-top:4px"><span style="color:var(--theme-text-muted);font-size:0.72rem">Notes: </span><span style="font-size:0.75rem">' + escapeHtml(c.sandbox_note) + '</span></div>');
      }
      // Reporting cadence — always show (affects gap-fill and time-spine merge)
      {
        const cadence = c.reporting_cadence || 'monthly';
        const cadenceBadge = cadence === 'monthly'
          ? '<span style="color:var(--theme-text-muted);font-size:0.75rem">monthly</span>'
          : '<span style="color:var(--theme-highlight,#f59e0b);font-size:0.75rem;font-weight:600">' + escapeHtml(cadence) + '</span>';
        detailParts.push(
          '<div style="display:flex;align-items:center;gap:8px;flex-wrap:wrap;margin-bottom:4px">'
          + '<span style="color:var(--theme-text-muted);font-size:0.72rem">Reporting cadence:</span>'
          + cadenceBadge
          + '<select id="cadence-sel-' + escapeAttr(c.ticker) + '" style="font-size:0.75rem">'
          + ['monthly','quarterly','annual'].map(function(v) {
              return '<option value="' + v + '"' + (v === cadence ? ' selected' : '') + '>' + v + '</option>';
            }).join('')
          + '</select>'
          + '<button class="btn btn-sm btn-secondary" style="font-size:0.72rem;padding:1px 7px"'
          +   ' data-ticker="' + escapeAttr(c.ticker) + '" onclick="saveCadence(this.getAttribute(\'data-ticker\'))">Save</button>'
          + '<span id="cadence-msg-' + escapeAttr(c.ticker) + '" style="font-size:0.72rem;color:var(--theme-text-muted)"></span>'
          + '</div>'
        );
      }
      if (c.cik) {
        const pivotDate = c.btc_first_filing_date || '';
        const pivotDisplay = pivotDate
          ? '<span style="color:var(--theme-success,#16a34a);font-size:0.75rem;font-weight:600">' + escapeHtml(pivotDate) + '</span>'
          : '<span style="color:var(--theme-text-muted);font-size:0.75rem">not detected — auto-detect runs on next EDGAR ingest</span>';
        detailParts.push(
          '<div style="margin-top:6px;display:flex;align-items:center;gap:8px;flex-wrap:wrap">'
          + '<span style="color:var(--theme-text-muted);font-size:0.72rem">BTC mining pivot date:</span>'
          + pivotDisplay
          + '<input id="btc-anchor-input-' + escapeAttr(c.ticker) + '" type="text" maxlength="10" placeholder="YYYY-MM-DD"'
          +   ' style="width:110px;font-size:0.75rem;padding:1px 4px" title="Override: set or clear the mining pivot date">'
          + '<button class="btn btn-sm btn-secondary" style="font-size:0.72rem;padding:1px 7px"'
          +   ' data-ticker="' + escapeAttr(c.ticker) + '" onclick="saveBtcAnchor(this.getAttribute(\'data-ticker\'))">Save</button>'
          + '<button class="btn btn-sm btn-secondary" style="font-size:0.72rem;padding:1px 7px"'
          +   ' data-ticker="' + escapeAttr(c.ticker) + '" onclick="detectBtcAnchor(this.getAttribute(\'data-ticker\'), false)">Detect</button>'
          + '<button class="btn btn-sm btn-secondary" style="font-size:0.72rem;padding:1px 7px"'
          +   ' data-ticker="' + escapeAttr(c.ticker) + '" onclick="detectBtcAnchor(this.getAttribute(\'data-ticker\'), true)">Re-detect</button>'
          + '<span id="btc-anchor-msg-' + escapeAttr(c.ticker) + '" style="font-size:0.72rem;color:var(--theme-text-muted)"></span>'
          + '</div>'
        );
        detailParts.push(
          '<div style="margin-top:6px;display:flex;align-items:center;gap:8px;flex-wrap:wrap">'
          + '<span style="color:var(--theme-text-muted);font-size:0.72rem">Backfill EDGAR:</span>'
          + '<input id="backfill-from-' + escapeAttr(c.ticker) + '" type="text" maxlength="10" placeholder="from YYYY-MM-DD"'
          +   ' style="width:128px;font-size:0.75rem;padding:1px 4px" title="Start date (leave blank to auto-detect gap)">'
          + '<input id="backfill-to-' + escapeAttr(c.ticker) + '" type="text" maxlength="10" placeholder="to YYYY-MM-DD"'
          +   ' style="width:128px;font-size:0.75rem;padding:1px 4px" title="End date (leave blank to auto-detect gap)">'
          + '<label style="font-size:0.72rem;display:flex;align-items:center;gap:3px">'
          +   '<input id="backfill-extract-' + escapeAttr(c.ticker) + '" type="checkbox"> auto-extract'
          + '</label>'
          + '<button class="btn btn-sm btn-secondary" style="font-size:0.72rem;padding:1px 7px"'
          +   ' data-ticker="' + escapeAttr(c.ticker) + '" onclick="triggerBackfill(this.getAttribute(\'data-ticker\'))">Backfill EDGAR</button>'
          + '<span id="backfill-msg-' + escapeAttr(c.ticker) + '" style="font-size:0.72rem;color:var(--theme-text-muted)"></span>'
          + '</div>'
          + '<div style="display:flex;gap:0.4rem;align-items:center;flex-wrap:wrap;margin-top:0.35rem">'
          + '<span style="font-size:0.72rem;color:var(--theme-text-muted)">Extraction attempts:</span>'
          + '<button class="btn btn-sm btn-secondary" style="font-size:0.72rem;padding:1px 7px"'
          +   ' data-ticker="' + escapeAttr(c.ticker) + '" onclick="resetExtractionAttempts(this.getAttribute(\'data-ticker\'))"'
          +   ' title="Zero extraction_attempts for pending/failed reports so they are picked up by the next extraction run">Reset attempt counters</button>'
          + '<span style="font-size:0.72rem;color:var(--theme-text-muted)">— unblocks reports silently skipped after 5 failures</span>'
          + '</div>'
        );
      }
      rows.push(
        '<tr id="' + escapeHtml(detailId) + '" style="display:none"><td colspan="7" style="background:var(--theme-bg-tertiary);padding:8px 16px">'
        + detailParts.join('')
        + '</td></tr>'
      );
    }
  });
  tbody.innerHTML = rows.join('');
}

async function loadScraperGovernance() {
  const msgEl = document.getElementById('gov-msg');
  const summaryEl = document.getElementById('gov-summary');
  const staleInput = document.getElementById('gov-stale-days');
  const staleDays = Math.max(1, Number(staleInput?.value || 30));
  if (msgEl) msgEl.textContent = 'Loading…';
  try {
    const resp = await fetch('/api/companies/scraper_governance?stale_days=' + encodeURIComponent(String(staleDays)), {
      cache: 'no-store',
    });
    const data = await resp.json();
    if (!resp.ok || !data.success) throw new Error(data.error?.message || ('HTTP ' + resp.status));
    const snap = data.data || {};
    const counts = snap.counts || {};
    const items = snap.items || [];
    _governanceByTicker = {};
    items.forEach(function(item) {
      _governanceByTicker[item.ticker] = item.governance_status;
    });
    if (summaryEl) {
      summaryEl.textContent =
        'Total ' + (counts.total ?? 0)
        + ' | needs_probe ' + (counts.needs_probe ?? 0)
        + ' | stale_skip ' + (counts.stale_skip ?? 0)
        + ' | skip_conflict_active_source ' + (counts.skip_conflict_active_source ?? 0);
    }
    if (msgEl) msgEl.textContent = 'updated';
    if (_companiesLoaded) renderCompanies();
  } catch (err) {
    if (msgEl) msgEl.textContent = 'failed';
    if (summaryEl) summaryEl.textContent = 'Failed to load governance: ' + String(err);
  }
}

async function runTickerBootstrapProbe(ticker, applyMode) {
  const msgEl = document.getElementById('gov-msg');
  if (msgEl) msgEl.textContent = 'Probing ' + ticker + '…';
  try {
    const resp = await fetch('/api/companies/' + encodeURIComponent(ticker) + '/bootstrap_probe', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({ apply_mode: !!applyMode }),
    });
    const data = await resp.json();
    if (!resp.ok || !data.success) throw new Error(data.error?.message || ('HTTP ' + resp.status));
    const d = data.data || {};
    showToast(
      ticker + ': recommended=' + (d.recommended_mode || 'skip')
      + ', active=' + String(d.active_candidates || 0)
      + (d.applied ? ' (applied)' : ''),
    );
    await loadCompanies();
  } catch (err) {
    showToast('Probe failed for ' + ticker + ': ' + String(err), true);
    if (msgEl) msgEl.textContent = 'probe failed';
  }
}

async function runGovernanceBootstrapAll(applyMode) {
  const msgEl = document.getElementById('gov-msg');
  const staleInput = document.getElementById('gov-stale-days');
  const staleDays = Math.max(1, Number(staleInput?.value || 30));
  if (msgEl) msgEl.textContent = 'Running batch probe…';
  try {
    const resp = await fetch('/api/companies/bootstrap_probe_all', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({
        stale_days: staleDays,
        apply_mode: !!applyMode,
      }),
    });
    const data = await resp.json();
    if (!resp.ok || !data.success) throw new Error(data.error?.message || ('HTTP ' + resp.status));
    const d = data.data || {};
    showToast(
      'Batch probe complete: '
      + String(d.completed || 0) + '/' + String(d.targeted || 0)
      + (d.failed ? (', failed=' + String(d.failed)) : ''),
      !!d.failed
    );
    await loadCompanies();
  } catch (err) {
    showToast('Batch probe failed: ' + String(err), true);
    if (msgEl) msgEl.textContent = 'batch failed';
  }
}

function toggleCompanyDetail(ticker) {
  const row = document.getElementById('cdetail-' + ticker);
  if (!row) return;
  row.style.display = row.style.display === 'none' ? '' : 'none';
}

function dbExportClick(evt) {
  const statusEl = document.getElementById('db-export-status');
  statusEl.style.color = 'var(--theme-text-muted)';
  statusEl.textContent = 'Preparing export…';
  // Let the browser follow the link normally; show feedback then clear after a moment.
  setTimeout(function() { statusEl.textContent = 'Download started — check your Downloads folder.'; }, 800);
  setTimeout(function() { statusEl.textContent = ''; }, 6000);
}

function onPurgeInput() {
  // Legacy stub — SCRAPE stage panel replaced by Data Management panel.
}

async function executePurge() {
  // Legacy stub — SCRAPE stage panel replaced by Data Management panel.
}

function _applyPurgeToCompanyState(purgedTicker, mode) {
  if (!_companies || !_companies.length) return;
  const resetCompanyOps = function(company) {
    return {
      ...company,
      scraper_status: 'never_run',
      last_scrape_at: null,
      last_scrape_error: null,
      probe_completed_at: null,
      scraper_issues_log: '',
    };
  };
  if (mode === 'hard_delete' && purgedTicker === 'ALL') {
    _companies = [];
  } else if (purgedTicker === 'ALL') {
    _companies = _companies.map(function(company) { return resetCompanyOps(company); });
  } else {
    _companies = _companies.map(function(company) {
      return company.ticker === purgedTicker ? resetCompanyOps(company) : company;
    });
  }
  renderCompanies();
}

async function syncCompanyConfig() {
  const msg = document.getElementById('companies-msg');
  msg.textContent = 'Syncing…';
  const resp = await fetch('/api/companies/sync', { method: 'POST' });
  const data = await resp.json();
  if (!data.success) { showToast(data.error?.message || 'Sync failed', true); msg.textContent = ''; return; }
  const r = data.data || {};
  const note = r.cleared_state ? ' (cleared state — no new inserts)' : '';
  msg.textContent = 'Synced: ' + (r.added || 0) + ' added, ' + (r.updated || 0) + ' updated' + note;
  _companiesLoaded = false;
  loadCompanies();
}

async function restoreCompaniesFromConfig() {
  const msg = document.getElementById('companies-msg');
  msg.textContent = 'Restoring…';
  const resp = await fetch('/api/companies/sync/restore', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ confirm: true }),
  });
  const data = await resp.json();
  if (!data.success) { showToast(data.error?.message || 'Restore failed', true); msg.textContent = ''; return; }
  const r = data.data || {};
  msg.textContent = 'Restored: ' + (r.added || 0) + ' added, ' + (r.updated || 0) + ' updated. Auto-sync re-enabled.';
  _companiesLoaded = false;
  loadCompanies();
}

function openAddCompany() {
  document.getElementById('add-company-form').style.display = '';
  document.getElementById('ac-error').textContent = '';
  const bulkMsg = document.getElementById('ac-bulk-msg');
  const bulkPreview = document.getElementById('ac-bulk-preview');
  if (bulkMsg) bulkMsg.textContent = '';
  if (bulkPreview) bulkPreview.textContent = '';
  onAddCompanyModeChange();
  document.getElementById('ac-ticker').focus();
}

function onAddCompanyModeChange() {
  const mode = (document.getElementById('ac-mode').value || 'skip').trim();
  const help = document.getElementById('ac-mode-help');
  const ir = document.getElementById('ac-ir-url');
  const rss = document.getElementById('ac-rss-url');
  const tmpl = document.getElementById('ac-url-template');
  const yr = document.getElementById('ac-pr-start-date');
  const skipReason = document.getElementById('ac-skip-reason');
  if (!help || !ir || !rss || !tmpl || !yr || !skipReason) return;

  // Reset mode-specific required flags before enabling current mode constraints.
  ir.required = false;
  rss.required = false;
  tmpl.required = false;
  yr.required = false;
  skipReason.required = false;

  if (mode === 'rss') {
    rss.required = false;
    help.textContent = 'rss: reads production PRs from RSS/feed endpoints. Required: RSS URL (or PRNewswire/GlobeNewswire URL).';
  } else if (mode === 'index') {
    ir.required = true;
    help.textContent = 'index: crawls IR index/listing page for PR links. Required: IR URL.';
  } else if (mode === 'template') {
    tmpl.required = true;
    yr.required = true;
    help.textContent = 'template: builds monthly PR URLs from a pattern. Required: Template URL and PR Start Year.';
  } else {
    help.textContent = 'skip: no scraping for this company. Optional: Skip Reason.';
  }
}

function _parseDelimitedRecords(rawText, delimiter) {
  const text = String(rawText || '').replace(/\r\n/g, '\n').replace(/\r/g, '\n');
  const rows = [];
  let row = [];
  let cur = '';
  let inQuotes = false;
  for (let i = 0; i < text.length; i += 1) {
    const ch = text[i];
    if (ch === '"') {
      if (inQuotes && text[i + 1] === '"') {
        cur += '"';
        i += 1;
      } else {
        inQuotes = !inQuotes;
      }
      continue;
    }
    if (ch === delimiter && !inQuotes) {
      row.push(cur.trim());
      cur = '';
      continue;
    }
    if (ch === '\n' && !inQuotes) {
      row.push(cur.trim());
      if (row.some(function(cell) { return String(cell || '').trim().length > 0; })) {
        rows.push(row);
      }
      row = [];
      cur = '';
      continue;
    }
    cur += ch;
  }
  if (inQuotes) {
    return { error: 'CSV parse error: unmatched quote detected.' };
  }
  row.push(cur.trim());
  if (row.some(function(cell) { return String(cell || '').trim().length > 0; })) {
    rows.push(row);
  }
  return { rows: rows };
}

function _normalizeBulkHeader(label) {
  return String(label || '').toLowerCase().replace(/[^a-z0-9]/g, '');
}

function _parseBulkCompanyRows(rawText) {
  const text = String(rawText || '');
  if (!text.trim()) {
    return { error: 'Paste header row plus at least one data row.' };
  }

  const firstLine = (text.match(/^[^\r\n]*/) || [''])[0];
  const tabCount = (firstLine.match(/\t/g) || []).length;
  const commaCount = (firstLine.match(/,/g) || []).length;
  const delimiter = tabCount > commaCount ? '\t' : ',';
  const parsed = _parseDelimitedRecords(text, delimiter);
  if (parsed.error) return parsed;
  const records = parsed.rows || [];
  if (records.length < 2) {
    return { error: 'Paste header row plus at least one data row.' };
  }

  const headers = records[0];
  const normalizedHeaders = headers.map(_normalizeBulkHeader);

  const aliases = {
    ticker: ['ticker', 'symbol', 'tickersymbol'],
    name: ['name', 'company', 'companyname'],
    ir_url: ['investorrelationbaseurl', 'investorrelationsbaseurl', 'irurl', 'ir', 'investorrelationurl'],
    cik: ['cik', 'cikedgar', 'edgarcik'],
    description: ['description', 'desc', 'note', 'notes'],
    prnewswire_url: ['prnewswire', 'prnewswireurl', 'prnews'],
    globenewswire_url: ['globenewswire', 'globenewswireurl', 'globalnewswire', 'globalnewswireurl'],
    scraper_mode: ['mode', 'scrapermode', 'scraper_mode', 'scrapemode'],
  };

  function findIndex(key) {
    const variants = aliases[key] || [];
    for (let i = 0; i < normalizedHeaders.length; i += 1) {
      if (variants.includes(normalizedHeaders[i])) return i;
    }
    return -1;
  }

  const idxTicker = findIndex('ticker');
  const idxName = findIndex('name');
  if (idxTicker < 0 || idxName < 0) {
    return { error: 'Header must include at least Ticker and Name columns.' };
  }
  const idxIr = findIndex('ir_url');
  const idxCik = findIndex('cik');
  const idxDesc = findIndex('description');
  const idxPrn = findIndex('prnewswire_url');
  const idxGnw = findIndex('globenewswire_url');
  const idxMode = findIndex('scraper_mode');
  const parse_warnings = [];
  const validModes = new Set(['rss', 'index', 'template', 'skip']);

  const rows = [];
  for (let i = 1; i < records.length; i += 1) {
    const cols = records[i];
    if (cols.length !== headers.length) {
      parse_warnings.push('row ' + (i + 1) + ': column count mismatch (' + cols.length + ' vs ' + headers.length + ')');
      continue;
    }
    const ticker = String(cols[idxTicker] || '').trim().toUpperCase();
    const name = String(cols[idxName] || '').trim();
    if (!ticker || !name) {
      parse_warnings.push('row ' + (i + 1) + ': ticker/name required');
      continue;
    }
    let mode = idxMode >= 0 ? String(cols[idxMode] || '').trim().toLowerCase() : '';
    if (mode && !validModes.has(mode)) {
      parse_warnings.push('row ' + (i + 1) + ': invalid mode "' + mode + '"');
      continue;
    }
    rows.push({
      ticker: ticker,
      name: name,
      ir_url: idxIr >= 0 ? String(cols[idxIr] || '').trim() : '',
      cik: idxCik >= 0 ? String(cols[idxCik] || '').trim() : '',
      description: idxDesc >= 0 ? String(cols[idxDesc] || '').trim() : '',
      prnewswire_url: idxPrn >= 0 ? String(cols[idxPrn] || '').trim() : '',
      globenewswire_url: idxGnw >= 0 ? String(cols[idxGnw] || '').trim() : '',
      scraper_mode: mode,
    });
  }
  if (!rows.length) {
    return { error: 'No valid rows found (Ticker + Name are required per row).' };
  }
  return { rows: rows, delimiter: delimiter, headers: headers, parse_warnings: parse_warnings };
}

function previewBulkCompanyPaste() {
  const msgEl = document.getElementById('ac-bulk-msg');
  const previewEl = document.getElementById('ac-bulk-preview');
  const raw = document.getElementById('ac-bulk-text').value || '';
  const parsed = _parseBulkCompanyRows(raw);
  if (parsed.error) {
    if (msgEl) {
      msgEl.style.color = 'var(--theme-danger)';
      msgEl.textContent = parsed.error;
    }
    if (previewEl) previewEl.textContent = '';
    return;
  }
  const sample = parsed.rows.slice(0, 8).map(function(r) { return r.ticker; }).join(', ');
  const warnCount = (parsed.parse_warnings || []).length;
  if (msgEl) {
    msgEl.style.color = warnCount ? 'var(--theme-warning)' : 'var(--theme-text-muted)';
    msgEl.textContent = 'Parsed ' + parsed.rows.length + ' rows' + (warnCount ? (' (' + warnCount + ' warnings)') : '') + '.';
  }
  if (previewEl) {
    previewEl.textContent = (warnCount
      ? ('Warnings: ' + parsed.parse_warnings.slice(0, 3).join(' | ') + (warnCount > 3 ? ' ...' : '') + ' | ')
      : '')
      + 'Preview: ' + sample + (parsed.rows.length > 8 ? ', ...' : '');
  }
}

async function submitBulkCompanies() {
  const msgEl = document.getElementById('ac-bulk-msg');
  const previewEl = document.getElementById('ac-bulk-preview');
  const raw = document.getElementById('ac-bulk-text').value || '';
  const parsed = _parseBulkCompanyRows(raw);
  if (parsed.error) {
    if (msgEl) {
      msgEl.style.color = 'var(--theme-danger)';
      msgEl.textContent = parsed.error;
    }
    return;
  }

  if (msgEl) {
    msgEl.style.color = 'var(--theme-text-muted)';
    msgEl.textContent = 'Importing ' + parsed.rows.length + ' rows...';
  }

  try {
    const existingResp = await fetch('/api/companies', { cache: 'no-store' });
    const existingJson = await existingResp.json();
    if (!existingResp.ok || !existingJson.success) {
      throw new Error(existingJson.error?.message || ('HTTP ' + existingResp.status));
    }
    const existing = new Set((existingJson.data || []).map(function(c) {
      return String(c.ticker || '').toUpperCase();
    }));

    let created = 0;
    let updated = 0;
    const failures = (parsed.parse_warnings || []).slice();
    for (let i = 0; i < parsed.rows.length; i += 1) {
      const row = parsed.rows[i];
      const rowMode = String(row.scraper_mode || '').trim().toLowerCase();
      const basePayload = {
        name: row.name,
        sector: 'BTC-miners',
      };
      if (row.ir_url) basePayload.ir_url = row.ir_url;
      if (row.cik) basePayload.cik = row.cik;
      if (row.description) basePayload.sandbox_note = row.description;
      if (row.prnewswire_url) basePayload.prnewswire_url = row.prnewswire_url;
      if (row.globenewswire_url) basePayload.globenewswire_url = row.globenewswire_url;
      if (rowMode) basePayload.scraper_mode = rowMode;

      try {
        if (existing.has(row.ticker)) {
          const updateResp = await fetch('/api/companies/' + encodeURIComponent(row.ticker), {
            method: 'PUT',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify(basePayload),
          });
          const updateJson = await updateResp.json();
          if (!updateResp.ok || !updateJson.success) {
            throw new Error(updateJson.error?.message || ('HTTP ' + updateResp.status));
          }
          updated += 1;
        } else {
          const createPayload = Object.assign({
            ticker: row.ticker,
            scraper_mode: rowMode || 'skip',
          }, basePayload);
          const createResp = await fetch('/api/companies', {
            method: 'POST',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify(createPayload),
          });
          const createJson = await createResp.json();
          if (!createResp.ok || !createJson.success) {
            throw new Error(createJson.error?.message || ('HTTP ' + createResp.status));
          }
          created += 1;
          existing.add(row.ticker);
        }
      } catch (rowErr) {
        failures.push(row.ticker + ': ' + String(rowErr));
      }
    }

    if (msgEl) {
      msgEl.style.color = failures.length ? 'var(--theme-danger)' : 'var(--theme-success)';
      msgEl.textContent =
        'Import complete. created=' + created + ', updated=' + updated + ', failed=' + failures.length;
    }
    if (previewEl) {
      previewEl.textContent = failures.length
        ? ('Failures: ' + failures.slice(0, 5).join(' | ') + (failures.length > 5 ? ' ...' : ''))
        : 'All rows imported successfully.';
    }
    _companiesLoaded = false;
    await loadCompanies();
  } catch (err) {
    if (msgEl) {
      msgEl.style.color = 'var(--theme-danger)';
      msgEl.textContent = 'Import failed: ' + String(err);
    }
  }
}

async function submitAddCompany() {
  const ticker = document.getElementById('ac-ticker').value.trim().toUpperCase();
  const name   = document.getElementById('ac-name').value.trim();
  const sector = document.getElementById('ac-sector').value.trim() || 'BTC-miners';
  const mode   = document.getElementById('ac-mode').value;
  const irUrl = document.getElementById('ac-ir-url').value.trim();
  const cik = document.getElementById('ac-cik').value.trim();
  const description = document.getElementById('ac-description').value.trim();
  const rssUrl = document.getElementById('ac-rss-url').value.trim();
  const prnewswireUrl = document.getElementById('ac-prnewswire-url').value.trim();
  const globenewswireUrl = document.getElementById('ac-globenewswire-url').value.trim();
  const urlTemplate = document.getElementById('ac-url-template').value.trim();
  const prStartDateRaw = document.getElementById('ac-pr-start-date').value.trim();
  const skipReason = document.getElementById('ac-skip-reason').value.trim();
  const reportingCadence = document.getElementById('ac-reporting-cadence').value || 'monthly';
  document.getElementById('ac-error').textContent = '';
  const payload = { ticker, name, sector, scraper_mode: mode, reporting_cadence: reportingCadence };
  if (irUrl) payload.ir_url = irUrl;
  if (cik) payload.cik = cik;
  if (description) payload.sandbox_note = description;
  if (rssUrl) payload.rss_url = rssUrl;
  if (prnewswireUrl) payload.prnewswire_url = prnewswireUrl;
  if (globenewswireUrl) payload.globenewswire_url = globenewswireUrl;
  if (urlTemplate) payload.url_template = urlTemplate;
  if (prStartDateRaw) payload.pr_start_date = prStartDateRaw;
  if (skipReason) payload.skip_reason = skipReason;

  const resp = await fetch('/api/companies', {
    method: 'POST',
    headers: {'Content-Type': 'application/json'},
    body: JSON.stringify(payload),
  });
  const data = await resp.json();
  if (!data.success) {
    document.getElementById('ac-error').textContent = data.error?.message || 'Error';
    if (resp.status === 409) {
      _companiesLoaded = false;
      loadCompanies();
    }
    return;
  }
  document.getElementById('add-company-form').style.display = 'none';
  _companiesLoaded = false;
  loadCompanies();
}

async function triggerScrape(ticker) {
  const resp = await fetch('/api/scrape/trigger/' + encodeURIComponent(ticker), { method: 'POST' });
  const data = await resp.json();
  if (!data.success) { showToast(data.error?.message || 'Failed to trigger scrape', true); return; }
  showToast('Scrape queued for ' + ticker);
  setTimeout(loadScrapeQueue, 1000);
}

async function saveCadence(ticker) {
  const sel = document.getElementById('cadence-sel-' + ticker);
  const msgEl = document.getElementById('cadence-msg-' + ticker);
  const val = sel?.value || 'monthly';
  msgEl.textContent = 'Saving…';
  try {
    const resp = await fetch('/api/companies/' + encodeURIComponent(ticker), {
      method: 'PUT',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ reporting_cadence: val }),
    });
    const data = await resp.json();
    if (!data.success) { msgEl.textContent = data.error?.message || 'Error'; return; }
    msgEl.textContent = 'Saved';
    await loadCompanies();
  } catch (e) {
    msgEl.textContent = 'Request failed';
  }
}

async function savePrStartYear(ticker) {
  const input = document.getElementById('pr-start-date-input-' + ticker);
  const msgEl = document.getElementById('pr-start-date-msg-' + ticker);
  const raw = (input?.value || '').trim();
  msgEl.textContent = 'Saving…';
  try {
    const resp = await fetch('/api/companies/' + encodeURIComponent(ticker), {
      method: 'PUT',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ pr_start_date: raw || null }),
    });
    const data = await resp.json();
    if (!data.success) { msgEl.textContent = data.error?.message || 'Error'; return; }
    msgEl.textContent = raw ? 'Saved ' + raw : 'Cleared';
    await loadCompanies();
  } catch (e) {
    msgEl.textContent = 'Request failed';
  }
}

async function saveBtcAnchor(ticker) {
  const input = document.getElementById('btc-anchor-input-' + ticker);
  const msgEl = document.getElementById('btc-anchor-msg-' + ticker);
  const val = (input?.value || '').trim();
  msgEl.textContent = 'Saving…';
  try {
    const resp = await fetch('/api/companies/' + encodeURIComponent(ticker), {
      method: 'PUT',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ btc_first_filing_date: val || null }),
    });
    const data = await resp.json();
    if (!data.success) { msgEl.textContent = data.error?.message || 'Error'; return; }
    msgEl.textContent = val ? 'Set to ' + val : 'Cleared (auto-detect on next ingest)';
    if (input) input.value = '';
    await loadCompanies();
  } catch (e) {
    msgEl.textContent = 'Request failed';
  }
}

async function detectBtcAnchor(ticker, force) {
  const msgEl = document.getElementById('btc-anchor-msg-' + ticker);
  msgEl.textContent = 'Querying EDGAR…';
  try {
    const resp = await fetch('/api/companies/' + encodeURIComponent(ticker) + '/detect_btc_anchor', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ force: !!force }),
    });
    const data = await resp.json();
    if (!data.success) { msgEl.textContent = data.error?.message || 'Error'; return; }
    const d = data.data || {};
    msgEl.textContent = d.detected ? ('Detected: ' + d.btc_first_filing_date) : 'No BTC filings found in EDGAR';
    await loadCompanies();
  } catch (e) {
    msgEl.textContent = 'Request failed';
  }
}

async function triggerBackfill(ticker) {
  const fromEl    = document.getElementById('backfill-from-' + ticker);
  const toEl      = document.getElementById('backfill-to-' + ticker);
  const extractEl = document.getElementById('backfill-extract-' + ticker);
  const msgEl     = document.getElementById('backfill-msg-' + ticker);
  if (!msgEl) return;

  const body = { auto_extract: !!(extractEl && extractEl.checked) };
  if (fromEl && fromEl.value.trim()) body.from_date = fromEl.value.trim();
  if (toEl && toEl.value.trim())     body.to_date   = toEl.value.trim();

  msgEl.textContent = 'Starting…';
  msgEl.style.color = 'var(--theme-text-muted)';
  try {
    const resp = await fetch('/api/backfill/' + encodeURIComponent(ticker), {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(body),
    });
    const data = await resp.json();
    if (!resp.ok || !data.success) {
      msgEl.textContent = data.error?.message || ('HTTP ' + resp.status);
      msgEl.style.color = 'var(--theme-danger,#dc2626)';
      return;
    }
    const d = data.data || {};
    if (d.detected) {
      if (fromEl) fromEl.value = d.from_date || '';
      if (toEl)   toEl.value   = d.to_date   || '';
    }
    msgEl.textContent = 'Running (' + (d.from_date || '?') + ' to ' + (d.to_date || '?') + ')…';
    _pollBackfill(ticker, d.task_id, msgEl);
  } catch (e) {
    msgEl.textContent = 'Request failed';
    msgEl.style.color = 'var(--theme-danger,#dc2626)';
  }
}

function _pollBackfill(ticker, taskId, msgEl) {
  const iv = setInterval(async function() {
    try {
      const resp = await fetch('/api/backfill/' + encodeURIComponent(taskId) + '/progress',
                               { cache: 'no-store' });
      if (!resp.ok) return;
      const data = await resp.json();
      if (!data.success) return;
      const s = data.data || {};
      if (s.status === 'complete') {
        clearInterval(iv);
        const n = s.reports_ingested || 0;
        const dp = s.data_points_extracted != null ? ' / ' + s.data_points_extracted + ' data points' : '';
        msgEl.textContent = 'Done — ' + n + ' report' + (n !== 1 ? 's' : '') + ' ingested' + dp;
        msgEl.style.color = 'var(--theme-success,#16a34a)';
      } else if (s.status === 'error') {
        clearInterval(iv);
        msgEl.textContent = 'Error: ' + (s.message || 'unknown');
        msgEl.style.color = 'var(--theme-danger,#dc2626)';
      } else {
        msgEl.textContent = 'Running (' + (s.phase || s.status) + ')…';
      }
    } catch (_) { /* network blip — keep polling */ }
  }, 2000);
}

async function enqueueAllScrapeJobs() {
  const btn = document.getElementById('scrape-all-btn');
  const msgEl = document.getElementById('scrape-all-msg');
  btn.disabled = true;
  msgEl.textContent = 'Enqueueing…';
  try {
    const resp = await fetch('/api/scrape/trigger-all', { method: 'POST' });
    const data = await resp.json();
    if (!data.success) {
      msgEl.textContent = data.error?.message || 'Failed';
      showToast(data.error?.message || 'Failed to enqueue scrapes', true);
      return;
    }
    const d = data.data || {};
    const n = (d.enqueued || []).length;
    const skipped = (d.skipped_mode || []).length;
    const already = (d.already_queued || []).length;
    msgEl.textContent = n + ' enqueued, ' + skipped + ' skipped (mode=skip), ' + already + ' already queued';
    showToast('Enqueued scrape jobs for ' + n + ' tickers');
    setTimeout(loadScrapeQueue, 1000);
  } catch (err) {
    msgEl.textContent = 'Error: ' + String(err);
    showToast('Request failed', true);
  } finally {
    btn.disabled = false;
  }
}

async function loadScrapeQueue() {
  const resp = await fetch('/api/scrape/queue', { cache: 'no-store' });
  if (!resp.ok) return;
  const data = await resp.json();
  const jobs = data.data || [];
  const tbody = document.getElementById('scrape-tbody');
  if (!jobs.length) {
    tbody.innerHTML = '<tr><td colspan="8" style="text-align:center;color:var(--theme-text-muted)">Queue is empty.</td></tr>';
    return;
  }
  tbody.innerHTML = jobs.map(function(j) {
    return '<tr>'
      + '<td style="font-size:0.72rem;color:var(--theme-text-muted)">' + escapeHtml(j.id) + '</td>'
      + '<td>' + escapeHtml(j.ticker) + '</td>'
      + '<td style="font-size:0.75rem">' + escapeHtml(j.job_type || '') + '</td>'
      + '<td><span class="scraper-badge scraper-badge-' + escapeHtml(j.status) + '">' + escapeHtml(j.status) + '</span></td>'
      + '<td style="font-size:0.72rem">' + escapeHtml((j.queued_at || '').slice(0, 16)) + '</td>'
      + '<td style="font-size:0.72rem">' + escapeHtml((j.started_at || '—').slice(0, 16)) + '</td>'
      + '<td style="font-size:0.72rem">' + escapeHtml((j.finished_at || '—').slice(0, 16)) + '</td>'
      + '<td style="font-size:0.72rem;color:var(--theme-danger);max-width:160px;overflow:hidden;text-overflow:ellipsis" title="' + escapeAttr(j.error_message || '') + '">' + escapeHtml((j.error_message || '').slice(0, 40)) + '</td>'
      + '</tr>';
  }).join('');
}

// ── Regime editor ────────────────────────────────────────────────────────────
async function editRegime(ticker) {
  _regimeTicker = ticker;
  document.getElementById('regime-editor-title-text').textContent = 'Regime windows - ' + ticker;
  document.getElementById('rw-error').textContent = '';
  document.getElementById('regime-editor').style.display = '';
  await refreshRegimeTags();
}

function closeRegimeEditor() {
  document.getElementById('regime-editor').style.display = 'none';
  _regimeTicker = null;
}

async function refreshRegimeTags() {
  if (!_regimeTicker) return;
  const resp = await fetch('/api/regime/' + encodeURIComponent(_regimeTicker));
  const data = await resp.json();
  const windows = data.data || [];
  const container = document.getElementById('regime-tags');
  if (!windows.length) {
    container.innerHTML = '<span style="font-size:0.75rem;color:var(--theme-text-muted)">No windows — defaults to monthly, last 36 months.</span>';
    return;
  }
  container.innerHTML = windows.map(function(w) {
    return '<span class="regime-tag">'
      + escapeHtml(w.cadence) + ' from ' + escapeHtml(w.start_date)
      + (w.end_date ? ' to ' + escapeHtml(w.end_date) : '')
      + (w.notes ? ' · ' + escapeHtml(w.notes) : '')
      + '<button class="regime-tag-del" data-id="' + escapeAttr(String(w.id)) + '" title="Remove" onclick="deleteRegimeWindow(this.getAttribute(\'data-id\'))">✕</button>'
      + '</span>';
  }).join('');
}

async function addRegimeWindow() {
  if (!_regimeTicker) return;
  const cadence    = document.getElementById('rw-cadence').value;
  const start_date = document.getElementById('rw-start').value;
  const end_date   = document.getElementById('rw-end').value || null;
  const notes      = document.getElementById('rw-notes').value;
  document.getElementById('rw-error').textContent = '';
  if (!start_date) { document.getElementById('rw-error').textContent = 'Start date required'; return; }
  const resp = await fetch('/api/regime/' + encodeURIComponent(_regimeTicker), {
    method: 'POST',
    headers: {'Content-Type': 'application/json'},
    body: JSON.stringify({ cadence, start_date, end_date, notes }),
  });
  const data = await resp.json();
  if (!data.success) { document.getElementById('rw-error').textContent = data.error?.message || 'Error'; return; }
  await refreshRegimeTags();
}

async function deleteRegimeWindow(windowId) {
  if (!_regimeTicker) return;
  await fetch('/api/regime/' + encodeURIComponent(_regimeTicker) + '/' + encodeURIComponent(windowId), { method: 'DELETE' });
  await refreshRegimeTags();
}


// ── Metric Schema (1.3.M) — SSOT management ──────────────────────────────────

async function loadMetricSchemaTable() {
  const tbody = document.getElementById('metric-schema-tbody');
  if (!tbody) return;
  tbody.innerHTML = '<tr><td colspan="7" style="color:var(--theme-text-muted);padding:6px">Loading...</td></tr>';
  try {
    const [mResp, kResp] = await Promise.all([
      fetch('/api/metric_schema?sector=BTC-miners'),
      fetch('/api/metric_keywords?all=1'),
    ]);
    if (!mResp.ok) throw new Error('HTTP ' + mResp.status);
    if (!kResp.ok) throw new Error('HTTP ' + kResp.status);
    const mBody = await mResp.json();
    const kBody = await kResp.json();
    if (!mBody.success) throw new Error(mBody.error?.message || 'Failed');
    const metrics = mBody.data || [];
    const allKws = kBody.success ? (kBody.data.keywords || []) : [];

    // Index keywords by metric_key
    const kwsByMetric = {};
    for (const kw of allKws) {
      if (!kwsByMetric[kw.metric_key]) kwsByMetric[kw.metric_key] = [];
      kwsByMetric[kw.metric_key].push(kw);
    }

    if (!metrics.length) {
      tbody.innerHTML = '<tr><td colspan="7" style="color:var(--theme-text-muted);padding:6px">No metrics defined.</td></tr>';
      return;
    }
    tbody.innerHTML = metrics.map(function(m) {
      const activeChecked = m.active ? 'checked' : '';
      const analystBadge = m.analyst_defined
        ? ' <span style="font-size:0.7rem;color:var(--theme-text-muted)">(custom)</span>' : '';
      const groupBadge = m.metric_group && m.metric_group !== 'other'
        ? `<span style="font-size:0.68rem;padding:1px 5px;border-radius:3px;background:rgba(100,100,200,0.15);color:var(--theme-text-muted);margin-left:3px">${escapeHtml(m.metric_group)}</span>`
        : '';
      const tierBadge = m.prompt_instructions
        ? `<span style="font-size:0.68rem;padding:1px 5px;border-radius:3px;background:rgba(59,130,246,0.15);color:#60a5fa;margin-left:3px" title="Serving from metric_schema.prompt_instructions">schema</span>`
        : `<span style="font-size:0.68rem;padding:1px 5px;border-radius:3px;background:var(--theme-bg-tertiary);color:var(--theme-text-muted);margin-left:3px" title="Serving from hardcoded default">default</span>`;
      const kws = kwsByMetric[m.key] || [];
      const chips = kws.map(function(kw) {
        const dimStyle = kw.active ? '' : 'opacity:0.5;';
        const titleAttr = kw.exclude_terms ? ` title="exclude: ${escapeHtml(kw.exclude_terms)}"` : '';
        return `<span class="kw-chip" style="${dimStyle}"${titleAttr}>${escapeHtml(kw.phrase)}<button class="kw-chip-del" onclick="deleteKw(${kw.id},'${escapeHtml(m.key)}')" title="Remove">&times;</button></span>`;
      }).join('');
      const addInput = `<input class="kw-add-input" type="text" placeholder="add phrase..."
        data-metric-key="${escapeHtml(m.key)}"
        onkeydown="if(event.key==='Enter'){addKwInline('${escapeHtml(m.key)}',this);event.preventDefault()}"
        style="font-size:0.75rem;padding:1px 5px;width:120px;background:var(--theme-bg-secondary);border:1px solid var(--theme-border);border-radius:3px;color:var(--theme-text);vertical-align:middle">`;
      const piVal = m.prompt_instructions || '';
      const qpVal = m.quarterly_prompt || '';
      return `<tr data-ms-id="${m.id}" data-ms-key="${escapeHtml(m.key)}" style="border-bottom:1px solid var(--theme-border)">
        <td style="padding:3px 6px;font-family:monospace;font-size:0.79rem;white-space:nowrap;vertical-align:top">${escapeHtml(m.key)}${analystBadge}${tierBadge}${groupBadge}</td>
        <td style="padding:3px 6px;vertical-align:top">
          <input type="text" value="${escapeHtml(m.label)}"
            style="font-size:0.8rem;padding:2px 5px;width:150px;background:var(--theme-bg-secondary);border:1px solid var(--theme-border);border-radius:3px;color:var(--theme-text)"
            onblur="saveMetricSchemaField(${m.id},'label',this.value)">
        </td>
        <td style="padding:3px 6px;vertical-align:top">
          <input type="text" value="${escapeHtml(m.unit)}"
            style="font-size:0.8rem;padding:2px 5px;width:62px;background:var(--theme-bg-secondary);border:1px solid var(--theme-border);border-radius:3px;color:var(--theme-text)"
            onblur="saveMetricSchemaField(${m.id},'unit',this.value)">
        </td>
        <td style="padding:3px 6px;vertical-align:top">
          <input type="text" value="${escapeHtml(m.metric_group || 'other')}"
            style="font-size:0.8rem;padding:2px 5px;width:70px;background:var(--theme-bg-secondary);border:1px solid var(--theme-border);border-radius:3px;color:var(--theme-text)"
            onblur="saveMetricSchemaField(${m.id},'metric_group',this.value)">
        </td>
        <td style="padding:3px 6px;vertical-align:middle">
          <div style="display:flex;flex-wrap:wrap;gap:3px;align-items:center">
            ${chips}${addInput}
          </div>
        </td>
        <td style="padding:3px 4px;text-align:center;vertical-align:top">
          <input type="checkbox" ${activeChecked} onchange="toggleMetricSchemaActive(${m.id}, this.checked)">
        </td>
        <td style="padding:3px 4px;vertical-align:top">
          <button class="btn btn-sm btn-secondary" onclick="togglePromptRow(${m.id})"
            style="font-size:0.72rem;padding:1px 6px;margin-bottom:2px">Prompt</button>
          <button class="btn btn-sm btn-danger" onclick="deleteMetricSchema(${m.id},'${escapeHtml(m.key)}')"
            style="font-size:0.72rem;padding:1px 6px">Del</button>
        </td>
      </tr>
      <tr id="prompt-row-${m.id}" class="prompt-detail-row">
        <td colspan="7" style="padding:6px 10px;background:var(--theme-bg-secondary)">
          <div style="display:flex;gap:1rem;flex-wrap:wrap">
            <div style="flex:1;min-width:200px">
              <label style="font-size:0.75rem;color:var(--theme-text-muted);display:block;margin-bottom:2px">Monthly prompt instructions</label>
              <textarea class="prompt-textarea" id="pi-${m.id}"
                onblur="saveMetricPromptField(${m.id},'prompt_instructions',this.value)">${escapeHtml(piVal)}</textarea>
            </div>
            <div style="flex:1;min-width:200px">
              <label style="font-size:0.75rem;color:var(--theme-text-muted);display:block;margin-bottom:2px">Quarterly/annual prompt instructions</label>
              <textarea class="prompt-textarea" id="qp-${m.id}"
                onblur="saveMetricPromptField(${m.id},'quarterly_prompt',this.value)">${escapeHtml(qpVal)}</textarea>
            </div>
          </div>
          <div style="font-size:0.72rem;color:var(--theme-text-muted);margin-top:4px">
            <span id="prompt-status-${m.id}"></span>
            Leave blank to use the built-in hardcoded default.
          </div>
        </td>
      </tr>`;
    }).join('');
  } catch (e) {
    tbody.innerHTML = `<tr><td colspan="7" style="color:var(--theme-danger);padding:6px">Error: ${escapeHtml(e.message)}</td></tr>`;
  }
}

// loadAllKeywordsTable is now an alias — both tables are combined
async function loadAllKeywordsTable() {
  return loadMetricSchemaTable();
}

async function saveKwField(kwId, metricKey, field, value) {
  try {
    const resp = await fetch(`/api/metric_schema/${metricKey}/keywords/${kwId}`, {
      method: 'PATCH',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({[field]: value}),
    });
    if (!resp.ok) throw new Error('HTTP ' + resp.status);
    const body = await resp.json();
    if (!body.success) throw new Error(body.error?.message || 'Failed');
    // Re-render row opacity for active toggle
    if (field === 'active') {
      const row = document.querySelector(`tr[data-kw-id="${kwId}"]`);
      if (row) row.style.opacity = value ? '1' : '0.5';
    }
  } catch (e) {
    showToast('Save failed: ' + e.message, 'error');
    loadAllKeywordsTable();
  }
}

async function deleteKw(kwId, metricKey) {
  try {
    const resp = await fetch(`/api/metric_schema/${metricKey}/keywords/${kwId}`, {method: 'DELETE'});
    if (!resp.ok) throw new Error('HTTP ' + resp.status);
    const body = await resp.json();
    if (!body.success) throw new Error(body.error?.message || 'Failed');
    await loadMetricSchemaTable();
  } catch (e) {
    showToast('Delete failed: ' + e.message, 'error');
  }
}

async function addKwInline(metricKey, inputEl) {
  const phrase = (inputEl.value || '').trim();
  if (!phrase) return;
  try {
    const resp = await fetch(`/api/metric_schema/${metricKey}/keywords`, {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({phrase}),
    });
    const body = await resp.json();
    if (!body.success && resp.status !== 409) throw new Error(body.error?.message || 'Failed');
    inputEl.value = '';
    await loadMetricSchemaTable();
  } catch (e) {
    showToast('Add failed: ' + e.message, 'error');
  }
}

async function submitBulkKeywords() {
  const textarea = document.getElementById('kw-bulk-paste');
  const statusEl = document.getElementById('kw-bulk-status');
  statusEl.style.display = 'none';
  const raw = textarea ? textarea.value.trim() : '';
  if (!raw) return;

  const lines = raw.split(/\n+/).map(l => l.trim()).filter(Boolean);

  // Detect format: if any line has >=4 tokens, treat as 5-column spreadsheet format
  // Key | Label | Unit | Pattern | Keywords(pipe-separated)
  let isSpreadsheetFormat = false;
  for (const line of lines) {
    if (_splitCsvLine(line).length >= 4) { isSpreadsheetFormat = true; break; }
  }

  if (isSpreadsheetFormat) {
    // Skip header row if col[0] is "key" or "label" (case-insensitive)
    let dataLines = lines;
    if (lines.length > 0) {
      const firstCol = _splitCsvLine(lines[0])[0].toLowerCase().trim();
      if (firstCol === 'key' || firstCol === 'label') dataLines = lines.slice(1);
    }

    let metricsCreated = 0, totalAdded = 0, totalSkipped = 0;
    const errors = [];

    for (const line of dataLines) {
      const tokens = _splitCsvLine(line);
      if (!tokens[0]) continue;
      const metricKey = tokens[0].trim().toLowerCase().replace(/\s+/g, '_');
      const label = tokens[1] ? tokens[1].trim() : metricKey;
      const unit = tokens[2] ? tokens[2].trim() : '';
      // tokens[3] = pattern column (ignored — managed separately)
      const kwRaw = tokens[4] ? tokens[4].trim() : '';
      const keywords = kwRaw ? kwRaw.split('|').map(k => k.trim()).filter(Boolean) : [];

      if (!metricKey) continue;

      // Create metric if it doesn't exist (409 = already exists, acceptable)
      try {
        const resp = await fetch('/api/metric_schema', {
          method: 'POST',
          headers: {'Content-Type': 'application/json'},
          body: JSON.stringify({key: metricKey, label, unit}),
        });
        const body = await resp.json();
        if (resp.ok && body.success) metricsCreated++;
        else if (resp.status !== 409) errors.push(`Create ${metricKey}: ${body.error?.message || 'failed'}`);
      } catch (e) {
        errors.push(`Create ${metricKey}: ${e.message}`);
        continue;
      }

      if (keywords.length) {
        try {
          const resp = await fetch(`/api/metric_schema/${metricKey}/keywords`, {
            method: 'POST',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify({phrases: keywords}),
          });
          const body = await resp.json();
          if (body.success) {
            totalAdded += body.data?.added ?? 0;
            totalSkipped += body.data?.skipped ?? 0;
          } else if (resp.status !== 409) {
            errors.push(`${metricKey} keywords: ${body.error?.message || 'failed'}`);
          }
        } catch (e) {
          errors.push(`${metricKey} keywords: ${e.message}`);
        }
      }
    }

    textarea.value = '';
    const parts = [];
    if (metricsCreated) parts.push(`${metricsCreated} metric${metricsCreated > 1 ? 's' : ''} created`);
    if (totalAdded) parts.push(`${totalAdded} keyword${totalAdded > 1 ? 's' : ''} added`);
    if (totalSkipped) parts.push(`${totalSkipped} duplicate${totalSkipped > 1 ? 's' : ''} skipped`);
    const msg = (parts.length ? parts.join(', ') : 'Nothing added') + (errors.length ? '. Errors: ' + errors.join('; ') : '.');
    statusEl.style.color = errors.length ? 'var(--theme-danger)' : 'var(--theme-success)';
    statusEl.textContent = msg;
    statusEl.style.display = 'block';
    await loadMetricSchemaTable();
    await loadAllKeywordsTable();
    return;
  }

  // Keyword-only format: metric_key, phrase[, exclude_terms] — one row per line
  const byMetric = {};
  const parseErrors = [];
  lines.forEach(function(line, i) {
    const tokens = _splitCsvLine(line);
    if (tokens.length < 2) {
      parseErrors.push(`Line ${i+1}: need at least metric_key and phrase`);
      return;
    }
    const metricKey = tokens[0].trim();
    const phrase = tokens[1].trim();
    const exclude = tokens[2] ? tokens[2].trim() : '';
    if (!metricKey || !phrase) return;
    if (!byMetric[metricKey]) byMetric[metricKey] = [];
    byMetric[metricKey].push({phrase, exclude});
  });

  if (parseErrors.length) {
    statusEl.style.color = 'var(--theme-danger)';
    statusEl.textContent = parseErrors.join('; ');
    statusEl.style.display = 'block';
    return;
  }

  const keys = Object.keys(byMetric);
  if (!keys.length) {
    statusEl.style.color = 'var(--theme-danger)';
    statusEl.textContent = 'No valid rows parsed.';
    statusEl.style.display = 'block';
    return;
  }

  let totalAdded = 0, totalSkipped = 0, errors = [];
  for (const key of keys) {
    const entries = byMetric[key];
    const hasExcludes = entries.some(e => e.exclude);
    if (!hasExcludes) {
      try {
        const resp = await fetch(`/api/metric_schema/${key}/keywords`, {
          method: 'POST',
          headers: {'Content-Type': 'application/json'},
          body: JSON.stringify({phrases: entries.map(e => e.phrase)}),
        });
        const body = await resp.json();
        if (body.success || body.error?.code === 'DUPLICATE') {
          totalAdded += body.data?.added ?? 0;
          totalSkipped += body.data?.skipped ?? 0;
        } else {
          errors.push(`${key}: ${body.error?.message || 'failed'}`);
        }
      } catch (e) {
        errors.push(`${key}: ${e.message}`);
      }
    } else {
      for (const entry of entries) {
        try {
          const payload = {phrase: entry.phrase};
          if (entry.exclude) payload.exclude_terms = entry.exclude;
          const resp = await fetch(`/api/metric_schema/${key}/keywords`, {
            method: 'POST',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify(payload),
          });
          const body = await resp.json();
          if (body.success) {
            totalAdded++;
          } else if (body.error?.code === 'DUPLICATE') {
            totalSkipped++;
          } else {
            errors.push(`${key}/${entry.phrase}: ${body.error?.message || 'failed'}`);
          }
        } catch (e) {
          errors.push(`${key}/${entry.phrase}: ${e.message}`);
        }
      }
    }
  }

  textarea.value = '';
  const msg = `Added ${totalAdded}${totalSkipped ? `, skipped ${totalSkipped} duplicate${totalSkipped > 1 ? 's' : ''}` : ''}.${errors.length ? ' Errors: ' + errors.join('; ') : ''}`;
  statusEl.style.color = errors.length ? 'var(--theme-danger)' : 'var(--theme-success)';
  statusEl.textContent = msg;
  statusEl.style.display = 'block';
  await loadAllKeywordsTable();
}

function _splitCsvLine(line) {
  // Minimal CSV tokeniser: handles "quoted, values" and tab/comma separators
  const tokens = [];
  let cur = '', inQuote = false;
  for (let i = 0; i < line.length; i++) {
    const ch = line[i];
    if (ch === '"') { inQuote = !inQuote; cur += ch; }
    else if (!inQuote && (ch === ',' || ch === '\t')) { tokens.push(cur); cur = ''; }
    else { cur += ch; }
  }
  tokens.push(cur);
  return tokens.map(t => t.trim());
}

function togglePromptRow(metricId) {
  var row = document.getElementById('prompt-row-' + metricId);
  if (!row) return;
  row.classList.toggle('open');
}

async function saveMetricPromptField(metricId, field, value) {
  var statusEl = document.getElementById('prompt-status-' + metricId);
  try {
    var resp = await fetch('/api/metric_schema/' + encodeURIComponent(metricId), {
      method: 'PATCH',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({[field]: value || null}),
    });
    var json = await resp.json();
    if (!resp.ok || !json.success) throw new Error(json.error && json.error.message || 'Save failed');
    if (statusEl) {
      statusEl.style.color = 'var(--theme-success,green)';
      statusEl.textContent = 'Saved';
      setTimeout(function() { if (statusEl) statusEl.textContent = ''; }, 2000);
    }
  } catch(err) {
    if (statusEl) {
      statusEl.style.color = 'var(--theme-danger)';
      statusEl.textContent = String(err);
    }
  }
}

async function toggleMetricSchemaActive(id, active) {
  try {
    const resp = await fetch(`/api/metric_schema/${id}`, {
      method: 'PATCH',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({active: active ? 1 : 0}),
    });
    if (!resp.ok) throw new Error('HTTP ' + resp.status);
    const body = await resp.json();
    if (!body.success) throw new Error(body.error?.message || 'Failed');
    // Refresh table
    loadMetricSchemaTable();
  } catch (e) {
    showToast('Failed to update metric: ' + e.message, 'error');
    loadMetricSchemaTable();  // revert checkbox to DB state
  }
}

async function saveMetricSchemaField(id, field, value) {
  try {
    const resp = await fetch(`/api/metric_schema/${id}`, {
      method: 'PATCH',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({[field]: value}),
    });
    if (!resp.ok) throw new Error('HTTP ' + resp.status);
    const body = await resp.json();
    if (!body.success) throw new Error(body.error?.message || 'Failed');
  } catch (e) {
    showToast('Failed to save: ' + e.message, 'error');
    loadMetricSchemaTable();
  }
}

async function deleteMetricSchema(id, key) {
  if (!confirm(`Delete metric "${key}"? This cannot be undone.`)) return;
  try {
    const resp = await fetch(`/api/metric_schema/${id}`, { method: 'DELETE' });
    if (!resp.ok) throw new Error('HTTP ' + resp.status);
    const body = await resp.json();
    if (!body.success) throw new Error(body.error?.message || 'Failed');
    loadMetricSchemaTable();
  } catch (e) {
    showToast('Delete failed: ' + e.message, 'error');
  }
}

async function addMetricSchema() {
  const key = document.getElementById('ms-add-key').value.trim();
  const label = document.getElementById('ms-add-label').value.trim();
  const unit = document.getElementById('ms-add-unit').value.trim();
  const errEl = document.getElementById('ms-add-error');
  errEl.style.display = 'none';
  if (!key || !label) {
    errEl.textContent = 'Key and Label are required.';
    errEl.style.display = 'block';
    return;
  }
  try {
    const resp = await fetch('/api/metric_schema', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({key, label, unit, sector: 'BTC-miners'}),
    });
    const body = await resp.json();
    if (!body.success) {
      errEl.textContent = body.error?.message || 'Failed';
      errEl.style.display = 'block';
      return;
    }
    document.getElementById('ms-add-key').value = '';
    document.getElementById('ms-add-label').value = '';
    document.getElementById('ms-add-unit').value = '';
    loadMetricSchemaTable();
    loadPatternMetricSelects();
  } catch (e) {
    errEl.textContent = 'Error: ' + e.message;
    errEl.style.display = 'block';
  }
}

// ── Snippet Analysis & Examples ───────────────────────────────────────────────

async function _populateExMetricSelect() {
  const sel = document.getElementById('ex-metric-select');
  if (!sel || sel.options.length > 1) return;
  try {
    const resp = await fetch('/api/metric_schema?sector=BTC-miners');
    const body = await resp.json();
    if (!body.success) return;
    sel.innerHTML = '<option value="">-- select metric --</option>';
    (body.data || []).forEach(m => {
      const opt = document.createElement('option');
      opt.value = m.key;
      opt.textContent = `${m.key} (${m.label})`;
      sel.appendChild(opt);
    });
  } catch (e) { /* silent */ }
}

async function analyzeSnippets() {
  const metric = document.getElementById('ex-metric-select').value;
  const ticker = (document.getElementById('ex-ticker-input').value || '').trim();
  if (!metric) { alert('Select a metric first.'); return; }
  const params = new URLSearchParams({limit: 500});
  if (ticker) params.set('ticker', ticker);
  try {
    const resp = await fetch(`/api/metric_schema/${metric}/snippet_analysis?${params}`);
    const body = await resp.json();
    if (!body.success) { alert(body.error?.message || 'Analysis failed'); return; }
    const d = body.data;
    const tableEl = document.getElementById('ex-table-rows');
    const proseEl = document.getElementById('ex-prose-ngrams');
    tableEl.innerHTML = '';
    proseEl.innerHTML = '';
    (d.table_rows || []).forEach(row => {
      tableEl.appendChild(_buildCandidateRow(metric, ticker, row));
    });
    (d.prose_ngrams || []).forEach(row => {
      proseEl.appendChild(_buildCandidateRow(metric, ticker, row));
    });
    document.getElementById('ex-analysis-results').style.display = 'block';
    loadExamplesForMetric();
  } catch (e) { alert('Error: ' + e.message); }
}

function _buildCandidateRow(metric, ticker, item) {
  const div = document.createElement('div');
  div.className = 'example-candidate-row';
  const badge = document.createElement('span');
  badge.className = 'frequency-badge';
  badge.textContent = item.frequency;
  const text = document.createElement('span');
  text.textContent = item.template;
  text.className = 'example-snippet-text';
  const btn = document.createElement('button');
  btn.className = 'btn btn-sm btn-secondary';
  btn.textContent = 'Save';
  btn.onclick = () => saveExampleFromAnalysis(metric, ticker || null, item.template, btn);
  div.appendChild(badge);
  div.appendChild(text);
  div.appendChild(btn);
  return div;
}

async function saveExampleFromAnalysis(metric, ticker, snippet, btn) {
  btn.disabled = true;
  try {
    const body = {snippet};
    if (ticker) body.ticker = ticker;
    const resp = await fetch(`/api/metric_schema/${metric}/examples`, {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify(body),
    });
    const json = await resp.json();
    if (json.success) {
      btn.textContent = 'Saved';
      loadExamplesForMetric();
    } else {
      btn.textContent = 'Error';
      btn.disabled = false;
    }
  } catch (e) { btn.textContent = 'Error'; btn.disabled = false; }
}

async function loadExamplesForMetric() {
  const metric = document.getElementById('ex-metric-select').value;
  const ticker = (document.getElementById('ex-ticker-input').value || '').trim();
  if (!metric) return;
  const params = new URLSearchParams({all: 1});
  if (ticker) params.set('ticker', ticker);
  try {
    const resp = await fetch(`/api/metric_schema/${metric}/examples?${params}`);
    const body = await resp.json();
    if (!body.success) return;
    const rows = body.data || [];
    const tbody = document.getElementById('ex-stored-tbody');
    tbody.innerHTML = '';
    rows.forEach(row => {
      const tr = document.createElement('tr');
      tr.style.borderBottom = '1px solid var(--theme-border)';
      tr.innerHTML = `
        <td style="padding:2px 6px">${row.ticker || '<em style="color:var(--theme-text-muted)">all</em>'}</td>
        <td class="example-snippet-text" style="padding:2px 6px;max-width:300px">${escapeHtml(row.snippet)}</td>
        <td style="padding:2px 6px">${row.label || ''}</td>
        <td style="padding:2px 6px;text-align:center">${row.active ? 'Y' : 'N'}</td>
        <td style="padding:2px 6px;text-align:center">
          <button class="btn btn-sm btn-danger" onclick="deleteExample(${row.id}, '${metric}')">Del</button>
        </td>`;
      tbody.appendChild(tr);
    });
    document.getElementById('ex-count-badge').textContent = rows.length;
    document.getElementById('ex-stored-section').style.display = 'block';
  } catch (e) { /* silent */ }
}

async function deleteExample(id, metric) {
  if (!confirm('Delete this example?')) return;
  try {
    const resp = await fetch(`/api/metric_schema/${metric}/examples/${id}`, {method: 'DELETE'});
    const body = await resp.json();
    if (body.success) loadExamplesForMetric();
    else alert(body.error?.message || 'Delete failed');
  } catch (e) { alert('Error: ' + e.message); }
}

async function addExampleManual() {
  const metric = document.getElementById('ex-metric-select').value;
  const ticker = (document.getElementById('ex-ticker-input').value || '').trim() || null;
  const snippet = document.getElementById('ex-add-snippet').value.trim();
  const label = document.getElementById('ex-add-label').value.trim() || null;
  const sourceType = document.getElementById('ex-add-source-type').value || null;
  const errEl = document.getElementById('ex-add-error');
  errEl.style.display = 'none';
  if (!metric) { errEl.textContent = 'Select a metric first.'; errEl.style.display = 'block'; return; }
  if (!snippet) { errEl.textContent = 'Snippet is required.'; errEl.style.display = 'block'; return; }
  try {
    const resp = await fetch(`/api/metric_schema/${metric}/examples`, {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({snippet, ticker, label, source_type: sourceType}),
    });
    const body = await resp.json();
    if (!body.success) {
      errEl.textContent = body.error?.message || 'Failed';
      errEl.style.display = 'block';
      return;
    }
    document.getElementById('ex-add-snippet').value = '';
    document.getElementById('ex-add-label').value = '';
    document.getElementById('ex-add-source-type').value = '';
    loadExamplesForMetric();
  } catch (e) {
    errEl.textContent = 'Error: ' + e.message;
    errEl.style.display = 'block';
  }
}

// ── Ollama Concurrency (1.4.O) ────────────────────────────────────────────────

async function loadOllamaSettings() {
  try {
    const resp = await fetch('/api/config');
    const json = await resp.json();
    if (!json.success) return;
    const map = {};
    (json.data || []).forEach(e => { map[e.key] = e.value; });

    const numParallel = map['ollama_num_parallel'] || '4';
    const keepAlive   = map['ollama_keep_alive']   || '2h';
    const maxLoaded   = map['ollama_max_loaded_models'] || '3';

    const npEl = document.getElementById('ollama-num-parallel');
    const kaEl = document.getElementById('ollama-keep-alive');
    const mlEl = document.getElementById('ollama-max-loaded-models');
    if (npEl) npEl.value = numParallel;
    if (kaEl) kaEl.value = keepAlive;
    if (mlEl) mlEl.value = maxLoaded;

    _updateOllamaServeCmd(maxLoaded);
    _seedWorkerInputs(numParallel);
  } catch(err) {
    console.warn('loadOllamaSettings failed', err);
  }
}

function _updateOllamaServeCmd(val) {
  const el = document.getElementById('ollama-serve-cmd');
  if (el) el.textContent = 'OLLAMA_MAX_LOADED_MODELS=' + val + ' ollama serve';
}

function _seedWorkerInputs(val) {
  const n = Math.max(1, parseInt(val, 10) || 4);
  const interpretEl = document.getElementById('interpret-extract-workers');
  const pipelineEl  = document.getElementById('pipeline-extract-workers');
  if (interpretEl) interpretEl.value = n;
  if (pipelineEl)  pipelineEl.value  = n;
}

async function saveOllamaSettings() {
  const msgEl = document.getElementById('ollama-settings-msg');
  const np = (document.getElementById('ollama-num-parallel')?.value || '').trim();
  const ka = (document.getElementById('ollama-keep-alive')?.value || '').trim();
  const ml = (document.getElementById('ollama-max-loaded-models')?.value || '').trim();

  if (!np || !ka || !ml) {
    if (msgEl) { msgEl.textContent = 'All three fields are required.'; msgEl.style.color = 'var(--theme-danger, #ef4444)'; }
    return;
  }

  try {
    await Promise.all([
      fetch('/api/config/ollama_num_parallel',    { method: 'POST', headers: {'Content-Type':'application/json'}, body: JSON.stringify({value: np}) }),
      fetch('/api/config/ollama_keep_alive',       { method: 'POST', headers: {'Content-Type':'application/json'}, body: JSON.stringify({value: ka}) }),
      fetch('/api/config/ollama_max_loaded_models',{ method: 'POST', headers: {'Content-Type':'application/json'}, body: JSON.stringify({value: ml}) }),
    ]);
    _updateOllamaServeCmd(ml);
    _seedWorkerInputs(np);
    if (msgEl) { msgEl.textContent = 'Saved.'; msgEl.style.color = 'var(--theme-text-muted)'; }
  } catch(err) {
    if (msgEl) { msgEl.textContent = 'Save failed: ' + String(err); msgEl.style.color = 'var(--theme-danger, #ef4444)'; }
  }
}

// ── llama-server Config (1.4.L) ───────────────────────────────────────────────

async function loadLlamaServerSettings() {
  try {
    const resp = await fetch('/api/config', { cache: 'no-store' });
    const data = await resp.json();
    const map = {};
    (data.data?.config || []).forEach(function(r) { map[r.key] = r.value; });

    const defaultsResp = await Promise.all([
      'llama_model_path','llama_parallel','llama_ctx_size','llama_n_predict',
      'llama_batch_size','llama_cache_type_k','llama_cache_type_v',
      'llama_flash_attn','llama_threads','llama_port',
    ].map(function(k) {
      return fetch('/api/config/' + k + '/default', { cache: 'no-store' })
        .then(function(r) { return r.json(); })
        .then(function(d) { return [k, d.data?.default ?? '']; })
        .catch(function() { return [k, '']; });
    }));
    const defaults = {};
    defaultsResp.forEach(function(pair) { defaults[pair[0]] = pair[1]; });

    function _val(k) { return map[k] !== undefined ? map[k] : defaults[k]; }

    const el = function(id) { return document.getElementById(id); };
    if (el('llama-model-path'))  el('llama-model-path').value  = _val('llama_model_path');
    if (el('llama-parallel'))    el('llama-parallel').value    = _val('llama_parallel');
    if (el('llama-ctx-size'))    el('llama-ctx-size').value    = _val('llama_ctx_size');
    if (el('llama-n-predict'))   el('llama-n-predict').value   = _val('llama_n_predict');
    if (el('llama-batch-size'))  el('llama-batch-size').value  = _val('llama_batch_size');
    if (el('llama-threads'))     el('llama-threads').value     = _val('llama_threads');
    if (el('llama-port'))        el('llama-port').value        = _val('llama_port');
    if (el('llama-cache-type-k')) el('llama-cache-type-k').value = _val('llama_cache_type_k');
    if (el('llama-cache-type-v')) el('llama-cache-type-v').value = _val('llama_cache_type_v');
    if (el('llama-flash-attn'))  el('llama-flash-attn').checked = (_val('llama_flash_attn') === '1');
    _updateLlamaCmdPreview();
  } catch(err) {
    console.warn('loadLlamaServerSettings failed', err);
  }
}

function _buildLlamaCommand() {
  const el = function(id) { return document.getElementById(id); };
  const model    = (el('llama-model-path')?.value  || '').trim();
  const parallel = (el('llama-parallel')?.value    || '4').trim();
  const ctx      = (el('llama-ctx-size')?.value    || '8192').trim();
  const npredict = (el('llama-n-predict')?.value   || '768').trim();
  const batch    = (el('llama-batch-size')?.value  || '4096').trim();
  const cacheK   = (el('llama-cache-type-k')?.value || 'q8_0').trim();
  const cacheV   = (el('llama-cache-type-v')?.value || 'q8_0').trim();
  const threads  = (el('llama-threads')?.value     || '4').trim();
  const port     = (el('llama-port')?.value        || '8080').trim();
  const flash    = el('llama-flash-attn')?.checked;
  let cmd = 'llama-server'
    + ' --model ' + model
    + ' --ctx-size ' + ctx
    + ' --n-predict ' + npredict
    + ' --parallel ' + parallel
    + ' --batch-size ' + batch
    + ' --cache-type-k ' + cacheK
    + ' --cache-type-v ' + cacheV
    + ' --threads ' + threads
    + ' --port ' + port
    + ' -ngl 99';
  if (flash) cmd += ' --flash-attn';
  return cmd;
}

function _updateLlamaCmdPreview() {
  const preview = document.getElementById('llama-cmd-preview');
  if (!preview) return;
  preview.textContent = _buildLlamaCommand();
  preview.style.display = 'block';
}

async function saveLlamaServerSettings() {
  const msgEl = document.getElementById('llama-settings-msg');
  const el = function(id) { return document.getElementById(id); };
  const parallel = (el('llama-parallel')?.value || '').trim();
  const entries = [
    ['llama_model_path',  (el('llama-model-path')?.value  || '').trim()],
    ['llama_parallel',    parallel],
    ['llama_ctx_size',    (el('llama-ctx-size')?.value    || '').trim()],
    ['llama_n_predict',   (el('llama-n-predict')?.value   || '').trim()],
    ['llama_batch_size',  (el('llama-batch-size')?.value  || '').trim()],
    ['llama_cache_type_k',(el('llama-cache-type-k')?.value || '').trim()],
    ['llama_cache_type_v',(el('llama-cache-type-v')?.value || '').trim()],
    ['llama_flash_attn',   el('llama-flash-attn')?.checked ? '1' : '0'],
    ['llama_threads',     (el('llama-threads')?.value     || '').trim()],
    ['llama_port',        (el('llama-port')?.value        || '').trim()],
  ];
  try {
    await Promise.all(entries.map(function(pair) {
      return fetch('/api/config/' + pair[0], {
        method: 'POST',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify({value: pair[1]}),
      });
    }));
    // Keep Extract Workers in sync with parallel slot count
    if (parallel) {
      await fetch('/api/config/ollama_num_parallel', {
        method: 'POST',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify({value: parallel}),
      });
      _seedWorkerInputs(parallel);
    }
    _updateLlamaCmdPreview();
    if (msgEl) { msgEl.textContent = 'Saved. Restart llama-server (./llm.sh) to apply.'; msgEl.style.color = 'var(--theme-text-muted)'; }
  } catch(err) {
    if (msgEl) { msgEl.textContent = 'Save failed: ' + String(err); msgEl.style.color = 'var(--theme-danger, #ef4444)'; }
  }
}

async function copyLlamaCommand() {
  const cmd = _buildLlamaCommand();
  const msgEl = document.getElementById('llama-settings-msg');
  try {
    await navigator.clipboard.writeText(cmd);
    if (msgEl) { msgEl.textContent = 'Command copied to clipboard.'; msgEl.style.color = 'var(--theme-text-muted)'; }
  } catch(_) {
    if (msgEl) { msgEl.textContent = cmd; msgEl.style.color = 'var(--theme-text-muted)'; }
  }
}

// ── Metric Rules tab ─────────────────────────────────────────────────────────

async function loadMetricRules() {
  const tbody = document.getElementById('rules-tbody');
  if (!tbody) return;
  tbody.innerHTML = '<tr><td colspan="6" style="color:var(--theme-text-muted)">Loading...</td></tr>';
  try {
    const resp = await fetch('/api/metric_rules');
    if (!resp.ok) throw new Error('HTTP ' + resp.status);
    const json = await resp.json();
    const rules = json.data || [];
    if (!rules.length) {
      tbody.innerHTML = '<tr><td colspan="6" style="color:var(--theme-text-muted)">No rules found.</td></tr>';
      return;
    }
    tbody.innerHTML = rules.map(function(r) {
      var m = escapeHtml(r.metric);
      return '<tr>' +
        '<td style="font-family:monospace">' + m + '</td>' +
        '<td><input class="rules-input" type="number" step="0.001" min="0" max="1" ' +
          'value="' + r.agreement_threshold + '" ' +
          'data-metric="' + m + '" data-field="agreement_threshold" style="width:72px" ' +
          'title="Max relative difference (fraction) between LLM and regex before routing to review. 0.02 = 2%."></td>' +
        '<td><input class="rules-input" type="number" step="0.01" min="0" ' +
          'value="' + r.outlier_threshold + '" ' +
          'data-metric="' + m + '" data-field="outlier_threshold" style="width:72px" ' +
          'title="Max deviation from trailing average (fraction) before flagging as outlier. 0.4 = 40%."></td>' +
        '<td><input class="rules-input" type="number" step="1" min="1" ' +
          'value="' + r.outlier_min_history + '" ' +
          'data-metric="' + m + '" data-field="outlier_min_history" style="width:55px" ' +
          'title="Minimum prior accepted data points required before outlier check applies."></td>' +
        '<td><input class="rules-input" type="number" step="1" min="0" ' +
          'value="' + (r.valid_range_min != null ? r.valid_range_min : '') + '" ' +
          'data-metric="' + m + '" data-field="valid_range_min" style="width:72px" ' +
          'placeholder="0" title="Values below this floor are discarded (confidence=0)."></td>' +
        '<td><input class="rules-input" type="number" step="1" min="0" ' +
          'value="' + (r.valid_range_max != null ? r.valid_range_max : '') + '" ' +
          'data-metric="' + m + '" data-field="valid_range_max" style="width:72px" ' +
          'placeholder="5000" title="Values above this ceiling are discarded (confidence=0)."></td>' +
        '<td><select class="rules-input" data-metric="' + m + '" data-field="enabled">' +
          '<option value="1"' + (r.enabled ? ' selected' : '') + '>On</option>' +
          '<option value="0"' + (!r.enabled ? ' selected' : '') + '>Off</option>' +
          '</select></td>' +
        '<td><button class="btn btn-xs btn-primary" onclick="saveMetricRule(\'' + m + '\')">Save</button>' +
          ' <button class="btn btn-xs btn-danger" onclick="deleteMetricRule(\'' + m + '\')">Delete</button>' +
          ' <span id="rules-msg-' + m + '" style="font-size:0.75rem;color:var(--theme-text-muted)"></span></td>' +
        '</tr>';
    }).join('');
    makeSortable('rules-table');
  } catch(err) {
    tbody.innerHTML = '<tr><td colspan="6" style="color:var(--theme-danger)">Error: ' + escapeHtml(String(err)) + '</td></tr>';
  }
}

async function syncMetricRules() {
  try {
    const resp = await fetch('/api/metric_rules/sync', { method: 'POST' });
    const json = await resp.json();
    if (!resp.ok || !json.success) throw new Error(json.error && json.error.message || 'Sync failed');
    const d = json.data;
    if (d.inserted_count > 0) {
      showToast('Inserted ' + d.inserted_count + ' rule(s): ' + d.inserted.join(', '));
    } else {
      showToast('All metric_schema keys already have rules. Nothing to insert.');
    }
    loadMetricRules();
  } catch(err) {
    showToast('Sync failed: ' + String(err), true);
  }
}

async function saveMetricRule(metric) {
  const getVal = function(field) {
    const el = document.querySelector('[data-metric="' + metric + '"][data-field="' + field + '"]');
    return el ? el.value : null;
  };
  var vrmin = getVal('valid_range_min');
  var vrmax = getVal('valid_range_max');
  const body = {
    agreement_threshold: parseFloat(getVal('agreement_threshold')),
    outlier_threshold: parseFloat(getVal('outlier_threshold')),
    outlier_min_history: parseInt(getVal('outlier_min_history'), 10),
    enabled: parseInt(getVal('enabled'), 10),
    valid_range_min: vrmin !== '' && vrmin != null ? parseFloat(vrmin) : null,
    valid_range_max: vrmax !== '' && vrmax != null ? parseFloat(vrmax) : null,
  };
  const msgEl = document.getElementById('rules-msg-' + metric);
  try {
    const resp = await fetch('/api/metric_rules/' + encodeURIComponent(metric), {
      method: 'PUT',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify(body),
    });
    const json = await resp.json();
    if (!resp.ok || !json.success) throw new Error(json.error && json.error.message || 'Save failed');
    if (msgEl) { msgEl.textContent = 'Saved'; setTimeout(function() { if (msgEl) msgEl.textContent = ''; }, 2000); }
  } catch(err) {
    if (msgEl) msgEl.style.color = 'var(--theme-danger)';
    if (msgEl) msgEl.textContent = String(err);
  }
}

async function deleteMetricRule(metric) {
  if (!confirm('Delete rule for "' + metric + '"? The pipeline will fall back to config.py defaults until a new rule is created.')) return;
  try {
    const resp = await fetch('/api/metric_rules/' + encodeURIComponent(metric), { method: 'DELETE' });
    const json = await resp.json();
    if (!resp.ok || !json.success) throw new Error(json.error && json.error.message || 'Delete failed');
    showToast('Deleted rule for ' + metric);
    loadMetricRules();
  } catch(err) {
    showToast('Delete failed: ' + String(err), true);
  }
}

var _delTicker = null;

function confirmDeleteCompany(ticker, name) {
  _delTicker = ticker;
  var body = document.getElementById('del-company-body');
  var modal = document.getElementById('del-company-modal');
  body.innerHTML = '<p>Checking data for <strong>' + escapeHtml(ticker) + '</strong>...</p>';
  document.getElementById('del-cascade-btn').style.display = 'none';
  document.getElementById('del-btn').style.display = 'none';
  modal.style.display = 'flex';

  fetch('/api/companies/' + encodeURIComponent(ticker), {
    method: 'DELETE',
    headers: {'Content-Type': 'application/json'},
    body: JSON.stringify({cascade: false})
  }).then(function(r) {
    return r.json().then(function(j) { return {status: r.status, body: j}; });
  }).then(function(res) {
    if (res.status === 409) {
      var counts = res.body.counts || {};
      var rows = Object.entries(counts).map(function(e) {
        return '<tr><td style="padding:1px 8px 1px 0">' + escapeHtml(e[0]) + '</td><td>' + e[1] + ' rows</td></tr>';
      }).join('');
      body.innerHTML =
        '<p>Company <strong>' + escapeHtml(ticker) + '</strong> has linked data:</p>'
        + '<table style="font-size:0.82rem;width:100%;margin-bottom:0.75rem">' + rows + '</table>'
        + '<p style="color:var(--theme-text-muted);font-size:0.82rem">Cascade delete will permanently remove all associated reports, data points, and extractions.</p>';
      document.getElementById('del-cascade-btn').style.display = '';
      document.getElementById('del-btn').style.display = 'none';
    } else if (res.status === 200) {
      body.innerHTML =
        '<p>Delete <strong>' + escapeHtml(ticker) + ' &mdash; ' + escapeHtml(name) + '</strong>?</p>'
        + '<p style="color:var(--theme-text-muted);font-size:0.82rem">This company has no linked data and can be safely removed.</p>';
      document.getElementById('del-cascade-btn').style.display = 'none';
      document.getElementById('del-btn').style.display = '';
    } else {
      body.innerHTML = '<p style="color:var(--theme-danger)">Unexpected response: ' + escapeHtml(String(res.status)) + '</p>';
    }
  }).catch(function(e) {
    body.innerHTML = '<p style="color:var(--theme-danger)">Error: ' + escapeHtml(e.message) + '</p>';
  });
}

function doDeleteCompany(cascade) {
  if (!_delTicker) return;
  fetch('/api/companies/' + encodeURIComponent(_delTicker), {
    method: 'DELETE',
    headers: {'Content-Type': 'application/json'},
    body: JSON.stringify({cascade: cascade})
  }).then(function(r) { return r.json(); })
  .then(function(j) {
    closeDeleteModal();
    if (j.success) {
      showToast(_delTicker + ' deleted');
      loadCompanies();
    } else {
      showToast('Delete failed: ' + ((j.error && j.error.message) ? j.error.message : 'unknown'), true);
    }
  }).catch(function(e) { showToast('Delete failed: ' + e.message, true); });
}

function closeDeleteModal() {
  document.getElementById('del-company-modal').style.display = 'none';
  _delTicker = null;
}

