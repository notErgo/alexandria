// Ingest pane behavior extracted from templates/ops.html.
let _ingestLoaded = false;
let _crawlPollTimer = null;
let _activePipelineSubTab = 'scrape';
let _crawlTaskId = null;
let _crawlLogTicker = null;
let _crawlSnaps = {};

async function triggerScrapeAll() {
  if (_acqTaskId) { showToast('An acquire task is already running', true); return; }
  _setAcquireButtonsEnabled(false);
  _acqTaskId = 'starting';
  _setAcqStatus('Scrape All: queued', false);
  _appendAcqLog(new Date().toISOString().slice(11, 19) + ' queued scrape-all');
  try {
    const resp = await fetch('/api/ingest/all', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({}),
    });
    const data = await resp.json();
    if (!resp.ok || !data.success) {
      throw new Error((data.error && data.error.message) || ('HTTP ' + resp.status));
    }
    _acqTaskId = data.data.task_id;
    _setAcqStatus('Scrape All: running', false);
    _appendAcqLog(new Date().toISOString().slice(11, 19) + ' running scrape-all (task ' + _acqTaskId + ')');
    _pollAcquireTask(_acqTaskId, 'all');
  } catch (err) {
    _acqTaskId = null;
    _setAcquireButtonsEnabled(true);
    _setAcqStatus('Scrape All: failed', true);
    _appendAcqLog(new Date().toISOString().slice(11, 19) + ' failed scrape-all (' + String(err) + ')');
    showToast('Scrape All failed: ' + String(err), true);
  }
}

function triggerScrapeReset() {
  if (!confirm('This will delete ALL raw reports and reset extraction state for all sources. This cannot be undone.\n\nType CONFIRM to proceed.')) return;
  const answer = prompt('Type RESET_SCRAPE to confirm deletion of all ingested documents:');
  if (answer !== 'RESET_SCRAPE') { showToast('Reset cancelled', false); return; }
  showToast('Use the Purge control in Interpret > QC to execute a full reset.', false);
  activateTab('interpret');
}

function triggerAcquireEdgarForCrawl(all) {
  const tickers = all ? [] : Array.from(_selectedCrawlTickers);
  return triggerAcquire('edgar', tickers.length ? tickers : undefined);
}

function loadIngest() {
  _ingestLoaded = true;
  _renderAllTickerBars();
  _pollCrawlStatus();
  _restorePipelineRun();
}

function _renderAllTickerBars() {
  _renderCrawlTickerSelector('crawl-ticker-selector', false);
  _renderCrawlTickerSelector('pipeline-ticker-selector', false);
  _renderInterpretTickerBar();
  _renderGapTickerBar();
}

function _renderResearchInputBar() {
  const bar = document.getElementById('research-ticker-bar');
  if (!bar) return;
  const actives = _companies.filter(function(c) { return c.active !== false; });
  bar.innerHTML = '';
  if (!actives.length) { bar.textContent = 'No companies configured.'; return; }
  actives.forEach(function(c) {
    const btn = document.createElement('button');
    btn.className = 'btn btn-sm btn-secondary crawl-ticker-btn' + (_selectedCrawlTickers.has(c.ticker) ? ' active' : '');
    btn.textContent = c.ticker;
    btn.dataset.ticker = c.ticker;
    btn.onclick = function() { _toggleCrawlTicker(c.ticker); };
    bar.appendChild(btn);
  });
}

function _renderInterpretTickerBar() {
  const bar = document.getElementById('interpret-ticker-bar');
  if (!bar) return;
  const actives = _companies.filter(function(c) { return c.active !== false; });
  bar.innerHTML = '';
  if (!actives.length) { bar.textContent = 'No companies configured.'; return; }
  actives.forEach(function(c) {
    const btn = document.createElement('button');
    btn.className = 'btn btn-sm btn-secondary crawl-ticker-btn' + (_selectedCrawlTickers.has(c.ticker) ? ' active' : '');
    btn.textContent = c.ticker;
    btn.dataset.ticker = c.ticker;
    btn.onclick = function() { _toggleCrawlTicker(c.ticker); };
    bar.appendChild(btn);
  });
}

function _buildCrawlTickerSelector() {
  _renderCrawlTickerSelector('crawl-ticker-selector', false);
}

function _buildEdgarTickerSelector() {
  _renderCrawlTickerSelector('edgar-ticker-selector', true);
}

const _selectedCrawlTickers = new Set();

function _toggleCrawlTicker(ticker) {
  if (_selectedCrawlTickers.has(ticker)) {
    _selectedCrawlTickers.delete(ticker);
  } else {
    _selectedCrawlTickers.add(ticker);
  }
  _renderAllTickerBars();
  if (_selectedCrawlTickers.size === 1) {
    loadCrawlPrompt(Array.from(_selectedCrawlTickers)[0]);
  }
}

function _renderCrawlTickerSelector(barId, cikOnly) {
  const bar = document.getElementById(barId);
  if (!bar) return;
  const actives = _companies.filter(function(c) { return c.active && (!cikOnly || c.cik); });
  bar.innerHTML = '';
  actives.forEach(function(c) {
    const btn = document.createElement('button');
    btn.className = 'btn btn-sm btn-secondary crawl-ticker-btn' + (_selectedCrawlTickers.has(c.ticker) ? ' active' : '');
    btn.textContent = c.ticker;
    btn.dataset.ticker = c.ticker;
    btn.onclick = function() { _toggleCrawlTicker(c.ticker); };
    bar.appendChild(btn);
  });
}

function _getSharedSelectedTickers() {
  return Array.from(_selectedCrawlTickers);
}

function _getCrawlSelectedTickers() { return _getSharedSelectedTickers(); }

async function triggerAcquireEdgar(all) {
  const tickers = all ? [] : _getSharedSelectedTickers();
  if (!all && !tickers.length) {
    setPipelineMsg('Select at least one company first, or use Acquire All', true);
    return;
  }
  return triggerAcquire('edgar', tickers);
}

async function startCrawlSelected() {
  const tickers = _getCrawlSelectedTickers();
  if (!tickers.length) {
    setPipelineMsg('Select at least one company first', true);
    return;
  }
  await startCrawlTickers(tickers);
}

async function stopCrawl() {
  try {
    const resp = await fetch('/api/crawl/stop', { method: 'POST', headers: {'Content-Type': 'application/json'}, body: '{}' });
    const data = await resp.json();
    if (data.success) {
      setPipelineMsg('Stop signalled (' + (data.data?.signalled || 0) + ' crawlers)');
    }
  } catch (err) {
    setPipelineMsg('Failed to stop: ' + String(err), true);
  }
}

async function startCrawlAll() {
  const tickers = _companies.length
    ? _companies.filter(function(c) { return c.active !== false; }).map(function(c) { return c.ticker; })
    : [];
  await startCrawlTickers(tickers);
}

async function loadCrawlOllamaModels() {
  await populateOllamaModelSelect('crawl-model');
}

function onCrawlProviderChange() {
  const provider = document.getElementById('crawl-provider')?.value;
  const wrap = document.getElementById('crawl-model-wrap');
  if (wrap) wrap.style.display = provider === 'ollama' ? '' : 'none';
}

async function loadCrawlPrompt(ticker) {
  const sourceEl = document.getElementById('crawl-prompt-source');
  try {
    const resp = await fetch('/api/crawl/prompt/' + ticker);
    const data = await resp.json();
    if (resp.ok && data.success) {
      document.getElementById('crawl-prompt-editor').value = data.data.prompt;
      if (sourceEl) sourceEl.textContent = '(' + (data.data.source === 'ticker_file' ? ticker + '_crawl.md' : 'master template') + ')';
    } else {
      document.getElementById('crawl-prompt-editor').value = '';
      if (sourceEl) sourceEl.textContent = '(no prompt file found)';
    }
  } catch (err) {
    if (sourceEl) sourceEl.textContent = '';
  }
}

async function startCrawlTickers(tickers) {
  if (!tickers.length) {
    setPipelineMsg('No active companies to crawl', true);
    return;
  }
  const provider = document.getElementById('crawl-provider')?.value || 'ollama';
  const model = document.getElementById('crawl-model')?.value || null;
  const prompt = document.getElementById('crawl-prompt-editor')?.value || null;
  setPipelineMsg('Starting crawl for ' + tickers.join(', ') + ' via ' + provider + '…');
  try {
    const body = {tickers: tickers, provider: provider};
    if (model && model.trim()) body.model = model.trim();
    if (prompt && prompt.trim()) body.prompt = prompt.trim();
    const resp = await fetch('/api/crawl/start', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify(body),
    });
    const data = await resp.json();
    if (!resp.ok || !data.success) throw new Error(data.error?.message || 'Start failed');
    _crawlTaskId = data.data.task_id;
    setPipelineMsg('Crawl started — task ' + _crawlTaskId.slice(0, 8) + '…');
    const stopBtn = document.getElementById('crawl-stop-btn');
    if (stopBtn) stopBtn.style.display = '';
    _startCrawlPoll();
  } catch (err) {
    setPipelineMsg('Failed to start crawl: ' + String(err), true);
  }
}

function _startCrawlPoll() {
  if (_crawlPollTimer) clearInterval(_crawlPollTimer);
  _crawlPollTimer = setInterval(_pollCrawlStatus, 2000);
}

async function _pollCrawlStatus() {
  try {
    const resp = await fetch('/api/crawl/status');
    if (!resp.ok) return;
    const data = await resp.json();
    const snaps = data.data || [];
    _crawlSnaps = {};
    snaps.forEach(function(s) { _crawlSnaps[s.ticker] = s; });
    _renderResearchGrid(snaps);

    const anyRunning = snaps.some(function(s) {
      return s.status === 'running' || s.status === 'pending';
    });
    if (anyRunning && !_crawlPollTimer) {
      _crawlPollTimer = setInterval(_pollCrawlStatus, 2000);
      const stopBtn = document.getElementById('crawl-stop-btn');
      if (stopBtn) stopBtn.style.display = '';
    }
    if (!anyRunning && _crawlPollTimer) {
      clearInterval(_crawlPollTimer);
      _crawlPollTimer = null;
      const stopBtn = document.getElementById('crawl-stop-btn');
      if (stopBtn) stopBtn.style.display = 'none';
    }

    if (_crawlLogTicker && _crawlSnaps[_crawlLogTicker]) {
      _renderResearchLog(_crawlSnaps[_crawlLogTicker]);
    }
  } catch (_e) {}
}

function _renderResearchGrid(snaps) {
  const grid = document.getElementById('research-grid');
  if (!grid) return;
  if (!snaps.length) {
    grid.innerHTML = '<p style="color:var(--theme-text-muted);font-size:0.85rem">No active crawl session. Click "Crawl All" or a ticker button to start.</p>';
    return;
  }
  grid.innerHTML = '';
  snaps.forEach(function(s) {
    const color = s.status === 'complete' ? 'var(--theme-success)'
      : s.status === 'failed' ? 'var(--theme-danger)'
      : s.status === 'running' ? 'var(--theme-accent)'
      : 'var(--theme-text-muted)';
    const card = document.createElement('div');
    card.className = 'card';
    card.style.cssText = 'padding:0.5rem;cursor:pointer;';
    card.onclick = function() { openResearchLog(s.ticker); };
    const lastLog = s.log && s.log.length ? s.log[0] : '';
    const ctxTokens = s.ctx_tokens || 0;
    const ctxLimit = s.ctx_limit || 0;
    const ctxPct = ctxLimit > 0 ? Math.round(ctxTokens / ctxLimit * 100) : 0;
    const ctxColor = ctxPct >= 85 ? '#f97316' : ctxPct >= 60 ? '#f59e0b' : 'var(--theme-text-muted)';
    const ctxStr = ctxLimit > 0
      ? (ctxTokens > 0 ? Math.round(ctxTokens / 1000) + 'k / ' + Math.round(ctxLimit / 1000) + 'k tokens (' + ctxPct + '%)' : ctxLimit > 0 ? 'limit ' + Math.round(ctxLimit / 1000) + 'k' : '')
      : '';
    card.innerHTML = [
      '<div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:0.25rem">',
      '  <span style="font-weight:600;font-size:0.85rem">' + escapeHtml(s.ticker) + '</span>',
      '  <span style="font-size:0.7rem;color:' + color + '">' + escapeHtml(s.status) + (s.api_calls ? ' (' + s.api_calls + ' calls)' : '') + '</span>',
      '</div>',
      '<div style="font-size:0.72rem;color:var(--theme-text-muted)">',
      '  Pages: ' + s.pages_fetched + ' &bull; Stored: ' + s.docs_stored + ' &bull; Skipped: ' + s.docs_skipped,
      '</div>',
      ctxStr ? '<div style="font-size:0.7rem;color:' + ctxColor + ';margin-top:0.2rem;font-family:ui-monospace,monospace">' + escapeHtml(ctxStr) + '</div>' : '',
      lastLog ? '<div style="font-size:0.68rem;color:var(--theme-text-muted);margin-top:0.25rem;overflow:hidden;white-space:nowrap;text-overflow:ellipsis" title="' + escapeAttr(lastLog) + '">' + escapeHtml(lastLog) + '</div>' : '',
    ].join('');
    grid.appendChild(card);
  });
}

function openResearchLog(ticker) {
  _crawlLogTicker = ticker;
  document.getElementById('research-log-ticker').textContent = ticker;
  document.getElementById('research-log-panel').style.display = '';
  const snap = _crawlSnaps[ticker];
  if (snap) _renderResearchLog(snap);
}

function closeResearchLog() {
  _crawlLogTicker = null;
  document.getElementById('research-log-panel').style.display = 'none';
}

function _renderResearchLog(snap) {
  const el = document.getElementById('research-log');
  if (!el) return;
  el.innerHTML = (snap.log || []).map(function(line) {
    const isWarn = line.indexOf('WARN') !== -1 || line.indexOf('ERROR') !== -1;
    const isToken = line.indexOf('tokens') !== -1 || line.indexOf('ctx ') !== -1;
    const color = isWarn ? '#f97316' : isToken ? 'var(--theme-accent)' : '';
    return '<div' + (color ? ' style="color:' + color + '"' : '') + '>' + escapeHtml(line) + '</div>';
  }).join('');
}

function setPipelineMsg(text, isError) {
  const el = document.getElementById('research-msg');
  if (!el) return;
  el.textContent = text;
  el.style.color = isError ? 'var(--theme-danger)' : 'var(--theme-text-muted)';
}
