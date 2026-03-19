// ── Help overlay ─────────────────────────────────────────────────────────────
function toggleHelp() {
  const el = document.getElementById('help-overlay');
  if (!el) return;
  el.classList.toggle('open');
}
document.addEventListener('keydown', function(e) {
  if (e.key === 'Escape') {
    const el = document.getElementById('help-overlay');
    if (el && el.classList.contains('open')) el.classList.remove('open');
  }
});

// ── Dev mode / path filter ───────────────────────────────────────────────────
//
// SSOT: ui_spec.json provides the authoritative path classification (critical /
// optional / later / n/a) for every component. On init, each [data-spec-id] and
// [data-ui-id] element is stamped with a data-path attribute from the spec;
// CSS body classes do the actual filtering.
//
// Cycle on each Dev button click (or Ctrl+Shift+D):
//   'all'               — show everything, spec-ids visible, tooltips active
//   'critical'          — critical path only; tooltips suppressed
//   'critical+optional' — critical + optional; tooltips suppressed

const _DEV_STATES = ['all', 'critical', 'critical+optional'];
const _DEV_LABELS  = { 'all': 'Dev', 'critical': 'Critical', 'critical+optional': '+Optional' };
const _DEV_BODY_CLASSES = {
  'all':               [],
  'critical':          ['path-filter-critical'],
  'critical+optional': ['path-filter-critical-optional'],
};
let _devPathState = 'all';

let _uiSpecPathMap = null;

async function _loadUiSpecPaths() {
  if (_uiSpecPathMap) return _uiSpecPathMap;
  try {
    const resp = await fetch('/static/data/ui_spec.json');
    const spec  = await resp.json();
    _uiSpecPathMap = {};
    (spec.components || []).forEach(function(c) {
      if (c.id && c.path && c.path !== 'n/a') _uiSpecPathMap[c.id] = c.path;
    });
  } catch (_e) { _uiSpecPathMap = {}; }
  return _uiSpecPathMap;
}

async function _applyDataPaths() {
  const pathMap = await _loadUiSpecPaths();
  ['data-spec-id', 'data-ui-id'].forEach(function(attr) {
    document.querySelectorAll('[' + attr + ']').forEach(function(el) {
      const path = pathMap[el.getAttribute(attr)];
      if (path) el.setAttribute('data-path', path);
    });
  });
}

// Save/restore native browser tooltips (title attributes) when suppressing them.
const _titleCache = new Map();

function _suppressTitles() {
  document.querySelectorAll('[title]').forEach(function(el) {
    if (!_titleCache.has(el)) _titleCache.set(el, el.getAttribute('title'));
    el.removeAttribute('title');
  });
}

function _restoreTitles() {
  _titleCache.forEach(function(val, el) {
    if (document.contains(el)) el.setAttribute('title', val);
  });
  _titleCache.clear();
}

function _applyDevState() {
  const body    = document.body;
  const btn     = document.getElementById('dev-mode-btn');
  const isDevOn = _devPathState !== 'all';

  body.classList.remove('path-filter-critical', 'path-filter-critical-optional', 'dev-no-tips');
  (_DEV_BODY_CLASSES[_devPathState] || []).forEach(function(c) { body.classList.add(c); });
  if (isDevOn) body.classList.add('dev-no-tips');

  if (isDevOn) { _suppressTitles(); } else { _restoreTitles(); }

  if (btn) {
    btn.textContent = _DEV_LABELS[_devPathState] || 'Dev';
    btn.classList.toggle('path-active', isDevOn);
  }

  try { localStorage.setItem('dev_path_state', _devPathState); } catch (_e) {}
}

function toggleDevMode() {
  const idx = _DEV_STATES.indexOf(_devPathState);
  _devPathState = _DEV_STATES[(idx + 1) % _DEV_STATES.length];
  _applyDevState();
}

document.addEventListener('keydown', function(e) {
  if (e.ctrlKey && e.shiftKey && e.key === 'D') { e.preventDefault(); toggleDevMode(); }
});

(async function _initDevMode() {
  await _applyDataPaths();
  try {
    const saved = localStorage.getItem('dev_path_state');
    if (_DEV_STATES.includes(saved)) _devPathState = saved;
  } catch (_e) {}
  _applyDevState();
})();

// ── UI Inspector toggle ────────────────────────────────────────────────────
// Toggles the base.html hover inspector (block highlight + floating chip).
// Stored in localStorage('devMode') — same key as base.html's applyDevMode().
function toggleInspect() {
  // 'no-dev-mode' on body = inspector OFF; absent = inspector ON
  const wasOff = document.body.classList.contains('no-dev-mode');
  document.body.classList.toggle('no-dev-mode', !wasOff);
  try { localStorage.setItem('devMode', wasOff ? 'true' : 'false'); } catch (_e) {}
  const btn = document.getElementById('inspect-btn');
  if (btn) btn.classList.toggle('path-active', wasOff); // lit = inspector ON
}

(function _initInspectBtn() {
  // Sync button visual state after base.html has applied devMode from localStorage.
  // Use setTimeout(0) to run after base.html's DOMContentLoaded handler.
  window.addEventListener('DOMContentLoaded', function() {
    setTimeout(function() {
      const isOff = document.body.classList.contains('no-dev-mode');
      const btn = document.getElementById('inspect-btn');
      if (btn) btn.classList.toggle('path-active', !isOff);
    }, 0);
  });
})();

// ── Tab routing ─────────────────────────────────────────────────────────────
const VALID_TABS = ['workflow', 'config', 'ingest', 'interpret', 'review', 'data', 'interrogate', 'health'];
const TAB_ALIASES = {
  companies: 'config', rules: 'config', settings: 'config', research: 'ingest',
  registry: 'data', explorer: 'data', qc: 'interpret', guide: 'workflow',
};
let _activeTab = null;

function activateTab(name) {
  // Resolve backward-compat aliases
  if (Object.prototype.hasOwnProperty.call(TAB_ALIASES, name)) {
    name = TAB_ALIASES[name] || 'config';
  }
  if (!VALID_TABS.includes(name)) name = 'config';
  if (_activeTab === name) return;
  _activeTab = name;
  document.querySelectorAll('.ops-tab').forEach(function(b) {
    b.classList.toggle('active', b.getAttribute('data-pane') === name);
  });
  document.querySelectorAll('.ops-pane').forEach(function(p) {
    p.classList.toggle('active', p.id === 'pane-' + name);
  });
  // Update URL without navigation
  const url = new URL(window.location.href);
  url.searchParams.set('tab', name);
  history.replaceState(null, '', url.toString());
  // Lazy-load tab data on first activation
  if (name === 'workflow' && !_workflowLoaded) loadWorkflow();
  if (name === 'config') {
    if (!_companiesLoaded) loadCompanies();
    loadSettings();
    loadMetricSchemaTable();
  }
  if (name === 'review') { if (!_reviewLoaded) loadReview(); if (!_keywordDictionary) loadKeywordDictionaryOptions(); if (!window._minerDataBooted) { window._minerDataBooted = true; if (typeof boot === 'function') boot(); } }

  if (name === 'data') { if (!_registryLoaded) loadRegistry(); if (!_metricsLoaded) loadExplorerMetrics(); if (!_keywordDictionary) loadKeywordDictionaryOptions(); if (!_explorerLoaded) loadExplorer(); }
  if (name === 'ingest') { loadIngest(); loadPipelineObservability(); loadCrawlOllamaModels(); }
  if (name === 'health') { loadHealthTab(false); }
  if (name === 'interpret') { loadOllamaModels(); loadPromptPreview(); pePopulateMetrics(); onSourceDocsChange(); _loadQCTable(); _loadGapMetrics(); loadInterpretMetricToggles(); if (_companiesLoaded) { _renderInterpretTickerBar(); _renderGapTickerBar(); } else loadCompanies().then(function() { _renderInterpretTickerBar(); _renderGapTickerBar(); }); }
}

// ── Pipeline sub-tab routing ─────────────────────────────────────────────────
function activatePipelineSubTab(pane, name) {
  const parentPane = document.getElementById('pane-' + pane);
  if (!parentPane) return;
  parentPane.querySelectorAll('.pipeline-sub-tab').forEach(function(btn) {
    btn.classList.toggle('active', btn.getAttribute('data-spane') === name);
  });
  parentPane.querySelectorAll('.pipeline-sub-pane').forEach(function(p) {
    p.classList.toggle('active', p.id === 'spane-' + pane + '-' + name);
  });
  // Lazy-load on sub-tab activation
  if (pane === 'config' && name === 'companies') { loadMetricSchemaTable(); }
  if (pane === 'config' && name === 'metricskeywords') { loadMetricSchemaTable(); _populateExMetricSelect(); }
  if (pane === 'config' && name === 'settings') { loadSettings(); loadMetricRules(); loadOllamaSettings(); loadLlamaServerSettings(); }
  if (pane === 'config' && name === 'dbpurge') { loadManagementInventory(); }
  if (pane === 'review' && name === 'miner' && !window._minerDataBooted) { window._minerDataBooted = true; boot(); }
  if (pane === 'interpret' && name === 'extract') { loadOllamaModels(); loadPromptPreview(); pePopulateMetrics(); onSourceDocsChange(); }
  if (pane === 'interpret' && name === 'qc') _loadQCTable();
}

// Tab button click — event delegation wired once (Anti-pattern #25)
document.addEventListener('DOMContentLoaded', function() {
  document.querySelector('.ops-tabs').addEventListener('click', function(e) {
    const btn = e.target.closest('.ops-tab');
    if (!btn) return;
    activateTab(btn.getAttribute('data-pane'));
  });

  // Read initial tab from URL params (also handles redirects from /coverage, /operations)
  const params = new URLSearchParams(window.location.search);
  const tab = params.get('tab') || 'workflow';
  // Pre-apply state filter from redirect (e.g. /review → /ops?tab=data&state=review_pending)
  const stateParam = params.get('state');
  if (stateParam) {
    const sel = document.getElementById('ex-state');
    if (sel) sel.value = stateParam;
  }
  activateTab(tab);

  makeSortable('companies-table');
  makeSortable('scrape-table');
  makeSortable('registry-table');
  makeSortable('review-table');
  loadKeywordDictionaryOptions();
  onPurgeInput();
});
