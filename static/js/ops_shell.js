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

// ── Tab routing ─────────────────────────────────────────────────────────────
const VALID_TABS = ['workflow', 'config', 'ingest', 'interpret', 'review', 'data', 'interrogate', 'health'];
const TAB_ALIASES = {
  companies: 'config', rules: 'config', settings: 'config', research: 'ingest',
  registry: 'data', explorer: 'data', guide: 'workflow',
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

  if (name === 'data') { if (!_registryLoaded) loadRegistry(); deInitCompanySelect(); deInitMetricSelect(); }
  if (name === 'ingest') { loadIngest(); loadPipelineObservability(); loadCrawlOllamaModels(); }
  if (name === 'health') { loadHealthTab(false); }
  if (name === 'interpret') { loadOllamaModels(); loadPromptPreview(); pePopulateMetrics(); onSourceDocsChange(); _loadGapMetrics(); loadInterpretMetricToggles(); if (_companiesLoaded) { _renderInterpretTickerBar(); _renderGapTickerBar(); } else loadCompanies().then(function() { _renderInterpretTickerBar(); _renderGapTickerBar(); }); }
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
  activateTab(tab);

  makeSortable('companies-table');
  makeSortable('scrape-table');
  makeSortable('registry-table');
  makeSortable('review-table');
  loadKeywordDictionaryOptions();
  onPurgeInput();
});
