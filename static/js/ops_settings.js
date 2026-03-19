// Settings and prompt-editor behavior extracted from templates/ops.html.
const _SETTINGS_GROUPS = [
  {
    label: 'Extraction',
    keys: [
      {key: 'confidence_review_threshold', label: 'Confidence review threshold', type: 'number',
       desc: 'Minimum confidence score (0–1) for a regex extraction to be auto-accepted into data_points. Extractions scoring below this threshold are routed to the review queue instead.'},
      {key: 'agreement_threshold_default', label: 'Agreement threshold (default)', type: 'number',
       desc: 'Maximum relative difference (as a fraction) allowed between LLM and regex values before the result is flagged as a disagreement and sent to review. Example: 0.02 = 2%.'},
      {key: 'outlier_min_history', label: 'Outlier min history', type: 'number',
       desc: 'Minimum number of prior accepted data points required before an outlier check is applied to a new value. Prevents false positives when history is sparse.'},
      {key: 'context_char_budget', label: 'Context char budget (monthly)', type: 'number',
       desc: 'Maximum characters of raw document text passed to the LLM per monthly extraction call. Larger budgets improve recall on long press releases but increase token usage.'},
      {key: 'context_char_budget_quarterly', label: 'Context char budget (quarterly)', type: 'number',
       desc: 'Maximum characters passed to the LLM for quarterly and annual report extraction. Annual filings are much larger, so this is typically set higher than the monthly budget.'},
      {key: 'context_max_windows', label: 'Context max windows', type: 'number',
       desc: 'Maximum number of context window slices (date-range chunks) the pipeline will try per extraction call before giving up. Each additional window retries with a different document segment.'},
      {key: 'context_fallback_confidence', label: 'Context fallback confidence', type: 'number',
       desc: 'Confidence score assigned to values extracted from fallback context windows (non-primary slices). Should be lower than the primary confidence to signal reduced certainty.'},
      {key: 'extract_num_ctx', label: 'Ollama context window — extraction (tokens)', type: 'number',
       desc: 'Context window size (num_ctx) passed to Ollama for batch extraction calls. Extraction prompts are typically 3–4k tokens so 8,192 is sufficient. Increase only for very long annual filings.'},
    ],
  },
  {
    label: 'LLM',
    keys: [
      {key: 'ollama_model', label: 'Ollama model', type: 'model_select',
       desc: 'Ollama model ID used for extraction and crawl. Must be pulled and available locally (e.g. qwen2.5:32b). Run "ollama list" to see installed models.'},
      {key: 'llm_timeout_seconds', label: 'LLM timeout (seconds)', type: 'number',
       desc: 'Seconds before an Ollama API call is abandoned. Increase for large models or slow hardware. Default is 300s (5 min).'},
      {key: 'llm_batch_preamble', label: 'Monthly batch preamble', type: 'textarea',
       desc: 'System prompt preamble injected before monthly extraction prompts. Overrides the compiled-in default. Leave blank to use the built-in default.'},
      {key: 'llm_quarterly_batch_preamble', label: 'Quarterly batch preamble', type: 'textarea',
       desc: 'System prompt preamble for quarterly report extraction. Overrides the compiled-in default.'},
      {key: 'llm_annual_batch_preamble', label: 'Annual batch preamble', type: 'textarea',
       desc: 'System prompt preamble for annual report (10-K/20-F) extraction. Overrides the compiled-in default.'},
    ],
  },
  {
    label: 'Crawl',
    keys: [
      {key: 'crawl_max_iterations', label: 'Max iterations per ticker', type: 'number',
       desc: 'Maximum number of tool-call rounds the LLM crawler runs per ticker before stopping. Each round may fetch a page, run a search, or store a document.'},
      {key: 'crawl_max_fetch_chars', label: 'Max fetch chars per page', type: 'number',
       desc: 'Maximum characters of page content returned to the LLM per fetch call. Limits context growth; longer pages are truncated. Default is 12,000 chars.'},
      {key: 'crawl_num_ctx', label: 'Ollama context window (tokens)', type: 'number',
       desc: 'Context window size (num_ctx) passed to Ollama for crawl calls. Larger values allow longer multi-turn conversations but require more VRAM. Default is 32,768. Reduce if you see OOM crashes.'},
      {key: 'bitcoin_mining_keywords', label: 'Bitcoin mining keywords (legacy)', type: 'text',
       desc: 'Legacy crawl setting. LLM extraction gates now use only active metric keywords from metric_schema.keywords.'},
    ],
  },
  {
    label: 'Pipeline',
    keys: [
      {key: 'pipeline_output_dir', label: 'Scout output directory', type: 'text',
       desc: 'Filesystem path where pipeline progress files and scout output are written. Must be writable by the web server process.'},
    ],
  },
];

let _settingsLoaded = false;
let _settingsDefaults = {};
let _settingsValues = {};

async function loadSettings() {
  if (_settingsLoaded) return;
  _settingsLoaded = true;
  try {
    const [valResp, defResp] = await Promise.all([
      fetch('/api/config'),
      fetch('/api/config/defaults'),
    ]);
    if (valResp.ok) {
      const data = await valResp.json();
      (Array.isArray(data.data) ? data.data : []).forEach(function(row) {
        _settingsValues[row.key] = row.value;
      });
    }
    if (defResp.ok) {
      const data = await defResp.json();
      Object.assign(_settingsDefaults, data.data || {});
    }
  } catch (_e) {}
  _renderSettingsGroups();
}

function peAutoResize(ta) {
  if (!ta) return;
  ta.style.height = 'auto';
  ta.style.height = (ta.scrollHeight + 2) + 'px';
  const cc = document.getElementById('pe-char-count');
  if (cc) cc.textContent = ta.value.length + ' chars';
}

function _preambleConfigKey(metric) {
  if (metric === '_preamble_monthly') return 'llm_batch_preamble';
  if (metric === '_preamble_quarterly') return 'llm_quarterly_batch_preamble';
  if (metric === '_preamble_annual') return 'llm_annual_batch_preamble';
  return '';
}

async function pePopulateMetrics() {
  const sel = document.getElementById('pe-metric-select');
  if (!sel || sel.options.length > 1) return;
  try {
    const resp = await fetch('/api/metric_schema');
    const data = await resp.json();
    const metrics = (data.data || data).map ? (data.data || data) : [];
    sel.innerHTML = '<option value="">Select metric…</option>';

    const preambleGroup = document.createElement('optgroup');
    preambleGroup.label = 'Batch Preambles';
    [
      {value: '_preamble_monthly', text: 'Monthly batch preamble'},
      {value: '_preamble_quarterly', text: 'Quarterly batch preamble (10-Q/10-K)'},
      {value: '_preamble_annual', text: 'Annual batch preamble (10-K/20-F)'},
    ].forEach(function(p) {
      const opt = document.createElement('option');
      opt.value = p.value;
      opt.textContent = p.text;
      preambleGroup.appendChild(opt);
    });
    sel.appendChild(preambleGroup);

    const groups = {};
    metrics.forEach(function(m) {
      const grp = m.metric_group || 'other';
      if (!groups[grp]) groups[grp] = [];
      groups[grp].push(m);
    });
    const groupOrder = ['production', 'holdings', 'sales', 'operations', 'ai_hpc', 'financial', 'other'];
    const sortedGroups = [...new Set([...groupOrder, ...Object.keys(groups)])].filter(function(g) { return groups[g]; });
    for (const grp of sortedGroups) {
      const og = document.createElement('optgroup');
      og.label = grp;
      groups[grp].forEach(function(m) {
        const opt = document.createElement('option');
        opt.value = m.key || m;
        const activeIndicator = m.active === 0 ? ' [inactive]' : '';
        opt.textContent = (m.label || m.key || m) + ' (' + (m.key || m) + ')' + activeIndicator;
        if (m.active === 0) opt.style.color = 'var(--theme-text-muted)';
        og.appendChild(opt);
      });
      sel.appendChild(og);
    }
  } catch (err) {
    sel.innerHTML = '<option value="">Failed to load metrics</option>';
  }
}

async function peLoadPrompt() {
  const sel = document.getElementById('pe-metric-select');
  const ta = document.getElementById('pe-prompt-text');
  const sourceLabel = document.getElementById('pe-source-label');
  const updatedAt = document.getElementById('pe-updated-at');
  const saveBtn = document.getElementById('pe-save-btn');
  const resetBtn = document.getElementById('pe-reset-btn');
  const msg = document.getElementById('pe-save-msg');

  if (!sel || !ta) return;
  const metric = sel.value;
  if (!metric) {
    ta.value = '';
    ta.placeholder = 'Select a metric to load its prompt…';
    sourceLabel.style.display = 'none';
    updatedAt.textContent = '';
    saveBtn.disabled = true;
    resetBtn.disabled = true;
    return;
  }

  msg.textContent = '';

  if (metric.startsWith('_preamble_')) {
    const configKey = _preambleConfigKey(metric);
    ta.value = 'Loading…';
    saveBtn.disabled = true;
    resetBtn.disabled = true;
    try {
      const defResp = await fetch('/api/config/' + encodeURIComponent(configKey) + '/default');
      const defData = await defResp.json();
      const defaultText = defData.data?.default || '';
      const currentVal = _settingsValues[configKey];
      if (currentVal) {
        ta.value = currentVal;
        sourceLabel.textContent = 'DB override';
        sourceLabel.style.display = 'inline-block';
        updatedAt.textContent = '';
        resetBtn.disabled = false;
      } else {
        ta.value = defaultText;
        sourceLabel.textContent = 'compiled-in default';
        sourceLabel.style.display = 'inline-block';
        updatedAt.textContent = '(no DB override — editing will create one)';
        resetBtn.disabled = true;
      }
      saveBtn.disabled = false;
      peAutoResize(ta);
    } catch (err) {
      ta.value = 'Failed to load: ' + String(err);
    }
    return;
  }

  ta.value = 'Loading…';
  saveBtn.disabled = true;
  resetBtn.disabled = true;

  try {
    const resp = await fetch('/api/llm_prompts/' + encodeURIComponent(metric));
    const data = await resp.json();
    if (!resp.ok || !data.success) {
      ta.value = 'Error loading prompt: ' + (data.error?.message || resp.status);
      return;
    }
    const dbRow = data.data?.prompt;
    const defaultText = data.data?.default_prompt || '';

    if (dbRow && dbRow.prompt_text) {
      ta.value = dbRow.prompt_text;
      sourceLabel.textContent = 'DB override';
      sourceLabel.style.cssText = 'font-size:0.72rem;padding:2px 7px;border-radius:3px;display:inline-block;background:rgba(59,130,246,0.15);color:#60a5fa';
      const ts = dbRow.updated_at ? ' — saved ' + dbRow.updated_at.slice(0, 16) : '';
      updatedAt.textContent = ts;
      resetBtn.disabled = false;
    } else {
      ta.value = defaultText;
      sourceLabel.textContent = 'hardcoded default';
      sourceLabel.style.cssText = 'font-size:0.72rem;padding:2px 7px;border-radius:3px;display:inline-block;background:var(--theme-bg-tertiary);color:var(--theme-text-muted)';
      updatedAt.textContent = '(no DB override — editing will create one)';
      resetBtn.disabled = true;
    }
    saveBtn.disabled = false;
    peAutoResize(ta);
  } catch (err) {
    ta.value = 'Failed to load: ' + String(err);
  }
}

async function peSavePrompt() {
  const sel = document.getElementById('pe-metric-select');
  const ta = document.getElementById('pe-prompt-text');
  const msg = document.getElementById('pe-save-msg');
  const saveBtn = document.getElementById('pe-save-btn');

  const metric = sel?.value;
  const text = ta?.value?.trim();
  if (!metric || !text) { msg.textContent = 'Select a metric and enter prompt text.'; return; }

  if (metric.startsWith('_preamble_')) {
    const configKey = _preambleConfigKey(metric);
    msg.textContent = 'Saving…';
    saveBtn.disabled = true;
    try {
      const resp = await fetch('/api/config/' + encodeURIComponent(configKey), {
        method: 'POST',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify({value: text}),
      });
      const data = await resp.json();
      if (resp.ok && (data.success !== false)) {
        _settingsValues[configKey] = text;
        msg.textContent = 'Saved. Takes effect on next extraction run.';
        await peLoadPrompt();
      } else {
        msg.textContent = 'Error: ' + (data.error?.message || resp.status);
      }
    } catch (err) {
      msg.textContent = 'Failed: ' + String(err);
    } finally {
      saveBtn.disabled = false;
    }
    return;
  }

  msg.textContent = 'Saving…';
  saveBtn.disabled = true;
  try {
    const resp = await fetch('/api/llm_prompts/' + encodeURIComponent(metric), {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({prompt_text: text}),
    });
    const data = await resp.json();
    if (resp.ok && data.success) {
      msg.textContent = 'Saved. Will take effect on next extraction run.';
      await peLoadPrompt();
    } else {
      msg.textContent = 'Error: ' + (data.error?.message || resp.status);
    }
  } catch (err) {
    msg.textContent = 'Failed: ' + String(err);
  } finally {
    saveBtn.disabled = false;
  }
}

async function peResetPrompt() {
  const sel = document.getElementById('pe-metric-select');
  const msg = document.getElementById('pe-save-msg');
  const metric = sel?.value;
  if (!metric) return;

  if (metric.startsWith('_preamble_')) {
    if (!confirm('Reset preamble to compiled-in default?')) return;
    const configKey = _preambleConfigKey(metric);
    msg.textContent = 'Resetting…';
    try {
      const resp = await fetch('/api/config/' + encodeURIComponent(configKey), {
        method: 'POST',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify({value: ''}),
      });
      const data = await resp.json();
      if (resp.ok && (data.success !== false)) {
        delete _settingsValues[configKey];
        await peLoadPrompt();
      } else {
        msg.textContent = 'Error: ' + (data.error?.message || resp.status);
      }
    } catch (err) {
      msg.textContent = 'Failed: ' + String(err);
    }
    return;
  }

  if (!confirm('Remove DB override for "' + metric + '"? The hardcoded default will be used on the next extraction run.')) return;

  msg.textContent = 'Resetting…';
  try {
    const resp = await fetch('/api/llm_prompts/' + encodeURIComponent(metric), {method: 'DELETE'});
    const data = await resp.json();
    if (resp.ok && data.success) {
      msg.textContent = 'DB override removed. Hardcoded default will be used.';
      await peLoadPrompt();
    } else {
      msg.textContent = 'Error: ' + (data.error?.message || resp.status);
    }
  } catch (err) {
    msg.textContent = 'Failed: ' + String(err);
  }
}

async function loadPromptPreview() {
  const ta = document.getElementById('prompt-preview-text');
  const tickerSel = document.getElementById('prompt-preview-ticker');
  if (!ta) return;

  if (tickerSel && tickerSel.options.length === 1 && _companies.length) {
    _companies.filter(function(c) { return c.active !== false; }).forEach(function(c) {
      const opt = document.createElement('option');
      opt.value = c.ticker;
      opt.textContent = c.ticker;
      tickerSel.appendChild(opt);
    });
  }

  const ticker = tickerSel?.value || '';
  const periodType = document.getElementById('prompt-preview-period-type')?.value || 'monthly';
  let url = '/api/llm_prompts/preview';
  const params = [];
  if (ticker) params.push('ticker=' + encodeURIComponent(ticker));
  if (periodType && periodType !== 'monthly') params.push('period_type=' + encodeURIComponent(periodType));
  if (params.length) url += '?' + params.join('&');
  ta.value = 'Loading...';
  try {
    const resp = await fetch(url);
    const data = await resp.json();
    if (resp.ok && data.success) {
      ta.value = data.data.prompt;
    } else {
      ta.value = 'Error: ' + (data.error?.message || resp.status);
    }
  } catch (err) {
    ta.value = 'Failed to load preview: ' + String(err);
  }
}

async function _settingsSaveGroup(groupLabel) {
  const group = _SETTINGS_GROUPS.find(function(g) { return g.label === groupLabel; });
  if (!group) return;
  const saves = [];
  for (const keyEntry of group.keys) {
    const key = keyEntry.key;
    const el = document.getElementById('setting-' + key);
    if (!el) continue;
    const val = el.value.trim();
    if (val === '') continue;
    saves.push(fetch('/api/config/' + encodeURIComponent(key), {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({value: val}),
    }));
  }
  try {
    await Promise.all(saves);
    document.getElementById('settings-msg-' + groupLabel).textContent = 'Saved.';
    setTimeout(function() {
      const el = document.getElementById('settings-msg-' + groupLabel);
      if (el) el.textContent = '';
    }, 2000);
  } catch (err) {
    document.getElementById('settings-msg-' + groupLabel).textContent = 'Error: ' + String(err);
  }
}

function _renderSettingsGroups() {
  const container = document.getElementById('settings-groups');
  if (!container) return;
  container.innerHTML = '';
  _SETTINGS_GROUPS.forEach(function(group) {
    const section = document.createElement('div');
    section.style.cssText = 'margin-bottom:1.5rem';
    const header = document.createElement('div');
    header.style.cssText = 'display:flex;gap:0.5rem;align-items:center;margin-bottom:0.5rem';
    const title = document.createElement('h3');
    title.style.cssText = 'font-size:0.85rem;margin:0';
    title.textContent = group.label;
    const saveBtn = document.createElement('button');
    saveBtn.className = 'btn btn-sm btn-primary';
    saveBtn.textContent = 'Save ' + group.label;
    saveBtn.onclick = function() { _settingsSaveGroup(group.label); };
    const msg = document.createElement('span');
    msg.id = 'settings-msg-' + group.label;
    msg.style.cssText = 'font-size:0.8rem;color:var(--theme-text-muted)';
    header.appendChild(title);
    header.appendChild(saveBtn);
    header.appendChild(msg);
    section.appendChild(header);

    group.keys.forEach(function(item) {
      const row = document.createElement('div');
      row.style.cssText = 'display:flex;gap:0.5rem;align-items:flex-start;margin-bottom:0.4rem';
      const lblWrap = document.createElement('div');
      lblWrap.style.cssText = 'width:240px;flex-shrink:0;display:flex;align-items:center;gap:0.3rem;padding-top:0.2rem';
      const lbl = document.createElement('label');
      lbl.htmlFor = 'setting-' + item.key;
      lbl.style.cssText = 'font-size:0.8rem';
      lbl.textContent = item.label;
      lblWrap.appendChild(lbl);
      if (item.desc) {
        const bubble = document.createElement('span');
        bubble.className = 'info-bubble';
        bubble.setAttribute('data-tip', item.desc);
        bubble.setAttribute('tabindex', '0');
        bubble.textContent = '?';
        lblWrap.appendChild(bubble);
      }
      let input;
      if (item.type === 'textarea') {
        input = document.createElement('textarea');
        input.rows = 4;
        input.style.cssText = 'flex:1;font-family:monospace;font-size:0.72rem;padding:0.2rem;border:1px solid var(--theme-border);border-radius:4px;background:var(--theme-surface);color:var(--theme-text)';
        input.id = 'setting-' + item.key;
        input.value = _settingsValues[item.key] || '';
        const defaultVal0 = _settingsDefaults[item.key];
        input.placeholder = defaultVal0 != null ? String(defaultVal0) : 'default';
      } else if (item.type === 'model_select') {
        input = document.createElement('select');
        input.style.cssText = 'width:220px;font-size:0.8rem;padding:0.2rem 0.4rem;border:1px solid var(--theme-border);border-radius:4px;background:var(--theme-surface);color:var(--theme-text)';
        input.id = 'setting-' + item.key;
        input.innerHTML = '<option value="">Loading models…</option>';
        populateOllamaModelSelect('setting-' + item.key);
      } else {
        input = document.createElement('input');
        input.type = item.type;
        input.style.cssText = 'width:160px;font-size:0.8rem;padding:0.2rem 0.4rem;border:1px solid var(--theme-border);border-radius:4px;background:var(--theme-surface);color:var(--theme-text)';
        input.id = 'setting-' + item.key;
        input.value = _settingsValues[item.key] || '';
        const defaultVal = _settingsDefaults[item.key];
        input.placeholder = defaultVal != null ? String(defaultVal) : 'default';
      }
      row.appendChild(lblWrap);
      row.appendChild(input);
      section.appendChild(row);
    });
    container.appendChild(section);
  });
}
