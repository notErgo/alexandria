/**
 * Operations Panel JS
 *
 * Anti-pattern #26: all dynamic values carried via data-* attributes.
 * Anti-pattern #27: event listeners wired once in DOMContentLoaded.
 */

// ── renderPendingExtractionTable ───────────────────────────────────────────

function renderPendingExtractionTable(pendingExtraction) {
  const loading = document.getElementById('pending-extraction-loading');
  const table = document.getElementById('pending-extraction-table');
  const tbody = document.getElementById('pending-extraction-tbody');
  const empty = document.getElementById('pending-extraction-empty');

  const tickers = Object.keys(pendingExtraction);
  loading.classList.add('hidden');

  if (tickers.length === 0) {
    table.style.display = 'none';
    empty.classList.remove('hidden');
    return;
  }

  table.style.display = '';
  empty.classList.add('hidden');
  tbody.innerHTML = '';

  tickers.forEach(function(ticker) {
    const info = pendingExtraction[ticker];
    const tr = document.createElement('tr');
    tr.innerHTML =
      '<td>' + escapeHtml(ticker) + '</td>' +
      '<td>' + escapeHtml(info.count) + '</td>' +
      '<td>' + escapeHtml(info.earliest || '—') + '</td>' +
      '<td>' + escapeHtml(info.latest || '—') + '</td>' +
      '<td><button class="btn btn-primary btn-sm" data-ticker="' + escapeAttr(ticker) + '" data-action="extract">Run Analysis</button></td>';
    tbody.appendChild(tr);
  });
}

// ── triggerExtraction ──────────────────────────────────────────────────────

function triggerExtraction(ticker, btn) {
  btn.disabled = true;
  btn.textContent = 'Starting analysis...';

  fetch('/api/operations/extract', {
    method: 'POST',
    headers: {'Content-Type': 'application/json'},
    body: JSON.stringify({ticker: ticker}),
  })
    .then(function(resp) {
      if (!resp.ok) {
        return resp.json().catch(function() { return {}; }).then(function(err) {
          throw new Error((err.error && err.error.message) || ('HTTP ' + resp.status));
        });
      }
      return resp.json();
    })
    .then(function(data) {
      if (!data.success) {
        btn.textContent = 'Error: ' + escapeHtml(data.error && data.error.message || 'Failed');
        btn.disabled = false;
        return;
      }
      const taskId = data.data.task_id;
      btn.textContent = 'Analyzing...';
      // Poll progress
      var pollInterval = setInterval(function() {
        fetch('/api/operations/extract/' + encodeURIComponent(taskId) + '/progress')
          .then(function(r) { return r.json(); })
          .then(function(prog) {
            if (!prog.success) {
              clearInterval(pollInterval);
              btn.textContent = 'Error';
              btn.disabled = false;
              return;
            }
            const p = prog.data;
            if (p.status === 'complete') {
              clearInterval(pollInterval);
              btn.textContent = 'Done — ' + p.data_points + ' data points extracted';
              btn.disabled = false;
              showToast('Extraction complete for ' + escapeHtml(ticker), false);
            } else if (p.status === 'error') {
              clearInterval(pollInterval);
              btn.textContent = 'Failed';
              btn.disabled = false;
              showToast('Extraction failed for ' + escapeHtml(ticker), true);
            } else {
              const processed = p.reports_processed || 0;
              const total = p.reports_total || '?';
              btn.textContent = 'Analyzing ' + processed + ' / ' + total + ' reports';
            }
          })
          .catch(function() {
            clearInterval(pollInterval);
            btn.disabled = false;
            btn.textContent = 'Run Analysis';
          });
      }, 1000);
    })
    .catch(function(err) {
      btn.textContent = 'Error';
      btn.disabled = false;
      showToast(String(err), true);
    });
}

// ── renderLegacyFilesList ─────────────────────────────────────────────────
// Each row: filename label | period input | Auto-detect btn | Set Period btn | Preview btn
// A preview panel (id="preview-{id}") is injected after the row when Preview is clicked.

function renderLegacyFilesList(legacyFiles) {
  const loading = document.getElementById('legacy-files-loading');
  const list = document.getElementById('legacy-files-list');
  const empty = document.getElementById('legacy-files-empty');

  loading.classList.add('hidden');

  if (!legacyFiles || legacyFiles.length === 0) {
    list.innerHTML = '';
    empty.classList.remove('hidden');
    return;
  }
  empty.classList.add('hidden');

  list.innerHTML = '';
  legacyFiles.forEach(function(file) {
    const idStr = escapeAttr(String(file.id));

    // Row
    const row = document.createElement('div');
    row.className = 'legacy-file-row';
    row.setAttribute('data-manifest-id', idStr);
    row.style.cssText = 'display:flex;align-items:center;gap:8px;margin-bottom:4px;font-size:0.82rem;flex-wrap:wrap';
    row.innerHTML =
      // Filename label — clicking opens preview
      '<span class="legacy-filename" style="color:var(--theme-text-secondary);flex:1;min-width:180px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;cursor:pointer;text-decoration:underline dotted" ' +
        'data-manifest-id="' + idStr + '" data-action="preview" title="Click to preview file contents">' +
        escapeHtml(file.ticker) + ' / ' + escapeHtml(file.filename) +
      '</span>' +
      // Period input
      '<input type="text" placeholder="YYYY-MM-01" maxlength="10" ' +
        'style="width:110px;font-size:0.82rem" ' +
        'data-manifest-id="' + idStr + '">' +
      // Auto-detect button
      '<button class="btn btn-secondary btn-sm" ' +
        'data-manifest-id="' + idStr + '" data-action="detect-period">Auto-detect</button>' +
      // Set Period button
      '<button class="btn btn-primary btn-sm" ' +
        'data-manifest-id="' + idStr + '" data-action="assign-period">Set Period</button>';

    list.appendChild(row);

    // Preview panel — hidden until triggered
    const preview = document.createElement('div');
    preview.id = 'preview-' + file.id;
    preview.className = 'hidden';
    preview.style.cssText = 'margin-bottom:12px;border:1px solid var(--theme-border);border-radius:6px;overflow:hidden';
    preview.innerHTML =
      '<div style="display:flex;justify-content:space-between;align-items:center;padding:6px 10px;background:var(--theme-bg-tertiary);font-size:0.78rem;color:var(--theme-text-muted)">' +
        '<span>' + escapeHtml(file.ticker) + ' / ' + escapeHtml(file.filename) + '</span>' +
        '<button class="btn btn-secondary btn-sm" data-manifest-id="' + idStr + '" data-action="close-preview">Close</button>' +
      '</div>' +
      '<div id="preview-content-' + file.id + '" style="height:400px;overflow:hidden">' +
        '<div style="padding:16px;color:var(--theme-text-muted)">Loading preview...</div>' +
      '</div>';
    list.appendChild(preview);
  });
}

// ── openPreview ────────────────────────────────────────────────────────────

function openPreview(manifestId) {
  const panel = document.getElementById('preview-' + manifestId);
  const contentEl = document.getElementById('preview-content-' + manifestId);
  if (!panel) return;

  if (!panel.classList.contains('hidden')) {
    // Already open — close it
    panel.classList.add('hidden');
    return;
  }

  panel.classList.remove('hidden');
  contentEl.innerHTML = '<div style="padding:16px;color:var(--theme-text-muted)">Loading preview...</div>';

  fetch('/api/operations/manifest/' + encodeURIComponent(manifestId) + '/preview')
    .then(function(resp) {
      const ct = resp.headers.get('content-type') || '';
      if (!resp.ok) {
        return resp.json().catch(function() { return {}; }).then(function(err) {
          throw new Error((err.error && err.error.message) || ('HTTP ' + resp.status));
        });
      }
      // HTML file — embed in sandboxed iframe
      if (ct.includes('text/html')) {
        return resp.text().then(function(html) {
          contentEl.style.height = '500px';
          const iframe = document.createElement('iframe');
          iframe.style.cssText = 'width:100%;height:100%;border:none;display:block;background:#fff';
          iframe.sandbox = 'allow-scripts allow-same-origin';
          contentEl.innerHTML = '';
          contentEl.appendChild(iframe);
          iframe.contentDocument.open();
          iframe.contentDocument.write(html);
          iframe.contentDocument.close();
        });
      }
      // PDF/plain text — show in a scrollable pre block
      return resp.text().then(function(text) {
        contentEl.style.height = '400px';
        contentEl.style.overflow = 'auto';
        contentEl.innerHTML = '<pre style="margin:0;padding:12px;font-size:0.78rem;color:var(--theme-text-primary);white-space:pre-wrap;word-break:break-word">' +
          escapeHtml(text) + '</pre>';
      });
    })
    .catch(function(err) {
      contentEl.innerHTML = '<div style="padding:16px;color:var(--theme-danger)">' + escapeHtml(String(err)) + '</div>';
    });
}

// ── detectPeriod ───────────────────────────────────────────────────────────

function detectPeriod(manifestId, row) {
  const detectBtn = row.querySelector('[data-action="detect-period"]');
  const input = row.querySelector('input[data-manifest-id="' + CSS.escape(String(manifestId)) + '"]');
  if (!detectBtn || !input) return;

  detectBtn.disabled = true;
  detectBtn.textContent = 'Detecting...';

  fetch('/api/operations/manifest/' + encodeURIComponent(manifestId) + '/detect_period', {
    method: 'POST',
    headers: {'Content-Type': 'application/json'},
  })
    .then(function(resp) {
      if (!resp.ok) {
        return resp.json().catch(function() { return {}; }).then(function(err) {
          throw new Error((err.error && err.error.message) || ('HTTP ' + resp.status));
        });
      }
      return resp.json();
    })
    .then(function(data) {
      detectBtn.disabled = false;
      if (!data.success) {
        detectBtn.textContent = 'Auto-detect';
        showToast(escapeHtml(data.error && data.error.message || 'Detection failed'), true);
        return;
      }
      const result = data.data;
      if (!result.period) {
        detectBtn.textContent = 'Auto-detect';
        showToast('Could not determine period automatically — please enter it manually', true);
        return;
      }
      // Pre-fill the input and show the method + confidence
      input.value = result.period;
      input.style.borderColor = 'var(--theme-success)';
      const methodLabel = result.method === 'llm' ? 'LLM' : 'Rules';
      const confPct = Math.round((result.confidence || 0) * 100);
      detectBtn.textContent = methodLabel + ' → ' + result.period + ' (' + confPct + '%)';
      showToast('Detected: ' + result.period + ' via ' + methodLabel + ' (' + confPct + '% confidence)', false);
    })
    .catch(function(err) {
      detectBtn.disabled = false;
      detectBtn.textContent = 'Auto-detect';
      showToast(String(err), true);
    });
}

// ── syncArchive ───────────────────────────────────────────────────────────

function syncArchive(btn, statusEl) {
  btn.disabled = true;
  statusEl.textContent = 'Scanning archive folder...';

  fetch('/api/manifest/scan', {method: 'POST'})
    .then(function(resp) {
      if (!resp.ok) throw new Error('HTTP ' + resp.status);
      return resp.json();
    })
    .then(function(data) {
      btn.disabled = false;
      if (data.success) {
        const r = data.data;
        statusEl.textContent = r.total_found + ' files found — ' + r.newly_discovered + ' newly registered, ' + r.legacy_undated + ' need dating';
        showToast('Archive scan complete', false);
        loadQueue();
      } else {
        statusEl.textContent = 'Error: ' + escapeHtml(data.error && data.error.message || 'Failed');
        showToast('Scan failed', true);
      }
    })
    .catch(function(err) {
      btn.disabled = false;
      statusEl.textContent = 'Error: ' + escapeHtml(String(err));
      showToast(String(err), true);
    });
}

// ── loadQueue ─────────────────────────────────────────────────────────────

function loadQueue() {
  fetch('/api/operations/queue')
    .then(function(resp) {
      if (!resp.ok) throw new Error('HTTP ' + resp.status);
      return resp.json();
    })
    .then(function(data) {
      if (data.success) {
        renderPendingExtractionTable(data.data.pending_extraction || {});
        renderLegacyFilesList(data.data.legacy_files || []);
      }
    })
    .catch(function(err) {
      console.error('Failed to load operations queue:', err);
      document.getElementById('pending-extraction-loading').textContent = 'Error loading queue';
      document.getElementById('legacy-files-loading').textContent = 'Error loading legacy files';
    });
}

// ── LLM model selector ─────────────────────────────────────────────────────

function loadLlmModels() {
  const select = document.getElementById('llm-model-select');
  const sourceEl = document.getElementById('llm-model-source');

  // Fetch available models and current setting in parallel
  Promise.all([
    fetch('/api/ollama/models').then(function(r) { if (!r.ok) throw new Error('HTTP ' + r.status); return r.json(); }),
    fetch('/api/config').then(function(r) { if (!r.ok) throw new Error('HTTP ' + r.status); return r.json(); }),
  ]).then(function(results) {
    const modelsData = results[0];
    const configData = results[1];

    const models = (modelsData.success && modelsData.data.models) || [];
    const source = (modelsData.success && modelsData.data.source) || 'unknown';

    // Find active model from config_settings, fall back to server default
    let active = '';
    if (configData.success) {
      const entry = (configData.data.config || []).find(function(c) { return c.key === 'ollama_model'; });
      if (entry) active = entry.value;
    }

    select.innerHTML = '';
    if (models.length === 0) {
      const opt = document.createElement('option');
      opt.value = '';
      opt.textContent = 'No models found';
      select.appendChild(opt);
    } else {
      models.forEach(function(m) {
        const opt = document.createElement('option');
        opt.value = m.name;
        opt.textContent = m.name;
        if (m.name === active) opt.selected = true;
        select.appendChild(opt);
      });
    }

    // If active model not in list (e.g. daemon returned different set), add it
    if (active && !models.find(function(m) { return m.name === active; })) {
      const opt = document.createElement('option');
      opt.value = active;
      opt.textContent = active + ' (current)';
      opt.selected = true;
      select.insertBefore(opt, select.firstChild);
    }

    sourceEl.textContent = source === 'daemon' ? 'Source: Ollama daemon (live)' : 'Source: installed manifests on disk (daemon not running)';
  }).catch(function(err) {
    select.innerHTML = '<option value="">Error loading models</option>';
    sourceEl.textContent = String(err);
  });
}

function saveLlmModel() {
  const select = document.getElementById('llm-model-select');
  const statusEl = document.getElementById('llm-model-status');
  const btn = document.getElementById('btn-save-llm-model');
  const model = select.value;
  if (!model) return;

  btn.disabled = true;
  statusEl.textContent = 'Saving…';

  fetch('/api/config/ollama_model', {
    method: 'POST',
    headers: {'Content-Type': 'application/json'},
    body: JSON.stringify({value: model}),
  })
    .then(function(resp) {
      if (!resp.ok) throw new Error('HTTP ' + resp.status);
      return resp.json();
    })
    .then(function(data) {
      btn.disabled = false;
      if (data.success) {
        statusEl.textContent = 'Saved — active on next extraction run';
        showToast('LLM model set to ' + escapeHtml(model), false);
      } else {
        statusEl.textContent = 'Error: ' + escapeHtml(data.error && data.error.message || 'Failed');
        showToast('Save failed', true);
      }
    })
    .catch(function(err) {
      btn.disabled = false;
      statusEl.textContent = 'Error: ' + escapeHtml(String(err));
      showToast(String(err), true);
    });
}

// ── DOMContentLoaded — wire everything once ───────────────────────────────

document.addEventListener('DOMContentLoaded', function() {
  // Initial data load
  loadQueue();
  loadLlmModels();

  // Sortable table (wired once, before rows exist — safe)
  makeSortable('pending-extraction-table');

  // LLM model save button
  document.getElementById('btn-save-llm-model').addEventListener('click', saveLlmModel);

  // Sync archive button — single listener
  document.getElementById('btn-sync-archive').addEventListener('click', function() {
    const statusEl = document.getElementById('sync-status');
    syncArchive(this, statusEl);
  });

  // Delegated click on pending extraction table (Anti-pattern #27)
  document.getElementById('pending-extraction-tbody').addEventListener('click', function(e) {
    const btn = e.target.closest('[data-action="extract"]');
    if (!btn) return;
    const ticker = btn.getAttribute('data-ticker');
    if (ticker) triggerExtraction(ticker, btn);
  });

  // Delegated click on legacy files list — handles preview, detect-period, assign-period,
  // close-preview from a single listener (Anti-pattern #27).
  document.getElementById('legacy-files-list').addEventListener('click', function(e) {

    // ── Preview / close-preview ──────────────────────────────────────────
    const previewTrigger = e.target.closest('[data-action="preview"], [data-action="close-preview"]');
    if (previewTrigger) {
      const manifestId = previewTrigger.getAttribute('data-manifest-id');
      if (manifestId) openPreview(manifestId);
      return;
    }

    // ── Auto-detect ──────────────────────────────────────────────────────
    const detectBtn = e.target.closest('[data-action="detect-period"]');
    if (detectBtn) {
      const manifestId = detectBtn.getAttribute('data-manifest-id');
      const row = detectBtn.closest('.legacy-file-row');
      if (manifestId && row) detectPeriod(manifestId, row);
      return;
    }

    // ── Assign period ────────────────────────────────────────────────────
    const assignBtn = e.target.closest('[data-action="assign-period"]');
    if (!assignBtn) return;
    const manifestId = assignBtn.getAttribute('data-manifest-id');
    const row = assignBtn.closest('.legacy-file-row');
    if (!row) return;
    const input = row.querySelector('input[data-manifest-id="' + CSS.escape(manifestId) + '"]');
    if (!input) return;
    const period = input.value.trim();
    if (!period.match(/^\d{4}-\d{2}-01$/)) {
      showToast('Period must be YYYY-MM-01 format (e.g. 2021-05-01)', true);
      return;
    }
    assignBtn.disabled = true;
    fetch('/api/operations/assign_period', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({manifest_id: parseInt(manifestId, 10), period: period}),
    })
      .then(function(resp) {
        if (!resp.ok) throw new Error('HTTP ' + resp.status);
        return resp.json();
      })
      .then(function(data) {
        assignBtn.disabled = false;
        if (data.success) {
          showToast('Period assigned: ' + escapeHtml(period), false);
          loadQueue();  // refresh list
        } else {
          showToast(escapeHtml(data.error && data.error.message || 'Failed'), true);
        }
      })
      .catch(function(err) {
        assignBtn.disabled = false;
        showToast(String(err), true);
      });
  });
});
