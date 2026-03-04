/**
 * Coverage Dashboard JS
 *
 * Anti-pattern #26: all dynamic data carried via data-* attributes,
 * read in a single delegated event listener.
 * Anti-pattern #27: event listeners wired once in DOMContentLoaded.
 */

// Map from API state string → CSS class
const STATE_CLASSES = {
  'accepted':                   'cell-accepted',
  'extracted_in_review':        'cell-in-review',
  'ingested_pending_extraction': 'cell-pending-extract',
  'pending_ingest':             'cell-pending-ingest',
  'legacy_undated':             'cell-legacy',
  'no_source':                  'cell-no-source',
};

const STATE_LABELS = {
  'accepted':                   'Clean data',
  'extracted_in_review':        'Awaiting review',
  'ingested_pending_extraction': 'Not yet analyzed',
  'pending_ingest':             'Found, not downloaded',
  'legacy_undated':             'Undated file',
  'no_source':                  'No file found',
};

// ── renderSummaryStrip ──────────────────────────────────────────────────────

function renderSummaryStrip(data) {
  document.getElementById('stat-reports').textContent = escapeHtml(data.total_reports);
  document.getElementById('stat-extracted').textContent = escapeHtml(data.extracted);
  document.getElementById('stat-dp').textContent = escapeHtml(data.accepted_data_points);
  document.getElementById('stat-review').textContent = escapeHtml(data.pending_review);
  document.getElementById('stat-manifest').textContent = escapeHtml(data.manifest_total);
  document.getElementById('stat-zero').textContent = escapeHtml(data.companies_with_zero_data);
}

// ── renderCoverageGrid ──────────────────────────────────────────────────────

function renderCoverageGrid(grid) {
  const container = document.getElementById('coverage-grid');
  container.innerHTML = '';

  // Collect tickers and periods (excluding 'summary')
  const tickers = Object.keys(grid).filter(k => k !== 'summary').sort();
  if (tickers.length === 0) {
    container.innerHTML = '<p style="color:var(--theme-text-secondary)">No companies found.</p>';
    return;
  }

  // Collect periods from first ticker
  const firstTicker = tickers[0];
  const periods = Object.keys(grid[firstTicker] || {}).sort();

  const table = document.createElement('table');
  table.className = 'coverage-grid-table';

  // Header row: ticker col + period cols
  const thead = document.createElement('thead');
  const headerRow = document.createElement('tr');
  const thEmpty = document.createElement('th');
  thEmpty.className = 'ticker-col';
  headerRow.appendChild(thEmpty);
  periods.forEach(function(period) {
    const th = document.createElement('th');
    th.className = 'period-col';
    // Show YYYY-MM only
    th.textContent = period.slice(0, 7);
    headerRow.appendChild(th);
  });
  thead.appendChild(headerRow);
  table.appendChild(thead);

  // Body rows: one per ticker
  const tbody = document.createElement('tbody');
  tickers.forEach(function(ticker) {
    const tr = document.createElement('tr');
    const tdTicker = document.createElement('td');
    tdTicker.className = 'ticker-col';
    tdTicker.textContent = ticker;
    tr.appendChild(tdTicker);

    periods.forEach(function(period) {
      const cell = (grid[ticker] || {})[period] || {};
      const state = cell.state || 'no_source';
      const cssClass = STATE_CLASSES[state] || 'cell-no-source';

      const td = document.createElement('td');
      const dot = document.createElement('div');
      dot.className = 'coverage-cell ' + cssClass;
      // Store ticker + period in data-* attributes (Anti-pattern #26)
      dot.setAttribute('data-ticker', ticker);
      dot.setAttribute('data-period', period);
      dot.setAttribute('data-state', state);
      dot.setAttribute('title', ticker + ' ' + period.slice(0, 7) + ': ' + (STATE_LABELS[state] || state));
      td.appendChild(dot);
      tr.appendChild(td);
    });
    tbody.appendChild(tr);
  });
  table.appendChild(tbody);
  container.appendChild(table);
}

// ── loadCellDetail ──────────────────────────────────────────────────────────

function loadCellDetail(ticker, period, state) {
  const panel = document.getElementById('cell-detail-panel');
  const title = document.getElementById('cell-detail-title');
  const content = document.getElementById('cell-detail-content');

  title.textContent = escapeHtml(ticker) + ' — ' + escapeHtml(period.slice(0, 7));
  content.innerHTML = '<p style="color:var(--theme-text-secondary)">Loading...</p>';
  panel.classList.remove('hidden');

  fetch('/api/coverage/assets/' + encodeURIComponent(ticker) + '/' + encodeURIComponent(period))
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
        content.innerHTML = '<p style="color:var(--theme-danger)">' + escapeHtml(data.error && data.error.message || 'Error') + '</p>';
        return;
      }
      const cell = data.data;
      let html = '';

      // Manifest entries
      if (cell.manifest && cell.manifest.length > 0) {
        html += '<div style="margin-bottom:8px"><strong>Manifest (' + cell.manifest.length + ')</strong></div>';
        cell.manifest.forEach(function(m) {
          html += '<div style="font-size:0.8rem;margin-bottom:4px;color:var(--theme-text-secondary)">'
            + escapeHtml(m.filename) + ' — ' + escapeHtml(m.ingest_state)
            + '</div>';
        });
      } else {
        html += '<p style="color:var(--theme-text-muted);font-size:0.85rem">No manifest entries.</p>';
      }

      // Reports
      if (cell.reports && cell.reports.length > 0) {
        html += '<div style="margin-bottom:4px;margin-top:8px"><strong>Reports (' + cell.reports.length + ')</strong></div>';
        cell.reports.forEach(function(r) {
          html += '<div style="font-size:0.8rem;margin-bottom:4px;color:var(--theme-text-secondary)">'
            + escapeHtml(r.source_type) + ' — '
            + (r.extracted_at ? 'extracted' : 'pending extraction')
            + '</div>';
        });
      } else {
        html += '<p style="color:var(--theme-text-muted);font-size:0.85rem">No reports for this period.</p>';
      }

      // Action links
      var actionHtml = '';
      if (state === 'extracted_in_review') {
        var reviewUrl = '/review?status=PENDING&ticker=' + encodeURIComponent(ticker)
          + '&period=' + encodeURIComponent(period.slice(0, 7));
        actionHtml += '<div style="margin-top:10px">'
          + '<a href="' + reviewUrl + '" class="btn btn-primary btn-sm">'
          + 'View pending review items for ' + escapeHtml(ticker) + ' ' + escapeHtml(period.slice(0, 7)) + ' →'
          + '</a></div>';
      } else if (state === 'ingested_pending_extraction') {
        actionHtml += '<div style="margin-top:10px">'
          + '<a href="/operations" class="btn btn-secondary btn-sm">Run analysis in Operations →</a>'
          + '</div>';
      }
      content.innerHTML = html + actionHtml;
    })
    .catch(function(err) {
      content.innerHTML = '<p style="color:var(--theme-danger)">' + escapeHtml(String(err)) + '</p>';
    });
}

// ── DOMContentLoaded — wire everything once ─────────────────────────────────

document.addEventListener('DOMContentLoaded', function() {
  // Load summary
  fetch('/api/coverage/summary')
    .then(function(resp) {
      if (!resp.ok) throw new Error('HTTP ' + resp.status);
      return resp.json();
    })
    .then(function(data) {
      if (data.success) renderSummaryStrip(data.data);
    })
    .catch(function(err) {
      console.error('Failed to load coverage summary:', err);
    });

  // Load grid
  fetch('/api/coverage/grid?months=36')
    .then(function(resp) {
      if (!resp.ok) throw new Error('HTTP ' + resp.status);
      return resp.json();
    })
    .then(function(data) {
      document.getElementById('coverage-grid-loading').classList.add('hidden');
      if (data.success) {
        renderCoverageGrid(data.data.grid);
      } else {
        document.getElementById('coverage-grid').innerHTML =
          '<p style="color:var(--theme-danger)">Failed to load grid.</p>';
      }
    })
    .catch(function(err) {
      document.getElementById('coverage-grid-loading').textContent = 'Error loading grid: ' + escapeHtml(String(err));
    });

  // Delegated click on coverage grid — single listener (Anti-pattern #27)
  document.getElementById('coverage-grid').addEventListener('click', function(e) {
    const dot = e.target.closest('[data-ticker]');
    if (!dot) return;
    const ticker = dot.getAttribute('data-ticker');
    const period = dot.getAttribute('data-period');
    const state = dot.getAttribute('data-state');
    if (ticker && period) {
      loadCellDetail(ticker, period, state);
    }
  });

  // Close detail panel
  document.getElementById('btn-close-detail').addEventListener('click', function() {
    document.getElementById('cell-detail-panel').classList.add('hidden');
  });
});
