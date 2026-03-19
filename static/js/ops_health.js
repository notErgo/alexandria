// Health pane behavior extracted from templates/ops.html.
let _healthLoaded = false;

function loadHealthTab(force) {
  if (_healthLoaded && !force) return;
  _healthLoaded = true;
  const tbody = document.getElementById('health-ticker-tbody');
  if (!tbody) return;
  tbody.innerHTML = '<tr><td colspan="6" style="padding:0.5rem;color:var(--theme-text-muted)">Loading companies…</td></tr>';
  fetch('/api/companies?active_only=false')
    .then(function(r) { return r.json(); })
    .then(function(body) {
      if (!body.success || !body.data) { tbody.innerHTML = '<tr><td colspan="6">Failed to load companies</td></tr>'; return; }
      tbody.innerHTML = '';
      body.data.forEach(function(co) {
        const tr = document.createElement('tr');
        tr.setAttribute('data-ticker', co.ticker);
        tr.style.borderBottom = '1px solid var(--theme-border)';
        tr.innerHTML = '<td style="padding:0.3rem 0.5rem;font-weight:600">' + escapeHtml(co.ticker) + '</td>'
          + '<td id="hc-outliers-' + co.ticker + '" style="text-align:center;padding:0.3rem 0.5rem" class="qc-flag-ok">--</td>'
          + '<td id="hc-gap-' + co.ticker + '" style="text-align:center;padding:0.3rem 0.5rem" class="qc-flag-ok">--</td>'
          + '<td id="hc-queue-' + co.ticker + '" style="text-align:center;padding:0.3rem 0.5rem" class="qc-flag-ok">--</td>'
          + '<td id="hc-backlog-' + co.ticker + '" style="text-align:center;padding:0.3rem 0.5rem" class="qc-flag-ok">--</td>'
          + '<td style="text-align:center;padding:0.3rem 0.5rem">'
          + '<button class="btn btn-xs btn-secondary" onclick="runHealthCheck(\'' + co.ticker + '\')">Run</button>'
          + ' <button class="btn btn-xs btn-secondary" onclick="resetOrphanedReports(\'' + co.ticker + '\')" title="Reset orphaned running reports to pending">Reset</button>'
          + '</td>';
        tbody.appendChild(tr);
      });
      const msg = document.getElementById('health-status-msg');
      if (msg) msg.textContent = body.data.length + ' companies loaded';
    })
    .catch(function(e) {
      tbody.innerHTML = '<tr><td colspan="6" class="qc-flag-err">Error: ' + escapeHtml(String(e)) + '</td></tr>';
    });
}

function runHealthCheck(ticker) {
  const cells = ['outliers', 'gap', 'queue', 'backlog'];
  cells.forEach(function(k) {
    const el = document.getElementById('hc-' + k + '-' + ticker);
    if (el) { el.textContent = '…'; el.className = 'qc-flag-ok'; }
  });
  document.querySelectorAll('.health-detail-for-' + ticker).forEach(function(r) { r.remove(); });

  fetch('/api/qc/ticker_report?ticker=' + encodeURIComponent(ticker))
    .then(function(r) { return r.json(); })
    .then(function(body) {
      if (!body.success) {
        cells.forEach(function(k) {
          const el = document.getElementById('hc-' + k + '-' + ticker);
          if (el) { el.textContent = 'err'; el.className = 'qc-flag-err'; }
        });
        return;
      }
      const c = body.data.checks;
      const outlierEl = document.getElementById('hc-outliers-' + ticker);
      if (outlierEl) {
        outlierEl.textContent = c.outliers.length;
        outlierEl.className = c.outliers.length > 0 ? 'qc-flag-warn' : 'qc-flag-ok';
        outlierEl.style.cursor = 'pointer';
        outlierEl.title = 'Click for details';
      }
      const gapEl = document.getElementById('hc-gap-' + ticker);
      if (gapEl) {
        const pct = (c.coverage_gaps.gap_ratio * 100).toFixed(1) + '%';
        gapEl.textContent = pct;
        gapEl.className = c.coverage_gaps.gap_ratio > 0.1 ? 'qc-flag-warn' : 'qc-flag-ok';
      }
      const queueEl = document.getElementById('hc-queue-' + ticker);
      if (queueEl) {
        queueEl.textContent = c.stuck_queue.llm_empty_count;
        queueEl.className = c.stuck_queue.flagged ? 'qc-flag-err' : 'qc-flag-ok';
      }
      const backlogEl = document.getElementById('hc-backlog-' + ticker);
      if (backlogEl) {
        const total = (c.extraction_backlog.pending || 0) + (c.extraction_backlog.orphaned_running || 0);
        backlogEl.textContent = total;
        backlogEl.className = c.extraction_backlog.orphaned_running > 0 ? 'qc-flag-warn' : 'qc-flag-ok';
      }
      _insertHealthDetailRow(ticker, body.data);
    })
    .catch(function() {
      cells.forEach(function(k) {
        const el = document.getElementById('hc-' + k + '-' + ticker);
        if (el) { el.textContent = 'err'; el.className = 'qc-flag-err'; }
      });
    });
}

function _insertHealthDetailRow(ticker, healthCard) {
  const tbody = document.getElementById('health-ticker-tbody');
  const tickerRow = tbody.querySelector('[data-ticker="' + ticker + '"]');
  if (!tickerRow) return;

  const c = healthCard.checks;
  const detailTr = document.createElement('tr');
  detailTr.className = 'health-detail-row health-detail-for-' + ticker;

  let html = '<td colspan="6">';
  if (c.outliers.length > 0) {
    html += '<strong>Outliers:</strong><ul style="margin:0.2rem 0 0.4rem 1.2rem;padding:0">';
    c.outliers.forEach(function(o) {
      html += '<li>' + escapeHtml(o.period) + ' ' + escapeHtml(o.metric)
        + ' = ' + o.value + ' (trailing avg ' + o.trailing_avg + ', dev ' + (o.deviation_pct * 100).toFixed(0) + '%)</li>';
    });
    html += '</ul>';
  }
  if (c.coverage_gaps.missing_periods && c.coverage_gaps.missing_periods.length > 0) {
    const shown = c.coverage_gaps.missing_periods.slice(0, 12);
    const extra = c.coverage_gaps.missing_periods.length - shown.length;
    html += '<strong>Missing periods (' + c.coverage_gaps.missing_periods.length + '):</strong> '
      + shown.map(escapeHtml).join(', ') + (extra > 0 ? ' + ' + extra + ' more' : '') + '<br>';
  }
  html += '<button class="btn btn-xs btn-secondary" style="margin-top:0.4rem" '
    + 'onclick="_toggleHealthHistory(\'' + ticker + '\', this)">Past Runs</button>';
  html += '</td>';
  detailTr.innerHTML = html;

  tickerRow.parentNode.insertBefore(detailTr, tickerRow.nextSibling);
}

function _toggleHealthHistory(ticker, btn) {
  const existing = document.querySelector('.health-history-section-' + ticker);
  if (existing) { existing.remove(); btn.textContent = 'Past Runs'; return; }
  btn.textContent = 'Loading…';
  fetch('/api/qc/ticker_history?ticker=' + encodeURIComponent(ticker) + '&limit=10')
    .then(function(r) { return r.json(); })
    .then(function(body) {
      btn.textContent = 'Hide Past Runs';
      if (!body.success || !body.data.length) {
        btn.insertAdjacentHTML('afterend', '<span class="health-history-section-' + ticker + '" style="font-size:0.75rem;margin-left:0.5rem;color:var(--theme-text-muted)"> No history yet</span>');
        return;
      }
      let h = '<div class="health-history-section-' + ticker + '" style="margin-top:0.5rem">'
        + '<table style="width:100%;font-size:0.75rem;border-collapse:collapse">'
        + '<thead><tr style="border-bottom:1px solid var(--theme-border)">'
        + '<th style="text-align:left;padding:0.2rem 0.4rem">Generated</th>'
        + '<th style="text-align:center;padding:0.2rem 0.4rem">Trigger</th>'
        + '<th style="text-align:center;padding:0.2rem 0.4rem">Outliers</th>'
        + '<th style="text-align:center;padding:0.2rem 0.4rem">Gap %</th>'
        + '<th style="text-align:center;padding:0.2rem 0.4rem">Stuck</th>'
        + '</tr></thead><tbody>';
      body.data.forEach(function(row) {
        const chk = row.checks || {};
        const outlierCount = (chk.outliers || []).length;
        const gapRatio = (chk.coverage_gaps || {}).gap_ratio || 0;
        const stuck = (chk.stuck_queue || {}).flagged ? 'yes' : 'no';
        h += '<tr class="health-history-row">'
          + '<td style="padding:0.2rem 0.4rem">' + escapeHtml(row.generated_at || '') + '</td>'
          + '<td style="text-align:center;padding:0.2rem 0.4rem">' + escapeHtml(row.trigger || '') + '</td>'
          + '<td style="text-align:center;padding:0.2rem 0.4rem">' + outlierCount + '</td>'
          + '<td style="text-align:center;padding:0.2rem 0.4rem">' + (gapRatio * 100).toFixed(1) + '%</td>'
          + '<td style="text-align:center;padding:0.2rem 0.4rem">' + stuck + '</td>'
          + '</tr>';
      });
      h += '</tbody></table></div>';
      btn.insertAdjacentHTML('afterend', h);
    })
    .catch(function() { btn.textContent = 'Past Runs'; });
}

function resetOrphanedReports(ticker) {
  fetch('/api/qc/reset_orphaned?ticker=' + encodeURIComponent(ticker), { method: 'POST' })
    .then(function(r) { return r.json(); })
    .then(function(body) {
      const msg = document.getElementById('health-status-msg');
      if (msg) msg.textContent = ticker + ': reset ' + (body.data ? body.data.reset_count : 0) + ' orphaned report(s)';
    })
    .catch(function() {});
}
