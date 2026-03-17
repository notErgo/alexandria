/**
 * doc_panel.js — Shared document viewer panel component.
 *
 * Provides a sliding document panel that renders raw document text with
 * highlighted source snippets for matched extraction candidates.
 *
 * Public API:
 *   DocPanel.init(panelId)            — wire the panel once on DOMContentLoaded
 *   DocPanel.open(title, rawText, matches) — display raw text with highlights
 *   DocPanel.close()                  — hide the panel
 *   DocPanel.buildHighlightedSource(rawText, matches) → HTML string
 *
 * Matches must be objects with: { metric, source_snippet, confidence, tier? }
 * The caller supplies METRIC_COLORS via DocPanel.setColors(map).
 */

const DocPanel = (function () {
  'use strict';

  // Metric highlight colours — defaults; caller may override via setColors().
  let _colors = {
    production_btc:   '#3b82f6',
    hodl_btc:         '#8b5cf6',
    sold_btc:         '#f59e0b',
    hashrate_eh:      '#10b981',
    realization_rate: '#f97316',
  };

  let _panelEl  = null;
  let _titleEl  = null;
  let _sourceEl = null;

  // ── Public ────────────────────────────────────────────────────────────────

  /**
   * Wire the panel element. Must be called once after DOM ready.
   * @param {string} panelId  id of the outer panel container element
   */
  function init(panelId) {
    _panelEl  = document.getElementById(panelId);
    if (!_panelEl) return;
    _titleEl  = _panelEl.querySelector('.doc-panel-title');
    _sourceEl = _panelEl.querySelector('.doc-source-view');

    // Close on Escape key
    document.addEventListener('keydown', function (e) {
      if (e.key === 'Escape') close();
    });
  }

  /**
   * Override the default metric colour map.
   * @param {Object} colorMap  e.g. { production_btc: '#3b82f6', ... }
   */
  function setColors(colorMap) {
    _colors = Object.assign({}, _colors, colorMap);
  }

  /**
   * Open the panel with the given document text and highlight matches.
   * @param {string} title    Displayed in the panel header
   * @param {string} rawText  Raw document text (plain text, not HTML)
   * @param {Array}  matches  Array of match objects with source_snippet fields
   */
  function open(title, rawText, matches) {
    if (!_panelEl) return;
    if (_titleEl) _titleEl.textContent = title || '—';
    if (_sourceEl) {
      if (rawText) {
        _sourceEl.innerHTML = buildHighlightedSource(rawText, matches || []);
        _sourceEl.style.display = '';
        var firstHl = _sourceEl.querySelector('.doc-hl');
        if (firstHl) firstHl.scrollIntoView({ behavior: 'smooth', block: 'center' });
      } else {
        _sourceEl.innerHTML = '<em style="color:var(--theme-text-muted)">No document text available.</em>';
        _sourceEl.style.display = '';
      }
    }
    _panelEl.classList.add('visible');
  }

  /** Hide the panel. */
  function close() {
    if (_panelEl) _panelEl.classList.remove('visible');
  }

  /**
   * Build an HTML string from rawText with coloured <span> wrappers around
   * source_snippet matches.  escapeHtml() (from base.html) makes tags render
   * as visible text while the highlight spans are real markup.
   *
   * @param  {string} rawText
   * @param  {Array}  matches  Each entry: { metric, source_snippet, confidence }
   * @return {string}  Safe HTML string suitable for innerHTML assignment.
   */
  function buildHighlightedSource(rawText, matches) {
    var regions = [];
    for (var j = 0; j < matches.length; j++) {
      var m = matches[j];
      if (!m.source_snippet) continue;
      var needle = m.source_snippet.replace(/\s+/g, ' ').trim().slice(0, 60);
      if (needle.length < 6) continue;
      var idx = rawText.toLowerCase().indexOf(needle.toLowerCase());
      if (idx < 0) continue;
      regions.push({
        start: idx,
        end:   idx + needle.length,
        color: _colors[m.metric] || '#9ca3af',
        label: m.metric_label || m.metric,
      });
    }
    regions.sort(function (a, b) { return a.start - b.start; });

    var html = '';
    var pos  = 0;
    for (var i = 0; i < regions.length; i++) {
      var r = regions[i];
      if (r.start < pos) continue; // skip overlapping
      html += escapeHtml(rawText.slice(pos, r.start));
      var titleAttr = escapeHtml(r.label);
      html += '<span class="doc-hl" style="background:' + r.color
        + '40;border-bottom:2px solid ' + r.color + '" title="' + titleAttr + '">'
        + escapeHtml(rawText.slice(r.start, r.end)) + '</span>';
      pos = r.end;
    }
    html += escapeHtml(rawText.slice(pos));
    return html;
  }

  // ── Expose ────────────────────────────────────────────────────────────────
  return { init, setColors, open, close, buildHighlightedSource };
})();
