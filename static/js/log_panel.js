/**
 * LogPanel — shared extraction log display widget.
 *
 * Renders structured log lines into a .acq-log container, applies
 * warning/error colour classes, and polls a progress endpoint for
 * incremental lines emitted by the backend pipeline.
 *
 * Usage:
 *   const panel = new LogPanel('extract-log', { maxLines: 30, storageKey: 'logpanel_extract' });
 *   panel.startPolling(taskId, '/api/operations/interpret/' + taskId + '/progress', {
 *     onProgress(data) { statusEl.textContent = ...; },
 *     onComplete(data) { showToast('Done'); },
 *     onError(data)    { showToast('Failed', true); },
 *     onFetchError(err){ showToast('Network error', true); },
 *   });
 *   panel.restore(callbacks);  // call on page load to replay logs from last session
 *   panel.stopPolling();
 *   panel.clear();             // also clears persisted storageKey entry
 */
class LogPanel {
  constructor(containerId, options) {
    this._id = containerId;
    this._maxLines = (options && options.maxLines) ? options.maxLines : 30;
    this._timer = null;
    this._logCount = 0;
    this._pollInterval = (options && options.pollInterval) ? options.pollInterval : 1000;
    this._storageKey = (options && options.storageKey) ? options.storageKey : null;
  }

  get el() {
    return document.getElementById(this._id);
  }

  /** Prepend a log line, applying a colour class based on level or content. */
  append(line, level) {
    const el = this.el;
    if (!el) return;
    el.style.display = '';
    const row = document.createElement('div');
    let cls = 'acq-log-row';
    const resolved = level || (
      /\bERROR\b/.test(line) ? 'error' :
      /\bWARN(ING)?\b/.test(line) ? 'warn' : null
    );
    if (resolved === 'error') cls += ' log-row-error';
    else if (resolved === 'warn') cls += ' acq-log-warn';
    row.className = cls;
    row.textContent = line;
    el.prepend(row);
    while (el.children.length > this._maxLines) {
      el.removeChild(el.lastChild);
    }
  }

  /** Clear the panel, reset the polling cursor, and remove any persisted state. */
  clear() {
    const el = this.el;
    if (el) {
      el.innerHTML = '';
      el.style.display = 'none';
    }
    this._logCount = 0;
    if (this._storageKey) {
      try { localStorage.removeItem(this._storageKey); } catch(e) {}
    }
  }

  /**
   * Start polling pollUrl every 1 s for incremental log lines.
   *
   * callbacks:
   *   onProgress(data)   — called on every poll while running (includes counters)
   *   onComplete(data)   — called once when status === 'complete'
   *   onError(data)      — called once when status === 'error'
   *   onFetchError(err)  — called if the HTTP request itself fails
   */
  startPolling(taskId, pollUrl, callbacks) {
    this.stopPolling();
    this._logCount = 0;
    if (this._storageKey) {
      try { localStorage.setItem(this._storageKey, JSON.stringify({taskId: taskId, pollUrl: pollUrl})); } catch(e) {}
    }
    const cb = callbacks || {};
    this._timer = setInterval(async () => {
      try {
        const resp = await fetch(pollUrl);
        if (!resp.ok) throw new Error('HTTP ' + resp.status);
        const body = await resp.json();
        if (!body.success) throw new Error((body.error && body.error.message) || 'progress failed');
        const p = body.data || {};
        const state = p.status || 'running';

        // Render any new log lines incrementally.
        const logs = p.logs || [];
        if (logs.length > this._logCount) {
          const newLines = logs.slice(this._logCount);
          newLines.forEach(line => this.append(line));
          this._logCount = logs.length;
        }

        if (cb.onProgress) cb.onProgress(p);

        if (state === 'running') return;

        // Terminal state reached.
        this.stopPolling();
        if (state === 'complete') {
          if (cb.onComplete) cb.onComplete(p);
        } else {
          if (cb.onError) cb.onError(p);
        }
      } catch (err) {
        this.stopPolling();
        if (cb.onFetchError) cb.onFetchError(err);
      }
    }, this._pollInterval);
  }

  /**
   * Restore from a previous session by re-polling the saved task.
   * If the task is still running, polling continues normally.
   * If the task is already terminal, one poll renders all logs and stops.
   * Silently does nothing if no storageKey is set or no saved state exists.
   */
  restore(callbacks) {
    if (!this._storageKey) return;
    let saved;
    try { saved = JSON.parse(localStorage.getItem(this._storageKey) || 'null'); } catch(e) { return; }
    if (!saved || !saved.taskId || !saved.pollUrl) return;
    this.startPolling(saved.taskId, saved.pollUrl, callbacks || {});
  }

  stopPolling() {
    if (this._timer !== null) {
      clearInterval(this._timer);
      this._timer = null;
    }
  }
}
