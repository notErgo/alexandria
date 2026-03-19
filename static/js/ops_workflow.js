// Workflow pane behavior extracted from templates/ops.html.
let _workflowLoaded = false;

async function loadWorkflow() {
  _workflowLoaded = true;
  const container = document.getElementById('workflow-stages');
  if (!container) return;
  try {
    const resp = await fetch('/static/data/workflow_spec.json');
    if (!resp.ok) throw new Error('HTTP ' + resp.status);
    const spec = await resp.json();
    container.innerHTML = _renderWorkflow(spec);
  } catch (err) {
    container.innerHTML = '<p style="color:var(--theme-danger)">Failed to load workflow_spec.json: ' + escapeHtml(String(err)) + '</p>';
  }
}

function _renderWorkflow(spec) {
  var stages = spec.stages || [];
  return stages.map(function(stage, si) {
    var stageNum = si + 1;
    var steps = stage.steps || [];
    var stepsHtml = steps.map(function(step) {
      var pathClass = 'wf-' + (step.path || 'optional');
      var pathLabel = {'critical': 'Critical', 'optional': 'Optional', 'later': 'Do later'}[step.path] || step.path;
      var navBtn = '';
      if (step.nav && step.nav.tab) {
        var navCall = step.nav.sub_tab
          ? 'activateTab(\'' + step.nav.tab + '\');activatePipelineSubTab(\'' + step.nav.tab + '\',\'' + step.nav.sub_tab + '\')'
          : 'activateTab(\'' + step.nav.tab + '\')';
        navBtn = '<button class="btn btn-xs btn-secondary wf-go-btn" onclick="' + navCall + '">Go</button>';
      }
      return '<div class="wf-step">'
        + '<div class="wf-step-header">'
        + '<span class="wf-path-badge ' + pathClass + '">' + pathLabel + '</span>'
        + '<span class="wf-step-label">' + escapeHtml(step.label) + '</span>'
        + '<span class="spec-id wf-specid">' + escapeHtml(step.spec_id) + '</span>'
        + navBtn
        + '</div>'
        + '<p class="wf-step-desc">' + escapeHtml(step.description) + '</p>'
        + (step.note ? '<p class="wf-step-note">' + escapeHtml(step.note) + '</p>' : '')
        + '</div>';
    }).join('');

    return '<div class="wf-stage">'
      + '<div class="wf-stage-header">'
      + '<span class="wf-stage-num">' + stageNum + '</span>'
      + '<span class="wf-stage-label">' + escapeHtml(stage.label) + '</span>'
      + '<span class="spec-id wf-specid">' + escapeHtml(stage.spec_id) + '</span>'
      + (stage.tab ? '<button class="btn btn-xs btn-secondary wf-go-btn" onclick="activateTab(\'' + stage.tab + '\')">Open tab</button>' : '')
      + '</div>'
      + '<p class="wf-stage-desc">' + escapeHtml(stage.description) + '</p>'
      + '<div class="wf-steps">' + stepsHtml + '</div>'
      + '</div>';
  }).join('<div class="wf-connector"></div>');
}
