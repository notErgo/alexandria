// Workflow pane behavior extracted from templates/ops.html.
let _workflowLoaded = false;

async function loadWorkflow() {
  _workflowLoaded = true;
  const container = document.getElementById('workflow-stages');
  if (!container) return;
  try {
    const [workflowResp, uiSpecResp, operationsResp] = await Promise.all([
      fetch('/static/data/workflow_spec.json'),
      fetch('/static/data/ui_spec.json'),
      fetch('/docs/architecture/operations.json'),
    ]);
    if (!workflowResp.ok) throw new Error('workflow_spec HTTP ' + workflowResp.status);
    if (!uiSpecResp.ok) throw new Error('ui_spec HTTP ' + uiSpecResp.status);
    if (!operationsResp.ok) throw new Error('operations HTTP ' + operationsResp.status);
    const workflowSpec = await workflowResp.json();
    const uiSpec = await uiSpecResp.json();
    const operations = await operationsResp.json();
    container.innerHTML = _renderWorkflow(workflowSpec, uiSpec, operations);
    if (typeof syncSpecIds === 'function') syncSpecIds(container);
  } catch (err) {
    container.innerHTML = '<p style="color:var(--theme-danger)">Failed to load workflow metadata: ' + escapeHtml(String(err)) + '</p>';
  }
}

function _indexWorkflowOperations(operations) {
  const byComponent = {};
  ((operations && operations.operations) || []).forEach(function(op) {
    (op.components || []).forEach(function(componentId) {
      if (!byComponent[componentId]) byComponent[componentId] = [];
      byComponent[componentId].push(op);
    });
  });
  return byComponent;
}

function _uniqueStrings(items) {
  return Array.from(new Set((items || []).filter(Boolean)));
}

function _cleanWorkflowLabel(label) {
  return String(label || '')
    .replace(/\s*>\s*/g, ' ')
    .replace(/\s+sub-(tab|pane)$/i, '')
    .replace(/\s+tab$/i, '')
    .replace(/\s+card$/i, '')
    .trim();
}

function _workflowMetaLines(componentMeta, componentOps) {
  const lines = [];
  if (componentMeta && componentMeta.route) lines.push('Route: ' + componentMeta.route);

  const apiLines = _uniqueStrings(
    []
      .concat((componentMeta && componentMeta.api_endpoints) || [])
      .concat((componentOps || []).map(function(op) {
        return op && op.trigger ? op.trigger.path : '';
      }))
  );
  if (apiLines.length) lines.push('API: ' + apiLines.join(', '));
  return lines;
}

function _workflowNavButton(nav, stageTab) {
  const target = nav || {};
  if (target.route) {
    return '<a class="btn btn-xs btn-secondary wf-go-btn" href="' + escapeAttr(target.route) + '">Open</a>';
  }
  const tab = target.tab || stageTab;
  if (!tab) return '';
  const navCall = target.sub_tab
    ? 'activateTab(\'' + tab + '\');activatePipelineSubTab(\'' + tab + '\',\'' + target.sub_tab + '\')'
    : 'activateTab(\'' + tab + '\')';
  return '<button class="btn btn-xs btn-secondary wf-go-btn" onclick="' + navCall + '">Go</button>';
}

function _renderWorkflow(spec, uiSpec, operations) {
  var specById = {};
  ((uiSpec && uiSpec.components) || []).forEach(function(component) {
    specById[component.id] = component;
  });
  var opsByComponent = _indexWorkflowOperations(operations);
  var stages = spec.stages || [];
  return stages.map(function(stage, si) {
    var stageNum = si + 1;
    var steps = stage.steps || [];
    var stageMeta = specById[stage.spec_id] || {};
    var stageLabel = stage.label || _cleanWorkflowLabel(stageMeta.name) || stage.spec_id;
    var stageDesc = stageMeta.description || stage.description || '';
    var stageMetaLines = _workflowMetaLines(stageMeta, opsByComponent[stage.spec_id]);
    var stageNav = _workflowNavButton(stage.nav, stage.tab);
    var stepsHtml = steps.map(function(step) {
      var stepMeta = specById[step.spec_id] || {};
      var path = step.path || stepMeta.path || 'optional';
      var pathClass = 'wf-' + path;
      var pathLabel = {'critical': 'Critical', 'optional': 'Optional', 'later': 'Do later'}[path] || path;
      var stepLabel = step.label || _cleanWorkflowLabel(stepMeta.name) || step.spec_id;
      var stepDesc = stepMeta.description || step.description || '';
      var stepNote = step.note || '';
      var metaLines = _workflowMetaLines(stepMeta, opsByComponent[step.spec_id]);
      var navBtn = _workflowNavButton(step.nav, stage.tab);
      return '<div class="wf-step" data-spec-id="' + escapeAttr(step.spec_id) + '">'
        + '<div class="wf-step-header">'
        + '<span class="wf-path-badge ' + pathClass + '">' + pathLabel + '</span>'
        + '<span class="wf-step-label">' + escapeHtml(stepLabel) + '</span>'
        + '<span class="spec-id wf-specid"></span>'
        + navBtn
        + '</div>'
        + (stepDesc ? '<p class="wf-step-desc">' + escapeHtml(stepDesc) + '</p>' : '')
        + (metaLines.length ? '<p class="wf-step-meta">' + escapeHtml(metaLines.join(' | ')) + '</p>' : '')
        + (stepNote ? '<p class="wf-step-note">' + escapeHtml(stepNote) + '</p>' : '')
        + '</div>';
    }).join('');

    return '<div class="wf-stage" data-spec-id="' + escapeAttr(stage.spec_id) + '">'
      + '<div class="wf-stage-header">'
      + '<span class="wf-stage-num">' + stageNum + '</span>'
      + '<span class="wf-stage-label">' + escapeHtml(stageLabel) + '</span>'
      + '<span class="spec-id wf-specid"></span>'
      + stageNav
      + '</div>'
      + (stageDesc ? '<p class="wf-stage-desc">' + escapeHtml(stageDesc) + '</p>' : '')
      + (stageMetaLines.length ? '<p class="wf-step-meta">' + escapeHtml(stageMetaLines.join(' | ')) + '</p>' : '')
      + '<div class="wf-steps">' + stepsHtml + '</div>'
      + '</div>';
  }).join('<div class="wf-connector"></div>');
}
