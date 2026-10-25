const { normalizeName: displayName, keyFor, edgeKey, aggregateGraph } = GraphModel;
const canvas = document.querySelector("#graph-canvas");
const ctx = canvas.getContext("2d");
let activityTicker = 0;

const state = {
  nodes: new Map(),
  revision: 0,
  canWrite: false,
  observations: [],
  edges: new Map(),
  hiddenEdges: new Map(),
  expanded: new Set(),
  selected: null,
  devTools: false,
  expanding: new Set(),
  autoExpanding: false,
  dragging: null,
  hovered: null,
  pan: { x: 0, y: 0 },
  zoom: 1,
  lastPointer: null,
  minConfidence: 0,
  loaded: false,
  searching: false,
  saveTimer: null,
  maxProminence: 1,
  graphCache: { edges: new Map(), stats: new Map() },
  view: "graph",
  tableSort: "popularity",
  tableDirection: "desc",
  tableFilter: "",
  identityFilter: "all",
  identitySuggestions: [],
  diagnostics: null,
  rootTrace: null,
  rootTraceCache: new Map(),
  rootMetricsCache: null,
  expansionRecommendationsCache: null,
  graphAnalysisVersion: 0,
  panelTab: "selected",
  activities: [],
  activityCounter: 0,
  physicsFrame: 0,
  lastRenderAt: 0,
  renderDirty: true,
  interactingUntil: 0,
  interactionWasActive: false
};

const els = {
  form: document.querySelector("#expand-form"),
  input: document.querySelector("#entity-input"),
  focusEntity: document.querySelector("#focus-entity"),
  minConfidence: document.querySelector("#min-confidence"),
  minConfidenceValue: document.querySelector("#min-confidence-value"),
  autoBudget: document.querySelector("#auto-budget"),
  autoExpand: document.querySelector("#auto-expand"),
  reset: document.querySelector("#reset-graph"),
  fitGraph: document.querySelector("#fit-graph"),
  centerSelected: document.querySelector("#center-selected"),
  graphView: document.querySelector("#graph-view"),
  tableView: document.querySelector("#table-view"),
  workspace: document.querySelector(".workspace"),
  tableWorkspace: document.querySelector("#table-workspace"),
  tableFilter: document.querySelector("#table-filter"),
  tableBody: document.querySelector("#entity-table-body"),
  status: document.querySelector("#status"),
  nodeCount: document.querySelector("#node-count"),
  edgeCount: document.querySelector("#edge-count"),
  observationCount: document.querySelector("#observation-count"),
  expandedCount: document.querySelector("#expanded-count"),
  activitySummary: document.querySelector("#activity-summary"),
  identitySummary: document.querySelector("#identity-summary"),
  activityList: document.querySelector("#activity-list"),
  rootsOutput: document.querySelector("#roots-output"),
  nextOutput: document.querySelector("#next-output"),
  selectedDetails: document.querySelector("#selected-details"),
  dedupeReview: document.querySelector("#dedupe-review"),
  previewOrphans: document.querySelector("#preview-orphans"),
  cleanOrphans: document.querySelector("#clean-orphans"),
  dedupeOutput: document.querySelector("#dedupe-output"),
  refreshDiagnostics: document.querySelector("#refresh-diagnostics"),
  runBenchmark: document.querySelector("#run-benchmark"),
  diagnosticsOutput: document.querySelector("#diagnostics-output"),
  identityFilters: document.querySelectorAll("[data-identity-filter]"),
  panelTabs: document.querySelectorAll("[data-panel-tab]"),
  panelPages: document.querySelectorAll("[data-panel-page]"),
  promptModal: document.querySelector("#prompt-modal"),
  promptModalContent: document.querySelector("#prompt-modal-content"),
  promptModalClose: document.querySelector("#prompt-modal-close")
};

const MIN_ZOOM = 0.04;
const MAX_ZOOM = 8;
const SLOW_LLM_MS = 10000;
const VERY_SLOW_LLM_MS = 25000;

const adminParam = new URLSearchParams(window.location.search).get("admin");
if (adminParam) {
  window.localStorage.setItem("influenceMapAdminToken", adminParam);
  window.history.replaceState({}, "", window.location.pathname);
}

function adminHeaders() {
  const token = window.localStorage.getItem("influenceMapAdminToken");
  return token ? { "x-admin-token": token } : {};
}

function escapeHtml(value) {
  return String(value ?? "")
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;")
    .replace(/'/g, "&#039;");
}

function markRenderDirty() {
  state.renderDirty = true;
}

function markInteracting(duration = 180) {
  state.interactingUntil = performance.now() + duration;
  markRenderDirty();
}

function invalidateGraphAnalysis() {
  state.graphAnalysisVersion += 1;
  state.rootTrace = null;
  state.rootTraceCache.clear();
  state.rootMetricsCache = null;
  state.expansionRecommendationsCache = null;
}

function ensureNode(name) {
  const id = keyFor(name);
  const label = displayName(name);
  if (!id) return null;
  if (!state.nodes.has(id)) {
    const angle = Math.random() * Math.PI * 2;
    const radius = 80 + Math.random() * 180;
    const rect = canvas.getBoundingClientRect();
    state.nodes.set(id, {
      id,
      name: label,
      aliases: [],
      popularity: 0,
      weightedPopularity: 0,
      x: rect.width / 2 + Math.cos(angle) * radius,
      y: rect.height / 2 + Math.sin(angle) * radius,
      vx: 0,
      vy: 0
    });
  } else if (label && label !== state.nodes.get(id).name && !state.nodes.get(id).aliases.includes(label)) {
    state.nodes.get(id).aliases.push(label);
  }
  return state.nodes.get(id);
}

function recomputeEdges() {
  let cache = state.graphCache;
  if (!cache || cache.version !== state.graphAnalysisVersion || cache.nodeCount !== state.nodes.size) {
    const result = aggregateGraph(state.nodes, state.observations);
    cache = state.graphCache = { version: state.graphAnalysisVersion, nodeCount: state.nodes.size, ...result };
    state.maxProminence = 1;
    for (const { node, observations, confidenceSum, neighbors } of cache.stats.values()) {
      node.popularity = observations;
      node.weightedPopularity = confidenceSum;
      node.prominence = confidenceSum + neighbors.size * 0.7 + Math.log2(1 + observations) * 1.4;
      state.maxProminence = Math.max(state.maxProminence, node.prominence);
    }
    cacheNodeRadii();
  }
  if (cache.minConfidence !== state.minConfidence) {
    state.edges = new Map();
    state.hiddenEdges = new Map();
    for (const [key, edge] of cache.edges) {
      (edge.confidence >= state.minConfidence ? state.edges : state.hiddenEdges).set(key, edge);
    }
    cache.minConfidence = state.minConfidence;
  }
}

function nodeProminence(node) {
  return Math.max(0, node.prominence || node.weightedPopularity || 0);
}

function maxNodeProminence() {
  return Math.max(1, state.maxProminence || 0);
}

function nodeRadius(node) {
  if (Number.isFinite(node.visualRadius)) return node.visualRadius;
  const baseRadius = 10;
  const visualRange = 30;
  const score = nodeProminence(node);
  const maxScore = maxNodeProminence();
  const share = Math.log1p(score) / Math.log1p(maxScore);
  return baseRadius + Math.max(0, share) * visualRange;
}

function cacheNodeRadii() {
  const baseRadius = 10;
  const visualRange = 30;
  const maxScore = maxNodeProminence();
  for (const node of state.nodes.values()) {
    const share = Math.log1p(nodeProminence(node)) / Math.log1p(maxScore);
    node.visualRadius = baseRadius + Math.max(0, share) * visualRange;
  }
}

function prominencePercent(node) {
  return Math.round((nodeProminence(node) / maxNodeProminence()) * 100);
}

function clampZoom(zoom) {
  return Math.max(MIN_ZOOM, Math.min(MAX_ZOOM, zoom));
}

function focusNode(node, targetZoom) {
  if (!node) return false;
  const rect = canvas.getBoundingClientRect();
  const nextZoom = Math.max(state.zoom, targetZoom || 1);
  state.zoom = clampZoom(nextZoom);
  state.pan = {
    x: rect.width / 2 - node.x * state.zoom,
    y: rect.height / 2 - node.y * state.zoom
  };
  state.selected = { type: "node", id: node.id };
  node.vx = 0;
  node.vy = 0;
  setStatus(`Focused ${node.name}.`);
  scheduleSave();
  updateUi();
  markRenderDirty();
  return true;
}

function clearSelection() {
  state.selected = null;
  state.hovered = null;
  setStatus("Selection cleared.");
  updateUi();
  markRenderDirty();
}

function selectedNodeId() {
  return state.selected && state.selected.type === "node" ? state.selected.id : "";
}

function highlightedNodeIds() {
  if (state.rootTrace && state.rootTrace.nodeIds && state.rootTrace.nodeIds.size) {
    return new Set(state.rootTrace.nodeIds);
  }
  const rootId = selectedNodeId() || state.hovered || "";
  const ids = new Set();
  if (!rootId) return ids;
  ids.add(rootId);
  for (const edge of [...state.edges.values(), ...state.hiddenEdges.values()]) {
    if (edge.from.id === rootId) ids.add(edge.to.id);
    if (edge.to.id === rootId) ids.add(edge.from.id);
  }
  return ids;
}

function focusNodeId() {
  return selectedNodeId() || state.hovered || "";
}

function edgeDirectionFor(edge, rootId) {
  if (state.rootTrace && state.rootTrace.edgeKeys && state.rootTrace.edgeKeys.has(edgeKey(edge.from.name, edge.to.name))) {
    return "root";
  }
  if (!rootId) return "";
  if (edge.to.id === rootId) return "incoming";
  if (edge.from.id === rootId) return "outgoing";
  return "";
}

function fitGraph() {
  const nodes = [...state.nodes.values()];
  if (!nodes.length) return;
  const rect = canvas.getBoundingClientRect();
  let minX = Infinity;
  let maxX = -Infinity;
  let minY = Infinity;
  let maxY = -Infinity;
  for (const node of nodes) {
    const radius = nodeRadius(node) + 80;
    minX = Math.min(minX, node.x - radius);
    maxX = Math.max(maxX, node.x + radius);
    minY = Math.min(minY, node.y - radius);
    maxY = Math.max(maxY, node.y + radius);
  }
  const width = Math.max(1, maxX - minX);
  const height = Math.max(1, maxY - minY);
  const zoom = clampZoom(Math.min(rect.width / width, rect.height / height) * 0.92);
  state.zoom = zoom;
  state.pan = {
    x: rect.width / 2 - ((minX + maxX) / 2) * zoom,
    y: rect.height / 2 - ((minY + maxY) / 2) * zoom
  };
  scheduleSave();
  updateUi();
}

function isNodeInViewport(node, margin = 120) {
  const rect = canvas.getBoundingClientRect();
  const left = (0 - state.pan.x) / state.zoom - margin;
  const right = (rect.width - state.pan.x) / state.zoom + margin;
  const top = (0 - state.pan.y) / state.zoom - margin;
  const bottom = (rect.height - state.pan.y) / state.zoom + margin;
  return node.x >= left && node.x <= right && node.y >= top && node.y <= bottom;
}

function visibleNodeCount() {
  let count = 0;
  for (const node of state.nodes.values()) {
    if (isNodeInViewport(node)) count += 1;
  }
  return count;
}

function viewportBounds(margin = 140) {
  const rect = canvas.getBoundingClientRect();
  return {
    left: (0 - state.pan.x) / state.zoom - margin,
    right: (rect.width - state.pan.x) / state.zoom + margin,
    top: (0 - state.pan.y) / state.zoom - margin,
    bottom: (rect.height - state.pan.y) / state.zoom + margin
  };
}

function pointInBounds(node, bounds) {
  return node.x >= bounds.left && node.x <= bounds.right && node.y >= bounds.top && node.y <= bounds.bottom;
}

function edgeInBounds(edge, bounds) {
  const minX = Math.min(edge.from.x, edge.to.x);
  const maxX = Math.max(edge.from.x, edge.to.x);
  const minY = Math.min(edge.from.y, edge.to.y);
  const maxY = Math.max(edge.from.y, edge.to.y);
  return maxX >= bounds.left && minX <= bounds.right && maxY >= bounds.top && minY <= bounds.bottom;
}

function findNodesByQuery(query) {
  const needle = keyFor(query);
  if (!needle) return [];
  if (state.nodes.has(needle)) return [state.nodes.get(needle)];
  return [...state.nodes.values()].filter(node => [node.name, ...(node.aliases || [])].some(name => keyFor(name).includes(needle)))
    .sort((left, right) => left.name.localeCompare(right.name));
}

function findNodeByQuery(query) {
  const matches = findNodesByQuery(query);
  return matches.length === 1 ? matches[0] : null;
}

function setView(view) {
  state.view = view === "table" ? "table" : "graph";
  els.workspace.hidden = state.view !== "graph";
  els.tableWorkspace.hidden = state.view !== "table";
  els.graphView.setAttribute("aria-pressed", String(state.view === "graph"));
  els.tableView.setAttribute("aria-pressed", String(state.view === "table"));
  if (state.view === "table") renderEntityTable();
  if (state.view === "graph") resizeCanvas();
}

function setPanelTab(tab) {
  state.panelTab = tab;
  state.rootTrace = null;
  markRenderDirty();
  els.panelTabs.forEach(button => {
    const active = button.getAttribute("data-panel-tab") === tab;
    button.setAttribute("aria-pressed", String(active));
  });
  els.panelPages.forEach(page => {
    page.hidden = page.getAttribute("data-panel-page") !== tab;
  });
}

function entityStats() {
  return [...state.graphCache.stats.values()].map(({ node, incoming, outgoing, observations, neighbors }) => ({
    id: node.id, name: node.name, aliases: node.aliases || [], incoming, outgoing,
    observations, connectionCount: neighbors.size, popularity: nodeProminence(node),
    expanded: state.expanded.has(node.id)
  }));
}

let analysisWorker = null;
let workerVersion = -1;
let analysisCounter = 0;
const analysisRequests = new Map();
const analysisLongTasks = [];
let analysisObserver = null;
if (typeof PerformanceObserver !== "undefined" && PerformanceObserver.supportedEntryTypes?.includes("longtask")) {
  analysisObserver = new PerformanceObserver(list => {
    analysisLongTasks.push(...list.getEntries());
    if (analysisLongTasks.length > 100) analysisLongTasks.splice(0, analysisLongTasks.length - 100);
  });
  analysisObserver.observe({ type: "longtask" });
}

function analysisSnapshot() {
  return {
    nodes: [...state.nodes.values()].map(({ id, name }) => ({ id, name })),
    observations: state.observations.map(({ from, to, confidence, sourceEntity }) => ({ from, to, confidence, sourceEntity })),
    expanded: [...state.expanded], expanding: [...state.expanding]
  };
}

async function requestAnalysis(kind, targetId = null) {
  const started = performance.now();
  const version = state.graphAnalysisVersion;
  const key = kind + ":" + version + ":" + (targetId || "");
  if (analysisRequests.has(key)) return analysisRequests.get(key);
  if (!analysisWorker) analysisWorker = new Worker("/analysis-worker.js");
  const id = ++analysisCounter;
  const promise = new Promise((resolve, reject) => {
    const receive = ({ data }) => {
      if (data.id !== id) return;
      cleanup();
      if (data.error) reject(new Error(data.error));
      else {
        resolve(data);
        requestAnimationFrame(() => window.setTimeout(() => {
          const panel = kind === "next" ? els.nextOutput : els.rootsOutput;
          const supported = typeof PerformanceObserver !== "undefined" && PerformanceObserver.supportedEntryTypes?.includes("longtask");
          analysisLongTasks.push(...(analysisObserver?.takeRecords() || []));
          const maximum = Math.max(0, ...analysisLongTasks.filter(entry => entry.startTime + entry.duration >= started).map(entry => entry.duration));
          panel.dataset.analysisMaxTaskMs = supported ? maximum.toFixed(2) : "unsupported";
        }, 0));
      }
    };
    const fail = () => { cleanup(); reject(new Error("Graph analysis worker failed")); };
    const timeout = window.setTimeout(() => { cleanup(); reject(new Error("Graph analysis timed out")); }, 30000);
    const cleanup = () => {
      window.clearTimeout(timeout);
      analysisWorker.removeEventListener("message", receive);
      analysisWorker.removeEventListener("error", fail);
      analysisRequests.delete(key);
    };
    analysisWorker.addEventListener("message", receive);
    analysisWorker.addEventListener("error", fail);
    const submitStarted = performance.now();
    const graph = workerVersion !== version ? analysisSnapshot() : undefined;
    analysisWorker.postMessage({ id, version, kind, targetId, graph });
    const panel = kind === "next" ? els.nextOutput : els.rootsOutput;
    panel.dataset.analysisSubmitMs = (performance.now() - submitStarted).toFixed(2);
    workerVersion = version;
  });
  analysisRequests.set(key, promise);
  return promise;
}

function expansionRecommendations(limit = 12) {
  return state.expansionRecommendationsCache?.version === state.graphAnalysisVersion
    ? state.expansionRecommendationsCache.items.filter(item => !state.expanded.has(item.node.id) && !state.expanding.has(item.node.id)).slice(0, limit) : [];
}

function globalInfluenceMetrics(limit = 10) {
  return state.rootMetricsCache?.version === state.graphAnalysisVersion ? state.rootMetricsCache.scores.slice(0, limit) : [];
}

function cachedTraceRoots(id) {
  return state.rootTraceCache.get(state.graphAnalysisVersion + ":" + id) || null;
}

function ensureNextAnalysis() {
  if (state.expansionRecommendationsCache?.version === state.graphAnalysisVersion) return Promise.resolve();
  return requestAnalysis("next").then(({ version, result }) => {
    if (version !== state.graphAnalysisVersion) return;
    state.expansionRecommendationsCache = { version, items: result };
    if (state.panelTab === "next") renderNextPanel();
  });
}

function ensureRootAnalysis(id) {
  if (state.rootMetricsCache?.version === state.graphAnalysisVersion && (!id || cachedTraceRoots(id))) return;
  requestAnalysis("roots", id).then(({ version, result }) => {
    if (version !== state.graphAnalysisVersion) return;
    state.rootMetricsCache = { version, scores: result.metrics };
    if (id) state.rootTraceCache.set(version + ":" + id, result.trace);
    if (state.panelTab === "roots") renderRootsPanel();
    markRenderDirty();
  }).catch(error => { els.rootsOutput.textContent = error.message; });
}

function metricBars(item) {
  const values = [
    ["Reach", item.reachScore],
    ["Root", item.rootScore],
    ["Bridge", item.bridgeScore],
    ["Diversity", item.diversityScore]
  ];
  const max = Math.max(1, ...values.map(([, value]) => Number(value) || 0));
  return `<div class="metric-bars">${values.map(([label, value]) => `
    <span><b style="width:${Math.max(4, Math.min(100, (Number(value || 0) / max) * 100)).toFixed(0)}%"></b>${label} ${Number(value || 0).toFixed(1)}</span>
  `).join("")}</div>`;
}

function expansionValueBars(item) {
  const values = [
    ["Regions", item.regions],
    ["Samples", item.observations],
    ["Mean score", item.avgConfidence * 10],
    ["Links", item.connections]
  ];
  const max = Math.max(1, ...values.map(([, value]) => Number(value) || 0));
  return `<div class="metric-bars next-bars">${values.map(([label, value]) => `
    <span><b style="width:${Math.max(4, Math.min(100, (Number(value || 0) / max) * 100)).toFixed(0)}%"></b>${label} ${label === "Mean score" ? item.avgConfidence.toFixed(2) : Number(value || 0).toFixed(0)}</span>
  `).join("")}</div>`;
}

function renderNextPanel() {
  if (!els.nextOutput) return;
  if (state.panelTab !== "next") return;
  if (state.expansionRecommendationsCache?.version !== state.graphAnalysisVersion) {
    els.nextOutput.textContent = "Calculating expansion suggestions in the background...";
    ensureNextAnalysis().catch(error => { els.nextOutput.textContent = error.message; });
    return;
  }
  const items = expansionRecommendations(12);
  if (!items.length) {
    els.nextOutput.innerHTML = `
      <div class="roots-empty">No high-value unexpanded frontier nodes yet. Search or expand an entity to create more candidates.</div>
    `;
    return;
  }
  els.nextOutput.innerHTML = `
    <div class="roots-summary">
      <strong>Expansion suggestions</strong>
      <span>This heuristic uses graph topology and sample history. It favors nodes that may connect regions, repeat across observations, or clarify ambiguous names.</span>
    </div>
    <ol class="next-list">${items.map(item => `
      <li>
        <button type="button" data-next-node="${escapeHtml(item.node.id)}">${escapeHtml(item.node.name)}</button>
        <span>value ${item.value.toFixed(2)} - ${item.observations} observation${item.observations === 1 ? "" : "s"} - ${item.connections} connection${item.connections === 1 ? "" : "s"}</span>
        <p>${escapeHtml(item.reasons.join("; "))}</p>
        ${expansionValueBars(item)}
        <button type="button" data-next-expand="${escapeHtml(item.node.id)}" ${state.canWrite ? "" : "disabled"}>Expand this next</button>
      </li>
    `).join("")}</ol>
  `;
}

function renderRootsPanel() {
  if (!els.rootsOutput) return;
  if (state.panelTab !== "roots") {
    state.rootTrace = null;
    return;
  }
  const selectedId = selectedNodeId();
  ensureRootAnalysis(selectedId);
  const trailblazers = globalInfluenceMetrics(10);
  if (!selectedId) {
    state.rootTrace = null;
    els.rootsOutput.innerHTML = `
      <div class="roots-empty">Select a node to trace its upstream lineage. Global trailblazers blend downstream reach, root-likeness, bridge influence, and branch diversity.</div>
      <h3>Global Trailblazers</h3>
      <ol class="roots-list">${trailblazers.map(item => `
        <li><button type="button" data-root-node="${escapeHtml(item.node.id)}">${escapeHtml(item.node.name)}</button><span>trailblazer ${item.score.toFixed(2)} - reaches ${item.downstream} - ${item.upstreamCount} upstream</span>${metricBars(item)}</li>
      `).join("")}</ol>
    `;
    return;
  }
  const trace = cachedTraceRoots(selectedId);
  state.rootTrace = trace;
  if (!trace) {
    els.rootsOutput.textContent = "Calculating upstream paths in the background...";
    return;
  }
  const rootRows = trace.roots.length ? trace.roots.map(root => `
    <li>
      <button type="button" data-root-node="${escapeHtml(root.node.id)}">${escapeHtml(root.node.name)}</button>
      <span>root score ${root.score.toFixed(2)} - ${root.paths} path${root.paths === 1 ? "" : "s"} - depth ${root.maxDepth} - ${escapeHtml(root.terminalReason)}</span>
    </li>
  `).join("") : `<li><span>No upstream roots found yet. Expand influences around ${escapeHtml(trace.target.name)} to grow the lineage.</span></li>`;
  const pathRows = trace.paths.length ? trace.paths.map(path => `
    <li>
      <strong>${escapeHtml(path.root.name)}</strong>
      <span>${escapeHtml(path.path.map(edge => edge.from.name).concat(trace.target.name).join(" -> "))}</span>
    </li>
  `).join("") : "";
  els.rootsOutput.innerHTML = `
    <div class="roots-summary">
      <strong>${escapeHtml(trace.target.name)}</strong>
      <span>${trace.roots.length} candidate root${trace.roots.length === 1 ? "" : "s"} from ${trace.paths.length} heuristic upstream path${trace.paths.length === 1 ? "" : "s"}.</span>
    </div>
    <h3>Candidate upstream endpoints</h3>
    <ol class="roots-list">${rootRows}</ol>
    ${pathRows ? `<h3>Strongest Paths</h3><ol class="roots-paths">${pathRows}</ol>` : ""}
    <h3>Global Trailblazers</h3>
    <ol class="roots-list compact">${trailblazers.map(item => `
      <li><button type="button" data-root-node="${escapeHtml(item.node.id)}">${escapeHtml(item.node.name)}</button><span>${item.score.toFixed(2)} trailblazer - ${item.downstream} downstream - ${item.upstreamCount} upstream</span>${metricBars(item)}</li>
    `).join("")}</ol>
  `;
}

function renderEntityTable() {
  if (!els.tableBody) return;
  const filter = keyFor(state.tableFilter);
  const rows = entityStats()
    .filter(item => !filter || keyFor(item.name).includes(filter) || item.aliases.some(alias => keyFor(alias).includes(filter)))
    .sort((a, b) => {
      const direction = state.tableDirection === "asc" ? 1 : -1;
      if (state.tableSort === "name") return a.name.localeCompare(b.name) * direction;
      const keys = {
        connections: "connectionCount",
        incoming: "incoming",
        outgoing: "outgoing",
        observations: "observations",
        popularity: "popularity",
        expanded: "expanded"
      };
      const key = keys[state.tableSort] || "popularity";
      const av = key === "expanded" ? Number(a.expanded) : Number(a[key] || 0);
      const bv = key === "expanded" ? Number(b.expanded) : Number(b[key] || 0);
      return (av - bv) * direction || a.name.localeCompare(b.name);
    });

  els.tableBody.innerHTML = "";
  for (const item of rows) {
    const row = document.createElement("tr");
    row.innerHTML = `
      <td><button type="button" data-node="${escapeHtml(item.id)}">${escapeHtml(item.name)}</button></td>
      <td>${item.connectionCount}</td>
      <td>${item.incoming}</td>
      <td>${item.outgoing}</td>
      <td>${item.observations}</td>
      <td>${item.popularity.toFixed(2)}</td>
      <td>${item.expanded ? "Expanded" : "Frontier"}</td>
    `;
    els.tableBody.appendChild(row);
  }
}

function resizeCanvas() {
  const rect = canvas.getBoundingClientRect();
  const scale = Math.min(window.devicePixelRatio || 1, 1.35);
  canvas.width = Math.max(600, Math.floor(rect.width * scale));
  canvas.height = Math.max(420, Math.floor(rect.height * scale));
  ctx.setTransform(scale, 0, 0, scale, 0, 0);
  markRenderDirty();
}

function setStatus(message) {
  els.status.textContent = message;
}

function activityTime(value) {
  const date = new Date(value);
  return Number.isFinite(date.valueOf()) ? date.toLocaleTimeString() : "";
}

function addActivity(type, title, detail = "", status = "queued", prompt = null) {
  const activity = {
    id: String(++state.activityCounter),
    type,
    title: displayName(title),
    detail: displayName(detail),
    status,
    prompt,
    stages: [],
    startedAt: status === "running" ? Date.now() : null,
    createdAt: new Date().toISOString(),
    updatedAt: new Date().toISOString()
  };
  state.activities.unshift(activity);
  state.activities = state.activities.slice(0, 12);
  renderActivityList();
  return activity.id;
}

function updateActivity(id, patch = {}) {
  const activity = state.activities.find(item => item.id === id);
  if (!activity) return;
  if (patch.status === "running" && !activity.startedAt) patch.startedAt = Date.now();
  Object.assign(activity, patch, { updatedAt: new Date().toISOString() });
  renderActivityList();
  const hasRunning = state.activities.some(item => item.status === "running" && item.startedAt);
  if (hasRunning && !activityTicker) {
    activityTicker = window.setInterval(() => {
      if (!state.activities.some(item => item.status === "running" && item.startedAt)) {
        window.clearInterval(activityTicker);
        activityTicker = 0;
        return;
      }
      renderActivityList();
    }, 1000);
  }
  updateOverview();
}

function updateOverview() {
  if (els.activitySummary) {
    const running = state.activities.find(item => item.status === "running");
    const latest = running || state.activities[0];
    els.activitySummary.querySelector("strong").textContent = latest
      ? running
        ? `${latest.title} (${activityElapsed(latest) || "running"})`
        : latest.title
      : "Idle";
  }
  if (els.identitySummary) {
    const pending = state.identitySuggestions.length;
    const merges = state.identitySuggestions.filter(item => identityActionFor(item) === "merge").length;
    const splits = state.identitySuggestions.filter(item => identityActionFor(item) === "split").length;
    els.identitySummary.querySelector("strong").textContent = pending
      ? `${pending} pending (${merges}M/${splits}S)`
      : "0 pending";
  }
}

function activityElapsed(activity) {
  if (!activity.startedAt || activity.status !== "running") return "";
  return `${Math.max(1, Math.round((Date.now() - activity.startedAt) / 1000))}s`;
}

function formatTimings(timings) {
  if (!Array.isArray(timings) || !timings.length) return "";
  return timings
    .filter(item => item && item.stage !== "total" && Number.isFinite(Number(item.ms)))
    .map(item => `${item.stage} ${(Number(item.ms) / 1000).toFixed(1)}s`)
    .join(", ");
}

function formatMs(ms) {
  const value = Number(ms);
  if (!Number.isFinite(value)) return "n/a";
  return value >= 1000 ? `${(value / 1000).toFixed(2)}s` : `${Math.round(value)}ms`;
}

function profileTotal(profile) {
  const total = Array.isArray(profile.timings)
    ? profile.timings.find(item => item && item.stage === "total")
    : null;
  return total ? total.ms : 0;
}

function renderDiagnostics(payload = state.diagnostics) {
  if (!els.diagnosticsOutput) return;
  if (!payload) {
    els.diagnosticsOutput.textContent = "Diagnostics have not loaded yet.";
    return;
  }
  const queue = payload.llmQueue || {};
  const counts = payload.counts || {};
  const profiles = Array.isArray(payload.profiles) ? payload.profiles.slice(0, 12) : [];
  const slowThresholdMs = Number(payload.slowThresholdMs || SLOW_LLM_MS);
  const profileRows = profiles.length ? profiles.map(profile => {
    const llm = profile.llm || {};
    const usage = profile.usage && profile.usage.expansion ? profile.usage.expansion : {};
    const requestMs = Number(llm.expansionRequestMs || llm.identityRequestMs || 0);
    const totalMs = Number(profileTotal(profile));
    const isSlow = requestMs >= slowThresholdMs || totalMs >= slowThresholdMs;
    const status = profile.error ? "failed" : isSlow ? "slow" : "done";
    const slowLabel = isSlow ? "Slow provider wait - " : "";
    return `
      <div class="diagnostic-profile ${status}">
        <strong>${escapeHtml(profile.entity || profile.type || "event")}</strong>
        <span>${escapeHtml(profile.type || "profile")} - ${escapeHtml(profile.provider || "")}/${escapeHtml(profile.model || "")}</span>
        <span>${escapeHtml(slowLabel)}Total ${escapeHtml(formatMs(totalMs))} - Provider request ${escapeHtml(formatMs(requestMs))} - Queue ${escapeHtml(formatMs(llm.expansionQueueMs || llm.identityQueueMs || 0))}</span>
        <span>Tokens ${Number(usage.totalTokens || 0)} - reasoning ${Number(usage.reasoningTokens || 0)} - ${escapeHtml(activityTime(profile.at))}</span>
        ${profile.error ? `<em>${escapeHtml(profile.error)}</em>` : ""}
      </div>
    `;
  }).join("") : "<p>No profiles recorded yet.</p>";
  els.diagnosticsOutput.innerHTML = `
    <div class="diagnostic-grid">
      <div><span>Provider</span><strong>${escapeHtml(payload.provider || "mock")}</strong></div>
      <div><span>Model</span><strong>${escapeHtml(payload.model || "none")}</strong></div>
      <div><span>Thinking</span><strong>${escapeHtml(payload.thinking || "n/a")}</strong></div>
      <div><span>JSON mode</span><strong>${payload.openRouterJsonMode ? "Fast" : "Strict"}</strong></div>
      <div><span>Queue</span><strong>${Number(queue.active || 0)} active / ${Number(queue.queued || 0)} waiting</strong></div>
      <div><span>Timeout</span><strong>${escapeHtml(formatMs(queue.timeoutMs))}</strong></div>
      <div><span>Database</span><strong>${Number(counts.nodes || 0)} nodes / ${Number(counts.observations || 0)} obs</strong></div>
      <div><span>Profile errors</span><strong>${Number(counts.errors || 0)}</strong></div>
      <div><span>Slow runs</span><strong>${Number(counts.slowProfiles || 0)}</strong></div>
    </div>
    <h3>Recent Model Runs</h3>
    <div class="diagnostic-profiles">${profileRows}</div>
  `;
}

async function loadDiagnostics() {
  if (!state.devTools || !els.diagnosticsOutput) return null;
  els.diagnosticsOutput.textContent = "Loading diagnostics...";
  const response = await fetch("/api/diagnostics", { headers: adminHeaders() });
  const payload = await response.json();
  if (!response.ok) throw new Error(payload.error || "Could not load diagnostics");
  state.diagnostics = payload;
  renderDiagnostics(payload);
  return payload;
}

async function runBenchmark() {
  if (!state.devTools || !els.diagnosticsOutput) return;
  const confirmed = window.confirm("Run benchmark will make two small model requests. Continue?");
  if (!confirmed) {
    setStatus("Benchmark cancelled.");
    return;
  }
  els.diagnosticsOutput.textContent = "Running model benchmark...";
  const response = await fetch("/api/llm-benchmark", {
    method: "POST",
    headers: adminHeaders()
  });
  const payload = await response.json();
  if (!response.ok) throw new Error(payload.error || "Benchmark failed");
  const rows = (payload.results || []).map(result => `${result.mode}: ${formatMs(result.ms)} (${Number(result.usage && result.usage.totalTokens || 0)} tokens)`).join(" | ");
  setStatus(`Benchmark: ${rows}`);
  await loadDiagnostics();
}

function promptTextFor(prompt) {
  const messages = prompt && prompt.messages ? prompt.messages : null;
  if (!messages) return "";
  return messages.map(message => {
    const stage = message.stage ? ` [${message.stage}]` : "";
    return `${message.role.toUpperCase()}${stage}\n${message.content}`;
  }).join("\n\n---\n\n");
}

function formatJson(value) {
  try {
    return JSON.stringify(typeof value === "string" ? JSON.parse(value) : value, null, 2);
  } catch {
    return String(value || "");
  }
}

function parsedUserPayload(prompt, stage = "") {
  const messages = prompt && prompt.messages ? prompt.messages : [];
  const user = messages.find(message => message.role === "user" && (!stage || message.stage === stage))
    || messages.find(message => message.role === "user");
  if (!user) return null;
  try {
    return JSON.parse(user.content);
  } catch {
    return null;
  }
}

function systemLinesFor(prompt, stage = "") {
  const messages = prompt && prompt.messages ? prompt.messages : [];
  const system = messages.find(message => message.role === "system" && (!stage || message.stage === stage))
    || messages.find(message => message.role === "system");
  return system ? system.content.split(". ").filter(Boolean) : [];
}

function contextRows(items, emptyLabel) {
  if (!items || !items.length) return `<p class="prompt-muted">${escapeHtml(emptyLabel)}</p>`;
  return `<ul class="prompt-list">${items.map(item => `
    <li>
      <strong>${escapeHtml(item.sourceEntity || item.entity || "Context")}</strong>
      <span>${escapeHtml(item.relation || "")}${Number.isFinite(Number(item.confidence)) ? ` - ${Math.round(Number(item.confidence) * 100)}%` : ""}</span>
    </li>
  `).join("")}</ul>`;
}

function renderPromptStage(prompt, stage, title) {
  const payload = parsedUserPayload(prompt, stage);
  if (!payload) {
    return "";
  }

  const isIdentity = payload.localCandidates;
  const isInfluence = !isIdentity && (payload.task || payload.entity || payload.context);
  const isDedupe = payload.candidates || payload.entityNames;
  const systemLines = systemLinesFor(prompt, stage);

  return `
    <section class="prompt-card">
      <h3>${escapeHtml(title)}</h3>
    <div class="prompt-summary">
      ${isInfluence ? `
        <div><span>Task</span><strong>${escapeHtml(payload.task || "Influence expansion")}</strong></div>
        <div><span>Entity</span><strong>${escapeHtml(payload.entity || "")}</strong></div>
      ` : ""}
      ${isDedupe ? `
        <div><span>Task</span><strong>Identity review</strong></div>
        <div><span>Candidate groups</span><strong>${Array.isArray(payload.candidates) ? payload.candidates.length : 0}</strong></div>
      ` : ""}
      ${isIdentity ? `
        <div><span>Task</span><strong>Identity check</strong></div>
        <div><span>Local candidates</span><strong>${Array.isArray(payload.localCandidates) ? payload.localCandidates.length : 0}</strong></div>
      ` : ""}
    </div>
    ${isIdentity ? `
      <section class="prompt-section">
        <h3>Possible Matches</h3>
        ${contextRows((payload.localCandidates || []).map(candidate => ({
          entity: candidate.name,
          relation: `${candidate.matchReason || "candidate"}${candidate.score ? `, ${Math.round(candidate.score * 100)}% local score` : ""}`
        })), "No local candidates were sent.")}
      </section>
    ` : ""}
    ${isInfluence ? `
      <section class="prompt-section">
        <h3>Context Used</h3>
        <h4>Discovered From</h4>
        ${contextRows(payload.context && payload.context.discoveredFrom, "No discovery context was sent.")}
        <h4>Connected To</h4>
        ${contextRows(payload.context && payload.context.connectedTo, "No nearby graph context was sent.")}
      </section>
    ` : ""}
    ${isDedupe ? `
      <section class="prompt-section">
        <h3>Identity Candidates</h3>
        <p class="prompt-muted">${Array.isArray(payload.entityNames) ? payload.entityNames.length : 0} entity names were included for context.</p>
        ${contextRows((payload.candidates || []).slice(0, 20).map(candidate => ({
          entity: (candidate.entities || []).join(", "),
          relation: candidate.reason || "Candidate group"
        })), "No candidates were sent.")}
      </section>
    ` : ""}
    <section class="prompt-section">
      <h3>System Instructions</h3>
      <ul class="prompt-rules">${systemLines.map(line => `<li>${escapeHtml(line.replace(/\.$/, ""))}</li>`).join("")}</ul>
    </section>
    </section>
  `;
}

function renderPromptView(prompt) {
  const promptText = promptTextFor(prompt);
  if (!promptText) return "";
  const stages = prompt && prompt.stages ? Object.keys(prompt.stages) : [];
  const stageHtml = stages.length
    ? stages.map(stage => renderPromptStage(
      { messages: prompt.stages[stage].messages },
      "",
      stage === "identity" ? "Identity Resolution" : stage === "hygiene" ? "Identity Hygiene" : "Influence Expansion"
    )).join("")
    : renderPromptStage(prompt, "", "Model Prompt");
  const payload = parsedUserPayload(prompt);

  return `
    ${stageHtml || `<pre class="prompt-raw">${escapeHtml(promptText)}</pre>`}
    <details class="prompt-raw-details">
      <summary>Raw prompt</summary>
      <pre class="prompt-raw">${escapeHtml(promptText)}</pre>
    </details>
    ${payload ? `<details class="prompt-raw-details">
      <summary>Parsed user payload</summary>
      <pre class="prompt-raw">${escapeHtml(formatJson(payload))}</pre>
    </details>` : ""}
  `;
}

function openPromptModal(prompt) {
  const html = renderPromptView(prompt);
  if (!html) return;
  els.promptModalContent.innerHTML = html;
  els.promptModal.hidden = false;
}

function closePromptModal() {
  els.promptModal.hidden = true;
  els.promptModalContent.innerHTML = "";
}

function renderActivityList() {
  if (!els.activityList) return;
  if (!state.activities.length) {
    els.activityList.textContent = "Nothing running.";
    return;
  }
  els.activityList.innerHTML = state.activities.map(activity => {
    const hasPrompt = Boolean(promptTextFor(activity.prompt));
    const stages = Array.isArray(activity.stages) && activity.stages.length
      ? `<ol class="activity-stages">${activity.stages.map(stage => `<li>${escapeHtml(stage)}</li>`).join("")}</ol>`
      : "";
    const elapsed = activityElapsed(activity);
    const classes = [activity.status, activity.slow ? "slow" : ""].filter(Boolean).join(" ");
    return `
    <div class="activity-item ${escapeHtml(classes)}" data-activity="${escapeHtml(activity.id)}">
      <div>
        <strong>${escapeHtml(activity.title)}</strong>
        <span>${escapeHtml(activity.detail || activity.type)}</span>
        ${stages}
        ${hasPrompt ? `<button type="button" class="prompt-button" data-prompt-activity="${escapeHtml(activity.id)}">View prompt</button>` : ""}
      </div>
      <em>${escapeHtml(activity.status)} ${escapeHtml(elapsed || activityTime(activity.updatedAt))}</em>
    </div>
  `;
  }).join("");
  updateOverview();
}

function hydrateGraph(graph) {
  const previous = new Map([...state.nodes.values()].filter(node => node.uid).map(node => [node.uid, { x: node.x, y: node.y }]));
  const selectedUid = state.selected?.type === "node" ? state.nodes.get(state.selected.id)?.uid : null;
  const view = state.loaded ? { pan: state.pan, zoom: state.zoom, minConfidence: state.minConfidence, positions: Object.fromEntries(previous) } : readLocalView();
  const minConfidence = Number(view.minConfidence ?? graph?.minConfidence);
  const zoom = Number(view.zoom ?? graph?.zoom);
  state.revision = Number(graph.revision) || state.revision;
  state.nodes.clear();
  state.observations = [];
  state.edges.clear();
  state.hiddenEdges.clear();
  state.expanded.clear();
  state.selected = null;
  state.pan = {
    x: Number(view.pan?.x ?? graph?.pan?.x) || 0,
    y: Number(view.pan?.y ?? graph?.pan?.y) || 0
  };
  state.zoom = Number.isFinite(zoom) ? clampZoom(zoom) : 1;
  state.minConfidence = Number.isFinite(minConfidence) ? Math.max(0, Math.min(1, minConfidence)) : 0;
  els.minConfidence.value = state.minConfidence;

  for (const node of Array.isArray(graph && graph.nodes) ? graph.nodes : []) {
    const hydrated = ensureNode(node.name);
    if (!hydrated) continue;
    hydrated.uid = node.uid;
    hydrated.aliases = Array.isArray(node.aliases) ? node.aliases.map(displayName).filter(Boolean) : hydrated.aliases;
    const x = Number(view.positions?.[node.uid]?.x ?? node.x);
    const y = Number(view.positions?.[node.uid]?.y ?? node.y);
    hydrated.x = Number.isFinite(x) ? x : hydrated.x;
    hydrated.y = Number.isFinite(y) ? y : hydrated.y;
    hydrated.vx = 0;
    hydrated.vy = 0;
  }

  for (const observation of Array.isArray(graph && graph.observations) ? graph.observations : []) {
    const from = displayName(observation.from);
    const to = displayName(observation.to);
    if (!from || !to || keyFor(from) === keyFor(to)) continue;
    ensureNode(from);
    ensureNode(to);
    state.observations.push({
      id: observation.id, fromUid: observation.fromUid, toUid: observation.toUid,
      evidenceType: observation.evidenceType || "unknown", sources: observation.sources || [],
      from,
      to,
      confidence: Math.max(0, Math.min(1, Number.isFinite(Number(observation.confidence)) ? Number(observation.confidence) : 0.5)),
      sourceEntity: displayName(observation.sourceEntity || ""),
      provider: displayName(observation.provider || ""),
      model: displayName(observation.model || ""),
      createdAt: observation.createdAt || ""
    });
  }

  for (const id of Array.isArray(graph && graph.expanded) ? graph.expanded : []) {
    const key = keyFor(id);
    if (key) state.expanded.add(key);
  }

  if (selectedUid) {
    const selected = [...state.nodes.values()].find(node => node.uid === selectedUid);
    if (selected) state.selected = { type: "node", id: selected.id };
  }
  invalidateGraphAnalysis();
  recomputeEdges();
  markRenderDirty();
}

async function loadGraph() {
  try {
    const response = await fetch("/api/graph");
    const payload = await response.json();
    if (!response.ok) throw new Error(payload.error || "Load failed");
    hydrateGraph(payload.data);
    state.loaded = true;
    const count = state.observations.length;
    if (count && !visibleNodeCount()) {
      fitGraph();
    }
    setStatus(count ? `Loaded saved graph with ${count} observations.` : "Ready.");
  } catch (error) {
    state.loaded = true;
    setStatus(`Could not load saved graph: ${error.message}`);
  }
  updateUi();
}

async function loadConfig() {
  try {
    const response = await fetch("/api/config", { headers: adminHeaders() });
    const config = await response.json();
    state.devTools = Boolean(config.devTools);
    state.canWrite = Boolean(config.canWrite);
    document.body.classList.toggle("dev-tools", state.devTools);
  } catch (error) {
    state.devTools = false;
    document.body.classList.remove("dev-tools");
  }
}

function readLocalView() {
  try { return JSON.parse(window.localStorage.getItem("influenceMapView") || "{}"); } catch { return {}; }
}

async function saveView() {
  if (!state.loaded) return;
  const positions = Object.fromEntries([...state.nodes.values()].filter(node => node.uid).map(node => [node.uid, { x: node.x, y: node.y }]));
  window.localStorage.setItem("influenceMapView", JSON.stringify({ pan: state.pan, zoom: state.zoom, minConfidence: state.minConfidence, positions }));
}

async function acceptServerPayload(payload) {
  const full = payload.graph || (payload.data?.nodes ? payload.data : null);
  if (full) {
    if (Number(full.revision) >= state.revision) hydrateGraph(full);
    return;
  }
  const deltas = Array.isArray(payload.changes) ? payload.changes : payload.changes ? [payload.changes] : [];
  for (const delta of deltas) {
    if (delta.revision <= state.revision) continue;
    if (delta.fromRevision !== state.revision) { await refreshGraph(); return; }
    const nodes = new Map([...state.nodes.values()].map(node => [node.uid, node]));
    for (const uid of delta.removedNodeUids) nodes.delete(uid);
    for (const node of delta.nodes) nodes.set(node.uid, node);
    const observations = new Map(state.observations.map(observation => [observation.id, observation]));
    for (const id of delta.removedObservationIds) observations.delete(id);
    for (const observation of delta.observations) observations.set(observation.id, observation);
    hydrateGraph({ nodes: [...nodes.values()], observations: [...observations.values()], expanded: delta.expanded, revision: delta.revision });
  }
}

async function refreshGraph() {
  const response = await fetch("/api/graph?since=" + state.revision);
  const payload = await response.json();
  if (!response.ok) throw new Error(payload.error || "Graph refresh failed");
  await acceptServerPayload(payload);
  updateUi();
}

function scheduleSave() {
  if (!state.loaded) return;
  window.clearTimeout(state.saveTimer);
  state.saveTimer = window.setTimeout(() => {
    saveView().catch(error => setStatus(`Could not save this browser's view: ${error.message}`));
  }, 350);
}

function expansionContextFor(node) {
  if (!node) return { discoveredFrom: [], connectedTo: [] };
  const discoveredFrom = [];
  const connectedTo = [];

  for (const obs of state.observations) {
    const fromId = keyFor(obs.from);
    const toId = keyFor(obs.to);
    if (fromId !== node.id && toId !== node.id) continue;
    const relation = `${obs.from} influenced ${obs.to}`;
    const otherName = fromId === node.id ? obs.to : obs.from;
    const sourceEntity = displayName(obs.sourceEntity || "");
    const item = {
      entity: displayName(otherName),
      relation,
      confidence: Number(obs.confidence) || 0
    };
    connectedTo.push(item);
    if (sourceEntity && keyFor(sourceEntity) !== node.id) {
      discoveredFrom.push({
        sourceEntity,
        relation,
        confidence: Number(obs.confidence) || 0
      });
    }
  }

  discoveredFrom.sort((a, b) => b.confidence - a.confidence);
  connectedTo.sort((a, b) => b.confidence - a.confidence);
  return {
    discoveredFrom: discoveredFrom.slice(0, 8),
    connectedTo: connectedTo.slice(0, 12)
  };
}

async function fetchInfluences(entity, context = {}) {
  const response = await fetch("/api/influences", {
    method: "POST",
    headers: { "content-type": "application/json", ...adminHeaders() },
    body: JSON.stringify({ entity, context, revision: state.revision, requestId: crypto.randomUUID() })
  });
  const payload = await response.json();
  if (!response.ok) { if (response.status === 409) await refreshGraph(); throw new Error(payload.error || "Request failed"); }
  return payload;
}

async function fetchInfluencePrompt(entity, context = {}) {
  if (!state.devTools) return null;
  const response = await fetch("/api/influence-prompt", {
    method: "POST",
    headers: { "content-type": "application/json", ...adminHeaders() },
    body: JSON.stringify({ entity, context })
  });
  const payload = await response.json();
  if (!response.ok) throw new Error(payload.error || "Prompt preview failed");
  return payload.debugPrompt || null;
}

function reviewExplanation(item) {
  if (item.action === "rename") return {
    title: "Rename to " + item.canonical, badge: "Rename", approveLabel: "Rename this entity",
    summary: "Change the display identity while retaining its permanent ID.",
    change: "Rename " + item.entities[0] + " to " + item.canonical + "; preserve samples and record the old name as an alias.",
    reject: "Reject if these names describe different entities."
  };
  const entities = (item.entities || []).map(displayName).filter(Boolean);
  const canonical = displayName(item.canonical || entities[0] || "");
  const source = entities.find(name => keyFor(name) !== keyFor(canonical)) || entities[0] || "";
  if (item.action === "merge") {
    return {
      title: `Merge into ${canonical}`,
      badge: "Merge",
      approveLabel: "Merge these nodes",
      summary: "These names appear to describe the same thing.",
      change: `Approving will replace the listed nodes with one node named "${canonical}". All observations and edges from the other names move onto that node.`,
      reject: "Reject if these are related but different things, such as a franchise and one game, a remake and an original, or a shared acronym."
    };
  }
  if (item.action === "split") {
    return {
      title: `Split from ${source || canonical}`,
      badge: "Split",
      approveLabel: "Move selected links",
      summary: "Some links on one ambiguous node may belong to a more specific node.",
      change: `Approving will create or use "${canonical}" and move only the listed links there. It will not merge the whole "${source || "source"}" node.`,
      reject: "Reject if those links should stay on the current node."
    };
  }
  return {
    title: `Check ${canonical}`,
    badge: "Check",
    approveLabel: "Mark reviewed",
    summary: "The model noticed a naming or identity signal, but it is not proposing a graph edit.",
    change: "Approving only marks this review as handled. It does not rename, merge, split, or move anything.",
    reject: "Reject if this signal is not useful."
  };
}

function identityActionFor(item) {
  return item.action === "merge" || item.action === "split" ? item.action : "identity";
}

function updateIdentityFilterButtons(suggestions) {
  const counts = { all: suggestions.length, merge: 0, split: 0, identity: 0 };
  for (const item of suggestions) counts[identityActionFor(item)] += 1;
  els.identityFilters.forEach(button => {
    const filter = button.getAttribute("data-identity-filter");
    button.setAttribute("aria-pressed", String(filter === state.identityFilter));
    const count = button.querySelector("span");
    if (count) count.textContent = String(counts[filter] || 0);
  });
}

function renderDedupeSuggestions(suggestions) {
  if (!els.dedupeOutput) return;
  if (!suggestions || !suggestions.length) {
    updateIdentityFilterButtons([]);
    state.identitySuggestions = [];
    updateOverview();
    els.dedupeOutput.textContent = "No pending identity decisions. Model-generated relationships still require independent verification.";
    return;
  }
  state.identitySuggestions = suggestions;
  updateOverview();
  updateIdentityFilterButtons(suggestions);
  const visible = state.identityFilter === "all"
    ? suggestions
    : suggestions.filter(item => identityActionFor(item) === state.identityFilter);
  if (!visible.length) {
    els.dedupeOutput.innerHTML = `
      <div class="identity-empty">
        No pending ${escapeHtml(state.identityFilter)} decisions. Splits only appear when the system has specific links it can safely move; checks appear when the model raises a signal without proposing a graph edit.
      </div>
    `;
    return;
  }
  els.dedupeOutput.innerHTML = visible.map(item => {
    const sourceKey = keyFor(item.entities?.find(name => keyFor(name) !== keyFor(item.canonical)));
    const moveCandidates = item.action === "split" ? state.observations.filter(observation => keyFor(observation.from) === sourceKey || keyFor(observation.to) === sourceKey) : [];
    const review = reviewExplanation(item);
    return `
    <div class="dedupe-group ${escapeHtml(item.action || "identity")}" data-suggestion="${item.id}">
      <div class="review-heading">
        <span>${escapeHtml(review.badge)}</span>
        <strong>${escapeHtml(review.title)}</strong>
      </div>
      <p class="review-summary">${escapeHtml(review.summary)}</p>
      <div class="review-transform">
        <span>Names involved</span>
        <strong>${escapeHtml((item.entities || []).join(" + "))}</strong>
        <span>Result if approved</span>
        <strong>${escapeHtml(review.change)}</strong>
      </div>
      <span>${Number(item.confidence || 0).toFixed(2)} identity score - ${escapeHtml(item.reason || "Identity decision")}</span>
      ${moveCandidates.length ? `<ul class="move-candidates">${moveCandidates.map(candidate => `
        <li>Sample #${Number(candidate.id)}: ${escapeHtml(candidate.from)} → ${escapeHtml(candidate.to)} <span>${Math.round(Number(candidate.confidence || 0) * 100)}%</span></li>
      `).join("")}</ul>` : ""}
      ${item.action === "split" ? `<label>Sample IDs to move (comma-separated)<input data-split-ids="${Number(item.id)}" value="${escapeHtml((item.metadata?.moveObservationIds || []).join(", "))}" autocomplete="off"></label>` : ""}
      <details class="dedupe-review-details">
        <summary>How to decide</summary>
        <p>${escapeHtml(review.reject)}</p>
      </details>
      <div class="dedupe-actions">
        <button type="button" data-dedupe-action="approve" data-id="${item.id}">${escapeHtml(review.approveLabel)}</button>
        <button type="button" data-dedupe-action="reject" data-id="${item.id}">Reject</button>
      </div>
    </div>
  `;
  }).join("");
}

async function loadDedupeSuggestions() {
  const response = await fetch("/api/dedupe-suggestions", { headers: adminHeaders() });
  const payload = await response.json();
  if (!response.ok) throw new Error(payload.error || "Could not load identity decisions");
  state.identitySuggestions = payload.suggestions || [];
  renderDedupeSuggestions(state.identitySuggestions);
}

async function decideDedupeSuggestion(id, action) {
  const activityId = addActivity("identity", action === "approve" ? "Approving identity decision" : "Rejecting identity decision", `Suggestion ${id}`, "running");
  const response = await fetch(`/api/dedupe-suggestions/${id}/${action}`, {
    method: "POST",
    headers: { "content-type": "application/json", ...adminHeaders() },
    body: JSON.stringify({ revision: state.revision, ...(action === "approve" && document.querySelector(`[data-split-ids="${Number(id)}"]`) ? { moveObservationIds: document.querySelector(`[data-split-ids="${Number(id)}"]`).value.split(",").map(value => value.trim()).filter(Boolean).map(Number) } : {}) })
  });
  const payload = await response.json();
  if (!response.ok) {
    if (response.status === 409) await refreshGraph();
    updateActivity(activityId, { status: "failed", detail: payload.error || "Identity decision failed" });
    throw new Error(payload.error || "Identity decision failed");
  }
  await acceptServerPayload(payload);
  state.identitySuggestions = payload.suggestions || [];
  renderDedupeSuggestions(state.identitySuggestions);
  updateUi();
  const result = payload.result || {};
  const detail = action === "approve"
    ? (result.reason || (result.changed ? "Applied to SQLite graph" : "Already resolved"))
    : "Marked rejected";
  updateActivity(activityId, { status: "done", detail });
  setStatus(action === "approve" ? detail : "Rejected identity decision.");
}

async function previewOrphanNodes() {
  const response = await fetch("/api/orphans", { headers: adminHeaders() });
  const payload = await response.json();
  if (!response.ok) throw new Error(payload.error || "Could not preview orphan nodes");
  const orphans = payload.orphans || [];
  if (els.cleanOrphans) els.cleanOrphans.disabled = !orphans.length;
  if (!orphans.length) {
    els.dedupeOutput.textContent = "No orphan nodes found.";
    setStatus("No orphan nodes found.");
    return;
  }
  els.dedupeOutput.innerHTML = `
    <div class="identity-empty">
      Found ${orphans.length} orphan node${orphans.length === 1 ? "" : "s"} with no observations. These do not affect edges, but they can clutter search and table views.
    </div>
    <ul class="orphan-list">${orphans.slice(0, 30).map(orphan => `
      <li>${escapeHtml(orphan.name)}${orphan.expanded ? " <span>expanded</span>" : ""}</li>
    `).join("")}</ul>
    ${orphans.length > 30 ? `<div class="identity-empty">Showing 30 of ${orphans.length}.</div>` : ""}
  `;
  setStatus(`Found ${orphans.length} orphan node${orphans.length === 1 ? "" : "s"}.`);
}

async function cleanOrphanNodes() {
  const preview = await fetch("/api/orphans", { headers: adminHeaders() });
  const previewPayload = await preview.json();
  if (!preview.ok) throw new Error(previewPayload.error || "Could not preview orphan nodes");
  const orphans = previewPayload.orphans || [];
  if (!orphans.length) {
    if (els.cleanOrphans) els.cleanOrphans.disabled = true;
    els.dedupeOutput.textContent = "No orphan nodes found.";
    return;
  }
  const confirmed = window.confirm(`Delete ${orphans.length} orphan node${orphans.length === 1 ? "" : "s"} with no observations? This removes local database rows.`);
  if (!confirmed) {
    setStatus("Orphan cleanup cancelled.");
    return;
  }
  const response = await fetch("/api/orphans", {
    method: "DELETE",
    headers: { "content-type": "application/json", ...adminHeaders() },
    body: JSON.stringify({ revision: state.revision })
  });
  const payload = await response.json();
  if (!response.ok) throw new Error(payload.error || "Could not clean orphan nodes");
  await acceptServerPayload(payload);
  if (els.cleanOrphans) els.cleanOrphans.disabled = true;
  els.dedupeOutput.textContent = `Removed ${payload.removed || 0} orphan node${Number(payload.removed || 0) === 1 ? "" : "s"}.`;
  setStatus(`Removed ${payload.removed || 0} orphan node${Number(payload.removed || 0) === 1 ? "" : "s"}.`);
  updateUi();
}

async function expandEntity(entity, options = {}) {
  if (!state.canWrite) throw new Error("Generation requires the private write-access link shown by the server.");
  let node = ensureNode(entity);
  if (!node) return;
  if (state.expanding.has(node.id)) return;
  if (state.expanding.size && !options.allowConcurrent) {
    setStatus("A model expansion is already running. Wait for it to finish before starting another.");
    return;
  }
  const activityId = options.activityId || addActivity("expand", `Expand ${node.name}`, "Waiting for model", "queued");
  const context = expansionContextFor(node);
  const contextCount = context.discoveredFrom.length + context.connectedTo.length;
  state.expanding.add(node.id);
  state.selected = { type: "node", id: node.id };
  updateActivity(activityId, {
    status: "running",
    title: `Expand ${node.name}`,
    detail: contextCount
      ? `Preparing prompt with ${contextCount} local context clues`
      : "Preparing model prompt",
    stages: ["Local context ready", "Waiting for model expansion"]
  });
  setStatus(`Expanding ${node.name}...`);
  updateUi();
  let slowProviderTimer = 0;
  let verySlowProviderTimer = 0;
  try {
    const wasExpanded = state.expanded.has(node.id);
    const beforeNodeCount = state.nodes.size;
    const beforeEdgeCount = state.edges.size + state.hiddenEdges.size;
    const beforeObservationCount = state.observations.length;
    fetchInfluencePrompt(node.name, context)
      .then(prompt => {
        if (prompt) updateActivity(activityId, { prompt });
      })
      .catch(error => updateActivity(activityId, { detail: `Prompt preview unavailable: ${error.message}` }));
    updateActivity(activityId, {
      detail: "Model is generating influence links...",
      stages: ["Local context ready", "Prompt preview running in parallel", "Model expansion in progress"]
    });
    slowProviderTimer = window.setTimeout(() => {
      updateActivity(activityId, {
        slow: true,
        detail: "Still waiting on the model provider. The app is alive; this is provider request time.",
        stages: ["Local context ready", "Prompt preview running in parallel", "Provider request still running"]
      });
      setStatus(`${node.name} is still waiting on the model provider...`);
      if (state.devTools) loadDiagnostics().catch(() => {});
    }, SLOW_LLM_MS);
    verySlowProviderTimer = window.setTimeout(() => {
      updateActivity(activityId, {
        slow: true,
        detail: "Provider request is unusually slow and may timeout if no response arrives.",
        stages: ["Local context ready", "Prompt preview running in parallel", "Provider request unusually slow"]
      });
      if (state.devTools) loadDiagnostics().catch(() => {});
    }, VERY_SLOW_LLM_MS);
    const payload = await fetchInfluences(node.name, context);
    window.clearTimeout(slowProviderTimer);
    window.clearTimeout(verySlowProviderTimer);
    slowProviderTimer = 0;
    verySlowProviderTimer = 0;
    await acceptServerPayload(payload);
    node = [...state.nodes.values()].find(candidate => candidate.uid === payload.entityUid) || state.nodes.get(keyFor(payload.data.entity)) || ensureNode(payload.data.entity);
    state.selected = { type: "node", id: node.id };
    if (payload.debugPrompt) updateActivity(activityId, { prompt: payload.debugPrompt });
    if (payload.identity) {
      const identityPercent = Math.round(Number(payload.identity.confidence || 0) * 100);
      updateActivity(activityId, {
        detail: `Identity ${payload.identity.decision}: ${payload.identity.canonicalName} (${identityPercent}%), applying expansion`,
        stages: ["Identity resolved", "Applying graph observations", "Queuing identity review"]
      });
    }
    const data = payload.data;
    const canonicalEntity = data.entity;
    const relationships = [...(data.influencedBy || []), ...(data.influenced || [])];
    if (state.devTools) loadDedupeSuggestions().catch(() => {});
    const addedObservations = state.observations.length - beforeObservationCount;
    const addedNodes = state.nodes.size - beforeNodeCount;
    const addedEdges = (state.edges.size + state.hiddenEdges.size) - beforeEdgeCount;
    if (!relationships.length || !addedObservations) {
      updateActivity(activityId, {
        status: "done",
        title: `No new links for ${canonicalEntity}`,
        detail: relationships.length ? "Returned links could not be added" : "Model returned no confident relationships"
      });
      setStatus(`No new relationships returned for ${canonicalEntity}. Try Sample again later or expand a connected node.`);
      return;
    }

    const shapeChanged = addedNodes > 0 || addedEdges > 0;
    const resultTitle = shapeChanged
      ? `${wasExpanded ? "Sampled" : "Expanded"} ${canonicalEntity}`
      : `Reinforced ${canonicalEntity}`;
    const resultDetail = shapeChanged
      ? `${payload.provider}/${payload.model} added ${addedObservations} observations, ${addedNodes} entities, ${addedEdges} edges`
      : `${payload.provider}/${payload.model} added ${addedObservations} observation${addedObservations === 1 ? "" : "s"} to existing links; graph shape unchanged`;
    const queuedReviews = Number(payload.identitySuggestions || 0)
      + Number(payload.localHygieneSuggestions || 0)
      + Number(payload.llmHygieneSuggestions || 0);
    const identityDetail = queuedReviews ? ` ${queuedReviews} identity review${queuedReviews === 1 ? "" : "s"} queued.` : "";
    const autoIdentityDetail = Number(payload.autoIdentityApplied || 0)
      ? ` ${Number(payload.autoIdentityApplied)} identity edit${Number(payload.autoIdentityApplied) === 1 ? "" : "s"} auto-applied.`
      : "";
    const resolvedDetail = payload.identity && payload.identity.canonicalName !== node.name
      ? ` Identity checked as ${payload.identity.canonicalName}.`
      : "";
    const timingDetail = formatTimings(payload.timings);
    const llm = payload.llm || {};
    const providerMs = Number(llm.expansionRequestMs || 0);
    const providerDetail = providerMs >= SLOW_LLM_MS ? ` Provider wait: ${formatMs(providerMs)}.` : "";
    const hygieneDetail = payload.scopedIdentityReview?.warning
      ? ` Identity review deferred: ${payload.scopedIdentityReview.warning}. Samples were saved.`
      : payload.hygieneQueued ? " Identity review completed." : "";
    updateActivity(activityId, {
      status: "done",
      title: resultTitle,
      slow: providerMs >= SLOW_LLM_MS,
      detail: `${resultDetail}${resolvedDetail}${autoIdentityDetail}${identityDetail}${hygieneDetail}${providerDetail}${timingDetail ? ` Timing: ${timingDetail}.` : ""}`,
      stages: []
    });
    setStatus(shapeChanged
      ? `${wasExpanded ? "Sampled" : "Expanded"} ${canonicalEntity} using ${payload.provider}/${payload.model}.`
      : `${canonicalEntity} returned existing relationships only; model sampling was repeated but the map shape did not grow.`);
    scheduleSave();
    window.setTimeout(scheduleSave, 2500);
    if (state.panelTab === "diagnostics") loadDiagnostics().catch(() => {});
  } catch (error) {
    updateActivity(activityId, { status: "failed", detail: error.message || "Expansion failed" });
    if (state.panelTab === "diagnostics") loadDiagnostics().catch(() => {});
    throw error;
  } finally {
    window.clearTimeout(slowProviderTimer);
    window.clearTimeout(verySlowProviderTimer);
    state.expanding.delete(node.id);
    state.expanding.delete(keyFor(entity));
    updateUi();
  }
}

async function searchEntity(entity) {
  if (state.searching) return;
  const query = displayName(entity);
  if (!query) return;
  if (state.view !== "graph") setView("graph");
  state.searching = true;
  els.focusEntity.disabled = true;
  try {
    const matches = findNodesByQuery(query);
    if (matches.length > 1) {
      state.selected = null;
      setPanelTab("selected");
      els.selectedDetails.innerHTML = "<p>Choose the intended entity, or enter a more specific name.</p>" + matches.slice(0, 12).map(node => `<button type="button" data-search-choice="${escapeHtml(node.id)}">${escapeHtml(node.name)}</button>`).join("");
      setStatus(matches.length + " existing entities match this query.");
      markRenderDirty();
      return;
    }
    const existing = matches[0];
    if (existing) {
      focusNode(existing, 1.15);
      if (state.canWrite && !state.expanded.has(existing.id)) {
        await expandEntity(existing.name);
        focusNode(state.nodes.get(state.selected?.id) || existing, 1.15);
      }
      return;
    }
    setStatus(`No existing node found for "${query}". Expanding it now...`);
    await expandEntity(query);
    const created = findNodeByQuery(query);
    if (created) focusNode(created, 1.15);
  } finally {
    state.searching = false;
    els.focusEntity.disabled = false;
  }
}

function getFrontier() {
  const recommended = expansionRecommendations(40).map(item => item.node);
  if (recommended.length) return recommended;
  const connected = new Map();
  for (const edge of state.edges.values()) {
    connected.set(edge.from.id, edge.from);
    connected.set(edge.to.id, edge.to);
  }
  return [...connected.values()]
    .filter(node => !state.expanded.has(node.id))
    .sort((a, b) => a.name.localeCompare(b.name));
}

async function autoExpandFrontier() {
  if (state.autoExpanding) {
    setStatus("Next-best expansion batch is already running.");
    return;
  }
  const budget = Math.max(1, Math.min(10, Number(els.autoBudget.value) || 1));
  els.autoBudget.value = String(budget);
  await ensureNextAnalysis();
  const frontier = getFrontier().slice(0, budget);
  if (!frontier.length) {
    setStatus("No unexpanded frontier nodes are available.");
    return;
  }
  const preview = frontier.slice(0, 6).map(node => node.name).join(", ");
  const more = frontier.length > 6 ? `, and ${frontier.length - 6} more` : "";
  const confirmed = window.confirm(`Expand next best will queue ${frontier.length} expansions (each may use several budgeted model calls) one after another:\n\n${preview}${more}\n\nContinue?`);
  if (!confirmed) {
    setStatus("Next-best expansion batch cancelled.");
    return;
  }
  state.autoExpanding = true;
  els.autoExpand.disabled = true;
  els.autoExpand.textContent = "Expanding batch...";
  const queued = frontier.map((node, index) => ({
    node,
    activityId: addActivity("expand", `Expand ${node.name}`, `Queued #${index + 1} by next-best ranking`, "queued")
  }));
  try {
    setStatus(`Queued ${queued.length} next-best expansion${queued.length === 1 ? "" : "s"}.`);
    for (let i = 0; i < queued.length; i += 1) {
      const queuedItem = queued[i];
      const next = queuedItem.node;
      if (!next) {
        setStatus("Next-best expansion batch ended early.");
        return;
      }
      setStatus(`Expanding ${i + 1}/${queued.length}: ${next.name}`);
      await expandEntity(next.name, { activityId: queuedItem && queuedItem.activityId });
    }
    setStatus(`Finished ${queued.length} next-best expansion${queued.length === 1 ? "" : "s"}.`);
  } finally {
    state.autoExpanding = false;
    els.autoExpand.disabled = false;
    els.autoExpand.textContent = "Expand next best";
    renderNextPanel();
  }
}

function simulatePhysics() {
  const nodes = [...state.nodes.values()];
  const edges = [...state.edges.values(), ...state.hiddenEdges.values()];
  state.physicsFrame += 1;
  for (const node of nodes) {
    node.vx *= 0.72;
    node.vy *= 0.72;
  }

  function pairForce(a, b, strength = 900, minDistance = 36) {
    let dx = a.x - b.x;
    let dy = a.y - b.y;
    if (Math.abs(dx) + Math.abs(dy) < 0.001) {
      const angle = ((a.id.charCodeAt(0) || 1) * 12.9898 + (b.id.charCodeAt(0) || 1) * 78.233) % (Math.PI * 2);
      dx = Math.cos(angle) * 0.01;
      dy = Math.sin(angle) * 0.01;
    }
    const distSq = Math.max(minDistance * minDistance, dx * dx + dy * dy);
    const dist = Math.sqrt(distSq);
    const force = strength / distSq;
    const fx = (dx / dist) * force;
    const fy = (dy / dist) * force;
    a.vx += fx;
    a.vy += fy;
    b.vx -= fx;
    b.vy -= fy;
  }

  function separateOverlap(a, b) {
    let dx = b.x - a.x;
    let dy = b.y - a.y;
    let dist = Math.sqrt(dx * dx + dy * dy);
    if (dist < 0.001) {
      const angle = ((a.id.length + 1) * 19.19 + (b.id.length + 1) * 31.31) % (Math.PI * 2);
      dx = Math.cos(angle);
      dy = Math.sin(angle);
      dist = 1;
    }
    const minDist = nodeRadius(a) + nodeRadius(b) + 14;
    if (dist >= minDist) return;
    const push = Math.min(8, (minDist - dist) * 0.18);
    const fx = (dx / dist) * push;
    const fy = (dy / dist) * push;
    if (state.dragging !== a.id) {
      a.x -= fx;
      a.y -= fy;
      a.vx -= fx * 0.08;
      a.vy -= fy * 0.08;
    }
    if (state.dragging !== b.id) {
      b.x += fx;
      b.y += fy;
      b.vx += fx * 0.08;
      b.vy += fy * 0.08;
    }
  }

  const n = nodes.length;
  if (n <= 520) {
    for (let i = 0; i < n; i += 1) {
      for (let j = i + 1; j < n; j += 1) pairForce(nodes[i], nodes[j], 1050);
    }
  } else {
    const stride = Math.max(31, Math.floor(Math.sqrt(n) * 7));
    const samples = Math.min(10, n - 1);
    const offset = state.physicsFrame % stride;
    for (let i = 0; i < n; i += 1) {
      for (let step = 1; step <= samples; step += 1) {
        const j = (i + offset + step * stride) % n;
        if (j > i) pairForce(nodes[i], nodes[j], 1200);
      }
    }
  }

  for (const edge of edges) {
    const dx = edge.to.x - edge.from.x;
    const dy = edge.to.y - edge.from.y;
    const dist = Math.max(1, Math.sqrt(dx * dx + dy * dy));
    const hidden = edge.confidence < state.minConfidence;
    const target = nodeRadius(edge.from) + nodeRadius(edge.to) + 105 + (1 - edge.confidence) * 80;
    const force = (dist - target) * (hidden ? 0.00045 : 0.00145);
    const fx = (dx / dist) * force;
    const fy = (dy / dist) * force;
    edge.from.vx += fx;
    edge.from.vy += fy;
    edge.to.vx -= fx;
    edge.to.vy -= fy;
  }

  const collisionCellSize = 130;
  const collisionKey = (x, y) => `${x},${y}`;
  const collisionPasses = n > 1200 ? 1 : 2;
  for (let pass = 0; pass < collisionPasses; pass += 1) {
    const grid = new Map();
    for (const node of nodes) {
      const cx = Math.floor(node.x / collisionCellSize);
      const cy = Math.floor(node.y / collisionCellSize);
      const key = collisionKey(cx, cy);
      const bucket = grid.get(key) || [];
      bucket.push(node);
      grid.set(key, bucket);
    }
    for (const [key, bucket] of grid) {
      const [cx, cy] = key.split(",").map(Number);
      for (let ox = -1; ox <= 1; ox += 1) {
        for (let oy = -1; oy <= 1; oy += 1) {
          const other = grid.get(collisionKey(cx + ox, cy + oy));
          if (!other) continue;
          const bucketLimit = n > 1200 ? Math.min(bucket.length, 18) : bucket.length;
          const otherLimit = n > 1200 ? Math.min(other.length, 18) : other.length;
          for (let i = 0; i < bucketLimit; i += 1) {
            const start = other === bucket ? i + 1 : 0;
            for (let j = start; j < otherLimit; j += 1) {
              separateOverlap(bucket[i], other[j]);
            }
          }
        }
      }
    }
  }

  for (const node of nodes) {
    if (state.dragging === node.id) continue;
    const speed = Math.hypot(node.vx, node.vy);
    if (speed > 3.2) {
      const scale = 3.2 / speed;
      node.vx *= scale;
      node.vy *= scale;
    } else if (speed < 0.01) {
      node.vx = 0;
      node.vy = 0;
    }
    node.x += node.vx;
    node.y += node.vy;
  }
}

function drawArrow(from, to, confidence, count, selected, hidden = false, faded = false, direction = "", simple = false) {
  const dx = to.x - from.x;
  const dy = to.y - from.y;
  const dist = Math.max(1, Math.sqrt(dx * dx + dy * dy));
  const ux = dx / dist;
  const uy = dy / dist;
  const startX = from.x + ux * (nodeRadius(from) + 3);
  const startY = from.y + uy * (nodeRadius(from) + 3);
  const endX = to.x - ux * (nodeRadius(to) + 5);
  const endY = to.y - uy * (nodeRadius(to) + 5);
  const evidence = Math.log2(Math.max(1, count) + 1);
  const width = 0.7 + confidence * 2.6 + Math.min(2.8, evidence * 0.55);
  const weak = confidence < 0.35;
  const dashed = hidden || (weak && !selected);

  const directional = direction === "incoming" || direction === "outgoing";
  const rootPath = direction === "root";
  const alpha = hidden
    ? (faded ? 0.045 : 0.12 + confidence * 0.12)
    : faded
      ? 0.055
      : weak
        ? 0.12 + confidence * 0.32
        : 0.22 + confidence * 0.55 + Math.min(0.12, evidence * 0.025);
  ctx.strokeStyle = rootPath
    ? `rgba(119, 77, 172, ${hidden ? 0.48 : 0.92})`
    : directional
    ? direction === "incoming"
      ? `rgba(44, 111, 187, ${hidden ? 0.42 : 0.88})`
      : `rgba(191, 112, 38, ${hidden ? 0.42 : 0.9})`
    : hidden
      ? `rgba(102, 112, 109, ${alpha})`
      : selected
        ? "#b4472f"
        : `rgba(31, 35, 38, ${alpha})`;
  ctx.lineWidth = rootPath ? Math.max(2.8, width + 1.2) : directional ? Math.max(2.2, width + 0.8) : hidden ? 0.9 : weak ? Math.max(0.8, width * 0.62) : width;
  if (dashed && !simple) ctx.setLineDash([5, 7]);
  ctx.beginPath();
  ctx.moveTo(startX, startY);
  ctx.lineTo(endX, endY);
  ctx.stroke();
  if (dashed && !simple) ctx.setLineDash([]);

  if (simple) return;

  const size = 6 + confidence * 3 + Math.min(2, evidence * 0.35);
  ctx.fillStyle = ctx.strokeStyle;
  ctx.beginPath();
  ctx.moveTo(endX, endY);
  ctx.lineTo(endX - ux * size - uy * size * 0.55, endY - uy * size + ux * size * 0.55);
  ctx.lineTo(endX - ux * size + uy * size * 0.55, endY - uy * size - ux * size * 0.55);
  ctx.closePath();
  ctx.fill();
}

function draw(timestamp = 0) {
  const largeRender = state.nodes.size > 1200;
  const interacting = performance.now() < state.interactingUntil;
  if (state.interactionWasActive && !interacting) markRenderDirty();
  state.interactionWasActive = interacting;
  const animated = state.dragging || state.expanding.size || state.autoExpanding;
  if (largeRender && !state.renderDirty && !animated) {
    requestAnimationFrame(draw);
    return;
  }
  const isInteracting = performance.now() < state.interactingUntil;
  const frameGap = largeRender ? (isInteracting ? 16 : 90) : 0;
  if (frameGap && timestamp && timestamp - state.lastRenderAt < frameGap) {
    requestAnimationFrame(draw);
    return;
  }
  state.lastRenderAt = timestamp || performance.now();
  const largeGraph = state.nodes.size > 1200;
  const shouldStepPhysics = !largeGraph || state.expanding.size;
  if (shouldStepPhysics) simulatePhysics();
  else state.physicsFrame += 1;
  const width = canvas.getBoundingClientRect().width;
  const height = canvas.getBoundingClientRect().height;
  ctx.clearRect(0, 0, width, height);
  ctx.save();
  ctx.translate(state.pan.x, state.pan.y);
  ctx.scale(state.zoom, state.zoom);
  const highlights = highlightedNodeIds();
  const rootId = focusNodeId();
  const hasFocus = highlights.size > 0;
  const bounds = viewportBounds(isInteracting ? 80 : 180);
  const simpleInteraction = largeRender && isInteracting;

  for (const edge of state.hiddenEdges.values()) {
    if (!edgeInBounds(edge, bounds)) continue;
    if (simpleInteraction && !hasFocus) continue;
    if (simpleInteraction && edge.confidence < 0.35) continue;
    const related = !hasFocus || (highlights.has(edge.from.id) && highlights.has(edge.to.id));
    if (simpleInteraction && hasFocus && !related) continue;
    drawArrow(edge.from, edge.to, edge.confidence, edge.count, false, true, !related, edgeDirectionFor(edge, rootId), simpleInteraction);
  }

  for (const [key, edge] of state.edges) {
    if (!edgeInBounds(edge, bounds)) continue;
    if (simpleInteraction && !hasFocus && edge.confidence < 0.32 && edge.count <= 1) continue;
    const selected = state.selected && state.selected.type === "edge" && state.selected.key === key;
    const related = !hasFocus || selected || (highlights.has(edge.from.id) && highlights.has(edge.to.id));
    if (simpleInteraction && hasFocus && !related) continue;
    drawArrow(edge.from, edge.to, edge.confidence, edge.count, selected, false, !related, edgeDirectionFor(edge, rootId), simpleInteraction);
  }

  for (const node of state.nodes.values()) {
    if (!pointInBounds(node, bounds)) continue;
    const expanded = state.expanded.has(node.id);
    const expanding = state.expanding.has(node.id);
    const selected = state.selected && state.selected.type === "node" && state.selected.id === node.id;
    const hovered = state.hovered === node.id;
    const related = !hasFocus || highlights.has(node.id);
    const radius = nodeRadius(node);
    const ambiguous = !simpleInteraction && !expanded && (node.aliases && node.aliases.length || (node.name.split(/\s+/).length <= 2 && node.popularity > 2));
    if (expanding) {
      const pulse = 5 + Math.sin(Date.now() / 180) * 3;
      ctx.strokeStyle = "rgba(180, 71, 47, 0.42)";
      ctx.lineWidth = 3;
      ctx.beginPath();
      ctx.arc(node.x, node.y, radius + 9 + pulse, 0, Math.PI * 2);
      ctx.stroke();
    }
    ctx.globalAlpha = related ? 1 : 0.24;
    ctx.fillStyle = expanded ? "#35a186" : "#f0b84d";
    ctx.strokeStyle = selected || hovered ? "#1f6fb8" : ambiguous ? "#b4472f" : expanded ? "#176e5f" : "#7a5a1b";
    ctx.lineWidth = selected || hovered ? 3.8 : ambiguous ? 2.4 : expanded ? 1.8 : 1.4;
    ctx.beginPath();
    ctx.arc(node.x, node.y, selected ? radius + 3 : radius, 0, Math.PI * 2);
    ctx.fill();
    ctx.stroke();
    if (expanded && !simpleInteraction) {
      ctx.strokeStyle = "rgba(23, 110, 95, 0.42)";
      ctx.lineWidth = 2;
      ctx.beginPath();
      ctx.arc(node.x, node.y, radius + 4, 0, Math.PI * 2);
      ctx.stroke();
    }
    if (ambiguous && !simpleInteraction) {
      ctx.setLineDash([3, 4]);
      ctx.strokeStyle = "rgba(180, 71, 47, 0.82)";
      ctx.lineWidth = 2;
      ctx.beginPath();
      ctx.arc(node.x, node.y, radius + 5.5, 0, Math.PI * 2);
      ctx.stroke();
      ctx.setLineDash([]);
    }
    ctx.globalAlpha = 1;

    const showLabel = !isInteracting && (selected || hovered || expanding || state.zoom > 0.65 || nodeProminence(node) > maxNodeProminence() * 0.18);
    if (!showLabel) continue;
    ctx.fillStyle = related ? "#1f2326" : "rgba(31, 35, 38, 0.35)";
    ctx.font = "13px system-ui, sans-serif";
    ctx.textAlign = "center";
    ctx.textBaseline = "top";
    const label = node.name.length > 28 ? `${node.name.slice(0, 25)}...` : node.name;
    ctx.fillText(label, node.x, node.y + radius + 7);
  }

  ctx.restore();
  state.renderDirty = false;
  requestAnimationFrame(draw);
}

function graphPoint(event) {
  const rect = canvas.getBoundingClientRect();
  return {
    x: (event.clientX - rect.left - state.pan.x) / state.zoom,
    y: (event.clientY - rect.top - state.pan.y) / state.zoom
  };
}

function hitNode(point) {
  for (const node of [...state.nodes.values()].reverse()) {
    const dx = point.x - node.x;
    const dy = point.y - node.y;
    if (Math.sqrt(dx * dx + dy * dy) <= nodeRadius(node) + 7) return node;
  }
  return null;
}

function distanceToSegment(point, a, b) {
  const dx = b.x - a.x;
  const dy = b.y - a.y;
  const lengthSq = dx * dx + dy * dy;
  if (lengthSq === 0) return Math.hypot(point.x - a.x, point.y - a.y);
  const t = Math.max(0, Math.min(1, ((point.x - a.x) * dx + (point.y - a.y) * dy) / lengthSq));
  return Math.hypot(point.x - (a.x + t * dx), point.y - (a.y + t * dy));
}

function hitEdge(point) {
  for (const [key, edge] of state.edges) {
    if (distanceToSegment(point, edge.from, edge.to) < 8 / state.zoom) return { key, edge };
  }
  return null;
}

function updateSelectedDetails() {
  if (!state.selected) {
    els.selectedDetails.innerHTML = `Click a node or edge.<br><button id="clear-selection" type="button" disabled>Clear selection</button>`;
    return;
  }
  if (state.selected.type === "node") {
    const node = state.nodes.get(state.selected.id);
    if (!node) {
      state.selected = null;
      els.selectedDetails.innerHTML = `Click a node or edge.<br><button id="clear-selection" type="button" disabled>Clear selection</button>`;
      return;
    }
    const incoming = [...state.edges.values()].filter(edge => edge.to.id === node.id).length;
    const outgoing = [...state.edges.values()].filter(edge => edge.from.id === node.id).length;
    const hiddenIncoming = [...state.hiddenEdges.values()].filter(edge => edge.to.id === node.id).length;
    const hiddenOutgoing = [...state.hiddenEdges.values()].filter(edge => edge.from.id === node.id).length;
    const hiddenLine = hiddenIncoming || hiddenOutgoing
      ? `<br>Filtered low-confidence links: ${hiddenIncoming} incoming, ${hiddenOutgoing} outgoing`
      : "";
    const aliases = node.aliases && node.aliases.length ? `<br>Aliases: ${node.aliases.slice(0, 4).map(escapeHtml).join(", ")}` : "";
    const isExpanding = state.expanding.has(node.id);
    const isExpanded = state.expanded.has(node.id);
    const stateLabel = isExpanding ? "Expanding now..." : isExpanded ? "Expanded" : "Not expanded yet";
    const actionLabel = isExpanding ? "Expanding..." : isExpanded ? "Sample again" : "Expand this entity";
    const actionHint = isExpanded && !isExpanding ? "<br><span>Runs another model pass and adds new observations without deleting existing ones.</span>" : "";
    const action = `<br><button id="expand-selected" type="button" ${isExpanding || !state.canWrite ? "disabled" : ""}>${actionLabel}</button><button id="trace-roots-selected" type="button">Trace roots</button><button id="clear-selection" type="button">Clear selection</button>${actionHint}`;
    els.selectedDetails.innerHTML = `<strong>${escapeHtml(node.name)}</strong><br>Visible incoming: ${incoming}<br>Visible outgoing: ${outgoing}${hiddenLine}<br>Observed weight: ${Number(node.weightedPopularity || 0).toFixed(2)} weighted observations<br>Prominence: ${prominencePercent(node)}% of current max${aliases}<br>${stateLabel}${action}`;
    const button = document.querySelector("#expand-selected");
    if (button) {
      button.addEventListener("click", () => expandEntity(node.name).catch(error => setStatus(error.message)));
    }
    const rootsButton = document.querySelector("#trace-roots-selected");
    if (rootsButton) rootsButton.addEventListener("click", () => {
      setPanelTab("roots");
      renderRootsPanel();
    });
    const clearButton = document.querySelector("#clear-selection");
    if (clearButton) clearButton.addEventListener("click", clearSelection);
    return;
  }
  const edge = state.edges.get(state.selected.key);
  if (!edge) {
    state.selected = null;
    els.selectedDetails.innerHTML = `Click a node or edge.<br><button id="clear-selection" type="button" disabled>Clear selection</button>`;
    return;
  }
  const rows = (edge.observations || []).slice(-8).reverse().map(obs => {
    const when = obs.createdAt ? new Date(obs.createdAt).toLocaleString() : "unknown time";
    const source = obs.sourceEntity ? ` sampled from ${escapeHtml(obs.sourceEntity)}` : "";
    const model = obs.model ? ` via ${escapeHtml(obs.model)}` : "";
    return `<li>${Number(obs.confidence).toFixed(3)}${source}${model}<br><span>${escapeHtml(when)} · ${escapeHtml(obs.evidenceType || "unknown")} · ${obs.sources?.length || 0} recorded source links</span></li>`;
  }).join("");
  els.selectedDetails.innerHTML = `<strong>${escapeHtml(edge.from.name)} → ${escapeHtml(edge.to.name)}</strong><br>Mean sample score: ${edge.confidence.toFixed(3)}<br>Observations: ${edge.count}<button id="clear-selection" type="button">Clear selection</button><ul class="observation-list">${rows}</ul>`;
  const clearButton = document.querySelector("#clear-selection");
  if (clearButton) clearButton.addEventListener("click", clearSelection);
}

function updateUi() {
  recomputeEdges();
  els.nodeCount.textContent = state.nodes.size;
  els.edgeCount.textContent = state.edges.size;
  els.observationCount.textContent = state.observations.length;
  els.expandedCount.textContent = state.expanded.size;
  els.minConfidenceValue.textContent = Number(state.minConfidence).toFixed(2);
  updateSelectedDetails();
  renderNextPanel();
  renderRootsPanel();
  if (state.view === "table") renderEntityTable();
  markRenderDirty();
}

canvas.addEventListener("pointerdown", event => {
  const point = graphPoint(event);
  const node = hitNode(point);
  if (node) {
    state.dragging = node.id;
    state.selected = { type: "node", id: node.id };
    canvas.setPointerCapture(event.pointerId);
    updateUi();
    markRenderDirty();
    return;
  }
  const edgeHit = hitEdge(point);
  if (edgeHit) {
    state.selected = { type: "edge", key: edgeHit.key };
    updateUi();
    markRenderDirty();
    return;
  }
  if (state.selected) {
    state.selected = null;
    updateUi();
    markRenderDirty();
  }
  state.lastPointer = { x: event.clientX, y: event.clientY };
});

canvas.addEventListener("pointermove", event => {
  if (state.dragging) {
    const node = state.nodes.get(state.dragging);
    const point = graphPoint(event);
    node.x = point.x;
    node.y = point.y;
    node.vx = 0;
    node.vy = 0;
    markInteracting();
    return;
  }
  if (state.lastPointer) {
    state.pan.x += event.clientX - state.lastPointer.x;
    state.pan.y += event.clientY - state.lastPointer.y;
    state.lastPointer = { x: event.clientX, y: event.clientY };
    markInteracting();
    return;
  }
  const hoverNode = hitNode(graphPoint(event));
  const nextHovered = hoverNode ? hoverNode.id : null;
  if (state.hovered !== nextHovered) {
    state.hovered = nextHovered;
    markRenderDirty();
  }
});

canvas.addEventListener("pointerleave", () => {
  state.hovered = null;
  markRenderDirty();
});

canvas.addEventListener("pointerup", () => {
  if (state.dragging || state.lastPointer) scheduleSave();
  state.dragging = null;
  state.lastPointer = null;
  markRenderDirty();
});

canvas.addEventListener("wheel", event => {
  event.preventDefault();
  const rect = canvas.getBoundingClientRect();
  const mouseX = event.clientX - rect.left;
  const mouseY = event.clientY - rect.top;
  const before = {
    x: (mouseX - state.pan.x) / state.zoom,
    y: (mouseY - state.pan.y) / state.zoom
  };
  const factor = Math.exp(-event.deltaY * 0.0012);
  state.zoom = clampZoom(state.zoom * factor);
  state.pan = {
    x: mouseX - before.x * state.zoom,
    y: mouseY - before.y * state.zoom
  };
  scheduleSave();
  markInteracting(260);
}, { passive: false });

canvas.addEventListener("dblclick", event => {
  const node = hitNode(graphPoint(event));
  if (!node) return;
  if (state.devTools) {
    expandEntity(node.name).catch(error => setStatus(error.message));
    return;
  }
  focusNode(node, 1.15);
});

els.form.addEventListener("submit", event => {
  event.preventDefault();
  searchEntity(els.input.value).catch(error => setStatus(error.message));
});

els.focusEntity.addEventListener("click", () => {
  searchEntity(els.input.value).catch(error => setStatus(error.message));
});

els.graphView.addEventListener("click", () => setView("graph"));
els.tableView.addEventListener("click", () => setView("table"));
els.fitGraph.addEventListener("click", fitGraph);
els.centerSelected.addEventListener("click", () => {
  const node = state.selected && state.selected.type === "node" ? state.nodes.get(state.selected.id) : null;
  if (node) focusNode(node, 1.15);
  else fitGraph();
});

els.panelTabs.forEach(button => {
  button.addEventListener("click", () => {
    const tab = button.getAttribute("data-panel-tab");
    setPanelTab(tab);
    if (tab === "diagnostics") {
      loadDiagnostics().catch(error => {
        if (els.diagnosticsOutput) els.diagnosticsOutput.textContent = error.message;
        setStatus(error.message);
      });
    } else if (tab === "roots") {
      renderRootsPanel();
    } else if (tab === "next") {
      renderNextPanel();
    } else if (tab === "dedupe" && state.devTools) {
      loadDedupeSuggestions().catch(error => {
        if (els.dedupeOutput) els.dedupeOutput.textContent = error.message;
        setStatus(error.message);
      });
    }
  });
});

if (els.activitySummary) {
  els.activitySummary.addEventListener("click", () => setPanelTab("activity"));
}

if (els.identitySummary) {
  els.identitySummary.addEventListener("click", () => setPanelTab("dedupe"));
}

if (els.refreshDiagnostics) {
  els.refreshDiagnostics.addEventListener("click", () => {
    loadDiagnostics().catch(error => {
      if (els.diagnosticsOutput) els.diagnosticsOutput.textContent = error.message;
      setStatus(error.message);
    });
  });
}

if (els.runBenchmark) {
  els.runBenchmark.addEventListener("click", () => {
    runBenchmark().catch(error => {
      if (els.diagnosticsOutput) els.diagnosticsOutput.textContent = error.message;
      setStatus(error.message);
    });
  });
}

els.activityList.addEventListener("click", event => {
  const button = event.target.closest("[data-prompt-activity]");
  if (!button) return;
  const activity = state.activities.find(item => item.id === button.getAttribute("data-prompt-activity"));
  if (activity) openPromptModal(activity.prompt);
});

els.promptModalClose.addEventListener("click", closePromptModal);
els.promptModal.addEventListener("click", event => {
  if (event.target.closest("[data-close-modal]")) closePromptModal();
});

document.querySelectorAll("[data-sort]").forEach(button => {
  button.addEventListener("click", () => {
    const sort = button.getAttribute("data-sort");
    if (state.tableSort === sort) {
      state.tableDirection = state.tableDirection === "asc" ? "desc" : "asc";
    } else {
      state.tableSort = sort;
      state.tableDirection = sort === "name" ? "asc" : "desc";
    }
    renderEntityTable();
  });
});

els.tableFilter.addEventListener("input", () => {
  state.tableFilter = els.tableFilter.value;
  renderEntityTable();
});

els.tableBody.addEventListener("click", event => {
  const button = event.target.closest("[data-node]");
  if (!button) return;
  const node = state.nodes.get(button.getAttribute("data-node"));
  if (!node) return;
  setView("graph");
  focusNode(node, 1.15);
});

if (els.rootsOutput) {
  els.rootsOutput.addEventListener("click", event => {
    const button = event.target.closest("[data-root-node]");
    if (!button) return;
    const node = state.nodes.get(button.getAttribute("data-root-node"));
    if (!node) return;
    state.selected = { type: "node", id: node.id };
    setView("graph");
    focusNode(node, 1.15);
    updateUi();
  });
}

if (els.nextOutput) {
  els.nextOutput.addEventListener("click", event => {
    const focusButton = event.target.closest("[data-next-node]");
    const expandButton = event.target.closest("[data-next-expand]");
    const id = (expandButton || focusButton) && (expandButton || focusButton).getAttribute(expandButton ? "data-next-expand" : "data-next-node");
    if (!id) return;
    const node = state.nodes.get(id);
    if (!node) return;
    state.selected = { type: "node", id: node.id };
    setView("graph");
    focusNode(node, 1.15);
    updateUi();
    if (expandButton) expandEntity(node.name).catch(error => setStatus(error.message));
  });
}

els.autoExpand.addEventListener("click", () => {
  autoExpandFrontier().catch(error => setStatus(error.message));
});

els.autoExpand.addEventListener("contextmenu", event => {
  event.preventDefault();
  setStatus("Expand next best needs a normal click and confirmation.");
});

els.dedupeReview.addEventListener("click", async () => {
  const confirmed = window.confirm("Run full identity sweep will call the model across a broad candidate set. Continue?");
  if (!confirmed) {
    setStatus("Identity sweep cancelled.");
    return;
  }
  const activityId = addActivity("identity", "Full identity sweep", "Model review in progress", "running");
  els.dedupeOutput.textContent = "Running a broader identity sweep and adding decisions to the approval list...";
  try {
    const response = await fetch("/api/dedupe-review", {
      method: "POST",
      headers: adminHeaders()
    });
    const payload = await response.json();
    if (!response.ok) throw new Error(payload.error || "Identity sweep failed");
    if (payload.debugPrompt) updateActivity(activityId, { prompt: payload.debugPrompt });
    await refreshGraph();
    const suggestions = payload.pending || [];
    if (!suggestions.length) {
      els.dedupeOutput.textContent = `No confident identity groups found from ${payload.candidates.length} local candidates.`;
      updateActivity(activityId, { status: "done", detail: "No confident identity groups found" });
      return;
    }
    state.identitySuggestions = suggestions;
    renderDedupeSuggestions(state.identitySuggestions);
    updateActivity(activityId, { status: "done", detail: `${payload.stored || 0} identity decisions added for approval` });
    setStatus(`Added ${payload.stored || 0} identity decisions for approval.`);
  } catch (error) {
    updateActivity(activityId, { status: "failed", detail: error.message });
    els.dedupeOutput.textContent = error.message;
  }
});

if (els.previewOrphans) {
  els.previewOrphans.addEventListener("click", () => {
    previewOrphanNodes().catch(error => {
      setStatus(error.message);
      if (els.dedupeOutput) els.dedupeOutput.textContent = error.message;
    });
  });
}

if (els.cleanOrphans) {
  els.cleanOrphans.addEventListener("click", () => {
    cleanOrphanNodes().catch(error => {
      setStatus(error.message);
      if (els.dedupeOutput) els.dedupeOutput.textContent = error.message;
    });
  });
}

els.identityFilters.forEach(button => {
  button.addEventListener("click", () => {
    state.identityFilter = button.getAttribute("data-identity-filter") || "all";
    renderDedupeSuggestions(state.identitySuggestions || []);
  });
});

els.dedupeOutput.addEventListener("click", event => {
  const button = event.target.closest("[data-dedupe-action]");
  if (!button) return;
  const id = button.getAttribute("data-id");
  const action = button.getAttribute("data-dedupe-action");
  button.disabled = true;
  button.textContent = action === "approve" ? "Applying..." : "Rejecting...";
  decideDedupeSuggestion(id, action).catch(error => {
    setStatus(error.message);
    loadDedupeSuggestions().catch(() => {});
  });
});

els.reset.addEventListener("click", async () => {
  if (!window.confirm("Reset clears the shared graph. An undo snapshot is retained in Identity history. Continue?")) return;
  try {
    const response = await fetch("/api/reset", { method: "POST", headers: { "content-type": "application/json", ...adminHeaders() }, body: JSON.stringify({ revision: state.revision }) });
    const payload = await response.json();
    if (!response.ok) { if (response.status === 409) await refreshGraph(); throw new Error(payload.error); }
    await acceptServerPayload(payload);
    updateUi();
    setStatus("Graph reset. Undo is available in Identity history until the next graph change.");
  } catch (error) { setStatus(error.message); }
});

els.minConfidence.addEventListener("input", () => {
  state.minConfidence = Number(els.minConfidence.value);
  scheduleSave();
  updateUi();
});

window.addEventListener("resize", () => {
  resizeCanvas();
  markRenderDirty();
});
window.addEventListener("keydown", event => {
  if (event.key === "Escape" && state.selected) {
    clearSelection();
  }
});

async function loadHistory() {
  const output = document.querySelector("#history-output");
  const response = await fetch("/api/history", { headers: adminHeaders() });
  const payload = await response.json();
  if (!response.ok) throw new Error(payload.error || "History could not be loaded");
  output.innerHTML = payload.history.map(event => `<div class="dedupe-group"><strong>${escapeHtml(event.action)}</strong><p>${escapeHtml(new Date(event.createdAt).toLocaleString())}</p><p>${escapeHtml(event.details.canonical || event.details.reason || "Graph mutation")}</p>${!event.undoneBy && event.afterRevision === payload.revision ? `<button type="button" data-undo-id="${Number(event.id)}">Undo this edit</button>` : "<span>Newer changes prevent a safe undo here.</span>"}</div>`).join("") || "No graph edits recorded yet.";
}

document.querySelector("#load-history").addEventListener("click", () => loadHistory().catch(error => setStatus(error.message)));
document.querySelector("#history-output").addEventListener("click", async event => {
  const button = event.target.closest("[data-undo-id]");
  if (!button || !window.confirm("Undo this latest graph edit? Its previous state will be restored and the undo recorded.")) return;
  try {
    const response = await fetch("/api/history/" + Number(button.dataset.undoId) + "/undo", { method: "POST", headers: { "content-type": "application/json", ...adminHeaders() }, body: JSON.stringify({ revision: state.revision }) });
    const payload = await response.json();
    if (!response.ok) throw new Error(payload.error);
    await acceptServerPayload(payload); updateUi(); await loadHistory(); await loadDedupeSuggestions();
    setStatus(payload.result.reason);
  } catch (error) { setStatus(error.message); await refreshGraph(); }
});


els.selectedDetails.addEventListener("click", event => {
  const choice = event.target.closest("[data-search-choice]");
  if (choice) focusNode(state.nodes.get(choice.getAttribute("data-search-choice")), 1.15);
});

resizeCanvas();
loadConfig().then(loadGraph);
draw();
