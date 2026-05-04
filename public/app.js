const canvas = document.querySelector("#graph-canvas");
const ctx = canvas.getContext("2d");

const state = {
  nodes: new Map(),
  observations: [],
  edges: new Map(),
  hiddenEdges: new Map(),
  expanded: new Set(),
  selected: null,
  devTools: false,
  expanding: new Set(),
  dragging: null,
  pan: { x: 0, y: 0 },
  zoom: 1,
  lastPointer: null,
  minConfidence: 0,
  loaded: false,
  saveTimer: null,
  saveVersion: 0,
  maxWeightedPopularity: 0
  ,
  view: "graph",
  tableSort: "popularity",
  tableDirection: "desc",
  tableFilter: "",
  activities: [],
  activityCounter: 0
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
  activityList: document.querySelector("#activity-list"),
  selectedDetails: document.querySelector("#selected-details"),
  dedupeReview: document.querySelector("#dedupe-review"),
  dedupeOutput: document.querySelector("#dedupe-output")
};

const MIN_ZOOM = 0.04;
const MAX_ZOOM = 8;

const adminParam = new URLSearchParams(window.location.search).get("admin");
if (adminParam) {
  window.localStorage.setItem("influenceMapAdminToken", adminParam);
  window.history.replaceState({}, "", window.location.pathname);
}

function adminHeaders() {
  const token = window.localStorage.getItem("influenceMapAdminToken");
  return token ? { "x-admin-token": token } : {};
}

function displayName(name) {
  return String(name || "").trim().replace(/\s+/g, " ");
}

function escapeHtml(value) {
  return String(value || "")
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;")
    .replace(/'/g, "&#039;");
}

function canonicalName(name) {
  return displayName(name)
    .toLowerCase()
    .replace(/&/g, " and ")
    .replace(/[()[\]{}'"`.,:;!?]/g, " ")
    .replace(/\s+/g, " ")
    .replace(/^the\s+/, "")
    .trim();
}

function keyFor(name) {
  return canonicalName(name);
}

function edgeKey(from, to) {
  return `${keyFor(from)}=>${keyFor(to)}`;
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

function renameNode(nodeOrName, nextName) {
  const canonical = displayName(nextName);
  const previousId = typeof nodeOrName === "string" ? keyFor(nodeOrName) : nodeOrName && nodeOrName.id;
  const nextId = keyFor(canonical);
  if (!previousId || !nextId || !canonical || !state.nodes.has(previousId)) {
    return state.nodes.get(previousId) || ensureNode(canonical);
  }

  const previous = state.nodes.get(previousId);
  const target = state.nodes.get(nextId);
  const aliases = new Set([
    previous.name,
    canonical,
    ...(previous.aliases || []),
    ...(target && target.aliases ? target.aliases : [])
  ].map(displayName).filter(Boolean));

  if (target && target !== previous) {
    target.name = canonical;
    target.aliases = [...aliases].filter(alias => alias !== canonical).slice(0, 20);
    target.popularity = Math.max(target.popularity || 0, previous.popularity || 0);
    target.weightedPopularity = Math.max(target.weightedPopularity || 0, previous.weightedPopularity || 0);
    target.x = Number.isFinite(target.x) ? target.x : previous.x;
    target.y = Number.isFinite(target.y) ? target.y : previous.y;
    state.nodes.delete(previousId);
  } else {
    previous.name = canonical;
    previous.aliases = [...aliases].filter(alias => alias !== canonical).slice(0, 20);
    if (previousId !== nextId) {
      previous.id = nextId;
      state.nodes.delete(previousId);
      state.nodes.set(nextId, previous);
    }
  }

  for (const observation of state.observations) {
    if (keyFor(observation.from) === previousId || keyFor(observation.from) === nextId) observation.from = canonical;
    if (keyFor(observation.to) === previousId || keyFor(observation.to) === nextId) observation.to = canonical;
    if (keyFor(observation.sourceEntity) === previousId || keyFor(observation.sourceEntity) === nextId) observation.sourceEntity = canonical;
  }
  if (state.expanded.has(previousId)) {
    state.expanded.delete(previousId);
    state.expanded.add(nextId);
  }
  if (state.selected && state.selected.type === "node" && state.selected.id === previousId) {
    state.selected.id = nextId;
  }
  return state.nodes.get(nextId);
}

function addObservation(from, to, confidence, meta = {}) {
  const source = ensureNode(from);
  const target = ensureNode(to);
  if (!source || !target || source.id === target.id) return;
  state.observations.push({
    from: source.name,
    to: target.name,
    confidence: Math.max(0, Math.min(1, Number(confidence) || 0.5)),
    sourceEntity: displayName(meta.sourceEntity || ""),
    provider: displayName(meta.provider || ""),
    model: displayName(meta.model || ""),
    createdAt: meta.createdAt || new Date().toISOString()
  });
  recomputeEdges();
}

function recomputeEdges() {
  const visibleGrouped = new Map();
  const hiddenGrouped = new Map();
  state.maxWeightedPopularity = 0;
  for (const node of state.nodes.values()) {
    node.popularity = 0;
    node.weightedPopularity = 0;
  }

  for (const obs of state.observations) {
    const source = ensureNode(obs.from);
    const target = ensureNode(obs.to);
    if (!source || !target) continue;
    source.popularity += 1;
    target.popularity += 1;
    source.weightedPopularity += obs.confidence;
    target.weightedPopularity += obs.confidence;
    state.maxWeightedPopularity = Math.max(
      state.maxWeightedPopularity,
      source.weightedPopularity,
      target.weightedPopularity
    );
  }

  for (const obs of state.observations) {
    const grouped = obs.confidence < state.minConfidence ? hiddenGrouped : visibleGrouped;
    const key = edgeKey(obs.from, obs.to);
    const current = grouped.get(key) || {
      from: ensureNode(obs.from),
      to: ensureNode(obs.to),
      sum: 0,
      count: 0,
      observations: []
    };
    current.sum += obs.confidence;
    current.count += 1;
    current.observations.push(obs);
    grouped.set(key, current);
  }

  function toEdges(grouped) {
    return new Map(
      [...grouped].map(([key, edge]) => [
        key,
        {
          from: edge.from,
          to: edge.to,
          confidence: edge.sum / edge.count,
          count: edge.count,
          observations: edge.observations
        }
      ])
    );
  }

  state.edges = toEdges(visibleGrouped);
  state.hiddenEdges = toEdges(hiddenGrouped);
}

function nodeRadius(node) {
  const baseRadius = 14;
  const visualRange = 28;
  const maxPopularity = Math.max(1, state.maxWeightedPopularity || 0);
  const share = Math.max(0, node.weightedPopularity || 0) / maxPopularity;
  return baseRadius + Math.sqrt(share) * visualRange;
}

function popularityPercent(node) {
  const maxPopularity = Math.max(1, state.maxWeightedPopularity || 0);
  return Math.round((Math.max(0, node.weightedPopularity || 0) / maxPopularity) * 100);
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
  return true;
}

function findNodeByQuery(query) {
  const needle = canonicalName(query);
  if (!needle) return null;
  if (state.nodes.has(needle)) return state.nodes.get(needle);

  const candidates = [...state.nodes.values()].filter(node => {
    const names = [node.name, ...(node.aliases || [])].map(canonicalName);
    return names.some(name => name.includes(needle) || needle.includes(name));
  });

  return candidates.sort((a, b) => {
    const scoreA = Math.abs(canonicalName(a.name).length - needle.length);
    const scoreB = Math.abs(canonicalName(b.name).length - needle.length);
    return scoreA - scoreB || b.weightedPopularity - a.weightedPopularity;
  })[0] || null;
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

function entityStats() {
  const stats = new Map([...state.nodes.values()].map(node => [node.id, {
    id: node.id,
    name: node.name,
    aliases: node.aliases || [],
    incoming: 0,
    outgoing: 0,
    connections: new Set(),
    observations: 0,
    popularity: node.weightedPopularity || 0,
    expanded: state.expanded.has(node.id)
  }]));

  for (const obs of state.observations) {
    const from = keyFor(obs.from);
    const to = keyFor(obs.to);
    if (stats.has(from)) {
      const item = stats.get(from);
      item.outgoing += 1;
      item.observations += 1;
      item.connections.add(to);
    }
    if (stats.has(to)) {
      const item = stats.get(to);
      item.incoming += 1;
      item.observations += 1;
      item.connections.add(from);
    }
  }

  return [...stats.values()].map(item => ({
    ...item,
    connectionCount: item.connections.size
  }));
}

function renderEntityTable() {
  if (!els.tableBody) return;
  const filter = canonicalName(state.tableFilter);
  const rows = entityStats()
    .filter(item => !filter || canonicalName(item.name).includes(filter) || item.aliases.some(alias => canonicalName(alias).includes(filter)))
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
      <td><button type="button" data-node="${item.id}">${item.name}</button></td>
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
  const scale = window.devicePixelRatio || 1;
  canvas.width = Math.max(600, Math.floor(rect.width * scale));
  canvas.height = Math.max(420, Math.floor(rect.height * scale));
  ctx.setTransform(scale, 0, 0, scale, 0, 0);
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
  Object.assign(activity, patch, { updatedAt: new Date().toISOString() });
  renderActivityList();
}

function renderActivityList() {
  if (!els.activityList) return;
  if (!state.activities.length) {
    els.activityList.textContent = "Nothing running.";
    return;
  }
  els.activityList.innerHTML = state.activities.map(activity => {
    const prompt = activity.prompt && activity.prompt.messages ? activity.prompt.messages : null;
    const promptText = prompt ? prompt.map(message => (
      `${message.role.toUpperCase()}\n${message.content}`
    )).join("\n\n---\n\n") : "";
    return `
    <div class="activity-item ${escapeHtml(activity.status)}">
      <div>
        <strong>${escapeHtml(activity.title)}</strong>
        <span>${escapeHtml(activity.detail || activity.type)}</span>
        ${promptText ? `<details class="prompt-debug"><summary>Prompt</summary><pre>${escapeHtml(promptText)}</pre></details>` : ""}
      </div>
      <em>${escapeHtml(activity.status)} ${escapeHtml(activityTime(activity.updatedAt))}</em>
    </div>
  `;
  }).join("");
}

function serializeGraph() {
  return {
    nodes: [...state.nodes.values()].map(node => ({
      name: node.name,
      aliases: node.aliases || [],
      x: node.x,
      y: node.y
    })),
    observations: state.observations.map(observation => ({ ...observation })),
    expanded: [...state.expanded],
    pan: { ...state.pan },
    zoom: state.zoom,
    minConfidence: state.minConfidence
  };
}

function hydrateGraph(graph) {
  const minConfidence = Number(graph && graph.minConfidence);
  const zoom = Number(graph && graph.zoom);
  state.nodes.clear();
  state.observations = [];
  state.edges.clear();
  state.hiddenEdges.clear();
  state.expanded.clear();
  state.selected = null;
  state.pan = {
    x: Number(graph && graph.pan && graph.pan.x) || 0,
    y: Number(graph && graph.pan && graph.pan.y) || 0
  };
  state.zoom = Number.isFinite(zoom) ? clampZoom(zoom) : 1;
  state.minConfidence = Number.isFinite(minConfidence) ? Math.max(0, Math.min(1, minConfidence)) : 0;
  els.minConfidence.value = state.minConfidence;

  for (const node of Array.isArray(graph && graph.nodes) ? graph.nodes : []) {
    const hydrated = ensureNode(node.name);
    if (!hydrated) continue;
    hydrated.aliases = Array.isArray(node.aliases) ? node.aliases.map(displayName).filter(Boolean) : hydrated.aliases;
    const x = Number(node.x);
    const y = Number(node.y);
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
      from,
      to,
      confidence: Math.max(0, Math.min(1, Number(observation.confidence) || 0.5)),
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

  recomputeEdges();
}

async function loadGraph() {
  try {
    const response = await fetch("/api/graph");
    const payload = await response.json();
    if (!response.ok) throw new Error(payload.error || "Load failed");
    hydrateGraph(payload.data);
    state.loaded = true;
    const count = state.observations.length;
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
    document.body.classList.toggle("dev-tools", state.devTools);
    if (state.devTools) loadDedupeSuggestions().catch(() => {});
  } catch (error) {
    state.devTools = false;
    document.body.classList.remove("dev-tools");
  }
}

async function saveGraph() {
  if (!state.loaded) return;
  const version = ++state.saveVersion;
  const response = await fetch("/api/graph", {
    method: "PUT",
    headers: { "content-type": "application/json" },
    body: JSON.stringify(serializeGraph())
  });
  const payload = await response.json();
  if (!response.ok) throw new Error(payload.error || "Save failed");
  if (version === state.saveVersion) {
    setStatus(`Saved graph at ${new Date(payload.data.savedAt).toLocaleTimeString()}.`);
  }
}

function scheduleSave() {
  if (!state.loaded) return;
  window.clearTimeout(state.saveTimer);
  state.saveTimer = window.setTimeout(() => {
    saveGraph().catch(error => setStatus(`Could not save graph: ${error.message}`));
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
    const relation = fromId === node.id
      ? `${obs.from} influenced ${obs.to}`
      : `${obs.from} influenced ${obs.to}`;
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
    body: JSON.stringify({ entity, provider: "deepseek", context })
  });
  const payload = await response.json();
  if (!response.ok) throw new Error(payload.error || "Request failed");
  return payload;
}

function renderDedupeSuggestions(suggestions) {
  if (!els.dedupeOutput) return;
  if (!suggestions || !suggestions.length) {
    els.dedupeOutput.textContent = "No pending duplicate suggestions.";
    return;
  }
  els.dedupeOutput.innerHTML = suggestions.map(item => `
    <div class="dedupe-group" data-suggestion="${item.id}">
      <strong>${escapeHtml(item.canonical)}</strong>
      <div>${escapeHtml(item.entities.join(", "))}</div>
      <span>${Math.round(Number(item.confidence || 0) * 100)}% - ${escapeHtml(item.reason || "Possible duplicate")}</span>
      <div class="dedupe-actions">
        <button type="button" data-dedupe-action="approve" data-id="${item.id}">Approve merge</button>
        <button type="button" data-dedupe-action="reject" data-id="${item.id}">Reject</button>
      </div>
    </div>
  `).join("");
}

async function loadDedupeSuggestions() {
  const response = await fetch("/api/dedupe-suggestions", { headers: adminHeaders() });
  const payload = await response.json();
  if (!response.ok) throw new Error(payload.error || "Could not load dedupe suggestions");
  renderDedupeSuggestions(payload.suggestions || []);
}

async function decideDedupeSuggestion(id, action) {
  const activityId = addActivity("dedupe", action === "approve" ? "Approving duplicate merge" : "Rejecting duplicate suggestion", `Suggestion ${id}`, "running");
  const response = await fetch(`/api/dedupe-suggestions/${id}/${action}`, {
    method: "POST",
    headers: adminHeaders()
  });
  const payload = await response.json();
  if (!response.ok) {
    updateActivity(activityId, { status: "failed", detail: payload.error || "Dedupe decision failed" });
    throw new Error(payload.error || "Dedupe decision failed");
  }
  if (payload.data) hydrateGraph(payload.data);
  renderDedupeSuggestions(payload.suggestions || []);
  updateUi();
  updateActivity(activityId, { status: "done", detail: action === "approve" ? "Merged into SQLite graph" : "Marked rejected" });
  setStatus(action === "approve" ? "Merged approved duplicate group." : "Rejected duplicate suggestion.");
}

async function expandEntity(entity, options = {}) {
  const node = ensureNode(entity);
  if (!node) return;
  if (state.expanding.has(node.id)) return;
  const activityId = options.activityId || addActivity("expand", `Expand ${node.name}`, "Waiting for DeepSeek", "queued");
  const context = expansionContextFor(node);
  const contextCount = context.discoveredFrom.length + context.connectedTo.length;
  state.expanding.add(node.id);
  state.selected = { type: "node", id: node.id };
  updateActivity(activityId, {
    status: "running",
    title: `Expand ${node.name}`,
    detail: contextCount ? `DeepSeek using ${contextCount} local context clues` : "DeepSeek request in progress"
  });
  setStatus(`Expanding ${node.name}...`);
  updateUi();
  try {
    const beforeNodeCount = state.nodes.size;
    const beforeEdgeCount = state.edges.size + state.hiddenEdges.size;
    const beforeObservationCount = state.observations.length;
    const payload = await fetchInfluences(node.name, context);
    if (payload.debugPrompt) updateActivity(activityId, { prompt: payload.debugPrompt });
    const data = payload.data;
    const canonicalNode = renameNode(node, data.entity);
    const canonicalEntity = canonicalNode ? canonicalNode.name : displayName(data.entity || node.name);
    const relationships = [...(data.influencedBy || []), ...(data.influenced || [])];

    for (const item of data.influencedBy || []) {
      addObservation(item.entity, canonicalEntity, item.confidence, {
        sourceEntity: canonicalEntity,
        provider: payload.provider,
        model: payload.model
      });
    }
    for (const item of data.influenced || []) {
      addObservation(canonicalEntity, item.entity, item.confidence, {
        sourceEntity: canonicalEntity,
        provider: payload.provider,
        model: payload.model
      });
    }

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

    const wasExpanded = state.expanded.has(keyFor(canonicalEntity));
    state.expanded.add(keyFor(canonicalEntity));
    const shapeChanged = addedNodes > 0 || addedEdges > 0;
    const resultTitle = shapeChanged
      ? `${wasExpanded ? "Sampled" : "Expanded"} ${canonicalEntity}`
      : `Reinforced ${canonicalEntity}`;
    const resultDetail = shapeChanged
      ? `${payload.provider}/${payload.model} added ${addedObservations} observations, ${addedNodes} entities, ${addedEdges} edges`
      : `${payload.provider}/${payload.model} added ${addedObservations} observation${addedObservations === 1 ? "" : "s"} to existing links; graph shape unchanged`;
    updateActivity(activityId, {
      status: "done",
      title: resultTitle,
      detail: resultDetail
    });
    setStatus(shapeChanged
      ? `${wasExpanded ? "Sampled" : "Expanded"} ${canonicalEntity} using ${payload.provider}/${payload.model}.`
      : `${canonicalEntity} returned existing relationships only; confidence evidence was reinforced but the map shape did not grow.`);
    scheduleSave();
    window.setTimeout(scheduleSave, 2500);
  } catch (error) {
    updateActivity(activityId, { status: "failed", detail: error.message || "Expansion failed" });
    throw error;
  } finally {
    state.expanding.delete(node.id);
    state.expanding.delete(keyFor(entity));
    updateUi();
  }
}

async function searchEntity(entity) {
  const query = displayName(entity);
  if (!query) return;
  const existing = findNodeByQuery(query);
  if (existing) {
    focusNode(existing, 1.15);
    if (!state.expanded.has(existing.id)) {
      await expandEntity(existing.name);
      focusNode(existing, 1.15);
    }
    return;
  }
  setStatus(`No existing node found for "${query}". Expanding it now...`);
  await expandEntity(query);
  const created = findNodeByQuery(query);
  if (created) focusNode(created, 1.15);
}

function getFrontier() {
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
  const budget = Math.max(1, Number(els.autoBudget.value) || 1);
  const queued = getFrontier().slice(0, budget).map(node => ({
    node,
    activityId: addActivity("expand", `Expand ${node.name}`, "Queued by auto frontier", "queued")
  }));
  if (!queued.length) {
    setStatus("Frontier is empty.");
    return;
  }
  for (let i = 0; i < budget; i += 1) {
    const queuedItem = queued[i];
    const next = queuedItem ? queuedItem.node : getFrontier()[0];
    if (!next) {
      setStatus("Frontier is empty.");
      return;
    }
    await expandEntity(next.name, { activityId: queuedItem && queuedItem.activityId });
  }
}

function simulatePhysics() {
  const nodes = [...state.nodes.values()];
  const edges = [...state.edges.values(), ...state.hiddenEdges.values()];
  for (const node of nodes) {
    node.vx *= 0.86;
    node.vy *= 0.86;
  }

  for (let i = 0; i < nodes.length; i += 1) {
    for (let j = i + 1; j < nodes.length; j += 1) {
      const a = nodes[i];
      const b = nodes[j];
      const dx = a.x - b.x;
      const dy = a.y - b.y;
      const distSq = Math.max(80, dx * dx + dy * dy);
      const force = 1800 / distSq;
      const dist = Math.sqrt(distSq);
      const fx = (dx / dist) * force;
      const fy = (dy / dist) * force;
      a.vx += fx;
      a.vy += fy;
      b.vx -= fx;
      b.vy -= fy;
    }
  }

  for (const edge of edges) {
    const dx = edge.to.x - edge.from.x;
    const dy = edge.to.y - edge.from.y;
    const dist = Math.max(1, Math.sqrt(dx * dx + dy * dy));
    const hidden = edge.confidence < state.minConfidence;
    const target = 120 + (1 - edge.confidence) * 90;
    const force = (dist - target) * (hidden ? 0.0012 : 0.003);
    const fx = (dx / dist) * force;
    const fy = (dy / dist) * force;
    edge.from.vx += fx;
    edge.from.vy += fy;
    edge.to.vx -= fx;
    edge.to.vy -= fy;
  }

  for (const node of nodes) {
    if (state.dragging === node.id) continue;
    node.x += node.vx;
    node.y += node.vy;
  }
}

function drawArrow(from, to, confidence, count, selected, hidden = false) {
  const dx = to.x - from.x;
  const dy = to.y - from.y;
  const dist = Math.max(1, Math.sqrt(dx * dx + dy * dy));
  const ux = dx / dist;
  const uy = dy / dist;
  const startX = from.x + ux * (nodeRadius(from) + 3);
  const startY = from.y + uy * (nodeRadius(from) + 3);
  const endX = to.x - ux * (nodeRadius(to) + 5);
  const endY = to.y - uy * (nodeRadius(to) + 5);
  const width = 1 + confidence * 3 + Math.min(2, Math.log2(Math.max(1, count)) * 0.7);
  const weak = confidence < 0.35;
  const dashed = hidden || (weak && !selected);

  ctx.strokeStyle = hidden
    ? "rgba(102, 112, 109, 0.18)"
    : selected
      ? "#b4472f"
      : `rgba(31, 35, 38, ${weak ? 0.16 + confidence * 0.34 : 0.2 + confidence * 0.48})`;
  ctx.lineWidth = hidden ? 1 : weak ? Math.max(1, width * 0.72) : width;
  if (dashed) ctx.setLineDash([5, 7]);
  ctx.beginPath();
  ctx.moveTo(startX, startY);
  ctx.lineTo(endX, endY);
  ctx.stroke();
  if (dashed) ctx.setLineDash([]);

  const size = 7 + confidence * 3;
  ctx.fillStyle = ctx.strokeStyle;
  ctx.beginPath();
  ctx.moveTo(endX, endY);
  ctx.lineTo(endX - ux * size - uy * size * 0.55, endY - uy * size + ux * size * 0.55);
  ctx.lineTo(endX - ux * size + uy * size * 0.55, endY - uy * size - ux * size * 0.55);
  ctx.closePath();
  ctx.fill();
}

function draw() {
  simulatePhysics();
  const width = canvas.getBoundingClientRect().width;
  const height = canvas.getBoundingClientRect().height;
  ctx.clearRect(0, 0, width, height);
  ctx.save();
  ctx.translate(state.pan.x, state.pan.y);
  ctx.scale(state.zoom, state.zoom);

  for (const edge of state.hiddenEdges.values()) {
    drawArrow(edge.from, edge.to, edge.confidence, edge.count, false, true);
  }

  for (const [key, edge] of state.edges) {
    drawArrow(edge.from, edge.to, edge.confidence, edge.count, state.selected && state.selected.type === "edge" && state.selected.key === key);
  }

  for (const node of state.nodes.values()) {
    const expanded = state.expanded.has(node.id);
    const expanding = state.expanding.has(node.id);
    const selected = state.selected && state.selected.type === "node" && state.selected.id === node.id;
    const radius = nodeRadius(node);
    if (expanding) {
      const pulse = 5 + Math.sin(Date.now() / 180) * 3;
      ctx.strokeStyle = "rgba(180, 71, 47, 0.42)";
      ctx.lineWidth = 3;
      ctx.beginPath();
      ctx.arc(node.x, node.y, radius + 9 + pulse, 0, Math.PI * 2);
      ctx.stroke();
    }
    ctx.fillStyle = expanded ? "#35a186" : "#f0b84d";
    ctx.strokeStyle = expanding || selected ? "#b4472f" : "#1f2326";
    ctx.lineWidth = expanding || selected ? 4 : 1.5;
    ctx.beginPath();
    ctx.arc(node.x, node.y, selected ? radius + 3 : radius, 0, Math.PI * 2);
    ctx.fill();
    ctx.stroke();

    ctx.fillStyle = "#1f2326";
    ctx.font = "13px system-ui, sans-serif";
    ctx.textAlign = "center";
    ctx.textBaseline = "top";
    const label = node.name.length > 28 ? `${node.name.slice(0, 25)}...` : node.name;
    ctx.fillText(label, node.x, node.y + radius + 7);
  }

  ctx.restore();
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
    els.selectedDetails.textContent = "Click a node or edge.";
    return;
  }
  if (state.selected.type === "node") {
    const node = state.nodes.get(state.selected.id);
    const incoming = [...state.edges.values()].filter(edge => edge.to.id === node.id).length;
    const outgoing = [...state.edges.values()].filter(edge => edge.from.id === node.id).length;
    const hiddenIncoming = [...state.hiddenEdges.values()].filter(edge => edge.to.id === node.id).length;
    const hiddenOutgoing = [...state.hiddenEdges.values()].filter(edge => edge.from.id === node.id).length;
    const hiddenLine = hiddenIncoming || hiddenOutgoing
      ? `<br>Filtered low-confidence links: ${hiddenIncoming} incoming, ${hiddenOutgoing} outgoing`
      : "";
    const aliases = node.aliases && node.aliases.length ? `<br>Aliases: ${node.aliases.slice(0, 4).join(", ")}` : "";
    const isExpanding = state.expanding.has(node.id);
    const isExpanded = state.expanded.has(node.id);
    const stateLabel = isExpanding ? "Expanding now..." : isExpanded ? "Expanded" : "Not expanded yet";
    const actionLabel = isExpanding ? "Expanding..." : isExpanded ? "Sample again" : "Expand this entity";
    const actionHint = isExpanded && !isExpanding ? "<br><span>Runs another model pass and adds new observations without deleting existing ones.</span>" : "";
    const action = `<br><button id="expand-selected" type="button" ${isExpanding ? "disabled" : ""}>${actionLabel}</button>${actionHint}`;
    els.selectedDetails.innerHTML = `<strong>${node.name}</strong><br>Visible incoming: ${incoming}<br>Visible outgoing: ${outgoing}${hiddenLine}<br>Popularity: ${Number(node.weightedPopularity || 0).toFixed(2)} weighted observations<br>Relative size: ${popularityPercent(node)}% of current max${aliases}<br>${stateLabel}${action}`;
    const button = document.querySelector("#expand-selected");
    if (button) {
      button.addEventListener("click", () => expandEntity(node.name).catch(error => setStatus(error.message)));
    }
    return;
  }
  const edge = state.edges.get(state.selected.key);
  const rows = (edge.observations || []).slice(-8).reverse().map(obs => {
    const when = obs.createdAt ? new Date(obs.createdAt).toLocaleString() : "unknown time";
    const source = obs.sourceEntity ? ` from ${obs.sourceEntity}` : "";
    const model = obs.model ? ` via ${obs.model}` : "";
    return `<li>${Number(obs.confidence).toFixed(3)}${source}${model}<br><span>${when}</span></li>`;
  }).join("");
  els.selectedDetails.innerHTML = `<strong>${edge.from.name} -> ${edge.to.name}</strong><br>Average confidence: ${edge.confidence.toFixed(3)}<br>Observations: ${edge.count}<ul class="observation-list">${rows}</ul>`;
}

function updateUi() {
  recomputeEdges();
  els.nodeCount.textContent = state.nodes.size;
  els.edgeCount.textContent = state.edges.size;
  els.observationCount.textContent = state.observations.length;
  els.expandedCount.textContent = state.expanded.size;
  els.minConfidenceValue.textContent = Number(state.minConfidence).toFixed(2);
  updateSelectedDetails();
  if (state.view === "table") renderEntityTable();
}

canvas.addEventListener("pointerdown", event => {
  const point = graphPoint(event);
  const node = hitNode(point);
  if (node) {
    state.dragging = node.id;
    state.selected = { type: "node", id: node.id };
    canvas.setPointerCapture(event.pointerId);
    updateUi();
    return;
  }
  const edgeHit = hitEdge(point);
  if (edgeHit) {
    state.selected = { type: "edge", key: edgeHit.key };
    updateUi();
    return;
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
    return;
  }
  if (state.lastPointer) {
    state.pan.x += event.clientX - state.lastPointer.x;
    state.pan.y += event.clientY - state.lastPointer.y;
    state.lastPointer = { x: event.clientX, y: event.clientY };
  }
});

canvas.addEventListener("pointerup", () => {
  if (state.dragging || state.lastPointer) scheduleSave();
  state.dragging = null;
  state.lastPointer = null;
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

els.autoExpand.addEventListener("click", () => {
  autoExpandFrontier().catch(error => setStatus(error.message));
});

els.dedupeReview.addEventListener("click", async () => {
  const activityId = addActivity("dedupe", "Review possible duplicates", "DeepSeek review in progress", "running");
  els.dedupeOutput.textContent = "Reviewing likely duplicates and adding suggestions to the approval list...";
  try {
    const response = await fetch("/api/dedupe-review", {
      method: "POST",
      headers: adminHeaders()
    });
    const payload = await response.json();
    if (!response.ok) throw new Error(payload.error || "Dedupe review failed");
    if (payload.debugPrompt) updateActivity(activityId, { prompt: payload.debugPrompt });
    const suggestions = payload.pending || [];
    if (!suggestions.length) {
      els.dedupeOutput.textContent = `No confident duplicate groups found from ${payload.candidates.length} local candidates.`;
      updateActivity(activityId, { status: "done", detail: "No confident duplicate groups found" });
      return;
    }
    renderDedupeSuggestions(suggestions);
    updateActivity(activityId, { status: "done", detail: `${payload.stored || 0} suggestions added for approval` });
    setStatus(`Added ${payload.stored || 0} dedupe suggestions for approval.`);
  } catch (error) {
    updateActivity(activityId, { status: "failed", detail: error.message });
    els.dedupeOutput.textContent = error.message;
  }
});

els.dedupeOutput.addEventListener("click", event => {
  const button = event.target.closest("[data-dedupe-action]");
  if (!button) return;
  const id = button.getAttribute("data-id");
  const action = button.getAttribute("data-dedupe-action");
  button.disabled = true;
  button.textContent = action === "approve" ? "Merging..." : "Rejecting...";
  decideDedupeSuggestion(id, action).catch(error => {
    setStatus(error.message);
    loadDedupeSuggestions().catch(() => {});
  });
});

els.reset.addEventListener("click", () => {
  state.nodes.clear();
  state.observations = [];
  state.edges.clear();
  state.hiddenEdges.clear();
  state.expanded.clear();
  state.selected = null;
  state.pan = { x: 0, y: 0 };
  state.zoom = 1;
  setStatus("Reset.");
  scheduleSave();
  updateUi();
});

els.minConfidence.addEventListener("input", () => {
  state.minConfidence = Number(els.minConfidence.value);
  scheduleSave();
  updateUi();
});

window.addEventListener("resize", resizeCanvas);

resizeCanvas();
loadConfig().then(loadGraph);
draw();
