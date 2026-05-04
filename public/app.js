const canvas = document.querySelector("#graph-canvas");
const ctx = canvas.getContext("2d");

const state = {
  nodes: new Map(),
  observations: [],
  edges: new Map(),
  expanded: new Set(),
  selected: null,
  devTools: false,
  expanding: new Set(),
  dragging: null,
  pan: { x: 0, y: 0 },
  zoom: 1,
  lastPointer: null,
  minConfidence: 0.35,
  loaded: false,
  saveTimer: null,
  saveVersion: 0,
  maxWeightedPopularity: 0
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
  status: document.querySelector("#status"),
  nodeCount: document.querySelector("#node-count"),
  edgeCount: document.querySelector("#edge-count"),
  observationCount: document.querySelector("#observation-count"),
  expandedCount: document.querySelector("#expanded-count"),
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
  const grouped = new Map();
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
    if (obs.confidence < state.minConfidence) continue;
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
  state.edges = new Map(
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
  state.expanded.clear();
  state.selected = null;
  state.pan = {
    x: Number(graph && graph.pan && graph.pan.x) || 0,
    y: Number(graph && graph.pan && graph.pan.y) || 0
  };
  state.zoom = Number.isFinite(zoom) ? clampZoom(zoom) : 1;
  state.minConfidence = Number.isFinite(minConfidence) ? Math.max(0, Math.min(1, minConfidence)) : 0.35;
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

async function fetchInfluences(entity) {
  const response = await fetch("/api/influences", {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({ entity, provider: "deepseek" })
  });
  const payload = await response.json();
  if (!response.ok) throw new Error(payload.error || "Request failed");
  return payload;
}

async function expandEntity(entity) {
  const node = ensureNode(entity);
  if (!node) return;
  if (state.expanding.has(node.id)) return;
  state.expanding.add(node.id);
  state.selected = { type: "node", id: node.id };
  setStatus(`Expanding ${node.name}...`);
  updateUi();
  try {
    const payload = await fetchInfluences(node.name);
    const data = payload.data;

    for (const item of data.influencedBy || []) {
      addObservation(item.entity, data.entity, item.confidence, {
        sourceEntity: data.entity,
        provider: payload.provider,
        model: payload.model
      });
    }
    for (const item of data.influenced || []) {
      addObservation(data.entity, item.entity, item.confidence, {
        sourceEntity: data.entity,
        provider: payload.provider,
        model: payload.model
      });
    }

    const wasExpanded = state.expanded.has(keyFor(data.entity));
    state.expanded.add(keyFor(data.entity));
    setStatus(`${wasExpanded ? "Sampled" : "Expanded"} ${data.entity} using ${payload.provider}/${payload.model}.`);
    scheduleSave();
    window.setTimeout(scheduleSave, 2500);
  } finally {
    state.expanding.delete(node.id);
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
  for (let i = 0; i < budget; i += 1) {
    const next = getFrontier()[0];
    if (!next) {
      setStatus("Frontier is empty.");
      return;
    }
    await expandEntity(next.name);
  }
}

function simulatePhysics() {
  const nodes = [...state.nodes.values()];
  const edges = [...state.edges.values()];
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
    const target = 120 + (1 - edge.confidence) * 90;
    const force = (dist - target) * 0.003;
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

function drawArrow(from, to, confidence, count, selected) {
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

  ctx.strokeStyle = selected ? "#b4472f" : `rgba(31, 35, 38, ${0.2 + confidence * 0.48})`;
  ctx.lineWidth = width;
  ctx.beginPath();
  ctx.moveTo(startX, startY);
  ctx.lineTo(endX, endY);
  ctx.stroke();

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
    const aliases = node.aliases && node.aliases.length ? `<br>Aliases: ${node.aliases.slice(0, 4).join(", ")}` : "";
    const isExpanding = state.expanding.has(node.id);
    const isExpanded = state.expanded.has(node.id);
    const stateLabel = isExpanding ? "Expanding now..." : isExpanded ? "Expanded" : "Not expanded yet";
    const actionLabel = isExpanding ? "Expanding..." : isExpanded ? "Sample again" : "Expand this entity";
    const actionHint = isExpanded && !isExpanding ? "<br><span>Runs another model pass and adds new observations without deleting existing ones.</span>" : "";
    const action = `<br><button id="expand-selected" type="button" ${isExpanding ? "disabled" : ""}>${actionLabel}</button>${actionHint}`;
    els.selectedDetails.innerHTML = `<strong>${node.name}</strong><br>Incoming: ${incoming}<br>Outgoing: ${outgoing}<br>Popularity: ${Number(node.weightedPopularity || 0).toFixed(2)} weighted observations<br>Relative size: ${popularityPercent(node)}% of current max${aliases}<br>${stateLabel}${action}`;
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

els.autoExpand.addEventListener("click", () => {
  autoExpandFrontier().catch(error => setStatus(error.message));
});

els.dedupeReview.addEventListener("click", async () => {
  els.dedupeOutput.textContent = "Reviewing likely duplicates...";
  try {
    const response = await fetch("/api/dedupe-review", {
      method: "POST",
      headers: adminHeaders()
    });
    const payload = await response.json();
    if (!response.ok) throw new Error(payload.error || "Dedupe review failed");
    const groups = payload.review && payload.review.groups ? payload.review.groups : [];
    if (!groups.length) {
      els.dedupeOutput.textContent = `No confident duplicate groups found from ${payload.candidates.length} local candidates.`;
      return;
    }
    els.dedupeOutput.innerHTML = groups.map(group => (
      `<div class="dedupe-group"><strong>${group.canonical}</strong><br>${group.entities.join(", ")}<br><span>${Math.round(group.confidence * 100)}% - ${group.reason}</span></div>`
    )).join("");
  } catch (error) {
    els.dedupeOutput.textContent = error.message;
  }
});

els.reset.addEventListener("click", () => {
  state.nodes.clear();
  state.observations = [];
  state.edges.clear();
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
