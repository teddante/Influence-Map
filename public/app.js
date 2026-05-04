const canvas = document.querySelector("#graph-canvas");
const ctx = canvas.getContext("2d");

const state = {
  nodes: new Map(),
  observations: [],
  edges: new Map(),
  expanded: new Set(),
  selected: null,
  dragging: null,
  pan: { x: 0, y: 0 },
  lastPointer: null,
  minConfidence: 0.35,
  loaded: false,
  saveTimer: null,
  saveVersion: 0
};

const els = {
  form: document.querySelector("#expand-form"),
  input: document.querySelector("#entity-input"),
  provider: document.querySelector("#provider-select"),
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
  frontierList: document.querySelector("#frontier-list"),
  tokensPerCall: document.querySelector("#tokens-per-call"),
  pricePerMillion: document.querySelector("#price-per-million"),
  costOutput: document.querySelector("#cost-output")
};

function keyFor(name) {
  return String(name || "").trim().replace(/\s+/g, " ").toLowerCase();
}

function displayName(name) {
  return String(name || "").trim().replace(/\s+/g, " ");
}

function edgeKey(from, to) {
  return `${keyFor(from)}=>${keyFor(to)}`;
}

function ensureNode(name) {
  const id = keyFor(name);
  if (!id) return null;
  if (!state.nodes.has(id)) {
    const angle = Math.random() * Math.PI * 2;
    const radius = 80 + Math.random() * 180;
    state.nodes.set(id, {
      id,
      name: displayName(name),
      x: canvas.width / 2 + Math.cos(angle) * radius,
      y: canvas.height / 2 + Math.sin(angle) * radius,
      vx: 0,
      vy: 0
    });
  }
  return state.nodes.get(id);
}

function addObservation(from, to, confidence) {
  const source = ensureNode(from);
  const target = ensureNode(to);
  if (!source || !target || source.id === target.id) return;
  state.observations.push({
    from: source.name,
    to: target.name,
    confidence: Math.max(0, Math.min(1, Number(confidence) || 0.5))
  });
  recomputeEdges();
}

function recomputeEdges() {
  const grouped = new Map();
  for (const obs of state.observations) {
    if (obs.confidence < state.minConfidence) continue;
    const key = edgeKey(obs.from, obs.to);
    const current = grouped.get(key) || {
      from: ensureNode(obs.from),
      to: ensureNode(obs.to),
      sum: 0,
      count: 0
    };
    current.sum += obs.confidence;
    current.count += 1;
    grouped.set(key, current);
  }
  state.edges = new Map(
    [...grouped].map(([key, edge]) => [
      key,
      {
        from: edge.from,
        to: edge.to,
        confidence: edge.sum / edge.count,
        count: edge.count
      }
    ])
  );
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
      x: node.x,
      y: node.y
    })),
    observations: state.observations.map(observation => ({ ...observation })),
    expanded: [...state.expanded],
    pan: { ...state.pan },
    minConfidence: state.minConfidence
  };
}

function hydrateGraph(graph) {
  const minConfidence = Number(graph && graph.minConfidence);
  state.nodes.clear();
  state.observations = [];
  state.edges.clear();
  state.expanded.clear();
  state.selected = null;
  state.pan = {
    x: Number(graph && graph.pan && graph.pan.x) || 0,
    y: Number(graph && graph.pan && graph.pan.y) || 0
  };
  state.minConfidence = Number.isFinite(minConfidence) ? Math.max(0, Math.min(1, minConfidence)) : 0.35;
  els.minConfidence.value = state.minConfidence;

  for (const node of Array.isArray(graph && graph.nodes) ? graph.nodes : []) {
    const hydrated = ensureNode(node.name);
    if (!hydrated) continue;
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
      confidence: Math.max(0, Math.min(1, Number(observation.confidence) || 0.5))
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
  const provider = els.provider.value === "mock" ? "mock" : "deepseek";
  const response = await fetch("/api/influences", {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({ entity, provider })
  });
  const payload = await response.json();
  if (!response.ok) throw new Error(payload.error || "Request failed");
  return payload;
}

async function expandEntity(entity) {
  const node = ensureNode(entity);
  if (!node) return;
  setStatus(`Expanding ${node.name}...`);
  const payload = await fetchInfluences(node.name);
  const data = payload.data;

  for (const item of data.influencedBy || []) {
    addObservation(item.entity, data.entity, item.confidence);
  }
  for (const item of data.influenced || []) {
    addObservation(data.entity, item.entity, item.confidence);
  }

  state.expanded.add(keyFor(data.entity));
  setStatus(`Expanded ${data.entity} using ${payload.provider}/${payload.model}.`);
  scheduleSave();
  updateUi();
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

function drawArrow(from, to, confidence, selected) {
  const dx = to.x - from.x;
  const dy = to.y - from.y;
  const dist = Math.max(1, Math.sqrt(dx * dx + dy * dy));
  const ux = dx / dist;
  const uy = dy / dist;
  const startX = from.x + ux * 20;
  const startY = from.y + uy * 20;
  const endX = to.x - ux * 22;
  const endY = to.y - uy * 22;
  const width = 1 + confidence * 3;

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

  for (const [key, edge] of state.edges) {
    drawArrow(edge.from, edge.to, edge.confidence, state.selected && state.selected.type === "edge" && state.selected.key === key);
  }

  for (const node of state.nodes.values()) {
    const expanded = state.expanded.has(node.id);
    const selected = state.selected && state.selected.type === "node" && state.selected.id === node.id;
    ctx.fillStyle = expanded ? "#35a186" : "#f0b84d";
    ctx.strokeStyle = selected ? "#b4472f" : "#1f2326";
    ctx.lineWidth = selected ? 4 : 1.5;
    ctx.beginPath();
    ctx.arc(node.x, node.y, selected ? 20 : 17, 0, Math.PI * 2);
    ctx.fill();
    ctx.stroke();

    ctx.fillStyle = "#1f2326";
    ctx.font = "13px system-ui, sans-serif";
    ctx.textAlign = "center";
    ctx.textBaseline = "top";
    const label = node.name.length > 28 ? `${node.name.slice(0, 25)}...` : node.name;
    ctx.fillText(label, node.x, node.y + 23);
  }

  ctx.restore();
  requestAnimationFrame(draw);
}

function graphPoint(event) {
  const rect = canvas.getBoundingClientRect();
  return {
    x: event.clientX - rect.left - state.pan.x,
    y: event.clientY - rect.top - state.pan.y
  };
}

function hitNode(point) {
  for (const node of [...state.nodes.values()].reverse()) {
    const dx = point.x - node.x;
    const dy = point.y - node.y;
    if (Math.sqrt(dx * dx + dy * dy) <= 24) return node;
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
    if (distanceToSegment(point, edge.from, edge.to) < 8) return { key, edge };
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
    els.selectedDetails.innerHTML = `<strong>${node.name}</strong><br>Incoming: ${incoming}<br>Outgoing: ${outgoing}<br>${state.expanded.has(node.id) ? "Expanded" : "Not expanded yet"}`;
    return;
  }
  const edge = state.edges.get(state.selected.key);
  els.selectedDetails.innerHTML = `<strong>${edge.from.name} -> ${edge.to.name}</strong><br>Average confidence: ${edge.confidence.toFixed(3)}<br>Observations: ${edge.count}`;
}

function updateCost() {
  const tokens = Math.max(0, Number(els.tokensPerCall.value) || 0);
  const price = Math.max(0, Number(els.pricePerMillion.value) || 0);
  const expandedCost = state.expanded.size * tokens / 1_000_000 * price;
  const budget = Math.max(0, Number(els.autoBudget.value) || 0);
  const budgetCost = budget * tokens / 1_000_000 * price;
  els.costOutput.innerHTML = `Current expanded cost: <strong>$${expandedCost.toFixed(4)}</strong><br>Next auto run: <strong>$${budgetCost.toFixed(4)}</strong><br>Per 1,000 expansions: <strong>$${(tokens * 1000 / 1_000_000 * price).toFixed(2)}</strong>`;
}

function updateUi() {
  recomputeEdges();
  els.nodeCount.textContent = state.nodes.size;
  els.edgeCount.textContent = state.edges.size;
  els.observationCount.textContent = state.observations.length;
  els.expandedCount.textContent = state.expanded.size;
  els.minConfidenceValue.textContent = Number(state.minConfidence).toFixed(2);
  updateSelectedDetails();
  updateCost();

  const frontier = getFrontier().slice(0, 30);
  els.frontierList.innerHTML = "";
  for (const node of frontier) {
    const button = document.createElement("button");
    button.type = "button";
    button.textContent = node.name;
    button.addEventListener("click", () => expandEntity(node.name).catch(error => setStatus(error.message)));
    els.frontierList.appendChild(button);
  }
  if (!frontier.length) {
    els.frontierList.textContent = "No frontier yet.";
  }
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

canvas.addEventListener("dblclick", event => {
  const node = hitNode(graphPoint(event));
  if (node) expandEntity(node.name).catch(error => setStatus(error.message));
});

els.form.addEventListener("submit", event => {
  event.preventDefault();
  expandEntity(els.input.value).catch(error => setStatus(error.message));
});

els.autoExpand.addEventListener("click", () => {
  autoExpandFrontier().catch(error => setStatus(error.message));
});

els.reset.addEventListener("click", () => {
  state.nodes.clear();
  state.observations = [];
  state.edges.clear();
  state.expanded.clear();
  state.selected = null;
  state.pan = { x: 0, y: 0 };
  setStatus("Reset.");
  scheduleSave();
  updateUi();
});

els.minConfidence.addEventListener("input", () => {
  state.minConfidence = Number(els.minConfidence.value);
  scheduleSave();
  updateUi();
});

els.tokensPerCall.addEventListener("input", updateCost);
els.pricePerMillion.addEventListener("input", updateCost);
els.autoBudget.addEventListener("input", updateCost);
window.addEventListener("resize", resizeCanvas);

resizeCanvas();
loadGraph();
draw();
