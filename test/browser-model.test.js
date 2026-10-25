const { test } = require("node:test");
const assert = require("node:assert/strict");
const fs = require("node:fs");
const vm = require("node:vm");
const path = require("node:path");
const { randomUUID } = require("node:crypto");

function browserModel() {
  let clock = 0, renders = 0, labels = 0;
  const storage = new Map(), elements = new Map(), calls = [];
  const canvasContext = new Proxy({ clearRect() { renders++; }, fillText() { labels++; } }, { get: (target, key) => target[key] || (() => {}) });
  const element = () => ({ value: "", textContent: "", innerHTML: "", dataset: {}, children: [], style: {},
    classList: { toggle() {}, add() {}, remove() {} }, addEventListener() {}, setAttribute() {},
    getBoundingClientRect: () => ({ width: 1200, height: 800, top: 0, left: 0 }), getContext: () => canvasContext,
    querySelector: () => element(), appendChild(row) { this.children.push(row); }
  });
  const select = selector => { if (!elements.has(selector)) elements.set(selector, element()); return elements.get(selector); };
  const context = vm.createContext({ document: { querySelector: select, querySelectorAll: () => [], createElement: element, body: element() },
    window: { location: { search: "" }, localStorage: { getItem: key => storage.get(key) || null, setItem: (key, value) => storage.set(key, value) },
      addEventListener() {}, setTimeout: () => 0, clearTimeout() {}, setInterval: () => 0, clearInterval() {} },
    URLSearchParams, performance: { now: () => clock }, crypto: { randomUUID }, requestAnimationFrame() {}, console,
    fetch: async (...args) => { calls.push(args); throw new Error("Unexpected network request"); }
  });
  const source = fs.readFileSync(path.resolve(__dirname, "../public/app.js"), "utf8").replace(/resizeCanvas\(\);\s*loadConfig\(\)\.then\(loadGraph\);\s*draw\(\);\s*$/, "");
  vm.runInContext(fs.readFileSync(path.resolve(__dirname, "../public/graph-model.js"), "utf8"), context);
  vm.runInContext(source, context);
  const run = code => vm.runInContext(code, context);
  return { run, context, select, calls, storage, setClock(value) { clock = value; }, get renders() { return renders; }, get labels() { return labels; } };
}

const graph = (revision = 1) => ({ revision, nodes: [{ name: "Alpha", uid: "a", x: 10, y: 20 }, { name: "Beta", uid: "b", x: 30, y: 40 }], observations: [
  { id: 1, from: "Alpha", to: "Beta", confidence: 0.1 }, { id: 2, from: "Alpha", to: "Beta", confidence: 0.9 }
], expanded: [] });

test("filtering applies to the complete edge average and retains its full sample history", () => {
  const model = browserModel(); model.context.fixture = graph();
  model.run("hydrateGraph(fixture);state.minConfidence=0.6;recomputeEdges()");
  assert.equal(model.run("state.edges.size"), 0);
  assert.equal(model.run("[...state.hiddenEdges.values()][0].confidence"), 0.5);
  assert.equal(model.run("([...state.edges.values(), ...state.hiddenEdges.values()])[0].count"), 2);
  model.run("state.minConfidence=0.4;recomputeEdges()");
  assert.equal(model.run("[...state.edges.values()][0].confidence"), 0.5);
  assert.equal(model.run("([...state.edges.values(), ...state.hiddenEdges.values()])[0].count"), 2);
});

test("table statistics retain all samples through filtering and update after new revisions", async () => {
  const model = browserModel(); model.context.fixture = graph();
  model.run("hydrateGraph(fixture);state.loaded=true;state.minConfidence=0.8;updateUi()");
  assert.equal(model.run("entityStats().find(node=>node.id==='alpha').outgoing"), 2);
  assert.equal(model.run("entityStats().find(node=>node.id==='beta').connectionCount"), 1);
  model.context.delta = { fromRevision: 1, revision: 2, nodes: [{ uid: "c", name: "Gamma" }],
    observations: [{ id: 3, from: "Beta", to: "Gamma", confidence: 0 }],
    removedNodeUids: [], removedObservationIds: [], expanded: [] };
  await model.run("acceptServerPayload({changes:[delta]})");
  assert.equal(model.run("entityStats().find(node=>node.id==='beta').observations"), 3);
  assert.equal(model.run("entityStats().find(node=>node.id==='beta').connectionCount"), 2);
  assert.equal(model.run("entityStats().find(node=>node.id==='gamma').incoming"), 1);
  assert.equal(model.run("([...state.edges.values(), ...state.hiddenEdges.values()]).find(edge=>edge.to.id==='gamma').confidence"), 0);
});

test("zero scores survive hydration and zoom/view saves never send graph writes", async () => {
  const model = browserModel(); model.context.fixture = graph();
  model.context.fixture.observations[0].confidence = 0;
  model.run("hydrateGraph(fixture);state.loaded=true;state.zoom=0.04;state.pan={x:50,y:25}");
  assert.equal(model.run("state.observations[0].confidence"), 0);
  await model.run("saveView()");
  const view = JSON.parse(model.storage.get("influenceMapView"));
  assert.equal(view.zoom, 0.04);
  assert.equal(view.observations, undefined);
  assert.equal(view.nodes, undefined);
  assert.equal(model.calls.length, 0);
  model.run("state.loaded=false;hydrateGraph(fixture)");
  assert.equal(model.run("state.zoom"), 0.04);
});

test("names, aliases, model metadata and table attributes escape stored HTML", () => {
  const model = browserModel(); model.context.fixture = graph();
  model.context.fixture.nodes[0].name = '<img src=x onerror="alert(1)">';
  model.context.fixture.nodes[0].aliases = ['<svg onload="alert(2)">'];
  for (const observation of model.context.fixture.observations) {
    observation.from = model.context.fixture.nodes[0].name;
    observation.model = '<img src=x onerror="alert(3)">';
    observation.sourceEntity = '<svg onload="alert(4)">';
  }
  model.run("hydrateGraph(fixture);state.selected={type:'node',id:[...state.nodes.keys()][0]};updateSelectedDetails();renderEntityTable()");
  assert.ok(model.select("#selected-details").innerHTML.includes("&lt;img"));
  assert.ok(!model.select("#selected-details").innerHTML.includes("<svg"));
  assert.ok(model.select("#entity-table-body").children.every(row => !row.innerHTML.includes("<img")));
  model.run("state.selected={type:'edge',key:[...state.edges.keys()][0]};updateSelectedDetails()");
  assert.ok(!model.select("#selected-details").innerHTML.includes("<img"));
  assert.ok(!model.select("#selected-details").innerHTML.includes("<svg"));
});

test("late full snapshots cannot overwrite newer graph revisions", async () => {
  const model = browserModel(); model.context.newer = graph(3); model.context.older = graph(2);
  model.context.newer.nodes.push({ name: "Gamma", uid: "c" });
  model.run("hydrateGraph(newer);state.loaded=true");
  await model.run("acceptServerPayload({data:older})");
  assert.equal(model.run("state.nodes.size"), 3);
  assert.equal(model.run("state.revision"), 3);
});

test("incremental changes preserve stable selection and reject out-of-order older changes", async () => {
  const model = browserModel(); model.context.fixture = graph();
  model.run("hydrateGraph(fixture);state.loaded=true;state.selected={type:'node',id:'alpha'}");
  model.context.delta = { fromRevision: 1, revision: 2, nodes: [{ uid: "a", name: "Renamed Alpha", id: "renamed alpha" }], observations: [
    { id: 1, from: "Renamed Alpha", to: "Beta", confidence: 0.1 }, { id: 2, from: "Renamed Alpha", to: "Beta", confidence: 0.9 }
  ], removedNodeUids: [], removedObservationIds: [], expanded: ["renamed alpha"] };
  await model.run("acceptServerPayload({changes:[delta]})");
  assert.equal(model.run("state.nodes.size"), 2);
  assert.equal(model.run("state.selected.id"), "renamed alpha");
  assert.equal(model.run("state.observations.length"), 2);
  await model.run("acceptServerPayload({changes:[delta]})");
  assert.equal(model.run("state.observations.length"), 2);
});

test("idle rendering restores labels after interaction expires and tab changes invalidate overlays", () => {
  const model = browserModel();
  model.run("hydrateGraph({revision:1,nodes:Array.from({length:1201},(_,i)=>({uid:String(i),name:'Node '+i,x:(i%40)*25,y:Math.floor(i/40)*25})),observations:[]});markInteracting(260);draw(0)");
  assert.equal(model.labels, 0);
  const renders = model.renders;
  model.setClock(1000); model.run("draw(1000)");
  assert.ok(model.renders > renders);
  assert.ok(model.labels > 0);
  model.run("state.renderDirty=false;setPanelTab('roots')");
  assert.equal(model.run("state.renderDirty"), true);
});

test("ambiguous searches expose choices and never generate or silently choose an identity", async () => {
  const model = browserModel();
  model.run("hydrateGraph({revision:1,nodes:[{name:'Rust (programming language)',uid:'language',aliases:['Rust']},{name:'Rust (video game)',uid:'game'}],observations:[]});state.loaded=true;state.canWrite=true;setView('table')");
  await model.run("searchEntity('Rust')");
  assert.equal(model.run("state.view"), "graph");
  assert.ok(model.select("#selected-details").innerHTML.includes("Rust (programming language)"));
  assert.ok(model.select("#selected-details").innerHTML.includes("Rust (video game)"));
  assert.equal(model.calls.length, 0);
});

module.exports = { browserModel };
