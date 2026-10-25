const { test } = require("node:test");
const assert = require("node:assert/strict");
const path = require("node:path");
const { Worker } = require("node:worker_threads");
const { createAnalysis } = require("../public/graph-analysis");

function graph(links) {
  links = links.map(pair => pair.map(id => id.toLowerCase()));
  const ids = [...new Set(links.flat())];
  const nodes = ids.map(id => ({ id, name: id }));
  const edges = links.map(([from, to]) => ({ key: `${from}=>${to}`, from, to, confidence: 0.8, count: 1 }));
  const observations = edges.map(edge => ({ from: edge.from, to: edge.to, confidence: edge.confidence, sourceEntity: edge.from }));
  return { nodes, edges, observations, expanded: [] };
}

test("bridge ranking counts connectivity partitions independent of drawing positions", () => {
  const snapshot = graph([["A", "B"], ["B", "C"], ["B", "D"], ["D", "C"], ["E", "F"]]);
  const engine = createAnalysis(snapshot);
  const regions = engine.removalRegionCounts();
  assert.equal(regions.get("b"), 2);
  assert.equal(regions.get("a"), 1);
  assert.equal(regions.get("c"), 1);
  assert.equal(regions.get("e"), 1);
  const before = engine.next(10).map(item => [item.node.id, item.value]);
  assert.equal(before.length, 6);
  snapshot.nodes.forEach((node, index) => { node.x = index * 1000; node.y = -index; });
  assert.deepEqual(createAnalysis(snapshot).next(10).map(item => [item.node.id, item.value]), before);
});

test("upstream traversal labels cycle boundaries and stays finite", () => {
  const engine = createAnalysis(graph([["A", "B"], ["B", "C"], ["C", "A"]]));
  const result = engine.roots("c");
  assert.ok(result.trace.roots.length > 0);
  assert.ok(result.trace.roots.some(root => root.terminalReason === "cycle boundary"));
  assert.ok(result.metrics.every(metric => Number.isFinite(metric.score)));
});

test("worker snapshots derive identical rankings from complete samples without transferred edges", () => {
  const snapshot = graph([["A", "B"], ["B", "C"], ["B", "D"], ["D", "C"]]);
  const reference = createAnalysis(snapshot);
  const derived = createAnalysis({ ...snapshot, edges: undefined });
  assert.deepEqual(derived.next(100), reference.next(100));
  assert.deepEqual(derived.roots("c").metrics, reference.roots("c").metrics);
  const roots = engine => engine.roots("c").trace.roots.map(({ node, score, terminalReason }) => [node.id, score, terminalReason]);
  assert.deepEqual(roots(derived), roots(reference));
});

test("the actual analysis worker processes independent snapshots and returns request versions", async t => {
  const analysisPath = path.resolve(__dirname, "../public/graph-analysis.js");
  const workerPath = path.resolve(__dirname, "../public/analysis-worker.js");
  const worker = new Worker(`const {parentPort}=require('node:worker_threads');global.self={postMessage:value=>parentPort.postMessage(value)};global.importScripts=()=>{self.InfluenceAnalysis=require(${JSON.stringify(analysisPath)})};require(${JSON.stringify(workerPath)});parentPort.on('message',data=>self.onmessage({data}));`, { eval: true });
  t.after(() => worker.terminate());
  const result = new Promise((resolve, reject) => { worker.once("message", resolve); worker.once("error", reject); });
  worker.postMessage({ id: 1, version: 9, kind: "next", graph: graph([["A", "B"], ["B", "C"]]) });
  const message = await result;
  assert.equal(message.id, 1);
  assert.equal(message.version, 9);
  assert.equal(message.result[0].node.id, "b");
});
