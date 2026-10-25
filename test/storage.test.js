const { test } = require("node:test");
const assert = require("node:assert/strict");
const { DatabaseSync } = require("node:sqlite");
const { GraphStore } = require("../lib/graph-store");
const { initDb } = require("../lib/schema");
const { WorkQueue, GenerationBudget } = require("../lib/http-safety");

function fixture(t) {
  const db = new DatabaseSync(":memory:");
  t.after(() => db.close());
  initDb(db);
  const store = new GraphStore(db, value => String(value || "").trim().toLowerCase());
  store.importGraph({ nodes: [{ name: "Alpha" }, { name: "Beta" }], observations: [
    { from: "Alpha", to: "Beta", confidence: 0, provider: "fixture", sourceEntity: "Original wording" }
  ] });
  return { db, store };
}

test("renames retain permanent entity and sample identities and historical provenance", t => {
  const { store } = fixture(t);
  const before = store.graph();
  store.edit({ action: "rename", canonical: "Alpha (band)", entities: ["Alpha"] }, before.revision);
  const after = store.graph();
  assert.equal(after.nodes.find(n => n.name === "Alpha (band)").uid, before.nodes.find(n => n.name === "Alpha").uid);
  assert.equal(after.observations[0].id, before.observations[0].id);
  assert.equal(after.observations[0].confidence, 0);
  assert.equal(after.observations[0].sourceEntity, "Original wording");
});

test("append is durable and idempotent; stale destructive edits are rejected", t => {
  const { store } = fixture(t);
  const old = store.graph();
  const response = { provider: "mock", model: "fixture", data: { entity: "Beta", influencedBy: [], influenced: [{ entity: "Gamma", confidence: 0.9 }] } };
  store.appendExpansion("request-one", response);
  store.appendExpansion("request-one", response);
  assert.equal(store.graph().observations.length, 2);
  assert.equal(store.graph().observations[0].id, old.observations[0].id);
  assert.throws(() => store.reset(old.revision), { status: 409 });
  assert.equal(store.graph().observations.length, 2);
  const delta = store.changesSince(old.revision);
  assert.equal(delta.changes.length, 1);
  assert.equal(delta.changes[0].observations.length, 1);
});

test("split uses exact durable observation IDs, retains unknown provenance and can be undone", t => {
  const { store } = fixture(t);
  const before = store.graph(), observation = before.observations[0];
  const edit = store.edit({ action: "split", canonical: "Alpha (band)", entities: ["Alpha"], metadata: { moveObservationIds: [observation.id] } }, before.revision);
  assert.equal(store.graph().observations[0].from, "Alpha (band)");
  assert.equal(store.graph().observations[0].id, observation.id);
  store.undo(edit.eventId, store.revision);
  assert.deepEqual(store.graph().observations, before.observations);
  assert.deepEqual(store.graph().nodes, before.nodes);
});

test("invalid splits fail atomically and undo cannot discard subsequent expansions", t => {
  const { store } = fixture(t);
  const before = store.graph();
  assert.throws(() => store.edit({ action: "split", canonical: "Other", entities: ["Alpha"], metadata: { moveObservationIds: [999] } }), { status: 409 });
  assert.deepEqual(store.graph(), before);
  const edit = store.edit({ action: "rename", canonical: "Alpha renamed", entities: ["Alpha"] });
  store.appendExpansion("after-edit", { provider: "mock", data: { entity: "Gamma", influencedBy: [], influenced: [] } });
  assert.throws(() => store.undo(edit.eventId, store.revision), { status: 409 });
});

test("split review works when the model lists the canonical target before the source", t => {
  const { store } = fixture(t);
  const id = store.graph().observations[0].id;
  store.edit({ action: "split", canonical: "Alpha (band)", entities: ["Alpha (band)", "Alpha"], metadata: { moveObservationIds: [id] } });
  assert.equal(store.graph().observations[0].from, "Alpha (band)");
  assert.equal(store.graph().observations[0].id, id);
});

test("merge and reset are audited and reversible without changing sample IDs", t => {
  const { store } = fixture(t);
  const before = store.graph();
  const merge = store.edit({ action: "merge", canonical: "ALPHA", entities: ["Alpha", "Beta"] });
  assert.equal(store.graph().nodes.length, 1);
  assert.equal(store.graph().nodes[0].name, "ALPHA");
  store.undo(merge.eventId, store.revision);
  assert.deepEqual(store.graph().observations, before.observations);
  const reset = store.reset(store.revision);
  assert.equal(store.graph().nodes.length, 0);
  assert.equal(store.setting("legacyMigrationComplete"), "1");
  store.undo(reset.eventId, store.revision);
  assert.deepEqual(store.graph().observations, before.observations);
  assert.equal(store.history().length, 5);
});

test("missing provenance is explicitly unknown and survives import", t => {
  const { store } = fixture(t);
  store.appendExpansion("unknown-check", { provider: "", data: { entity: "Gamma", influencedBy: [{ entity: "Delta", confidence: 0.5 }], influenced: [] } });
  const sample = store.graph().observations.at(-1);
  assert.equal(sample.evidenceType, "unknown");
  assert.deepEqual(sample.sources, []);
});

test("model queue is bounded and disconnected queued work is removed", async () => {
  const queue = new WorkQueue(1, 1, 1000);
  const first = await queue.acquire();
  const cancellation = new AbortController();
  const second = queue.acquire(cancellation.signal);
  await assert.rejects(queue.acquire(), { status: 429 });
  cancellation.abort();
  await assert.rejects(second, { status: 499 });
  assert.equal(queue.queued, 0);
  first.release(); first.release();
  assert.equal(queue.active, 0);
});

test("queued work has a deadline and releases its queue entry without disturbing the active slot", async () => {
  const queue = new WorkQueue(1, 1, 20);
  const active = await queue.acquire();
  await assert.rejects(queue.acquire(), { status: 429 });
  assert.equal(queue.queued, 0);
  assert.equal(queue.active, 1);
  active.release();
  assert.equal(queue.active, 0);
});

test("request, token and configured dollar reservations survive constructing a new budget", t => {
  const { store } = fixture(t);
  const messages = [{ role: "user", content: "fixture" }];
  const requestBudget = new GenerationBudget(store, { requests: 1 });
  requestBudget.reserve(messages, 128);
  assert.throws(() => new GenerationBudget(store, { requests: 1 }).reserve(messages, 128), { status: 429 });
  const tokenBudget = new GenerationBudget(store, { tokens: 100 });
  assert.throws(() => tokenBudget.reserve(messages, 128), { status: 429 });
  const dollars = new GenerationBudget(store, { dollars: 0.00001, inputRate: 1, outputRate: 1 });
  assert.throws(() => dollars.reserve(messages, 128), { status: 429 });
  assert.throws(() => new GenerationBudget(store, { tokens: NaN }), /Invalid/);
});

test("legacy Rust correction separates only matching game samples and remains reversible", t => {
  const { store } = fixture(t);
  const { correctRustIdentity } = require("../lib/rust-identity-correction");
  const legacy = { observations: [0.9, 0.85, 0.9].map(confidence => ({ from: "DayZ", to: "Rust", confidence })) };
  store.mutate("fixture", {}, () => {
    for (const sample of legacy.observations) store.insertObservation({ ...sample, to: "Rust (programming language)" });
    store.insertObservation({ from: "C++", to: "Rust (programming language)", confidence: 0.9, provider: "fixture" });
  });
  const before = store.graph();
  const correction = correctRustIdentity(store, legacy);
  assert.equal(correction.changed, true);
  const after = store.graph();
  assert.equal(after.observations.filter(sample => sample.to === "Rust (video game)").length, 3);
  assert.equal(after.observations.find(sample => sample.from === "C++").to, "Rust (programming language)");
  assert.deepEqual(after.observations.map(sample => sample.id), before.observations.map(sample => sample.id));
  assert.ok(after.observations.filter(sample => sample.to === "Rust (video game)").every(sample => sample.evidenceType === "unknown" && sample.sources.length === 0));
  assert.equal(correctRustIdentity(store, legacy).changed, false);
  store.undo(correction.eventId, store.revision);
  assert.deepEqual(store.graph().observations, before.observations);
});

test("cached expansion retries follow stable identities through later renames and merges", t => {
  const { store } = fixture(t);
  store.appendExpansion("before-rename", { provider: "mock", requestedEntity: "Alpha", data: { entity: "Alpha", influencedBy: [], influenced: [] } });
  const originalUid = store.expansion("before-rename").entityUid;
  store.edit({ action: "rename", canonical: "Alpha renamed", entities: ["Alpha"] });
  assert.equal(store.expansion("before-rename").data.entity, "Alpha renamed");
  store.edit({ action: "merge", canonical: "Beta", entities: ["Alpha renamed", "Beta"] });
  const cached = store.expansion("before-rename");
  assert.equal(cached.data.entity, "Beta");
  assert.equal(store.node(originalUid).uid, cached.entityUid);
  assert.equal(store.graph().nodes.length, 1);
  store.reset(store.revision);
  assert.throws(() => store.expansion("before-rename"), { status: 409 });
  assert.equal(store.graph().nodes.length, 0);
});
