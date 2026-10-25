const { test } = require("node:test");
const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");
const http = require("node:http");
const { DatabaseSync } = require("node:sqlite");
const { startServer, temporaryDirectory } = require("./helpers");

test("anonymous browsing is allowed; graph writes and model calls require private access", async t => {
  const server = await startServer(t);
  assert.equal((await server.request("/api/graph", { authenticated: false })).status, 200);
  const config = await server.request("/api/config", { authenticated: false });
  assert.equal(config.data.canWrite, false);
  for (const endpoint of ["/api/influences", "/api/reset", "/api/dedupe-review", "/api/llm-benchmark"]) {
    assert.equal((await server.request(endpoint, { method: "POST", body: {}, authenticated: false })).status, 403);
  }
  assert.equal((await server.request("/api/graph", { method: "PUT", body: {}, authenticated: false })).status, 403);
  assert.equal((await server.request("/api/graph", { method: "PUT", body: {} })).status, 405);
  assert.equal((await server.request("/api/reset", { method: "POST", body: {}, headers: { origin: "https://example.invalid" } })).status, 403);
  assert.match(config.headers.get("content-security-policy"), /script-src 'self'/);
});

test("malformed paths, malformed JSON and oversized bodies cannot terminate the server", async t => {
  const server = await startServer(t);
  for (const pathname of ["/%", "/%00", "/%E0%A4%A"]) {
    const result = await fetch(server.base + pathname);
    assert.equal(result.status, 400);
  }
  assert.equal((await server.request("/api/influences", { method: "POST", body: "{" })).status, 400);
  assert.equal((await server.request("/api/influences", { method: "POST", body: JSON.stringify({ entity: "x".repeat(1_000_001) }) })).status, 413);
  assert.equal((await server.request("/api/graph")).status, 200);
  assert.equal(server.child.exitCode, null);
});

test("mock expansion persists on the server and duplicate request IDs never add samples", async t => {
  const server = await startServer(t);
  const before = (await server.request("/api/graph")).data.data;
  const body = { entity: "DayZ", requestId: "dayz-one", revision: before.revision };
  const result = await server.request("/api/influences", { method: "POST", body });
  assert.equal(result.status, 200);
  assert.equal(result.data.provider, "mock");
  assert.equal(result.data.graph, undefined);
  assert.ok(result.data.changes[0].observations.length > 0);
  const first = (await server.request("/api/graph")).data.data;
  const duplicate = await server.request("/api/influences", { method: "POST", body });
  assert.equal(duplicate.status, 200);
  const second = (await server.request("/api/graph")).data.data;
  assert.deepEqual(second, first);
  assert.ok(first.nodes.some(node => node.name === "Rust (video game)"));
  assert.ok(first.observations.every(sample => sample.evidenceType === "demo"));
  assert.equal((await server.request("/api/influences", { method: "POST", body: { ...body, entity: "Other" } })).status, 409);
  assert.equal((await server.request("/api/reset", { method: "POST", body: { revision: before.revision } })).status, 409);
});

test("legacy import preserves provenance, runs once, and an empty reset survives restart", async t => {
  const root = temporaryDirectory(t);
  fs.writeFileSync(path.join(root, "legacy.json"), JSON.stringify({ nodes: [{ name: "Alpha" }, { name: "Beta" }], observations: [
    { id: 42, from: "Alpha", to: "Beta", confidence: 0, provider: "historic-fixture", model: "old-model", sourceEntity: "Original", createdAt: "2020-01-01T00:00:00.000Z" }
  ] }));
  const one = await startServer(t, root);
  const graph = (await one.request("/api/graph")).data.data;
  assert.equal(graph.observations[0].id, 42);
  assert.equal(graph.observations[0].confidence, 0);
  assert.equal(graph.observations[0].model, "old-model");
  assert.equal(graph.observations[0].sourceEntity, "Original");
  const reset = await one.request("/api/reset", { method: "POST", body: { revision: graph.revision } });
  assert.equal(reset.status, 200);
  await one.stop();
  const two = await startServer(t, root);
  assert.equal((await two.request("/api/graph")).data.data.nodes.length, 0);
  const undo = await two.request(`/api/history/${reset.data.eventId}/undo`, { method: "POST", body: { revision: reset.data.revision } });
  assert.equal(undo.status, 200);
  assert.equal((await two.request("/api/graph")).data.data.observations[0].id, 42);
  assert.ok(fs.readdirSync(path.join(root, "backups")).some(file => file.endsWith(".sqlite")));
});

test("manual rename and split approval apply the actual edits, with revision conflicts", async t => {
  const server = await startServer(t);
  await server.stop();
  const db = new DatabaseSync(path.join(server.root, "graph.sqlite"));
  const { GraphStore } = require("../lib/graph-store");
  const store = new GraphStore(db, value => String(value).toLowerCase());
  store.importGraph({ nodes: [{ name: "Alpha" }, { name: "Beta" }], observations: [{ from: "Alpha", to: "Beta", confidence: 0.8 }] });
  const old = store.graph();
  const insert = db.prepare("INSERT INTO dedupe_suggestions (canonical,entities,confidence,reason,status,action,metadata,entities_key,created_at,updated_at) VALUES (?,?,0.8,'fixture','pending',?,?,?,'2026-01-01','2026-01-01')");
  const renameId = Number(insert.run("Alpha renamed", '["Alpha","Alpha renamed"]', "rename", "{}", "rename-fixture").lastInsertRowid);
  db.close();
  const restarted = await startServer(t, server.root);
  const rename = await restarted.request(`/api/dedupe-suggestions/${renameId}/approve`, { method: "POST", body: { revision: old.revision } });
  assert.equal(rename.status, 200);
  const after = (await restarted.request("/api/graph")).data.data;
  assert.equal(after.nodes.find(node => node.name === "Alpha renamed").uid, old.nodes.find(node => node.name === "Alpha").uid);
  assert.equal(after.observations[0].id, old.observations[0].id);
  assert.equal((await restarted.request(`/api/dedupe-suggestions/${renameId}/approve`, { method: "POST", body: { revision: old.revision } })).status, 409);
  await restarted.stop();
  const splitDb = new DatabaseSync(path.join(server.root, "graph.sqlite"));
  const splitId = Number(splitDb.prepare("INSERT INTO dedupe_suggestions (canonical,entities,confidence,reason,status,action,metadata,entities_key,created_at,updated_at) VALUES ('Alpha (band)',?,0.9,'fixture','pending','split','{}','split-fixture','2026-01-01','2026-01-01')").run('["Alpha renamed","Alpha (band)"]').lastInsertRowid);
  splitDb.close();
  const splitServer = await startServer(t, server.root);
  assert.equal((await splitServer.request(`/api/dedupe-suggestions/${splitId}/approve`, { method: "POST", body: { revision: after.revision } })).status, 409);
  const split = await splitServer.request(`/api/dedupe-suggestions/${splitId}/approve`, { method: "POST", body: { revision: after.revision, moveObservationIds: [old.observations[0].id] } });
  assert.equal(split.status, 200);
  assert.equal((await splitServer.request("/api/graph")).data.data.observations[0].from, "Alpha (band)");
});

test("paid-provider protocol is tested exclusively against a loopback stub", async t => {
  let calls = 0;
  const provider = http.createServer(async (req, res) => {
    let raw = "";
    for await (const part of req) raw += part;
    const request = JSON.parse(raw);
    assert.ok(request.max_tokens > 0 && request.max_tokens <= 8192);
    calls++;
    const user = JSON.parse(request.messages.at(-1).content);
    const content = request.messages[0].content.includes("identity hygiene") ? { groups: [] }
      : { entity: user.entity, influencedBy: [{ entity: "Separate source", confidence: 0.8 }], influenced: [] };
    res.writeHead(200, { "content-type": "application/json" });
    res.end(JSON.stringify({ choices: [{ message: { content: JSON.stringify(content) }, finish_reason: "stop" }], usage: { prompt_tokens: 10, completion_tokens: 20 } }));
  });
  await new Promise(resolve => provider.listen(0, "127.0.0.1", resolve));
  t.after(() => { provider.closeAllConnections(); return new Promise(resolve => provider.close(resolve)); });
  const server = await startServer(t, undefined, { INFLUENCE_MAP_LLM_PROVIDER: "deepseek", DEEPSEEK_API_KEY: "loopback-fixture", DEEPSEEK_BASE_URL: `http://127.0.0.1:${provider.address().port}`, INFLUENCE_MAP_LLM_DAILY_REQUESTS: "1" });
  const revision = (await server.request("/api/graph")).data.data.revision;
  const result = await server.request("/api/influences", { method: "POST", body: { entity: "Fresh target", revision, requestId: "live-protocol-fixture" } });
  assert.equal(result.status, 200);
  assert.equal(calls, 1);
  assert.equal((await server.request("/api/graph")).data.data.observations.length, 1);
  const blocked = await server.request("/api/influences", { method: "POST", body: { entity: "Unrelated target", requestId: "over-budget" } });
  assert.equal(blocked.status, 429);
  assert.equal(calls, 1);
});

test("mutation rate limits apply even to authenticated clients", async t => {
  const server = await startServer(t, undefined, { INFLUENCE_MAP_MUTATIONS_PER_MINUTE: "2" });
  for (let i = 0; i < 2; i++) assert.equal((await server.request("/api/influences", { method: "POST", body: { entity: "Fixture", requestId: `rate-${i}` } })).status, 200);
  assert.equal((await server.request("/api/influences", { method: "POST", body: { entity: "Fixture" } })).status, 429);
});

test("OpenRouter keeps its headers, token limits and JSON response modes after transport refactoring", async t => {
  const received = [];
  const provider = http.createServer(async (req, res) => {
    let body = "";
    for await (const part of req) body += part;
    const request = JSON.parse(body);
    received.push({ headers: req.headers, body: request });
    const entity = JSON.parse(request.messages.at(-1).content).entity;
    res.writeHead(200, { "content-type": "application/json" });
    res.end(JSON.stringify({ choices: [{ message: { content: JSON.stringify({ entity, influencedBy: [], influenced: [] }) } }] }));
  });
  await new Promise(resolve => provider.listen(0, "127.0.0.1", resolve));
  t.after(() => { provider.closeAllConnections(); return new Promise(resolve => provider.close(resolve)); });
  for (const mode of ["0", "1"]) {
    const server = await startServer(t, undefined, {
      INFLUENCE_MAP_LLM_PROVIDER: "openrouter", OPENROUTER_API_KEY: "loopback-fixture",
      OPENROUTER_BASE_URL: `http://127.0.0.1:${provider.address().port}`,
      OPENROUTER_MODEL: "fixture-model", OPENROUTER_JSON_MODE: mode,
      OPENROUTER_REASONING: "none", OPENROUTER_HTTP_REFERER: "http://fixture.invalid",
      OPENROUTER_APP_TITLE: "Fixture", INFLUENCE_MAP_LLM_MAX_TOKENS: "512"
    });
    const result = await server.request("/api/influences", { method: "POST", body: { entity: "Fresh target", requestId: "fixture" } });
    assert.equal(result.status, 200);
    const { headers, body } = received.at(-1);
    assert.equal(headers.authorization, "Bearer loopback-fixture");
    assert.equal(headers["http-referer"], "http://fixture.invalid");
    assert.equal(headers["x-openrouter-title"], "Fixture");
    assert.equal(body.model, "fixture-model");
    assert.equal(body.max_tokens, 512);
    assert.deepEqual(body.reasoning, { effort: "none", exclude: true });
    assert.equal(body.response_format.type, mode === "1" ? "json_object" : "json_schema");
    if (mode === "0") {
      const { strict, schema } = body.response_format.json_schema;
      assert.equal(strict, true);
      assert.equal(schema.additionalProperties, false);
      assert.deepEqual(schema.required, ["entity", "influencedBy", "influenced"]);
      assert.deepEqual(schema.properties.influenced.items.required, ["entity", "confidence"]);
    }
    await server.stop();
  }
  assert.equal(received.length, 2);
});

test("scoped model splits stay reviewable instead of being dropped or overwritten by local merge heuristics", async t => {
  let failHygiene = false;
  const provider = http.createServer(async (req, res) => {
    let raw = ""; for await (const chunk of req) raw += chunk;
    const messages = JSON.parse(raw).messages;
    if (failHygiene && messages[0].content.includes("identity hygiene")) {
      res.writeHead(500); res.end("fixture provider failure"); return;
    }
    const content = messages[0].content.includes("identity hygiene")
      ? { groups: [{ action: "split", canonical: "Shared title (band)", entities: ["Shared title", "Shared title (band)"], confidence: 0.9, reason: "Separate contextual senses" }] }
      : { entity: JSON.parse(messages.at(-1).content).entity, influencedBy: [{ entity: "Shared title", confidence: 0.8 }], influenced: [] };
    res.setHeader("content-type", "application/json");
    res.end(JSON.stringify({ choices: [{ message: { content: JSON.stringify(content) } }] }));
  });
  await new Promise(resolve => provider.listen(0, "127.0.0.1", resolve));
  t.after(() => { provider.closeAllConnections(); return new Promise(resolve => provider.close(resolve)); });
  const root = temporaryDirectory(t);
  fs.writeFileSync(path.join(root, "legacy.json"), JSON.stringify({ nodes: [{ name: "Shared title" }, { name: "Shared title (band)" }], observations: [] }));
  const server = await startServer(t, root, { INFLUENCE_MAP_LLM_PROVIDER: "deepseek", DEEPSEEK_API_KEY: "loopback-fixture", DEEPSEEK_BASE_URL: `http://127.0.0.1:${provider.address().port}` });
  const result = await server.request("/api/influences", { method: "POST", body: { entity: "Fresh target", requestId: "scoped-split" } });
  assert.equal(result.status, 200);
  const pending = (await server.request("/api/dedupe-suggestions")).data.suggestions;
  assert.equal(pending.length, 1);
  assert.equal(pending[0].action, "split");
  assert.equal(pending[0].metadata.needsLinkReview, true);
  assert.equal((await server.request("/api/graph")).data.data.observations.length, 1);
  failHygiene = true;
  const ancillaryFailure = await server.request("/api/influences", { method: "POST", body: { entity: "Another fresh target", requestId: "hygiene-failure" } });
  assert.equal(ancillaryFailure.status, 200);
  assert.match(ancillaryFailure.data.scopedIdentityReview.warning, /HTTP 500/);
  assert.equal((await server.request("/api/graph")).data.data.observations.length, 2);
  assert.ok((await server.request("/api/diagnostics")).data.profiles.some(profile => profile.type === "identity-hygiene-error"));
});

test("HTTP queue limits, duplicate in-flight IDs and disconnect cancellation preserve the active request", { timeout: 10000 }, async t => {
  let finish;
  const provider = http.createServer(async (req, res) => {
    for await (const chunk of req) { /* consume the request before exposing the gate */ }
    finish = () => { res.setHeader("content-type", "application/json"); res.end(JSON.stringify({ choices: [{ message: { content: JSON.stringify({ entity: "First", influencedBy: [], influenced: [] }) } }] })); };
  });
  await new Promise(resolve => provider.listen(0, "127.0.0.1", resolve));
  t.after(() => { provider.closeAllConnections(); return new Promise(resolve => provider.close(resolve)); });
  const server = await startServer(t, undefined, { INFLUENCE_MAP_LLM_PROVIDER: "deepseek", DEEPSEEK_API_KEY: "loopback-fixture", DEEPSEEK_BASE_URL: `http://127.0.0.1:${provider.address().port}`, INFLUENCE_MAP_LLM_MAX_QUEUE: "1" });
  const first = server.request("/api/influences", { method: "POST", body: { entity: "First", requestId: "active" } });
  async function until(predicate) {
    for (let attempt = 0; attempt < 100; attempt++) { if (await predicate()) return; await new Promise(resolve => setTimeout(resolve, 5)); }
    throw new Error("Expected queue state was not reached");
  }
  await until(() => Boolean(finish));
  for (let duplicate = 0; duplicate < 2; duplicate++) assert.equal((await server.request("/api/influences", { method: "POST", body: { entity: "First", requestId: "active" } })).status, 409);
  const cancellation = new AbortController();
  const queued = fetch(server.base + "/api/influences", { method: "POST", headers: { "content-type": "application/json", "x-admin-token": "test-private-token" }, body: JSON.stringify({ entity: "Second", requestId: "queued" }), signal: cancellation.signal }).catch(error => error);
  await until(async () => (await server.request("/api/diagnostics")).data.llmQueue.queued === 1);
  assert.equal((await server.request("/api/influences", { method: "POST", body: { entity: "Third", requestId: "full" } })).status, 429);
  cancellation.abort(); await queued;
  await until(async () => (await server.request("/api/diagnostics")).data.llmQueue.queued === 0);
  finish();
  assert.equal((await first).status, 200);
  const graph = (await server.request("/api/graph")).data.data;
  assert.deepEqual(graph.nodes.map(node => node.name), ["First"]);
});

test("two simultaneous successful expansions preserve both neighborhoods from the same starting revision", async t => {
  const provider = http.createServer(async (req, res) => {
    let raw = ""; for await (const part of req) raw += part;
    const entity = JSON.parse(JSON.parse(raw).messages.at(-1).content).entity;
    await new Promise(resolve => setTimeout(resolve, 20));
    res.setHeader("content-type", "application/json");
    res.end(JSON.stringify({ choices: [{ message: { content: JSON.stringify({ entity, influencedBy: [{ entity: "Independent origin", confidence: 0.8 }], influenced: [] }) } }] }));
  });
  await new Promise(resolve => provider.listen(0, "127.0.0.1", resolve));
  t.after(() => { provider.closeAllConnections(); return new Promise(resolve => provider.close(resolve)); });
  const server = await startServer(t, undefined, { INFLUENCE_MAP_LLM_PROVIDER: "deepseek", DEEPSEEK_API_KEY: "loopback-fixture", DEEPSEEK_BASE_URL: `http://127.0.0.1:${provider.address().port}`, INFLUENCE_MAP_LLM_CONCURRENCY: "2" });
  const revision = (await server.request("/api/graph")).data.data.revision;
  const results = await Promise.all(["First target", "Second target"].map((entity, index) => server.request("/api/influences", { method: "POST", body: { entity, requestId: `simultaneous-${index}`, revision } })));
  assert.ok(results.every(result => result.status === 200));
  const graph = (await server.request("/api/graph")).data.data;
  assert.deepEqual(graph.observations.map(sample => sample.to).sort(), ["First target", "Second target"]);
  assert.equal(new Set(graph.observations.map(sample => sample.id)).size, 2);
});
