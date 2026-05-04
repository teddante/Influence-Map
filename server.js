const http = require("http");
const fs = require("fs");
const path = require("path");
const { DatabaseSync } = require("node:sqlite");

const PORT = Number(process.env.PORT || 3000);
const PUBLIC_DIR = path.join(__dirname, "public");
const DATA_FILE = process.env.INFLUENCE_MAP_DATA_FILE || path.join(__dirname, "data", "graph.json");
const DB_FILE = process.env.INFLUENCE_MAP_DB_FILE || path.join(__dirname, "data", "influence-map.sqlite");
const DEEPSEEK_API_KEY = process.env.DEEPSEEK_API_KEY || "";
const DEEPSEEK_MODEL = process.env.DEEPSEEK_MODEL || "deepseek-v4-flash";
const DEEPSEEK_BASE_URL = process.env.DEEPSEEK_BASE_URL || "https://api.deepseek.com";
const DEEPSEEK_THINKING = process.env.DEEPSEEK_THINKING || "disabled";
const OPENROUTER_API_KEY = process.env.OPENROUTER_API_KEY || "";
const OPENROUTER_MODEL = process.env.OPENROUTER_MODEL || "google/gemini-3.1-flash-lite-preview";
const OPENROUTER_BASE_URL = process.env.OPENROUTER_BASE_URL || "https://openrouter.ai/api/v1";
const OPENROUTER_REASONING = process.env.OPENROUTER_REASONING || "none";
const DEFAULT_LLM_PROVIDER = (process.env.INFLUENCE_MAP_LLM_PROVIDER || "deepseek").toLowerCase();
const LLM_MAX_CONCURRENCY = Math.max(1, Number(process.env.INFLUENCE_MAP_LLM_CONCURRENCY || 1) || 1);
const LLM_REQUEST_TIMEOUT_MS = Math.max(10_000, Number(process.env.INFLUENCE_MAP_LLM_TIMEOUT_MS || 45_000) || 45_000);
const LLM_MAX_TOKENS = Math.max(0, Number(process.env.INFLUENCE_MAP_LLM_MAX_TOKENS || 0) || 0);
const IDENTITY_AUTO_APPROVE = process.env.INFLUENCE_MAP_IDENTITY_AUTO_APPROVE !== "0";
const IDENTITY_AUTO_MERGE_CONFIDENCE = Math.max(0.5, Math.min(1, Number(process.env.INFLUENCE_MAP_AUTO_MERGE_CONFIDENCE || 0.72) || 0.72));
const IDENTITY_AUTO_SPLIT_CONFIDENCE = Math.max(0.5, Math.min(1, Number(process.env.INFLUENCE_MAP_AUTO_SPLIT_CONFIDENCE || 0.78) || 0.78));
const DEV_TOOLS = process.env.INFLUENCE_MAP_DEV_TOOLS === "1";
const ADMIN_TOKEN = process.env.INFLUENCE_MAP_ADMIN_TOKEN || "";
const profileEvents = [];
let activeLlmRequests = 0;
const queuedLlmRequests = [];

const mimeTypes = {
  ".html": "text/html; charset=utf-8",
  ".css": "text/css; charset=utf-8",
  ".js": "text/javascript; charset=utf-8",
  ".json": "application/json; charset=utf-8"
};

function sendJson(res, status, data) {
  const body = JSON.stringify(data);
  res.writeHead(status, {
    "content-type": "application/json; charset=utf-8",
    "content-length": Buffer.byteLength(body)
  });
  res.end(body);
}

function readBody(req, maxBytes = 1_000_000) {
  return new Promise((resolve, reject) => {
    let body = "";
    req.on("data", chunk => {
      body += chunk;
      if (body.length > maxBytes) {
        req.destroy();
        reject(new Error("Request body too large"));
      }
    });
    req.on("end", () => resolve(body));
    req.on("error", reject);
  });
}

function deepSeekRequestBody({ messages, temperature, maxTokens }) {
  const body = {
    model: DEEPSEEK_MODEL,
    messages,
    response_format: { type: "json_object" },
    thinking: { type: DEEPSEEK_THINKING === "enabled" ? "enabled" : "disabled" },
    temperature
  };
  if (Number.isFinite(maxTokens)) body.max_tokens = maxTokens;
  return body;
}

function providerConfig(providerName = DEFAULT_LLM_PROVIDER) {
  const provider = normalizeName(providerName).toLowerCase();
  if (provider === "openrouter" || provider === "google") {
    return {
      provider: "openrouter",
      apiKey: OPENROUTER_API_KEY,
      model: OPENROUTER_MODEL,
      baseUrl: OPENROUTER_BASE_URL.replace(/\/$/, ""),
      endpoint: "/chat/completions"
    };
  }
  return {
    provider: "deepseek",
    apiKey: DEEPSEEK_API_KEY,
    model: DEEPSEEK_MODEL,
    baseUrl: DEEPSEEK_BASE_URL.replace(/\/$/, ""),
    endpoint: "/chat/completions"
  };
}

const influenceResponseSchema = {
  type: "object",
  additionalProperties: false,
  properties: {
    entity: { type: "string" },
    influencedBy: {
      type: "array",
      items: {
        type: "object",
        additionalProperties: false,
        properties: {
          entity: { type: "string" },
          confidence: { type: "number" }
        },
        required: ["entity", "confidence"]
      }
    },
    influenced: {
      type: "array",
      items: {
        type: "object",
        additionalProperties: false,
        properties: {
          entity: { type: "string" },
          confidence: { type: "number" }
        },
        required: ["entity", "confidence"]
      }
    }
  },
  required: ["entity", "influencedBy", "influenced"]
};

const identityResponseSchema = {
  type: "object",
  additionalProperties: false,
  properties: {
    canonicalName: { type: "string" },
    displayName: { type: "string" },
    disambiguation: { type: "string" },
    aliases: { type: "array", items: { type: "string" } },
    decision: { type: "string", enum: ["same", "rename", "merge", "split", "new", "uncertain"] },
    confidence: { type: "number" },
    reason: { type: "string" }
  },
  required: ["canonicalName", "displayName", "disambiguation", "aliases", "decision", "confidence", "reason"]
};

const dedupeResponseSchema = {
  type: "object",
  additionalProperties: false,
  properties: {
    groups: {
      type: "array",
      items: {
        type: "object",
        additionalProperties: false,
        properties: {
          canonical: { type: "string" },
          entities: { type: "array", items: { type: "string" } },
          confidence: { type: "number" },
          reason: { type: "string" }
        },
        required: ["canonical", "entities", "confidence", "reason"]
      }
    }
  },
  required: ["groups"]
};

const scopedIdentityResponseSchema = {
  type: "object",
  additionalProperties: false,
  properties: {
    groups: {
      type: "array",
      items: {
        type: "object",
        additionalProperties: false,
        properties: {
          action: { type: "string", enum: ["merge", "split", "keep-separate"] },
          canonical: { type: "string" },
          entities: { type: "array", items: { type: "string" } },
          confidence: { type: "number" },
          reason: { type: "string" }
        },
        required: ["action", "canonical", "entities", "confidence", "reason"]
      }
    }
  },
  required: ["groups"]
};

function structuredResponseFormat(name, schema) {
  return {
    type: "json_schema",
    json_schema: {
      name,
      strict: true,
      schema
    }
  };
}

function llmResponseFormat({ responseSchemaName, responseSchema, jsonModeOnly }) {
  if (jsonModeOnly || !responseSchema) return { type: "json_object" };
  return structuredResponseFormat(responseSchemaName || "influence_map_response", responseSchema);
}

function llmRequestBody(config, { messages, temperature, maxTokens, responseSchemaName, responseSchema, jsonModeOnly }) {
  if (config.provider === "deepseek") return deepSeekRequestBody({ messages, temperature, maxTokens });
  const body = {
    model: config.model,
    messages,
    response_format: llmResponseFormat({
      responseSchemaName,
      responseSchema,
      jsonModeOnly: typeof jsonModeOnly === "boolean" ? jsonModeOnly : process.env.OPENROUTER_JSON_MODE === "1"
    }),
    temperature,
    reasoning: {
      effort: OPENROUTER_REASONING,
      exclude: true
    }
  };
  if (Number.isFinite(maxTokens)) body.max_tokens = maxTokens;
  return body;
}

function llmHeaders(config) {
  const headers = {
    "content-type": "application/json",
    "authorization": `Bearer ${config.apiKey}`
  };
  if (config.provider === "openrouter") {
    headers["HTTP-Referer"] = process.env.OPENROUTER_HTTP_REFERER || "http://localhost";
    headers["X-OpenRouter-Title"] = process.env.OPENROUTER_APP_TITLE || "Influence Map";
  }
  return headers;
}

function acquireLlmSlot() {
  const started = process.hrtime.bigint();
  if (activeLlmRequests < LLM_MAX_CONCURRENCY) {
    activeLlmRequests += 1;
    return Promise.resolve({ queueMs: elapsedMs(started) });
  }
  return new Promise(resolve => {
    queuedLlmRequests.push(() => {
      activeLlmRequests += 1;
      resolve({ queueMs: elapsedMs(started) });
    });
  });
}

function releaseLlmSlot() {
  activeLlmRequests = Math.max(0, activeLlmRequests - 1);
  const next = queuedLlmRequests.shift();
  if (next) next();
}

async function callChatCompletion(config, prompt, options) {
  const slot = await acquireLlmSlot();
  const started = process.hrtime.bigint();
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), LLM_REQUEST_TIMEOUT_MS);
  try {
    const response = await fetch(`${config.baseUrl}${config.endpoint}`, {
      method: "POST",
      headers: llmHeaders(config),
      signal: controller.signal,
      body: JSON.stringify(llmRequestBody(config, {
        messages: prompt.messages,
        temperature: options.temperature,
        maxTokens: options.maxTokens,
        responseSchemaName: options.responseSchemaName,
        responseSchema: options.responseSchema,
        jsonModeOnly: options.jsonModeOnly
      }))
    });

    if (!response.ok) {
      const text = await response.text();
      throw new Error(`${config.provider} ${response.status}: ${text.slice(0, 500)}`);
    }

    const data = await response.json();
    const content = data.choices && data.choices[0] && data.choices[0].message && data.choices[0].message.content;
    return {
      data,
      content,
      usage: usageSummary(data),
      finishReason: data.choices && data.choices[0] && data.choices[0].finish_reason,
      ms: elapsedMs(started),
      queueMs: slot.queueMs,
      id: data.id || ""
    };
  } catch (error) {
    if (error && error.name === "AbortError") {
      throw new Error(`${config.provider} request timed out after ${Math.round(LLM_REQUEST_TIMEOUT_MS / 1000)}s`);
    }
    throw error;
  } finally {
    clearTimeout(timeout);
    releaseLlmSlot();
  }
}

function usageSummary(data) {
  const usage = data && data.usage ? data.usage : {};
  const details = usage.completion_tokens_details || {};
  return {
    promptTokens: usage.prompt_tokens || 0,
    completionTokens: usage.completion_tokens || 0,
    totalTokens: usage.total_tokens || 0,
    reasoningTokens: details.reasoning_tokens || 0
  };
}

function normalizeName(name) {
  return String(name || "")
    .trim()
    .replace(/\s+/g, " ");
}

function clampConfidence(value) {
  const number = Number(value);
  if (!Number.isFinite(number)) return 0.5;
  return Math.max(0, Math.min(1, number));
}

function sanitizeItems(items) {
  if (!Array.isArray(items)) return [];
  const seen = new Set();
  const clean = [];
  for (const item of items) {
    const entity = normalizeName(item && item.entity);
    const key = entity.toLowerCase();
    if (!entity || seen.has(key)) continue;
    seen.add(key);
    clean.push({
      entity,
      confidence: Number(clampConfidence(item.confidence).toFixed(3))
    });
  }
  return clean;
}

function sanitizeInfluenceResponse(entity, data) {
  const canonicalEntity = normalizeName(
    data && (data.entity || data.canonicalEntity || data.canonicalName || data.name)
  ) || entity;
  return {
    entity: canonicalEntity,
    influencedBy: sanitizeItems(data && data.influencedBy),
    influenced: sanitizeItems(data && data.influenced)
  };
}

function sanitizeExpansionContext(context) {
  const clean = {
    discoveredFrom: [],
    connectedTo: []
  };
  if (!context || typeof context !== "object") return clean;

  for (const item of Array.isArray(context.discoveredFrom) ? context.discoveredFrom : []) {
    const sourceEntity = normalizeName(item && item.sourceEntity);
    const relation = normalizeName(item && item.relation);
    const confidence = clampConfidence(item && item.confidence);
    if (!sourceEntity && !relation) continue;
    clean.discoveredFrom.push({ sourceEntity, relation, confidence });
  }

  for (const item of Array.isArray(context.connectedTo) ? context.connectedTo : []) {
    const entity = normalizeName(item && item.entity);
    const relation = normalizeName(item && item.relation);
    const confidence = clampConfidence(item && item.confidence);
    if (!entity && !relation) continue;
    clean.connectedTo.push({ entity, relation, confidence });
  }

  clean.discoveredFrom = clean.discoveredFrom.slice(0, 8);
  clean.connectedTo = clean.connectedTo.slice(0, 12);
  return clean;
}

function finiteNumber(value, fallback) {
  const number = Number(value);
  return Number.isFinite(number) ? number : fallback;
}

function sanitizeGraphData(data) {
  const minConfidence = data && Object.prototype.hasOwnProperty.call(data, "minConfidence")
    ? clampConfidence(data.minConfidence)
    : 0;
  const savedAtDate = new Date(data && data.savedAt);
  const savedAt = Number.isFinite(savedAtDate.valueOf()) ? savedAtDate.toISOString() : new Date().toISOString();

  const nodes = [];
  const nodeById = new Map();
  for (const node of Array.isArray(data && data.nodes) ? data.nodes : []) {
    const name = normalizeName(node && node.name);
    const id = keyFor(name);
    if (!id) continue;
    const aliases = Array.isArray(node && node.aliases)
      ? node.aliases.map(normalizeName).filter(Boolean)
      : [];
    if (!nodeById.has(id)) {
      nodeById.set(id, {
        id,
        name,
        aliases: [],
        x: finiteNumber(node && node.x, 0),
        y: finiteNumber(node && node.y, 0)
      });
    }
    const current = nodeById.get(id);
    for (const alias of [name, ...aliases]) {
      if (alias && alias !== current.name && !current.aliases.includes(alias)) {
        current.aliases.push(alias);
      }
    }
  }
  nodes.push(...[...nodeById.values()].map(node => ({
    ...node,
    aliases: node.aliases.slice(0, 12)
  })));
  const nameById = new Map(nodes.map(node => [node.id, node.name]));

  const observations = [];
  for (const observation of Array.isArray(data && data.observations) ? data.observations : []) {
    const rawFrom = normalizeName(observation && observation.from);
    const rawTo = normalizeName(observation && observation.to);
    const fromKey = keyFor(rawFrom);
    const toKey = keyFor(rawTo);
    if (!rawFrom || !rawTo || fromKey === toKey) continue;
    const from = nameById.get(fromKey) || rawFrom;
    const to = nameById.get(toKey) || rawTo;
    observations.push({
      from,
      to,
      confidence: Number(clampConfidence(observation.confidence).toFixed(3))
    });
  }

  const expanded = [];
  const seenExpanded = new Set();
  for (const name of Array.isArray(data && data.expanded) ? data.expanded : []) {
    const key = keyFor(name);
    if (!key || seenExpanded.has(key)) continue;
    seenExpanded.add(key);
    expanded.push(key);
  }

  return {
    version: 1,
    savedAt,
    nodes,
    observations,
    expanded,
    pan: {
      x: finiteNumber(data && data.pan && data.pan.x, 0),
      y: finiteNumber(data && data.pan && data.pan.y, 0)
    },
    zoom: Math.max(0.25, Math.min(3, finiteNumber(data && data.zoom, 1))),
    minConfidence
  };
}

function keyFor(name) {
  return normalizeName(name)
    .toLowerCase()
    .replace(/&/g, " and ")
    .replace(/[()[\]{}'"`.,:;!?]/g, " ")
    .replace(/\s+/g, " ")
    .replace(/^the\s+/, "")
    .trim();
}

function nowIso() {
  return new Date().toISOString();
}

function elapsedMs(start) {
  return Math.round(Number(process.hrtime.bigint() - start) / 1_000_000);
}

function configuredMaxTokens() {
  return LLM_MAX_TOKENS > 0 ? LLM_MAX_TOKENS : undefined;
}

function recordProfile(event) {
  const profile = {
    ...event,
    at: nowIso()
  };
  profileEvents.unshift(profile);
  profileEvents.length = Math.min(profileEvents.length, 80);
  try {
    const timings = Array.isArray(profile.timings) ? profile.timings : [];
    const total = timings.find(item => item && item.stage === "total");
    const usage = profile.usage && profile.usage.expansion ? profile.usage.expansion : {};
    const llm = profile.llm || {};
    db.prepare(`
      INSERT INTO llm_profiles (
        event_type, provider, model, entity, status, total_ms, request_ms, queue_ms,
        prompt_tokens, completion_tokens, reasoning_tokens, error, event_json, created_at
      )
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    `).run(
      normalizeName(profile.type || "event"),
      normalizeName(profile.provider || ""),
      normalizeName(profile.model || ""),
      normalizeName(profile.entity || ""),
      profile.error ? "error" : "ok",
      Number(total && total.ms) || 0,
      Number(llm.expansionRequestMs || llm.identityRequestMs) || null,
      Number(llm.expansionQueueMs || llm.identityQueueMs) || null,
      Number(usage.promptTokens) || null,
      Number(usage.completionTokens) || null,
      Number(usage.reasoningTokens) || null,
      normalizeName(profile.error || ""),
      JSON.stringify(profile),
      profile.at
    );
    db.prepare(`
      DELETE FROM llm_profiles
      WHERE id NOT IN (
        SELECT id FROM llm_profiles ORDER BY id DESC LIMIT 300
      )
    `).run();
  } catch (error) {
    console.warn(`Could not persist profile: ${error.message}`);
  }
}

function parseJson(value, fallback) {
  try {
    return value ? JSON.parse(value) : fallback;
  } catch {
    return fallback;
  }
}

function adminTokenFrom(req, url) {
  return req.headers["x-admin-token"] || url.searchParams.get("admin") || "";
}

function hasAdminAccess(req, url) {
  if (!ADMIN_TOKEN) return DEV_TOOLS;
  return DEV_TOOLS && adminTokenFrom(req, url) === ADMIN_TOKEN;
}

function requireAdmin(req, res, url) {
  if (hasAdminAccess(req, url)) return true;
  sendJson(res, 403, { error: "Admin access required" });
  return false;
}

fs.mkdirSync(path.dirname(DB_FILE), { recursive: true });
const db = new DatabaseSync(DB_FILE);

function initDb() {
  db.exec(`
    PRAGMA journal_mode = WAL;
    CREATE TABLE IF NOT EXISTS nodes (
      id TEXT PRIMARY KEY,
      name TEXT NOT NULL,
      aliases TEXT NOT NULL DEFAULT '[]',
      x REAL NOT NULL DEFAULT 0,
      y REAL NOT NULL DEFAULT 0,
      expanded INTEGER NOT NULL DEFAULT 0,
      created_at TEXT NOT NULL,
      updated_at TEXT NOT NULL
    );
    CREATE TABLE IF NOT EXISTS observations (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      from_id TEXT NOT NULL,
      to_id TEXT NOT NULL,
      confidence REAL NOT NULL,
      source_entity TEXT,
      provider TEXT,
      model TEXT,
      created_at TEXT NOT NULL
    );
    CREATE INDEX IF NOT EXISTS idx_observations_from ON observations(from_id);
    CREATE INDEX IF NOT EXISTS idx_observations_to ON observations(to_id);
    CREATE TABLE IF NOT EXISTS dedupe_suggestions (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      canonical TEXT NOT NULL,
      entities TEXT NOT NULL,
      confidence REAL NOT NULL DEFAULT 0,
      reason TEXT NOT NULL DEFAULT '',
      status TEXT NOT NULL DEFAULT 'pending',
      action TEXT NOT NULL DEFAULT 'merge',
      metadata TEXT NOT NULL DEFAULT '{}',
      entities_key TEXT NOT NULL,
      created_at TEXT NOT NULL,
      updated_at TEXT NOT NULL,
      reviewed_at TEXT
    );
    DROP INDEX IF EXISTS idx_dedupe_suggestions_key;
    CREATE UNIQUE INDEX IF NOT EXISTS idx_dedupe_suggestions_pending_key
      ON dedupe_suggestions(entities_key)
      WHERE status = 'pending';
    CREATE TABLE IF NOT EXISTS settings (
      key TEXT PRIMARY KEY,
      value TEXT NOT NULL
    );
    CREATE TABLE IF NOT EXISTS llm_profiles (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      event_type TEXT NOT NULL,
      provider TEXT NOT NULL DEFAULT '',
      model TEXT NOT NULL DEFAULT '',
      entity TEXT NOT NULL DEFAULT '',
      status TEXT NOT NULL DEFAULT 'ok',
      total_ms INTEGER NOT NULL DEFAULT 0,
      request_ms INTEGER,
      queue_ms INTEGER,
      prompt_tokens INTEGER,
      completion_tokens INTEGER,
      reasoning_tokens INTEGER,
      error TEXT NOT NULL DEFAULT '',
      event_json TEXT NOT NULL,
      created_at TEXT NOT NULL
    );
    CREATE INDEX IF NOT EXISTS idx_llm_profiles_created_at ON llm_profiles(created_at);
    CREATE INDEX IF NOT EXISTS idx_llm_profiles_status ON llm_profiles(status);
  `);
  const suggestionColumns = db.prepare("PRAGMA table_info(dedupe_suggestions)").all().map(row => row.name);
  if (!suggestionColumns.includes("action")) {
    db.exec("ALTER TABLE dedupe_suggestions ADD COLUMN action TEXT NOT NULL DEFAULT 'merge'");
  }
  if (!suggestionColumns.includes("metadata")) {
    db.exec("ALTER TABLE dedupe_suggestions ADD COLUMN metadata TEXT NOT NULL DEFAULT '{}'");
  }
}

function getSetting(key, fallback) {
  const row = db.prepare("SELECT value FROM settings WHERE key = ?").get(key);
  return row ? row.value : fallback;
}

function setSetting(key, value) {
  db.prepare(`
    INSERT INTO settings (key, value)
    VALUES (?, ?)
    ON CONFLICT(key) DO UPDATE SET value = excluded.value
  `).run(key, String(value));
}

function rowToProfile(row) {
  const parsed = parseJson(row.event_json, {});
  return {
    ...parsed,
    profileId: row.id,
    at: parsed.at || row.created_at
  };
}

function recentProfiles(limit = 40) {
  return db.prepare(`
    SELECT * FROM llm_profiles
    ORDER BY id DESC
    LIMIT ?
  `).all(Math.max(1, Math.min(100, Number(limit) || 40))).map(rowToProfile);
}

function profileDurationMs(profile) {
  const total = Array.isArray(profile && profile.timings)
    ? profile.timings.find(item => item && item.stage === "total")
    : null;
  const llm = profile && profile.llm ? profile.llm : {};
  return Math.max(
    Number(total && total.ms) || 0,
    Number(llm.expansionRequestMs || llm.identityRequestMs || 0)
  );
}

function diagnosticCounts() {
  const nodes = db.prepare("SELECT COUNT(*) AS count FROM nodes").get().count;
  const observations = db.prepare("SELECT COUNT(*) AS count FROM observations").get().count;
  const pendingIdentity = db.prepare("SELECT COUNT(*) AS count FROM dedupe_suggestions WHERE status = 'pending'").get().count;
  const profiles = db.prepare("SELECT COUNT(*) AS count FROM llm_profiles").get().count;
  const errors = db.prepare("SELECT COUNT(*) AS count FROM llm_profiles WHERE status = 'error'").get().count;
  const slowProfiles = db.prepare("SELECT event_json FROM llm_profiles")
    .all()
    .map(row => parseJson(row.event_json, {}))
    .filter(profile => profileDurationMs(profile) >= 10000)
    .length;
  return { nodes, observations, pendingIdentity, profiles, errors, slowProfiles };
}

function diagnosticSummary() {
  const config = providerConfig(DEFAULT_LLM_PROVIDER);
  const recent = recentProfiles(20);
  return {
    provider: config.apiKey ? config.provider : "mock",
    model: config.apiKey ? config.model : "",
    thinking: config.provider === "openrouter" ? OPENROUTER_REASONING : DEEPSEEK_THINKING === "enabled" ? "enabled" : "disabled",
    openRouterJsonMode: process.env.OPENROUTER_JSON_MODE === "1",
    llmQueue: {
      active: activeLlmRequests,
      queued: queuedLlmRequests.length,
      concurrency: LLM_MAX_CONCURRENCY,
      timeoutMs: LLM_REQUEST_TIMEOUT_MS,
      maxTokens: configuredMaxTokens() || null
    },
    counts: diagnosticCounts(),
    profiles: recent,
    slowThresholdMs: 10000,
    generatedAt: nowIso()
  };
}

function upsertNode(name, options = {}) {
  const cleanName = normalizeName(name);
  const id = keyFor(cleanName);
  if (!id) return null;
  const existing = db.prepare("SELECT * FROM nodes WHERE id = ?").get(id);
  const timestamp = nowIso();
  if (!existing) {
    db.prepare(`
      INSERT INTO nodes (id, name, aliases, x, y, expanded, created_at, updated_at)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    `).run(
      id,
      cleanName,
      JSON.stringify(options.aliases || []),
      finiteNumber(options.x, 0),
      finiteNumber(options.y, 0),
      options.expanded ? 1 : 0,
      timestamp,
      timestamp
    );
    return id;
  }

  const aliases = parseJson(existing.aliases, []);
  for (const alias of [cleanName, ...(options.aliases || [])].map(normalizeName).filter(Boolean)) {
    if (alias !== existing.name && !aliases.includes(alias)) aliases.push(alias);
  }
  db.prepare(`
    UPDATE nodes
    SET name = ?,
        aliases = ?,
        x = ?,
        y = ?,
        expanded = CASE WHEN ? THEN 1 ELSE expanded END,
        updated_at = ?
    WHERE id = ?
  `).run(
    existing.name || cleanName,
    JSON.stringify(aliases.slice(0, 20)),
    Number.isFinite(Number(options.x)) ? Number(options.x) : existing.x,
    Number.isFinite(Number(options.y)) ? Number(options.y) : existing.y,
    options.expanded ? 1 : 0,
    timestamp,
    id
  );
  return id;
}

function nodeRowsByIds(ids) {
  const unique = [...new Set(ids.map(keyFor).filter(Boolean))];
  if (!unique.length) return [];
  return unique
    .map(id => db.prepare("SELECT * FROM nodes WHERE id = ?").get(id))
    .filter(Boolean);
}

function mergeNodes(canonical, entities) {
  const canonicalName = normalizeName(canonical);
  const canonicalId = keyFor(canonicalName);
  const entityIds = [...new Set((entities || []).map(keyFor).filter(Boolean))];
  if (!canonicalId || entityIds.length < 2) {
    throw new Error("Merge needs a canonical name and at least two entities");
  }

  const rows = nodeRowsByIds([canonicalId, ...entityIds]);
  if (rows.length < 2) return { changed: false, reason: "Already resolved; fewer than two matching nodes remain" };

  const timestamp = nowIso();
  const aliases = new Set([canonicalName]);
  let x = 0;
  let y = 0;
  let coordinateCount = 0;
  let expanded = false;

  for (const row of rows) {
    aliases.add(row.name);
    for (const alias of parseJson(row.aliases, [])) aliases.add(alias);
    if (Number.isFinite(Number(row.x)) && Number.isFinite(Number(row.y))) {
      x += Number(row.x);
      y += Number(row.y);
      coordinateCount += 1;
    }
    expanded = expanded || Boolean(row.expanded);
  }

  upsertNode(canonicalName, {
    aliases: [...aliases].filter(alias => alias && alias !== canonicalName),
    x: coordinateCount ? x / coordinateCount : 0,
    y: coordinateCount ? y / coordinateCount : 0,
    expanded
  });

  const idsToMove = rows.map(row => row.id).filter(id => id !== canonicalId);
  const updateFrom = db.prepare("UPDATE observations SET from_id = ? WHERE from_id = ?");
  const updateTo = db.prepare("UPDATE observations SET to_id = ? WHERE to_id = ?");
  const updateSource = db.prepare("UPDATE observations SET source_entity = ? WHERE lower(source_entity) = lower(?)");
  for (const id of idsToMove) {
    updateFrom.run(canonicalId, id);
    updateTo.run(canonicalId, id);
  }
  for (const row of rows) {
    updateSource.run(canonicalName, row.name);
    for (const alias of parseJson(row.aliases, [])) updateSource.run(canonicalName, alias);
  }
  db.prepare("DELETE FROM observations WHERE from_id = to_id").run();
  for (const id of idsToMove) {
    db.prepare("DELETE FROM nodes WHERE id = ?").run(id);
  }
  db.prepare("UPDATE nodes SET name = ?, aliases = ?, expanded = ?, updated_at = ? WHERE id = ?").run(
    canonicalName,
    JSON.stringify([...aliases].filter(alias => alias && alias !== canonicalName).slice(0, 40)),
    expanded ? 1 : 0,
    timestamp,
    canonicalId
  );
  return {
    changed: Boolean(idsToMove.length),
    reason: idsToMove.length ? `Merged ${idsToMove.length} node${idsToMove.length === 1 ? "" : "s"}` : "Already canonical"
  };
}

function renameNodeInDb(source, canonical) {
  const sourceName = normalizeName(source);
  const canonicalName = normalizeName(canonical);
  const sourceId = keyFor(sourceName);
  const canonicalId = keyFor(canonicalName);
  if (!sourceId || !canonicalId || !sourceName || !canonicalName) {
    throw new Error("Rename needs a source and canonical name");
  }
  if (sourceId === canonicalId) {
    const row = db.prepare("SELECT * FROM nodes WHERE id = ?").get(sourceId);
    if (!row || row.name === canonicalName) return { changed: false, reason: "Already named correctly" };
    const aliases = new Set(parseJson(row.aliases, []));
    aliases.add(row.name);
    db.prepare("UPDATE nodes SET name = ?, aliases = ?, updated_at = ? WHERE id = ?").run(
      canonicalName,
      JSON.stringify([...aliases].filter(alias => alias && alias !== canonicalName).slice(0, 40)),
      nowIso(),
      sourceId
    );
    db.prepare("UPDATE observations SET source_entity = ? WHERE lower(source_entity) = lower(?)").run(canonicalName, sourceName);
    return { changed: true, reason: `Renamed ${sourceName} to ${canonicalName}` };
  }
  return mergeNodes(canonicalName, [sourceName, canonicalName]);
}

function approveSplitSuggestion(suggestion) {
  const parsed = rowToSuggestion(suggestion);
  return applySplitDecision(parsed);
}

function applySplitDecision(parsed) {
  const metadata = parsed.metadata || {};
  const ids = Array.isArray(metadata.moveObservationIds)
    ? metadata.moveObservationIds.map(Number).filter(Number.isFinite)
    : [];
  if (!ids.length) return { changed: false, moved: 0, reason: "No movable links were attached to this split review" };

  const sourceId = keyFor(parsed.entities[0]);
  const canonicalKey = keyFor(parsed.canonical);
  const sourceRow = sourceId ? db.prepare("SELECT * FROM nodes WHERE id = ?").get(sourceId) : null;
  const canonicalExists = canonicalKey ? db.prepare("SELECT id FROM nodes WHERE id = ?").get(canonicalKey) : null;
  const canonicalId = upsertNode(parsed.canonical, canonicalExists ? {} : {
    x: sourceRow ? Number(sourceRow.x) + 36 : 0,
    y: sourceRow ? Number(sourceRow.y) + 36 : 0
  });
  if (!canonicalId || !sourceId || canonicalId === sourceId) return { changed: false, moved: 0, reason: "Split target is already the source node" };

  const updateFrom = db.prepare("UPDATE observations SET from_id = ? WHERE id = ? AND from_id = ?");
  const updateTo = db.prepare("UPDATE observations SET to_id = ? WHERE id = ? AND to_id = ?");
  const updateSource = db.prepare("UPDATE observations SET source_entity = ? WHERE id = ? AND lower(source_entity) = lower(?)");
  let moved = 0;
  for (const id of ids) {
    const fromResult = updateFrom.run(canonicalId, id, sourceId);
    const toResult = updateTo.run(canonicalId, id, sourceId);
    const changes = fromResult.changes + toResult.changes;
    if (changes) updateSource.run(parsed.canonical, id, parsed.entities[0]);
    moved += changes;
  }
  db.prepare("DELETE FROM observations WHERE from_id = to_id").run();
  return {
    changed: moved > 0,
    moved,
    reason: moved > 0 ? `Moved ${moved} link${moved === 1 ? "" : "s"}` : "Already resolved; no listed links still belong to the source node"
  };
}

function graphFromDb() {
  const nodes = db.prepare("SELECT * FROM nodes ORDER BY name COLLATE NOCASE").all().map(row => ({
    id: row.id,
    name: row.name,
    aliases: parseJson(row.aliases, []),
    x: row.x,
    y: row.y
  }));
  const nameById = new Map(nodes.map(node => [node.id, node.name]));
  const observations = db.prepare("SELECT * FROM observations ORDER BY id").all().map(row => ({
    id: row.id,
    from: nameById.get(row.from_id) || row.from_id,
    to: nameById.get(row.to_id) || row.to_id,
    confidence: row.confidence,
    sourceEntity: row.source_entity || "",
    provider: row.provider || "",
    model: row.model || "",
    createdAt: row.created_at
  }));
  return {
    version: 2,
    savedAt: getSetting("savedAt", nowIso()),
    nodes,
    observations,
    expanded: db.prepare("SELECT id FROM nodes WHERE expanded = 1 ORDER BY name COLLATE NOCASE").all().map(row => row.id),
    pan: parseJson(getSetting("pan", ""), { x: 0, y: 0 }),
    zoom: Number(getSetting("zoom", "1")) || 1,
    minConfidence: clampConfidence(getSetting("minConfidence", "0"))
  };
}

function writeGraphToDb(data, options = {}) {
  const graph = sanitizeGraphData(data);
  const timestamp = nowIso();
  try {
    db.exec("BEGIN");
    db.exec("DELETE FROM observations; DELETE FROM nodes;");
    const touchedNames = new Set();
    for (const node of graph.nodes) {
      touchedNames.add(node.name);
      for (const alias of node.aliases || []) touchedNames.add(alias);
      upsertNode(node.name, {
        aliases: node.aliases,
        x: node.x,
        y: node.y,
        expanded: graph.expanded.includes(node.id)
      });
    }
    const insertObservation = db.prepare(`
      INSERT INTO observations (from_id, to_id, confidence, source_entity, provider, model, created_at)
      VALUES (?, ?, ?, ?, ?, ?, ?)
    `);
    for (const observation of Array.isArray(data && data.observations) ? data.observations : graph.observations) {
      const fromId = upsertNode(observation.from);
      const toId = upsertNode(observation.to);
      if (!fromId || !toId || fromId === toId) continue;
      touchedNames.add(observation.from);
      touchedNames.add(observation.to);
      if (observation.sourceEntity) touchedNames.add(observation.sourceEntity);
      insertObservation.run(
        fromId,
        toId,
        Number(clampConfidence(observation.confidence).toFixed(3)),
        normalizeName(observation.sourceEntity || ""),
        normalizeName(observation.provider || ""),
        normalizeName(observation.model || ""),
        normalizeName(observation.createdAt || "") || timestamp
      );
    }
    setSetting("savedAt", timestamp);
    setSetting("pan", JSON.stringify(graph.pan));
    setSetting("zoom", graph.zoom);
    setSetting("minConfidence", graph.minConfidence);
    db.exec("COMMIT");
    if (options.runIdentityHygiene) localIdentityHygiene([...touchedNames]);
  } catch (error) {
    db.exec("ROLLBACK");
    throw error;
  }
  return graphFromDb();
}

function migrateJsonToDb() {
  const hasRows = db.prepare("SELECT COUNT(*) AS count FROM nodes").get().count > 0;
  if (hasRows || !fs.existsSync(DATA_FILE)) return;
  const raw = fs.readFileSync(DATA_FILE, "utf8").replace(/^\uFEFF/, "");
  const graph = sanitizeGraphData(JSON.parse(raw));
  writeGraphToDb(graph, { runIdentityHygiene: true });
  console.log(`Migrated ${graph.nodes.length} nodes and ${graph.observations.length} observations to ${DB_FILE}`);
}

function mockInfluences(entity) {
  const seed = normalizeName(entity);
  const library = {
    "dayz": {
      influencedBy: [
        ["ARMA 2", 0.96],
        ["Operation Flashpoint: Cold War Crisis", 0.78],
        ["Survival horror games", 0.66],
        ["Zombie apocalypse fiction", 0.62],
        ["Military simulation games", 0.72]
      ],
      influenced: [
        ["PUBG: Battlegrounds", 0.74],
        ["Fortnite Battle Royale", 0.46],
        ["Rust", 0.57],
        ["H1Z1", 0.69],
        ["Escape from Tarkov", 0.42]
      ]
    },
    "joy division": {
      influencedBy: [
        ["David Bowie", 0.82],
        ["The Velvet Underground", 0.78],
        ["Iggy Pop", 0.72],
        ["Kraftwerk", 0.53],
        ["The Doors", 0.49]
      ],
      influenced: [
        ["Interpol", 0.88],
        ["The Cure", 0.61],
        ["Nine Inch Nails", 0.55],
        ["Editors", 0.76],
        ["The National", 0.52]
      ]
    },
    "david bowie": {
      influencedBy: [
        ["The Velvet Underground", 0.86],
        ["The Beatles", 0.69],
        ["Little Richard", 0.63],
        ["Anthony Newley", 0.61],
        ["Jacques Brel", 0.58]
      ],
      influenced: [
        ["Joy Division", 0.82],
        ["Nine Inch Nails", 0.77],
        ["Lady Gaga", 0.74],
        ["Brian Eno", 0.55],
        ["Gary Numan", 0.67]
      ]
    }
  };
  const known = library[seed.toLowerCase()];
  if (known) {
    return sanitizeInfluenceResponse(seed, {
      entity: seed,
      influencedBy: known.influencedBy.map(([name, confidence]) => ({ entity: name, confidence })),
      influenced: known.influenced.map(([name, confidence]) => ({ entity: name, confidence }))
    });
  }
  return sanitizeInfluenceResponse(seed, {
    entity: seed,
    influencedBy: [],
    influenced: []
  });
}

function buildInfluenceMessages(entity, context = {}) {
  const cleanContext = sanitizeExpansionContext(context);
  const system = [
    "You generate influence graph data as strict json.",
    "Return only a JSON object matching this shape:",
    "{\"entity\":\"Name\",\"influencedBy\":[{\"entity\":\"Name\",\"confidence\":0.0}],\"influenced\":[{\"entity\":\"Name\",\"confidence\":0.0}]}",
    "Use the best canonical display name for every entity, similar to a Wikipedia article title.",
    "If the user gives lowercase, slang, initials, or a partial title, put the official commonly recognized name in entity.",
    "If the requested name is ambiguous, use the supplied local graph context to choose the intended entity sense.",
    "If a bare name could mean multiple entities, disambiguate the entity field with a concise parenthetical qualifier such as medium, year, role, or domain.",
    "For works with likely title ambiguity, prefer concise year plus medium disambiguation such as \"The Shining (1980 film)\" or \"Quake (1996 video game)\".",
    "Do not add years to people, bands, movements, genres, or uniquely named entities unless the year is part of the recognized name.",
    "Prefer discoveredFrom context over earlier expansion results when the two conflict, because discoveredFrom describes why the node entered the graph.",
    "Do not switch to a broader generic concept just because the bare text is common; keep the specific contextual entity.",
    "Use context only to identify the intended entity sense; do not limit the output to the supplied context.",
    "Treat context as disambiguation metadata, not as evidence that a relationship is true and not as a request to repeat or favor those entities.",
    "Once the entity sense is resolved, reason from general knowledge about that entity, and include only relationships you would still judge significant without seeing the context.",
    "After resolving the intended entity, include a compact useful set of broader significant influences and things influenced by it, including relationships not already present in the context.",
    "Avoid returning only the supplied context relationships unless no other confident relationships are known.",
    "Do not invent, pad, or force relationships just to make the graph grow; empty or small arrays are acceptable when confidence is low.",
    "Confidence is an estimated probability from 0 to 1.",
    "Use any kind of cultural entity: people, works, media, scenes, styles, technologies, or movements.",
    "Do not include explanations, sources, markdown, comments, or extra keys.",
    "Return at most 8 influencedBy items and at most 8 influenced items.",
    "Prefer the strongest relationships over coverage; avoid weak filler."
  ].join(" ");

  const user = JSON.stringify({
    task: "Resolve the canonical contextual entity name, then generate influence graph arrays.",
    entity,
    context: cleanContext
  });

  return {
    messages: [
      { role: "system", content: system },
      { role: "user", content: user }
    ],
    context: cleanContext
  };
}

function topNeighborsForId(id, limit = 8) {
  const rows = db.prepare(`
    SELECT other.name, MAX(o.confidence) AS confidence, COUNT(*) AS count
    FROM (
      SELECT to_id AS other_id, confidence FROM observations WHERE from_id = ?
      UNION ALL
      SELECT from_id AS other_id, confidence FROM observations WHERE to_id = ?
    ) o
    JOIN nodes other ON other.id = o.other_id
    GROUP BY other.id
    ORDER BY confidence DESC, count DESC, other.name COLLATE NOCASE
    LIMIT ?
  `).all(id, id, limit);
  return rows.map(row => row.name);
}

function identityCandidatesFor(entity, context = {}) {
  const query = normalizeName(entity);
  const queryKey = keyFor(query);
  if (!queryKey) return [];
  const nodes = db.prepare("SELECT id, name, aliases, expanded FROM nodes ORDER BY name COLLATE NOCASE").all()
    .map(row => ({
      id: row.id,
      name: row.name,
      aliases: parseJson(row.aliases, []),
      expanded: Boolean(row.expanded)
    }));
  const scored = new Map();

  function add(node, reason, score) {
    if (!node || !node.id) return;
    const current = scored.get(node.id);
    if (!current || score > current.score) {
      scored.set(node.id, { node, reason, score });
    }
  }

  for (const node of nodes) {
    const names = [node.name, ...(node.aliases || [])];
    const keys = names.map(keyFor);
    if (node.id === queryKey || keys.includes(queryKey)) add(node, "exact name or alias", 1);
    if (compactKey(node.name) === compactKey(query)) add(node, "same compact form", 0.94);
    if (baseTitleKey(node.name) && baseTitleKey(node.name) === baseTitleKey(query)) add(node, "same base title", 0.78);
    if (isAcronymLike(query) && acronymFor(node.name) === queryKey) add(node, "query matches initials", 0.86);
    if (isAcronymLike(node.name) && acronymFor(query) === node.id) add(node, "candidate matches initials", 0.82);
    if (node.id.includes(queryKey) || queryKey.includes(node.id)) add(node, "name contains query", 0.7);
    if (tokenOverlapScore(node.name, query) >= 0.75) add(node, "high token overlap", 0.68);
  }

  const cleanContext = sanitizeExpansionContext(context);
  const contextKeys = new Set([
    ...cleanContext.connectedTo.map(item => keyFor(item.entity)),
    ...cleanContext.discoveredFrom.map(item => keyFor(item.sourceEntity))
  ].filter(Boolean));

  return [...scored.values()]
    .sort((a, b) => b.score - a.score || a.node.name.localeCompare(b.node.name))
    .slice(0, 8)
    .map(item => ({
      name: item.node.name,
      aliases: item.node.aliases.slice(0, 6),
      expanded: item.node.expanded,
      matchReason: item.reason,
      score: Number(item.score.toFixed(2)),
      topNeighbors: topNeighborsForId(item.node.id, 8),
      contextOverlap: topNeighborsForId(item.node.id, 12).filter(name => contextKeys.has(keyFor(name))).slice(0, 5)
    }));
}

function shouldResolveIdentity(entity, context = {}) {
  const clean = sanitizeExpansionContext(context);
  const candidates = identityCandidatesFor(entity, clean);
  const bare = !hasDisambiguator(entity) && keyFor(entity).split(/\s+/).length <= 3;
  const hasContext = clean.connectedTo.length || clean.discoveredFrom.length;
  const strongCandidates = candidates.filter(candidate => keyFor(candidate.name) !== keyFor(entity) && candidate.score >= 0.82);
  const hasContextOverlap = candidates.some(candidate => keyFor(candidate.name) !== keyFor(entity) && candidate.contextOverlap.length);
  return {
    candidates,
    shouldResolve: Boolean(strongCandidates.length || (bare && hasContext && hasContextOverlap))
  };
}

function buildIdentityMessages(entity, context = {}) {
  const cleanContext = sanitizeExpansionContext(context);
  const { candidates } = shouldResolveIdentity(entity, cleanContext);
  const system = [
    "You resolve entity identity for an influence graph as strict json.",
    "Return only this JSON shape:",
    "{\"canonicalName\":\"Name\",\"displayName\":\"Name\",\"disambiguation\":\"short phrase\",\"aliases\":[\"Name\"],\"decision\":\"same|rename|merge|split|new|uncertain\",\"confidence\":0.0,\"reason\":\"short reason\"}",
    "Use local candidates and context only to identify what exact entity is intended.",
    "Do not decide influence relationships here.",
    "Do not merge series/franchises with individual works, adaptations with sources, sequels with originals, or broad concepts with specific entities.",
    "When a bare title likely means a specific work, use a concise Wikipedia-style disambiguated canonicalName.",
    "For ambiguous works, prefer year plus medium in canonicalName when commonly known, but do not add years to people, bands, genres, or movements.",
    "If unsure, use decision uncertain and keep the most literal canonicalName."
  ].join(" ");
  const user = JSON.stringify({
    entity,
    localCandidates: candidates,
    context: cleanContext
  });
  return {
    messages: [
      { role: "system", content: system },
      { role: "user", content: user }
    ],
    candidates,
    context: cleanContext
  };
}

function sanitizeIdentityResponse(entity, data) {
  const canonicalName = normalizeName(data && data.canonicalName) || normalizeName(entity);
  const displayNameValue = normalizeName(data && data.displayName) || canonicalName;
  const allowed = new Set(["same", "rename", "merge", "split", "new", "uncertain"]);
  const decision = allowed.has(normalizeName(data && data.decision).toLowerCase())
    ? normalizeName(data.decision).toLowerCase()
    : "uncertain";
  return {
    canonicalName,
    displayName: displayNameValue,
    disambiguation: normalizeName(data && data.disambiguation),
    aliases: Array.isArray(data && data.aliases) ? data.aliases.map(normalizeName).filter(Boolean).slice(0, 8) : [],
    decision,
    confidence: clampConfidence(data && data.confidence),
    reason: normalizeName(data && data.reason)
  };
}

async function callIdentityResolution(entity, context = {}, config = providerConfig()) {
  const prompt = buildIdentityMessages(entity, context);
  const result = await callChatCompletion(config, prompt, {
    temperature: 0.1,
    maxTokens: configuredMaxTokens(),
    responseSchemaName: "identity_resolution",
    responseSchema: identityResponseSchema
  });
  if (!result.content) throw new Error(`${config.provider} returned empty identity content`);
  return {
    data: sanitizeIdentityResponse(entity, JSON.parse(result.content)),
    prompt,
    usage: result.usage,
    finishReason: result.finishReason,
    requestMs: result.ms,
    queueMs: result.queueMs
  };
}

function usableIdentityResolution(identity) {
  if (!identity || !identity.canonicalName) return false;
  if (identity.decision === "uncertain" || identity.decision === "new") return false;
  return identity.confidence >= 0.55;
}

function buildPromptBundle(stages) {
  const cleanStages = Object.fromEntries(
    Object.entries(stages || {}).filter(([, prompt]) => prompt && Array.isArray(prompt.messages))
  );
  const messages = [];
  for (const [stage, prompt] of Object.entries(cleanStages)) {
    for (const message of prompt.messages) {
      messages.push({
        ...message,
        stage
      });
    }
  }
  return {
    stages: cleanStages,
    messages
  };
}

async function callDeepSeek(entity, context = {}, config = providerConfig()) {
  const prompt = buildInfluenceMessages(entity, context);
  const result = await callChatCompletion(config, prompt, {
    temperature: 0.55,
    maxTokens: configuredMaxTokens(),
    responseSchemaName: "influence_expansion",
    responseSchema: influenceResponseSchema
  });
  if (!result.content) throw new Error(`${config.provider} returned empty content`);
  return {
    data: sanitizeInfluenceResponse(entity, JSON.parse(result.content)),
    prompt,
    usage: result.usage,
    finishReason: result.finishReason,
    requestMs: result.ms,
    queueMs: result.queueMs
  };
}

function buildDedupeMessages(candidates) {
  const entityNames = db.prepare("SELECT name FROM nodes ORDER BY name COLLATE NOCASE").all()
    .map(row => row.name)
    .slice(0, 700);
  const system = [
    "You review possible duplicate entity names in an influence graph.",
    "Return strict JSON only matching this shape:",
    "{\"groups\":[{\"canonical\":\"Name\",\"entities\":[\"Name\"],\"confidence\":0.0,\"reason\":\"short reason\"}]}",
    "Use every practical dedupe method you can: canonical titles, case differences, punctuation differences, acronyms, alternate spellings, subtitles, series/work disambiguation, and common aliases.",
    "Only group names that refer to the same real entity, work, person, series, or concept.",
    "Treat bare titles plus clarifying descriptors as possible duplicates only when they clearly mean the same entity, such as a title and the same title with year/medium disambiguation.",
    "Do not merge broad related concepts with specific works.",
    "Do not merge a franchise or series with an individual installment unless the names clearly refer to the same entity.",
    "Do not merge adaptations, remakes, sequels, parent series, lists, genres, scenes, or eras with the specific work/entity.",
    "Use a Wikipedia-style canonical title where possible.",
    "Keep reasons short and do not include markdown."
  ].join(" ");

  const user = JSON.stringify({ candidates, entityNames }).slice(0, 18000);
  return {
    messages: [
      { role: "system", content: system },
      { role: "user", content: user }
    ]
  };
}

async function callDedupeReview(candidates, config = providerConfig()) {
  const prompt = buildDedupeMessages(candidates);
  if (!config.apiKey || !candidates.length) {
    return { groups: [], prompt };
  }

  const result = await callChatCompletion(config, prompt, {
    temperature: 0.1,
    maxTokens: configuredMaxTokens(),
    responseSchemaName: "dedupe_review",
    responseSchema: dedupeResponseSchema
  });
  if (!result.content) return { groups: [], prompt };
  const parsed = JSON.parse(result.content);
  return {
    groups: Array.isArray(parsed.groups) ? parsed.groups.map(group => ({
      canonical: normalizeName(group.canonical),
      entities: Array.isArray(group.entities) ? group.entities.map(normalizeName).filter(Boolean) : [],
      confidence: clampConfidence(group.confidence),
      reason: normalizeName(group.reason)
    })).filter(group => group.canonical && group.entities.length > 1) : [],
    prompt,
    usage: result.usage,
    finishReason: result.finishReason,
    requestMs: result.ms,
    queueMs: result.queueMs
  };
}

function scopedDedupeCandidates(names = []) {
  return targetedDedupeCandidates(names).slice(0, 12);
}

function buildScopedIdentityMessages(names, candidates) {
  const compactCandidates = (candidates || []).slice(0, 12).map(candidate => ({
    reason: candidate.reason,
    entities: (candidate.entities || []).slice(0, 6)
  }));
  const system = [
    "You adjudicate identity hygiene for an influence graph as strict json.",
    "Return only this JSON shape:",
    "{\"groups\":[{\"action\":\"merge|split|keep-separate\",\"canonical\":\"Name\",\"entities\":[\"Name\"],\"confidence\":0.0,\"reason\":\"short reason\"}]}",
    "Use merge only when all entities are the same exact real entity or title.",
    "Use split when a bare or ambiguous entity appears to contain observations for a different specific sense.",
    "Use keep-separate for franchises vs individual works, adaptations vs sources, sequels vs originals, broad concepts vs specific entities, and shared names that are different things.",
    "Prefer no group over a weak group.",
    "Keep reasons short."
  ].join(" ");
  const user = JSON.stringify({
    touchedEntities: [...new Set((names || []).map(normalizeName).filter(Boolean))].slice(0, 40),
    heuristicCandidates: compactCandidates
  });
  return {
    messages: [
      { role: "system", content: system },
      { role: "user", content: user }
    ],
    candidates: compactCandidates
  };
}

function sanitizeScopedIdentityGroups(groups) {
  return (Array.isArray(groups) ? groups : [])
    .map(group => {
      const action = normalizeName(group.action).toLowerCase();
      return {
        action: ["merge", "split", "keep-separate"].includes(action) ? action : "keep-separate",
        canonical: normalizeName(group.canonical),
        entities: Array.isArray(group.entities) ? group.entities.map(normalizeName).filter(Boolean).slice(0, 8) : [],
        confidence: clampConfidence(group.confidence),
        reason: normalizeName(group.reason)
      };
    })
    .filter(group => group.canonical && group.entities.length > 1 && group.confidence >= 0.6);
}

async function callScopedIdentityReview(names, candidates, config = providerConfig()) {
  const prompt = buildScopedIdentityMessages(names, candidates);
  if (!config.apiKey || !candidates.length) {
    return { groups: [], prompt };
  }

  const result = await callChatCompletion(config, prompt, {
    temperature: 0.1,
    maxTokens: configuredMaxTokens(),
    responseSchemaName: "scoped_identity_review",
    responseSchema: scopedIdentityResponseSchema
  });
  if (!result.content) return { groups: [], prompt };
  const parsed = JSON.parse(result.content);
  return {
    groups: sanitizeScopedIdentityGroups(parsed.groups),
    prompt,
    usage: result.usage,
    finishReason: result.finishReason,
    requestMs: result.ms,
    queueMs: result.queueMs
  };
}

function suggestionKey(entities) {
  return [...new Set((entities || []).map(keyFor).filter(Boolean))].sort().join("|");
}

function rowToSuggestion(row) {
  return {
    id: row.id,
    canonical: row.canonical,
    entities: parseJson(row.entities, []),
    confidence: row.confidence,
    reason: row.reason,
    action: row.action || "merge",
    metadata: parseJson(row.metadata, {}),
    status: row.status,
    createdAt: row.created_at,
    updatedAt: row.updated_at,
    reviewedAt: row.reviewed_at || ""
  };
}

function pendingDedupeSuggestions() {
  return db.prepare(`
    SELECT * FROM dedupe_suggestions
    WHERE status = 'pending'
    ORDER BY confidence DESC, created_at DESC
  `).all().map(rowToSuggestion);
}

function storeDedupeSuggestions(groups) {
  const timestamp = nowIso();
  const existingPending = db.prepare(`
    SELECT id FROM dedupe_suggestions
    WHERE entities_key = ? AND status = 'pending'
  `);
  const update = db.prepare(`
    UPDATE dedupe_suggestions
    SET canonical = ?,
        entities = ?,
        confidence = ?,
        reason = ?,
        action = ?,
        metadata = ?,
        updated_at = ?
    WHERE id = ?
  `);
  const insert = db.prepare(`
    INSERT INTO dedupe_suggestions (canonical, entities, confidence, reason, action, metadata, status, entities_key, created_at, updated_at)
    VALUES (?, ?, ?, ?, ?, ?, 'pending', ?, ?, ?)
  `);
  let stored = 0;
  for (const group of groups || []) {
    const entities = [...new Set((group.entities || []).map(normalizeName).filter(Boolean))];
    const key = suggestionKey(entities);
    if (!group.canonical || entities.length < 2 || !key) continue;
    const canonical = normalizeName(group.canonical);
    const serialized = JSON.stringify(entities);
    const confidence = Number(clampConfidence(group.confidence).toFixed(3));
    const reason = normalizeName(group.reason);
    const action = ["merge", "identity", "split", "rename"].includes(group.action) ? group.action : "merge";
    const metadata = JSON.stringify(group.metadata || {});
    const existing = existingPending.get(key);
    if (existing) {
      update.run(canonical, serialized, confidence, reason, action, metadata, timestamp, existing.id);
    } else {
      insert.run(canonical, serialized, confidence, reason, action, metadata, key, timestamp, timestamp);
    }
    stored += 1;
  }
  return stored;
}

function splitCandidatesFor(requestedEntity, canonicalEntity, context = {}) {
  const requestedId = keyFor(requestedEntity);
  const canonicalId = keyFor(canonicalEntity);
  if (!requestedId || !canonicalId || requestedId === canonicalId) return [];
  const cleanContext = sanitizeExpansionContext(context);
  const contextKeys = new Set();
  for (const item of cleanContext.discoveredFrom) {
    if (item.sourceEntity) contextKeys.add(keyFor(item.sourceEntity));
  }
  for (const item of cleanContext.connectedTo) {
    if (item.entity) contextKeys.add(keyFor(item.entity));
  }
  if (!contextKeys.size) return [];

  const rows = db.prepare(`
    SELECT o.id, o.from_id, o.to_id, o.confidence, o.source_entity,
           nf.name AS from_name, nt.name AS to_name
    FROM observations o
    JOIN nodes nf ON nf.id = o.from_id
    JOIN nodes nt ON nt.id = o.to_id
    WHERE o.from_id = ? OR o.to_id = ?
    ORDER BY o.confidence DESC, o.id DESC
  `).all(requestedId, requestedId);

  return rows
    .filter(row => {
      const otherId = row.from_id === requestedId ? row.to_id : row.from_id;
      return contextKeys.has(otherId) || contextKeys.has(keyFor(row.source_entity || ""));
    })
    .map(row => ({
      id: row.id,
      from: row.from_name,
      to: row.to_name,
      confidence: row.confidence,
      sourceEntity: row.source_entity || ""
    }))
    .slice(0, 24);
}

function expansionIdentityGroup(requestedEntity, canonicalEntity, context = {}, identity = null) {
  const requested = normalizeName(requestedEntity);
  const canonical = normalizeName(canonicalEntity);
  if (!requested || !canonical || keyFor(requested) === keyFor(canonical)) return null;
  const moveCandidates = splitCandidatesFor(requested, canonical, context);
  const confidence = clampConfidence(identity && identity.confidence);
  return {
    canonical,
    entities: [requested, canonical],
    confidence: confidence || (moveCandidates.length ? 0.72 : 0.66),
    action: moveCandidates.length ? "split" : "rename",
    metadata: {
      moveObservationIds: moveCandidates.map(item => item.id),
      moveCandidates,
      identityDecision: identity && identity.decision,
      identityReason: identity && identity.reason
    },
    reason: moveCandidates.length
      ? `Split review: model resolved "${requested}" as "${canonical}" and found ${moveCandidates.length} context-linked observation${moveCandidates.length === 1 ? "" : "s"} that may belong there`
      : `Expansion identity review: model resolved "${requested}" as "${canonical}"`
  };
}

function storeExpansionIdentitySuggestion(requestedEntity, canonicalEntity, context = {}, identity = null) {
  const group = expansionIdentityGroup(requestedEntity, canonicalEntity, context, identity);
  return group ? storeDedupeSuggestions([group]) : 0;
}

function canonicalFromNames(names) {
  const sorted = [...names].sort((a, b) => {
    const acronymA = acronymFor(a) === keyFor(a);
    const acronymB = acronymFor(b) === keyFor(b);
    if (acronymA !== acronymB) return acronymA ? 1 : -1;
    return b.length - a.length || a.localeCompare(b);
  });
  return sorted[0] || names[0] || "";
}

function identityKind(name) {
  const key = keyFor(name);
  const kinds = new Set();
  if (/\bfilm\b|\bmovie\b/.test(key)) kinds.add("film");
  if (/\bvideo game\b|\bgame\b/.test(key)) kinds.add("game");
  if (/\bseries\b|\bfranchise\b/.test(key)) kinds.add("series");
  if (/\balbum\b/.test(key)) kinds.add("album");
  if (/\bnovel\b|\bbook\b/.test(key)) kinds.add("book");
  if (/\bband\b/.test(key)) kinds.add("band");
  if (/\btv\b|\btelevision\b/.test(key)) kinds.add("television");
  return kinds;
}

function identityYears(name) {
  return [...keyFor(name).matchAll(/\b(19|20)\d{2}\b/g)].map(match => match[0]);
}

function compatibleTitleVariants(names) {
  const clean = (names || []).map(normalizeName).filter(Boolean);
  if (clean.length < 2) return false;
  const base = baseTitleKey(clean[0]);
  if (!base || clean.some(name => baseTitleKey(name) !== base)) return false;

  const yearSets = clean.map(identityYears).filter(years => years.length);
  const years = new Set(yearSets.flat());
  if (yearSets.length > 1 && years.size > 1) return false;

  const kindSets = clean.map(identityKind);
  const explicitKinds = kindSets.filter(set => set.size);
  if (!explicitKinds.length) return false;
  const allKinds = new Set(explicitKinds.flatMap(set => [...set]));
  if (allKinds.has("series") && allKinds.size > 1) return false;

  const compatiblePairs = [
    ["film"],
    ["game"],
    ["album"],
    ["book"],
    ["band"],
    ["television"]
  ];
  return compatiblePairs.some(group => {
    const allowed = new Set(group);
    return explicitKinds.every(set => [...set].every(kind => allowed.has(kind)));
  });
}

function identityConflictReason(names = []) {
  const clean = (names || []).map(normalizeName).filter(Boolean);
  const kindSets = clean.map(identityKind);
  const explicitKinds = kindSets.filter(set => set.size);
  const allKinds = new Set(explicitKinds.flatMap(set => [...set]));
  if (allKinds.has("series") && allKinds.size > 1) {
    return "series/franchise mixed with a specific work";
  }

  const yearSets = clean.map(identityYears).filter(years => years.length);
  const years = new Set(yearSets.flat());
  if (yearSets.length > 1 && years.size > 1) {
    return "conflicting years";
  }

  const bases = new Set(clean.map(baseTitleKey).filter(Boolean));
  if (bases.size === 1 && clean.some(hasDisambiguator) && !compatibleTitleVariants(clean)) {
    return "same title but incompatible disambiguators";
  }
  return "";
}

function canAutoApplyIdentity(group) {
  if (!IDENTITY_AUTO_APPROVE || !group) return { ok: false, reason: "automatic identity approval disabled" };
  const action = group.action === "split" ? "split" : group.action === "merge" || group.action === "rename" ? "merge" : "";
  if (!action) return { ok: false, reason: "not an actionable identity edit" };
  const entities = [...new Set((group.entities || []).map(normalizeName).filter(Boolean))];
  if (entities.length < 2) return { ok: false, reason: "needs at least two entities" };
  if (entities.length > 4) return { ok: false, reason: "too many entities for automatic edit" };
  const confidence = clampConfidence(group.confidence);
  const threshold = action === "split" ? IDENTITY_AUTO_SPLIT_CONFIDENCE : IDENTITY_AUTO_MERGE_CONFIDENCE;
  if (confidence < threshold) return { ok: false, reason: `confidence below ${Math.round(threshold * 100)}%` };
  const conflict = identityConflictReason([group.canonical, ...entities]);
  if (conflict) return { ok: false, reason: conflict };
  if (action === "split") {
    const ids = group.metadata && Array.isArray(group.metadata.moveObservationIds) ? group.metadata.moveObservationIds : [];
    if (!ids.length) return { ok: false, reason: "split has no exact movable observation ids" };
  }
  return { ok: true, reason: "high-confidence LLM identity decision" };
}

function applyAutoIdentityGroups(groups = []) {
  const applied = [];
  const queued = [];
  const skipped = [];
  for (const group of groups || []) {
    if (!group || group.action === "keep-separate") {
      skipped.push({ action: group && group.action || "keep-separate", reason: group && group.reason || "keep separate" });
      continue;
    }
    const gate = canAutoApplyIdentity(group);
    if (!gate.ok) {
      queued.push({
        ...group,
        metadata: {
          ...(group.metadata || {}),
          autoApplySkipped: gate.reason
        },
        reason: `${group.reason || "LLM identity decision"}; queued because ${gate.reason}`
      });
      continue;
    }
    db.exec("BEGIN");
    try {
      const result = group.action === "split"
        ? applySplitDecision(group)
        : group.action === "rename"
          ? renameNodeInDb(group.entities[0], group.canonical)
          : mergeNodes(group.canonical, group.entities);
      db.exec("COMMIT");
      applied.push({
        action: group.action,
        canonical: group.canonical,
        entities: group.entities,
        confidence: clampConfidence(group.confidence),
        reason: group.reason || gate.reason,
        result
      });
    } catch (error) {
      db.exec("ROLLBACK");
      queued.push({
        ...group,
        metadata: {
          ...(group.metadata || {}),
          autoApplyError: error.message
        },
        reason: `${group.reason || "LLM identity decision"}; queued because automatic apply failed: ${error.message}`
      });
    }
  }
  if (applied.length) {
    setSetting("savedAt", nowIso());
  }
  return { applied, queued, skipped };
}

function localDedupeSuggestions(candidates) {
  const trustedReasons = new Set([
    "same compact punctuation-free form",
    "exact acronym-name match",
    "same base title with compatible disambiguators"
  ]);
  return (candidates || [])
    .filter(candidate => trustedReasons.has(candidate.reason)
      || (candidate.reason === "same base title with disambiguator" && compatibleTitleVariants(candidate.entities)))
    .map(candidate => ({
      canonical: canonicalFromNames(candidate.entities || []),
      entities: candidate.entities || [],
      confidence: candidate.reason.includes("base title") ? 0.68 : 0.72,
      reason: `Local heuristic: ${candidate.reason}`
    }))
    .filter(group => group.canonical && group.entities.length > 1);
}

function targetedTitleVariantCandidates(names = []) {
  const touched = new Set((names || []).map(keyFor).filter(Boolean));
  if (!touched.size) return [];
  const nodes = db.prepare("SELECT id, name FROM nodes ORDER BY name COLLATE NOCASE").all();
  const byBase = new Map();
  for (const node of nodes) {
    const base = baseTitleKey(node.name);
    if (!base || !hasDisambiguator(node.name)) continue;
    const bucket = byBase.get(base) || [];
    bucket.push(node);
    byBase.set(base, bucket);
  }

  const groups = new Map();
  for (const bucket of byBase.values()) {
    for (let i = 0; i < bucket.length; i += 1) {
      for (let j = i + 1; j < bucket.length; j += 1) {
        const a = bucket[i];
        const b = bucket[j];
        if (!touched.has(a.id) && !touched.has(b.id)) continue;
        if (!compatibleTitleVariants([a.name, b.name])) continue;
        const entities = [a.name, b.name];
        const key = suggestionKey(entities);
        if (!groups.has(key)) {
          groups.set(key, {
            reason: "same base title with compatible disambiguators",
            entities
          });
        }
      }
    }
  }
  return [...groups.values()];
}

function nodesForIdentityScan() {
  return db.prepare("SELECT id, name, aliases FROM nodes ORDER BY name COLLATE NOCASE").all()
    .map(row => {
      const aliases = parseJson(row.aliases, []);
      return {
        id: row.id,
        name: row.name,
        aliases,
        allNames: [row.name, ...aliases].map(normalizeName).filter(Boolean)
      };
    });
}

function targetedDedupeCandidates(names = []) {
  const inputNames = [...new Set((names || []).map(normalizeName).filter(Boolean))];
  const touchedKeys = new Set(inputNames.map(keyFor).filter(Boolean));
  if (!touchedKeys.size) return [];

  const nodes = nodesForIdentityScan();
  const knownKeys = new Set();
  for (const node of nodes) {
    knownKeys.add(node.id);
    for (const name of node.allNames) knownKeys.add(keyFor(name));
  }
  for (const name of inputNames) {
    const key = keyFor(name);
    if (!key || knownKeys.has(key)) continue;
    nodes.push({
      id: key,
      name,
      aliases: [],
      allNames: [name]
    });
    knownKeys.add(key);
  }

  const touchedNodes = nodes.filter(node => touchedKeys.has(node.id)
    || node.allNames.some(name => touchedKeys.has(keyFor(name))));
  if (!touchedNodes.length) return targetedTitleVariantCandidates(names);

  const groups = new Map();
  function add(reason, items) {
    const uniqueById = new Map();
    for (const item of items || []) {
      if (item && item.id && item.name) uniqueById.set(item.id, item);
    }
    const namesForKey = [...uniqueById.values()].map(item => item.name);
    if (namesForKey.length < 2) return;
    const key = namesForKey.map(keyFor).sort().join("|");
    if (!groups.has(key)) groups.set(key, { reason, entities: namesForKey });
  }

  const seenPairs = new Set();
  for (const touched of touchedNodes) {
    for (const node of nodes) {
      if (touched.id === node.id) continue;
      const pairKey = [touched.id, node.id].sort().join("|");
      if (seenPairs.has(pairKey)) continue;
      seenPairs.add(pairKey);

      const touchedNames = touched.allNames.length ? touched.allNames : [touched.name];
      const nodeNames = node.allNames.length ? node.allNames : [node.name];
      const bestNames = [touched.name, node.name];

      if (touched.id.replace(/s$/, "") === node.id.replace(/s$/, "")) {
        add("same normalized singular form", [touched, node]);
      }

      if (touched.id.length >= 4 && node.id.length >= 4 && (touched.id.includes(node.id) || node.id.includes(touched.id))) {
        add("one normalized name contains the other", [touched, node]);
      }

      for (const touchedName of touchedNames) {
        for (const nodeName of nodeNames) {
          const touchedCompact = compactKey(touchedName);
          const nodeCompact = compactKey(nodeName);
          if (touchedCompact.length >= 5 && touchedCompact === nodeCompact) {
            add("same compact punctuation-free form", [touched, node]);
          }
          if (isAcronymLike(touchedName) && acronymFor(nodeName) === keyFor(touchedName)) {
            add("exact acronym-name match", [touched, node]);
          }
          if (isAcronymLike(nodeName) && acronymFor(touchedName) === keyFor(nodeName)) {
            add("exact acronym-name match", [touched, node]);
          }
          if (tokenOverlapScore(touchedName, nodeName) >= 0.8) {
            add("high token overlap", [touched, node]);
          }
          if (hasDisambiguator(touchedName)
            && hasDisambiguator(nodeName)
            && baseTitleKey(touchedName) === baseTitleKey(nodeName)
            && compatibleTitleVariants([touchedName, nodeName])) {
            add("same base title with compatible disambiguators", [touched, node]);
          }
          if (hasDisambiguator(touchedName) && baseTitleKey(touchedName) === keyFor(nodeName)) {
            add("same base title with disambiguator", [touched, node]);
          }
          if (hasDisambiguator(nodeName) && baseTitleKey(nodeName) === keyFor(touchedName)) {
            add("same base title with disambiguator", [touched, node]);
          }
        }
      }

      if (compatibleTitleVariants(bestNames)) {
        add("same base title with compatible disambiguators", [touched, node]);
      }
    }
  }

  for (const candidate of targetedTitleVariantCandidates(names)) {
    const key = (candidate.entities || []).map(keyFor).sort().join("|");
    if (key && !groups.has(key)) groups.set(key, candidate);
  }

  return [...groups.values()].slice(0, 80);
}

function localIdentityHygiene(names = []) {
  try {
    const autoIdentity = applyAutoIdentityGroups(localDedupeSuggestions(targetedDedupeCandidates(names)));
    return autoIdentity.applied.length + storeDedupeSuggestions(autoIdentity.queued);
  } catch (error) {
    console.warn(`Local identity hygiene skipped: ${error.message}`);
    return 0;
  }
}

function autoResolvePendingIdentity(limit = 100) {
  const suggestions = pendingDedupeSuggestions().slice(0, Math.max(1, Number(limit) || 100));
  const actionable = suggestions.filter(suggestion => ["merge", "split", "rename"].includes(suggestion.action));
  const autoIdentity = applyAutoIdentityGroups(actionable);
  const timestamp = nowIso();
  for (const item of autoIdentity.applied) {
    const key = suggestionKey(item.entities);
    if (!key) continue;
    db.prepare(`
      UPDATE dedupe_suggestions
      SET status = 'approved', updated_at = ?, reviewed_at = ?
      WHERE entities_key = ? AND status = 'pending'
    `).run(timestamp, timestamp, key);
  }
  for (const suggestion of suggestions) {
    if (suggestion.action !== "identity") continue;
    db.prepare(`
      UPDATE dedupe_suggestions
      SET status = 'approved', updated_at = ?, reviewed_at = ?
      WHERE id = ?
    `).run(timestamp, timestamp, suggestion.id);
  }
  return {
    applied: autoIdentity.applied.length,
    deferred: autoIdentity.queued.length,
    clearedChecks: suggestions.filter(suggestion => suggestion.action === "identity").length
  };
}

function namesFromInfluenceResponse(entity, data) {
  return [
    entity,
    data && data.entity,
    ...((data && data.influencedBy) || []).map(item => item.entity),
    ...((data && data.influenced) || []).map(item => item.entity)
  ].map(normalizeName).filter(Boolean);
}

function storeScopedIdentityGroups(groups) {
  const actionable = [];
  for (const group of groups || []) {
    if (group.action === "keep-separate") continue;
    actionable.push({
      canonical: group.canonical,
      entities: group.entities,
      confidence: group.confidence,
      action: group.action === "split" ? "identity" : "merge",
      metadata: {
        llmAction: group.action,
        automatedReview: true
      },
      reason: `LLM scoped identity: ${group.reason || group.action}`
    });
  }
  return storeDedupeSuggestions(actionable);
}

function actionableScopedIdentityGroups(groups) {
  const actionable = [];
  for (const group of groups || []) {
    if (group.action === "keep-separate") continue;
    if (group.action === "split") continue;
    actionable.push({
      canonical: group.canonical,
      entities: group.entities,
      confidence: group.confidence,
      action: "merge",
      metadata: {
        llmAction: group.action,
        automatedReview: true
      },
      reason: `LLM scoped identity: ${group.reason || group.action}`
    });
  }
  return actionable;
}

function acronymFor(name) {
  return normalizeName(name)
    .split(/\s+/)
    .filter(part => !["the", "a", "an", "of", "and"].includes(part.toLowerCase()))
    .map(part => part[0])
    .join("")
    .toLowerCase();
}

function isAcronymLike(name) {
  const key = keyFor(name);
  return /^[a-z0-9]{2,8}$/.test(key) && key === compactKey(name);
}

function compactKey(name) {
  return keyFor(name).replace(/\s+/g, "");
}

function baseTitleKey(name) {
  return keyFor(name)
    .replace(/\b(19|20)\d{2}s?\b/g, " ")
    .replace(/\bfilm\b|\bmovie\b|\bvideo game\b|\bgame\b|\balbum\b|\bnovel\b|\bbook\b|\bseries\b|\bfranchise\b|\btv\b|\btelevision\b/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

function hasDisambiguator(name) {
  return /[(),:]|\b(19|20)\d{2}s?\b|\bfilm\b|\bmovie\b|\bvideo game\b|\bgame\b|\balbum\b|\bnovel\b|\bbook\b|\bseries\b|\bfranchise\b|\btv\b|\btelevision\b/i.test(name);
}

function dedupeTokens(name) {
  return keyFor(name)
    .split(/\s+/)
    .map(token => token.replace(/s$/, ""))
    .filter(token => token && !["the", "a", "an", "of", "and", "or", "for", "game", "video", "s"].includes(token));
}

function tokenOverlapScore(a, b) {
  const aTokens = new Set(dedupeTokens(a));
  const bTokens = new Set(dedupeTokens(b));
  if (!aTokens.size || !bTokens.size) return 0;
  let shared = 0;
  for (const token of aTokens) {
    if (bTokens.has(token)) shared += 1;
  }
  return shared / Math.min(aTokens.size, bTokens.size);
}

function dedupeCandidatesFromDb(limit = 500) {
  const nodes = db.prepare("SELECT id, name, aliases FROM nodes ORDER BY name COLLATE NOCASE").all()
    .map(row => ({
      id: row.id,
      name: row.name,
      aliases: parseJson(row.aliases, []),
      allNames: [row.name, ...parseJson(row.aliases, [])].map(normalizeName).filter(Boolean)
    }));
  const groups = new Map();

  function add(reason, items) {
    const names = [...new Set(items.map(item => item.name).filter(Boolean))];
    if (names.length < 2) return;
    const key = names.map(keyFor).sort().join("|");
    if (!groups.has(key)) groups.set(key, { reason, entities: names });
  }

  const byNoPlural = new Map();
  const byCompact = new Map();
  const byBaseTitle = new Map();
  for (const node of nodes) {
    const singular = node.id.replace(/s$/, "");
    const bucket = byNoPlural.get(singular) || [];
    bucket.push(node);
    byNoPlural.set(singular, bucket);

    for (const name of node.allNames) {
      const compact = compactKey(name);
      if (compact.length >= 5) {
        const compactBucket = byCompact.get(compact) || [];
        compactBucket.push(node);
        byCompact.set(compact, compactBucket);
      }

      const baseTitle = baseTitleKey(name);
      if (baseTitle && baseTitle !== keyFor(name) && hasDisambiguator(name)) {
        const baseBucket = byBaseTitle.get(baseTitle) || [];
        baseBucket.push(node);
        byBaseTitle.set(baseTitle, baseBucket);
      }
    }
  }
  for (const bucket of byNoPlural.values()) add("same normalized singular form", bucket);
  for (const bucket of byCompact.values()) add("same compact punctuation-free form", bucket);
  for (const [baseTitle, bucket] of byBaseTitle) {
    const exactBase = nodes.find(node => node.id === baseTitle);
    add("same base title with disambiguator", exactBase ? [exactBase, ...bucket] : bucket);
  }

  for (let i = 0; i < nodes.length; i += 1) {
    for (let j = i + 1; j < nodes.length; j += 1) {
      const a = nodes[i];
      const b = nodes[j];
      if (a.id.length < 4 || b.id.length < 4) continue;
      if (a.id.includes(b.id) || b.id.includes(a.id)) add("one normalized name contains the other", [a, b]);
      if (isAcronymLike(a.name) && acronymFor(b.name) === a.id) add("exact acronym-name match", [a, b]);
      if (isAcronymLike(b.name) && acronymFor(a.name) === b.id) add("exact acronym-name match", [a, b]);
      if (tokenOverlapScore(a.name, b.name) >= 0.8) add("high token overlap", [a, b]);
      if (hasDisambiguator(a.name)
        && hasDisambiguator(b.name)
        && baseTitleKey(a.name) === baseTitleKey(b.name)
        && compatibleTitleVariants([a.name, b.name])) {
        add("same base title with compatible disambiguators", [a, b]);
      }
      if (hasDisambiguator(a.name) && baseTitleKey(a.name) === b.id) add("same base title with disambiguator", [a, b]);
      if (hasDisambiguator(b.name) && baseTitleKey(b.name) === a.id) add("same base title with disambiguator", [a, b]);
    }
  }

  const values = [...groups.values()];
  return Number.isFinite(limit) ? values.slice(0, limit) : values;
}

async function handleApiInfluences(req, res) {
  const started = process.hrtime.bigint();
  const timings = [];
  let entity = "";
  let generationEntity = "";
  let config = providerConfig(DEFAULT_LLM_PROVIDER);
  let identityResult = null;
  try {
    const raw = await readBody(req);
    const body = raw ? JSON.parse(raw) : {};
    entity = normalizeName(body.entity);
    const context = sanitizeExpansionContext(body.context);
    config = providerConfig(body.provider || DEFAULT_LLM_PROVIDER);
    const provider = config.provider;
    if (!entity) return sendJson(res, 400, { error: "Missing entity" });

    if (provider === "mock") {
      return sendJson(res, 200, {
        provider: "mock",
        model: "local-demo",
        data: mockInfluences(entity)
      });
    }

    if (!config.apiKey) {
      return sendJson(res, 503, {
        error: `Live generation is not configured. Start the server with ${provider === "openrouter" ? "OPENROUTER_API_KEY" : "DEEPSEEK_API_KEY"} or use mock mode in dev tools.`
      });
    }

    const identityPlan = shouldResolveIdentity(entity, context);
    generationEntity = entity;
    if (identityPlan.shouldResolve) {
      const stage = process.hrtime.bigint();
      identityResult = await callIdentityResolution(entity, context, config);
      timings.push({ stage: "identity", ms: elapsedMs(stage) });
      if (usableIdentityResolution(identityResult.data)) {
        generationEntity = identityResult.data.canonicalName;
      }
    }

    const expansionStage = process.hrtime.bigint();
    const result = await callDeepSeek(generationEntity, context, config);
    timings.push({ stage: "expansion", ms: elapsedMs(expansionStage) });
    const touchedNames = namesFromInfluenceResponse(entity, result.data);
    const identityEditStage = process.hrtime.bigint();
    const scopedCandidates = scopedDedupeCandidates(touchedNames);
    const hygieneQueued = scopedCandidates.length > 0;
    const scopedReview = hygieneQueued
      ? await callScopedIdentityReview(touchedNames, scopedCandidates, config)
      : { groups: [], prompt: null };
    const expansionIdentity = expansionIdentityGroup(entity, result.data.entity, context, identityResult && identityResult.data);
    const scopedGroups = actionableScopedIdentityGroups(scopedReview.groups);
    const autoIdentity = applyAutoIdentityGroups([
      ...(expansionIdentity ? [expansionIdentity] : []),
      ...scopedGroups
    ]);
    const identitySuggestions = storeDedupeSuggestions(autoIdentity.queued);
    const postExpansionLocalSuggestions = localIdentityHygiene(touchedNames);
    timings.push({ stage: "identity hygiene", ms: elapsedMs(identityEditStage) });
    const hygienePrompt = scopedReview.prompt || (hygieneQueued ? buildScopedIdentityMessages(touchedNames, scopedCandidates) : null);
    const responseTimings = [
      ...timings,
      { stage: "total", ms: elapsedMs(started) }
    ];
    recordProfile({
      type: "expand",
      entity,
      generationEntity,
      provider: config.provider,
      model: config.model,
      thinking: config.provider === "openrouter" ? OPENROUTER_REASONING : DEEPSEEK_THINKING === "enabled" ? "enabled" : "disabled",
      identityRan: Boolean(identityResult),
      hygieneQueued,
      autoIdentityApplied: autoIdentity.applied.length,
      autoIdentityQueued: autoIdentity.queued.length,
      scopedCandidates: scopedCandidates.length,
      timings: responseTimings,
      llm: {
        identityQueueMs: identityResult && identityResult.queueMs,
        identityRequestMs: identityResult && identityResult.requestMs,
        hygieneQueueMs: scopedReview.queueMs,
        hygieneRequestMs: scopedReview.requestMs,
        expansionQueueMs: result.queueMs,
        expansionRequestMs: result.requestMs,
        concurrency: LLM_MAX_CONCURRENCY,
        timeoutMs: LLM_REQUEST_TIMEOUT_MS,
        maxTokens: configuredMaxTokens() || null
      },
      usage: {
        identity: identityResult && identityResult.usage,
        expansion: result.usage,
        hygiene: scopedReview.usage
      },
      finishReason: {
        identity: identityResult && identityResult.finishReason,
        expansion: result.finishReason,
        hygiene: scopedReview.finishReason
      }
    });
    const response = {
      provider: config.provider,
      model: config.model,
      data: result.data,
      identity: identityResult ? identityResult.data : null,
      identityCandidates: identityPlan.candidates,
      scopedIdentityReview: {
        candidates: scopedCandidates.length,
        groups: scopedReview.groups,
        queued: hygieneQueued
      },
      identitySuggestions,
      llmHygieneSuggestions: autoIdentity.queued.length,
      localHygieneSuggestions: postExpansionLocalSuggestions,
      autoIdentityApplied: autoIdentity.applied.length,
      autoIdentity,
      hygieneQueued,
      usage: {
        identity: identityResult && identityResult.usage,
        expansion: result.usage,
        hygiene: scopedReview.usage
      },
      llm: {
        identityQueueMs: identityResult && identityResult.queueMs,
        identityRequestMs: identityResult && identityResult.requestMs,
        hygieneQueueMs: scopedReview.queueMs,
        hygieneRequestMs: scopedReview.requestMs,
        expansionQueueMs: result.queueMs,
        expansionRequestMs: result.requestMs,
        concurrency: LLM_MAX_CONCURRENCY,
        timeoutMs: LLM_REQUEST_TIMEOUT_MS,
        maxTokens: configuredMaxTokens() || null
      },
      finishReason: {
        identity: identityResult && identityResult.finishReason,
        expansion: result.finishReason,
        hygiene: scopedReview.finishReason
      },
      graph: autoIdentity.applied.length ? graphFromDb() : null,
      timings: responseTimings
    };
    if (hasAdminAccess(req, new URL(req.url, `http://${req.headers.host}`))) {
      response.debugPrompt = buildPromptBundle({
        identity: identityResult && identityResult.prompt,
        expansion: result.prompt,
        hygiene: hygienePrompt
      });
    }
    return sendJson(res, 200, response);
  } catch (error) {
    recordProfile({
      type: "expand-error",
      entity,
      generationEntity,
      provider: config.provider,
      model: config.model,
      thinking: config.provider === "openrouter" ? OPENROUTER_REASONING : DEEPSEEK_THINKING === "enabled" ? "enabled" : "disabled",
      identityRan: Boolean(identityResult),
      timings: [
        ...timings,
        { stage: "total", ms: elapsedMs(started) }
      ],
      llm: {
        identityQueueMs: identityResult && identityResult.queueMs,
        identityRequestMs: identityResult && identityResult.requestMs,
        concurrency: LLM_MAX_CONCURRENCY,
        active: activeLlmRequests,
        queued: queuedLlmRequests.length,
        timeoutMs: LLM_REQUEST_TIMEOUT_MS,
        maxTokens: configuredMaxTokens() || null
      },
      error: error.message || "Influence generation failed"
    });
    sendJson(res, 500, { error: error.message || "Influence generation failed" });
  }
}

async function handleApiInfluencePrompt(req, res, url) {
  if (!requireAdmin(req, res, url)) return;
  try {
    const raw = await readBody(req);
    const body = raw ? JSON.parse(raw) : {};
    const entity = normalizeName(body.entity);
    const context = sanitizeExpansionContext(body.context);
    if (!entity) return sendJson(res, 400, { error: "Missing entity" });
    const identityPlan = shouldResolveIdentity(entity, context);
    return sendJson(res, 200, {
      debugPrompt: buildPromptBundle({
        identity: identityPlan.shouldResolve ? buildIdentityMessages(entity, context) : null,
        expansion: buildInfluenceMessages(entity, context)
      }),
      identityCandidates: identityPlan.candidates,
      identityWillRun: identityPlan.shouldResolve
    });
  } catch (error) {
    sendJson(res, 500, { error: error.message || "Prompt preview failed" });
  }
}

async function readGraphData() {
  return graphFromDb();
}

async function writeGraphData(data) {
  return writeGraphToDb(data, { runIdentityHygiene: false });
}

async function normalizeGraphFile() {
  const graph = graphFromDb();
  writeGraphToDb(graph, { runIdentityHygiene: true });
  console.log(`Normalized SQLite graph data at ${DB_FILE}`);
}

async function handleApiGraph(req, res) {
  try {
    if (req.method === "GET") {
      return sendJson(res, 200, {
        data: await readGraphData()
      });
    }

    if (req.method === "PUT") {
      const raw = await readBody(req, 10_000_000);
      const body = raw ? JSON.parse(raw) : {};
      return sendJson(res, 200, {
        data: await writeGraphData(body)
      });
    }

    return sendJson(res, 405, { error: "Method not allowed" });
  } catch (error) {
    sendJson(res, 500, { error: error.message || "Graph persistence failed" });
  }
}

async function handleApiDedupeReview(req, res, url) {
  if (!requireAdmin(req, res, url)) return;
  try {
    const config = providerConfig(DEFAULT_LLM_PROVIDER);
    const candidates = dedupeCandidatesFromDb();
    const review = await callDedupeReview(candidates, config);
    const localGroups = localDedupeSuggestions(candidates);
    const autoIdentity = applyAutoIdentityGroups([...localGroups, ...review.groups.map(group => ({ ...group, action: group.action || "merge" }))]);
    const stored = storeDedupeSuggestions(autoIdentity.queued);
    const response = {
      candidates,
      review,
      localGroups,
      stored,
      autoIdentity,
      pending: pendingDedupeSuggestions(),
      provider: config.apiKey ? config.provider : "",
      model: config.apiKey ? config.model : "",
      generatedAt: nowIso()
    };
    if (hasAdminAccess(req, url)) response.debugPrompt = review.prompt;
    sendJson(res, 200, response);
  } catch (error) {
    sendJson(res, 500, { error: error.message || "Dedupe review failed" });
  }
}

async function handleApiDedupeSuggestions(req, res, url) {
  if (!requireAdmin(req, res, url)) return;
  try {
    const autoIdentity = autoResolvePendingIdentity();
    sendJson(res, 200, {
      suggestions: pendingDedupeSuggestions(),
      autoIdentity
    });
  } catch (error) {
    sendJson(res, 500, { error: error.message || "Could not load dedupe suggestions" });
  }
}

async function handleApiProfiles(req, res, url) {
  if (!requireAdmin(req, res, url)) return;
  try {
    sendJson(res, 200, {
      profiles: recentProfiles(80),
      llmQueue: {
        active: activeLlmRequests,
        queued: queuedLlmRequests.length,
        concurrency: LLM_MAX_CONCURRENCY,
        timeoutMs: LLM_REQUEST_TIMEOUT_MS
      },
      generatedAt: nowIso()
    });
  } catch (error) {
    sendJson(res, 500, { error: error.message || "Could not load profiles" });
  }
}

async function handleApiDiagnostics(req, res, url) {
  if (!requireAdmin(req, res, url)) return;
  try {
    sendJson(res, 200, diagnosticSummary());
  } catch (error) {
    sendJson(res, 500, { error: error.message || "Could not load diagnostics" });
  }
}

async function handleApiLlmBenchmark(req, res, url) {
  if (!requireAdmin(req, res, url)) return;
  try {
    const config = providerConfig(DEFAULT_LLM_PROVIDER);
    if (!config.apiKey) return sendJson(res, 503, { error: `No API key configured for ${config.provider}` });
    const prompt = {
      messages: [
        {
          role: "system",
          content: "Return strict JSON only matching {\"entity\":\"Name\",\"influencedBy\":[],\"influenced\":[]}."
        },
        {
          role: "user",
          content: JSON.stringify({ entity: "The Beatles", task: "Return a tiny valid influence response with at most one item per array." })
        }
      ]
    };
    const modes = [
      { name: "json_object", jsonModeOnly: true },
      { name: "json_schema", jsonModeOnly: false }
    ];
    const results = [];
    for (const mode of modes) {
      const started = process.hrtime.bigint();
      const result = await callChatCompletion(config, prompt, {
        temperature: 0.1,
        maxTokens: configuredMaxTokens(),
        responseSchemaName: "benchmark_influence",
        responseSchema: influenceResponseSchema,
        jsonModeOnly: mode.jsonModeOnly
      });
      results.push({
        mode: mode.name,
        ms: elapsedMs(started),
        requestMs: result.ms,
        queueMs: result.queueMs,
        usage: result.usage,
        finishReason: result.finishReason,
        responseId: result.id
      });
      recordProfile({
        type: `benchmark-${mode.name}`,
        entity: "Benchmark",
        provider: config.provider,
        model: config.model,
        thinking: config.provider === "openrouter" ? OPENROUTER_REASONING : DEEPSEEK_THINKING,
        timings: [
          { stage: "request", ms: result.ms },
          { stage: "total", ms: elapsedMs(started) }
        ],
        llm: {
          expansionQueueMs: result.queueMs,
          expansionRequestMs: result.ms,
          concurrency: LLM_MAX_CONCURRENCY,
          timeoutMs: LLM_REQUEST_TIMEOUT_MS,
          maxTokens: configuredMaxTokens() || null
        },
        usage: {
          expansion: result.usage
        },
        finishReason: {
          expansion: result.finishReason
        }
      });
    }
    sendJson(res, 200, {
      provider: config.provider,
      model: config.model,
      reasoning: config.provider === "openrouter" ? OPENROUTER_REASONING : DEEPSEEK_THINKING,
      results,
      generatedAt: nowIso()
    });
  } catch (error) {
    sendJson(res, 500, { error: error.message || "LLM benchmark failed" });
  }
}

async function handleApiDedupeDecision(req, res, url, id, decision) {
  if (!requireAdmin(req, res, url)) return;
  try {
    const suggestion = db.prepare("SELECT * FROM dedupe_suggestions WHERE id = ?").get(id);
    if (!suggestion) return sendJson(res, 404, { error: "Suggestion not found" });
    if (suggestion.status !== "pending") return sendJson(res, 409, { error: "Suggestion has already been reviewed" });

    const timestamp = nowIso();
    let decisionResult = { changed: false, reason: decision === "approve" ? "Marked reviewed" : "Rejected" };
    if (decision === "approve") {
      db.exec("BEGIN");
      let touchedNames = [];
      try {
        const parsed = rowToSuggestion(suggestion);
        touchedNames = [parsed.canonical, ...parsed.entities];
        if (parsed.action === "merge") {
          decisionResult = mergeNodes(parsed.canonical, parsed.entities);
          setSetting("savedAt", timestamp);
        } else if (parsed.action === "split") {
          decisionResult = approveSplitSuggestion(suggestion);
          setSetting("savedAt", timestamp);
        } else {
          decisionResult = { changed: false, reason: "Identity check marked reviewed" };
        }
        db.prepare(`
          UPDATE dedupe_suggestions
          SET status = 'approved', updated_at = ?, reviewed_at = ?
          WHERE id = ?
        `).run(timestamp, timestamp, id);
        db.exec("COMMIT");
        localIdentityHygiene(touchedNames);
      } catch (error) {
        db.exec("ROLLBACK");
        throw error;
      }
    } else {
      db.prepare(`
        UPDATE dedupe_suggestions
        SET status = 'rejected', updated_at = ?, reviewed_at = ?
        WHERE id = ?
      `).run(timestamp, timestamp, id);
    }

    sendJson(res, 200, {
      suggestions: pendingDedupeSuggestions(),
      data: graphFromDb(),
      result: decisionResult
    });
  } catch (error) {
    sendJson(res, 500, { error: error.message || "Dedupe decision failed" });
  }
}

function serveStatic(req, res) {
  const url = new URL(req.url, `http://${req.headers.host}`);
  const requestedPath = url.pathname === "/" ? "/index.html" : url.pathname;
  const safePath = path
    .normalize(decodeURIComponent(requestedPath))
    .replace(/^(\.\.[/\\])+/, "");
  const filePath = path.join(PUBLIC_DIR, safePath.replace(/^[/\\]/, ""));

  if (!filePath.startsWith(PUBLIC_DIR)) {
    res.writeHead(403);
    res.end("Forbidden");
    return;
  }

  fs.readFile(filePath, (error, data) => {
    if (error) {
      res.writeHead(404);
      res.end("Not found");
      return;
    }
    const ext = path.extname(filePath);
    res.writeHead(200, { "content-type": mimeTypes[ext] || "application/octet-stream" });
    res.end(data);
  });
}

initDb();
migrateJsonToDb();

const server = http.createServer((req, res) => {
  const url = new URL(req.url, `http://${req.headers.host}`);
  if (req.method === "POST" && url.pathname === "/api/influences") {
    handleApiInfluences(req, res);
    return;
  }
  if (req.method === "POST" && url.pathname === "/api/influence-prompt") {
    handleApiInfluencePrompt(req, res, url);
    return;
  }
  if ((req.method === "GET" || req.method === "PUT") && url.pathname === "/api/graph") {
    handleApiGraph(req, res);
    return;
  }
  if (req.method === "POST" && url.pathname === "/api/dedupe-review") {
    handleApiDedupeReview(req, res, url);
    return;
  }
  if (req.method === "GET" && url.pathname === "/api/dedupe-suggestions") {
    handleApiDedupeSuggestions(req, res, url);
    return;
  }
  if (req.method === "GET" && url.pathname === "/api/profiles") {
    handleApiProfiles(req, res, url);
    return;
  }
  if (req.method === "GET" && url.pathname === "/api/diagnostics") {
    handleApiDiagnostics(req, res, url);
    return;
  }
  if (req.method === "POST" && url.pathname === "/api/llm-benchmark") {
    handleApiLlmBenchmark(req, res, url);
    return;
  }
  const decisionMatch = url.pathname.match(/^\/api\/dedupe-suggestions\/(\d+)\/(approve|reject)$/);
  if (req.method === "POST" && decisionMatch) {
    handleApiDedupeDecision(req, res, url, Number(decisionMatch[1]), decisionMatch[2]);
    return;
  }
  if (req.method === "GET" && url.pathname === "/api/config") {
    const config = providerConfig(DEFAULT_LLM_PROVIDER);
    sendJson(res, 200, {
      devTools: hasAdminAccess(req, url),
      adminRequired: DEV_TOOLS && Boolean(ADMIN_TOKEN),
      provider: config.apiKey ? config.provider : "mock",
      model: config.apiKey ? config.model : "",
      thinking: config.provider === "openrouter" ? OPENROUTER_REASONING : DEEPSEEK_THINKING === "enabled" ? "enabled" : "disabled"
    });
    return;
  }
  if (req.method === "GET") {
    serveStatic(req, res);
    return;
  }
  res.writeHead(405);
  res.end("Method not allowed");
});

server.listen(PORT, () => {
  const config = providerConfig(DEFAULT_LLM_PROVIDER);
  console.log(`Influence graph running at http://localhost:${PORT}`);
  console.log(config.apiKey ? `Using ${config.provider}/${config.model}` : `No API key set for ${config.provider}; using mock mode`);
  normalizeGraphFile();
});
