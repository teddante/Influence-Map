const http = require("node:http");
const fs = require("node:fs");
const path = require("node:path");
const { DatabaseSync } = require("node:sqlite");
const { randomUUID, timingSafeEqual } = require("node:crypto");
const { AsyncLocalStorage } = require("node:async_hooks");
const { initDb } = require("./lib/schema");
const { verifiedBackup } = require("./lib/database-backup");
const { GraphStore, ConflictError } = require("./lib/graph-store");
const { HttpError, readJson, GenerationBudget } = require("./lib/http-safety");
const requestContext = new AsyncLocalStorage();

const {
  PORT, HOST, PUBLIC_DIR, DATA_FILE, DB_FILE, DEV_TOOLS, ADMIN_TOKEN,
  DEEPSEEK_THINKING, OPENROUTER_REASONING, OPENROUTER_JSON_MODE,
  DEFAULT_LLM_PROVIDER, LLM_MAX_CONCURRENCY, LLM_REQUEST_TIMEOUT_MS, LLM_MAX_TOKENS,
  GENERATION_BUDGET, MUTATIONS_PER_MINUTE,
  IDENTITY_AUTO_APPROVE, IDENTITY_AUTO_MERGE_CONFIDENCE, IDENTITY_AUTO_SPLIT_CONFIDENCE
} = require("./lib/config");
const { normalizeName, keyFor } = require("./public/graph-model");
const {
  suggestionKey, targetedDedupeCandidates, dedupeCandidates,
  canonicalFromNames, compatibleTitleVariants, identityConflictReason, looksLikeSpellingVariant,
  acronymFor, isAcronymLike, compactKey, baseTitleKey, hasDisambiguator, tokenOverlapScore
} = require("./lib/identity");
const {
  providerConfig, createLlmClient, elapsedMs, influenceResponseSchema,
  identityResponseSchema, dedupeResponseSchema, scopedIdentityResponseSchema
} = require("./lib/llm-client");

const mutationRequests = new Map();
const pendingExpansions = new Set();

const mimeTypes = {
  ".html": "text/html; charset=utf-8",
  ".css": "text/css; charset=utf-8",
  ".js": "text/javascript; charset=utf-8",
  ".json": "application/json; charset=utf-8"
};

function sendJson(res, status, data) {
  if (res.destroyed || res.writableEnded) return;
  const body = JSON.stringify(data);
  res.writeHead(status, {
    "content-type": "application/json; charset=utf-8",
    "content-length": Buffer.byteLength(body)
  });
  res.end(body);
}

function errorStatus(error) { return error.status || (error instanceof SyntaxError ? 400 : 500); }

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
    if (!entity || entity.length > 500 || seen.has(key) || item.confidence === null || !Number.isFinite(Number(item.confidence))) continue;
    seen.add(key);
    clean.push({
      entity,
      confidence: Number(clampConfidence(item.confidence).toFixed(3))
    });
  }
  return clean.slice(0, 8);
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

function nowIso() {
  return new Date().toISOString();
}

function recordProfile(event) {
  const profile = {
    ...event,
    at: nowIso()
  };
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

function adminTokenFrom(req) { return String(req.headers["x-admin-token"] || ""); }

function hasWriteAccess(req) {
  const given = Buffer.from(adminTokenFrom(req)), expected = Buffer.from(ADMIN_TOKEN);
  return given.length === expected.length && timingSafeEqual(given, expected);
}

function hasAdminAccess(req) { return DEV_TOOLS && hasWriteAccess(req); }

function requireAdmin(req, res) {
  if (hasAdminAccess(req)) return true;
  sendJson(res, 403, { error: "Admin access required" });
  return false;
}

fs.mkdirSync(path.dirname(DB_FILE), { recursive: true });
const db = new DatabaseSync(DB_FILE);

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
    openRouterJsonMode: OPENROUTER_JSON_MODE,
    llmQueue: {
      active: llmQueue.active,
      queued: llmQueue.queued,
      concurrency: LLM_MAX_CONCURRENCY,
      timeoutMs: LLM_REQUEST_TIMEOUT_MS,
      maxTokens: LLM_MAX_TOKENS
    },
    counts: diagnosticCounts(),
    profiles: recent,
    slowThresholdMs: 10000,
    generatedAt: nowIso()
  };
}

function migrateJsonToDb() {
  if (store.setting("legacyMigrationComplete") === "1") return;
  const hasRows = db.prepare("SELECT COUNT(*) AS count FROM nodes").get().count > 0;
  if (!hasRows && fs.existsSync(DATA_FILE)) {
    const graph = JSON.parse(fs.readFileSync(DATA_FILE, "utf8").replace(/^\uFEFF/, ""));
    store.importGraph(graph);
  }
  store.setSetting("legacyMigrationComplete", "1");
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
        ["Rust (video game)", 0.57],
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
    "Confidence is an uncalibrated model judgment score from 0 to 1; source verification is recorded separately.",
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
    maxTokens: LLM_MAX_TOKENS,
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

async function callInfluenceGeneration(entity, context = {}, config = providerConfig()) {
  const prompt = buildInfluenceMessages(entity, context);
  const result = await callChatCompletion(config, prompt, {
    temperature: 0.55,
    maxTokens: LLM_MAX_TOKENS,
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
    maxTokens: LLM_MAX_TOKENS,
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
  return targetedDedupeCandidates(nodesForIdentityScan(), names).slice(0, 12);
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
    maxTokens: LLM_MAX_TOKENS,
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
    SELECT id, action, confidence, metadata FROM dedupe_suggestions
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
    if (existing && reason.startsWith("Local heuristic:") && parseJson(existing.metadata, {}).automatedReview) continue;
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

function canAutoApplyIdentity(group) {
  if (!IDENTITY_AUTO_APPROVE || !group) return { ok: false, reason: "automatic identity approval disabled" };
  if (String(group.reason || "").startsWith("Local heuristic:")) return { ok: false, reason: "heuristic candidates require manual identity review" };
  const action = group.action === "split" ? "split" : group.action === "merge" || group.action === "rename" ? "merge" : "";
  if (!action) return { ok: false, reason: "not an actionable identity edit" };
  const entities = [...new Set((group.entities || []).map(normalizeName).filter(Boolean))];
  if (entities.length < 2) return { ok: false, reason: "needs at least two entities" };
  if (entities.length > 4) return { ok: false, reason: "too many entities for automatic edit" };
  const confidence = clampConfidence(group.confidence);
  const threshold = action === "split" ? IDENTITY_AUTO_SPLIT_CONFIDENCE : IDENTITY_AUTO_MERGE_CONFIDENCE;
  const conflict = identityConflictReason([group.canonical, ...entities]);
  if (conflict) return { ok: false, reason: conflict };
  const spellingVariant = entities.some(entity => looksLikeSpellingVariant(entity, group.canonical));
  if (confidence < threshold && !(action === "split" && spellingVariant && confidence >= 0.7)) {
    return { ok: false, reason: `confidence below ${Math.round(threshold * 100)}%` };
  }
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
    try {
      const { result, eventId } = store.edit(group);
      applied.push({
        eventId,
        action: group.action,
        canonical: group.canonical,
        entities: group.entities,
        confidence: clampConfidence(group.confidence),
        reason: group.reason || gate.reason,
        result
      });
    } catch (error) {
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
      action: "merge",
      canonical: canonicalFromNames(candidate.entities || []),
      entities: candidate.entities || [],
      confidence: candidate.reason.includes("base title") ? 0.68 : 0.72,
      reason: `Local heuristic: ${candidate.reason}`
    }))
    .filter(group => group.canonical && group.entities.length > 1);
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

function localIdentityHygiene(names = []) {
  try {
    const autoIdentity = applyAutoIdentityGroups(localDedupeSuggestions(targetedDedupeCandidates(nodesForIdentityScan(), names)));
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

function actionableScopedIdentityGroups(groups) {
  const actionable = [];
  for (const group of groups || []) {
    if (group.action === "keep-separate") continue;
    // Preserve scoped split decisions for exact-link review instead of dropping them.
    actionable.push({
      canonical: group.canonical,
      entities: group.entities,
      confidence: group.confidence,
      action: group.action === "split" ? "split" : "merge",
      metadata: {
        needsLinkReview: group.action === "split",
        llmAction: group.action,
        automatedReview: true
      },
      reason: `LLM scoped identity: ${group.reason || group.action}`
    });
  }
  return actionable;
}

async function handleApiInfluences(req, res) {
  const started = process.hrtime.bigint();
  const timings = [];
  let entity = "";
  let generationEntity = "";
  let config = providerConfig(DEFAULT_LLM_PROVIDER);
  let identityResult = null;
  let requestId;
  let ownsRequest = false;
  try {
    const body = await readJson(req);
    entity = normalizeName(body.entity);
    if (entity.length > 500) throw new HttpError(400, "Entity name is too long");
    requestId = String(body.requestId || randomUUID());
    if (requestId.length > 100) throw new HttpError(400, "Invalid request ID");
    const cached = store.expansion(requestId);
    if (cached) {
      if (cached.requestedEntity && cached.requestedEntity !== entity) throw new ConflictError("Request ID belongs to another entity");
      return sendJson(res, 200, { ...cached, ...store.changesSince(body.revision) });
    }
    if (pendingExpansions.has(requestId)) throw new ConflictError("This expansion is already running");
    if (body.revision !== undefined) store.assertRevision(body.revision);
    pendingExpansions.add(requestId);
    ownsRequest = true;
    const context = sanitizeExpansionContext(body.context);
    config = providerConfig(body.provider || DEFAULT_LLM_PROVIDER);
    if (!entity) return sendJson(res, 400, { error: "Missing entity" });

    if (config.provider === "mock") {
      const response = store.appendExpansion(requestId, { provider: "mock", model: "local-demo", requestedEntity: entity, data: mockInfluences(entity) });
      return sendJson(res, 200, { ...response, ...store.changesSince(body.revision) });
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
    const result = await callInfluenceGeneration(generationEntity, context, config);
    timings.push({ stage: "expansion", ms: elapsedMs(expansionStage) });
    store.appendExpansion(requestId, { provider: config.provider, model: config.model, requestedEntity: entity, data: result.data });
    const touchedNames = namesFromInfluenceResponse(entity, result.data);
    const identityEditStage = process.hrtime.bigint();
    const scopedCandidates = scopedDedupeCandidates(touchedNames);
    const hygieneQueued = scopedCandidates.length > 0;
    const scopedReview = hygieneQueued
      ? await callScopedIdentityReview(touchedNames, scopedCandidates, config).catch(error => {
        recordProfile({ type: "identity-hygiene-error", entity, provider: config.provider, model: config.model, error: error.message });
        return { groups: [], warning: error.message };
      })
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
    const modelDetails = {
      llm: {
        identityQueueMs: identityResult && identityResult.queueMs,
        identityRequestMs: identityResult && identityResult.requestMs,
        hygieneQueueMs: scopedReview.queueMs,
        hygieneRequestMs: scopedReview.requestMs,
        expansionQueueMs: result.queueMs,
        expansionRequestMs: result.requestMs,
        concurrency: LLM_MAX_CONCURRENCY,
        timeoutMs: LLM_REQUEST_TIMEOUT_MS,
        maxTokens: LLM_MAX_TOKENS
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
    };
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
      ...modelDetails
    });
    const response = {
      provider: config.provider,
      model: config.model,
      data: result.data,
      identity: identityResult ? identityResult.data : null,
      identityCandidates: identityPlan.candidates,
      scopedIdentityReview: {
        warning: scopedReview.warning || null,
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
      ...modelDetails,
      ...store.changesSince(body.revision),
      timings: responseTimings
    };
    if (hasAdminAccess(req)) {
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
        active: llmQueue.active,
        queued: llmQueue.queued,
        timeoutMs: LLM_REQUEST_TIMEOUT_MS,
        maxTokens: LLM_MAX_TOKENS
      },
      error: error.message || "Influence generation failed"
    });
    sendJson(res, errorStatus(error), { error: error.message || "Influence generation failed" });
  } finally {
    if (ownsRequest) pendingExpansions.delete(requestId);
  }
}

async function handleApiInfluencePrompt(req, res) {
  if (!requireAdmin(req, res)) return;
  try {
    const body = await readJson(req);
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
    sendJson(res, errorStatus(error), { error: error.message || "Prompt preview failed" });
  }
}

async function handleApiGraph(req, res, url) {
  if (req.method !== "GET") return sendJson(res, 405, { error: "Whole-graph writes were removed. Use the authenticated mutation endpoints." });
  return sendJson(res, 200, url.searchParams.has("since") ? store.changesSince(Number(url.searchParams.get("since"))) : { data: store.graph() });
}

async function handleApiDedupeReview(req, res) {
  if (!requireAdmin(req, res)) return;
  try {
    const config = providerConfig(DEFAULT_LLM_PROVIDER);
    const candidates = dedupeCandidates(nodesForIdentityScan());
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
    if (hasAdminAccess(req)) response.debugPrompt = review.prompt;
    sendJson(res, 200, response);
  } catch (error) {
    sendJson(res, errorStatus(error), { error: error.message || "Dedupe review failed" });
  }
}

async function handleApiDedupeSuggestions(req, res) {
  if (!requireAdmin(req, res)) return;
  try {
    sendJson(res, 200, { suggestions: pendingDedupeSuggestions() });
  } catch (error) {
    sendJson(res, errorStatus(error), { error: error.message || "Could not load dedupe suggestions" });
  }
}

async function handleApiDedupeAutoResolve(req, res) {
  if (!requireAdmin(req, res)) return;
  try {
    const autoIdentity = autoResolvePendingIdentity();
    sendJson(res, 200, {
      suggestions: pendingDedupeSuggestions(),
      autoIdentity,
      data: autoIdentity.applied ? store.graph() : null
    });
  } catch (error) {
    sendJson(res, errorStatus(error), { error: error.message || "Could not auto-resolve dedupe suggestions" });
  }
}

function orphanNodes() {
  return db.prepare(`
    SELECT n.id, n.name, n.expanded, n.aliases, n.created_at, n.updated_at
    FROM nodes n
    LEFT JOIN observations o1 ON o1.from_id = n.id
    LEFT JOIN observations o2 ON o2.to_id = n.id
    WHERE o1.id IS NULL AND o2.id IS NULL
    ORDER BY n.updated_at DESC, n.name COLLATE NOCASE
  `).all().map(row => ({
    id: row.id,
    name: row.name,
    expanded: Boolean(row.expanded),
    aliases: parseJson(row.aliases, []),
    createdAt: row.created_at,
    updatedAt: row.updated_at
  }));
}

async function handleApiOrphans(req, res) {
  if (!requireAdmin(req, res)) return;
  try {
    if (req.method === "GET") {
      return sendJson(res, 200, { orphans: orphanNodes() });
    }
    if (req.method === "DELETE") {
      const body = await readJson(req);
      const orphans = orphanNodes();
      const mutation = store.mutate("orphan-cleanup", { names: orphans.map(node => node.name) }, () => {
        const remove = db.prepare("DELETE FROM nodes WHERE id=?");
        for (const node of orphans) remove.run(node.id);
      }, { expectedRevision: body.revision });
      return sendJson(res, 200, { removed: orphans.length, eventId: mutation.eventId, ...store.changesSince(body.revision) });
    }
    return sendJson(res, 405, { error: "Method not allowed" });
  } catch (error) {
    return sendJson(res, errorStatus(error), { error: error.message || "Orphan cleanup failed" });
  }
}

async function handleApiProfiles(req, res) {
  if (!requireAdmin(req, res)) return;
  try {
    sendJson(res, 200, {
      profiles: recentProfiles(80),
      llmQueue: {
        active: llmQueue.active,
        queued: llmQueue.queued,
        concurrency: LLM_MAX_CONCURRENCY,
        timeoutMs: LLM_REQUEST_TIMEOUT_MS
      },
      generatedAt: nowIso()
    });
  } catch (error) {
    sendJson(res, errorStatus(error), { error: error.message || "Could not load profiles" });
  }
}

async function handleApiDiagnostics(req, res) {
  if (!requireAdmin(req, res)) return;
  try {
    sendJson(res, 200, diagnosticSummary());
  } catch (error) {
    sendJson(res, errorStatus(error), { error: error.message || "Could not load diagnostics" });
  }
}

async function handleApiLlmBenchmark(req, res) {
  if (!requireAdmin(req, res)) return;
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
        maxTokens: LLM_MAX_TOKENS,
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
          maxTokens: LLM_MAX_TOKENS
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
    sendJson(res, errorStatus(error), { error: error.message || "LLM benchmark failed" });
  }
}

async function handleApiDedupeDecision(req, res, id, decision) {
  if (!requireAdmin(req, res)) return;
  try {
    const body = await readJson(req);
    store.assertRevision(body.revision);
    const suggestion = db.prepare("SELECT * FROM dedupe_suggestions WHERE id=?").get(id);
    if (!suggestion) throw new HttpError(404, "Suggestion not found");
    if (suggestion.status !== "pending") throw new ConflictError("Suggestion has already been reviewed");
    let result = { changed: false, reason: "Rejected" };
    let eventId = null;
    if (decision === "approve") {
      const group = rowToSuggestion(suggestion);
      if (Array.isArray(body.moveObservationIds)) group.metadata.moveObservationIds = body.moveObservationIds;
      if (["merge", "split", "rename"].includes(group.action)) ({ result, eventId } = store.edit(group, body.revision));
      else result = { changed: false, reason: "Identity check marked reviewed" };
    }
    db.prepare("UPDATE dedupe_suggestions SET status=?,reviewed_at=?,updated_at=? WHERE id=?").run(decision === "approve" ? "approved" : "rejected", nowIso(), nowIso(), id);
    sendJson(res, 200, { suggestions: pendingDedupeSuggestions(), ...store.changesSince(body.revision), result, eventId });
  } catch (error) { sendJson(res, errorStatus(error), { error: error.message }); }
}

function serveStatic(req, res, url) {
  const requestedPath = url.pathname === "/" ? "/index.html" : url.pathname;
  let decoded;
  try { decoded = decodeURIComponent(requestedPath); } catch { throw new HttpError(400, "Malformed URL encoding"); }
  if (decoded.includes("\0")) throw new HttpError(400, "Malformed file path");
  const filePath = path.resolve(PUBLIC_DIR, `.${decoded.replace(/\\/g, "/")}`);

  if (!filePath.startsWith(PUBLIC_DIR + path.sep)) {
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

verifiedBackup(db, DB_FILE);
initDb(db);
const store = new GraphStore(db);
migrateJsonToDb();
const generationBudget = new GenerationBudget(store, GENERATION_BUDGET);

const { callChatCompletion, llmQueue } = createLlmClient(generationBudget, requestContext);

function securityHeaders(res) {
  res.setHeader("content-security-policy", "default-src 'self'; script-src 'self'; style-src 'self' 'unsafe-inline'; img-src 'self' data:; object-src 'none'; base-uri 'none'; frame-ancestors 'none'; form-action 'self'");
  res.setHeader("x-content-type-options", "nosniff");
  res.setHeader("referrer-policy", "no-referrer");
  res.setHeader("cache-control", "no-store");
}

const routes = new Map([
  ["POST /api/influences", handleApiInfluences],
  ["POST /api/influence-prompt", handleApiInfluencePrompt],
  ["GET /api/graph", handleApiGraph],
  ["PUT /api/graph", handleApiGraph],
  ["POST /api/dedupe-review", handleApiDedupeReview],
  ["GET /api/dedupe-suggestions", handleApiDedupeSuggestions],
  ["POST /api/dedupe-auto-resolve", handleApiDedupeAutoResolve],
  ["GET /api/orphans", handleApiOrphans],
  ["DELETE /api/orphans", handleApiOrphans],
  ["GET /api/profiles", handleApiProfiles],
  ["GET /api/diagnostics", handleApiDiagnostics],
  ["POST /api/llm-benchmark", handleApiLlmBenchmark],
]);

async function dispatch(req, res) {
  const url = new URL(req.url, "http://localhost");
  if (url.pathname.startsWith("/api/") && req.method !== "GET") {
    if (pendingExpansions.size && /(?:reset|history|dedupe|orphans)/.test(url.pathname)) throw new ConflictError("Wait for active expansions before editing graph identities");
    if (!hasWriteAccess(req)) throw new HttpError(403, "Write access required. Open the private access link from the server terminal.");
    if (req.headers.origin && req.headers.origin !== "http://" + req.headers.host) throw new HttpError(403, "Cross-origin mutation rejected");
    const minute = Math.floor(Date.now() / 60000);
    const key = req.socket.remoteAddress;
    const rate = mutationRequests.get(key);
    const count = rate?.minute === minute ? rate.count + 1 : 1;
    mutationRequests.set(key, { minute, count });
    for (const [address, entry] of mutationRequests) if (entry.minute < minute) mutationRequests.delete(address);
    if (count > MUTATIONS_PER_MINUTE) throw new HttpError(429, "Mutation rate limit reached; retry next minute");
  }
  if (req.method === "POST" && url.pathname === "/api/reset") {
    const body = await readJson(req);
    const result = store.reset(body.revision);
    return sendJson(res, 200, { ...result, ...store.changesSince(body.revision) });
  }
  if (req.method === "GET" && url.pathname === "/api/history") {
    if (!requireAdmin(req, res)) return;
    return sendJson(res, 200, { history: store.history(), revision: store.revision });
  }
  const undo = url.pathname.match(/^\/api\/history\/(\d+)\/undo$/);
  if (req.method === "POST" && undo) {
    if (!requireAdmin(req, res)) return;
    const body = await readJson(req);
    const result = store.undo(Number(undo[1]), body.revision);
    return sendJson(res, 200, { ...result, ...store.changesSince(body.revision) });
  }
  const handler = routes.get(req.method + " " + url.pathname);
  if (handler) return handler(req, res, url);
  const decisionMatch = url.pathname.match(/^\/api\/dedupe-suggestions\/(\d+)\/(approve|reject)$/);
  if (req.method === "POST" && decisionMatch) {
    return handleApiDedupeDecision(req, res, Number(decisionMatch[1]), decisionMatch[2]);
  }
  if (req.method === "GET" && url.pathname === "/api/config") {
    const config = providerConfig(DEFAULT_LLM_PROVIDER);
    sendJson(res, 200, {
      devTools: hasAdminAccess(req),
      canWrite: hasWriteAccess(req),
      revision: store.revision,
      adminRequired: DEV_TOOLS && Boolean(ADMIN_TOKEN),
      provider: config.apiKey ? config.provider : "mock",
      model: config.apiKey ? config.model : "",
      thinking: config.provider === "openrouter" ? OPENROUTER_REASONING : DEEPSEEK_THINKING === "enabled" ? "enabled" : "disabled"
    });
    return;
  }
  if (req.method === "GET") {
    serveStatic(req, res, url);
    return;
  }
  res.writeHead(405);
  res.end("Method not allowed");
}

const server = http.createServer((req, res) => {
  securityHeaders(res);
  const cancellation = new AbortController();
  res.on("close", () => { if (!res.writableEnded) cancellation.abort(); });
  requestContext.run({ signal: cancellation.signal }, () => {
    dispatch(req, res).catch(error => sendJson(res, errorStatus(error), { error: error.message || "Request failed" }));
  });
});

server.listen(PORT, HOST, () => {
  const config = providerConfig(DEFAULT_LLM_PROVIDER);
  console.log(`Influence graph running at http://${HOST}:${server.address().port}`);
  if (!process.env.INFLUENCE_MAP_ADMIN_TOKEN) console.log(`Private write access: http://${HOST}:${server.address().port}/?admin=${ADMIN_TOKEN}`);
  console.log(config.apiKey ? `Using ${config.provider}/${config.model}` : `No API key set for ${config.provider}; using mock mode`);
});
