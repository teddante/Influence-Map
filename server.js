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
const DEV_TOOLS = process.env.INFLUENCE_MAP_DEV_TOOLS === "1";
const ADMIN_TOKEN = process.env.INFLUENCE_MAP_ADMIN_TOKEN || "";

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

function approveSplitSuggestion(suggestion) {
  const parsed = rowToSuggestion(suggestion);
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

function writeGraphToDb(data) {
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
    localIdentityHygiene([...touchedNames]);
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
  writeGraphToDb(graph);
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
    "After resolving the intended entity, include broader significant influences and things influenced by it, including relationships not already present in the context.",
    "Avoid returning only the supplied context relationships unless no other confident relationships are known.",
    "Do not invent, pad, or force relationships just to make the graph grow; empty or small arrays are acceptable when confidence is low.",
    "Confidence is an estimated probability from 0 to 1.",
    "Use any kind of cultural entity: people, works, media, scenes, styles, technologies, or movements.",
    "Do not include explanations, sources, markdown, comments, or extra keys.",
    "Include as many significant relationships as you can fit confidently, but avoid weak filler."
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
  const hasCompetingCandidates = candidates.filter(candidate => keyFor(candidate.name) !== keyFor(entity)).length > 0;
  return {
    candidates,
    shouldResolve: Boolean(candidates.length && (hasCompetingCandidates || (bare && hasContext)))
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

async function callIdentityResolution(entity, context = {}) {
  const prompt = buildIdentityMessages(entity, context);
  const response = await fetch(`${DEEPSEEK_BASE_URL}/chat/completions`, {
    method: "POST",
    headers: {
      "content-type": "application/json",
      "authorization": `Bearer ${DEEPSEEK_API_KEY}`
    },
    body: JSON.stringify({
      model: DEEPSEEK_MODEL,
      messages: prompt.messages,
      response_format: { type: "json_object" },
      temperature: 0.1,
      max_tokens: 600
    })
  });

  if (!response.ok) {
    const text = await response.text();
    throw new Error(`DeepSeek identity ${response.status}: ${text.slice(0, 500)}`);
  }

  const data = await response.json();
  const content = data.choices && data.choices[0] && data.choices[0].message && data.choices[0].message.content;
  if (!content) throw new Error("DeepSeek returned empty identity content");
  return {
    data: sanitizeIdentityResponse(entity, JSON.parse(content)),
    prompt
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

async function callDeepSeek(entity, context = {}) {
  const prompt = buildInfluenceMessages(entity, context);
  const response = await fetch(`${DEEPSEEK_BASE_URL}/chat/completions`, {
    method: "POST",
    headers: {
      "content-type": "application/json",
      "authorization": `Bearer ${DEEPSEEK_API_KEY}`
    },
    body: JSON.stringify({
      model: DEEPSEEK_MODEL,
      messages: prompt.messages,
      response_format: { type: "json_object" },
      temperature: 0.7,
      max_tokens: 1800
    })
  });

  if (!response.ok) {
    const text = await response.text();
    throw new Error(`DeepSeek ${response.status}: ${text.slice(0, 500)}`);
  }

  const data = await response.json();
  const content = data.choices && data.choices[0] && data.choices[0].message && data.choices[0].message.content;
  if (!content) throw new Error("DeepSeek returned empty content");
  return {
    data: sanitizeInfluenceResponse(entity, JSON.parse(content)),
    prompt
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

async function callDedupeReview(candidates) {
  const prompt = buildDedupeMessages(candidates);
  if (!DEEPSEEK_API_KEY || !candidates.length) {
    return { groups: [], prompt };
  }

  const response = await fetch(`${DEEPSEEK_BASE_URL}/chat/completions`, {
    method: "POST",
    headers: {
      "content-type": "application/json",
      "authorization": `Bearer ${DEEPSEEK_API_KEY}`
    },
    body: JSON.stringify({
      model: DEEPSEEK_MODEL,
      messages: prompt.messages,
      response_format: { type: "json_object" },
      temperature: 0.1,
      max_tokens: 1800
    })
  });

  if (!response.ok) {
    const text = await response.text();
    throw new Error(`DeepSeek ${response.status}: ${text.slice(0, 500)}`);
  }

  const data = await response.json();
  const content = data.choices && data.choices[0] && data.choices[0].message && data.choices[0].message.content;
  if (!content) return { groups: [], prompt };
  const parsed = JSON.parse(content);
  return {
    groups: Array.isArray(parsed.groups) ? parsed.groups.map(group => ({
      canonical: normalizeName(group.canonical),
      entities: Array.isArray(group.entities) ? group.entities.map(normalizeName).filter(Boolean) : [],
      confidence: clampConfidence(group.confidence),
      reason: normalizeName(group.reason)
    })).filter(group => group.canonical && group.entities.length > 1) : [],
    prompt
  };
}

function scopedDedupeCandidates(names = []) {
  const touched = new Set((names || []).map(keyFor).filter(Boolean));
  if (!touched.size) return [];
  return dedupeCandidatesFromDb(5000)
    .filter(candidate => (candidate.entities || []).some(name => touched.has(keyFor(name))))
    .slice(0, 12);
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

async function callScopedIdentityReview(names, candidates) {
  const prompt = buildScopedIdentityMessages(names, candidates);
  if (!DEEPSEEK_API_KEY || !candidates.length) {
    return { groups: [], prompt };
  }

  const response = await fetch(`${DEEPSEEK_BASE_URL}/chat/completions`, {
    method: "POST",
    headers: {
      "content-type": "application/json",
      "authorization": `Bearer ${DEEPSEEK_API_KEY}`
    },
    body: JSON.stringify({
      model: DEEPSEEK_MODEL,
      messages: prompt.messages,
      response_format: { type: "json_object" },
      temperature: 0.1,
      max_tokens: 900
    })
  });

  if (!response.ok) {
    const text = await response.text();
    throw new Error(`DeepSeek identity hygiene ${response.status}: ${text.slice(0, 500)}`);
  }

  const data = await response.json();
  const content = data.choices && data.choices[0] && data.choices[0].message && data.choices[0].message.content;
  if (!content) return { groups: [], prompt };
  const parsed = JSON.parse(content);
  return {
    groups: sanitizeScopedIdentityGroups(parsed.groups),
    prompt
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

function storeExpansionIdentitySuggestion(requestedEntity, canonicalEntity, context = {}) {
  const requested = normalizeName(requestedEntity);
  const canonical = normalizeName(canonicalEntity);
  if (!requested || !canonical || keyFor(requested) === keyFor(canonical)) return 0;
  const moveCandidates = splitCandidatesFor(requested, canonical, context);
  return storeDedupeSuggestions([{
    canonical,
    entities: [requested, canonical],
    confidence: moveCandidates.length ? 0.72 : 0.66,
    action: moveCandidates.length ? "split" : "identity",
    metadata: {
      moveObservationIds: moveCandidates.map(item => item.id),
      moveCandidates
    },
    reason: moveCandidates.length
      ? `Split review: model resolved "${requested}" as "${canonical}" and found ${moveCandidates.length} context-linked observation${moveCandidates.length === 1 ? "" : "s"} that may belong there`
      : `Expansion identity review: model resolved "${requested}" as "${canonical}"`
  }]);
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

function localIdentityHygiene(names = []) {
  try {
    const touched = new Set((names || []).map(keyFor).filter(Boolean));
    const candidates = dedupeCandidatesFromDb(5000);
    const scoped = touched.size
      ? candidates.filter(candidate => (candidate.entities || []).some(name => touched.has(keyFor(name))))
      : candidates;
    return storeDedupeSuggestions(localDedupeSuggestions([
      ...scoped,
      ...targetedTitleVariantCandidates(names)
    ]));
  } catch (error) {
    console.warn(`Local identity hygiene skipped: ${error.message}`);
    return 0;
  }
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
  try {
    const raw = await readBody(req);
    const body = raw ? JSON.parse(raw) : {};
    const entity = normalizeName(body.entity);
    const context = sanitizeExpansionContext(body.context);
    const provider = body.provider || "deepseek";
    if (!entity) return sendJson(res, 400, { error: "Missing entity" });

    if (provider === "mock") {
      return sendJson(res, 200, {
        provider: "mock",
        model: "local-demo",
        data: mockInfluences(entity)
      });
    }

    if (!DEEPSEEK_API_KEY) {
      return sendJson(res, 503, {
        error: "Live generation is not configured. Start the server with DEEPSEEK_API_KEY or use mock mode in dev tools."
      });
    }

    const identityPlan = shouldResolveIdentity(entity, context);
    let identityResult = null;
    let generationEntity = entity;
    if (identityPlan.shouldResolve) {
      identityResult = await callIdentityResolution(entity, context);
      if (usableIdentityResolution(identityResult.data)) {
        generationEntity = identityResult.data.canonicalName;
      }
    }

    const result = await callDeepSeek(generationEntity, context);
    const touchedNames = namesFromInfluenceResponse(entity, result.data);
    const scopedCandidates = scopedDedupeCandidates(touchedNames);
    const scopedReview = await callScopedIdentityReview(touchedNames, scopedCandidates);
    const identitySuggestions = storeExpansionIdentitySuggestion(entity, result.data.entity, context);
    const llmHygieneSuggestions = storeScopedIdentityGroups(scopedReview.groups);
    const localHygieneSuggestions = localIdentityHygiene(touchedNames);
    const response = {
      provider: "deepseek",
      model: DEEPSEEK_MODEL,
      data: result.data,
      identity: identityResult ? identityResult.data : null,
      identityCandidates: identityPlan.candidates,
      scopedIdentityReview: {
        candidates: scopedCandidates.length,
        groups: scopedReview.groups
      },
      identitySuggestions,
      llmHygieneSuggestions,
      localHygieneSuggestions
    };
    if (hasAdminAccess(req, new URL(req.url, `http://${req.headers.host}`))) {
      response.debugPrompt = buildPromptBundle({
        identity: identityResult && identityResult.prompt,
        expansion: result.prompt,
        hygiene: scopedReview.prompt
      });
    }
    return sendJson(res, 200, response);
  } catch (error) {
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
  return writeGraphToDb(data);
}

async function normalizeGraphFile() {
  const graph = graphFromDb();
  writeGraphToDb(graph);
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
    const candidates = dedupeCandidatesFromDb();
    const review = await callDedupeReview(candidates);
    const localGroups = localDedupeSuggestions(candidates);
    const stored = storeDedupeSuggestions([...localGroups, ...review.groups]);
    const response = {
      candidates,
      review,
      localGroups,
      stored,
      pending: pendingDedupeSuggestions(),
      model: DEEPSEEK_API_KEY ? DEEPSEEK_MODEL : "",
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
    sendJson(res, 200, { suggestions: pendingDedupeSuggestions() });
  } catch (error) {
    sendJson(res, 500, { error: error.message || "Could not load dedupe suggestions" });
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
  const decisionMatch = url.pathname.match(/^\/api\/dedupe-suggestions\/(\d+)\/(approve|reject)$/);
  if (req.method === "POST" && decisionMatch) {
    handleApiDedupeDecision(req, res, url, Number(decisionMatch[1]), decisionMatch[2]);
    return;
  }
  if (req.method === "GET" && url.pathname === "/api/config") {
    sendJson(res, 200, {
      devTools: hasAdminAccess(req, url),
      adminRequired: DEV_TOOLS && Boolean(ADMIN_TOKEN),
      provider: DEEPSEEK_API_KEY ? "deepseek" : "mock"
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
  console.log(`Influence graph running at http://localhost:${PORT}`);
  console.log(DEEPSEEK_API_KEY ? `Using ${DEEPSEEK_MODEL}` : "No DEEPSEEK_API_KEY set; using mock mode");
  normalizeGraphFile();
});
