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
  return {
    entity: normalizeName(data && data.entity) || entity,
    influencedBy: sanitizeItems(data && data.influencedBy),
    influenced: sanitizeItems(data && data.influenced)
  };
}

function finiteNumber(value, fallback) {
  const number = Number(value);
  return Number.isFinite(number) ? number : fallback;
}

function sanitizeGraphData(data) {
  const minConfidence = data && Object.prototype.hasOwnProperty.call(data, "minConfidence")
    ? clampConfidence(data.minConfidence)
    : 0.35;
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
    CREATE TABLE IF NOT EXISTS settings (
      key TEXT PRIMARY KEY,
      value TEXT NOT NULL
    );
  `);
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
    minConfidence: Number(getSetting("minConfidence", "0.35")) || 0.35
  };
}

function writeGraphToDb(data) {
  const graph = sanitizeGraphData(data);
  const timestamp = nowIso();
  try {
    db.exec("BEGIN");
    db.exec("DELETE FROM observations; DELETE FROM nodes;");
    for (const node of graph.nodes) {
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

async function callDeepSeek(entity) {
  const system = [
    "You generate influence graph data as strict json.",
    "Return only a JSON object matching this shape:",
    "{\"entity\":\"Name\",\"influencedBy\":[{\"entity\":\"Name\",\"confidence\":0.0}],\"influenced\":[{\"entity\":\"Name\",\"confidence\":0.0}]}",
    "Confidence is an estimated probability from 0 to 1.",
    "Use any kind of cultural entity: people, works, media, scenes, styles, technologies, or movements.",
    "Do not include explanations, sources, markdown, comments, or extra keys.",
    "Include as many significant relationships as you can fit confidently, but avoid weak filler."
  ].join(" ");

  const response = await fetch(`${DEEPSEEK_BASE_URL}/chat/completions`, {
    method: "POST",
    headers: {
      "content-type": "application/json",
      "authorization": `Bearer ${DEEPSEEK_API_KEY}`
    },
    body: JSON.stringify({
      model: DEEPSEEK_MODEL,
      messages: [
        { role: "system", content: system },
        { role: "user", content: `Generate json influence graph arrays for: ${entity}` }
      ],
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
  return sanitizeInfluenceResponse(entity, JSON.parse(content));
}

async function callDedupeReview(candidates) {
  if (!DEEPSEEK_API_KEY || !candidates.length) {
    return { groups: [] };
  }
  const system = [
    "You review possible duplicate entity names in an influence graph.",
    "Return strict JSON only matching this shape:",
    "{\"groups\":[{\"canonical\":\"Name\",\"entities\":[\"Name\"],\"confidence\":0.0,\"reason\":\"short reason\"}]}",
    "Only group names that refer to the same real entity, work, person, series, or concept.",
    "Do not merge broad related concepts with specific works.",
    "Keep reasons short and do not include markdown."
  ].join(" ");

  const response = await fetch(`${DEEPSEEK_BASE_URL}/chat/completions`, {
    method: "POST",
    headers: {
      "content-type": "application/json",
      "authorization": `Bearer ${DEEPSEEK_API_KEY}`
    },
    body: JSON.stringify({
      model: DEEPSEEK_MODEL,
      messages: [
        { role: "system", content: system },
        { role: "user", content: JSON.stringify({ candidates }).slice(0, 12000) }
      ],
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
  if (!content) return { groups: [] };
  const parsed = JSON.parse(content);
  return {
    groups: Array.isArray(parsed.groups) ? parsed.groups.map(group => ({
      canonical: normalizeName(group.canonical),
      entities: Array.isArray(group.entities) ? group.entities.map(normalizeName).filter(Boolean) : [],
      confidence: clampConfidence(group.confidence),
      reason: normalizeName(group.reason)
    })).filter(group => group.canonical && group.entities.length > 1) : []
  };
}

function acronymFor(name) {
  return normalizeName(name)
    .split(/\s+/)
    .filter(part => !["the", "a", "an", "of", "and"].includes(part.toLowerCase()))
    .map(part => part[0])
    .join("")
    .toLowerCase();
}

function dedupeCandidatesFromDb() {
  const nodes = db.prepare("SELECT id, name, aliases FROM nodes ORDER BY name COLLATE NOCASE").all()
    .map(row => ({ id: row.id, name: row.name, aliases: parseJson(row.aliases, []) }));
  const groups = new Map();

  function add(reason, items) {
    const names = [...new Set(items.map(item => item.name).filter(Boolean))];
    if (names.length < 2) return;
    const key = names.map(keyFor).sort().join("|");
    if (!groups.has(key)) groups.set(key, { reason, entities: names });
  }

  const byNoPlural = new Map();
  for (const node of nodes) {
    const singular = node.id.replace(/s$/, "");
    const bucket = byNoPlural.get(singular) || [];
    bucket.push(node);
    byNoPlural.set(singular, bucket);
  }
  for (const bucket of byNoPlural.values()) add("same normalized singular form", bucket);

  for (let i = 0; i < nodes.length; i += 1) {
    for (let j = i + 1; j < nodes.length; j += 1) {
      const a = nodes[i];
      const b = nodes[j];
      if (a.id.length < 4 || b.id.length < 4) continue;
      if (a.id.includes(b.id) || b.id.includes(a.id)) add("one normalized name contains the other", [a, b]);
      if (acronymFor(a.name) && acronymFor(a.name) === b.id) add("acronym match", [a, b]);
      if (acronymFor(b.name) && acronymFor(b.name) === a.id) add("acronym match", [a, b]);
    }
  }

  return [...groups.values()].slice(0, 40);
}

async function handleApiInfluences(req, res) {
  try {
    const raw = await readBody(req);
    const body = raw ? JSON.parse(raw) : {};
    const entity = normalizeName(body.entity);
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

    const data = await callDeepSeek(entity);
    return sendJson(res, 200, {
      provider: "deepseek",
      model: DEEPSEEK_MODEL,
      data
    });
  } catch (error) {
    sendJson(res, 500, { error: error.message || "Influence generation failed" });
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
    sendJson(res, 200, {
      candidates,
      review,
      model: DEEPSEEK_API_KEY ? DEEPSEEK_MODEL : "",
      generatedAt: nowIso()
    });
  } catch (error) {
    sendJson(res, 500, { error: error.message || "Dedupe review failed" });
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
  if ((req.method === "GET" || req.method === "PUT") && url.pathname === "/api/graph") {
    handleApiGraph(req, res);
    return;
  }
  if (req.method === "POST" && url.pathname === "/api/dedupe-review") {
    handleApiDedupeReview(req, res, url);
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
