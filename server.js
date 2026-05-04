const http = require("http");
const fs = require("fs");
const path = require("path");

const PORT = Number(process.env.PORT || 3000);
const PUBLIC_DIR = path.join(__dirname, "public");
const DATA_FILE = process.env.INFLUENCE_MAP_DATA_FILE || path.join(__dirname, "data", "graph.json");
const DEEPSEEK_API_KEY = process.env.DEEPSEEK_API_KEY || "";
const DEEPSEEK_MODEL = process.env.DEEPSEEK_MODEL || "deepseek-v4-flash";
const DEEPSEEK_BASE_URL = process.env.DEEPSEEK_BASE_URL || "https://api.deepseek.com";
const DEV_TOOLS = process.env.INFLUENCE_MAP_DEV_TOOLS === "1";

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
  try {
    const raw = await fs.promises.readFile(DATA_FILE, "utf8");
    return sanitizeGraphData(JSON.parse(raw));
  } catch (error) {
    if (error.code === "ENOENT") return sanitizeGraphData({});
    throw error;
  }
}

async function writeGraphData(data) {
  const graph = sanitizeGraphData({
    ...data,
    savedAt: new Date().toISOString()
  });
  await fs.promises.mkdir(path.dirname(DATA_FILE), { recursive: true });
  await fs.promises.writeFile(DATA_FILE, `${JSON.stringify(graph, null, 2)}\n`, "utf8");
  return graph;
}

async function normalizeGraphFile() {
  try {
    const raw = await fs.promises.readFile(DATA_FILE, "utf8");
    const graph = sanitizeGraphData(JSON.parse(raw));
    await fs.promises.mkdir(path.dirname(DATA_FILE), { recursive: true });
    await fs.promises.writeFile(DATA_FILE, `${JSON.stringify(graph, null, 2)}\n`, "utf8");
    console.log(`Normalized graph data at ${DATA_FILE}`);
  } catch (error) {
    if (error.code !== "ENOENT") {
      console.warn(`Could not normalize graph data: ${error.message}`);
    }
  }
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
  if (req.method === "GET" && url.pathname === "/api/config") {
    sendJson(res, 200, {
      devTools: DEV_TOOLS,
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
