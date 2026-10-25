const { randomUUID } = require("node:crypto");
const { normalizeName, keyFor: defaultKeyFor } = require("../public/graph-model");

class ConflictError extends Error {
  constructor(message) { super(message); this.status = 409; }
}

// `id` is the legacy name lookup key. `uid` is the permanent entity identity;
// it survives renames and is used by external references and browser layouts.
class GraphStore {
  constructor(db, keyFor = defaultKeyFor) {
    this.db = db;
    this.statements = new Map();
    this.keyFor = keyFor;
    db.exec("BEGIN IMMEDIATE");
    try {
      const columns = table => new Set(db.prepare(`PRAGMA table_info(${table})`).all().map(row => row.name));
      if (!columns("nodes").has("uid")) db.exec("ALTER TABLE nodes ADD COLUMN uid TEXT");
      for (const row of db.prepare("SELECT id FROM nodes WHERE uid IS NULL").all()) {
        db.prepare("UPDATE nodes SET uid = ? WHERE id = ?").run(randomUUID(), row.id);
      }
      db.exec("CREATE UNIQUE INDEX IF NOT EXISTS idx_nodes_uid ON nodes(uid)");
      if (!columns("observations").has("evidence_type")) {
        db.exec("ALTER TABLE observations ADD COLUMN evidence_type TEXT NOT NULL DEFAULT 'unknown'");
        db.exec("UPDATE observations SET evidence_type = CASE WHEN provider = 'mock' THEN 'demo' WHEN provider IS NOT NULL AND provider != '' THEN 'model-sample' ELSE 'unknown' END");
      }
      if (!columns("observations").has("sources")) db.exec("ALTER TABLE observations ADD COLUMN sources TEXT NOT NULL DEFAULT '[]'");
      db.exec(`
        CREATE TABLE IF NOT EXISTS graph_changes (revision INTEGER PRIMARY KEY, changes_json TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS graph_history (
          id INTEGER PRIMARY KEY AUTOINCREMENT, action TEXT NOT NULL, details TEXT NOT NULL,
          before_json TEXT NOT NULL, after_revision INTEGER NOT NULL, created_at TEXT NOT NULL,
          undone_by INTEGER
        );
        CREATE TABLE IF NOT EXISTS expansions (request_id TEXT PRIMARY KEY, response_json TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS entity_redirects (from_uid TEXT PRIMARY KEY, to_uid TEXT NOT NULL);
      `);
      if (!this.setting("revision")) this.setSetting("revision", 1);
      db.exec("COMMIT");
    } catch (error) { db.exec("ROLLBACK"); throw error; }
  }

  prepare(sql) {
    if (!this.statements.has(sql)) this.statements.set(sql, this.db.prepare(sql));
    return this.statements.get(sql);
  }

  setting(key, fallback = "") { return this.prepare("SELECT value FROM settings WHERE key = ?").get(key)?.value ?? fallback; }
  setSetting(key, value) {
    this.prepare("INSERT INTO settings (key,value) VALUES (?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value").run(key, String(value));
  }
  get revision() { return Number(this.setting("revision", "1")); }
  assertRevision(expected) {
    if (!Number.isSafeInteger(expected) || expected !== this.revision) throw new ConflictError("The graph changed. Refresh before applying this edit.");
  }

  node(nameOrUid) {
    let reference = nameOrUid;
    const visited = new Set();
    while (!visited.has(reference)) {
      visited.add(reference);
      const node = this.prepare("SELECT * FROM nodes WHERE uid = ? OR id = ?").get(reference, this.keyFor(reference));
      if (node) return node;
      const redirect = this.prepare("SELECT to_uid FROM entity_redirects WHERE from_uid=?").get(reference);
      if (!redirect) return undefined;
      reference = redirect.to_uid;
    }
    throw new Error("Entity redirect cycle");
  }

  upsertNode(name, options = {}) {
    name = normalizeName(name);
    const id = this.keyFor(name);
    if (!id || name.length > 500) return null;
    const existing = this.prepare("SELECT * FROM nodes WHERE id = ?").get(id);
    const now = new Date().toISOString();
    if (!existing) {
      this.prepare("INSERT INTO nodes (id,uid,name,aliases,x,y,expanded,created_at,updated_at) VALUES (?,?,?,?,?,?,?,?,?)").run(
        id, randomUUID(), name, JSON.stringify(options.aliases || []), Number(options.x) || 0,
        Number(options.y) || 0, options.expanded ? 1 : 0, now, now
      );
    } else {
      const aliases = [...new Set([...JSON.parse(existing.aliases), ...(options.aliases || []), name])].filter(alias => alias !== existing.name);
      this.prepare("UPDATE nodes SET aliases=?,x=?,y=?,expanded=?,updated_at=? WHERE id=?").run(
        JSON.stringify(aliases), Number.isFinite(options.x) ? options.x : existing.x,
        Number.isFinite(options.y) ? options.y : existing.y, options.expanded ? 1 : existing.expanded, now, id
      );
    }
    return id;
  }

  graph() {
    const rows = this.prepare("SELECT * FROM nodes ORDER BY name COLLATE NOCASE").all();
    const byId = new Map(rows.map(row => [row.id, row]));
    return {
      version: 3, revision: this.revision, savedAt: this.setting("savedAt", new Date().toISOString()),
      nodes: rows.map(row => ({ id: row.id, uid: row.uid, name: row.name, aliases: JSON.parse(row.aliases), x: row.x, y: row.y })),
      observations: this.prepare("SELECT * FROM observations ORDER BY id").all().map(row => ({
        id: row.id, from: byId.get(row.from_id)?.name || row.from_id, to: byId.get(row.to_id)?.name || row.to_id,
        fromUid: byId.get(row.from_id)?.uid, toUid: byId.get(row.to_id)?.uid,
        confidence: row.confidence, sourceEntity: row.source_entity || "", provider: row.provider || "",
        model: row.model || "", createdAt: row.created_at, evidenceType: row.evidence_type, sources: JSON.parse(row.sources)
      })),
      expanded: rows.filter(row => row.expanded).map(row => row.id),
      pan: JSON.parse(this.setting("pan", '{"x":0,"y":0}')),
      zoom: Math.max(0.04, Math.min(8, Number(this.setting("zoom", "1")) || 1)),
      minConfidence: Math.max(0, Math.min(1, Number(this.setting("minConfidence", "0")) || 0))
    };
  }

  rawSnapshot() {
    return Object.fromEntries(["nodes", "observations", "expansions", "entity_redirects"].map(table => [table, this.prepare(`SELECT * FROM ${table}`).all()]));
  }

  static diff(before, after) {
    const difference = (oldRows, newRows, key) => {
      const old = new Map(oldRows.map(row => [row[key], row]));
      const next = new Map(newRows.map(row => [row[key], row]));
      return { changed: newRows.filter(row => JSON.stringify(row) !== JSON.stringify(old.get(row[key]))), removed: [...old.keys()].filter(id => !next.has(id)) };
    };
    const nodes = difference(before.nodes, after.nodes, "uid");
    const observations = difference(before.observations, after.observations, "id");
    return { fromRevision: before.revision, revision: after.revision, nodes: nodes.changed,
      observations: observations.changed, removedNodeUids: nodes.removed, removedObservationIds: observations.removed,
      expanded: after.expanded, savedAt: after.savedAt };
  }

  mutate(action, details, operation, { expectedRevision, audit = true } = {}) {
    if (expectedRevision !== undefined) this.assertRevision(expectedRevision);
    const before = this.graph();
    const rawBefore = audit ? this.rawSnapshot() : null;
    this.db.exec("BEGIN IMMEDIATE");
    try {
      const result = operation();
      this.setSetting("revision", before.revision + 1);
      this.setSetting("savedAt", new Date().toISOString());
      const after = this.graph();
      const changes = GraphStore.diff(before, after);
      this.prepare("INSERT INTO graph_changes VALUES (?,?)").run(after.revision, JSON.stringify(changes));
      let eventId = null;
      if (audit) eventId = Number(this.prepare("INSERT INTO graph_history (action,details,before_json,after_revision,created_at) VALUES (?,?,?,?,?)").run(
        action, JSON.stringify(details), JSON.stringify(rawBefore), after.revision, after.savedAt
      ).lastInsertRowid);
      this.db.exec("COMMIT");
      return { result, changes, revision: after.revision, eventId };
    } catch (error) { this.db.exec("ROLLBACK"); throw error; }
  }

  changesSince(revision) {
    if (!Number.isSafeInteger(revision) || revision < 1 || revision > this.revision) return { graph: this.graph() };
    const changes = this.prepare("SELECT changes_json FROM graph_changes WHERE revision > ? ORDER BY revision").all(revision).map(row => JSON.parse(row.changes_json));
    if (changes.length !== this.revision - revision) return { graph: this.graph() };
    return { changes, revision: this.revision };
  }

  insertObservation(observation, defaults = {}) {
    const from = this.upsertNode(observation.from), to = this.upsertNode(observation.to);
    if (!from || !to || from === to) return null;
    const confidence = Number(observation.confidence);
    if (!Number.isFinite(confidence) || confidence < 0 || confidence > 1) throw new Error("Invalid observation confidence");
    const provider = observation.provider || defaults.provider || "";
    const evidenceType = observation.evidenceType || (provider === "mock" ? "demo" : provider ? "model-sample" : "unknown");
    const values = [from, to, confidence, observation.sourceEntity || defaults.sourceEntity || "", provider,
      observation.model || defaults.model || "", observation.createdAt || new Date().toISOString(), evidenceType, JSON.stringify(observation.sources || [])];
    const fields = "from_id,to_id,confidence,source_entity,provider,model,created_at,evidence_type,sources";
    if (Number.isSafeInteger(observation.id)) {
      return Number(this.prepare(`INSERT INTO observations (id,${fields}) VALUES (?,?,?,?,?,?,?,?,?,?)`).run(observation.id, ...values).lastInsertRowid);
    }
    return Number(this.prepare(`INSERT INTO observations (${fields}) VALUES (?,?,?,?,?,?,?,?,?)`).run(...values).lastInsertRowid);
  }

  importGraph(data) {
    if (this.prepare("SELECT COUNT(*) n FROM nodes").get().n) throw new ConflictError("Import requires an empty database");
    return this.mutate("import", { legacy: true }, () => {
      for (const node of data.nodes || []) this.upsertNode(node.name, { ...node, expanded: (data.expanded || []).includes(this.keyFor(node.name)) });
      for (const observation of data.observations || []) this.insertObservation(observation);
      if (data.pan && Number.isFinite(data.pan.x) && Number.isFinite(data.pan.y)) this.setSetting("pan", JSON.stringify(data.pan));
      if (Number.isFinite(data.zoom)) this.setSetting("zoom", Math.max(0.04, Math.min(8, data.zoom)));
      if (Number.isFinite(data.minConfidence)) this.setSetting("minConfidence", Math.max(0, Math.min(1, data.minConfidence)));
      this.setSetting("legacyMigrationComplete", "1");
    });
  }

  expansion(requestId) {
    const row = this.prepare("SELECT response_json FROM expansions WHERE request_id=?").get(requestId);
    if (!row) return null;
    const response = JSON.parse(row.response_json);
    const current = response.entityUid ? this.node(response.entityUid) : null;
    if (response.entityUid && !current) throw new ConflictError("This expansion's entity was removed. Refresh before intentionally starting a new request.");
    return current ? { ...response, entityUid: current.uid, data: { ...response.data, entity: current.name } } : response;
  }

  appendExpansion(requestId, response) {
    const existing = this.expansion(requestId);
    if (existing) return existing;
    const data = response.data;
    const { revision } = this.mutate("expansion", { requestId, entity: data.entity }, () => {
      this.upsertNode(data.entity, { expanded: true });
      const center = this.node(data.entity);
      response.entityUid = center.uid;
      const neighbors = [...new Set([...data.influencedBy, ...data.influenced].map(item => item.entity))];
      neighbors.forEach((name, index) => {
        if (this.node(name)) return;
        const angle = index * Math.PI * 2 / Math.max(1, neighbors.length);
        this.upsertNode(name, { x: center.x + Math.cos(angle) * 180, y: center.y + Math.sin(angle) * 180 });
      });
      const meta = { provider: response.provider, model: response.model, sourceEntity: data.entity };
      for (const item of data.influencedBy) this.insertObservation({ from: item.entity, to: data.entity, confidence: item.confidence }, meta);
      for (const item of data.influenced) this.insertObservation({ from: data.entity, to: item.entity, confidence: item.confidence }, meta);
      // Cache in the same transaction as the observations: retries never duplicate samples.
      this.prepare("INSERT INTO expansions VALUES (?,?)").run(requestId, JSON.stringify(response));
    }, { audit: false });
    return { ...response, revision };
  }

  rename(source, canonical) {
    const row = this.node(source);
    if (!row) throw new ConflictError("Rename source no longer exists");
    const next = this.keyFor(canonical);
    const target = this.node(canonical);
    if (target && target.uid !== row.uid) throw new ConflictError("Rename target exists; review a merge instead");
    const aliases = [...new Set([...JSON.parse(row.aliases), row.name])].filter(alias => alias !== canonical);
    this.prepare("UPDATE nodes SET id=?,name=?,aliases=?,updated_at=? WHERE uid=?").run(next, canonical, JSON.stringify(aliases), new Date().toISOString(), row.uid);
    this.prepare("UPDATE observations SET from_id=? WHERE from_id=?").run(next, row.id);
    this.prepare("UPDATE observations SET to_id=? WHERE to_id=?").run(next, row.id);
    // source_entity records the original expansion identity, so retain its historical wording.
    return { changed: row.name !== canonical, reason: `Renamed ${row.name} to ${canonical}`, uid: row.uid };
  }

  merge(canonical, entities) {
    const rows = [...new Map(entities.map(name => this.node(name)).filter(Boolean).map(row => [row.uid, row])).values()];
    if (rows.length < 2) throw new ConflictError("Merge needs at least two existing entities");
    let target = this.node(canonical);
    if (!target) { this.rename(rows[0].uid, canonical); target = this.node(rows[0].uid); }
    if (!rows.some(row => row.uid === target.uid)) throw new ConflictError("Canonical entity must be included in the merge");
    if (target.name !== canonical) { this.rename(target.uid, canonical); target = this.node(target.uid); }
    const aliases = [...new Set(rows.flatMap(row => [row.name, ...JSON.parse(row.aliases)]))].filter(name => name !== canonical);
    for (const row of rows) {
      if (row.uid === target.uid) continue;
      this.prepare("UPDATE observations SET from_id=? WHERE from_id=?").run(target.id, row.id);
      this.prepare("UPDATE observations SET to_id=? WHERE to_id=?").run(target.id, row.id);
      this.prepare("DELETE FROM nodes WHERE uid=?").run(row.uid);
      this.prepare("UPDATE entity_redirects SET to_uid=? WHERE to_uid=?").run(target.uid, row.uid);
      this.prepare("INSERT INTO entity_redirects (from_uid,to_uid) VALUES (?,?) ON CONFLICT(from_uid) DO UPDATE SET to_uid=excluded.to_uid").run(row.uid, target.uid);
    }
    this.prepare("UPDATE nodes SET aliases=?,expanded=?,updated_at=? WHERE uid=?").run(JSON.stringify(aliases), rows.some(row => row.expanded) ? 1 : 0, new Date().toISOString(), target.uid);
    // Retain samples that become self-links in the audit snapshot; hide them from graph traversal.
    this.prepare("DELETE FROM observations WHERE from_id=to_id").run();
    return { changed: true, reason: `Merged ${rows.length} entities`, uid: target.uid };
  }

  split(group) {
    const sourceName = group.entities.find(name => this.keyFor(name) !== this.keyFor(group.canonical));
    const source = sourceName ? this.node(sourceName) : null;
    if (!source) throw new ConflictError("Split source no longer exists");
    const ids = [...new Set(group.metadata?.moveObservationIds || [])];
    if (!ids.length || !ids.every(Number.isSafeInteger)) throw new ConflictError("Select exact observation IDs before applying this split");
    for (const id of ids) {
      const observation = this.prepare("SELECT * FROM observations WHERE id=?").get(id);
      if (!observation || (observation.from_id !== source.id && observation.to_id !== source.id)) throw new ConflictError(`Observation ${id} changed; review this split again`);
    }
    const targetId = this.upsertNode(group.canonical, this.node(group.canonical) ? {} : { x: source.x + 36, y: source.y + 36 });
    if (targetId === source.id) throw new ConflictError("Split target must be a different entity");
    for (const id of ids) {
      this.prepare("UPDATE observations SET from_id=CASE WHEN from_id=? THEN ? ELSE from_id END,to_id=CASE WHEN to_id=? THEN ? ELSE to_id END WHERE id=?").run(source.id, targetId, source.id, targetId, id);
    }
    return { changed: true, moved: ids.length, reason: `Moved ${ids.length} samples to ${group.canonical}` };
  }

  edit(group, expectedRevision) {
    return this.mutate(group.action, group, () => {
      if (group.action === "rename") return this.rename(group.entities[0], group.canonical);
      if (group.action === "merge") return this.merge(group.canonical, group.entities);
      if (group.action === "split") return this.split(group);
      throw new Error("Unsupported identity action");
    }, { expectedRevision });
  }

  reset(expectedRevision) {
    return this.mutate("reset", {}, () => {
      this.db.exec("DELETE FROM observations; DELETE FROM nodes; DELETE FROM entity_redirects;");
      this.prepare("UPDATE dedupe_suggestions SET status='superseded' WHERE status='pending'").run();
      this.setSetting("legacyMigrationComplete", "1");
      return { changed: true, reason: "Graph reset; an undo snapshot was retained" };
    }, { expectedRevision });
  }

  history() { return this.prepare("SELECT id,action,details,after_revision AS afterRevision,created_at AS createdAt,undone_by AS undoneBy FROM graph_history ORDER BY id DESC LIMIT 100").all().map(row => ({ ...row, details: JSON.parse(row.details) })); }

  undo(id, expectedRevision) {
    this.assertRevision(expectedRevision);
    const event = this.prepare("SELECT * FROM graph_history WHERE id=?").get(id);
    if (!event || event.undone_by || event.after_revision !== this.revision) throw new ConflictError("Only the latest unchanged graph edit can be undone; newer data must be preserved");
    const snapshot = JSON.parse(event.before_json);
    return this.mutate("undo", { eventId: id }, () => {
      this.db.exec("DELETE FROM observations; DELETE FROM nodes; DELETE FROM expansions; DELETE FROM entity_redirects;");
      for (const table of ["nodes", "observations", "expansions", "entity_redirects"]) {
        for (const row of snapshot[table] || []) {
          const keys = Object.keys(row);
          this.prepare(`INSERT INTO ${table} (${keys.join(",")}) VALUES (${keys.map(() => "?").join(",")})`).run(...keys.map(key => row[key]));
        }
      }
      this.prepare("UPDATE graph_history SET undone_by=? WHERE id=?").run(this.revision + 1, id);
      return { changed: true, reason: `Undid ${event.action}` };
    }, { expectedRevision });
  }
}

module.exports = { GraphStore, ConflictError };
