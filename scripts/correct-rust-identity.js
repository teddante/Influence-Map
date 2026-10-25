const fs = require("node:fs");
const path = require("node:path");
const { DatabaseSync } = require("node:sqlite");
const { GraphStore } = require("../lib/graph-store");
const { initDb } = require("../lib/schema");
const { verifiedBackup } = require("../lib/database-backup");
const { correctRustIdentity } = require("../lib/rust-identity-correction");

const args = process.argv.slice(2);
const value = flag => args[args.indexOf(flag) + 1];
if (!args.includes("--db") || !args.includes("--legacy") || !args.includes("--apply")) {
  throw new Error("Usage: node scripts/correct-rust-identity.js --db PATH --legacy PATH --apply (stop the app first)");
}
const databaseFile = path.resolve(value("--db"));
if (!fs.existsSync(databaseFile)) throw new Error("Existing database required");
const legacy = JSON.parse(fs.readFileSync(path.resolve(value("--legacy")), "utf8").replace(/^\uFEFF/, ""));
const db = new DatabaseSync(databaseFile);
try {
  const backup = verifiedBackup(db, databaseFile);
  const baseline = db.prepare("SELECT id,from_id,to_id,confidence,source_entity,provider,model,created_at FROM observations ORDER BY id").all();
  initDb(db);
  const store = new GraphStore(db);
  store.setSetting("legacyMigrationComplete", "1");
  const correction = correctRustIdentity(store, legacy);
  const after = db.prepare("SELECT id,from_id,to_id,confidence,source_entity,provider,model,created_at FROM observations ORDER BY id").all();
  if (baseline.length !== after.length) throw new Error("Observation count changed unexpectedly; restore verified backup " + backup);
  let endpointsChanged = 0;
  baseline.forEach((before, index) => {
    const current = after[index];
    if (before.id !== current.id || JSON.stringify({ ...before, to_id: current.to_id }) !== JSON.stringify(current)) throw new Error("Unexpected sample mutation; restore backup " + backup);
    if (before.to_id !== current.to_id) endpointsChanged++;
  });
  if (endpointsChanged !== (correction.changed ? 3 : 0)) throw new Error("Unexpected number of corrected endpoints");
  if (db.prepare("PRAGMA integrity_check").get().integrity_check !== "ok") throw new Error("Post-correction integrity check failed");
  console.log(JSON.stringify({ backup, changed: correction.changed, eventId: correction.eventId, revision: store.revision,
    observationsPreserved: after.length, correctedEndpoints: endpointsChanged, nodes: store.graph().nodes.length, integrity: "ok" }, null, 2));
} finally { db.close(); }
