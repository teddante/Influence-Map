const fs = require("node:fs");
const path = require("node:path");
const { randomUUID } = require("node:crypto");
const { DatabaseSync } = require("node:sqlite");

function verifiedBackup(db, databaseFile) {
  if (databaseFile === ":memory:" || !fs.statSync(databaseFile).size) return null;
  const directory = path.join(path.dirname(databaseFile), "backups");
  fs.mkdirSync(directory, { recursive: true });
  const file = path.join(directory, "snapshot-" + Date.now() + "-" + randomUUID() + ".sqlite");
  db.exec("VACUUM INTO '" + file.replace(/'/g, "''") + "'");
  const copy = new DatabaseSync(file, { readOnly: true });
  try {
    if (copy.prepare("PRAGMA integrity_check").get().integrity_check !== "ok") throw new Error("Backup integrity verification failed");
    for (const { name } of db.prepare("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'").all()) {
      const identifier = '"' + name.replace(/"/g, '""') + '"';
      if (db.prepare(`SELECT COUNT(*) n FROM ${identifier}`).get().n !== copy.prepare(`SELECT COUNT(*) n FROM ${identifier}`).get().n) throw new Error("Backup record count differs: " + name);
    }
  } finally { copy.close(); }
  return file;
}

module.exports = { verifiedBackup };
