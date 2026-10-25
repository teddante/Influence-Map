function initDb(db) {
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

module.exports = { initDb };
