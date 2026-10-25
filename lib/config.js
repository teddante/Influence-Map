const fs = require("node:fs");
const path = require("node:path");
const { parseEnv } = require("node:util");
const { randomBytes } = require("node:crypto");
const ROOT_DIR = path.resolve(__dirname, "..");

function loadEnvFile(filePath) {
  if (!fs.existsSync(filePath)) return;
  for (const [key, value] of Object.entries(parseEnv(fs.readFileSync(filePath, "utf8")))) {
    if (!Object.hasOwn(process.env, key)) process.env[key] = value;
  }
}

loadEnvFile(path.join(ROOT_DIR, ".env"));
loadEnvFile(path.join(ROOT_DIR, ".env.local"));

const PORT = Number(process.env.PORT || 3000);
const HOST = process.env.HOST || "127.0.0.1";
const PUBLIC_DIR = path.join(ROOT_DIR, "public");
const DATA_FILE = process.env.INFLUENCE_MAP_DATA_FILE || path.join(ROOT_DIR, "data", "graph.json");
const DB_FILE = process.env.INFLUENCE_MAP_DB_FILE || path.join(ROOT_DIR, "data", "influence-map.sqlite");
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
const LLM_MAX_QUEUE = Math.max(1, Number(process.env.INFLUENCE_MAP_LLM_MAX_QUEUE) || 8);
const LLM_MAX_TOKENS = Math.max(128, Math.min(8192, Number(process.env.INFLUENCE_MAP_LLM_MAX_TOKENS) || 2048));
const MUTATIONS_PER_MINUTE = Number(process.env.INFLUENCE_MAP_MUTATIONS_PER_MINUTE) || 30;
const OPENROUTER_JSON_MODE = process.env.OPENROUTER_JSON_MODE === "1";
const OPENROUTER_HTTP_REFERER = process.env.OPENROUTER_HTTP_REFERER || "http://localhost";
const OPENROUTER_APP_TITLE = process.env.OPENROUTER_APP_TITLE || "Influence Map";
const GENERATION_BUDGET = {
  requests: Number(process.env.INFLUENCE_MAP_LLM_DAILY_REQUESTS || 100),
  tokens: Number(process.env.INFLUENCE_MAP_LLM_DAILY_TOKENS || 100000),
  dollars: Number(process.env.INFLUENCE_MAP_LLM_DAILY_USD || 0),
  inputRate: Number(process.env.INFLUENCE_MAP_LLM_INPUT_USD_PER_MILLION || 0),
  outputRate: Number(process.env.INFLUENCE_MAP_LLM_OUTPUT_USD_PER_MILLION || 0)
};
const IDENTITY_AUTO_APPROVE = process.env.INFLUENCE_MAP_IDENTITY_AUTO_APPROVE === "1";
const IDENTITY_AUTO_MERGE_CONFIDENCE = Math.max(0.5, Math.min(1, Number(process.env.INFLUENCE_MAP_AUTO_MERGE_CONFIDENCE || 0.72) || 0.72));
const IDENTITY_AUTO_SPLIT_CONFIDENCE = Math.max(0.5, Math.min(1, Number(process.env.INFLUENCE_MAP_AUTO_SPLIT_CONFIDENCE || 0.78) || 0.78));
const DEV_TOOLS = process.env.INFLUENCE_MAP_DEV_TOOLS === "1";
const ADMIN_TOKEN = process.env.INFLUENCE_MAP_ADMIN_TOKEN || randomBytes(32).toString("hex");

module.exports = {
  PORT, HOST, PUBLIC_DIR, DATA_FILE, DB_FILE, DEV_TOOLS, ADMIN_TOKEN,
  DEEPSEEK_API_KEY, DEEPSEEK_MODEL, DEEPSEEK_BASE_URL, DEEPSEEK_THINKING,
  OPENROUTER_API_KEY, OPENROUTER_MODEL, OPENROUTER_BASE_URL, OPENROUTER_REASONING,
  OPENROUTER_JSON_MODE, OPENROUTER_HTTP_REFERER, OPENROUTER_APP_TITLE,
  DEFAULT_LLM_PROVIDER, LLM_MAX_CONCURRENCY, LLM_MAX_QUEUE, LLM_REQUEST_TIMEOUT_MS, LLM_MAX_TOKENS,
  GENERATION_BUDGET, MUTATIONS_PER_MINUTE,
  IDENTITY_AUTO_APPROVE, IDENTITY_AUTO_MERGE_CONFIDENCE, IDENTITY_AUTO_SPLIT_CONFIDENCE
};
