const fs = require("node:fs");
const os = require("node:os");
const path = require("node:path");
const { spawn } = require("node:child_process");
const directoryServers = new Map();

function temporaryDirectory(t) {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "influence-map-test-"));
  directoryServers.set(root, []);
  t.after(async () => {
    for (const stop of directoryServers.get(root)) await stop();
    const resolved = path.resolve(root);
    if (!resolved.toLowerCase().startsWith((path.resolve(os.tmpdir()) + path.sep + "influence-map-test-").toLowerCase())) throw new Error("Unsafe temporary directory cleanup");
    fs.rmSync(resolved, { recursive: true, force: true });
    directoryServers.delete(root);
  });
  return root;
}

async function startServer(t, root = temporaryDirectory(t), extraEnv = {}) {
  const child = spawn(process.execPath, [path.resolve(__dirname, "..", "server.js")], {
    env: { ...process.env, HOST: "127.0.0.1", PORT: "0", DEEPSEEK_API_KEY: "", OPENROUTER_API_KEY: "",
      INFLUENCE_MAP_LLM_PROVIDER: "mock", INFLUENCE_MAP_IDENTITY_AUTO_APPROVE: "0",
      INFLUENCE_MAP_ADMIN_TOKEN: "test-private-token", INFLUENCE_MAP_DEV_TOOLS: "1",
      INFLUENCE_MAP_MUTATIONS_PER_MINUTE: "1000", INFLUENCE_MAP_LLM_DAILY_USD: "0",
      INFLUENCE_MAP_DB_FILE: path.join(root, "graph.sqlite"), INFLUENCE_MAP_DATA_FILE: path.join(root, "legacy.json"), ...extraEnv },
    windowsHide: true, stdio: ["ignore", "pipe", "pipe"]
  });
  let output = "";
  const closed = new Promise(resolve => child.once("exit", resolve));
  const stop = async () => { if (child.exitCode === null && child.signalCode === null) child.kill(); await closed; };
  directoryServers.get(root)?.push(stop);
  t.after(stop);
  const base = await new Promise((resolve, reject) => {
    const timeout = setTimeout(() => { child.kill(); reject(new Error("Server startup timed out: " + output)); }, 10000);
    child.stdout.on("data", chunk => {
      output += chunk;
      const match = output.match(/Influence graph running at (http:\/\/127\.0\.0\.1:\d+)/);
      if (match) { clearTimeout(timeout); resolve(match[1]); }
    });
    child.stderr.on("data", chunk => { output += chunk; });
    child.once("exit", code => { clearTimeout(timeout); reject(new Error("Server exited " + code + ": " + output)); });
  });
  async function request(url, { method = "GET", body, authenticated = true, headers = {} } = {}) {
    const response = await fetch(base + url, {
      method, headers: { ...(authenticated ? { "x-admin-token": "test-private-token" } : {}),
        ...(body !== undefined ? { "content-type": "application/json" } : {}), ...headers },
      body: body === undefined ? undefined : typeof body === "string" ? body : JSON.stringify(body)
    });
    return { status: response.status, headers: response.headers, data: await response.json() };
  }
  return { root, base, child, request, stop };
}

module.exports = { temporaryDirectory, startServer };
