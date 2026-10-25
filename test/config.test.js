const { test } = require("node:test");
const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");
const { execFileSync } = require("node:child_process");
const { temporaryDirectory } = require("./helpers");

test("environment files support native quoting and comments while preserving shell and first-file precedence", t => {
  const root = temporaryDirectory(t);
  fs.mkdirSync(path.join(root, "lib"));
  const configFile = path.join(root, "lib/config.js");
  fs.copyFileSync(path.join(__dirname, "../lib/config.js"), configFile);
  fs.writeFileSync(path.join(root, ".env"), "PORT=4100 # comment\nDEEPSEEK_MODEL='model # literal'\n");
  fs.writeFileSync(path.join(root, ".env.local"), "HOST=from-local\nPORT=4200\nOPENROUTER_MODEL=from-local\n");
  const env = { ...process.env, HOST: "from-shell" };
  for (const key of ["PORT", "DEEPSEEK_MODEL", "OPENROUTER_MODEL"]) delete env[key];
  const output = execFileSync(process.execPath, ["-e", `const c = require(process.argv[1]); console.log(JSON.stringify([c.HOST, c.PORT, c.DEEPSEEK_MODEL, c.OPENROUTER_MODEL]));`, configFile], { env, encoding: "utf8", windowsHide: true });
  assert.deepEqual(JSON.parse(output), ["from-shell", 4100, "model # literal", "from-local"]);
});
