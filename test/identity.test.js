const { test } = require("node:test");
const assert = require("node:assert/strict");
const { keyFor } = require("../public/graph-model");
const { compatibleTitleVariants, dedupeCandidates, targetedDedupeCandidates } = require("../lib/identity");

test("title comparisons preserve distinct works, years and entity senses", () => {
  assert.equal(compatibleTitleVariants(["Quake", "Quake (1996 video game)"]), true);
  assert.equal(compatibleTitleVariants(["Dune (1965 novel)", "Dune (1965 book)"]), true);
  for (const names of [
    ["The Shining (1980 film)", "The Shining (1997 television series)"],
    ["Dune (1965 novel)", "Dune (1984 film)"],
    ["Rust (video game)", "Rust (programming language)"],
    ["Quake (franchise)", "Quake (1996 video game)"]
  ]) assert.equal(compatibleTitleVariants(names), false, names.join(" / "));
});

test("full and scoped identity scans retain alias candidates without changing the input nodes", () => {
  const nodes = [
    { name: "Operation Flashpoint", aliases: ["OpFlash"] },
    { name: "OpFlash", aliases: [] },
    { name: "Unrelated entity", aliases: [] }
  ].map(node => ({ ...node, id: keyFor(node.name), allNames: [node.name, ...node.aliases] }));
  const before = JSON.stringify(nodes);
  const full = dedupeCandidates(nodes);
  const scoped = targetedDedupeCandidates(nodes, ["OpFlash", "New entity"]);
  for (const groups of [full, scoped]) {
    assert.ok(groups.some(group => group.entities.includes("Operation Flashpoint") && group.entities.includes("OpFlash")));
    assert.ok(groups.every(group => !group.entities.includes("Unrelated entity")));
  }
  assert.equal(JSON.stringify(nodes), before);
});
