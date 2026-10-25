(function (root) {
  function normalizeName(name) {
    return String(name || "")
      .trim()
      .replace(/\s+/g, " ");
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

  function edgeKey(from, to) { return keyFor(from) + "=>" + keyFor(to); }

  // Aggregate complete sample histories once, before applying any view filter.
  function aggregateGraph(nodes, observations) {
    const stats = new Map([...nodes.values()].map(node => [node.id, {
      node, observations: 0, confidenceSum: 0, neighbors: new Set(),
      sourceEntities: new Set(), incoming: 0, outgoing: 0
    }]));
    const edges = new Map();
    const keys = new Map();
    const nameKey = name => {
      if (!keys.has(name)) keys.set(name, keyFor(name));
      return keys.get(name);
    };
    for (const observation of observations) {
      const from = nameKey(observation.from), to = nameKey(observation.to);
      const source = stats.get(from), target = stats.get(to);
      if (!source || !target || from === to) continue;
      const confidence = Math.max(0, Math.min(1, Number(observation.confidence) || 0));
      const sourceEntity = observation.sourceEntity ? nameKey(observation.sourceEntity) : null;
      source.outgoing++;
      target.incoming++;
      for (const [item, neighbor] of [[source, to], [target, from]]) {
        item.observations++;
        item.confidenceSum += confidence;
        item.neighbors.add(neighbor);
        if (sourceEntity) item.sourceEntities.add(sourceEntity);
      }
      const key = from + "=>" + to;
      let edge = edges.get(key);
      if (!edge) {
        edge = { key, from: source.node, to: target.node, confidence: 0, count: 0, observations: [] };
        edges.set(key, edge);
      }
      edge.confidence += confidence;
      edge.count++;
      edge.observations.push(observation);
    }
    for (const edge of edges.values()) edge.confidence /= edge.count;
    return { edges, stats };
  }

  const model = { normalizeName, keyFor, edgeKey, aggregateGraph };
  if (typeof module !== "undefined") module.exports = model;
  else root.GraphModel = model;
})(typeof self !== "undefined" ? self : globalThis);
