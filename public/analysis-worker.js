importScripts("/graph-model.js", "/graph-analysis.js");
let version = -1, engine;
self.onmessage = ({ data }) => {
  try {
    if (data.graph) { engine = self.InfluenceAnalysis.createAnalysis(data.graph); version = data.version; }
    if (!engine || version !== data.version) throw new Error("Analysis snapshot is stale");
    const result = data.kind === "next" ? engine.next(100) : engine.roots(data.targetId);
    self.postMessage({ id: data.id, version, result });
  } catch (error) { self.postMessage({ id: data.id, version: data.version, error: error.message }); }
};
