// Shared by the analysis worker and deterministic Node regression tests.
(function (root) {
  const { keyFor, aggregateGraph } = typeof module !== "undefined" ? require("./graph-model") : root.GraphModel;
  function createAnalysis(graph) {
    const state = { nodes: new Map(graph.nodes.map(node => [node.id, node])), observations: graph.observations,
      expanded: new Set(graph.expanded), expanding: new Set(graph.expanding || []),
      rootTraceCache: new Map(), rootMetricsCache: null, expansionRecommendationsCache: null };
    const aggregated = aggregateGraph(state.nodes, state.observations);
    const edges = graph.edges
      ? graph.edges.map(edge => ({ ...edge, from: state.nodes.get(edge.from), to: state.nodes.get(edge.to) }))
      : [...aggregated.edges.values()].map(({ key, from, to, confidence, count }) => ({ key, from, to, confidence, count }));
    const incoming = new Map(), outgoing = new Map();
    for (const edge of edges) {
      const sources = incoming.get(edge.to.id) || [];
      const targets = outgoing.get(edge.from.id) || [];
      sources.push(edge);
      targets.push(edge);
      incoming.set(edge.to.id, sources);
      outgoing.set(edge.from.id, targets);
    }
    for (const bucket of incoming.values()) {
      bucket.sort((a, b) => (b.confidence * Math.log2(b.count + 1)) - (a.confidence * Math.log2(a.count + 1)));
    }

    function traceRoots(targetId, options = {}) {
      const target = state.nodes.get(targetId);
      if (!target) return null;
      const maxDepth = Math.max(2, Math.min(10, Number(options.depth || 7)));
      const branchLimit = Math.max(2, Math.min(8, Number(options.branchLimit || 5)));
      const roots = new Map();
      const paths = [];
      const traceNodeIds = new Set([target.id]);
      const traceEdgeKeys = new Set();

      function addRoot(node, path, score, depth, terminalReason) {
        const existing = roots.get(node.id) || {
          node,
          score: 0,
          paths: 0,
          bestScore: 0,
          maxDepth: 0,
          terminalReason
        };
        existing.score += score;
        existing.paths += 1;
        existing.bestScore = Math.max(existing.bestScore, score);
        existing.maxDepth = Math.max(existing.maxDepth, depth);
        roots.set(node.id, existing);
        paths.push({ root: node, path: [...path], score, depth, terminalReason });
        for (const step of path) {
          traceNodeIds.add(step.from.id);
          traceNodeIds.add(step.to.id);
          traceEdgeKeys.add(step.key);
        }
      }

      function walk(node, path, score, visited) {
        const depth = path.length;
        const incomingEdges = (incoming.get(node.id) || [])
          .filter(edge => !visited.has(edge.from.id))
          .slice(0, branchLimit);
        if (!incomingEdges.length || depth >= maxDepth) {
          addRoot(node, path, score * (1 + depth * 0.12), depth, incomingEdges.length ? "depth limit" : (incoming.get(node.id) || []).length ? "cycle boundary" : "no known upstream samples");
          return;
        }
        for (const edge of incomingEdges) {
          const nextScore = score * Math.max(0.08, edge.confidence) * (1 + Math.log2(edge.count + 1) * 0.08);
          walk(edge.from, [edge, ...path], nextScore, new Set([...visited, edge.from.id]));
        }
      }

      walk(target, [], 1, new Set([target.id]));
      return {
        target,
        nodeIds: traceNodeIds,
        edgeKeys: traceEdgeKeys,
        roots: [...roots.values()]
          .filter(item => item.node.id !== target.id)
          .sort((a, b) => b.score - a.score || b.maxDepth - a.maxDepth)
          .slice(0, 12),
        paths: paths
          .filter(item => item.root.id !== target.id && item.path.length)
          .sort((a, b) => b.score - a.score || b.depth - a.depth)
          .slice(0, 8)
      };
    }

    function cachedTraceRoots(targetId, options = {}) {
      const depth = Math.max(2, Math.min(10, Number(options.depth || 7)));
      const branchLimit = Math.max(2, Math.min(8, Number(options.branchLimit || 5)));
      const key = `${targetId}:${depth}:${branchLimit}`;
      if (state.rootTraceCache.has(key)) return state.rootTraceCache.get(key);
      const trace = traceRoots(targetId, { depth, branchLimit });
      state.rootTraceCache.set(key, trace);
      if (state.rootTraceCache.size > 400) {
        const first = state.rootTraceCache.keys().next().value;
        state.rootTraceCache.delete(first);
      }
      return trace;
    }

    function globalInfluenceMetrics(limit = 12) {
      const requestedLimit = Math.max(1, Number(limit) || 12);
      if (
        state.rootMetricsCache
        && state.rootMetricsCache.limit >= requestedLimit
      ) {
        return state.rootMetricsCache.scores.slice(0, requestedLimit);
      }

      const bridgeCounts = new Map();
      const rootCounts = new Map();

      for (const node of state.nodes.values()) {
        const trace = cachedTraceRoots(node.id, { depth: 5, branchLimit: 3 });
        if (!trace) continue;
        for (const root of trace.roots.slice(0, 8)) {
          rootCounts.set(root.node.id, (rootCounts.get(root.node.id) || 0) + root.score);
        }
        for (const path of trace.paths.slice(0, 6)) {
          const inner = path.path.slice(1, -1);
          for (const edge of inner) {
            bridgeCounts.set(edge.to.id, (bridgeCounts.get(edge.to.id) || 0) + path.score);
          }
        }
      }

      const scores = [];
      for (const node of state.nodes.values()) {
        const seen = new Set([node.id]);
        let frontier = [{ node, depth: 0, score: 1 }];
        let downstream = 0;
        let confidenceFlow = 0;
        const firstBranches = new Set();
        for (let depth = 0; depth < 6; depth += 1) {
          const next = [];
          for (const item of frontier) {
            for (const edge of outgoing.get(item.node.id) || []) {
              if (seen.has(edge.to.id)) continue;
              seen.add(edge.to.id);
              if (depth === 0) firstBranches.add(edge.to.id);
              const edgeScore = item.score * Math.max(0.1, edge.confidence) * (1 + Math.log2(edge.count + 1) * 0.08);
              downstream += 1 / (depth + 1);
              confidenceFlow += edgeScore;
              next.push({ node: edge.to, depth: depth + 1, score: edgeScore });
            }
          }
          frontier = next
            .sort((a, b) => b.score - a.score)
            .slice(0, 90);
          if (!frontier.length) break;
        }
        const upstreamCount = (incoming.get(node.id) || []).length;
        const directOut = (outgoing.get(node.id) || []).length;
        const reachScore = downstream + confidenceFlow;
        const rootScore = (rootCounts.get(node.id) || 0) * (1 / Math.sqrt(1 + upstreamCount));
        const bridgeScore = bridgeCounts.get(node.id) || 0;
        const diversityScore = Math.sqrt(firstBranches.size) * Math.log2(2 + directOut);
        const sourceBias = 1 / Math.sqrt(1 + upstreamCount * 0.65);
        const trailblazerScore = (reachScore * 0.42 + rootScore * 0.34 + bridgeScore * 0.16 + diversityScore * 0.08) * sourceBias;
        if (trailblazerScore > 0) {
          scores.push({
            node,
            score: trailblazerScore,
            reachScore,
            rootScore,
            bridgeScore,
            diversityScore,
            downstream: seen.size - 1,
            directOut,
            upstreamCount
          });
        }
      }
      const sorted = scores.sort((a, b) => b.score - a.score);
      state.rootMetricsCache = {
        limit: Math.max(requestedLimit, 20),
        scores: sorted.slice(0, Math.max(requestedLimit, 20))
      };
      return state.rootMetricsCache.scores.slice(0, requestedLimit);
    }

    function removalRegionCounts() {
      const adjacency = new Map([...state.nodes.keys()].map(id => [id, new Set()]));
      for (const edge of edges) {
        adjacency.get(edge.from.id)?.add(edge.to.id);
        adjacency.get(edge.to.id)?.add(edge.from.id);
      }
      const discovered = new Map(), low = new Map(), parents = new Map(), separated = new Map(), children = new Map();
      let tick = 0;
      for (const id of adjacency.keys()) {
        if (discovered.has(id)) continue;
        discovered.set(id, ++tick); low.set(id, tick);
        const stack = [{ id, neighbors: [...adjacency.get(id)], index: 0 }];
        while (stack.length) {
          const frame = stack.at(-1);
          if (frame.index < frame.neighbors.length) {
            const next = frame.neighbors[frame.index++];
            if (!discovered.has(next)) {
              parents.set(next, frame.id); children.set(frame.id, (children.get(frame.id) || 0) + 1);
              discovered.set(next, ++tick); low.set(next, tick);
              stack.push({ id: next, neighbors: [...adjacency.get(next)], index: 0 });
            } else if (parents.get(frame.id) !== next) low.set(frame.id, Math.min(low.get(frame.id), discovered.get(next)));
          } else {
            stack.pop();
            const parent = parents.get(frame.id);
            if (parent !== undefined) {
              low.set(parent, Math.min(low.get(parent), low.get(frame.id)));
              if (low.get(frame.id) >= discovered.get(parent)) separated.set(parent, (separated.get(parent) || 0) + 1);
            }
          }
        }
      }
      return new Map([...adjacency.keys()].map(id => [id, parents.has(id) ? (separated.get(id) || 0) + 1 : children.get(id) || 0]));
    }

    function expansionRecommendations(limit = 12) {
      const requestedLimit = Math.max(1, Number(limit) || 12);
      if (
        state.expansionRecommendationsCache
        && state.expansionRecommendationsCache.limit >= requestedLimit
      ) {
        return state.expansionRecommendationsCache.items.slice(0, requestedLimit);
      }

      const stats = aggregated.stats;

      const regionCounts = removalRegionCounts();
      const strongCounts = new Map();
      for (const edge of edges) if (edge.confidence >= 0.55) {
        strongCounts.set(edge.from.id, (strongCounts.get(edge.from.id) || 0) + 1);
        strongCounts.set(edge.to.id, (strongCounts.get(edge.to.id) || 0) + 1);
      }
      const items = [];
      for (const stat of stats.values()) {
        const node = stat.node;
        if (state.expanded.has(node.id) || state.expanding.has(node.id) || !stat.observations) continue;
        const regionCount = Math.max(1, regionCounts.get(node.id) || 0);
        const avgConfidence = stat.confidenceSum / Math.max(1, stat.observations);
        const strongLinks = strongCounts.get(node.id) || 0;
        const name = keyFor(node.name);
        const hasQualifier = /\(|\[|\b(1[0-9]{3}|20[0-9]{2})\b/.test(node.name);
        const shortName = name.split(" ").filter(Boolean).length <= 2;
        const ambiguityScore = shortName && !hasQualifier && stat.observations >= 2 ? 1 : 0;
        const recurrenceScore = Math.log2(1 + stat.observations) * 1.25;
        const bridgeScore = Math.log2(1 + regionCount) * Math.sqrt(1 + stat.neighbors.size);
        const sourceScore = Math.log2(1 + stat.sourceEntities.size);
        const confidenceScore = avgConfidence * 1.4;
        const directionScore = stat.incoming && stat.outgoing ? 0.75 : 0.25;
        const strongLinkScore = Math.log2(1 + strongLinks) * 0.5;
        const value = recurrenceScore + bridgeScore + sourceScore + confidenceScore + directionScore + strongLinkScore + ambiguityScore;
        const reasons = [];
        if (regionCount > 1) reasons.push(`touches ${regionCount} map neighborhoods`);
        if (stat.sourceEntities.size > 1) reasons.push(`seen from ${stat.sourceEntities.size} sampled entities`);
        if (stat.incoming && stat.outgoing) reasons.push("has both incoming and outgoing hints");
        if (ambiguityScore) reasons.push("short or ambiguous name may clarify identity");
        if (!reasons.length) reasons.push("unexpanded frontier node with existing evidence");
        items.push({
          node,
          value,
          observations: stat.observations,
          connections: stat.neighbors.size,
          avgConfidence,
          regions: regionCount,
          reasons
        });
      }

      const sorted = items.sort((a, b) => b.value - a.value || b.observations - a.observations || a.node.name.localeCompare(b.node.name));
      state.expansionRecommendationsCache = {
        limit: Math.max(requestedLimit, 30),
        items: sorted.slice(0, Math.max(requestedLimit, 30))
      };
      return state.expansionRecommendationsCache.items.slice(0, requestedLimit);
    }


    return { next: limit => expansionRecommendations(limit), roots: id => ({ trace: id ? cachedTraceRoots(id) : null, metrics: globalInfluenceMetrics(10) }), removalRegionCounts };
  }
  if (typeof module !== "undefined") module.exports = { createAnalysis };
  else root.InfluenceAnalysis = { createAnalysis };
})(typeof self !== "undefined" ? self : globalThis);
