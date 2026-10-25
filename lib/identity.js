const { normalizeName, keyFor } = require("../public/graph-model");

function canonicalFromNames(names) {
  const sorted = [...names].sort((a, b) => {
    const acronymA = acronymFor(a) === keyFor(a);
    const acronymB = acronymFor(b) === keyFor(b);
    if (acronymA !== acronymB) return acronymA ? 1 : -1;
    return b.length - a.length || a.localeCompare(b);
  });
  return sorted[0] || names[0] || "";
}

function identityKind(name) {
  const key = keyFor(name);
  const kinds = new Set();
  if (/\bfilm\b|\bmovie\b/.test(key)) kinds.add("film");
  if (/\bvideo game\b|\bgame\b/.test(key)) kinds.add("game");
  if (/\bfranchise\b/.test(key)) kinds.add("franchise");
  if (/\btv series\b|\btelevision series\b/.test(key)) kinds.add("tv-series");
  else if (/\bseries\b/.test(key)) kinds.add("series");
  if (/\balbum\b/.test(key)) kinds.add("album");
  if (/\bnovel\b|\bbook\b/.test(key)) kinds.add("book");
  if (/\bband\b/.test(key)) kinds.add("band");
  if (!kinds.has("tv-series") && /\btv\b|\btelevision\b/.test(key)) kinds.add("television");
  return kinds;
}

function identityYears(name) {
  return [...keyFor(name).matchAll(/\b(19|20)\d{2}\b/g)].map(match => match[0]);
}

function compatibleTitleVariants(names) {
  const clean = (names || []).map(normalizeName).filter(Boolean);
  if (clean.length < 2) return false;
  const base = baseTitleKey(clean[0]);
  if (!base || clean.some(name => baseTitleKey(name) !== base)) return false;

  const yearSets = clean.map(identityYears).filter(years => years.length);
  const years = new Set(yearSets.flat());
  if (yearSets.length > 1 && years.size > 1) return false;

  const kindSets = clean.map(identityKind);
  const explicitKinds = kindSets.filter(set => set.size);
  if (!explicitKinds.length) return false;
  const allKinds = new Set(explicitKinds.flatMap(set => [...set]));
  if ((allKinds.has("series") || allKinds.has("franchise")) && allKinds.size > 1) return false;

  return allKinds.size === 1
    && ["film", "game", "album", "book", "band", "tv-series", "television"].includes([...allKinds][0]);
}

function identityConflictReason(names = []) {
  const clean = (names || []).map(normalizeName).filter(Boolean);
  const kindSets = clean.map(identityKind);
  const explicitKinds = kindSets.filter(set => set.size);
  const allKinds = new Set(explicitKinds.flatMap(set => [...set]));
  if ((allKinds.has("series") || allKinds.has("franchise")) && allKinds.size > 1) {
    return "series/franchise mixed with a specific work";
  }

  const yearSets = clean.map(identityYears).filter(years => years.length);
  const years = new Set(yearSets.flat());
  if (yearSets.length > 1 && years.size > 1) {
    return "conflicting years";
  }

  const bases = new Set(clean.map(baseTitleKey).filter(Boolean));
  if (bases.size === 1 && clean.some(hasDisambiguator) && !compatibleTitleVariants(clean)) {
    return "same title but incompatible disambiguators";
  }
  return "";
}

function editDistance(a, b) {
  const left = String(a || "");
  const right = String(b || "");
  if (left === right) return 0;
  if (!left) return right.length;
  if (!right) return left.length;
  let previous = Array.from({ length: right.length + 1 }, (_, index) => index);
  let current = Array(right.length + 1).fill(0);
  for (let i = 1; i <= left.length; i += 1) {
    current[0] = i;
    for (let j = 1; j <= right.length; j += 1) {
      const cost = left[i - 1] === right[j - 1] ? 0 : 1;
      current[j] = Math.min(
        previous[j] + 1,
        current[j - 1] + 1,
        previous[j - 1] + cost
      );
    }
    [previous, current] = [current, previous];
  }
  return previous[right.length];
}

function looksLikeSpellingVariant(a, b) {
  const left = compactKey(baseTitleKey(a) || a);
  const right = compactKey(baseTitleKey(b) || b);
  if (left.length < 6 || right.length < 6) return false;
  const distance = editDistance(left, right);
  return distance <= 2 || distance / Math.max(left.length, right.length) <= 0.18;
}

function acronymFor(name) {
  return normalizeName(name)
    .split(/\s+/)
    .filter(part => !["the", "a", "an", "of", "and"].includes(part.toLowerCase()))
    .map(part => part[0])
    .join("")
    .toLowerCase();
}

function isAcronymLike(name) {
  const key = keyFor(name);
  return /^[a-z0-9]{2,8}$/.test(key) && key === compactKey(name);
}

function compactKey(name) {
  return keyFor(name).replace(/\s+/g, "");
}

function baseTitleKey(name) {
  return keyFor(name)
    .replace(/\b(19|20)\d{2}s?\b/g, " ")
    .replace(/\bfilm\b|\bmovie\b|\bvideo game\b|\bgame\b|\balbum\b|\bnovel\b|\bbook\b|\bseries\b|\bfranchise\b|\btv\b|\btelevision\b/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

function hasDisambiguator(name) {
  return /[(),:]|\b(19|20)\d{2}s?\b|\bfilm\b|\bmovie\b|\bvideo game\b|\bgame\b|\balbum\b|\bnovel\b|\bbook\b|\bseries\b|\bfranchise\b|\btv\b|\btelevision\b/i.test(name);
}

function dedupeTokens(name) {
  return keyFor(name)
    .split(/\s+/)
    .map(token => token.replace(/s$/, ""))
    .filter(token => token && !["the", "a", "an", "of", "and", "or", "for", "game", "video", "s"].includes(token));
}

function tokenOverlapScore(a, b) {
  return tokenOverlap(new Set(dedupeTokens(a)), new Set(dedupeTokens(b)));
}

function tokenOverlap(aTokens, bTokens) {
  if (!aTokens.size || !bTokens.size) return 0;
  let shared = 0;
  for (const token of aTokens) {
    if (bTokens.has(token)) shared += 1;
  }
  return shared / Math.min(aTokens.size, bTokens.size);
}

// Reuse normalized forms and token sets throughout each candidate scan.
function createNameIndex() {
  const cache = new Map();
  return name => {
    if (!cache.has(name)) cache.set(name, {
      key: keyFor(name), compact: compactKey(name), acronym: acronymFor(name),
      acronymLike: isAcronymLike(name), baseTitle: baseTitleKey(name),
      disambiguated: hasDisambiguator(name), tokens: new Set(dedupeTokens(name))
    });
    return cache.get(name);
  };
}

function suggestionKey(entities) {
  return [...new Set((entities || []).map(keyFor).filter(Boolean))].sort().join("|");
}

function targetedTitleVariantCandidates(nodes, names = []) {
  const touched = new Set((names || []).map(keyFor).filter(Boolean));
  if (!touched.size) return [];
  const byBase = new Map();
  for (const node of nodes) {
    const base = baseTitleKey(node.name);
    if (!base || !hasDisambiguator(node.name)) continue;
    const bucket = byBase.get(base) || [];
    bucket.push(node);
    byBase.set(base, bucket);
  }

  const groups = new Map();
  for (const bucket of byBase.values()) {
    for (let i = 0; i < bucket.length; i += 1) {
      for (let j = i + 1; j < bucket.length; j += 1) {
        const a = bucket[i];
        const b = bucket[j];
        if (!touched.has(a.id) && !touched.has(b.id)) continue;
        if (!compatibleTitleVariants([a.name, b.name])) continue;
        const entities = [a.name, b.name];
        const key = suggestionKey(entities);
        if (!groups.has(key)) {
          groups.set(key, {
            reason: "same base title with compatible disambiguators",
            entities
          });
        }
      }
    }
  }
  return [...groups.values()];
}

function targetedDedupeCandidates(existingNodes, names = []) {
  const describe = createNameIndex();
  const inputNames = [...new Set((names || []).map(normalizeName).filter(Boolean))];
  const touchedKeys = new Set(inputNames.map(keyFor).filter(Boolean));
  if (!touchedKeys.size) return [];

  const nodes = [...existingNodes];
  const knownKeys = new Set();
  for (const node of nodes) {
    knownKeys.add(node.id);
    for (const name of node.allNames) knownKeys.add(keyFor(name));
  }
  for (const name of inputNames) {
    const key = keyFor(name);
    if (!key || knownKeys.has(key)) continue;
    nodes.push({
      id: key,
      name,
      aliases: [],
      allNames: [name]
    });
    knownKeys.add(key);
  }

  const touchedNodes = nodes.filter(node => touchedKeys.has(node.id)
    || node.allNames.some(name => touchedKeys.has(keyFor(name))));
  if (!touchedNodes.length) return targetedTitleVariantCandidates(existingNodes, names);

  const groups = new Map();
  function add(reason, items) {
    const uniqueById = new Map();
    for (const item of items || []) {
      if (item && item.id && item.name) uniqueById.set(item.id, item);
    }
    const namesForKey = [...uniqueById.values()].map(item => item.name);
    if (namesForKey.length < 2) return;
    const key = namesForKey.map(keyFor).sort().join("|");
    if (!groups.has(key)) groups.set(key, { reason, entities: namesForKey });
  }

  const seenPairs = new Set();
  for (const touched of touchedNodes) {
    for (const node of nodes) {
      if (touched.id === node.id) continue;
      const pairKey = [touched.id, node.id].sort().join("|");
      if (seenPairs.has(pairKey)) continue;
      seenPairs.add(pairKey);

      const touchedNames = touched.allNames.length ? touched.allNames : [touched.name];
      const nodeNames = node.allNames.length ? node.allNames : [node.name];
      const bestNames = [touched.name, node.name];

      if (touched.id.replace(/s$/, "") === node.id.replace(/s$/, "")) {
        add("same normalized singular form", [touched, node]);
      }

      if (touched.id.length >= 4 && node.id.length >= 4 && (touched.id.includes(node.id) || node.id.includes(touched.id))) {
        add("one normalized name contains the other", [touched, node]);
      }

      for (const touchedName of touchedNames) {
        for (const nodeName of nodeNames) {
          const a = describe(touchedName), b = describe(nodeName);
          if (a.compact.length >= 5 && a.compact === b.compact) {
            add("same compact punctuation-free form", [touched, node]);
          }
          if (a.acronymLike && b.acronym === a.key) {
            add("exact acronym-name match", [touched, node]);
          }
          if (b.acronymLike && a.acronym === b.key) {
            add("exact acronym-name match", [touched, node]);
          }
          if (tokenOverlap(a.tokens, b.tokens) >= 0.8) {
            add("high token overlap", [touched, node]);
          }
          if (a.disambiguated
            && b.disambiguated
            && a.baseTitle === b.baseTitle
            && compatibleTitleVariants([touchedName, nodeName])) {
            add("same base title with compatible disambiguators", [touched, node]);
          }
          if (a.disambiguated && a.baseTitle === b.key) {
            add("same base title with disambiguator", [touched, node]);
          }
          if (b.disambiguated && b.baseTitle === a.key) {
            add("same base title with disambiguator", [touched, node]);
          }
        }
      }

      if (describe(touched.name).baseTitle === describe(node.name).baseTitle && compatibleTitleVariants(bestNames)) {
        add("same base title with compatible disambiguators", [touched, node]);
      }
    }
  }

  for (const candidate of targetedTitleVariantCandidates(existingNodes, names)) {
    const key = (candidate.entities || []).map(keyFor).sort().join("|");
    if (key && !groups.has(key)) groups.set(key, candidate);
  }

  return [...groups.values()].slice(0, 80);
}

function dedupeCandidates(nodes, limit = 500) {
  const describe = createNameIndex();
  const groups = new Map();

  function add(reason, items) {
    const names = [...new Set(items.map(item => item.name).filter(Boolean))];
    if (names.length < 2) return;
    const key = names.map(keyFor).sort().join("|");
    if (!groups.has(key)) groups.set(key, { reason, entities: names });
  }

  const byNoPlural = new Map();
  const byCompact = new Map();
  const byBaseTitle = new Map();
  for (const node of nodes) {
    const singular = node.id.replace(/s$/, "");
    const bucket = byNoPlural.get(singular) || [];
    bucket.push(node);
    byNoPlural.set(singular, bucket);

    for (const name of node.allNames) {
      const features = describe(name);
      const compact = features.compact;
      if (compact.length >= 5) {
        const compactBucket = byCompact.get(compact) || [];
        compactBucket.push(node);
        byCompact.set(compact, compactBucket);
      }

      const baseTitle = features.baseTitle;
      if (baseTitle && baseTitle !== features.key && features.disambiguated) {
        const baseBucket = byBaseTitle.get(baseTitle) || [];
        baseBucket.push(node);
        byBaseTitle.set(baseTitle, baseBucket);
      }
    }
  }
  for (const bucket of byNoPlural.values()) add("same normalized singular form", bucket);
  for (const bucket of byCompact.values()) add("same compact punctuation-free form", bucket);
  for (const [baseTitle, bucket] of byBaseTitle) {
    const exactBase = nodes.find(node => node.id === baseTitle);
    add("same base title with disambiguator", exactBase ? [exactBase, ...bucket] : bucket);
  }

  for (let i = 0; i < nodes.length; i += 1) {
    for (let j = i + 1; j < nodes.length; j += 1) {
      const a = nodes[i];
      const b = nodes[j];
      const left = describe(a.name), right = describe(b.name);
      if (a.id.length < 4 || b.id.length < 4) continue;
      if (a.id.includes(b.id) || b.id.includes(a.id)) add("one normalized name contains the other", [a, b]);
      if (left.acronymLike && right.acronym === a.id) add("exact acronym-name match", [a, b]);
      if (right.acronymLike && left.acronym === b.id) add("exact acronym-name match", [a, b]);
      if (tokenOverlap(left.tokens, right.tokens) >= 0.8) add("high token overlap", [a, b]);
      if (left.disambiguated
        && right.disambiguated
        && left.baseTitle === right.baseTitle
        && compatibleTitleVariants([a.name, b.name])) {
        add("same base title with compatible disambiguators", [a, b]);
      }
      if (left.disambiguated && left.baseTitle === b.id) add("same base title with disambiguator", [a, b]);
      if (right.disambiguated && right.baseTitle === a.id) add("same base title with disambiguator", [a, b]);
    }
  }

  const values = [...groups.values()];
  return Number.isFinite(limit) ? values.slice(0, limit) : values;
}

module.exports = {
  suggestionKey, targetedDedupeCandidates, dedupeCandidates,
  canonicalFromNames, compatibleTitleVariants, identityConflictReason, looksLikeSpellingVariant,
  acronymFor, isAcronymLike, compactKey, baseTitleKey, hasDisambiguator, tokenOverlapScore
};
