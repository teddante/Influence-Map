// This repairs identity only. It does not validate the original influence scores.
// The legacy file contains exactly these three DayZ -> bare "Rust" samples.
// Official identity references distinguish the survival game from the language.
const identityReferences = ["https://facepunch.com/games/rust", "https://rust-lang.org/"];

function correctRustIdentity(store, legacy) {
  if (store.setting("rustIdentityCorrection")) return { changed: false, reason: "Correction already recorded (including any later undo)" };
  const language = store.node("Rust (programming language)");
  if (!language) return { changed: false, reason: "No contaminated language entity found" };
  const original = (legacy.observations || []).filter(sample => sample.from === "DayZ" && sample.to === "Rust");
  if (original.length !== 3 || JSON.stringify(original.map(sample => sample.confidence).sort()) !== "[0.85,0.9,0.9]") throw new Error("Legacy Rust evidence differs; review instead of guessing");
  const samples = store.db.prepare(`SELECT o.* FROM observations o JOIN nodes source ON source.id=o.from_id
    WHERE o.to_id=? AND source.name='DayZ' AND COALESCE(o.provider,'')='' AND COALESCE(o.source_entity,'')='' ORDER BY o.id`).all(language.id);
  if (samples.length !== 3 || JSON.stringify(samples.map(sample => sample.confidence).sort()) !== "[0.85,0.9,0.9]") throw new Error("Current Rust samples differ from the legacy evidence; review required");
  const correction = store.edit({ action: "split", canonical: "Rust (video game)", entities: [language.name, "Rust (video game)"],
    reason: "Restore the game identity of the three legacy DayZ → bare Rust samples. Preserve their scores and unknown influence provenance; official references establish the two different Rust identities.",
    identityReferences, metadata: { moveObservationIds: samples.map(sample => sample.id), legacySamples: original, relationshipVerified: false }
  }, store.revision);
  store.setSetting("rustIdentityCorrection", correction.eventId);
  return { changed: true, ...correction };
}

module.exports = { correctRustIdentity, identityReferences };
