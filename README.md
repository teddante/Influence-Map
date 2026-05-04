# Influence Map

Influence Map is a tiny prototype for exploring cultural influence as a generated graph. Start from an entity, expand what influenced it and what it influenced, and let repeated observations average into confidence-weighted edges.

## Features

- Minimal Node server with no runtime dependencies
- Static canvas-based graph UI
- DeepSeek native API support via OpenAI-compatible chat completions
- JSON structured output using `response_format: { "type": "json_object" }`
- Mock mode when no API key is configured
- Observation averaging for repeated entity-to-entity relationships
- Frontier auto-expansion with a call budget
- Simple LLM cost estimator
- Server-side graph persistence to a local JSON file
- SQLite graph storage with JSON migration on first start
- Existing-node search with graph focus
- Wheel/trackpad zoom and drag panning
- Deterministic name dedupe for simple variants
- Node size based on weighted observation prominence
- Public search mode with bulk expansion controls hidden by default

## Requirements

- Node.js 18 or newer
- A DeepSeek API key for live generation

## Quick Start

```powershell
npm start
```

Open http://localhost:3000.

Without a configured provider API key, the app runs in mock mode so the interface is usable immediately.

## DeepSeek Setup

Set these environment variables before starting the server:

```powershell
$env:DEEPSEEK_API_KEY="your_key_here"
$env:DEEPSEEK_MODEL="deepseek-v4-flash"
$env:DEEPSEEK_BASE_URL="https://api.deepseek.com"
$env:DEEPSEEK_THINKING="disabled"
$env:PORT="3000"
npm start
```

The browser never receives the API key. It calls the local server at `/api/influences`, and the server calls DeepSeek.
For this graph-building workflow, `DEEPSEEK_THINKING` defaults to `disabled` so the small JSON identity and expansion calls stay fast and cheap. Set it to `enabled` only when you intentionally want slower reasoning-mode calls.

## OpenRouter / Gemini Setup

OpenRouter can be used as the server-side provider for Google's Gemini Flash Lite model:

```powershell
$env:INFLUENCE_MAP_LLM_PROVIDER="openrouter"
$env:OPENROUTER_API_KEY="your_openrouter_key_here"
$env:OPENROUTER_MODEL="google/gemini-3.1-flash-lite-preview"
$env:OPENROUTER_BASE_URL="https://openrouter.ai/api/v1"
$env:OPENROUTER_REASONING="none"
$env:OPENROUTER_JSON_MODE="1"
$env:INFLUENCE_MAP_LLM_CONCURRENCY="1"
$env:INFLUENCE_MAP_LLM_TIMEOUT_MS="45000"
$env:PORT="3000"
npm start
```

The browser still does not receive the API key. `OPENROUTER_REASONING=none` keeps Gemini's thinking mode off for this low-latency JSON workflow. `OPENROUTER_JSON_MODE=1` uses OpenRouter JSON mode, which is faster for this app than strict schema mode while still being parsed and sanitized by the server. Set it to `0` if you want stricter schema enforcement and can accept higher latency.
By default, the server sends no `max_tokens` cap. Set `INFLUENCE_MAP_LLM_MAX_TOKENS` only if you want to force one. `INFLUENCE_MAP_LLM_CONCURRENCY=1` avoids provider-side queue spikes when several expansions are clicked at once.

## Persistence

The graph is saved automatically by the local server and restored when the app starts. By default, SQLite data is written to `./data/influence-map.sqlite`, which is ignored by git. If an older `./data/graph.json` exists and the SQLite database is empty, the server migrates it on first start.

To store it somewhere else:

```powershell
$env:INFLUENCE_MAP_DATA_FILE="C:\tmp\influence-map.json"
$env:INFLUENCE_MAP_DB_FILE="C:\tmp\influence-map.sqlite"
npm start
```

The server also normalizes the saved graph on startup so older duplicate node variants are folded through the current deterministic dedupe rules.

## Dev Tools

Bulk frontier expansion and reset controls are hidden from normal visitors. To enable them for local curation:

```powershell
$env:INFLUENCE_MAP_DEV_TOOLS="1"
$env:INFLUENCE_MAP_ADMIN_TOKEN="choose-a-private-token"
npm start
```

When `INFLUENCE_MAP_ADMIN_TOKEN` is set, open the app once with `?admin=choose-a-private-token` to unlock dev tools in that browser. The token is stored in local storage and sent as `x-admin-token` for admin-only endpoints.

The UI uses whichever server-side provider is configured. Searching for a missing entity requires live generation; if the selected provider has no API key configured, the server returns an error instead of adding generic placeholder relationships.

Dev mode includes an LLM-assisted dedupe review. The server first builds cheap local duplicate candidates, then sends only those candidates to the configured provider for a JSON review. It reports suggested groups but does not merge automatically.

Dev mode also includes a Diagnostics tab. It shows the active provider/model, queue state, database counts, recent successful and failed model calls, token usage, and request timings. Diagnostics are persisted in SQLite in `llm_profiles`, so restarts do not erase the trail. The benchmark button runs a tiny provider check without changing the graph.

For Codex browser automation, this machine can use the bundled Node runtime by setting `NODE_REPL_NODE_PATH` to:

```text
C:\Users\edwar\.cache\codex-runtimes\codex-primary-runtime\dependencies\node\bin\node.exe
```

Restart the Codex desktop app after setting it so the browser automation plugin can pick it up.

## Data Shape

Each model response is expected to be strict JSON:

```json
{
  "entity": "Joy Division",
  "influencedBy": [
    { "entity": "David Bowie", "confidence": 0.82 }
  ],
  "influenced": [
    { "entity": "Interpol", "confidence": 0.88 }
  ]
}
```

The app converts that into directed observations:

```txt
David Bowie -> Joy Division
Joy Division -> Interpol
```

Repeated observations for the same directed edge are averaged into one visible graph edge. Observations store confidence, source expansion entity, provider/model, and timestamp so selected edges can show their underlying history.

Entity names are deduped with a cheap deterministic key: case, punctuation, whitespace, leading `the`, and `&`/`and` variants are normalized before nodes and edges are merged. Popularity is the unbounded weighted number of observations connected to that entity, so prominence emerges from repeated graph evidence rather than another LLM field. Circle size is a relative visual scale based on the current graph's most prominent entity, while the raw popularity score keeps growing.

## Notes

This prototype treats the LLM as an estimate generator, not a citation engine. The confidence values are useful for exploration and ranking, but they are not verified historical evidence.
