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

Without `DEEPSEEK_API_KEY`, the app runs in mock mode so the interface is usable immediately.

## DeepSeek Setup

Set these environment variables before starting the server:

```powershell
$env:DEEPSEEK_API_KEY="your_key_here"
$env:DEEPSEEK_MODEL="deepseek-v4-flash"
$env:DEEPSEEK_BASE_URL="https://api.deepseek.com"
$env:PORT="3000"
npm start
```

The browser never receives the API key. It calls the local server at `/api/influences`, and the server calls DeepSeek.

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

The UI always requests DeepSeek for generation. Searching for a missing entity requires live generation; if no `DEEPSEEK_API_KEY` is configured, the server returns an error instead of adding generic placeholder relationships.

Dev mode includes an LLM-assisted dedupe review. The server first builds cheap local duplicate candidates, then sends only those candidates to DeepSeek for a JSON review. It reports suggested groups but does not merge automatically.

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
