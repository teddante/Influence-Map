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

The graph is saved automatically by the local server and restored when the app starts. By default, data is written to `./data/graph.json`, which is ignored by git.

To store it somewhere else:

```powershell
$env:INFLUENCE_MAP_DATA_FILE="C:\tmp\influence-map.json"
npm start
```

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

Repeated observations for the same directed edge are averaged into one visible graph edge.

## Notes

This prototype treats the LLM as an estimate generator, not a citation engine. The confidence values are useful for exploration and ranking, but they are not verified historical evidence.
