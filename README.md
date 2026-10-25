# Influence Map

A local tool for exploring proposed cultural influences as a directed graph. Arrows point from an influencer to something influenced. The graph retains individual model samples, their scores, and available provenance.

Model scores are uncalibrated judgments. Repeating a model call measures consistency, and prominence reflects this map's sampling history. Upstream paths and rankings are heuristics whose results depend on coverage, depth limits and cycles. They do not establish historical causation. Source links and evidence type are stored separately; missing provenance stays `unknown`.

## Run

Use **Node.js 24.x** (tested with 24.15.0). There are no runtime dependencies or build step; the server serves the browser files directly.

```powershell
npm start
```

The server binds to `127.0.0.1:3000` by default. Open the private write-access URL printed in its terminal to enable generation in that browser. Without a configured token, the server creates a new private token for each process; use its new link after restarting.

To keep a stable token, set `INFLUENCE_MAP_ADMIN_TOKEN` in your shell or `.env.local`, then open `http://127.0.0.1:3000/?admin=YOUR_TOKEN` once. The browser stores the token locally and removes it from the address bar. It sends the token in `x-admin-token`; API keys stay on the server. Environment files use Node's native dotenv parser, including quotes and comments. Existing shell variables take precedence; `.env` loads first and `.env.local` fills missing values.

Anonymous visitors can browse and focus existing entities. All graph mutations and model calls require private write access. `HOST` and `PORT` configure the listening address and port; choose network access deliberately if changing `HOST`.

## Mock and live generation

With no provider key, the selected provider falls back to **mock mode**. `INFLUENCE_MAP_LLM_PROVIDER=mock` selects it explicitly, even when live keys exist. Mock mode never contacts a provider. It provides demo neighborhoods for DayZ, Joy Division and David Bowie; unknown entities return no demo links and are marked sampled. Demo samples are labeled separately from live model samples.

For DeepSeek:

```powershell
$env:INFLUENCE_MAP_LLM_PROVIDER="deepseek"
$env:DEEPSEEK_API_KEY="your_key_here"
$env:DEEPSEEK_MODEL="deepseek-v4-flash"
$env:DEEPSEEK_BASE_URL="https://api.deepseek.com"
$env:DEEPSEEK_THINKING="disabled"
npm start
```

For OpenRouter:

```powershell
$env:INFLUENCE_MAP_LLM_PROVIDER="openrouter"
$env:OPENROUTER_API_KEY="your_key_here"
$env:OPENROUTER_MODEL="google/gemini-3.1-flash-lite-preview"
$env:OPENROUTER_BASE_URL="https://openrouter.ai/api/v1"
$env:OPENROUTER_REASONING="none"
$env:OPENROUTER_JSON_MODE="1"
npm start
```

These are the app's existing model defaults, not a guarantee of provider availability. Configure a model available on your account. JSON mode is sanitized server-side; `OPENROUTER_JSON_MODE=0` requests a strict response schema instead.

Generation may involve an identity call, an influence call and a scoped identity-review call. A successfully generated neighborhood is committed before ancillary identity review, so that review failing cannot discard the samples.

## Limits and billing controls

The defaults are:

| Setting | Default | Meaning |
| --- | ---: | --- |
| `INFLUENCE_MAP_MUTATIONS_PER_MINUTE` | 30 | Authenticated mutations per client address |
| `INFLUENCE_MAP_LLM_CONCURRENCY` | 1 | Active provider calls |
| `INFLUENCE_MAP_LLM_MAX_QUEUE` | 8 | Maximum waiting provider calls |
| `INFLUENCE_MAP_LLM_TIMEOUT_MS` | 45000 | Provider timeout and maximum queue wait |
| `INFLUENCE_MAP_LLM_MAX_TOKENS` | 2048 | Output cap per call, constrained to 128–8192 |
| `INFLUENCE_MAP_LLM_DAILY_REQUESTS` | 100 | Daily provider-call cap |
| `INFLUENCE_MAP_LLM_DAILY_TOKENS` | 100000 | Daily reserved input/output token cap |

Daily reservations persist in SQLite and use UTC days. Input reservations conservatively use UTF-8 prompt bytes plus message overhead; prompts above the 20,000-byte reservation limit are rejected. Reservations are retained after failures or cancellation because a provider may still bill the call. Queue capacity, deadlines and disconnect cancellation prevent unlimited waiting requests. One expansion may consume several provider-call reservations.

An optional currency budget uses `INFLUENCE_MAP_LLM_DAILY_USD`, `INFLUENCE_MAP_LLM_INPUT_USD_PER_MILLION` and `INFLUENCE_MAP_LLM_OUTPUT_USD_PER_MILLION`. A positive currency budget requires positive input/output prices. Supply conservative prices for the configured model, including applicable reasoning-token billing. This is a reservation estimate based on your configured rates; provider invoices remain authoritative. Currency limiting is off by default, while request and token caps are always enforced.

## Graph persistence and identity

The server commits observations incrementally. Each entity has a permanent `uid`; its legacy normalized `id` is a name lookup key and can change on rename. Redundant UIDs retired by a merge retain durable redirects to the surviving identity. Observation IDs remain stable. References include `fromUid` and `toUid`. Expansion request IDs make retries idempotent.

Browsers receive revisioned changes and cannot replace the graph. Stale destructive edits receive HTTP 409 and require a refresh. Pan, zoom (0.04–8), score filters and node layouts are stored only in that browser. Interacting with a view cannot erase another visitor's graph additions.

The score filter applies **after** each directed edge is averaged across its complete sample history. Filtering never removes samples from that mean or from upstream analysis. Zero is a valid score.

SQLite defaults to `data/influence-map.sqlite`. Use `INFLUENCE_MAP_DB_FILE` to change it. An existing `data/graph.json` is imported once into a new empty database, preserving available sample metadata; `INFLUENCE_MAP_DATA_FILE` changes that legacy input. A persistent migration marker prevents reset graphs from importing the old file again.

A verified SQLite backup is made in the database's `backups/` directory before startup schema changes. Keep the initial source/data backup when upgrading this prototype. Backups are retained until you remove them deliberately.

## Identity review and undo

Set `INFLUENCE_MAP_DEV_TOOLS=1`, then open the app with private access to enable Identity, Diagnostics, maintenance and batch-expansion controls.

Automatic identity application is **off by default**. `INFLUENCE_MAP_IDENTITY_AUTO_APPROVE=1` enables gated application of model decisions. Local spelling/title heuristics remain review candidates. Review entity senses carefully: shared names, franchises, adaptations and individual works can refer to different things.

Manual approval implements merges, renames and splits. A split lists the source node's current samples and requires exact sample IDs to move. Scoped model split suggestions remain pending until this information is reviewed. Renames retain the entity UID and sample IDs; the historical expansion source wording remains unchanged.

Identity edits, reset and orphan cleanup retain an audit record and an undo snapshot. Open **Identity → Edit history and undo**. Only the latest edit can be undone while its resulting graph revision is still current; newer samples are protected from being overwritten. Samples that collapse into self-relations during a merge remain recoverable in that edit's snapshot. Restore an older verified database backup separately when a later snapshot cannot safely be undone. Stop the app before replacing a database.

The known legacy Rust game/language mix-up has a bounded repair command:

```powershell
node scripts/correct-rust-identity.js --db "C:\path\influence-map.sqlite" --legacy "C:\path\graph.json" --apply
```

Stop the app first. The command verifies a backup, matches the three specific original DayZ → bare Rust samples, and moves only those samples to `Rust (video game)`. It preserves IDs, scores, timestamps and unknown provenance; it does not verify the influence claim itself. Official identity references are [Facepunch's game page](https://facepunch.com/games/rust) and the [Rust language website](https://rust-lang.org/). The correction is audited, can be undone before later graph changes, and will not silently reapply after an undo.

## Navigation and diagnostics

Search focuses a unique existing entity. Ambiguous matches show explicit choices. With private access, a missing entity can generate a neighborhood. Table view supports filtering, sorting and graph focus. Drag to pan or reposition nodes, scroll to zoom, and use Fit or Center to navigate.

Next-expansion and upstream analysis run in a Web Worker. Connectivity partitions are calculated in one traversal, independent of the drawing layout. The main thread keeps rendering and accepting input while analysis runs. Development diagnostics retain recent model call timing and usage; they do not constitute a full billing ledger.

## API changes

- `GET /api/graph` returns a snapshot; `GET /api/graph?since=REVISION` returns ordered deltas, with a snapshot fallback for an unavailable revision.
- `POST /api/influences` accepts `entity`, `revision` and a unique `requestId`; the server persists before replying.
- Whole-graph `PUT /api/graph` is removed (405 for authenticated clients).
- `POST /api/reset`, identity approval and undo require the current `revision`.
- `GET /api/history` and `POST /api/history/ID/undo` require authenticated development access.
- Mutations require `x-admin-token`. Cross-origin mutations are rejected. Static responses use a restrictive script policy, and untrusted UI text is escaped.

## Project layout

| Path | Responsibility |
| --- | --- |
| `server.js` | HTTP routes, generation workflow and identity review |
| `lib/config.js` | Environment loading and server/provider limits |
| `lib/llm-client.js` | Provider protocol, bounded queue and budget reservations |
| `lib/graph-store.js` and `lib/schema.js` | SQLite persistence, revisions, migrations and undo |
| `lib/identity.js` | Pure name comparisons and identity candidate discovery |
| `public/graph-model.js` | Shared name keys and complete-history edge/statistics aggregation |
| `public/app.js` | Browser interaction, view state and canvas rendering |
| `public/analysis-worker.js` and `public/graph-analysis.js` | Background rankings and upstream traversal |
| `test/` | Isolated storage, HTTP, configuration, browser-model and worker regression checks |

The server, browser and worker share the graph model without a bundler. Browser statistics are cached until the graph changes; score filters partition the existing edges. SQLite statements are reused per store connection. Runtime data, secrets and server logs stay outside version control.

## Validation

```powershell
npm test
```

The suite uses isolated databases, child servers, a loopback provider stub, browser-model tests and the actual analysis worker. It makes **no paid provider calls**. It covers persistence/restart, stable IDs, idempotency, revision conflicts, merge/split/rename and undo, authorization, malformed requests, queue/rate/budget bounds, HTML escaping, aggregation, zero scores, view storage and rendering invalidation.

Browser smoke checks should also cover Graph/Table visibility, ambiguous search, score filters, zoom, upstream highlighting, identity history and a mock expansion at the saved dataset size. For the ranking performance gate, run three cold Next-panel loads and inspect `#next-output`'s `data-analysis-submit-ms` and `data-analysis-max-task-ms`. A supported Long Tasks observer must show no ranking-related task above 100 ms; `0.00` means no observed task reached the observer's 50 ms reporting threshold. These measurements do not establish the factual accuracy of the generated graph.
