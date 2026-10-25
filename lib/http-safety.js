class HttpError extends Error {
  constructor(status, message) { super(message); this.status = status; }
}

async function readJson(req, maxBytes = 1_000_000) {
  const chunks = await new Promise((resolve, reject) => {
    const parts = [];
    let bytes = 0, oversized = false;
    req.on("data", chunk => {
      bytes += chunk.length;
      if (bytes > maxBytes) {
        if (!oversized) reject(new HttpError(413, "Request body too large"));
        oversized = true;
        parts.length = 0;
      } else if (!oversized) parts.push(chunk);
    });
    req.on("end", () => { if (!oversized) resolve(parts); });
    req.on("error", reject);
    req.on("aborted", () => reject(new HttpError(400, "Request body was interrupted")));
  });
  try {
    const data = JSON.parse(Buffer.concat(chunks).toString("utf8") || "{}");
    if (!data || typeof data !== "object" || Array.isArray(data)) throw new Error();
    return data;
  } catch { throw new HttpError(400, "Expected a JSON object"); }
}

class WorkQueue {
  constructor(concurrency = 1, maxQueued = 8, timeoutMs = 45_000) {
    if (![concurrency, maxQueued, timeoutMs].every(value => Number.isSafeInteger(value) && value > 0)) throw new Error("Invalid queue limits");
    this.concurrency = concurrency; this.maxQueued = maxQueued; this.timeoutMs = timeoutMs;
    this.active = 0; this.waiting = [];
  }
  get queued() { return this.waiting.length; }
  acquire(signal) {
    if (signal?.aborted) return Promise.reject(new HttpError(499, "Request cancelled"));
    if (this.active < this.concurrency) { this.active++; return Promise.resolve(this.slot()); }
    if (this.queued >= this.maxQueued) return Promise.reject(new HttpError(429, "Model queue is full. Try again later."));
    return new Promise((resolve, reject) => {
      const cancel = error => { this.waiting = this.waiting.filter(item => item !== entry); cleanup(); reject(error); };
      const abort = () => cancel(new HttpError(499, "Request cancelled"));
      const cleanup = () => { clearTimeout(timer); signal?.removeEventListener("abort", abort); };
      const entry = () => { cleanup(); this.active++; resolve(this.slot()); };
      const timer = setTimeout(() => cancel(new HttpError(429, "Model queue wait timed out")), this.timeoutMs);
      signal?.addEventListener("abort", abort, { once: true });
      this.waiting.push(entry);
    });
  }
  slot() {
    let released = false;
    return { release: () => {
      if (released) return;
      released = true; this.active--;
      this.waiting.shift()?.();
    } };
  }
}

class GenerationBudget {
  constructor(store, options = {}) {
    this.store = store;
    this.requests = options.requests ?? 100;
    this.tokens = options.tokens ?? 100_000;
    this.inputLimit = options.inputLimit ?? 20_000;
    this.dollars = options.dollars ?? 0;
    this.inputRate = options.inputRate ?? 0;
    this.outputRate = options.outputRate ?? 0;
    if (![this.requests, this.tokens, this.inputLimit].every(value => Number.isSafeInteger(value) && value > 0)
      || ![this.dollars, this.inputRate, this.outputRate].every(value => Number.isFinite(value) && value >= 0)) throw new Error("Invalid generation budget configuration");
  }
  reserve(messages, maxOutputTokens) {
    // UTF-8 bytes plus chat overhead conservatively bound tokenized prompt size.
    // Keep the reservation even on timeout: the provider may still bill the call.
    const input = Buffer.byteLength(JSON.stringify(messages)) + messages.length * 128 + 512;
    if (input > this.inputLimit) throw new HttpError(413, "Model prompt exceeds the configured input budget");
    if (this.dollars && !(this.inputRate > 0 && this.outputRate > 0)) throw new HttpError(503, "Set input/output token prices before enabling the dollar budget");
    const key = `generationBudget:${new Date().toISOString().slice(0, 10)}`;
    const used = JSON.parse(this.store.setting(key, '{"requests":0,"tokens":0,"dollars":0}'));
    const tokens = input + maxOutputTokens;
    const dollars = (input * this.inputRate + maxOutputTokens * this.outputRate) / 1_000_000;
    if (used.requests + 1 > this.requests || used.tokens + tokens > this.tokens || (this.dollars && used.dollars + dollars > this.dollars)) {
      throw new HttpError(429, "Daily generation budget reached");
    }
    this.store.setSetting(key, JSON.stringify({ requests: used.requests + 1, tokens: used.tokens + tokens, dollars: used.dollars + dollars }));
  }
}

module.exports = { HttpError, readJson, WorkQueue, GenerationBudget };
