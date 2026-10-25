const { WorkQueue, HttpError } = require("./http-safety");
const { normalizeName } = require("../public/graph-model");
const {
  DEEPSEEK_API_KEY, DEEPSEEK_MODEL, DEEPSEEK_BASE_URL, DEEPSEEK_THINKING,
  OPENROUTER_API_KEY, OPENROUTER_MODEL, OPENROUTER_BASE_URL, OPENROUTER_REASONING,
  OPENROUTER_JSON_MODE, OPENROUTER_HTTP_REFERER, OPENROUTER_APP_TITLE,
  DEFAULT_LLM_PROVIDER, LLM_MAX_CONCURRENCY, LLM_MAX_QUEUE, LLM_REQUEST_TIMEOUT_MS, LLM_MAX_TOKENS
} = require("./config");

function providerConfig(providerName = DEFAULT_LLM_PROVIDER) {
  const provider = normalizeName(providerName).toLowerCase();
  if (provider === "mock") return { provider: "mock", apiKey: "", model: "local-demo" };
  if (provider === "openrouter" || provider === "google") {
    if (!OPENROUTER_API_KEY) return { provider: "mock", apiKey: "", model: "local-demo" };
    return {
      provider: "openrouter",
      apiKey: OPENROUTER_API_KEY,
      model: OPENROUTER_MODEL,
      baseUrl: OPENROUTER_BASE_URL.replace(/\/$/, ""),
      endpoint: "/chat/completions"
    };
  }
  if (!DEEPSEEK_API_KEY) return { provider: "mock", apiKey: "", model: "local-demo" };
  return {
    provider: "deepseek",
    apiKey: DEEPSEEK_API_KEY,
    model: DEEPSEEK_MODEL,
    baseUrl: DEEPSEEK_BASE_URL.replace(/\/$/, ""),
    endpoint: "/chat/completions"
  };
}

const objectSchema = properties => ({ type: "object", additionalProperties: false, properties, required: Object.keys(properties) });
const string = { type: "string" };
const number = { type: "number" };
const names = { type: "array", items: string };
const relations = { type: "array", items: objectSchema({ entity: string, confidence: number }) };
const influenceResponseSchema = objectSchema({ entity: string, influencedBy: relations, influenced: relations });
const identityResponseSchema = objectSchema({
  canonicalName: string, displayName: string, disambiguation: string, aliases: names,
  decision: { type: "string", enum: ["same", "rename", "merge", "split", "new", "uncertain"] },
  confidence: number, reason: string
});
const groupProperties = { canonical: string, entities: names, confidence: number, reason: string };
const dedupeResponseSchema = objectSchema({ groups: { type: "array", items: objectSchema(groupProperties) } });
const scopedIdentityResponseSchema = objectSchema({ groups: { type: "array", items: objectSchema({
  action: { type: "string", enum: ["merge", "split", "keep-separate"] }, ...groupProperties
}) } });

function llmRequestBody(config, { messages, temperature, maxTokens, responseSchemaName, responseSchema, jsonModeOnly = OPENROUTER_JSON_MODE }) {
  const body = {
    model: config.model,
    messages,
    temperature,
    response_format: config.provider === "deepseek" || jsonModeOnly || !responseSchema
      ? { type: "json_object" }
      : { type: "json_schema", json_schema: { name: responseSchemaName || "influence_map_response", strict: true, schema: responseSchema } }
  };
  if (config.provider === "deepseek") body.thinking = { type: DEEPSEEK_THINKING === "enabled" ? "enabled" : "disabled" };
  else body.reasoning = { effort: OPENROUTER_REASONING, exclude: true };
  if (Number.isFinite(maxTokens)) body.max_tokens = maxTokens;
  return body;
}

function llmHeaders(config) {
  const headers = {
    "content-type": "application/json",
    "authorization": `Bearer ${config.apiKey}`
  };
  if (config.provider === "openrouter") {
    headers["HTTP-Referer"] = OPENROUTER_HTTP_REFERER;
    headers["X-OpenRouter-Title"] = OPENROUTER_APP_TITLE;
  }
  return headers;
}

function usageSummary(data) {
  const usage = data?.usage || {};
  const details = usage.completion_tokens_details || {};
  return {
    promptTokens: usage.prompt_tokens || 0,
    completionTokens: usage.completion_tokens || 0,
    totalTokens: usage.total_tokens || 0,
    reasoningTokens: details.reasoning_tokens || 0
  };
}

function elapsedMs(start) {
  return Math.round(Number(process.hrtime.bigint() - start) / 1_000_000);
}

function createLlmClient(budget, requestContext) {
  const llmQueue = new WorkQueue(LLM_MAX_CONCURRENCY, LLM_MAX_QUEUE, LLM_REQUEST_TIMEOUT_MS);
  async function acquireLlmSlot() {
    const started = process.hrtime.bigint();
    const slot = await llmQueue.acquire(requestContext.getStore()?.signal);
    return { ...slot, queueMs: elapsedMs(started) };
  }

  async function callChatCompletion(config, prompt, options) {
    const slot = await acquireLlmSlot();
    const started = process.hrtime.bigint();
    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), LLM_REQUEST_TIMEOUT_MS);
    const signal = AbortSignal.any([controller.signal, ...(requestContext.getStore()?.signal ? [requestContext.getStore().signal] : [])]);
    try {
      budget.reserve(prompt.messages, LLM_MAX_TOKENS);
      const response = await fetch(`${config.baseUrl}${config.endpoint}`, {
        method: "POST",
        headers: llmHeaders(config),
        signal,
        body: JSON.stringify(llmRequestBody(config, { ...options, messages: prompt.messages }))
      });

      if (!response.ok) {
        await response.body?.cancel();
        throw new HttpError(502, `${config.provider} request failed (HTTP ${response.status})`);
      }

      const data = await response.json();
      const content = data.choices?.[0]?.message?.content;
      return {
        data,
        content,
        usage: usageSummary(data),
        finishReason: data.choices?.[0]?.finish_reason,
        ms: elapsedMs(started),
        queueMs: slot.queueMs,
        id: data.id || ""
      };
    } catch (error) {
      if (error && error.name === "AbortError") {
        throw new Error(`${config.provider} request timed out after ${Math.round(LLM_REQUEST_TIMEOUT_MS / 1000)}s`);
      }
      throw error;
    } finally {
      clearTimeout(timeout);
      slot.release();
    }
  }
  return { callChatCompletion, llmQueue };
}

module.exports = {
  providerConfig, createLlmClient, elapsedMs, influenceResponseSchema,
  identityResponseSchema, dedupeResponseSchema, scopedIdentityResponseSchema
};
