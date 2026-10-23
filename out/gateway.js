"use strict";
/*
 * Local gateway speaking the OpenAI *Responses* API to Cursor.
 *
 * Cursor's private-inference path only speaks Responses
 *   (daemon.cjs: api_backend must be "responses"; a gateway that advertises
 *    "chat_completions" is rejected by resolvePrivateInferenceModel).
 *
 *   Cursor  --Responses-->  gateway  --+--> upstream "responses"  (pass-through)
 *                                      +--> upstream "chat"       (Chat Completions)
 *                                      +--> upstream "messages"   (Anthropic Messages)
 *
 * Every upstream adapter drives one ResponsesEmitter, which produces either a single
 * Responses JSON object or a Responses SSE stream, depending on what Cursor asked for —
 * independent of whether the upstream itself streams (endpoint.streaming === false).
 *
 * Loopback only: the gateway spends the upstream credentials you configure.
 */

const http = require("node:http");
const crypto = require("node:crypto");
const fs = require("node:fs");
const path = require("node:path");
const { EventEmitter } = require("node:events");
const { Readable } = require("node:stream");

/** Cursor capability probe. Served as-is on GET /v1/models. Shipped inside the extension. */
const DEFAULT_CATALOG = path.join(__dirname, "models.json");

/** GET /v1/models reuses the last discovery attempt for one minute. */
const DEFAULT_MODELS_TTL_MS = 60 * 1000;
const UPSTREAM_MODELS_TIMEOUT_MS = 5 * 1000;
const SUPPORTED_API_TYPES = new Set(["responses", "openai_responses", "chat_completions", "openai_chat", "anthropic_messages"]);

const ANTHROPIC_VERSION = "2023-06-01";
const DEFAULT_TIMEOUT_MS = 15 * 60 * 1000;
const EFFORT_BUDGET = { minimal: 1024, low: 2048, medium: 8192, high: 24576, xhigh: 32768 };

/*
 * Cursor's context summarizer (local-agent runtime) sends this system prompt and wraps
 * its instructions in <summarization_request>. It reuses the chat model's reasoning
 * effort, so a thinking model spends ~30k tokens (~5 min) thinking per summary while the
 * turn is blocked on "Summarizing chat context"; the wait gets aborted and the summary
 * is discarded. Summaries are sent without thinking and with a capped output instead.
 */
const SUMMARIZER_SYSTEM_PREFIX = "You are an intelligent assistant, tasked with summarizing the following conversation";
const SUMMARIZATION_MARKER = "<summarization_request>";
const SUMMARY_MAX_TOKENS = 32000;

function rid(prefix) {
  return `${prefix}_${crypto.randomBytes(12).toString("hex")}`;
}

function readBody(req) {
  return new Promise((resolve, reject) => {
    const chunks = [];
    req.on("data", (c) => chunks.push(c));
    req.on("end", () => resolve(Buffer.concat(chunks).toString("utf8")));
    req.on("error", reject);
  });
}

function sendJson(res, code, obj) {
  const body = JSON.stringify(obj);
  res.writeHead(code, { "content-type": "application/json", "content-length": Buffer.byteLength(body) });
  res.end(body);
}

function joinUrl(base, suffix) {
  const b = String(base).replace(/\/+$/, "");
  return b.endsWith(suffix) ? b : b + suffix;
}

function safeJsonParse(s, fallback) {
  try {
    return JSON.parse(s);
  } catch {
    return fallback;
  }
}

/**
 * Yield {event, data} frames from an SSE body (ReadableStream of a fetch Response).
 * Chunks can split mid-line or mid-UTF8 sequence, so we buffer until a blank line
 * (\n\n or \r\n\r\n) marks the end of a frame.
 */
async function* sseFrames(body) {
  const decoder = new TextDecoder();
  const reader = body.getReader();
  let buf = "";
  for (;;) {
    const { done, value } = await reader.read();
    if (done) break;
    buf += decoder.decode(value, { stream: true });
    let m;
    while ((m = /\r?\n\r?\n/.exec(buf))) {
      const frame = buf.slice(0, m.index);
      buf = buf.slice(m.index + m[0].length);
      let event = "message";
      const data = [];
      for (const line of frame.split(/\r?\n/)) {
        if (line.startsWith("event:")) event = line.slice(6).trim();
        // Per the SSE spec, strip at most one leading space after "data:" so significant
        // indentation inside the payload is preserved.
        else if (line.startsWith("data:")) data.push(line.slice(5).replace(/^ /, ""));
      }
      if (data.length) yield { event, data: data.join("\n") };
    }
  }
}

/* ======================================================================== *
 * ResponsesEmitter — the single place that knows the Responses wire format.
 *
 * Bridges both streaming (SSE) and non-streaming callers through one state
 * machine. Only one output item (message, reasoning, or tool call) is open
 * at a time; starting a new item automatically closes and finalizes the
 * previous one so event ordering stays valid.
 * ======================================================================== */

class ResponsesEmitter {
  constructor(res, model, stream) {
    this.res = res;
    this.model = model;
    this.stream = stream;
    this.id = rid("resp");
    this.createdAt = Math.floor(Date.now() / 1000);
    this.seq = 0;
    this.output = [];
    this.open = undefined; // {kind, item, index, text}
    this.started = false;
    this.finished = false;
    this.usage = undefined;
  }

  snapshot(status) {
    return {
      id: this.id,
      object: "response",
      created_at: this.createdAt,
      status,
      model: this.model,
      output: status === "in_progress" ? [] : this.output,
      usage: this.usage
    };
  }

  ev(type, payload) {
    if (!this.stream) return;
    this.res.write(`event: ${type}\ndata: ${JSON.stringify({ type, sequence_number: this.seq++, ...payload })}\n\n`);
  }

  start() {
    if (this.started) return;
    this.started = true;
    if (this.stream) {
      this.res.writeHead(200, { "content-type": "text/event-stream", "cache-control": "no-cache", connection: "keep-alive" });
      this.ev("response.created", { response: this.snapshot("in_progress") });
      this.ev("response.in_progress", { response: this.snapshot("in_progress") });
    }
  }

  openItem(kind, item) {
    this.close();
    this.start();
    const index = this.output.length;
    this.output.push(item);
    this.open = { kind, item, index, text: "" };
    this.ev("response.output_item.added", { output_index: index, item: { ...item } });
    return this.open;
  }

  text(delta) {
    if (!delta) return;
    let o = this.open;
    if (!o || o.kind !== "message") {
      o = this.openItem("message", { id: rid("msg"), type: "message", role: "assistant", status: "in_progress", content: [] });
      this.ev("response.content_part.added", {
        item_id: o.item.id,
        output_index: o.index,
        content_index: 0,
        part: { type: "output_text", text: "", annotations: [] }
      });
    }
    o.text += delta;
    this.ev("response.output_text.delta", { item_id: o.item.id, output_index: o.index, content_index: 0, delta });
  }

  reasoning(delta) {
    if (!delta) return;
    let o = this.open;
    if (!o || o.kind !== "reasoning") {
      o = this.openItem("reasoning", { id: rid("rs"), type: "reasoning", summary: [] });
      this.ev("response.reasoning_summary_part.added", {
        item_id: o.item.id,
        output_index: o.index,
        summary_index: 0,
        part: { type: "summary_text", text: "" }
      });
    }
    o.text += delta;
    this.ev("response.reasoning_summary_text.delta", { item_id: o.item.id, output_index: o.index, summary_index: 0, delta });
  }

  /**
   * Custom (freeform) tools: the upstream sees a function with one string argument
   * `input`; Cursor must get back a `custom_tool_call` item whose `input` is that string.
   * The arguments are buffered and the whole item is emitted on close().
   */
  customToolStart(callId, name) {
    this.close();
    this.pendingCustom = { callId: callId || rid("call"), name, buf: "" };
  }

  toolStart(callId, name) {
    this.openItem("function_call", {
      id: rid("fc"),
      type: "function_call",
      call_id: callId || rid("call"),
      name,
      arguments: "",
      status: "in_progress"
    });
  }

  toolArgs(delta) {
    if (this.pendingCustom) {
      this.pendingCustom.buf += delta || "";
      return;
    }
    const o = this.open;
    if (!o || o.kind !== "function_call" || !delta) return;
    o.text += delta;
    this.ev("response.function_call_arguments.delta", { item_id: o.item.id, output_index: o.index, delta });
  }

  /** Emit a complete tool call at once (used when the upstream does not stream). */
  toolCall(callId, name, args, custom = false) {
    if (custom) this.customToolStart(callId, name);
    else this.toolStart(callId, name);
    this.toolArgs(typeof args === "string" ? args : JSON.stringify(args ?? {}));
    this.close();
  }

  flushCustom() {
    const p = this.pendingCustom;
    if (!p) return;
    this.pendingCustom = undefined;
    // Unwrap the synthetic {"input": "..."} wrapper we asked the upstream to produce;
    // if the model emitted raw text instead of JSON, fall back to the raw buffer.
    const parsed = safeJsonParse(p.buf || "{}", undefined);
    const input = parsed && typeof parsed === "object" && typeof parsed.input === "string" ? parsed.input : p.buf;
    const o = this.openItem("custom_tool_call", {
      id: rid("ctc"),
      type: "custom_tool_call",
      call_id: p.callId,
      name: p.name,
      input: "",
      status: "in_progress"
    });
    o.text = input;
    if (input) this.ev("response.custom_tool_call_input.delta", { item_id: o.item.id, output_index: o.index, delta: input });
    this.close();
  }

  close() {
    if (this.pendingCustom) return this.flushCustom();
    const o = this.open;
    if (!o) return;
    this.open = undefined;
    const { item, index } = o;
    if (o.kind === "message") {
      item.content = [{ type: "output_text", text: o.text, annotations: [] }];
      item.status = "completed";
      this.ev("response.output_text.done", { item_id: item.id, output_index: index, content_index: 0, text: o.text });
      this.ev("response.content_part.done", { item_id: item.id, output_index: index, content_index: 0, part: item.content[0] });
    } else if (o.kind === "reasoning") {
      item.summary = [{ type: "summary_text", text: o.text }];
      this.ev("response.reasoning_summary_text.done", { item_id: item.id, output_index: index, summary_index: 0, text: o.text });
      this.ev("response.reasoning_summary_part.done", { item_id: item.id, output_index: index, summary_index: 0, part: item.summary[0] });
    } else if (o.kind === "function_call") {
      item.arguments = o.text || "{}";
      item.status = "completed";
      this.ev("response.function_call_arguments.done", { item_id: item.id, output_index: index, arguments: item.arguments });
    } else if (o.kind === "custom_tool_call") {
      item.input = o.text || "";
      item.status = "completed";
      this.ev("response.custom_tool_call_input.done", { item_id: item.id, output_index: index, input: item.input });
    }
    this.ev("response.output_item.done", { output_index: index, item: { ...item } });
  }

  setUsage(input, output) {
    if (input === undefined && output === undefined) return;
    const i = input ?? this.usage?.input_tokens ?? 0;
    const o = output ?? this.usage?.output_tokens ?? 0;
    this.usage = { input_tokens: i, output_tokens: o, total_tokens: i + o };
  }

  /**
   * @param {string} [incompleteReason] Responses incomplete_details.reason.
   *   "max_output_tokens" is what the local agent runtime maps to finishReason
   *   "length", which continues the turn. A plain "completed" with no output is
   *   treated as a successful empty reply and the turn stops with no error.
   */
  finish(incompleteReason) {
    if (this.finished) return;
    this.close();
    this.finished = true;
    const status = incompleteReason ? "incomplete" : "completed";
    const snap = this.snapshot(status);
    if (incompleteReason) snap.incomplete_details = { reason: incompleteReason };
    if (this.stream) {
      this.start();
      this.ev(incompleteReason ? "response.incomplete" : "response.completed", { response: snap });
      this.res.end();
    } else {
      sendJson(this.res, 200, snap);
    }
  }

  fail(status, message) {
    if (this.finished) return;
    this.finished = true;
    if (!this.stream || !this.started) {
      if (!this.res.headersSent) return sendJson(this.res, status, { error: { message } });
      return this.res.end();
    }
    this.close();
    this.ev("response.failed", { response: { ...this.snapshot("failed"), error: { code: "upstream_error", message } } });
    this.res.end();
  }
}

/* ======================================================================== *
 * Request translation: Responses -> Chat Completions
 * ======================================================================== */

function partText(p) {
  if (typeof p === "string") return p;
  if (!p || typeof p !== "object") return "";
  return p.text ?? p.input_text ?? p.output_text ?? "";
}

function imageUrlOf(p) {
  if (!p || p.type !== "input_image") return undefined;
  return typeof p.image_url === "string" ? p.image_url : p.image_url?.url;
}

function toChatContent(content) {
  if (typeof content === "string") return content;
  if (!Array.isArray(content)) return "";
  const hasImage = content.some((p) => imageUrlOf(p));
  if (!hasImage) return content.map(partText).filter(Boolean).join("\n");
  return content
    .map((p) => {
      const url = imageUrlOf(p);
      if (url) return { type: "image_url", image_url: { url } };
      const t = partText(p);
      return t ? { type: "text", text: t } : undefined;
    })
    .filter(Boolean);
}

function inputItems(body) {
  return typeof body.input === "string" ? [{ type: "message", role: "user", content: body.input }] : body.input || [];
}

function responsesToChatMessages(body) {
  const messages = [];
  if (typeof body.instructions === "string" && body.instructions.trim()) {
    messages.push({ role: "system", content: body.instructions });
  }
  for (const item of inputItems(body)) {
    if (!item || typeof item !== "object") continue;
    const type = item.type ?? (item.role ? "message" : undefined);
    if (type === "message") {
      // Many OpenAI-compatible servers reject the "developer" role; map it to "system".
      const role = item.role === "developer" ? "system" : item.role || "user";
      messages.push({ role, content: toChatContent(item.content) });
    } else if (type === "function_call" || type === "custom_tool_call") {
      const args = type === "custom_tool_call" ? JSON.stringify({ input: item.input ?? "" }) : item.arguments || "{}";
      const call = { id: item.call_id || item.id, type: "function", function: { name: item.name, arguments: args } };
      // Responses lists parallel tool calls as separate top-level items, whereas Chat
      // Completions expects them grouped onto a single assistant message's tool_calls array.
      const last = messages[messages.length - 1];
      if (last && last.role === "assistant" && !last.content) (last.tool_calls ||= []).push(call);
      else messages.push({ role: "assistant", content: null, tool_calls: [call] });
    } else if (type === "function_call_output" || type === "custom_tool_call_output") {
      messages.push({
        role: "tool",
        tool_call_id: item.call_id,
        content: typeof item.output === "string" ? item.output : JSON.stringify(item.output)
      });
    }
    // reasoning items are dropped: chat upstreams cannot take them back.
  }
  return messages;
}

const CUSTOM_INPUT_SCHEMA = {
  type: "object",
  properties: { input: { type: "string", description: "The raw input for this tool." } },
  required: ["input"]
};

/** Function tools as-is; custom (freeform) tools become a function with one string argument. */
function functionTools(tools) {
  const out = [];
  for (const t of Array.isArray(tools) ? tools : []) {
    if (!t || !t.name) continue;
    if (t.type === "function") out.push(t);
    else if (t.type === "custom") {
      const fmt = t.format && t.format.type === "grammar" ? `\nThe input must follow this ${t.format.syntax || "grammar"}:\n${t.format.definition}` : "";
      out.push({ type: "function", name: t.name, description: `${t.description || ""}${fmt}`, parameters: CUSTOM_INPUT_SCHEMA });
    }
    // provider-defined tools (web_search_preview, …) cannot be forwarded: dropped.
  }
  return out;
}

function customToolNames(tools) {
  return new Set((Array.isArray(tools) ? tools : []).filter((t) => t && t.type === "custom" && t.name).map((t) => t.name));
}

function toChatTools(tools) {
  const fns = functionTools(tools);
  return fns.length
    ? fns.map((t) => ({
        type: "function",
        function: { name: t.name, description: t.description, parameters: t.parameters || { type: "object", properties: {} } }
      }))
    : undefined;
}

function toChatToolChoice(tc) {
  if (!tc) return undefined;
  if (typeof tc === "string") return tc; // auto | none | required
  if (tc.type === "function" && tc.name) return { type: "function", function: { name: tc.name } };
  return undefined;
}

/* ======================================================================== *
 * Request translation: Responses -> Anthropic Messages
 * ======================================================================== */

function toAnthropicImage(url) {
  const m = /^data:([^;]+);base64,(.*)$/s.exec(url);
  if (m) return { type: "image", source: { type: "base64", media_type: m[1], data: m[2] } };
  return { type: "image", source: { type: "url", url } };
}

function toAnthropicBlocks(content) {
  if (typeof content === "string") return content ? [{ type: "text", text: content }] : [];
  if (!Array.isArray(content)) return [];
  const blocks = [];
  for (const p of content) {
    const url = imageUrlOf(p);
    if (url) blocks.push(toAnthropicImage(url));
    else {
      const t = partText(p);
      if (t) blocks.push({ type: "text", text: t });
    }
  }
  return blocks;
}

function responsesToAnthropic(body) {
  const system = [];
  if (typeof body.instructions === "string" && body.instructions.trim()) system.push(body.instructions);
  const messages = [];
  // Anthropic requires strictly alternating user/assistant turns; merge adjacent
  // items that map to the same role (e.g. multiple tool_result blocks in a row).
  const push = (role, blocks) => {
    if (!blocks.length) return;
    const last = messages[messages.length - 1];
    if (last && last.role === role) last.content.push(...blocks);
    else messages.push({ role, content: [...blocks] });
  };
  for (const item of inputItems(body)) {
    if (!item || typeof item !== "object") continue;
    const type = item.type ?? (item.role ? "message" : undefined);
    if (type === "message") {
      if (item.role === "system" || item.role === "developer") {
        const t = toAnthropicBlocks(item.content).filter((b) => b.type === "text").map((b) => b.text).join("\n");
        if (t) system.push(t);
      } else push(item.role === "assistant" ? "assistant" : "user", toAnthropicBlocks(item.content));
    } else if (type === "function_call" || type === "custom_tool_call") {
      const input = type === "custom_tool_call" ? { input: item.input ?? "" } : safeJsonParse(item.arguments || "{}", {});
      push("assistant", [{ type: "tool_use", id: item.call_id || item.id, name: item.name, input }]);
    } else if (type === "function_call_output" || type === "custom_tool_call_output") {
      push("user", [
        {
          type: "tool_result",
          tool_use_id: item.call_id,
          content: typeof item.output === "string" ? item.output : JSON.stringify(item.output)
        }
      ]);
    }
  }
  return { system: system.join("\n\n") || undefined, messages };
}

function toAnthropicTools(tools) {
  const fns = functionTools(tools);
  return fns.length
    ? fns.map((t) => ({ name: t.name, description: t.description, input_schema: t.parameters || { type: "object", properties: {} } }))
    : undefined;
}

function toAnthropicToolChoice(tc) {
  if (!tc) return undefined;
  if (tc === "auto") return { type: "auto" };
  if (tc === "required") return { type: "any" };
  if (tc === "none") return { type: "none" };
  if (tc.type === "function" && tc.name) return { type: "tool", name: tc.name };
  return undefined;
}

/* ======================================================================== *
 * Summarization requests
 * ======================================================================== */

function blocksText(content) {
  if (typeof content === "string") return content;
  if (!Array.isArray(content)) return "";
  return content.map(partText).join("");
}

function isSummarization(systemText, lastUserText) {
  // Require both the summarizer system prompt AND the <summarization_request> tag in the
  // last user message so ordinary turns that happen to discuss <summarization_request>
  // in code or docs still keep their normal thinking budget.
  return String(systemText || "").trimStart().startsWith(SUMMARIZER_SYSTEM_PREFIX) && String(lastUserText || "").includes(SUMMARIZATION_MARKER);
}

function capTokens(v, cap) {
  return typeof v === "number" && v > 0 ? Math.min(v, cap) : cap;
}

/** Anthropic Messages body. Returns a lean copy, or undefined when it is not a summary. */
function leanAnthropicSummary(body, cap) {
  const last = [...(body.messages || [])].reverse().find((m) => m && m.role === "user");
  if (!isSummarization(blocksText(body.system), last && blocksText(last.content))) return undefined;
  const out = { ...body, max_tokens: capTokens(body.max_tokens, cap) };
  delete out.thinking;
  if (out.output_config && typeof out.output_config === "object") {
    const { effort, ...rest } = out.output_config;
    if (Object.keys(rest).length) out.output_config = rest;
    else delete out.output_config;
  }
  return out;
}

/** Chat Completions body. */
function leanChatSummary(body, cap) {
  const msgs = body.messages || [];
  const sys = msgs.filter((m) => m && (m.role === "system" || m.role === "developer")).map((m) => blocksText(m.content)).join("\n");
  const last = [...msgs].reverse().find((m) => m && m.role === "user");
  if (!isSummarization(sys, last && blocksText(last.content))) return undefined;
  const out = { ...body };
  delete out.reasoning_effort;
  delete out.reasoning;
  delete out.thinking;
  if (out.max_completion_tokens !== undefined) out.max_completion_tokens = capTokens(out.max_completion_tokens, cap);
  else out.max_tokens = capTokens(out.max_tokens, cap);
  return out;
}

/** Responses body. */
function leanResponsesSummary(body, cap) {
  const items = inputItems(body);
  const sys = [typeof body.instructions === "string" ? body.instructions : ""]
    .concat(items.filter((i) => i && (i.role === "system" || i.role === "developer")).map((i) => blocksText(i.content)))
    .join("\n");
  const last = [...items].reverse().find((i) => i && i.role === "user");
  if (!isSummarization(sys, last && blocksText(last.content))) return undefined;
  const out = { ...body, max_output_tokens: capTokens(body.max_output_tokens, cap) };
  delete out.reasoning;
  return out;
}

/* ======================================================================== *
 * Gateway
 * ======================================================================== */

class Gateway {
  /**
   * @param {{host:string, port:number, upstream:string, catalogPath?:string}} cfg
   * @param {(msg:string)=>void} [log]
   * @param {(ep:any)=>Promise<string|undefined>} [resolveApiKey]
   */
  constructor(cfg, log, resolveApiKey) {
    this.cfg = cfg;
    this.log = log || (() => {});
    this.resolveApiKey = resolveApiKey || (async (ep) => (ep.apiKeyEnv ? process.env[ep.apiKeyEnv] : undefined));
    this.server = undefined;
    this.catalogPath = cfg.catalogPath || DEFAULT_CATALOG;
    this.upstream = (cfg.upstream || process.env.CURSOR_LOCAL_MODELS_UPSTREAM || "").replace(/\/+$/, "");
    this.apiKeyEnv = cfg.apiKeyEnv || "CURSOR_LOCAL_MODELS_API_KEY";
    this.upstreamHeaders = cfg.upstreamHeaders || {};
    this.modelsRefreshMs = Number.isFinite(cfg.modelsRefreshMs) ? cfg.modelsRefreshMs : DEFAULT_MODELS_TTL_MS;
    this.modelsTimeoutMs = Number.isFinite(cfg.modelsTimeoutMs) ? cfg.modelsTimeoutMs : UPSTREAM_MODELS_TIMEOUT_MS;
    this.lastModelsRefresh = 0;
    this.modelsRefreshPromise = undefined;
    this.upstreamModelsWarning = false;
    // 0 or CURSOR_LOCAL_MODELS_SUMMARY_THINKING=1 passes summarization requests through unchanged.
    this.summaryMaxTokens =
      process.env.CURSOR_LOCAL_MODELS_SUMMARY_THINKING === "1" ? 0 : cfg.summaryMaxTokens === undefined ? SUMMARY_MAX_TOKENS : Number(cfg.summaryMaxTokens) || 0;
    // Catalog membership alone is not proof of provider availability.
    this.applyCatalog([]);
  }

  /** Lean copy of a summarization body (see SUMMARIZER_SYSTEM_PREFIX), else the body itself. */
  leanSummary(kind, body, ep) {
    if (!this.summaryMaxTokens) return body;
    const fn = kind === "messages" ? leanAnthropicSummary : kind === "chat" ? leanChatSummary : leanResponsesSummary;
    const lean = fn(body, this.summaryMaxTokens);
    if (!lean) return body;
    this.log(`   ${ep.id}: context summarization -> thinking off, output cap ${this.summaryMaxTokens}`);
    return lean;
  }

  applyCatalog(models) {
    this.catalog = models;
    this.byId = new Map(models.map((m) => [m.id, this.endpointFromCatalog(m)]));
  }

  upstreamModelsEndpoint() {
    return joinUrl(this.upstream, "/models");
  }

  async fetchUpstreamModels() {
    if (!this.upstream) throw new Error("no upstream configured");
    const url = this.upstreamModelsEndpoint();
    const headers = { accept: "application/json", ...(this.upstreamHeaders || {}) };
    const key = (await this.resolveApiKey({ apiKeyEnv: this.apiKeyEnv })) || (this.apiKeyEnv && process.env[this.apiKeyEnv]);
    if (key) headers.authorization = `Bearer ${key}`;
    const res = await globalThis.fetch(url, { headers, signal: AbortSignal.timeout(this.modelsTimeoutMs) });
    if (!res.ok) throw new Error(`GET ${url} -> ${res.status}`);
    const doc = await res.json();
    const items = Array.isArray(doc) ? doc : doc && doc.data;
    if (!Array.isArray(items) || items.some((m) => !isObject(m) || typeof m.id !== "string" || !m.id.trim())) {
      throw new Error("malformed upstream /models response");
    }
    return items;
  }

  /**
   * Refresh once per modelsRefreshMs (or immediately when forced). Failures keep the
   * current list; the upstream is not retried until the TTL expires.
   */
  refreshUpstreamModels(force = false) {
    const fresh = this.lastModelsRefresh && Date.now() - this.lastModelsRefresh < this.modelsRefreshMs;
    if (!force && fresh) return Promise.resolve(false);
    if (this.modelsRefreshPromise) return this.modelsRefreshPromise;
    this.modelsRefreshPromise = (async () => {
      try {
        const items = await this.fetchUpstreamModels();
        const local = this.loadCatalog();
        const models = availableCatalogModels(local, items);
        this.applyCatalog(models);
        this.lastModelsRefresh = Date.now();
        this.upstreamModelsWarning = false;
        this.log(`upstream /models: ${items.length} model(s), ${models.length} catalog matches served`);
        return true;
      } catch (err) {
        this.lastModelsRefresh = Date.now();
        if (!this.upstreamModelsWarning) {
          this.upstreamModelsWarning = true;
          this.log(`model discovery unavailable (${err && err.message}); retaining ${this.catalog.length} last discovered model(s)`);
        }
        return false;
      } finally {
        this.modelsRefreshPromise = undefined;
      }
    })();
    return this.modelsRefreshPromise;
  }

  endpointFromCatalog(m) {
    const apiType = (m.api_types || [])[0];
    const api = apiType === "anthropic_messages" ? "messages" : apiType === "responses" || apiType === "openai_responses" ? "responses" : "chat";
    const caps = m.capabilities || {};
    return {
      id: m.id,
      displayName: m.name || m.id,
      upstream: this.upstream,
      upstreamModel: m.id,
      api,
      apiKeyEnv: this.apiKeyEnv,
      headers: this.upstreamHeaders,
      streaming: caps.supports_streaming !== false,
      toolCalling: caps.supports_tool_use !== false,
      thinking: caps.supports_reasoning === true,
      vision: caps.supports_vision === true,
      contextWindow: caps.context_length,
      maxOutputTokens: caps.max_output_tokens
    };
  }

  /** Map keys are canonical IDs; list-form catalogs remain useful for fixtures. */
  loadCatalog() {
    const doc = JSON.parse(fs.readFileSync(this.catalogPath, "utf8"));
    if (Array.isArray(doc)) return doc;
    if (!isObject(doc)) throw new Error("malformed model catalog");
    if (Array.isArray(doc.data)) return doc.data;
    return Object.entries(doc)
      .filter(([, value]) => isObject(value))
      .map(([id, value]) => ({ ...value, id }));
  }

  async start() {
    if (this.server) return this.port();
    await this.refreshUpstreamModels(true);
    return new Promise((resolve, reject) => {
      this.server = http.createServer((req, res) => {
        this.handle(req, res).catch((err) => {
          this.log(`gateway error: ${err && err.message}`);
          if (!res.headersSent) sendJson(res, 500, { error: { message: String(err && err.message) } });
          else res.end();
        });
      });
      this.server.on("error", reject);
      this.server.listen(this.cfg.port, this.cfg.host, () => {
        this.log(`gateway listening on http://${this.cfg.host}:${this.port()}/v1`);
        resolve(this.port());
      });
    });
  }

  stop() {
    return new Promise((resolve) => {
      if (!this.server) return resolve();
      const s = this.server;
      this.server = undefined;
      s.closeAllConnections?.();
      s.close(() => resolve());
    });
  }

  port() {
    const a = this.server && this.server.address();
    return a && typeof a === "object" ? a.port : this.cfg.port;
  }

  async handle(req, res) {
    const url = new URL(req.url, "http://127.0.0.1");
    const p = url.pathname.replace(/\/+$/, "");
    if (req.method === "GET" && (p === "/v1/models" || p === "/models")) return this.models(res);
    if (req.method === "POST" && (p === "/v1/responses" || p === "/responses")) return this.responses(req, res);
    // Direct base-URL mode (Settings > Models > Override OpenAI Base URL) posts
    // Chat Completions here. Private inference still posts /v1/responses.
    if (req.method === "POST" && (p === "/v1/chat/completions" || p === "/chat/completions")) return this.chatCompletions(req, res);
    // Direct Anthropic base URL. Private inference still posts /v1/responses.
    if (req.method === "POST" && (p === "/v1/messages" || p === "/messages")) return this.messages(req, res);
    sendJson(res, 404, { error: { message: `no route for ${req.method} ${p}` } });
  }

  /**
   * Probe shape the local-agent runtime reads. The catalog file is an id-keyed
   * map; this rebuilds the OpenAI list the probe requires.
   */
  async models(res) {
    await this.refreshUpstreamModels();
    sendJson(res, 200, {
      object: "list",
      data: this.catalog.map((m) => ({
        id: m.id,
        object: "model",
        owned_by: m.owned_by,
        name: m.name,
        api_types: m.api_types,
        capabilities: m.capabilities
      }))
    });
  }

  async headersFor(ep, clientKey) {
    const api = ep.api || "chat";
    const h = { "content-type": "application/json", accept: "application/json, text/event-stream" };
    const key = (await this.resolveApiKey(ep)) || clientKey;
    if (key) {
      h.authorization = `Bearer ${key}`;
      if (api === "messages") h["x-api-key"] = key;
    }
    if (api === "messages") h["anthropic-version"] = ANTHROPIC_VERSION;
    Object.assign(h, ep.headers || ep.extraHeaders || {});
    return h;
  }

  /**
   * Anthropic Messages in, Anthropic Messages out. Used when Cursor talks to the
   * gateway as an Anthropic base URL. Chat upstreams are translated.
   */
  async messages(req, res) {
    const body = safeJsonParse((await readBody(req)) || "{}", undefined);
    if (!body) return sendJson(res, 400, { error: { message: "invalid JSON body" } });
    const ep = this.endpointFor(body.model);
    if (!ep) return this.unknownModel(res, body.model);

    const ac = new AbortController();
    res.on("close", () => {
      if (!res.writableEnded) ac.abort();
    });
    const signal = AbortSignal.any ? AbortSignal.any([ac.signal, AbortSignal.timeout(ep.timeoutMs || DEFAULT_TIMEOUT_MS)]) : ac.signal;
    const stream = body.stream === true && ep.streaming !== false;
    const clientKey = bearer(req.headers.authorization) || bearer(req.headers["x-api-key"]);
    const beta = req.headers["anthropic-beta"];
    const t0 = Date.now();
    this.log(`-> ${ep.id} [messages${stream ? ", stream" : ""}] model=${body.model}`);
    try {
      await this.forwardMessages(beta ? { ...ep, headers: { ...(ep.headers || {}), "anthropic-beta": beta } } : ep, this.leanSummary("messages", body, ep), res, signal, stream, clientKey);
      this.log(`<- ${ep.id} ok ${Date.now() - t0}ms`);
    } catch (err) {
      const msg = err && err.name === "AbortError" ? "aborted" : String((err && err.message) || err);
      this.log(`<- ${ep.id} failed after ${Date.now() - t0}ms: ${msg}`);
      if (!res.headersSent) sendJson(res, err.status && err.status >= 400 && err.status < 600 ? err.status : 502, { error: { message: msg } });
      else res.end();
    }
  }

  /** Anthropic Messages request from Cursor, answered in Anthropic Messages. */
  async forwardMessages(ep, body, res, signal, stream, clientKey) {
    if ((ep.api || "chat") !== "messages") {
      const e = new Error(`model "${body.model}" is not an Anthropic Messages upstream; Cursor posted /messages`);
      e.status = 400;
      throw e;
    }
    const payload = { ...body, model: ep.upstreamModel || ep.id, stream };
    const r = await this.post(ep, joinUrl(ep.upstream, "/messages"), payload, signal, clientKey);
    if (!stream) {
      const obj = await r.json();
      return sendJson(res, 200, { ...obj, model: body.model });
    }
    res.writeHead(200, { "content-type": "text/event-stream", "cache-control": "no-cache", connection: "keep-alive" });
    const reader = r.body.getReader();
    for (;;) {
      const { done, value } = await reader.read();
      if (done) break;
      res.write(Buffer.from(value));
    }
    res.end();
  }

  /**
   * Chat Completions in, Chat Completions out. Used when Cursor talks to the
   * gateway as a plain OpenAI base URL rather than through the Responses loop.
   */
  async chatCompletions(req, res) {
    const body = safeJsonParse((await readBody(req)) || "{}", undefined);
    if (!body) return sendJson(res, 400, { error: { message: "invalid JSON body" } });
    const ep = this.endpointFor(body.model);
    if (!ep) return this.unknownModel(res, body.model);

    const ac = new AbortController();
    res.on("close", () => {
      if (!res.writableEnded) ac.abort();
    });
    const signal = AbortSignal.any ? AbortSignal.any([ac.signal, AbortSignal.timeout(ep.timeoutMs || DEFAULT_TIMEOUT_MS)]) : ac.signal;
    const stream = body.stream === true && ep.streaming !== false;
    const clientKey = bearer(req.headers.authorization);
    const t0 = Date.now();
    this.log(`-> ${ep.id} [chat-completions${stream ? ", stream" : ""}] model=${body.model}`);
    try {
      await this.forwardChat(ep, this.leanSummary("chat", body, ep), res, signal, stream, clientKey);
      this.log(`<- ${ep.id} ok ${Date.now() - t0}ms`);
    } catch (err) {
      const msg = err && err.name === "AbortError" ? "aborted" : String((err && err.message) || err);
      this.log(`<- ${ep.id} failed after ${Date.now() - t0}ms: ${msg}`);
      if (!res.headersSent) sendJson(res, err.status && err.status >= 400 && err.status < 600 ? err.status : 502, { error: { message: msg } });
      else res.end();
    }
  }

  async responses(req, res) {
    const body = safeJsonParse((await readBody(req)) || "{}", undefined);
    if (!body) return sendJson(res, 400, { error: { message: "invalid JSON body" } });

    const ep = this.endpointFor(body.model);
    if (!ep) return this.unknownModel(res, body.model);

    const ac = new AbortController();
    res.on("close", () => {
      if (!res.writableEnded) ac.abort();
    });
    const signal = AbortSignal.any ? AbortSignal.any([ac.signal, AbortSignal.timeout(ep.timeoutMs || DEFAULT_TIMEOUT_MS)]) : ac.signal;

    const api = ep.api || "chat";
    const clientStream = body.stream === true;
    const upstreamStream = clientStream && ep.streaming !== false;
    // Cursor may send its own provider key; pass it through instead of relying on the stored one.
    const clientKey = bearer(req.headers.authorization) || bearer(req.headers["x-api-key"]);
    const t0 = Date.now();
    this.log(`-> ${ep.id} [${api}${upstreamStream ? ", stream" : ""}${clientStream && !upstreamStream ? ", synth-stream" : ""}]`);

    try {
      const b = this.leanSummary("responses", body, ep);
      if (api === "responses") await this.viaResponses(ep, b, res, signal, upstreamStream, clientKey);
      else if (api === "messages") await this.viaMessages(ep, b, res, signal, clientStream, upstreamStream, clientKey);
      else await this.viaChat(ep, b, res, signal, clientStream, upstreamStream, clientKey);
      this.log(`<- ${ep.id} ok ${Date.now() - t0}ms`);
    } catch (err) {
      const msg = err && err.name === "AbortError" ? "aborted" : String((err && err.message) || err);
      this.log(`<- ${ep.id} failed after ${Date.now() - t0}ms: ${msg}`);
      if (!res.headersSent) sendJson(res, err.status && err.status >= 400 && err.status < 600 ? err.status : 502, { error: { message: msg } });
      else res.end();
    }
  }

  endpointFor(model) {
    return this.byId.get(model);
  }

  unknownModel(res, model) {
    return sendJson(res, 404, { error: { message: `unknown model "${model}". Known: ${[...this.byId.keys()].join(", ")}` } });
  }

  /**
   * Chat Completions request from Cursor, answered in Chat Completions.
   * messages upstreams are translated; chat/responses upstreams are forwarded.
   */
  async forwardChat(ep, body, res, signal, stream, clientKey) {
    const api = ep.api || "chat";
    if (api === "messages") return this.messagesAsChat(ep, body, res, signal, stream, clientKey);
    const payload = {
      ...body,
      model: ep.upstreamModel || ep.id,
      stream,
      ...(stream ? { stream_options: body.stream_options || { include_usage: true } } : {})
    };
    const suffix = api === "responses" ? "/responses" : "/chat/completions";
    // A responses upstream cannot take a Chat Completions body. Reject clearly
    // rather than forwarding a body it will not understand.
    if (api === "responses") {
      const e = new Error(`model "${body.model}" is a Responses upstream; Cursor posted /chat/completions`);
      e.status = 400;
      throw e;
    }
    const r = await this.post(ep, joinUrl(ep.upstream, suffix), payload, signal, clientKey);
    if (!stream) {
      const obj = await r.json();
      return sendJson(res, 200, { ...obj, model: body.model });
    }
    res.writeHead(200, { "content-type": "text/event-stream", "cache-control": "no-cache", connection: "keep-alive" });
    const reader = r.body.getReader();
    for (;;) {
      const { done, value } = await reader.read();
      if (done) break;
      res.write(Buffer.from(value));
    }
    res.end();
  }

  /** Anthropic Messages upstream, spoken back to Cursor as Chat Completions. */
  async messagesAsChat(ep, body, res, signal, stream, clientKey) {
    const system = [];
    const messages = [];
    for (const m of body.messages || []) {
      if (!m) continue;
      if (m.role === "system" || m.role === "developer") {
        const t = typeof m.content === "string" ? m.content : JSON.stringify(m.content ?? "");
        if (t) system.push(t);
      } else messages.push({ role: m.role === "assistant" ? "assistant" : "user", content: typeof m.content === "string" ? m.content : JSON.stringify(m.content ?? "") });
    }
    const payload = {
      model: ep.upstreamModel || ep.id,
      system: system.join("\n\n") || undefined,
      messages,
      max_tokens: body.max_tokens ?? body.max_completion_tokens ?? ep.maxOutputTokens ?? 8192,
      temperature: body.temperature,
      stream
    };
    const r = await this.post(ep, joinUrl(ep.upstream, "/messages"), payload, signal, clientKey);
    if (!stream) {
      const msg = await r.json();
      const text = (msg.content || []).filter((b) => b.type === "text").map((b) => b.text).join("");
      return sendJson(res, 200, {
        id: rid("chatcmpl"),
        object: "chat.completion",
        created: Math.floor(Date.now() / 1000),
        model: body.model,
        choices: [{ index: 0, message: { role: "assistant", content: text }, finish_reason: msg.stop_reason === "max_tokens" ? "length" : "stop" }],
        usage: { prompt_tokens: msg.usage?.input_tokens, completion_tokens: msg.usage?.output_tokens }
      });
    }
    res.writeHead(200, { "content-type": "text/event-stream", "cache-control": "no-cache", connection: "keep-alive" });
    const id = rid("chatcmpl");
    const write = (obj) => res.write(`data: ${JSON.stringify(obj)}\n\n`);
    try {
      for await (const { event, data } of sseFrames(r.body)) {
        const e = safeJsonParse(data, undefined);
        if (!e) continue;
        const type = e.type || event;
        if (type === "error") throw new Error(e.error?.message || JSON.stringify(e));
        if (type === "content_block_delta" && e.delta?.type === "text_delta") {
          write({ id, object: "chat.completion.chunk", model: body.model, choices: [{ index: 0, delta: { content: e.delta.text } }] });
        }
      }
    } catch (err) {
      write({ error: { message: String(err.message || err) } });
    }
    res.write("data: [DONE]\n\n");
    res.end();
  }

  async post(ep, url, payload, signal, clientKey) {
    const r = await globalThis.fetch(url, { method: "POST", headers: await this.headersFor(ep, clientKey), body: JSON.stringify(payload), signal });
    if (!r.ok) {
      const text = await r.text().catch(() => "");
      const e = new Error(`${url} -> HTTP ${r.status}: ${text.slice(0, 2000)}`);
      e.status = r.status;
      throw e;
    }
    return r;
  }

  /* ---------- upstream speaks Responses: pass through ---------- */
  async viaResponses(ep, body, res, signal, upstreamStream, clientKey) {
    const payload = { ...body, model: ep.upstreamModel || ep.id, stream: upstreamStream };
    const r = await this.post(ep, joinUrl(ep.upstream, "/responses"), payload, signal, clientKey);
    if (!upstreamStream) {
      const obj = await r.json();
      if (body.stream !== true) return sendJson(res, 200, { ...obj, model: ep.id });
      // Client wants a stream but the upstream does not stream: replay the object.
      const em = new ResponsesEmitter(res, ep.id, true);
      for (const item of obj.output || []) {
        if (item.type === "message") em.text((item.content || []).map(partText).join(""));
        else if (item.type === "reasoning") em.reasoning((item.summary || []).map(partText).join(""));
        else if (item.type === "function_call") em.toolCall(item.call_id, item.name, item.arguments);
      }
      em.setUsage(obj.usage?.input_tokens, obj.usage?.output_tokens);
      return em.finish();
    }
    res.writeHead(200, { "content-type": "text/event-stream", "cache-control": "no-cache", connection: "keep-alive" });
    const reader = r.body.getReader();
    for (;;) {
      const { done, value } = await reader.read();
      if (done) break;
      res.write(Buffer.from(value));
    }
    res.end();
  }

  /* ---------- upstream speaks Chat Completions ---------- */
  async viaChat(ep, body, res, signal, clientStream, upstreamStream, clientKey) {
    const tools = ep.toolCalling === false ? undefined : toChatTools(body.tools);
    const payload = {
      model: ep.upstreamModel || ep.id,
      messages: responsesToChatMessages(body),
      tools,
      tool_choice: tools ? toChatToolChoice(body.tool_choice) : undefined,
      max_tokens: body.max_output_tokens ?? undefined,
      temperature: body.temperature,
      top_p: body.top_p,
      stream: upstreamStream,
      stream_options: upstreamStream ? { include_usage: true } : undefined
    };
    if (ep.thinking && ep.enableThinking !== false && body.reasoning?.effort) payload.reasoning_effort = body.reasoning.effort;

    const r = await this.post(ep, joinUrl(ep.upstream, "/chat/completions"), payload, signal, clientKey);
    const em = new ResponsesEmitter(res, ep.id, clientStream);
    const custom = customToolNames(body.tools);

    if (!upstreamStream) {
      const chat = await r.json();
      const msg = chat.choices?.[0]?.message || {};
      em.reasoning(msg.reasoning_content ?? msg.reasoning);
      em.text(typeof msg.content === "string" ? msg.content : Array.isArray(msg.content) ? msg.content.map(partText).join("") : "");
      for (const tc of msg.tool_calls || []) em.toolCall(tc.id, tc.function?.name, tc.function?.arguments || "{}", custom.has(tc.function?.name));
      em.setUsage(chat.usage?.prompt_tokens, chat.usage?.completion_tokens);
      return em.finish(lengthStop(chat.choices?.[0]?.finish_reason));
    }

    // Tool-call deltas are interleaved by index, so collect them and emit at the end;
    // text and reasoning are streamed live.
    const calls = new Map();
    let chatFinish;
    try {
      for await (const { data } of sseFrames(r.body)) {
        if (data === "[DONE]") break;
        const chunk = safeJsonParse(data, undefined);
        if (!chunk) continue;
        if (chunk.error) throw new Error(chunk.error.message || JSON.stringify(chunk.error));
        if (chunk.usage) em.setUsage(chunk.usage.prompt_tokens, chunk.usage.completion_tokens);
        const fr = chunk.choices?.[0]?.finish_reason;
        if (fr) chatFinish = fr;
        const delta = chunk.choices?.[0]?.delta;
        if (!delta) continue;
        em.reasoning(delta.reasoning_content ?? delta.reasoning);
        if (typeof delta.content === "string") em.text(delta.content);
        for (const tc of delta.tool_calls || []) {
          const i = tc.index ?? calls.size;
          const cur = calls.get(i) || { id: undefined, name: "", args: "" };
          if (tc.id) cur.id = tc.id;
          if (tc.function?.name) cur.name += tc.function.name;
          if (tc.function?.arguments) cur.args += tc.function.arguments;
          calls.set(i, cur);
        }
      }
    } catch (err) {
      return em.fail(502, String(err.message || err));
    }
    for (const [, c] of [...calls.entries()].sort((a, b) => a[0] - b[0])) em.toolCall(c.id, c.name, c.args || "{}", custom.has(c.name));
    em.finish(lengthStop(chatFinish));
  }

  /* ---------- upstream speaks Anthropic Messages ---------- */
  async viaMessages(ep, body, res, signal, clientStream, upstreamStream, clientKey) {
    const { system, messages } = responsesToAnthropic(body);
    const tools = ep.toolCalling === false ? undefined : toAnthropicTools(body.tools);
    const payload = {
      model: ep.upstreamModel || ep.id,
      system,
      messages,
      tools,
      tool_choice: tools ? toAnthropicToolChoice(body.tool_choice) : undefined,
      max_tokens: body.max_output_tokens ?? ep.maxOutputTokens ?? 8192,
      temperature: body.temperature,
      stream: upstreamStream
    };
    const effort = body.reasoning?.effort;
    if (ep.thinking && ep.enableThinking !== false && effort && effort !== "none") {
      const budget = EFFORT_BUDGET[effort] ?? EFFORT_BUDGET.medium;
      payload.thinking = { type: "enabled", budget_tokens: budget };
      if (payload.max_tokens <= budget) payload.max_tokens = budget + 4096;
      delete payload.temperature; // Anthropic rejects temperature with extended thinking
    }

    const r = await this.post(ep, joinUrl(ep.upstream, "/messages"), payload, signal, clientKey);
    const em = new ResponsesEmitter(res, ep.id, clientStream);
    const custom = customToolNames(body.tools);

    if (!upstreamStream) {
      const msg = await r.json();
      for (const b of msg.content || []) {
        if (b.type === "thinking") em.reasoning(b.thinking);
        else if (b.type === "text") em.text(b.text);
        else if (b.type === "tool_use") em.toolCall(b.id, b.name, b.input, custom.has(b.name));
      }
      em.setUsage(msg.usage?.input_tokens, msg.usage?.output_tokens);
      return em.finish(lengthStop(msg.stop_reason));
    }

    let msgStop;
    try {
      for await (const { event, data } of sseFrames(r.body)) {
        const e = safeJsonParse(data, undefined);
        if (!e) continue;
        const type = e.type || event;
        if (type === "error") throw new Error(e.error?.message || JSON.stringify(e));
        if (type === "message_start") em.setUsage(e.message?.usage?.input_tokens, undefined);
        else if (type === "content_block_start") {
          const b = e.content_block || {};
          if (b.type === "tool_use") custom.has(b.name) ? em.customToolStart(b.id, b.name) : em.toolStart(b.id, b.name);
          else if (b.type === "text" && b.text) em.text(b.text);
          else if (b.type === "thinking" && b.thinking) em.reasoning(b.thinking);
        } else if (type === "content_block_delta") {
          const d = e.delta || {};
          if (d.type === "text_delta") em.text(d.text);
          else if (d.type === "thinking_delta") em.reasoning(d.thinking);
          else if (d.type === "input_json_delta") em.toolArgs(d.partial_json);
        } else if (type === "content_block_stop") em.close();
        else if (type === "message_delta") {
          em.setUsage(undefined, e.usage?.output_tokens);
          if (e.delta?.stop_reason) msgStop = e.delta.stop_reason;
        } else if (type === "message_stop") break;
      }
    } catch (err) {
      return em.fail(502, String(err.message || err));
    }
    em.finish(lengthStop(msgStop));
  }
}

/**
 * Extract a bearer token or raw API key from an incoming header.
 * Ignore the placeholder "local" token that local-mode setups use when the gateway
 * itself holds the real upstream credentials.
 */
function bearer(header) {
  const raw = String(header || "").trim();
  const m = /^Bearer\s+(\S+)/i.exec(raw);
  const key = m ? m[1] : raw && !/\s/.test(raw) ? raw : undefined;
  return key && key !== "local" ? key : undefined;
}

/** Upstream stop reason that means we hit the output token cap, mapped to Responses' incomplete reason. */
function lengthStop(reason) {
  return reason === "length" || reason === "max_tokens" ? "max_output_tokens" : undefined;
}

function isObject(value) {
  return value !== null && typeof value === "object" && !Array.isArray(value);
}

/** Discovery supplies availability only; all advertised metadata comes from the catalog. */
function availableCatalogModels(local, upstream) {
  const available = new Set(upstream.map((m) => m.id));
  const seen = new Set();
  return local.filter((m) => {
    if (!isObject(m) || typeof m.id !== "string" || !m.id.trim() || !available.has(m.id) || seen.has(m.id)) return false;
    if (!Array.isArray(m.api_types) || !m.api_types.length || !m.api_types.every((api) => SUPPORTED_API_TYPES.has(api))) return false;
    if (!isObject(m.capabilities)) return false;
    seen.add(m.id);
    return true;
  });
}

module.exports = {
  Gateway,
  ResponsesEmitter,
  sseFrames,
  // exported for tests
  availableCatalogModels,
  responsesToChatMessages,
  responsesToAnthropic,
  toChatTools,
  toAnthropicTools
};
