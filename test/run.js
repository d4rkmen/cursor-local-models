"use strict";
/*
 * Self-contained tests: no network beyond loopback, no secrets read.
 *   node test/run.js
 */
const assert = require("node:assert/strict");
const http = require("node:http");
const fs = require("node:fs");
const os = require("node:os");
const path = require("node:path");

const { Gateway, availableCatalogModels } = require("../out/gateway");

const fixturePath = path.join(__dirname, "fixtures", "models.json");
const fixtureCatalog = JSON.parse(fs.readFileSync(fixturePath, "utf8"));
const realFetch = globalThis.fetch;
globalThis.fetch = (url, options) => {
  assert.equal(new URL(url).hostname, "127.0.0.1", "tests must use loopback only");
  return realFetch(url, options);
};

let failures = 0;
async function test(name, fn) {
  try {
    await fn();
    console.log(`ok   ${name}`);
  } catch (e) {
    failures++;
    console.log(`FAIL ${name}\n     ${e.stack.split("\n").join("\n     ")}`);
  }
}

/* ----------------------------- mock upstream ----------------------------- */

function mockUpstream() {
  const seen = [];
  const discovery = {
    status: 200,
    raw: undefined,
    body: {
      object: "list",
      data: [
        ...Object.keys(fixtureCatalog).map((id) => ({ id })),
        { id: "acme/unknown", name: "Unknown", api_types: ["responses"], capabilities: { supports_tool_use: true } }
      ]
    }
  };
  const server = http.createServer(async (req, res) => {
    const chunks = [];
    for await (const c of req) chunks.push(c);
    const body = JSON.parse(Buffer.concat(chunks).toString() || "{}");
    seen.push({ url: req.url, headers: req.headers, body });
    const sse = (frames) => {
      res.writeHead(200, { "content-type": "text/event-stream" });
      for (const f of frames) res.write(f);
      res.end();
    };
    if (req.url.endsWith("/models")) {
      res.writeHead(discovery.status, { "content-type": "application/json" });
      return res.end(discovery.raw === undefined ? JSON.stringify(discovery.body) : discovery.raw);
    }
    if (req.url.endsWith("/chat/completions")) {
      if (body.stream) {
        return sse([
          `data: ${JSON.stringify({ choices: [{ delta: { reasoning_content: "think " } }] })}\n\n`,
          `data: ${JSON.stringify({ choices: [{ delta: { content: "Hel" } }] })}\n\n`,
          `data: ${JSON.stringify({ choices: [{ delta: { content: "lo" } }] })}\n\n`,
          `data: ${JSON.stringify({ choices: [{ delta: { tool_calls: [{ index: 0, id: "c1", function: { name: "read_file", arguments: '{"pa' } }] } }] })}\n\n`,
          `data: ${JSON.stringify({ choices: [{ delta: { tool_calls: [{ index: 0, function: { arguments: 'th":"a"}' } }] } }] })}\n\n`,
          `data: ${JSON.stringify({ choices: [], usage: { prompt_tokens: 7, completion_tokens: 3, total_tokens: 10 } })}\n\n`,
          "data: [DONE]\n\n"
        ]);
      }
      res.writeHead(200, { "content-type": "application/json" });
      return res.end(
        JSON.stringify({
          choices: [{ message: { content: "Hello", tool_calls: [{ id: "c1", type: "function", function: { name: "read_file", arguments: '{"path":"a"}' } }] } }],
          usage: { prompt_tokens: 7, completion_tokens: 3 }
        })
      );
    }
    if (req.url.endsWith("/messages")) {
      const ev = (type, o) => `event: ${type}\ndata: ${JSON.stringify({ type, ...o })}\n\n`;
      // A cap hit with only thinking: the case that silently ends an agent turn
      // when reported as a normal completion.
      if (body.stream && body.max_tokens === 1) {
        return sse([
          ev("message_start", { message: { usage: { input_tokens: 9 } } }),
          ev("content_block_start", { index: 0, content_block: { type: "thinking", thinking: "" } }),
          ev("content_block_delta", { index: 0, delta: { type: "thinking_delta", thinking: "still thinking" } }),
          ev("content_block_stop", { index: 0 }),
          ev("message_delta", { delta: { stop_reason: "max_tokens" }, usage: { output_tokens: 4096 } }),
          ev("message_stop", {})
        ]);
      }
      if (body.stream) {
        return sse([
          ev("message_start", { message: { usage: { input_tokens: 11 } } }),
          ev("content_block_start", { index: 0, content_block: { type: "thinking", thinking: "" } }),
          ev("content_block_delta", { index: 0, delta: { type: "thinking_delta", thinking: "hmm" } }),
          ev("content_block_stop", { index: 0 }),
          ev("content_block_start", { index: 1, content_block: { type: "text", text: "" } }),
          ev("content_block_delta", { index: 1, delta: { type: "text_delta", text: "Hi" } }),
          ev("content_block_stop", { index: 1 }),
          ev("content_block_start", { index: 2, content_block: { type: "tool_use", id: "tu1", name: "ls", input: {} } }),
          ev("content_block_delta", { index: 2, delta: { type: "input_json_delta", partial_json: '{"dir":' } }),
          ev("content_block_delta", { index: 2, delta: { type: "input_json_delta", partial_json: '"."}' } }),
          ev("content_block_stop", { index: 2 }),
          ev("message_delta", { delta: { stop_reason: "end_turn" }, usage: { output_tokens: 5 } }),
          ev("message_stop", {})
        ]);
      }
      res.writeHead(200, { "content-type": "application/json" });
      return res.end(
        JSON.stringify({
          content: [
            { type: "thinking", thinking: "hmm" },
            { type: "text", text: "Hi" },
            { type: "tool_use", id: "tu1", name: "ls", input: { dir: "." } }
          ],
          usage: { input_tokens: 11, output_tokens: 5 }
        })
      );
    }
    res.writeHead(404).end();
  });
  return new Promise((r) => server.listen(0, "127.0.0.1", () => r({ server, seen, discovery, url: `http://127.0.0.1:${server.address().port}/v1` })));
}

async function parseSse(r) {
  const text = await r.text();
  return text
    .split("\n\n")
    .filter(Boolean)
    .map((f) => JSON.parse(f.split("\n").find((l) => l.startsWith("data:")).slice(5)));
}

const REQ = {
  instructions: "You are terse.",
  input: [
    { type: "message", role: "user", content: [{ type: "input_text", text: "list files" }] },
    { type: "function_call", call_id: "prev1", name: "ls", arguments: '{"dir":"/"}' },
    { type: "function_call_output", call_id: "prev1", output: "a b" }
  ],
  tools: [{ type: "function", name: "read_file", description: "read", parameters: { type: "object", properties: { path: { type: "string" } } } }],
  reasoning: { effort: "low" },
  max_output_tokens: 1000
};

function summarize(events) {
  const types = events.map((e) => e.type);
  const text = events.filter((e) => e.type === "response.output_text.delta").map((e) => e.delta).join("");
  const reasoning = events.filter((e) => e.type === "response.reasoning_summary_text.delta").map((e) => e.delta).join("");
  const done = events.find((e) => e.type === "response.completed" || e.type === "response.incomplete");
  const calls = (done.response.output || []).filter((o) => o.type === "function_call");
  return { types, text, reasoning, calls, usage: done.response.usage, status: done.response.status, incomplete: done.response.incomplete_details, seqOk: events.every((e, i) => e.sequence_number === i) };
}

(async () => {
  const up = await mockUpstream();
  const keyStore = { PROVIDER_API_KEY: "sk-test-1" };
  const catalogPath = fixturePath;
  const gw = new Gateway(
    { host: "127.0.0.1", port: 0, catalogPath, upstream: up.url, apiKeyEnv: "PROVIDER_API_KEY", upstreamHeaders: { "x-custom-upstream": "true" } },
    () => {},
    async (ep) => keyStore[ep.apiKeyEnv]
  );
  const port = await gw.start();
  const base = `http://127.0.0.1:${port}/v1`;
  const call = (model, stream) =>
    fetch(`${base}/responses`, { method: "POST", headers: { "content-type": "application/json" }, body: JSON.stringify({ ...REQ, model, stream }) });
  await test("GET /v1/models serves only discovered catalog IDs without invented metadata", async () => {
    const j = await (await fetch(`${base}/models`)).json();
    assert.equal(j.object, "list");
    assert.deepEqual(j.data, Object.entries(fixtureCatalog).map(([id, metadata]) => ({ id, object: "model", ...metadata })));
    assert.equal(gw.endpointFor("acme/unknown"), undefined);
  });

  await test("catalog intersection validates metadata and never enriches from upstream", async () => {
    const supported = ["responses", "openai_responses", "chat_completions", "openai_chat", "anthropic_messages"];
    const valid = supported.map((api) => ({ id: `vendor/${api}`, api_types: [api], capabilities: {} }));
    const invalid = [
      { id: "missing-api", capabilities: {} },
      { id: "empty-api", api_types: [], capabilities: {} },
      { id: "unknown-api", api_types: ["chat"], capabilities: {} },
      { id: "mixed-api", api_types: ["responses", "unknown"], capabilities: {} },
      { id: "string-api", api_types: "responses", capabilities: {} },
      { id: "missing-caps", api_types: ["responses"] },
      ...[null, [], "caps", false].map((capabilities, i) => ({ id: `bad-caps-${i}`, api_types: ["responses"], capabilities }))
    ];
    const local = [...valid, ...invalid, { id: "catalog-only", api_types: ["responses"], capabilities: {} }];
    const upstream = [...valid, ...invalid].map(({ id }) => ({ id, name: "Provider", api_types: ["responses"], capabilities: { supports_tool_use: true } }));
    upstream.push({ id: "unknown" }, { id: "vendor-responses" }, { id: "vendor/responses" });
    assert.deepEqual(availableCatalogModels(local, upstream), valid);
    assert.deepEqual(availableCatalogModels(valid, [{ id: "VENDOR/responses" }, { id: "vendor-responses" }]), []);
  });

  await test("inference rejects fuzzy aliases, case changes, prefixes and unknown IDs", async () => {
    const before = up.seen.length;
    for (const route of ["responses", "chat/completions", "messages"]) {
      for (const model of ["vendor-chat", "chat", "Vendor/chat", "custom-vendor-chat", "custom-deepseek-v4-1-flash", "acme/unknown"]) {
        const r = await fetch(`${base}/${route}`, {
          method: "POST",
          headers: { "content-type": "application/json" },
          body: JSON.stringify({ model, input: "hi", messages: [{ role: "user", content: "hi" }] })
        });
        assert.equal(r.status, 404, `${route}: ${model}`);
        await r.text();
      }
    }
    assert.equal(up.seen.length, before);
  });

  await test("POST /v1/chat/completions forwards an exact model ID", async () => {
    const r = await fetch(`${base}/chat/completions`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({
        model: "deepseek/deepseek-v4-flash",
        stream: true,
        messages: [{ role: "user", content: "hi" }]
      })
    });
    assert.equal(r.status, 200, await r.clone().text());
    assert.match(r.headers.get("content-type"), /text\/event-stream/);
    const raw = await r.text();
    assert.match(raw, /\[DONE\]/);
    const last = up.seen.at(-1);
    assert.ok(last.url.endsWith("/chat/completions"), last.url);
    assert.equal(last.body.model, "deepseek/deepseek-v4-flash");
    assert.equal(last.body.messages[0].content, "hi");
  });

  await test("chat completions forwards the client bearer token when no upstream key is configured", async () => {
    const bare = new Gateway({ host: "127.0.0.1", port: 0, catalogPath, upstream: up.url, apiKeyEnv: "MISSING" }, () => {}, async () => undefined);
    const barePort = await bare.start();
    const r = await fetch(`http://127.0.0.1:${barePort}/v1/chat/completions`, {
      method: "POST",
      headers: { "content-type": "application/json", authorization: "Bearer sk-from-cursor" },
      body: JSON.stringify({ model: "deepseek/deepseek-v4-flash", stream: false, messages: [{ role: "user", content: "hi" }] })
    });
    assert.equal(r.status, 200, await r.clone().text());
    await r.text();
    assert.equal(up.seen.at(-1).headers.authorization, "Bearer sk-from-cursor");
    await bare.stop();
  });

  await test("POST /v1/messages forwards an Anthropic body and rewrites the model", async () => {
    const r = await fetch(`${base}/messages`, {
      method: "POST",
      headers: { "content-type": "application/json", "x-api-key": "sk-from-cursor" },
      body: JSON.stringify({
        model: "anthropic/claude-opus-5-5",
        max_tokens: 128000,
        stream: false,
        messages: [{ role: "user", content: "hi" }]
      })
    });
    assert.equal(r.status, 200, await r.clone().text());
    const j = await r.json();
    assert.equal(j.model, "anthropic/claude-opus-5-5");
    assert.equal(j.content.find((b) => b.type === "text").text, "Hi");
    const last = up.seen.at(-1);
    assert.ok(last.url.endsWith("/messages"));
    assert.equal(last.body.model, "anthropic/claude-opus-5-5");
    assert.equal(last.body.messages[0].content, "hi");
    assert.equal(last.headers["x-api-key"], "sk-test-1");
  });

  await test("chat upstream, streaming: text+reasoning live, tool args reassembled, headers+key forwarded", async () => {
    const s = summarize(await parseSse(await call("vendor/chat", true)));
    assert.equal(s.text, "Hello");
    assert.equal(s.reasoning, "think ");
    assert.equal(s.calls.length, 1);
    assert.equal(s.calls[0].name, "read_file");
    assert.deepEqual(JSON.parse(s.calls[0].arguments), { path: "a" });
    assert.equal(s.calls[0].call_id, "c1");
    assert.deepEqual(s.usage, { input_tokens: 7, output_tokens: 3, total_tokens: 10 });
    assert.ok(s.seqOk, "sequence numbers contiguous");
    const last = up.seen.at(-1);
    assert.equal(last.headers.authorization, "Bearer sk-test-1");
    assert.equal(last.headers["x-custom-upstream"], "true");
    assert.equal(last.body.model, "vendor/chat");
    assert.equal(last.body.stream, true);
    assert.equal(last.body.reasoning_effort, "low");
    // history translation: function_call + function_call_output round-trip
    const roles = last.body.messages.map((m) => m.role);
    assert.deepEqual(roles, ["system", "user", "assistant", "tool"]);
    assert.equal(last.body.messages[2].tool_calls[0].id, "prev1");
    assert.equal(last.body.messages[3].tool_call_id, "prev1");
  });

  await test("chat upstream with streaming:false -> upstream called non-streaming, stream synthesised", async () => {
    const s = summarize(await parseSse(await call("vendor/chat-ns", true)));
    assert.equal(up.seen.at(-1).body.stream, false);
    assert.equal(s.text, "Hello");
    assert.equal(s.calls.length, 1);
  });

  await test("chat upstream, non-streaming client -> single Responses object", async () => {
    const j = await (await call("vendor/chat", false)).json();
    assert.equal(j.object, "response");
    assert.equal(j.status, "completed");
    assert.equal(j.output.find((o) => o.type === "message").content[0].text, "Hello");
  });

  await test("messages upstream, streaming: thinking, text, tool_use; anthropic headers; history as tool_use/tool_result", async () => {
    const s = summarize(await parseSse(await call("anthropic/x", true)));
    assert.equal(s.text, "Hi");
    assert.equal(s.reasoning, "hmm");
    assert.equal(s.calls.length, 1);
    assert.deepEqual(JSON.parse(s.calls[0].arguments), { dir: "." });
    assert.deepEqual(s.usage, { input_tokens: 11, output_tokens: 5, total_tokens: 16 });
    const last = up.seen.at(-1);
    assert.ok(last.url.endsWith("/v1/messages"));
    assert.equal(last.headers["x-api-key"], "sk-test-1");
    assert.equal(last.headers["anthropic-version"], "2023-06-01");
    assert.equal(last.body.system, "You are terse.");
    assert.equal(last.body.thinking.type, "enabled");
    assert.ok(last.body.max_tokens > last.body.thinking.budget_tokens);
    assert.equal(last.body.temperature, undefined);
    assert.deepEqual(
      last.body.messages.map((m) => [m.role, m.content.map((b) => b.type)]),
      [["user", ["text"]], ["assistant", ["tool_use"]], ["user", ["tool_result"]]]
    );
    assert.equal(last.body.tools[0].input_schema.type, "object");
    assert.equal(s.status, "completed");
  });

  await test("messages upstream hitting max_tokens is reported incomplete, not as a successful empty reply", async () => {
    const r = await fetch(`${base}/responses`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ ...REQ, model: "anthropic/x", stream: true, max_output_tokens: 1, reasoning: { effort: "none" } })
    });
    const s = summarize(await parseSse(r));
    assert.equal(s.status, "incomplete");
    assert.deepEqual(s.incomplete, { reason: "max_output_tokens" });
    assert.ok(s.types.includes("response.incomplete"));
    assert.equal(s.text, "");
    assert.equal(s.reasoning, "still thinking");
    assert.equal(up.seen.at(-1).body.max_tokens, 1);
  });

  await test("messages upstream with streaming:false -> synthesised stream", async () => {
    const s = summarize(await parseSse(await call("anthropic/x-ns", true)));
    assert.equal(up.seen.at(-1).body.stream, false);
    assert.equal(s.text, "Hi");
    assert.equal(s.reasoning, "hmm");
    assert.deepEqual(JSON.parse(s.calls[0].arguments), { dir: "." });
  });

  await test("custom (freeform) tool: sent as 1-arg function, returned as custom_tool_call; history round-trips", async () => {
    const req = {
      model: "vendor/chat-ns",
      stream: true,
      input: [
        { type: "message", role: "user", content: "patch it" },
        { type: "custom_tool_call", call_id: "cp1", name: "apply_patch", input: "*** Begin Patch" },
        { type: "custom_tool_call_output", call_id: "cp1", output: "ok" }
      ],
      tools: [{ type: "custom", name: "read_file", description: "freeform read", format: { type: "grammar", syntax: "lark", definition: "start: /.+/" } }]
    };
    const r = await fetch(`${base}/responses`, { method: "POST", headers: { "content-type": "application/json" }, body: JSON.stringify(req) });
    const events = await parseSse(r);
    const done = events.find((e) => e.type === "response.completed");
    const ctc = done.response.output.filter((o) => o.type === "custom_tool_call");
    assert.equal(ctc.length, 1, "custom_tool_call emitted");
    assert.equal(ctc[0].name, "read_file");
    // mock returns arguments {"path":"a"} (no `input` key) -> raw string is passed through
    assert.equal(ctc[0].input, '{"path":"a"}');
    assert.ok(events.some((e) => e.type === "response.custom_tool_call_input.done"));
    const sent = up.seen.at(-1).body;
    assert.deepEqual(sent.tools[0].function.parameters.required, ["input"]);
    assert.match(sent.tools[0].function.description, /lark/);
    assert.deepEqual(JSON.parse(sent.messages[1].tool_calls[0].function.arguments), { input: "*** Begin Patch" });
    assert.equal(sent.messages[2].role, "tool");
  });

  const SUMMARY_SYS =
    "You are an intelligent assistant, tasked with summarizing the following conversation. You MUST follow the instructions given in the <summarization_request> tags.";
  const SUMMARY_USER = "user: fix the bug\n\nassistant: done\n<summarization_request>Summarize.</summarization_request>";

  await test("context summarization over /v1/messages: thinking and effort dropped, output capped, betas forwarded", async () => {
    const r = await fetch(`${base}/messages`, {
      method: "POST",
      headers: { "content-type": "application/json", "anthropic-beta": "context-1m-2025-08-07" },
      body: JSON.stringify({
        model: "anthropic/claude-opus-5-5",
        max_tokens: 128000,
        stream: true,
        thinking: { type: "adaptive", display: "summarized" },
        output_config: { effort: "high" },
        system: [{ type: "text", text: SUMMARY_SYS }],
        messages: [{ role: "user", content: [{ type: "text", text: SUMMARY_USER }] }]
      })
    });
    assert.equal(r.status, 200, await r.clone().text());
    await r.text();
    const last = up.seen.at(-1);
    assert.equal(last.body.thinking, undefined);
    assert.equal(last.body.output_config, undefined);
    assert.equal(last.body.max_tokens, 32000);
    assert.equal(last.headers["anthropic-beta"], "context-1m-2025-08-07");
  });

  await test("ordinary /v1/messages turn keeps thinking and max_tokens", async () => {
    const r = await fetch(`${base}/messages`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({
        model: "anthropic/claude-opus-5-5",
        max_tokens: 128000,
        stream: false,
        thinking: { type: "adaptive" },
        output_config: { effort: "high" },
        system: "You are Cursor's agent. <summarization_request> appears only in user text.",
        messages: [{ role: "user", content: SUMMARY_USER }]
      })
    });
    await r.text();
    const last = up.seen.at(-1);
    assert.deepEqual(last.body.thinking, { type: "adaptive" });
    assert.deepEqual(last.body.output_config, { effort: "high" });
    assert.equal(last.body.max_tokens, 128000);
  });

  await test("context summarization over /v1/chat/completions and /v1/responses: reasoning dropped, output capped", async () => {
    let r = await fetch(`${base}/chat/completions`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({
        model: "deepseek/deepseek-v4-flash",
        stream: false,
        reasoning_effort: "high",
        max_tokens: 384000,
        messages: [
          { role: "system", content: SUMMARY_SYS },
          { role: "user", content: SUMMARY_USER }
        ]
      })
    });
    await r.text();
    let last = up.seen.at(-1);
    assert.equal(last.body.reasoning_effort, undefined);
    assert.equal(last.body.max_tokens, 32000);

    r = await fetch(`${base}/responses`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ model: "anthropic/x", stream: false, instructions: SUMMARY_SYS, input: SUMMARY_USER, reasoning: { effort: "high" }, max_output_tokens: 128000 })
    });
    await r.text();
    last = up.seen.at(-1);
    assert.equal(last.body.thinking, undefined);
    assert.equal(last.body.max_tokens, 32000);
  });

  await test("discovery refresh: initial failure, TTL, catalog reload, metadata isolation, malformed responses and empty lists", async () => {
    const dir = fs.mkdtempSync(path.join(os.tmpdir(), "local-models-test-"));
    const tempCatalog = path.join(dir, "models.json");
    const provider = await mockUpstream();
    const other = await mockUpstream();
    const metadata = { name: "Catalog Chat", owned_by: "catalog-owner", api_types: ["openai_chat"], capabilities: {} };
    const writeCatalog = (value) => fs.writeFileSync(tempCatalog, JSON.stringify(value));
    writeCatalog({ "vendor/chat": { ...metadata, upstream: other.url }, "catalog-only": metadata });
    provider.discovery.status = 503;
    const local = new Gateway({
      host: "127.0.0.1", port: 0, upstream: provider.url, catalogPath: tempCatalog,
      endpoints: [{ id: "explicit", upstream: other.url }],
      upstreamModelsUrl: `${other.url}/models`, mergeUpstreamModels: false
    }, () => {}, async () => "test-key");
    try {
      assert.deepEqual(local.catalog, []);
      assert.equal(local.byId.size, 0);
      assert.equal(local.modelsRefreshMs, 60000);
      const localBase = `http://127.0.0.1:${await local.start()}/v1`;
      const getModels = async () => (await (await fetch(`${localBase}/models`)).json()).data;
      const expire = () => { local.lastModelsRefresh = Date.now() - 60001; };
      const discoveryCount = () => provider.seen.filter((r) => r.url.endsWith("/models")).length;
      assert.deepEqual(await getModels(), []);
      assert.equal(discoveryCount(), 1, "failed attempts respect TTL");

      provider.discovery.status = 200;
      provider.discovery.body = { data: [{
        id: "vendor/chat", name: "Provider Name", owned_by: "provider-owner", api_types: ["anthropic_messages"],
        capabilities: { supports_tool_use: true, supports_streaming: false }, upstream: other.url, upstreamModel: "other-model"
      }, { id: "unknown", ...metadata }] };
      expire();
      const expected = [{ id: "vendor/chat", object: "model", ...metadata }];
      assert.deepEqual(await getModels(), expected);
      assert.equal(local.endpointFor("catalog-only"), undefined);
      assert.equal(local.endpointFor("unknown"), undefined);
      assert.equal(local.endpointFor("explicit"), undefined);
      const routed = await fetch(`${localBase}/responses`, {
        method: "POST", headers: { "content-type": "application/json" },
        body: JSON.stringify({ model: "vendor/chat", input: "hi" })
      });
      assert.equal(routed.status, 200);
      await routed.text();
      assert.equal(provider.seen.at(-1).url, "/v1/chat/completions");
      assert.equal(provider.seen.at(-1).body.model, "vendor/chat");
      assert.equal(other.seen.length, 0, "neither discovery nor inference can override the single upstream");

      const updated = { ...metadata, name: "Reloaded", capabilities: { context_length: 42 } };
      writeCatalog({ "vendor/chat": updated, "new/model": metadata });
      provider.discovery.body = { data: [{ id: "vendor/chat" }, { id: "new/model" }] };
      assert.deepEqual(await getModels(), expected, "unexpired requests retain the snapshot");
      assert.equal(discoveryCount(), 2);
      expire();
      const reloaded = [{ id: "vendor/chat", object: "model", ...updated }, { id: "new/model", object: "model", ...metadata }];
      const concurrent = await Promise.all([getModels(), getModels(), getModels()]);
      for (const result of concurrent) assert.deepEqual(result, reloaded);
      assert.equal(discoveryCount(), 3, "concurrent GETs share one refresh");
      assert.ok(local.endpointFor("new/model"));

      for (const body of [null, {}, { data: null }, { data: {} }, { error: "unavailable" }, { data: [null] }, { data: [{ id: 7 }] }, { data: [{ id: "vendor/chat" }, {}] }]) {
        provider.discovery.body = body;
        expire();
        assert.deepEqual(await getModels(), reloaded, `malformed response: ${JSON.stringify(body)}`);
        assert.ok(local.endpointFor("new/model"));
      }
      provider.discovery.raw = "{invalid json";
      expire();
      assert.deepEqual(await getModels(), reloaded);
      provider.discovery.raw = undefined;
      provider.discovery.status = 503;
      expire();
      assert.deepEqual(await getModels(), reloaded);

      provider.discovery.status = 200;
      provider.discovery.body = { data: [{ id: "vendor/chat" }] };
      fs.writeFileSync(tempCatalog, "{invalid catalog");
      expire();
      assert.deepEqual(await getModels(), reloaded, "catalog read errors retain the successful snapshot");
      writeCatalog({ "vendor/chat": updated });
      expire();
      assert.deepEqual(await getModels(), [reloaded[0]]);
      assert.equal(local.endpointFor("new/model"), undefined);

      provider.discovery.body = { data: [] };
      expire();
      assert.deepEqual(await getModels(), []);
      assert.equal(local.byId.size, 0);
      provider.discovery.status = 503;
      expire();
      assert.deepEqual(await getModels(), [], "a successful empty list is retained on failure");
      provider.discovery.status = 200;
      provider.discovery.body = [{ id: "vendor/chat" }];
      expire();
      assert.deepEqual(await getModels(), [reloaded[0]], "array-form discovery recovers");
      provider.discovery.body = [];
      expire();
      assert.deepEqual(await getModels(), []);
    } finally {
      await local.stop();
      provider.server.closeAllConnections();
      other.server.closeAllConnections();
      await Promise.all([new Promise((resolve) => provider.server.close(resolve)), new Promise((resolve) => other.server.close(resolve))]);
      fs.rmSync(dir, { recursive: true, force: true });
    }
  });

  await test("bundled catalog ignores alternate catalog environment override", async () => {
    const previous = process.env.CURSOR_LOCAL_MODELS_CATALOG;
    try {
      process.env.CURSOR_LOCAL_MODELS_CATALOG = fixturePath;
      const bundled = new Gateway({ host: "127.0.0.1", port: 0, upstream: up.url });
      assert.equal(bundled.catalogPath, path.join(__dirname, "../out/models.json"));
      assert.deepEqual(bundled.catalog, []);
    } finally {
      if (previous === undefined) delete process.env.CURSOR_LOCAL_MODELS_CATALOG;
      else process.env.CURSOR_LOCAL_MODELS_CATALOG = previous;
    }
  });

  await test("unknown model -> 404 with known ids", async () => {
    const r = await call("nope", true);
    assert.equal(r.status, 404);
    assert.match((await r.json()).error.message, /vendor\/chat/);
  });

  await gw.stop();
  up.server.close();

  console.log(failures ? `\n${failures} failure(s)` : "\nall passed");
  process.exit(failures ? 1 : 0);
})();
