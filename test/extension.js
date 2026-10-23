"use strict";

const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");
const vm = require("node:vm");

(async () => {
  const commands = new Map();
  const settings = { enabled: true, upstream: "http://127.0.0.1:11434/v1", port: 41100, apiKeyEnv: "TEST_PROVIDER_KEY" };
  const stored = new Map();
  const instances = [];
  let changed;
  let input;
  let active = 0;
  const output = { info() {}, show() {}, dispose() {} };
  class MockGateway {
    constructor(config, log, resolveKey) {
      this.cfg = config;
      this.resolveKey = resolveKey;
      this.catalogPath = "out/models.json";
      this.catalog = [{ id: "vendor/model" }];
      instances.push(this);
    }
    async start() {
      assert.equal(active, 0, "restarts must release the previous listener");
      active++;
      await Promise.resolve();
    }
    async stop() { active--; }
    port() { return this.cfg.port; }
  }
  const vscode = {
    workspace: {
      getConfiguration: () => ({ get: (key, fallback) => settings[key] ?? fallback }),
      onDidChangeConfiguration: (fn) => { changed = fn; return { dispose() {} }; }
    },
    window: {
      createOutputChannel: () => output,
      showInputBox: async () => input,
      showInformationMessage() {}
    },
    commands: {
      registerCommand: (id, fn) => { commands.set(id, fn); return { dispose() {} }; }
    }
  };
  const sandbox = {
    module: { exports: {} },
    process: { env: { TEST_PROVIDER_KEY: "test-env-key" } },
    URL,
    // Any filesystem import in the entry point fails: activation and status must be read-only.
    require(name) {
      if (name === "vscode") return vscode;
      if (name === "./gateway") return { Gateway: MockGateway };
      throw new Error(`Unexpected entry-point dependency: ${name}`);
    }
  };
  vm.runInNewContext(fs.readFileSync(path.join(__dirname, "../out/extension.js"), "utf8"), sandbox);
  const extension = sandbox.module.exports;
  const context = {
    subscriptions: [],
    secrets: {
      get: async (key) => stored.get(key),
      store: async (key, value) => stored.set(key, value),
      delete: async (key) => stored.delete(key)
    }
  };
  await extension.activate(context);
  assert.equal(instances.length, 1);
  assert.equal(commands.has("cursorLocalModels.writeConfig"), false);
  await commands.get("cursorLocalModels.status")();
  assert.equal(instances.length, 1, "status must not restart or construct a gateway");
  await Promise.all([commands.get("cursorLocalModels.restartGateway")(), commands.get("cursorLocalModels.restartGateway")()]);
  assert.equal(active, 1);
  assert.equal(await instances.at(-1).resolveKey({ apiKeyEnv: "TEST_PROVIDER_KEY" }), "test-env-key");
  input = "test-stored-key";
  await commands.get("cursorLocalModels.setApiKey")();
  assert.equal(await instances.at(-1).resolveKey({ apiKeyEnv: "TEST_PROVIDER_KEY" }), input);
  input = "";
  await commands.get("cursorLocalModels.setApiKey")();
  assert.equal(await instances.at(-1).resolveKey({ apiKeyEnv: "TEST_PROVIDER_KEY" }), "test-env-key");
  settings.enabled = false;
  changed({ affectsConfiguration: () => true });
  await extension.deactivate();
  assert.equal(active, 0);
  console.log("ok   extension lifecycle: no filesystem dependencies, read-only status, serialized restarts, key precedence");
})().catch((error) => { console.error(error); process.exitCode = 1; });
