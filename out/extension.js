"use strict";

const vscode = require("vscode");
const { Gateway } = require("./gateway");

// The gateway can spend the provider's credentials, so it must stay loopback-only.
const HOST = "127.0.0.1";
const UPSTREAM_SECRET = "cursorLocalModels.key.upstream";
let gateway;
let output;
let secrets;
let restartQueue = Promise.resolve();
let shuttingDown = false;

function cfg() {
  const c = vscode.workspace.getConfiguration("cursorLocalModels");
  return {
    enabled: c.get("enabled", true),
    upstream: String(c.get("upstream", "") || process.env.CURSOR_LOCAL_MODELS_UPSTREAM || "").trim(),
    port: c.get("port", 41100),
    apiKeyEnv: c.get("apiKeyEnv", "CURSOR_LOCAL_MODELS_API_KEY"),
    host: HOST
  };
}

function log(message) {
  output?.info(message);
}

async function resolveApiKey(ep) {
  return (await secrets.get(UPSTREAM_SECRET)) || (ep.apiKeyEnv ? process.env[ep.apiKeyEnv] : undefined);
}

async function replaceGateway() {
  if (gateway) {
    await gateway.stop();
    gateway = undefined;
  }
  if (shuttingDown) return;
  const c = cfg();
  if (!c.enabled) {
    log("gateway disabled by configuration");
    return;
  }
  if (!c.upstream) {
    log("Set cursorLocalModels.upstream to the provider base URL, including /v1.");
    return;
  }
  const url = new URL(c.upstream);
  if (!["http:", "https:"].includes(url.protocol) || url.username || url.password || url.search || url.hash) {
    throw new Error("Provider URL must use HTTP(S), without embedded credentials, query parameters, or fragments.");
  }
  if (["127.0.0.1", "localhost", "[::1]"].includes(url.hostname) && Number(url.port || (url.protocol === "https:" ? 443 : 80)) === c.port) {
    throw new Error("The provider URL must not point back to this gateway.");
  }
  const next = new Gateway(c, log, resolveApiKey);
  try {
    await next.start();
    gateway = next;
  } catch (error) {
    await next.stop();
    throw error;
  }
}

function restartGateway() {
  // Settings events and palette commands may overlap; finish releasing the old port first.
  const pending = restartQueue.then(replaceGateway);
  restartQueue = pending.catch((error) => log(`gateway restart failed: ${error.message}`));
  return pending;
}

async function setApiKey() {
  const stored = !!(await secrets.get(UPSTREAM_SECRET));
  const value = await vscode.window.showInputBox({
    title: "Provider API key (empty to clear)",
    prompt: stored ? "A key is stored. Enter a replacement, or leave empty to clear it." : "Stored in SecretStorage, never in settings.",
    password: true,
    ignoreFocusOut: true
  });
  if (value === undefined) return;
  if (value.trim()) await secrets.store(UPSTREAM_SECRET, value.trim());
  else await secrets.delete(UPSTREAM_SECRET);
  // Discovery needs the provider key too; don't leave a failed unauthenticated probe cached.
  await restartGateway();
  vscode.window.showInformationMessage("Cursor Local Models: key updated. Refresh models in Cursor PI.");
}

function statusText() {
  const c = cfg();
  return [
    "Cursor Local Models — Cursor PI gateway",
    `gateway: ${gateway ? `http://${HOST}:${gateway.port()}/v1` : "stopped"}`,
    `provider configured: ${c.upstream ? "yes" : "no"}`,
    `API key environment: ${c.apiKeyEnv} (${process.env[c.apiKeyEnv] ? "set" : "not set"}; stored key takes precedence)`,
    `catalog: ${gateway ? gateway.catalogPath : "out/models.json"}`,
    `available catalog models: ${gateway ? gateway.catalog.length : 0}`,
    ...(gateway ? gateway.catalog.map((model) => `  ${model.id}`) : []),
    "Configure Cursor PI's Gateway base URL to point to this gateway. No TOML or launcher is required."
  ].join("\n");
}

async function activate(context) {
  shuttingDown = false;
  secrets = context.secrets;
  output = vscode.window.createOutputChannel("Cursor Local Models", { log: true });
  context.subscriptions.push(output);
  context.subscriptions.push(
    vscode.commands.registerCommand("cursorLocalModels.setApiKey", setApiKey),
    vscode.commands.registerCommand("cursorLocalModels.status", () => {
      output.show(true);
      log(statusText());
    }),
    vscode.commands.registerCommand("cursorLocalModels.restartGateway", async () => {
      await restartGateway();
      log(statusText());
    }),
    vscode.workspace.onDidChangeConfiguration((event) => {
      if (event.affectsConfiguration("cursorLocalModels")) {
        restartGateway().catch(() => {});
      }
    })
  );
  await restartGateway();
}

async function deactivate() {
  shuttingDown = true;
  await restartQueue;
  if (gateway) await gateway.stop();
  gateway = undefined;
}

module.exports = { activate, deactivate };
