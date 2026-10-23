# Cursor Local Models

A local loopback gateway for **Cursor Private Inference (Cursor PI)** that lets you use your own OpenAI-compatible, Anthropic, or local model server (OpenRouter, LiteLLM, vLLM, Ollama, LM Studio, llama.cpp) with Cursor's built-in Agent loop, tools, checkpoints, and MCP.

## Why you need this

Pointing Cursor Private Inference directly at a standard provider or local server runs into three problems:

1. **Missing model capabilities (4,096 token cap).** Cursor PI probes `GET /v1/models` for a custom `api_types` and `capabilities` schema. Standard OpenAI-compatible servers only return `{ id, object, owned_by }`, causing Cursor to either hide the models or fall back to a hardcoded **4,096 `max_output_tokens`** limit and wrong context/vision/reasoning settings.
2. **API dialect mismatch.** Cursor's local agent loop speaks the OpenAI **Responses** API (`/v1/responses`), while most providers and local servers expose **Chat Completions** (`/v1/chat/completions`) or **Anthropic Messages** (`/v1/messages`).
3. **Stalled chat summarization.** When a conversation grows long, Cursor summarizes prior turns using the active model's reasoning settings. On thinking models, background summarization can burn tens of thousands of reasoning tokens and stall the chat on *"Summarizing chat context"*.

**Cursor Local Models** runs a lightweight gateway on `http://127.0.0.1:41100/v1` between Cursor PI and your provider:

- **Enriches `/v1/models`**: Fetches the live model list from your provider and attaches accurate `api_types`, `context_length`, `max_output_tokens`, tool-calling, reasoning, and vision metadata from the bundled `out/models.json` catalog.
- **Translates APIs on the fly**: Converts text, reasoning summaries, and tool calls between OpenAI Responses, Chat Completions, and Anthropic Messages (streaming and non-streaming).
- **Speeds up context summaries**: Automatically strips extended thinking and caps output tokens on Cursor's `<summarization_request>` turns so long chats don't hang.

## Setup

1. Install the extension in Cursor Private Inference.
2. Set `cursorLocalModels.upstream` to your provider's base URL, including `/v1` (e.g. `http://127.0.0.1:11434/v1`).
3. If your provider requires authentication, run **Cursor Local Models: Set API key** from the Command Palette (stored in VS Code `SecretStorage`), or set the environment variable named in `cursorLocalModels.apiKeyEnv` (`CURSOR_LOCAL_MODELS_API_KEY`).
4. In Cursor PI, open **Settings → Models → Gateway** and set:
   - **Base URL**: `http://127.0.0.1:41100/v1`
   - **API Key**: `local` (when the extension holds the real provider key)
5. Refresh the model list in Cursor PI and pick a model.

## How model discovery works

On startup (and on `GET /v1/models` after a 60-second cache TTL), the gateway fetches your provider's `/models` list and intersects it with `out/models.json` by **exact, case-sensitive model ID**:

- **Provider `/models`** determines which models are currently available.
- **`out/models.json`** determines each model's `api_types` and `capabilities`.
- Only models present in both are advertised to Cursor. To add a new model, add its exact provider ID and capabilities to `out/models.json`.

## Commands

| Command | Description |
|---|---|
| `Cursor Local Models: Set API key` | Save or clear the upstream provider API key in `SecretStorage` and refresh discovery. |
| `Cursor Local Models: Show status` | Show the gateway URL, upstream status, and matched catalog models in the Output panel. |
| `Cursor Local Models: Restart gateway` | Restart the loopback server and immediately reload `out/models.json` and `/models`. |

## Settings

| Setting | Default | Description |
|---|---|---|
| `cursorLocalModels.enabled` | `true` | Enable the loopback gateway. |
| `cursorLocalModels.upstream` | `""` | Provider base URL, including `/v1` (or set `CURSOR_LOCAL_MODELS_UPSTREAM`). |
| `cursorLocalModels.port` | `41100` | Loopback port on `127.0.0.1`. |
| `cursorLocalModels.apiKeyEnv` | `"CURSOR_LOCAL_MODELS_API_KEY"` | Fallback environment variable for the provider API key when none is stored in `SecretStorage`. |

## Security & limitations

- **Loopback only:** The gateway binds strictly to `127.0.0.1`. API keys are stored in `SecretStorage` or read from environment variables, never written to `settings.json`.
- **Agent & Chat only:** This extension powers Cursor's Agent/Chat models. For inline ghost-text completions (since Cursor Tab is disabled in Private Inference), pair it with the **Local Autocomplete** extension.
