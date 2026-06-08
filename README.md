# Poke-Obsidian

An Obsidian plugin that connects a local vault to Poke over a secure outbound WebSocket.

## What It Does

The plugin connects to a Poke-Obsidian gateway over WebSocket:

```text
wss://api.poke.com/obsidian/sync
```

It supports these gateway actions:

- `list_files`
- `read_file`
- `search_vault`
- `write_file`

Write access is disabled unless the user enables it in plugin settings.

## User Setup

1. Install and enable the `Poke-Obsidian` plugin in Obsidian.
2. Add the Poke-Obsidian Recipe in Poke.
3. Ask Poke to set up Obsidian. Poke will generate a Gateway URL and Connection token for your Poke account.
4. In Obsidian, open Settings -> Poke-Obsidian.
5. Paste the Gateway URL and Connection token from Poke.
6. Wait for the plugin status to show `Connected`.

Users should get their connection token from Poke during Recipe onboarding. They should not create their own token or reuse another user's token.

Write access stays off by default. Enable `Allow writes` only if you want Poke to create or overwrite markdown files in the vault.

## Build

```bash
pnpm install
pnpm build
```

`main.ts` is the TypeScript source. `main.js` is the compiled plugin entrypoint that Obsidian loads, so it is committed for manual installs and plugin-manager workflows.

## Install

Copy these files into an Obsidian vault plugin folder:

```text
manifest.json
main.js
styles.css
```

For example:

```bash
mkdir -p "/path/to/Vault/.obsidian/plugins/poke-obsidian"
cp manifest.json main.js styles.css "/path/to/Vault/.obsidian/plugins/poke-obsidian/"
```

Then enable `Poke-Obsidian` in Obsidian community plugin settings.

## Gateway

This plugin expects a Poke-compatible gateway at `wss://api.poke.com/obsidian/sync`. The gateway is responsible for issuing pairing tokens, accepting outbound plugin connections, and routing Poke MCP tool calls to the connected vault.

A development gateway implementation lives in [`gateway/`](gateway/). It exposes the MCP endpoint for Poke and the WebSocket endpoint for connected plugins.

The gateway exposes an MCP tool named `obsidian_create_connection_token`. Recipes should call that tool during onboarding and give the returned `gatewayUrl` and `connectionToken` to the user.

## Recipe Instructions

Use onboarding copy like this in Poke Kitchen:

```text
When the user starts setup, call obsidian_create_connection_token.
Tell the user to install and enable the Poke-Obsidian plugin in Obsidian.
Give them the returned Gateway URL and Connection token.
Tell them to paste both into Settings -> Poke-Obsidian and wait for Connected.
After they say it is connected, call obsidian_status before reading, searching, or writing vault files.
Do not ask the user to make up a token or paste a token from GitHub.
Write access is off by default. Only use write_file after the user confirms they enabled Allow writes in Obsidian.
```
