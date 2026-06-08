# Poke-Obsidian

An Obsidian plugin that connects a local vault to Poke over a secure outbound WebSocket.

## What It Does

The plugin connects to:

```text
wss://api.poke.com/obsidian/sync
```

It supports these gateway actions:

- `list_files`
- `read_file`
- `search_vault`
- `write_file`

Write access is disabled unless the user enables it in plugin settings.

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
