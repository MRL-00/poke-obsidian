# Poke Gateway

Connect your Obsidian vault to Poke so Poke can search, read, and, if you allow it, write markdown notes.

The plugin makes an outbound WebSocket connection to the hosted Poke gateway:

```text
wss://obsidian.matt-nz.com/obsidian/sync
```

No inbound ports, tunnels, local web servers, or router changes are required.

## Setup For Users

You do **not** need to create a Poke integration, API key, MCP server, or gateway. That is already handled by the shared Poke Recipe.

1. Install and enable the `Poke Gateway` plugin in Obsidian.
2. Add the Poke Gateway Recipe in Poke: `https://poke.com/r/uy--WqwhZ9P`.
3. Ask Poke to set up Obsidian.
4. Poke will generate a connection token for your Poke account.
5. In Obsidian, open `Settings -> Poke Gateway`.
6. Paste the connection token Poke gives you.
7. Confirm the Gateway URL is:

```text
wss://obsidian.matt-nz.com/obsidian/sync
```

8. Wait for the plugin status to show `Connected`.

After that, ask Poke to list, search, or read notes from your vault.

Write access is off by default. Turn on `Allow writes` only if you want Poke to create or overwrite markdown files in the vault.

## What Poke Can Do

The connected gateway supports:

- `list_files`
- `read_file`
- `search_vault`
- `write_file`

Only markdown files are supported. Write requests are rejected unless `Allow writes` is enabled in Obsidian.

## Install Manually

Copy these files into an Obsidian vault plugin folder:

```text
manifest.json
main.js
styles.css
```

For example:

```bash
mkdir -p "/path/to/Vault/.obsidian/plugins/poke-gateway"
cp manifest.json main.js styles.css "/path/to/Vault/.obsidian/plugins/poke-gateway/"
```

Then enable `Poke Gateway` in Obsidian community plugin settings.

## Build

```bash
pnpm install
pnpm build
```

`main.ts` is the TypeScript source. `main.js` is the compiled plugin entrypoint that Obsidian loads, so it is committed for manual installs and plugin-manager workflows.

## Maintainer Setup

Only the Recipe owner needs this section.

Create one shared Poke MCP integration template:

```text
Name: Obsidian
Server URL: https://obsidian.matt-nz.com/mcp
Authentication: Bearer/API key using the Cloudflare MCP_SERVER_TOKEN secret
```

Then create a Poke Recipe that uses that integration as a shared integration.

Recipe context should say:

```text
When onboarding, call obsidian_create_connection_token.
Tell the user to install and enable the Poke Gateway plugin in Obsidian.
Give them the returned Gateway URL and Connection token.
Tell them to paste both into Settings -> Poke Gateway and wait for Connected.
Then call obsidian_status before reading, searching, or writing vault files.
Do not ask users to invent a token or copy one from GitHub.
Write access is off by default; only use writes after the user enables Allow writes.
```

The user should never see or enter the shared MCP integration API key. They only receive their own per-user connection token from Poke during onboarding.
