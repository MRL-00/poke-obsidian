# Poke Gateway

Connect your Obsidian vault to Poke so Poke can search, read, and, if you allow it, write markdown notes.

The plugin makes an outbound WebSocket connection to the hosted Poke gateway:

```text
wss://obsidian.matt-nz.com/obsidian/sync
```

No inbound ports, tunnels, local web servers, or router changes are required.

## Setup For Users

You do **not** need to host a server, run a tunnel, create a gateway, or make a Cloudflare account.

1. Install and enable the `Poke Gateway` plugin in Obsidian.
2. In Obsidian, open `Settings -> Poke Gateway`.
3. Copy the generated `Connection token`.
4. Add the Poke Gateway Recipe in Poke: `https://poke.com/r/uy--WqwhZ9P`.
5. When Poke asks for the `Poke Obsidian` key, paste the Obsidian connection token.
6. Confirm the Gateway URL in Obsidian is:

```text
wss://obsidian.matt-nz.com/obsidian/sync
```

7. Wait for the plugin status to show `Connected`.

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

Create one Poke MCP integration template:

```text
Name: Poke Obsidian
Server URL: https://obsidian.matt-nz.com/mcp
Authentication: API key required
```

Then create a Poke Recipe that uses that integration.

Recipe context should say:

```text
Tell the user to install and enable the Poke Gateway plugin in Obsidian.
Tell them to open Settings -> Poke Gateway and copy the generated Connection token.
Tell them to paste that Connection token into the Poke Obsidian Add Key field.
Then call obsidian_status before reading, searching, or writing vault files.
Do not ask users to invent a token, copy one from GitHub, or use the maintainer API key.
Write access is off by default; only use writes after the user enables Allow writes.
```

Each user uses their own per-vault Obsidian connection token as the Poke integration key.
