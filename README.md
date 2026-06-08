# Poke Gateway

Connect your Obsidian vault to Poke so Poke can search, read, and, if you allow it, write markdown notes.

The plugin makes an outbound WebSocket connection to the hosted Poke gateway:

```text
wss://obsidian.matt-nz.com/obsidian/sync
```

No inbound ports, tunnels, local web servers, or router changes are required.

## Setup For Users

You do **not** need to host a server, run a tunnel, create a gateway, or make a Cloudflare account.

The plugin is not yet listed in Obsidian's community plugin directory. Until it is approved there, install it manually from the GitHub release:

1. Download the latest release files from `https://github.com/MRL-00/poke-obsidian/releases`:

```text
manifest.json
main.js
styles.css
```

2. In your vault, create this folder:

```text
.obsidian/plugins/poke-gateway
```

3. Move the three downloaded files into that folder.
4. Restart Obsidian or reload plugins.
5. In Obsidian, open `Settings -> Community plugins` and enable `Poke Gateway`.
6. Open `Settings -> Poke Gateway`.
7. Copy the generated `Connection token`.
8. Add the Poke Gateway Recipe in Poke: `https://poke.com/r/uy--WqwhZ9P`.
9. When Poke asks for the `Poke Obsidian` key, paste the Obsidian connection token.
10. Confirm the Gateway URL in Obsidian is:

```text
wss://obsidian.matt-nz.com/obsidian/sync
```

11. Wait for the plugin status to show `Connected`.

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

When creating the template, Poke may ask for an API key to test the connection. Use your own current Obsidian `Connection token` for that test. Poke says this value is used only for connection testing and is not stored on the template.

Then create a Poke Recipe that uses that integration.

Recipe context should say:

```text
Tell the user that the Poke Gateway Obsidian plugin is not yet in Obsidian's community plugin directory.
Tell them to install it manually from the latest GitHub release at https://github.com/MRL-00/poke-obsidian/releases by downloading manifest.json, main.js, and styles.css.
Tell them to create .obsidian/plugins/poke-gateway inside their vault and move those three files into that folder.
Tell them to restart Obsidian or reload plugins, then enable Poke Gateway from Settings -> Community plugins.
Tell them to open Settings -> Poke Gateway and copy the generated Connection token.
Tell them to paste that Connection token into the Poke Obsidian Add Key field.
Then call obsidian_status before reading, searching, or writing vault files.
Do not ask users to invent a token, copy one from GitHub, or use the maintainer API key.
Do not send users to the gateway folder in the GitHub repository; the plugin release files are at the repository releases page.
Write access is off by default; only use writes after the user enables Allow writes.
```

Each user uses their own per-vault Obsidian connection token as the Poke integration key.
