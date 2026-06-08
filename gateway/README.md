# Poke-Obsidian Gateway

The gateway exposes:

- `POST /mcp` for Poke MCP calls.
- `GET /obsidian/sync?token=...` for outbound WebSocket connections from the Obsidian plugin.
- `POST /pairing-tokens` for creating short-lived plugin pairing tokens during development.

## Local Dev

```bash
cd gateway
pnpm install
pnpm build
pnpm start
```

Create a pairing token:

```bash
curl -s -X POST http://localhost:3000/pairing-tokens \
  -H 'Content-Type: application/json' \
  -d '{"userId":"dev-user"}'
```

Set the Obsidian plugin gateway URL to:

```text
ws://localhost:3000/obsidian/sync
```

Paste the returned token into the plugin settings.

Point Poke at:

```text
http://localhost:3000/mcp
```

For local Poke recipe testing:

```bash
npx poke@latest tunnel http://localhost:3000/mcp -n "Poke-Obsidian Gateway" --recipe
```

## Auth

Set `MCP_SERVER_TOKEN` to require `Authorization: Bearer <token>` on `/mcp`.

Set `GATEWAY_ADMIN_TOKEN` to require `Authorization: Bearer <token>` on `POST /pairing-tokens`.

## Dev Token Lifetime

Pairing tokens default to 24 hours for development. Override with:

```bash
PAIRING_TOKEN_TTL_MS=86400000
```
