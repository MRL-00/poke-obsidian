# Poke Gateway Development Gateway

The gateway exposes:

- `POST /mcp` for Poke MCP calls.
- `GET /obsidian/sync?token=...` for outbound WebSocket connections from the Obsidian plugin.
- `obsidian_create_connection_token` as an MCP tool for creating per-user plugin connection tokens during Recipe onboarding.
- `POST /pairing-tokens` for creating short-lived plugin pairing tokens during development.

## Production Setup

Set these environment variables:

```bash
PUBLIC_BASE_URL=https://your-gateway.example.com
MCP_SERVER_TOKEN=<shared-secret-configured-in-poke>
CONNECTION_TOKEN_SECRET=<long-random-secret-for-signed-plugin-tokens>
```

`CONNECTION_TOKEN_SECRET` enables restart-safe, per-user connection tokens. Without it, generated tokens are short-lived in-memory development tokens.

When creating the Poke integration, point Poke at:

```text
https://your-gateway.example.com/mcp
```

If you set `MCP_SERVER_TOKEN`, configure the same API key in Poke so requests include:

```text
Authorization: Bearer <shared-secret-configured-in-poke>
```

The Recipe should instruct Poke to call `obsidian_create_connection_token` during onboarding, then tell the user to paste the returned `gatewayUrl` and `connectionToken` into Settings -> Poke Gateway.

## Local Dev

```bash
cd gateway
pnpm install
pnpm build
PORT=3001 \
PUBLIC_BASE_URL=http://localhost:3001 \
DEV_CONNECTION_TOKEN=dev-token \
pnpm start
```

Create a pairing token:

```bash
curl -s -X POST http://localhost:3001/pairing-tokens \
  -H 'Content-Type: application/json' \
  -d '{"userId":"dev-user"}'
```

Set the Obsidian plugin gateway URL to:

```text
ws://localhost:3001/obsidian/sync
```

Paste the returned token into the plugin settings. For restart-safe local testing, use `dev-token` when `DEV_CONNECTION_TOKEN=dev-token` is set.

Point Poke at:

```text
http://localhost:3001/mcp
```

For local Poke recipe testing:

```bash
npx poke@latest tunnel http://localhost:3001/mcp -n "Poke Gateway Dev Gateway" --recipe
```

## Auth

Set `MCP_SERVER_TOKEN` to require `Authorization: Bearer <token>` on `/mcp`.

Set `GATEWAY_ADMIN_TOKEN` to require `Authorization: Bearer <token>` on `POST /pairing-tokens`.

Set `CONNECTION_TOKEN_SECRET` in production so Recipe-generated plugin tokens are signed and survive gateway restarts.

## Dev Token Lifetime

Pairing tokens default to 24 hours for development. Override with:

```bash
PAIRING_TOKEN_TTL_MS=86400000
```

For restart-safe local testing, use the stable development connection token:

```bash
DEV_CONNECTION_TOKEN=dev-token
```

Then set the Obsidian plugin connection token to `dev-token`. This bypasses pairing-token expiry and in-memory token loss during gateway restarts.

Do not set `DEV_CONNECTION_TOKEN` in production.
