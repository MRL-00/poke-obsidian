# Poke-Obsidian Cloudflare Worker

This is the production gateway target for Poke-Obsidian.

It runs as:

- a Cloudflare Worker for `/mcp`, `/health`, and `/obsidian/sync`
- a Durable Object per Poke user for the live Obsidian WebSocket

## Deploy

```bash
cd worker
pnpm install
pnpm deploy
```

The current production endpoint is:

```text
https://obsidian.matt-nz.com/mcp
```

Set production secrets:

```bash
printf '%s' '<long-random-secret>' | pnpm exec wrangler secret put CONNECTION_TOKEN_SECRET
printf '%s' '<shared-secret-configured-in-poke>' | pnpm exec wrangler secret put MCP_SERVER_TOKEN
```

## Poke Integration

Only the Recipe owner creates this integration. End users do not create a Poke MCP integration or enter this API key.

Create a shared Poke MCP integration template with:

```text
Name: Obsidian
URL: https://obsidian.matt-nz.com/mcp
API key: <same value as MCP_SERVER_TOKEN>
```

The Recipe onboarding should tell Poke to call `obsidian_create_connection_token`. Poke should give the returned `gatewayUrl` and `connectionToken` to the user, and the user pastes those into Settings -> Poke-Obsidian.

## User Flow

1. User installs and enables the Poke-Obsidian plugin in Obsidian.
2. User adds the Poke Recipe.
3. Poke calls `obsidian_create_connection_token`.
4. User pastes the returned Gateway URL and Connection token into the plugin settings.
5. Plugin status changes to `Connected`.
