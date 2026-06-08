import http from "node:http";
import { createMcpExpressApp } from "@modelcontextprotocol/sdk/server/express.js";
import { StreamableHTTPServerTransport } from "@modelcontextprotocol/sdk/server/streamableHttp.js";
import type { NextFunction, Request, Response } from "express";
import { WebSocketServer } from "ws";
import { loadConfig } from "./config.js";
import { createPokeObsidianMcpServer } from "./mcpServer.js";
import { PluginRpc } from "./pluginRpc.js";
import { SessionStore } from "./sessionStore.js";

const config = loadConfig();
const store = new SessionStore(config.pairingTokenTtlMs);
const rpc = new PluginRpc(config.pluginRequestTimeoutMs);
const app = createMcpExpressApp({ host: "0.0.0.0" });
const httpServer = http.createServer(app);
const webSocketServer = new WebSocketServer({
	server: httpServer,
	path: "/obsidian/sync",
});

app.get("/health", (_req: Request, res: Response) => {
	res.json({
		status: "ok",
		...store.getStats(),
	});
});

app.post("/pairing-tokens", requireAdminToken, (req: Request, res: Response) => {
	const body = isRecord(req.body) ? req.body : {};
	const userId = typeof body.userId === "string" && body.userId.trim() ? body.userId.trim() : "dev-user";
	const pairingToken = store.createPairingToken(userId);

	res.json({
		token: pairingToken.token,
		userId: pairingToken.userId,
		expiresAt: new Date(pairingToken.expiresAt).toISOString(),
		gatewayUrl: `${toWebSocketBaseUrl(config.publicBaseUrl)}/obsidian/sync`,
		mcpUrl: `${config.publicBaseUrl}/mcp`,
	});
});

app.use("/mcp", requireMcpToken);

app.post("/mcp", async (req: Request, res: Response) => {
	const userId = getPokeUserId(req);
	const server = createPokeObsidianMcpServer(store, rpc, { userId });
	const transport = new StreamableHTTPServerTransport({
		sessionIdGenerator: undefined,
	});

	try {
		await server.connect(transport);
		await transport.handleRequest(req, res, req.body);

		res.on("close", () => {
			void transport.close();
			void server.close();
		});
	} catch (error) {
		console.error("Error handling MCP request", error);

		if (!res.headersSent) {
			res.status(500).json({
				jsonrpc: "2.0",
				error: {
					code: -32603,
					message: "Internal server error",
				},
				id: null,
			});
		}
	}
});

app.get("/mcp", (_req: Request, res: Response) => {
	res.status(405).json({
		jsonrpc: "2.0",
		error: {
			code: -32000,
			message: "Method not allowed. Use POST for Streamable HTTP.",
		},
		id: null,
	});
});

webSocketServer.on("connection", (socket, request) => {
	const url = new URL(request.url ?? "", config.publicBaseUrl);
	const token = url.searchParams.get("token") ?? "";
	const pluginId = url.searchParams.get("plugin") ?? "unknown";
	const version = url.searchParams.get("version") ?? "unknown";
	const pairingToken = store.getPairingToken(token);

	if (!pairingToken) {
		console.warn(`Rejected Obsidian plugin connection: invalid or expired token (${pluginId}@${version})`);
		socket.close(4001, "Invalid or expired pairing token");
		return;
	}

	store.registerSession(pairingToken.userId, socket, {
		pluginId,
		version,
	});
	console.log(`Obsidian plugin connected: user=${pairingToken.userId} plugin=${pluginId} version=${version}`);

	socket.on("close", () => {
		store.removeSession(pairingToken.userId, socket);
		console.log(`Obsidian plugin disconnected: user=${pairingToken.userId}`);
	});
});

httpServer.listen(config.port, () => {
	console.log(`Poke-Obsidian Gateway listening on ${config.publicBaseUrl}`);
	console.log(`MCP endpoint: ${config.publicBaseUrl}/mcp`);
	console.log(`Plugin WebSocket endpoint: ${toWebSocketBaseUrl(config.publicBaseUrl)}/obsidian/sync`);
});

function requireMcpToken(req: Request, res: Response, next: NextFunction): void {
	requireBearerToken(config.mcpServerToken, req, res, next);
}

function requireAdminToken(req: Request, res: Response, next: NextFunction): void {
	requireBearerToken(config.adminToken, req, res, next);
}

function requireBearerToken(expectedToken: string, req: Request, res: Response, next: NextFunction): void {
	if (!expectedToken) {
		next();
		return;
	}

	if (req.header("Authorization") !== `Bearer ${expectedToken}`) {
		res.status(401).json({ error: "Unauthorized" });
		return;
	}

	next();
}

function getPokeUserId(req: Request): string {
	const userId = req.header("X-Poke-User-Id") ?? req.header("x-poke-user-id");

	if (userId?.trim()) {
		return userId.trim();
	}

	return "dev-user";
}

function toWebSocketBaseUrl(publicBaseUrl: string): string {
	if (publicBaseUrl.startsWith("https://")) {
		return `wss://${publicBaseUrl.slice("https://".length)}`;
	}

	if (publicBaseUrl.startsWith("http://")) {
		return `ws://${publicBaseUrl.slice("http://".length)}`;
	}

	return publicBaseUrl;
}

function isRecord(value: unknown): value is Record<string, unknown> {
	return typeof value === "object" && value !== null && !Array.isArray(value);
}
