export interface Env {
	OBSIDIAN_SESSION: DurableObjectNamespace;
	CONNECTION_TOKEN_SECRET?: string;
	MCP_SERVER_TOKEN?: string;
	PUBLIC_BASE_URL?: string;
	PLUGIN_REQUEST_TIMEOUT_MS?: string;
}

interface JsonRpcRequest {
	jsonrpc?: unknown;
	id?: unknown;
	method?: unknown;
	params?: unknown;
}

interface PluginResponse {
	id?: unknown;
	status?: unknown;
	payload?: unknown;
}

interface PendingPluginRequest {
	resolve: (payload: Record<string, unknown>) => void;
	reject: (error: Error) => void;
	timeoutId: number;
}

const DEFAULT_PLUGIN_REQUEST_TIMEOUT_MS = 30_000;
const TOKEN_PREFIX = "pkobs";

const tools = [
	{
		name: "obsidian_create_connection_token",
		description: "Create a connection token and gateway URL for pairing the current Poke user with the Poke Gateway plugin.",
		inputSchema: { type: "object", properties: {}, additionalProperties: false },
	},
	{
		name: "obsidian_status",
		description: "Check whether this Poke user has an Obsidian vault connected.",
		inputSchema: { type: "object", properties: {}, additionalProperties: false },
	},
	{
		name: "obsidian_list_files",
		description: "List all markdown file paths in the connected user's Obsidian vault.",
		inputSchema: { type: "object", properties: {}, additionalProperties: false },
	},
	{
		name: "obsidian_read_file",
		description: "Read a markdown file from the connected user's Obsidian vault.",
		inputSchema: {
			type: "object",
			properties: {
				path: { type: "string", description: "Vault-relative markdown path, for example Projects/Plan.md." },
			},
			required: ["path"],
			additionalProperties: false,
		},
	},
	{
		name: "obsidian_search_vault",
		description: "Search markdown files in the connected user's Obsidian vault.",
		inputSchema: {
			type: "object",
			properties: {
				query: { type: "string", description: "Text to search for." },
			},
			required: ["query"],
			additionalProperties: false,
		},
	},
	{
		name: "obsidian_write_file",
		description: "Create or overwrite a markdown file in the connected user's Obsidian vault. The plugin must have writes enabled.",
		inputSchema: {
			type: "object",
			properties: {
				path: { type: "string", description: "Vault-relative markdown path, for example Inbox/Poke.md." },
				content: { type: "string", description: "Complete markdown content to write." },
			},
			required: ["path", "content"],
			additionalProperties: false,
		},
	},
];

export default {
	async fetch(request: Request, env: Env): Promise<Response> {
		const url = new URL(request.url);

		if (request.method === "OPTIONS") {
			return new Response(null, { status: 204, headers: corsHeaders() });
		}

		if (url.pathname === "/health") {
			return jsonResponse({ status: "ok" });
		}

		if (url.pathname === "/obsidian/sync") {
			return handlePluginWebSocket(request, env);
		}

		if (isMcpPath(url.pathname)) {
			return handleMcpRequest(request, env);
		}

		return jsonResponse({ error: "Not found" }, 404);
	},
} satisfies ExportedHandler<Env>;

export class ObsidianSession {
	private pending = new Map<string, PendingPluginRequest>();

	constructor(
		private readonly state: DurableObjectState,
		private readonly env: Env
	) {}

	async fetch(request: Request): Promise<Response> {
		const url = new URL(request.url);

		if (url.pathname === "/connect") {
			return this.connect();
		}

		if (url.pathname === "/status") {
			return jsonResponse(this.getStatus());
		}

		if (url.pathname === "/rpc") {
			const body = (await request.json()) as unknown;

			if (!isRecord(body) || typeof body.action !== "string" || !isRecord(body.params)) {
				return jsonResponse({ error: "Invalid RPC request" }, 400);
			}

			try {
				return jsonResponse(await this.callPlugin(body.action, body.params));
			} catch (error) {
				return jsonResponse({ error: error instanceof Error ? error.message : String(error) }, 502);
			}
		}

		return jsonResponse({ error: "Not found" }, 404);
	}

	webSocketMessage(_socket: WebSocket, message: string | ArrayBuffer): void {
		if (typeof message !== "string") {
			return;
		}

		let response: PluginResponse;

		try {
			response = JSON.parse(message) as PluginResponse;
		} catch {
			return;
		}

		const id = typeof response.id === "string" ? response.id : "";
		const pending = this.pending.get(id);

		if (!pending) {
			return;
		}

		clearTimeout(pending.timeoutId);
		this.pending.delete(id);

		if (response.status === "success") {
			pending.resolve(isRecord(response.payload) ? response.payload : {});
			return;
		}

		const payload = isRecord(response.payload) ? response.payload : {};
		const error = typeof payload.error === "string" ? payload.error : "Plugin request failed";
		pending.reject(new Error(error));
	}

	webSocketClose(): void {
		this.rejectAllPending("Obsidian plugin disconnected");
	}

	webSocketError(): void {
		this.rejectAllPending("Obsidian plugin socket errored");
	}

	private connect(): Response {
		const pair = new WebSocketPair();
		const client = pair[0];
		const server = pair[1];

		for (const socket of this.state.getWebSockets()) {
			socket.close(4000, "Replaced by a newer Poke Gateway connection");
		}

		this.state.acceptWebSocket(server);

		return new Response(null, {
			status: 101,
			webSocket: client,
		});
	}

	private getStatus(): Record<string, unknown> {
		const sockets = this.state.getWebSockets();

		return {
			connected: sockets.length > 0,
			connectedPlugins: sockets.length,
		};
	}

	private async callPlugin(action: string, params: Record<string, unknown>): Promise<Record<string, unknown>> {
		const socket = this.state.getWebSockets()[0];

		if (!socket) {
			throw new Error("No Obsidian vault is connected for this Poke user");
		}

		const id = `msg_${crypto.randomUUID()}`;
		const timeoutMs = readNumber(this.env.PLUGIN_REQUEST_TIMEOUT_MS, DEFAULT_PLUGIN_REQUEST_TIMEOUT_MS);

		const response = new Promise<Record<string, unknown>>((resolve, reject) => {
			const timeoutId = setTimeout(() => {
				this.pending.delete(id);
				reject(new Error("Plugin request timed out"));
			}, timeoutMs);

			this.pending.set(id, { resolve, reject, timeoutId });
		});

		socket.send(JSON.stringify({ id, action, params }));

		return response;
	}

	private rejectAllPending(message: string): void {
		for (const [id, pending] of this.pending) {
			clearTimeout(pending.timeoutId);
			pending.reject(new Error(message));
			this.pending.delete(id);
		}
	}
}

async function handlePluginWebSocket(request: Request, env: Env): Promise<Response> {
	if (request.headers.get("Upgrade")?.toLowerCase() !== "websocket") {
		return jsonResponse({ error: "Expected WebSocket upgrade" }, 426);
	}

	if (!env.CONNECTION_TOKEN_SECRET) {
		return jsonResponse({ error: "CONNECTION_TOKEN_SECRET is required" }, 500);
	}

	const url = new URL(request.url);
	const token = url.searchParams.get("token") ?? "";
	const verifiedToken = await verifyConnectionToken(env.CONNECTION_TOKEN_SECRET, token);

	if (!verifiedToken) {
		return jsonResponse({ error: "Invalid connection token" }, 401);
	}

	const connectUrl = new URL(request.url);
	connectUrl.pathname = "/connect";
	connectUrl.search = "";

	const stub = env.OBSIDIAN_SESSION.get(env.OBSIDIAN_SESSION.idFromName(verifiedToken.userId));
	return stub.fetch(new Request(connectUrl, request));
}

async function handleMcpRequest(request: Request, env: Env): Promise<Response> {
	if (request.method !== "POST") {
		return mcpErrorResponse(request, null, -32000, "Method not allowed. Use POST for Streamable HTTP.", 405);
	}

	if (!isAuthorized(request, env)) {
		return jsonResponse({ error: "Unauthorized" }, 401);
	}

	let rpcRequest: JsonRpcRequest;

	try {
		rpcRequest = (await request.json()) as JsonRpcRequest;
	} catch {
		return mcpErrorResponse(request, null, -32700, "Parse error", 400);
	}

	const id = typeof rpcRequest.id === "string" || typeof rpcRequest.id === "number" ? rpcRequest.id : null;
	const method = typeof rpcRequest.method === "string" ? rpcRequest.method : "";

	try {
		if (method === "initialize") {
			return mcpResultResponse(request, id, {
				protocolVersion: "2025-06-18",
				capabilities: { tools: {} },
				serverInfo: { name: "poke-obsidian-gateway", version: "0.1.0" },
			});
		}

		if (method.startsWith("notifications/")) {
			return new Response(null, { status: 202, headers: corsHeaders() });
		}

		if (method === "ping") {
			return mcpResultResponse(request, id, {});
		}

		if (method === "tools/list") {
			return mcpResultResponse(request, id, { tools });
		}

		if (method === "tools/call") {
			return mcpResultResponse(request, id, await handleToolCall(request, env, rpcRequest.params));
		}

		return mcpErrorResponse(request, id, -32601, `Method not found: ${method || "(missing)"}`, 404);
	} catch (error) {
		return mcpErrorResponse(request, id, -32603, error instanceof Error ? error.message : String(error), 500);
	}
}

async function handleToolCall(request: Request, env: Env, params: unknown): Promise<Record<string, unknown>> {
	if (!isRecord(params) || typeof params.name !== "string") {
		throw new Error("Tool name is required");
	}

	const args = isRecord(params.arguments) ? params.arguments : {};
	const userId = getPokeUserId(request);

	switch (params.name) {
		case "obsidian_create_connection_token":
			return toJsonToolResult(await createConnectionInstructions(request, env, userId));
		case "obsidian_status":
			return toJsonToolResult(await callSession(env, userId, "/status"));
		case "obsidian_list_files":
			return toJsonToolResult(await callPlugin(env, userId, "list_files", {}));
		case "obsidian_read_file":
			return toJsonToolResult(await callPlugin(env, userId, "read_file", args));
		case "obsidian_search_vault":
			return toJsonToolResult(await callPlugin(env, userId, "search_vault", args));
		case "obsidian_write_file":
			return toJsonToolResult(await callPlugin(env, userId, "write_file", args));
		default:
			throw new Error(`Unknown tool: ${params.name}`);
	}
}

async function createConnectionInstructions(request: Request, env: Env, userId: string): Promise<Record<string, unknown>> {
	if (!env.CONNECTION_TOKEN_SECRET) {
		throw new Error("CONNECTION_TOKEN_SECRET is required");
	}

	const publicBaseUrl = (env.PUBLIC_BASE_URL || new URL(request.url).origin).replace(/\/+$/, "");

	return {
		connectionToken: await createConnectionToken(env.CONNECTION_TOKEN_SECRET, userId),
		gatewayUrl: `${toWebSocketBaseUrl(publicBaseUrl)}/obsidian/sync`,
		expiresAt: null,
		instructions: [
			"Install and enable the Poke Gateway plugin in Obsidian.",
			"Open Obsidian Settings, then Poke Gateway.",
			"Paste the Gateway URL and Connection token.",
			"Wait for the status to show Connected.",
		],
	};
}

async function callPlugin(env: Env, userId: string, action: string, params: Record<string, unknown>): Promise<Record<string, unknown>> {
	return callSession(env, userId, "/rpc", { action, params });
}

async function callSession(env: Env, userId: string, path: string, body?: Record<string, unknown>): Promise<Record<string, unknown>> {
	const stub = env.OBSIDIAN_SESSION.get(env.OBSIDIAN_SESSION.idFromName(userId));
	const response = await stub.fetch(`https://session.local${path}`, {
		method: body ? "POST" : "GET",
		headers: body ? { "Content-Type": "application/json" } : undefined,
		body: body ? JSON.stringify(body) : undefined,
	});
	const payload = (await response.json()) as unknown;

	if (!response.ok) {
		const error = isRecord(payload) && typeof payload.error === "string" ? payload.error : "Session request failed";
		throw new Error(error);
	}

	return isRecord(payload) ? payload : {};
}

async function createConnectionToken(secret: string, userId: string): Promise<string> {
	const payload = toBase64Url(
		new TextEncoder().encode(
			JSON.stringify({
				userId,
				issuedAt: Date.now(),
			})
		)
	);
	const signature = await sign(secret, payload);

	return `${TOKEN_PREFIX}_${payload}.${signature}`;
}

async function verifyConnectionToken(secret: string, token: string): Promise<{ userId: string; issuedAt: number } | null> {
	if (!token.startsWith(`${TOKEN_PREFIX}_`)) {
		return null;
	}

	const unsignedToken = token.slice(`${TOKEN_PREFIX}_`.length);
	const separatorIndex = unsignedToken.lastIndexOf(".");

	if (separatorIndex === -1) {
		return null;
	}

	const payload = unsignedToken.slice(0, separatorIndex);
	const signature = unsignedToken.slice(separatorIndex + 1);

	if (signature !== (await sign(secret, payload))) {
		return null;
	}

	try {
		const parsed = JSON.parse(new TextDecoder().decode(fromBase64Url(payload))) as unknown;

		if (!isRecord(parsed) || typeof parsed.userId !== "string" || typeof parsed.issuedAt !== "number") {
			return null;
		}

		return {
			userId: parsed.userId,
			issuedAt: parsed.issuedAt,
		};
	} catch {
		return null;
	}
}

async function sign(secret: string, payload: string): Promise<string> {
	const key = await crypto.subtle.importKey(
		"raw",
		new TextEncoder().encode(secret),
		{ name: "HMAC", hash: "SHA-256" },
		false,
		["sign"]
	);
	const signature = await crypto.subtle.sign("HMAC", key, new TextEncoder().encode(payload));

	return toBase64Url(new Uint8Array(signature));
}

function mcpResultResponse(request: Request, id: string | number | null, result: Record<string, unknown>): Response {
	return mcpResponse(request, { jsonrpc: "2.0", id, result });
}

function mcpErrorResponse(
	request: Request,
	id: string | number | null,
	code: number,
	message: string,
	status = 500
): Response {
	return mcpResponse(request, { jsonrpc: "2.0", id, error: { code, message } }, status);
}

function mcpResponse(request: Request, payload: Record<string, unknown>, status = 200): Response {
	return jsonResponse(payload, status);
}

function toJsonToolResult(value: unknown): Record<string, unknown> {
	return {
		content: [
			{
				type: "text",
				text: JSON.stringify(value, null, 2),
			},
		],
	};
}

function isAuthorized(request: Request, env: Env): boolean {
	if (!env.MCP_SERVER_TOKEN) {
		return true;
	}

	return request.headers.get("Authorization") === `Bearer ${env.MCP_SERVER_TOKEN}`;
}

function getPokeUserId(request: Request): string {
	return request.headers.get("X-Poke-User-Id")?.trim() || "dev-user";
}

function isMcpPath(path: string): boolean {
	return path === "/mcp" || path.endsWith("/mcp");
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

function jsonResponse(payload: unknown, status = 200): Response {
	return new Response(JSON.stringify(payload), {
		status,
		headers: {
			...corsHeaders(),
			"Content-Type": "application/json",
		},
	});
}

function corsHeaders(): Record<string, string> {
	return {
		"Access-Control-Allow-Origin": "*",
		"Access-Control-Allow-Methods": "GET,POST,OPTIONS",
		"Access-Control-Allow-Headers": "Authorization,Content-Type,X-Poke-User-Id",
	};
}

function readNumber(value: string | undefined, fallback: number): number {
	const parsed = Number(value);
	return Number.isFinite(parsed) ? parsed : fallback;
}

function isRecord(value: unknown): value is Record<string, unknown> {
	return typeof value === "object" && value !== null && !Array.isArray(value);
}

function toBase64Url(bytes: Uint8Array): string {
	let binary = "";

	for (const byte of bytes) {
		binary += String.fromCharCode(byte);
	}

	return btoa(binary).replace(/\+/g, "-").replace(/\//g, "_").replace(/=+$/g, "");
}

function fromBase64Url(value: string): Uint8Array {
	const padded = value.replace(/-/g, "+").replace(/_/g, "/").padEnd(Math.ceil(value.length / 4) * 4, "=");
	const binary = atob(padded);
	const bytes = new Uint8Array(binary.length);

	for (let index = 0; index < binary.length; index += 1) {
		bytes[index] = binary.charCodeAt(index);
	}

	return bytes;
}
