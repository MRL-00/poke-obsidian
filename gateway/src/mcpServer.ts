import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { z } from "zod";
import { PluginRpc } from "./pluginRpc.js";
import type { SessionStore } from "./sessionStore.js";

export interface McpContext {
	userId: string;
	routeSingleConnectedPlugin: boolean;
	pluginWebSocketUrl: string;
	createConnectionToken: (userId: string) => {
		token: string;
		expiresAt: string | null;
	};
}

export function createPokeObsidianMcpServer(store: SessionStore, rpc: PluginRpc, context: McpContext): McpServer {
	const server = new McpServer({
		name: "poke-obsidian-gateway",
		version: "0.1.0",
	});

	server.registerTool(
		"obsidian_create_connection_token",
		{
			description: "Create a connection token and gateway URL for pairing the current Poke user with the Poke-Obsidian plugin.",
			inputSchema: {},
		},
		async () => {
			const connection = context.createConnectionToken(context.userId);

			return toJsonToolResult({
				connectionToken: connection.token,
				gatewayUrl: context.pluginWebSocketUrl,
				expiresAt: connection.expiresAt,
				instructions: [
					"Install and enable the Poke-Obsidian community plugin in Obsidian.",
					"Open Obsidian Settings, then Poke-Obsidian.",
					"Paste the Gateway URL and Connection token.",
					"Wait for the status to show Connected.",
				],
			});
		}
	);

	server.registerTool(
		"obsidian_status",
		{
			description: "Check whether this Poke user has an Obsidian vault connected.",
			inputSchema: {},
		},
		async () => {
			const directSession = store.getSession(context.userId);
			const session = resolveSession(store, context);

			return toJsonToolResult({
				connected: Boolean(resolveSession(store, context)),
				userId: context.userId,
				routedUserId: session?.userId ?? null,
				usedSinglePluginFallback: Boolean(!directSession && session),
				connectedAt: session ? new Date(session.connectedAt).toISOString() : null,
				pluginId: session?.pluginId ?? null,
				version: session?.version ?? null,
			});
		}
	);

	server.registerTool(
		"obsidian_list_files",
		{
			description: "List all markdown file paths in the connected user's Obsidian vault.",
			inputSchema: {},
		},
		async () => toJsonToolResult(await callPlugin(store, rpc, context, "list_files", {}))
	);

	server.registerTool(
		"obsidian_read_file",
		{
			description: "Read a markdown file from the connected user's Obsidian vault.",
			inputSchema: {
				path: z.string().describe("Vault-relative markdown path, for example Projects/Plan.md."),
			},
		},
		async ({ path }) => toJsonToolResult(await callPlugin(store, rpc, context, "read_file", { path }))
	);

	server.registerTool(
		"obsidian_search_vault",
		{
			description: "Search markdown files in the connected user's Obsidian vault.",
			inputSchema: {
				query: z.string().describe("Text to search for."),
			},
		},
		async ({ query }) => toJsonToolResult(await callPlugin(store, rpc, context, "search_vault", { query }))
	);

	server.registerTool(
		"obsidian_write_file",
		{
			description: "Create or overwrite a markdown file in the connected user's Obsidian vault. The plugin must have writes enabled.",
			inputSchema: {
				path: z.string().describe("Vault-relative markdown path, for example Inbox/Poke.md."),
				content: z.string().describe("Complete markdown content to write."),
			},
		},
		async ({ path, content }) => toJsonToolResult(await callPlugin(store, rpc, context, "write_file", { path, content }))
	);

	return server;
}

async function callPlugin(
	store: SessionStore,
	rpc: PluginRpc,
	context: McpContext,
	action: "list_files" | "read_file" | "write_file" | "search_vault",
	params: Record<string, unknown>
): Promise<Record<string, unknown>> {
	const session = resolveSession(store, context);

	if (!session) {
		throw new Error("No Obsidian vault is connected for this Poke user");
	}

	return rpc.call(session.socket, action, params);
}

function resolveSession(store: SessionStore, context: McpContext) {
	return store.getSession(context.userId) ?? (context.routeSingleConnectedPlugin ? store.getSingleSession() : null);
}

function toJsonToolResult(value: unknown) {
	return {
		content: [
			{
				type: "text" as const,
				text: JSON.stringify(value, null, 2),
			},
		],
	};
}
