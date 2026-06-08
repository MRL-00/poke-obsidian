import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { z } from "zod";
import { PluginRpc } from "./pluginRpc.js";
import type { SessionStore } from "./sessionStore.js";

export interface McpContext {
	userId: string;
}

export function createPokeObsidianMcpServer(store: SessionStore, rpc: PluginRpc, context: McpContext): McpServer {
	const server = new McpServer({
		name: "poke-obsidian-gateway",
		version: "0.1.0",
	});

	server.registerTool(
		"obsidian_status",
		{
			description: "Check whether this Poke user has an Obsidian vault connected.",
			inputSchema: {},
		},
		async () => {
			const session = store.getSession(context.userId);

			return toJsonToolResult({
				connected: Boolean(session),
				userId: context.userId,
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
		async () => toJsonToolResult(await callPlugin(store, rpc, context.userId, "list_files", {}))
	);

	server.registerTool(
		"obsidian_read_file",
		{
			description: "Read a markdown file from the connected user's Obsidian vault.",
			inputSchema: {
				path: z.string().describe("Vault-relative markdown path, for example Projects/Plan.md."),
			},
		},
		async ({ path }) => toJsonToolResult(await callPlugin(store, rpc, context.userId, "read_file", { path }))
	);

	server.registerTool(
		"obsidian_search_vault",
		{
			description: "Search markdown files in the connected user's Obsidian vault.",
			inputSchema: {
				query: z.string().describe("Text to search for."),
			},
		},
		async ({ query }) => toJsonToolResult(await callPlugin(store, rpc, context.userId, "search_vault", { query }))
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
		async ({ path, content }) => toJsonToolResult(await callPlugin(store, rpc, context.userId, "write_file", { path, content }))
	);

	return server;
}

async function callPlugin(
	store: SessionStore,
	rpc: PluginRpc,
	userId: string,
	action: "list_files" | "read_file" | "write_file" | "search_vault",
	params: Record<string, unknown>
): Promise<Record<string, unknown>> {
	const session = store.getSession(userId);

	if (!session) {
		throw new Error("No Obsidian vault is connected for this Poke user");
	}

	return rpc.call(session.socket, action, params);
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
