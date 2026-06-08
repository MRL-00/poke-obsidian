import { existsSync, readFileSync } from "node:fs";
import { resolve } from "node:path";

export interface Config {
	port: number;
	publicBaseUrl: string;
	mcpServerToken: string;
	adminToken: string;
	pairingTokenTtlMs: number;
	pluginRequestTimeoutMs: number;
	routeSingleConnectedPlugin: boolean;
}

export function loadConfig(): Config {
	loadDotEnv();

	return {
		port: readNumber("PORT", 3000),
		publicBaseUrl: readString("PUBLIC_BASE_URL", "http://localhost:3000").replace(/\/+$/, ""),
		mcpServerToken: readString("MCP_SERVER_TOKEN", ""),
		adminToken: readString("GATEWAY_ADMIN_TOKEN", ""),
		pairingTokenTtlMs: readNumber("PAIRING_TOKEN_TTL_MS", 600_000),
		pluginRequestTimeoutMs: readNumber("PLUGIN_REQUEST_TIMEOUT_MS", 30_000),
		routeSingleConnectedPlugin: readBoolean("ROUTE_SINGLE_CONNECTED_PLUGIN", false),
	};
}

function loadDotEnv(): void {
	const path = resolve(process.cwd(), ".env");

	if (!existsSync(path)) {
		return;
	}

	for (const line of readFileSync(path, "utf8").split(/\r?\n/)) {
		const trimmed = line.trim();

		if (!trimmed || trimmed.startsWith("#")) {
			continue;
		}

		const separatorIndex = trimmed.indexOf("=");

		if (separatorIndex === -1) {
			continue;
		}

		const key = trimmed.slice(0, separatorIndex).trim();
		const value = trimmed.slice(separatorIndex + 1).trim().replace(/^['"]|['"]$/g, "");

		if (key && process.env[key] === undefined) {
			process.env[key] = value;
		}
	}
}

function readString(name: string, fallback: string): string {
	return process.env[name]?.trim() || fallback;
}

function readNumber(name: string, fallback: number): number {
	const value = Number(process.env[name]);
	return Number.isFinite(value) ? value : fallback;
}

function readBoolean(name: string, fallback: boolean): boolean {
	const value = process.env[name]?.trim().toLowerCase();

	if (!value) {
		return fallback;
	}

	return ["1", "true", "yes", "on"].includes(value);
}
