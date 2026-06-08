import { randomUUID } from "node:crypto";
import type { WebSocket } from "ws";
import type { ConnectedPlugin, PendingPairingToken } from "./types.js";

export interface PluginSession extends ConnectedPlugin {
	socket: WebSocket;
}

export class SessionStore {
	private pairingTokens = new Map<string, PendingPairingToken>();
	private sessionsByUserId = new Map<string, PluginSession>();

	constructor(private readonly tokenTtlMs: number) {}

	createPairingToken(userId: string): PendingPairingToken {
		this.pruneExpiredTokens();

		const token: PendingPairingToken = {
			token: randomUUID(),
			userId,
			createdAt: Date.now(),
			expiresAt: Date.now() + this.tokenTtlMs,
		};

		this.pairingTokens.set(token.token, token);
		return token;
	}

	getPairingToken(token: string): PendingPairingToken | null {
		this.pruneExpiredTokens();

		const pairingToken = this.pairingTokens.get(token);

		if (!pairingToken) {
			return null;
		}
		return pairingToken;
	}

	registerSession(userId: string, socket: WebSocket, details: Omit<ConnectedPlugin, "userId" | "connectedAt">): void {
		const existingSession = this.sessionsByUserId.get(userId);

		if (existingSession && existingSession.socket !== socket) {
			existingSession.socket.close(4000, "Replaced by a newer Poke-Obsidian connection");
		}

		this.sessionsByUserId.set(userId, {
			userId,
			socket,
			connectedAt: Date.now(),
			...details,
		});
	}

	removeSession(userId: string, socket: WebSocket): void {
		const existingSession = this.sessionsByUserId.get(userId);

		if (existingSession?.socket === socket) {
			this.sessionsByUserId.delete(userId);
		}
	}

	getSession(userId: string): PluginSession | null {
		return this.sessionsByUserId.get(userId) ?? null;
	}

	getStats(): Record<string, unknown> {
		this.pruneExpiredTokens();

		return {
			connectedPlugins: this.sessionsByUserId.size,
			pendingPairingTokens: this.pairingTokens.size,
			users: [...this.sessionsByUserId.values()].map((session) => ({
				userId: session.userId,
				connectedAt: new Date(session.connectedAt).toISOString(),
				pluginId: session.pluginId,
				version: session.version,
			})),
		};
	}

	private pruneExpiredTokens(): void {
		const now = Date.now();

		for (const [token, pairingToken] of this.pairingTokens) {
			if (pairingToken.expiresAt <= now) {
				this.pairingTokens.delete(token);
			}
		}
	}
}
