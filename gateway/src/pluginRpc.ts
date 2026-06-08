import { randomUUID } from "node:crypto";
import { WebSocket, type RawData } from "ws";
import type { PluginRequest, PluginResponse } from "./types.js";

export class PluginRpc {
	constructor(private readonly timeoutMs: number) {}

	call(socket: WebSocket, action: PluginRequest["action"], params: Record<string, unknown>): Promise<Record<string, unknown>> {
		if (socket.readyState !== WebSocket.OPEN) {
			throw new Error("Obsidian plugin is not connected");
		}

		const id = `msg_${randomUUID()}`;
		const request: PluginRequest = { id, action, params };

		return new Promise((resolve, reject) => {
			const timeout = setTimeout(() => {
				socket.off("message", onMessage);
				reject(new Error(`Timed out waiting for Obsidian plugin response to ${action}`));
			}, this.timeoutMs);

			const onMessage = (rawMessage: RawData) => {
				let response: PluginResponse;

				try {
					response = JSON.parse(rawMessageToString(rawMessage)) as PluginResponse;
				} catch {
					return;
				}

				if (response.id !== id) {
					return;
				}

				clearTimeout(timeout);
				socket.off("message", onMessage);

				if (response.status === "error") {
					reject(new Error(String(response.payload.error ?? "Unknown Obsidian plugin error")));
					return;
				}

				resolve(response.payload);
			};

			socket.on("message", onMessage);
			socket.send(JSON.stringify(request), (error) => {
				if (!error) {
					return;
				}

				clearTimeout(timeout);
				socket.off("message", onMessage);
				reject(error);
			});
		});
	}
}

function rawMessageToString(rawMessage: RawData): string {
	if (typeof rawMessage === "string") {
		return rawMessage;
	}

	if (Buffer.isBuffer(rawMessage)) {
		return rawMessage.toString("utf8");
	}

	if (Array.isArray(rawMessage)) {
		return Buffer.concat(rawMessage).toString("utf8");
	}

	return Buffer.from(rawMessage).toString("utf8");
}
