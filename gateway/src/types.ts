export interface PendingPairingToken {
	token: string;
	userId: string;
	expiresAt: number;
	createdAt: number;
}

export interface PluginRequest {
	id: string;
	action: "list_files" | "read_file" | "write_file" | "search_vault";
	params: Record<string, unknown>;
}

export interface PluginResponse {
	id: string;
	status: "success" | "error";
	payload: Record<string, unknown>;
}

export interface ConnectedPlugin {
	userId: string;
	connectedAt: number;
	version: string;
	pluginId: string;
}
