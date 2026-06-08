import { App, Notice, Plugin, PluginSettingTab, Setting, TFile } from "obsidian";

const DEFAULT_GATEWAY_URL = "wss://api.poke.com/obsidian/sync";
const BASE_RECONNECT_DELAY_MS = 1_000;
const MAX_RECONNECT_DELAY_MS = 30_000;
const REQUEST_TIMEOUT_MS = 30_000;
const MAX_SNIPPET_LENGTH = 180;

type ConnectionState = "connected" | "connecting" | "disconnected";

interface PokeObsidianSettings {
	gatewayUrl: string;
	connectionToken: string;
	allowWrite: boolean;
}

interface IncomingMessage {
	id?: unknown;
	action?: unknown;
	params?: unknown;
}

interface ResponseMessage {
	id: string | null;
	status: "success" | "error";
	payload: Record<string, unknown>;
}

interface SearchMatch {
	path: string;
	snippet: string;
}

const DEFAULT_SETTINGS: PokeObsidianSettings = {
	gatewayUrl: DEFAULT_GATEWAY_URL,
	connectionToken: "",
	allowWrite: false,
};

export default class PokeObsidianPlugin extends Plugin {
	settings: PokeObsidianSettings = DEFAULT_SETTINGS;
	private socket: WebSocket | null = null;
	private statusBarItemEl: HTMLElement | null = null;
	private settingsTab: PokeObsidianSettingTab | null = null;
	private connectionState: ConnectionState = "disconnected";
	private reconnectAttempts = 0;
	private reconnectTimer: number | null = null;
	private unloadRequested = false;

	async onload(): Promise<void> {
		await this.loadSettings();

		this.statusBarItemEl = this.addStatusBarItem();
		this.statusBarItemEl.addClass("poke-status");
		this.setConnectionState("disconnected");

		this.settingsTab = new PokeObsidianSettingTab(this.app, this);
		this.addSettingTab(this.settingsTab);

		if (this.settings.connectionToken) {
			this.connect();
		}
	}

	onunload(): void {
		this.unloadRequested = true;
		this.clearReconnectTimer();
		this.closeSocket();
		this.setConnectionState("disconnected");
	}

	async loadSettings(): Promise<void> {
		this.settings = Object.assign({}, DEFAULT_SETTINGS, await this.loadData());
	}

	async saveSettings(): Promise<void> {
		await this.saveData(this.settings);
	}

	getConnectionState(): ConnectionState {
		return this.connectionState;
	}

	async updateConnectionToken(token: string): Promise<void> {
		this.settings.connectionToken = token.trim();
		await this.saveSettings();
		this.reconnectNow();
	}

	async updateGatewayUrl(gatewayUrl: string): Promise<void> {
		this.settings.gatewayUrl = gatewayUrl.trim() || DEFAULT_GATEWAY_URL;
		await this.saveSettings();
		this.reconnectNow();
	}

	async updateAllowWrite(allowWrite: boolean): Promise<void> {
		this.settings.allowWrite = allowWrite;
		await this.saveSettings();
	}

	reconnectNow(): void {
		this.clearReconnectTimer();
		this.reconnectAttempts = 0;
		this.closeSocket();

		if (!this.settings.connectionToken) {
			this.setConnectionState("disconnected");
			return;
		}

		this.connect();
	}

	private connect(): void {
		if (this.unloadRequested || !this.settings.connectionToken) {
			this.setConnectionState("disconnected");
			return;
		}

		if (
			this.socket &&
			(this.socket.readyState === WebSocket.CONNECTING || this.socket.readyState === WebSocket.OPEN)
		) {
			return;
		}

		this.setConnectionState("connecting");

		const url = this.buildGatewayUrl();

		if (!url) {
			this.setConnectionState("disconnected");
			return;
		}

		try {
			console.log(`Poke-Obsidian connecting to ${redactToken(url)}`);
			this.socket = new WebSocket(url.toString());
		} catch (error) {
			this.handleConnectionError(error);
			this.scheduleReconnect();
			return;
		}

		this.socket.onopen = () => {
			console.log("Poke-Obsidian connected");
			this.reconnectAttempts = 0;
			this.setConnectionState("connected");
		};

		this.socket.onmessage = (event: MessageEvent<string>) => {
			void this.withRequestTimeout(this.handleSocketMessage(event.data), null);
		};

		this.socket.onerror = (event) => {
			this.handleConnectionError(new Error(`WebSocket connection error: ${JSON.stringify(event)}`));
		};

		this.socket.onclose = (event) => {
			console.log(`Poke-Obsidian disconnected: code=${event.code} reason=${event.reason || "(none)"}`);
			this.socket = null;

			if (this.unloadRequested) {
				this.setConnectionState("disconnected");
				return;
			}

			this.setConnectionState("disconnected");
			this.scheduleReconnect();
		};
	}

	private scheduleReconnect(): void {
		if (this.unloadRequested || !this.settings.connectionToken || this.reconnectTimer !== null) {
			return;
		}

		const delay = Math.min(BASE_RECONNECT_DELAY_MS * 2 ** this.reconnectAttempts, MAX_RECONNECT_DELAY_MS);
		this.reconnectAttempts += 1;

		this.reconnectTimer = window.setTimeout(() => {
			this.reconnectTimer = null;
			this.connect();
		}, delay);
	}

	private clearReconnectTimer(): void {
		if (this.reconnectTimer !== null) {
			window.clearTimeout(this.reconnectTimer);
			this.reconnectTimer = null;
		}
	}

	private closeSocket(): void {
		if (!this.socket) {
			return;
		}

		this.socket.onopen = null;
		this.socket.onmessage = null;
		this.socket.onerror = null;
		this.socket.onclose = null;

		if (this.socket.readyState === WebSocket.CONNECTING || this.socket.readyState === WebSocket.OPEN) {
			this.socket.close();
		}

		this.socket = null;
	}

	private async withRequestTimeout(task: Promise<void>, id: string | null): Promise<void> {
		let timeoutId: number | null = null;

		const timeout = new Promise<void>((_resolve, reject) => {
			timeoutId = window.setTimeout(() => reject(new Error("Request timed out")), REQUEST_TIMEOUT_MS);
		});

		try {
			await Promise.race([task, timeout]);
		} catch (error) {
			this.sendResponse({
				id,
				status: "error",
				payload: { error: error instanceof Error ? error.message : String(error) },
			});
		} finally {
			if (timeoutId !== null) {
				window.clearTimeout(timeoutId);
			}
		}
	}

	private async handleSocketMessage(rawMessage: string): Promise<void> {
		let incoming: IncomingMessage;

		try {
			incoming = JSON.parse(rawMessage) as IncomingMessage;
		} catch {
			this.sendResponse({
				id: null,
				status: "error",
				payload: { error: "Invalid JSON message" },
			});
			return;
		}

		const id = typeof incoming.id === "string" ? incoming.id : null;
		const action = typeof incoming.action === "string" ? incoming.action : "";
		const params = isRecord(incoming.params) ? incoming.params : {};

		if (!id) {
			this.sendResponse({
				id,
				status: "error",
				payload: { error: "Message id is required" },
			});
			return;
		}

		try {
			const payload = await this.handleAction(action, params);
			this.sendResponse({ id, status: "success", payload });
		} catch (error) {
			this.sendResponse({
				id,
				status: "error",
				payload: { error: error instanceof Error ? error.message : String(error) },
			});
		}
	}

	private async handleAction(action: string, params: Record<string, unknown>): Promise<Record<string, unknown>> {
		switch (action) {
			case "list":
			case "list_files":
				return this.listFiles();
			case "read":
			case "read_file":
				return this.readFile(params);
			case "write":
			case "write_file":
				return this.writeFile(params);
			case "search":
			case "search_vault":
				return this.searchVault(params);
			default:
				throw new Error(`Unsupported action: ${action || "(missing)"}`);
		}
	}

	private listFiles(): Record<string, unknown> {
		return {
			files: this.app.vault.getMarkdownFiles().map((file) => file.path),
		};
	}

	private async readFile(params: Record<string, unknown>): Promise<Record<string, unknown>> {
		const path = getRequiredString(params, "path");
		const file = this.getMarkdownFile(path);
		const content = await this.app.vault.adapter.read(file.path);

		return {
			path: file.path,
			content,
		};
	}

	private async writeFile(params: Record<string, unknown>): Promise<Record<string, unknown>> {
		if (!this.settings.allowWrite) {
			throw new Error("Write access is disabled in Poke-Obsidian settings");
		}

		const path = getRequiredString(params, "path");
		const content = getRequiredString(params, "content");
		const normalizedPath = this.normalizeMarkdownPath(path);

		await this.ensureParentFolders(normalizedPath);
		await this.app.vault.adapter.write(normalizedPath, content);

		return {
			path: normalizedPath,
			bytes: new TextEncoder().encode(content).length,
		};
	}

	private async searchVault(params: Record<string, unknown>): Promise<Record<string, unknown>> {
		const query = getRequiredString(params, "query").trim();

		if (!query) {
			throw new Error("Search query cannot be empty");
		}

		const normalizedQuery = query.toLocaleLowerCase();
		const matches: SearchMatch[] = [];

		for (const file of this.app.vault.getMarkdownFiles()) {
			const content = await this.app.vault.adapter.read(file.path);
			const index = content.toLocaleLowerCase().indexOf(normalizedQuery);

			if (index === -1) {
				continue;
			}

			matches.push({
				path: file.path,
				snippet: makeSnippet(content, index, query.length),
			});
		}

		return { matches };
	}

	private getMarkdownFile(path: string): TFile {
		const normalizedPath = this.normalizeMarkdownPath(path);
		const file = this.app.vault.getAbstractFileByPath(normalizedPath);

		if (!(file instanceof TFile)) {
			throw new Error(`File not found: ${normalizedPath}`);
		}

		if (file.extension !== "md") {
			throw new Error(`Only markdown files are supported: ${normalizedPath}`);
		}

		return file;
	}

	private normalizeMarkdownPath(path: string): string {
		const normalizedPath = path.trim().replace(/^\/+/, "");

		if (!normalizedPath) {
			throw new Error("Path cannot be empty");
		}

		const parts = normalizedPath.split("/").filter(Boolean);

		if (parts.includes("..") || parts.some((part) => part === ".")) {
			throw new Error("Path cannot contain parent or current-directory segments");
		}

		if (!normalizedPath.toLocaleLowerCase().endsWith(".md")) {
			throw new Error("Only markdown file paths ending in .md are supported");
		}

		return parts.join("/");
	}

	private async ensureParentFolders(filePath: string): Promise<void> {
		const parentParts = filePath.split("/").slice(0, -1);
		let currentPath = "";

		for (const part of parentParts) {
			currentPath = currentPath ? `${currentPath}/${part}` : part;

			if (!(await this.app.vault.adapter.exists(currentPath))) {
				await this.app.vault.adapter.mkdir(currentPath);
			}
		}
	}

	private buildGatewayUrl(): URL | null {
		let url: URL;

		try {
			url = new URL(this.settings.gatewayUrl || DEFAULT_GATEWAY_URL);
		} catch {
			new Notice("Invalid Poke-Obsidian gateway URL");
			return null;
		}

		if (url.protocol !== "ws:" && url.protocol !== "wss:") {
			new Notice("Poke-Obsidian gateway URL must start with ws:// or wss://");
			return null;
		}

		url.searchParams.set("token", this.settings.connectionToken);
		url.searchParams.set("plugin", this.manifest.id);
		url.searchParams.set("version", this.manifest.version);

		return url;
	}

	private sendResponse(response: ResponseMessage): void {
		if (!this.socket || this.socket.readyState !== WebSocket.OPEN) {
			return;
		}

		this.socket.send(JSON.stringify(response));
	}

	private setConnectionState(state: ConnectionState): void {
		this.connectionState = state;

		if (this.statusBarItemEl) {
			this.statusBarItemEl.setText(`Poke: ${capitalize(state)}`);
			this.statusBarItemEl.removeClass("is-connected", "is-connecting", "is-disconnected");
			this.statusBarItemEl.addClass(`is-${state}`);
		}

		this.settingsTab?.updateStatus(state);
	}

	private handleConnectionError(error: unknown): void {
		console.error("Poke-Obsidian connection error", error);
	}
}

class PokeObsidianSettingTab extends PluginSettingTab {
	private plugin: PokeObsidianPlugin;
	private statusEl: HTMLElement | null = null;

	constructor(app: App, plugin: PokeObsidianPlugin) {
		super(app, plugin);
		this.plugin = plugin;
	}

	display(): void {
		const { containerEl } = this;
		containerEl.empty();

		containerEl.createEl("h2", { text: "Poke-Obsidian" });

		new Setting(containerEl)
			.setName("Gateway URL")
			.setDesc("WebSocket endpoint used to connect this vault to Poke.")
			.addText((text) => {
				text
					.setPlaceholder(DEFAULT_GATEWAY_URL)
					.setValue(this.plugin.settings.gatewayUrl || DEFAULT_GATEWAY_URL)
					.onChange(async (value) => {
						await this.plugin.updateGatewayUrl(value);
					});
			});

		new Setting(containerEl)
			.setName("Connection token")
			.setDesc("Pairs this vault with Poke.")
			.addText((text) => {
				text
					.setPlaceholder("Paste token")
					.setValue(this.plugin.settings.connectionToken)
					.onChange(async (value) => {
						await this.plugin.updateConnectionToken(value);
					});

				text.inputEl.type = "password";
			});

		new Setting(containerEl)
			.setName("Allow writes")
			.setDesc("Allow Poke to create or overwrite markdown files in this vault.")
			.addToggle((toggle) => {
				toggle
					.setValue(this.plugin.settings.allowWrite)
					.onChange(async (value) => {
						await this.plugin.updateAllowWrite(value);
					});
			});

		const statusSetting = new Setting(containerEl)
			.setName("Connection status")
			.setDesc("Current gateway connection state.")
			.addExtraButton((button) => {
				button
					.setIcon("refresh-cw")
					.setTooltip("Reconnect")
					.onClick(() => {
						this.plugin.reconnectNow();
						new Notice("Reconnecting Poke-Obsidian");
					});
			});

		this.statusEl = statusSetting.controlEl.createSpan();
		this.statusEl.addClass("poke-status");
		this.updateStatus(this.plugin.getConnectionState());
	}

	updateStatus(state: ConnectionState): void {
		if (!this.statusEl) {
			return;
		}

		this.statusEl.setText(capitalize(state));
		this.statusEl.removeClass("is-connected", "is-connecting", "is-disconnected");
		this.statusEl.addClass(`is-${state}`);
	}
}

function getRequiredString(params: Record<string, unknown>, key: string): string {
	const value = params[key];

	if (typeof value !== "string") {
		throw new Error(`Missing required string param: ${key}`);
	}

	return value;
}

function isRecord(value: unknown): value is Record<string, unknown> {
	return typeof value === "object" && value !== null && !Array.isArray(value);
}

function makeSnippet(content: string, index: number, matchLength: number): string {
	const halfWindow = Math.floor((MAX_SNIPPET_LENGTH - matchLength) / 2);
	const start = Math.max(0, index - halfWindow);
	const end = Math.min(content.length, index + matchLength + halfWindow);
	const prefix = start > 0 ? "..." : "";
	const suffix = end < content.length ? "..." : "";

	return `${prefix}${content.slice(start, end).replace(/\s+/g, " ").trim()}${suffix}`;
}

function capitalize(value: string): string {
	return value.charAt(0).toLocaleUpperCase() + value.slice(1);
}

function redactToken(url: URL): string {
	const copy = new URL(url.toString());

	if (copy.searchParams.has("token")) {
		copy.searchParams.set("token", "***");
	}

	return copy.toString();
}
