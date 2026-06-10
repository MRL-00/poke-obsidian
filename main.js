"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
const obsidian_1 = require("obsidian");
const DEFAULT_GATEWAY_URL = "wss://obsidian.matt-nz.com/obsidian/sync";
const DEFAULT_VAULT_FOLDER = "Poke";
const BASE_RECONNECT_DELAY_MS = 1000;
const MAX_RECONNECT_DELAY_MS = 30000;
const REQUEST_TIMEOUT_MS = 30000;
const MAX_SNIPPET_LENGTH = 180;
const GENERATED_TOKEN_PREFIX = "pkobs_vault_";
const DEFAULT_SETTINGS = {
    gatewayUrl: DEFAULT_GATEWAY_URL,
    connectionToken: "",
    vaultFolder: DEFAULT_VAULT_FOLDER,
    allowWrite: false,
};
class PokeObsidianPlugin extends obsidian_1.Plugin {
    constructor() {
        super(...arguments);
        this.settings = DEFAULT_SETTINGS;
        this.socket = null;
        this.statusBarItemEl = null;
        this.settingsTab = null;
        this.connectionState = "disconnected";
        this.reconnectAttempts = 0;
        this.reconnectTimer = null;
        this.unloadRequested = false;
    }
    async onload() {
        await this.loadSettings();
        if (!this.settings.connectionToken) {
            this.settings.connectionToken = generateConnectionToken();
            await this.saveSettings();
        }
        this.statusBarItemEl = this.addStatusBarItem();
        this.statusBarItemEl.addClass("poke-status");
        this.setConnectionState("disconnected");
        this.settingsTab = new PokeObsidianSettingTab(this.app, this);
        this.addSettingTab(this.settingsTab);
        this.connect();
    }
    onunload() {
        this.unloadRequested = true;
        this.clearReconnectTimer();
        this.closeSocket();
        this.setConnectionState("disconnected");
    }
    async loadSettings() {
        const savedSettings = await this.loadData();
        const settingsRecord = isRecord(savedSettings) ? savedSettings : {};
        this.settings = {
            gatewayUrl: getOptionalString(settingsRecord, "gatewayUrl", DEFAULT_GATEWAY_URL),
            connectionToken: getOptionalString(settingsRecord, "connectionToken", ""),
            vaultFolder: getOptionalString(settingsRecord, "vaultFolder", DEFAULT_VAULT_FOLDER),
            allowWrite: typeof settingsRecord.allowWrite === "boolean" ? settingsRecord.allowWrite : DEFAULT_SETTINGS.allowWrite,
        };
        if (this.settings.connectionToken &&
            !Object.prototype.hasOwnProperty.call(settingsRecord, "vaultFolder")) {
            this.settings.vaultFolder = "";
        }
    }
    async saveSettings() {
        await this.saveData(this.settings);
    }
    getConnectionState() {
        return this.connectionState;
    }
    async updateConnectionToken(token) {
        this.settings.connectionToken = token.trim();
        await this.saveSettings();
        this.reconnectNow();
    }
    async updateGatewayUrl(gatewayUrl) {
        this.settings.gatewayUrl = gatewayUrl.trim() || DEFAULT_GATEWAY_URL;
        await this.saveSettings();
        this.reconnectNow();
    }
    async updateAllowWrite(allowWrite) {
        this.settings.allowWrite = allowWrite;
        await this.saveSettings();
    }
    async updateVaultFolder(vaultFolder) {
        this.settings.vaultFolder = vaultFolder.trim();
        await this.saveSettings();
    }
    reconnectNow() {
        this.clearReconnectTimer();
        this.reconnectAttempts = 0;
        this.closeSocket();
        if (!this.settings.connectionToken) {
            this.setConnectionState("disconnected");
            return;
        }
        this.connect();
    }
    connect() {
        if (this.unloadRequested || !this.settings.connectionToken) {
            this.setConnectionState("disconnected");
            return;
        }
        if (this.socket &&
            (this.socket.readyState === WebSocket.CONNECTING || this.socket.readyState === WebSocket.OPEN)) {
            return;
        }
        this.setConnectionState("connecting");
        const url = this.buildGatewayUrl();
        if (!url) {
            this.setConnectionState("disconnected");
            return;
        }
        try {
            console.log(`Poke Gateway connecting to ${redactToken(url)}`);
            this.socket = new WebSocket(url.toString());
        }
        catch (error) {
            this.handleConnectionError(error);
            this.scheduleReconnect();
            return;
        }
        this.socket.onopen = () => {
            console.log("Poke Gateway connected");
            this.reconnectAttempts = 0;
            this.setConnectionState("connected");
        };
        this.socket.onmessage = (event) => {
            void this.withRequestTimeout(this.handleSocketMessage(event.data), null);
        };
        this.socket.onerror = (event) => {
            this.handleConnectionError(new Error(`WebSocket connection error: ${JSON.stringify(event)}`));
        };
        this.socket.onclose = (event) => {
            console.log(`Poke Gateway disconnected: code=${event.code} reason=${event.reason || "(none)"}`);
            this.socket = null;
            if (this.unloadRequested) {
                this.setConnectionState("disconnected");
                return;
            }
            this.setConnectionState("disconnected");
            this.scheduleReconnect();
        };
    }
    scheduleReconnect() {
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
    clearReconnectTimer() {
        if (this.reconnectTimer !== null) {
            window.clearTimeout(this.reconnectTimer);
            this.reconnectTimer = null;
        }
    }
    closeSocket() {
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
    async withRequestTimeout(task, id) {
        let timeoutId = null;
        const timeout = new Promise((_resolve, reject) => {
            timeoutId = window.setTimeout(() => reject(new Error("Request timed out")), REQUEST_TIMEOUT_MS);
        });
        try {
            await Promise.race([task, timeout]);
        }
        catch (error) {
            this.sendResponse({
                id,
                status: "error",
                payload: { error: error instanceof Error ? error.message : String(error) },
            });
        }
        finally {
            if (timeoutId !== null) {
                window.clearTimeout(timeoutId);
            }
        }
    }
    async handleSocketMessage(rawMessage) {
        let incoming;
        try {
            incoming = JSON.parse(rawMessage);
        }
        catch (_a) {
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
        }
        catch (error) {
            this.sendResponse({
                id,
                status: "error",
                payload: { error: error instanceof Error ? error.message : String(error) },
            });
        }
    }
    async handleAction(action, params) {
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
    async listFiles() {
        return {
            files: await this.listMarkdownPaths(),
        };
    }
    async readFile(params) {
        const path = getRequiredString(params, "path");
        const normalizedPath = this.normalizeMarkdownPath(path);
        this.ensurePathInVaultFolder(normalizedPath);
        const file = this.getMarkdownFile(normalizedPath);
        const content = await this.app.vault.adapter.read(file.path);
        return {
            path: file.path,
            content,
        };
    }
    async writeFile(params) {
        if (!this.settings.allowWrite) {
            throw new Error("Write access is disabled in Poke Gateway settings");
        }
        const path = getRequiredString(params, "path");
        const content = getRequiredString(params, "content");
        const normalizedPath = this.normalizeMarkdownPath(path);
        this.ensurePathInVaultFolder(normalizedPath);
        await this.ensureParentFolders(normalizedPath);
        await this.app.vault.adapter.write(normalizedPath, content);
        return {
            path: normalizedPath,
            bytes: new TextEncoder().encode(content).length,
        };
    }
    async searchVault(params) {
        const query = getRequiredString(params, "query").trim();
        if (!query) {
            throw new Error("Search query cannot be empty");
        }
        const normalizedQuery = query.toLocaleLowerCase();
        const matches = [];
        for (const path of await this.listMarkdownPaths()) {
            const content = await this.app.vault.adapter.read(path);
            const index = content.toLocaleLowerCase().indexOf(normalizedQuery);
            if (index === -1) {
                continue;
            }
            matches.push({
                path,
                snippet: makeSnippet(content, index, query.length),
            });
        }
        return { matches };
    }
    getMarkdownFile(path) {
        const file = this.app.vault.getAbstractFileByPath(path);
        if (!(file instanceof obsidian_1.TFile)) {
            throw new Error(`File not found: ${path}`);
        }
        if (file.extension !== "md") {
            throw new Error(`Only markdown files are supported: ${path}`);
        }
        return file;
    }
    normalizeMarkdownPath(path) {
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
    getVaultFolder() {
        return normalizeVaultFolder(this.settings.vaultFolder);
    }
    ensurePathInVaultFolder(path) {
        const folder = this.getVaultFolder();
        if (folder && !path.startsWith(`${folder}/`)) {
            throw new Error(`Path is outside the configured vault access folder: ${folder}`);
        }
    }
    async listMarkdownPaths() {
        const folder = this.getVaultFolder();
        if (folder && !(await this.app.vault.adapter.exists(folder))) {
            return [];
        }
        const paths = await this.listMarkdownPathsInFolder(folder);
        return paths.sort((a, b) => a.localeCompare(b));
    }
    async listMarkdownPathsInFolder(folder) {
        const listing = await this.app.vault.adapter.list(folder);
        const paths = listing.files.filter(isMarkdownPath);
        for (const childFolder of listing.folders) {
            paths.push(...(await this.listMarkdownPathsInFolder(childFolder)));
        }
        return paths;
    }
    async ensureParentFolders(filePath) {
        const parentParts = filePath.split("/").slice(0, -1);
        let currentPath = "";
        for (const part of parentParts) {
            currentPath = currentPath ? `${currentPath}/${part}` : part;
            if (!(await this.app.vault.adapter.exists(currentPath))) {
                await this.app.vault.adapter.mkdir(currentPath);
            }
        }
    }
    buildGatewayUrl() {
        let url;
        try {
            url = new URL(this.settings.gatewayUrl || DEFAULT_GATEWAY_URL);
        }
        catch (_a) {
            new obsidian_1.Notice("Invalid Poke Gateway URL");
            return null;
        }
        if (url.protocol !== "ws:" && url.protocol !== "wss:") {
            new obsidian_1.Notice("Poke Gateway URL must start with ws:// or wss://");
            return null;
        }
        url.searchParams.set("token", this.settings.connectionToken);
        url.searchParams.set("plugin", this.manifest.id);
        url.searchParams.set("version", this.manifest.version);
        return url;
    }
    sendResponse(response) {
        if (!this.socket || this.socket.readyState !== WebSocket.OPEN) {
            return;
        }
        this.socket.send(JSON.stringify(response));
    }
    setConnectionState(state) {
        var _a;
        this.connectionState = state;
        if (this.statusBarItemEl) {
            this.statusBarItemEl.setText(`Poke: ${capitalize(state)}`);
            this.statusBarItemEl.removeClass("is-connected", "is-connecting", "is-disconnected");
            this.statusBarItemEl.addClass(`is-${state}`);
        }
        (_a = this.settingsTab) === null || _a === void 0 ? void 0 : _a.updateStatus(state);
    }
    handleConnectionError(error) {
        console.error("Poke Gateway connection error", error);
    }
}
exports.default = PokeObsidianPlugin;
class PokeObsidianSettingTab extends obsidian_1.PluginSettingTab {
    constructor(app, plugin) {
        super(app, plugin);
        this.statusEl = null;
        this.plugin = plugin;
    }
    display() {
        const { containerEl } = this;
        containerEl.empty();
        containerEl.createEl("h2", { text: "Poke Gateway" });
        this.renderGatewayUrlSetting(new obsidian_1.Setting(containerEl)
            .setName("Gateway URL")
            .setDesc("WebSocket endpoint used to connect this vault to Poke."));
        this.renderVaultFolderSetting(new obsidian_1.Setting(containerEl)
            .setName("Vault access folder")
            .setDesc("Limit Poke to markdown files in this folder. Leave blank to allow all markdown files."));
        this.renderConnectionTokenSetting(new obsidian_1.Setting(containerEl)
            .setName("Connection token")
            .setDesc("Paste this token into Poke's Add Key field for the Obsidian recipe."));
        this.renderAllowWriteSetting(new obsidian_1.Setting(containerEl)
            .setName("Allow writes")
            .setDesc("Allow Poke to create or overwrite markdown files in this vault."));
        this.renderConnectionStatusSetting(new obsidian_1.Setting(containerEl)
            .setName("Connection status")
            .setDesc("Current gateway connection state."));
    }
    renderGatewayUrlSetting(setting) {
        setting.addText((text) => {
            text
                .setPlaceholder(DEFAULT_GATEWAY_URL)
                .setValue(this.plugin.settings.gatewayUrl || DEFAULT_GATEWAY_URL)
                .onChange(async (value) => {
                await this.plugin.updateGatewayUrl(value);
            });
        });
    }
    renderVaultFolderSetting(setting) {
        setting.addText((text) => {
            text
                .setPlaceholder(DEFAULT_VAULT_FOLDER)
                .setValue(this.plugin.settings.vaultFolder)
                .onChange(async (value) => {
                await this.plugin.updateVaultFolder(value);
            });
        });
    }
    renderConnectionTokenSetting(setting) {
        let tokenInput = null;
        setting
            .addText((text) => {
            text
                .setPlaceholder("Paste token")
                .setValue(this.plugin.settings.connectionToken)
                .onChange(async (value) => {
                await this.plugin.updateConnectionToken(value);
            });
            text.inputEl.type = "password";
            tokenInput = text.inputEl;
        })
            .addExtraButton((button) => {
            button
                .setIcon("text-cursor-input")
                .setTooltip("Reveal and select token")
                .onClick(() => {
                if (!tokenInput) {
                    return;
                }
                tokenInput.type = "text";
                tokenInput.focus();
                tokenInput.select();
                new obsidian_1.Notice("Poke Gateway token selected");
            });
        })
            .addExtraButton((button) => {
            button
                .setIcon("refresh-cw")
                .setTooltip("Generate new token")
                .onClick(async () => {
                await this.plugin.updateConnectionToken(generateConnectionToken());
                this.display();
                new obsidian_1.Notice("Generated a new Poke Gateway token");
            });
        });
    }
    renderAllowWriteSetting(setting) {
        setting.addToggle((toggle) => {
            toggle
                .setValue(this.plugin.settings.allowWrite)
                .onChange(async (value) => {
                await this.plugin.updateAllowWrite(value);
            });
        });
    }
    renderConnectionStatusSetting(setting) {
        setting.addExtraButton((button) => {
            button
                .setIcon("refresh-cw")
                .setTooltip("Reconnect")
                .onClick(() => {
                this.plugin.reconnectNow();
                new obsidian_1.Notice("Reconnecting Poke Gateway");
            });
        });
        this.statusEl = setting.controlEl.createSpan();
        this.statusEl.addClass("poke-status");
        this.updateStatus(this.plugin.getConnectionState());
    }
    updateStatus(state) {
        if (!this.statusEl) {
            return;
        }
        this.statusEl.setText(capitalize(state));
        this.statusEl.removeClass("is-connected", "is-connecting", "is-disconnected");
        this.statusEl.addClass(`is-${state}`);
    }
}
function getRequiredString(params, key) {
    const value = params[key];
    if (typeof value !== "string") {
        throw new Error(`Missing required string param: ${key}`);
    }
    return value;
}
function getOptionalString(params, key, fallback) {
    const value = params[key];
    return typeof value === "string" ? value : fallback;
}
function isRecord(value) {
    return typeof value === "object" && value !== null && !Array.isArray(value);
}
function isMarkdownPath(path) {
    return path.toLocaleLowerCase().endsWith(".md");
}
function normalizeVaultFolder(path) {
    const normalizedPath = path.trim().replace(/^\/+|\/+$/g, "");
    if (!normalizedPath) {
        return "";
    }
    const parts = normalizedPath.split("/").filter(Boolean);
    if (parts.includes("..") || parts.some((part) => part === ".")) {
        throw new Error("Vault access folder cannot contain parent or current-directory segments");
    }
    return parts.join("/");
}
function makeSnippet(content, index, matchLength) {
    const halfWindow = Math.floor((MAX_SNIPPET_LENGTH - matchLength) / 2);
    const start = Math.max(0, index - halfWindow);
    const end = Math.min(content.length, index + matchLength + halfWindow);
    const prefix = start > 0 ? "..." : "";
    const suffix = end < content.length ? "..." : "";
    return `${prefix}${content.slice(start, end).replace(/\s+/g, " ").trim()}${suffix}`;
}
function capitalize(value) {
    return value.charAt(0).toLocaleUpperCase() + value.slice(1);
}
function generateConnectionToken() {
    const bytes = new Uint8Array(32);
    crypto.getRandomValues(bytes);
    return `${GENERATED_TOKEN_PREFIX}${toBase64Url(bytes)}`;
}
function toBase64Url(bytes) {
    let binary = "";
    for (const byte of bytes) {
        binary += String.fromCharCode(byte);
    }
    return btoa(binary).replace(/\+/g, "-").replace(/\//g, "_").replace(/=+$/g, "");
}
function redactToken(url) {
    const copy = new URL(url.toString());
    if (copy.searchParams.has("token")) {
        copy.searchParams.set("token", "***");
    }
    return copy.toString();
}
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoibWFpbi5qcyIsInNvdXJjZVJvb3QiOiIiLCJzb3VyY2VzIjpbIm1haW4udHMiXSwibmFtZXMiOltdLCJtYXBwaW5ncyI6Ijs7QUFBQSx1Q0FBaUY7QUFFakYsTUFBTSxtQkFBbUIsR0FBRywwQ0FBMEMsQ0FBQztBQUN2RSxNQUFNLG9CQUFvQixHQUFHLE1BQU0sQ0FBQztBQUNwQyxNQUFNLHVCQUF1QixHQUFHLElBQUssQ0FBQztBQUN0QyxNQUFNLHNCQUFzQixHQUFHLEtBQU0sQ0FBQztBQUN0QyxNQUFNLGtCQUFrQixHQUFHLEtBQU0sQ0FBQztBQUNsQyxNQUFNLGtCQUFrQixHQUFHLEdBQUcsQ0FBQztBQUMvQixNQUFNLHNCQUFzQixHQUFHLGNBQWMsQ0FBQztBQTRCOUMsTUFBTSxnQkFBZ0IsR0FBeUI7SUFDOUMsVUFBVSxFQUFFLG1CQUFtQjtJQUMvQixlQUFlLEVBQUUsRUFBRTtJQUNuQixXQUFXLEVBQUUsb0JBQW9CO0lBQ2pDLFVBQVUsRUFBRSxLQUFLO0NBQ2pCLENBQUM7QUFFRixNQUFxQixrQkFBbUIsU0FBUSxpQkFBTTtJQUF0RDs7UUFDQyxhQUFRLEdBQXlCLGdCQUFnQixDQUFDO1FBQzFDLFdBQU0sR0FBcUIsSUFBSSxDQUFDO1FBQ2hDLG9CQUFlLEdBQXVCLElBQUksQ0FBQztRQUMzQyxnQkFBVyxHQUFrQyxJQUFJLENBQUM7UUFDbEQsb0JBQWUsR0FBb0IsY0FBYyxDQUFDO1FBQ2xELHNCQUFpQixHQUFHLENBQUMsQ0FBQztRQUN0QixtQkFBYyxHQUFrQixJQUFJLENBQUM7UUFDckMsb0JBQWUsR0FBRyxLQUFLLENBQUM7SUEyY2pDLENBQUM7SUF6Y0EsS0FBSyxDQUFDLE1BQU07UUFDWCxNQUFNLElBQUksQ0FBQyxZQUFZLEVBQUUsQ0FBQztRQUUxQixJQUFJLENBQUMsSUFBSSxDQUFDLFFBQVEsQ0FBQyxlQUFlLEVBQUUsQ0FBQztZQUNwQyxJQUFJLENBQUMsUUFBUSxDQUFDLGVBQWUsR0FBRyx1QkFBdUIsRUFBRSxDQUFDO1lBQzFELE1BQU0sSUFBSSxDQUFDLFlBQVksRUFBRSxDQUFDO1FBQzNCLENBQUM7UUFFRCxJQUFJLENBQUMsZUFBZSxHQUFHLElBQUksQ0FBQyxnQkFBZ0IsRUFBRSxDQUFDO1FBQy9DLElBQUksQ0FBQyxlQUFlLENBQUMsUUFBUSxDQUFDLGFBQWEsQ0FBQyxDQUFDO1FBQzdDLElBQUksQ0FBQyxrQkFBa0IsQ0FBQyxjQUFjLENBQUMsQ0FBQztRQUV4QyxJQUFJLENBQUMsV0FBVyxHQUFHLElBQUksc0JBQXNCLENBQUMsSUFBSSxDQUFDLEdBQUcsRUFBRSxJQUFJLENBQUMsQ0FBQztRQUM5RCxJQUFJLENBQUMsYUFBYSxDQUFDLElBQUksQ0FBQyxXQUFXLENBQUMsQ0FBQztRQUVyQyxJQUFJLENBQUMsT0FBTyxFQUFFLENBQUM7SUFDaEIsQ0FBQztJQUVELFFBQVE7UUFDUCxJQUFJLENBQUMsZUFBZSxHQUFHLElBQUksQ0FBQztRQUM1QixJQUFJLENBQUMsbUJBQW1CLEVBQUUsQ0FBQztRQUMzQixJQUFJLENBQUMsV0FBVyxFQUFFLENBQUM7UUFDbkIsSUFBSSxDQUFDLGtCQUFrQixDQUFDLGNBQWMsQ0FBQyxDQUFDO0lBQ3pDLENBQUM7SUFFRCxLQUFLLENBQUMsWUFBWTtRQUNqQixNQUFNLGFBQWEsR0FBWSxNQUFNLElBQUksQ0FBQyxRQUFRLEVBQUUsQ0FBQztRQUNyRCxNQUFNLGNBQWMsR0FBRyxRQUFRLENBQUMsYUFBYSxDQUFDLENBQUMsQ0FBQyxDQUFDLGFBQWEsQ0FBQyxDQUFDLENBQUMsRUFBRSxDQUFDO1FBRXBFLElBQUksQ0FBQyxRQUFRLEdBQUc7WUFDZixVQUFVLEVBQUUsaUJBQWlCLENBQUMsY0FBYyxFQUFFLFlBQVksRUFBRSxtQkFBbUIsQ0FBQztZQUNoRixlQUFlLEVBQUUsaUJBQWlCLENBQUMsY0FBYyxFQUFFLGlCQUFpQixFQUFFLEVBQUUsQ0FBQztZQUN6RSxXQUFXLEVBQUUsaUJBQWlCLENBQUMsY0FBYyxFQUFFLGFBQWEsRUFBRSxvQkFBb0IsQ0FBQztZQUNuRixVQUFVLEVBQ1QsT0FBTyxjQUFjLENBQUMsVUFBVSxLQUFLLFNBQVMsQ0FBQyxDQUFDLENBQUMsY0FBYyxDQUFDLFVBQVUsQ0FBQyxDQUFDLENBQUMsZ0JBQWdCLENBQUMsVUFBVTtTQUN6RyxDQUFDO1FBRUYsSUFDQyxJQUFJLENBQUMsUUFBUSxDQUFDLGVBQWU7WUFDN0IsQ0FBQyxNQUFNLENBQUMsU0FBUyxDQUFDLGNBQWMsQ0FBQyxJQUFJLENBQUMsY0FBYyxFQUFFLGFBQWEsQ0FBQyxFQUNuRSxDQUFDO1lBQ0YsSUFBSSxDQUFDLFFBQVEsQ0FBQyxXQUFXLEdBQUcsRUFBRSxDQUFDO1FBQ2hDLENBQUM7SUFDRixDQUFDO0lBRUQsS0FBSyxDQUFDLFlBQVk7UUFDakIsTUFBTSxJQUFJLENBQUMsUUFBUSxDQUFDLElBQUksQ0FBQyxRQUFRLENBQUMsQ0FBQztJQUNwQyxDQUFDO0lBRUQsa0JBQWtCO1FBQ2pCLE9BQU8sSUFBSSxDQUFDLGVBQWUsQ0FBQztJQUM3QixDQUFDO0lBRUQsS0FBSyxDQUFDLHFCQUFxQixDQUFDLEtBQWE7UUFDeEMsSUFBSSxDQUFDLFFBQVEsQ0FBQyxlQUFlLEdBQUcsS0FBSyxDQUFDLElBQUksRUFBRSxDQUFDO1FBQzdDLE1BQU0sSUFBSSxDQUFDLFlBQVksRUFBRSxDQUFDO1FBQzFCLElBQUksQ0FBQyxZQUFZLEVBQUUsQ0FBQztJQUNyQixDQUFDO0lBRUQsS0FBSyxDQUFDLGdCQUFnQixDQUFDLFVBQWtCO1FBQ3hDLElBQUksQ0FBQyxRQUFRLENBQUMsVUFBVSxHQUFHLFVBQVUsQ0FBQyxJQUFJLEVBQUUsSUFBSSxtQkFBbUIsQ0FBQztRQUNwRSxNQUFNLElBQUksQ0FBQyxZQUFZLEVBQUUsQ0FBQztRQUMxQixJQUFJLENBQUMsWUFBWSxFQUFFLENBQUM7SUFDckIsQ0FBQztJQUVELEtBQUssQ0FBQyxnQkFBZ0IsQ0FBQyxVQUFtQjtRQUN6QyxJQUFJLENBQUMsUUFBUSxDQUFDLFVBQVUsR0FBRyxVQUFVLENBQUM7UUFDdEMsTUFBTSxJQUFJLENBQUMsWUFBWSxFQUFFLENBQUM7SUFDM0IsQ0FBQztJQUVELEtBQUssQ0FBQyxpQkFBaUIsQ0FBQyxXQUFtQjtRQUMxQyxJQUFJLENBQUMsUUFBUSxDQUFDLFdBQVcsR0FBRyxXQUFXLENBQUMsSUFBSSxFQUFFLENBQUM7UUFDL0MsTUFBTSxJQUFJLENBQUMsWUFBWSxFQUFFLENBQUM7SUFDM0IsQ0FBQztJQUVELFlBQVk7UUFDWCxJQUFJLENBQUMsbUJBQW1CLEVBQUUsQ0FBQztRQUMzQixJQUFJLENBQUMsaUJBQWlCLEdBQUcsQ0FBQyxDQUFDO1FBQzNCLElBQUksQ0FBQyxXQUFXLEVBQUUsQ0FBQztRQUVuQixJQUFJLENBQUMsSUFBSSxDQUFDLFFBQVEsQ0FBQyxlQUFlLEVBQUUsQ0FBQztZQUNwQyxJQUFJLENBQUMsa0JBQWtCLENBQUMsY0FBYyxDQUFDLENBQUM7WUFDeEMsT0FBTztRQUNSLENBQUM7UUFFRCxJQUFJLENBQUMsT0FBTyxFQUFFLENBQUM7SUFDaEIsQ0FBQztJQUVPLE9BQU87UUFDZCxJQUFJLElBQUksQ0FBQyxlQUFlLElBQUksQ0FBQyxJQUFJLENBQUMsUUFBUSxDQUFDLGVBQWUsRUFBRSxDQUFDO1lBQzVELElBQUksQ0FBQyxrQkFBa0IsQ0FBQyxjQUFjLENBQUMsQ0FBQztZQUN4QyxPQUFPO1FBQ1IsQ0FBQztRQUVELElBQ0MsSUFBSSxDQUFDLE1BQU07WUFDWCxDQUFDLElBQUksQ0FBQyxNQUFNLENBQUMsVUFBVSxLQUFLLFNBQVMsQ0FBQyxVQUFVLElBQUksSUFBSSxDQUFDLE1BQU0sQ0FBQyxVQUFVLEtBQUssU0FBUyxDQUFDLElBQUksQ0FBQyxFQUM3RixDQUFDO1lBQ0YsT0FBTztRQUNSLENBQUM7UUFFRCxJQUFJLENBQUMsa0JBQWtCLENBQUMsWUFBWSxDQUFDLENBQUM7UUFFdEMsTUFBTSxHQUFHLEdBQUcsSUFBSSxDQUFDLGVBQWUsRUFBRSxDQUFDO1FBRW5DLElBQUksQ0FBQyxHQUFHLEVBQUUsQ0FBQztZQUNWLElBQUksQ0FBQyxrQkFBa0IsQ0FBQyxjQUFjLENBQUMsQ0FBQztZQUN4QyxPQUFPO1FBQ1IsQ0FBQztRQUVELElBQUksQ0FBQztZQUNKLE9BQU8sQ0FBQyxHQUFHLENBQUMsOEJBQThCLFdBQVcsQ0FBQyxHQUFHLENBQUMsRUFBRSxDQUFDLENBQUM7WUFDOUQsSUFBSSxDQUFDLE1BQU0sR0FBRyxJQUFJLFNBQVMsQ0FBQyxHQUFHLENBQUMsUUFBUSxFQUFFLENBQUMsQ0FBQztRQUM3QyxDQUFDO1FBQUMsT0FBTyxLQUFLLEVBQUUsQ0FBQztZQUNoQixJQUFJLENBQUMscUJBQXFCLENBQUMsS0FBSyxDQUFDLENBQUM7WUFDbEMsSUFBSSxDQUFDLGlCQUFpQixFQUFFLENBQUM7WUFDekIsT0FBTztRQUNSLENBQUM7UUFFRCxJQUFJLENBQUMsTUFBTSxDQUFDLE1BQU0sR0FBRyxHQUFHLEVBQUU7WUFDekIsT0FBTyxDQUFDLEdBQUcsQ0FBQyx3QkFBd0IsQ0FBQyxDQUFDO1lBQ3RDLElBQUksQ0FBQyxpQkFBaUIsR0FBRyxDQUFDLENBQUM7WUFDM0IsSUFBSSxDQUFDLGtCQUFrQixDQUFDLFdBQVcsQ0FBQyxDQUFDO1FBQ3RDLENBQUMsQ0FBQztRQUVGLElBQUksQ0FBQyxNQUFNLENBQUMsU0FBUyxHQUFHLENBQUMsS0FBMkIsRUFBRSxFQUFFO1lBQ3ZELEtBQUssSUFBSSxDQUFDLGtCQUFrQixDQUFDLElBQUksQ0FBQyxtQkFBbUIsQ0FBQyxLQUFLLENBQUMsSUFBSSxDQUFDLEVBQUUsSUFBSSxDQUFDLENBQUM7UUFDMUUsQ0FBQyxDQUFDO1FBRUYsSUFBSSxDQUFDLE1BQU0sQ0FBQyxPQUFPLEdBQUcsQ0FBQyxLQUFLLEVBQUUsRUFBRTtZQUMvQixJQUFJLENBQUMscUJBQXFCLENBQUMsSUFBSSxLQUFLLENBQUMsK0JBQStCLElBQUksQ0FBQyxTQUFTLENBQUMsS0FBSyxDQUFDLEVBQUUsQ0FBQyxDQUFDLENBQUM7UUFDL0YsQ0FBQyxDQUFDO1FBRUYsSUFBSSxDQUFDLE1BQU0sQ0FBQyxPQUFPLEdBQUcsQ0FBQyxLQUFLLEVBQUUsRUFBRTtZQUMvQixPQUFPLENBQUMsR0FBRyxDQUFDLG1DQUFtQyxLQUFLLENBQUMsSUFBSSxXQUFXLEtBQUssQ0FBQyxNQUFNLElBQUksUUFBUSxFQUFFLENBQUMsQ0FBQztZQUNoRyxJQUFJLENBQUMsTUFBTSxHQUFHLElBQUksQ0FBQztZQUVuQixJQUFJLElBQUksQ0FBQyxlQUFlLEVBQUUsQ0FBQztnQkFDMUIsSUFBSSxDQUFDLGtCQUFrQixDQUFDLGNBQWMsQ0FBQyxDQUFDO2dCQUN4QyxPQUFPO1lBQ1IsQ0FBQztZQUVELElBQUksQ0FBQyxrQkFBa0IsQ0FBQyxjQUFjLENBQUMsQ0FBQztZQUN4QyxJQUFJLENBQUMsaUJBQWlCLEVBQUUsQ0FBQztRQUMxQixDQUFDLENBQUM7SUFDSCxDQUFDO0lBRU8saUJBQWlCO1FBQ3hCLElBQUksSUFBSSxDQUFDLGVBQWUsSUFBSSxDQUFDLElBQUksQ0FBQyxRQUFRLENBQUMsZUFBZSxJQUFJLElBQUksQ0FBQyxjQUFjLEtBQUssSUFBSSxFQUFFLENBQUM7WUFDNUYsT0FBTztRQUNSLENBQUM7UUFFRCxNQUFNLEtBQUssR0FBRyxJQUFJLENBQUMsR0FBRyxDQUFDLHVCQUF1QixHQUFHLENBQUMsSUFBSSxJQUFJLENBQUMsaUJBQWlCLEVBQUUsc0JBQXNCLENBQUMsQ0FBQztRQUN0RyxJQUFJLENBQUMsaUJBQWlCLElBQUksQ0FBQyxDQUFDO1FBRTVCLElBQUksQ0FBQyxjQUFjLEdBQUcsTUFBTSxDQUFDLFVBQVUsQ0FBQyxHQUFHLEVBQUU7WUFDNUMsSUFBSSxDQUFDLGNBQWMsR0FBRyxJQUFJLENBQUM7WUFDM0IsSUFBSSxDQUFDLE9BQU8sRUFBRSxDQUFDO1FBQ2hCLENBQUMsRUFBRSxLQUFLLENBQUMsQ0FBQztJQUNYLENBQUM7SUFFTyxtQkFBbUI7UUFDMUIsSUFBSSxJQUFJLENBQUMsY0FBYyxLQUFLLElBQUksRUFBRSxDQUFDO1lBQ2xDLE1BQU0sQ0FBQyxZQUFZLENBQUMsSUFBSSxDQUFDLGNBQWMsQ0FBQyxDQUFDO1lBQ3pDLElBQUksQ0FBQyxjQUFjLEdBQUcsSUFBSSxDQUFDO1FBQzVCLENBQUM7SUFDRixDQUFDO0lBRU8sV0FBVztRQUNsQixJQUFJLENBQUMsSUFBSSxDQUFDLE1BQU0sRUFBRSxDQUFDO1lBQ2xCLE9BQU87UUFDUixDQUFDO1FBRUQsSUFBSSxDQUFDLE1BQU0sQ0FBQyxNQUFNLEdBQUcsSUFBSSxDQUFDO1FBQzFCLElBQUksQ0FBQyxNQUFNLENBQUMsU0FBUyxHQUFHLElBQUksQ0FBQztRQUM3QixJQUFJLENBQUMsTUFBTSxDQUFDLE9BQU8sR0FBRyxJQUFJLENBQUM7UUFDM0IsSUFBSSxDQUFDLE1BQU0sQ0FBQyxPQUFPLEdBQUcsSUFBSSxDQUFDO1FBRTNCLElBQUksSUFBSSxDQUFDLE1BQU0sQ0FBQyxVQUFVLEtBQUssU0FBUyxDQUFDLFVBQVUsSUFBSSxJQUFJLENBQUMsTUFBTSxDQUFDLFVBQVUsS0FBSyxTQUFTLENBQUMsSUFBSSxFQUFFLENBQUM7WUFDbEcsSUFBSSxDQUFDLE1BQU0sQ0FBQyxLQUFLLEVBQUUsQ0FBQztRQUNyQixDQUFDO1FBRUQsSUFBSSxDQUFDLE1BQU0sR0FBRyxJQUFJLENBQUM7SUFDcEIsQ0FBQztJQUVPLEtBQUssQ0FBQyxrQkFBa0IsQ0FBQyxJQUFtQixFQUFFLEVBQWlCO1FBQ3RFLElBQUksU0FBUyxHQUFrQixJQUFJLENBQUM7UUFFcEMsTUFBTSxPQUFPLEdBQUcsSUFBSSxPQUFPLENBQU8sQ0FBQyxRQUFRLEVBQUUsTUFBTSxFQUFFLEVBQUU7WUFDdEQsU0FBUyxHQUFHLE1BQU0sQ0FBQyxVQUFVLENBQUMsR0FBRyxFQUFFLENBQUMsTUFBTSxDQUFDLElBQUksS0FBSyxDQUFDLG1CQUFtQixDQUFDLENBQUMsRUFBRSxrQkFBa0IsQ0FBQyxDQUFDO1FBQ2pHLENBQUMsQ0FBQyxDQUFDO1FBRUgsSUFBSSxDQUFDO1lBQ0osTUFBTSxPQUFPLENBQUMsSUFBSSxDQUFDLENBQUMsSUFBSSxFQUFFLE9BQU8sQ0FBQyxDQUFDLENBQUM7UUFDckMsQ0FBQztRQUFDLE9BQU8sS0FBSyxFQUFFLENBQUM7WUFDaEIsSUFBSSxDQUFDLFlBQVksQ0FBQztnQkFDakIsRUFBRTtnQkFDRixNQUFNLEVBQUUsT0FBTztnQkFDZixPQUFPLEVBQUUsRUFBRSxLQUFLLEVBQUUsS0FBSyxZQUFZLEtBQUssQ0FBQyxDQUFDLENBQUMsS0FBSyxDQUFDLE9BQU8sQ0FBQyxDQUFDLENBQUMsTUFBTSxDQUFDLEtBQUssQ0FBQyxFQUFFO2FBQzFFLENBQUMsQ0FBQztRQUNKLENBQUM7Z0JBQVMsQ0FBQztZQUNWLElBQUksU0FBUyxLQUFLLElBQUksRUFBRSxDQUFDO2dCQUN4QixNQUFNLENBQUMsWUFBWSxDQUFDLFNBQVMsQ0FBQyxDQUFDO1lBQ2hDLENBQUM7UUFDRixDQUFDO0lBQ0YsQ0FBQztJQUVPLEtBQUssQ0FBQyxtQkFBbUIsQ0FBQyxVQUFrQjtRQUNuRCxJQUFJLFFBQXlCLENBQUM7UUFFOUIsSUFBSSxDQUFDO1lBQ0osUUFBUSxHQUFHLElBQUksQ0FBQyxLQUFLLENBQUMsVUFBVSxDQUFvQixDQUFDO1FBQ3RELENBQUM7UUFBQyxXQUFNLENBQUM7WUFDUixJQUFJLENBQUMsWUFBWSxDQUFDO2dCQUNqQixFQUFFLEVBQUUsSUFBSTtnQkFDUixNQUFNLEVBQUUsT0FBTztnQkFDZixPQUFPLEVBQUUsRUFBRSxLQUFLLEVBQUUsc0JBQXNCLEVBQUU7YUFDMUMsQ0FBQyxDQUFDO1lBQ0gsT0FBTztRQUNSLENBQUM7UUFFRCxNQUFNLEVBQUUsR0FBRyxPQUFPLFFBQVEsQ0FBQyxFQUFFLEtBQUssUUFBUSxDQUFDLENBQUMsQ0FBQyxRQUFRLENBQUMsRUFBRSxDQUFDLENBQUMsQ0FBQyxJQUFJLENBQUM7UUFDaEUsTUFBTSxNQUFNLEdBQUcsT0FBTyxRQUFRLENBQUMsTUFBTSxLQUFLLFFBQVEsQ0FBQyxDQUFDLENBQUMsUUFBUSxDQUFDLE1BQU0sQ0FBQyxDQUFDLENBQUMsRUFBRSxDQUFDO1FBQzFFLE1BQU0sTUFBTSxHQUFHLFFBQVEsQ0FBQyxRQUFRLENBQUMsTUFBTSxDQUFDLENBQUMsQ0FBQyxDQUFDLFFBQVEsQ0FBQyxNQUFNLENBQUMsQ0FBQyxDQUFDLEVBQUUsQ0FBQztRQUVoRSxJQUFJLENBQUMsRUFBRSxFQUFFLENBQUM7WUFDVCxJQUFJLENBQUMsWUFBWSxDQUFDO2dCQUNqQixFQUFFO2dCQUNGLE1BQU0sRUFBRSxPQUFPO2dCQUNmLE9BQU8sRUFBRSxFQUFFLEtBQUssRUFBRSx3QkFBd0IsRUFBRTthQUM1QyxDQUFDLENBQUM7WUFDSCxPQUFPO1FBQ1IsQ0FBQztRQUVELElBQUksQ0FBQztZQUNKLE1BQU0sT0FBTyxHQUFHLE1BQU0sSUFBSSxDQUFDLFlBQVksQ0FBQyxNQUFNLEVBQUUsTUFBTSxDQUFDLENBQUM7WUFDeEQsSUFBSSxDQUFDLFlBQVksQ0FBQyxFQUFFLEVBQUUsRUFBRSxNQUFNLEVBQUUsU0FBUyxFQUFFLE9BQU8sRUFBRSxDQUFDLENBQUM7UUFDdkQsQ0FBQztRQUFDLE9BQU8sS0FBSyxFQUFFLENBQUM7WUFDaEIsSUFBSSxDQUFDLFlBQVksQ0FBQztnQkFDakIsRUFBRTtnQkFDRixNQUFNLEVBQUUsT0FBTztnQkFDZixPQUFPLEVBQUUsRUFBRSxLQUFLLEVBQUUsS0FBSyxZQUFZLEtBQUssQ0FBQyxDQUFDLENBQUMsS0FBSyxDQUFDLE9BQU8sQ0FBQyxDQUFDLENBQUMsTUFBTSxDQUFDLEtBQUssQ0FBQyxFQUFFO2FBQzFFLENBQUMsQ0FBQztRQUNKLENBQUM7SUFDRixDQUFDO0lBRU8sS0FBSyxDQUFDLFlBQVksQ0FBQyxNQUFjLEVBQUUsTUFBK0I7UUFDekUsUUFBUSxNQUFNLEVBQUUsQ0FBQztZQUNoQixLQUFLLE1BQU0sQ0FBQztZQUNaLEtBQUssWUFBWTtnQkFDaEIsT0FBTyxJQUFJLENBQUMsU0FBUyxFQUFFLENBQUM7WUFDekIsS0FBSyxNQUFNLENBQUM7WUFDWixLQUFLLFdBQVc7Z0JBQ2YsT0FBTyxJQUFJLENBQUMsUUFBUSxDQUFDLE1BQU0sQ0FBQyxDQUFDO1lBQzlCLEtBQUssT0FBTyxDQUFDO1lBQ2IsS0FBSyxZQUFZO2dCQUNoQixPQUFPLElBQUksQ0FBQyxTQUFTLENBQUMsTUFBTSxDQUFDLENBQUM7WUFDL0IsS0FBSyxRQUFRLENBQUM7WUFDZCxLQUFLLGNBQWM7Z0JBQ2xCLE9BQU8sSUFBSSxDQUFDLFdBQVcsQ0FBQyxNQUFNLENBQUMsQ0FBQztZQUNqQztnQkFDQyxNQUFNLElBQUksS0FBSyxDQUFDLHVCQUF1QixNQUFNLElBQUksV0FBVyxFQUFFLENBQUMsQ0FBQztRQUNsRSxDQUFDO0lBQ0YsQ0FBQztJQUVPLEtBQUssQ0FBQyxTQUFTO1FBQ3RCLE9BQU87WUFDTixLQUFLLEVBQUUsTUFBTSxJQUFJLENBQUMsaUJBQWlCLEVBQUU7U0FDckMsQ0FBQztJQUNILENBQUM7SUFFTyxLQUFLLENBQUMsUUFBUSxDQUFDLE1BQStCO1FBQ3JELE1BQU0sSUFBSSxHQUFHLGlCQUFpQixDQUFDLE1BQU0sRUFBRSxNQUFNLENBQUMsQ0FBQztRQUMvQyxNQUFNLGNBQWMsR0FBRyxJQUFJLENBQUMscUJBQXFCLENBQUMsSUFBSSxDQUFDLENBQUM7UUFDeEQsSUFBSSxDQUFDLHVCQUF1QixDQUFDLGNBQWMsQ0FBQyxDQUFDO1FBRTdDLE1BQU0sSUFBSSxHQUFHLElBQUksQ0FBQyxlQUFlLENBQUMsY0FBYyxDQUFDLENBQUM7UUFDbEQsTUFBTSxPQUFPLEdBQUcsTUFBTSxJQUFJLENBQUMsR0FBRyxDQUFDLEtBQUssQ0FBQyxPQUFPLENBQUMsSUFBSSxDQUFDLElBQUksQ0FBQyxJQUFJLENBQUMsQ0FBQztRQUU3RCxPQUFPO1lBQ04sSUFBSSxFQUFFLElBQUksQ0FBQyxJQUFJO1lBQ2YsT0FBTztTQUNQLENBQUM7SUFDSCxDQUFDO0lBRU8sS0FBSyxDQUFDLFNBQVMsQ0FBQyxNQUErQjtRQUN0RCxJQUFJLENBQUMsSUFBSSxDQUFDLFFBQVEsQ0FBQyxVQUFVLEVBQUUsQ0FBQztZQUMvQixNQUFNLElBQUksS0FBSyxDQUFDLG1EQUFtRCxDQUFDLENBQUM7UUFDdEUsQ0FBQztRQUVELE1BQU0sSUFBSSxHQUFHLGlCQUFpQixDQUFDLE1BQU0sRUFBRSxNQUFNLENBQUMsQ0FBQztRQUMvQyxNQUFNLE9BQU8sR0FBRyxpQkFBaUIsQ0FBQyxNQUFNLEVBQUUsU0FBUyxDQUFDLENBQUM7UUFDckQsTUFBTSxjQUFjLEdBQUcsSUFBSSxDQUFDLHFCQUFxQixDQUFDLElBQUksQ0FBQyxDQUFDO1FBQ3hELElBQUksQ0FBQyx1QkFBdUIsQ0FBQyxjQUFjLENBQUMsQ0FBQztRQUU3QyxNQUFNLElBQUksQ0FBQyxtQkFBbUIsQ0FBQyxjQUFjLENBQUMsQ0FBQztRQUMvQyxNQUFNLElBQUksQ0FBQyxHQUFHLENBQUMsS0FBSyxDQUFDLE9BQU8sQ0FBQyxLQUFLLENBQUMsY0FBYyxFQUFFLE9BQU8sQ0FBQyxDQUFDO1FBRTVELE9BQU87WUFDTixJQUFJLEVBQUUsY0FBYztZQUNwQixLQUFLLEVBQUUsSUFBSSxXQUFXLEVBQUUsQ0FBQyxNQUFNLENBQUMsT0FBTyxDQUFDLENBQUMsTUFBTTtTQUMvQyxDQUFDO0lBQ0gsQ0FBQztJQUVPLEtBQUssQ0FBQyxXQUFXLENBQUMsTUFBK0I7UUFDeEQsTUFBTSxLQUFLLEdBQUcsaUJBQWlCLENBQUMsTUFBTSxFQUFFLE9BQU8sQ0FBQyxDQUFDLElBQUksRUFBRSxDQUFDO1FBRXhELElBQUksQ0FBQyxLQUFLLEVBQUUsQ0FBQztZQUNaLE1BQU0sSUFBSSxLQUFLLENBQUMsOEJBQThCLENBQUMsQ0FBQztRQUNqRCxDQUFDO1FBRUQsTUFBTSxlQUFlLEdBQUcsS0FBSyxDQUFDLGlCQUFpQixFQUFFLENBQUM7UUFDbEQsTUFBTSxPQUFPLEdBQWtCLEVBQUUsQ0FBQztRQUVsQyxLQUFLLE1BQU0sSUFBSSxJQUFJLE1BQU0sSUFBSSxDQUFDLGlCQUFpQixFQUFFLEVBQUUsQ0FBQztZQUNuRCxNQUFNLE9BQU8sR0FBRyxNQUFNLElBQUksQ0FBQyxHQUFHLENBQUMsS0FBSyxDQUFDLE9BQU8sQ0FBQyxJQUFJLENBQUMsSUFBSSxDQUFDLENBQUM7WUFDeEQsTUFBTSxLQUFLLEdBQUcsT0FBTyxDQUFDLGlCQUFpQixFQUFFLENBQUMsT0FBTyxDQUFDLGVBQWUsQ0FBQyxDQUFDO1lBRW5FLElBQUksS0FBSyxLQUFLLENBQUMsQ0FBQyxFQUFFLENBQUM7Z0JBQ2xCLFNBQVM7WUFDVixDQUFDO1lBRUQsT0FBTyxDQUFDLElBQUksQ0FBQztnQkFDWixJQUFJO2dCQUNKLE9BQU8sRUFBRSxXQUFXLENBQUMsT0FBTyxFQUFFLEtBQUssRUFBRSxLQUFLLENBQUMsTUFBTSxDQUFDO2FBQ2xELENBQUMsQ0FBQztRQUNKLENBQUM7UUFFRCxPQUFPLEVBQUUsT0FBTyxFQUFFLENBQUM7SUFDcEIsQ0FBQztJQUVPLGVBQWUsQ0FBQyxJQUFZO1FBQ25DLE1BQU0sSUFBSSxHQUFHLElBQUksQ0FBQyxHQUFHLENBQUMsS0FBSyxDQUFDLHFCQUFxQixDQUFDLElBQUksQ0FBQyxDQUFDO1FBRXhELElBQUksQ0FBQyxDQUFDLElBQUksWUFBWSxnQkFBSyxDQUFDLEVBQUUsQ0FBQztZQUM5QixNQUFNLElBQUksS0FBSyxDQUFDLG1CQUFtQixJQUFJLEVBQUUsQ0FBQyxDQUFDO1FBQzVDLENBQUM7UUFFRCxJQUFJLElBQUksQ0FBQyxTQUFTLEtBQUssSUFBSSxFQUFFLENBQUM7WUFDN0IsTUFBTSxJQUFJLEtBQUssQ0FBQyxzQ0FBc0MsSUFBSSxFQUFFLENBQUMsQ0FBQztRQUMvRCxDQUFDO1FBRUQsT0FBTyxJQUFJLENBQUM7SUFDYixDQUFDO0lBRU8scUJBQXFCLENBQUMsSUFBWTtRQUN6QyxNQUFNLGNBQWMsR0FBRyxJQUFJLENBQUMsSUFBSSxFQUFFLENBQUMsT0FBTyxDQUFDLE1BQU0sRUFBRSxFQUFFLENBQUMsQ0FBQztRQUV2RCxJQUFJLENBQUMsY0FBYyxFQUFFLENBQUM7WUFDckIsTUFBTSxJQUFJLEtBQUssQ0FBQyxzQkFBc0IsQ0FBQyxDQUFDO1FBQ3pDLENBQUM7UUFFRCxNQUFNLEtBQUssR0FBRyxjQUFjLENBQUMsS0FBSyxDQUFDLEdBQUcsQ0FBQyxDQUFDLE1BQU0sQ0FBQyxPQUFPLENBQUMsQ0FBQztRQUV4RCxJQUFJLEtBQUssQ0FBQyxRQUFRLENBQUMsSUFBSSxDQUFDLElBQUksS0FBSyxDQUFDLElBQUksQ0FBQyxDQUFDLElBQUksRUFBRSxFQUFFLENBQUMsSUFBSSxLQUFLLEdBQUcsQ0FBQyxFQUFFLENBQUM7WUFDaEUsTUFBTSxJQUFJLEtBQUssQ0FBQywwREFBMEQsQ0FBQyxDQUFDO1FBQzdFLENBQUM7UUFFRCxJQUFJLENBQUMsY0FBYyxDQUFDLGlCQUFpQixFQUFFLENBQUMsUUFBUSxDQUFDLEtBQUssQ0FBQyxFQUFFLENBQUM7WUFDekQsTUFBTSxJQUFJLEtBQUssQ0FBQyxzREFBc0QsQ0FBQyxDQUFDO1FBQ3pFLENBQUM7UUFFRCxPQUFPLEtBQUssQ0FBQyxJQUFJLENBQUMsR0FBRyxDQUFDLENBQUM7SUFDeEIsQ0FBQztJQUVPLGNBQWM7UUFDckIsT0FBTyxvQkFBb0IsQ0FBQyxJQUFJLENBQUMsUUFBUSxDQUFDLFdBQVcsQ0FBQyxDQUFDO0lBQ3hELENBQUM7SUFFTyx1QkFBdUIsQ0FBQyxJQUFZO1FBQzNDLE1BQU0sTUFBTSxHQUFHLElBQUksQ0FBQyxjQUFjLEVBQUUsQ0FBQztRQUVyQyxJQUFJLE1BQU0sSUFBSSxDQUFDLElBQUksQ0FBQyxVQUFVLENBQUMsR0FBRyxNQUFNLEdBQUcsQ0FBQyxFQUFFLENBQUM7WUFDOUMsTUFBTSxJQUFJLEtBQUssQ0FBQyx1REFBdUQsTUFBTSxFQUFFLENBQUMsQ0FBQztRQUNsRixDQUFDO0lBQ0YsQ0FBQztJQUVPLEtBQUssQ0FBQyxpQkFBaUI7UUFDOUIsTUFBTSxNQUFNLEdBQUcsSUFBSSxDQUFDLGNBQWMsRUFBRSxDQUFDO1FBRXJDLElBQUksTUFBTSxJQUFJLENBQUMsQ0FBQyxNQUFNLElBQUksQ0FBQyxHQUFHLENBQUMsS0FBSyxDQUFDLE9BQU8sQ0FBQyxNQUFNLENBQUMsTUFBTSxDQUFDLENBQUMsRUFBRSxDQUFDO1lBQzlELE9BQU8sRUFBRSxDQUFDO1FBQ1gsQ0FBQztRQUVELE1BQU0sS0FBSyxHQUFHLE1BQU0sSUFBSSxDQUFDLHlCQUF5QixDQUFDLE1BQU0sQ0FBQyxDQUFDO1FBQzNELE9BQU8sS0FBSyxDQUFDLElBQUksQ0FBQyxDQUFDLENBQUMsRUFBRSxDQUFDLEVBQUUsRUFBRSxDQUFDLENBQUMsQ0FBQyxhQUFhLENBQUMsQ0FBQyxDQUFDLENBQUMsQ0FBQztJQUNqRCxDQUFDO0lBRU8sS0FBSyxDQUFDLHlCQUF5QixDQUFDLE1BQWM7UUFDckQsTUFBTSxPQUFPLEdBQUcsTUFBTSxJQUFJLENBQUMsR0FBRyxDQUFDLEtBQUssQ0FBQyxPQUFPLENBQUMsSUFBSSxDQUFDLE1BQU0sQ0FBQyxDQUFDO1FBQzFELE1BQU0sS0FBSyxHQUFHLE9BQU8sQ0FBQyxLQUFLLENBQUMsTUFBTSxDQUFDLGNBQWMsQ0FBQyxDQUFDO1FBRW5ELEtBQUssTUFBTSxXQUFXLElBQUksT0FBTyxDQUFDLE9BQU8sRUFBRSxDQUFDO1lBQzNDLEtBQUssQ0FBQyxJQUFJLENBQUMsR0FBRyxDQUFDLE1BQU0sSUFBSSxDQUFDLHlCQUF5QixDQUFDLFdBQVcsQ0FBQyxDQUFDLENBQUMsQ0FBQztRQUNwRSxDQUFDO1FBRUQsT0FBTyxLQUFLLENBQUM7SUFDZCxDQUFDO0lBRU8sS0FBSyxDQUFDLG1CQUFtQixDQUFDLFFBQWdCO1FBQ2pELE1BQU0sV0FBVyxHQUFHLFFBQVEsQ0FBQyxLQUFLLENBQUMsR0FBRyxDQUFDLENBQUMsS0FBSyxDQUFDLENBQUMsRUFBRSxDQUFDLENBQUMsQ0FBQyxDQUFDO1FBQ3JELElBQUksV0FBVyxHQUFHLEVBQUUsQ0FBQztRQUVyQixLQUFLLE1BQU0sSUFBSSxJQUFJLFdBQVcsRUFBRSxDQUFDO1lBQ2hDLFdBQVcsR0FBRyxXQUFXLENBQUMsQ0FBQyxDQUFDLEdBQUcsV0FBVyxJQUFJLElBQUksRUFBRSxDQUFDLENBQUMsQ0FBQyxJQUFJLENBQUM7WUFFNUQsSUFBSSxDQUFDLENBQUMsTUFBTSxJQUFJLENBQUMsR0FBRyxDQUFDLEtBQUssQ0FBQyxPQUFPLENBQUMsTUFBTSxDQUFDLFdBQVcsQ0FBQyxDQUFDLEVBQUUsQ0FBQztnQkFDekQsTUFBTSxJQUFJLENBQUMsR0FBRyxDQUFDLEtBQUssQ0FBQyxPQUFPLENBQUMsS0FBSyxDQUFDLFdBQVcsQ0FBQyxDQUFDO1lBQ2pELENBQUM7UUFDRixDQUFDO0lBQ0YsQ0FBQztJQUVPLGVBQWU7UUFDdEIsSUFBSSxHQUFRLENBQUM7UUFFYixJQUFJLENBQUM7WUFDSixHQUFHLEdBQUcsSUFBSSxHQUFHLENBQUMsSUFBSSxDQUFDLFFBQVEsQ0FBQyxVQUFVLElBQUksbUJBQW1CLENBQUMsQ0FBQztRQUNoRSxDQUFDO1FBQUMsV0FBTSxDQUFDO1lBQ1IsSUFBSSxpQkFBTSxDQUFDLDBCQUEwQixDQUFDLENBQUM7WUFDdkMsT0FBTyxJQUFJLENBQUM7UUFDYixDQUFDO1FBRUQsSUFBSSxHQUFHLENBQUMsUUFBUSxLQUFLLEtBQUssSUFBSSxHQUFHLENBQUMsUUFBUSxLQUFLLE1BQU0sRUFBRSxDQUFDO1lBQ3ZELElBQUksaUJBQU0sQ0FBQyxrREFBa0QsQ0FBQyxDQUFDO1lBQy9ELE9BQU8sSUFBSSxDQUFDO1FBQ2IsQ0FBQztRQUVELEdBQUcsQ0FBQyxZQUFZLENBQUMsR0FBRyxDQUFDLE9BQU8sRUFBRSxJQUFJLENBQUMsUUFBUSxDQUFDLGVBQWUsQ0FBQyxDQUFDO1FBQzdELEdBQUcsQ0FBQyxZQUFZLENBQUMsR0FBRyxDQUFDLFFBQVEsRUFBRSxJQUFJLENBQUMsUUFBUSxDQUFDLEVBQUUsQ0FBQyxDQUFDO1FBQ2pELEdBQUcsQ0FBQyxZQUFZLENBQUMsR0FBRyxDQUFDLFNBQVMsRUFBRSxJQUFJLENBQUMsUUFBUSxDQUFDLE9BQU8sQ0FBQyxDQUFDO1FBRXZELE9BQU8sR0FBRyxDQUFDO0lBQ1osQ0FBQztJQUVPLFlBQVksQ0FBQyxRQUF5QjtRQUM3QyxJQUFJLENBQUMsSUFBSSxDQUFDLE1BQU0sSUFBSSxJQUFJLENBQUMsTUFBTSxDQUFDLFVBQVUsS0FBSyxTQUFTLENBQUMsSUFBSSxFQUFFLENBQUM7WUFDL0QsT0FBTztRQUNSLENBQUM7UUFFRCxJQUFJLENBQUMsTUFBTSxDQUFDLElBQUksQ0FBQyxJQUFJLENBQUMsU0FBUyxDQUFDLFFBQVEsQ0FBQyxDQUFDLENBQUM7SUFDNUMsQ0FBQztJQUVPLGtCQUFrQixDQUFDLEtBQXNCOztRQUNoRCxJQUFJLENBQUMsZUFBZSxHQUFHLEtBQUssQ0FBQztRQUU3QixJQUFJLElBQUksQ0FBQyxlQUFlLEVBQUUsQ0FBQztZQUMxQixJQUFJLENBQUMsZUFBZSxDQUFDLE9BQU8sQ0FBQyxTQUFTLFVBQVUsQ0FBQyxLQUFLLENBQUMsRUFBRSxDQUFDLENBQUM7WUFDM0QsSUFBSSxDQUFDLGVBQWUsQ0FBQyxXQUFXLENBQUMsY0FBYyxFQUFFLGVBQWUsRUFBRSxpQkFBaUIsQ0FBQyxDQUFDO1lBQ3JGLElBQUksQ0FBQyxlQUFlLENBQUMsUUFBUSxDQUFDLE1BQU0sS0FBSyxFQUFFLENBQUMsQ0FBQztRQUM5QyxDQUFDO1FBRUQsTUFBQSxJQUFJLENBQUMsV0FBVywwQ0FBRSxZQUFZLENBQUMsS0FBSyxDQUFDLENBQUM7SUFDdkMsQ0FBQztJQUVPLHFCQUFxQixDQUFDLEtBQWM7UUFDM0MsT0FBTyxDQUFDLEtBQUssQ0FBQywrQkFBK0IsRUFBRSxLQUFLLENBQUMsQ0FBQztJQUN2RCxDQUFDO0NBQ0Q7QUFuZEQscUNBbWRDO0FBRUQsTUFBTSxzQkFBdUIsU0FBUSwyQkFBZ0I7SUFJcEQsWUFBWSxHQUFRLEVBQUUsTUFBMEI7UUFDL0MsS0FBSyxDQUFDLEdBQUcsRUFBRSxNQUFNLENBQUMsQ0FBQztRQUhaLGFBQVEsR0FBdUIsSUFBSSxDQUFDO1FBSTNDLElBQUksQ0FBQyxNQUFNLEdBQUcsTUFBTSxDQUFDO0lBQ3RCLENBQUM7SUFFRCxPQUFPO1FBQ04sTUFBTSxFQUFFLFdBQVcsRUFBRSxHQUFHLElBQUksQ0FBQztRQUM3QixXQUFXLENBQUMsS0FBSyxFQUFFLENBQUM7UUFFcEIsV0FBVyxDQUFDLFFBQVEsQ0FBQyxJQUFJLEVBQUUsRUFBRSxJQUFJLEVBQUUsY0FBYyxFQUFFLENBQUMsQ0FBQztRQUVyRCxJQUFJLENBQUMsdUJBQXVCLENBQzNCLElBQUksa0JBQU8sQ0FBQyxXQUFXLENBQUM7YUFDdEIsT0FBTyxDQUFDLGFBQWEsQ0FBQzthQUN0QixPQUFPLENBQUMsd0RBQXdELENBQUMsQ0FDbkUsQ0FBQztRQUNGLElBQUksQ0FBQyx3QkFBd0IsQ0FDNUIsSUFBSSxrQkFBTyxDQUFDLFdBQVcsQ0FBQzthQUN0QixPQUFPLENBQUMscUJBQXFCLENBQUM7YUFDOUIsT0FBTyxDQUFDLHVGQUF1RixDQUFDLENBQ2xHLENBQUM7UUFDRixJQUFJLENBQUMsNEJBQTRCLENBQ2hDLElBQUksa0JBQU8sQ0FBQyxXQUFXLENBQUM7YUFDdEIsT0FBTyxDQUFDLGtCQUFrQixDQUFDO2FBQzNCLE9BQU8sQ0FBQyxxRUFBcUUsQ0FBQyxDQUNoRixDQUFDO1FBQ0YsSUFBSSxDQUFDLHVCQUF1QixDQUMzQixJQUFJLGtCQUFPLENBQUMsV0FBVyxDQUFDO2FBQ3RCLE9BQU8sQ0FBQyxjQUFjLENBQUM7YUFDdkIsT0FBTyxDQUFDLGlFQUFpRSxDQUFDLENBQzVFLENBQUM7UUFDRixJQUFJLENBQUMsNkJBQTZCLENBQ2pDLElBQUksa0JBQU8sQ0FBQyxXQUFXLENBQUM7YUFDdEIsT0FBTyxDQUFDLG1CQUFtQixDQUFDO2FBQzVCLE9BQU8sQ0FBQyxtQ0FBbUMsQ0FBQyxDQUM5QyxDQUFDO0lBQ0gsQ0FBQztJQUVPLHVCQUF1QixDQUFDLE9BQWdCO1FBQy9DLE9BQU8sQ0FBQyxPQUFPLENBQUMsQ0FBQyxJQUFJLEVBQUUsRUFBRTtZQUN4QixJQUFJO2lCQUNGLGNBQWMsQ0FBQyxtQkFBbUIsQ0FBQztpQkFDbkMsUUFBUSxDQUFDLElBQUksQ0FBQyxNQUFNLENBQUMsUUFBUSxDQUFDLFVBQVUsSUFBSSxtQkFBbUIsQ0FBQztpQkFDaEUsUUFBUSxDQUFDLEtBQUssRUFBRSxLQUFLLEVBQUUsRUFBRTtnQkFDekIsTUFBTSxJQUFJLENBQUMsTUFBTSxDQUFDLGdCQUFnQixDQUFDLEtBQUssQ0FBQyxDQUFDO1lBQzNDLENBQUMsQ0FBQyxDQUFDO1FBQ0wsQ0FBQyxDQUFDLENBQUM7SUFDSixDQUFDO0lBRU8sd0JBQXdCLENBQUMsT0FBZ0I7UUFDaEQsT0FBTyxDQUFDLE9BQU8sQ0FBQyxDQUFDLElBQUksRUFBRSxFQUFFO1lBQ3hCLElBQUk7aUJBQ0YsY0FBYyxDQUFDLG9CQUFvQixDQUFDO2lCQUNwQyxRQUFRLENBQUMsSUFBSSxDQUFDLE1BQU0sQ0FBQyxRQUFRLENBQUMsV0FBVyxDQUFDO2lCQUMxQyxRQUFRLENBQUMsS0FBSyxFQUFFLEtBQUssRUFBRSxFQUFFO2dCQUN6QixNQUFNLElBQUksQ0FBQyxNQUFNLENBQUMsaUJBQWlCLENBQUMsS0FBSyxDQUFDLENBQUM7WUFDNUMsQ0FBQyxDQUFDLENBQUM7UUFDTCxDQUFDLENBQUMsQ0FBQztJQUNKLENBQUM7SUFFTyw0QkFBNEIsQ0FBQyxPQUFnQjtRQUNwRCxJQUFJLFVBQVUsR0FBNEIsSUFBSSxDQUFDO1FBRS9DLE9BQU87YUFDTCxPQUFPLENBQUMsQ0FBQyxJQUFJLEVBQUUsRUFBRTtZQUNqQixJQUFJO2lCQUNGLGNBQWMsQ0FBQyxhQUFhLENBQUM7aUJBQzdCLFFBQVEsQ0FBQyxJQUFJLENBQUMsTUFBTSxDQUFDLFFBQVEsQ0FBQyxlQUFlLENBQUM7aUJBQzlDLFFBQVEsQ0FBQyxLQUFLLEVBQUUsS0FBSyxFQUFFLEVBQUU7Z0JBQ3pCLE1BQU0sSUFBSSxDQUFDLE1BQU0sQ0FBQyxxQkFBcUIsQ0FBQyxLQUFLLENBQUMsQ0FBQztZQUNoRCxDQUFDLENBQUMsQ0FBQztZQUVKLElBQUksQ0FBQyxPQUFPLENBQUMsSUFBSSxHQUFHLFVBQVUsQ0FBQztZQUMvQixVQUFVLEdBQUcsSUFBSSxDQUFDLE9BQU8sQ0FBQztRQUMzQixDQUFDLENBQUM7YUFDRCxjQUFjLENBQUMsQ0FBQyxNQUFNLEVBQUUsRUFBRTtZQUMxQixNQUFNO2lCQUNKLE9BQU8sQ0FBQyxtQkFBbUIsQ0FBQztpQkFDNUIsVUFBVSxDQUFDLHlCQUF5QixDQUFDO2lCQUNyQyxPQUFPLENBQUMsR0FBRyxFQUFFO2dCQUNiLElBQUksQ0FBQyxVQUFVLEVBQUUsQ0FBQztvQkFDakIsT0FBTztnQkFDUixDQUFDO2dCQUVELFVBQVUsQ0FBQyxJQUFJLEdBQUcsTUFBTSxDQUFDO2dCQUN6QixVQUFVLENBQUMsS0FBSyxFQUFFLENBQUM7Z0JBQ25CLFVBQVUsQ0FBQyxNQUFNLEVBQUUsQ0FBQztnQkFDcEIsSUFBSSxpQkFBTSxDQUFDLDZCQUE2QixDQUFDLENBQUM7WUFDM0MsQ0FBQyxDQUFDLENBQUM7UUFDTCxDQUFDLENBQUM7YUFDRCxjQUFjLENBQUMsQ0FBQyxNQUFNLEVBQUUsRUFBRTtZQUMxQixNQUFNO2lCQUNKLE9BQU8sQ0FBQyxZQUFZLENBQUM7aUJBQ3JCLFVBQVUsQ0FBQyxvQkFBb0IsQ0FBQztpQkFDaEMsT0FBTyxDQUFDLEtBQUssSUFBSSxFQUFFO2dCQUNuQixNQUFNLElBQUksQ0FBQyxNQUFNLENBQUMscUJBQXFCLENBQUMsdUJBQXVCLEVBQUUsQ0FBQyxDQUFDO2dCQUNuRSxJQUFJLENBQUMsT0FBTyxFQUFFLENBQUM7Z0JBQ2YsSUFBSSxpQkFBTSxDQUFDLG9DQUFvQyxDQUFDLENBQUM7WUFDbEQsQ0FBQyxDQUFDLENBQUM7UUFDTCxDQUFDLENBQUMsQ0FBQztJQUNMLENBQUM7SUFFTyx1QkFBdUIsQ0FBQyxPQUFnQjtRQUMvQyxPQUFPLENBQUMsU0FBUyxDQUFDLENBQUMsTUFBTSxFQUFFLEVBQUU7WUFDNUIsTUFBTTtpQkFDSixRQUFRLENBQUMsSUFBSSxDQUFDLE1BQU0sQ0FBQyxRQUFRLENBQUMsVUFBVSxDQUFDO2lCQUN6QyxRQUFRLENBQUMsS0FBSyxFQUFFLEtBQUssRUFBRSxFQUFFO2dCQUN6QixNQUFNLElBQUksQ0FBQyxNQUFNLENBQUMsZ0JBQWdCLENBQUMsS0FBSyxDQUFDLENBQUM7WUFDM0MsQ0FBQyxDQUFDLENBQUM7UUFDTCxDQUFDLENBQUMsQ0FBQztJQUNKLENBQUM7SUFFTyw2QkFBNkIsQ0FBQyxPQUFnQjtRQUNyRCxPQUFPLENBQUMsY0FBYyxDQUFDLENBQUMsTUFBTSxFQUFFLEVBQUU7WUFDakMsTUFBTTtpQkFDSixPQUFPLENBQUMsWUFBWSxDQUFDO2lCQUNyQixVQUFVLENBQUMsV0FBVyxDQUFDO2lCQUN2QixPQUFPLENBQUMsR0FBRyxFQUFFO2dCQUNiLElBQUksQ0FBQyxNQUFNLENBQUMsWUFBWSxFQUFFLENBQUM7Z0JBQzNCLElBQUksaUJBQU0sQ0FBQywyQkFBMkIsQ0FBQyxDQUFDO1lBQ3pDLENBQUMsQ0FBQyxDQUFDO1FBQ0wsQ0FBQyxDQUFDLENBQUM7UUFFSCxJQUFJLENBQUMsUUFBUSxHQUFHLE9BQU8sQ0FBQyxTQUFTLENBQUMsVUFBVSxFQUFFLENBQUM7UUFDL0MsSUFBSSxDQUFDLFFBQVEsQ0FBQyxRQUFRLENBQUMsYUFBYSxDQUFDLENBQUM7UUFDdEMsSUFBSSxDQUFDLFlBQVksQ0FBQyxJQUFJLENBQUMsTUFBTSxDQUFDLGtCQUFrQixFQUFFLENBQUMsQ0FBQztJQUNyRCxDQUFDO0lBRUQsWUFBWSxDQUFDLEtBQXNCO1FBQ2xDLElBQUksQ0FBQyxJQUFJLENBQUMsUUFBUSxFQUFFLENBQUM7WUFDcEIsT0FBTztRQUNSLENBQUM7UUFFRCxJQUFJLENBQUMsUUFBUSxDQUFDLE9BQU8sQ0FBQyxVQUFVLENBQUMsS0FBSyxDQUFDLENBQUMsQ0FBQztRQUN6QyxJQUFJLENBQUMsUUFBUSxDQUFDLFdBQVcsQ0FBQyxjQUFjLEVBQUUsZUFBZSxFQUFFLGlCQUFpQixDQUFDLENBQUM7UUFDOUUsSUFBSSxDQUFDLFFBQVEsQ0FBQyxRQUFRLENBQUMsTUFBTSxLQUFLLEVBQUUsQ0FBQyxDQUFDO0lBQ3ZDLENBQUM7Q0FDRDtBQUVELFNBQVMsaUJBQWlCLENBQUMsTUFBK0IsRUFBRSxHQUFXO0lBQ3RFLE1BQU0sS0FBSyxHQUFHLE1BQU0sQ0FBQyxHQUFHLENBQUMsQ0FBQztJQUUxQixJQUFJLE9BQU8sS0FBSyxLQUFLLFFBQVEsRUFBRSxDQUFDO1FBQy9CLE1BQU0sSUFBSSxLQUFLLENBQUMsa0NBQWtDLEdBQUcsRUFBRSxDQUFDLENBQUM7SUFDMUQsQ0FBQztJQUVELE9BQU8sS0FBSyxDQUFDO0FBQ2QsQ0FBQztBQUVELFNBQVMsaUJBQWlCLENBQUMsTUFBK0IsRUFBRSxHQUFXLEVBQUUsUUFBZ0I7SUFDeEYsTUFBTSxLQUFLLEdBQUcsTUFBTSxDQUFDLEdBQUcsQ0FBQyxDQUFDO0lBQzFCLE9BQU8sT0FBTyxLQUFLLEtBQUssUUFBUSxDQUFDLENBQUMsQ0FBQyxLQUFLLENBQUMsQ0FBQyxDQUFDLFFBQVEsQ0FBQztBQUNyRCxDQUFDO0FBRUQsU0FBUyxRQUFRLENBQUMsS0FBYztJQUMvQixPQUFPLE9BQU8sS0FBSyxLQUFLLFFBQVEsSUFBSSxLQUFLLEtBQUssSUFBSSxJQUFJLENBQUMsS0FBSyxDQUFDLE9BQU8sQ0FBQyxLQUFLLENBQUMsQ0FBQztBQUM3RSxDQUFDO0FBRUQsU0FBUyxjQUFjLENBQUMsSUFBWTtJQUNuQyxPQUFPLElBQUksQ0FBQyxpQkFBaUIsRUFBRSxDQUFDLFFBQVEsQ0FBQyxLQUFLLENBQUMsQ0FBQztBQUNqRCxDQUFDO0FBRUQsU0FBUyxvQkFBb0IsQ0FBQyxJQUFZO0lBQ3pDLE1BQU0sY0FBYyxHQUFHLElBQUksQ0FBQyxJQUFJLEVBQUUsQ0FBQyxPQUFPLENBQUMsWUFBWSxFQUFFLEVBQUUsQ0FBQyxDQUFDO0lBRTdELElBQUksQ0FBQyxjQUFjLEVBQUUsQ0FBQztRQUNyQixPQUFPLEVBQUUsQ0FBQztJQUNYLENBQUM7SUFFRCxNQUFNLEtBQUssR0FBRyxjQUFjLENBQUMsS0FBSyxDQUFDLEdBQUcsQ0FBQyxDQUFDLE1BQU0sQ0FBQyxPQUFPLENBQUMsQ0FBQztJQUV4RCxJQUFJLEtBQUssQ0FBQyxRQUFRLENBQUMsSUFBSSxDQUFDLElBQUksS0FBSyxDQUFDLElBQUksQ0FBQyxDQUFDLElBQUksRUFBRSxFQUFFLENBQUMsSUFBSSxLQUFLLEdBQUcsQ0FBQyxFQUFFLENBQUM7UUFDaEUsTUFBTSxJQUFJLEtBQUssQ0FBQyx5RUFBeUUsQ0FBQyxDQUFDO0lBQzVGLENBQUM7SUFFRCxPQUFPLEtBQUssQ0FBQyxJQUFJLENBQUMsR0FBRyxDQUFDLENBQUM7QUFDeEIsQ0FBQztBQUVELFNBQVMsV0FBVyxDQUFDLE9BQWUsRUFBRSxLQUFhLEVBQUUsV0FBbUI7SUFDdkUsTUFBTSxVQUFVLEdBQUcsSUFBSSxDQUFDLEtBQUssQ0FBQyxDQUFDLGtCQUFrQixHQUFHLFdBQVcsQ0FBQyxHQUFHLENBQUMsQ0FBQyxDQUFDO0lBQ3RFLE1BQU0sS0FBSyxHQUFHLElBQUksQ0FBQyxHQUFHLENBQUMsQ0FBQyxFQUFFLEtBQUssR0FBRyxVQUFVLENBQUMsQ0FBQztJQUM5QyxNQUFNLEdBQUcsR0FBRyxJQUFJLENBQUMsR0FBRyxDQUFDLE9BQU8sQ0FBQyxNQUFNLEVBQUUsS0FBSyxHQUFHLFdBQVcsR0FBRyxVQUFVLENBQUMsQ0FBQztJQUN2RSxNQUFNLE1BQU0sR0FBRyxLQUFLLEdBQUcsQ0FBQyxDQUFDLENBQUMsQ0FBQyxLQUFLLENBQUMsQ0FBQyxDQUFDLEVBQUUsQ0FBQztJQUN0QyxNQUFNLE1BQU0sR0FBRyxHQUFHLEdBQUcsT0FBTyxDQUFDLE1BQU0sQ0FBQyxDQUFDLENBQUMsS0FBSyxDQUFDLENBQUMsQ0FBQyxFQUFFLENBQUM7SUFFakQsT0FBTyxHQUFHLE1BQU0sR0FBRyxPQUFPLENBQUMsS0FBSyxDQUFDLEtBQUssRUFBRSxHQUFHLENBQUMsQ0FBQyxPQUFPLENBQUMsTUFBTSxFQUFFLEdBQUcsQ0FBQyxDQUFDLElBQUksRUFBRSxHQUFHLE1BQU0sRUFBRSxDQUFDO0FBQ3JGLENBQUM7QUFFRCxTQUFTLFVBQVUsQ0FBQyxLQUFhO0lBQ2hDLE9BQU8sS0FBSyxDQUFDLE1BQU0sQ0FBQyxDQUFDLENBQUMsQ0FBQyxpQkFBaUIsRUFBRSxHQUFHLEtBQUssQ0FBQyxLQUFLLENBQUMsQ0FBQyxDQUFDLENBQUM7QUFDN0QsQ0FBQztBQUVELFNBQVMsdUJBQXVCO0lBQy9CLE1BQU0sS0FBSyxHQUFHLElBQUksVUFBVSxDQUFDLEVBQUUsQ0FBQyxDQUFDO0lBQ2pDLE1BQU0sQ0FBQyxlQUFlLENBQUMsS0FBSyxDQUFDLENBQUM7SUFDOUIsT0FBTyxHQUFHLHNCQUFzQixHQUFHLFdBQVcsQ0FBQyxLQUFLLENBQUMsRUFBRSxDQUFDO0FBQ3pELENBQUM7QUFFRCxTQUFTLFdBQVcsQ0FBQyxLQUFpQjtJQUNyQyxJQUFJLE1BQU0sR0FBRyxFQUFFLENBQUM7SUFFaEIsS0FBSyxNQUFNLElBQUksSUFBSSxLQUFLLEVBQUUsQ0FBQztRQUMxQixNQUFNLElBQUksTUFBTSxDQUFDLFlBQVksQ0FBQyxJQUFJLENBQUMsQ0FBQztJQUNyQyxDQUFDO0lBRUQsT0FBTyxJQUFJLENBQUMsTUFBTSxDQUFDLENBQUMsT0FBTyxDQUFDLEtBQUssRUFBRSxHQUFHLENBQUMsQ0FBQyxPQUFPLENBQUMsS0FBSyxFQUFFLEdBQUcsQ0FBQyxDQUFDLE9BQU8sQ0FBQyxNQUFNLEVBQUUsRUFBRSxDQUFDLENBQUM7QUFDakYsQ0FBQztBQUVELFNBQVMsV0FBVyxDQUFDLEdBQVE7SUFDNUIsTUFBTSxJQUFJLEdBQUcsSUFBSSxHQUFHLENBQUMsR0FBRyxDQUFDLFFBQVEsRUFBRSxDQUFDLENBQUM7SUFFckMsSUFBSSxJQUFJLENBQUMsWUFBWSxDQUFDLEdBQUcsQ0FBQyxPQUFPLENBQUMsRUFBRSxDQUFDO1FBQ3BDLElBQUksQ0FBQyxZQUFZLENBQUMsR0FBRyxDQUFDLE9BQU8sRUFBRSxLQUFLLENBQUMsQ0FBQztJQUN2QyxDQUFDO0lBRUQsT0FBTyxJQUFJLENBQUMsUUFBUSxFQUFFLENBQUM7QUFDeEIsQ0FBQyIsInNvdXJjZXNDb250ZW50IjpbImltcG9ydCB7IEFwcCwgTm90aWNlLCBQbHVnaW4sIFBsdWdpblNldHRpbmdUYWIsIFNldHRpbmcsIFRGaWxlIH0gZnJvbSBcIm9ic2lkaWFuXCI7XG5cbmNvbnN0IERFRkFVTFRfR0FURVdBWV9VUkwgPSBcIndzczovL29ic2lkaWFuLm1hdHQtbnouY29tL29ic2lkaWFuL3N5bmNcIjtcbmNvbnN0IERFRkFVTFRfVkFVTFRfRk9MREVSID0gXCJQb2tlXCI7XG5jb25zdCBCQVNFX1JFQ09OTkVDVF9ERUxBWV9NUyA9IDFfMDAwO1xuY29uc3QgTUFYX1JFQ09OTkVDVF9ERUxBWV9NUyA9IDMwXzAwMDtcbmNvbnN0IFJFUVVFU1RfVElNRU9VVF9NUyA9IDMwXzAwMDtcbmNvbnN0IE1BWF9TTklQUEVUX0xFTkdUSCA9IDE4MDtcbmNvbnN0IEdFTkVSQVRFRF9UT0tFTl9QUkVGSVggPSBcInBrb2JzX3ZhdWx0X1wiO1xuXG50eXBlIENvbm5lY3Rpb25TdGF0ZSA9IFwiY29ubmVjdGVkXCIgfCBcImNvbm5lY3RpbmdcIiB8IFwiZGlzY29ubmVjdGVkXCI7XG5cbmludGVyZmFjZSBQb2tlT2JzaWRpYW5TZXR0aW5ncyB7XG5cdGdhdGV3YXlVcmw6IHN0cmluZztcblx0Y29ubmVjdGlvblRva2VuOiBzdHJpbmc7XG5cdHZhdWx0Rm9sZGVyOiBzdHJpbmc7XG5cdGFsbG93V3JpdGU6IGJvb2xlYW47XG59XG5cbmludGVyZmFjZSBJbmNvbWluZ01lc3NhZ2Uge1xuXHRpZD86IHVua25vd247XG5cdGFjdGlvbj86IHVua25vd247XG5cdHBhcmFtcz86IHVua25vd247XG59XG5cbmludGVyZmFjZSBSZXNwb25zZU1lc3NhZ2Uge1xuXHRpZDogc3RyaW5nIHwgbnVsbDtcblx0c3RhdHVzOiBcInN1Y2Nlc3NcIiB8IFwiZXJyb3JcIjtcblx0cGF5bG9hZDogUmVjb3JkPHN0cmluZywgdW5rbm93bj47XG59XG5cbmludGVyZmFjZSBTZWFyY2hNYXRjaCB7XG5cdHBhdGg6IHN0cmluZztcblx0c25pcHBldDogc3RyaW5nO1xufVxuXG5jb25zdCBERUZBVUxUX1NFVFRJTkdTOiBQb2tlT2JzaWRpYW5TZXR0aW5ncyA9IHtcblx0Z2F0ZXdheVVybDogREVGQVVMVF9HQVRFV0FZX1VSTCxcblx0Y29ubmVjdGlvblRva2VuOiBcIlwiLFxuXHR2YXVsdEZvbGRlcjogREVGQVVMVF9WQVVMVF9GT0xERVIsXG5cdGFsbG93V3JpdGU6IGZhbHNlLFxufTtcblxuZXhwb3J0IGRlZmF1bHQgY2xhc3MgUG9rZU9ic2lkaWFuUGx1Z2luIGV4dGVuZHMgUGx1Z2luIHtcblx0c2V0dGluZ3M6IFBva2VPYnNpZGlhblNldHRpbmdzID0gREVGQVVMVF9TRVRUSU5HUztcblx0cHJpdmF0ZSBzb2NrZXQ6IFdlYlNvY2tldCB8IG51bGwgPSBudWxsO1xuXHRwcml2YXRlIHN0YXR1c0Jhckl0ZW1FbDogSFRNTEVsZW1lbnQgfCBudWxsID0gbnVsbDtcblx0cHJpdmF0ZSBzZXR0aW5nc1RhYjogUG9rZU9ic2lkaWFuU2V0dGluZ1RhYiB8IG51bGwgPSBudWxsO1xuXHRwcml2YXRlIGNvbm5lY3Rpb25TdGF0ZTogQ29ubmVjdGlvblN0YXRlID0gXCJkaXNjb25uZWN0ZWRcIjtcblx0cHJpdmF0ZSByZWNvbm5lY3RBdHRlbXB0cyA9IDA7XG5cdHByaXZhdGUgcmVjb25uZWN0VGltZXI6IG51bWJlciB8IG51bGwgPSBudWxsO1xuXHRwcml2YXRlIHVubG9hZFJlcXVlc3RlZCA9IGZhbHNlO1xuXG5cdGFzeW5jIG9ubG9hZCgpOiBQcm9taXNlPHZvaWQ+IHtcblx0XHRhd2FpdCB0aGlzLmxvYWRTZXR0aW5ncygpO1xuXG5cdFx0aWYgKCF0aGlzLnNldHRpbmdzLmNvbm5lY3Rpb25Ub2tlbikge1xuXHRcdFx0dGhpcy5zZXR0aW5ncy5jb25uZWN0aW9uVG9rZW4gPSBnZW5lcmF0ZUNvbm5lY3Rpb25Ub2tlbigpO1xuXHRcdFx0YXdhaXQgdGhpcy5zYXZlU2V0dGluZ3MoKTtcblx0XHR9XG5cblx0XHR0aGlzLnN0YXR1c0Jhckl0ZW1FbCA9IHRoaXMuYWRkU3RhdHVzQmFySXRlbSgpO1xuXHRcdHRoaXMuc3RhdHVzQmFySXRlbUVsLmFkZENsYXNzKFwicG9rZS1zdGF0dXNcIik7XG5cdFx0dGhpcy5zZXRDb25uZWN0aW9uU3RhdGUoXCJkaXNjb25uZWN0ZWRcIik7XG5cblx0XHR0aGlzLnNldHRpbmdzVGFiID0gbmV3IFBva2VPYnNpZGlhblNldHRpbmdUYWIodGhpcy5hcHAsIHRoaXMpO1xuXHRcdHRoaXMuYWRkU2V0dGluZ1RhYih0aGlzLnNldHRpbmdzVGFiKTtcblxuXHRcdHRoaXMuY29ubmVjdCgpO1xuXHR9XG5cblx0b251bmxvYWQoKTogdm9pZCB7XG5cdFx0dGhpcy51bmxvYWRSZXF1ZXN0ZWQgPSB0cnVlO1xuXHRcdHRoaXMuY2xlYXJSZWNvbm5lY3RUaW1lcigpO1xuXHRcdHRoaXMuY2xvc2VTb2NrZXQoKTtcblx0XHR0aGlzLnNldENvbm5lY3Rpb25TdGF0ZShcImRpc2Nvbm5lY3RlZFwiKTtcblx0fVxuXG5cdGFzeW5jIGxvYWRTZXR0aW5ncygpOiBQcm9taXNlPHZvaWQ+IHtcblx0XHRjb25zdCBzYXZlZFNldHRpbmdzOiB1bmtub3duID0gYXdhaXQgdGhpcy5sb2FkRGF0YSgpO1xuXHRcdGNvbnN0IHNldHRpbmdzUmVjb3JkID0gaXNSZWNvcmQoc2F2ZWRTZXR0aW5ncykgPyBzYXZlZFNldHRpbmdzIDoge307XG5cblx0XHR0aGlzLnNldHRpbmdzID0ge1xuXHRcdFx0Z2F0ZXdheVVybDogZ2V0T3B0aW9uYWxTdHJpbmcoc2V0dGluZ3NSZWNvcmQsIFwiZ2F0ZXdheVVybFwiLCBERUZBVUxUX0dBVEVXQVlfVVJMKSxcblx0XHRcdGNvbm5lY3Rpb25Ub2tlbjogZ2V0T3B0aW9uYWxTdHJpbmcoc2V0dGluZ3NSZWNvcmQsIFwiY29ubmVjdGlvblRva2VuXCIsIFwiXCIpLFxuXHRcdFx0dmF1bHRGb2xkZXI6IGdldE9wdGlvbmFsU3RyaW5nKHNldHRpbmdzUmVjb3JkLCBcInZhdWx0Rm9sZGVyXCIsIERFRkFVTFRfVkFVTFRfRk9MREVSKSxcblx0XHRcdGFsbG93V3JpdGU6XG5cdFx0XHRcdHR5cGVvZiBzZXR0aW5nc1JlY29yZC5hbGxvd1dyaXRlID09PSBcImJvb2xlYW5cIiA/IHNldHRpbmdzUmVjb3JkLmFsbG93V3JpdGUgOiBERUZBVUxUX1NFVFRJTkdTLmFsbG93V3JpdGUsXG5cdFx0fTtcblxuXHRcdGlmIChcblx0XHRcdHRoaXMuc2V0dGluZ3MuY29ubmVjdGlvblRva2VuICYmXG5cdFx0XHQhT2JqZWN0LnByb3RvdHlwZS5oYXNPd25Qcm9wZXJ0eS5jYWxsKHNldHRpbmdzUmVjb3JkLCBcInZhdWx0Rm9sZGVyXCIpXG5cdFx0KSB7XG5cdFx0XHR0aGlzLnNldHRpbmdzLnZhdWx0Rm9sZGVyID0gXCJcIjtcblx0XHR9XG5cdH1cblxuXHRhc3luYyBzYXZlU2V0dGluZ3MoKTogUHJvbWlzZTx2b2lkPiB7XG5cdFx0YXdhaXQgdGhpcy5zYXZlRGF0YSh0aGlzLnNldHRpbmdzKTtcblx0fVxuXG5cdGdldENvbm5lY3Rpb25TdGF0ZSgpOiBDb25uZWN0aW9uU3RhdGUge1xuXHRcdHJldHVybiB0aGlzLmNvbm5lY3Rpb25TdGF0ZTtcblx0fVxuXG5cdGFzeW5jIHVwZGF0ZUNvbm5lY3Rpb25Ub2tlbih0b2tlbjogc3RyaW5nKTogUHJvbWlzZTx2b2lkPiB7XG5cdFx0dGhpcy5zZXR0aW5ncy5jb25uZWN0aW9uVG9rZW4gPSB0b2tlbi50cmltKCk7XG5cdFx0YXdhaXQgdGhpcy5zYXZlU2V0dGluZ3MoKTtcblx0XHR0aGlzLnJlY29ubmVjdE5vdygpO1xuXHR9XG5cblx0YXN5bmMgdXBkYXRlR2F0ZXdheVVybChnYXRld2F5VXJsOiBzdHJpbmcpOiBQcm9taXNlPHZvaWQ+IHtcblx0XHR0aGlzLnNldHRpbmdzLmdhdGV3YXlVcmwgPSBnYXRld2F5VXJsLnRyaW0oKSB8fCBERUZBVUxUX0dBVEVXQVlfVVJMO1xuXHRcdGF3YWl0IHRoaXMuc2F2ZVNldHRpbmdzKCk7XG5cdFx0dGhpcy5yZWNvbm5lY3ROb3coKTtcblx0fVxuXG5cdGFzeW5jIHVwZGF0ZUFsbG93V3JpdGUoYWxsb3dXcml0ZTogYm9vbGVhbik6IFByb21pc2U8dm9pZD4ge1xuXHRcdHRoaXMuc2V0dGluZ3MuYWxsb3dXcml0ZSA9IGFsbG93V3JpdGU7XG5cdFx0YXdhaXQgdGhpcy5zYXZlU2V0dGluZ3MoKTtcblx0fVxuXG5cdGFzeW5jIHVwZGF0ZVZhdWx0Rm9sZGVyKHZhdWx0Rm9sZGVyOiBzdHJpbmcpOiBQcm9taXNlPHZvaWQ+IHtcblx0XHR0aGlzLnNldHRpbmdzLnZhdWx0Rm9sZGVyID0gdmF1bHRGb2xkZXIudHJpbSgpO1xuXHRcdGF3YWl0IHRoaXMuc2F2ZVNldHRpbmdzKCk7XG5cdH1cblxuXHRyZWNvbm5lY3ROb3coKTogdm9pZCB7XG5cdFx0dGhpcy5jbGVhclJlY29ubmVjdFRpbWVyKCk7XG5cdFx0dGhpcy5yZWNvbm5lY3RBdHRlbXB0cyA9IDA7XG5cdFx0dGhpcy5jbG9zZVNvY2tldCgpO1xuXG5cdFx0aWYgKCF0aGlzLnNldHRpbmdzLmNvbm5lY3Rpb25Ub2tlbikge1xuXHRcdFx0dGhpcy5zZXRDb25uZWN0aW9uU3RhdGUoXCJkaXNjb25uZWN0ZWRcIik7XG5cdFx0XHRyZXR1cm47XG5cdFx0fVxuXG5cdFx0dGhpcy5jb25uZWN0KCk7XG5cdH1cblxuXHRwcml2YXRlIGNvbm5lY3QoKTogdm9pZCB7XG5cdFx0aWYgKHRoaXMudW5sb2FkUmVxdWVzdGVkIHx8ICF0aGlzLnNldHRpbmdzLmNvbm5lY3Rpb25Ub2tlbikge1xuXHRcdFx0dGhpcy5zZXRDb25uZWN0aW9uU3RhdGUoXCJkaXNjb25uZWN0ZWRcIik7XG5cdFx0XHRyZXR1cm47XG5cdFx0fVxuXG5cdFx0aWYgKFxuXHRcdFx0dGhpcy5zb2NrZXQgJiZcblx0XHRcdCh0aGlzLnNvY2tldC5yZWFkeVN0YXRlID09PSBXZWJTb2NrZXQuQ09OTkVDVElORyB8fCB0aGlzLnNvY2tldC5yZWFkeVN0YXRlID09PSBXZWJTb2NrZXQuT1BFTilcblx0XHQpIHtcblx0XHRcdHJldHVybjtcblx0XHR9XG5cblx0XHR0aGlzLnNldENvbm5lY3Rpb25TdGF0ZShcImNvbm5lY3RpbmdcIik7XG5cblx0XHRjb25zdCB1cmwgPSB0aGlzLmJ1aWxkR2F0ZXdheVVybCgpO1xuXG5cdFx0aWYgKCF1cmwpIHtcblx0XHRcdHRoaXMuc2V0Q29ubmVjdGlvblN0YXRlKFwiZGlzY29ubmVjdGVkXCIpO1xuXHRcdFx0cmV0dXJuO1xuXHRcdH1cblxuXHRcdHRyeSB7XG5cdFx0XHRjb25zb2xlLmxvZyhgUG9rZSBHYXRld2F5IGNvbm5lY3RpbmcgdG8gJHtyZWRhY3RUb2tlbih1cmwpfWApO1xuXHRcdFx0dGhpcy5zb2NrZXQgPSBuZXcgV2ViU29ja2V0KHVybC50b1N0cmluZygpKTtcblx0XHR9IGNhdGNoIChlcnJvcikge1xuXHRcdFx0dGhpcy5oYW5kbGVDb25uZWN0aW9uRXJyb3IoZXJyb3IpO1xuXHRcdFx0dGhpcy5zY2hlZHVsZVJlY29ubmVjdCgpO1xuXHRcdFx0cmV0dXJuO1xuXHRcdH1cblxuXHRcdHRoaXMuc29ja2V0Lm9ub3BlbiA9ICgpID0+IHtcblx0XHRcdGNvbnNvbGUubG9nKFwiUG9rZSBHYXRld2F5IGNvbm5lY3RlZFwiKTtcblx0XHRcdHRoaXMucmVjb25uZWN0QXR0ZW1wdHMgPSAwO1xuXHRcdFx0dGhpcy5zZXRDb25uZWN0aW9uU3RhdGUoXCJjb25uZWN0ZWRcIik7XG5cdFx0fTtcblxuXHRcdHRoaXMuc29ja2V0Lm9ubWVzc2FnZSA9IChldmVudDogTWVzc2FnZUV2ZW50PHN0cmluZz4pID0+IHtcblx0XHRcdHZvaWQgdGhpcy53aXRoUmVxdWVzdFRpbWVvdXQodGhpcy5oYW5kbGVTb2NrZXRNZXNzYWdlKGV2ZW50LmRhdGEpLCBudWxsKTtcblx0XHR9O1xuXG5cdFx0dGhpcy5zb2NrZXQub25lcnJvciA9IChldmVudCkgPT4ge1xuXHRcdFx0dGhpcy5oYW5kbGVDb25uZWN0aW9uRXJyb3IobmV3IEVycm9yKGBXZWJTb2NrZXQgY29ubmVjdGlvbiBlcnJvcjogJHtKU09OLnN0cmluZ2lmeShldmVudCl9YCkpO1xuXHRcdH07XG5cblx0XHR0aGlzLnNvY2tldC5vbmNsb3NlID0gKGV2ZW50KSA9PiB7XG5cdFx0XHRjb25zb2xlLmxvZyhgUG9rZSBHYXRld2F5IGRpc2Nvbm5lY3RlZDogY29kZT0ke2V2ZW50LmNvZGV9IHJlYXNvbj0ke2V2ZW50LnJlYXNvbiB8fCBcIihub25lKVwifWApO1xuXHRcdFx0dGhpcy5zb2NrZXQgPSBudWxsO1xuXG5cdFx0XHRpZiAodGhpcy51bmxvYWRSZXF1ZXN0ZWQpIHtcblx0XHRcdFx0dGhpcy5zZXRDb25uZWN0aW9uU3RhdGUoXCJkaXNjb25uZWN0ZWRcIik7XG5cdFx0XHRcdHJldHVybjtcblx0XHRcdH1cblxuXHRcdFx0dGhpcy5zZXRDb25uZWN0aW9uU3RhdGUoXCJkaXNjb25uZWN0ZWRcIik7XG5cdFx0XHR0aGlzLnNjaGVkdWxlUmVjb25uZWN0KCk7XG5cdFx0fTtcblx0fVxuXG5cdHByaXZhdGUgc2NoZWR1bGVSZWNvbm5lY3QoKTogdm9pZCB7XG5cdFx0aWYgKHRoaXMudW5sb2FkUmVxdWVzdGVkIHx8ICF0aGlzLnNldHRpbmdzLmNvbm5lY3Rpb25Ub2tlbiB8fCB0aGlzLnJlY29ubmVjdFRpbWVyICE9PSBudWxsKSB7XG5cdFx0XHRyZXR1cm47XG5cdFx0fVxuXG5cdFx0Y29uc3QgZGVsYXkgPSBNYXRoLm1pbihCQVNFX1JFQ09OTkVDVF9ERUxBWV9NUyAqIDIgKiogdGhpcy5yZWNvbm5lY3RBdHRlbXB0cywgTUFYX1JFQ09OTkVDVF9ERUxBWV9NUyk7XG5cdFx0dGhpcy5yZWNvbm5lY3RBdHRlbXB0cyArPSAxO1xuXG5cdFx0dGhpcy5yZWNvbm5lY3RUaW1lciA9IHdpbmRvdy5zZXRUaW1lb3V0KCgpID0+IHtcblx0XHRcdHRoaXMucmVjb25uZWN0VGltZXIgPSBudWxsO1xuXHRcdFx0dGhpcy5jb25uZWN0KCk7XG5cdFx0fSwgZGVsYXkpO1xuXHR9XG5cblx0cHJpdmF0ZSBjbGVhclJlY29ubmVjdFRpbWVyKCk6IHZvaWQge1xuXHRcdGlmICh0aGlzLnJlY29ubmVjdFRpbWVyICE9PSBudWxsKSB7XG5cdFx0XHR3aW5kb3cuY2xlYXJUaW1lb3V0KHRoaXMucmVjb25uZWN0VGltZXIpO1xuXHRcdFx0dGhpcy5yZWNvbm5lY3RUaW1lciA9IG51bGw7XG5cdFx0fVxuXHR9XG5cblx0cHJpdmF0ZSBjbG9zZVNvY2tldCgpOiB2b2lkIHtcblx0XHRpZiAoIXRoaXMuc29ja2V0KSB7XG5cdFx0XHRyZXR1cm47XG5cdFx0fVxuXG5cdFx0dGhpcy5zb2NrZXQub25vcGVuID0gbnVsbDtcblx0XHR0aGlzLnNvY2tldC5vbm1lc3NhZ2UgPSBudWxsO1xuXHRcdHRoaXMuc29ja2V0Lm9uZXJyb3IgPSBudWxsO1xuXHRcdHRoaXMuc29ja2V0Lm9uY2xvc2UgPSBudWxsO1xuXG5cdFx0aWYgKHRoaXMuc29ja2V0LnJlYWR5U3RhdGUgPT09IFdlYlNvY2tldC5DT05ORUNUSU5HIHx8IHRoaXMuc29ja2V0LnJlYWR5U3RhdGUgPT09IFdlYlNvY2tldC5PUEVOKSB7XG5cdFx0XHR0aGlzLnNvY2tldC5jbG9zZSgpO1xuXHRcdH1cblxuXHRcdHRoaXMuc29ja2V0ID0gbnVsbDtcblx0fVxuXG5cdHByaXZhdGUgYXN5bmMgd2l0aFJlcXVlc3RUaW1lb3V0KHRhc2s6IFByb21pc2U8dm9pZD4sIGlkOiBzdHJpbmcgfCBudWxsKTogUHJvbWlzZTx2b2lkPiB7XG5cdFx0bGV0IHRpbWVvdXRJZDogbnVtYmVyIHwgbnVsbCA9IG51bGw7XG5cblx0XHRjb25zdCB0aW1lb3V0ID0gbmV3IFByb21pc2U8dm9pZD4oKF9yZXNvbHZlLCByZWplY3QpID0+IHtcblx0XHRcdHRpbWVvdXRJZCA9IHdpbmRvdy5zZXRUaW1lb3V0KCgpID0+IHJlamVjdChuZXcgRXJyb3IoXCJSZXF1ZXN0IHRpbWVkIG91dFwiKSksIFJFUVVFU1RfVElNRU9VVF9NUyk7XG5cdFx0fSk7XG5cblx0XHR0cnkge1xuXHRcdFx0YXdhaXQgUHJvbWlzZS5yYWNlKFt0YXNrLCB0aW1lb3V0XSk7XG5cdFx0fSBjYXRjaCAoZXJyb3IpIHtcblx0XHRcdHRoaXMuc2VuZFJlc3BvbnNlKHtcblx0XHRcdFx0aWQsXG5cdFx0XHRcdHN0YXR1czogXCJlcnJvclwiLFxuXHRcdFx0XHRwYXlsb2FkOiB7IGVycm9yOiBlcnJvciBpbnN0YW5jZW9mIEVycm9yID8gZXJyb3IubWVzc2FnZSA6IFN0cmluZyhlcnJvcikgfSxcblx0XHRcdH0pO1xuXHRcdH0gZmluYWxseSB7XG5cdFx0XHRpZiAodGltZW91dElkICE9PSBudWxsKSB7XG5cdFx0XHRcdHdpbmRvdy5jbGVhclRpbWVvdXQodGltZW91dElkKTtcblx0XHRcdH1cblx0XHR9XG5cdH1cblxuXHRwcml2YXRlIGFzeW5jIGhhbmRsZVNvY2tldE1lc3NhZ2UocmF3TWVzc2FnZTogc3RyaW5nKTogUHJvbWlzZTx2b2lkPiB7XG5cdFx0bGV0IGluY29taW5nOiBJbmNvbWluZ01lc3NhZ2U7XG5cblx0XHR0cnkge1xuXHRcdFx0aW5jb21pbmcgPSBKU09OLnBhcnNlKHJhd01lc3NhZ2UpIGFzIEluY29taW5nTWVzc2FnZTtcblx0XHR9IGNhdGNoIHtcblx0XHRcdHRoaXMuc2VuZFJlc3BvbnNlKHtcblx0XHRcdFx0aWQ6IG51bGwsXG5cdFx0XHRcdHN0YXR1czogXCJlcnJvclwiLFxuXHRcdFx0XHRwYXlsb2FkOiB7IGVycm9yOiBcIkludmFsaWQgSlNPTiBtZXNzYWdlXCIgfSxcblx0XHRcdH0pO1xuXHRcdFx0cmV0dXJuO1xuXHRcdH1cblxuXHRcdGNvbnN0IGlkID0gdHlwZW9mIGluY29taW5nLmlkID09PSBcInN0cmluZ1wiID8gaW5jb21pbmcuaWQgOiBudWxsO1xuXHRcdGNvbnN0IGFjdGlvbiA9IHR5cGVvZiBpbmNvbWluZy5hY3Rpb24gPT09IFwic3RyaW5nXCIgPyBpbmNvbWluZy5hY3Rpb24gOiBcIlwiO1xuXHRcdGNvbnN0IHBhcmFtcyA9IGlzUmVjb3JkKGluY29taW5nLnBhcmFtcykgPyBpbmNvbWluZy5wYXJhbXMgOiB7fTtcblxuXHRcdGlmICghaWQpIHtcblx0XHRcdHRoaXMuc2VuZFJlc3BvbnNlKHtcblx0XHRcdFx0aWQsXG5cdFx0XHRcdHN0YXR1czogXCJlcnJvclwiLFxuXHRcdFx0XHRwYXlsb2FkOiB7IGVycm9yOiBcIk1lc3NhZ2UgaWQgaXMgcmVxdWlyZWRcIiB9LFxuXHRcdFx0fSk7XG5cdFx0XHRyZXR1cm47XG5cdFx0fVxuXG5cdFx0dHJ5IHtcblx0XHRcdGNvbnN0IHBheWxvYWQgPSBhd2FpdCB0aGlzLmhhbmRsZUFjdGlvbihhY3Rpb24sIHBhcmFtcyk7XG5cdFx0XHR0aGlzLnNlbmRSZXNwb25zZSh7IGlkLCBzdGF0dXM6IFwic3VjY2Vzc1wiLCBwYXlsb2FkIH0pO1xuXHRcdH0gY2F0Y2ggKGVycm9yKSB7XG5cdFx0XHR0aGlzLnNlbmRSZXNwb25zZSh7XG5cdFx0XHRcdGlkLFxuXHRcdFx0XHRzdGF0dXM6IFwiZXJyb3JcIixcblx0XHRcdFx0cGF5bG9hZDogeyBlcnJvcjogZXJyb3IgaW5zdGFuY2VvZiBFcnJvciA/IGVycm9yLm1lc3NhZ2UgOiBTdHJpbmcoZXJyb3IpIH0sXG5cdFx0XHR9KTtcblx0XHR9XG5cdH1cblxuXHRwcml2YXRlIGFzeW5jIGhhbmRsZUFjdGlvbihhY3Rpb246IHN0cmluZywgcGFyYW1zOiBSZWNvcmQ8c3RyaW5nLCB1bmtub3duPik6IFByb21pc2U8UmVjb3JkPHN0cmluZywgdW5rbm93bj4+IHtcblx0XHRzd2l0Y2ggKGFjdGlvbikge1xuXHRcdFx0Y2FzZSBcImxpc3RcIjpcblx0XHRcdGNhc2UgXCJsaXN0X2ZpbGVzXCI6XG5cdFx0XHRcdHJldHVybiB0aGlzLmxpc3RGaWxlcygpO1xuXHRcdFx0Y2FzZSBcInJlYWRcIjpcblx0XHRcdGNhc2UgXCJyZWFkX2ZpbGVcIjpcblx0XHRcdFx0cmV0dXJuIHRoaXMucmVhZEZpbGUocGFyYW1zKTtcblx0XHRcdGNhc2UgXCJ3cml0ZVwiOlxuXHRcdFx0Y2FzZSBcIndyaXRlX2ZpbGVcIjpcblx0XHRcdFx0cmV0dXJuIHRoaXMud3JpdGVGaWxlKHBhcmFtcyk7XG5cdFx0XHRjYXNlIFwic2VhcmNoXCI6XG5cdFx0XHRjYXNlIFwic2VhcmNoX3ZhdWx0XCI6XG5cdFx0XHRcdHJldHVybiB0aGlzLnNlYXJjaFZhdWx0KHBhcmFtcyk7XG5cdFx0XHRkZWZhdWx0OlxuXHRcdFx0XHR0aHJvdyBuZXcgRXJyb3IoYFVuc3VwcG9ydGVkIGFjdGlvbjogJHthY3Rpb24gfHwgXCIobWlzc2luZylcIn1gKTtcblx0XHR9XG5cdH1cblxuXHRwcml2YXRlIGFzeW5jIGxpc3RGaWxlcygpOiBQcm9taXNlPFJlY29yZDxzdHJpbmcsIHVua25vd24+PiB7XG5cdFx0cmV0dXJuIHtcblx0XHRcdGZpbGVzOiBhd2FpdCB0aGlzLmxpc3RNYXJrZG93blBhdGhzKCksXG5cdFx0fTtcblx0fVxuXG5cdHByaXZhdGUgYXN5bmMgcmVhZEZpbGUocGFyYW1zOiBSZWNvcmQ8c3RyaW5nLCB1bmtub3duPik6IFByb21pc2U8UmVjb3JkPHN0cmluZywgdW5rbm93bj4+IHtcblx0XHRjb25zdCBwYXRoID0gZ2V0UmVxdWlyZWRTdHJpbmcocGFyYW1zLCBcInBhdGhcIik7XG5cdFx0Y29uc3Qgbm9ybWFsaXplZFBhdGggPSB0aGlzLm5vcm1hbGl6ZU1hcmtkb3duUGF0aChwYXRoKTtcblx0XHR0aGlzLmVuc3VyZVBhdGhJblZhdWx0Rm9sZGVyKG5vcm1hbGl6ZWRQYXRoKTtcblxuXHRcdGNvbnN0IGZpbGUgPSB0aGlzLmdldE1hcmtkb3duRmlsZShub3JtYWxpemVkUGF0aCk7XG5cdFx0Y29uc3QgY29udGVudCA9IGF3YWl0IHRoaXMuYXBwLnZhdWx0LmFkYXB0ZXIucmVhZChmaWxlLnBhdGgpO1xuXG5cdFx0cmV0dXJuIHtcblx0XHRcdHBhdGg6IGZpbGUucGF0aCxcblx0XHRcdGNvbnRlbnQsXG5cdFx0fTtcblx0fVxuXG5cdHByaXZhdGUgYXN5bmMgd3JpdGVGaWxlKHBhcmFtczogUmVjb3JkPHN0cmluZywgdW5rbm93bj4pOiBQcm9taXNlPFJlY29yZDxzdHJpbmcsIHVua25vd24+PiB7XG5cdFx0aWYgKCF0aGlzLnNldHRpbmdzLmFsbG93V3JpdGUpIHtcblx0XHRcdHRocm93IG5ldyBFcnJvcihcIldyaXRlIGFjY2VzcyBpcyBkaXNhYmxlZCBpbiBQb2tlIEdhdGV3YXkgc2V0dGluZ3NcIik7XG5cdFx0fVxuXG5cdFx0Y29uc3QgcGF0aCA9IGdldFJlcXVpcmVkU3RyaW5nKHBhcmFtcywgXCJwYXRoXCIpO1xuXHRcdGNvbnN0IGNvbnRlbnQgPSBnZXRSZXF1aXJlZFN0cmluZyhwYXJhbXMsIFwiY29udGVudFwiKTtcblx0XHRjb25zdCBub3JtYWxpemVkUGF0aCA9IHRoaXMubm9ybWFsaXplTWFya2Rvd25QYXRoKHBhdGgpO1xuXHRcdHRoaXMuZW5zdXJlUGF0aEluVmF1bHRGb2xkZXIobm9ybWFsaXplZFBhdGgpO1xuXG5cdFx0YXdhaXQgdGhpcy5lbnN1cmVQYXJlbnRGb2xkZXJzKG5vcm1hbGl6ZWRQYXRoKTtcblx0XHRhd2FpdCB0aGlzLmFwcC52YXVsdC5hZGFwdGVyLndyaXRlKG5vcm1hbGl6ZWRQYXRoLCBjb250ZW50KTtcblxuXHRcdHJldHVybiB7XG5cdFx0XHRwYXRoOiBub3JtYWxpemVkUGF0aCxcblx0XHRcdGJ5dGVzOiBuZXcgVGV4dEVuY29kZXIoKS5lbmNvZGUoY29udGVudCkubGVuZ3RoLFxuXHRcdH07XG5cdH1cblxuXHRwcml2YXRlIGFzeW5jIHNlYXJjaFZhdWx0KHBhcmFtczogUmVjb3JkPHN0cmluZywgdW5rbm93bj4pOiBQcm9taXNlPFJlY29yZDxzdHJpbmcsIHVua25vd24+PiB7XG5cdFx0Y29uc3QgcXVlcnkgPSBnZXRSZXF1aXJlZFN0cmluZyhwYXJhbXMsIFwicXVlcnlcIikudHJpbSgpO1xuXG5cdFx0aWYgKCFxdWVyeSkge1xuXHRcdFx0dGhyb3cgbmV3IEVycm9yKFwiU2VhcmNoIHF1ZXJ5IGNhbm5vdCBiZSBlbXB0eVwiKTtcblx0XHR9XG5cblx0XHRjb25zdCBub3JtYWxpemVkUXVlcnkgPSBxdWVyeS50b0xvY2FsZUxvd2VyQ2FzZSgpO1xuXHRcdGNvbnN0IG1hdGNoZXM6IFNlYXJjaE1hdGNoW10gPSBbXTtcblxuXHRcdGZvciAoY29uc3QgcGF0aCBvZiBhd2FpdCB0aGlzLmxpc3RNYXJrZG93blBhdGhzKCkpIHtcblx0XHRcdGNvbnN0IGNvbnRlbnQgPSBhd2FpdCB0aGlzLmFwcC52YXVsdC5hZGFwdGVyLnJlYWQocGF0aCk7XG5cdFx0XHRjb25zdCBpbmRleCA9IGNvbnRlbnQudG9Mb2NhbGVMb3dlckNhc2UoKS5pbmRleE9mKG5vcm1hbGl6ZWRRdWVyeSk7XG5cblx0XHRcdGlmIChpbmRleCA9PT0gLTEpIHtcblx0XHRcdFx0Y29udGludWU7XG5cdFx0XHR9XG5cblx0XHRcdG1hdGNoZXMucHVzaCh7XG5cdFx0XHRcdHBhdGgsXG5cdFx0XHRcdHNuaXBwZXQ6IG1ha2VTbmlwcGV0KGNvbnRlbnQsIGluZGV4LCBxdWVyeS5sZW5ndGgpLFxuXHRcdFx0fSk7XG5cdFx0fVxuXG5cdFx0cmV0dXJuIHsgbWF0Y2hlcyB9O1xuXHR9XG5cblx0cHJpdmF0ZSBnZXRNYXJrZG93bkZpbGUocGF0aDogc3RyaW5nKTogVEZpbGUge1xuXHRcdGNvbnN0IGZpbGUgPSB0aGlzLmFwcC52YXVsdC5nZXRBYnN0cmFjdEZpbGVCeVBhdGgocGF0aCk7XG5cblx0XHRpZiAoIShmaWxlIGluc3RhbmNlb2YgVEZpbGUpKSB7XG5cdFx0XHR0aHJvdyBuZXcgRXJyb3IoYEZpbGUgbm90IGZvdW5kOiAke3BhdGh9YCk7XG5cdFx0fVxuXG5cdFx0aWYgKGZpbGUuZXh0ZW5zaW9uICE9PSBcIm1kXCIpIHtcblx0XHRcdHRocm93IG5ldyBFcnJvcihgT25seSBtYXJrZG93biBmaWxlcyBhcmUgc3VwcG9ydGVkOiAke3BhdGh9YCk7XG5cdFx0fVxuXG5cdFx0cmV0dXJuIGZpbGU7XG5cdH1cblxuXHRwcml2YXRlIG5vcm1hbGl6ZU1hcmtkb3duUGF0aChwYXRoOiBzdHJpbmcpOiBzdHJpbmcge1xuXHRcdGNvbnN0IG5vcm1hbGl6ZWRQYXRoID0gcGF0aC50cmltKCkucmVwbGFjZSgvXlxcLysvLCBcIlwiKTtcblxuXHRcdGlmICghbm9ybWFsaXplZFBhdGgpIHtcblx0XHRcdHRocm93IG5ldyBFcnJvcihcIlBhdGggY2Fubm90IGJlIGVtcHR5XCIpO1xuXHRcdH1cblxuXHRcdGNvbnN0IHBhcnRzID0gbm9ybWFsaXplZFBhdGguc3BsaXQoXCIvXCIpLmZpbHRlcihCb29sZWFuKTtcblxuXHRcdGlmIChwYXJ0cy5pbmNsdWRlcyhcIi4uXCIpIHx8IHBhcnRzLnNvbWUoKHBhcnQpID0+IHBhcnQgPT09IFwiLlwiKSkge1xuXHRcdFx0dGhyb3cgbmV3IEVycm9yKFwiUGF0aCBjYW5ub3QgY29udGFpbiBwYXJlbnQgb3IgY3VycmVudC1kaXJlY3Rvcnkgc2VnbWVudHNcIik7XG5cdFx0fVxuXG5cdFx0aWYgKCFub3JtYWxpemVkUGF0aC50b0xvY2FsZUxvd2VyQ2FzZSgpLmVuZHNXaXRoKFwiLm1kXCIpKSB7XG5cdFx0XHR0aHJvdyBuZXcgRXJyb3IoXCJPbmx5IG1hcmtkb3duIGZpbGUgcGF0aHMgZW5kaW5nIGluIC5tZCBhcmUgc3VwcG9ydGVkXCIpO1xuXHRcdH1cblxuXHRcdHJldHVybiBwYXJ0cy5qb2luKFwiL1wiKTtcblx0fVxuXG5cdHByaXZhdGUgZ2V0VmF1bHRGb2xkZXIoKTogc3RyaW5nIHtcblx0XHRyZXR1cm4gbm9ybWFsaXplVmF1bHRGb2xkZXIodGhpcy5zZXR0aW5ncy52YXVsdEZvbGRlcik7XG5cdH1cblxuXHRwcml2YXRlIGVuc3VyZVBhdGhJblZhdWx0Rm9sZGVyKHBhdGg6IHN0cmluZyk6IHZvaWQge1xuXHRcdGNvbnN0IGZvbGRlciA9IHRoaXMuZ2V0VmF1bHRGb2xkZXIoKTtcblxuXHRcdGlmIChmb2xkZXIgJiYgIXBhdGguc3RhcnRzV2l0aChgJHtmb2xkZXJ9L2ApKSB7XG5cdFx0XHR0aHJvdyBuZXcgRXJyb3IoYFBhdGggaXMgb3V0c2lkZSB0aGUgY29uZmlndXJlZCB2YXVsdCBhY2Nlc3MgZm9sZGVyOiAke2ZvbGRlcn1gKTtcblx0XHR9XG5cdH1cblxuXHRwcml2YXRlIGFzeW5jIGxpc3RNYXJrZG93blBhdGhzKCk6IFByb21pc2U8c3RyaW5nW10+IHtcblx0XHRjb25zdCBmb2xkZXIgPSB0aGlzLmdldFZhdWx0Rm9sZGVyKCk7XG5cblx0XHRpZiAoZm9sZGVyICYmICEoYXdhaXQgdGhpcy5hcHAudmF1bHQuYWRhcHRlci5leGlzdHMoZm9sZGVyKSkpIHtcblx0XHRcdHJldHVybiBbXTtcblx0XHR9XG5cblx0XHRjb25zdCBwYXRocyA9IGF3YWl0IHRoaXMubGlzdE1hcmtkb3duUGF0aHNJbkZvbGRlcihmb2xkZXIpO1xuXHRcdHJldHVybiBwYXRocy5zb3J0KChhLCBiKSA9PiBhLmxvY2FsZUNvbXBhcmUoYikpO1xuXHR9XG5cblx0cHJpdmF0ZSBhc3luYyBsaXN0TWFya2Rvd25QYXRoc0luRm9sZGVyKGZvbGRlcjogc3RyaW5nKTogUHJvbWlzZTxzdHJpbmdbXT4ge1xuXHRcdGNvbnN0IGxpc3RpbmcgPSBhd2FpdCB0aGlzLmFwcC52YXVsdC5hZGFwdGVyLmxpc3QoZm9sZGVyKTtcblx0XHRjb25zdCBwYXRocyA9IGxpc3RpbmcuZmlsZXMuZmlsdGVyKGlzTWFya2Rvd25QYXRoKTtcblxuXHRcdGZvciAoY29uc3QgY2hpbGRGb2xkZXIgb2YgbGlzdGluZy5mb2xkZXJzKSB7XG5cdFx0XHRwYXRocy5wdXNoKC4uLihhd2FpdCB0aGlzLmxpc3RNYXJrZG93blBhdGhzSW5Gb2xkZXIoY2hpbGRGb2xkZXIpKSk7XG5cdFx0fVxuXG5cdFx0cmV0dXJuIHBhdGhzO1xuXHR9XG5cblx0cHJpdmF0ZSBhc3luYyBlbnN1cmVQYXJlbnRGb2xkZXJzKGZpbGVQYXRoOiBzdHJpbmcpOiBQcm9taXNlPHZvaWQ+IHtcblx0XHRjb25zdCBwYXJlbnRQYXJ0cyA9IGZpbGVQYXRoLnNwbGl0KFwiL1wiKS5zbGljZSgwLCAtMSk7XG5cdFx0bGV0IGN1cnJlbnRQYXRoID0gXCJcIjtcblxuXHRcdGZvciAoY29uc3QgcGFydCBvZiBwYXJlbnRQYXJ0cykge1xuXHRcdFx0Y3VycmVudFBhdGggPSBjdXJyZW50UGF0aCA/IGAke2N1cnJlbnRQYXRofS8ke3BhcnR9YCA6IHBhcnQ7XG5cblx0XHRcdGlmICghKGF3YWl0IHRoaXMuYXBwLnZhdWx0LmFkYXB0ZXIuZXhpc3RzKGN1cnJlbnRQYXRoKSkpIHtcblx0XHRcdFx0YXdhaXQgdGhpcy5hcHAudmF1bHQuYWRhcHRlci5ta2RpcihjdXJyZW50UGF0aCk7XG5cdFx0XHR9XG5cdFx0fVxuXHR9XG5cblx0cHJpdmF0ZSBidWlsZEdhdGV3YXlVcmwoKTogVVJMIHwgbnVsbCB7XG5cdFx0bGV0IHVybDogVVJMO1xuXG5cdFx0dHJ5IHtcblx0XHRcdHVybCA9IG5ldyBVUkwodGhpcy5zZXR0aW5ncy5nYXRld2F5VXJsIHx8IERFRkFVTFRfR0FURVdBWV9VUkwpO1xuXHRcdH0gY2F0Y2gge1xuXHRcdFx0bmV3IE5vdGljZShcIkludmFsaWQgUG9rZSBHYXRld2F5IFVSTFwiKTtcblx0XHRcdHJldHVybiBudWxsO1xuXHRcdH1cblxuXHRcdGlmICh1cmwucHJvdG9jb2wgIT09IFwid3M6XCIgJiYgdXJsLnByb3RvY29sICE9PSBcIndzczpcIikge1xuXHRcdFx0bmV3IE5vdGljZShcIlBva2UgR2F0ZXdheSBVUkwgbXVzdCBzdGFydCB3aXRoIHdzOi8vIG9yIHdzczovL1wiKTtcblx0XHRcdHJldHVybiBudWxsO1xuXHRcdH1cblxuXHRcdHVybC5zZWFyY2hQYXJhbXMuc2V0KFwidG9rZW5cIiwgdGhpcy5zZXR0aW5ncy5jb25uZWN0aW9uVG9rZW4pO1xuXHRcdHVybC5zZWFyY2hQYXJhbXMuc2V0KFwicGx1Z2luXCIsIHRoaXMubWFuaWZlc3QuaWQpO1xuXHRcdHVybC5zZWFyY2hQYXJhbXMuc2V0KFwidmVyc2lvblwiLCB0aGlzLm1hbmlmZXN0LnZlcnNpb24pO1xuXG5cdFx0cmV0dXJuIHVybDtcblx0fVxuXG5cdHByaXZhdGUgc2VuZFJlc3BvbnNlKHJlc3BvbnNlOiBSZXNwb25zZU1lc3NhZ2UpOiB2b2lkIHtcblx0XHRpZiAoIXRoaXMuc29ja2V0IHx8IHRoaXMuc29ja2V0LnJlYWR5U3RhdGUgIT09IFdlYlNvY2tldC5PUEVOKSB7XG5cdFx0XHRyZXR1cm47XG5cdFx0fVxuXG5cdFx0dGhpcy5zb2NrZXQuc2VuZChKU09OLnN0cmluZ2lmeShyZXNwb25zZSkpO1xuXHR9XG5cblx0cHJpdmF0ZSBzZXRDb25uZWN0aW9uU3RhdGUoc3RhdGU6IENvbm5lY3Rpb25TdGF0ZSk6IHZvaWQge1xuXHRcdHRoaXMuY29ubmVjdGlvblN0YXRlID0gc3RhdGU7XG5cblx0XHRpZiAodGhpcy5zdGF0dXNCYXJJdGVtRWwpIHtcblx0XHRcdHRoaXMuc3RhdHVzQmFySXRlbUVsLnNldFRleHQoYFBva2U6ICR7Y2FwaXRhbGl6ZShzdGF0ZSl9YCk7XG5cdFx0XHR0aGlzLnN0YXR1c0Jhckl0ZW1FbC5yZW1vdmVDbGFzcyhcImlzLWNvbm5lY3RlZFwiLCBcImlzLWNvbm5lY3RpbmdcIiwgXCJpcy1kaXNjb25uZWN0ZWRcIik7XG5cdFx0XHR0aGlzLnN0YXR1c0Jhckl0ZW1FbC5hZGRDbGFzcyhgaXMtJHtzdGF0ZX1gKTtcblx0XHR9XG5cblx0XHR0aGlzLnNldHRpbmdzVGFiPy51cGRhdGVTdGF0dXMoc3RhdGUpO1xuXHR9XG5cblx0cHJpdmF0ZSBoYW5kbGVDb25uZWN0aW9uRXJyb3IoZXJyb3I6IHVua25vd24pOiB2b2lkIHtcblx0XHRjb25zb2xlLmVycm9yKFwiUG9rZSBHYXRld2F5IGNvbm5lY3Rpb24gZXJyb3JcIiwgZXJyb3IpO1xuXHR9XG59XG5cbmNsYXNzIFBva2VPYnNpZGlhblNldHRpbmdUYWIgZXh0ZW5kcyBQbHVnaW5TZXR0aW5nVGFiIHtcblx0cHJpdmF0ZSBwbHVnaW46IFBva2VPYnNpZGlhblBsdWdpbjtcblx0cHJpdmF0ZSBzdGF0dXNFbDogSFRNTEVsZW1lbnQgfCBudWxsID0gbnVsbDtcblxuXHRjb25zdHJ1Y3RvcihhcHA6IEFwcCwgcGx1Z2luOiBQb2tlT2JzaWRpYW5QbHVnaW4pIHtcblx0XHRzdXBlcihhcHAsIHBsdWdpbik7XG5cdFx0dGhpcy5wbHVnaW4gPSBwbHVnaW47XG5cdH1cblxuXHRkaXNwbGF5KCk6IHZvaWQge1xuXHRcdGNvbnN0IHsgY29udGFpbmVyRWwgfSA9IHRoaXM7XG5cdFx0Y29udGFpbmVyRWwuZW1wdHkoKTtcblxuXHRcdGNvbnRhaW5lckVsLmNyZWF0ZUVsKFwiaDJcIiwgeyB0ZXh0OiBcIlBva2UgR2F0ZXdheVwiIH0pO1xuXG5cdFx0dGhpcy5yZW5kZXJHYXRld2F5VXJsU2V0dGluZyhcblx0XHRcdG5ldyBTZXR0aW5nKGNvbnRhaW5lckVsKVxuXHRcdFx0XHQuc2V0TmFtZShcIkdhdGV3YXkgVVJMXCIpXG5cdFx0XHRcdC5zZXREZXNjKFwiV2ViU29ja2V0IGVuZHBvaW50IHVzZWQgdG8gY29ubmVjdCB0aGlzIHZhdWx0IHRvIFBva2UuXCIpLFxuXHRcdCk7XG5cdFx0dGhpcy5yZW5kZXJWYXVsdEZvbGRlclNldHRpbmcoXG5cdFx0XHRuZXcgU2V0dGluZyhjb250YWluZXJFbClcblx0XHRcdFx0LnNldE5hbWUoXCJWYXVsdCBhY2Nlc3MgZm9sZGVyXCIpXG5cdFx0XHRcdC5zZXREZXNjKFwiTGltaXQgUG9rZSB0byBtYXJrZG93biBmaWxlcyBpbiB0aGlzIGZvbGRlci4gTGVhdmUgYmxhbmsgdG8gYWxsb3cgYWxsIG1hcmtkb3duIGZpbGVzLlwiKSxcblx0XHQpO1xuXHRcdHRoaXMucmVuZGVyQ29ubmVjdGlvblRva2VuU2V0dGluZyhcblx0XHRcdG5ldyBTZXR0aW5nKGNvbnRhaW5lckVsKVxuXHRcdFx0XHQuc2V0TmFtZShcIkNvbm5lY3Rpb24gdG9rZW5cIilcblx0XHRcdFx0LnNldERlc2MoXCJQYXN0ZSB0aGlzIHRva2VuIGludG8gUG9rZSdzIEFkZCBLZXkgZmllbGQgZm9yIHRoZSBPYnNpZGlhbiByZWNpcGUuXCIpLFxuXHRcdCk7XG5cdFx0dGhpcy5yZW5kZXJBbGxvd1dyaXRlU2V0dGluZyhcblx0XHRcdG5ldyBTZXR0aW5nKGNvbnRhaW5lckVsKVxuXHRcdFx0XHQuc2V0TmFtZShcIkFsbG93IHdyaXRlc1wiKVxuXHRcdFx0XHQuc2V0RGVzYyhcIkFsbG93IFBva2UgdG8gY3JlYXRlIG9yIG92ZXJ3cml0ZSBtYXJrZG93biBmaWxlcyBpbiB0aGlzIHZhdWx0LlwiKSxcblx0XHQpO1xuXHRcdHRoaXMucmVuZGVyQ29ubmVjdGlvblN0YXR1c1NldHRpbmcoXG5cdFx0XHRuZXcgU2V0dGluZyhjb250YWluZXJFbClcblx0XHRcdFx0LnNldE5hbWUoXCJDb25uZWN0aW9uIHN0YXR1c1wiKVxuXHRcdFx0XHQuc2V0RGVzYyhcIkN1cnJlbnQgZ2F0ZXdheSBjb25uZWN0aW9uIHN0YXRlLlwiKSxcblx0XHQpO1xuXHR9XG5cblx0cHJpdmF0ZSByZW5kZXJHYXRld2F5VXJsU2V0dGluZyhzZXR0aW5nOiBTZXR0aW5nKTogdm9pZCB7XG5cdFx0c2V0dGluZy5hZGRUZXh0KCh0ZXh0KSA9PiB7XG5cdFx0XHR0ZXh0XG5cdFx0XHRcdC5zZXRQbGFjZWhvbGRlcihERUZBVUxUX0dBVEVXQVlfVVJMKVxuXHRcdFx0XHQuc2V0VmFsdWUodGhpcy5wbHVnaW4uc2V0dGluZ3MuZ2F0ZXdheVVybCB8fCBERUZBVUxUX0dBVEVXQVlfVVJMKVxuXHRcdFx0XHQub25DaGFuZ2UoYXN5bmMgKHZhbHVlKSA9PiB7XG5cdFx0XHRcdFx0YXdhaXQgdGhpcy5wbHVnaW4udXBkYXRlR2F0ZXdheVVybCh2YWx1ZSk7XG5cdFx0XHRcdH0pO1xuXHRcdH0pO1xuXHR9XG5cblx0cHJpdmF0ZSByZW5kZXJWYXVsdEZvbGRlclNldHRpbmcoc2V0dGluZzogU2V0dGluZyk6IHZvaWQge1xuXHRcdHNldHRpbmcuYWRkVGV4dCgodGV4dCkgPT4ge1xuXHRcdFx0dGV4dFxuXHRcdFx0XHQuc2V0UGxhY2Vob2xkZXIoREVGQVVMVF9WQVVMVF9GT0xERVIpXG5cdFx0XHRcdC5zZXRWYWx1ZSh0aGlzLnBsdWdpbi5zZXR0aW5ncy52YXVsdEZvbGRlcilcblx0XHRcdFx0Lm9uQ2hhbmdlKGFzeW5jICh2YWx1ZSkgPT4ge1xuXHRcdFx0XHRcdGF3YWl0IHRoaXMucGx1Z2luLnVwZGF0ZVZhdWx0Rm9sZGVyKHZhbHVlKTtcblx0XHRcdFx0fSk7XG5cdFx0fSk7XG5cdH1cblxuXHRwcml2YXRlIHJlbmRlckNvbm5lY3Rpb25Ub2tlblNldHRpbmcoc2V0dGluZzogU2V0dGluZyk6IHZvaWQge1xuXHRcdGxldCB0b2tlbklucHV0OiBIVE1MSW5wdXRFbGVtZW50IHwgbnVsbCA9IG51bGw7XG5cblx0XHRzZXR0aW5nXG5cdFx0XHQuYWRkVGV4dCgodGV4dCkgPT4ge1xuXHRcdFx0XHR0ZXh0XG5cdFx0XHRcdFx0LnNldFBsYWNlaG9sZGVyKFwiUGFzdGUgdG9rZW5cIilcblx0XHRcdFx0XHQuc2V0VmFsdWUodGhpcy5wbHVnaW4uc2V0dGluZ3MuY29ubmVjdGlvblRva2VuKVxuXHRcdFx0XHRcdC5vbkNoYW5nZShhc3luYyAodmFsdWUpID0+IHtcblx0XHRcdFx0XHRcdGF3YWl0IHRoaXMucGx1Z2luLnVwZGF0ZUNvbm5lY3Rpb25Ub2tlbih2YWx1ZSk7XG5cdFx0XHRcdFx0fSk7XG5cblx0XHRcdFx0dGV4dC5pbnB1dEVsLnR5cGUgPSBcInBhc3N3b3JkXCI7XG5cdFx0XHRcdHRva2VuSW5wdXQgPSB0ZXh0LmlucHV0RWw7XG5cdFx0XHR9KVxuXHRcdFx0LmFkZEV4dHJhQnV0dG9uKChidXR0b24pID0+IHtcblx0XHRcdFx0YnV0dG9uXG5cdFx0XHRcdFx0LnNldEljb24oXCJ0ZXh0LWN1cnNvci1pbnB1dFwiKVxuXHRcdFx0XHRcdC5zZXRUb29sdGlwKFwiUmV2ZWFsIGFuZCBzZWxlY3QgdG9rZW5cIilcblx0XHRcdFx0XHQub25DbGljaygoKSA9PiB7XG5cdFx0XHRcdFx0XHRpZiAoIXRva2VuSW5wdXQpIHtcblx0XHRcdFx0XHRcdFx0cmV0dXJuO1xuXHRcdFx0XHRcdFx0fVxuXG5cdFx0XHRcdFx0XHR0b2tlbklucHV0LnR5cGUgPSBcInRleHRcIjtcblx0XHRcdFx0XHRcdHRva2VuSW5wdXQuZm9jdXMoKTtcblx0XHRcdFx0XHRcdHRva2VuSW5wdXQuc2VsZWN0KCk7XG5cdFx0XHRcdFx0XHRuZXcgTm90aWNlKFwiUG9rZSBHYXRld2F5IHRva2VuIHNlbGVjdGVkXCIpO1xuXHRcdFx0XHRcdH0pO1xuXHRcdFx0fSlcblx0XHRcdC5hZGRFeHRyYUJ1dHRvbigoYnV0dG9uKSA9PiB7XG5cdFx0XHRcdGJ1dHRvblxuXHRcdFx0XHRcdC5zZXRJY29uKFwicmVmcmVzaC1jd1wiKVxuXHRcdFx0XHRcdC5zZXRUb29sdGlwKFwiR2VuZXJhdGUgbmV3IHRva2VuXCIpXG5cdFx0XHRcdFx0Lm9uQ2xpY2soYXN5bmMgKCkgPT4ge1xuXHRcdFx0XHRcdFx0YXdhaXQgdGhpcy5wbHVnaW4udXBkYXRlQ29ubmVjdGlvblRva2VuKGdlbmVyYXRlQ29ubmVjdGlvblRva2VuKCkpO1xuXHRcdFx0XHRcdFx0dGhpcy5kaXNwbGF5KCk7XG5cdFx0XHRcdFx0XHRuZXcgTm90aWNlKFwiR2VuZXJhdGVkIGEgbmV3IFBva2UgR2F0ZXdheSB0b2tlblwiKTtcblx0XHRcdFx0XHR9KTtcblx0XHRcdH0pO1xuXHR9XG5cblx0cHJpdmF0ZSByZW5kZXJBbGxvd1dyaXRlU2V0dGluZyhzZXR0aW5nOiBTZXR0aW5nKTogdm9pZCB7XG5cdFx0c2V0dGluZy5hZGRUb2dnbGUoKHRvZ2dsZSkgPT4ge1xuXHRcdFx0dG9nZ2xlXG5cdFx0XHRcdC5zZXRWYWx1ZSh0aGlzLnBsdWdpbi5zZXR0aW5ncy5hbGxvd1dyaXRlKVxuXHRcdFx0XHQub25DaGFuZ2UoYXN5bmMgKHZhbHVlKSA9PiB7XG5cdFx0XHRcdFx0YXdhaXQgdGhpcy5wbHVnaW4udXBkYXRlQWxsb3dXcml0ZSh2YWx1ZSk7XG5cdFx0XHRcdH0pO1xuXHRcdH0pO1xuXHR9XG5cblx0cHJpdmF0ZSByZW5kZXJDb25uZWN0aW9uU3RhdHVzU2V0dGluZyhzZXR0aW5nOiBTZXR0aW5nKTogdm9pZCB7XG5cdFx0c2V0dGluZy5hZGRFeHRyYUJ1dHRvbigoYnV0dG9uKSA9PiB7XG5cdFx0XHRidXR0b25cblx0XHRcdFx0LnNldEljb24oXCJyZWZyZXNoLWN3XCIpXG5cdFx0XHRcdC5zZXRUb29sdGlwKFwiUmVjb25uZWN0XCIpXG5cdFx0XHRcdC5vbkNsaWNrKCgpID0+IHtcblx0XHRcdFx0XHR0aGlzLnBsdWdpbi5yZWNvbm5lY3ROb3coKTtcblx0XHRcdFx0XHRuZXcgTm90aWNlKFwiUmVjb25uZWN0aW5nIFBva2UgR2F0ZXdheVwiKTtcblx0XHRcdFx0fSk7XG5cdFx0fSk7XG5cblx0XHR0aGlzLnN0YXR1c0VsID0gc2V0dGluZy5jb250cm9sRWwuY3JlYXRlU3BhbigpO1xuXHRcdHRoaXMuc3RhdHVzRWwuYWRkQ2xhc3MoXCJwb2tlLXN0YXR1c1wiKTtcblx0XHR0aGlzLnVwZGF0ZVN0YXR1cyh0aGlzLnBsdWdpbi5nZXRDb25uZWN0aW9uU3RhdGUoKSk7XG5cdH1cblxuXHR1cGRhdGVTdGF0dXMoc3RhdGU6IENvbm5lY3Rpb25TdGF0ZSk6IHZvaWQge1xuXHRcdGlmICghdGhpcy5zdGF0dXNFbCkge1xuXHRcdFx0cmV0dXJuO1xuXHRcdH1cblxuXHRcdHRoaXMuc3RhdHVzRWwuc2V0VGV4dChjYXBpdGFsaXplKHN0YXRlKSk7XG5cdFx0dGhpcy5zdGF0dXNFbC5yZW1vdmVDbGFzcyhcImlzLWNvbm5lY3RlZFwiLCBcImlzLWNvbm5lY3RpbmdcIiwgXCJpcy1kaXNjb25uZWN0ZWRcIik7XG5cdFx0dGhpcy5zdGF0dXNFbC5hZGRDbGFzcyhgaXMtJHtzdGF0ZX1gKTtcblx0fVxufVxuXG5mdW5jdGlvbiBnZXRSZXF1aXJlZFN0cmluZyhwYXJhbXM6IFJlY29yZDxzdHJpbmcsIHVua25vd24+LCBrZXk6IHN0cmluZyk6IHN0cmluZyB7XG5cdGNvbnN0IHZhbHVlID0gcGFyYW1zW2tleV07XG5cblx0aWYgKHR5cGVvZiB2YWx1ZSAhPT0gXCJzdHJpbmdcIikge1xuXHRcdHRocm93IG5ldyBFcnJvcihgTWlzc2luZyByZXF1aXJlZCBzdHJpbmcgcGFyYW06ICR7a2V5fWApO1xuXHR9XG5cblx0cmV0dXJuIHZhbHVlO1xufVxuXG5mdW5jdGlvbiBnZXRPcHRpb25hbFN0cmluZyhwYXJhbXM6IFJlY29yZDxzdHJpbmcsIHVua25vd24+LCBrZXk6IHN0cmluZywgZmFsbGJhY2s6IHN0cmluZyk6IHN0cmluZyB7XG5cdGNvbnN0IHZhbHVlID0gcGFyYW1zW2tleV07XG5cdHJldHVybiB0eXBlb2YgdmFsdWUgPT09IFwic3RyaW5nXCIgPyB2YWx1ZSA6IGZhbGxiYWNrO1xufVxuXG5mdW5jdGlvbiBpc1JlY29yZCh2YWx1ZTogdW5rbm93bik6IHZhbHVlIGlzIFJlY29yZDxzdHJpbmcsIHVua25vd24+IHtcblx0cmV0dXJuIHR5cGVvZiB2YWx1ZSA9PT0gXCJvYmplY3RcIiAmJiB2YWx1ZSAhPT0gbnVsbCAmJiAhQXJyYXkuaXNBcnJheSh2YWx1ZSk7XG59XG5cbmZ1bmN0aW9uIGlzTWFya2Rvd25QYXRoKHBhdGg6IHN0cmluZyk6IGJvb2xlYW4ge1xuXHRyZXR1cm4gcGF0aC50b0xvY2FsZUxvd2VyQ2FzZSgpLmVuZHNXaXRoKFwiLm1kXCIpO1xufVxuXG5mdW5jdGlvbiBub3JtYWxpemVWYXVsdEZvbGRlcihwYXRoOiBzdHJpbmcpOiBzdHJpbmcge1xuXHRjb25zdCBub3JtYWxpemVkUGF0aCA9IHBhdGgudHJpbSgpLnJlcGxhY2UoL15cXC8rfFxcLyskL2csIFwiXCIpO1xuXG5cdGlmICghbm9ybWFsaXplZFBhdGgpIHtcblx0XHRyZXR1cm4gXCJcIjtcblx0fVxuXG5cdGNvbnN0IHBhcnRzID0gbm9ybWFsaXplZFBhdGguc3BsaXQoXCIvXCIpLmZpbHRlcihCb29sZWFuKTtcblxuXHRpZiAocGFydHMuaW5jbHVkZXMoXCIuLlwiKSB8fCBwYXJ0cy5zb21lKChwYXJ0KSA9PiBwYXJ0ID09PSBcIi5cIikpIHtcblx0XHR0aHJvdyBuZXcgRXJyb3IoXCJWYXVsdCBhY2Nlc3MgZm9sZGVyIGNhbm5vdCBjb250YWluIHBhcmVudCBvciBjdXJyZW50LWRpcmVjdG9yeSBzZWdtZW50c1wiKTtcblx0fVxuXG5cdHJldHVybiBwYXJ0cy5qb2luKFwiL1wiKTtcbn1cblxuZnVuY3Rpb24gbWFrZVNuaXBwZXQoY29udGVudDogc3RyaW5nLCBpbmRleDogbnVtYmVyLCBtYXRjaExlbmd0aDogbnVtYmVyKTogc3RyaW5nIHtcblx0Y29uc3QgaGFsZldpbmRvdyA9IE1hdGguZmxvb3IoKE1BWF9TTklQUEVUX0xFTkdUSCAtIG1hdGNoTGVuZ3RoKSAvIDIpO1xuXHRjb25zdCBzdGFydCA9IE1hdGgubWF4KDAsIGluZGV4IC0gaGFsZldpbmRvdyk7XG5cdGNvbnN0IGVuZCA9IE1hdGgubWluKGNvbnRlbnQubGVuZ3RoLCBpbmRleCArIG1hdGNoTGVuZ3RoICsgaGFsZldpbmRvdyk7XG5cdGNvbnN0IHByZWZpeCA9IHN0YXJ0ID4gMCA/IFwiLi4uXCIgOiBcIlwiO1xuXHRjb25zdCBzdWZmaXggPSBlbmQgPCBjb250ZW50Lmxlbmd0aCA/IFwiLi4uXCIgOiBcIlwiO1xuXG5cdHJldHVybiBgJHtwcmVmaXh9JHtjb250ZW50LnNsaWNlKHN0YXJ0LCBlbmQpLnJlcGxhY2UoL1xccysvZywgXCIgXCIpLnRyaW0oKX0ke3N1ZmZpeH1gO1xufVxuXG5mdW5jdGlvbiBjYXBpdGFsaXplKHZhbHVlOiBzdHJpbmcpOiBzdHJpbmcge1xuXHRyZXR1cm4gdmFsdWUuY2hhckF0KDApLnRvTG9jYWxlVXBwZXJDYXNlKCkgKyB2YWx1ZS5zbGljZSgxKTtcbn1cblxuZnVuY3Rpb24gZ2VuZXJhdGVDb25uZWN0aW9uVG9rZW4oKTogc3RyaW5nIHtcblx0Y29uc3QgYnl0ZXMgPSBuZXcgVWludDhBcnJheSgzMik7XG5cdGNyeXB0by5nZXRSYW5kb21WYWx1ZXMoYnl0ZXMpO1xuXHRyZXR1cm4gYCR7R0VORVJBVEVEX1RPS0VOX1BSRUZJWH0ke3RvQmFzZTY0VXJsKGJ5dGVzKX1gO1xufVxuXG5mdW5jdGlvbiB0b0Jhc2U2NFVybChieXRlczogVWludDhBcnJheSk6IHN0cmluZyB7XG5cdGxldCBiaW5hcnkgPSBcIlwiO1xuXG5cdGZvciAoY29uc3QgYnl0ZSBvZiBieXRlcykge1xuXHRcdGJpbmFyeSArPSBTdHJpbmcuZnJvbUNoYXJDb2RlKGJ5dGUpO1xuXHR9XG5cblx0cmV0dXJuIGJ0b2EoYmluYXJ5KS5yZXBsYWNlKC9cXCsvZywgXCItXCIpLnJlcGxhY2UoL1xcLy9nLCBcIl9cIikucmVwbGFjZSgvPSskL2csIFwiXCIpO1xufVxuXG5mdW5jdGlvbiByZWRhY3RUb2tlbih1cmw6IFVSTCk6IHN0cmluZyB7XG5cdGNvbnN0IGNvcHkgPSBuZXcgVVJMKHVybC50b1N0cmluZygpKTtcblxuXHRpZiAoY29weS5zZWFyY2hQYXJhbXMuaGFzKFwidG9rZW5cIikpIHtcblx0XHRjb3B5LnNlYXJjaFBhcmFtcy5zZXQoXCJ0b2tlblwiLCBcIioqKlwiKTtcblx0fVxuXG5cdHJldHVybiBjb3B5LnRvU3RyaW5nKCk7XG59XG4iXX0=