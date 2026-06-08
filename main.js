"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
const obsidian_1 = require("obsidian");
const DEFAULT_GATEWAY_URL = "wss://obsidian.matt-nz.com/obsidian/sync";
const BASE_RECONNECT_DELAY_MS = 1000;
const MAX_RECONNECT_DELAY_MS = 30000;
const REQUEST_TIMEOUT_MS = 30000;
const MAX_SNIPPET_LENGTH = 180;
const GENERATED_TOKEN_PREFIX = "pkobs_vault_";
const DEFAULT_SETTINGS = {
    gatewayUrl: DEFAULT_GATEWAY_URL,
    connectionToken: "",
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
        this.settings = Object.assign({}, DEFAULT_SETTINGS, await this.loadData());
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
    listFiles() {
        return {
            files: this.app.vault.getMarkdownFiles().map((file) => file.path),
        };
    }
    async readFile(params) {
        const path = getRequiredString(params, "path");
        const file = this.getMarkdownFile(path);
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
    getMarkdownFile(path) {
        const normalizedPath = this.normalizeMarkdownPath(path);
        const file = this.app.vault.getAbstractFileByPath(normalizedPath);
        if (!(file instanceof obsidian_1.TFile)) {
            throw new Error(`File not found: ${normalizedPath}`);
        }
        if (file.extension !== "md") {
            throw new Error(`Only markdown files are supported: ${normalizedPath}`);
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
        new obsidian_1.Setting(containerEl)
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
        new obsidian_1.Setting(containerEl)
            .setName("Connection token")
            .setDesc("Paste this token into Poke's Add Key field for the Obsidian recipe.")
            .addText((text) => {
            text
                .setPlaceholder("Paste token")
                .setValue(this.plugin.settings.connectionToken)
                .onChange(async (value) => {
                await this.plugin.updateConnectionToken(value);
            });
            text.inputEl.type = "password";
        })
            .addExtraButton((button) => {
            button
                .setIcon("copy")
                .setTooltip("Copy token")
                .onClick(async () => {
                await navigator.clipboard.writeText(this.plugin.settings.connectionToken);
                new obsidian_1.Notice("Poke Gateway token copied");
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
        new obsidian_1.Setting(containerEl)
            .setName("Allow writes")
            .setDesc("Allow Poke to create or overwrite markdown files in this vault.")
            .addToggle((toggle) => {
            toggle
                .setValue(this.plugin.settings.allowWrite)
                .onChange(async (value) => {
                await this.plugin.updateAllowWrite(value);
            });
        });
        const statusSetting = new obsidian_1.Setting(containerEl)
            .setName("Connection status")
            .setDesc("Current gateway connection state.")
            .addExtraButton((button) => {
            button
                .setIcon("refresh-cw")
                .setTooltip("Reconnect")
                .onClick(() => {
                this.plugin.reconnectNow();
                new obsidian_1.Notice("Reconnecting Poke Gateway");
            });
        });
        this.statusEl = statusSetting.controlEl.createSpan();
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
function isRecord(value) {
    return typeof value === "object" && value !== null && !Array.isArray(value);
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
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoibWFpbi5qcyIsInNvdXJjZVJvb3QiOiIiLCJzb3VyY2VzIjpbIm1haW4udHMiXSwibmFtZXMiOltdLCJtYXBwaW5ncyI6Ijs7QUFBQSx1Q0FBaUY7QUFFakYsTUFBTSxtQkFBbUIsR0FBRywwQ0FBMEMsQ0FBQztBQUN2RSxNQUFNLHVCQUF1QixHQUFHLElBQUssQ0FBQztBQUN0QyxNQUFNLHNCQUFzQixHQUFHLEtBQU0sQ0FBQztBQUN0QyxNQUFNLGtCQUFrQixHQUFHLEtBQU0sQ0FBQztBQUNsQyxNQUFNLGtCQUFrQixHQUFHLEdBQUcsQ0FBQztBQUMvQixNQUFNLHNCQUFzQixHQUFHLGNBQWMsQ0FBQztBQTJCOUMsTUFBTSxnQkFBZ0IsR0FBeUI7SUFDOUMsVUFBVSxFQUFFLG1CQUFtQjtJQUMvQixlQUFlLEVBQUUsRUFBRTtJQUNuQixVQUFVLEVBQUUsS0FBSztDQUNqQixDQUFDO0FBRUYsTUFBcUIsa0JBQW1CLFNBQVEsaUJBQU07SUFBdEQ7O1FBQ0MsYUFBUSxHQUF5QixnQkFBZ0IsQ0FBQztRQUMxQyxXQUFNLEdBQXFCLElBQUksQ0FBQztRQUNoQyxvQkFBZSxHQUF1QixJQUFJLENBQUM7UUFDM0MsZ0JBQVcsR0FBa0MsSUFBSSxDQUFDO1FBQ2xELG9CQUFlLEdBQW9CLGNBQWMsQ0FBQztRQUNsRCxzQkFBaUIsR0FBRyxDQUFDLENBQUM7UUFDdEIsbUJBQWMsR0FBa0IsSUFBSSxDQUFDO1FBQ3JDLG9CQUFlLEdBQUcsS0FBSyxDQUFDO0lBaVpqQyxDQUFDO0lBL1lBLEtBQUssQ0FBQyxNQUFNO1FBQ1gsTUFBTSxJQUFJLENBQUMsWUFBWSxFQUFFLENBQUM7UUFFMUIsSUFBSSxDQUFDLElBQUksQ0FBQyxRQUFRLENBQUMsZUFBZSxFQUFFLENBQUM7WUFDcEMsSUFBSSxDQUFDLFFBQVEsQ0FBQyxlQUFlLEdBQUcsdUJBQXVCLEVBQUUsQ0FBQztZQUMxRCxNQUFNLElBQUksQ0FBQyxZQUFZLEVBQUUsQ0FBQztRQUMzQixDQUFDO1FBRUQsSUFBSSxDQUFDLGVB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LHVCQUF1QjtJQUMvQixNQUFNLEtBQUssR0FBRyxJQUFJLFVBQVUsQ0FBQyxFQUFFLENBQUMsQ0FBQztJQUNqQyxNQUFNLENBQUMsZUFBZSxDQUFDLEtBQUssQ0FBQyxDQUFDO0lBQzlCLE9BQU8sR0FBRyxzQkFBc0IsR0FBRyxXQUFXLENBQUMsS0FBSyxDQUFDLEVBQUUsQ0FBQztBQUN6RCxDQUFDO0FBRUQsU0FBUyxXQUFXLENBQUMsS0FBaUI7SUFDckMsSUFBSSxNQUFNLEdBQUcsRUFBRSxDQUFDO0lBRWhCLEtBQUssTUFBTSxJQUFJLElBQUksS0FBSyxFQUFFLENBQUM7UUFDMUIsTUFBTSxJQUFJLE1BQU0sQ0FBQyxZQUFZLENBQUMsSUFBSSxDQUFDLENBQUM7SUFDckMsQ0FBQztJQUVELE9BQU8sSUFBSSxDQUFDLE1BQU0sQ0FBQyxDQUFDLE9BQU8sQ0FBQyxLQUFLLEVBQUUsR0FBRyxDQUFDLENBQUMsT0FBTyxDQUFDLEtBQUssRUFBRSxHQUFHLENBQUMsQ0FBQyxPQUFPLENBQUMsTUFBTSxFQUFFLEVBQUUsQ0FBQyxDQUFDO0FBQ2pGLENBQUM7QUFFRCxTQUFTLFdBQVcsQ0FBQyxHQUFRO0lBQzVCLE1BQU0sSUFBSSxHQUFHLElBQUksR0FBRyxDQUFDLEdBQUcsQ0FBQyxRQUFRLEVBQUUsQ0FBQyxDQUFDO0lBRXJDLElBQUksSUFBSSxDQUFDLFlBQVksQ0FBQyxHQUFHLENBQUMsT0FBTyxDQUFDLEVBQUUsQ0FBQztRQUNwQyxJQUFJLENBQUMsWUFBWSxDQUFDLEdBQUcsQ0FBQyxPQUFPLEVBQUUsS0FBSyxDQUFDLENBQUM7SUFDdkMsQ0FBQztJQUVELE9BQU8sSUFBSSxDQUFDLFFBQVEsRUFBRSxDQUFDO0FBQ3hCLENBQUMiLCJzb3VyY2VzQ29udGVudCI6WyJpbXBvcnQgeyBBcHAsIE5vdGljZSwgUGx1Z2luLCBQbHVnaW5TZXR0aW5nVGFiLCBTZXR0aW5nLCBURmlsZSB9IGZyb20gXCJvYnNpZGlhblwiO1xuXG5jb25zdCBERUZBVUxUX0dBVEVXQVlfVVJMID0gXCJ3c3M6Ly9vYnNpZGlhbi5tYXR0LW56LmNvbS9vYnNpZGlhbi9zeW5jXCI7XG5jb25zdCBCQVNFX1JFQ09OTkVDVF9ERUxBWV9NUyA9IDFfMDAwO1xuY29uc3QgTUFYX1JFQ09OTkVDVF9ERUxBWV9NUyA9IDMwXzAwMDtcbmNvbnN0IFJFUVVFU1RfVElNRU9VVF9NUyA9IDMwXzAwMDtcbmNvbnN0IE1BWF9TTklQUEVUX0xFTkdUSCA9IDE4MDtcbmNvbnN0IEdFTkVSQVRFRF9UT0tFTl9QUkVGSVggPSBcInBrb2JzX3ZhdWx0X1wiO1xuXG50eXBlIENvbm5lY3Rpb25TdGF0ZSA9IFwiY29ubmVjdGVkXCIgfCBcImNvbm5lY3RpbmdcIiB8IFwiZGlzY29ubmVjdGVkXCI7XG5cbmludGVyZmFjZSBQb2tlT2JzaWRpYW5TZXR0aW5ncyB7XG5cdGdhdGV3YXlVcmw6IHN0cmluZztcblx0Y29ubmVjdGlvblRva2VuOiBzdHJpbmc7XG5cdGFsbG93V3JpdGU6IGJvb2xlYW47XG59XG5cbmludGVyZmFjZSBJbmNvbWluZ01lc3NhZ2Uge1xuXHRpZD86IHVua25vd247XG5cdGFjdGlvbj86IHVua25vd247XG5cdHBhcmFtcz86IHVua25vd247XG59XG5cbmludGVyZmFjZSBSZXNwb25zZU1lc3NhZ2Uge1xuXHRpZDogc3RyaW5nIHwgbnVsbDtcblx0c3RhdHVzOiBcInN1Y2Nlc3NcIiB8IFwiZXJyb3JcIjtcblx0cGF5bG9hZDogUmVjb3JkPHN0cmluZywgdW5rbm93bj47XG59XG5cbmludGVyZmFjZSBTZWFyY2hNYXRjaCB7XG5cdHBhdGg6IHN0cmluZztcblx0c25pcHBldDogc3RyaW5nO1xufVxuXG5jb25zdCBERUZBVUxUX1NFVFRJTkdTOiBQb2tlT2JzaWRpYW5TZXR0aW5ncyA9IHtcblx0Z2F0ZXdheVVybDogREVGQVVMVF9HQVRFV0FZX1VSTCxcblx0Y29ubmVjdGlvblRva2VuOiBcIlwiLFxuXHRhbGxvd1dyaXRlOiBmYWxzZSxcbn07XG5cbmV4cG9ydCBkZWZhdWx0IGNsYXNzIFBva2VPYnNpZGlhblBsdWdpbiBleHRlbmRzIFBsdWdpbiB7XG5cdHNldHRpbmdzOiBQb2tlT2JzaWRpYW5TZXR0aW5ncyA9IERFRkFVTFRfU0VUVElOR1M7XG5cdHByaXZhdGUgc29ja2V0OiBXZWJTb2NrZXQgfCBudWxsID0gbnVsbDtcblx0cHJpdmF0ZSBzdGF0dXNCYXJJdGVtRWw6IEhUTUxFbGVtZW50IHwgbnVsbCA9IG51bGw7XG5cdHByaXZhdGUgc2V0dGluZ3NUYWI6IFBva2VPYnNpZGlhblNldHRpbmdUYWIgfCBudWxsID0gbnVsbDtcblx0cHJpdmF0ZSBjb25uZWN0aW9uU3RhdGU6IENvbm5lY3Rpb25TdGF0ZSA9IFwiZGlzY29ubmVjdGVkXCI7XG5cdHByaXZhdGUgcmVjb25uZWN0QXR0ZW1wdHMgPSAwO1xuXHRwcml2YXRlIHJlY29ubmVjdFRpbWVyOiBudW1iZXIgfCBudWxsID0gbnVsbDtcblx0cHJpdmF0ZSB1bmxvYWRSZXF1ZXN0ZWQgPSBmYWxzZTtcblxuXHRhc3luYyBvbmxvYWQoKTogUHJvbWlzZTx2b2lkPiB7XG5cdFx0YXdhaXQgdGhpcy5sb2FkU2V0dGluZ3MoKTtcblxuXHRcdGlmICghdGhpcy5zZXR0aW5ncy5jb25uZWN0aW9uVG9rZW4pIHtcblx0XHRcdHRoaXMuc2V0dGluZ3MuY29ubmVjdGlvblRva2VuID0gZ2VuZXJhdGVDb25uZWN0aW9uVG9rZW4oKTtcblx0XHRcdGF3YWl0IHRoaXMuc2F2ZVNldHRpbmdzKCk7XG5cdFx0fVxuXG5cdFx0dGhpcy5zdGF0dXNCYXJJdGVtRWwgPSB0aGlzLmFkZFN0YXR1c0Jhckl0ZW0oKTtcblx0XHR0aGlzLnN0YXR1c0Jhckl0ZW1FbC5hZGRDbGFzcyhcInBva2Utc3RhdHVzXCIpO1xuXHRcdHRoaXMuc2V0Q29ubmVjdGlvblN0YXRlKFwiZGlzY29ubmVjdGVkXCIpO1xuXG5cdFx0dGhpcy5zZXR0aW5nc1RhYiA9IG5ldyBQb2tlT2JzaWRpYW5TZXR0aW5nVGFiKHRoaXMuYXBwLCB0aGlzKTtcblx0XHR0aGlzLmFkZFNldHRpbmdUYWIodGhpcy5zZXR0aW5nc1RhYik7XG5cblx0XHR0aGlzLmNvbm5lY3QoKTtcblx0fVxuXG5cdG9udW5sb2FkKCk6IHZvaWQge1xuXHRcdHRoaXMudW5sb2FkUmVxdWVzdGVkID0gdHJ1ZTtcblx0XHR0aGlzLmNsZWFyUmVjb25uZWN0VGltZXIoKTtcblx0XHR0aGlzLmNsb3NlU29ja2V0KCk7XG5cdFx0dGhpcy5zZXRDb25uZWN0aW9uU3RhdGUoXCJkaXNjb25uZWN0ZWRcIik7XG5cdH1cblxuXHRhc3luYyBsb2FkU2V0dGluZ3MoKTogUHJvbWlzZTx2b2lkPiB7XG5cdFx0dGhpcy5zZXR0aW5ncyA9IE9iamVjdC5hc3NpZ24oe30sIERFRkFVTFRfU0VUVElOR1MsIGF3YWl0IHRoaXMubG9hZERhdGEoKSk7XG5cdH1cblxuXHRhc3luYyBzYXZlU2V0dGluZ3MoKTogUHJvbWlzZTx2b2lkPiB7XG5cdFx0YXdhaXQgdGhpcy5zYXZlRGF0YSh0aGlzLnNldHRpbmdzKTtcblx0fVxuXG5cdGdldENvbm5lY3Rpb25TdGF0ZSgpOiBDb25uZWN0aW9uU3RhdGUge1xuXHRcdHJldHVybiB0aGlzLmNvbm5lY3Rpb25TdGF0ZTtcblx0fVxuXG5cdGFzeW5jIHVwZGF0ZUNvbm5lY3Rpb25Ub2tlbih0b2tlbjogc3RyaW5nKTogUHJvbWlzZTx2b2lkPiB7XG5cdFx0dGhpcy5zZXR0aW5ncy5jb25uZWN0aW9uVG9rZW4gPSB0b2tlbi50cmltKCk7XG5cdFx0YXdhaXQgdGhpcy5zYXZlU2V0dGluZ3MoKTtcblx0XHR0aGlzLnJlY29ubmVjdE5vdygpO1xuXHR9XG5cblx0YXN5bmMgdXBkYXRlR2F0ZXdheVVybChnYXRld2F5VXJsOiBzdHJpbmcpOiBQcm9taXNlPHZvaWQ+IHtcblx0XHR0aGlzLnNldHRpbmdzLmdhdGV3YXlVcmwgPSBnYXRld2F5VXJsLnRyaW0oKSB8fCBERUZBVUxUX0dBVEVXQVlfVVJMO1xuXHRcdGF3YWl0IHRoaXMuc2F2ZVNldHRpbmdzKCk7XG5cdFx0dGhpcy5yZWNvbm5lY3ROb3coKTtcblx0fVxuXG5cdGFzeW5jIHVwZGF0ZUFsbG93V3JpdGUoYWxsb3dXcml0ZTogYm9vbGVhbik6IFByb21pc2U8dm9pZD4ge1xuXHRcdHRoaXMuc2V0dGluZ3MuYWxsb3dXcml0ZSA9IGFsbG93V3JpdGU7XG5cdFx0YXdhaXQgdGhpcy5zYXZlU2V0dGluZ3MoKTtcblx0fVxuXG5cdHJlY29ubmVjdE5vdygpOiB2b2lkIHtcblx0XHR0aGlzLmNsZWFyUmVjb25uZWN0VGltZXIoKTtcblx0XHR0aGlzLnJlY29ubmVjdEF0dGVtcHRzID0gMDtcblx0XHR0aGlzLmNsb3NlU29ja2V0KCk7XG5cblx0XHRpZiAoIXRoaXMuc2V0dGluZ3MuY29ubmVjdGlvblRva2VuKSB7XG5cdFx0XHR0aGlzLnNldENvbm5lY3Rpb25TdGF0ZShcImRpc2Nvbm5lY3RlZFwiKTtcblx0XHRcdHJldHVybjtcblx0XHR9XG5cblx0XHR0aGlzLmNvbm5lY3QoKTtcblx0fVxuXG5cdHByaXZhdGUgY29ubmVjdCgpOiB2b2lkIHtcblx0XHRpZiAodGhpcy51bmxvYWRSZXF1ZXN0ZWQgfHwgIXRoaXMuc2V0dGluZ3MuY29ubmVjdGlvblRva2VuKSB7XG5cdFx0XHR0aGlzLnNldENvbm5lY3Rpb25TdGF0ZShcImRpc2Nvbm5lY3RlZFwiKTtcblx0XHRcdHJldHVybjtcblx0XHR9XG5cblx0XHRpZiAoXG5cdFx0XHR0aGlzLnNvY2tldCAmJlxuXHRcdFx0KHRoaXMuc29ja2V0LnJlYWR5U3RhdGUgPT09IFdlYlNvY2tldC5DT05ORUNUSU5HIHx8IHRoaXMuc29ja2V0LnJlYWR5U3RhdGUgPT09IFdlYlNvY2tldC5PUEVOKVxuXHRcdCkge1xuXHRcdFx0cmV0dXJuO1xuXHRcdH1cblxuXHRcdHRoaXMuc2V0Q29ubmVjdGlvblN0YXRlKFwiY29ubmVjdGluZ1wiKTtcblxuXHRcdGNvbnN0IHVybCA9IHRoaXMuYnVpbGRHYXRld2F5VXJsKCk7XG5cblx0XHRpZiAoIXVybCkge1xuXHRcdFx0dGhpcy5zZXRDb25uZWN0aW9uU3RhdGUoXCJkaXNjb25uZWN0ZWRcIik7XG5cdFx0XHRyZXR1cm47XG5cdFx0fVxuXG5cdFx0dHJ5IHtcblx0XHRcdGNvbnNvbGUubG9nKGBQb2tlIEdhdGV3YXkgY29ubmVjdGluZyB0byAke3JlZGFjdFRva2VuKHVybCl9YCk7XG5cdFx0XHR0aGlzLnNvY2tldCA9IG5ldyBXZWJTb2NrZXQodXJsLnRvU3RyaW5nKCkpO1xuXHRcdH0gY2F0Y2ggKGVycm9yKSB7XG5cdFx0XHR0aGlzLmhhbmRsZUNvbm5lY3Rpb25FcnJvcihlcnJvcik7XG5cdFx0XHR0aGlzLnNjaGVkdWxlUmVjb25uZWN0KCk7XG5cdFx0XHRyZXR1cm47XG5cdFx0fVxuXG5cdFx0dGhpcy5zb2NrZXQub25vcGVuID0gKCkgPT4ge1xuXHRcdFx0Y29uc29sZS5sb2coXCJQb2tlIEdhdGV3YXkgY29ubmVjdGVkXCIpO1xuXHRcdFx0dGhpcy5yZWNvbm5lY3RBdHRlbXB0cyA9IDA7XG5cdFx0XHR0aGlzLnNldENvbm5lY3Rpb25TdGF0ZShcImNvbm5lY3RlZFwiKTtcblx0XHR9O1xuXG5cdFx0dGhpcy5zb2NrZXQub25tZXNzYWdlID0gKGV2ZW50OiBNZXNzYWdlRXZlbnQ8c3RyaW5nPikgPT4ge1xuXHRcdFx0dm9pZCB0aGlzLndpdGhSZXF1ZXN0VGltZW91dCh0aGlzLmhhbmRsZVNvY2tldE1lc3NhZ2UoZXZlbnQuZGF0YSksIG51bGwpO1xuXHRcdH07XG5cblx0XHR0aGlzLnNvY2tldC5vbmVycm9yID0gKGV2ZW50KSA9PiB7XG5cdFx0XHR0aGlzLmhhbmRsZUNvbm5lY3Rpb25FcnJvcihuZXcgRXJyb3IoYFdlYlNvY2tldCBjb25uZWN0aW9uIGVycm9yOiAke0pTT04uc3RyaW5naWZ5KGV2ZW50KX1gKSk7XG5cdFx0fTtcblxuXHRcdHRoaXMuc29ja2V0Lm9uY2xvc2UgPSAoZXZlbnQpID0+IHtcblx0XHRcdGNvbnNvbGUubG9nKGBQb2tlIEdhdGV3YXkgZGlzY29ubmVjdGVkOiBjb2RlPSR7ZXZlbnQuY29kZX0gcmVhc29uPSR7ZXZlbnQucmVhc29uIHx8IFwiKG5vbmUpXCJ9YCk7XG5cdFx0XHR0aGlzLnNvY2tldCA9IG51bGw7XG5cblx0XHRcdGlmICh0aGlzLnVubG9hZFJlcXVlc3RlZCkge1xuXHRcdFx0XHR0aGlzLnNldENvbm5lY3Rpb25TdGF0ZShcImRpc2Nvbm5lY3RlZFwiKTtcblx0XHRcdFx0cmV0dXJuO1xuXHRcdFx0fVxuXG5cdFx0XHR0aGlzLnNldENvbm5lY3Rpb25TdGF0ZShcImRpc2Nvbm5lY3RlZFwiKTtcblx0XHRcdHRoaXMuc2NoZWR1bGVSZWNvbm5lY3QoKTtcblx0XHR9O1xuXHR9XG5cblx0cHJpdmF0ZSBzY2hlZHVsZVJlY29ubmVjdCgpOiB2b2lkIHtcblx0XHRpZiAodGhpcy51bmxvYWRSZXF1ZXN0ZWQgfHwgIXRoaXMuc2V0dGluZ3MuY29ubmVjdGlvblRva2VuIHx8IHRoaXMucmVjb25uZWN0VGltZXIgIT09IG51bGwpIHtcblx0XHRcdHJldHVybjtcblx0XHR9XG5cblx0XHRjb25zdCBkZWxheSA9IE1hdGgubWluKEJBU0VfUkVDT05ORUNUX0RFTEFZX01TICogMiAqKiB0aGlzLnJlY29ubmVjdEF0dGVtcHRzLCBNQVhfUkVDT05ORUNUX0RFTEFZX01TKTtcblx0XHR0aGlzLnJlY29ubmVjdEF0dGVtcHRzICs9IDE7XG5cblx0XHR0aGlzLnJlY29ubmVjdFRpbWVyID0gd2luZG93LnNldFRpbWVvdXQoKCkgPT4ge1xuXHRcdFx0dGhpcy5yZWNvbm5lY3RUaW1lciA9IG51bGw7XG5cdFx0XHR0aGlzLmNvbm5lY3QoKTtcblx0XHR9LCBkZWxheSk7XG5cdH1cblxuXHRwcml2YXRlIGNsZWFyUmVjb25uZWN0VGltZXIoKTogdm9pZCB7XG5cdFx0aWYgKHRoaXMucmVjb25uZWN0VGltZXIgIT09IG51bGwpIHtcblx0XHRcdHdpbmRvdy5jbGVhclRpbWVvdXQodGhpcy5yZWNvbm5lY3RUaW1lcik7XG5cdFx0XHR0aGlzLnJlY29ubmVjdFRpbWVyID0gbnVsbDtcblx0XHR9XG5cdH1cblxuXHRwcml2YXRlIGNsb3NlU29ja2V0KCk6IHZvaWQge1xuXHRcdGlmICghdGhpcy5zb2NrZXQpIHtcblx0XHRcdHJldHVybjtcblx0XHR9XG5cblx0XHR0aGlzLnNvY2tldC5vbm9wZW4gPSBudWxsO1xuXHRcdHRoaXMuc29ja2V0Lm9ubWVzc2FnZSA9IG51bGw7XG5cdFx0dGhpcy5zb2NrZXQub25lcnJvciA9IG51bGw7XG5cdFx0dGhpcy5zb2NrZXQub25jbG9zZSA9IG51bGw7XG5cblx0XHRpZiAodGhpcy5zb2NrZXQucmVhZHlTdGF0ZSA9PT0gV2ViU29ja2V0LkNPTk5FQ1RJTkcgfHwgdGhpcy5zb2NrZXQucmVhZHlTdGF0ZSA9PT0gV2ViU29ja2V0Lk9QRU4pIHtcblx0XHRcdHRoaXMuc29ja2V0LmNsb3NlKCk7XG5cdFx0fVxuXG5cdFx0dGhpcy5zb2NrZXQgPSBudWxsO1xuXHR9XG5cblx0cHJpdmF0ZSBhc3luYyB3aXRoUmVxdWVzdFRpbWVvdXQodGFzazogUHJvbWlzZTx2b2lkPiwgaWQ6IHN0cmluZyB8IG51bGwpOiBQcm9taXNlPHZvaWQ+IHtcblx0XHRsZXQgdGltZW91dElkOiBudW1iZXIgfCBudWxsID0gbnVsbDtcblxuXHRcdGNvbnN0IHRpbWVvdXQgPSBuZXcgUHJvbWlzZTx2b2lkPigoX3Jlc29sdmUsIHJlamVjdCkgPT4ge1xuXHRcdFx0dGltZW91dElkID0gd2luZG93LnNldFRpbWVvdXQoKCkgPT4gcmVqZWN0KG5ldyBFcnJvcihcIlJlcXVlc3QgdGltZWQgb3V0XCIpKSwgUkVRVUVTVF9USU1FT1VUX01TKTtcblx0XHR9KTtcblxuXHRcdHRyeSB7XG5cdFx0XHRhd2FpdCBQcm9taXNlLnJhY2UoW3Rhc2ssIHRpbWVvdXRdKTtcblx0XHR9IGNhdGNoIChlcnJvcikge1xuXHRcdFx0dGhpcy5zZW5kUmVzcG9uc2Uoe1xuXHRcdFx0XHRpZCxcblx0XHRcdFx0c3RhdHVzOiBcImVycm9yXCIsXG5cdFx0XHRcdHBheWxvYWQ6IHsgZXJyb3I6IGVycm9yIGluc3RhbmNlb2YgRXJyb3IgPyBlcnJvci5tZXNzYWdlIDogU3RyaW5nKGVycm9yKSB9LFxuXHRcdFx0fSk7XG5cdFx0fSBmaW5hbGx5IHtcblx0XHRcdGlmICh0aW1lb3V0SWQgIT09IG51bGwpIHtcblx0XHRcdFx0d2luZG93LmNsZWFyVGltZW91dCh0aW1lb3V0SWQpO1xuXHRcdFx0fVxuXHRcdH1cblx0fVxuXG5cdHByaXZhdGUgYXN5bmMgaGFuZGxlU29ja2V0TWVzc2FnZShyYXdNZXNzYWdlOiBzdHJpbmcpOiBQcm9taXNlPHZvaWQ+IHtcblx0XHRsZXQgaW5jb21pbmc6IEluY29taW5nTWVzc2FnZTtcblxuXHRcdHRyeSB7XG5cdFx0XHRpbmNvbWluZyA9IEpTT04ucGFyc2UocmF3TWVzc2FnZSkgYXMgSW5jb21pbmdNZXNzYWdlO1xuXHRcdH0gY2F0Y2gge1xuXHRcdFx0dGhpcy5zZW5kUmVzcG9uc2Uoe1xuXHRcdFx0XHRpZDogbnVsbCxcblx0XHRcdFx0c3RhdHVzOiBcImVycm9yXCIsXG5cdFx0XHRcdHBheWxvYWQ6IHsgZXJyb3I6IFwiSW52YWxpZCBKU09OIG1lc3NhZ2VcIiB9LFxuXHRcdFx0fSk7XG5cdFx0XHRyZXR1cm47XG5cdFx0fVxuXG5cdFx0Y29uc3QgaWQgPSB0eXBlb2YgaW5jb21pbmcuaWQgPT09IFwic3RyaW5nXCIgPyBpbmNvbWluZy5pZCA6IG51bGw7XG5cdFx0Y29uc3QgYWN0aW9uID0gdHlwZW9mIGluY29taW5nLmFjdGlvbiA9PT0gXCJzdHJpbmdcIiA/IGluY29taW5nLmFjdGlvbiA6IFwiXCI7XG5cdFx0Y29uc3QgcGFyYW1zID0gaXNSZWNvcmQoaW5jb21pbmcucGFyYW1zKSA/IGluY29taW5nLnBhcmFtcyA6IHt9O1xuXG5cdFx0aWYgKCFpZCkge1xuXHRcdFx0dGhpcy5zZW5kUmVzcG9uc2Uoe1xuXHRcdFx0XHRpZCxcblx0XHRcdFx0c3RhdHVzOiBcImVycm9yXCIsXG5cdFx0XHRcdHBheWxvYWQ6IHsgZXJyb3I6IFwiTWVzc2FnZSBpZCBpcyByZXF1aXJlZFwiIH0sXG5cdFx0XHR9KTtcblx0XHRcdHJldHVybjtcblx0XHR9XG5cblx0XHR0cnkge1xuXHRcdFx0Y29uc3QgcGF5bG9hZCA9IGF3YWl0IHRoaXMuaGFuZGxlQWN0aW9uKGFjdGlvbiwgcGFyYW1zKTtcblx0XHRcdHRoaXMuc2VuZFJlc3BvbnNlKHsgaWQsIHN0YXR1czogXCJzdWNjZXNzXCIsIHBheWxvYWQgfSk7XG5cdFx0fSBjYXRjaCAoZXJyb3IpIHtcblx0XHRcdHRoaXMuc2VuZFJlc3BvbnNlKHtcblx0XHRcdFx0aWQsXG5cdFx0XHRcdHN0YXR1czogXCJlcnJvclwiLFxuXHRcdFx0XHRwYXlsb2FkOiB7IGVycm9yOiBlcnJvciBpbnN0YW5jZW9mIEVycm9yID8gZXJyb3IubWVzc2FnZSA6IFN0cmluZyhlcnJvcikgfSxcblx0XHRcdH0pO1xuXHRcdH1cblx0fVxuXG5cdHByaXZhdGUgYXN5bmMgaGFuZGxlQWN0aW9uKGFjdGlvbjogc3RyaW5nLCBwYXJhbXM6IFJlY29yZDxzdHJpbmcsIHVua25vd24+KTogUHJvbWlzZTxSZWNvcmQ8c3RyaW5nLCB1bmtub3duPj4ge1xuXHRcdHN3aXRjaCAoYWN0aW9uKSB7XG5cdFx0XHRjYXNlIFwibGlzdFwiOlxuXHRcdFx0Y2FzZSBcImxpc3RfZmlsZXNcIjpcblx0XHRcdFx0cmV0dXJuIHRoaXMubGlzdEZpbGVzKCk7XG5cdFx0XHRjYXNlIFwicmVhZFwiOlxuXHRcdFx0Y2FzZSBcInJlYWRfZmlsZVwiOlxuXHRcdFx0XHRyZXR1cm4gdGhpcy5yZWFkRmlsZShwYXJhbXMpO1xuXHRcdFx0Y2FzZSBcIndyaXRlXCI6XG5cdFx0XHRjYXNlIFwid3JpdGVfZmlsZVwiOlxuXHRcdFx0XHRyZXR1cm4gdGhpcy53cml0ZUZpbGUocGFyYW1zKTtcblx0XHRcdGNhc2UgXCJzZWFyY2hcIjpcblx0XHRcdGNhc2UgXCJzZWFyY2hfdmF1bHRcIjpcblx0XHRcdFx0cmV0dXJuIHRoaXMuc2VhcmNoVmF1bHQocGFyYW1zKTtcblx0XHRcdGRlZmF1bHQ6XG5cdFx0XHRcdHRocm93IG5ldyBFcnJvcihgVW5zdXBwb3J0ZWQgYWN0aW9uOiAke2FjdGlvbiB8fCBcIihtaXNzaW5nKVwifWApO1xuXHRcdH1cblx0fVxuXG5cdHByaXZhdGUgbGlzdEZpbGVzKCk6IFJlY29yZDxzdHJpbmcsIHVua25vd24+IHtcblx0XHRyZXR1cm4ge1xuXHRcdFx0ZmlsZXM6IHRoaXMuYXBwLnZhdWx0LmdldE1hcmtkb3duRmlsZXMoKS5tYXAoKGZpbGUpID0+IGZpbGUucGF0aCksXG5cdFx0fTtcblx0fVxuXG5cdHByaXZhdGUgYXN5bmMgcmVhZEZpbGUocGFyYW1zOiBSZWNvcmQ8c3RyaW5nLCB1bmtub3duPik6IFByb21pc2U8UmVjb3JkPHN0cmluZywgdW5rbm93bj4+IHtcblx0XHRjb25zdCBwYXRoID0gZ2V0UmVxdWlyZWRTdHJpbmcocGFyYW1zLCBcInBhdGhcIik7XG5cdFx0Y29uc3QgZmlsZSA9IHRoaXMuZ2V0TWFya2Rvd25GaWxlKHBhdGgpO1xuXHRcdGNvbnN0IGNvbnRlbnQgPSBhd2FpdCB0aGlzLmFwcC52YXVsdC5hZGFwdGVyLnJlYWQoZmlsZS5wYXRoKTtcblxuXHRcdHJldHVybiB7XG5cdFx0XHRwYXRoOiBmaWxlLnBhdGgsXG5cdFx0XHRjb250ZW50LFxuXHRcdH07XG5cdH1cblxuXHRwcml2YXRlIGFzeW5jIHdyaXRlRmlsZShwYXJhbXM6IFJlY29yZDxzdHJpbmcsIHVua25vd24+KTogUHJvbWlzZTxSZWNvcmQ8c3RyaW5nLCB1bmtub3duPj4ge1xuXHRcdGlmICghdGhpcy5zZXR0aW5ncy5hbGxvd1dyaXRlKSB7XG5cdFx0XHR0aHJvdyBuZXcgRXJyb3IoXCJXcml0ZSBhY2Nlc3MgaXMgZGlzYWJsZWQgaW4gUG9rZSBHYXRld2F5IHNldHRpbmdzXCIpO1xuXHRcdH1cblxuXHRcdGNvbnN0IHBhdGggPSBnZXRSZXF1aXJlZFN0cmluZyhwYXJhbXMsIFwicGF0aFwiKTtcblx0XHRjb25zdCBjb250ZW50ID0gZ2V0UmVxdWlyZWRTdHJpbmcocGFyYW1zLCBcImNvbnRlbnRcIik7XG5cdFx0Y29uc3Qgbm9ybWFsaXplZFBhdGggPSB0aGlzLm5vcm1hbGl6ZU1hcmtkb3duUGF0aChwYXRoKTtcblxuXHRcdGF3YWl0IHRoaXMuZW5zdXJlUGFyZW50Rm9sZGVycyhub3JtYWxpemVkUGF0aCk7XG5cdFx0YXdhaXQgdGhpcy5hcHAudmF1bHQuYWRhcHRlci53cml0ZShub3JtYWxpemVkUGF0aCwgY29udGVudCk7XG5cblx0XHRyZXR1cm4ge1xuXHRcdFx0cGF0aDogbm9ybWFsaXplZFBhdGgsXG5cdFx0XHRieXRlczogbmV3IFRleHRFbmNvZGVyKCkuZW5jb2RlKGNvbnRlbnQpLmxlbmd0aCxcblx0XHR9O1xuXHR9XG5cblx0cHJpdmF0ZSBhc3luYyBzZWFyY2hWYXVsdChwYXJhbXM6IFJlY29yZDxzdHJpbmcsIHVua25vd24+KTogUHJvbWlzZTxSZWNvcmQ8c3RyaW5nLCB1bmtub3duPj4ge1xuXHRcdGNvbnN0IHF1ZXJ5ID0gZ2V0UmVxdWlyZWRTdHJpbmcocGFyYW1zLCBcInF1ZXJ5XCIpLnRyaW0oKTtcblxuXHRcdGlmICghcXVlcnkpIHtcblx0XHRcdHRocm93IG5ldyBFcnJvcihcIlNlYXJjaCBxdWVyeSBjYW5ub3QgYmUgZW1wdHlcIik7XG5cdFx0fVxuXG5cdFx0Y29uc3Qgbm9ybWFsaXplZFF1ZXJ5ID0gcXVlcnkudG9Mb2NhbGVMb3dlckNhc2UoKTtcblx0XHRjb25zdCBtYXRjaGVzOiBTZWFyY2hNYXRjaFtdID0gW107XG5cblx0XHRmb3IgKGNvbnN0IGZpbGUgb2YgdGhpcy5hcHAudmF1bHQuZ2V0TWFya2Rvd25GaWxlcygpKSB7XG5cdFx0XHRjb25zdCBjb250ZW50ID0gYXdhaXQgdGhpcy5hcHAudmF1bHQuYWRhcHRlci5yZWFkKGZpbGUucGF0aCk7XG5cdFx0XHRjb25zdCBpbmRleCA9IGNvbnRlbnQudG9Mb2NhbGVMb3dlckNhc2UoKS5pbmRleE9mKG5vcm1hbGl6ZWRRdWVyeSk7XG5cblx0XHRcdGlmIChpbmRleCA9PT0gLTEpIHtcblx0XHRcdFx0Y29udGludWU7XG5cdFx0XHR9XG5cblx0XHRcdG1hdGNoZXMucHVzaCh7XG5cdFx0XHRcdHBhdGg6IGZpbGUucGF0aCxcblx0XHRcdFx0c25pcHBldDogbWFrZVNuaXBwZXQoY29udGVudCwgaW5kZXgsIHF1ZXJ5Lmxlbmd0aCksXG5cdFx0XHR9KTtcblx0XHR9XG5cblx0XHRyZXR1cm4geyBtYXRjaGVzIH07XG5cdH1cblxuXHRwcml2YXRlIGdldE1hcmtkb3duRmlsZShwYXRoOiBzdHJpbmcpOiBURmlsZSB7XG5cdFx0Y29uc3Qgbm9ybWFsaXplZFBhdGggPSB0aGlzLm5vcm1hbGl6ZU1hcmtkb3duUGF0aChwYXRoKTtcblx0XHRjb25zdCBmaWxlID0gdGhpcy5hcHAudmF1bHQuZ2V0QWJzdHJhY3RGaWxlQnlQYXRoKG5vcm1hbGl6ZWRQYXRoKTtcblxuXHRcdGlmICghKGZpbGUgaW5zdGFuY2VvZiBURmlsZSkpIHtcblx0XHRcdHRocm93IG5ldyBFcnJvcihgRmlsZSBub3QgZm91bmQ6ICR7bm9ybWFsaXplZFBhdGh9YCk7XG5cdFx0fVxuXG5cdFx0aWYgKGZpbGUuZXh0ZW5zaW9uICE9PSBcIm1kXCIpIHtcblx0XHRcdHRocm93IG5ldyBFcnJvcihgT25seSBtYXJrZG93biBmaWxlcyBhcmUgc3VwcG9ydGVkOiAke25vcm1hbGl6ZWRQYXRofWApO1xuXHRcdH1cblxuXHRcdHJldHVybiBmaWxlO1xuXHR9XG5cblx0cHJpdmF0ZSBub3JtYWxpemVNYXJrZG93blBhdGgocGF0aDogc3RyaW5nKTogc3RyaW5nIHtcblx0XHRjb25zdCBub3JtYWxpemVkUGF0aCA9IHBhdGgudHJpbSgpLnJlcGxhY2UoL15cXC8rLywgXCJcIik7XG5cblx0XHRpZiAoIW5vcm1hbGl6ZWRQYXRoKSB7XG5cdFx0XHR0aHJvdyBuZXcgRXJyb3IoXCJQYXRoIGNhbm5vdCBiZSBlbXB0eVwiKTtcblx0XHR9XG5cblx0XHRjb25zdCBwYXJ0cyA9IG5vcm1hbGl6ZWRQYXRoLnNwbGl0KFwiL1wiKS5maWx0ZXIoQm9vbGVhbik7XG5cblx0XHRpZiAocGFydHMuaW5jbHVkZXMoXCIuLlwiKSB8fCBwYXJ0cy5zb21lKChwYXJ0KSA9PiBwYXJ0ID09PSBcIi5cIikpIHtcblx0XHRcdHRocm93IG5ldyBFcnJvcihcIlBhdGggY2Fubm90IGNvbnRhaW4gcGFyZW50IG9yIGN1cnJlbnQtZGlyZWN0b3J5IHNlZ21lbnRzXCIpO1xuXHRcdH1cblxuXHRcdGlmICghbm9ybWFsaXplZFBhdGgudG9Mb2NhbGVMb3dlckNhc2UoKS5lbmRzV2l0aChcIi5tZFwiKSkge1xuXHRcdFx0dGhyb3cgbmV3IEVycm9yKFwiT25seSBtYXJrZG93biBmaWxlIHBhdGhzIGVuZGluZyBpbiAubWQgYXJlIHN1cHBvcnRlZFwiKTtcblx0XHR9XG5cblx0XHRyZXR1cm4gcGFydHMuam9pbihcIi9cIik7XG5cdH1cblxuXHRwcml2YXRlIGFzeW5jIGVuc3VyZVBhcmVudEZvbGRlcnMoZmlsZVBhdGg6IHN0cmluZyk6IFByb21pc2U8dm9pZD4ge1xuXHRcdGNvbnN0IHBhcmVudFBhcnRzID0gZmlsZVBhdGguc3BsaXQoXCIvXCIpLnNsaWNlKDAsIC0xKTtcblx0XHRsZXQgY3VycmVudFBhdGggPSBcIlwiO1xuXG5cdFx0Zm9yIChjb25zdCBwYXJ0IG9mIHBhcmVudFBhcnRzKSB7XG5cdFx0XHRjdXJyZW50UGF0aCA9IGN1cnJlbnRQYXRoID8gYCR7Y3VycmVudFBhdGh9LyR7cGFydH1gIDogcGFydDtcblxuXHRcdFx0aWYgKCEoYXdhaXQgdGhpcy5hcHAudmF1bHQuYWRhcHRlci5leGlzdHMoY3VycmVudFBhdGgpKSkge1xuXHRcdFx0XHRhd2FpdCB0aGlzLmFwcC52YXVsdC5hZGFwdGVyLm1rZGlyKGN1cnJlbnRQYXRoKTtcblx0XHRcdH1cblx0XHR9XG5cdH1cblxuXHRwcml2YXRlIGJ1aWxkR2F0ZXdheVVybCgpOiBVUkwgfCBudWxsIHtcblx0XHRsZXQgdXJsOiBVUkw7XG5cblx0XHR0cnkge1xuXHRcdFx0dXJsID0gbmV3IFVSTCh0aGlzLnNldHRpbmdzLmdhdGV3YXlVcmwgfHwgREVGQVVMVF9HQVRFV0FZX1VSTCk7XG5cdFx0fSBjYXRjaCB7XG5cdFx0XHRuZXcgTm90aWNlKFwiSW52YWxpZCBQb2tlIEdhdGV3YXkgVVJMXCIpO1xuXHRcdFx0cmV0dXJuIG51bGw7XG5cdFx0fVxuXG5cdFx0aWYgKHVybC5wcm90b2NvbCAhPT0gXCJ3czpcIiAmJiB1cmwucHJvdG9jb2wgIT09IFwid3NzOlwiKSB7XG5cdFx0XHRuZXcgTm90aWNlKFwiUG9rZSBHYXRld2F5IFVSTCBtdXN0IHN0YXJ0IHdpdGggd3M6Ly8gb3Igd3NzOi8vXCIpO1xuXHRcdFx0cmV0dXJuIG51bGw7XG5cdFx0fVxuXG5cdFx0dXJsLnNlYXJjaFBhcmFtcy5zZXQoXCJ0b2tlblwiLCB0aGlzLnNldHRpbmdzLmNvbm5lY3Rpb25Ub2tlbik7XG5cdFx0dXJsLnNlYXJjaFBhcmFtcy5zZXQoXCJwbHVnaW5cIiwgdGhpcy5tYW5pZmVzdC5pZCk7XG5cdFx0dXJsLnNlYXJjaFBhcmFtcy5zZXQoXCJ2ZXJzaW9uXCIsIHRoaXMubWFuaWZlc3QudmVyc2lvbik7XG5cblx0XHRyZXR1cm4gdXJsO1xuXHR9XG5cblx0cHJpdmF0ZSBzZW5kUmVzcG9uc2UocmVzcG9uc2U6IFJlc3BvbnNlTWVzc2FnZSk6IHZvaWQge1xuXHRcdGlmICghdGhpcy5zb2NrZXQgfHwgdGhpcy5zb2NrZXQucmVhZHlTdGF0ZSAhPT0gV2ViU29ja2V0Lk9QRU4pIHtcblx0XHRcdHJldHVybjtcblx0XHR9XG5cblx0XHR0aGlzLnNvY2tldC5zZW5kKEpTT04uc3RyaW5naWZ5KHJlc3BvbnNlKSk7XG5cdH1cblxuXHRwcml2YXRlIHNldENvbm5lY3Rpb25TdGF0ZShzdGF0ZTogQ29ubmVjdGlvblN0YXRlKTogdm9pZCB7XG5cdFx0dGhpcy5jb25uZWN0aW9uU3RhdGUgPSBzdGF0ZTtcblxuXHRcdGlmICh0aGlzLnN0YXR1c0Jhckl0ZW1FbCkge1xuXHRcdFx0dGhpcy5zdGF0dXNCYXJJdGVtRWwuc2V0VGV4dChgUG9rZTogJHtjYXBpdGFsaXplKHN0YXRlKX1gKTtcblx0XHRcdHRoaXMuc3RhdHVzQmFySXRlbUVsLnJlbW92ZUNsYXNzKFwiaXMtY29ubmVjdGVkXCIsIFwiaXMtY29ubmVjdGluZ1wiLCBcImlzLWRpc2Nvbm5lY3RlZFwiKTtcblx0XHRcdHRoaXMuc3RhdHVzQmFySXRlbUVsLmFkZENsYXNzKGBpcy0ke3N0YXRlfWApO1xuXHRcdH1cblxuXHRcdHRoaXMuc2V0dGluZ3NUYWI/LnVwZGF0ZVN0YXR1cyhzdGF0ZSk7XG5cdH1cblxuXHRwcml2YXRlIGhhbmRsZUNvbm5lY3Rpb25FcnJvcihlcnJvcjogdW5rbm93bik6IHZvaWQge1xuXHRcdGNvbnNvbGUuZXJyb3IoXCJQb2tlIEdhdGV3YXkgY29ubmVjdGlvbiBlcnJvclwiLCBlcnJvcik7XG5cdH1cbn1cblxuY2xhc3MgUG9rZU9ic2lkaWFuU2V0dGluZ1RhYiBleHRlbmRzIFBsdWdpblNldHRpbmdUYWIge1xuXHRwcml2YXRlIHBsdWdpbjogUG9rZU9ic2lkaWFuUGx1Z2luO1xuXHRwcml2YXRlIHN0YXR1c0VsOiBIVE1MRWxlbWVudCB8IG51bGwgPSBudWxsO1xuXG5cdGNvbnN0cnVjdG9yKGFwcDogQXBwLCBwbHVnaW46IFBva2VPYnNpZGlhblBsdWdpbikge1xuXHRcdHN1cGVyKGFwcCwgcGx1Z2luKTtcblx0XHR0aGlzLnBsdWdpbiA9IHBsdWdpbjtcblx0fVxuXG5cdGRpc3BsYXkoKTogdm9pZCB7XG5cdFx0Y29uc3QgeyBjb250YWluZXJFbCB9ID0gdGhpcztcblx0XHRjb250YWluZXJFbC5lbXB0eSgpO1xuXG5cdFx0Y29udGFpbmVyRWwuY3JlYXRlRWwoXCJoMlwiLCB7IHRleHQ6IFwiUG9rZSBHYXRld2F5XCIgfSk7XG5cblx0XHRuZXcgU2V0dGluZyhjb250YWluZXJFbClcblx0XHRcdC5zZXROYW1lKFwiR2F0ZXdheSBVUkxcIilcblx0XHRcdC5zZXREZXNjKFwiV2ViU29ja2V0IGVuZHBvaW50IHVzZWQgdG8gY29ubmVjdCB0aGlzIHZhdWx0IHRvIFBva2UuXCIpXG5cdFx0XHQuYWRkVGV4dCgodGV4dCkgPT4ge1xuXHRcdFx0XHR0ZXh0XG5cdFx0XHRcdFx0LnNldFBsYWNlaG9sZGVyKERFRkFVTFRfR0FURVdBWV9VUkwpXG5cdFx0XHRcdFx0LnNldFZhbHVlKHRoaXMucGx1Z2luLnNldHRpbmdzLmdhdGV3YXlVcmwgfHwgREVGQVVMVF9HQVRFV0FZX1VSTClcblx0XHRcdFx0XHQub25DaGFuZ2UoYXN5bmMgKHZhbHVlKSA9PiB7XG5cdFx0XHRcdFx0XHRhd2FpdCB0aGlzLnBsdWdpbi51cGRhdGVHYXRld2F5VXJsKHZhbHVlKTtcblx0XHRcdFx0XHR9KTtcblx0XHRcdH0pO1xuXG5cdFx0bmV3IFNldHRpbmcoY29udGFpbmVyRWwpXG5cdFx0XHQuc2V0TmFtZShcIkNvbm5lY3Rpb24gdG9rZW5cIilcblx0XHRcdC5zZXREZXNjKFwiUGFzdGUgdGhpcyB0b2tlbiBpbnRvIFBva2UncyBBZGQgS2V5IGZpZWxkIGZvciB0aGUgT2JzaWRpYW4gcmVjaXBlLlwiKVxuXHRcdFx0LmFkZFRleHQoKHRleHQpID0+IHtcblx0XHRcdFx0dGV4dFxuXHRcdFx0XHRcdC5zZXRQbGFjZWhvbGRlcihcIlBhc3RlIHRva2VuXCIpXG5cdFx0XHRcdFx0LnNldFZhbHVlKHRoaXMucGx1Z2luLnNldHRpbmdzLmNvbm5lY3Rpb25Ub2tlbilcblx0XHRcdFx0XHQub25DaGFuZ2UoYXN5bmMgKHZhbHVlKSA9PiB7XG5cdFx0XHRcdFx0XHRhd2FpdCB0aGlzLnBsdWdpbi51cGRhdGVDb25uZWN0aW9uVG9rZW4odmFsdWUpO1xuXHRcdFx0XHRcdH0pO1xuXG5cdFx0XHRcdHRleHQuaW5wdXRFbC50eXBlID0gXCJwYXNzd29yZFwiO1xuXHRcdFx0fSlcblx0XHRcdC5hZGRFeHRyYUJ1dHRvbigoYnV0dG9uKSA9PiB7XG5cdFx0XHRcdGJ1dHRvblxuXHRcdFx0XHRcdC5zZXRJY29uKFwiY29weVwiKVxuXHRcdFx0XHRcdC5zZXRUb29sdGlwKFwiQ29weSB0b2tlblwiKVxuXHRcdFx0XHRcdC5vbkNsaWNrKGFzeW5jICgpID0+IHtcblx0XHRcdFx0XHRcdGF3YWl0IG5hdmlnYXRvci5jbGlwYm9hcmQud3JpdGVUZXh0KHRoaXMucGx1Z2luLnNldHRpbmdzLmNvbm5lY3Rpb25Ub2tlbik7XG5cdFx0XHRcdFx0XHRuZXcgTm90aWNlKFwiUG9rZSBHYXRld2F5IHRva2VuIGNvcGllZFwiKTtcblx0XHRcdFx0XHR9KTtcblx0XHRcdH0pXG5cdFx0XHQuYWRkRXh0cmFCdXR0b24oKGJ1dHRvbikgPT4ge1xuXHRcdFx0XHRidXR0b25cblx0XHRcdFx0XHQuc2V0SWNvbihcInJlZnJlc2gtY3dcIilcblx0XHRcdFx0XHQuc2V0VG9vbHRpcChcIkdlbmVyYXRlIG5ldyB0b2tlblwiKVxuXHRcdFx0XHRcdC5vbkNsaWNrKGFzeW5jICgpID0+IHtcblx0XHRcdFx0XHRcdGF3YWl0IHRoaXMucGx1Z2luLnVwZGF0ZUNvbm5lY3Rpb25Ub2tlbihnZW5lcmF0ZUNvbm5lY3Rpb25Ub2tlbigpKTtcblx0XHRcdFx0XHRcdHRoaXMuZGlzcGxheSgpO1xuXHRcdFx0XHRcdFx0bmV3IE5vdGljZShcIkdlbmVyYXRlZCBhIG5ldyBQb2tlIEdhdGV3YXkgdG9rZW5cIik7XG5cdFx0XHRcdFx0fSk7XG5cdFx0XHR9KTtcblxuXHRcdG5ldyBTZXR0aW5nKGNvbnRhaW5lckVsKVxuXHRcdFx0LnNldE5hbWUoXCJBbGxvdyB3cml0ZXNcIilcblx0XHRcdC5zZXREZXNjKFwiQWxsb3cgUG9rZSB0byBjcmVhdGUgb3Igb3ZlcndyaXRlIG1hcmtkb3duIGZpbGVzIGluIHRoaXMgdmF1bHQuXCIpXG5cdFx0XHQuYWRkVG9nZ2xlKCh0b2dnbGUpID0+IHtcblx0XHRcdFx0dG9nZ2xlXG5cdFx0XHRcdFx0LnNldFZhbHVlKHRoaXMucGx1Z2luLnNldHRpbmdzLmFsbG93V3JpdGUpXG5cdFx0XHRcdFx0Lm9uQ2hhbmdlKGFzeW5jICh2YWx1ZSkgPT4ge1xuXHRcdFx0XHRcdFx0YXdhaXQgdGhpcy5wbHVnaW4udXBkYXRlQWxsb3dXcml0ZSh2YWx1ZSk7XG5cdFx0XHRcdFx0fSk7XG5cdFx0XHR9KTtcblxuXHRcdGNvbnN0IHN0YXR1c1NldHRpbmcgPSBuZXcgU2V0dGluZyhjb250YWluZXJFbClcblx0XHRcdC5zZXROYW1lKFwiQ29ubmVjdGlvbiBzdGF0dXNcIilcblx0XHRcdC5zZXREZXNjKFwiQ3VycmVudCBnYXRld2F5IGNvbm5lY3Rpb24gc3RhdGUuXCIpXG5cdFx0XHQuYWRkRXh0cmFCdXR0b24oKGJ1dHRvbikgPT4ge1xuXHRcdFx0XHRidXR0b25cblx0XHRcdFx0XHQuc2V0SWNvbihcInJlZnJlc2gtY3dcIilcblx0XHRcdFx0XHQuc2V0VG9vbHRpcChcIlJlY29ubmVjdFwiKVxuXHRcdFx0XHRcdC5vbkNsaWNrKCgpID0+IHtcblx0XHRcdFx0XHRcdHRoaXMucGx1Z2luLnJlY29ubmVjdE5vdygpO1xuXHRcdFx0XHRcdFx0bmV3IE5vdGljZShcIlJlY29ubmVjdGluZyBQb2tlIEdhdGV3YXlcIik7XG5cdFx0XHRcdFx0fSk7XG5cdFx0XHR9KTtcblxuXHRcdHRoaXMuc3RhdHVzRWwgPSBzdGF0dXNTZXR0aW5nLmNvbnRyb2xFbC5jcmVhdGVTcGFuKCk7XG5cdFx0dGhpcy5zdGF0dXNFbC5hZGRDbGFzcyhcInBva2Utc3RhdHVzXCIpO1xuXHRcdHRoaXMudXBkYXRlU3RhdHVzKHRoaXMucGx1Z2luLmdldENvbm5lY3Rpb25TdGF0ZSgpKTtcblx0fVxuXG5cdHVwZGF0ZVN0YXR1cyhzdGF0ZTogQ29ubmVjdGlvblN0YXRlKTogdm9pZCB7XG5cdFx0aWYgKCF0aGlzLnN0YXR1c0VsKSB7XG5cdFx0XHRyZXR1cm47XG5cdFx0fVxuXG5cdFx0dGhpcy5zdGF0dXNFbC5zZXRUZXh0KGNhcGl0YWxpemUoc3RhdGUpKTtcblx0XHR0aGlzLnN0YXR1c0VsLnJlbW92ZUNsYXNzKFwiaXMtY29ubmVjdGVkXCIsIFwiaXMtY29ubmVjdGluZ1wiLCBcImlzLWRpc2Nvbm5lY3RlZFwiKTtcblx0XHR0aGlzLnN0YXR1c0VsLmFkZENsYXNzKGBpcy0ke3N0YXRlfWApO1xuXHR9XG59XG5cbmZ1bmN0aW9uIGdldFJlcXVpcmVkU3RyaW5nKHBhcmFtczogUmVjb3JkPHN0cmluZywgdW5rbm93bj4sIGtleTogc3RyaW5nKTogc3RyaW5nIHtcblx0Y29uc3QgdmFsdWUgPSBwYXJhbXNba2V5XTtcblxuXHRpZiAodHlwZW9mIHZhbHVlICE9PSBcInN0cmluZ1wiKSB7XG5cdFx0dGhyb3cgbmV3IEVycm9yKGBNaXNzaW5nIHJlcXVpcmVkIHN0cmluZyBwYXJhbTogJHtrZXl9YCk7XG5cdH1cblxuXHRyZXR1cm4gdmFsdWU7XG59XG5cbmZ1bmN0aW9uIGlzUmVjb3JkKHZhbHVlOiB1bmtub3duKTogdmFsdWUgaXMgUmVjb3JkPHN0cmluZywgdW5rbm93bj4ge1xuXHRyZXR1cm4gdHlwZW9mIHZhbHVlID09PSBcIm9iamVjdFwiICYmIHZhbHVlICE9PSBudWxsICYmICFBcnJheS5pc0FycmF5KHZhbHVlKTtcbn1cblxuZnVuY3Rpb24gbWFrZVNuaXBwZXQoY29udGVudDogc3RyaW5nLCBpbmRleDogbnVtYmVyLCBtYXRjaExlbmd0aDogbnVtYmVyKTogc3RyaW5nIHtcblx0Y29uc3QgaGFsZldpbmRvdyA9IE1hdGguZmxvb3IoKE1BWF9TTklQUEVUX0xFTkdUSCAtIG1hdGNoTGVuZ3RoKSAvIDIpO1xuXHRjb25zdCBzdGFydCA9IE1hdGgubWF4KDAsIGluZGV4IC0gaGFsZldpbmRvdyk7XG5cdGNvbnN0IGVuZCA9IE1hdGgubWluKGNvbnRlbnQubGVuZ3RoLCBpbmRleCArIG1hdGNoTGVuZ3RoICsgaGFsZldpbmRvdyk7XG5cdGNvbnN0IHByZWZpeCA9IHN0YXJ0ID4gMCA/IFwiLi4uXCIgOiBcIlwiO1xuXHRjb25zdCBzdWZmaXggPSBlbmQgPCBjb250ZW50Lmxlbmd0aCA/IFwiLi4uXCIgOiBcIlwiO1xuXG5cdHJldHVybiBgJHtwcmVmaXh9JHtjb250ZW50LnNsaWNlKHN0YXJ0LCBlbmQpLnJlcGxhY2UoL1xccysvZywgXCIgXCIpLnRyaW0oKX0ke3N1ZmZpeH1gO1xufVxuXG5mdW5jdGlvbiBjYXBpdGFsaXplKHZhbHVlOiBzdHJpbmcpOiBzdHJpbmcge1xuXHRyZXR1cm4gdmFsdWUuY2hhckF0KDApLnRvTG9jYWxlVXBwZXJDYXNlKCkgKyB2YWx1ZS5zbGljZSgxKTtcbn1cblxuZnVuY3Rpb24gZ2VuZXJhdGVDb25uZWN0aW9uVG9rZW4oKTogc3RyaW5nIHtcblx0Y29uc3QgYnl0ZXMgPSBuZXcgVWludDhBcnJheSgzMik7XG5cdGNyeXB0by5nZXRSYW5kb21WYWx1ZXMoYnl0ZXMpO1xuXHRyZXR1cm4gYCR7R0VORVJBVEVEX1RPS0VOX1BSRUZJWH0ke3RvQmFzZTY0VXJsKGJ5dGVzKX1gO1xufVxuXG5mdW5jdGlvbiB0b0Jhc2U2NFVybChieXRlczogVWludDhBcnJheSk6IHN0cmluZyB7XG5cdGxldCBiaW5hcnkgPSBcIlwiO1xuXG5cdGZvciAoY29uc3QgYnl0ZSBvZiBieXRlcykge1xuXHRcdGJpbmFyeSArPSBTdHJpbmcuZnJvbUNoYXJDb2RlKGJ5dGUpO1xuXHR9XG5cblx0cmV0dXJuIGJ0b2EoYmluYXJ5KS5yZXBsYWNlKC9cXCsvZywgXCItXCIpLnJlcGxhY2UoL1xcLy9nLCBcIl9cIikucmVwbGFjZSgvPSskL2csIFwiXCIpO1xufVxuXG5mdW5jdGlvbiByZWRhY3RUb2tlbih1cmw6IFVSTCk6IHN0cmluZyB7XG5cdGNvbnN0IGNvcHkgPSBuZXcgVVJMKHVybC50b1N0cmluZygpKTtcblxuXHRpZiAoY29weS5zZWFyY2hQYXJhbXMuaGFzKFwidG9rZW5cIikpIHtcblx0XHRjb3B5LnNlYXJjaFBhcmFtcy5zZXQoXCJ0b2tlblwiLCBcIioqKlwiKTtcblx0fVxuXG5cdHJldHVybiBjb3B5LnRvU3RyaW5nKCk7XG59XG4iXX0=