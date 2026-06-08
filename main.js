"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
const obsidian_1 = require("obsidian");
const DEFAULT_GATEWAY_URL = "wss://obsidian.matt-nz.com/obsidian/sync";
const BASE_RECONNECT_DELAY_MS = 1000;
const MAX_RECONNECT_DELAY_MS = 30000;
const REQUEST_TIMEOUT_MS = 30000;
const MAX_SNIPPET_LENGTH = 180;
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
        this.statusBarItemEl = this.addStatusBarItem();
        this.statusBarItemEl.addClass("poke-status");
        this.setConnectionState("disconnected");
        this.settingsTab = new PokeObsidianSettingTab(this.app, this);
        this.addSettingTab(this.settingsTab);
        if (this.settings.connectionToken) {
            this.connect();
        }
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
function redactToken(url) {
    const copy = new URL(url.toString());
    if (copy.searchParams.has("token")) {
        copy.searchParams.set("token", "***");
    }
    return copy.toString();
}
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoibWFpbi5qcyIsInNvdXJjZVJvb3QiOiIiLCJzb3VyY2VzIjpbIm1haW4udHMiXSwibmFtZXMiOltdLCJtYXBwaW5ncyI6Ijs7QUFBQSx1Q0FBaUY7QUFFakYsTUFBTSxtQkFBbUIsR0FBRywwQ0FBMEMsQ0FBQztBQUN2RSxNQUFNLHVCQUF1QixHQUFHLElBQUssQ0FBQztBQUN0QyxNQUFNLHNCQUFzQixHQUFHLEtBQU0sQ0FBQztBQUN0QyxNQUFNLGtCQUFrQixHQUFHLEtBQU0sQ0FBQztBQUNsQyxNQUFNLGtCQUFrQixHQUFHLEdBQUcsQ0FBQztBQTJCL0IsTUFBTSxnQkFBZ0IsR0FBeUI7SUFDOUMsVUFBVSxFQUFFLG1CQUFtQjtJQUMvQixlQUFlLEVBQUUsRUFBRTtJQUNuQixVQUFVLEVBQUUsS0FBSztDQUNqQixDQUFDO0FBRUYsTUFBcUIsa0JBQW1CLFNBQVEsaUJBQU07SUFBdEQ7O1FBQ0MsYUFBUSxHQUF5QixnQkFBZ0IsQ0FBQztRQUMxQyxXQUFNLEdBQXFCLElBQUksQ0FBQztRQUNoQyxvQkFBZSxHQUF1QixJQUFJLENBQUM7UUFDM0MsZ0JBQVcsR0FBa0MsSUFBSSxDQUFDO1FBQ2xELG9CQUFlLEdBQW9CLGNBQWMsQ0FBQztRQUNsRCxzQkFBaUIsR0FBRyxDQUFDLENBQUM7UUFDdEIsbUJBQWMsR0FBa0IsSUFBSSxDQUFDO1FBQ3JDLG9CQUFlLEdBQUcsS0FBSyxDQUFDO0lBOFlqQyxDQUFDO0lBNVlBLEtBQUssQ0FBQyxNQUFNO1FBQ1gsTUFBTSxJQUFJLENBQUMsWUFBWSxFQUFFLENBQUM7UUFFMUIsSUFBSSxDQUFDLGVBQWUsR0FBRyxJQUFJLENBQUMsZ0JBQWdCLEVBQUUsQ0FBQztRQUMvQyxJQUFJLENBQUMsZUFBZSxDQUFDLFFBQVEsQ0FBQyxhQUFhLENBQUMsQ0FBQztRQUM3QyxJQUFJLENBQUMsa0JBQWtCLENBQUMsY0FBYyxDQUFDLENBQUM7UUFFeEMsSUFBSSxDQUFDLFdBQVcsR0FBRyxJQUFJLHNCQUFzQixDQUFDLElBQUksQ0FBQyxH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R0FBRyxNQUFNLEVBQUUsQ0FBQztBQUNyRixDQUFDO0FBRUQsU0FBUyxVQUFVLENBQUMsS0FBYTtJQUNoQyxPQUFPLEtBQUssQ0FBQyxNQUFNLENBQUMsQ0FBQyxDQUFDLENBQUMsaUJBQWlCLEVBQUUsR0FBRyxLQUFLLENBQUMsS0FBSyxDQUFDLENBQUMsQ0FBQyxDQUFDO0FBQzdELENBQUM7QUFFRCxTQUFTLFdBQVcsQ0FBQyxHQUFRO0lBQzVCLE1BQU0sSUFBSSxHQUFHLElBQUksR0FBRyxDQUFDLEdBQUcsQ0FBQyxRQUFRLEVBQUUsQ0FBQyxDQUFDO0lBRXJDLElBQUksSUFBSSxDQUFDLFlBQVksQ0FBQyxHQUFHLENBQUMsT0FBTyxDQUFDLEVBQUUsQ0FBQztRQUNwQyxJQUFJLENBQUMsWUFBWSxDQUFDLEdBQUcsQ0FBQyxPQUFPLEVBQUUsS0FBSyxDQUFDLENBQUM7SUFDdkMsQ0FBQztJQUVELE9BQU8sSUFBSSxDQUFDLFFBQVEsRUFBRSxDQUFDO0FBQ3hCLENBQUMiLCJzb3VyY2VzQ29udGVudCI6WyJpbXBvcnQgeyBBcHAsIE5vdGljZSwgUGx1Z2luLCBQbHVnaW5TZXR0aW5nVGFiLCBTZXR0aW5nLCBURmlsZSB9IGZyb20gXCJvYnNpZGlhblwiO1xuXG5jb25zdCBERUZBVUxUX0dBVEVXQVlfVVJMID0gXCJ3c3M6Ly9vYnNpZGlhbi5tYXR0LW56LmNvbS9vYnNpZGlhbi9zeW5jXCI7XG5jb25zdCBCQVNFX1JFQ09OTkVDVF9ERUxBWV9NUyA9IDFfMDAwO1xuY29uc3QgTUFYX1JFQ09OTkVDVF9ERUxBWV9NUyA9IDMwXzAwMDtcbmNvbnN0IFJFUVVFU1RfVElNRU9VVF9NUyA9IDMwXzAwMDtcbmNvbnN0IE1BWF9TTklQUEVUX0xFTkdUSCA9IDE4MDtcblxudHlwZSBDb25uZWN0aW9uU3RhdGUgPSBcImNvbm5lY3RlZFwiIHwgXCJjb25uZWN0aW5nXCIgfCBcImRpc2Nvbm5lY3RlZFwiO1xuXG5pbnRlcmZhY2UgUG9rZU9ic2lkaWFuU2V0dGluZ3Mge1xuXHRnYXRld2F5VXJsOiBzdHJpbmc7XG5cdGNvbm5lY3Rpb25Ub2tlbjogc3RyaW5nO1xuXHRhbGxvd1dyaXRlOiBib29sZWFuO1xufVxuXG5pbnRlcmZhY2UgSW5jb21pbmdNZXNzYWdlIHtcblx0aWQ/OiB1bmtub3duO1xuXHRhY3Rpb24/OiB1bmtub3duO1xuXHRwYXJhbXM/OiB1bmtub3duO1xufVxuXG5pbnRlcmZhY2UgUmVzcG9uc2VNZXNzYWdlIHtcblx0aWQ6IHN0cmluZyB8IG51bGw7XG5cdHN0YXR1czogXCJzdWNjZXNzXCIgfCBcImVycm9yXCI7XG5cdHBheWxvYWQ6IFJlY29yZDxzdHJpbmcsIHVua25vd24+O1xufVxuXG5pbnRlcmZhY2UgU2VhcmNoTWF0Y2gge1xuXHRwYXRoOiBzdHJpbmc7XG5cdHNuaXBwZXQ6IHN0cmluZztcbn1cblxuY29uc3QgREVGQVVMVF9TRVRUSU5HUzogUG9rZU9ic2lkaWFuU2V0dGluZ3MgPSB7XG5cdGdhdGV3YXlVcmw6IERFRkFVTFRfR0FURVdBWV9VUkwsXG5cdGNvbm5lY3Rpb25Ub2tlbjogXCJcIixcblx0YWxsb3dXcml0ZTogZmFsc2UsXG59O1xuXG5leHBvcnQgZGVmYXVsdCBjbGFzcyBQb2tlT2JzaWRpYW5QbHVnaW4gZXh0ZW5kcyBQbHVnaW4ge1xuXHRzZXR0aW5nczogUG9rZU9ic2lkaWFuU2V0dGluZ3MgPSBERUZBVUxUX1NFVFRJTkdTO1xuXHRwcml2YXRlIHNvY2tldDogV2ViU29ja2V0IHwgbnVsbCA9IG51bGw7XG5cdHByaXZhdGUgc3RhdHVzQmFySXRlbUVsOiBIVE1MRWxlbWVudCB8IG51bGwgPSBudWxsO1xuXHRwcml2YXRlIHNldHRpbmdzVGFiOiBQb2tlT2JzaWRpYW5TZXR0aW5nVGFiIHwgbnVsbCA9IG51bGw7XG5cdHByaXZhdGUgY29ubmVjdGlvblN0YXRlOiBDb25uZWN0aW9uU3RhdGUgPSBcImRpc2Nvbm5lY3RlZFwiO1xuXHRwcml2YXRlIHJlY29ubmVjdEF0dGVtcHRzID0gMDtcblx0cHJpdmF0ZSByZWNvbm5lY3RUaW1lcjogbnVtYmVyIHwgbnVsbCA9IG51bGw7XG5cdHByaXZhdGUgdW5sb2FkUmVxdWVzdGVkID0gZmFsc2U7XG5cblx0YXN5bmMgb25sb2FkKCk6IFByb21pc2U8dm9pZD4ge1xuXHRcdGF3YWl0IHRoaXMubG9hZFNldHRpbmdzKCk7XG5cblx0XHR0aGlzLnN0YXR1c0Jhckl0ZW1FbCA9IHRoaXMuYWRkU3RhdHVzQmFySXRlbSgpO1xuXHRcdHRoaXMuc3RhdHVzQmFySXRlbUVsLmFkZENsYXNzKFwicG9rZS1zdGF0dXNcIik7XG5cdFx0dGhpcy5zZXRDb25uZWN0aW9uU3RhdGUoXCJkaXNjb25uZWN0ZWRcIik7XG5cblx0XHR0aGlzLnNldHRpbmdzVGFiID0gbmV3IFBva2VPYnNpZGlhblNldHRpbmdUYWIodGhpcy5hcHAsIHRoaXMpO1xuXHRcdHRoaXMuYWRkU2V0dGluZ1RhYih0aGlzLnNldHRpbmdzVGFiKTtcblxuXHRcdGlmICh0aGlzLnNldHRpbmdzLmNvbm5lY3Rpb25Ub2tlbikge1xuXHRcdFx0dGhpcy5jb25uZWN0KCk7XG5cdFx0fVxuXHR9XG5cblx0b251bmxvYWQoKTogdm9pZCB7XG5cdFx0dGhpcy51bmxvYWRSZXF1ZXN0ZWQgPSB0cnVlO1xuXHRcdHRoaXMuY2xlYXJSZWNvbm5lY3RUaW1lcigpO1xuXHRcdHRoaXMuY2xvc2VTb2NrZXQoKTtcblx0XHR0aGlzLnNldENvbm5lY3Rpb25TdGF0ZShcImRpc2Nvbm5lY3RlZFwiKTtcblx0fVxuXG5cdGFzeW5jIGxvYWRTZXR0aW5ncygpOiBQcm9taXNlPHZvaWQ+IHtcblx0XHR0aGlzLnNldHRpbmdzID0gT2JqZWN0LmFzc2lnbih7fSwgREVGQVVMVF9TRVRUSU5HUywgYXdhaXQgdGhpcy5sb2FkRGF0YSgpKTtcblx0fVxuXG5cdGFzeW5jIHNhdmVTZXR0aW5ncygpOiBQcm9taXNlPHZvaWQ+IHtcblx0XHRhd2FpdCB0aGlzLnNhdmVEYXRhKHRoaXMuc2V0dGluZ3MpO1xuXHR9XG5cblx0Z2V0Q29ubmVjdGlvblN0YXRlKCk6IENvbm5lY3Rpb25TdGF0ZSB7XG5cdFx0cmV0dXJuIHRoaXMuY29ubmVjdGlvblN0YXRlO1xuXHR9XG5cblx0YXN5bmMgdXBkYXRlQ29ubmVjdGlvblRva2VuKHRva2VuOiBzdHJpbmcpOiBQcm9taXNlPHZvaWQ+IHtcblx0XHR0aGlzLnNldHRpbmdzLmNvbm5lY3Rpb25Ub2tlbiA9IHRva2VuLnRyaW0oKTtcblx0XHRhd2FpdCB0aGlzLnNhdmVTZXR0aW5ncygpO1xuXHRcdHRoaXMucmVjb25uZWN0Tm93KCk7XG5cdH1cblxuXHRhc3luYyB1cGRhdGVHYXRld2F5VXJsKGdhdGV3YXlVcmw6IHN0cmluZyk6IFByb21pc2U8dm9pZD4ge1xuXHRcdHRoaXMuc2V0dGluZ3MuZ2F0ZXdheVVybCA9IGdhdGV3YXlVcmwudHJpbSgpIHx8IERFRkFVTFRfR0FURVdBWV9VUkw7XG5cdFx0YXdhaXQgdGhpcy5zYXZlU2V0dGluZ3MoKTtcblx0XHR0aGlzLnJlY29ubmVjdE5vdygpO1xuXHR9XG5cblx0YXN5bmMgdXBkYXRlQWxsb3dXcml0ZShhbGxvd1dyaXRlOiBib29sZWFuKTogUHJvbWlzZTx2b2lkPiB7XG5cdFx0dGhpcy5zZXR0aW5ncy5hbGxvd1dyaXRlID0gYWxsb3dXcml0ZTtcblx0XHRhd2FpdCB0aGlzLnNhdmVTZXR0aW5ncygpO1xuXHR9XG5cblx0cmVjb25uZWN0Tm93KCk6IHZvaWQge1xuXHRcdHRoaXMuY2xlYXJSZWNvbm5lY3RUaW1lcigpO1xuXHRcdHRoaXMucmVjb25uZWN0QXR0ZW1wdHMgPSAwO1xuXHRcdHRoaXMuY2xvc2VTb2NrZXQoKTtcblxuXHRcdGlmICghdGhpcy5zZXR0aW5ncy5jb25uZWN0aW9uVG9rZW4pIHtcblx0XHRcdHRoaXMuc2V0Q29ubmVjdGlvblN0YXRlKFwiZGlzY29ubmVjdGVkXCIpO1xuXHRcdFx0cmV0dXJuO1xuXHRcdH1cblxuXHRcdHRoaXMuY29ubmVjdCgpO1xuXHR9XG5cblx0cHJpdmF0ZSBjb25uZWN0KCk6IHZvaWQge1xuXHRcdGlmICh0aGlzLnVubG9hZFJlcXVlc3RlZCB8fCAhdGhpcy5zZXR0aW5ncy5jb25uZWN0aW9uVG9rZW4pIHtcblx0XHRcdHRoaXMuc2V0Q29ubmVjdGlvblN0YXRlKFwiZGlzY29ubmVjdGVkXCIpO1xuXHRcdFx0cmV0dXJuO1xuXHRcdH1cblxuXHRcdGlmIChcblx0XHRcdHRoaXMuc29ja2V0ICYmXG5cdFx0XHQodGhpcy5zb2NrZXQucmVhZHlTdGF0ZSA9PT0gV2ViU29ja2V0LkNPTk5FQ1RJTkcgfHwgdGhpcy5zb2NrZXQucmVhZHlTdGF0ZSA9PT0gV2ViU29ja2V0Lk9QRU4pXG5cdFx0KSB7XG5cdFx0XHRyZXR1cm47XG5cdFx0fVxuXG5cdFx0dGhpcy5zZXRDb25uZWN0aW9uU3RhdGUoXCJjb25uZWN0aW5nXCIpO1xuXG5cdFx0Y29uc3QgdXJsID0gdGhpcy5idWlsZEdhdGV3YXlVcmwoKTtcblxuXHRcdGlmICghdXJsKSB7XG5cdFx0XHR0aGlzLnNldENvbm5lY3Rpb25TdGF0ZShcImRpc2Nvbm5lY3RlZFwiKTtcblx0XHRcdHJldHVybjtcblx0XHR9XG5cblx0XHR0cnkge1xuXHRcdFx0Y29uc29sZS5sb2coYFBva2UgR2F0ZXdheSBjb25uZWN0aW5nIHRvICR7cmVkYWN0VG9rZW4odXJsKX1gKTtcblx0XHRcdHRoaXMuc29ja2V0ID0gbmV3IFdlYlNvY2tldCh1cmwudG9TdHJpbmcoKSk7XG5cdFx0fSBjYXRjaCAoZXJyb3IpIHtcblx0XHRcdHRoaXMuaGFuZGxlQ29ubmVjdGlvbkVycm9yKGVycm9yKTtcblx0XHRcdHRoaXMuc2NoZWR1bGVSZWNvbm5lY3QoKTtcblx0XHRcdHJldHVybjtcblx0XHR9XG5cblx0XHR0aGlzLnNvY2tldC5vbm9wZW4gPSAoKSA9PiB7XG5cdFx0XHRjb25zb2xlLmxvZyhcIlBva2UgR2F0ZXdheSBjb25uZWN0ZWRcIik7XG5cdFx0XHR0aGlzLnJlY29ubmVjdEF0dGVtcHRzID0gMDtcblx0XHRcdHRoaXMuc2V0Q29ubmVjdGlvblN0YXRlKFwiY29ubmVjdGVkXCIpO1xuXHRcdH07XG5cblx0XHR0aGlzLnNvY2tldC5vbm1lc3NhZ2UgPSAoZXZlbnQ6IE1lc3NhZ2VFdmVudDxzdHJpbmc+KSA9PiB7XG5cdFx0XHR2b2lkIHRoaXMud2l0aFJlcXVlc3RUaW1lb3V0KHRoaXMuaGFuZGxlU29ja2V0TWVzc2FnZShldmVudC5kYXRhKSwgbnVsbCk7XG5cdFx0fTtcblxuXHRcdHRoaXMuc29ja2V0Lm9uZXJyb3IgPSAoZXZlbnQpID0+IHtcblx0XHRcdHRoaXMuaGFuZGxlQ29ubmVjdGlvbkVycm9yKG5ldyBFcnJvcihgV2ViU29ja2V0IGNvbm5lY3Rpb24gZXJyb3I6ICR7SlNPTi5zdHJpbmdpZnkoZXZlbnQpfWApKTtcblx0XHR9O1xuXG5cdFx0dGhpcy5zb2NrZXQub25jbG9zZSA9IChldmVudCkgPT4ge1xuXHRcdFx0Y29uc29sZS5sb2coYFBva2UgR2F0ZXdheSBkaXNjb25uZWN0ZWQ6IGNvZGU9JHtldmVudC5jb2RlfSByZWFzb249JHtldmVudC5yZWFzb24gfHwgXCIobm9uZSlcIn1gKTtcblx0XHRcdHRoaXMuc29ja2V0ID0gbnVsbDtcblxuXHRcdFx0aWYgKHRoaXMudW5sb2FkUmVxdWVzdGVkKSB7XG5cdFx0XHRcdHRoaXMuc2V0Q29ubmVjdGlvblN0YXRlKFwiZGlzY29ubmVjdGVkXCIpO1xuXHRcdFx0XHRyZXR1cm47XG5cdFx0XHR9XG5cblx0XHRcdHRoaXMuc2V0Q29ubmVjdGlvblN0YXRlKFwiZGlzY29ubmVjdGVkXCIpO1xuXHRcdFx0dGhpcy5zY2hlZHVsZVJlY29ubmVjdCgpO1xuXHRcdH07XG5cdH1cblxuXHRwcml2YXRlIHNjaGVkdWxlUmVjb25uZWN0KCk6IHZvaWQge1xuXHRcdGlmICh0aGlzLnVubG9hZFJlcXVlc3RlZCB8fCAhdGhpcy5zZXR0aW5ncy5jb25uZWN0aW9uVG9rZW4gfHwgdGhpcy5yZWNvbm5lY3RUaW1lciAhPT0gbnVsbCkge1xuXHRcdFx0cmV0dXJuO1xuXHRcdH1cblxuXHRcdGNvbnN0IGRlbGF5ID0gTWF0aC5taW4oQkFTRV9SRUNPTk5FQ1RfREVMQVlfTVMgKiAyICoqIHRoaXMucmVjb25uZWN0QXR0ZW1wdHMsIE1BWF9SRUNPTk5FQ1RfREVMQVlfTVMpO1xuXHRcdHRoaXMucmVjb25uZWN0QXR0ZW1wdHMgKz0gMTtcblxuXHRcdHRoaXMucmVjb25uZWN0VGltZXIgPSB3aW5kb3cuc2V0VGltZW91dCgoKSA9PiB7XG5cdFx0XHR0aGlzLnJlY29ubmVjdFRpbWVyID0gbnVsbDtcblx0XHRcdHRoaXMuY29ubmVjdCgpO1xuXHRcdH0sIGRlbGF5KTtcblx0fVxuXG5cdHByaXZhdGUgY2xlYXJSZWNvbm5lY3RUaW1lcigpOiB2b2lkIHtcblx0XHRpZiAodGhpcy5yZWNvbm5lY3RUaW1lciAhPT0gbnVsbCkge1xuXHRcdFx0d2luZG93LmNsZWFyVGltZW91dCh0aGlzLnJlY29ubmVjdFRpbWVyKTtcblx0XHRcdHRoaXMucmVjb25uZWN0VGltZXIgPSBudWxsO1xuXHRcdH1cblx0fVxuXG5cdHByaXZhdGUgY2xvc2VTb2NrZXQoKTogdm9pZCB7XG5cdFx0aWYgKCF0aGlzLnNvY2tldCkge1xuXHRcdFx0cmV0dXJuO1xuXHRcdH1cblxuXHRcdHRoaXMuc29ja2V0Lm9ub3BlbiA9IG51bGw7XG5cdFx0dGhpcy5zb2NrZXQub25tZXNzYWdlID0gbnVsbDtcblx0XHR0aGlzLnNvY2tldC5vbmVycm9yID0gbnVsbDtcblx0XHR0aGlzLnNvY2tldC5vbmNsb3NlID0gbnVsbDtcblxuXHRcdGlmICh0aGlzLnNvY2tldC5yZWFkeVN0YXRlID09PSBXZWJTb2NrZXQuQ09OTkVDVElORyB8fCB0aGlzLnNvY2tldC5yZWFkeVN0YXRlID09PSBXZWJTb2NrZXQuT1BFTikge1xuXHRcdFx0dGhpcy5zb2NrZXQuY2xvc2UoKTtcblx0XHR9XG5cblx0XHR0aGlzLnNvY2tldCA9IG51bGw7XG5cdH1cblxuXHRwcml2YXRlIGFzeW5jIHdpdGhSZXF1ZXN0VGltZW91dCh0YXNrOiBQcm9taXNlPHZvaWQ+LCBpZDogc3RyaW5nIHwgbnVsbCk6IFByb21pc2U8dm9pZD4ge1xuXHRcdGxldCB0aW1lb3V0SWQ6IG51bWJlciB8IG51bGwgPSBudWxsO1xuXG5cdFx0Y29uc3QgdGltZW91dCA9IG5ldyBQcm9taXNlPHZvaWQ+KChfcmVzb2x2ZSwgcmVqZWN0KSA9PiB7XG5cdFx0XHR0aW1lb3V0SWQgPSB3aW5kb3cuc2V0VGltZW91dCgoKSA9PiByZWplY3QobmV3IEVycm9yKFwiUmVxdWVzdCB0aW1lZCBvdXRcIikpLCBSRVFVRVNUX1RJTUVPVVRfTVMpO1xuXHRcdH0pO1xuXG5cdFx0dHJ5IHtcblx0XHRcdGF3YWl0IFByb21pc2UucmFjZShbdGFzaywgdGltZW91dF0pO1xuXHRcdH0gY2F0Y2ggKGVycm9yKSB7XG5cdFx0XHR0aGlzLnNlbmRSZXNwb25zZSh7XG5cdFx0XHRcdGlkLFxuXHRcdFx0XHRzdGF0dXM6IFwiZXJyb3JcIixcblx0XHRcdFx0cGF5bG9hZDogeyBlcnJvcjogZXJyb3IgaW5zdGFuY2VvZiBFcnJvciA/IGVycm9yLm1lc3NhZ2UgOiBTdHJpbmcoZXJyb3IpIH0sXG5cdFx0XHR9KTtcblx0XHR9IGZpbmFsbHkge1xuXHRcdFx0aWYgKHRpbWVvdXRJZCAhPT0gbnVsbCkge1xuXHRcdFx0XHR3aW5kb3cuY2xlYXJUaW1lb3V0KHRpbWVvdXRJZCk7XG5cdFx0XHR9XG5cdFx0fVxuXHR9XG5cblx0cHJpdmF0ZSBhc3luYyBoYW5kbGVTb2NrZXRNZXNzYWdlKHJhd01lc3NhZ2U6IHN0cmluZyk6IFByb21pc2U8dm9pZD4ge1xuXHRcdGxldCBpbmNvbWluZzogSW5jb21pbmdNZXNzYWdlO1xuXG5cdFx0dHJ5IHtcblx0XHRcdGluY29taW5nID0gSlNPTi5wYXJzZShyYXdNZXNzYWdlKSBhcyBJbmNvbWluZ01lc3NhZ2U7XG5cdFx0fSBjYXRjaCB7XG5cdFx0XHR0aGlzLnNlbmRSZXNwb25zZSh7XG5cdFx0XHRcdGlkOiBudWxsLFxuXHRcdFx0XHRzdGF0dXM6IFwiZXJyb3JcIixcblx0XHRcdFx0cGF5bG9hZDogeyBlcnJvcjogXCJJbnZhbGlkIEpTT04gbWVzc2FnZVwiIH0sXG5cdFx0XHR9KTtcblx0XHRcdHJldHVybjtcblx0XHR9XG5cblx0XHRjb25zdCBpZCA9IHR5cGVvZiBpbmNvbWluZy5pZCA9PT0gXCJzdHJpbmdcIiA/IGluY29taW5nLmlkIDogbnVsbDtcblx0XHRjb25zdCBhY3Rpb24gPSB0eXBlb2YgaW5jb21pbmcuYWN0aW9uID09PSBcInN0cmluZ1wiID8gaW5jb21pbmcuYWN0aW9uIDogXCJcIjtcblx0XHRjb25zdCBwYXJhbXMgPSBpc1JlY29yZChpbmNvbWluZy5wYXJhbXMpID8gaW5jb21pbmcucGFyYW1zIDoge307XG5cblx0XHRpZiAoIWlkKSB7XG5cdFx0XHR0aGlzLnNlbmRSZXNwb25zZSh7XG5cdFx0XHRcdGlkLFxuXHRcdFx0XHRzdGF0dXM6IFwiZXJyb3JcIixcblx0XHRcdFx0cGF5bG9hZDogeyBlcnJvcjogXCJNZXNzYWdlIGlkIGlzIHJlcXVpcmVkXCIgfSxcblx0XHRcdH0pO1xuXHRcdFx0cmV0dXJuO1xuXHRcdH1cblxuXHRcdHRyeSB7XG5cdFx0XHRjb25zdCBwYXlsb2FkID0gYXdhaXQgdGhpcy5oYW5kbGVBY3Rpb24oYWN0aW9uLCBwYXJhbXMpO1xuXHRcdFx0dGhpcy5zZW5kUmVzcG9uc2UoeyBpZCwgc3RhdHVzOiBcInN1Y2Nlc3NcIiwgcGF5bG9hZCB9KTtcblx0XHR9IGNhdGNoIChlcnJvcikge1xuXHRcdFx0dGhpcy5zZW5kUmVzcG9uc2Uoe1xuXHRcdFx0XHRpZCxcblx0XHRcdFx0c3RhdHVzOiBcImVycm9yXCIsXG5cdFx0XHRcdHBheWxvYWQ6IHsgZXJyb3I6IGVycm9yIGluc3RhbmNlb2YgRXJyb3IgPyBlcnJvci5tZXNzYWdlIDogU3RyaW5nKGVycm9yKSB9LFxuXHRcdFx0fSk7XG5cdFx0fVxuXHR9XG5cblx0cHJpdmF0ZSBhc3luYyBoYW5kbGVBY3Rpb24oYWN0aW9uOiBzdHJpbmcsIHBhcmFtczogUmVjb3JkPHN0cmluZywgdW5rbm93bj4pOiBQcm9taXNlPFJlY29yZDxzdHJpbmcsIHVua25vd24+PiB7XG5cdFx0c3dpdGNoIChhY3Rpb24pIHtcblx0XHRcdGNhc2UgXCJsaXN0XCI6XG5cdFx0XHRjYXNlIFwibGlzdF9maWxlc1wiOlxuXHRcdFx0XHRyZXR1cm4gdGhpcy5saXN0RmlsZXMoKTtcblx0XHRcdGNhc2UgXCJyZWFkXCI6XG5cdFx0XHRjYXNlIFwicmVhZF9maWxlXCI6XG5cdFx0XHRcdHJldHVybiB0aGlzLnJlYWRGaWxlKHBhcmFtcyk7XG5cdFx0XHRjYXNlIFwid3JpdGVcIjpcblx0XHRcdGNhc2UgXCJ3cml0ZV9maWxlXCI6XG5cdFx0XHRcdHJldHVybiB0aGlzLndyaXRlRmlsZShwYXJhbXMpO1xuXHRcdFx0Y2FzZSBcInNlYXJjaFwiOlxuXHRcdFx0Y2FzZSBcInNlYXJjaF92YXVsdFwiOlxuXHRcdFx0XHRyZXR1cm4gdGhpcy5zZWFyY2hWYXVsdChwYXJhbXMpO1xuXHRcdFx0ZGVmYXVsdDpcblx0XHRcdFx0dGhyb3cgbmV3IEVycm9yKGBVbnN1cHBvcnRlZCBhY3Rpb246ICR7YWN0aW9uIHx8IFwiKG1pc3NpbmcpXCJ9YCk7XG5cdFx0fVxuXHR9XG5cblx0cHJpdmF0ZSBsaXN0RmlsZXMoKTogUmVjb3JkPHN0cmluZywgdW5rbm93bj4ge1xuXHRcdHJldHVybiB7XG5cdFx0XHRmaWxlczogdGhpcy5hcHAudmF1bHQuZ2V0TWFya2Rvd25GaWxlcygpLm1hcCgoZmlsZSkgPT4gZmlsZS5wYXRoKSxcblx0XHR9O1xuXHR9XG5cblx0cHJpdmF0ZSBhc3luYyByZWFkRmlsZShwYXJhbXM6IFJlY29yZDxzdHJpbmcsIHVua25vd24+KTogUHJvbWlzZTxSZWNvcmQ8c3RyaW5nLCB1bmtub3duPj4ge1xuXHRcdGNvbnN0IHBhdGggPSBnZXRSZXF1aXJlZFN0cmluZyhwYXJhbXMsIFwicGF0aFwiKTtcblx0XHRjb25zdCBmaWxlID0gdGhpcy5nZXRNYXJrZG93bkZpbGUocGF0aCk7XG5cdFx0Y29uc3QgY29udGVudCA9IGF3YWl0IHRoaXMuYXBwLnZhdWx0LmFkYXB0ZXIucmVhZChmaWxlLnBhdGgpO1xuXG5cdFx0cmV0dXJuIHtcblx0XHRcdHBhdGg6IGZpbGUucGF0aCxcblx0XHRcdGNvbnRlbnQsXG5cdFx0fTtcblx0fVxuXG5cdHByaXZhdGUgYXN5bmMgd3JpdGVGaWxlKHBhcmFtczogUmVjb3JkPHN0cmluZywgdW5rbm93bj4pOiBQcm9taXNlPFJlY29yZDxzdHJpbmcsIHVua25vd24+PiB7XG5cdFx0aWYgKCF0aGlzLnNldHRpbmdzLmFsbG93V3JpdGUpIHtcblx0XHRcdHRocm93IG5ldyBFcnJvcihcIldyaXRlIGFjY2VzcyBpcyBkaXNhYmxlZCBpbiBQb2tlIEdhdGV3YXkgc2V0dGluZ3NcIik7XG5cdFx0fVxuXG5cdFx0Y29uc3QgcGF0aCA9IGdldFJlcXVpcmVkU3RyaW5nKHBhcmFtcywgXCJwYXRoXCIpO1xuXHRcdGNvbnN0IGNvbnRlbnQgPSBnZXRSZXF1aXJlZFN0cmluZyhwYXJhbXMsIFwiY29udGVudFwiKTtcblx0XHRjb25zdCBub3JtYWxpemVkUGF0aCA9IHRoaXMubm9ybWFsaXplTWFya2Rvd25QYXRoKHBhdGgpO1xuXG5cdFx0YXdhaXQgdGhpcy5lbnN1cmVQYXJlbnRGb2xkZXJzKG5vcm1hbGl6ZWRQYXRoKTtcblx0XHRhd2FpdCB0aGlzLmFwcC52YXVsdC5hZGFwdGVyLndyaXRlKG5vcm1hbGl6ZWRQYXRoLCBjb250ZW50KTtcblxuXHRcdHJldHVybiB7XG5cdFx0XHRwYXRoOiBub3JtYWxpemVkUGF0aCxcblx0XHRcdGJ5dGVzOiBuZXcgVGV4dEVuY29kZXIoKS5lbmNvZGUoY29udGVudCkubGVuZ3RoLFxuXHRcdH07XG5cdH1cblxuXHRwcml2YXRlIGFzeW5jIHNlYXJjaFZhdWx0KHBhcmFtczogUmVjb3JkPHN0cmluZywgdW5rbm93bj4pOiBQcm9taXNlPFJlY29yZDxzdHJpbmcsIHVua25vd24+PiB7XG5cdFx0Y29uc3QgcXVlcnkgPSBnZXRSZXF1aXJlZFN0cmluZyhwYXJhbXMsIFwicXVlcnlcIikudHJpbSgpO1xuXG5cdFx0aWYgKCFxdWVyeSkge1xuXHRcdFx0dGhyb3cgbmV3IEVycm9yKFwiU2VhcmNoIHF1ZXJ5IGNhbm5vdCBiZSBlbXB0eVwiKTtcblx0XHR9XG5cblx0XHRjb25zdCBub3JtYWxpemVkUXVlcnkgPSBxdWVyeS50b0xvY2FsZUxvd2VyQ2FzZSgpO1xuXHRcdGNvbnN0IG1hdGNoZXM6IFNlYXJjaE1hdGNoW10gPSBbXTtcblxuXHRcdGZvciAoY29uc3QgZmlsZSBvZiB0aGlzLmFwcC52YXVsdC5nZXRNYXJrZG93bkZpbGVzKCkpIHtcblx0XHRcdGNvbnN0IGNvbnRlbnQgPSBhd2FpdCB0aGlzLmFwcC52YXVsdC5hZGFwdGVyLnJlYWQoZmlsZS5wYXRoKTtcblx0XHRcdGNvbnN0IGluZGV4ID0gY29udGVudC50b0xvY2FsZUxvd2VyQ2FzZSgpLmluZGV4T2Yobm9ybWFsaXplZFF1ZXJ5KTtcblxuXHRcdFx0aWYgKGluZGV4ID09PSAtMSkge1xuXHRcdFx0XHRjb250aW51ZTtcblx0XHRcdH1cblxuXHRcdFx0bWF0Y2hlcy5wdXNoKHtcblx0XHRcdFx0cGF0aDogZmlsZS5wYXRoLFxuXHRcdFx0XHRzbmlwcGV0OiBtYWtlU25pcHBldChjb250ZW50LCBpbmRleCwgcXVlcnkubGVuZ3RoKSxcblx0XHRcdH0pO1xuXHRcdH1cblxuXHRcdHJldHVybiB7IG1hdGNoZXMgfTtcblx0fVxuXG5cdHByaXZhdGUgZ2V0TWFya2Rvd25GaWxlKHBhdGg6IHN0cmluZyk6IFRGaWxlIHtcblx0XHRjb25zdCBub3JtYWxpemVkUGF0aCA9IHRoaXMubm9ybWFsaXplTWFya2Rvd25QYXRoKHBhdGgpO1xuXHRcdGNvbnN0IGZpbGUgPSB0aGlzLmFwcC52YXVsdC5nZXRBYnN0cmFjdEZpbGVCeVBhdGgobm9ybWFsaXplZFBhdGgpO1xuXG5cdFx0aWYgKCEoZmlsZSBpbnN0YW5jZW9mIFRGaWxlKSkge1xuXHRcdFx0dGhyb3cgbmV3IEVycm9yKGBGaWxlIG5vdCBmb3VuZDogJHtub3JtYWxpemVkUGF0aH1gKTtcblx0XHR9XG5cblx0XHRpZiAoZmlsZS5leHRlbnNpb24gIT09IFwibWRcIikge1xuXHRcdFx0dGhyb3cgbmV3IEVycm9yKGBPbmx5IG1hcmtkb3duIGZpbGVzIGFyZSBzdXBwb3J0ZWQ6ICR7bm9ybWFsaXplZFBhdGh9YCk7XG5cdFx0fVxuXG5cdFx0cmV0dXJuIGZpbGU7XG5cdH1cblxuXHRwcml2YXRlIG5vcm1hbGl6ZU1hcmtkb3duUGF0aChwYXRoOiBzdHJpbmcpOiBzdHJpbmcge1xuXHRcdGNvbnN0IG5vcm1hbGl6ZWRQYXRoID0gcGF0aC50cmltKCkucmVwbGFjZSgvXlxcLysvLCBcIlwiKTtcblxuXHRcdGlmICghbm9ybWFsaXplZFBhdGgpIHtcblx0XHRcdHRocm93IG5ldyBFcnJvcihcIlBhdGggY2Fubm90IGJlIGVtcHR5XCIpO1xuXHRcdH1cblxuXHRcdGNvbnN0IHBhcnRzID0gbm9ybWFsaXplZFBhdGguc3BsaXQoXCIvXCIpLmZpbHRlcihCb29sZWFuKTtcblxuXHRcdGlmIChwYXJ0cy5pbmNsdWRlcyhcIi4uXCIpIHx8IHBhcnRzLnNvbWUoKHBhcnQpID0+IHBhcnQgPT09IFwiLlwiKSkge1xuXHRcdFx0dGhyb3cgbmV3IEVycm9yKFwiUGF0aCBjYW5ub3QgY29udGFpbiBwYXJlbnQgb3IgY3VycmVudC1kaXJlY3Rvcnkgc2VnbWVudHNcIik7XG5cdFx0fVxuXG5cdFx0aWYgKCFub3JtYWxpemVkUGF0aC50b0xvY2FsZUxvd2VyQ2FzZSgpLmVuZHNXaXRoKFwiLm1kXCIpKSB7XG5cdFx0XHR0aHJvdyBuZXcgRXJyb3IoXCJPbmx5IG1hcmtkb3duIGZpbGUgcGF0aHMgZW5kaW5nIGluIC5tZCBhcmUgc3VwcG9ydGVkXCIpO1xuXHRcdH1cblxuXHRcdHJldHVybiBwYXJ0cy5qb2luKFwiL1wiKTtcblx0fVxuXG5cdHByaXZhdGUgYXN5bmMgZW5zdXJlUGFyZW50Rm9sZGVycyhmaWxlUGF0aDogc3RyaW5nKTogUHJvbWlzZTx2b2lkPiB7XG5cdFx0Y29uc3QgcGFyZW50UGFydHMgPSBmaWxlUGF0aC5zcGxpdChcIi9cIikuc2xpY2UoMCwgLTEpO1xuXHRcdGxldCBjdXJyZW50UGF0aCA9IFwiXCI7XG5cblx0XHRmb3IgKGNvbnN0IHBhcnQgb2YgcGFyZW50UGFydHMpIHtcblx0XHRcdGN1cnJlbnRQYXRoID0gY3VycmVudFBhdGggPyBgJHtjdXJyZW50UGF0aH0vJHtwYXJ0fWAgOiBwYXJ0O1xuXG5cdFx0XHRpZiAoIShhd2FpdCB0aGlzLmFwcC52YXVsdC5hZGFwdGVyLmV4aXN0cyhjdXJyZW50UGF0aCkpKSB7XG5cdFx0XHRcdGF3YWl0IHRoaXMuYXBwLnZhdWx0LmFkYXB0ZXIubWtkaXIoY3VycmVudFBhdGgpO1xuXHRcdFx0fVxuXHRcdH1cblx0fVxuXG5cdHByaXZhdGUgYnVpbGRHYXRld2F5VXJsKCk6IFVSTCB8IG51bGwge1xuXHRcdGxldCB1cmw6IFVSTDtcblxuXHRcdHRyeSB7XG5cdFx0XHR1cmwgPSBuZXcgVVJMKHRoaXMuc2V0dGluZ3MuZ2F0ZXdheVVybCB8fCBERUZBVUxUX0dBVEVXQVlfVVJMKTtcblx0XHR9IGNhdGNoIHtcblx0XHRcdG5ldyBOb3RpY2UoXCJJbnZhbGlkIFBva2UgR2F0ZXdheSBVUkxcIik7XG5cdFx0XHRyZXR1cm4gbnVsbDtcblx0XHR9XG5cblx0XHRpZiAodXJsLnByb3RvY29sICE9PSBcIndzOlwiICYmIHVybC5wcm90b2NvbCAhPT0gXCJ3c3M6XCIpIHtcblx0XHRcdG5ldyBOb3RpY2UoXCJQb2tlIEdhdGV3YXkgVVJMIG11c3Qgc3RhcnQgd2l0aCB3czovLyBvciB3c3M6Ly9cIik7XG5cdFx0XHRyZXR1cm4gbnVsbDtcblx0XHR9XG5cblx0XHR1cmwuc2VhcmNoUGFyYW1zLnNldChcInRva2VuXCIsIHRoaXMuc2V0dGluZ3MuY29ubmVjdGlvblRva2VuKTtcblx0XHR1cmwuc2VhcmNoUGFyYW1zLnNldChcInBsdWdpblwiLCB0aGlzLm1hbmlmZXN0LmlkKTtcblx0XHR1cmwuc2VhcmNoUGFyYW1zLnNldChcInZlcnNpb25cIiwgdGhpcy5tYW5pZmVzdC52ZXJzaW9uKTtcblxuXHRcdHJldHVybiB1cmw7XG5cdH1cblxuXHRwcml2YXRlIHNlbmRSZXNwb25zZShyZXNwb25zZTogUmVzcG9uc2VNZXNzYWdlKTogdm9pZCB7XG5cdFx0aWYgKCF0aGlzLnNvY2tldCB8fCB0aGlzLnNvY2tldC5yZWFkeVN0YXRlICE9PSBXZWJTb2NrZXQuT1BFTikge1xuXHRcdFx0cmV0dXJuO1xuXHRcdH1cblxuXHRcdHRoaXMuc29ja2V0LnNlbmQoSlNPTi5zdHJpbmdpZnkocmVzcG9uc2UpKTtcblx0fVxuXG5cdHByaXZhdGUgc2V0Q29ubmVjdGlvblN0YXRlKHN0YXRlOiBDb25uZWN0aW9uU3RhdGUpOiB2b2lkIHtcblx0XHR0aGlzLmNvbm5lY3Rpb25TdGF0ZSA9IHN0YXRlO1xuXG5cdFx0aWYgKHRoaXMuc3RhdHVzQmFySXRlbUVsKSB7XG5cdFx0XHR0aGlzLnN0YXR1c0Jhckl0ZW1FbC5zZXRUZXh0KGBQb2tlOiAke2NhcGl0YWxpemUoc3RhdGUpfWApO1xuXHRcdFx0dGhpcy5zdGF0dXNCYXJJdGVtRWwucmVtb3ZlQ2xhc3MoXCJpcy1jb25uZWN0ZWRcIiwgXCJpcy1jb25uZWN0aW5nXCIsIFwiaXMtZGlzY29ubmVjdGVkXCIpO1xuXHRcdFx0dGhpcy5zdGF0dXNCYXJJdGVtRWwuYWRkQ2xhc3MoYGlzLSR7c3RhdGV9YCk7XG5cdFx0fVxuXG5cdFx0dGhpcy5zZXR0aW5nc1RhYj8udXBkYXRlU3RhdHVzKHN0YXRlKTtcblx0fVxuXG5cdHByaXZhdGUgaGFuZGxlQ29ubmVjdGlvbkVycm9yKGVycm9yOiB1bmtub3duKTogdm9pZCB7XG5cdFx0Y29uc29sZS5lcnJvcihcIlBva2UgR2F0ZXdheSBjb25uZWN0aW9uIGVycm9yXCIsIGVycm9yKTtcblx0fVxufVxuXG5jbGFzcyBQb2tlT2JzaWRpYW5TZXR0aW5nVGFiIGV4dGVuZHMgUGx1Z2luU2V0dGluZ1RhYiB7XG5cdHByaXZhdGUgcGx1Z2luOiBQb2tlT2JzaWRpYW5QbHVnaW47XG5cdHByaXZhdGUgc3RhdHVzRWw6IEhUTUxFbGVtZW50IHwgbnVsbCA9IG51bGw7XG5cblx0Y29uc3RydWN0b3IoYXBwOiBBcHAsIHBsdWdpbjogUG9rZU9ic2lkaWFuUGx1Z2luKSB7XG5cdFx0c3VwZXIoYXBwLCBwbHVnaW4pO1xuXHRcdHRoaXMucGx1Z2luID0gcGx1Z2luO1xuXHR9XG5cblx0ZGlzcGxheSgpOiB2b2lkIHtcblx0XHRjb25zdCB7IGNvbnRhaW5lckVsIH0gPSB0aGlzO1xuXHRcdGNvbnRhaW5lckVsLmVtcHR5KCk7XG5cblx0XHRjb250YWluZXJFbC5jcmVhdGVFbChcImgyXCIsIHsgdGV4dDogXCJQb2tlIEdhdGV3YXlcIiB9KTtcblxuXHRcdG5ldyBTZXR0aW5nKGNvbnRhaW5lckVsKVxuXHRcdFx0LnNldE5hbWUoXCJHYXRld2F5IFVSTFwiKVxuXHRcdFx0LnNldERlc2MoXCJXZWJTb2NrZXQgZW5kcG9pbnQgdXNlZCB0byBjb25uZWN0IHRoaXMgdmF1bHQgdG8gUG9rZS5cIilcblx0XHRcdC5hZGRUZXh0KCh0ZXh0KSA9PiB7XG5cdFx0XHRcdHRleHRcblx0XHRcdFx0XHQuc2V0UGxhY2Vob2xkZXIoREVGQVVMVF9HQVRFV0FZX1VSTClcblx0XHRcdFx0XHQuc2V0VmFsdWUodGhpcy5wbHVnaW4uc2V0dGluZ3MuZ2F0ZXdheVVybCB8fCBERUZBVUxUX0dBVEVXQVlfVVJMKVxuXHRcdFx0XHRcdC5vbkNoYW5nZShhc3luYyAodmFsdWUpID0+IHtcblx0XHRcdFx0XHRcdGF3YWl0IHRoaXMucGx1Z2luLnVwZGF0ZUdhdGV3YXlVcmwodmFsdWUpO1xuXHRcdFx0XHRcdH0pO1xuXHRcdFx0fSk7XG5cblx0XHRuZXcgU2V0dGluZyhjb250YWluZXJFbClcblx0XHRcdC5zZXROYW1lKFwiQ29ubmVjdGlvbiB0b2tlblwiKVxuXHRcdFx0LnNldERlc2MoXCJQYWlycyB0aGlzIHZhdWx0IHdpdGggUG9rZS5cIilcblx0XHRcdC5hZGRUZXh0KCh0ZXh0KSA9PiB7XG5cdFx0XHRcdHRleHRcblx0XHRcdFx0XHQuc2V0UGxhY2Vob2xkZXIoXCJQYXN0ZSB0b2tlblwiKVxuXHRcdFx0XHRcdC5zZXRWYWx1ZSh0aGlzLnBsdWdpbi5zZXR0aW5ncy5jb25uZWN0aW9uVG9rZW4pXG5cdFx0XHRcdFx0Lm9uQ2hhbmdlKGFzeW5jICh2YWx1ZSkgPT4ge1xuXHRcdFx0XHRcdFx0YXdhaXQgdGhpcy5wbHVnaW4udXBkYXRlQ29ubmVjdGlvblRva2VuKHZhbHVlKTtcblx0XHRcdFx0XHR9KTtcblxuXHRcdFx0XHR0ZXh0LmlucHV0RWwudHlwZSA9IFwicGFzc3dvcmRcIjtcblx0XHRcdH0pO1xuXG5cdFx0bmV3IFNldHRpbmcoY29udGFpbmVyRWwpXG5cdFx0XHQuc2V0TmFtZShcIkFsbG93IHdyaXRlc1wiKVxuXHRcdFx0LnNldERlc2MoXCJBbGxvdyBQb2tlIHRvIGNyZWF0ZSBvciBvdmVyd3JpdGUgbWFya2Rvd24gZmlsZXMgaW4gdGhpcyB2YXVsdC5cIilcblx0XHRcdC5hZGRUb2dnbGUoKHRvZ2dsZSkgPT4ge1xuXHRcdFx0XHR0b2dnbGVcblx0XHRcdFx0XHQuc2V0VmFsdWUodGhpcy5wbHVnaW4uc2V0dGluZ3MuYWxsb3dXcml0ZSlcblx0XHRcdFx0XHQub25DaGFuZ2UoYXN5bmMgKHZhbHVlKSA9PiB7XG5cdFx0XHRcdFx0XHRhd2FpdCB0aGlzLnBsdWdpbi51cGRhdGVBbGxvd1dyaXRlKHZhbHVlKTtcblx0XHRcdFx0XHR9KTtcblx0XHRcdH0pO1xuXG5cdFx0Y29uc3Qgc3RhdHVzU2V0dGluZyA9IG5ldyBTZXR0aW5nKGNvbnRhaW5lckVsKVxuXHRcdFx0LnNldE5hbWUoXCJDb25uZWN0aW9uIHN0YXR1c1wiKVxuXHRcdFx0LnNldERlc2MoXCJDdXJyZW50IGdhdGV3YXkgY29ubmVjdGlvbiBzdGF0ZS5cIilcblx0XHRcdC5hZGRFeHRyYUJ1dHRvbigoYnV0dG9uKSA9PiB7XG5cdFx0XHRcdGJ1dHRvblxuXHRcdFx0XHRcdC5zZXRJY29uKFwicmVmcmVzaC1jd1wiKVxuXHRcdFx0XHRcdC5zZXRUb29sdGlwKFwiUmVjb25uZWN0XCIpXG5cdFx0XHRcdFx0Lm9uQ2xpY2soKCkgPT4ge1xuXHRcdFx0XHRcdFx0dGhpcy5wbHVnaW4ucmVjb25uZWN0Tm93KCk7XG5cdFx0XHRcdFx0XHRuZXcgTm90aWNlKFwiUmVjb25uZWN0aW5nIFBva2UgR2F0ZXdheVwiKTtcblx0XHRcdFx0XHR9KTtcblx0XHRcdH0pO1xuXG5cdFx0dGhpcy5zdGF0dXNFbCA9IHN0YXR1c1NldHRpbmcuY29udHJvbEVsLmNyZWF0ZVNwYW4oKTtcblx0XHR0aGlzLnN0YXR1c0VsLmFkZENsYXNzKFwicG9rZS1zdGF0dXNcIik7XG5cdFx0dGhpcy51cGRhdGVTdGF0dXModGhpcy5wbHVnaW4uZ2V0Q29ubmVjdGlvblN0YXRlKCkpO1xuXHR9XG5cblx0dXBkYXRlU3RhdHVzKHN0YXRlOiBDb25uZWN0aW9uU3RhdGUpOiB2b2lkIHtcblx0XHRpZiAoIXRoaXMuc3RhdHVzRWwpIHtcblx0XHRcdHJldHVybjtcblx0XHR9XG5cblx0XHR0aGlzLnN0YXR1c0VsLnNldFRleHQoY2FwaXRhbGl6ZShzdGF0ZSkpO1xuXHRcdHRoaXMuc3RhdHVzRWwucmVtb3ZlQ2xhc3MoXCJpcy1jb25uZWN0ZWRcIiwgXCJpcy1jb25uZWN0aW5nXCIsIFwiaXMtZGlzY29ubmVjdGVkXCIpO1xuXHRcdHRoaXMuc3RhdHVzRWwuYWRkQ2xhc3MoYGlzLSR7c3RhdGV9YCk7XG5cdH1cbn1cblxuZnVuY3Rpb24gZ2V0UmVxdWlyZWRTdHJpbmcocGFyYW1zOiBSZWNvcmQ8c3RyaW5nLCB1bmtub3duPiwga2V5OiBzdHJpbmcpOiBzdHJpbmcge1xuXHRjb25zdCB2YWx1ZSA9IHBhcmFtc1trZXldO1xuXG5cdGlmICh0eXBlb2YgdmFsdWUgIT09IFwic3RyaW5nXCIpIHtcblx0XHR0aHJvdyBuZXcgRXJyb3IoYE1pc3NpbmcgcmVxdWlyZWQgc3RyaW5nIHBhcmFtOiAke2tleX1gKTtcblx0fVxuXG5cdHJldHVybiB2YWx1ZTtcbn1cblxuZnVuY3Rpb24gaXNSZWNvcmQodmFsdWU6IHVua25vd24pOiB2YWx1ZSBpcyBSZWNvcmQ8c3RyaW5nLCB1bmtub3duPiB7XG5cdHJldHVybiB0eXBlb2YgdmFsdWUgPT09IFwib2JqZWN0XCIgJiYgdmFsdWUgIT09IG51bGwgJiYgIUFycmF5LmlzQXJyYXkodmFsdWUpO1xufVxuXG5mdW5jdGlvbiBtYWtlU25pcHBldChjb250ZW50OiBzdHJpbmcsIGluZGV4OiBudW1iZXIsIG1hdGNoTGVuZ3RoOiBudW1iZXIpOiBzdHJpbmcge1xuXHRjb25zdCBoYWxmV2luZG93ID0gTWF0aC5mbG9vcigoTUFYX1NOSVBQRVRfTEVOR1RIIC0gbWF0Y2hMZW5ndGgpIC8gMik7XG5cdGNvbnN0IHN0YXJ0ID0gTWF0aC5tYXgoMCwgaW5kZXggLSBoYWxmV2luZG93KTtcblx0Y29uc3QgZW5kID0gTWF0aC5taW4oY29udGVudC5sZW5ndGgsIGluZGV4ICsgbWF0Y2hMZW5ndGggKyBoYWxmV2luZG93KTtcblx0Y29uc3QgcHJlZml4ID0gc3RhcnQgPiAwID8gXCIuLi5cIiA6IFwiXCI7XG5cdGNvbnN0IHN1ZmZpeCA9IGVuZCA8IGNvbnRlbnQubGVuZ3RoID8gXCIuLi5cIiA6IFwiXCI7XG5cblx0cmV0dXJuIGAke3ByZWZpeH0ke2NvbnRlbnQuc2xpY2Uoc3RhcnQsIGVuZCkucmVwbGFjZSgvXFxzKy9nLCBcIiBcIikudHJpbSgpfSR7c3VmZml4fWA7XG59XG5cbmZ1bmN0aW9uIGNhcGl0YWxpemUodmFsdWU6IHN0cmluZyk6IHN0cmluZyB7XG5cdHJldHVybiB2YWx1ZS5jaGFyQXQoMCkudG9Mb2NhbGVVcHBlckNhc2UoKSArIHZhbHVlLnNsaWNlKDEpO1xufVxuXG5mdW5jdGlvbiByZWRhY3RUb2tlbih1cmw6IFVSTCk6IHN0cmluZyB7XG5cdGNvbnN0IGNvcHkgPSBuZXcgVVJMKHVybC50b1N0cmluZygpKTtcblxuXHRpZiAoY29weS5zZWFyY2hQYXJhbXMuaGFzKFwidG9rZW5cIikpIHtcblx0XHRjb3B5LnNlYXJjaFBhcmFtcy5zZXQoXCJ0b2tlblwiLCBcIioqKlwiKTtcblx0fVxuXG5cdHJldHVybiBjb3B5LnRvU3RyaW5nKCk7XG59XG4iXX0=