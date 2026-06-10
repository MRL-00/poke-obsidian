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
        new obsidian_1.Setting(containerEl).setName("Poke Gateway").setHeading();
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
                const token = generateConnectionToken();
                await this.plugin.updateConnectionToken(token);
                if (tokenInput) {
                    tokenInput.value = token;
                }
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
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoibWFpbi5qcyIsInNvdXJjZVJvb3QiOiIiLCJzb3VyY2VzIjpbIm1haW4udHMiXSwibmFtZXMiOltdLCJtYXBwaW5ncyI6Ijs7QUFBQSx1Q0FBaUY7QUFFakYsTUFBTSxtQkFBbUIsR0FBRywwQ0FBMEMsQ0FBQztBQUN2RSxNQUFNLG9CQUFvQixHQUFHLE1BQU0sQ0FBQztBQUNwQyxNQUFNLHVCQUF1QixHQUFHLElBQUssQ0FBQztBQUN0QyxNQUFNLHNCQUFzQixHQUFHLEtBQU0sQ0FBQztBQUN0QyxNQUFNLGtCQUFrQixHQUFHLEtBQU0sQ0FBQztBQUNsQyxNQUFNLGtCQUFrQixHQUFHLEdBQUcsQ0FBQztBQUMvQixNQUFNLHNCQUFzQixHQUFHLGNBQWMsQ0FBQztBQTRCOUMsTUFBTSxnQkFBZ0IsR0FBeUI7SUFDOUMsVUFBVSxFQUFFLG1CQUFtQjtJQUMvQixlQUFlLEVBQUUsRUFBRTtJQUNuQixXQUFXLEVBQUUsb0JBQW9CO0lBQ2pDLFVBQVUsRUFBRSxLQUFLO0NBQ2pCLENBQUM7QUFFRixNQUFxQixrQkFBbUIsU0FBUSxpQkFBTTtJQUF0RDs7UUFDQyxhQUFRLEdBQXlCLGdCQUFnQixDQUFDO1FBQzFDLFdBQU0sR0FBcUIsSUFBSSxDQUFDO1FBQ2hDLG9CQUFlLEdBQXVCLElBQUksQ0FBQztRQUMzQyxnQkFBVyxHQUFrQyxJQUFJLENBQUM7UUFDbEQsb0JBQWUsR0FBb0IsY0FBYyxDQUFDO1FBQ2xELHNCQUFpQixHQUFHLENBQUMsQ0FBQztRQUN0QixtQkFBYyxHQUFrQixJQUFJLENBQUM7UUFDckMsb0JBQWUsR0FBRyxLQUFLLENBQUM7SUEyY2pDLENBQUM7SUF6Y0EsS0FBSyxDQUFDLE1BQU07UUFDWCxNQUFNLElBQUksQ0FBQyxZQUFZLEVBQUUsQ0FBQztRQUUxQixJQUFJLENBQUMsSUFBSSxDQUFDLFFBQVEsQ0FBQyxlQUFlLEVBQUUsQ0FBQztZQUNwQyxJQUFJLENBQUMsUUFBUSxDQUFDLGVBQWUsR0FBRyx1QkFBdUIsRUFBRSxDQUFDO1lBQzFELE1B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LENBQUMsS0FBSyxDQUFDLEVBQUUsQ0FBQztBQUN6RCxDQUFDO0FBRUQsU0FBUyxXQUFXLENBQUMsS0FBaUI7SUFDckMsSUFBSSxNQUFNLEdBQUcsRUFBRSxDQUFDO0lBRWhCLEtBQUssTUFBTSxJQUFJLElBQUksS0FBSyxFQUFFLENBQUM7UUFDMUIsTUFBTSxJQUFJLE1BQU0sQ0FBQyxZQUFZLENBQUMsSUFBSSxDQUFDLENBQUM7SUFDckMsQ0FBQztJQUVELE9BQU8sSUFBSSxDQUFDLE1BQU0sQ0FBQyxDQUFDLE9BQU8sQ0FBQyxLQUFLLEVBQUUsR0FBRyxDQUFDLENBQUMsT0FBTyxDQUFDLEtBQUssRUFBRSxHQUFHLENBQUMsQ0FBQyxPQUFPLENBQUMsTUFBTSxFQUFFLEVBQUUsQ0FBQyxDQUFDO0FBQ2pGLENBQUM7QUFFRCxTQUFTLFdBQVcsQ0FBQyxHQUFRO0lBQzVCLE1BQU0sSUFBSSxHQUFHLElBQUksR0FBRyxDQUFDLEdBQUcsQ0FBQyxRQUFRLEVBQUUsQ0FBQyxDQUFDO0lBRXJDLElBQUksSUFBSSxDQUFDLFlBQVksQ0FBQyxHQUFHLENBQUMsT0FBTyxDQUFDLEVBQUUsQ0FBQztRQUNwQyxJQUFJLENBQUMsWUFBWSxDQUFDLEdBQUcsQ0FBQyxPQUFPLEVBQUUsS0FBSyxDQUFDLENBQUM7SUFDdkMsQ0FBQztJQUVELE9BQU8sSUFBSSxDQUFDLFFBQVEsRUFBRSxDQUFDO0FBQ3hCLENBQUMiLCJzb3VyY2VzQ29udGVudCI6WyJpbXBvcnQgeyBBcHAsIE5vdGljZSwgUGx1Z2luLCBQbHVnaW5TZXR0aW5nVGFiLCBTZXR0aW5nLCBURmlsZSB9IGZyb20gXCJvYnNpZGlhblwiO1xuXG5jb25zdCBERUZBVUxUX0dBVEVXQVlfVVJMID0gXCJ3c3M6Ly9vYnNpZGlhbi5tYXR0LW56LmNvbS9vYnNpZGlhbi9zeW5jXCI7XG5jb25zdCBERUZBVUxUX1ZBVUxUX0ZPTERFUiA9IFwiUG9rZVwiO1xuY29uc3QgQkFTRV9SRUNPTk5FQ1RfREVMQVlfTVMgPSAxXzAwMDtcbmNvbnN0IE1BWF9SRUNPTk5FQ1RfREVMQVlfTVMgPSAzMF8wMDA7XG5jb25zdCBSRVFVRVNUX1RJTUVPVVRfTVMgPSAzMF8wMDA7XG5jb25zdCBNQVhfU05JUFBFVF9MRU5HVEggPSAxODA7XG5jb25zdCBHRU5FUkFURURfVE9LRU5fUFJFRklYID0gXCJwa29ic192YXVsdF9cIjtcblxudHlwZSBDb25uZWN0aW9uU3RhdGUgPSBcImNvbm5lY3RlZFwiIHwgXCJjb25uZWN0aW5nXCIgfCBcImRpc2Nvbm5lY3RlZFwiO1xuXG5pbnRlcmZhY2UgUG9rZU9ic2lkaWFuU2V0dGluZ3Mge1xuXHRnYXRld2F5VXJsOiBzdHJpbmc7XG5cdGNvbm5lY3Rpb25Ub2tlbjogc3RyaW5nO1xuXHR2YXVsdEZvbGRlcjogc3RyaW5nO1xuXHRhbGxvd1dyaXRlOiBib29sZWFuO1xufVxuXG5pbnRlcmZhY2UgSW5jb21pbmdNZXNzYWdlIHtcblx0aWQ/OiB1bmtub3duO1xuXHRhY3Rpb24/OiB1bmtub3duO1xuXHRwYXJhbXM/OiB1bmtub3duO1xufVxuXG5pbnRlcmZhY2UgUmVzcG9uc2VNZXNzYWdlIHtcblx0aWQ6IHN0cmluZyB8IG51bGw7XG5cdHN0YXR1czogXCJzdWNjZXNzXCIgfCBcImVycm9yXCI7XG5cdHBheWxvYWQ6IFJlY29yZDxzdHJpbmcsIHVua25vd24+O1xufVxuXG5pbnRlcmZhY2UgU2VhcmNoTWF0Y2gge1xuXHRwYXRoOiBzdHJpbmc7XG5cdHNuaXBwZXQ6IHN0cmluZztcbn1cblxuY29uc3QgREVGQVVMVF9TRVRUSU5HUzogUG9rZU9ic2lkaWFuU2V0dGluZ3MgPSB7XG5cdGdhdGV3YXlVcmw6IERFRkFVTFRfR0FURVdBWV9VUkwsXG5cdGNvbm5lY3Rpb25Ub2tlbjogXCJcIixcblx0dmF1bHRGb2xkZXI6IERFRkFVTFRfVkFVTFRfRk9MREVSLFxuXHRhbGxvd1dyaXRlOiBmYWxzZSxcbn07XG5cbmV4cG9ydCBkZWZhdWx0IGNsYXNzIFBva2VPYnNpZGlhblBsdWdpbiBleHRlbmRzIFBsdWdpbiB7XG5cdHNldHRpbmdzOiBQb2tlT2JzaWRpYW5TZXR0aW5ncyA9IERFRkFVTFRfU0VUVElOR1M7XG5cdHByaXZhdGUgc29ja2V0OiBXZWJTb2NrZXQgfCBudWxsID0gbnVsbDtcblx0cHJpdmF0ZSBzdGF0dXNCYXJJdGVtRWw6IEhUTUxFbGVtZW50IHwgbnVsbCA9IG51bGw7XG5cdHByaXZhdGUgc2V0dGluZ3NUYWI6IFBva2VPYnNpZGlhblNldHRpbmdUYWIgfCBudWxsID0gbnVsbDtcblx0cHJpdmF0ZSBjb25uZWN0aW9uU3RhdGU6IENvbm5lY3Rpb25TdGF0ZSA9IFwiZGlzY29ubmVjdGVkXCI7XG5cdHByaXZhdGUgcmVjb25uZWN0QXR0ZW1wdHMgPSAwO1xuXHRwcml2YXRlIHJlY29ubmVjdFRpbWVyOiBudW1iZXIgfCBudWxsID0gbnVsbDtcblx0cHJpdmF0ZSB1bmxvYWRSZXF1ZXN0ZWQgPSBmYWxzZTtcblxuXHRhc3luYyBvbmxvYWQoKTogUHJvbWlzZTx2b2lkPiB7XG5cdFx0YXdhaXQgdGhpcy5sb2FkU2V0dGluZ3MoKTtcblxuXHRcdGlmICghdGhpcy5zZXR0aW5ncy5jb25uZWN0aW9uVG9rZW4pIHtcblx0XHRcdHRoaXMuc2V0dGluZ3MuY29ubmVjdGlvblRva2VuID0gZ2VuZXJhdGVDb25uZWN0aW9uVG9rZW4oKTtcblx0XHRcdGF3YWl0IHRoaXMuc2F2ZVNldHRpbmdzKCk7XG5cdFx0fVxuXG5cdFx0dGhpcy5zdGF0dXNCYXJJdGVtRWwgPSB0aGlzLmFkZFN0YXR1c0Jhckl0ZW0oKTtcblx0XHR0aGlzLnN0YXR1c0Jhckl0ZW1FbC5hZGRDbGFzcyhcInBva2Utc3RhdHVzXCIpO1xuXHRcdHRoaXMuc2V0Q29ubmVjdGlvblN0YXRlKFwiZGlzY29ubmVjdGVkXCIpO1xuXG5cdFx0dGhpcy5zZXR0aW5nc1RhYiA9IG5ldyBQb2tlT2JzaWRpYW5TZXR0aW5nVGFiKHRoaXMuYXBwLCB0aGlzKTtcblx0XHR0aGlzLmFkZFNldHRpbmdUYWIodGhpcy5zZXR0aW5nc1RhYik7XG5cblx0XHR0aGlzLmNvbm5lY3QoKTtcblx0fVxuXG5cdG9udW5sb2FkKCk6IHZvaWQge1xuXHRcdHRoaXMudW5sb2FkUmVxdWVzdGVkID0gdHJ1ZTtcblx0XHR0aGlzLmNsZWFyUmVjb25uZWN0VGltZXIoKTtcblx0XHR0aGlzLmNsb3NlU29ja2V0KCk7XG5cdFx0dGhpcy5zZXRDb25uZWN0aW9uU3RhdGUoXCJkaXNjb25uZWN0ZWRcIik7XG5cdH1cblxuXHRhc3luYyBsb2FkU2V0dGluZ3MoKTogUHJvbWlzZTx2b2lkPiB7XG5cdFx0Y29uc3Qgc2F2ZWRTZXR0aW5nczogdW5rbm93biA9IGF3YWl0IHRoaXMubG9hZERhdGEoKTtcblx0XHRjb25zdCBzZXR0aW5nc1JlY29yZCA9IGlzUmVjb3JkKHNhdmVkU2V0dGluZ3MpID8gc2F2ZWRTZXR0aW5ncyA6IHt9O1xuXG5cdFx0dGhpcy5zZXR0aW5ncyA9IHtcblx0XHRcdGdhdGV3YXlVcmw6IGdldE9wdGlvbmFsU3RyaW5nKHNldHRpbmdzUmVjb3JkLCBcImdhdGV3YXlVcmxcIiwgREVGQVVMVF9HQVRFV0FZX1VSTCksXG5cdFx0XHRjb25uZWN0aW9uVG9rZW46IGdldE9wdGlvbmFsU3RyaW5nKHNldHRpbmdzUmVjb3JkLCBcImNvbm5lY3Rpb25Ub2tlblwiLCBcIlwiKSxcblx0XHRcdHZhdWx0Rm9sZGVyOiBnZXRPcHRpb25hbFN0cmluZyhzZXR0aW5nc1JlY29yZCwgXCJ2YXVsdEZvbGRlclwiLCBERUZBVUxUX1ZBVUxUX0ZPTERFUiksXG5cdFx0XHRhbGxvd1dyaXRlOlxuXHRcdFx0XHR0eXBlb2Ygc2V0dGluZ3NSZWNvcmQuYWxsb3dXcml0ZSA9PT0gXCJib29sZWFuXCIgPyBzZXR0aW5nc1JlY29yZC5hbGxvd1dyaXRlIDogREVGQVVMVF9TRVRUSU5HUy5hbGxvd1dyaXRlLFxuXHRcdH07XG5cblx0XHRpZiAoXG5cdFx0XHR0aGlzLnNldHRpbmdzLmNvbm5lY3Rpb25Ub2tlbiAmJlxuXHRcdFx0IU9iamVjdC5wcm90b3R5cGUuaGFzT3duUHJvcGVydHkuY2FsbChzZXR0aW5nc1JlY29yZCwgXCJ2YXVsdEZvbGRlclwiKVxuXHRcdCkge1xuXHRcdFx0dGhpcy5zZXR0aW5ncy52YXVsdEZvbGRlciA9IFwiXCI7XG5cdFx0fVxuXHR9XG5cblx0YXN5bmMgc2F2ZVNldHRpbmdzKCk6IFByb21pc2U8dm9pZD4ge1xuXHRcdGF3YWl0IHRoaXMuc2F2ZURhdGEodGhpcy5zZXR0aW5ncyk7XG5cdH1cblxuXHRnZXRDb25uZWN0aW9uU3RhdGUoKTogQ29ubmVjdGlvblN0YXRlIHtcblx0XHRyZXR1cm4gdGhpcy5jb25uZWN0aW9uU3RhdGU7XG5cdH1cblxuXHRhc3luYyB1cGRhdGVDb25uZWN0aW9uVG9rZW4odG9rZW46IHN0cmluZyk6IFByb21pc2U8dm9pZD4ge1xuXHRcdHRoaXMuc2V0dGluZ3MuY29ubmVjdGlvblRva2VuID0gdG9rZW4udHJpbSgpO1xuXHRcdGF3YWl0IHRoaXMuc2F2ZVNldHRpbmdzKCk7XG5cdFx0dGhpcy5yZWNvbm5lY3ROb3coKTtcblx0fVxuXG5cdGFzeW5jIHVwZGF0ZUdhdGV3YXlVcmwoZ2F0ZXdheVVybDogc3RyaW5nKTogUHJvbWlzZTx2b2lkPiB7XG5cdFx0dGhpcy5zZXR0aW5ncy5nYXRld2F5VXJsID0gZ2F0ZXdheVVybC50cmltKCkgfHwgREVGQVVMVF9HQVRFV0FZX1VSTDtcblx0XHRhd2FpdCB0aGlzLnNhdmVTZXR0aW5ncygpO1xuXHRcdHRoaXMucmVjb25uZWN0Tm93KCk7XG5cdH1cblxuXHRhc3luYyB1cGRhdGVBbGxvd1dyaXRlKGFsbG93V3JpdGU6IGJvb2xlYW4pOiBQcm9taXNlPHZvaWQ+IHtcblx0XHR0aGlzLnNldHRpbmdzLmFsbG93V3JpdGUgPSBhbGxvd1dyaXRlO1xuXHRcdGF3YWl0IHRoaXMuc2F2ZVNldHRpbmdzKCk7XG5cdH1cblxuXHRhc3luYyB1cGRhdGVWYXVsdEZvbGRlcih2YXVsdEZvbGRlcjogc3RyaW5nKTogUHJvbWlzZTx2b2lkPiB7XG5cdFx0dGhpcy5zZXR0aW5ncy52YXVsdEZvbGRlciA9IHZhdWx0Rm9sZGVyLnRyaW0oKTtcblx0XHRhd2FpdCB0aGlzLnNhdmVTZXR0aW5ncygpO1xuXHR9XG5cblx0cmVjb25uZWN0Tm93KCk6IHZvaWQge1xuXHRcdHRoaXMuY2xlYXJSZWNvbm5lY3RUaW1lcigpO1xuXHRcdHRoaXMucmVjb25uZWN0QXR0ZW1wdHMgPSAwO1xuXHRcdHRoaXMuY2xvc2VTb2NrZXQoKTtcblxuXHRcdGlmICghdGhpcy5zZXR0aW5ncy5jb25uZWN0aW9uVG9rZW4pIHtcblx0XHRcdHRoaXMuc2V0Q29ubmVjdGlvblN0YXRlKFwiZGlzY29ubmVjdGVkXCIpO1xuXHRcdFx0cmV0dXJuO1xuXHRcdH1cblxuXHRcdHRoaXMuY29ubmVjdCgpO1xuXHR9XG5cblx0cHJpdmF0ZSBjb25uZWN0KCk6IHZvaWQge1xuXHRcdGlmICh0aGlzLnVubG9hZFJlcXVlc3RlZCB8fCAhdGhpcy5zZXR0aW5ncy5jb25uZWN0aW9uVG9rZW4pIHtcblx0XHRcdHRoaXMuc2V0Q29ubmVjdGlvblN0YXRlKFwiZGlzY29ubmVjdGVkXCIpO1xuXHRcdFx0cmV0dXJuO1xuXHRcdH1cblxuXHRcdGlmIChcblx0XHRcdHRoaXMuc29ja2V0ICYmXG5cdFx0XHQodGhpcy5zb2NrZXQucmVhZHlTdGF0ZSA9PT0gV2ViU29ja2V0LkNPTk5FQ1RJTkcgfHwgdGhpcy5zb2NrZXQucmVhZHlTdGF0ZSA9PT0gV2ViU29ja2V0Lk9QRU4pXG5cdFx0KSB7XG5cdFx0XHRyZXR1cm47XG5cdFx0fVxuXG5cdFx0dGhpcy5zZXRDb25uZWN0aW9uU3RhdGUoXCJjb25uZWN0aW5nXCIpO1xuXG5cdFx0Y29uc3QgdXJsID0gdGhpcy5idWlsZEdhdGV3YXlVcmwoKTtcblxuXHRcdGlmICghdXJsKSB7XG5cdFx0XHR0aGlzLnNldENvbm5lY3Rpb25TdGF0ZShcImRpc2Nvbm5lY3RlZFwiKTtcblx0XHRcdHJldHVybjtcblx0XHR9XG5cblx0XHR0cnkge1xuXHRcdFx0Y29uc29sZS5sb2coYFBva2UgR2F0ZXdheSBjb25uZWN0aW5nIHRvICR7cmVkYWN0VG9rZW4odXJsKX1gKTtcblx0XHRcdHRoaXMuc29ja2V0ID0gbmV3IFdlYlNvY2tldCh1cmwudG9TdHJpbmcoKSk7XG5cdFx0fSBjYXRjaCAoZXJyb3IpIHtcblx0XHRcdHRoaXMuaGFuZGxlQ29ubmVjdGlvbkVycm9yKGVycm9yKTtcblx0XHRcdHRoaXMuc2NoZWR1bGVSZWNvbm5lY3QoKTtcblx0XHRcdHJldHVybjtcblx0XHR9XG5cblx0XHR0aGlzLnNvY2tldC5vbm9wZW4gPSAoKSA9PiB7XG5cdFx0XHRjb25zb2xlLmxvZyhcIlBva2UgR2F0ZXdheSBjb25uZWN0ZWRcIik7XG5cdFx0XHR0aGlzLnJlY29ubmVjdEF0dGVtcHRzID0gMDtcblx0XHRcdHRoaXMuc2V0Q29ubmVjdGlvblN0YXRlKFwiY29ubmVjdGVkXCIpO1xuXHRcdH07XG5cblx0XHR0aGlzLnNvY2tldC5vbm1lc3NhZ2UgPSAoZXZlbnQ6IE1lc3NhZ2VFdmVudDxzdHJpbmc+KSA9PiB7XG5cdFx0XHR2b2lkIHRoaXMud2l0aFJlcXVlc3RUaW1lb3V0KHRoaXMuaGFuZGxlU29ja2V0TWVzc2FnZShldmVudC5kYXRhKSwgbnVsbCk7XG5cdFx0fTtcblxuXHRcdHRoaXMuc29ja2V0Lm9uZXJyb3IgPSAoZXZlbnQpID0+IHtcblx0XHRcdHRoaXMuaGFuZGxlQ29ubmVjdGlvbkVycm9yKG5ldyBFcnJvcihgV2ViU29ja2V0IGNvbm5lY3Rpb24gZXJyb3I6ICR7SlNPTi5zdHJpbmdpZnkoZXZlbnQpfWApKTtcblx0XHR9O1xuXG5cdFx0dGhpcy5zb2NrZXQub25jbG9zZSA9IChldmVudCkgPT4ge1xuXHRcdFx0Y29uc29sZS5sb2coYFBva2UgR2F0ZXdheSBkaXNjb25uZWN0ZWQ6IGNvZGU9JHtldmVudC5jb2RlfSByZWFzb249JHtldmVudC5yZWFzb24gfHwgXCIobm9uZSlcIn1gKTtcblx0XHRcdHRoaXMuc29ja2V0ID0gbnVsbDtcblxuXHRcdFx0aWYgKHRoaXMudW5sb2FkUmVxdWVzdGVkKSB7XG5cdFx0XHRcdHRoaXMuc2V0Q29ubmVjdGlvblN0YXRlKFwiZGlzY29ubmVjdGVkXCIpO1xuXHRcdFx0XHRyZXR1cm47XG5cdFx0XHR9XG5cblx0XHRcdHRoaXMuc2V0Q29ubmVjdGlvblN0YXRlKFwiZGlzY29ubmVjdGVkXCIpO1xuXHRcdFx0dGhpcy5zY2hlZHVsZVJlY29ubmVjdCgpO1xuXHRcdH07XG5cdH1cblxuXHRwcml2YXRlIHNjaGVkdWxlUmVjb25uZWN0KCk6IHZvaWQge1xuXHRcdGlmICh0aGlzLnVubG9hZFJlcXVlc3RlZCB8fCAhdGhpcy5zZXR0aW5ncy5jb25uZWN0aW9uVG9rZW4gfHwgdGhpcy5yZWNvbm5lY3RUaW1lciAhPT0gbnVsbCkge1xuXHRcdFx0cmV0dXJuO1xuXHRcdH1cblxuXHRcdGNvbnN0IGRlbGF5ID0gTWF0aC5taW4oQkFTRV9SRUNPTk5FQ1RfREVMQVlfTVMgKiAyICoqIHRoaXMucmVjb25uZWN0QXR0ZW1wdHMsIE1BWF9SRUNPTk5FQ1RfREVMQVlfTVMpO1xuXHRcdHRoaXMucmVjb25uZWN0QXR0ZW1wdHMgKz0gMTtcblxuXHRcdHRoaXMucmVjb25uZWN0VGltZXIgPSB3aW5kb3cuc2V0VGltZW91dCgoKSA9PiB7XG5cdFx0XHR0aGlzLnJlY29ubmVjdFRpbWVyID0gbnVsbDtcblx0XHRcdHRoaXMuY29ubmVjdCgpO1xuXHRcdH0sIGRlbGF5KTtcblx0fVxuXG5cdHByaXZhdGUgY2xlYXJSZWNvbm5lY3RUaW1lcigpOiB2b2lkIHtcblx0XHRpZiAodGhpcy5yZWNvbm5lY3RUaW1lciAhPT0gbnVsbCkge1xuXHRcdFx0d2luZG93LmNsZWFyVGltZW91dCh0aGlzLnJlY29ubmVjdFRpbWVyKTtcblx0XHRcdHRoaXMucmVjb25uZWN0VGltZXIgPSBudWxsO1xuXHRcdH1cblx0fVxuXG5cdHByaXZhdGUgY2xvc2VTb2NrZXQoKTogdm9pZCB7XG5cdFx0aWYgKCF0aGlzLnNvY2tldCkge1xuXHRcdFx0cmV0dXJuO1xuXHRcdH1cblxuXHRcdHRoaXMuc29ja2V0Lm9ub3BlbiA9IG51bGw7XG5cdFx0dGhpcy5zb2NrZXQub25tZXNzYWdlID0gbnVsbDtcblx0XHR0aGlzLnNvY2tldC5vbmVycm9yID0gbnVsbDtcblx0XHR0aGlzLnNvY2tldC5vbmNsb3NlID0gbnVsbDtcblxuXHRcdGlmICh0aGlzLnNvY2tldC5yZWFkeVN0YXRlID09PSBXZWJTb2NrZXQuQ09OTkVDVElORyB8fCB0aGlzLnNvY2tldC5yZWFkeVN0YXRlID09PSBXZWJTb2NrZXQuT1BFTikge1xuXHRcdFx0dGhpcy5zb2NrZXQuY2xvc2UoKTtcblx0XHR9XG5cblx0XHR0aGlzLnNvY2tldCA9IG51bGw7XG5cdH1cblxuXHRwcml2YXRlIGFzeW5jIHdpdGhSZXF1ZXN0VGltZW91dCh0YXNrOiBQcm9taXNlPHZvaWQ+LCBpZDogc3RyaW5nIHwgbnVsbCk6IFByb21pc2U8dm9pZD4ge1xuXHRcdGxldCB0aW1lb3V0SWQ6IG51bWJlciB8IG51bGwgPSBudWxsO1xuXG5cdFx0Y29uc3QgdGltZW91dCA9IG5ldyBQcm9taXNlPHZvaWQ+KChfcmVzb2x2ZSwgcmVqZWN0KSA9PiB7XG5cdFx0XHR0aW1lb3V0SWQgPSB3aW5kb3cuc2V0VGltZW91dCgoKSA9PiByZWplY3QobmV3IEVycm9yKFwiUmVxdWVzdCB0aW1lZCBvdXRcIikpLCBSRVFVRVNUX1RJTUVPVVRfTVMpO1xuXHRcdH0pO1xuXG5cdFx0dHJ5IHtcblx0XHRcdGF3YWl0IFByb21pc2UucmFjZShbdGFzaywgdGltZW91dF0pO1xuXHRcdH0gY2F0Y2ggKGVycm9yKSB7XG5cdFx0XHR0aGlzLnNlbmRSZXNwb25zZSh7XG5cdFx0XHRcdGlkLFxuXHRcdFx0XHRzdGF0dXM6IFwiZXJyb3JcIixcblx0XHRcdFx0cGF5bG9hZDogeyBlcnJvcjogZXJyb3IgaW5zdGFuY2VvZiBFcnJvciA/IGVycm9yLm1lc3NhZ2UgOiBTdHJpbmcoZXJyb3IpIH0sXG5cdFx0XHR9KTtcblx0XHR9IGZpbmFsbHkge1xuXHRcdFx0aWYgKHRpbWVvdXRJZCAhPT0gbnVsbCkge1xuXHRcdFx0XHR3aW5kb3cuY2xlYXJUaW1lb3V0KHRpbWVvdXRJZCk7XG5cdFx0XHR9XG5cdFx0fVxuXHR9XG5cblx0cHJpdmF0ZSBhc3luYyBoYW5kbGVTb2NrZXRNZXNzYWdlKHJhd01lc3NhZ2U6IHN0cmluZyk6IFByb21pc2U8dm9pZD4ge1xuXHRcdGxldCBpbmNvbWluZzogSW5jb21pbmdNZXNzYWdlO1xuXG5cdFx0dHJ5IHtcblx0XHRcdGluY29taW5nID0gSlNPTi5wYXJzZShyYXdNZXNzYWdlKSBhcyBJbmNvbWluZ01lc3NhZ2U7XG5cdFx0fSBjYXRjaCB7XG5cdFx0XHR0aGlzLnNlbmRSZXNwb25zZSh7XG5cdFx0XHRcdGlkOiBudWxsLFxuXHRcdFx0XHRzdGF0dXM6IFwiZXJyb3JcIixcblx0XHRcdFx0cGF5bG9hZDogeyBlcnJvcjogXCJJbnZhbGlkIEpTT04gbWVzc2FnZVwiIH0sXG5cdFx0XHR9KTtcblx0XHRcdHJldHVybjtcblx0XHR9XG5cblx0XHRjb25zdCBpZCA9IHR5cGVvZiBpbmNvbWluZy5pZCA9PT0gXCJzdHJpbmdcIiA/IGluY29taW5nLmlkIDogbnVsbDtcblx0XHRjb25zdCBhY3Rpb24gPSB0eXBlb2YgaW5jb21pbmcuYWN0aW9uID09PSBcInN0cmluZ1wiID8gaW5jb21pbmcuYWN0aW9uIDogXCJcIjtcblx0XHRjb25zdCBwYXJhbXMgPSBpc1JlY29yZChpbmNvbWluZy5wYXJhbXMpID8gaW5jb21pbmcucGFyYW1zIDoge307XG5cblx0XHRpZiAoIWlkKSB7XG5cdFx0XHR0aGlzLnNlbmRSZXNwb25zZSh7XG5cdFx0XHRcdGlkLFxuXHRcdFx0XHRzdGF0dXM6IFwiZXJyb3JcIixcblx0XHRcdFx0cGF5bG9hZDogeyBlcnJvcjogXCJNZXNzYWdlIGlkIGlzIHJlcXVpcmVkXCIgfSxcblx0XHRcdH0pO1xuXHRcdFx0cmV0dXJuO1xuXHRcdH1cblxuXHRcdHRyeSB7XG5cdFx0XHRjb25zdCBwYXlsb2FkID0gYXdhaXQgdGhpcy5oYW5kbGVBY3Rpb24oYWN0aW9uLCBwYXJhbXMpO1xuXHRcdFx0dGhpcy5zZW5kUmVzcG9uc2UoeyBpZCwgc3RhdHVzOiBcInN1Y2Nlc3NcIiwgcGF5bG9hZCB9KTtcblx0XHR9IGNhdGNoIChlcnJvcikge1xuXHRcdFx0dGhpcy5zZW5kUmVzcG9uc2Uoe1xuXHRcdFx0XHRpZCxcblx0XHRcdFx0c3RhdHVzOiBcImVycm9yXCIsXG5cdFx0XHRcdHBheWxvYWQ6IHsgZXJyb3I6IGVycm9yIGluc3RhbmNlb2YgRXJyb3IgPyBlcnJvci5tZXNzYWdlIDogU3RyaW5nKGVycm9yKSB9LFxuXHRcdFx0fSk7XG5cdFx0fVxuXHR9XG5cblx0cHJpdmF0ZSBhc3luYyBoYW5kbGVBY3Rpb24oYWN0aW9uOiBzdHJpbmcsIHBhcmFtczogUmVjb3JkPHN0cmluZywgdW5rbm93bj4pOiBQcm9taXNlPFJlY29yZDxzdHJpbmcsIHVua25vd24+PiB7XG5cdFx0c3dpdGNoIChhY3Rpb24pIHtcblx0XHRcdGNhc2UgXCJsaXN0XCI6XG5cdFx0XHRjYXNlIFwibGlzdF9maWxlc1wiOlxuXHRcdFx0XHRyZXR1cm4gdGhpcy5saXN0RmlsZXMoKTtcblx0XHRcdGNhc2UgXCJyZWFkXCI6XG5cdFx0XHRjYXNlIFwicmVhZF9maWxlXCI6XG5cdFx0XHRcdHJldHVybiB0aGlzLnJlYWRGaWxlKHBhcmFtcyk7XG5cdFx0XHRjYXNlIFwid3JpdGVcIjpcblx0XHRcdGNhc2UgXCJ3cml0ZV9maWxlXCI6XG5cdFx0XHRcdHJldHVybiB0aGlzLndyaXRlRmlsZShwYXJhbXMpO1xuXHRcdFx0Y2FzZSBcInNlYXJjaFwiOlxuXHRcdFx0Y2FzZSBcInNlYXJjaF92YXVsdFwiOlxuXHRcdFx0XHRyZXR1cm4gdGhpcy5zZWFyY2hWYXVsdChwYXJhbXMpO1xuXHRcdFx0ZGVmYXVsdDpcblx0XHRcdFx0dGhyb3cgbmV3IEVycm9yKGBVbnN1cHBvcnRlZCBhY3Rpb246ICR7YWN0aW9uIHx8IFwiKG1pc3NpbmcpXCJ9YCk7XG5cdFx0fVxuXHR9XG5cblx0cHJpdmF0ZSBhc3luYyBsaXN0RmlsZXMoKTogUHJvbWlzZTxSZWNvcmQ8c3RyaW5nLCB1bmtub3duPj4ge1xuXHRcdHJldHVybiB7XG5cdFx0XHRmaWxlczogYXdhaXQgdGhpcy5saXN0TWFya2Rvd25QYXRocygpLFxuXHRcdH07XG5cdH1cblxuXHRwcml2YXRlIGFzeW5jIHJlYWRGaWxlKHBhcmFtczogUmVjb3JkPHN0cmluZywgdW5rbm93bj4pOiBQcm9taXNlPFJlY29yZDxzdHJpbmcsIHVua25vd24+PiB7XG5cdFx0Y29uc3QgcGF0aCA9IGdldFJlcXVpcmVkU3RyaW5nKHBhcmFtcywgXCJwYXRoXCIpO1xuXHRcdGNvbnN0IG5vcm1hbGl6ZWRQYXRoID0gdGhpcy5ub3JtYWxpemVNYXJrZG93blBhdGgocGF0aCk7XG5cdFx0dGhpcy5lbnN1cmVQYXRoSW5WYXVsdEZvbGRlcihub3JtYWxpemVkUGF0aCk7XG5cblx0XHRjb25zdCBmaWxlID0gdGhpcy5nZXRNYXJrZG93bkZpbGUobm9ybWFsaXplZFBhdGgpO1xuXHRcdGNvbnN0IGNvbnRlbnQgPSBhd2FpdCB0aGlzLmFwcC52YXVsdC5hZGFwdGVyLnJlYWQoZmlsZS5wYXRoKTtcblxuXHRcdHJldHVybiB7XG5cdFx0XHRwYXRoOiBmaWxlLnBhdGgsXG5cdFx0XHRjb250ZW50LFxuXHRcdH07XG5cdH1cblxuXHRwcml2YXRlIGFzeW5jIHdyaXRlRmlsZShwYXJhbXM6IFJlY29yZDxzdHJpbmcsIHVua25vd24+KTogUHJvbWlzZTxSZWNvcmQ8c3RyaW5nLCB1bmtub3duPj4ge1xuXHRcdGlmICghdGhpcy5zZXR0aW5ncy5hbGxvd1dyaXRlKSB7XG5cdFx0XHR0aHJvdyBuZXcgRXJyb3IoXCJXcml0ZSBhY2Nlc3MgaXMgZGlzYWJsZWQgaW4gUG9rZSBHYXRld2F5IHNldHRpbmdzXCIpO1xuXHRcdH1cblxuXHRcdGNvbnN0IHBhdGggPSBnZXRSZXF1aXJlZFN0cmluZyhwYXJhbXMsIFwicGF0aFwiKTtcblx0XHRjb25zdCBjb250ZW50ID0gZ2V0UmVxdWlyZWRTdHJpbmcocGFyYW1zLCBcImNvbnRlbnRcIik7XG5cdFx0Y29uc3Qgbm9ybWFsaXplZFBhdGggPSB0aGlzLm5vcm1hbGl6ZU1hcmtkb3duUGF0aChwYXRoKTtcblx0XHR0aGlzLmVuc3VyZVBhdGhJblZhdWx0Rm9sZGVyKG5vcm1hbGl6ZWRQYXRoKTtcblxuXHRcdGF3YWl0IHRoaXMuZW5zdXJlUGFyZW50Rm9sZGVycyhub3JtYWxpemVkUGF0aCk7XG5cdFx0YXdhaXQgdGhpcy5hcHAudmF1bHQuYWRhcHRlci53cml0ZShub3JtYWxpemVkUGF0aCwgY29udGVudCk7XG5cblx0XHRyZXR1cm4ge1xuXHRcdFx0cGF0aDogbm9ybWFsaXplZFBhdGgsXG5cdFx0XHRieXRlczogbmV3IFRleHRFbmNvZGVyKCkuZW5jb2RlKGNvbnRlbnQpLmxlbmd0aCxcblx0XHR9O1xuXHR9XG5cblx0cHJpdmF0ZSBhc3luYyBzZWFyY2hWYXVsdChwYXJhbXM6IFJlY29yZDxzdHJpbmcsIHVua25vd24+KTogUHJvbWlzZTxSZWNvcmQ8c3RyaW5nLCB1bmtub3duPj4ge1xuXHRcdGNvbnN0IHF1ZXJ5ID0gZ2V0UmVxdWlyZWRTdHJpbmcocGFyYW1zLCBcInF1ZXJ5XCIpLnRyaW0oKTtcblxuXHRcdGlmICghcXVlcnkpIHtcblx0XHRcdHRocm93IG5ldyBFcnJvcihcIlNlYXJjaCBxdWVyeSBjYW5ub3QgYmUgZW1wdHlcIik7XG5cdFx0fVxuXG5cdFx0Y29uc3Qgbm9ybWFsaXplZFF1ZXJ5ID0gcXVlcnkudG9Mb2NhbGVMb3dlckNhc2UoKTtcblx0XHRjb25zdCBtYXRjaGVzOiBTZWFyY2hNYXRjaFtdID0gW107XG5cblx0XHRmb3IgKGNvbnN0IHBhdGggb2YgYXdhaXQgdGhpcy5saXN0TWFya2Rvd25QYXRocygpKSB7XG5cdFx0XHRjb25zdCBjb250ZW50ID0gYXdhaXQgdGhpcy5hcHAudmF1bHQuYWRhcHRlci5yZWFkKHBhdGgpO1xuXHRcdFx0Y29uc3QgaW5kZXggPSBjb250ZW50LnRvTG9jYWxlTG93ZXJDYXNlKCkuaW5kZXhPZihub3JtYWxpemVkUXVlcnkpO1xuXG5cdFx0XHRpZiAoaW5kZXggPT09IC0xKSB7XG5cdFx0XHRcdGNvbnRpbnVlO1xuXHRcdFx0fVxuXG5cdFx0XHRtYXRjaGVzLnB1c2goe1xuXHRcdFx0XHRwYXRoLFxuXHRcdFx0XHRzbmlwcGV0OiBtYWtlU25pcHBldChjb250ZW50LCBpbmRleCwgcXVlcnkubGVuZ3RoKSxcblx0XHRcdH0pO1xuXHRcdH1cblxuXHRcdHJldHVybiB7IG1hdGNoZXMgfTtcblx0fVxuXG5cdHByaXZhdGUgZ2V0TWFya2Rvd25GaWxlKHBhdGg6IHN0cmluZyk6IFRGaWxlIHtcblx0XHRjb25zdCBmaWxlID0gdGhpcy5hcHAudmF1bHQuZ2V0QWJzdHJhY3RGaWxlQnlQYXRoKHBhdGgpO1xuXG5cdFx0aWYgKCEoZmlsZSBpbnN0YW5jZW9mIFRGaWxlKSkge1xuXHRcdFx0dGhyb3cgbmV3IEVycm9yKGBGaWxlIG5vdCBmb3VuZDogJHtwYXRofWApO1xuXHRcdH1cblxuXHRcdGlmIChmaWxlLmV4dGVuc2lvbiAhPT0gXCJtZFwiKSB7XG5cdFx0XHR0aHJvdyBuZXcgRXJyb3IoYE9ubHkgbWFya2Rvd24gZmlsZXMgYXJlIHN1cHBvcnRlZDogJHtwYXRofWApO1xuXHRcdH1cblxuXHRcdHJldHVybiBmaWxlO1xuXHR9XG5cblx0cHJpdmF0ZSBub3JtYWxpemVNYXJrZG93blBhdGgocGF0aDogc3RyaW5nKTogc3RyaW5nIHtcblx0XHRjb25zdCBub3JtYWxpemVkUGF0aCA9IHBhdGgudHJpbSgpLnJlcGxhY2UoL15cXC8rLywgXCJcIik7XG5cblx0XHRpZiAoIW5vcm1hbGl6ZWRQYXRoKSB7XG5cdFx0XHR0aHJvdyBuZXcgRXJyb3IoXCJQYXRoIGNhbm5vdCBiZSBlbXB0eVwiKTtcblx0XHR9XG5cblx0XHRjb25zdCBwYXJ0cyA9IG5vcm1hbGl6ZWRQYXRoLnNwbGl0KFwiL1wiKS5maWx0ZXIoQm9vbGVhbik7XG5cblx0XHRpZiAocGFydHMuaW5jbHVkZXMoXCIuLlwiKSB8fCBwYXJ0cy5zb21lKChwYXJ0KSA9PiBwYXJ0ID09PSBcIi5cIikpIHtcblx0XHRcdHRocm93IG5ldyBFcnJvcihcIlBhdGggY2Fubm90IGNvbnRhaW4gcGFyZW50IG9yIGN1cnJlbnQtZGlyZWN0b3J5IHNlZ21lbnRzXCIpO1xuXHRcdH1cblxuXHRcdGlmICghbm9ybWFsaXplZFBhdGgudG9Mb2NhbGVMb3dlckNhc2UoKS5lbmRzV2l0aChcIi5tZFwiKSkge1xuXHRcdFx0dGhyb3cgbmV3IEVycm9yKFwiT25seSBtYXJrZG93biBmaWxlIHBhdGhzIGVuZGluZyBpbiAubWQgYXJlIHN1cHBvcnRlZFwiKTtcblx0XHR9XG5cblx0XHRyZXR1cm4gcGFydHMuam9pbihcIi9cIik7XG5cdH1cblxuXHRwcml2YXRlIGdldFZhdWx0Rm9sZGVyKCk6IHN0cmluZyB7XG5cdFx0cmV0dXJuIG5vcm1hbGl6ZVZhdWx0Rm9sZGVyKHRoaXMuc2V0dGluZ3MudmF1bHRGb2xkZXIpO1xuXHR9XG5cblx0cHJpdmF0ZSBlbnN1cmVQYXRoSW5WYXVsdEZvbGRlcihwYXRoOiBzdHJpbmcpOiB2b2lkIHtcblx0XHRjb25zdCBmb2xkZXIgPSB0aGlzLmdldFZhdWx0Rm9sZGVyKCk7XG5cblx0XHRpZiAoZm9sZGVyICYmICFwYXRoLnN0YXJ0c1dpdGgoYCR7Zm9sZGVyfS9gKSkge1xuXHRcdFx0dGhyb3cgbmV3IEVycm9yKGBQYXRoIGlzIG91dHNpZGUgdGhlIGNvbmZpZ3VyZWQgdmF1bHQgYWNjZXNzIGZvbGRlcjogJHtmb2xkZXJ9YCk7XG5cdFx0fVxuXHR9XG5cblx0cHJpdmF0ZSBhc3luYyBsaXN0TWFya2Rvd25QYXRocygpOiBQcm9taXNlPHN0cmluZ1tdPiB7XG5cdFx0Y29uc3QgZm9sZGVyID0gdGhpcy5nZXRWYXVsdEZvbGRlcigpO1xuXG5cdFx0aWYgKGZvbGRlciAmJiAhKGF3YWl0IHRoaXMuYXBwLnZhdWx0LmFkYXB0ZXIuZXhpc3RzKGZvbGRlcikpKSB7XG5cdFx0XHRyZXR1cm4gW107XG5cdFx0fVxuXG5cdFx0Y29uc3QgcGF0aHMgPSBhd2FpdCB0aGlzLmxpc3RNYXJrZG93blBhdGhzSW5Gb2xkZXIoZm9sZGVyKTtcblx0XHRyZXR1cm4gcGF0aHMuc29ydCgoYSwgYikgPT4gYS5sb2NhbGVDb21wYXJlKGIpKTtcblx0fVxuXG5cdHByaXZhdGUgYXN5bmMgbGlzdE1hcmtkb3duUGF0aHNJbkZvbGRlcihmb2xkZXI6IHN0cmluZyk6IFByb21pc2U8c3RyaW5nW10+IHtcblx0XHRjb25zdCBsaXN0aW5nID0gYXdhaXQgdGhpcy5hcHAudmF1bHQuYWRhcHRlci5saXN0KGZvbGRlcik7XG5cdFx0Y29uc3QgcGF0aHMgPSBsaXN0aW5nLmZpbGVzLmZpbHRlcihpc01hcmtkb3duUGF0aCk7XG5cblx0XHRmb3IgKGNvbnN0IGNoaWxkRm9sZGVyIG9mIGxpc3RpbmcuZm9sZGVycykge1xuXHRcdFx0cGF0aHMucHVzaCguLi4oYXdhaXQgdGhpcy5saXN0TWFya2Rvd25QYXRoc0luRm9sZGVyKGNoaWxkRm9sZGVyKSkpO1xuXHRcdH1cblxuXHRcdHJldHVybiBwYXRocztcblx0fVxuXG5cdHByaXZhdGUgYXN5bmMgZW5zdXJlUGFyZW50Rm9sZGVycyhmaWxlUGF0aDogc3RyaW5nKTogUHJvbWlzZTx2b2lkPiB7XG5cdFx0Y29uc3QgcGFyZW50UGFydHMgPSBmaWxlUGF0aC5zcGxpdChcIi9cIikuc2xpY2UoMCwgLTEpO1xuXHRcdGxldCBjdXJyZW50UGF0aCA9IFwiXCI7XG5cblx0XHRmb3IgKGNvbnN0IHBhcnQgb2YgcGFyZW50UGFydHMpIHtcblx0XHRcdGN1cnJlbnRQYXRoID0gY3VycmVudFBhdGggPyBgJHtjdXJyZW50UGF0aH0vJHtwYXJ0fWAgOiBwYXJ0O1xuXG5cdFx0XHRpZiAoIShhd2FpdCB0aGlzLmFwcC52YXVsdC5hZGFwdGVyLmV4aXN0cyhjdXJyZW50UGF0aCkpKSB7XG5cdFx0XHRcdGF3YWl0IHRoaXMuYXBwLnZhdWx0LmFkYXB0ZXIubWtkaXIoY3VycmVudFBhdGgpO1xuXHRcdFx0fVxuXHRcdH1cblx0fVxuXG5cdHByaXZhdGUgYnVpbGRHYXRld2F5VXJsKCk6IFVSTCB8IG51bGwge1xuXHRcdGxldCB1cmw6IFVSTDtcblxuXHRcdHRyeSB7XG5cdFx0XHR1cmwgPSBuZXcgVVJMKHRoaXMuc2V0dGluZ3MuZ2F0ZXdheVVybCB8fCBERUZBVUxUX0dBVEVXQVlfVVJMKTtcblx0XHR9IGNhdGNoIHtcblx0XHRcdG5ldyBOb3RpY2UoXCJJbnZhbGlkIFBva2UgR2F0ZXdheSBVUkxcIik7XG5cdFx0XHRyZXR1cm4gbnVsbDtcblx0XHR9XG5cblx0XHRpZiAodXJsLnByb3RvY29sICE9PSBcIndzOlwiICYmIHVybC5wcm90b2NvbCAhPT0gXCJ3c3M6XCIpIHtcblx0XHRcdG5ldyBOb3RpY2UoXCJQb2tlIEdhdGV3YXkgVVJMIG11c3Qgc3RhcnQgd2l0aCB3czovLyBvciB3c3M6Ly9cIik7XG5cdFx0XHRyZXR1cm4gbnVsbDtcblx0XHR9XG5cblx0XHR1cmwuc2VhcmNoUGFyYW1zLnNldChcInRva2VuXCIsIHRoaXMuc2V0dGluZ3MuY29ubmVjdGlvblRva2VuKTtcblx0XHR1cmwuc2VhcmNoUGFyYW1zLnNldChcInBsdWdpblwiLCB0aGlzLm1hbmlmZXN0LmlkKTtcblx0XHR1cmwuc2VhcmNoUGFyYW1zLnNldChcInZlcnNpb25cIiwgdGhpcy5tYW5pZmVzdC52ZXJzaW9uKTtcblxuXHRcdHJldHVybiB1cmw7XG5cdH1cblxuXHRwcml2YXRlIHNlbmRSZXNwb25zZShyZXNwb25zZTogUmVzcG9uc2VNZXNzYWdlKTogdm9pZCB7XG5cdFx0aWYgKCF0aGlzLnNvY2tldCB8fCB0aGlzLnNvY2tldC5yZWFkeVN0YXRlICE9PSBXZWJTb2NrZXQuT1BFTikge1xuXHRcdFx0cmV0dXJuO1xuXHRcdH1cblxuXHRcdHRoaXMuc29ja2V0LnNlbmQoSlNPTi5zdHJpbmdpZnkocmVzcG9uc2UpKTtcblx0fVxuXG5cdHByaXZhdGUgc2V0Q29ubmVjdGlvblN0YXRlKHN0YXRlOiBDb25uZWN0aW9uU3RhdGUpOiB2b2lkIHtcblx0XHR0aGlzLmNvbm5lY3Rpb25TdGF0ZSA9IHN0YXRlO1xuXG5cdFx0aWYgKHRoaXMuc3RhdHVzQmFySXRlbUVsKSB7XG5cdFx0XHR0aGlzLnN0YXR1c0Jhckl0ZW1FbC5zZXRUZXh0KGBQb2tlOiAke2NhcGl0YWxpemUoc3RhdGUpfWApO1xuXHRcdFx0dGhpcy5zdGF0dXNCYXJJdGVtRWwucmVtb3ZlQ2xhc3MoXCJpcy1jb25uZWN0ZWRcIiwgXCJpcy1jb25uZWN0aW5nXCIsIFwiaXMtZGlzY29ubmVjdGVkXCIpO1xuXHRcdFx0dGhpcy5zdGF0dXNCYXJJdGVtRWwuYWRkQ2xhc3MoYGlzLSR7c3RhdGV9YCk7XG5cdFx0fVxuXG5cdFx0dGhpcy5zZXR0aW5nc1RhYj8udXBkYXRlU3RhdHVzKHN0YXRlKTtcblx0fVxuXG5cdHByaXZhdGUgaGFuZGxlQ29ubmVjdGlvbkVycm9yKGVycm9yOiB1bmtub3duKTogdm9pZCB7XG5cdFx0Y29uc29sZS5lcnJvcihcIlBva2UgR2F0ZXdheSBjb25uZWN0aW9uIGVycm9yXCIsIGVycm9yKTtcblx0fVxufVxuXG5jbGFzcyBQb2tlT2JzaWRpYW5TZXR0aW5nVGFiIGV4dGVuZHMgUGx1Z2luU2V0dGluZ1RhYiB7XG5cdHByaXZhdGUgcGx1Z2luOiBQb2tlT2JzaWRpYW5QbHVnaW47XG5cdHByaXZhdGUgc3RhdHVzRWw6IEhUTUxFbGVtZW50IHwgbnVsbCA9IG51bGw7XG5cblx0Y29uc3RydWN0b3IoYXBwOiBBcHAsIHBsdWdpbjogUG9rZU9ic2lkaWFuUGx1Z2luKSB7XG5cdFx0c3VwZXIoYXBwLCBwbHVnaW4pO1xuXHRcdHRoaXMucGx1Z2luID0gcGx1Z2luO1xuXHR9XG5cblx0ZGlzcGxheSgpOiB2b2lkIHtcblx0XHRjb25zdCB7IGNvbnRhaW5lckVsIH0gPSB0aGlzO1xuXHRcdGNvbnRhaW5lckVsLmVtcHR5KCk7XG5cblx0XHRuZXcgU2V0dGluZyhjb250YWluZXJFbCkuc2V0TmFtZShcIlBva2UgR2F0ZXdheVwiKS5zZXRIZWFkaW5nKCk7XG5cblx0XHR0aGlzLnJlbmRlckdhdGV3YXlVcmxTZXR0aW5nKFxuXHRcdFx0bmV3IFNldHRpbmcoY29udGFpbmVyRWwpXG5cdFx0XHRcdC5zZXROYW1lKFwiR2F0ZXdheSBVUkxcIilcblx0XHRcdFx0LnNldERlc2MoXCJXZWJTb2NrZXQgZW5kcG9pbnQgdXNlZCB0byBjb25uZWN0IHRoaXMgdmF1bHQgdG8gUG9rZS5cIiksXG5cdFx0KTtcblx0XHR0aGlzLnJlbmRlclZhdWx0Rm9sZGVyU2V0dGluZyhcblx0XHRcdG5ldyBTZXR0aW5nKGNvbnRhaW5lckVsKVxuXHRcdFx0XHQuc2V0TmFtZShcIlZhdWx0IGFjY2VzcyBmb2xkZXJcIilcblx0XHRcdFx0LnNldERlc2MoXCJMaW1pdCBQb2tlIHRvIG1hcmtkb3duIGZpbGVzIGluIHRoaXMgZm9sZGVyLiBMZWF2ZSBibGFuayB0byBhbGxvdyBhbGwgbWFya2Rvd24gZmlsZXMuXCIpLFxuXHRcdCk7XG5cdFx0dGhpcy5yZW5kZXJDb25uZWN0aW9uVG9rZW5TZXR0aW5nKFxuXHRcdFx0bmV3IFNldHRpbmcoY29udGFpbmVyRWwpXG5cdFx0XHRcdC5zZXROYW1lKFwiQ29ubmVjdGlvbiB0b2tlblwiKVxuXHRcdFx0XHQuc2V0RGVzYyhcIlBhc3RlIHRoaXMgdG9rZW4gaW50byBQb2tlJ3MgQWRkIEtleSBmaWVsZCBmb3IgdGhlIE9ic2lkaWFuIHJlY2lwZS5cIiksXG5cdFx0KTtcblx0XHR0aGlzLnJlbmRlckFsbG93V3JpdGVTZXR0aW5nKFxuXHRcdFx0bmV3IFNldHRpbmcoY29udGFpbmVyRWwpXG5cdFx0XHRcdC5zZXROYW1lKFwiQWxsb3cgd3JpdGVzXCIpXG5cdFx0XHRcdC5zZXREZXNjKFwiQWxsb3cgUG9rZSB0byBjcmVhdGUgb3Igb3ZlcndyaXRlIG1hcmtkb3duIGZpbGVzIGluIHRoaXMgdmF1bHQuXCIpLFxuXHRcdCk7XG5cdFx0dGhpcy5yZW5kZXJDb25uZWN0aW9uU3RhdHVzU2V0dGluZyhcblx0XHRcdG5ldyBTZXR0aW5nKGNvbnRhaW5lckVsKVxuXHRcdFx0XHQuc2V0TmFtZShcIkNvbm5lY3Rpb24gc3RhdHVzXCIpXG5cdFx0XHRcdC5zZXREZXNjKFwiQ3VycmVudCBnYXRld2F5IGNvbm5lY3Rpb24gc3RhdGUuXCIpLFxuXHRcdCk7XG5cdH1cblxuXHRwcml2YXRlIHJlbmRlckdhdGV3YXlVcmxTZXR0aW5nKHNldHRpbmc6IFNldHRpbmcpOiB2b2lkIHtcblx0XHRzZXR0aW5nLmFkZFRleHQoKHRleHQpID0+IHtcblx0XHRcdHRleHRcblx0XHRcdFx0LnNldFBsYWNlaG9sZGVyKERFRkFVTFRfR0FURVdBWV9VUkwpXG5cdFx0XHRcdC5zZXRWYWx1ZSh0aGlzLnBsdWdpbi5zZXR0aW5ncy5nYXRld2F5VXJsIHx8IERFRkFVTFRfR0FURVdBWV9VUkwpXG5cdFx0XHRcdC5vbkNoYW5nZShhc3luYyAodmFsdWUpID0+IHtcblx0XHRcdFx0XHRhd2FpdCB0aGlzLnBsdWdpbi51cGRhdGVHYXRld2F5VXJsKHZhbHVlKTtcblx0XHRcdFx0fSk7XG5cdFx0fSk7XG5cdH1cblxuXHRwcml2YXRlIHJlbmRlclZhdWx0Rm9sZGVyU2V0dGluZyhzZXR0aW5nOiBTZXR0aW5nKTogdm9pZCB7XG5cdFx0c2V0dGluZy5hZGRUZXh0KCh0ZXh0KSA9PiB7XG5cdFx0XHR0ZXh0XG5cdFx0XHRcdC5zZXRQbGFjZWhvbGRlcihERUZBVUxUX1ZBVUxUX0ZPTERFUilcblx0XHRcdFx0LnNldFZhbHVlKHRoaXMucGx1Z2luLnNldHRpbmdzLnZhdWx0Rm9sZGVyKVxuXHRcdFx0XHQub25DaGFuZ2UoYXN5bmMgKHZhbHVlKSA9PiB7XG5cdFx0XHRcdFx0YXdhaXQgdGhpcy5wbHVnaW4udXBkYXRlVmF1bHRGb2xkZXIodmFsdWUpO1xuXHRcdFx0XHR9KTtcblx0XHR9KTtcblx0fVxuXG5cdHByaXZhdGUgcmVuZGVyQ29ubmVjdGlvblRva2VuU2V0dGluZyhzZXR0aW5nOiBTZXR0aW5nKTogdm9pZCB7XG5cdFx0bGV0IHRva2VuSW5wdXQ6IEhUTUxJbnB1dEVsZW1lbnQgfCBudWxsID0gbnVsbDtcblxuXHRcdHNldHRpbmdcblx0XHRcdC5hZGRUZXh0KCh0ZXh0KSA9PiB7XG5cdFx0XHRcdHRleHRcblx0XHRcdFx0XHQuc2V0UGxhY2Vob2xkZXIoXCJQYXN0ZSB0b2tlblwiKVxuXHRcdFx0XHRcdC5zZXRWYWx1ZSh0aGlzLnBsdWdpbi5zZXR0aW5ncy5jb25uZWN0aW9uVG9rZW4pXG5cdFx0XHRcdFx0Lm9uQ2hhbmdlKGFzeW5jICh2YWx1ZSkgPT4ge1xuXHRcdFx0XHRcdFx0YXdhaXQgdGhpcy5wbHVnaW4udXBkYXRlQ29ubmVjdGlvblRva2VuKHZhbHVlKTtcblx0XHRcdFx0XHR9KTtcblxuXHRcdFx0XHR0ZXh0LmlucHV0RWwudHlwZSA9IFwicGFzc3dvcmRcIjtcblx0XHRcdFx0dG9rZW5JbnB1dCA9IHRleHQuaW5wdXRFbDtcblx0XHRcdH0pXG5cdFx0XHQuYWRkRXh0cmFCdXR0b24oKGJ1dHRvbikgPT4ge1xuXHRcdFx0XHRidXR0b25cblx0XHRcdFx0XHQuc2V0SWNvbihcInRleHQtY3Vyc29yLWlucHV0XCIpXG5cdFx0XHRcdFx0LnNldFRvb2x0aXAoXCJSZXZlYWwgYW5kIHNlbGVjdCB0b2tlblwiKVxuXHRcdFx0XHRcdC5vbkNsaWNrKCgpID0+IHtcblx0XHRcdFx0XHRcdGlmICghdG9rZW5JbnB1dCkge1xuXHRcdFx0XHRcdFx0XHRyZXR1cm47XG5cdFx0XHRcdFx0XHR9XG5cblx0XHRcdFx0XHRcdHRva2VuSW5wdXQudHlwZSA9IFwidGV4dFwiO1xuXHRcdFx0XHRcdFx0dG9rZW5JbnB1dC5mb2N1cygpO1xuXHRcdFx0XHRcdFx0dG9rZW5JbnB1dC5zZWxlY3QoKTtcblx0XHRcdFx0XHRcdG5ldyBOb3RpY2UoXCJQb2tlIEdhdGV3YXkgdG9rZW4gc2VsZWN0ZWRcIik7XG5cdFx0XHRcdFx0fSk7XG5cdFx0XHR9KVxuXHRcdFx0LmFkZEV4dHJhQnV0dG9uKChidXR0b24pID0+IHtcblx0XHRcdFx0YnV0dG9uXG5cdFx0XHRcdFx0LnNldEljb24oXCJyZWZyZXNoLWN3XCIpXG5cdFx0XHRcdFx0LnNldFRvb2x0aXAoXCJHZW5lcmF0ZSBuZXcgdG9rZW5cIilcblx0XHRcdFx0XHQub25DbGljayhhc3luYyAoKSA9PiB7XG5cdFx0XHRcdFx0XHRjb25zdCB0b2tlbiA9IGdlbmVyYXRlQ29ubmVjdGlvblRva2VuKCk7XG5cdFx0XHRcdFx0XHRhd2FpdCB0aGlzLnBsdWdpbi51cGRhdGVDb25uZWN0aW9uVG9rZW4odG9rZW4pO1xuXHRcdFx0XHRcdFx0aWYgKHRva2VuSW5wdXQpIHtcblx0XHRcdFx0XHRcdFx0dG9rZW5JbnB1dC52YWx1ZSA9IHRva2VuO1xuXHRcdFx0XHRcdFx0fVxuXHRcdFx0XHRcdFx0bmV3IE5vdGljZShcIkdlbmVyYXRlZCBhIG5ldyBQb2tlIEdhdGV3YXkgdG9rZW5cIik7XG5cdFx0XHRcdFx0fSk7XG5cdFx0XHR9KTtcblx0fVxuXG5cdHByaXZhdGUgcmVuZGVyQWxsb3dXcml0ZVNldHRpbmcoc2V0dGluZzogU2V0dGluZyk6IHZvaWQge1xuXHRcdHNldHRpbmcuYWRkVG9nZ2xlKCh0b2dnbGUpID0+IHtcblx0XHRcdHRvZ2dsZVxuXHRcdFx0XHQuc2V0VmFsdWUodGhpcy5wbHVnaW4uc2V0dGluZ3MuYWxsb3dXcml0ZSlcblx0XHRcdFx0Lm9uQ2hhbmdlKGFzeW5jICh2YWx1ZSkgPT4ge1xuXHRcdFx0XHRcdGF3YWl0IHRoaXMucGx1Z2luLnVwZGF0ZUFsbG93V3JpdGUodmFsdWUpO1xuXHRcdFx0XHR9KTtcblx0XHR9KTtcblx0fVxuXG5cdHByaXZhdGUgcmVuZGVyQ29ubmVjdGlvblN0YXR1c1NldHRpbmcoc2V0dGluZzogU2V0dGluZyk6IHZvaWQge1xuXHRcdHNldHRpbmcuYWRkRXh0cmFCdXR0b24oKGJ1dHRvbikgPT4ge1xuXHRcdFx0YnV0dG9uXG5cdFx0XHRcdC5zZXRJY29uKFwicmVmcmVzaC1jd1wiKVxuXHRcdFx0XHQuc2V0VG9vbHRpcChcIlJlY29ubmVjdFwiKVxuXHRcdFx0XHQub25DbGljaygoKSA9PiB7XG5cdFx0XHRcdFx0dGhpcy5wbHVnaW4ucmVjb25uZWN0Tm93KCk7XG5cdFx0XHRcdFx0bmV3IE5vdGljZShcIlJlY29ubmVjdGluZyBQb2tlIEdhdGV3YXlcIik7XG5cdFx0XHRcdH0pO1xuXHRcdH0pO1xuXG5cdFx0dGhpcy5zdGF0dXNFbCA9IHNldHRpbmcuY29udHJvbEVsLmNyZWF0ZVNwYW4oKTtcblx0XHR0aGlzLnN0YXR1c0VsLmFkZENsYXNzKFwicG9rZS1zdGF0dXNcIik7XG5cdFx0dGhpcy51cGRhdGVTdGF0dXModGhpcy5wbHVnaW4uZ2V0Q29ubmVjdGlvblN0YXRlKCkpO1xuXHR9XG5cblx0dXBkYXRlU3RhdHVzKHN0YXRlOiBDb25uZWN0aW9uU3RhdGUpOiB2b2lkIHtcblx0XHRpZiAoIXRoaXMuc3RhdHVzRWwpIHtcblx0XHRcdHJldHVybjtcblx0XHR9XG5cblx0XHR0aGlzLnN0YXR1c0VsLnNldFRleHQoY2FwaXRhbGl6ZShzdGF0ZSkpO1xuXHRcdHRoaXMuc3RhdHVzRWwucmVtb3ZlQ2xhc3MoXCJpcy1jb25uZWN0ZWRcIiwgXCJpcy1jb25uZWN0aW5nXCIsIFwiaXMtZGlzY29ubmVjdGVkXCIpO1xuXHRcdHRoaXMuc3RhdHVzRWwuYWRkQ2xhc3MoYGlzLSR7c3RhdGV9YCk7XG5cdH1cbn1cblxuZnVuY3Rpb24gZ2V0UmVxdWlyZWRTdHJpbmcocGFyYW1zOiBSZWNvcmQ8c3RyaW5nLCB1bmtub3duPiwga2V5OiBzdHJpbmcpOiBzdHJpbmcge1xuXHRjb25zdCB2YWx1ZSA9IHBhcmFtc1trZXldO1xuXG5cdGlmICh0eXBlb2YgdmFsdWUgIT09IFwic3RyaW5nXCIpIHtcblx0XHR0aHJvdyBuZXcgRXJyb3IoYE1pc3NpbmcgcmVxdWlyZWQgc3RyaW5nIHBhcmFtOiAke2tleX1gKTtcblx0fVxuXG5cdHJldHVybiB2YWx1ZTtcbn1cblxuZnVuY3Rpb24gZ2V0T3B0aW9uYWxTdHJpbmcocGFyYW1zOiBSZWNvcmQ8c3RyaW5nLCB1bmtub3duPiwga2V5OiBzdHJpbmcsIGZhbGxiYWNrOiBzdHJpbmcpOiBzdHJpbmcge1xuXHRjb25zdCB2YWx1ZSA9IHBhcmFtc1trZXldO1xuXHRyZXR1cm4gdHlwZW9mIHZhbHVlID09PSBcInN0cmluZ1wiID8gdmFsdWUgOiBmYWxsYmFjaztcbn1cblxuZnVuY3Rpb24gaXNSZWNvcmQodmFsdWU6IHVua25vd24pOiB2YWx1ZSBpcyBSZWNvcmQ8c3RyaW5nLCB1bmtub3duPiB7XG5cdHJldHVybiB0eXBlb2YgdmFsdWUgPT09IFwib2JqZWN0XCIgJiYgdmFsdWUgIT09IG51bGwgJiYgIUFycmF5LmlzQXJyYXkodmFsdWUpO1xufVxuXG5mdW5jdGlvbiBpc01hcmtkb3duUGF0aChwYXRoOiBzdHJpbmcpOiBib29sZWFuIHtcblx0cmV0dXJuIHBhdGgudG9Mb2NhbGVMb3dlckNhc2UoKS5lbmRzV2l0aChcIi5tZFwiKTtcbn1cblxuZnVuY3Rpb24gbm9ybWFsaXplVmF1bHRGb2xkZXIocGF0aDogc3RyaW5nKTogc3RyaW5nIHtcblx0Y29uc3Qgbm9ybWFsaXplZFBhdGggPSBwYXRoLnRyaW0oKS5yZXBsYWNlKC9eXFwvK3xcXC8rJC9nLCBcIlwiKTtcblxuXHRpZiAoIW5vcm1hbGl6ZWRQYXRoKSB7XG5cdFx0cmV0dXJuIFwiXCI7XG5cdH1cblxuXHRjb25zdCBwYXJ0cyA9IG5vcm1hbGl6ZWRQYXRoLnNwbGl0KFwiL1wiKS5maWx0ZXIoQm9vbGVhbik7XG5cblx0aWYgKHBhcnRzLmluY2x1ZGVzKFwiLi5cIikgfHwgcGFydHMuc29tZSgocGFydCkgPT4gcGFydCA9PT0gXCIuXCIpKSB7XG5cdFx0dGhyb3cgbmV3IEVycm9yKFwiVmF1bHQgYWNjZXNzIGZvbGRlciBjYW5ub3QgY29udGFpbiBwYXJlbnQgb3IgY3VycmVudC1kaXJlY3Rvcnkgc2VnbWVudHNcIik7XG5cdH1cblxuXHRyZXR1cm4gcGFydHMuam9pbihcIi9cIik7XG59XG5cbmZ1bmN0aW9uIG1ha2VTbmlwcGV0KGNvbnRlbnQ6IHN0cmluZywgaW5kZXg6IG51bWJlciwgbWF0Y2hMZW5ndGg6IG51bWJlcik6IHN0cmluZyB7XG5cdGNvbnN0IGhhbGZXaW5kb3cgPSBNYXRoLmZsb29yKChNQVhfU05JUFBFVF9MRU5HVEggLSBtYXRjaExlbmd0aCkgLyAyKTtcblx0Y29uc3Qgc3RhcnQgPSBNYXRoLm1heCgwLCBpbmRleCAtIGhhbGZXaW5kb3cpO1xuXHRjb25zdCBlbmQgPSBNYXRoLm1pbihjb250ZW50Lmxlbmd0aCwgaW5kZXggKyBtYXRjaExlbmd0aCArIGhhbGZXaW5kb3cpO1xuXHRjb25zdCBwcmVmaXggPSBzdGFydCA+IDAgPyBcIi4uLlwiIDogXCJcIjtcblx0Y29uc3Qgc3VmZml4ID0gZW5kIDwgY29udGVudC5sZW5ndGggPyBcIi4uLlwiIDogXCJcIjtcblxuXHRyZXR1cm4gYCR7cHJlZml4fSR7Y29udGVudC5zbGljZShzdGFydCwgZW5kKS5yZXBsYWNlKC9cXHMrL2csIFwiIFwiKS50cmltKCl9JHtzdWZmaXh9YDtcbn1cblxuZnVuY3Rpb24gY2FwaXRhbGl6ZSh2YWx1ZTogc3RyaW5nKTogc3RyaW5nIHtcblx0cmV0dXJuIHZhbHVlLmNoYXJBdCgwKS50b0xvY2FsZVVwcGVyQ2FzZSgpICsgdmFsdWUuc2xpY2UoMSk7XG59XG5cbmZ1bmN0aW9uIGdlbmVyYXRlQ29ubmVjdGlvblRva2VuKCk6IHN0cmluZyB7XG5cdGNvbnN0IGJ5dGVzID0gbmV3IFVpbnQ4QXJyYXkoMzIpO1xuXHRjcnlwdG8uZ2V0UmFuZG9tVmFsdWVzKGJ5dGVzKTtcblx0cmV0dXJuIGAke0dFTkVSQVRFRF9UT0tFTl9QUkVGSVh9JHt0b0Jhc2U2NFVybChieXRlcyl9YDtcbn1cblxuZnVuY3Rpb24gdG9CYXNlNjRVcmwoYnl0ZXM6IFVpbnQ4QXJyYXkpOiBzdHJpbmcge1xuXHRsZXQgYmluYXJ5ID0gXCJcIjtcblxuXHRmb3IgKGNvbnN0IGJ5dGUgb2YgYnl0ZXMpIHtcblx0XHRiaW5hcnkgKz0gU3RyaW5nLmZyb21DaGFyQ29kZShieXRlKTtcblx0fVxuXG5cdHJldHVybiBidG9hKGJpbmFyeSkucmVwbGFjZSgvXFwrL2csIFwiLVwiKS5yZXBsYWNlKC9cXC8vZywgXCJfXCIpLnJlcGxhY2UoLz0rJC9nLCBcIlwiKTtcbn1cblxuZnVuY3Rpb24gcmVkYWN0VG9rZW4odXJsOiBVUkwpOiBzdHJpbmcge1xuXHRjb25zdCBjb3B5ID0gbmV3IFVSTCh1cmwudG9TdHJpbmcoKSk7XG5cblx0aWYgKGNvcHkuc2VhcmNoUGFyYW1zLmhhcyhcInRva2VuXCIpKSB7XG5cdFx0Y29weS5zZWFyY2hQYXJhbXMuc2V0KFwidG9rZW5cIiwgXCIqKipcIik7XG5cdH1cblxuXHRyZXR1cm4gY29weS50b1N0cmluZygpO1xufVxuIl19