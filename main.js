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
    getSettingDefinitions() {
        return [
            {
                type: "group",
                heading: "Poke Gateway",
                items: [
                    {
                        name: "Gateway URL",
                        desc: "WebSocket endpoint used to connect this vault to Poke.",
                        render: (setting) => this.renderGatewayUrlSetting(setting),
                    },
                    {
                        name: "Vault access folder",
                        desc: "Limit Poke to markdown files in this folder. Leave blank to allow all markdown files.",
                        render: (setting) => this.renderVaultFolderSetting(setting),
                    },
                    {
                        name: "Connection token",
                        desc: "Paste this token into Poke's Add Key field for the Obsidian recipe.",
                        render: (setting) => this.renderConnectionTokenSetting(setting),
                    },
                    {
                        name: "Allow writes",
                        desc: "Allow Poke to create or overwrite markdown files in this vault.",
                        render: (setting) => this.renderAllowWriteSetting(setting),
                    },
                    {
                        name: "Connection status",
                        desc: "Current gateway connection state.",
                        render: (setting) => this.renderConnectionStatusSetting(setting),
                    },
                ],
            },
        ];
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
                this.update();
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
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoibWFpbi5qcyIsInNvdXJjZVJvb3QiOiIiLCJzb3VyY2VzIjpbIm1haW4udHMiXSwibmFtZXMiOltdLCJtYXBwaW5ncyI6Ijs7QUFBQSx1Q0FBa0g7QUFFbEgsTUFBTSxtQkFBbUIsR0FBRywwQ0FBMEMsQ0FBQztBQUN2RSxNQUFNLG9CQUFvQixHQUFHLE1BQU0sQ0FBQztBQUNwQyxNQUFNLHVCQUF1QixHQUFHLElBQUssQ0FBQztBQUN0QyxNQUFNLHNCQUFzQixHQUFHLEtBQU0sQ0FBQztBQUN0QyxNQUFNLGtCQUFrQixHQUFHLEtBQU0sQ0FBQztBQUNsQyxNQUFNLGtCQUFrQixHQUFHLEdBQUcsQ0FBQztBQUMvQixNQUFNLHNCQUFzQixHQUFHLGNBQWMsQ0FBQztBQTRCOUMsTUFBTSxnQkFBZ0IsR0FBeUI7SUFDOUMsVUFBVSxFQUFFLG1CQUFtQjtJQUMvQixlQUFlLEVBQUUsRUFBRTtJQUNuQixXQUFXLEVBQUUsb0JBQW9CO0lBQ2pDLFVBQVUsRUFBRSxLQUFLO0NBQ2pCLENBQUM7QUFFRixNQUFxQixrQkFBbUIsU0FBUSxpQkFBTTtJQUF0RDs7UUFDQyxhQUFRLEdBQXlCLGdCQUFnQixDQUFDO1FBQzFDLFdBQU0sR0FBcUIsSUFBSSxDQUFDO1FBQ2hDLG9CQUFlLEdBQXVCLElBQUksQ0FBQztRQUMzQyxnQkFBVyxHQUFrQyxJQUFJLENBQUM7UUFDbEQsb0JBQWUsR0FBb0IsY0FBYyxDQUFDO1FBQ2xELHNCQUFpQixHQUFHLENBQUMsQ0FBQztRQUN0QixtQkFBYyxHQUFrQixJQUFJLENBQUM7UUFDckMsb0JBQWUsR0FBRyxLQUFLLENBQUM7SUEyY2pDLENBQUM7SUF6Y0EsS0FBSyxDQUFDLE1BQU07UUFDWCxNQUFNLElBQUksQ0FBQyxZQUFZLEVBQUUsQ0FBQztRQUUxQixJQUFJLENBQUMsSUFBSSxDQUFDLFFBQVEsQ0FBQyxlQUFlLEVBQUUsQ0FBQztZQUNwQyxJQUFJLENBQUMsUUFBUSxDQUFDLGVBQWUsR0FBRyx1QkFBdUIsRUFBRSxDQUFDO1lBQzFELE1B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O0lBQ3JDLElBQUksTUFBTSxHQUFHLEVBQUUsQ0FBQztJQUVoQixLQUFLLE1BQU0sSUFBSSxJQUFJLEtBQUssRUFBRSxDQUFDO1FBQzFCLE1BQU0sSUFBSSxNQUFNLENBQUMsWUFBWSxDQUFDLElBQUksQ0FBQyxDQUFDO0lBQ3JDLENBQUM7SUFFRCxPQUFPLElBQUksQ0FBQyxNQUFNLENBQUMsQ0FBQyxPQUFPLENBQUMsS0FBSyxFQUFFLEdBQUcsQ0FBQyxDQUFDLE9BQU8sQ0FBQyxLQUFLLEVBQUUsR0FBRyxDQUFDLENBQUMsT0FBTyxDQUFDLE1BQU0sRUFBRSxFQUFFLENBQUMsQ0FBQztBQUNqRixDQUFDO0FBRUQsU0FBUyxXQUFXLENBQUMsR0FBUTtJQUM1QixNQUFNLElBQUksR0FBRyxJQUFJLEdBQUcsQ0FBQyxHQUFHLENBQUMsUUFBUSxFQUFFLENBQUMsQ0FBQztJQUVyQyxJQUFJLElBQUksQ0FBQyxZQUFZLENBQUMsR0FBRyxDQUFDLE9BQU8sQ0FBQyxFQUFFLENBQUM7UUFDcEMsSUFBSSxDQUFDLFlBQVksQ0FBQyxHQUFHLENBQUMsT0FBTyxFQUFFLEtBQUssQ0FBQyxDQUFDO0lBQ3ZDLENBQUM7SUFFRCxPQUFPLElBQUksQ0FBQyxRQUFRLEVBQUUsQ0FBQztBQUN4QixDQUFDIiwic291cmNlc0NvbnRlbnQiOlsiaW1wb3J0IHsgQXBwLCBOb3RpY2UsIFBsdWdpbiwgUGx1Z2luU2V0dGluZ1RhYiwgVEZpbGUsIHR5cGUgU2V0dGluZywgdHlwZSBTZXR0aW5nRGVmaW5pdGlvbkl0ZW0gfSBmcm9tIFwib2JzaWRpYW5cIjtcblxuY29uc3QgREVGQVVMVF9HQVRFV0FZX1VSTCA9IFwid3NzOi8vb2JzaWRpYW4ubWF0dC1uei5jb20vb2JzaWRpYW4vc3luY1wiO1xuY29uc3QgREVGQVVMVF9WQVVMVF9GT0xERVIgPSBcIlBva2VcIjtcbmNvbnN0IEJBU0VfUkVDT05ORUNUX0RFTEFZX01TID0gMV8wMDA7XG5jb25zdCBNQVhfUkVDT05ORUNUX0RFTEFZX01TID0gMzBfMDAwO1xuY29uc3QgUkVRVUVTVF9USU1FT1VUX01TID0gMzBfMDAwO1xuY29uc3QgTUFYX1NOSVBQRVRfTEVOR1RIID0gMTgwO1xuY29uc3QgR0VORVJBVEVEX1RPS0VOX1BSRUZJWCA9IFwicGtvYnNfdmF1bHRfXCI7XG5cbnR5cGUgQ29ubmVjdGlvblN0YXRlID0gXCJjb25uZWN0ZWRcIiB8IFwiY29ubmVjdGluZ1wiIHwgXCJkaXNjb25uZWN0ZWRcIjtcblxuaW50ZXJmYWNlIFBva2VPYnNpZGlhblNldHRpbmdzIHtcblx0Z2F0ZXdheVVybDogc3RyaW5nO1xuXHRjb25uZWN0aW9uVG9rZW46IHN0cmluZztcblx0dmF1bHRGb2xkZXI6IHN0cmluZztcblx0YWxsb3dXcml0ZTogYm9vbGVhbjtcbn1cblxuaW50ZXJmYWNlIEluY29taW5nTWVzc2FnZSB7XG5cdGlkPzogdW5rbm93bjtcblx0YWN0aW9uPzogdW5rbm93bjtcblx0cGFyYW1zPzogdW5rbm93bjtcbn1cblxuaW50ZXJmYWNlIFJlc3BvbnNlTWVzc2FnZSB7XG5cdGlkOiBzdHJpbmcgfCBudWxsO1xuXHRzdGF0dXM6IFwic3VjY2Vzc1wiIHwgXCJlcnJvclwiO1xuXHRwYXlsb2FkOiBSZWNvcmQ8c3RyaW5nLCB1bmtub3duPjtcbn1cblxuaW50ZXJmYWNlIFNlYXJjaE1hdGNoIHtcblx0cGF0aDogc3RyaW5nO1xuXHRzbmlwcGV0OiBzdHJpbmc7XG59XG5cbmNvbnN0IERFRkFVTFRfU0VUVElOR1M6IFBva2VPYnNpZGlhblNldHRpbmdzID0ge1xuXHRnYXRld2F5VXJsOiBERUZBVUxUX0dBVEVXQVlfVVJMLFxuXHRjb25uZWN0aW9uVG9rZW46IFwiXCIsXG5cdHZhdWx0Rm9sZGVyOiBERUZBVUxUX1ZBVUxUX0ZPTERFUixcblx0YWxsb3dXcml0ZTogZmFsc2UsXG59O1xuXG5leHBvcnQgZGVmYXVsdCBjbGFzcyBQb2tlT2JzaWRpYW5QbHVnaW4gZXh0ZW5kcyBQbHVnaW4ge1xuXHRzZXR0aW5nczogUG9rZU9ic2lkaWFuU2V0dGluZ3MgPSBERUZBVUxUX1NFVFRJTkdTO1xuXHRwcml2YXRlIHNvY2tldDogV2ViU29ja2V0IHwgbnVsbCA9IG51bGw7XG5cdHByaXZhdGUgc3RhdHVzQmFySXRlbUVsOiBIVE1MRWxlbWVudCB8IG51bGwgPSBudWxsO1xuXHRwcml2YXRlIHNldHRpbmdzVGFiOiBQb2tlT2JzaWRpYW5TZXR0aW5nVGFiIHwgbnVsbCA9IG51bGw7XG5cdHByaXZhdGUgY29ubmVjdGlvblN0YXRlOiBDb25uZWN0aW9uU3RhdGUgPSBcImRpc2Nvbm5lY3RlZFwiO1xuXHRwcml2YXRlIHJlY29ubmVjdEF0dGVtcHRzID0gMDtcblx0cHJpdmF0ZSByZWNvbm5lY3RUaW1lcjogbnVtYmVyIHwgbnVsbCA9IG51bGw7XG5cdHByaXZhdGUgdW5sb2FkUmVxdWVzdGVkID0gZmFsc2U7XG5cblx0YXN5bmMgb25sb2FkKCk6IFByb21pc2U8dm9pZD4ge1xuXHRcdGF3YWl0IHRoaXMubG9hZFNldHRpbmdzKCk7XG5cblx0XHRpZiAoIXRoaXMuc2V0dGluZ3MuY29ubmVjdGlvblRva2VuKSB7XG5cdFx0XHR0aGlzLnNldHRpbmdzLmNvbm5lY3Rpb25Ub2tlbiA9IGdlbmVyYXRlQ29ubmVjdGlvblRva2VuKCk7XG5cdFx0XHRhd2FpdCB0aGlzLnNhdmVTZXR0aW5ncygpO1xuXHRcdH1cblxuXHRcdHRoaXMuc3RhdHVzQmFySXRlbUVsID0gdGhpcy5hZGRTdGF0dXNCYXJJdGVtKCk7XG5cdFx0dGhpcy5zdGF0dXNCYXJJdGVtRWwuYWRkQ2xhc3MoXCJwb2tlLXN0YXR1c1wiKTtcblx0XHR0aGlzLnNldENvbm5lY3Rpb25TdGF0ZShcImRpc2Nvbm5lY3RlZFwiKTtcblxuXHRcdHRoaXMuc2V0dGluZ3NUYWIgPSBuZXcgUG9rZU9ic2lkaWFuU2V0dGluZ1RhYih0aGlzLmFwcCwgdGhpcyk7XG5cdFx0dGhpcy5hZGRTZXR0aW5nVGFiKHRoaXMuc2V0dGluZ3NUYWIpO1xuXG5cdFx0dGhpcy5jb25uZWN0KCk7XG5cdH1cblxuXHRvbnVubG9hZCgpOiB2b2lkIHtcblx0XHR0aGlzLnVubG9hZFJlcXVlc3RlZCA9IHRydWU7XG5cdFx0dGhpcy5jbGVhclJlY29ubmVjdFRpbWVyKCk7XG5cdFx0dGhpcy5jbG9zZVNvY2tldCgpO1xuXHRcdHRoaXMuc2V0Q29ubmVjdGlvblN0YXRlKFwiZGlzY29ubmVjdGVkXCIpO1xuXHR9XG5cblx0YXN5bmMgbG9hZFNldHRpbmdzKCk6IFByb21pc2U8dm9pZD4ge1xuXHRcdGNvbnN0IHNhdmVkU2V0dGluZ3M6IHVua25vd24gPSBhd2FpdCB0aGlzLmxvYWREYXRhKCk7XG5cdFx0Y29uc3Qgc2V0dGluZ3NSZWNvcmQgPSBpc1JlY29yZChzYXZlZFNldHRpbmdzKSA/IHNhdmVkU2V0dGluZ3MgOiB7fTtcblxuXHRcdHRoaXMuc2V0dGluZ3MgPSB7XG5cdFx0XHRnYXRld2F5VXJsOiBnZXRPcHRpb25hbFN0cmluZyhzZXR0aW5nc1JlY29yZCwgXCJnYXRld2F5VXJsXCIsIERFRkFVTFRfR0FURVdBWV9VUkwpLFxuXHRcdFx0Y29ubmVjdGlvblRva2VuOiBnZXRPcHRpb25hbFN0cmluZyhzZXR0aW5nc1JlY29yZCwgXCJjb25uZWN0aW9uVG9rZW5cIiwgXCJcIiksXG5cdFx0XHR2YXVsdEZvbGRlcjogZ2V0T3B0aW9uYWxTdHJpbmcoc2V0dGluZ3NSZWNvcmQsIFwidmF1bHRGb2xkZXJcIiwgREVGQVVMVF9WQVVMVF9GT0xERVIpLFxuXHRcdFx0YWxsb3dXcml0ZTpcblx0XHRcdFx0dHlwZW9mIHNldHRpbmdzUmVjb3JkLmFsbG93V3JpdGUgPT09IFwiYm9vbGVhblwiID8gc2V0dGluZ3NSZWNvcmQuYWxsb3dXcml0ZSA6IERFRkFVTFRfU0VUVElOR1MuYWxsb3dXcml0ZSxcblx0XHR9O1xuXG5cdFx0aWYgKFxuXHRcdFx0dGhpcy5zZXR0aW5ncy5jb25uZWN0aW9uVG9rZW4gJiZcblx0XHRcdCFPYmplY3QucHJvdG90eXBlLmhhc093blByb3BlcnR5LmNhbGwoc2V0dGluZ3NSZWNvcmQsIFwidmF1bHRGb2xkZXJcIilcblx0XHQpIHtcblx0XHRcdHRoaXMuc2V0dGluZ3MudmF1bHRGb2xkZXIgPSBcIlwiO1xuXHRcdH1cblx0fVxuXG5cdGFzeW5jIHNhdmVTZXR0aW5ncygpOiBQcm9taXNlPHZvaWQ+IHtcblx0XHRhd2FpdCB0aGlzLnNhdmVEYXRhKHRoaXMuc2V0dGluZ3MpO1xuXHR9XG5cblx0Z2V0Q29ubmVjdGlvblN0YXRlKCk6IENvbm5lY3Rpb25TdGF0ZSB7XG5cdFx0cmV0dXJuIHRoaXMuY29ubmVjdGlvblN0YXRlO1xuXHR9XG5cblx0YXN5bmMgdXBkYXRlQ29ubmVjdGlvblRva2VuKHRva2VuOiBzdHJpbmcpOiBQcm9taXNlPHZvaWQ+IHtcblx0XHR0aGlzLnNldHRpbmdzLmNvbm5lY3Rpb25Ub2tlbiA9IHRva2VuLnRyaW0oKTtcblx0XHRhd2FpdCB0aGlzLnNhdmVTZXR0aW5ncygpO1xuXHRcdHRoaXMucmVjb25uZWN0Tm93KCk7XG5cdH1cblxuXHRhc3luYyB1cGRhdGVHYXRld2F5VXJsKGdhdGV3YXlVcmw6IHN0cmluZyk6IFByb21pc2U8dm9pZD4ge1xuXHRcdHRoaXMuc2V0dGluZ3MuZ2F0ZXdheVVybCA9IGdhdGV3YXlVcmwudHJpbSgpIHx8IERFRkFVTFRfR0FURVdBWV9VUkw7XG5cdFx0YXdhaXQgdGhpcy5zYXZlU2V0dGluZ3MoKTtcblx0XHR0aGlzLnJlY29ubmVjdE5vdygpO1xuXHR9XG5cblx0YXN5bmMgdXBkYXRlQWxsb3dXcml0ZShhbGxvd1dyaXRlOiBib29sZWFuKTogUHJvbWlzZTx2b2lkPiB7XG5cdFx0dGhpcy5zZXR0aW5ncy5hbGxvd1dyaXRlID0gYWxsb3dXcml0ZTtcblx0XHRhd2FpdCB0aGlzLnNhdmVTZXR0aW5ncygpO1xuXHR9XG5cblx0YXN5bmMgdXBkYXRlVmF1bHRGb2xkZXIodmF1bHRGb2xkZXI6IHN0cmluZyk6IFByb21pc2U8dm9pZD4ge1xuXHRcdHRoaXMuc2V0dGluZ3MudmF1bHRGb2xkZXIgPSB2YXVsdEZvbGRlci50cmltKCk7XG5cdFx0YXdhaXQgdGhpcy5zYXZlU2V0dGluZ3MoKTtcblx0fVxuXG5cdHJlY29ubmVjdE5vdygpOiB2b2lkIHtcblx0XHR0aGlzLmNsZWFyUmVjb25uZWN0VGltZXIoKTtcblx0XHR0aGlzLnJlY29ubmVjdEF0dGVtcHRzID0gMDtcblx0XHR0aGlzLmNsb3NlU29ja2V0KCk7XG5cblx0XHRpZiAoIXRoaXMuc2V0dGluZ3MuY29ubmVjdGlvblRva2VuKSB7XG5cdFx0XHR0aGlzLnNldENvbm5lY3Rpb25TdGF0ZShcImRpc2Nvbm5lY3RlZFwiKTtcblx0XHRcdHJldHVybjtcblx0XHR9XG5cblx0XHR0aGlzLmNvbm5lY3QoKTtcblx0fVxuXG5cdHByaXZhdGUgY29ubmVjdCgpOiB2b2lkIHtcblx0XHRpZiAodGhpcy51bmxvYWRSZXF1ZXN0ZWQgfHwgIXRoaXMuc2V0dGluZ3MuY29ubmVjdGlvblRva2VuKSB7XG5cdFx0XHR0aGlzLnNldENvbm5lY3Rpb25TdGF0ZShcImRpc2Nvbm5lY3RlZFwiKTtcblx0XHRcdHJldHVybjtcblx0XHR9XG5cblx0XHRpZiAoXG5cdFx0XHR0aGlzLnNvY2tldCAmJlxuXHRcdFx0KHRoaXMuc29ja2V0LnJlYWR5U3RhdGUgPT09IFdlYlNvY2tldC5DT05ORUNUSU5HIHx8IHRoaXMuc29ja2V0LnJlYWR5U3RhdGUgPT09IFdlYlNvY2tldC5PUEVOKVxuXHRcdCkge1xuXHRcdFx0cmV0dXJuO1xuXHRcdH1cblxuXHRcdHRoaXMuc2V0Q29ubmVjdGlvblN0YXRlKFwiY29ubmVjdGluZ1wiKTtcblxuXHRcdGNvbnN0IHVybCA9IHRoaXMuYnVpbGRHYXRld2F5VXJsKCk7XG5cblx0XHRpZiAoIXVybCkge1xuXHRcdFx0dGhpcy5zZXRDb25uZWN0aW9uU3RhdGUoXCJkaXNjb25uZWN0ZWRcIik7XG5cdFx0XHRyZXR1cm47XG5cdFx0fVxuXG5cdFx0dHJ5IHtcblx0XHRcdGNvbnNvbGUubG9nKGBQb2tlIEdhdGV3YXkgY29ubmVjdGluZyB0byAke3JlZGFjdFRva2VuKHVybCl9YCk7XG5cdFx0XHR0aGlzLnNvY2tldCA9IG5ldyBXZWJTb2NrZXQodXJsLnRvU3RyaW5nKCkpO1xuXHRcdH0gY2F0Y2ggKGVycm9yKSB7XG5cdFx0XHR0aGlzLmhhbmRsZUNvbm5lY3Rpb25FcnJvcihlcnJvcik7XG5cdFx0XHR0aGlzLnNjaGVkdWxlUmVjb25uZWN0KCk7XG5cdFx0XHRyZXR1cm47XG5cdFx0fVxuXG5cdFx0dGhpcy5zb2NrZXQub25vcGVuID0gKCkgPT4ge1xuXHRcdFx0Y29uc29sZS5sb2coXCJQb2tlIEdhdGV3YXkgY29ubmVjdGVkXCIpO1xuXHRcdFx0dGhpcy5yZWNvbm5lY3RBdHRlbXB0cyA9IDA7XG5cdFx0XHR0aGlzLnNldENvbm5lY3Rpb25TdGF0ZShcImNvbm5lY3RlZFwiKTtcblx0XHR9O1xuXG5cdFx0dGhpcy5zb2NrZXQub25tZXNzYWdlID0gKGV2ZW50OiBNZXNzYWdlRXZlbnQ8c3RyaW5nPikgPT4ge1xuXHRcdFx0dm9pZCB0aGlzLndpdGhSZXF1ZXN0VGltZW91dCh0aGlzLmhhbmRsZVNvY2tldE1lc3NhZ2UoZXZlbnQuZGF0YSksIG51bGwpO1xuXHRcdH07XG5cblx0XHR0aGlzLnNvY2tldC5vbmVycm9yID0gKGV2ZW50KSA9PiB7XG5cdFx0XHR0aGlzLmhhbmRsZUNvbm5lY3Rpb25FcnJvcihuZXcgRXJyb3IoYFdlYlNvY2tldCBjb25uZWN0aW9uIGVycm9yOiAke0pTT04uc3RyaW5naWZ5KGV2ZW50KX1gKSk7XG5cdFx0fTtcblxuXHRcdHRoaXMuc29ja2V0Lm9uY2xvc2UgPSAoZXZlbnQpID0+IHtcblx0XHRcdGNvbnNvbGUubG9nKGBQb2tlIEdhdGV3YXkgZGlzY29ubmVjdGVkOiBjb2RlPSR7ZXZlbnQuY29kZX0gcmVhc29uPSR7ZXZlbnQucmVhc29uIHx8IFwiKG5vbmUpXCJ9YCk7XG5cdFx0XHR0aGlzLnNvY2tldCA9IG51bGw7XG5cblx0XHRcdGlmICh0aGlzLnVubG9hZFJlcXVlc3RlZCkge1xuXHRcdFx0XHR0aGlzLnNldENvbm5lY3Rpb25TdGF0ZShcImRpc2Nvbm5lY3RlZFwiKTtcblx0XHRcdFx0cmV0dXJuO1xuXHRcdFx0fVxuXG5cdFx0XHR0aGlzLnNldENvbm5lY3Rpb25TdGF0ZShcImRpc2Nvbm5lY3RlZFwiKTtcblx0XHRcdHRoaXMuc2NoZWR1bGVSZWNvbm5lY3QoKTtcblx0XHR9O1xuXHR9XG5cblx0cHJpdmF0ZSBzY2hlZHVsZVJlY29ubmVjdCgpOiB2b2lkIHtcblx0XHRpZiAodGhpcy51bmxvYWRSZXF1ZXN0ZWQgfHwgIXRoaXMuc2V0dGluZ3MuY29ubmVjdGlvblRva2VuIHx8IHRoaXMucmVjb25uZWN0VGltZXIgIT09IG51bGwpIHtcblx0XHRcdHJldHVybjtcblx0XHR9XG5cblx0XHRjb25zdCBkZWxheSA9IE1hdGgubWluKEJBU0VfUkVDT05ORUNUX0RFTEFZX01TICogMiAqKiB0aGlzLnJlY29ubmVjdEF0dGVtcHRzLCBNQVhfUkVDT05ORUNUX0RFTEFZX01TKTtcblx0XHR0aGlzLnJlY29ubmVjdEF0dGVtcHRzICs9IDE7XG5cblx0XHR0aGlzLnJlY29ubmVjdFRpbWVyID0gd2luZG93LnNldFRpbWVvdXQoKCkgPT4ge1xuXHRcdFx0dGhpcy5yZWNvbm5lY3RUaW1lciA9IG51bGw7XG5cdFx0XHR0aGlzLmNvbm5lY3QoKTtcblx0XHR9LCBkZWxheSk7XG5cdH1cblxuXHRwcml2YXRlIGNsZWFyUmVjb25uZWN0VGltZXIoKTogdm9pZCB7XG5cdFx0aWYgKHRoaXMucmVjb25uZWN0VGltZXIgIT09IG51bGwpIHtcblx0XHRcdHdpbmRvdy5jbGVhclRpbWVvdXQodGhpcy5yZWNvbm5lY3RUaW1lcik7XG5cdFx0XHR0aGlzLnJlY29ubmVjdFRpbWVyID0gbnVsbDtcblx0XHR9XG5cdH1cblxuXHRwcml2YXRlIGNsb3NlU29ja2V0KCk6IHZvaWQge1xuXHRcdGlmICghdGhpcy5zb2NrZXQpIHtcblx0XHRcdHJldHVybjtcblx0XHR9XG5cblx0XHR0aGlzLnNvY2tldC5vbm9wZW4gPSBudWxsO1xuXHRcdHRoaXMuc29ja2V0Lm9ubWVzc2FnZSA9IG51bGw7XG5cdFx0dGhpcy5zb2NrZXQub25lcnJvciA9IG51bGw7XG5cdFx0dGhpcy5zb2NrZXQub25jbG9zZSA9IG51bGw7XG5cblx0XHRpZiAodGhpcy5zb2NrZXQucmVhZHlTdGF0ZSA9PT0gV2ViU29ja2V0LkNPTk5FQ1RJTkcgfHwgdGhpcy5zb2NrZXQucmVhZHlTdGF0ZSA9PT0gV2ViU29ja2V0Lk9QRU4pIHtcblx0XHRcdHRoaXMuc29ja2V0LmNsb3NlKCk7XG5cdFx0fVxuXG5cdFx0dGhpcy5zb2NrZXQgPSBudWxsO1xuXHR9XG5cblx0cHJpdmF0ZSBhc3luYyB3aXRoUmVxdWVzdFRpbWVvdXQodGFzazogUHJvbWlzZTx2b2lkPiwgaWQ6IHN0cmluZyB8IG51bGwpOiBQcm9taXNlPHZvaWQ+IHtcblx0XHRsZXQgdGltZW91dElkOiBudW1iZXIgfCBudWxsID0gbnVsbDtcblxuXHRcdGNvbnN0IHRpbWVvdXQgPSBuZXcgUHJvbWlzZTx2b2lkPigoX3Jlc29sdmUsIHJlamVjdCkgPT4ge1xuXHRcdFx0dGltZW91dElkID0gd2luZG93LnNldFRpbWVvdXQoKCkgPT4gcmVqZWN0KG5ldyBFcnJvcihcIlJlcXVlc3QgdGltZWQgb3V0XCIpKSwgUkVRVUVTVF9USU1FT1VUX01TKTtcblx0XHR9KTtcblxuXHRcdHRyeSB7XG5cdFx0XHRhd2FpdCBQcm9taXNlLnJhY2UoW3Rhc2ssIHRpbWVvdXRdKTtcblx0XHR9IGNhdGNoIChlcnJvcikge1xuXHRcdFx0dGhpcy5zZW5kUmVzcG9uc2Uoe1xuXHRcdFx0XHRpZCxcblx0XHRcdFx0c3RhdHVzOiBcImVycm9yXCIsXG5cdFx0XHRcdHBheWxvYWQ6IHsgZXJyb3I6IGVycm9yIGluc3RhbmNlb2YgRXJyb3IgPyBlcnJvci5tZXNzYWdlIDogU3RyaW5nKGVycm9yKSB9LFxuXHRcdFx0fSk7XG5cdFx0fSBmaW5hbGx5IHtcblx0XHRcdGlmICh0aW1lb3V0SWQgIT09IG51bGwpIHtcblx0XHRcdFx0d2luZG93LmNsZWFyVGltZW91dCh0aW1lb3V0SWQpO1xuXHRcdFx0fVxuXHRcdH1cblx0fVxuXG5cdHByaXZhdGUgYXN5bmMgaGFuZGxlU29ja2V0TWVzc2FnZShyYXdNZXNzYWdlOiBzdHJpbmcpOiBQcm9taXNlPHZvaWQ+IHtcblx0XHRsZXQgaW5jb21pbmc6IEluY29taW5nTWVzc2FnZTtcblxuXHRcdHRyeSB7XG5cdFx0XHRpbmNvbWluZyA9IEpTT04ucGFyc2UocmF3TWVzc2FnZSkgYXMgSW5jb21pbmdNZXNzYWdlO1xuXHRcdH0gY2F0Y2gge1xuXHRcdFx0dGhpcy5zZW5kUmVzcG9uc2Uoe1xuXHRcdFx0XHRpZDogbnVsbCxcblx0XHRcdFx0c3RhdHVzOiBcImVycm9yXCIsXG5cdFx0XHRcdHBheWxvYWQ6IHsgZXJyb3I6IFwiSW52YWxpZCBKU09OIG1lc3NhZ2VcIiB9LFxuXHRcdFx0fSk7XG5cdFx0XHRyZXR1cm47XG5cdFx0fVxuXG5cdFx0Y29uc3QgaWQgPSB0eXBlb2YgaW5jb21pbmcuaWQgPT09IFwic3RyaW5nXCIgPyBpbmNvbWluZy5pZCA6IG51bGw7XG5cdFx0Y29uc3QgYWN0aW9uID0gdHlwZW9mIGluY29taW5nLmFjdGlvbiA9PT0gXCJzdHJpbmdcIiA/IGluY29taW5nLmFjdGlvbiA6IFwiXCI7XG5cdFx0Y29uc3QgcGFyYW1zID0gaXNSZWNvcmQoaW5jb21pbmcucGFyYW1zKSA/IGluY29taW5nLnBhcmFtcyA6IHt9O1xuXG5cdFx0aWYgKCFpZCkge1xuXHRcdFx0dGhpcy5zZW5kUmVzcG9uc2Uoe1xuXHRcdFx0XHRpZCxcblx0XHRcdFx0c3RhdHVzOiBcImVycm9yXCIsXG5cdFx0XHRcdHBheWxvYWQ6IHsgZXJyb3I6IFwiTWVzc2FnZSBpZCBpcyByZXF1aXJlZFwiIH0sXG5cdFx0XHR9KTtcblx0XHRcdHJldHVybjtcblx0XHR9XG5cblx0XHR0cnkge1xuXHRcdFx0Y29uc3QgcGF5bG9hZCA9IGF3YWl0IHRoaXMuaGFuZGxlQWN0aW9uKGFjdGlvbiwgcGFyYW1zKTtcblx0XHRcdHRoaXMuc2VuZFJlc3BvbnNlKHsgaWQsIHN0YXR1czogXCJzdWNjZXNzXCIsIHBheWxvYWQgfSk7XG5cdFx0fSBjYXRjaCAoZXJyb3IpIHtcblx0XHRcdHRoaXMuc2VuZFJlc3BvbnNlKHtcblx0XHRcdFx0aWQsXG5cdFx0XHRcdHN0YXR1czogXCJlcnJvclwiLFxuXHRcdFx0XHRwYXlsb2FkOiB7IGVycm9yOiBlcnJvciBpbnN0YW5jZW9mIEVycm9yID8gZXJyb3IubWVzc2FnZSA6IFN0cmluZyhlcnJvcikgfSxcblx0XHRcdH0pO1xuXHRcdH1cblx0fVxuXG5cdHByaXZhdGUgYXN5bmMgaGFuZGxlQWN0aW9uKGFjdGlvbjogc3RyaW5nLCBwYXJhbXM6IFJlY29yZDxzdHJpbmcsIHVua25vd24+KTogUHJvbWlzZTxSZWNvcmQ8c3RyaW5nLCB1bmtub3duPj4ge1xuXHRcdHN3aXRjaCAoYWN0aW9uKSB7XG5cdFx0XHRjYXNlIFwibGlzdFwiOlxuXHRcdFx0Y2FzZSBcImxpc3RfZmlsZXNcIjpcblx0XHRcdFx0cmV0dXJuIHRoaXMubGlzdEZpbGVzKCk7XG5cdFx0XHRjYXNlIFwicmVhZFwiOlxuXHRcdFx0Y2FzZSBcInJlYWRfZmlsZVwiOlxuXHRcdFx0XHRyZXR1cm4gdGhpcy5yZWFkRmlsZShwYXJhbXMpO1xuXHRcdFx0Y2FzZSBcIndyaXRlXCI6XG5cdFx0XHRjYXNlIFwid3JpdGVfZmlsZVwiOlxuXHRcdFx0XHRyZXR1cm4gdGhpcy53cml0ZUZpbGUocGFyYW1zKTtcblx0XHRcdGNhc2UgXCJzZWFyY2hcIjpcblx0XHRcdGNhc2UgXCJzZWFyY2hfdmF1bHRcIjpcblx0XHRcdFx0cmV0dXJuIHRoaXMuc2VhcmNoVmF1bHQocGFyYW1zKTtcblx0XHRcdGRlZmF1bHQ6XG5cdFx0XHRcdHRocm93IG5ldyBFcnJvcihgVW5zdXBwb3J0ZWQgYWN0aW9uOiAke2FjdGlvbiB8fCBcIihtaXNzaW5nKVwifWApO1xuXHRcdH1cblx0fVxuXG5cdHByaXZhdGUgYXN5bmMgbGlzdEZpbGVzKCk6IFByb21pc2U8UmVjb3JkPHN0cmluZywgdW5rbm93bj4+IHtcblx0XHRyZXR1cm4ge1xuXHRcdFx0ZmlsZXM6IGF3YWl0IHRoaXMubGlzdE1hcmtkb3duUGF0aHMoKSxcblx0XHR9O1xuXHR9XG5cblx0cHJpdmF0ZSBhc3luYyByZWFkRmlsZShwYXJhbXM6IFJlY29yZDxzdHJpbmcsIHVua25vd24+KTogUHJvbWlzZTxSZWNvcmQ8c3RyaW5nLCB1bmtub3duPj4ge1xuXHRcdGNvbnN0IHBhdGggPSBnZXRSZXF1aXJlZFN0cmluZyhwYXJhbXMsIFwicGF0aFwiKTtcblx0XHRjb25zdCBub3JtYWxpemVkUGF0aCA9IHRoaXMubm9ybWFsaXplTWFya2Rvd25QYXRoKHBhdGgpO1xuXHRcdHRoaXMuZW5zdXJlUGF0aEluVmF1bHRGb2xkZXIobm9ybWFsaXplZFBhdGgpO1xuXG5cdFx0Y29uc3QgZmlsZSA9IHRoaXMuZ2V0TWFya2Rvd25GaWxlKG5vcm1hbGl6ZWRQYXRoKTtcblx0XHRjb25zdCBjb250ZW50ID0gYXdhaXQgdGhpcy5hcHAudmF1bHQuYWRhcHRlci5yZWFkKGZpbGUucGF0aCk7XG5cblx0XHRyZXR1cm4ge1xuXHRcdFx0cGF0aDogZmlsZS5wYXRoLFxuXHRcdFx0Y29udGVudCxcblx0XHR9O1xuXHR9XG5cblx0cHJpdmF0ZSBhc3luYyB3cml0ZUZpbGUocGFyYW1zOiBSZWNvcmQ8c3RyaW5nLCB1bmtub3duPik6IFByb21pc2U8UmVjb3JkPHN0cmluZywgdW5rbm93bj4+IHtcblx0XHRpZiAoIXRoaXMuc2V0dGluZ3MuYWxsb3dXcml0ZSkge1xuXHRcdFx0dGhyb3cgbmV3IEVycm9yKFwiV3JpdGUgYWNjZXNzIGlzIGRpc2FibGVkIGluIFBva2UgR2F0ZXdheSBzZXR0aW5nc1wiKTtcblx0XHR9XG5cblx0XHRjb25zdCBwYXRoID0gZ2V0UmVxdWlyZWRTdHJpbmcocGFyYW1zLCBcInBhdGhcIik7XG5cdFx0Y29uc3QgY29udGVudCA9IGdldFJlcXVpcmVkU3RyaW5nKHBhcmFtcywgXCJjb250ZW50XCIpO1xuXHRcdGNvbnN0IG5vcm1hbGl6ZWRQYXRoID0gdGhpcy5ub3JtYWxpemVNYXJrZG93blBhdGgocGF0aCk7XG5cdFx0dGhpcy5lbnN1cmVQYXRoSW5WYXVsdEZvbGRlcihub3JtYWxpemVkUGF0aCk7XG5cblx0XHRhd2FpdCB0aGlzLmVuc3VyZVBhcmVudEZvbGRlcnMobm9ybWFsaXplZFBhdGgpO1xuXHRcdGF3YWl0IHRoaXMuYXBwLnZhdWx0LmFkYXB0ZXIud3JpdGUobm9ybWFsaXplZFBhdGgsIGNvbnRlbnQpO1xuXG5cdFx0cmV0dXJuIHtcblx0XHRcdHBhdGg6IG5vcm1hbGl6ZWRQYXRoLFxuXHRcdFx0Ynl0ZXM6IG5ldyBUZXh0RW5jb2RlcigpLmVuY29kZShjb250ZW50KS5sZW5ndGgsXG5cdFx0fTtcblx0fVxuXG5cdHByaXZhdGUgYXN5bmMgc2VhcmNoVmF1bHQocGFyYW1zOiBSZWNvcmQ8c3RyaW5nLCB1bmtub3duPik6IFByb21pc2U8UmVjb3JkPHN0cmluZywgdW5rbm93bj4+IHtcblx0XHRjb25zdCBxdWVyeSA9IGdldFJlcXVpcmVkU3RyaW5nKHBhcmFtcywgXCJxdWVyeVwiKS50cmltKCk7XG5cblx0XHRpZiAoIXF1ZXJ5KSB7XG5cdFx0XHR0aHJvdyBuZXcgRXJyb3IoXCJTZWFyY2ggcXVlcnkgY2Fubm90IGJlIGVtcHR5XCIpO1xuXHRcdH1cblxuXHRcdGNvbnN0IG5vcm1hbGl6ZWRRdWVyeSA9IHF1ZXJ5LnRvTG9jYWxlTG93ZXJDYXNlKCk7XG5cdFx0Y29uc3QgbWF0Y2hlczogU2VhcmNoTWF0Y2hbXSA9IFtdO1xuXG5cdFx0Zm9yIChjb25zdCBwYXRoIG9mIGF3YWl0IHRoaXMubGlzdE1hcmtkb3duUGF0aHMoKSkge1xuXHRcdFx0Y29uc3QgY29udGVudCA9IGF3YWl0IHRoaXMuYXBwLnZhdWx0LmFkYXB0ZXIucmVhZChwYXRoKTtcblx0XHRcdGNvbnN0IGluZGV4ID0gY29udGVudC50b0xvY2FsZUxvd2VyQ2FzZSgpLmluZGV4T2Yobm9ybWFsaXplZFF1ZXJ5KTtcblxuXHRcdFx0aWYgKGluZGV4ID09PSAtMSkge1xuXHRcdFx0XHRjb250aW51ZTtcblx0XHRcdH1cblxuXHRcdFx0bWF0Y2hlcy5wdXNoKHtcblx0XHRcdFx0cGF0aCxcblx0XHRcdFx0c25pcHBldDogbWFrZVNuaXBwZXQoY29udGVudCwgaW5kZXgsIHF1ZXJ5Lmxlbmd0aCksXG5cdFx0XHR9KTtcblx0XHR9XG5cblx0XHRyZXR1cm4geyBtYXRjaGVzIH07XG5cdH1cblxuXHRwcml2YXRlIGdldE1hcmtkb3duRmlsZShwYXRoOiBzdHJpbmcpOiBURmlsZSB7XG5cdFx0Y29uc3QgZmlsZSA9IHRoaXMuYXBwLnZhdWx0LmdldEFic3RyYWN0RmlsZUJ5UGF0aChwYXRoKTtcblxuXHRcdGlmICghKGZpbGUgaW5zdGFuY2VvZiBURmlsZSkpIHtcblx0XHRcdHRocm93IG5ldyBFcnJvcihgRmlsZSBub3QgZm91bmQ6ICR7cGF0aH1gKTtcblx0XHR9XG5cblx0XHRpZiAoZmlsZS5leHRlbnNpb24gIT09IFwibWRcIikge1xuXHRcdFx0dGhyb3cgbmV3IEVycm9yKGBPbmx5IG1hcmtkb3duIGZpbGVzIGFyZSBzdXBwb3J0ZWQ6ICR7cGF0aH1gKTtcblx0XHR9XG5cblx0XHRyZXR1cm4gZmlsZTtcblx0fVxuXG5cdHByaXZhdGUgbm9ybWFsaXplTWFya2Rvd25QYXRoKHBhdGg6IHN0cmluZyk6IHN0cmluZyB7XG5cdFx0Y29uc3Qgbm9ybWFsaXplZFBhdGggPSBwYXRoLnRyaW0oKS5yZXBsYWNlKC9eXFwvKy8sIFwiXCIpO1xuXG5cdFx0aWYgKCFub3JtYWxpemVkUGF0aCkge1xuXHRcdFx0dGhyb3cgbmV3IEVycm9yKFwiUGF0aCBjYW5ub3QgYmUgZW1wdHlcIik7XG5cdFx0fVxuXG5cdFx0Y29uc3QgcGFydHMgPSBub3JtYWxpemVkUGF0aC5zcGxpdChcIi9cIikuZmlsdGVyKEJvb2xlYW4pO1xuXG5cdFx0aWYgKHBhcnRzLmluY2x1ZGVzKFwiLi5cIikgfHwgcGFydHMuc29tZSgocGFydCkgPT4gcGFydCA9PT0gXCIuXCIpKSB7XG5cdFx0XHR0aHJvdyBuZXcgRXJyb3IoXCJQYXRoIGNhbm5vdCBjb250YWluIHBhcmVudCBvciBjdXJyZW50LWRpcmVjdG9yeSBzZWdtZW50c1wiKTtcblx0XHR9XG5cblx0XHRpZiAoIW5vcm1hbGl6ZWRQYXRoLnRvTG9jYWxlTG93ZXJDYXNlKCkuZW5kc1dpdGgoXCIubWRcIikpIHtcblx0XHRcdHRocm93IG5ldyBFcnJvcihcIk9ubHkgbWFya2Rvd24gZmlsZSBwYXRocyBlbmRpbmcgaW4gLm1kIGFyZSBzdXBwb3J0ZWRcIik7XG5cdFx0fVxuXG5cdFx0cmV0dXJuIHBhcnRzLmpvaW4oXCIvXCIpO1xuXHR9XG5cblx0cHJpdmF0ZSBnZXRWYXVsdEZvbGRlcigpOiBzdHJpbmcge1xuXHRcdHJldHVybiBub3JtYWxpemVWYXVsdEZvbGRlcih0aGlzLnNldHRpbmdzLnZhdWx0Rm9sZGVyKTtcblx0fVxuXG5cdHByaXZhdGUgZW5zdXJlUGF0aEluVmF1bHRGb2xkZXIocGF0aDogc3RyaW5nKTogdm9pZCB7XG5cdFx0Y29uc3QgZm9sZGVyID0gdGhpcy5nZXRWYXVsdEZvbGRlcigpO1xuXG5cdFx0aWYgKGZvbGRlciAmJiAhcGF0aC5zdGFydHNXaXRoKGAke2ZvbGRlcn0vYCkpIHtcblx0XHRcdHRocm93IG5ldyBFcnJvcihgUGF0aCBpcyBvdXRzaWRlIHRoZSBjb25maWd1cmVkIHZhdWx0IGFjY2VzcyBmb2xkZXI6ICR7Zm9sZGVyfWApO1xuXHRcdH1cblx0fVxuXG5cdHByaXZhdGUgYXN5bmMgbGlzdE1hcmtkb3duUGF0aHMoKTogUHJvbWlzZTxzdHJpbmdbXT4ge1xuXHRcdGNvbnN0IGZvbGRlciA9IHRoaXMuZ2V0VmF1bHRGb2xkZXIoKTtcblxuXHRcdGlmIChmb2xkZXIgJiYgIShhd2FpdCB0aGlzLmFwcC52YXVsdC5hZGFwdGVyLmV4aXN0cyhmb2xkZXIpKSkge1xuXHRcdFx0cmV0dXJuIFtdO1xuXHRcdH1cblxuXHRcdGNvbnN0IHBhdGhzID0gYXdhaXQgdGhpcy5saXN0TWFya2Rvd25QYXRoc0luRm9sZGVyKGZvbGRlcik7XG5cdFx0cmV0dXJuIHBhdGhzLnNvcnQoKGEsIGIpID0+IGEubG9jYWxlQ29tcGFyZShiKSk7XG5cdH1cblxuXHRwcml2YXRlIGFzeW5jIGxpc3RNYXJrZG93blBhdGhzSW5Gb2xkZXIoZm9sZGVyOiBzdHJpbmcpOiBQcm9taXNlPHN0cmluZ1tdPiB7XG5cdFx0Y29uc3QgbGlzdGluZyA9IGF3YWl0IHRoaXMuYXBwLnZhdWx0LmFkYXB0ZXIubGlzdChmb2xkZXIpO1xuXHRcdGNvbnN0IHBhdGhzID0gbGlzdGluZy5maWxlcy5maWx0ZXIoaXNNYXJrZG93blBhdGgpO1xuXG5cdFx0Zm9yIChjb25zdCBjaGlsZEZvbGRlciBvZiBsaXN0aW5nLmZvbGRlcnMpIHtcblx0XHRcdHBhdGhzLnB1c2goLi4uKGF3YWl0IHRoaXMubGlzdE1hcmtkb3duUGF0aHNJbkZvbGRlcihjaGlsZEZvbGRlcikpKTtcblx0XHR9XG5cblx0XHRyZXR1cm4gcGF0aHM7XG5cdH1cblxuXHRwcml2YXRlIGFzeW5jIGVuc3VyZVBhcmVudEZvbGRlcnMoZmlsZVBhdGg6IHN0cmluZyk6IFByb21pc2U8dm9pZD4ge1xuXHRcdGNvbnN0IHBhcmVudFBhcnRzID0gZmlsZVBhdGguc3BsaXQoXCIvXCIpLnNsaWNlKDAsIC0xKTtcblx0XHRsZXQgY3VycmVudFBhdGggPSBcIlwiO1xuXG5cdFx0Zm9yIChjb25zdCBwYXJ0IG9mIHBhcmVudFBhcnRzKSB7XG5cdFx0XHRjdXJyZW50UGF0aCA9IGN1cnJlbnRQYXRoID8gYCR7Y3VycmVudFBhdGh9LyR7cGFydH1gIDogcGFydDtcblxuXHRcdFx0aWYgKCEoYXdhaXQgdGhpcy5hcHAudmF1bHQuYWRhcHRlci5leGlzdHMoY3VycmVudFBhdGgpKSkge1xuXHRcdFx0XHRhd2FpdCB0aGlzLmFwcC52YXVsdC5hZGFwdGVyLm1rZGlyKGN1cnJlbnRQYXRoKTtcblx0XHRcdH1cblx0XHR9XG5cdH1cblxuXHRwcml2YXRlIGJ1aWxkR2F0ZXdheVVybCgpOiBVUkwgfCBudWxsIHtcblx0XHRsZXQgdXJsOiBVUkw7XG5cblx0XHR0cnkge1xuXHRcdFx0dXJsID0gbmV3IFVSTCh0aGlzLnNldHRpbmdzLmdhdGV3YXlVcmwgfHwgREVGQVVMVF9HQVRFV0FZX1VSTCk7XG5cdFx0fSBjYXRjaCB7XG5cdFx0XHRuZXcgTm90aWNlKFwiSW52YWxpZCBQb2tlIEdhdGV3YXkgVVJMXCIpO1xuXHRcdFx0cmV0dXJuIG51bGw7XG5cdFx0fVxuXG5cdFx0aWYgKHVybC5wcm90b2NvbCAhPT0gXCJ3czpcIiAmJiB1cmwucHJvdG9jb2wgIT09IFwid3NzOlwiKSB7XG5cdFx0XHRuZXcgTm90aWNlKFwiUG9rZSBHYXRld2F5IFVSTCBtdXN0IHN0YXJ0IHdpdGggd3M6Ly8gb3Igd3NzOi8vXCIpO1xuXHRcdFx0cmV0dXJuIG51bGw7XG5cdFx0fVxuXG5cdFx0dXJsLnNlYXJjaFBhcmFtcy5zZXQoXCJ0b2tlblwiLCB0aGlzLnNldHRpbmdzLmNvbm5lY3Rpb25Ub2tlbik7XG5cdFx0dXJsLnNlYXJjaFBhcmFtcy5zZXQoXCJwbHVnaW5cIiwgdGhpcy5tYW5pZmVzdC5pZCk7XG5cdFx0dXJsLnNlYXJjaFBhcmFtcy5zZXQoXCJ2ZXJzaW9uXCIsIHRoaXMubWFuaWZlc3QudmVyc2lvbik7XG5cblx0XHRyZXR1cm4gdXJsO1xuXHR9XG5cblx0cHJpdmF0ZSBzZW5kUmVzcG9uc2UocmVzcG9uc2U6IFJlc3BvbnNlTWVzc2FnZSk6IHZvaWQge1xuXHRcdGlmICghdGhpcy5zb2NrZXQgfHwgdGhpcy5zb2NrZXQucmVhZHlTdGF0ZSAhPT0gV2ViU29ja2V0Lk9QRU4pIHtcblx0XHRcdHJldHVybjtcblx0XHR9XG5cblx0XHR0aGlzLnNvY2tldC5zZW5kKEpTT04uc3RyaW5naWZ5KHJlc3BvbnNlKSk7XG5cdH1cblxuXHRwcml2YXRlIHNldENvbm5lY3Rpb25TdGF0ZShzdGF0ZTogQ29ubmVjdGlvblN0YXRlKTogdm9pZCB7XG5cdFx0dGhpcy5jb25uZWN0aW9uU3RhdGUgPSBzdGF0ZTtcblxuXHRcdGlmICh0aGlzLnN0YXR1c0Jhckl0ZW1FbCkge1xuXHRcdFx0dGhpcy5zdGF0dXNCYXJJdGVtRWwuc2V0VGV4dChgUG9rZTogJHtjYXBpdGFsaXplKHN0YXRlKX1gKTtcblx0XHRcdHRoaXMuc3RhdHVzQmFySXRlbUVsLnJlbW92ZUNsYXNzKFwiaXMtY29ubmVjdGVkXCIsIFwiaXMtY29ubmVjdGluZ1wiLCBcImlzLWRpc2Nvbm5lY3RlZFwiKTtcblx0XHRcdHRoaXMuc3RhdHVzQmFySXRlbUVsLmFkZENsYXNzKGBpcy0ke3N0YXRlfWApO1xuXHRcdH1cblxuXHRcdHRoaXMuc2V0dGluZ3NUYWI/LnVwZGF0ZVN0YXR1cyhzdGF0ZSk7XG5cdH1cblxuXHRwcml2YXRlIGhhbmRsZUNvbm5lY3Rpb25FcnJvcihlcnJvcjogdW5rbm93bik6IHZvaWQge1xuXHRcdGNvbnNvbGUuZXJyb3IoXCJQb2tlIEdhdGV3YXkgY29ubmVjdGlvbiBlcnJvclwiLCBlcnJvcik7XG5cdH1cbn1cblxuY2xhc3MgUG9rZU9ic2lkaWFuU2V0dGluZ1RhYiBleHRlbmRzIFBsdWdpblNldHRpbmdUYWIge1xuXHRwcml2YXRlIHBsdWdpbjogUG9rZU9ic2lkaWFuUGx1Z2luO1xuXHRwcml2YXRlIHN0YXR1c0VsOiBIVE1MRWxlbWVudCB8IG51bGwgPSBudWxsO1xuXG5cdGNvbnN0cnVjdG9yKGFwcDogQXBwLCBwbHVnaW46IFBva2VPYnNpZGlhblBsdWdpbikge1xuXHRcdHN1cGVyKGFwcCwgcGx1Z2luKTtcblx0XHR0aGlzLnBsdWdpbiA9IHBsdWdpbjtcblx0fVxuXG5cdGdldFNldHRpbmdEZWZpbml0aW9ucygpOiBTZXR0aW5nRGVmaW5pdGlvbkl0ZW1bXSB7XG5cdFx0cmV0dXJuIFtcblx0XHRcdHtcblx0XHRcdFx0dHlwZTogXCJncm91cFwiLFxuXHRcdFx0XHRoZWFkaW5nOiBcIlBva2UgR2F0ZXdheVwiLFxuXHRcdFx0XHRpdGVtczogW1xuXHRcdFx0XHRcdHtcblx0XHRcdFx0XHRcdG5hbWU6IFwiR2F0ZXdheSBVUkxcIixcblx0XHRcdFx0XHRcdGRlc2M6IFwiV2ViU29ja2V0IGVuZHBvaW50IHVzZWQgdG8gY29ubmVjdCB0aGlzIHZhdWx0IHRvIFBva2UuXCIsXG5cdFx0XHRcdFx0XHRyZW5kZXI6IChzZXR0aW5nKSA9PiB0aGlzLnJlbmRlckdhdGV3YXlVcmxTZXR0aW5nKHNldHRpbmcpLFxuXHRcdFx0XHRcdH0sXG5cdFx0XHRcdFx0e1xuXHRcdFx0XHRcdFx0bmFtZTogXCJWYXVsdCBhY2Nlc3MgZm9sZGVyXCIsXG5cdFx0XHRcdFx0XHRkZXNjOiBcIkxpbWl0IFBva2UgdG8gbWFya2Rvd24gZmlsZXMgaW4gdGhpcyBmb2xkZXIuIExlYXZlIGJsYW5rIHRvIGFsbG93IGFsbCBtYXJrZG93biBmaWxlcy5cIixcblx0XHRcdFx0XHRcdHJlbmRlcjogKHNldHRpbmcpID0+IHRoaXMucmVuZGVyVmF1bHRGb2xkZXJTZXR0aW5nKHNldHRpbmcpLFxuXHRcdFx0XHRcdH0sXG5cdFx0XHRcdFx0e1xuXHRcdFx0XHRcdFx0bmFtZTogXCJDb25uZWN0aW9uIHRva2VuXCIsXG5cdFx0XHRcdFx0XHRkZXNjOiBcIlBhc3RlIHRoaXMgdG9rZW4gaW50byBQb2tlJ3MgQWRkIEtleSBmaWVsZCBmb3IgdGhlIE9ic2lkaWFuIHJlY2lwZS5cIixcblx0XHRcdFx0XHRcdHJlbmRlcjogKHNldHRpbmcpID0+IHRoaXMucmVuZGVyQ29ubmVjdGlvblRva2VuU2V0dGluZyhzZXR0aW5nKSxcblx0XHRcdFx0XHR9LFxuXHRcdFx0XHRcdHtcblx0XHRcdFx0XHRcdG5hbWU6IFwiQWxsb3cgd3JpdGVzXCIsXG5cdFx0XHRcdFx0XHRkZXNjOiBcIkFsbG93IFBva2UgdG8gY3JlYXRlIG9yIG92ZXJ3cml0ZSBtYXJrZG93biBmaWxlcyBpbiB0aGlzIHZhdWx0LlwiLFxuXHRcdFx0XHRcdFx0cmVuZGVyOiAoc2V0dGluZykgPT4gdGhpcy5yZW5kZXJBbGxvd1dyaXRlU2V0dGluZyhzZXR0aW5nKSxcblx0XHRcdFx0XHR9LFxuXHRcdFx0XHRcdHtcblx0XHRcdFx0XHRcdG5hbWU6IFwiQ29ubmVjdGlvbiBzdGF0dXNcIixcblx0XHRcdFx0XHRcdGRlc2M6IFwiQ3VycmVudCBnYXRld2F5IGNvbm5lY3Rpb24gc3RhdGUuXCIsXG5cdFx0XHRcdFx0XHRyZW5kZXI6IChzZXR0aW5nKSA9PiB0aGlzLnJlbmRlckNvbm5lY3Rpb25TdGF0dXNTZXR0aW5nKHNldHRpbmcpLFxuXHRcdFx0XHRcdH0sXG5cdFx0XHRcdF0sXG5cdFx0XHR9LFxuXHRcdF07XG5cdH1cblxuXHRwcml2YXRlIHJlbmRlckdhdGV3YXlVcmxTZXR0aW5nKHNldHRpbmc6IFNldHRpbmcpOiB2b2lkIHtcblx0XHRzZXR0aW5nLmFkZFRleHQoKHRleHQpID0+IHtcblx0XHRcdHRleHRcblx0XHRcdFx0LnNldFBsYWNlaG9sZGVyKERFRkFVTFRfR0FURVdBWV9VUkwpXG5cdFx0XHRcdC5zZXRWYWx1ZSh0aGlzLnBsdWdpbi5zZXR0aW5ncy5nYXRld2F5VXJsIHx8IERFRkFVTFRfR0FURVdBWV9VUkwpXG5cdFx0XHRcdC5vbkNoYW5nZShhc3luYyAodmFsdWUpID0+IHtcblx0XHRcdFx0XHRhd2FpdCB0aGlzLnBsdWdpbi51cGRhdGVHYXRld2F5VXJsKHZhbHVlKTtcblx0XHRcdFx0fSk7XG5cdFx0fSk7XG5cdH1cblxuXHRwcml2YXRlIHJlbmRlclZhdWx0Rm9sZGVyU2V0dGluZyhzZXR0aW5nOiBTZXR0aW5nKTogdm9pZCB7XG5cdFx0c2V0dGluZy5hZGRUZXh0KCh0ZXh0KSA9PiB7XG5cdFx0XHR0ZXh0XG5cdFx0XHRcdC5zZXRQbGFjZWhvbGRlcihERUZBVUxUX1ZBVUxUX0ZPTERFUilcblx0XHRcdFx0LnNldFZhbHVlKHRoaXMucGx1Z2luLnNldHRpbmdzLnZhdWx0Rm9sZGVyKVxuXHRcdFx0XHQub25DaGFuZ2UoYXN5bmMgKHZhbHVlKSA9PiB7XG5cdFx0XHRcdFx0YXdhaXQgdGhpcy5wbHVnaW4udXBkYXRlVmF1bHRGb2xkZXIodmFsdWUpO1xuXHRcdFx0XHR9KTtcblx0XHR9KTtcblx0fVxuXG5cdHByaXZhdGUgcmVuZGVyQ29ubmVjdGlvblRva2VuU2V0dGluZyhzZXR0aW5nOiBTZXR0aW5nKTogdm9pZCB7XG5cdFx0bGV0IHRva2VuSW5wdXQ6IEhUTUxJbnB1dEVsZW1lbnQgfCBudWxsID0gbnVsbDtcblxuXHRcdHNldHRpbmdcblx0XHRcdC5hZGRUZXh0KCh0ZXh0KSA9PiB7XG5cdFx0XHRcdHRleHRcblx0XHRcdFx0XHQuc2V0UGxhY2Vob2xkZXIoXCJQYXN0ZSB0b2tlblwiKVxuXHRcdFx0XHRcdC5zZXRWYWx1ZSh0aGlzLnBsdWdpbi5zZXR0aW5ncy5jb25uZWN0aW9uVG9rZW4pXG5cdFx0XHRcdFx0Lm9uQ2hhbmdlKGFzeW5jICh2YWx1ZSkgPT4ge1xuXHRcdFx0XHRcdFx0YXdhaXQgdGhpcy5wbHVnaW4udXBkYXRlQ29ubmVjdGlvblRva2VuKHZhbHVlKTtcblx0XHRcdFx0XHR9KTtcblxuXHRcdFx0XHR0ZXh0LmlucHV0RWwudHlwZSA9IFwicGFzc3dvcmRcIjtcblx0XHRcdFx0dG9rZW5JbnB1dCA9IHRleHQuaW5wdXRFbDtcblx0XHRcdH0pXG5cdFx0XHQuYWRkRXh0cmFCdXR0b24oKGJ1dHRvbikgPT4ge1xuXHRcdFx0XHRidXR0b25cblx0XHRcdFx0XHQuc2V0SWNvbihcInRleHQtY3Vyc29yLWlucHV0XCIpXG5cdFx0XHRcdFx0LnNldFRvb2x0aXAoXCJSZXZlYWwgYW5kIHNlbGVjdCB0b2tlblwiKVxuXHRcdFx0XHRcdC5vbkNsaWNrKCgpID0+IHtcblx0XHRcdFx0XHRcdGlmICghdG9rZW5JbnB1dCkge1xuXHRcdFx0XHRcdFx0XHRyZXR1cm47XG5cdFx0XHRcdFx0XHR9XG5cblx0XHRcdFx0XHRcdHRva2VuSW5wdXQudHlwZSA9IFwidGV4dFwiO1xuXHRcdFx0XHRcdFx0dG9rZW5JbnB1dC5mb2N1cygpO1xuXHRcdFx0XHRcdFx0dG9rZW5JbnB1dC5zZWxlY3QoKTtcblx0XHRcdFx0XHRcdG5ldyBOb3RpY2UoXCJQb2tlIEdhdGV3YXkgdG9rZW4gc2VsZWN0ZWRcIik7XG5cdFx0XHRcdFx0fSk7XG5cdFx0XHR9KVxuXHRcdFx0LmFkZEV4dHJhQnV0dG9uKChidXR0b24pID0+IHtcblx0XHRcdFx0YnV0dG9uXG5cdFx0XHRcdFx0LnNldEljb24oXCJyZWZyZXNoLWN3XCIpXG5cdFx0XHRcdFx0LnNldFRvb2x0aXAoXCJHZW5lcmF0ZSBuZXcgdG9rZW5cIilcblx0XHRcdFx0XHQub25DbGljayhhc3luYyAoKSA9PiB7XG5cdFx0XHRcdFx0XHRhd2FpdCB0aGlzLnBsdWdpbi51cGRhdGVDb25uZWN0aW9uVG9rZW4oZ2VuZXJhdGVDb25uZWN0aW9uVG9rZW4oKSk7XG5cdFx0XHRcdFx0XHR0aGlzLnVwZGF0ZSgpO1xuXHRcdFx0XHRcdFx0bmV3IE5vdGljZShcIkdlbmVyYXRlZCBhIG5ldyBQb2tlIEdhdGV3YXkgdG9rZW5cIik7XG5cdFx0XHRcdFx0fSk7XG5cdFx0XHR9KTtcblx0fVxuXG5cdHByaXZhdGUgcmVuZGVyQWxsb3dXcml0ZVNldHRpbmcoc2V0dGluZzogU2V0dGluZyk6IHZvaWQge1xuXHRcdHNldHRpbmcuYWRkVG9nZ2xlKCh0b2dnbGUpID0+IHtcblx0XHRcdHRvZ2dsZVxuXHRcdFx0XHQuc2V0VmFsdWUodGhpcy5wbHVnaW4uc2V0dGluZ3MuYWxsb3dXcml0ZSlcblx0XHRcdFx0Lm9uQ2hhbmdlKGFzeW5jICh2YWx1ZSkgPT4ge1xuXHRcdFx0XHRcdGF3YWl0IHRoaXMucGx1Z2luLnVwZGF0ZUFsbG93V3JpdGUodmFsdWUpO1xuXHRcdFx0XHR9KTtcblx0XHR9KTtcblx0fVxuXG5cdHByaXZhdGUgcmVuZGVyQ29ubmVjdGlvblN0YXR1c1NldHRpbmcoc2V0dGluZzogU2V0dGluZyk6IHZvaWQge1xuXHRcdHNldHRpbmcuYWRkRXh0cmFCdXR0b24oKGJ1dHRvbikgPT4ge1xuXHRcdFx0YnV0dG9uXG5cdFx0XHRcdC5zZXRJY29uKFwicmVmcmVzaC1jd1wiKVxuXHRcdFx0XHQuc2V0VG9vbHRpcChcIlJlY29ubmVjdFwiKVxuXHRcdFx0XHQub25DbGljaygoKSA9PiB7XG5cdFx0XHRcdFx0dGhpcy5wbHVnaW4ucmVjb25uZWN0Tm93KCk7XG5cdFx0XHRcdFx0bmV3IE5vdGljZShcIlJlY29ubmVjdGluZyBQb2tlIEdhdGV3YXlcIik7XG5cdFx0XHRcdH0pO1xuXHRcdH0pO1xuXG5cdFx0dGhpcy5zdGF0dXNFbCA9IHNldHRpbmcuY29udHJvbEVsLmNyZWF0ZVNwYW4oKTtcblx0XHR0aGlzLnN0YXR1c0VsLmFkZENsYXNzKFwicG9rZS1zdGF0dXNcIik7XG5cdFx0dGhpcy51cGRhdGVTdGF0dXModGhpcy5wbHVnaW4uZ2V0Q29ubmVjdGlvblN0YXRlKCkpO1xuXHR9XG5cblx0dXBkYXRlU3RhdHVzKHN0YXRlOiBDb25uZWN0aW9uU3RhdGUpOiB2b2lkIHtcblx0XHRpZiAoIXRoaXMuc3RhdHVzRWwpIHtcblx0XHRcdHJldHVybjtcblx0XHR9XG5cblx0XHR0aGlzLnN0YXR1c0VsLnNldFRleHQoY2FwaXRhbGl6ZShzdGF0ZSkpO1xuXHRcdHRoaXMuc3RhdHVzRWwucmVtb3ZlQ2xhc3MoXCJpcy1jb25uZWN0ZWRcIiwgXCJpcy1jb25uZWN0aW5nXCIsIFwiaXMtZGlzY29ubmVjdGVkXCIpO1xuXHRcdHRoaXMuc3RhdHVzRWwuYWRkQ2xhc3MoYGlzLSR7c3RhdGV9YCk7XG5cdH1cbn1cblxuZnVuY3Rpb24gZ2V0UmVxdWlyZWRTdHJpbmcocGFyYW1zOiBSZWNvcmQ8c3RyaW5nLCB1bmtub3duPiwga2V5OiBzdHJpbmcpOiBzdHJpbmcge1xuXHRjb25zdCB2YWx1ZSA9IHBhcmFtc1trZXldO1xuXG5cdGlmICh0eXBlb2YgdmFsdWUgIT09IFwic3RyaW5nXCIpIHtcblx0XHR0aHJvdyBuZXcgRXJyb3IoYE1pc3NpbmcgcmVxdWlyZWQgc3RyaW5nIHBhcmFtOiAke2tleX1gKTtcblx0fVxuXG5cdHJldHVybiB2YWx1ZTtcbn1cblxuZnVuY3Rpb24gZ2V0T3B0aW9uYWxTdHJpbmcocGFyYW1zOiBSZWNvcmQ8c3RyaW5nLCB1bmtub3duPiwga2V5OiBzdHJpbmcsIGZhbGxiYWNrOiBzdHJpbmcpOiBzdHJpbmcge1xuXHRjb25zdCB2YWx1ZSA9IHBhcmFtc1trZXldO1xuXHRyZXR1cm4gdHlwZW9mIHZhbHVlID09PSBcInN0cmluZ1wiID8gdmFsdWUgOiBmYWxsYmFjaztcbn1cblxuZnVuY3Rpb24gaXNSZWNvcmQodmFsdWU6IHVua25vd24pOiB2YWx1ZSBpcyBSZWNvcmQ8c3RyaW5nLCB1bmtub3duPiB7XG5cdHJldHVybiB0eXBlb2YgdmFsdWUgPT09IFwib2JqZWN0XCIgJiYgdmFsdWUgIT09IG51bGwgJiYgIUFycmF5LmlzQXJyYXkodmFsdWUpO1xufVxuXG5mdW5jdGlvbiBpc01hcmtkb3duUGF0aChwYXRoOiBzdHJpbmcpOiBib29sZWFuIHtcblx0cmV0dXJuIHBhdGgudG9Mb2NhbGVMb3dlckNhc2UoKS5lbmRzV2l0aChcIi5tZFwiKTtcbn1cblxuZnVuY3Rpb24gbm9ybWFsaXplVmF1bHRGb2xkZXIocGF0aDogc3RyaW5nKTogc3RyaW5nIHtcblx0Y29uc3Qgbm9ybWFsaXplZFBhdGggPSBwYXRoLnRyaW0oKS5yZXBsYWNlKC9eXFwvK3xcXC8rJC9nLCBcIlwiKTtcblxuXHRpZiAoIW5vcm1hbGl6ZWRQYXRoKSB7XG5cdFx0cmV0dXJuIFwiXCI7XG5cdH1cblxuXHRjb25zdCBwYXJ0cyA9IG5vcm1hbGl6ZWRQYXRoLnNwbGl0KFwiL1wiKS5maWx0ZXIoQm9vbGVhbik7XG5cblx0aWYgKHBhcnRzLmluY2x1ZGVzKFwiLi5cIikgfHwgcGFydHMuc29tZSgocGFydCkgPT4gcGFydCA9PT0gXCIuXCIpKSB7XG5cdFx0dGhyb3cgbmV3IEVycm9yKFwiVmF1bHQgYWNjZXNzIGZvbGRlciBjYW5ub3QgY29udGFpbiBwYXJlbnQgb3IgY3VycmVudC1kaXJlY3Rvcnkgc2VnbWVudHNcIik7XG5cdH1cblxuXHRyZXR1cm4gcGFydHMuam9pbihcIi9cIik7XG59XG5cbmZ1bmN0aW9uIG1ha2VTbmlwcGV0KGNvbnRlbnQ6IHN0cmluZywgaW5kZXg6IG51bWJlciwgbWF0Y2hMZW5ndGg6IG51bWJlcik6IHN0cmluZyB7XG5cdGNvbnN0IGhhbGZXaW5kb3cgPSBNYXRoLmZsb29yKChNQVhfU05JUFBFVF9MRU5HVEggLSBtYXRjaExlbmd0aCkgLyAyKTtcblx0Y29uc3Qgc3RhcnQgPSBNYXRoLm1heCgwLCBpbmRleCAtIGhhbGZXaW5kb3cpO1xuXHRjb25zdCBlbmQgPSBNYXRoLm1pbihjb250ZW50Lmxlbmd0aCwgaW5kZXggKyBtYXRjaExlbmd0aCArIGhhbGZXaW5kb3cpO1xuXHRjb25zdCBwcmVmaXggPSBzdGFydCA+IDAgPyBcIi4uLlwiIDogXCJcIjtcblx0Y29uc3Qgc3VmZml4ID0gZW5kIDwgY29udGVudC5sZW5ndGggPyBcIi4uLlwiIDogXCJcIjtcblxuXHRyZXR1cm4gYCR7cHJlZml4fSR7Y29udGVudC5zbGljZShzdGFydCwgZW5kKS5yZXBsYWNlKC9cXHMrL2csIFwiIFwiKS50cmltKCl9JHtzdWZmaXh9YDtcbn1cblxuZnVuY3Rpb24gY2FwaXRhbGl6ZSh2YWx1ZTogc3RyaW5nKTogc3RyaW5nIHtcblx0cmV0dXJuIHZhbHVlLmNoYXJBdCgwKS50b0xvY2FsZVVwcGVyQ2FzZSgpICsgdmFsdWUuc2xpY2UoMSk7XG59XG5cbmZ1bmN0aW9uIGdlbmVyYXRlQ29ubmVjdGlvblRva2VuKCk6IHN0cmluZyB7XG5cdGNvbnN0IGJ5dGVzID0gbmV3IFVpbnQ4QXJyYXkoMzIpO1xuXHRjcnlwdG8uZ2V0UmFuZG9tVmFsdWVzKGJ5dGVzKTtcblx0cmV0dXJuIGAke0dFTkVSQVRFRF9UT0tFTl9QUkVGSVh9JHt0b0Jhc2U2NFVybChieXRlcyl9YDtcbn1cblxuZnVuY3Rpb24gdG9CYXNlNjRVcmwoYnl0ZXM6IFVpbnQ4QXJyYXkpOiBzdHJpbmcge1xuXHRsZXQgYmluYXJ5ID0gXCJcIjtcblxuXHRmb3IgKGNvbnN0IGJ5dGUgb2YgYnl0ZXMpIHtcblx0XHRiaW5hcnkgKz0gU3RyaW5nLmZyb21DaGFyQ29kZShieXRlKTtcblx0fVxuXG5cdHJldHVybiBidG9hKGJpbmFyeSkucmVwbGFjZSgvXFwrL2csIFwiLVwiKS5yZXBsYWNlKC9cXC8vZywgXCJfXCIpLnJlcGxhY2UoLz0rJC9nLCBcIlwiKTtcbn1cblxuZnVuY3Rpb24gcmVkYWN0VG9rZW4odXJsOiBVUkwpOiBzdHJpbmcge1xuXHRjb25zdCBjb3B5ID0gbmV3IFVSTCh1cmwudG9TdHJpbmcoKSk7XG5cblx0aWYgKGNvcHkuc2VhcmNoUGFyYW1zLmhhcyhcInRva2VuXCIpKSB7XG5cdFx0Y29weS5zZWFyY2hQYXJhbXMuc2V0KFwidG9rZW5cIiwgXCIqKipcIik7XG5cdH1cblxuXHRyZXR1cm4gY29weS50b1N0cmluZygpO1xufVxuIl19