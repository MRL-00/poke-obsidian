import { createHmac, timingSafeEqual } from "node:crypto";

const TOKEN_PREFIX = "pkobs";

export interface VerifiedConnectionToken {
	userId: string;
	issuedAt: number;
}

export function createConnectionToken(secret: string, userId: string): string {
	const payload = toBase64Url(
		JSON.stringify({
			userId,
			issuedAt: Date.now(),
		})
	);
	const signature = sign(secret, payload);

	return `${TOKEN_PREFIX}_${payload}.${signature}`;
}

export function verifyConnectionToken(secret: string, token: string): VerifiedConnectionToken | null {
	if (!token.startsWith(`${TOKEN_PREFIX}_`)) {
		return null;
	}

	const unsignedToken = token.slice(`${TOKEN_PREFIX}_`.length);
	const separatorIndex = unsignedToken.lastIndexOf(".");

	if (separatorIndex === -1) {
		return null;
	}

	const payload = unsignedToken.slice(0, separatorIndex);
	const signature = unsignedToken.slice(separatorIndex + 1);

	if (!safeEqual(signature, sign(secret, payload))) {
		return null;
	}

	try {
		const parsed = JSON.parse(fromBase64Url(payload)) as unknown;

		if (!isRecord(parsed) || typeof parsed.userId !== "string" || typeof parsed.issuedAt !== "number") {
			return null;
		}

		return {
			userId: parsed.userId,
			issuedAt: parsed.issuedAt,
		};
	} catch {
		return null;
	}
}

function sign(secret: string, payload: string): string {
	return createHmac("sha256", secret).update(payload).digest("base64url");
}

function toBase64Url(value: string): string {
	return Buffer.from(value, "utf8").toString("base64url");
}

function fromBase64Url(value: string): string {
	return Buffer.from(value, "base64url").toString("utf8");
}

function safeEqual(left: string, right: string): boolean {
	const leftBuffer = Buffer.from(left);
	const rightBuffer = Buffer.from(right);

	if (leftBuffer.length !== rightBuffer.length) {
		return false;
	}

	return timingSafeEqual(leftBuffer, rightBuffer);
}

function isRecord(value: unknown): value is Record<string, unknown> {
	return typeof value === "object" && value !== null && !Array.isArray(value);
}
