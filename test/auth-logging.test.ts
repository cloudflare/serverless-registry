import { describe, expect, test, vi } from "vitest";
import { stripUsernamePasswordFromHeader } from "../src/auth";
import { RegistryTokens, newRegistryTokens } from "../src/token";

describe("auth structured logging", () => {
  test("stripUsernamePasswordFromHeader logs a structured error on malformed Basic credentials", () => {
    const errorSpy = vi.spyOn(console, "error").mockImplementation(() => {});
    const request = new Request("https://registry.com/v2/", {
      headers: { Authorization: "Basic !!!not-valid-base64!!!" },
    });

    const result = stripUsernamePasswordFromHeader(request);

    expect(result).toEqual({ verified: false, payload: null });
    expect(errorSpy).toHaveBeenCalled();
    const logged = JSON.parse(errorSpy.mock.calls.at(-1)![0] as string);
    expect(logged).toMatchObject({ level: "error", event: "basic_auth_decode_failed" });

    errorSpy.mockRestore();
  });
});

describe("token structured logging", () => {
  test("verifyToken logs a structured warning when the token cannot be parsed", async () => {
    const [, publicKey] = await RegistryTokens.createPrivateAndPublicKey();
    const registryTokens = await newRegistryTokens(publicKey);
    const warnSpy = vi.spyOn(console, "warn").mockImplementation(() => {});

    const result = await registryTokens.verifyToken(new Request("https://registry.com/v2/"), "not-a-real-jwt");

    expect(result.verified).toBe(false);
    expect(warnSpy).toHaveBeenCalled();
    const logged = JSON.parse(warnSpy.mock.calls.at(-1)![0] as string);
    expect(logged).toMatchObject({ level: "warn", event: "jwt_verify_error" });

    warnSpy.mockRestore();
  });
});
