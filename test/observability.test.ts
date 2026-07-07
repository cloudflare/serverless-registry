import { afterEach, describe, expect, test, vi } from "vitest";
import worker from "../index";
import type { Env } from "..";
import { env } from "cloudflare:workers";
import { createExecutionContext, reset, waitOnExecutionContext } from "cloudflare:test";

afterEach(async () => {
  await reset();
});

function createRequest(method: string, path: string, headers: Record<string, string> = {}): Request {
  return new Request(new URL("https://registry.com" + path), { method, headers });
}

function basicAuth(username: string, password: string): string {
  return `Basic ${btoa(`${username}:${password}`)}`;
}

describe("observability", () => {
  test("emits a structured warn log and a failure metric on denied auth", async () => {
    const warnSpy = vi.spyOn(console, "warn").mockImplementation(() => {});
    const bindings = env as Env;
    const metricsSpy = vi.spyOn(bindings.METRICS!, "writeDataPoint");

    const ctx = createExecutionContext();
    const res = await worker.fetch(
      createRequest("GET", "/v2/", { Authorization: basicAuth("hello", "wrong") }),
      bindings,
      ctx,
    );
    await waitOnExecutionContext(ctx);

    expect(res.status).toBe(401);

    expect(warnSpy).toHaveBeenCalled();
    const logged = JSON.parse(warnSpy.mock.calls.at(-1)![0] as string);
    expect(logged).toMatchObject({ level: "warn", event: "auth_denied" });

    expect(metricsSpy).toHaveBeenCalledWith(
      expect.objectContaining({ blobs: ["registry", "auth_denied"], indexes: ["401"] }),
    );

    warnSpy.mockRestore();
  });

  test("emits a success metric on a successful request", async () => {
    const bindings = env as Env;
    const metricsSpy = vi.spyOn(bindings.METRICS!, "writeDataPoint");

    const ctx = createExecutionContext();
    const res = await worker.fetch(
      createRequest("GET", "/v2/", { Authorization: basicAuth("hello", "world") }),
      bindings,
      ctx,
    );
    await waitOnExecutionContext(ctx);

    expect(res.status).toBe(200);
    expect(metricsSpy).toHaveBeenCalledWith(
      expect.objectContaining({ blobs: ["registry", "success"], indexes: ["200"] }),
    );
  });
});
