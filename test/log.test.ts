import { describe, expect, test, vi } from "vitest";
import { log } from "../src/log";

describe("log", () => {
  test("info emits a structured JSON line via console.log", () => {
    const spy = vi.spyOn(console, "log").mockImplementation(() => {});

    log.info("test_event", { foo: "bar" });

    expect(spy).toHaveBeenCalledTimes(1);
    const parsed = JSON.parse(spy.mock.calls[0][0] as string);
    expect(parsed).toMatchObject({ level: "info", event: "test_event", foo: "bar" });
    expect(typeof parsed.ts).toBe("string");

    spy.mockRestore();
  });

  test("warn emits a structured JSON line via console.warn", () => {
    const spy = vi.spyOn(console, "warn").mockImplementation(() => {});

    log.warn("auth_denied", { authmode: "basic" });

    expect(spy).toHaveBeenCalledTimes(1);
    const parsed = JSON.parse(spy.mock.calls[0][0] as string);
    expect(parsed).toMatchObject({ level: "warn", event: "auth_denied", authmode: "basic" });

    spy.mockRestore();
  });

  test("error emits a structured JSON line via console.error", () => {
    const spy = vi.spyOn(console, "error").mockImplementation(() => {});

    log.error("unhandled_error", { message: "boom" });

    expect(spy).toHaveBeenCalledTimes(1);
    const parsed = JSON.parse(spy.mock.calls[0][0] as string);
    expect(parsed).toMatchObject({ level: "error", event: "unhandled_error", message: "boom" });

    spy.mockRestore();
  });

  test("omitted fields still produce a valid structured line", () => {
    const spy = vi.spyOn(console, "log").mockImplementation(() => {});

    log.info("no_fields_event");

    const parsed = JSON.parse(spy.mock.calls[0][0] as string);
    expect(parsed).toMatchObject({ level: "info", event: "no_fields_event" });

    spy.mockRestore();
  });
});
