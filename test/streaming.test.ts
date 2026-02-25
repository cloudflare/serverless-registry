import { describe, expect, test } from "vitest";
import { getStreamSize } from "../src/utils";
import { limit } from "../src/chunk";

describe("Streaming Utilities", () => {
  test("getStreamSize extracts size correctly from Content-Length", () => {
    const headers = new Headers({
      "Content-Length": "1024",
    });
    expect(getStreamSize(headers)).toBe(1024);
  });

  test("getStreamSize extracts size correctly from Content-Range (standard)", () => {
    const headers = new Headers({
      "Content-Range": "bytes 0-52428799/104857600",
    });
    expect(getStreamSize(headers)).toBe(52428800);
  });

  test("getStreamSize extracts size correctly from Content-Range (without total)", () => {
    const headers = new Headers({
      "Content-Range": "bytes 500-999",
    });
    expect(getStreamSize(headers)).toBe(500);
  });

  test("getStreamSize returns undefined when no size headers are present", () => {
    const headers = new Headers();
    expect(getStreamSize(headers)).toBeUndefined();
  });
});

describe("Stream limit function", () => {
  test("limit correctly truncates a stream", async () => {
    const data = new Uint8Array([1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);
    const stream = new Blob([data]).stream();
    const limitedStream = limit(stream, 5);
    
    const reader = limitedStream.getReader();
    const chunks = [];
    while (true) {
      const { done, value } = await reader.read();
      if (done) break;
      chunks.push(...value);
    }
    
    expect(chunks).toHaveLength(5);
    expect(new Uint8Array(chunks)).toEqual(new Uint8Array([1, 2, 3, 4, 5]));
  });

  test("limit handles stream smaller than limit", async () => {
    const data = new Uint8Array([1, 2, 3]);
    const stream = new Blob([data]).stream();
    const limitedStream = limit(stream, 10);
    
    const reader = limitedStream.getReader();
    const chunks = [];
    while (true) {
      const { done, value } = await reader.read();
      if (done) break;
      chunks.push(...value);
    }
    
    expect(chunks).toHaveLength(3);
    expect(new Uint8Array(chunks)).toEqual(new Uint8Array([1, 2, 3]));
  });
});
