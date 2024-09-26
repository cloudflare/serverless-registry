import { Env } from "..";
import { InternalError } from "./errors";
import { Chunk } from "./registry/r2";

// 5MiB
export const MINIMUM_CHUNK = 1024 * 1024 * 5;

// 5GiB
export const MAXIMUM_CHUNK = MINIMUM_CHUNK * 1024;

// 100MB
export const MAXIMUM_CHUNK_UPLOAD_SIZE = 1000 * 1000 * 100;

export const getHelperR2Path = (id: string): string => {
  return `${id}-helper`;
};

export async function getChunkBlob(env: Env, chunk: Chunk): Promise<Blob | null> {
  switch (chunk.type) {
    case "multi-part-chunk":
      // not necessary, this is a correct chunk always.
      return null;
    case "small-chunk":
    case "multi-part-chunk-no-same-size":
      const res = await env.REGISTRY.get(chunk.r2Path);
      if (res === null) {
        throw new InternalError();
      }

      // I wish we could somehow return this as stream, but we can't as 'combine'
      // below wouldn't work
      const blob = await res.blob();
      return blob;
  }
}

/**
 * limit creates a FixedLengthStream that manually reads and writes from streamInput to avoid piping.
 * We can't pipeTo because it's disallowed on TransformStream.
 *
 * This is just a hint for workerd, the passed reader is expected to be handled by the caller to make sure it's of the same size as limitBytes
 *
 */
export function limit(streamInput: ReadableStream, limitBytes: number): ReadableStream {
  if (streamInput instanceof FixedLengthStream) return streamInput;
  const stream = new FixedLengthStream(limitBytes, {});

  (async () => {
    const w = stream.writable.getWriter();
    const r = streamInput.getReader();
    let written = 0;
    while (true) {
      const { done, value } = await r.read();
      if (done) break;
      await w.write(value);
      written += value.length;
      if (written >= limitBytes) break;
    }

    r.releaseLock();
    w.releaseLock();
    await stream.writable.close();
  })();

  return stream.readable;
}

export async function* split(
  stream: ReadableStream,
  size: number,
  maxSize: number,
): AsyncGenerator<[ReadableStream, number]> {
  let leftOver: Uint8Array | undefined = undefined;
  while (size > maxSize) {
    size -= maxSize;
    const identity = new IdentityTransformStream();
    // The way we split is creating another stream that we write to that we know we will only write maxSize.
    // The leftovers will be written on the next write when we know they don't surpass maxSize.
    const chunkTask = (async () => {
      const writer = identity.writable.getWriter();
      const reader = stream.getReader();
      let writtenUntilNow = 0;
      // write the previous leftovers
      if (leftOver) {
        await writer.write(leftOver);
        writtenUntilNow += (leftOver as Uint8Array).length;
        leftOver = undefined;
      }

      // While we don't surpass maxSize or reader is not closed, keep writing
      while (!leftOver) {
        const { value, done } = await reader.read();
        if (done) break;
        const v = value as Uint8Array;
        let toWrite = v.slice(0, Math.min(maxSize, v.length));
        // So we're about to write this slice that would surpass maxSize.
        // Let's split it and carry it as leftovers for next iteration
        if (v.length + writtenUntilNow > maxSize) {
          const diff = v.length + writtenUntilNow - maxSize;
          toWrite = v.slice(0, v.length - diff);
          leftOver = v.slice(v.length - diff);
        }

        if (toWrite.length) {
          writtenUntilNow += toWrite.length;
          await writer.write(toWrite);
        }
      }

      // release everything
      reader.releaseLock();
      writer.releaseLock();
      await identity.writable.close();
    })();
    yield [limit(identity.readable, maxSize), maxSize];
    await chunkTask;
  }

  const lastIdentity = new TransformStream();
  // Flush the leftovers and the rest of the reader
  const t = (async () => {
    const writer = lastIdentity.writable.getWriter();
    if (leftOver) {
      await writer.write(leftOver);
    }

    for await (const value of stream.values()) {
      const v = value as Uint8Array;
      await writer.write(v);
    }

    writer.releaseLock();
    await lastIdentity.writable.close();
  })()
    .then()
    .catch((err) => {
      console.log("Error on last identity", err);
      throw err;
    });

  yield [limit(lastIdentity.readable, size), size];
  await t;
  await stream.getReader().closed;
}
