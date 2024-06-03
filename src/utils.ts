import { ZodError } from "zod";

export async function readableToBlob(
  reader: ReadableStreamDefaultReader,
  ...multiwriters: WritableStreamDefaultWriter[]
): Promise<Blob> {
  const blobs = [];
  while (true) {
    const value = await reader.read();
    if (value.done) break;
    blobs.push(value.value);
    const promises = [];
    for (const writer of multiwriters) {
      promises.push(writer.write(value.value));
    }

    await Promise.all(promises);
  }

  return new Blob(blobs);
}

export async function readerToBlob(readaleStream: ReadableStream, ...multiwriters: WritableStreamDefaultWriter[]) {
  const reader = readaleStream.getReader();
  const blobs = [];
  while (true) {
    const value = await reader.read();
    if (value.done) break;
    blobs.push(value.value);
    const promises = [];
    for (const writer of multiwriters) {
      promises.push(writer.write(value.value));
    }

    await Promise.all(promises);
  }

  return new Blob(blobs);
}

export async function consumeReadable(reader: ReadableStreamDefaultReader) {
  while (true) {
    const value = await reader.read();
    if (value.done) break;
  }
}

export function errorString(err: unknown): string {
  if (err instanceof ZodError) {
    const errorsMsg = err.errors
      .map((zodIssue) => `- ${zodIssue.code}: ${zodIssue.message}: ${zodIssue.path}`)
      .join("\n\t");
    return `zod error: ${errorsMsg}`;
  }

  if (err instanceof Error) {
    return `error ${err.name}: ${err.message}: ${err.cause}: ${err.stack}`;
  }

  return "unknown error: " + JSON.stringify(err);
}

export async function wrap<T, E = unknown>(fn: Promise<T>): Promise<[T, null] | [null, E]> {
  return fn.then((data) => [data, null] as [T, null]).catch((err) => [null, err as unknown as E] as [null, E]);
}

export function jsonHeaders(): { "content-type": "application/json" } {
  return { "content-type": "application/json" };
}
