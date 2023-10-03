import jwt from "@tsndr/cloudflare-worker-jwt";
import { Router } from "itty-router";
import { BlobUnknownError, ManifestUnknownError } from "./v2-errors";
import { InternalError, RangeError, ServerError } from "./errors";
import { readableToBlob, readerToBlob } from "./utils";
import { SHA256_PREFIX_LEN, hexToDigest } from "./user";
import { ManifestTagsListTooBigError } from "./v2-responses";
import { Env } from "..";
import {
  MINIMUM_CHUNK,
  MAXIMUM_CHUNK,
  getChunkBlob,
  getHelperR2Path,
  limit,
  split,
  MAXIMUM_CHUNK_UPLOAD_SIZE,
} from "./chunk";

export const REGISTRY_ACCOUNT_ID_HEADER_NAME = "registry-account-id";

const v2Router = Router({ base: "/v2/" });

v2Router.get("/", async (_req, _env: Env) => {
  return new Response();
});

v2Router.delete("/:name+/manifests/:reference", async (req, env: Env) => {
  // deleting a manifest works by retrieving the """main""" manifest that its key is a sha,
  // and then going through every tag and removing it
  //
  // after removing every tag, it's safe to remove the main manifest.
  //
  // if the transaction ends in an inconsistent state, the client can call this endpoint again
  // and we would try to delete everything again
  //
  // we limit 1k tag deletions per request. If more we will return an error so client retries.
  //
  // If somehow we need to remove by paginating, we accept a last query param

  const { last } = req.query;
  const { name, reference } = req.params;
  // Reference is ALWAYS a sha256
  const manifest = await env.REGISTRY.head(`${name}/manifests/${reference}`);
  if (manifest === null) {
    return new Response(JSON.stringify(ManifestUnknownError), { status: 404 });
  }

  const tags = await env.REGISTRY.list({
    prefix: `${name}/manifests`,
    limit: 1000,
    startAfter: last?.toString(),
  });
  for (const tag of tags.objects) {
    if (!tag.checksums.sha256) {
      continue;
    }

    if (hexToDigest(tag.checksums.sha256) === reference) {
      await env.REGISTRY.delete(tag.key);
    }
  }

  if (tags.truncated) {
    return new Response(JSON.stringify(ManifestTagsListTooBigError), {
      status: 400,
      headers: {
        "Link": `${req.url}/last=${tags.objects[tags.objects.length - 1]}; rel=next`,
        "Content-Type": "application/json",
      },
    });
  }

  // Last but not least, delete the digest manifest
  await env.REGISTRY.delete(`${name}/manifests/${reference}`);
  return new Response("", {
    status: 202,
    headers: {
      "Content-Length": "None",
    },
  });
});

v2Router.head("/:name+/manifests/:reference", async (req, env: Env) => {
  const { name, reference } = req.params;

  const res = await env.REGISTRY.head(`${name}/manifests/${reference}`);

  if (!res) {
    return new Response(JSON.stringify(ManifestUnknownError), { status: 404 });
  }

  const digestHeader: Record<string, string> = {};
  if (res.checksums.sha256 === null) {
    throw new ServerError("invalid checksum from R2 backend");
  }

  digestHeader["Docker-Content-Digest"] = hexToDigest(res.checksums.sha256!);

  return new Response(null, {
    headers: {
      "Content-Length": res.size.toString(),
      "Content-Type": res.httpMetadata!.contentType!,
      ...digestHeader,
    },
  });
});

v2Router.get("/:name+/manifests/:reference", async (req, env: Env) => {
  const { name, reference } = req.params;
  const res: R2ObjectBody | null = await env.REGISTRY.get(`${name}/manifests/${reference}`);

  if (!res) {
    return new Response(JSON.stringify(ManifestUnknownError), { status: 404 });
  }

  const digestHeader: Record<string, string> = {};

  if (res.checksums.sha256 === null) {
    throw new ServerError("invalid checksum from R2 backend");
  }

  digestHeader["Docker-Content-Digest"] = hexToDigest(res.checksums.sha256!);

  return new Response(res.body, {
    headers: {
      "Content-Length": res.size.toString(),
      "Content-Type": res.httpMetadata!.contentType!,
      ...digestHeader,
    },
  });
});

v2Router.put("/:name+/manifests/:reference", async (req, env: Env) => {
  if (!req.headers.get("Content-Type")) {
    throw new ServerError("Content type not defined", 400);
  }

  const { name, reference } = req.params;
  const sha256 = new crypto.DigestStream("SHA-256");
  const reader = req.body!.getReader();
  const shaWriter = sha256.getWriter();
  const blob = await readableToBlob(reader, shaWriter);
  reader.releaseLock();
  shaWriter.close();
  const digest = await sha256.digest;
  const digestStr = hexToDigest(digest);
  const [arr1, arr2] = reference === digestStr ? [blob.stream(), blob.stream()] : blob.stream().tee();
  const putReference = async () => {
    // if the reference is the same as a digest, it's not necessary to insert
    if (reference === digestStr) return;
    // TODO: If we're overriding an existing manifest here, should we update the original manifest references?
    return await env.REGISTRY.put(`${name}/manifests/${reference}`, arr1, {
      sha256: digest,
      httpMetadata: {
        contentType: req.headers.get("Content-Type")!,
      },
    });
  };

  await Promise.all([
    putReference(),
    // this is the "main" manifest
    env.REGISTRY.put(`${name}/manifests/${digestStr}`, arr2, {
      sha256: digest,
      httpMetadata: {
        contentType: req.headers.get("Content-Type")!,
      },
    }),
  ]);

  return new Response(null, {
    status: 201,
    headers: {
      "Location": `/v2/${name}/manifests/${reference}`,
      "Docker-Content-Digest": hexToDigest(digest),
    },
  });
});

v2Router.get("/:name+/blobs/:digest", async (req, env: Env) => {
  const { name, digest } = req.params;
  const res: R2ObjectBody | null = await env.REGISTRY.get(`${name}/blobs/${digest}`);
  if (!res) {
    return new Response(JSON.stringify(BlobUnknownError), { status: 404 });
  }

  const digestHeader: Record<string, string> = {};
  if (res.checksums.sha256 === null) {
    throw new ServerError("invalid checksum from R2 backend");
  }

  digestHeader["Docker-Content-Digest"] = hexToDigest(res.checksums.sha256!);
  return new Response(res.body, {
    headers: { ...digestHeader, "Content-Length": `${res.size}`, "Content-Type": "application/gzip" },
  });
});

export type Chunk =
  | {
      // Chunk that is less than 5GiB and respects the Chunk chain.
      // If you want to append to the chunk list see the following rules:
      // - If same size as last chunk, it's a normal multi-part-chunk.
      // - If not same size as last chunk, it's a multi-part-chunk-no-same-size.
      // - If less than 10MB, it's a small-chunk.
      type: "multi-part-chunk";
      uploadId: string;
      size: number;
    }
  | {
      // Chunk that is less than 5GiB, however it's the last chunk uploaded
      // but it's above 10MB. If you want to append from this, you have to also upload to a different object
      // so you can recover this chunk
      type: "multi-part-chunk-no-same-size";
      uploadId: string;
      r2Path: string;
      size: number;
    }
  | {
      // Small chunk that is less than 10MB
      type: "small-chunk";
      size: number;
      r2Path: string;
      uploadId: string;
    };

export type State = {
  parts: R2UploadedPart[];
  chunks: Chunk[];
  uploadId: string;
  registryUploadId: string;
  byteRange: number;
  name: string;
};

export async function encodeState(state: State, env: Env): Promise<string> {
  // 20 min timeout
  const jwtSignature = await jwt.sign(
    { ...state, exp: Math.floor(Date.now() / 1000) + 60 * 20 },
    env.JWT_STATE_SECRET,
    {
      algorithm: "HS256",
    },
  );
  // 15min of expiration
  await env.UPLOADS.put(state.registryUploadId, jwtSignature);
  return jwtSignature;
}

async function decodeStateString(state: string, env: Env, skipKVVerification = false): Promise<State | RangeError> {
  const ok = await jwt.verify(state, env.JWT_STATE_SECRET, { algorithm: "HS256" });
  if (!ok) {
    throw new InternalError();
  }

  const stateObject = jwt.decode(state).payload as unknown as State;
  if (!skipKVVerification) {
    const lastState = await env.UPLOADS.get(stateObject.registryUploadId);
    if (lastState !== null && lastState !== state) {
      const s = await decodeStateString(lastState, env);
      if (s instanceof RangeError) return s;
      return new RangeError(lastState, s);
    }
  }

  return stateObject;
}

async function decodeState(req: Request & { query: { _state?: string } }, env: Env): Promise<State | RangeError> {
  if (!req.query._state) {
    throw new InternalError();
  }

  return decodeStateString(req.query._state, env);
}

v2Router.delete("/:name+/blobs/uploads/:id", async (req, env: Env) => {
  const {} = req.params;
  const state: State | RangeError = await decodeState(req, env);
  if (state instanceof RangeError) return state;
  const upload = env.REGISTRY.resumeMultipartUpload(state.registryUploadId, state.registryUploadId);
  await upload.abort();
  return new Response(null, { status: 204, headers: { "Content-Length": "0" } });
});

// this is the first thing that the client asks for in an upload
v2Router.post("/:name+/blobs/uploads/", async (req, env: Env) => {
  const { name } = req.params;

  // Generate a unique ID for this upload
  const uuid = crypto.randomUUID();

  const upload = await env.REGISTRY.createMultipartUpload(uuid);
  const state = { uploadId: upload.uploadId, parts: [], registryUploadId: uuid, byteRange: 0, name, chunks: [] };
  const stateStr = await encodeState(state, env);

  // Return a res with a Location header indicating where to send the data to complete the upload
  return new Response(null, {
    status: 202,
    headers: {
      "Content-Length": "0",
      "Content-Range": "0-0",
      "Range": "0-0",
      "Location": `/v2/${name}/blobs/uploads/${uuid}?_state=${stateStr}`,
      "Docker-Upload-UUID": uuid,
      "OCI-Chunk-Min-Length": `${MINIMUM_CHUNK}`,
      "OCI-Chunk-Max-Length": `${MAXIMUM_CHUNK_UPLOAD_SIZE}`,
    },
  });
});

v2Router.get("/:name+/blobs/uploads/:uuid", async (req, env: Env) => {
  const { name, uuid } = req.params;
  const stateStr = await env.UPLOADS.get(uuid);
  if (!stateStr) {
    return new Response(null, { status: 404 });
  }

  const state = await decodeStateString(stateStr, env, true);
  // kind of unreachable, as RangeErrors are only thrown for invalid states
  if (state instanceof RangeError) {
    throw new InternalError();
  }

  return new Response(null, {
    status: 204,
    headers: {
      "Location": `/v2/${name}/blobs/uploads/${uuid}?_state=${await encodeState(state, env)}`,
      // Note that the HTTP Range header byte ranges are inclusive and that will be honored, even in non-standard use cases.
      "Range": `0-${state.byteRange - 1}`,
      "Docker-Upload-UUID": state.registryUploadId,
    },
  });
});

v2Router.patch("/:name+/blobs/uploads/:uuid", async (req, env: Env) => {
  const { name, uuid } = req.params;
  const contentRange = req.headers.get("Content-Range");
  const [start, end] = contentRange?.split("-") ?? [undefined, undefined];

  const state = await decodeState(req, env);
  if (state instanceof RangeError) return state;
  if (
    start !== undefined &&
    end !== undefined &&
    (state.byteRange !== +start || state.byteRange >= +end || +start >= +end)
  ) {
    return new RangeError(req.query._state as string, state);
  }

  if (state.parts.length >= 10000) {
    console.error("We're trying to upload 1k parts");
    throw new InternalError();
  }

  // Use the r2 bindings to store the chunk data in Workers KV
  const upload = env.REGISTRY.resumeMultipartUpload(`${uuid}`, state.uploadId);

  if (req.body == null) {
    return new Response(null, { status: 400 });
  }

  // This function tries to handle the following cases:
  //  1. Normal case where all chunks are the same and they are all above 5MiB except the last one
  //  2. Case where the chunk is bigger than 5GiB, in that case it will try to split it
  //  3. Case where the chunk is less than the previous one and the previous one was already different
  //
  // The reason we have to do this is that we have to attend the rules of R2:
  // Limitations
  //    Object part sizes must be at least 5MiB but no larger than 5GiB. All parts except the last one must be the same size. The last part has no minimum size, but must be the same or smaller than the other parts.
  //    The maximum number of parts is 10,000.
  //    Most S3 clients conform to these expectations.
  const appendStreamKnownLength = async (stream: ReadableStream, size: number) => {
    // This is the normal code-path, hopefully by hinting with headers on the POST call all clients respect this
    if (
      (state.chunks.length === 0 ||
        (state.chunks[state.chunks.length - 1].size === size &&
          state.chunks[state.chunks.length - 1].type === "multi-part-chunk")) &&
      size <= MAXIMUM_CHUNK &&
      size >= MINIMUM_CHUNK
    ) {
      state.chunks.push({
        type: "multi-part-chunk",
        size,
        uploadId: uuid,
      });
      const part = await upload.uploadPart(state.parts.length + 1, stream);
      state.parts.push(part);
      return;
    }

    // This happens when maximum chunk's is surpassed, so we basically have to split this stream.
    // You can test very easy this branch of code by putting MAXIMUM_CHUNK == MINIMUM_CHUNK and docker pushing against the server.
    if (size > MAXIMUM_CHUNK) {
      for await (const [reader, chunkSize] of split(stream, size, MAXIMUM_CHUNK)) {
        await appendStreamKnownLength(reader, chunkSize);
      }

      return undefined;
    }

    const lastChunk = state.chunks.length ? state.chunks[state.chunks.length - 1] : undefined;
    // This is a bad scenario, we uploaded a chunk and we have to copy.
    if (
      env.PUSH_COMPATIBILITY_MODE === "full" &&
      lastChunk &&
      (lastChunk.type === "small-chunk" || lastChunk.type === "multi-part-chunk-no-same-size")
    ) {
      // nullability: getChunkStream for small-chunk always returns stream
      const chunkStream = (await getChunkBlob(env, lastChunk))!;
      // pop as we're going to override last part
      state.chunks.pop();
      state.parts.pop();
      const blob = await readerToBlob(stream);
      const streamCombined = new Blob([chunkStream, blob]);
      await appendStreamKnownLength(limit(streamCombined.stream(), size + lastChunk.size), size + lastChunk.size);
      return;
    }

    // Only allow this branch when the last pushed chunk is multi-part-chunk
    // as we will upload the part directly. This is a normal workflow if the client is a good citizen
    if (
      (lastChunk && lastChunk.size > size) ||
      (size < MINIMUM_CHUNK && (!lastChunk || lastChunk.type === "multi-part-chunk"))
    ) {
      const path = getHelperR2Path(uuid);
      state.chunks.push({
        type: size < MINIMUM_CHUNK ? "small-chunk" : "multi-part-chunk-no-same-size",
        size,
        uploadId: uuid,
        r2Path: path,
      });

      if (env.PUSH_COMPATIBILITY_MODE === "full") {
        const [stream1, stream2] = limit(stream, size).tee();
        const partTask = upload.uploadPart(state.parts.length + 1, stream1);
        // We can totally disable this, however we are risking that the client sends another small chunk.
        // Maybe instead we can throw range error
        const dateInOneHour = new Date();
        dateInOneHour.setTime(dateInOneHour.getTime() + 60 * 60 * 1000);
        const headers = {
          // https://www.rfc-editor.org/rfc/rfc1123 date format
          // Objects will typically be removed from a bucket within 24 hours of the x-amz-expiration value.
          "x-amz-expiration": dateInOneHour.toUTCString(),
        } as const;
        const r2RegistryObjectTask = env.REGISTRY.put(path, stream2, {
          httpMetadata: new Headers(headers),
          customMetadata: headers,
        });
        state.parts.push(await partTask);
        await r2RegistryObjectTask;
        return;
      }

      state.parts.push(await upload.uploadPart(state.parts.length + 1, stream));
      return;
    }

    // we know here that size >= MINIMUM_CHUNK and size >= lastChunk.size, this is just super inefficient, maybe in the future just throw RangeError here...
    if (env.PUSH_COMPATIBILITY_MODE === "full" && lastChunk && size >= lastChunk.size) {
      console.warn(
        "The client is being a bad citizen by trying to send a new chunk bigger than the chunk it sent. If this is giving problems disable this codepath altogether",
      );
      for await (const [chunk, chunkSize] of split(stream, size, lastChunk.size)) {
        await appendStreamKnownLength(chunk, chunkSize);
      }

      return undefined;
    }

    if (env.PUSH_COMPATIBILITY_MODE === "full") {
      throw new ServerError("unreachable", 500);
    }

    return new RangeError(req.query._state as string, state);
  };

  let contentLengthStr = req.headers.get("Content-Length");
  let stream: ReadableStream = req.body;
  if (contentLengthStr === null) {
    // This is only necessary locally.
    // In production we always receive a Content-Length.
    const blob = await req.blob();
    stream = blob.stream();
    contentLengthStr = `${blob.size}`;
  }

  const contentLength = +contentLengthStr;
  const res = await appendStreamKnownLength(stream, contentLength);
  if (res instanceof RangeError) return res;
  state.byteRange += contentLength;
  const stateStr = await encodeState(state, env);

  // Return a res indicating that the chunk was successfully uploaded
  return new Response(null, {
    status: 202,
    headers: {
      "Location": `/v2/${name}/blobs/uploads/${uuid}?_state=${stateStr}`,
      // Note that the HTTP Range header byte ranges are inclusive and that will be honored, even in non-standard use cases.
      "Range": `0-${state.byteRange - 1}`,
      "Docker-Upload-UUID": uuid,
    },
  });
});

v2Router.put("/:name+/blobs/uploads/:uuid", async (req, env: Env) => {
  const { name, uuid } = req.params;
  const { digest } = req.query;

  const state = await decodeState(req, env);
  if (state instanceof RangeError) return state;
  if (state.registryUploadId !== uuid || state.name !== name) {
    throw new InternalError();
  }

  if (state.parts.length === 0) {
    await env.REGISTRY.put(`${name}/blobs/${digest}`, req.body, {
      sha256: (digest as string).slice(SHA256_PREFIX_LEN),
    });
  } else {
    const upload = env.REGISTRY.resumeMultipartUpload(uuid, state.uploadId);
    await upload.complete(state.parts);
    const obj = await env.REGISTRY.get(uuid);
    const put = env.REGISTRY.put(`${name}/blobs/${digest}`, obj!.body, {
      sha256: (digest as string).slice(SHA256_PREFIX_LEN),
    });

    await put;
    await env.REGISTRY.delete(uuid);
  }

  return new Response(null, {
    status: 201,
    headers: {
      "Content-Length": "0",
      "Docker-Content-Digest": `${digest}`,
      "Location": `/v2/${name}/blobs/${digest}`,
    },
  });
});

v2Router.head("/:name+/blobs/:digest", async (req, env: Env) => {
  const { name, digest } = req.params;

  const res = await env.REGISTRY.head(`${name}/blobs/${digest}`);

  if (!res) {
    return new Response(JSON.stringify(BlobUnknownError), { status: 404 });
  }

  const digestHeader: Record<string, string> = {};
  if (res.checksums.sha256 === null) {
    throw new ServerError("invalid checksum from R2 backend");
  }

  digestHeader["Docker-Content-Digest"] = hexToDigest(res.checksums.sha256!);

  return new Response(null, {
    headers: {
      "Content-Length": res.size.toString(),
      ...digestHeader,
    },
  });
});

export type TagsList = {
  name: string;
  tags: string[];
};

v2Router.get("/:name+/tags/list", async (req, env: Env) => {
  const { name } = req.params;
  const { n: nStr = 50, last } = req.query;
  const n = +nStr;
  if (isNaN(n)) {
    throw new ServerError("invalid 'n' parameter", 400);
  }

  const tags = await env.REGISTRY.list({
    prefix: `${name}/manifests`,
    limit: n,
    startAfter: last ? `${name}/manifests/${last}` : undefined,
  });

  const keys = tags.objects.map((object) => object.key.split("/").pop()!);
  return new Response(
    JSON.stringify({
      name,
      tags: keys,
    }),
    {
      status: 200,
      headers: {
        "Content-Type": "application/json",
        "Link": `${req.url}?n=${n}&last=${keys.length ? keys[keys.length - 1] : ""}; rel=next`,
      },
    },
  );
});

v2Router.delete("/:name+/blobs/:digest", async (req, env: Env) => {
  const { name, digest } = req.params;

  const res = await env.REGISTRY.head(`${name}/blobs/${digest}`);

  if (!res) {
    return new Response(JSON.stringify(BlobUnknownError), { status: 404 });
  }

  await env.REGISTRY.delete(`${name}/blobs/${digest}`);
  return new Response(null, {
    status: 202,
    headers: {
      "Content-Length": "None",
    },
  });
});

export default v2Router;
