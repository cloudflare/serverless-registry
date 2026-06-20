import { Env } from "../..";
import jwt from "@tsndr/cloudflare-worker-jwt";
import {
  MINIMUM_CHUNK,
  MAXIMUM_CHUNK_UPLOAD_SIZE,
  MAXIMUM_CHUNK,
  getChunkBlob,
  getHelperR2Path,
  limit,
  split,
} from "../chunk";
import { InternalError, ManifestError, RangeError, ServerError } from "../errors";
import { SHA256_PREFIX_LEN, getSHA256, hexToDigest, isValidDigest } from "../user";
import { errorString, jsonHeaders, readableToBlob, readerToBlob, wrap } from "../utils";
import { BlobUnknownError, DigestInvalidError, ManifestUnknownError } from "../v2-errors";
import {
  CheckLayerResponse,
  CheckManifestResponse,
  FinishedUploadObject,
  GetLayerResponse,
  GetManifestResponse,
  ListReferrersResponse,
  ListRepositoriesResponse,
  PutManifestResponse,
  ReferrerDescriptor,
  Registry,
  RegistryError,
  UploadId,
  UploadObject,
  wrapError,
} from "./registry";
import { GarbageCollectionMode, GarbageCollector } from "./garbage-collector";
import { ManifestSchema, manifestSchema } from "../manifest";

export const ociImageIndexContentType = "application/vnd.oci.image.index.v1+json";

function referrersPrefix(name: string, digest: string): string {
  return `${name}/_referrers/${digest}/`;
}

function referrersPath(name: string, subjectDigest: string, digest: string): string {
  return `${referrersPrefix(name, subjectDigest)}${digest}`;
}

function artifactTypeFromManifest(manifest: ManifestSchema): string | undefined {
  if (manifest.schemaVersion !== 2) {
    return undefined;
  }

  if (manifest.artifactType && manifest.artifactType.length > 0) {
    return manifest.artifactType;
  }

  if ("manifests" in manifest) {
    return undefined;
  }

  return manifest.config.mediaType.length > 0 ? manifest.config.mediaType : undefined;
}

function descriptorFromManifest(manifest: ManifestSchema, digest: string, size: number): ReferrerDescriptor | null {
  if (manifest.schemaVersion !== 2 || manifest.subject === undefined) {
    return null;
  }

  const descriptor: ReferrerDescriptor = {
    mediaType: manifest.mediaType,
    digest,
    size,
  };

  const artifactType = artifactTypeFromManifest(manifest);
  if (artifactType !== undefined) {
    descriptor.artifactType = artifactType;
  }

  if (manifest.annotations !== undefined) {
    descriptor.annotations = manifest.annotations;
  }

  return descriptor;
}

function parseStoredReferrerDescriptor(descriptor: unknown, expectedDigest: string): ReferrerDescriptor | null {
  if (typeof descriptor !== "object" || descriptor === null) {
    return null;
  }

  const candidate = descriptor as Record<string, unknown>;
  if (
    typeof candidate.mediaType !== "string" ||
    typeof candidate.digest !== "string" ||
    candidate.digest !== expectedDigest ||
    typeof candidate.size !== "number" ||
    !Number.isInteger(candidate.size) ||
    candidate.size < 0
  ) {
    return null;
  }

  const parsedDescriptor: ReferrerDescriptor = {
    mediaType: candidate.mediaType,
    digest: candidate.digest,
    size: candidate.size,
  };

  if (candidate.artifactType !== undefined) {
    if (typeof candidate.artifactType !== "string") {
      return null;
    }
    parsedDescriptor.artifactType = candidate.artifactType;
  }

  if (candidate.annotations !== undefined) {
    if (typeof candidate.annotations !== "object" || candidate.annotations === null) {
      return null;
    }

    const annotations = Object.entries(candidate.annotations);
    if (!annotations.every((entry): entry is [string, string] => typeof entry[1] === "string")) {
      return null;
    }
    parsedDescriptor.annotations = Object.fromEntries(annotations);
  }

  return parsedDescriptor;
}

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

export function getRegistryUploadsPath(state: { registryUploadId: string; name: string }): string {
  return `${state.name}/uploads/${state.registryUploadId}`;
}

export async function getJWT(env: Env, state: { registryUploadId: string; name: string }): Promise<string | null> {
  const stateObject = await env.REGISTRY.get(getRegistryUploadsPath(state));
  if (stateObject === null) return null;
  try {
    const metadata = await stateObject.json<{ jwt?: string }>();
    if (!metadata) return null;
    if (!metadata.jwt || typeof metadata.jwt !== "string") {
      return null;
    }
    return metadata.jwt;
  } catch (e) {
    console.error("Error parsing metadata", e);
    return null;
  }
}

export async function encodeState(state: State, env: Env): Promise<{ jwt: string; hash: string }> {
  const jwtSignature = await jwt.sign(
    { ...state, exp: Math.floor(Date.now() / 1000) + 60 * 60 * 2 },
    // TODO: Remove JWT encoding and use something faster like just plain bas64
    "secret-doesnt-matter-anymore-we-are-using-jwt-for-encoding-state",
    {
      algorithm: "HS256",
    },
  );

  await env.REGISTRY.put(getRegistryUploadsPath(state), JSON.stringify({ jwt: jwtSignature }));
  return { jwt: jwtSignature, hash: await getSHA256(jwtSignature, "") };
}

export const symlinkHeader = "X-Serverless-Registry-Symlink";

export async function getUploadState(
  name: string,
  uploadId: string,
  env: Env,
  verifyHash: string | undefined,
): Promise<{ state: State; stateStr: string; hash: string } | RangeError | null> {
  const stateStr = await getJWT(env, { registryUploadId: uploadId, name: name });
  if (stateStr === null) {
    return null;
  }

  const stateStrHash = await getSHA256(stateStr, "");
  // We are skipping state jwt verifying as it doesn't make sense anymore, we are already verifying it's the latest by looking into R2 and comparing the hash.
  const stateObject = jwt.decode<State>(stateStr).payload;
  if (!stateObject) {
    console.error("Payload property is not in the JWT");
    throw new InternalError();
  }

  if (verifyHash !== undefined && stateStrHash !== verifyHash) {
    return new RangeError(stateStrHash, stateObject);
  }

  return { state: stateObject, stateStr: stateStr, hash: stateStrHash };
}

export class R2Registry implements Registry {
  private gc: GarbageCollector;

  constructor(private env: Env) {
    this.gc = new GarbageCollector(this.env.REGISTRY);
  }

  async manifestExists(name: string, reference: string): Promise<RegistryError | CheckManifestResponse> {
    const [res, err] = await wrap(this.env.REGISTRY.head(`${name}/manifests/${reference}`));
    if (err) {
      return wrapError("manifestExists", err);
    }

    if (!res) {
      return {
        exists: false,
      };
    }

    if (res.checksums.sha256 === null) {
      return { response: new ServerError("invalid checksum from R2 backend") };
    }

    return {
      exists: true,
      digest: hexToDigest(res.checksums.sha256!),
      contentType: res.httpMetadata!.contentType!,
      size: res.size,
    };
  }

  async listRepositories(limit?: number, last?: string): Promise<RegistryError | ListRepositoriesResponse> {
    // The idea in listRepositories is list all entries in the R2 bucket and map them to repositories.
    // The bucket also contains internal keys like _referrers/, but repository discovery only derives
    // repositories from manifest paths with the following shape:
    //   <path>/manifests/<name>
    // This means we slice the last two items in the key and add them to our hash map.
    // At the end, we start skipping entries until we find another unique key, then we return that entry as startAfter.

    const options = {
      limit: limit ?? 1000,
      startAfter: last ?? undefined,
    };
    const repositories = new Set<string>();
    let totalRecords = 0;
    let lastSeen: string | undefined;
    const objectExistsInPath = (entry?: string) => {
      if (entry === undefined) return false;
      const parts = entry.split("/");
      const repository = parts.slice(0, parts.length - 2).join("/");
      return repositories.has(repository);
    };

    const repositoriesOrder: string[] = [];
    const addObjectPath = (object: R2Object) => {
      if (totalRecords >= options.limit && !objectExistsInPath(object.key)) {
        return;
      }

      // update lastSeen for cursoring purposes
      lastSeen = object.key;
      // don't add if seen before
      if (totalRecords >= options.limit) return;
      // skip either 'manifests' or 'blobs'
      // name format is:
      // <path>/<'blobs' | 'manifests'>/<name>
      const parts = object.key.split("/");
      if (parts[parts.length - 2] !== "manifests") {
        return;
      }
      const repository = parts.slice(0, parts.length - 2).join("/");

      if (repositories.has(repository)) return;
      totalRecords++;
      repositories.add(repository);
      repositoriesOrder.push(repository);
    };

    const r2Objects = await this.env.REGISTRY.list({
      limit: 50,
      startAfter: options.startAfter,
    });
    r2Objects.objects.forEach((path) => addObjectPath(path));
    let cursor = r2Objects.truncated ? r2Objects.cursor : undefined;
    while (cursor !== undefined && totalRecords < options.limit) {
      const next = await this.env.REGISTRY.list({
        limit: 50,
        cursor,
      });
      next.objects.forEach((path) => addObjectPath(path));
      if (next.truncated) {
        cursor = next.cursor;
      } else {
        cursor = undefined;
      }
    }

    while (cursor !== undefined && typeof lastSeen === "string" && objectExistsInPath(lastSeen)) {
      const nextList: R2Objects = await this.env.REGISTRY.list({
        limit: 50,
        cursor,
      });

      let found = false;
      // Search for the next object in the list
      for (const object of nextList.objects) {
        if (!objectExistsInPath(lastSeen)) {
          found = true;
          break;
        }

        lastSeen = object.key;
      }

      if (found) break;

      if (nextList.truncated) {
        // jump to the next list and try to find a
        // repository that hasn't been returned in this response
        cursor = nextList.cursor;
      } else {
        // we arrived to the end of the list, no more cursor
        cursor = undefined;
      }
    }

    return {
      repositories: repositoriesOrder,
      cursor: lastSeen,
    };
  }

  async verifyManifest(name: string, manifest: ManifestSchema) {
    if (manifest.schemaVersion === 2 && "manifests" in manifest) {
      for (const manifestElement of manifest.manifests) {
        const key = manifestElement.digest;
        const res = await this.env.REGISTRY.head(`${name}/manifests/${key}`);
        if (res === null) {
          console.error(`Manifest with digest ${key} doesn't exist`);
          return new ManifestError("BLOB_UNKNOWN", `unknown manifest ${key}`);
        }
      }

      return null;
    }

    const layers: string[] =
      manifest.schemaVersion === 1
        ? manifest.fsLayers.map((layer) => layer.blobSum)
        : [...manifest.layers.map((layer) => layer.digest), manifest.config.digest];
    for (const key of layers) {
      const res = await this.env.REGISTRY.head(`${name}/blobs/${key}`);
      if (res === null) {
        console.error(`Digest ${key} doesn't exist`);
        return new ManifestError("BLOB_UNKNOWN", `unknown blob ${key}`);
      }
    }

    return null;
  }

  async listReferrers(
    name: string,
    digest: string,
    options?: {
      artifactType?: string;
      limit?: number;
      last?: string;
    },
  ): Promise<ListReferrersResponse | RegistryError> {
    if (options?.last !== undefined && !isValidDigest(options.last)) {
      return {
        response: new ServerError("invalid referrers cursor", 400),
      };
    }

    const limit = Math.min(Math.max(Math.trunc(options?.limit ?? 100), 1), 1000);
    const prefix = referrersPrefix(name, digest);
    const manifests: ReferrerDescriptor[] = [];
    const pageSize = Math.min(Math.max(limit + 1, 100), 1000);

    let objects = await this.env.REGISTRY.list({
      prefix,
      limit: pageSize,
      startAfter: options?.last ? referrersPath(name, digest, options.last) : undefined,
    });

    while (true) {
      for (const object of objects.objects) {
        const descriptorObject = await this.env.REGISTRY.get(object.key);
        if (descriptorObject === null) {
          continue;
        }

        const expectedDigest = object.key.split("/").pop();
        if (expectedDigest === undefined) {
          continue;
        }

        let descriptorJSON: unknown;
        try {
          descriptorJSON = await descriptorObject.json();
        } catch {
          continue;
        }

        const descriptor = parseStoredReferrerDescriptor(descriptorJSON, expectedDigest);
        if (descriptor === null) {
          continue;
        }

        if (options?.artifactType !== undefined && descriptor.artifactType !== options.artifactType) {
          continue;
        }

        manifests.push(descriptor);
        if (manifests.length > limit) {
          return {
            manifests: manifests.slice(0, limit),
            cursor: manifests[limit - 1].digest,
          };
        }
      }

      if (!objects.truncated) {
        break;
      }

      objects = await this.env.REGISTRY.list({
        prefix,
        limit: pageSize,
        cursor: objects.cursor,
      });
    }

    return {
      manifests,
    };
  }

  async putManifest(
    namespace: string,
    reference: string,
    readableStream: ReadableStream,
    {
      contentType,
      checkLayers,
    }: {
      contentType: string;
      checkLayers?: boolean;
    },
  ): Promise<PutManifestResponse | RegistryError> {
    const key = await this.gc.markForInsertion(namespace);
    try {
      return this.putManifestInner(
        namespace,
        reference,
        readableStream,
        contentType,
        checkLayers !== undefined ? checkLayers === true : true,
      );
    } finally {
      // if this fails, at some point it will be expired
      await this.gc.cleanInsertion(namespace, key);
    }
  }

  async putManifestInner(
    name: string,
    reference: string,
    readableStream: ReadableStream,
    contentType: string,
    checkLayers: boolean,
  ): Promise<PutManifestResponse | RegistryError> {
    const gcMarker = await this.gc.getGCMarker(name);
    const env = this.env;
    const sha256 = new crypto.DigestStream("SHA-256");
    const reader = readableStream.getReader();
    const shaWriter = sha256.getWriter();
    const blob = await readableToBlob(reader, shaWriter);
    reader.releaseLock();
    shaWriter.close();
    const digest = await sha256.digest;
    const digestStr = hexToDigest(digest);
    const text = await blob.text();
    let manifestJSON: unknown;
    try {
      manifestJSON = JSON.parse(text);
    } catch {
      return {
        response: new ManifestError("MANIFEST_INVALID", "invalid manifest JSON"),
      };
    }

    const manifestResult = manifestSchema.safeParse(manifestJSON);
    if (!manifestResult.success) {
      const firstIssue = manifestResult.error.issues[0];
      const path = firstIssue?.path.length ? `${firstIssue.path.join(".")}: ` : "";
      return {
        response: new ManifestError("MANIFEST_INVALID", `${path}${firstIssue?.message ?? "invalid manifest"}`),
      };
    }

    const manifest = manifestResult.data;
    const subjectDigest = manifest.schemaVersion === 2 ? manifest.subject?.digest : undefined;
    if (subjectDigest !== undefined && !isValidDigest(subjectDigest)) {
      return {
        response: new ManifestError("MANIFEST_INVALID", `invalid subject digest ${subjectDigest}`),
      };
    }
    if (subjectDigest !== undefined) {
      const [subjectManifest, subjectManifestErr] = await wrap(env.REGISTRY.head(`${name}/manifests/${subjectDigest}`));
      if (subjectManifestErr) {
        return wrapError("putManifestInner", subjectManifestErr);
      }
      if (subjectManifest === null) {
        return {
          response: new ManifestError("BLOB_UNKNOWN", `unknown subject ${subjectDigest}`),
        };
      }
    }

    const referrerDescriptor = descriptorFromManifest(manifest, digestStr, blob.size);
    if (checkLayers) {
      const verifyManifestErr = await this.verifyManifest(name, manifest);
      if (verifyManifestErr !== null) return { response: verifyManifestErr };
    }

    if (!(await this.gc.checkCanInsertData(name, gcMarker))) {
      console.error("Manifest can't be uploaded as there is/was a garbage collection going");
      return { response: new ServerError("garbage collection is on-going... check with registry administrator", 500) };
    }

    const customMetadata = {
      hasSubject: subjectDigest !== undefined ? "true" : "false",
      ...(subjectDigest !== undefined ? { subjectDigest } : {}),
    };

    const putReference = async () => {
      // if the reference is the same as a digest, it's not necessary to insert
      if (reference === digestStr) return;
      return await env.REGISTRY.put(`${name}/manifests/${reference}`, text, {
        sha256: digest,
        httpMetadata: {
          contentType,
        },
        customMetadata,
      });
    };

    const putTasks: Promise<unknown>[] = [
      putReference(),
      // this is the "main" manifest
      env.REGISTRY.put(`${name}/manifests/${digestStr}`, text, {
        sha256: digest,
        httpMetadata: {
          contentType,
        },
        customMetadata,
      }),
    ];

    if (referrerDescriptor !== null && subjectDigest !== undefined) {
      putTasks.push(
        env.REGISTRY.put(referrersPath(name, subjectDigest, digestStr), JSON.stringify(referrerDescriptor), {
          httpMetadata: {
            contentType: "application/json",
          },
        }),
      );
    }

    await Promise.all(putTasks);
    return {
      digest: hexToDigest(digest),
      location: `/v2/${name}/manifests/${reference}`,
      subject: subjectDigest,
    };
  }

  async getManifest(name: string, reference: string): Promise<RegistryError | GetManifestResponse> {
    const [res, err] = await wrap(this.env.REGISTRY.get(`${name}/manifests/${reference}`));
    if (err) {
      return wrapError("getManifest", err);
    }

    if (!res) {
      return {
        response: new Response(JSON.stringify(ManifestUnknownError), { status: 404 }),
      };
    }

    return {
      stream: res.body!,
      digest: hexToDigest(res.checksums.sha256!),
      size: res.size,
      contentType: res.httpMetadata!.contentType!,
    };
  }

  async mountExistingLayer(
    sourceName: string,
    digest: string,
    destinationName: string,
  ): Promise<RegistryError | FinishedUploadObject> {
    const sourceLayerPath = `${sourceName}/blobs/${digest}`;
    const [res, err] = await wrap(this.env.REGISTRY.head(sourceLayerPath));
    if (err) {
      return wrapError("mountExistingLayer", err);
    }
    if (!res) {
      return wrapError("mountExistingLayer", "Layer not found");
    } else {
      const destinationLayerPath = `${destinationName}/blobs/${digest}`;
      if (sourceLayerPath === destinationLayerPath) {
        // Bad request
        throw new InternalError();
      }
      // Prevent recursive symlink
      if (res.customMetadata && symlinkHeader in res.customMetadata) {
        return await this.mountExistingLayer(res.customMetadata[symlinkHeader], digest, destinationName);
      }
      // Trying to mount a layer from sourceLayerPath to destinationLayerPath

      // Create linked file with custom metadata
      const [newFile, error] = await wrap(
        this.env.REGISTRY.put(destinationLayerPath, sourceLayerPath, {
          sha256: await getSHA256(sourceLayerPath, ""),
          httpMetadata: res.httpMetadata,
          customMetadata: { [symlinkHeader]: sourceName }, // Storing target repository name in metadata (to easily resolve recursive layer mounting)
        }),
      );
      if (error) {
        return wrapError("mountExistingLayer", error);
      }
      if (newFile && "response" in newFile) {
        return wrapError("mountExistingLayer", newFile.response);
      }

      return {
        digest: hexToDigest(res.checksums.sha256!),
        location: `/v2/${destinationLayerPath}`,
      };
    }
  }

  async layerExists(name: string, tag: string): Promise<RegistryError | CheckLayerResponse> {
    const [res, err] = await wrap(this.env.REGISTRY.head(`${name}/blobs/${tag}`));
    if (err) {
      return wrapError("layerExists", err);
    }

    if (!res) {
      return {
        exists: false,
      };
    }

    return {
      digest: hexToDigest(res.checksums.sha256!),
      size: res.size,
      exists: true,
    };
  }

  async getLayer(name: string, digest: string): Promise<RegistryError | GetLayerResponse> {
    const [res, err] = await wrap(this.env.REGISTRY.get(`${name}/blobs/${digest}`));
    if (err) {
      return wrapError("getLayer", err);
    }

    if (!res) {
      return {
        response: new Response(JSON.stringify(BlobUnknownError), { status: 404 }),
      };
    }

    // Handle R2 symlink
    if (res.customMetadata && symlinkHeader in res.customMetadata) {
      const layerPath = await res.text();
      // Symlink detected! Will download layer from "layerPath"
      const [linkName, linkDigest] = layerPath.split("/blobs/");
      if (linkName == name && linkDigest == digest) {
        return {
          response: new Response(JSON.stringify(BlobUnknownError), { status: 404 }),
        };
      }
      return await this.env.REGISTRY_CLIENT.getLayer(linkName, linkDigest);
    }

    return {
      stream: res.body!,
      digest: hexToDigest(res.checksums.sha256!),
      size: res.size,
    };
  }

  async startUpload(namespace: string): Promise<RegistryError | UploadObject> {
    // Generate a unique ID for this upload
    const uuid = crypto.randomUUID();

    const upload = await this.env.REGISTRY.createMultipartUpload(uuid);
    const state = {
      uploadId: upload.uploadId,
      parts: [],
      registryUploadId: uuid,
      byteRange: 0,
      name: namespace,
      chunks: [],
    };
    const hashedJwtState = await encodeState(state, this.env);
    return {
      maximumBytesPerChunk: MAXIMUM_CHUNK_UPLOAD_SIZE,
      minimumBytesPerChunk: MINIMUM_CHUNK,
      id: uuid,
      location: `/v2/${namespace}/blobs/uploads/${uuid}?_stateHash=${hashedJwtState.hash}`,
      range: [0, 0],
    };
  }

  async getUpload(namespace: string, uploadId: string): Promise<UploadObject | RegistryError> {
    const state = await getUploadState(namespace, uploadId, this.env, undefined);
    if (state === null) {
      return {
        response: new Response(null, { status: 404 }),
      };
    }
    // kind of unreachable, as RangeErrors are only thrown for invalid states
    if (state instanceof RangeError) {
      return { response: new InternalError() };
    }

    return {
      id: uploadId,
      maximumBytesPerChunk: MAXIMUM_CHUNK_UPLOAD_SIZE,
      minimumBytesPerChunk: MINIMUM_CHUNK,
      location: `/v2/${namespace}/blobs/uploads/${uploadId}?_stateHash=${state.hash}`,
      // Note that the HTTP Range header byte ranges are inclusive and that will be honored, even in non-standard use cases.
      range: [0, state.state.byteRange - 1],
    };
  }

  async uploadChunk(
    namespace: string,
    uploadId: string,
    location: string,
    stream: ReadableStream,
    length?: number | undefined,
    range?: [number, number] | undefined,
  ): Promise<RegistryError | UploadObject> {
    const urlObject = new URL("https://r2-registry.com" + location);
    const stateHash = urlObject.searchParams.get("_stateHash");
    if (stateHash === null) {
      console.error("State hash is missing");
      return { response: new InternalError() };
    }

    const hashedState = await getUploadState(namespace, uploadId, this.env, stateHash);
    if (hashedState instanceof RangeError)
      return {
        response: hashedState,
      };

    const state = hashedState?.state;
    if (!state) {
      return { response: new InternalError() };
    }

    const [start, end] = range ?? [undefined, undefined];
    if (
      start !== undefined &&
      end !== undefined &&
      (state.byteRange !== +start || state.byteRange >= +end || +start >= +end)
    ) {
      return { response: new RangeError(stateHash, state) };
    }

    if (state.parts.length >= 10000) {
      console.error("We're trying to upload 1k parts");
      return { response: new InternalError() };
    }

    const upload = this.env.REGISTRY.resumeMultipartUpload(state.registryUploadId, state.uploadId);
    const uuid = state.registryUploadId;
    const env = this.env;

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

      return new RangeError(stateHash, state);
    };

    if (length === undefined) {
      console.error("Length needs to be defined");
      return {
        response: new InternalError(),
      };
    }

    const res = await appendStreamKnownLength(stream, length);
    state.byteRange += length;
    if (res instanceof RangeError)
      return {
        response: res,
      };

    const hashedJwtState = await encodeState(state, env);
    return {
      id: uuid,
      range: [0, state.byteRange - 1],
      location: `/v2/${namespace}/blobs/uploads/${uuid}?_stateHash=${hashedJwtState.hash}`,
    };
  }

  async finishUpload(
    namespace: string,
    uploadId: string,
    location: string,
    expectedSha: string,
    stream?: ReadableStream | undefined,
    length?: number | undefined,
  ): Promise<RegistryError | FinishedUploadObject> {
    const urlObject = new URL("https://r2-registry.com" + location);
    const stateHash = urlObject.searchParams.get("_stateHash");
    if (stateHash === null) {
      console.error("State hash is missing");
      return { response: new InternalError() };
    }

    const hashedState = await getUploadState(namespace, uploadId, this.env, stateHash);
    if (hashedState instanceof RangeError) {
      return {
        response: hashedState,
      };
    }
    if (hashedState === null || !hashedState.state) {
      return { response: new InternalError() };
    }
    const state = hashedState.state;

    const uuid = state.registryUploadId;

    // Commit the finished content under the client-claimed digest. R2 verifies the sha256 we
    // hand it against the bytes it stored, so a digest the client got wrong surfaces here as a
    // checksum-mismatch rejection — translate that into a 400 DIGEST_INVALID rather than letting
    // it bubble up as an opaque 500. Any other failure is a genuine server error.
    const putBlob = async (body: ReadableStream | Uint8Array | null): Promise<RegistryError | null> => {
      const [, err] = await wrap(
        this.env.REGISTRY.put(`${namespace}/blobs/${expectedSha}`, body, {
          sha256: (expectedSha as string).slice(SHA256_PREFIX_LEN),
        }),
      );
      if (err === null) return null;
      const message = errorString(err);
      // Matches the wording R2 uses when the stored bytes don't hash to the requested sha256.
      // This couples to the runtime's error text; the unit test asserts the 400 body so a future
      // wording change surfaces as a test failure rather than a silent regression to 500.
      if (/checksum|did not match/i.test(message)) {
        return {
          response: new Response(JSON.stringify(DigestInvalidError()), { status: 400, headers: jsonHeaders() }),
        };
      }
      console.error("finishUpload put failed:", message);
      return { response: new InternalError() };
    };

    if (state.parts.length === 0) {
      // No multipart parts were staged: the whole blob arrives in this request body (a monolithic
      // PUT), or it is a zero-byte blob. An absent body is a valid empty blob — store empty bytes
      // (R2 requires a known-length body, so a length-less empty stream cannot be used here) and
      // let the checksum check confirm the client really claimed the empty digest.
      if (length && length > MAXIMUM_CHUNK) {
        console.error("Surpasses MAXIMUM_CHUNK");
        return {
          response: new InternalError(),
        };
      }

      // With bytes to store, the request body carries its own (known) length. With none, it is a
      // zero-byte blob — hand R2 empty bytes rather than a length-less empty stream, which it rejects.
      const putErr = await putBlob(length && length > 0 ? stream! : new Uint8Array(0));
      if (putErr) return putErr;
    } else {
      const upload = this.env.REGISTRY.resumeMultipartUpload(uuid, state.uploadId);
      // A final chunk carried by the finalizing PUT is appended beforehand via uploadChunk (the
      // same path a PATCH uses), so the staged parts are complete here. See the PUT handler.
      await upload.complete(state.parts);
      const obj = await this.env.REGISTRY.get(uuid);
      const putErr = await putBlob(obj!.body);
      await this.env.REGISTRY.delete(uuid);
      if (putErr) return putErr;
    }

    await this.env.REGISTRY.delete(getRegistryUploadsPath(state));

    return {
      digest: expectedSha,
      location: `/v2/${namespace}/blobs/${expectedSha}`,
    };
  }

  async cancelUpload(name: string, uploadId: UploadId): Promise<true | RegistryError> {
    const hashedState = await getUploadState(name, uploadId, this.env, undefined);
    if (hashedState instanceof RangeError) {
      return { response: new InternalError() };
    }
    if (hashedState === null || !hashedState.state) {
      return {
        response: new Response(null, { status: 404 }),
      };
    }
    const state = hashedState.state;

    const upload = this.env.REGISTRY.resumeMultipartUpload(state.registryUploadId, state.uploadId);
    await upload.abort();
    await this.env.REGISTRY.delete(getRegistryUploadsPath(state));
    return true;
  }

  async monolithicUpload(
    namespace: string,
    sha256: string,
    stream: ReadableStream,
    size?: number,
  ): Promise<FinishedUploadObject | RegistryError | false> {
    if (!size) {
      const blob = await readableToBlob(stream.getReader());
      stream = blob.stream();
      size = blob.size;
    }

    if (size > MAXIMUM_CHUNK) {
      return false;
    }

    await this.env.REGISTRY.put(`${namespace}/blobs/${sha256}`, stream, {
      sha256: (sha256 as string).slice(SHA256_PREFIX_LEN),
    });
    return {
      digest: sha256,
      location: `/v2/${namespace}/blobs/${sha256}`,
    };
  }

  async garbageCollection(namespace: string, mode: GarbageCollectionMode): Promise<boolean> {
    return await this.gc.collect({ name: namespace, mode: mode });
  }
}
