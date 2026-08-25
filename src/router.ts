import { Router } from "itty-router";
import { BlobUnknownError, ManifestUnknownError } from "./v2-errors";
import { InternalError, ServerError } from "./errors";
import { errorString, jsonHeaders, wrap } from "./utils";
import { hexToDigest, isValidDigest } from "./user";
import { ManifestTagsListTooBigError } from "./v2-responses";
import { Env } from "..";
import { MINIMUM_CHUNK, MAXIMUM_CHUNK, MAXIMUM_CHUNK_UPLOAD_SIZE } from "./chunk";
import {
  CheckLayerResponse,
  CheckManifestResponse,
  FinishedUploadObject,
  GetLayerResponse,
  GetManifestResponse,
  PutManifestResponse,
  RegistryError,
  UploadObject,
  registries,
} from "./registry/registry";
import { RegistryHTTPClient } from "./registry/http";
import { ociImageIndexContentType } from "./registry/r2";

const maxReferrersListLimit = 1000;
const isOpaqueReferrersCursor = (cursor: string) => cursor.startsWith("/v2/");

function formatNextLink(url: URL): string {
  return `<${url.toString()}>; rel="next"`;
}

const v2Router = Router({ base: "/v2/" });

v2Router.get("/", async (_req, _env: Env) => {
  return new Response();
});

v2Router.get("/_catalog", async (req, env: Env) => {
  const { n, last } = req.query;
  const response = await env.REGISTRY_CLIENT.listRepositories(
    n ? parseInt(n?.toString()) : undefined,
    last?.toString(),
  );
  if ("response" in response) {
    return response.response;
  }

  const url = new URL(req.url);
  return new Response(
    JSON.stringify({
      repositories: response.repositories,
    }),
    {
      headers: {
        "Link": `${url.protocol}//${url.hostname}${url.pathname}?n=${n ?? 1000}&last=${response.cursor ?? ""}; rel=next`,
        "Content-Type": "application/json",
      },
    },
  );
});

v2Router.delete("/:name+/manifests/:reference", async (req, env: Env) => {
  // Deleting by digest removes every tag alias that points at that digest before removing
  // the digest manifest itself. Deleting by tag only removes that tag entry.
  //
  // If the deleted manifest is itself a referrer, remove its native subject-linked index entry.
  // Subject-side referrer cleanup can be deferred until GC prunes those manifests.
  //
  // If the transaction ends in an inconsistent state, the client can call this endpoint again
  // and we will retry the cleanup.
  //
  // We scan up to the requested number of manifest entries (default 1k) while looking for matching
  // tag aliases. If more entries remain, return an error so the client can continue with the
  // provided cursor.
  //
  // If somehow we need to remove by paginating, we accept a last query param.

  const { last, limit } = req.query;
  const { name, reference } = req.params;
  const manifest = await env.REGISTRY.head(`${name}/manifests/${reference}`);
  if (manifest === null) {
    return new Response(JSON.stringify(ManifestUnknownError(reference)), { status: 404, headers: jsonHeaders() });
  }
  const manifestDigest = hexToDigest(manifest.checksums.sha256!);
  let subjectDigest = manifest.customMetadata?.subjectDigest;
  if (subjectDigest === undefined && manifest.customMetadata?.hasSubject !== "false") {
    const manifestBody = await env.REGISTRY.get(`${name}/manifests/${reference}`);
    if (manifestBody !== null) {
      try {
        const manifestJSON = (await manifestBody.json()) as { subject?: { digest: string } };
        subjectDigest = manifestJSON.subject?.digest;
      } catch {
        subjectDigest = undefined;
      }
    }
  }
  const limitInt = parseInt(limit?.toString() ?? "1000", 10);
  if (!Number.isInteger(limitInt) || limitInt <= 0 || limitInt > 1000) {
    throw new ServerError("invalid 'limit' parameter", 400);
  }
  const tags = await env.REGISTRY.list({
    prefix: `${name}/manifests`,
    limit: limitInt,
    cursor: last?.toString(),
  });
  for (const tag of tags.objects) {
    if (!tag.checksums.sha256) {
      continue;
    }

    if (hexToDigest(tag.checksums.sha256) === reference && tag.key !== `${name}/manifests/${reference}`) {
      await env.REGISTRY.delete(tag.key);
    }
  }

  const url = new URL(req.url);
  if (tags.truncated) {
    url.searchParams.set("last", tags.truncated ? tags.cursor : "");
    return new Response(JSON.stringify(ManifestTagsListTooBigError), {
      status: 400,
      headers: {
        "Link": formatNextLink(url),
        "Content-Type": "application/json",
      },
    });
  }

  // Last but not least, delete the manifest entry and remove the deleted manifest's own referrer
  // index entry if it points at another subject.
  await env.REGISTRY.delete(`${name}/manifests/${reference}`);
  if (reference === manifestDigest) {
    if (subjectDigest !== undefined && isValidDigest(subjectDigest)) {
      await env.REGISTRY.delete(`${name}/_referrers/${subjectDigest}/${manifestDigest}`);
    }
  }
  return new Response("", {
    status: 202,
    headers: {
      "Content-Length": "None",
    },
  });
});

v2Router.head("/:name+/manifests/:reference", async (req, env: Env) => {
  const { name, reference } = req.params;
  const res = await env.REGISTRY_CLIENT.manifestExists(name, reference);
  if ("exists" in res && res.exists) {
    return new Response(null, {
      headers: {
        "Content-Length": res.size.toString(),
        "Content-Type": res.contentType,
        "Docker-Content-Digest": res.digest,
      },
    });
  }

  let checkManifestResponse: CheckManifestResponse | null = null;
  const registryList = registries(env);
  for (const registry of registryList) {
    const client = new RegistryHTTPClient(env, registry);
    const response = await client.manifestExists(name, reference);
    if ("response" in response) {
      continue;
    }

    if (response.exists) {
      checkManifestResponse = {
        size: response.size,
        digest: response.digest,
        contentType: response.contentType,
        exists: true,
      };

      // If the error is that it doesn't exist
      if ("exists" in res && !res.exists) {
        const manifestResponse = await client.getManifest(name, response.digest);
        if ("response" in manifestResponse) {
          console.warn(
            "Can't sync with fallback registry because it has returned an error:",
            manifestResponse.response.status,
          );
          break;
        }

        const [putResponse, err] = await wrap(
          env.REGISTRY_CLIENT.putManifest(name, reference, manifestResponse.stream, {
            contentType: manifestResponse.contentType,
            checkLayers: false,
          }),
        );
        if (err) {
          console.error("Error sync manifest into client:", errorString(err));
        }

        if (putResponse && "response" in putResponse) {
          console.error("Error sync manifest into client (non 200 status):", putResponse.response.status);
        }
      }

      break;
    }
  }

  if (checkManifestResponse === null || !checkManifestResponse.exists)
    return new Response(JSON.stringify(ManifestUnknownError(reference)), { status: 404, headers: jsonHeaders() });

  return new Response(null, {
    headers: {
      "Content-Length": checkManifestResponse.size.toString(),
      "Content-Type": checkManifestResponse.contentType,
      "Docker-Content-Digest": checkManifestResponse.digest,
    },
  });
});

v2Router.get("/:name+/manifests/:reference", async (req, env: Env, context: ExecutionContext) => {
  const { name, reference } = req.params;
  const res = await env.REGISTRY_CLIENT.getManifest(name, reference);
  if (!("response" in res)) {
    return new Response(res.stream, {
      headers: {
        "Content-Length": res.size.toString(),
        "Content-Type": res.contentType,
        "Docker-Content-Digest": res.digest,
      },
    });
  }

  let getManifestResponse: GetManifestResponse | null = null;
  const registriesList = registries(env);
  for (const registry of registriesList) {
    const client = new RegistryHTTPClient(env, registry);
    const response = await client.getManifest(name, reference);
    if ("response" in response) {
      continue;
    }

    getManifestResponse = response;
    if (res.response.status !== 404) {
      // Don't upload the manifest if there is an error
      break;
    }

    const [s1, s2] = getManifestResponse.stream.tee();
    getManifestResponse.stream = s1;
    context.waitUntil(
      (async () => {
        const [response, err] = await wrap(
          env.REGISTRY_CLIENT.putManifest(name, reference, s2, {
            contentType: getManifestResponse.contentType,
            checkLayers: false,
          }),
        );
        if (err) {
          console.error("Error uploading asynchronously the manifest ", reference, "into main registry");
          return;
        }

        if (response && "response" in response) {
          console.error("Error uploading asynchronously manifest:", response.response.status);
        }
      })(),
    );
    break;
  }

  if (getManifestResponse === null)
    return new Response(JSON.stringify(ManifestUnknownError(reference)), { status: 404, headers: jsonHeaders() });

  return new Response(getManifestResponse.stream, {
    headers: {
      "Content-Length": getManifestResponse.size.toString(),
      "Content-Type": getManifestResponse.contentType,
      "Docker-Content-Digest": getManifestResponse.digest,
    },
  });
});

v2Router.put("/:name+/manifests/:reference", async (req, env: Env) => {
  if (!req.headers.get("Content-Type")) {
    throw new ServerError("Content type not defined", 400);
  }

  const { name, reference } = req.params;
  const [res, err] = await wrap<PutManifestResponse | RegistryError, Error>(
    env.REGISTRY_CLIENT.putManifest(name, reference, req.body!, { contentType: req.headers.get("Content-Type")! }),
  );
  if (err) {
    console.error("Error putting manifest:", errorString(err));
    return new InternalError();
  }

  if ("response" in res) {
    return res.response;
  }

  return new Response(null, {
    status: 201,
    headers: {
      "Location": res.location,
      "Docker-Content-Digest": res.digest,
      ...(res.subject ? { "OCI-Subject": res.subject } : {}),
    },
  });
});

v2Router.get("/:name+/referrers/:digest", async (req, env: Env) => {
  const { name, digest } = req.params;
  if (!isValidDigest(digest)) {
    throw new ServerError("invalid digest", 400);
  }

  const { artifactType, last, n: nStr = 100 } = req.query;
  const n = Number(nStr);
  if (!Number.isInteger(n) || n <= 0 || n > maxReferrersListLimit) {
    throw new ServerError("invalid 'n' parameter", 400);
  }
  if (last !== undefined && !isValidDigest(last.toString()) && !isOpaqueReferrersCursor(last.toString())) {
    throw new ServerError("invalid 'last' parameter", 400);
  }

  const response = await env.REGISTRY_CLIENT.listReferrers(name, digest, {
    artifactType: artifactType?.toString(),
    last: last?.toString(),
    limit: n,
  });
  if ("response" in response) {
    return response.response;
  }

  const url = new URL(req.url);
  url.searchParams.set("n", `${n}`);
  if (artifactType !== undefined) {
    url.searchParams.set("artifactType", artifactType.toString());
  }
  if (response.cursor !== undefined) {
    url.searchParams.set("last", response.cursor);
  }

  const headers: Record<string, string> = {
    "Content-Type": ociImageIndexContentType,
  };
  if (artifactType !== undefined) {
    headers["OCI-Filters-Applied"] = "artifactType";
  }
  if (response.cursor !== undefined) {
    headers.Link = formatNextLink(url);
  }

  return new Response(
    JSON.stringify({
      schemaVersion: 2,
      mediaType: ociImageIndexContentType,
      manifests: response.manifests,
    }),
    {
      status: 200,
      headers,
    },
  );
});

v2Router.get("/:name+/blobs/:digest", async (req, env: Env, context: ExecutionContext) => {
  const { name, digest } = req.params;
  const res = await env.REGISTRY_CLIENT.getLayer(name, digest);
  if (!("response" in res)) {
    return new Response(res.stream, {
      headers: {
        "Docker-Content-Digest": res.digest,
        "Content-Length": `${res.size}`,
      },
    });
  }

  let layerResponse: GetLayerResponse | null = null;
  const registriesList = registries(env);
  for (const registry of registriesList) {
    const client = new RegistryHTTPClient(env, registry);
    const response = await client.getLayer(name, digest);
    if ("response" in response) {
      continue;
    }

    layerResponse = response;
    const [s1, s2] = layerResponse.stream.tee();
    layerResponse.stream = s1;
    context.waitUntil(
      (async () => {
        const [response, err] = await wrap(env.REGISTRY_CLIENT.monolithicUpload(name, digest, s2, layerResponse.size));
        if (err) {
          console.error("Error uploading asynchronously the layer ", digest, "into main registry");
          return;
        }

        if (response === false) {
          console.error("Layer might be too big for the registry client", layerResponse.size);
        }
      })(),
    );
    break;
  }

  if (layerResponse === null) return new Response(JSON.stringify(BlobUnknownError), { status: 404 });

  return new Response(layerResponse.stream, {
    headers: {
      "Docker-Content-Digest": layerResponse.digest,
      "Content-Length": `${layerResponse.size}`,
    },
  });
});

v2Router.delete("/:name+/blobs/uploads/:id", async (req, env: Env) => {
  const { name, id } = req.params;
  const [res, err] = await wrap<true | RegistryError, Error>(env.REGISTRY_CLIENT.cancelUpload(name, id));
  if (err) {
    console.error("Error cancelling upload:", errorString(err));
    return new InternalError();
  }

  if (res !== true && "response" in res) {
    return res.response;
  }

  return new Response(null, { status: 204, headers: { "Content-Length": "0" } });
});

// this is the first thing that the client asks for in an upload
v2Router.post("/:name+/blobs/uploads/", async (req, env: Env) => {
  const { name } = req.params;
  const { from, mount } = req.query;
  if (mount !== undefined && from !== undefined) {
    // Try to create a new upload from an existing layer on another repository
    const [finishedUploadObject, err] = await wrap<FinishedUploadObject | RegistryError, Error>(
      env.REGISTRY_CLIENT.mountExistingLayer(from.toString(), mount.toString(), name),
    );
    // If there is an error, fallback to the default layer upload system
    if (!(err || (finishedUploadObject && "response" in finishedUploadObject))) {
      return new Response(null, {
        status: 201,
        headers: {
          "Content-Length": "0",
          "Location": finishedUploadObject.location,
          "Docker-Content-Digest": finishedUploadObject.digest,
        },
      });
    }
  }
  // Upload a new layer
  const [uploadObject, err] = await wrap<UploadObject | RegistryError, Error>(env.REGISTRY_CLIENT.startUpload(name));

  if (err) {
    return new InternalError();
  }

  if ("response" in uploadObject) {
    return uploadObject.response;
  }

  const range = `${uploadObject.range.join("-")}`;
  // Return a res with a Location header indicating where to send the data to complete the upload
  return new Response(null, {
    status: 202,
    headers: {
      "Content-Length": "0",
      "Content-Range": range,
      "Range": range,
      "Location": uploadObject.location,
      "Docker-Upload-UUID": uploadObject.id,
      "OCI-Chunk-Min-Length": `${Math.max(MINIMUM_CHUNK, uploadObject.minimumBytesPerChunk ?? MINIMUM_CHUNK)}`,
      "OCI-Chunk-Max-Length": `${Math.min(
        MAXIMUM_CHUNK_UPLOAD_SIZE,
        uploadObject.maximumBytesPerChunk ?? MAXIMUM_CHUNK,
      )}`,
    },
  });
});

v2Router.get("/:name+/blobs/uploads/:uuid", async (req, env: Env) => {
  const { name, uuid } = req.params;
  const [uploadObject, err] = await wrap<UploadObject | RegistryError, Error>(
    env.REGISTRY_CLIENT.getUpload(name, uuid),
  );

  if (err) {
    return new InternalError();
  }

  if ("response" in uploadObject) {
    return uploadObject.response;
  }

  return new Response(null, {
    status: 204,
    headers: {
      "Location": uploadObject.location,
      // Note that the HTTP Range header byte ranges are inclusive and that will be honored, even in non-standard use cases.
      "Range": `${uploadObject.range.join("-")}`,
      "Docker-Upload-UUID": uploadObject.id,
      "OCI-Chunk-Min-Length": `${Math.max(MINIMUM_CHUNK, uploadObject.minimumBytesPerChunk ?? MINIMUM_CHUNK)}`,
      "OCI-Chunk-Max-Length": `${Math.min(
        MAXIMUM_CHUNK_UPLOAD_SIZE,
        uploadObject.maximumBytesPerChunk ?? MAXIMUM_CHUNK,
      )}`,
    },
  });
});

v2Router.patch("/:name+/blobs/uploads/:uuid", async (req, env: Env) => {
  const { name, uuid } = req.params;
  const contentRange = req.headers.get("Content-Range");
  const [start, end] = contentRange?.split("-") ?? [undefined, undefined];

  if (req.body == null) {
    return new Response(null, { status: 400 });
  }

  let contentLengthString = req.headers.get("Content-Length");
  let stream = req.body;
  if (!contentLengthString) {
    const blob = await req.blob();
    contentLengthString = `${blob.size}`;
    stream = blob.stream();
  }

  const url = new URL(req.url);
  const [res, err] = await wrap<UploadObject | RegistryError, Error>(
    env.REGISTRY_CLIENT.uploadChunk(
      name,
      uuid,
      url.pathname + "?" + url.searchParams.toString(),
      stream,
      +contentLengthString,
      end !== undefined && start !== undefined ? [+start, +end] : undefined,
    ),
  );
  if (err) {
    console.error("Uploading chunk:", errorString(err));
    return new InternalError();
  }

  if ("response" in res) {
    return res.response;
  }

  // Return a res indicating that the chunk was successfully uploaded
  return new Response(null, {
    status: 202,
    headers: {
      "Location": res.location,
      // Note that the HTTP Range header byte ranges are inclusive and that will be honored, even in non-standard use cases.
      "Range": `${res.range.join("-")}`,
      "Docker-Upload-UUID": res.id,
    },
  });
});

v2Router.put("/:name+/blobs/uploads/:uuid", async (req, env: Env) => {
  const { name, uuid } = req.params;
  const { digest } = req.query;

  const url = new URL(req.url);
  let location = url.pathname + "?" + url.searchParams.toString();
  const contentLength = +(req.headers.get("Content-Length") ?? "0");

  // A finalizing PUT may carry the last chunk. Append it through the same path a PATCH uses, so
  // small chunks are combined into a valid part and an out-of-order chunk is rejected with 416
  // (instead of corrupting the assembled blob). finishUpload then completes the staged parts.
  if (req.body && contentLength > 0) {
    const contentRange = req.headers.get("Content-Range");
    const [start, end] = contentRange?.split("-") ?? [undefined, undefined];
    const [chunk, chunkErr] = await wrap<UploadObject | RegistryError, Error>(
      env.REGISTRY_CLIENT.uploadChunk(
        name,
        uuid,
        location,
        req.body,
        contentLength,
        end !== undefined && start !== undefined ? [+start, +end] : undefined,
      ),
    );
    if (chunkErr) {
      return new InternalError();
    }
    if ("response" in chunk) {
      return chunk.response;
    }
    location = chunk.location;
  }

  const [res, err] = await wrap<FinishedUploadObject | RegistryError, Error>(
    env.REGISTRY_CLIENT.finishUpload(name, uuid, location, digest! as string),
  );

  if (err) {
    return new InternalError();
  }

  if ("response" in res) {
    return res.response;
  }

  return new Response(null, {
    status: 201,
    headers: {
      "Content-Length": "0",
      "Docker-Content-Digest": res.digest,
      "Location": res.location,
    },
  });
});

v2Router.head("/:name+/blobs/:tag", async (req, env: Env) => {
  const { name, tag } = req.params;

  const res = await env.REGISTRY.head(`${name}/blobs/${tag}`);
  let layerExistsResponse: CheckLayerResponse | null = null;
  if (!res) {
    const registryList = registries(env);
    for (const registry of registryList) {
      const client = new RegistryHTTPClient(env, registry);
      const response = await client.layerExists(name, tag);
      if ("response" in response) {
        continue;
      }

      if (response.exists) {
        layerExistsResponse = response;
        break;
      }
    }

    if (layerExistsResponse === null || !layerExistsResponse.exists)
      return new Response(JSON.stringify(BlobUnknownError), { status: 404 });
  } else {
    if (res.checksums.sha256 === null) {
      throw new ServerError("invalid checksum from R2 backend");
    }

    layerExistsResponse = {
      digest: hexToDigest(res.checksums.sha256!),
      size: res.size,
      exists: true,
    };
  }

  return new Response(null, {
    headers: {
      "Content-Length": layerExistsResponse.size.toString(),
      "Docker-Content-Digest": layerExistsResponse.digest,
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
  if (isNaN(n) || n <= 0) {
    throw new ServerError("invalid 'n' parameter", 400);
  }

  let tags = await env.REGISTRY.list({
    prefix: `${name}/manifests`,
    limit: n,
    startAfter: last ? `${name}/manifests/${last}` : undefined,
  });
  // Filter out sha256 manifest
  let manifestTags = tags.objects.filter((tag) => !tag.key.startsWith(`${name}/manifests/sha256:`));
  // If results are truncated and the manifest filter removed some result, extend the search to reach the n number of results expected by the client
  while (tags.objects.length > 0 && tags.truncated && manifestTags.length !== n) {
    tags = await env.REGISTRY.list({
      prefix: `${name}/manifests`,
      limit: n - manifestTags.length,
      cursor: tags.cursor,
    });
    // Filter out sha256 manifest
    manifestTags = manifestTags.concat(tags.objects.filter((tag) => !tag.key.startsWith(`${name}/manifests/sha256:`)));
  }

  const keys = manifestTags.map((object) => object.key.split("/").pop()!);
  const url = new URL(req.url);
  url.searchParams.set("n", `${n}`);
  url.searchParams.set("last", keys.length ? keys[keys.length - 1] : "");
  const responseHeaders: { "Content-Type": string; "Link"?: string } = {
    "Content-Type": "application/json",
  };
  // Only supply a next link if the previous result is truncated
  if (tags.truncated) {
    responseHeaders.Link = `${url.toString()}; rel=next`;
  }
  return new Response(
    JSON.stringify({
      name,
      tags: keys,
    }),
    {
      status: 200,
      headers: responseHeaders,
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

v2Router.post("/:name+/gc", async (req, env: Env) => {
  const { name } = req.params;

  const mode = req.query.mode ?? "unreferenced";
  if (mode !== "unreferenced" && mode !== "untagged") {
    throw new ServerError("Mode must be either 'unreferenced' or 'untagged'", 400);
  }
  const result = await env.REGISTRY_CLIENT.garbageCollection(name, mode);
  return new Response(JSON.stringify({ success: result }));
});

export default v2Router;
