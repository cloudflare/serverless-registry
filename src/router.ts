import { Router } from "itty-router";
import { BlobUnknownError, ManifestUnknownError } from "./v2-errors";
import { InternalError, ServerError } from "./errors";
import { errorString, jsonHeaders, wrap } from "./utils";
import { hexToDigest } from "./user";
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
        Link: `${url.protocol}//${url.hostname}${url.pathname}?n=${n ?? 1000}&last=${response.cursor ?? ""}; rel=next`,
      },
    },
  );
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

  const { last, limit } = req.query;
  const { name, reference } = req.params;
  // Reference is ALWAYS a sha256
  const manifest = await env.REGISTRY.head(`${name}/manifests/${reference}`);
  if (manifest === null) {
    return new Response(JSON.stringify(ManifestUnknownError(reference)), { status: 404, headers: jsonHeaders() });
  }
  const limitInt = parseInt(limit?.toString() ?? "1000", 10);
  const tags = await env.REGISTRY.list({
    prefix: `${name}/manifests`,
    limit: isNaN(limitInt) ? 1000 : limitInt,
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

  const url = new URL(req.url);
  if (tags.truncated) {
    url.searchParams.set("last", tags.truncated ? tags.cursor : "");
    return new Response(JSON.stringify(ManifestTagsListTooBigError), {
      status: 400,
      headers: {
        "Link": `${url.toString()}; rel=next`,
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
          env.REGISTRY_CLIENT.putManifest(name, reference, manifestResponse.stream, manifestResponse.contentType),
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
          env.REGISTRY_CLIENT.putManifest(name, reference, s2, getManifestResponse.contentType),
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
    env.REGISTRY_CLIENT.putManifest(name, reference, req.body!, req.headers.get("Content-Type")!),
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
    },
  });
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

  // if (req.headers.get("x-fail") === "true") {
  //   const digest = new crypto.DigestStream("SHA-256");
  //   req.body.pipeTo(digest);
  //   await digest.digest;
  //   return new Response(null, { status: 500 });
  // }

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
  const [res, err] = await wrap<FinishedUploadObject | RegistryError, Error>(
    env.REGISTRY_CLIENT.finishUpload(
      name,
      uuid,
      url.pathname + "?" + url.searchParams.toString(),
      digest! as string,
      req.body ?? undefined,
      +(req.headers.get("Content-Length") ?? "0"),
    ),
  );

  if (err) {
    console.error("Error uploading manifest", errorString(err));
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
  if (isNaN(n)) {
    throw new ServerError("invalid 'n' parameter", 400);
  }

  const tags = await env.REGISTRY.list({
    prefix: `${name}/manifests`,
    limit: n,
    startAfter: last ? `${name}/manifests/${last}` : undefined,
  });

  const keys = tags.objects.map((object) => object.key.split("/").pop()!);
  const url = new URL(req.url);
  url.searchParams.set("n", `${n}`);
  url.searchParams.set("last", keys.length ? keys[keys.length - 1] : "");
  return new Response(
    JSON.stringify({
      name,
      tags: keys,
    }),
    {
      status: 200,
      headers: {
        "Content-Type": "application/json",
        "Link": `${url.toString()}; rel=next`,
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
