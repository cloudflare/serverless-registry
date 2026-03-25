import { afterAll, describe, expect, test } from "vitest";
import { SHA256_PREFIX_LEN, getSHA256 } from "../src/user";
import { TagsList } from "../src/router";
import { Env } from "..";
import { RegistryTokens } from "../src/token";
import { RegistryAuthProtocolTokenPayload } from "../src/auth";
import { registries } from "../src/registry/registry";
import type { ReferrerDescriptor } from "../src/registry/registry";
import { isDockerDotIO, RegistryHTTPClient } from "../src/registry/http";
import { encode } from "@cfworker/base64url";
import { ManifestSchema } from "../src/manifest";
import { limit } from "../src/chunk";
import worker from "../index";
import { createExecutionContext, env, waitOnExecutionContext } from "cloudflare:test";

type ReferrersIndex = {
  schemaVersion: 2;
  mediaType: string;
  manifests: ReferrerDescriptor[];
};

async function generateManifest(name: string, schemaVersion: 1 | 2 = 2): Promise<ManifestSchema> {
  // Layer data
  const layerData = Math.random().toString(36).substring(2); // Random string data
  const layerSha256 = await getSHA256(layerData);
  // Upload layer data
  const res = await fetch(createRequest("POST", `/v2/${name}/blobs/uploads/`, null, {}));
  expect(res.ok).toBeTruthy();
  const blob = new Blob([layerData]).stream();
  const stream = limit(blob, layerData.length);
  const res2 = await fetch(createRequest("PATCH", res.headers.get("location")!, stream, {}));
  expect(res2.ok).toBeTruthy();
  const last = await fetch(createRequest("PUT", res2.headers.get("location")! + "&digest=" + layerSha256, null, {}));
  expect(last.ok).toBeTruthy();
  // Config data
  const configData = Math.random().toString(36).substring(2); // Random string data
  const configSha256 = await getSHA256(configData);
  if (schemaVersion === 2) {
    // Upload config layer
    const configRes = await fetch(createRequest("POST", `/v2/${name}/blobs/uploads/`, null, {}));
    expect(configRes.ok).toBeTruthy();
    const configBlob = new Blob([configData]).stream();
    const configStream = limit(configBlob, configData.length);
    const configRes2 = await fetch(createRequest("PATCH", configRes.headers.get("location")!, configStream, {}));
    expect(configRes2.ok).toBeTruthy();
    const configLast = await fetch(
      createRequest("PUT", configRes2.headers.get("location")! + "&digest=" + configSha256, null, {}),
    );
    expect(configLast.ok).toBeTruthy();
  }
  return schemaVersion === 1
    ? {
        schemaVersion,
        fsLayers: [{ blobSum: layerSha256 }],
        architecture: "amd64",
      }
    : {
        schemaVersion,
        layers: [
          { size: layerData.length, digest: layerSha256, mediaType: "shouldbeanything" },
          { size: layerData.length, digest: layerSha256, mediaType: "shouldbeanything" },
        ],
        config: { size: configData.length, digest: configSha256, mediaType: "configmediatypeshouldntbechecked" },
        mediaType: "shouldalsobeanythingforretrocompatibility",
      };
}

async function generateManifestList(amdManifest: ManifestSchema, armManifest: ManifestSchema): Promise<ManifestSchema> {
  const amdManifestData = JSON.stringify(amdManifest);
  const armManifestData = JSON.stringify(armManifest);
  return {
    schemaVersion: 2,
    mediaType: "application/vnd.docker.distribution.manifest.list.v2+json",
    manifests: [
      {
        mediaType: "application/vnd.docker.distribution.manifest.v2+json",
        size: amdManifestData.length,
        digest: await getSHA256(amdManifestData),
        platform: {
          architecture: "amd64",
          os: "linux",
        },
      },
      {
        mediaType: "application/vnd.docker.distribution.manifest.v2+json",
        size: armManifestData.length,
        digest: await getSHA256(armManifestData),
        platform: {
          architecture: "arm64",
          os: "linux",
        },
      },
    ],
  };
}

function createRequest(method: string, path: string, body: ReadableStream | null, headers = {}) {
  return new Request(new URL("https://registry.com" + path), { method, body: body, headers });
}

function shuffleArray<T>(array: T[]) {
  for (let i = array.length - 1; i > 0; i--) {
    const j = Math.floor(Math.random() * (i + 1));
    [array[i], array[j]] = [array[j], array[i]];
  }

  return array;
}

function parseLinkHeaderURL(link: string): URL {
  const rawURL = link.split(";")[0].trim();
  const href = rawURL.startsWith("<") && rawURL.endsWith(">") ? rawURL.slice(1, -1) : rawURL;
  return new URL(href);
}

function numberedDigest(index: number): string {
  return `sha256:${index.toString(16).padStart(64, "0")}`;
}

function usernamePasswordToAuth(username: string, password: string): string {
  return `Basic ${btoa(`${username}:${password}`)}`;
}

async function fetchUnauth(r: Request): Promise<Response> {
  const ctx = createExecutionContext();
  const res = await worker.fetch(r, env as Env, ctx);
  await waitOnExecutionContext(ctx);
  return res as Response;
}

async function fetch(r: Request): Promise<Response> {
  r.headers.append("Authorization", usernamePasswordToAuth(username, "world"));
  return await fetchUnauth(r);
}

const username = "hello";

describe("v2", () => {
  test("/v2", async () => {
    const response = await fetch(createRequest("GET", "/v2/", null));
    expect(response.status).toBe(200);
  });

  test("Username password authenticatiom fails gracefully when wrong format", async () => {
    const res = await fetchUnauth(
      createRequest("GET", `/v2/`, null, {
        Authorization: `Basic ${encode("hello")}:${encode("t")}`,
      }),
    );
    expect(res.status).toBe(401);
  });

  test("Username password authenticatiom fails gracefully when password is wrong", async () => {
    const cred = encode(`hello:t`);
    const res = await fetchUnauth(
      createRequest("GET", `/v2/`, null, {
        Authorization: `Basic ${cred}`,
      }),
    );
    expect(res.status).toBe(401);
  });

  test("Simple username password authenticatiom fails gracefully when password is wrong", async () => {
    const cred = encode(`hello:t`);
    const res = await fetchUnauth(
      createRequest("GET", `/v2/`, null, {
        Authorization: `Basic ${cred}`,
      }),
    );
    expect(res.status).toBe(401);
  });

  test("Simple username password authenticatiom fails gracefully when username is wrong", async () => {
    const cred = encode(`hell0:world`);
    const res = await fetchUnauth(
      createRequest("GET", `/v2/`, null, {
        Authorization: `Basic ${cred}`,
      }),
    );
    expect(res.status).toBe(401);
  });

  test("Simple username password authentication", async () => {
    const res = await fetchUnauth(createRequest("GET", `/v2/`, null, {}));
    expect(res.status).toBe(401);
    expect(res.ok).toBeFalsy();
    const resAuth = await fetchUnauth(
      createRequest("GET", `/v2/`, null, {
        Authorization: usernamePasswordToAuth("hellO", "worlD"),
      }),
    );
    expect(resAuth.status).toBe(401);
    expect(resAuth.ok).toBeFalsy();
    const resAuthCorrect = await fetchUnauth(
      createRequest("GET", `/v2/`, null, {
        Authorization: usernamePasswordToAuth("hello", "world"),
      }),
    );
    expect(resAuthCorrect.ok).toBeTruthy();
  });
});

function getImageManifestV2(schema: ManifestSchema) {
  if (schema.schemaVersion !== 2 || "manifests" in schema) {
    throw new Error("expected OCI image manifest");
  }

  return schema;
}

function manifestSize(schema: ManifestSchema): number {
  return new Blob([JSON.stringify(schema)]).size;
}

async function uploadManifest(
  name: string,
  schema: ManifestSchema,
  tag?: string,
): Promise<{ sha256: string; response: Response }> {
  const data = JSON.stringify(schema);
  const sha256 = await getSHA256(data);
  if (!tag) {
    tag = sha256;
  }

  const response = await fetch(
    createRequest("PUT", `/v2/${name}/manifests/${tag}`, new Blob([data]).stream(), {
      "Content-Type": "application/gzip",
    }),
  );
  if (!response.ok) {
    throw new Error(await response.text());
  }
  expect(response.ok).toBeTruthy();
  expect(response.headers.get("docker-content-digest")).toEqual(sha256);
  return { sha256, response };
}

async function createManifest(name: string, schema: ManifestSchema, tag?: string): Promise<{ sha256: string }> {
  const { sha256 } = await uploadManifest(name, schema, tag);
  return { sha256 };
}

function getLayersFromManifest(schema: ManifestSchema): string[] {
  const layersDigest = [];
  if (schema.schemaVersion === 1) {
    for (const layer of schema.fsLayers) {
      layersDigest.push(layer.blobSum);
    }
  } else if (schema.schemaVersion === 2 && !("manifests" in schema)) {
    layersDigest.push(schema.config.digest);
    for (const layer of schema.layers) {
      layersDigest.push(layer.digest);
    }
  }
  return layersDigest;
}

async function mountLayersFromManifest(from: string, schema: ManifestSchema, name: string): Promise<number> {
  const layersDigest = getLayersFromManifest(schema);

  for (const layerDigest of layersDigest) {
    const res = await fetch(
      createRequest("POST", `/v2/${name}/blobs/uploads/?from=${from}&mount=${layerDigest}`, null, {}),
    );
    if (!res.ok) {
      throw new Error(await res.text());
    }
    expect(res.ok).toBeTruthy();
    expect(res.status).toEqual(201);
    expect(res.headers.get("docker-content-digest")).toEqual(layerDigest);
  }

  return layersDigest.length;
}

async function getReferrersIndex(name: string, digest: string, params?: URLSearchParams) {
  const query = params?.toString();
  const response = await fetch(
    createRequest("GET", `/v2/${name}/referrers/${digest}${query ? `?${query}` : ""}`, null),
  );
  expect(response.ok).toBeTruthy();
  return {
    response,
    body: (await response.json()) as ReferrersIndex,
  };
}

async function seedReferrerIndex(name: string, subjectDigest: string, descriptors: ReferrerDescriptor[]) {
  const bindings = env as Env;
  for (let start = 0; start < descriptors.length; start += 100) {
    await Promise.all(
      descriptors.slice(start, start + 100).map((descriptor) => {
        return bindings.REGISTRY.put(
          `${name}/_referrers/${subjectDigest}/${descriptor.digest}`,
          JSON.stringify(descriptor),
          {
            httpMetadata: {
              contentType: "application/json",
            },
          },
        );
      }),
    );
  }
}

describe("v2 manifests", () => {
  test("HEAD /v2/:name/manifests/:reference NOT FOUND", async () => {
    const response = await fetch(createRequest("GET", "/v2/notfound/manifests/reference", null));
    expect(response.status).toBe(404);
    const json = await response.json();
    expect(json).toEqual({
      errors: [
        {
          code: "MANIFEST_UNKNOWN",
          message: "manifest unknown",
          detail: {
            Tag: "reference",
          },
        },
      ],
    });
  });

  test("HEAD /v2/:name/manifests/:reference works", async () => {
    const reference = "123456";
    const name = "name";
    const data = "{}";
    const sha256 = await getSHA256(data);
    const bindings = env as Env;
    await bindings.REGISTRY.put(`${name}/manifests/${reference}`, "{}", {
      httpMetadata: { contentType: "application/gzip" },
      sha256: sha256.slice(SHA256_PREFIX_LEN),
    });
    const res = await fetch(createRequest("HEAD", `/v2/${name}/manifests/${reference}`, null));
    expect(res.ok).toBeTruthy();
    expect(Object.fromEntries(res.headers)).toEqual({
      "content-length": "2",
      "content-type": "application/gzip",
      "docker-content-digest": sha256,
    });
    await bindings.REGISTRY.delete(`${name}/manifests/${reference}`);
  });

  test("PUT then DELETE /v2/:name/manifests/:reference works", async () => {
    const { sha256 } = await createManifest("hello-world", await generateManifest("hello-world"), "hello");
    const bindings = env as Env;

    {
      const listObjects = await bindings.REGISTRY.list({ prefix: "hello-world/blobs/" });
      expect(listObjects.objects.length).toEqual(2);

      const gcRes = await fetch(new Request("http://registry.com/v2/hello-world/gc", { method: "POST" }));
      if (!gcRes.ok) {
        throw new Error(`${gcRes.status}: ${await gcRes.text()}`);
      }

      const listObjectsAfterGC = await bindings.REGISTRY.list({ prefix: "hello-world/blobs/" });
      expect(listObjectsAfterGC.objects.length).toEqual(2);
    }

    expect(await bindings.REGISTRY.head(`hello-world/manifests/hello`)).toBeTruthy();
    const res = await fetch(createRequest("DELETE", `/v2/hello-world/manifests/${sha256}`, null));
    expect(res.status).toEqual(202);
    expect(await bindings.REGISTRY.head(`hello-world/manifests/${sha256}`)).toBeNull();
    expect(await bindings.REGISTRY.head(`hello-world/manifests/hello`)).toBeNull();

    const listObjects = await bindings.REGISTRY.list({ prefix: "hello-world/blobs/" });
    expect(listObjects.objects.length).toEqual(2);

    const listObjectsManifests = await bindings.REGISTRY.list({ prefix: "hello-world/manifests/" });
    expect(listObjectsManifests.objects.length).toEqual(0);

    const gcRes = await fetch(new Request("http://registry.com/v2/hello-world/gc", { method: "POST" }));
    if (!gcRes.ok) {
      throw new Error(`${gcRes.status}: ${await gcRes.text()}`);
    }

    const listObjectsAfterGC = await bindings.REGISTRY.list({ prefix: "hello-world/blobs/" });
    expect(listObjectsAfterGC.objects.length).toEqual(0);
  });

  test("PUT multiple parts then DELETE /v2/:name/manifests/:reference works", async () => {
    const { sha256 } = await createManifest("hello/world", await generateManifest("hello/world"), "hello");
    const bindings = env as Env;
    expect(await bindings.REGISTRY.head(`hello/world/manifests/hello`)).toBeTruthy();
    const res = await fetch(createRequest("DELETE", `/v2/hello/world/manifests/${sha256}`, null));
    expect(res.status).toEqual(202);
    expect(await bindings.REGISTRY.head(`hello/world/manifests/${sha256}`)).toBeNull();
    expect(await bindings.REGISTRY.head(`hello/world/manifests/hello`)).toBeNull();
  });

  test("PUT then list tags with GET /v2/:name/tags/list", async () => {
    await createManifest("hello-world-list", await generateManifest("hello-world-list"), `hello`);
    const expectedRes = ["hello"];
    for (let i = 0; i < 10; i++) {
      expectedRes.push(`hello-${i}`);
    }

    expectedRes.sort();
    const shuffledRes = shuffleArray([...expectedRes]);
    for (const tag of shuffledRes) {
      await createManifest("hello-world-list", await generateManifest("hello-world-list"), tag);
    }

    const tagsRes = await fetch(createRequest("GET", `/v2/hello-world-list/tags/list?n=1000`, null));
    const tags = (await tagsRes.json()) as TagsList;
    expect(tags.name).toEqual("hello-world-list");
    expect(tags.tags).toEqual(expectedRes);

    const tagsResEmpty = await fetch(createRequest("GET", `/v2/hello-world-list-empty/tags/list`, null));
    const tagsEmpty = (await tagsResEmpty.json()) as TagsList;
    expect(tagsEmpty.tags).toHaveLength(0);
  });

  test("Upload manifests with recursive layer mounting", async () => {
    const repoA = "app-a";
    const repoB = "app-b";
    const repoC = "app-c";

    // Generate manifest
    const appManifest = await generateManifest(repoA);
    // Create architecture specific repository
    await createManifest(repoA, appManifest, `latest`);

    // Upload app from repoA to repoB
    await mountLayersFromManifest(repoA, appManifest, repoB);
    await createManifest(repoB, appManifest, `latest`);

    // Upload app from repoB to repoC
    await mountLayersFromManifest(repoB, appManifest, repoC);
    await createManifest(repoC, appManifest, `latest`);

    const bindings = env as Env;
    // Check manifest count
    {
      const manifestCountA = (await bindings.REGISTRY.list({ prefix: `${repoA}/manifests/` })).objects.length;
      const manifestCountB = (await bindings.REGISTRY.list({ prefix: `${repoB}/manifests/` })).objects.length;
      const manifestCountC = (await bindings.REGISTRY.list({ prefix: `${repoC}/manifests/` })).objects.length;
      expect(manifestCountA).toEqual(manifestCountB);
      expect(manifestCountA).toEqual(manifestCountC);
    }
    // Check blobs count
    {
      const layersCountA = (await bindings.REGISTRY.list({ prefix: `${repoA}/blobs/` })).objects.length;
      const layersCountB = (await bindings.REGISTRY.list({ prefix: `${repoB}/blobs/` })).objects.length;
      const layersCountC = (await bindings.REGISTRY.list({ prefix: `${repoC}/blobs/` })).objects.length;
      expect(layersCountA).toEqual(layersCountB);
      expect(layersCountA).toEqual(layersCountC);
    }
    // Check symlink direct layer target
    for (const layer of getLayersFromManifest(appManifest)) {
      const repoLayerB = await bindings.REGISTRY.get(`${repoB}/blobs/${layer}`);
      const repoLayerC = await bindings.REGISTRY.get(`${repoC}/blobs/${layer}`);
      expect(repoLayerB).not.toBeNull();
      expect(repoLayerC).not.toBeNull();
      if (repoLayerB !== null && repoLayerC !== null) {
        // Check if both symlink target the same original blob
        expect(await repoLayerB.text()).toEqual(`${repoA}/blobs/${layer}`);
        expect(await repoLayerC.text()).toEqual(`${repoA}/blobs/${layer}`);
        // Check layer download follow symlink
        const layerSource = await fetch(createRequest("GET", `/v2/${repoA}/blobs/${layer}`, null));
        expect(layerSource.ok).toBeTruthy();
        const sourceData = await layerSource.bytes();
        const layerB = await fetch(createRequest("GET", `/v2/${repoB}/blobs/${layer}`, null));
        expect(layerB.ok).toBeTruthy();
        const layerC = await fetch(createRequest("GET", `/v2/${repoC}/blobs/${layer}`, null));
        expect(layerC.ok).toBeTruthy();
        expect(await layerB.bytes()).toEqual(sourceData);
        expect(await layerC.bytes()).toEqual(sourceData);
      }
    }
  });
});

describe("v2 referrers", () => {
  test("PUT with subject indexes referrers and paginates results", async () => {
    const name = "referrers-index";
    const bindings = env as Env;
    const subjectManifest = getImageManifestV2(await generateManifest(name));
    const { sha256: subjectDigest } = await createManifest(name, subjectManifest, "latest");

    const subjectDescriptor = {
      mediaType: subjectManifest.mediaType,
      digest: subjectDigest,
      size: manifestSize(subjectManifest),
    };
    const artifactType = "application/vnd.cloudchamber.btrfs-chain.v1";
    const artifactManifestOne = {
      ...getImageManifestV2(await generateManifest(name)),
      artifactType,
      annotations: {
        "org.opencontainers.image.title": "chain-one",
      },
      subject: subjectDescriptor,
    } satisfies ManifestSchema;
    const artifactManifestTwo = {
      ...getImageManifestV2(await generateManifest(name)),
      artifactType,
      annotations: {
        "org.opencontainers.image.title": "chain-two",
      },
      subject: subjectDescriptor,
    } satisfies ManifestSchema;

    const { sha256: firstArtifactDigest, response: firstArtifactResponse } = await uploadManifest(
      name,
      artifactManifestOne,
    );
    const { sha256: secondArtifactDigest } = await createManifest(name, artifactManifestTwo);

    expect(firstArtifactResponse.headers.get("oci-subject")).toEqual(subjectDigest);

    const descriptorObject = await bindings.REGISTRY.get(`${name}/_referrers/${subjectDigest}/${firstArtifactDigest}`);
    expect(descriptorObject).not.toBeNull();
    expect(await descriptorObject?.json()).toEqual({
      mediaType: artifactManifestOne.mediaType,
      digest: firstArtifactDigest,
      size: manifestSize(artifactManifestOne),
      artifactType,
      annotations: artifactManifestOne.annotations,
    });

    const firstPage = await getReferrersIndex(name, subjectDigest, new URLSearchParams({ n: "1" }));
    expect(firstPage.response.headers.get("content-type")).toEqual("application/vnd.oci.image.index.v1+json");
    expect(firstPage.body.schemaVersion).toEqual(2);
    expect(firstPage.body.mediaType).toEqual("application/vnd.oci.image.index.v1+json");
    expect(firstPage.body.manifests).toHaveLength(1);
    expect(firstPage.response.headers.get("Link")).toMatch(/^<.*>; rel="next"$/);

    const nextURL = parseLinkHeaderURL(firstPage.response.headers.get("Link")!);
    const secondPage = await getReferrersIndex(name, subjectDigest, nextURL.searchParams);
    expect(secondPage.body.manifests).toHaveLength(1);
    expect(secondPage.response.headers.get("Link")).toBeNull();

    const returnedDigests = new Set([firstPage.body.manifests[0].digest, secondPage.body.manifests[0].digest]);
    expect(returnedDigests).toEqual(new Set([firstArtifactDigest, secondArtifactDigest]));
  });

  test("GET /v2/:name/referrers/:digest filters descriptors and returns empty results", async () => {
    const name = "referrers-filter";
    const subjectManifest = getImageManifestV2(await generateManifest(name));
    const { sha256: subjectDigest } = await createManifest(name, subjectManifest, "latest");
    const subjectDescriptor = {
      mediaType: subjectManifest.mediaType,
      digest: subjectDigest,
      size: manifestSize(subjectManifest),
    };

    const explicitArtifactType = "application/vnd.cloudchamber.btrfs-chain.v1";
    const derivedArtifactType = "application/vnd.cloudchamber.chain.config.v1+json";
    const explicitArtifactManifest = {
      ...getImageManifestV2(await generateManifest(name)),
      artifactType: explicitArtifactType,
      subject: subjectDescriptor,
    } satisfies ManifestSchema;
    const derivedArtifactBase = getImageManifestV2(await generateManifest(name));
    const derivedArtifactManifest = {
      ...derivedArtifactBase,
      config: {
        ...derivedArtifactBase.config,
        mediaType: derivedArtifactType,
      },
      subject: subjectDescriptor,
    } satisfies ManifestSchema;

    const { sha256: explicitArtifactDigest } = await createManifest(name, explicitArtifactManifest);
    const { sha256: derivedArtifactDigest } = await createManifest(name, derivedArtifactManifest);

    const explicitResult = await getReferrersIndex(
      name,
      subjectDigest,
      new URLSearchParams({ artifactType: explicitArtifactType }),
    );
    expect(explicitResult.response.headers.get("OCI-Filters-Applied")).toEqual("artifactType");
    expect(explicitResult.body.manifests.map((manifest) => manifest.digest)).toEqual([explicitArtifactDigest]);

    const derivedResult = await getReferrersIndex(
      name,
      subjectDigest,
      new URLSearchParams({ artifactType: derivedArtifactType }),
    );
    expect(derivedResult.body.manifests.map((manifest) => manifest.digest)).toEqual([derivedArtifactDigest]);
    expect(derivedResult.body.manifests[0].artifactType).toEqual(derivedArtifactType);

    const emptyResult = await getReferrersIndex(
      name,
      subjectDigest,
      new URLSearchParams({ artifactType: "application/vnd.cloudchamber.missing.v1" }),
    );
    expect(emptyResult.response.headers.get("OCI-Filters-Applied")).toEqual("artifactType");
    expect(emptyResult.body.manifests).toEqual([]);
  });

  test("GET /v2/:name/referrers/:digest paginates filtered results across storage pages", async () => {
    const name = "referrers-filter-pagination";
    const subjectManifest = getImageManifestV2(await generateManifest(name));
    const { sha256: subjectDigest } = await createManifest(name, subjectManifest, "latest");
    const matchingArtifactType = "application/vnd.cloudchamber.match.v1";
    const otherArtifactType = "application/vnd.cloudchamber.other.v1";

    await seedReferrerIndex(
      name,
      subjectDigest,
      Array.from({ length: 122 }, (_, index) => {
        return {
          mediaType: "application/vnd.oci.image.manifest.v1+json",
          digest: numberedDigest(index),
          size: index + 1,
          artifactType: index % 2 === 0 ? matchingArtifactType : otherArtifactType,
        } satisfies ReferrerDescriptor;
      }),
    );

    const firstPage = await getReferrersIndex(
      name,
      subjectDigest,
      new URLSearchParams({ artifactType: matchingArtifactType, n: "60" }),
    );
    expect(firstPage.body.manifests).toHaveLength(60);
    expect(firstPage.response.headers.get("Link")).toMatch(/^<.*>; rel="next"$/);

    const nextURL = parseLinkHeaderURL(firstPage.response.headers.get("Link")!);
    expect(nextURL.searchParams.get("artifactType")).toEqual(matchingArtifactType);
    expect(nextURL.searchParams.get("n")).toEqual("60");

    const secondPage = await getReferrersIndex(name, subjectDigest, nextURL.searchParams);
    expect(secondPage.body.manifests).toEqual([
      {
        mediaType: "application/vnd.oci.image.manifest.v1+json",
        digest: numberedDigest(120),
        size: 121,
        artifactType: matchingArtifactType,
      },
    ]);
    expect(secondPage.response.headers.get("Link")).toBeNull();
  });

  test("GET /v2/:name/referrers/:digest ignores invalid native descriptor entries", async () => {
    const name = "referrers-invalid-native-entry";
    const bindings = env as Env;
    const subjectManifest = getImageManifestV2(await generateManifest(name));
    const { sha256: subjectDigest } = await createManifest(name, subjectManifest, "latest");
    const artifactManifest = {
      ...getImageManifestV2(await generateManifest(name)),
      artifactType: "application/vnd.cloudchamber.btrfs-chain.v1",
      subject: {
        mediaType: subjectManifest.mediaType,
        digest: subjectDigest,
        size: manifestSize(subjectManifest),
      },
    } satisfies ManifestSchema;

    const { sha256: artifactDigest } = await createManifest(name, artifactManifest);
    await bindings.REGISTRY.put(`${name}/_referrers/${subjectDigest}/${numberedDigest(3000)}`, "{", {
      httpMetadata: { contentType: "application/json" },
    });
    await bindings.REGISTRY.put(
      `${name}/_referrers/${subjectDigest}/${numberedDigest(3001)}`,
      JSON.stringify({
        mediaType: "application/vnd.oci.image.manifest.v1+json",
        digest: numberedDigest(3002),
        size: 1,
      }),
      {
        httpMetadata: { contentType: "application/json" },
      },
    );
    await bindings.REGISTRY.put(
      `${name}/_referrers/${subjectDigest}/${numberedDigest(3003)}`,
      JSON.stringify({
        mediaType: "application/vnd.oci.image.manifest.v1+json",
        digest: numberedDigest(3003),
        size: -1,
      }),
      {
        httpMetadata: { contentType: "application/json" },
      },
    );

    const referrers = await getReferrersIndex(name, subjectDigest);
    expect(referrers.body.manifests).toEqual([
      {
        mediaType: artifactManifest.mediaType,
        digest: artifactDigest,
        size: manifestSize(artifactManifest),
        artifactType: artifactManifest.artifactType,
        annotations: artifactManifest.annotations,
      },
    ]);
  });

  test("subject-bearing OCI indexes are indexed and cleaned up", async () => {
    const name = "referrers-index-artifact";
    const bindings = env as Env;
    const subjectManifest = getImageManifestV2(await generateManifest(name));
    const { sha256: subjectDigest } = await createManifest(name, subjectManifest, "latest");

    const childManifest = getImageManifestV2(await generateManifest(name));
    const { sha256: childDigest } = await createManifest(name, childManifest, "child");

    const artifactType = "application/vnd.cloudchamber.btrfs-chain.v1";
    const artifactIndex = {
      schemaVersion: 2,
      mediaType: "application/vnd.oci.image.index.v1+json",
      artifactType,
      annotations: {
        "org.opencontainers.image.title": "chain-index",
      },
      subject: {
        mediaType: subjectManifest.mediaType,
        digest: subjectDigest,
        size: manifestSize(subjectManifest),
      },
      manifests: [
        {
          mediaType: childManifest.mediaType,
          digest: childDigest,
          size: manifestSize(childManifest),
          annotations: {
            "org.opencontainers.image.ref.name": "child",
          },
          urls: ["https://example.invalid/referrer-child"],
        },
      ],
    } satisfies ManifestSchema;

    const { sha256: artifactDigest, response } = await uploadManifest(name, artifactIndex, "artifact-index");
    expect(response.headers.get("oci-subject")).toEqual(subjectDigest);

    const descriptorObject = await bindings.REGISTRY.get(`${name}/_referrers/${subjectDigest}/${artifactDigest}`);
    expect(descriptorObject).not.toBeNull();
    expect(await descriptorObject?.json()).toEqual({
      mediaType: artifactIndex.mediaType,
      digest: artifactDigest,
      size: manifestSize(artifactIndex),
      artifactType,
      annotations: artifactIndex.annotations,
    });

    const referrers = await getReferrersIndex(name, subjectDigest, new URLSearchParams({ artifactType }));
    expect(referrers.body.manifests).toEqual([
      {
        mediaType: artifactIndex.mediaType,
        digest: artifactDigest,
        size: manifestSize(artifactIndex),
        artifactType,
        annotations: artifactIndex.annotations,
      },
    ]);

    const digestDeleteResponse = await fetch(createRequest("DELETE", `/v2/${name}/manifests/${artifactDigest}`, null));
    expect(digestDeleteResponse.status).toEqual(202);
    expect(await bindings.REGISTRY.head(`${name}/_referrers/${subjectDigest}/${artifactDigest}`)).toBeNull();
  });

  test("DELETE manifest digest cleans up referrer entries", async () => {
    const name = "referrers-delete";
    const bindings = env as Env;
    const subjectManifest = getImageManifestV2(await generateManifest(name));
    const { sha256: subjectDigest } = await createManifest(name, subjectManifest, "latest");
    const artifactManifest = {
      ...getImageManifestV2(await generateManifest(name)),
      artifactType: "application/vnd.cloudchamber.btrfs-chain.v1",
      subject: {
        mediaType: subjectManifest.mediaType,
        digest: subjectDigest,
        size: manifestSize(subjectManifest),
      },
    } satisfies ManifestSchema;

    const { sha256: artifactDigest } = await createManifest(name, artifactManifest, "artifact");
    expect(await bindings.REGISTRY.head(`${name}/_referrers/${subjectDigest}/${artifactDigest}`)).toBeTruthy();

    const tagDeleteResponse = await fetch(createRequest("DELETE", `/v2/${name}/manifests/artifact`, null));
    expect(tagDeleteResponse.status).toEqual(202);
    expect(await bindings.REGISTRY.head(`${name}/_referrers/${subjectDigest}/${artifactDigest}`)).toBeTruthy();

    const digestDeleteResponse = await fetch(createRequest("DELETE", `/v2/${name}/manifests/${artifactDigest}`, null));
    expect(digestDeleteResponse.status).toEqual(202);
    expect(await bindings.REGISTRY.head(`${name}/_referrers/${subjectDigest}/${artifactDigest}`)).toBeNull();

    const referrers = await getReferrersIndex(name, subjectDigest);
    expect(referrers.body.manifests).toEqual([]);
  });

  test("DELETE manifest digest paginates tag cleanup before removing referrer entries", async () => {
    const name = "referrers-delete-pagination";
    const bindings = env as Env;
    const subjectManifest = getImageManifestV2(await generateManifest(name));
    const { sha256: subjectDigest } = await createManifest(name, subjectManifest, "latest");
    const artifactManifest = {
      ...getImageManifestV2(await generateManifest(name)),
      artifactType: "application/vnd.cloudchamber.btrfs-chain.v1",
      subject: {
        mediaType: subjectManifest.mediaType,
        digest: subjectDigest,
        size: manifestSize(subjectManifest),
      },
    } satisfies ManifestSchema;

    const aliases = ["zz-artifact-a", "zz-artifact-b", "zz-artifact-c"];
    let artifactDigest = "";
    for (const alias of aliases) {
      const { sha256 } = await createManifest(name, artifactManifest, alias);
      artifactDigest = sha256;
    }

    let deletePath = `/v2/${name}/manifests/${artifactDigest}?limit=1`;
    let attempts = 0;
    while (true) {
      attempts++;
      const response = await fetch(createRequest("DELETE", deletePath, null));
      if (response.status === 202) {
        break;
      }

      expect(response.status).toEqual(400);
      expect(response.headers.get("Link")).toMatch(/^<.*>; rel="next"$/);
      expect(await bindings.REGISTRY.head(`${name}/manifests/${artifactDigest}`)).toBeTruthy();
      expect(await bindings.REGISTRY.head(`${name}/_referrers/${subjectDigest}/${artifactDigest}`)).toBeTruthy();

      const nextURL = parseLinkHeaderURL(response.headers.get("Link")!);
      deletePath = `${nextURL.pathname}${nextURL.search}`;
    }

    expect(attempts).toBeGreaterThan(1);
    expect(await bindings.REGISTRY.head(`${name}/manifests/${artifactDigest}`)).toBeNull();
    expect(await bindings.REGISTRY.head(`${name}/_referrers/${subjectDigest}/${artifactDigest}`)).toBeNull();
    for (const alias of aliases) {
      expect(await bindings.REGISTRY.head(`${name}/manifests/${alias}`)).toBeNull();
    }
  });

  test("DELETE manifest digest rejects invalid pagination limits", async () => {
    const name = "referrers-delete-invalid-limit";
    const subjectManifest = getImageManifestV2(await generateManifest(name));
    const { sha256: subjectDigest } = await createManifest(name, subjectManifest, "latest");
    const artifactManifest = {
      ...getImageManifestV2(await generateManifest(name)),
      artifactType: "application/vnd.cloudchamber.btrfs-chain.v1",
      subject: {
        mediaType: subjectManifest.mediaType,
        digest: subjectDigest,
        size: manifestSize(subjectManifest),
      },
    } satisfies ManifestSchema;
    const { sha256: artifactDigest } = await createManifest(name, artifactManifest, "artifact");

    const response = await fetch(createRequest("DELETE", `/v2/${name}/manifests/${artifactDigest}?limit=0`, null));
    expect(response.status).toEqual(400);
  });

  test("PUT retagging a referrer keeps prior digests discoverable while they still exist", async () => {
    const name = "referrers-retag";
    const bindings = env as Env;
    const subjectManifest = getImageManifestV2(await generateManifest(name));
    const { sha256: subjectDigest } = await createManifest(name, subjectManifest, "latest");
    const firstArtifactManifest = {
      ...getImageManifestV2(await generateManifest(name)),
      artifactType: "application/vnd.cloudchamber.btrfs-chain.v1",
      annotations: {
        "org.opencontainers.image.title": "chain-one",
      },
      subject: {
        mediaType: subjectManifest.mediaType,
        digest: subjectDigest,
        size: manifestSize(subjectManifest),
      },
    } satisfies ManifestSchema;
    const secondArtifactManifest = {
      ...getImageManifestV2(await generateManifest(name)),
      artifactType: "application/vnd.cloudchamber.btrfs-chain.v1",
      annotations: {
        "org.opencontainers.image.title": "chain-two",
      },
      subject: {
        mediaType: subjectManifest.mediaType,
        digest: subjectDigest,
        size: manifestSize(subjectManifest),
      },
    } satisfies ManifestSchema;

    const { sha256: firstArtifactDigest } = await createManifest(name, firstArtifactManifest, "artifact");
    const { sha256: secondArtifactDigest } = await createManifest(name, secondArtifactManifest, "artifact");

    expect(firstArtifactDigest).not.toEqual(secondArtifactDigest);
    expect(await bindings.REGISTRY.head(`${name}/_referrers/${subjectDigest}/${firstArtifactDigest}`)).toBeTruthy();

    const referrers = await getReferrersIndex(name, subjectDigest);
    expect(new Set(referrers.body.manifests.map((manifest) => manifest.digest))).toEqual(
      new Set([firstArtifactDigest, secondArtifactDigest]),
    );

    await runGarbageCollector(name, "untagged");
    expect(await bindings.REGISTRY.head(`${name}/manifests/${firstArtifactDigest}`)).toBeTruthy();
    expect(await bindings.REGISTRY.head(`${name}/manifests/${secondArtifactDigest}`)).toBeTruthy();
  });

  test("DELETE manifest digest cleans up referrer entries for legacy subject metadata", async () => {
    const name = "referrers-delete-legacy";
    const bindings = env as Env;
    const subjectManifest = getImageManifestV2(await generateManifest(name));
    const { sha256: subjectDigest } = await createManifest(name, subjectManifest, "latest");
    const artifactManifest = {
      ...getImageManifestV2(await generateManifest(name)),
      artifactType: "application/vnd.cloudchamber.btrfs-chain.v1",
      subject: {
        mediaType: subjectManifest.mediaType,
        digest: subjectDigest,
        size: manifestSize(subjectManifest),
      },
    } satisfies ManifestSchema;

    const { sha256: artifactDigest } = await createManifest(name, artifactManifest, "artifact");
    const artifactText = JSON.stringify(artifactManifest);
    await bindings.REGISTRY.put(`${name}/manifests/${artifactDigest}`, artifactText, {
      httpMetadata: { contentType: "application/gzip" },
      sha256: artifactDigest.slice(SHA256_PREFIX_LEN),
    });

    const digestDeleteResponse = await fetch(createRequest("DELETE", `/v2/${name}/manifests/${artifactDigest}`, null));
    expect(digestDeleteResponse.status).toEqual(202);
    expect(await bindings.REGISTRY.head(`${name}/_referrers/${subjectDigest}/${artifactDigest}`)).toBeNull();
  });

  test("GC keeps live subject referrers and prunes them after subject removal", async () => {
    const name = "referrers-gc";
    const bindings = env as Env;
    const subjectManifest = getImageManifestV2(await generateManifest(name));
    const { sha256: subjectDigest } = await createManifest(name, subjectManifest, "latest");
    const artifactManifest = {
      ...getImageManifestV2(await generateManifest(name)),
      artifactType: "application/vnd.cloudchamber.btrfs-chain.v1",
      subject: {
        mediaType: subjectManifest.mediaType,
        digest: subjectDigest,
        size: manifestSize(subjectManifest),
      },
    } satisfies ManifestSchema;

    const { sha256: artifactDigest } = await createManifest(name, artifactManifest);

    await runGarbageCollector(name, "both");
    expect(await bindings.REGISTRY.head(`${name}/manifests/${artifactDigest}`)).toBeTruthy();

    const subjectDeleteResponse = await fetch(createRequest("DELETE", `/v2/${name}/manifests/${subjectDigest}`, null));
    expect(subjectDeleteResponse.status).toEqual(202);

    expect((await bindings.REGISTRY.list({ prefix: `${name}/_referrers/${subjectDigest}/` })).objects).toHaveLength(1);
    expect((await getReferrersIndex(name, subjectDigest)).body.manifests).toEqual([
      {
        mediaType: artifactManifest.mediaType,
        digest: artifactDigest,
        size: manifestSize(artifactManifest),
        artifactType: artifactManifest.artifactType,
        annotations: artifactManifest.annotations,
      },
    ]);

    await runGarbageCollector(name, "both");
    expect(await bindings.REGISTRY.head(`${name}/manifests/${artifactDigest}`)).toBeNull();
    expect(await bindings.REGISTRY.head(`${name}/_referrers/${subjectDigest}/${artifactDigest}`)).toBeNull();

    const referrers = await getReferrersIndex(name, subjectDigest);
    expect(referrers.body.manifests).toEqual([]);
  });

  test("untagged GC keeps tagged referrers indexed after subject removal", async () => {
    const name = "referrers-gc-tagged-referrer";
    const bindings = env as Env;
    const subjectManifest = getImageManifestV2(await generateManifest(name));
    const { sha256: subjectDigest } = await createManifest(name, subjectManifest, "latest");
    const artifactManifest = {
      ...getImageManifestV2(await generateManifest(name)),
      artifactType: "application/vnd.cloudchamber.btrfs-chain.v1",
      annotations: {
        "org.opencontainers.image.title": "tagged-referrer",
      },
      subject: {
        mediaType: subjectManifest.mediaType,
        digest: subjectDigest,
        size: manifestSize(subjectManifest),
      },
    } satisfies ManifestSchema;

    const { sha256: artifactDigest } = await createManifest(name, artifactManifest, "artifact");

    const subjectTagDeleteResponse = await fetch(createRequest("DELETE", `/v2/${name}/manifests/latest`, null));
    expect(subjectTagDeleteResponse.status).toEqual(202);

    await runGarbageCollector(name, "untagged");
    expect(await bindings.REGISTRY.head(`${name}/manifests/${subjectDigest}`)).toBeNull();
    expect(await bindings.REGISTRY.head(`${name}/manifests/${artifactDigest}`)).toBeTruthy();
    expect(await bindings.REGISTRY.head(`${name}/manifests/artifact`)).toBeTruthy();
    expect(await bindings.REGISTRY.head(`${name}/_referrers/${subjectDigest}/${artifactDigest}`)).toBeTruthy();

    const referrers = await getReferrersIndex(name, subjectDigest);
    expect(referrers.body.manifests).toEqual([
      {
        mediaType: artifactManifest.mediaType,
        digest: artifactDigest,
        size: manifestSize(artifactManifest),
        artifactType: artifactManifest.artifactType,
        annotations: artifactManifest.annotations,
      },
    ]);
  });

  test("GC keeps child manifests of live referrer OCI indexes", async () => {
    const name = "referrers-gc-index-children";
    const bindings = env as Env;
    const subjectManifest = getImageManifestV2(await generateManifest(name));
    const { sha256: subjectDigest } = await createManifest(name, subjectManifest, "latest");
    const childManifest = getImageManifestV2(await generateManifest(name));
    const { sha256: childDigest } = await createManifest(name, childManifest);
    const artifactIndex = {
      schemaVersion: 2,
      mediaType: "application/vnd.oci.image.index.v1+json",
      artifactType: "application/vnd.cloudchamber.btrfs-chain.v1",
      subject: {
        mediaType: subjectManifest.mediaType,
        digest: subjectDigest,
        size: manifestSize(subjectManifest),
      },
      manifests: [
        {
          mediaType: childManifest.mediaType,
          digest: childDigest,
          size: manifestSize(childManifest),
        },
      ],
    } satisfies ManifestSchema;

    const { sha256: artifactDigest } = await createManifest(name, artifactIndex);

    await runGarbageCollector(name, "untagged");
    expect(await bindings.REGISTRY.head(`${name}/manifests/${artifactDigest}`)).toBeTruthy();
    expect(await bindings.REGISTRY.head(`${name}/manifests/${childDigest}`)).toBeTruthy();

    const subjectTagDeleteResponse = await fetch(createRequest("DELETE", `/v2/${name}/manifests/latest`, null));
    expect(subjectTagDeleteResponse.status).toEqual(202);

    await runGarbageCollector(name, "untagged");
    expect(await bindings.REGISTRY.head(`${name}/manifests/${subjectDigest}`)).toBeNull();
    expect(await bindings.REGISTRY.head(`${name}/manifests/${artifactDigest}`)).toBeNull();
    expect(await bindings.REGISTRY.head(`${name}/manifests/${childDigest}`)).toBeNull();
  });

  test("untagged GC removes referrers whose subjects are pruned in the same pass", async () => {
    const name = "referrers-gc-untagged";
    const bindings = env as Env;
    const subjectManifest = getImageManifestV2(await generateManifest(name));
    const { sha256: subjectDigest } = await createManifest(name, subjectManifest, "latest");
    const artifactManifest = {
      ...getImageManifestV2(await generateManifest(name)),
      artifactType: "application/vnd.cloudchamber.btrfs-chain.v1",
      subject: {
        mediaType: subjectManifest.mediaType,
        digest: subjectDigest,
        size: manifestSize(subjectManifest),
      },
    } satisfies ManifestSchema;

    const { sha256: artifactDigest } = await createManifest(name, artifactManifest);

    const subjectTagDeleteResponse = await fetch(createRequest("DELETE", `/v2/${name}/manifests/latest`, null));
    expect(subjectTagDeleteResponse.status).toEqual(202);

    await runGarbageCollector(name, "untagged");
    expect(await bindings.REGISTRY.head(`${name}/manifests/${subjectDigest}`)).toBeNull();
    expect(await bindings.REGISTRY.head(`${name}/manifests/${artifactDigest}`)).toBeNull();
    expect(await bindings.REGISTRY.head(`${name}/_referrers/${subjectDigest}/${artifactDigest}`)).toBeNull();

    const referrers = await getReferrersIndex(name, subjectDigest);
    expect(referrers.body.manifests).toEqual([]);
  });

  test("PUT /v2/:name/manifests/:reference rejects invalid subject digests", async () => {
    const name = "referrers-invalid-subject";
    const artifactManifest = {
      ...getImageManifestV2(await generateManifest(name)),
      artifactType: "application/vnd.cloudchamber.btrfs-chain.v1",
      subject: {
        mediaType: "application/vnd.oci.image.manifest.v1+json",
        digest: "sha256/not-a-digest",
        size: 123,
      },
    } satisfies ManifestSchema;

    const response = await fetch(
      createRequest("PUT", `/v2/${name}/manifests/artifact`, new Blob([JSON.stringify(artifactManifest)]).stream(), {
        "Content-Type": "application/gzip",
      }),
    );
    expect(response.status).toEqual(400);
  });

  test("PUT /v2/:name/manifests/:reference rejects missing local subjects", async () => {
    const name = "referrers-missing-subject";
    const bindings = env as Env;
    const missingSubjectDigest = numberedDigest(4500);
    const artifactManifest = {
      ...getImageManifestV2(await generateManifest(name)),
      artifactType: "application/vnd.cloudchamber.btrfs-chain.v1",
      subject: {
        mediaType: "application/vnd.oci.image.manifest.v1+json",
        digest: missingSubjectDigest,
        size: 123,
      },
    } satisfies ManifestSchema;
    const artifactDigest = await getSHA256(JSON.stringify(artifactManifest));

    const response = await fetch(
      createRequest("PUT", `/v2/${name}/manifests/artifact`, new Blob([JSON.stringify(artifactManifest)]).stream(), {
        "Content-Type": "application/gzip",
      }),
    );

    expect(response.status).toEqual(400);
    expect((await response.json()) as { errors: { code: string; message: string }[] }).toEqual({
      errors: [expect.objectContaining({ code: "BLOB_UNKNOWN", message: `unknown subject ${missingSubjectDigest}` })],
    });
    expect(await bindings.REGISTRY.head(`${name}/manifests/artifact`)).toBeNull();
    expect(await bindings.REGISTRY.head(`${name}/manifests/${artifactDigest}`)).toBeNull();
    expect(await bindings.REGISTRY.head(`${name}/_referrers/${missingSubjectDigest}/${artifactDigest}`)).toBeNull();
  });

  test("PUT /v2/:name/manifests/:reference rejects invalid subject-bearing OCI indexes", async () => {
    const name = "referrers-invalid-index";
    const subjectManifest = getImageManifestV2(await generateManifest(name));
    const { sha256: subjectDigest } = await createManifest(name, subjectManifest, "latest");
    const invalidIndex = {
      schemaVersion: 2,
      mediaType: "application/vnd.oci.image.index.v1+json",
      artifactType: "application/vnd.cloudchamber.btrfs-chain.v1",
      subject: {
        mediaType: subjectManifest.mediaType,
        digest: subjectDigest,
        size: manifestSize(subjectManifest),
      },
      manifests: [
        {
          mediaType: "application/vnd.oci.image.manifest.v1+json",
          digest: numberedDigest(4000),
          size: "not-a-number",
        },
      ],
    };

    const response = await fetch(
      createRequest("PUT", `/v2/${name}/manifests/invalid-index`, new Blob([JSON.stringify(invalidIndex)]).stream(), {
        "Content-Type": "application/vnd.oci.image.index.v1+json",
      }),
    );
    expect(response.status).toEqual(400);
    expect((await response.json()) as { errors: { code: string }[] }).toEqual({
      errors: [expect.objectContaining({ code: "MANIFEST_INVALID" })],
    });
  });

  test("GET /v2/:name/referrers/:digest accepts OCI digest syntax", async () => {
    const name = "referrers-digest-syntax";
    const subjectDigest = "sha512.b64:AbCdEf0123_-=";

    await seedReferrerIndex(name, subjectDigest, [
      {
        mediaType: "application/vnd.oci.image.manifest.v1+json",
        digest: numberedDigest(1),
        size: 1,
      },
    ]);

    const response = await fetch(createRequest("GET", `/v2/${name}/referrers/${subjectDigest}`, null));
    expect(response.status).toEqual(200);
    expect(((await response.json()) as ReferrersIndex).manifests).toHaveLength(1);
  });

  test("GET /v2/:name/referrers/:digest rejects invalid digests and pagination params", async () => {
    const validDigest = numberedDigest(9999);
    const uppercaseDigest = `sha256:${"A".repeat(64)}`;

    const invalidDigestResponse = await fetch(
      createRequest("GET", "/v2/referrers-invalid/referrers/not-a-digest", null),
    );
    expect(invalidDigestResponse.status).toEqual(400);

    const uppercaseDigestResponse = await fetch(
      createRequest("GET", `/v2/referrers-invalid/referrers/${uppercaseDigest}`, null),
    );
    expect(uppercaseDigestResponse.status).toEqual(400);

    const zeroLimitResponse = await fetch(
      createRequest("GET", `/v2/referrers-invalid/referrers/${validDigest}?n=0`, null),
    );
    expect(zeroLimitResponse.status).toEqual(400);

    const fractionalLimitResponse = await fetch(
      createRequest("GET", `/v2/referrers-invalid/referrers/${validDigest}?n=1.5`, null),
    );
    expect(fractionalLimitResponse.status).toEqual(400);

    const oversizedLimitResponse = await fetch(
      createRequest("GET", `/v2/referrers-invalid/referrers/${validDigest}?n=1001`, null),
    );
    expect(oversizedLimitResponse.status).toEqual(400);

    const invalidLastResponse = await fetch(
      createRequest("GET", `/v2/referrers-invalid/referrers/${validDigest}?last=not-a-digest`, null),
    );
    expect(invalidLastResponse.status).toEqual(400);
  });
});

describe("tokens", async () => {
  test("auth payload push on /v2", async () => {
    const { verified } = RegistryTokens.verifyPayload(createRequest("GET", "/v2/", null), {
      capabilities: ["push"],
    } as RegistryAuthProtocolTokenPayload);
    expect(verified).toBeTruthy();
  });

  test("auth payload pull on /v2", async () => {
    const { verified } = RegistryTokens.verifyPayload(createRequest("GET", "/v2/", null), {
      capabilities: ["pull"],
    } as RegistryAuthProtocolTokenPayload);
    expect(verified).toBeTruthy();
  });

  test("auth payload push on /v2/whatever with HEAD", async () => {
    const { verified } = RegistryTokens.verifyPayload(createRequest("HEAD", "/v2/whatever", null), {
      capabilities: ["push"],
    } as RegistryAuthProtocolTokenPayload);
    expect(verified).toBeTruthy();
  });

  test("auth payload push on /v2/whatever with mutations", async () => {
    for (const mutationMethod of ["PATCH", "POST", "DELETE"]) {
      const { verified } = RegistryTokens.verifyPayload(createRequest(mutationMethod, "/v2/whatever", null), {
        capabilities: ["push"],
      } as RegistryAuthProtocolTokenPayload);
      expect(verified).toBeTruthy();
    }
  });

  test("auth payload pull on /v2/whatever with mutations", async () => {
    for (const mutationMethod of ["PATCH", "POST", "DELETE"]) {
      const { verified } = RegistryTokens.verifyPayload(createRequest(mutationMethod, "/v2/whatever", null), {
        capabilities: ["pull"],
      } as RegistryAuthProtocolTokenPayload);
      expect(verified).toBeFalsy();
    }
  });

  test("auth payload push/pull on /v2/whatever with mutations", async () => {
    for (const mutationMethod of ["PATCH", "POST", "DELETE"]) {
      const { verified } = RegistryTokens.verifyPayload(createRequest(mutationMethod, "/v2/whatever", null), {
        capabilities: ["pull", "push"],
      } as RegistryAuthProtocolTokenPayload);
      expect(verified).toBeTruthy();
    }
  });

  test("auth payload push on GET", async () => {
    const { verified } = RegistryTokens.verifyPayload(createRequest("GET", "/v2/whatever", null), {
      capabilities: ["push"],
    } as RegistryAuthProtocolTokenPayload);
    expect(verified).toBeFalsy();
  });

  test("auth payload push/pull on GET", async () => {
    const { verified } = RegistryTokens.verifyPayload(createRequest("GET", "/v2/whatever", null), {
      capabilities: ["push", "pull"],
    } as RegistryAuthProtocolTokenPayload);
    expect(verified).toBeTruthy();
  });
});

test("registries configuration", async () => {
  const testCases = [
    {
      configuration: undefined,
      expected: [],
      error: "",
      partialError: false,
    },
    {
      configuration: "[]",
      expected: [],
      error: "",
      partialError: false,
    },
    {
      configuration: "{}",
      expected: [],
      error: "Error parsing registries JSON: zod error: - invalid_type: Expected array, received object: ",
      partialError: false,
    },
    {
      configuration: "[{}]",
      expected: [],
      error: "Error parsing registries JSON: zod error: - invalid_type: Required: 0,registry",
      partialError: false,
    },
    {
      configuration: `[{ "registry": "no-url/hello-world" }]`,
      expected: [],
      error: "Error parsing registries JSON: zod error: - invalid_string: Invalid url: 0,registry",
      partialError: false,
    },
    {
      configuration: "bla bla bla no json",
      expected: [],
      error: "Error parsing registries JSON: error SyntaxError: Unexpected token",
      partialError: true,
    },
    {
      configuration: `[{
        "registry": "https://hello.com/domain",
        "username": "hello world",
        "password_env": "PASSWORD_ENV"
      }]`,
      expected: [
        {
          registry: "https://hello.com/domain",
          username: "hello world",
          password_env: "PASSWORD_ENV",
        },
      ],
      partialError: false,
      error: "",
    },
    {
      configuration: `[{
        "registry": "https://hello.com/domain",
        "username": "hello world",
        "password_env": "PASSWORD_ENV"
      }, {
        "registry": "https://hello2.com/domain",
        "username": "hello world 2",
        "password_env": "PASSWORD_ENV 2"
      }]`,
      expected: [
        {
          registry: "https://hello.com/domain",
          username: "hello world",
          password_env: "PASSWORD_ENV",
        },
        {
          registry: "https://hello2.com/domain",
          username: "hello world 2",
          password_env: "PASSWORD_ENV 2",
        },
      ],
      partialError: false,
      error: "",
    },
    {
      configuration: `[{
        "registry": "https://hello.com/domain"
      }]`,
      expected: [
        {
          registry: "https://hello.com/domain",
        },
      ],
      partialError: false,
      error: "",
    },
  ] as const;

  const bindings = env as Env;
  const bindingCopy = { ...bindings };
  for (const testCase of testCases) {
    bindingCopy.REGISTRIES_JSON = testCase.configuration;
    const expectErrorOutput = testCase.error !== "";
    let calledError = false;
    const prevConsoleError = console.error;
    console.error = (output) => {
      if (!testCase.partialError) {
        expect(output).toEqual(testCase.error);
      } else {
        expect(output).toContain(testCase.error);
      }

      calledError = true;
    };
    const r = registries(bindingCopy);
    expect(r).toEqual(testCase.expected);
    expect(calledError).toEqual(expectErrorOutput);
    console.error = prevConsoleError;
  }
});

describe("http client", () => {
  const bindings = env as Env;
  let envBindings = { ...bindings };
  const prevFetch = global.fetch;

  afterAll(() => {
    global.fetch = prevFetch;
  });

  test("test manifest exists", async () => {
    envBindings = { ...bindings };
    envBindings.JWT_REGISTRY_TOKENS_PUBLIC_KEY = "";
    envBindings.PASSWORD = "world";
    envBindings.USERNAME = "hello";
    envBindings.REGISTRIES_JSON = undefined;
    global.fetch = (async (input: Parameters<typeof global.fetch>[0], init?: Parameters<typeof global.fetch>[1]) => {
      return fetch(new Request(input as string | URL | Request, init));
    }) as typeof global.fetch;
    const client = new RegistryHTTPClient(envBindings, {
      registry: "https://localhost",
      password_env: "PASSWORD",
      username,
    });
    const res = await client.manifestExists("namespace/hello", "latest");
    if ("response" in res) {
      expect(await res.response.json()).toEqual({ status: res.response.status });
    }

    expect("exists" in res && res.exists).toBe(false);
  });

  test("test manifest exists with readonly", async () => {
    envBindings = { ...bindings };
    envBindings.JWT_REGISTRY_TOKENS_PUBLIC_KEY = "";
    envBindings.PASSWORD = "";
    envBindings.USERNAME = "";
    envBindings.READONLY_PASSWORD = "world";
    envBindings.READONLY_USERNAME = "hello";
    envBindings.REGISTRIES_JSON = undefined;
    global.fetch = (async (input: Parameters<typeof global.fetch>[0], init?: Parameters<typeof global.fetch>[1]) => {
      return fetch(new Request(input as string | URL | Request, init));
    }) as typeof global.fetch;
    const client = new RegistryHTTPClient(envBindings, {
      registry: "https://localhost",
      password_env: "PASSWORD",
      username,
    });
    const res = await client.manifestExists("namespace/hello", "latest");
    if ("response" in res) {
      expect(await res.response.json()).toEqual({ status: res.response.status });
    }

    expect("exists" in res && res.exists).toBe(false);
  });

  test("test list referrers", async () => {
    const name = "http-client-referrers";
    const subjectDigest = numberedDigest(9997);
    const artifactType = "application/vnd.cloudchamber.btrfs-chain.v1";
    const firstArtifactDigest = numberedDigest(9998);
    const secondArtifactDigest = numberedDigest(9999);
    const firstDescriptor = {
      mediaType: "application/vnd.oci.image.manifest.v1+json",
      digest: firstArtifactDigest,
      size: 123,
      artifactType,
      annotations: {
        "org.opencontainers.image.title": "http-client-one",
      },
    } satisfies ReferrerDescriptor;
    const secondDescriptor = {
      mediaType: "application/vnd.oci.image.manifest.v1+json",
      digest: secondArtifactDigest,
      size: 456,
      artifactType,
      annotations: {
        "org.opencontainers.image.title": "http-client-two",
      },
    } satisfies ReferrerDescriptor;

    envBindings = { ...bindings };
    envBindings.JWT_REGISTRY_TOKENS_PUBLIC_KEY = "";
    envBindings.PASSWORD = "world";
    envBindings.USERNAME = "hello";
    envBindings.REGISTRIES_JSON = undefined;
    let referrersRequests = 0;
    global.fetch = (async (input: Parameters<typeof global.fetch>[0], init?: Parameters<typeof global.fetch>[1]) => {
      const request = new Request(input as string | URL | Request, init);
      const url = new URL(request.url);
      if (url.pathname === "/v2/" || url.pathname === "/v2") {
        return new Response(null, { status: 200 });
      }

      expect(url.pathname).toEqual(`/v2/${name}/referrers/${subjectDigest}`);

      referrersRequests++;
      if (referrersRequests === 1) {
        expect(url.searchParams.get("artifactType")).toEqual(artifactType);
        expect(url.searchParams.get("n")).toEqual("1");
        expect(url.searchParams.get("last")).toBeNull();
        const firstResponse = new Response(
          JSON.stringify({
            schemaVersion: 2,
            mediaType: "application/vnd.oci.image.index.v1+json",
            manifests: [firstDescriptor],
          }),
          {
            status: 200,
            headers: {
              "Content-Type": "application/vnd.oci.image.index.v1+json",
              "Link": `</v2/${name}/referrers/${subjectDigest}?page=2>; rel="next"`,
            },
          },
        );
        Object.defineProperty(firstResponse, "url", {
          value: `https://localhost/v2/${name}/referrers/${subjectDigest}?artifactType=${encodeURIComponent(
            artifactType,
          )}&n=1`,
        });
        return firstResponse;
      }

      expect(url.searchParams.get("page")).toEqual("2");
      return new Response(
        JSON.stringify({
          schemaVersion: 2,
          mediaType: "application/vnd.oci.image.index.v1+json",
          manifests: [secondDescriptor],
        }),
        {
          status: 200,
          headers: {
            "Content-Type": "application/vnd.oci.image.index.v1+json",
          },
        },
      );
    }) as typeof global.fetch;
    const client = new RegistryHTTPClient(envBindings, {
      registry: "https://localhost",
      password_env: "PASSWORD",
      username,
    });

    const firstPage = await client.listReferrers(name, subjectDigest, { artifactType, limit: 1 });
    if ("response" in firstPage) {
      expect(await firstPage.response.json()).toEqual({ status: firstPage.response.status });
      throw new Error("expected listReferrers to succeed");
    }

    expect(firstPage.cursor).toEqual(`/v2/${name}/referrers/${subjectDigest}?page=2`);
    expect(firstPage.manifests).toEqual([firstDescriptor]);

    const secondPage = await client.listReferrers(name, subjectDigest, {
      artifactType,
      limit: 1,
      last: firstPage.cursor,
    });
    if ("response" in secondPage) {
      expect(await secondPage.response.json()).toEqual({ status: secondPage.response.status });
      throw new Error("expected paginated listReferrers to succeed");
    }

    expect(secondPage.cursor).toBeUndefined();
    expect(secondPage.manifests).toEqual([secondDescriptor]);
    expect(new Set([firstPage.manifests[0].digest, secondPage.manifests[0].digest])).toEqual(
      new Set([firstArtifactDigest, secondArtifactDigest]),
    );
  });

  test("test list referrers selects rel next from multi-link headers", async () => {
    const name = "http-client-referrers-multilink";
    const subjectDigest = numberedDigest(9970);
    const firstDescriptor = {
      mediaType: "application/vnd.oci.image.manifest.v1+json",
      digest: numberedDigest(9971),
      size: 11,
    } satisfies ReferrerDescriptor;
    const secondDescriptor = {
      mediaType: "application/vnd.oci.image.manifest.v1+json",
      digest: numberedDigest(9972),
      size: 22,
    } satisfies ReferrerDescriptor;

    envBindings = { ...bindings };
    envBindings.JWT_REGISTRY_TOKENS_PUBLIC_KEY = "";
    envBindings.PASSWORD = "world";
    envBindings.USERNAME = "hello";
    envBindings.REGISTRIES_JSON = undefined;
    let referrersRequests = 0;
    global.fetch = (async (input: Parameters<typeof global.fetch>[0], init?: Parameters<typeof global.fetch>[1]) => {
      const request = new Request(input as string | URL | Request, init);
      const url = new URL(request.url);
      if (url.pathname === "/v2/" || url.pathname === "/v2") {
        return new Response(null, { status: 200 });
      }

      expect(url.pathname).toEqual(`/v2/${name}/referrers/${subjectDigest}`);
      referrersRequests++;
      if (referrersRequests === 1) {
        const firstResponse = new Response(
          JSON.stringify({
            schemaVersion: 2,
            mediaType: "application/vnd.oci.image.index.v1+json",
            manifests: [firstDescriptor],
          }),
          {
            status: 200,
            headers: {
              "Content-Type": "application/vnd.oci.image.index.v1+json",
              "Link": `</v2/${name}/tags/list?n=1>; rel="alternate", </v2/${name}/referrers/${subjectDigest}?page=2>; rel="next"`,
            },
          },
        );
        Object.defineProperty(firstResponse, "url", {
          value: `https://localhost/v2/${name}/referrers/${subjectDigest}`,
        });
        return firstResponse;
      }

      expect(url.searchParams.get("page")).toEqual("2");
      return new Response(
        JSON.stringify({
          schemaVersion: 2,
          mediaType: "application/vnd.oci.image.index.v1+json",
          manifests: [secondDescriptor],
        }),
        {
          status: 200,
          headers: {
            "Content-Type": "application/vnd.oci.image.index.v1+json",
          },
        },
      );
    }) as typeof global.fetch;

    const client = new RegistryHTTPClient(envBindings, {
      registry: "https://localhost",
      password_env: "PASSWORD",
      username,
    });

    const firstPage = await client.listReferrers(name, subjectDigest, { limit: 1 });
    if ("response" in firstPage) {
      expect(await firstPage.response.json()).toEqual({ status: firstPage.response.status });
      throw new Error("expected multi-link listReferrers to succeed");
    }

    expect(firstPage.cursor).toEqual(`/v2/${name}/referrers/${subjectDigest}?page=2`);

    const secondPage = await client.listReferrers(name, subjectDigest, { limit: 1, last: firstPage.cursor });
    if ("response" in secondPage) {
      expect(await secondPage.response.json()).toEqual({ status: secondPage.response.status });
      throw new Error("expected paginated multi-link listReferrers to succeed");
    }

    expect(secondPage.manifests).toEqual([secondDescriptor]);
  });

  test("test list referrers ignores same-origin next links for different paths", async () => {
    const name = "http-client-referrers-wrong-path";
    const subjectDigest = numberedDigest(9960);
    const descriptor = {
      mediaType: "application/vnd.oci.image.manifest.v1+json",
      digest: numberedDigest(9961),
      size: 333,
    } satisfies ReferrerDescriptor;

    envBindings = { ...bindings };
    envBindings.JWT_REGISTRY_TOKENS_PUBLIC_KEY = "";
    envBindings.PASSWORD = "world";
    envBindings.USERNAME = "hello";
    envBindings.REGISTRIES_JSON = undefined;
    global.fetch = (async (input: Parameters<typeof global.fetch>[0], init?: Parameters<typeof global.fetch>[1]) => {
      const request = new Request(input as string | URL | Request, init);
      const url = new URL(request.url);
      if (url.pathname === "/v2/" || url.pathname === "/v2") {
        return new Response(null, { status: 200 });
      }

      const response = new Response(
        JSON.stringify({
          schemaVersion: 2,
          mediaType: "application/vnd.oci.image.index.v1+json",
          manifests: [descriptor],
        }),
        {
          status: 200,
          headers: {
            "Content-Type": "application/vnd.oci.image.index.v1+json",
            "Link": `</v2/${name}/tags/list?n=1>; rel="next"`,
          },
        },
      );
      Object.defineProperty(response, "url", {
        value: `https://localhost/v2/${name}/referrers/${subjectDigest}`,
      });
      return response;
    }) as typeof global.fetch;

    const client = new RegistryHTTPClient(envBindings, {
      registry: "https://localhost",
      password_env: "PASSWORD",
      username,
    });

    const res = await client.listReferrers(name, subjectDigest);
    if ("response" in res) {
      expect(await res.response.json()).toEqual({ status: res.response.status });
      throw new Error("expected listReferrers to ignore wrong-path next links");
    }

    expect(res.cursor).toBeUndefined();
    expect(res.manifests).toEqual([descriptor]);
  });

  test("test list referrers does not fall back after an opaque next-link 404", async () => {
    const name = "http-client-referrers-opaque-404";
    const subjectDigest = numberedDigest(9980);
    const descriptor = {
      mediaType: "application/vnd.oci.image.manifest.v1+json",
      digest: numberedDigest(9981),
      size: 111,
    } satisfies ReferrerDescriptor;

    envBindings = { ...bindings };
    envBindings.JWT_REGISTRY_TOKENS_PUBLIC_KEY = "";
    envBindings.PASSWORD = "world";
    envBindings.USERNAME = "hello";
    envBindings.REGISTRIES_JSON = undefined;
    let referrersRequests = 0;
    global.fetch = (async (input: Parameters<typeof global.fetch>[0], init?: Parameters<typeof global.fetch>[1]) => {
      const request = new Request(input as string | URL | Request, init);
      const url = new URL(request.url);
      if (url.pathname === "/v2/" || url.pathname === "/v2") {
        return new Response(null, { status: 200 });
      }

      expect(url.pathname).toEqual(`/v2/${name}/referrers/${subjectDigest}`);
      referrersRequests++;
      if (referrersRequests === 1) {
        const firstResponse = new Response(
          JSON.stringify({
            schemaVersion: 2,
            mediaType: "application/vnd.oci.image.index.v1+json",
            manifests: [descriptor],
          }),
          {
            status: 200,
            headers: {
              "Content-Type": "application/vnd.oci.image.index.v1+json",
              "Link": `</v2/${name}/referrers/${subjectDigest}?page=2>; rel="next"`,
            },
          },
        );
        Object.defineProperty(firstResponse, "url", {
          value: `https://localhost/v2/${name}/referrers/${subjectDigest}`,
        });
        return firstResponse;
      }

      expect(url.searchParams.get("page")).toEqual("2");
      return new Response(JSON.stringify({ status: 404 }), {
        status: 404,
        headers: { "Content-Type": "application/json" },
      });
    }) as typeof global.fetch;

    const client = new RegistryHTTPClient(envBindings, {
      registry: "https://localhost",
      password_env: "PASSWORD",
      username,
    });

    const firstPage = await client.listReferrers(name, subjectDigest, { limit: 1 });
    if ("response" in firstPage) {
      expect(await firstPage.response.json()).toEqual({ status: firstPage.response.status });
      throw new Error("expected the initial opaque pagination request to succeed");
    }

    const secondPage = await client.listReferrers(name, subjectDigest, { limit: 1, last: firstPage.cursor });
    expect("response" in secondPage).toBeTruthy();
    if ("response" in secondPage) {
      expect(secondPage.response.status).toEqual(404);
    }
  });

  test("test list referrers ignores cross-origin next links", async () => {
    const name = "http-client-referrers-xorigin";
    const subjectDigest = numberedDigest(9010);
    const descriptor = {
      mediaType: "application/vnd.oci.image.manifest.v1+json",
      digest: numberedDigest(9011),
      size: 654,
    } satisfies ReferrerDescriptor;

    envBindings = { ...bindings };
    envBindings.JWT_REGISTRY_TOKENS_PUBLIC_KEY = "";
    envBindings.PASSWORD = "world";
    envBindings.USERNAME = "hello";
    envBindings.REGISTRIES_JSON = undefined;
    global.fetch = (async (input: Parameters<typeof global.fetch>[0], init?: Parameters<typeof global.fetch>[1]) => {
      const request = new Request(input as string | URL | Request, init);
      const url = new URL(request.url);
      if (url.pathname === "/v2/" || url.pathname === "/v2") {
        return new Response(null, { status: 200 });
      }

      const response = new Response(
        JSON.stringify({
          schemaVersion: 2,
          mediaType: "application/vnd.oci.image.index.v1+json",
          manifests: [descriptor],
        }),
        {
          status: 200,
          headers: {
            "Content-Type": "application/vnd.oci.image.index.v1+json",
            "Link": `<https://attacker.example/v2/${name}/referrers/${subjectDigest}?page=2>; rel="next"`,
          },
        },
      );
      Object.defineProperty(response, "url", {
        value: `https://localhost/v2/${name}/referrers/${subjectDigest}`,
      });
      return response;
    }) as typeof global.fetch;

    const client = new RegistryHTTPClient(envBindings, {
      registry: "https://localhost",
      password_env: "PASSWORD",
      username,
    });

    const res = await client.listReferrers(name, subjectDigest);
    if ("response" in res) {
      expect(await res.response.json()).toEqual({ status: res.response.status });
      throw new Error("expected listReferrers to ignore unsafe next links");
    }

    expect(res.cursor).toBeUndefined();
    expect(res.manifests).toEqual([descriptor]);
  });
});

describe("push and catalog", () => {
  test("push and then use the catalog", async () => {
    await createManifest("hello-world-main", await generateManifest("hello-world-main"), "hello");
    await createManifest("hello-world-main", await generateManifest("hello-world-main"), "latest");
    await createManifest("hello-world-main", await generateManifest("hello-world-main"), "hello-2");
    await createManifest("hello", await generateManifest("hello"), "hello");
    await createManifest("hello/hello", await generateManifest("hello/hello"), "hello");

    const response = await fetch(createRequest("GET", "/v2/_catalog", null));
    expect(response.ok).toBeTruthy();
    const body = (await response.json()) as { repositories: string[] };
    expect(body).toEqual({
      repositories: ["hello-world-main", "hello/hello", "hello"],
    });
    const expectedRepositories = body.repositories;
    const tagsRes = await fetch(createRequest("GET", `/v2/hello-world-main/tags/list?n=1000`, null));
    const tags = (await tagsRes.json()) as TagsList;
    expect(tags.name).toEqual("hello-world-main");
    expect(tags.tags).toEqual(["hello", "hello-2", "latest"]);

    const repositoryBuildUp: string[] = [];
    let currentPath = "/v2/_catalog?n=1";
    for (let i = 0; i < 3; i++) {
      const response = await fetch(createRequest("GET", currentPath, null));
      expect(response.ok).toBeTruthy();
      const body = (await response.json()) as { repositories: string[] };
      if (body.repositories.length === 0) {
        break;
      }
      expect(body.repositories).toHaveLength(1);

      repositoryBuildUp.push(...body.repositories);
      const url = parseLinkHeaderURL(response.headers.get("Link")!);
      currentPath = url.pathname + url.search;
    }

    expect(repositoryBuildUp).toEqual(expectedRepositories);

    // Check blobs count
    const bindings = env as Env;
    {
      const listObjects = await bindings.REGISTRY.list({ prefix: "hello-world-main/blobs/" });
      expect(listObjects.objects.length).toEqual(6);
    }
    {
      const listObjects = await bindings.REGISTRY.list({ prefix: "hello/blobs/" });
      expect(listObjects.objects.length).toEqual(2);
    }
    {
      const listObjects = await bindings.REGISTRY.list({ prefix: "hello/hello/blobs/" });
      expect(listObjects.objects.length).toEqual(2);
    }
  });

  test("(v1) push and then use the catalog", async () => {
    await createManifest("hello-world-main", await generateManifest("hello-world-main", 1), "hello");
    await createManifest("hello-world-main", await generateManifest("hello-world-main", 1), "latest");
    await createManifest("hello-world-main", await generateManifest("hello-world-main", 1), "hello-2");
    await createManifest("hello", await generateManifest("hello", 1), "hello");
    await createManifest("hello/hello", await generateManifest("hello/hello", 1), "hello");

    const response = await fetch(createRequest("GET", "/v2/_catalog", null));
    expect(response.ok).toBeTruthy();
    const body = (await response.json()) as { repositories: string[] };
    expect(body).toEqual({
      repositories: ["hello-world-main", "hello/hello", "hello"],
    });
    const expectedRepositories = body.repositories;
    const tagsRes = await fetch(createRequest("GET", `/v2/hello-world-main/tags/list?n=1000`, null));
    const tags = (await tagsRes.json()) as TagsList;
    expect(tags.name).toEqual("hello-world-main");
    expect(tags.tags).toEqual(["hello", "hello-2", "latest"]);

    const repositoryBuildUp: string[] = [];
    let currentPath = "/v2/_catalog?n=1";
    for (let i = 0; i < 3; i++) {
      const response = await fetch(createRequest("GET", currentPath, null));
      expect(response.ok).toBeTruthy();
      const body = (await response.json()) as { repositories: string[] };
      if (body.repositories.length === 0) {
        break;
      }
      expect(body.repositories).toHaveLength(1);

      repositoryBuildUp.push(...body.repositories);
      const url = parseLinkHeaderURL(response.headers.get("Link")!);
      currentPath = url.pathname + url.search;
    }

    expect(repositoryBuildUp).toEqual(expectedRepositories);

    // Check blobs count
    const bindings = env as Env;
    {
      const listObjects = await bindings.REGISTRY.list({ prefix: "hello-world-main/blobs/" });
      expect(listObjects.objects.length).toEqual(3);
    }
    {
      const listObjects = await bindings.REGISTRY.list({ prefix: "hello/blobs/" });
      expect(listObjects.objects.length).toEqual(1);
    }
    {
      const listObjects = await bindings.REGISTRY.list({ prefix: "hello/hello/blobs/" });
      expect(listObjects.objects.length).toEqual(1);
    }
  });

  test("catalog ignores referrer index entries", async () => {
    const name = "catalog-referrers";
    const subjectManifest = getImageManifestV2(await generateManifest(name));
    const { sha256: subjectDigest } = await createManifest(name, subjectManifest, "latest");
    const artifactManifest = {
      ...getImageManifestV2(await generateManifest(name)),
      artifactType: "application/vnd.cloudchamber.btrfs-chain.v1",
      subject: {
        mediaType: subjectManifest.mediaType,
        digest: subjectDigest,
        size: manifestSize(subjectManifest),
      },
    } satisfies ManifestSchema;
    await createManifest(name, artifactManifest, "artifact");

    const response = await fetch(createRequest("GET", "/v2/_catalog?n=1000", null));
    expect(response.ok).toBeTruthy();
    const body = (await response.json()) as { repositories: string[] };

    expect(body.repositories).toContain(name);
    expect(body.repositories.filter((repository) => repository.startsWith(`${name}/_referrers`))).toEqual([]);
  });
});

async function createManifestList(name: string, tag?: string): Promise<string[]> {
  // Generate manifest
  const amdManifest = await generateManifest(name);
  const armManifest = await generateManifest(name);
  const manifestList = await generateManifestList(amdManifest, armManifest);

  if (!tag) {
    const manifestListData = JSON.stringify(manifestList);
    tag = await getSHA256(manifestListData);
  }
  const { sha256: amdSha256 } = await createManifest(name, amdManifest);
  const { sha256: armSha256 } = await createManifest(name, armManifest);
  const { sha256 } = await createManifest(name, manifestList, tag);
  return [amdSha256, armSha256, sha256];
}

describe("v2 manifest-list", () => {
  test("Upload manifest-list", async () => {
    const name = "m-arch";
    const tag = "app";
    const manifestsSha256 = await createManifestList(name, tag);

    const bindings = env as Env;
    expect(await bindings.REGISTRY.head(`${name}/manifests/${tag}`)).toBeTruthy();
    for (const digest of manifestsSha256) {
      expect(await bindings.REGISTRY.head(`${name}/manifests/${digest}`)).toBeTruthy();
    }

    // Delete tag only
    const res = await fetch(createRequest("DELETE", `/v2/${name}/manifests/${tag}`, null));
    expect(res.status).toEqual(202);
    expect(await bindings.REGISTRY.head(`${name}/manifests/${tag}`)).toBeNull();
    for (const digest of manifestsSha256) {
      expect(await bindings.REGISTRY.head(`${name}/manifests/${digest}`)).toBeTruthy();
    }

    // Check blobs count (2 config and 2 layer)
    {
      const listObjects = await bindings.REGISTRY.list({ prefix: `${name}/blobs/` });
      expect(listObjects.objects.length).toEqual(4);
    }

    for (const digest of manifestsSha256) {
      const res = await fetch(createRequest("DELETE", `/v2/${name}/manifests/${digest}`, null));
      expect(res.status).toEqual(202);
    }
    for (const digest of manifestsSha256) {
      expect(await bindings.REGISTRY.head(`${name}/manifests/${digest}`)).toBeNull();
    }
  });

  test("Reject docker manifest-list descriptors without platform", async () => {
    const name = "m-arch-invalid";
    const amdManifest = await generateManifest(name);
    const armManifest = await generateManifest(name);
    const { sha256: amdSha256 } = await createManifest(name, amdManifest);
    const { sha256: armSha256 } = await createManifest(name, armManifest);
    const invalidManifestList = {
      schemaVersion: 2,
      mediaType: "application/vnd.docker.distribution.manifest.list.v2+json",
      manifests: [
        {
          mediaType: "application/vnd.docker.distribution.manifest.v2+json",
          size: JSON.stringify(amdManifest).length,
          digest: amdSha256,
        },
        {
          mediaType: "application/vnd.docker.distribution.manifest.v2+json",
          size: JSON.stringify(armManifest).length,
          digest: armSha256,
        },
      ],
    };

    const response = await fetch(
      createRequest("PUT", `/v2/${name}/manifests/app`, new Blob([JSON.stringify(invalidManifestList)]).stream(), {
        "Content-Type": "application/vnd.docker.distribution.manifest.list.v2+json",
      }),
    );
    expect(response.status).toEqual(400);
    expect((await response.json()) as { errors: { code: string }[] }).toEqual({
      errors: [expect.objectContaining({ code: "MANIFEST_INVALID" })],
    });
  });

  test("Upload manifest-list with layer mounting", async () => {
    const preprodName = "m-arch-pp";
    const prodName = "m-arch";
    const tag = "app";
    // Generate manifest
    const amdManifest = await generateManifest(preprodName);
    const armManifest = await generateManifest(preprodName);
    // Create architecture specific repository
    await createManifest(preprodName, amdManifest, `${tag}-amd`);
    await createManifest(preprodName, armManifest, `${tag}-arm`);

    // Create manifest-list on prod repository
    const bindings = env as Env;
    // Step 1 mount blobs
    await mountLayersFromManifest(preprodName, amdManifest, prodName);
    await mountLayersFromManifest(preprodName, armManifest, prodName);
    // Check blobs count (2 config and 2 layer)
    {
      const listObjects = await bindings.REGISTRY.list({ prefix: `${preprodName}/blobs/` });
      expect(listObjects.objects.length).toEqual(4);
    }
    {
      const listObjects = await bindings.REGISTRY.list({ prefix: `${prodName}/blobs/` });
      expect(listObjects.objects.length).toEqual(4);
    }

    // Step 2 create manifest
    const { sha256: amdSha256 } = await createManifest(prodName, amdManifest);
    const { sha256: armSha256 } = await createManifest(prodName, armManifest);

    // Step 3 create manifest list
    const manifestList = await generateManifestList(amdManifest, armManifest);
    const { sha256 } = await createManifest(prodName, manifestList, tag);

    expect(await bindings.REGISTRY.head(`${prodName}/manifests/${tag}`)).toBeTruthy();
    expect(await bindings.REGISTRY.head(`${prodName}/manifests/${sha256}`)).toBeTruthy();
    expect(await bindings.REGISTRY.head(`${prodName}/manifests/${amdSha256}`)).toBeTruthy();
    expect(await bindings.REGISTRY.head(`${prodName}/manifests/${armSha256}`)).toBeTruthy();

    // Check symlink binding
    expect(amdManifest.schemaVersion === 2).toBeTruthy();
    expect("manifests" in amdManifest).toBeFalsy();
    if (amdManifest.schemaVersion === 2 && !("manifests" in amdManifest)) {
      const layerDigest = amdManifest.layers[0].digest;
      const layerSource = await fetch(createRequest("GET", `/v2/${preprodName}/blobs/${layerDigest}`, null));
      expect(layerSource.ok).toBeTruthy();
      const layerLinked = await fetch(createRequest("GET", `/v2/${prodName}/blobs/${layerDigest}`, null));
      expect(layerLinked.ok).toBeTruthy();
      expect(await layerLinked.text()).toEqual(await layerSource.text());
    }
  });
});

async function runGarbageCollector(name: string, mode: "unreferenced" | "untagged" | "both"): Promise<void> {
  if (mode === "unreferenced" || mode === "both") {
    const gcRes = await fetch(createRequest("POST", `/v2/${name}/gc?mode=unreferenced`, null));
    if (!gcRes.ok) {
      throw new Error(`${gcRes.status}: ${await gcRes.text()}`);
    }
    expect(gcRes.status).toEqual(200);
    const response: { success: boolean } = await gcRes.json();
    expect(response.success).toBeTruthy();
  }
  if (mode === "untagged" || mode === "both") {
    const gcRes = await fetch(createRequest("POST", `/v2/${name}/gc?mode=untagged`, null));
    if (!gcRes.ok) {
      throw new Error(`${gcRes.status}: ${await gcRes.text()}`);
    }
    expect(gcRes.status).toEqual(200);
    const response: { success: boolean } = await gcRes.json();
    expect(response.success).toBeTruthy();
  }
}

describe("garbage collector", () => {
  test("Single arch image", async () => {
    const name = "hello";
    const manifestOld = await generateManifest(name);
    await createManifest(name, manifestOld, "v1");
    const manifestLatest = await generateManifest(name);
    await createManifest(name, manifestLatest, "v2");
    await createManifest(name, manifestLatest, "app");
    const bindings = env as Env;
    // Check no action needed
    {
      const listManifests = await bindings.REGISTRY.list({ prefix: `${name}/manifests/` });
      expect(listManifests.objects.length).toEqual(5);
      const listBlobs = await bindings.REGISTRY.list({ prefix: `${name}/blobs/` });
      expect(listBlobs.objects.length).toEqual(4);
    }

    await runGarbageCollector(name, "both");

    {
      const listManifests = await bindings.REGISTRY.list({ prefix: `${name}/manifests/` });
      expect(listManifests.objects.length).toEqual(5);
      const listBlobs = await bindings.REGISTRY.list({ prefix: `${name}/blobs/` });
      expect(listBlobs.objects.length).toEqual(4);
    }
    // Removing manifest tag - GC untagged mode will clean image
    const res = await fetch(createRequest("DELETE", `/v2/${name}/manifests/v1`, null));
    expect(res.status).toEqual(202);
    await runGarbageCollector(name, "unreferenced");
    {
      const listManifests = await bindings.REGISTRY.list({ prefix: `${name}/manifests/` });
      expect(listManifests.objects.length).toEqual(4);
      const listBlobs = await bindings.REGISTRY.list({ prefix: `${name}/blobs/` });
      expect(listBlobs.objects.length).toEqual(4);
    }
    await runGarbageCollector(name, "untagged");
    {
      const listManifests = await bindings.REGISTRY.list({ prefix: `${name}/manifests/` });
      expect(listManifests.objects.length).toEqual(3);
      const listBlobs = await bindings.REGISTRY.list({ prefix: `${name}/blobs/` });
      expect(listBlobs.objects.length).toEqual(2);
    }
    // Add an unreferenced blobs
    {
      const { sha256: tempSha256 } = await createManifest(name, await generateManifest(name));
      const res = await fetch(createRequest("DELETE", `/v2/${name}/manifests/${tempSha256}`, null));
      expect(res.status).toEqual(202);
    }
    // Removed manifest - GC unreferenced mode will clean blobs
    {
      const listManifests = await bindings.REGISTRY.list({ prefix: `${name}/manifests/` });
      expect(listManifests.objects.length).toEqual(3);
      const listBlobs = await bindings.REGISTRY.list({ prefix: `${name}/blobs/` });
      expect(listBlobs.objects.length).toEqual(4);
    }

    await runGarbageCollector(name, "unreferenced");
    {
      const listManifests = await bindings.REGISTRY.list({ prefix: `${name}/manifests/` });
      expect(listManifests.objects.length).toEqual(3);
      const listBlobs = await bindings.REGISTRY.list({ prefix: `${name}/blobs/` });
      expect(listBlobs.objects.length).toEqual(2);
    }
  });

  test("Multi-arch image", async () => {
    const name = "hello";
    await createManifestList(name, "app");
    const bindings = env as Env;
    // Check no action needed
    {
      const listManifests = await bindings.REGISTRY.list({ prefix: `${name}/manifests/` });
      expect(listManifests.objects.length).toEqual(4);
      const listBlobs = await bindings.REGISTRY.list({ prefix: `${name}/blobs/` });
      expect(listBlobs.objects.length).toEqual(4);
    }

    await runGarbageCollector(name, "both");

    {
      const listManifests = await bindings.REGISTRY.list({ prefix: `${name}/manifests/` });
      expect(listManifests.objects.length).toEqual(4);
      const listBlobs = await bindings.REGISTRY.list({ prefix: `${name}/blobs/` });
      expect(listBlobs.objects.length).toEqual(4);
    }

    // Add unreferenced blobs

    {
      const manifests = await createManifestList(name, "bis");
      for (const manifest of manifests) {
        const res = await fetch(createRequest("DELETE", `/v2/${name}/manifests/${manifest}`, null));
        expect(res.status).toEqual(202);
      }
    }
    {
      const listManifests = await bindings.REGISTRY.list({ prefix: `${name}/manifests/` });
      expect(listManifests.objects.length).toEqual(4);
      const listBlobs = await bindings.REGISTRY.list({ prefix: `${name}/blobs/` });
      expect(listBlobs.objects.length).toEqual(8);
    }

    await runGarbageCollector(name, "unreferenced");

    {
      const listManifests = await bindings.REGISTRY.list({ prefix: `${name}/manifests/` });
      expect(listManifests.objects.length).toEqual(4);
      const listBlobs = await bindings.REGISTRY.list({ prefix: `${name}/blobs/` });
      expect(listBlobs.objects.length).toEqual(4);
    }

    // Add untagged manifest
    {
      const res = await fetch(createRequest("DELETE", `/v2/${name}/manifests/app`, null));
      expect(res.status).toEqual(202);
    }
    {
      const listManifests = await bindings.REGISTRY.list({ prefix: `${name}/manifests/` });
      expect(listManifests.objects.length).toEqual(3);
      const listBlobs = await bindings.REGISTRY.list({ prefix: `${name}/blobs/` });
      expect(listBlobs.objects.length).toEqual(4);
    }

    await runGarbageCollector(name, "unreferenced");

    {
      const listManifests = await bindings.REGISTRY.list({ prefix: `${name}/manifests/` });
      expect(listManifests.objects.length).toEqual(3);
      const listBlobs = await bindings.REGISTRY.list({ prefix: `${name}/blobs/` });
      expect(listBlobs.objects.length).toEqual(4);
    }

    await runGarbageCollector(name, "untagged");

    {
      const listManifests = await bindings.REGISTRY.list({ prefix: `${name}/manifests/` });
      expect(listManifests.objects.length).toEqual(0);
      const listBlobs = await bindings.REGISTRY.list({ prefix: `${name}/blobs/` });
      expect(listBlobs.objects.length).toEqual(0);
    }
  });

  test("Multi-arch image with symlink layers", async () => {
    // Deploy multi-repo multi-arch image
    const preprodBame = "m-arch-pp";
    const prodName = "m-arch";
    const tag = "app";
    // Generate manifest
    const amdManifest = await generateManifest(preprodBame);
    const armManifest = await generateManifest(preprodBame);
    // Create architecture specific repository
    await createManifest(preprodBame, amdManifest, `${tag}-amd`);
    await createManifest(preprodBame, armManifest, `${tag}-arm`);

    // Create manifest-list on prod repository
    const bindings = env as Env;
    // Step 1 mount blobs
    await mountLayersFromManifest(preprodBame, amdManifest, prodName);
    await mountLayersFromManifest(preprodBame, armManifest, prodName);

    // Step 2 create manifest
    await createManifest(prodName, amdManifest);
    await createManifest(prodName, armManifest);

    // Step 3 create manifest list
    const manifestList = await generateManifestList(amdManifest, armManifest);
    await createManifest(prodName, manifestList, tag);

    // Check no action needed
    {
      const listManifests = await bindings.REGISTRY.list({ prefix: `${preprodBame}/manifests/` });
      expect(listManifests.objects.length).toEqual(4);
      const listBlobs = await bindings.REGISTRY.list({ prefix: `${preprodBame}/blobs/` });
      expect(listBlobs.objects.length).toEqual(4);
    }

    await runGarbageCollector(preprodBame, "both");

    {
      const listManifests = await bindings.REGISTRY.list({ prefix: `${preprodBame}/manifests/` });
      expect(listManifests.objects.length).toEqual(4);
      const listBlobs = await bindings.REGISTRY.list({ prefix: `${preprodBame}/blobs/` });
      expect(listBlobs.objects.length).toEqual(4);
    }

    // Untagged preprod repo
    {
      const res = await fetch(createRequest("DELETE", `/v2/${preprodBame}/manifests/${tag}-amd`, null));
      expect(res.status).toEqual(202);
      const res2 = await fetch(createRequest("DELETE", `/v2/${preprodBame}/manifests/${tag}-arm`, null));
      expect(res2.status).toEqual(202);
    }
    await runGarbageCollector(preprodBame, "unreferenced");
    {
      const listManifests = await bindings.REGISTRY.list({ prefix: `${preprodBame}/manifests/` });
      expect(listManifests.objects.length).toEqual(2);
      const listBlobs = await bindings.REGISTRY.list({ prefix: `${preprodBame}/blobs/` });
      expect(listBlobs.objects.length).toEqual(4);
    }
    await runGarbageCollector(preprodBame, "untagged");
    {
      const listManifests = await bindings.REGISTRY.list({ prefix: `${preprodBame}/manifests/` });
      expect(listManifests.objects.length).toEqual(0);
      const listBlobs = await bindings.REGISTRY.list({ prefix: `${preprodBame}/blobs/` });
      expect(listBlobs.objects.length).toEqual(4);
    }

    // Untagged prod repo
    {
      const res = await fetch(createRequest("DELETE", `/v2/${prodName}/manifests/${tag}`, null));
      expect(res.status).toEqual(202);
    }
    {
      const listManifests = await bindings.REGISTRY.list({ prefix: `${prodName}/manifests/` });
      expect(listManifests.objects.length).toEqual(3);
      const listBlobs = await bindings.REGISTRY.list({ prefix: `${prodName}/blobs/` });
      expect(listBlobs.objects.length).toEqual(4);
    }
    await runGarbageCollector(prodName, "untagged");
    {
      const listManifests = await bindings.REGISTRY.list({ prefix: `${prodName}/manifests/` });
      expect(listManifests.objects.length).toEqual(0);
      const listBlobs = await bindings.REGISTRY.list({ prefix: `${prodName}/blobs/` });
      expect(listBlobs.objects.length).toEqual(0);
    }
    await runGarbageCollector(preprodBame, "unreferenced");
    {
      const listManifests = await bindings.REGISTRY.list({ prefix: `${prodName}/manifests/` });
      expect(listManifests.objects.length).toEqual(0);
      const listBlobs = await bindings.REGISTRY.list({ prefix: `${prodName}/blobs/` });
      expect(listBlobs.objects.length).toEqual(0);
    }
  });
});

test("docker.io", () => {
  const t = [
    ["https://docker.io", true],
    ["https://d.docker.io", true],
    ["https://d.dockerr.io", false],
    ["https://dddocker.io", false],
    ["https://docker.ioo", false],
    ["https://d.docker.com", false],
    ["https://docker.com", false],
    ["http://docker.io", false],
  ] as const;
  for (const testCase of t) {
    const isDocker = isDockerDotIO(new URL(testCase[0]));
    if (isDocker !== testCase[1]) {
      throw new Error(`Expected ${testCase[1]} on ${testCase[0]} but got ${isDocker}`);
    }
  }
});
