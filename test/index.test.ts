import { afterAll, describe, expect, test } from "vitest";
import { SHA256_PREFIX_LEN, getSHA256 } from "../src/user";
import { TagsList } from "../src/router";
import { Env } from "..";
import { RegistryTokens } from "../src/token";
import { RegistryAuthProtocolTokenPayload } from "../src/auth";
import { registries } from "../src/registry/registry";
import { RegistryHTTPClient } from "../src/registry/http";
import { encode } from "@cfworker/base64url";
import { ManifestSchema } from "../src/manifest";
import { limit } from "../src/chunk";
import worker from "../index";
import { createExecutionContext, env, waitOnExecutionContext } from "cloudflare:test";

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
  r.headers.append("Authorization", usernamePasswordToAuth("hello", "world"));
  return await fetchUnauth(r);
}

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

async function createManifest(name: string, schema: ManifestSchema, tag?: string): Promise<{ sha256: string }> {
  const data = JSON.stringify(schema);
  const sha256 = await getSHA256(data);
  if (!tag) {
    tag = sha256;
  }

  const res = await fetch(
    createRequest("PUT", `/v2/${name}/manifests/${tag}`, new Blob([data]).stream(), {
      "Content-Type": "application/gzip",
    }),
  );
  if (!res.ok) {
    throw new Error(await res.text());
  }
  expect(res.ok).toBeTruthy();
  expect(res.headers.get("docker-content-digest")).toEqual(sha256);
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
    const manifestList = new Set<string>();
    const { sha256 } = await createManifest("hello-world-list", await generateManifest("hello-world-list"), `hello`);
    manifestList.add(sha256);
    const expectedRes = ["hello"];
    for (let i = 0; i < 40; i++) {
      expectedRes.push(`hello-${i}`);
    }

    expectedRes.sort();
    const shuffledRes = shuffleArray([...expectedRes]);
    for (const tag of shuffledRes) {
      const { sha256 } = await createManifest("hello-world-list", await generateManifest("hello-world-list"), tag);
      manifestList.add(sha256);
    }

    const tagsRes = await fetch(createRequest("GET", `/v2/hello-world-list/tags/list?n=1000`, null));
    const tags = (await tagsRes.json()) as TagsList;
    expect(tags.name).toEqual("hello-world-list");
    expect(tags.tags).toEqual(expectedRes);

    for (const manifestSha256 of manifestList) {
      const res = await fetch(createRequest("DELETE", `/v2/hello-world-list/manifests/${manifestSha256}`, null));
      expect(res.ok).toBeTruthy();
    }
    const tagsResEmpty = await fetch(createRequest("GET", `/v2/hello-world-list/tags/list`, null));
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
      error:
        "Error parsing registries JSON: zod error: - invalid_type: Required: 0,registry\n\t- invalid_type: Required: 0,password_env\n\t- invalid_type: Required: 0,username",
      partialError: false,
    },
    {
      configuration: `[{ "registry": "no-url/hello-world" }]`,
      expected: [],
      error:
        "Error parsing registries JSON: zod error: - invalid_string: Invalid url: 0,registry\n\t- invalid_type: Required: 0,password_env\n\t- invalid_type: Required: 0,username",
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
    global.fetch = async function (r: RequestInfo): Promise<Response> {
      return fetch(new Request(r));
    };
    const client = new RegistryHTTPClient(envBindings, {
      registry: "https://localhost",
      password_env: "PASSWORD",
      username: "hello",
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
    global.fetch = async function (r: RequestInfo): Promise<Response> {
      return fetch(new Request(r));
    };
    const client = new RegistryHTTPClient(envBindings, {
      registry: "https://localhost",
      password_env: "PASSWORD",
      username: "hello",
    });
    const res = await client.manifestExists("namespace/hello", "latest");
    if ("response" in res) {
      expect(await res.response.json()).toEqual({ status: res.response.status });
    }

    expect("exists" in res && res.exists).toBe(false);
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
    expect(tags.tags).toEqual([
      "hello",
      "hello-2",
      "latest",
    ]);

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
      const url = new URL(response.headers.get("Link")!.split(";")[0].trim());
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
    expect(tags.tags).toEqual([
      "hello",
      "hello-2",
      "latest",
    ]);

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
      const url = new URL(response.headers.get("Link")!.split(";")[0].trim());
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
