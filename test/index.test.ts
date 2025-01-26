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
  const layer_data = Math.random().toString(36).substring(2); // Random string data
  const layer_sha256 = await getSHA256(layer_data);
  // Upload layer data
  const res = await fetch(createRequest("POST", `/v2/${name}/blobs/uploads/`, null, {}));
  expect(res.ok).toBeTruthy();
  const blob = new Blob([layer_data]).stream();
  const stream = limit(blob, layer_data.length);
  const res2 = await fetch(createRequest("PATCH", res.headers.get("location")!, stream, {}));
  expect(res2.ok).toBeTruthy();
  const last = await fetch(createRequest("PUT", res2.headers.get("location")! + "&digest=" + layer_sha256, null, {}));
  expect(last.ok).toBeTruthy();
  // Config data
  const config_data = Math.random().toString(36).substring(2); // Random string data
  const config_sha256 = await getSHA256(config_data);
  if (schemaVersion === 2) {
    // Upload config layer
    const config_res = await fetch(createRequest("POST", `/v2/${name}/blobs/uploads/`, null, {}));
    expect(config_res.ok).toBeTruthy();
    const config_blob = new Blob([config_data]).stream();
    const config_stream = limit(config_blob, config_data.length);
    const config_res2 = await fetch(createRequest("PATCH", config_res.headers.get("location")!, config_stream, {}));
    expect(config_res2.ok).toBeTruthy();
    const config_last = await fetch(
      createRequest("PUT", config_res2.headers.get("location")! + "&digest=" + config_sha256, null, {}),
    );
    expect(config_last.ok).toBeTruthy();
  }
  return schemaVersion === 1
    ? {
        schemaVersion,
        fsLayers: [{ blobSum: layer_sha256 }],
        architecture: "amd64",
      }
    : {
        schemaVersion,
        layers: [
          { size: layer_data.length, digest: layer_sha256, mediaType: "shouldbeanything" },
          { size: layer_data.length, digest: layer_sha256, mediaType: "shouldbeanything" },
        ],
        config: { size: config_data.length, digest: config_sha256, mediaType: "configmediatypeshouldntbechecked" },
        mediaType: "shouldalsobeanythingforretrocompatibility",
      };
}

async function generateManifestList(
  amd_manifest: ManifestSchema,
  arm_manifest: ManifestSchema,
): Promise<ManifestSchema> {
  const amd_manifest_data = JSON.stringify(amd_manifest);
  const arm_manifest_data = JSON.stringify(arm_manifest);
  return {
    schemaVersion: 2,
    mediaType: "application/vnd.docker.distribution.manifest.list.v2+json",
    manifests: [
      {
        mediaType: "application/vnd.docker.distribution.manifest.v2+json",
        size: amd_manifest_data.length,
        digest: await getSHA256(amd_manifest_data),
        platform: {
          architecture: "amd64",
          os: "linux",
        },
      },
      {
        mediaType: "application/vnd.docker.distribution.manifest.v2+json",
        size: arm_manifest_data.length,
        digest: await getSHA256(arm_manifest_data),
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

async function mountLayersFromManifest(from: string, schema: ManifestSchema, name: string): Promise<number> {
  const layers_digest = [];
  if (schema.schemaVersion === 1) {
    for (const layer of schema.fsLayers) {
      layers_digest.push(layer.blobSum);
    }
  } else if (schema.schemaVersion === 2 && !("manifests" in schema)) {
    layers_digest.push(schema.config.digest);
    for (const layer of schema.layers) {
      layers_digest.push(layer.digest);
    }
  }

  for (const layer_digest of layers_digest) {
    const res = await fetch(
      createRequest("POST", `/v2/${name}/blobs/uploads/?from=${from}&mount=${layer_digest}`, null, {}),
    );
    if (!res.ok) {
      throw new Error(await res.text());
    }
    expect(res.ok).toBeTruthy();
    expect(res.status).toEqual(201);
    expect(res.headers.get("docker-content-digest")).toEqual(layer_digest);
  }

  return layers_digest.length;
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
    const manifest_list = new Set<string>();
    const { sha256 } = await createManifest("hello-world-list", await generateManifest("hello-world-list"), `hello`);
    manifest_list.add(sha256);
    const expectedRes = ["hello", sha256];
    for (let i = 0; i < 40; i++) {
      expectedRes.push(`hello-${i}`);
    }

    expectedRes.sort();
    const shuffledRes = shuffleArray([...expectedRes]);
    for (const tag of shuffledRes) {
      const { sha256 } = await createManifest("hello-world-list", await generateManifest("hello-world-list"), tag);
      manifest_list.add(sha256);
    }

    const tagsRes = await fetch(createRequest("GET", `/v2/hello-world-list/tags/list?n=1000`, null));
    const tags = (await tagsRes.json()) as TagsList;
    expect(tags.name).toEqual("hello-world-list");
    expect(tags.tags).containSubset(expectedRes); // TODO this test will be overwrite by PR #89 once merged

    for (const manifest_sha256 of manifest_list) {
      const res = await fetch(createRequest("DELETE", `/v2/hello-world-list/manifests/${manifest_sha256}`, null));
      expect(res.ok).toBeTruthy();
    }
    const tagsResEmpty = await fetch(createRequest("GET", `/v2/hello-world-list/tags/list`, null));
    const tagsEmpty = (await tagsResEmpty.json()) as TagsList;
    expect(tagsEmpty.tags).toHaveLength(0);
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
    expect(tags.tags).containSubset(["hello", "hello-2", "latest"]); // TODO this test will be overwrite by PR #89 once merged

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
    expect(tags.tags).containSubset(["hello", "hello-2", "latest"]); // TODO this test will be overwrite by PR #89 once merged

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
  const amd_manifest = await generateManifest(name);
  const arm_manifest = await generateManifest(name);
  const manifest_list = await generateManifestList(amd_manifest, arm_manifest);

  if (!tag) {
    const manifest_list_data = JSON.stringify(manifest_list);
    tag = await getSHA256(manifest_list_data);
  }
  const { sha256: amd_sha256 } = await createManifest(name, amd_manifest);
  const { sha256: arm_sha256 } = await createManifest(name, arm_manifest);
  const { sha256 } = await createManifest(name, manifest_list, tag);
  return [amd_sha256, arm_sha256, sha256];
}

describe("v2 manifest-list", () => {
  test("Upload manifest-list", async () => {
    const name = "m-arch";
    const tag = "app";
    const manifests_sha256 = await createManifestList(name, tag);

    const bindings = env as Env;
    expect(await bindings.REGISTRY.head(`${name}/manifests/${tag}`)).toBeTruthy();
    for (const digest of manifests_sha256) {
      expect(await bindings.REGISTRY.head(`${name}/manifests/${digest}`)).toBeTruthy();
    }

    // Delete tag only
    const res = await fetch(createRequest("DELETE", `/v2/${name}/manifests/${tag}`, null));
    expect(res.status).toEqual(202);
    expect(await bindings.REGISTRY.head(`${name}/manifests/${tag}`)).toBeNull();
    for (const digest of manifests_sha256) {
      expect(await bindings.REGISTRY.head(`${name}/manifests/${digest}`)).toBeTruthy();
    }

    // Check blobs count (2 config and 2 layer)
    {
      const listObjects = await bindings.REGISTRY.list({ prefix: `${name}/blobs/` });
      expect(listObjects.objects.length).toEqual(4);
    }

    for (const digest of manifests_sha256) {
      const res = await fetch(createRequest("DELETE", `/v2/${name}/manifests/${digest}`, null));
      expect(res.status).toEqual(202);
    }
    for (const digest of manifests_sha256) {
      expect(await bindings.REGISTRY.head(`${name}/manifests/${digest}`)).toBeNull();
    }
  });

  test("Upload manifest-list with layer mounting", async () => {
    const preprod_name = "m-arch-pp";
    const prod_name = "m-arch";
    const tag = "app";
    // Generate manifest
    const amd_manifest = await generateManifest(preprod_name);
    const arm_manifest = await generateManifest(preprod_name);
    // Create architecture specific repository
    await createManifest(preprod_name, amd_manifest, `${tag}-amd`);
    await createManifest(preprod_name, arm_manifest, `${tag}-arm`);

    // Create manifest-list on prod repository
    const bindings = env as Env;
    // Step 1 mount blobs
    await mountLayersFromManifest(preprod_name, amd_manifest, prod_name);
    await mountLayersFromManifest(preprod_name, arm_manifest, prod_name);
    // Check blobs count (2 config and 2 layer)
    {
      const listObjects = await bindings.REGISTRY.list({ prefix: `${preprod_name}/blobs/` });
      expect(listObjects.objects.length).toEqual(4);
    }
    {
      const listObjects = await bindings.REGISTRY.list({ prefix: `${prod_name}/blobs/` });
      expect(listObjects.objects.length).toEqual(4);
    }

    // Step 2 create manifest
    const { sha256: amd_sha256 } = await createManifest(prod_name, amd_manifest);
    const { sha256: arm_sha256 } = await createManifest(prod_name, arm_manifest);

    // Step 3 create manifest list
    const manifest_list = await generateManifestList(amd_manifest, arm_manifest);
    const { sha256 } = await createManifest(prod_name, manifest_list, tag);

    expect(await bindings.REGISTRY.head(`${prod_name}/manifests/${tag}`)).toBeTruthy();
    expect(await bindings.REGISTRY.head(`${prod_name}/manifests/${sha256}`)).toBeTruthy();
    expect(await bindings.REGISTRY.head(`${prod_name}/manifests/${amd_sha256}`)).toBeTruthy();
    expect(await bindings.REGISTRY.head(`${prod_name}/manifests/${arm_sha256}`)).toBeTruthy();

    // Check symlink binding
    expect(amd_manifest.schemaVersion === 2).toBeTruthy();
    expect("manifests" in amd_manifest).toBeFalsy();
    if (amd_manifest.schemaVersion === 2 && !("manifests" in amd_manifest)) {
      const layer_digest = amd_manifest.layers[0].digest;
      const layer_source = await fetch(createRequest("GET", `/v2/${preprod_name}/blobs/${layer_digest}`, null));
      expect(layer_source.ok).toBeTruthy();
      const layer_linked = await fetch(createRequest("GET", `/v2/${prod_name}/blobs/${layer_digest}`, null));
      expect(layer_linked.ok).toBeTruthy();
      expect(await layer_linked.text()).toEqual(await layer_source.text())
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
    const manifest_old = await generateManifest(name);
    await createManifest(name, manifest_old, "v1");
    const manifest_latest = await generateManifest(name);
    await createManifest(name, manifest_latest, "v2");
    await createManifest(name, manifest_latest, "app");
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
      const { sha256: temp_sha256 } = await createManifest(name, await generateManifest(name));
      const res = await fetch(createRequest("DELETE", `/v2/${name}/manifests/${temp_sha256}`, null));
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
    const preprod_name = "m-arch-pp";
    const prod_name = "m-arch";
    const tag = "app";
    // Generate manifest
    const amd_manifest = await generateManifest(preprod_name);
    const arm_manifest = await generateManifest(preprod_name);
    // Create architecture specific repository
    await createManifest(preprod_name, amd_manifest, `${tag}-amd`);
    await createManifest(preprod_name, arm_manifest, `${tag}-arm`);

    // Create manifest-list on prod repository
    const bindings = env as Env;
    // Step 1 mount blobs
    await mountLayersFromManifest(preprod_name, amd_manifest, prod_name);
    await mountLayersFromManifest(preprod_name, arm_manifest, prod_name);

    // Step 2 create manifest
    await createManifest(prod_name, amd_manifest);
    await createManifest(prod_name, arm_manifest);

    // Step 3 create manifest list
    const manifest_list = await generateManifestList(amd_manifest, arm_manifest);
    await createManifest(prod_name, manifest_list, tag);

    // Check no action needed
    {
      const listManifests = await bindings.REGISTRY.list({ prefix: `${preprod_name}/manifests/` });
      expect(listManifests.objects.length).toEqual(4);
      const listBlobs = await bindings.REGISTRY.list({ prefix: `${preprod_name}/blobs/` });
      expect(listBlobs.objects.length).toEqual(4);
    }

    await runGarbageCollector(preprod_name, "both");

    {
      const listManifests = await bindings.REGISTRY.list({ prefix: `${preprod_name}/manifests/` });
      expect(listManifests.objects.length).toEqual(4);
      const listBlobs = await bindings.REGISTRY.list({ prefix: `${preprod_name}/blobs/` });
      expect(listBlobs.objects.length).toEqual(4);
    }

    // Untagged preprod repo
    {
      const res = await fetch(createRequest("DELETE", `/v2/${preprod_name}/manifests/${tag}-amd`, null));
      expect(res.status).toEqual(202);
      const res2 = await fetch(createRequest("DELETE", `/v2/${preprod_name}/manifests/${tag}-arm`, null));
      expect(res2.status).toEqual(202);
    }
    await runGarbageCollector(preprod_name, "unreferenced");
    {
      const listManifests = await bindings.REGISTRY.list({ prefix: `${preprod_name}/manifests/` });
      expect(listManifests.objects.length).toEqual(2);
      const listBlobs = await bindings.REGISTRY.list({ prefix: `${preprod_name}/blobs/` });
      expect(listBlobs.objects.length).toEqual(4);
    }
    await runGarbageCollector(preprod_name, "untagged");
    {
      const listManifests = await bindings.REGISTRY.list({ prefix: `${preprod_name}/manifests/` });
      expect(listManifests.objects.length).toEqual(0);
      const listBlobs = await bindings.REGISTRY.list({ prefix: `${preprod_name}/blobs/` });
      expect(listBlobs.objects.length).toEqual(4);
    }

    // Untagged prod repo
    {
      const res = await fetch(createRequest("DELETE", `/v2/${prod_name}/manifests/${tag}`, null));
      expect(res.status).toEqual(202);
    }
    {
      const listManifests = await bindings.REGISTRY.list({ prefix: `${prod_name}/manifests/` });
      expect(listManifests.objects.length).toEqual(3);
      const listBlobs = await bindings.REGISTRY.list({ prefix: `${prod_name}/blobs/` });
      expect(listBlobs.objects.length).toEqual(4);
    }
    await runGarbageCollector(prod_name, "untagged");
    {
      const listManifests = await bindings.REGISTRY.list({ prefix: `${prod_name}/manifests/` });
      expect(listManifests.objects.length).toEqual(0);
      const listBlobs = await bindings.REGISTRY.list({ prefix: `${prod_name}/blobs/` });
      expect(listBlobs.objects.length).toEqual(0);
    }
    await runGarbageCollector(preprod_name, "unreferenced");
    {
      const listManifests = await bindings.REGISTRY.list({ prefix: `${prod_name}/manifests/` });
      expect(listManifests.objects.length).toEqual(0);
      const listBlobs = await bindings.REGISTRY.list({ prefix: `${prod_name}/blobs/` });
      expect(listBlobs.objects.length).toEqual(0);
    }
  });
});
