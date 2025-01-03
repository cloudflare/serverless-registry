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
  const data = "bla";
  const sha256 = await getSHA256(data);
  const res = await fetch(createRequest("POST", `/v2/${name}/blobs/uploads/`, null, {}));
  expect(res.ok).toBeTruthy();
  const blob = new Blob([data]).stream();
  const stream = limit(blob, data.length);
  const res2 = await fetch(createRequest("PATCH", res.headers.get("location")!, stream, {}));
  expect(res2.ok).toBeTruthy();
  const last = await fetch(createRequest("PUT", res2.headers.get("location")! + "&digest=" + sha256, null, {}));
  if (!last.ok) {
    throw new Error(await last.text());
  }

  return schemaVersion === 1
    ? {
        schemaVersion,
        fsLayers: [{ blobSum: sha256 }],
        architecture: "amd64",
      }
    : {
        schemaVersion,
        layers: [
          { size: data.length, digest: sha256, mediaType: "shouldbeanything" },
          { size: data.length, digest: sha256, mediaType: "shouldbeanything" },
        ],
        config: { size: data.length, digest: sha256, mediaType: "configmediatypeshouldntbechecked" },
        mediaType: "shouldalsobeanythingforretrocompatibility",
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
      expect(listObjects.objects.length).toEqual(1);

      const gcRes = await fetch(new Request("http://registry.com/v2/hello-world/gc", { method: "POST" }));
      if (!gcRes.ok) {
        throw new Error(`${gcRes.status}: ${await gcRes.text()}`);
      }

      const listObjectsAfterGC = await bindings.REGISTRY.list({ prefix: "hello-world/blobs/" });
      expect(listObjectsAfterGC.objects.length).toEqual(1);
    }

    expect(await bindings.REGISTRY.head(`hello-world/manifests/hello`)).toBeTruthy();
    const res = await fetch(createRequest("DELETE", `/v2/hello-world/manifests/${sha256}`, null));
    expect(res.status).toEqual(202);
    expect(await bindings.REGISTRY.head(`hello-world/manifests/${sha256}`)).toBeNull();
    expect(await bindings.REGISTRY.head(`hello-world/manifests/hello`)).toBeNull();

    const listObjects = await bindings.REGISTRY.list({ prefix: "hello-world/blobs/" });
    expect(listObjects.objects.length).toEqual(1);

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
    const { sha256 } = await createManifest("hello-world-list", await generateManifest("hello-world-list"), `hello`);
    const expectedRes = ["hello", sha256];
    for (let i = 0; i < 50; i++) {
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

    const res = await fetch(createRequest("DELETE", `/v2/hello-world-list/manifests/${sha256}`, null));
    expect(res.ok).toBeTruthy();
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
    expect(tags.tags).toEqual([
      "hello",
      "hello-2",
      "latest",
      "sha256:a8a29b609fa044cf3ee9a79b57a6fbfb59039c3e9c4f38a57ecb76238bf0dec6",
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
      "sha256:a70525d2dd357c6ece8d9e0a5a232e34ca3bbceaa1584d8929cdbbfc81238210",
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
  });
});
