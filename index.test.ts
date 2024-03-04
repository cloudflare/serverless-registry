import { afterAll, beforeEach, describe, expect, test } from "vitest";
import { SHA256_PREFIX_LEN, getSHA256 } from "./src/user";
import v2Router, { TagsList } from "./src/router";
import { Env } from ".";
import * as fetchAuth from "./index";
import { RegistryTokens } from "./src/token";
import { RegistryAuthProtocolTokenPayload } from "./src/auth";
import { registries } from "./src/registry/registry";
import { RegistryHTTPClient } from "./src/registry/http";

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

const bindings = getMiniflareBindings() as Env;
bindings.JWT_STATE_SECRET = "hello-world";
async function fetchUnauth(r: Request): Promise<Response> {
  const res = await v2Router.handle(r, bindings);
  return res as Response;
}

async function fetch(r: Request): Promise<Response> {
  return fetchAuth.default.fetch(r, bindings);
}

describe("v2", () => {
  test("/v2", async () => {
    const response = await fetchUnauth(createRequest("GET", "/v2/", null));
    expect(response.status).toBe(200);
  });
});

async function createManifest(name: string, data: string, tag?: string): Promise<{ sha256: string }> {
  const sha256 = await getSHA256(data);
  if (!tag) {
    tag = sha256;
  }

  const res = await fetchUnauth(
    createRequest("PUT", `/v2/${name}/manifests/${tag}`, new Blob([data]).stream(), {
      "Content-Type": "application/gzip",
    }),
  );
  expect(res.ok).toBeTruthy();
  expect(res.headers.get("docker-content-digest")).toEqual(sha256);
  return { sha256 };
}

describe("v2 manifests", () => {
  test("Simple username password authentication", async () => {
    const bindings = getMiniflareBindings() as Env;
    bindings.USERNAME = "hello";
    bindings.PASSWORD = "world";
    const res = await fetch(createRequest("GET", `/v2/`, null, {}));
    expect(res.status).toBe(401);
    expect(res.ok).toBeFalsy();
    const resAuth = await fetch(
      createRequest("GET", `/v2/`, null, {
        Authorization: usernamePasswordToAuth("hellO", "worlD"),
      }),
    );
    expect(resAuth.ok).toBeFalsy();
    const resAuthCorrect = await fetch(
      createRequest("GET", `/v2/`, null, {
        Authorization: usernamePasswordToAuth("hello", "world"),
      }),
    );
    expect(resAuthCorrect.ok).toBeTruthy();
  });

  test("HEAD /v2/:name/manifests/:reference NOT FOUND", async () => {
    const response = await fetchUnauth(createRequest("GET", "/v2/notfound/manifests/reference", null));
    expect(response.status).toBe(404);
    const json = await response.json();
    expect(json).toEqual({
      errors: [
        {
          code: "MANIFEST_UNKNOWN",
          message: "manifest unknown",
          detail: "This error is returned when the manifest, identified by name and tag is unknown to the repository.",
        },
      ],
    });
  });

  test("HEAD /v2/:name/manifests/:reference works", async () => {
    const reference = "123456";
    const name = "name";
    const data = "{}";
    const sha256 = await getSHA256(data);
    await bindings.REGISTRY.put(`${name}/manifests/${reference}`, "{}", {
      httpMetadata: { contentType: "application/gzip" },
      sha256: sha256.slice(SHA256_PREFIX_LEN),
    });
    const res = await fetchUnauth(createRequest("HEAD", `/v2/${name}/manifests/${reference}`, null));
    expect(res.ok).toBeTruthy();
    expect(Object.fromEntries(res.headers)).toEqual({
      "content-length": "2",
      "content-type": "application/gzip",
      "docker-content-digest": sha256,
    });
    await bindings.REGISTRY.delete(`${name}/manifests/${reference}`);
  });

  test("PUT /v2/:name/manifests/:reference works", () => createManifest("hello-world-main", "{}", "hello"));

  test("PUT then DELETE /v2/:name/manifests/:reference works", async () => {
    const { sha256 } = await createManifest("hello-world", "{}", "hello");
    expect(await bindings.REGISTRY.head(`hello-world/manifests/hello`)).toBeTruthy();
    const res = await fetchUnauth(createRequest("DELETE", `/v2/hello-world/manifests/${sha256}`, null));
    expect(res.status).toEqual(202);
    expect(await bindings.REGISTRY.head(`hello-world/manifests/${sha256}`)).toBeNull();
    expect(await bindings.REGISTRY.head(`hello-world/manifests/hello`)).toBeNull();
  });

  test("PUT multiple parts then DELETE /v2/:name/manifests/:reference works", async () => {
    const { sha256 } = await createManifest("hello/world", "{}", "hello");
    expect(await bindings.REGISTRY.head(`hello/world/manifests/hello`)).toBeTruthy();
    const res = await fetchUnauth(createRequest("DELETE", `/v2/hello/world/manifests/${sha256}`, null));
    expect(res.status).toEqual(202);
    expect(await bindings.REGISTRY.head(`hello/world/manifests/${sha256}`)).toBeNull();
    expect(await bindings.REGISTRY.head(`hello/world/manifests/hello`)).toBeNull();
  });

  test("PUT then list tags with GET /v2/:name/tags/list", async () => {
    const { sha256 } = await createManifest("hello-world-list", "{}", `hello`);
    const expectedRes = ["hello", sha256];
    for (let i = 0; i < 500; i++) {
      expectedRes.push(`hello-${i}`);
    }

    expectedRes.sort();
    const shuffledRes = shuffleArray([...expectedRes]);
    for (const tag of shuffledRes) {
      await createManifest("hello-world-list", "{}", tag);
    }

    const tagsRes = await fetchUnauth(createRequest("GET", `/v2/hello-world-list/tags/list?n=1000`, null));
    const tags = (await tagsRes.json()) as TagsList;
    expect(tags.name).toEqual("hello-world-list");
    expect(tags.tags).toEqual(expectedRes);

    const res = await fetchUnauth(createRequest("DELETE", `/v2/hello-world-list/manifests/${sha256}`, null));
    expect(res.ok).toBeTruthy();
    const tagsResEmpty = await fetchUnauth(createRequest("GET", `/v2/hello-world-list/tags/list`, null));
    const tagsEmpty = (await tagsResEmpty.json()) as TagsList;
    expect(tagsEmpty.tags).toHaveLength(0);
  });
});

describe("tokens", async () => {
  test("auth payload push on /v2", async () => {
    const auth = new RegistryTokens({} as JsonWebKey);
    const { verified } = auth.verifyPayload(createRequest("GET", "/v2/", null), {
      capabilities: ["push"],
    } as RegistryAuthProtocolTokenPayload);
    expect(verified).toBeTruthy();
  });

  test("auth payload pull on /v2", async () => {
    const auth = new RegistryTokens({} as JsonWebKey);
    const { verified } = auth.verifyPayload(createRequest("GET", "/v2/", null), {
      capabilities: ["pull"],
    } as RegistryAuthProtocolTokenPayload);
    expect(verified).toBeTruthy();
  });

  test("auth payload push on /v2/whatever with HEAD", async () => {
    const auth = new RegistryTokens({} as JsonWebKey);
    const { verified } = auth.verifyPayload(createRequest("HEAD", "/v2/whatever", null), {
      capabilities: ["push"],
    } as RegistryAuthProtocolTokenPayload);
    expect(verified).toBeTruthy();
  });

  test("auth payload push on /v2/whatever with mutations", async () => {
    for (const mutationMethod of ["PATCH", "POST", "DELETE"]) {
      const auth = new RegistryTokens({} as JsonWebKey);
      const { verified } = auth.verifyPayload(createRequest(mutationMethod, "/v2/whatever", null), {
        capabilities: ["push"],
      } as RegistryAuthProtocolTokenPayload);
      expect(verified).toBeTruthy();
    }
  });

  test("auth payload pull on /v2/whatever with mutations", async () => {
    for (const mutationMethod of ["PATCH", "POST", "DELETE"]) {
      const auth = new RegistryTokens({} as JsonWebKey);
      const { verified } = auth.verifyPayload(createRequest(mutationMethod, "/v2/whatever", null), {
        capabilities: ["pull"],
      } as RegistryAuthProtocolTokenPayload);
      expect(verified).toBeFalsy();
    }
  });

  test("auth payload push/pull on /v2/whatever with mutations", async () => {
    for (const mutationMethod of ["PATCH", "POST", "DELETE"]) {
      const auth = new RegistryTokens({} as JsonWebKey);
      const { verified } = auth.verifyPayload(createRequest(mutationMethod, "/v2/whatever", null), {
        capabilities: ["pull", "push"],
      } as RegistryAuthProtocolTokenPayload);
      expect(verified).toBeTruthy();
    }
  });

  test("auth payload push on GET", async () => {
    const auth = new RegistryTokens({} as JsonWebKey);
    const { verified } = auth.verifyPayload(createRequest("GET", "/v2/whatever", null), {
      capabilities: ["push"],
    } as RegistryAuthProtocolTokenPayload);
    expect(verified).toBeFalsy();
  });

  test("auth payload push/pull on GET", async () => {
    const auth = new RegistryTokens({} as JsonWebKey);
    const { verified } = auth.verifyPayload(createRequest("GET", "/v2/whatever", null), {
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
  let envBindings = { ...bindings };
  const prevFetch = global.fetch;
  beforeEach(() => {
    global.fetch = async function (info, init) {
      const r = new Request(info, init);
      return fetchAuth.default.fetch(r, envBindings);
    };
  });

  afterAll(() => {
    global.fetch = prevFetch;
  });

  test("test manifest exists", async () => {
    envBindings = { ...bindings };
    envBindings.JWT_STATE_SECRET = "hello";
    envBindings.JWT_REGISTRY_TOKENS_PUBLIC_KEY = "";
    envBindings.PASSWORD = "123456";
    envBindings.USERNAME = "v1";
    envBindings.REGISTRIES_JSON = undefined;
    const client = new RegistryHTTPClient(envBindings, {
      registry: "https://localhost",
      password_env: "PASSWORD",
      username: "v1",
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
    await createManifest("hello-world-main", "{}", "hello");
    await createManifest("hello-world-main", "{}", "latest");
    await createManifest("hello-world-main", "{}", "hello-2");
    await createManifest("hello", "{}", "hello");
    await createManifest("hello/hello", "{}", "hello");

    const response = await fetchUnauth(createRequest("GET", "/v2/_catalog", null));
    expect(response.ok).toBeTruthy();
    const body = (await response.json()) as { repositories: string[] };
    expect(body).toEqual({
      repositories: ["hello-world-main", "hello/hello", "hello"],
    });
    const expectedRepositories = body.repositories;
    const tagsRes = await fetchUnauth(createRequest("GET", `/v2/hello-world-main/tags/list?n=1000`, null));
    const tags = (await tagsRes.json()) as TagsList;
    expect(tags.name).toEqual("hello-world-main");
    expect(tags.tags).toEqual([
      "hello",
      "hello-2",
      "latest",
      "sha256:44136fa355b3678a1146ad16f7e8649e94fb4fc21fe77e8310c060f61caaff8a",
    ]);

    const repositoryBuildUp: string[] = [];
    let currentPath = "/v2/_catalog?n=1";
    for (let i = 0; i < 3; i++) {
      const response = await fetchUnauth(createRequest("GET", currentPath, null));
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
