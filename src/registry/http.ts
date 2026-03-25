import { Env } from "../..";
import { InternalError, ServerError } from "../errors";
import { errorString } from "../utils";
import { GarbageCollectionMode } from "./garbage-collector";
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
  RegistryConfiguration,
  RegistryError,
  UploadId,
  UploadObject,
} from "./registry";
import { ociImageIndexContentType } from "./r2";

type AuthContext = {
  authType: AuthType;
  service: string;
  realm: string;
  scope: string;
};

export function isDockerDotIO(url: URL): boolean {
  const regex = /^https:\/\/([\w\d]+\.)?docker\.io$/;
  return regex.test(url.origin);
}

type HTTPContext = {
  // The auth context for this request
  authContext: AuthContext;
  // The configured base repository for the request.
  // For example, with GCR this will the username. This should
  // be included in every request, and can be combined with deeper namespaces
  repository: string;
  // If Basic based authentication, this is <username>':'<password> encoded in base64
  // If Bearer based authentication, this is the token that was returned by the Oauth/token endpoint
  accessToken: string;
};

export const manifestTypes = [
  "application/vnd.docker.distribution.manifest.list.v2+json",
  "application/vnd.oci.image.index.v1+json",
  "application/vnd.docker.distribution.manifest.v1+prettyjws",
  "application/json",
  "application/vnd.oci.image.manifest.v1+json",
  "application/vnd.docker.distribution.manifest.v2+json",
] as const;

export type ManifestType = (typeof manifestTypes)[number];

function splitLinkHeaderValues(link: string): string[] {
  const values: string[] = [];
  let current = "";
  let insideAngleBrackets = false;
  let insideQuotes = false;

  for (const character of link) {
    if (character === '"' && !insideAngleBrackets) {
      insideQuotes = !insideQuotes;
    } else if (character === "<" && !insideQuotes) {
      insideAngleBrackets = true;
    } else if (character === ">" && !insideQuotes) {
      insideAngleBrackets = false;
    } else if (character === "," && !insideAngleBrackets && !insideQuotes) {
      values.push(current.trim());
      current = "";
      continue;
    }

    current += character;
  }

  if (current.length > 0) {
    values.push(current.trim());
  }

  return values;
}

function parseLinkHeaderURL(link: string, baseURL: string): URL | null {
  for (const linkValue of splitLinkHeaderValues(link)) {
    const targetStart = linkValue.indexOf("<");
    const targetEnd = linkValue.indexOf(">", targetStart + 1);
    if (targetStart === -1 || targetEnd === -1) {
      continue;
    }

    const params = linkValue
      .slice(targetEnd + 1)
      .split(";")
      .map((param) => param.trim())
      .filter((param) => param.length > 0);
    const isNext = params.some((param) => {
      const [name, ...valueParts] = param.split("=");
      if (name.toLowerCase() !== "rel" || valueParts.length === 0) {
        return false;
      }

      const value = valueParts.join("=").trim().replace(/^"|"$/g, "");
      return value.split(/\s+/).includes("next");
    });
    if (!isNext) {
      continue;
    }

    const href = linkValue.slice(targetStart + 1, targetEnd).trim();
    try {
      return new URL(href);
    } catch {
      if (baseURL.length === 0) {
        return null;
      }

      try {
        return new URL(href, baseURL);
      } catch {
        return null;
      }
    }
  }

  return null;
}

function isOpaqueReferrersCursor(cursor: string): boolean {
  return cursor.startsWith("/v2/");
}

function normalizeReferrersCursor(nextURL: URL, requestURL: URL): string | undefined {
  if (nextURL.origin !== requestURL.origin || nextURL.pathname !== requestURL.pathname) {
    return undefined;
  }

  return `${nextURL.pathname}${nextURL.search}`;
}

function ctxIntoHeaders(ctx: HTTPContext): Headers {
  const headers = new Headers();
  if (ctx.authContext.authType === "none") {
    console.warn(
      "Your registry",
      ctx.authContext.service,
      "is not using any kind of authentication, making it exposed to the internet",
    );
    return headers;
  }

  headers.append("Authorization", (ctx.authContext.authType === "basic" ? "Basic" : "Bearer") + " " + ctx.accessToken);
  return headers;
}

function ctxIntoRequest(ctx: HTTPContext, url: URL, method: string, path: string, body?: BodyInit): Request {
  const urlReq = `${url.protocol}//${url.host}/v2${
    ctx.repository === "" || ctx.repository === "/" ? "/" : ctx.repository + "/"
  }${path}`;
  return new Request(urlReq, {
    method,
    body,
    redirect: "follow",
    headers: ctxIntoHeaders(ctx),
  });
}

function authHeaderIntoAuthContext(urlObject: URL, authenticateHeader: string): AuthContext {
  const url = urlObject.toString();
  const parts = authenticateHeader.split(" ");
  if (parts.length === 0) {
    throw new Error(`can't retrieve WWW-Authenticate header in /v2 endpoint on registry ${url}: malformed`);
  }

  const authType = parts[0].toLowerCase();
  switch (authType) {
    case "bearer":
    case "basic":
      break;
    case "none":
      throw new Error("unsupported auth type for getting an auth context");
    default:
      throw new Error(`unsupported auth type in WWW-Authenticate on registry ${url}: ${parts[0]}`);
  }

  const variables = parts[1].split(",");
  const authContextOptional: Partial<AuthContext> = {};
  variables.forEach((variable) => {
    const firstEqual = variable.indexOf("=");
    if (firstEqual === -1) {
      throw new Error(`expected '=' but didn't encounter it on Auth header on registry ${url}`);
    }

    const name = variable.slice(0, firstEqual);
    let value = variable.slice(firstEqual + 1);

    if (value.length >= 2 && value[0] === `"` && value[value.length - 1] === `"`) {
      value = value.slice(1, value.length - 1);
    }

    switch (name) {
      case "realm":
      case "scope":
      case "service":
        authContextOptional[name] = value;
        break;
      default:
        console.debug(`unknown auth attribute ${name} on registry ${url}`);
    }
  });

  if (!authContextOptional.realm) throw new Error(`expected a realm on the auth header in repository ${url}`);
  try {
    const urlRealm = new URL(authContextOptional.realm);
    // if service is not defined, define it by setting it to be the same as the realm's host.
    // at the end service will be used in the request to know which service to hit
    authContextOptional.service ??= urlRealm.host;
  } catch {
    throw new Error(`invalid url in realm in repository ${url}`);
  }

  return {
    authType,
    realm: authContextOptional.realm!,
    scope: authContextOptional.scope ?? "",
    service: authContextOptional.service ?? "",
  };
}

// RegistryHTTPClient implements a registry client that is able to pull/push to the configured registry
export class RegistryHTTPClient implements Registry {
  private url: URL;

  constructor(
    private env: Env,
    private configuration: RegistryConfiguration,
  ) {
    this.url = new URL(configuration.registry);
  }

  authBase64(): string {
    const configuration = this.configuration;
    if (configuration.username === undefined) {
      return "";
    }

    return btoa(this.configuration.username + ":" + this.password());
  }

  password(): string {
    const configuration = this.configuration;
    if (configuration.username === undefined) {
      return "";
    }

    return (this.env as unknown as Record<string, string>)[configuration.password_env] ?? "";
  }

  async authenticate(namespace: string): Promise<HTTPContext> {
    const emptyAuthentication = {
      authContext: {
        authType: "none",
        realm: "",
        scope: "",
        service: this.url.host,
      },
      repository: this.url.pathname,
      accessToken: "",
    } as const;

    const res = await fetch(`${this.url.protocol}//${this.url.host}/v2/`, {
      headers: {
        "User-Agent": "Docker-Client/24.0.5 (linux)",
        "Accept-Encoding": "gzip",
      },
    });

    if (res.ok) {
      return emptyAuthentication;
    }

    if (res.status !== 401) {
      throw new Error(`registry ${this.url.host}/v2 answered with unexpected status code ${res.status}`);
    }

    // see https://distribution.github.io/distribution/spec/auth/token/
    const authenticateHeader = res.headers.get("WWW-Authenticate");
    if (authenticateHeader === null) {
      throw new Error(`can't retrieve WWW-Authenticate header in /v2 endpoint on registry ${this.url.toString()}`);
    }

    const authCtx = authHeaderIntoAuthContext(this.url, authenticateHeader);
    if (!authCtx.scope) authCtx.scope = namespace;
    switch (authCtx.authType) {
      case "bearer":
        return await this.authenticateBearer(authCtx);
      case "basic":
        return await this.authenticateBasic(authCtx);
      default:
        throw new Error("unreachable");
    }
  }

  async monolithicUpload(
    _namespace: string,
    _expectedSha: string,
    _stream: ReadableStream<any>,
    _size: number,
  ): Promise<false | FinishedUploadObject | RegistryError> {
    // please, just give me chunked things
    return false;
  }

  async authenticateBearerSimple(ctx: AuthContext, params: URLSearchParams) {
    params.delete("password");
    console.log("sending authentication parameters:", ctx.realm + "?" + params.toString());

    return await fetch(ctx.realm + "?" + params.toString(), {
      headers: {
        "Accept": "application/json",
        "User-Agent": "Docker-Client/24.0.5 (linux)",
        ...(this.configuration.username !== undefined ? { Authorization: "Basic " + this.authBase64() } : {}),
      },
    });
  }

  async authenticateBearer(ctx: AuthContext): Promise<HTTPContext> {
    const params = new URLSearchParams({
      service: ctx.service,
      // explicitely include that we don't want an offline_token.
      scope: `repository:${ctx.scope}:pull,push`,
      client_id: "r2registry",
      grant_type: this.configuration.username === undefined ? "none" : "password",
      password: this.configuration.username === undefined ? "" : this.password(),
    });
    let response = await fetch(ctx.realm, {
      headers: {
        "Content-Type": "application/x-www-form-urlencoded",
        "User-Agent": "Docker-Client/24.0.5 (linux)",
      },
      method: "POST",
      body: params.toString(),
    });
    if (response.status === 404 || response.status === 405 || response.status == 401) {
      console.debug(
        this.url.toString(),
        "Oauth 404/401/405... Falling back to simple token authentication, see https://distribution.github.io/distribution/spec/auth/token",
      );
      const responseSimple = await this.authenticateBearerSimple(ctx, params);
      if (responseSimple.ok) {
        response = responseSimple;
      } else {
        console.error(`Oauth fallback also failed: ${responseSimple.status} ${await responseSimple.text()}`);
      }
    }

    if (!response.ok) {
      throw new Error(
        `unexpected ${response.status} from ${this.url.toString()} when Oauth authenticating: ${await response.text()}`,
      );
    }

    const t = await response.text();
    try {
      const response: {
        access_token?: string;
        expires_in: number;
        repository: string;
        token?: string;
      } = JSON.parse(t);
      console.debug(
        `Authenticated with registry ${this.url.toString()} successfully, got token that expires in ${
          response.expires_in
        } seconds`,
      );

      if (!response.access_token && !response.token) {
        console.error(
          "Oauth response doesn't have access_token field, doing fallback to password_env, however this might mean that we will 401 later",
        );
      }

      return {
        authContext: ctx,
        repository: response.repository ?? this.url.pathname,
        accessToken: response.access_token ?? response.token ?? this.authBase64(),
      };
    } catch (err) {
      console.error(
        "Doing json response in authentication: ",
        errorString(err),
        t.slice(0, Math.min(t.length, 100)),
        "status",
        response.status,
      );
      throw err;
    }
  }

  async authenticateBasic(ctx: AuthContext): Promise<HTTPContext> {
    const res = await fetch(ctx.realm, {
      headers: {
        Authorization: "Basic " + this.authBase64(),
      },
    });

    if (!res.ok) {
      throw new Error(`couldn't authenticate with registry ${ctx.realm}: ${JSON.stringify(await res.json())}`);
    }

    return {
      authContext: ctx,
      accessToken: this.authBase64(),
      repository: this.url.pathname.slice(1),
    };
  }

  async manifestExists(name: string, tag: string): Promise<CheckManifestResponse | RegistryError> {
    const namespace = name.includes("/") || !isDockerDotIO(this.url) ? name : `library/${name}`;
    try {
      const ctx = await this.authenticate(namespace);
      const req = ctxIntoRequest(ctx, this.url, "HEAD", `${namespace}/manifests/${tag}`);
      req.headers.append("Accept", manifestTypes.join(", "));
      const res = await fetch(req);
      if (!res.ok && res.status !== 404) {
        console.warn(req.url, "->", res.status, "getting manifest:", await res.text());
        return {
          response: res,
        };
      }

      console.log("->", req.url, res.status);
      return {
        exists: res.ok,
        digest: res.headers.get("Docker-Content-Digest") as string,
        size: +(res.headers.get("Content-Length") ?? "0"),
        contentType: res.headers.get("Content-Type") ?? "",
      };
    } catch (err) {
      console.error(`Error doing manifest exists with ${namespace} and ${tag}: ` + errorString(err));
      return {
        response: new InternalError(),
      };
    }
  }

  async getManifest(name: string, digest: string): Promise<GetManifestResponse | RegistryError> {
    const namespace = name.includes("/") || !isDockerDotIO(this.url) ? name : `library/${name}`;
    try {
      const ctx = await this.authenticate(namespace);
      const req = ctxIntoRequest(ctx, this.url, "GET", `${namespace}/manifests/${digest}`);
      req.headers.append("Accept", manifestTypes.join(", "));
      const res = await fetch(req);
      console.log(req.method, res.status, res.url);
      if (!res.ok) {
        return {
          response: res,
        };
      }

      if (res.body === null) {
        throw new Error("body is null");
      }

      return {
        size: +(res.headers.get("Content-Length") ?? "0"),
        stream: res.body,
        digest: res.headers.get("Docker-Content-Digest") ?? digest,
        contentType: res.headers.get("Content-Type") ?? "",
      };
    } catch (err) {
      console.error(`Error doing get manifest with ${namespace} and ${digest}: ` + errorString(err));
      return {
        response: new InternalError(),
      };
    }
  }

  async layerExists(name: string, digest: string): Promise<CheckLayerResponse | RegistryError> {
    const namespace = name.includes("/") || !isDockerDotIO(this.url) ? name : `library/${name}`;
    try {
      const ctx = await this.authenticate(namespace);
      const res = await fetch(ctxIntoRequest(ctx, this.url, "HEAD", `${namespace}/blobs/${digest}`));
      if (res.status === 404) {
        return {
          exists: false,
        };
      }

      if (!res.ok) {
        return {
          response: res,
        };
      }

      const contentLengthString = res.headers.get("Content-Length");
      if (contentLengthString === null) {
        throw new Error("content-length header is not setup");
      }

      const contentLength = +contentLengthString;
      if (isNaN(contentLength)) {
        throw new Error("content-length header is not a number");
      }

      return {
        exists: true,
        size: contentLength,
        digest: res.headers.get("Docker-Content-Digest") ?? digest,
      };
    } catch (err) {
      console.error(`Error doing layer exists with ${namespace} and ${digest}: ` + errorString(err));
      return {
        response: new InternalError(),
      };
    }
  }

  async getLayer(name: string, digest: string): Promise<GetLayerResponse | RegistryError> {
    const namespace = name.includes("/") || !isDockerDotIO(this.url) ? name : `library/${name}`;
    try {
      const ctx = await this.authenticate(namespace);
      const req = ctxIntoRequest(ctx, this.url, "GET", `${namespace}/blobs/${digest}`);
      let res = await fetch(req);
      if (!res.ok) {
        // This means we got a redirect, so let's try again this URL but
        // without any headers. Services like S3 reject authorization headers altogether
        // if the authentication is included in the URL.
        if (res.url !== req.url) {
          const redirectResponse = await fetch(new Request(res.url));
          if (!redirectResponse.ok) {
            return {
              response: res,
            };
          }

          res = redirectResponse;
        } else {
          return {
            response: res,
          };
        }
      }

      if (res.body === null) {
        throw new Error("returned body is null");
      }

      return {
        stream: res.body,
        size: +(res.headers.get("Content-Length") ?? "0"),
        digest: res.headers.get("Digest-Content-Digest") ?? digest,
      };
    } catch (err) {
      console.error(`Error doing get layer with ${namespace} and ${digest}: ` + errorString(err));
      return {
        response: new InternalError(),
      };
    }
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
    const namespace = name.includes("/") || !isDockerDotIO(this.url) ? name : `library/${name}`;
    try {
      const ctx = await this.authenticate(namespace);
      const baseRequest = ctxIntoRequest(ctx, this.url, "GET", `${namespace}/referrers/${digest}`);
      const baseURL = new URL(baseRequest.url);
      let req: Request;
      const usesOpaqueCursor = options?.last !== undefined && isOpaqueReferrersCursor(options.last);
      if (usesOpaqueCursor) {
        const nextCursor = options!.last!;
        const nextURL = nextCursor.startsWith("/")
          ? new URL(nextCursor, `${this.url.protocol}//${this.url.host}`)
          : new URL(nextCursor);
        if (nextURL.origin !== baseURL.origin || nextURL.pathname !== baseURL.pathname) {
          return {
            response: new ServerError("invalid referrers cursor", 400),
          };
        }
        req = new Request(nextURL, {
          method: "GET",
          headers: ctxIntoHeaders(ctx),
        });
      } else {
        const params = new URLSearchParams();
        if (options?.artifactType !== undefined) {
          params.set("artifactType", options.artifactType);
        }
        if (options?.limit !== undefined) {
          params.set("n", `${options.limit}`);
        }
        if (options?.last !== undefined) {
          params.set("last", options.last);
        }

        req = ctxIntoRequest(
          ctx,
          this.url,
          "GET",
          `${namespace}/referrers/${digest}${params.size > 0 ? `?${params.toString()}` : ""}`,
        );
      }

      const res = await fetch(req);
      console.log(req.method, res.status, res.url);
      if (!res.ok) {
        return {
          response: res,
        };
      }

      const body = (await res.json()) as {
        manifests?: ReferrerDescriptor[];
        mediaType?: string;
        schemaVersion?: number;
      };
      const contentType = res.headers.get("Content-Type")?.split(";")[0].trim();
      if (
        contentType !== ociImageIndexContentType ||
        body.schemaVersion !== 2 ||
        body.mediaType !== ociImageIndexContentType ||
        !Array.isArray(body.manifests)
      ) {
        return {
          response: new InternalError(),
        };
      }

      const next = res.headers.get("Link");
      let cursor: string | undefined;
      if (next !== null) {
        const nextURL = parseLinkHeaderURL(next, res.url);
        cursor = nextURL !== null ? normalizeReferrersCursor(nextURL, new URL(req.url)) : undefined;
      }

      return {
        manifests: body.manifests,
        cursor,
      };
    } catch (err) {
      console.error(`Error doing list referrers with ${namespace} and ${digest}: ` + errorString(err));
      return {
        response: new InternalError(),
      };
    }
  }

  mountExistingLayer(
    _sourceName: string,
    _digest: string,
    _destinationName: string,
  ): Promise<RegistryError | FinishedUploadObject> {
    throw new Error("unimplemented");
  }

  putManifest(
    _namespace: string,
    _reference: string,
    _readableStream: ReadableStream<any>,
    {}: {},
  ): Promise<PutManifestResponse | RegistryError> {
    throw new Error("unimplemented");
  }

  startUpload(_namespace: string): Promise<UploadObject | RegistryError> {
    throw new Error("unimplemented");
  }

  cancelUpload(_namespace: string, _uploadId: UploadId): Promise<true | RegistryError> {
    throw new Error("unimplemented");
  }

  getUpload(_namespace: string, _uploadId: string): Promise<UploadObject | RegistryError> {
    throw new Error("unimplemented");
  }

  async uploadChunk(
    _namespace: string,
    _uploadId: string,
    _location: string,
    _stream: ReadableStream<any>,
    _length?: number | undefined,
    _range?: [number, number] | undefined,
  ): Promise<RegistryError | UploadObject> {
    throw new Error("unimplemented");
  }

  finishUpload(
    _namespace: string,
    _uploadId: string,
    _location: string,
    _expectedSha: string,
    _stream?: ReadableStream<any> | undefined,
    _length?: number | undefined,
  ): Promise<RegistryError | FinishedUploadObject> {
    throw new Error("unimplemented");
  }

  async listRepositories(_limit?: number, _last?: string): Promise<RegistryError | ListRepositoriesResponse> {
    throw new Error("unimplemented");
  }

  garbageCollection(_namespace: string, _mode: GarbageCollectionMode): Promise<boolean> {
    throw new Error("unimplemented");
  }
}

// AuthType defined the supported auth types
type AuthType = "basic" | "bearer" | "none";
