import { Env } from "../..";
import { InternalError } from "../errors";
import { errorString } from "../utils";
import { GarbageCollectionMode } from "./garbage-collector";
import {
  CheckLayerResponse,
  CheckManifestResponse,
  FinishedUploadObject,
  GetLayerResponse,
  GetManifestResponse,
  ListRepositoriesResponse,
  PutManifestResponse,
  Registry,
  RegistryConfiguration,
  RegistryError,
  UploadId,
  UploadObject,
} from "./registry";

type AuthContext = {
  authType: AuthType;
  service: string;
  realm: string;
  scope: string;
};

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
  const urlReq = `${url.protocol}//${ctx.authContext.service}/v2${
    ctx.repository === "" ? "/" : ctx.repository + "/"
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
    const value = variable.slice(firstEqual + 1);
    const isMalformed = value.length < 2 || value[0] !== `"` || value[value.length - 1] !== `"`;
    if (isMalformed) {
      throw new Error(`malformed value on auth header on registry ${url}`);
    }

    const trimmedValue = value.slice(1, value.length - 1);
    switch (name) {
      case "realm":
      case "scope":
      case "service":
        authContextOptional[name] = trimmedValue;
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

  constructor(private env: Env, private configuration: RegistryConfiguration) {
    this.url = new URL(configuration.registry);
  }

  authBase64(): string {
    return btoa(this.configuration.username + ":" + this.password());
  }

  password(): string {
    return (this.env as unknown as Record<string, string>)[this.configuration.password_env] ?? "";
  }

  async authenticate(): Promise<HTTPContext> {
    const res = await fetch(`${this.url.protocol}//${this.url.host}/v2/`, {
      headers: {
        "User-Agent": "Docker-Client/24.0.5 (linux)",
        "Accept-Encoding": "gzip",
      },
    });

    if (res.ok) {
      return {
        authContext: {
          authType: "none",
          realm: "",
          scope: "",
          service: this.url.host,
        },
        repository: this.url.pathname,
        accessToken: "",
      };
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

  async authenticateBearerSimple(ctx: AuthContext, params: URLSearchParams): Promise<Response> {
    params.delete("password");
    console.log("sending authentication parameters:", ctx.realm + "?" + params.toString());
    return await fetch(ctx.realm + "?" + params.toString(), {
      headers: {
        "Authorization": "Basic " + this.authBase64(),
        "Accept": "application/json",
        "User-Agent": "Docker-Client/24.0.5 (linux)",
      },
    });
  }

  async authenticateBearer(ctx: AuthContext): Promise<HTTPContext> {
    const params = new URLSearchParams({
      service: ctx.service,
      // explicitely include that we don't want an offline_token.
      scope: `repository:${this.url.pathname.slice(1)}/image:pull,push`,
      client_id: "r2registry",
      grant_type: "password",
      password: this.password(),
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

  async manifestExists(namespace: string, tag: string): Promise<CheckManifestResponse | RegistryError> {
    try {
      const ctx = await this.authenticate();
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

  async getManifest(namespace: string, digest: string): Promise<GetManifestResponse | RegistryError> {
    try {
      const ctx = await this.authenticate();
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

  async layerExists(namespace: string, digest: string): Promise<CheckLayerResponse | RegistryError> {
    try {
      const ctx = await this.authenticate();
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

  async getLayer(namespace: string, digest: string): Promise<GetLayerResponse | RegistryError> {
    try {
      const ctx = await this.authenticate();
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

  putManifest(
    _namespace: string,
    _reference: string,
    _readableStream: ReadableStream<any>,
    _contentType: string,
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
