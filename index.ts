/**
 * The core server that runs on a Cloudflare worker.
 */

import { Router } from "itty-router";
import { AuthErrorResponse, InternalError } from "./src/errors";
import v2Router from "./src/router";
import { authenticationMethodFromEnv } from "./src/authentication-method";
import { RegistryTokens } from "./src/token";
import type { RegistryAuthProtocolTokenPayload } from "./src/auth";
import { Registry } from "./src/registry/registry";
import { R2Registry } from "./src/registry/r2";

// A full compatibility mode means that the r2 registry will try its best to
// help the client on the layer push. See how we let the client push layers with chunked uploads for more information.
type PushCompatibilityMode = "full" | "none";

export interface Env {
  REGISTRY: R2Bucket;
  ENVIRONMENT: string;
  JWT_REGISTRY_TOKENS_PUBLIC_KEY?: string;
  USERNAME?: string;
  PASSWORD?: string;
  READONLY_USERNAME?: string;
  READONLY_PASSWORD?: string;
  READONLY_ANONYMOUS?: string; // "true" allows unauthenticated pulls (GET/HEAD); writes still require auth
  PUSH_COMPATIBILITY_MODE?: PushCompatibilityMode;
  REGISTRIES_JSON?: string; // should be in the format of RegistryConfiguration[];
  REGISTRY_CLIENT: Registry;
}

const router = Router();

/**
 * V2 Api
 */
router.all("/v2/*", v2Router.fetch);

router.all("*", () => new Response("Not Found.", { status: 404 }));

export default {
  async fetch(request: Request, env: Env, context?: ExecutionContext) {
    if (!ensureConfig(env)) {
      return new AuthErrorResponse(request);
    }

    const authMethod = await authenticationMethodFromEnv(env);
    if (!authMethod) {
      return new AuthErrorResponse(request);
    }

    const credentials =
      env.READONLY_ANONYMOUS === "true" && !request.headers.get("Authorization")
        ? RegistryTokens.verifyPayload(request, anonymousPullPayload())
        : await authMethod.checkCredentials(request);
    if (!credentials.verified) {
      console.warn(`Not Authorized. authmode=${authMethod.authmode}. verified=false`);
      return new AuthErrorResponse(request);
    }

    env.REGISTRY_CLIENT = new R2Registry(env);
    try {
      // Dispatch the request to the appropriate route
      const res = await router.fetch(request, env, context);
      return res;
    } catch (err) {
      if (err instanceof Response) {
        console.warn(`${request.method} ${err.status} ${err.url}`);
        return err;
      }

      // Unexpected error
      if (err instanceof Error) {
        console.error(
          "An error has been thrown by the router:\n",
          `${err.name}: ${err.message}: ${err.cause}: ${err.stack}`,
        );
        return new InternalError();
      }

      console.error(
        "An error has been thrown and is neither a Response or an Error, JSON.stringify() =",
        JSON.stringify(err),
      );
      return new InternalError();
    }
  },
} satisfies ExportedHandler<Env>;

// anonymousPullPayload returns a principal holding only the "pull" capability. It authorizes
// unauthenticated read requests (GET/HEAD) when READONLY_ANONYMOUS is enabled; writes still fail
// because verifyPayload requires the "push" capability for POST/PUT/PATCH/DELETE.
function anonymousPullPayload(): RegistryAuthProtocolTokenPayload {
  return {
    username: "anonymous",
    capabilities: ["pull"],
    // verifyPayload compares exp against Date.now() expressed in seconds, so exp is seconds.
    exp: Math.floor(Date.now() / 1000) + 3600,
    aud: "",
  };
}

const ensureConfig = (env: Env): boolean => {
  if (!env.REGISTRY) {
    console.error(
      "env.REGISTRY is not setup. Please setup an R2 bucket and add the binding in your wrangler config file. Try 'npx wrangler --env production r2 bucket create r2-registry'",
    );
    return false;
  }

  return true;
};
