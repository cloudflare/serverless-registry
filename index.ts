/**
 * The core server that runs on a Cloudflare worker.
 */

import { Router } from "itty-router";
import { AuthErrorResponse } from "./src/errors";
import v2Router from "./src/router";
import { authenticationMethodFromEnv } from "./src/authentication-method";

// A full compatibility mode means that the r2 registry will try its best to
// help the client on the layer push. See how we let the client push layers with chunked uploads for more information.
type PushCompatibilityMode = "full" | "none";

export interface Env {
  REGISTRY: R2Bucket;
  ENVIRONMENT: string;
  JWT_REGISTRY_TOKENS_PUBLIC_KEY?: string;
  USERNAME?: string;
  PASSWORD?: string;
  JWT_STATE_SECRET: string;
  UPLOADS: KVNamespace;
  PUSH_COMPATIBILITY_MODE?: PushCompatibilityMode;
}

const router = Router();

/**
 * V2 Api
 */
router.all("/v2/*", v2Router.handle);

router.all("*", () => new Response("Not Found.", { status: 404 }));

export default {
  async fetch(request: Request, env: Env) {
    if (!ensureConfig(env)) {
      return new AuthErrorResponse(request);
    }

    const authMethod = await authenticationMethodFromEnv(env);
    if (!authMethod) {
      return new AuthErrorResponse(request);
    }

    const credentials = await authMethod.checkCredentials(request);
    if (!credentials.verified) {
      console.warn(`Not Authorized. authmode=${authMethod.authmode}. verified=false`);
      return new AuthErrorResponse(request);
    }

    // Dispatch the request to the appropriate route
    return router.handle(request, env);
  },
};


const ensureConfig = (env: Env): boolean => {
  if (!env.REGISTRY) {
    console.error("env.REGISTRY is not setup. Please setup an R2 bucket and add the binding in wrangler.toml. Try 'wrangler --env production r2 bucket create r2-registry'");
    return false;
  }

  if (!env.UPLOADS) {
    console.error("env.UPLOADS is not setup. Please setup a KV namespace and add the binding in wrangler.toml. Try 'wrangler --env production kv:namespace create r2_registry_uploads'");
    return false;
  }

  if (!env.JWT_STATE_SECRET) {
    console.error(`env.JWT_STATE_SECRET is not set. Please setup this secret using wrnagler. Try 'echo \`node -e "console.log(crypto.randomUUID())"\` | wrangler --env production secret put JWT_STATE_SECRET'`);
    return false;
  }

  return true;
}
