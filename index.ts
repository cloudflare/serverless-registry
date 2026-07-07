/**
 * The core server that runs on a Cloudflare worker.
 */

import { Router } from "itty-router";
import { AuthErrorResponse, InternalError } from "./src/errors";
import v2Router from "./src/router";
import { authenticationMethodFromEnv } from "./src/authentication-method";
import { Registry } from "./src/registry/registry";
import { R2Registry } from "./src/registry/r2";
import { log } from "./src/log";

// A full compatibility mode means that the r2 registry will try its best to
// help the client on the layer push. See how we let the client push layers with chunked uploads for more information.
type PushCompatibilityMode = "full" | "none";

export interface Env {
  REGISTRY: R2Bucket;
  METRICS?: AnalyticsEngineDataset;
  ENVIRONMENT: string;
  JWT_REGISTRY_TOKENS_PUBLIC_KEY?: string;
  USERNAME?: string;
  PASSWORD?: string;
  READONLY_USERNAME?: string;
  READONLY_PASSWORD?: string;
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

function recordMetric(env: Env, outcome: string, status: number, durationMs: number): void {
  // blobs[0] is a fixed "registry" source tag: the METRICS dataset is shared
  // with the auth Worker (a separate deployable, instrumented in the
  // companion devprod-infra plan), so the alerting Worker's queries group by
  // this dimension to compute per-source error rates.
  env.METRICS?.writeDataPoint({
    blobs: ["registry", outcome],
    doubles: [durationMs],
    indexes: [String(status)],
  });
}

export default {
  async fetch(request: Request, env: Env, context?: ExecutionContext) {
    const start = Date.now();

    if (!ensureConfig(env)) {
      recordMetric(env, "config_error", 500, Date.now() - start);
      return new AuthErrorResponse(request);
    }

    const authMethod = await authenticationMethodFromEnv(env);
    if (!authMethod) {
      recordMetric(env, "config_error", 500, Date.now() - start);
      return new AuthErrorResponse(request);
    }

    const credentials = await authMethod.checkCredentials(request);
    if (!credentials.verified) {
      log.warn("auth_denied", { authmode: authMethod.authmode });
      recordMetric(env, "auth_denied", 401, Date.now() - start);
      return new AuthErrorResponse(request);
    }

    env.REGISTRY_CLIENT = new R2Registry(env);
    try {
      // Dispatch the request to the appropriate route
      const res = await router.fetch(request, env, context);
      recordMetric(env, "success", res.status, Date.now() - start);
      return res;
    } catch (err) {
      if (err instanceof Response) {
        log.warn("router_error_response", { method: request.method, status: err.status, url: err.url });
        recordMetric(env, "router_error", err.status, Date.now() - start);
        return err;
      }

      // Unexpected error
      if (err instanceof Error) {
        log.error("unhandled_error", {
          name: err.name,
          message: err.message,
          cause: err.cause ? String(err.cause) : null,
          stack: err.stack ?? null,
        });
        recordMetric(env, "unhandled_error", 500, Date.now() - start);
        return new InternalError();
      }

      log.error("unhandled_non_error", { value: JSON.stringify(err) });
      recordMetric(env, "unhandled_error", 500, Date.now() - start);
      return new InternalError();
    }
  },
} satisfies ExportedHandler<Env>;

const ensureConfig = (env: Env): boolean => {
  if (!env.REGISTRY) {
    log.error("missing_registry_binding", {
      hint: "Setup an R2 bucket and add the binding in wrangler.toml. Try 'npx wrangler --env production r2 bucket create r2-registry'",
    });
    return false;
  }

  return true;
};
