import { Env } from "..";
import { newRegistryTokens } from "./token";
import { UserAuthenticator } from "./user";
import type { AuthenticatorCredentials } from "./user";

export async function authenticationMethodFromEnv(env: Env) {
  if (env.JWT_REGISTRY_TOKENS_PUBLIC_KEY) {
    return await newRegistryTokens(env.JWT_REGISTRY_TOKENS_PUBLIC_KEY);
  } else if (env.USERNAME && env.PASSWORD) {
    const credentials: AuthenticatorCredentials[] = [
      { username: env.USERNAME, password: env.PASSWORD, capabilities: ["pull", "push"] }
    ];

    if (env.READONLY_USERNAME && env.READONLY_PASSWORD) {
      credentials.push({ username: env.READONLY_USERNAME, password: env.READONLY_PASSWORD, capabilities: ["pull"] });
    }

    return new UserAuthenticator(credentials);
  }

  console.error("Either env.JWT_REGISTRY_TOKENS_PUBLIC_KEY must be set or both env.USERNAME, env.PASSWORD must be set.");

  // invalid configuration
  return undefined;
}
