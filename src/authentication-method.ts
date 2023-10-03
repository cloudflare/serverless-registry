import { Env } from "..";
import { newRegistryTokens } from "./token";
import { UserAuthenticator } from "./user";

export async function authenticationMethodFromEnv(env: Env) {
  if (env.JWT_REGISTRY_TOKENS_PUBLIC_KEY) {
    return await newRegistryTokens(env.JWT_REGISTRY_TOKENS_PUBLIC_KEY);
  } else if (env.USERNAME && env.PASSWORD) {
    return new UserAuthenticator(env.USERNAME, env.PASSWORD);
  }

  console.error("Either env.JWT_REGISTRY_TOKENS_PUBLIC_KEY must be set or both env.USERNAME, env.PASSWORD must be set.");

  // invalid configuration
  return undefined;
}
