import { Env } from "../..";
import { errorString } from "../utils";
import z from "zod";

// Defines a registry and how it's configured
const registryConfiguration = z.object({
  registry: z.string().url(),
  password_env: z.string(),
  username: z.string(),
});

export type RegistryConfiguration = z.infer<typeof registryConfiguration>;

export function registries(env: Env): RegistryConfiguration[] {
  if (env.REGISTRIES_JSON === undefined || env.REGISTRIES_JSON.length === 0) {
    return [];
  }

  try {
    const jsonObject = JSON.parse(env.REGISTRIES_JSON);
    return registryConfiguration.array().parse(jsonObject);
  } catch (err) {
    console.error("Error parsing registries JSON: " + errorString(err));
    return [];
  }
}

// RegistryHTTPClient implements a registry client that is able to pull/push to the configured registry
export class RegistryHTTPClient {
  constructor(private env: Env, private configuration: RegistryConfiguration) {}

  // todo: implementation
}
