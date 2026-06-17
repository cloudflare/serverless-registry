import { cloudflareTest } from "@cloudflare/vitest-pool-workers";
import { defineConfig } from "vitest/config";

export default defineConfig({
  plugins: [
    cloudflareTest({
      wrangler: { configPath: "./test/wrangler.test.jsonc", environment: "dev" },
    }),
  ],
  test: {
    silent: "passed-only",
  },
});
