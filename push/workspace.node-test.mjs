import assert from "node:assert/strict";
import { existsSync } from "node:fs";
import { mkdir, writeFile } from "node:fs/promises";
import path from "node:path";
import { test } from "node:test";

import { withPushWorkspace } from "./workspace.mjs";

test("removes generated push files after success", async () => {
  let workspaceRoot = "";

  const result = await withPushWorkspace("image-id", async (workspace) => {
    workspaceRoot = path.dirname(workspace.tarFile);
    await Promise.all([
      writeFile(workspace.tarFile, "image"),
      mkdir(workspace.imagePath, { recursive: true }),
      mkdir(workspace.cacheFolder, { recursive: true }),
    ]);
    return "pushed";
  });

  assert.equal(result, "pushed");
  assert.equal(existsSync(workspaceRoot), false);
});

test("removes generated push files after failure and preserves the error", async () => {
  const failure = new Error("push failed");
  let workspaceRoot = "";
  let thrown;

  try {
    await withPushWorkspace("image-id", async (workspace) => {
      workspaceRoot = path.dirname(workspace.tarFile);
      await writeFile(workspace.tarFile, "image");
      throw failure;
    });
  } catch (error) {
    thrown = error;
  }

  assert.equal(thrown, failure);
  assert.equal(existsSync(workspaceRoot), false);
});
