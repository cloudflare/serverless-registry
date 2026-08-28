import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import path from "node:path";

/**
 * @typedef {object} PushWorkspace
 * @property {string} tarFile
 * @property {string} imagePath
 * @property {string} cacheFolder
 */

/**
 * @template T
 * @param {string} imageID
 * @param {(workspace: Readonly<PushWorkspace>) => Promise<T>} run
 * @returns {Promise<T>}
 */
export async function withPushWorkspace(imageID, run) {
  const workspaceRoot = await mkdtemp(path.join(tmpdir(), "serverless-registry-push-"));
  const workspace = {
    tarFile: path.join(workspaceRoot, `${imageID}.tar`),
    imagePath: path.join(workspaceRoot, "image"),
    cacheFolder: path.join(workspaceRoot, "cache"),
  };

  try {
    return await run(workspace);
  } finally {
    await rm(workspaceRoot, { force: true, recursive: true });
  }
}
