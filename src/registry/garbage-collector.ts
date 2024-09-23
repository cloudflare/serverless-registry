// We have 2 modes for the garbage collector, unreferenced and untagged.
// Unreferenced will delete all blobs that are not referenced by any manifest.
// Untagged will delete all blobs that are not referenced by any manifest and are not tagged.

import { ManifestSchema } from "../manifest";

export type GarbageCollectionMode = "unreferenced" | "untagged";
export type GCOptions = {
  name: string;
  mode: GarbageCollectionMode;
};

// The garbage collector checks for dangling layers in the namespace. It's a lock free
// GC, but on-conflict (when there is an ongoing manifest insertion, or an ongoing garbage collection),
// the methods can throw errors.
//
// Summary:
//          insertParent() {
//              mark = updateInsertMark(); // mark insertion
//              defer cleanInsertMark(mark);
//              checkEveryChildIsOK();
//              getDeletionMarkIsFalse(); // make sure not ongoing deletion mark after checking child is in db
//              insertParent(); // insert parent in db
//           }
//
//           gc() {
//             setDeletionMark() // marks deletion as gc
//             defer { cleanDeletionMark(); } // clean up mark
//             checkNotOngoingInsertMark() // makes sure not ongoing updateInsertMark
//             deleteChildrenWithoutParent(); // go ahead and clean children
//           }
//
// This makes it so: after every layer is OK we can proceed and insert the manifest, as there is no ongoing GC
// In the GC code, if there is an insertion on-going, there is an error.
export class GarbageCollector {
  private registry: R2Bucket;

  constructor(registry: R2Bucket) {
    this.registry = registry;
  }

  async markForGarbageCollection(namespace: string): Promise<string> {
    const etag = crypto.randomUUID();
    const deletion = await this.registry.put(`${namespace}/deletion`, etag);
    if (deletion === null) throw new Error("unreachable");
    return etag;
  }

  async cleanupGarbageCollectionMark(namespace: string) {
    await this.registry.delete(`${namespace}/deletion`);
  }

  async checkCanInsertData(namespace: string): Promise<boolean> {
    const deletion = await this.registry.head(`${namespace}/deletion`);
    if (deletion === null) {
      return true;
    }

    return false;
  }

  // If successful, it inserted in R2 that its going
  // to start inserting data that might conflight with GC.
  async markForInsertion(namespace: string): Promise<string> {
    const uid = crypto.randomUUID();
    const deletion = await this.registry.put(`${namespace}/insertion/${uid}`, uid);
    if (deletion === null) throw new Error("unreachable");
    return uid;
  }

  async cleanInsertion(namespace: string, tag: string) {
    await this.registry.delete(`${namespace}/insertion/${tag}`);
  }

  async checkIfGCCanContinue(namespace: string): Promise<boolean> {
    const objects = await this.registry.list({ prefix: `${namespace}/insertion` });
    for (const object of objects.objects) {
      if (object.uploaded.getTime() + 1000 * 60 <= Date.now()) {
        await this.registry.delete(object.key);
      } else {
        return false;
      }
    }

    // call again to clean more
    if (objects.truncated) return false;
    return true;
  }

  private async list(prefix: string, callback: (object: R2Object) => Promise<boolean>): Promise<boolean> {
    const listed = await this.registry.list({ prefix });
    for (const object of listed.objects) {
      if ((await callback(object)) === false) {
        return false;
      }
    }

    let truncated = listed.truncated;
    let cursor = listed.truncated ? listed.cursor : undefined;

    while (truncated) {
      const next = await this.registry.list({ prefix, cursor });
      for (const object of next.objects) {
        if ((await callback(object)) === false) {
          return false;
        }
      }
      truncated = next.truncated;
      cursor = truncated ? cursor : undefined;
    }
    return true;
  }

  async collect(options: GCOptions): Promise<boolean> {
    await this.markForGarbageCollection(options.name);
    try {
      return await this.collectInner(options);
    } finally {
      // if this fails, user can always call a custom endpoint to clean it up
      await this.cleanupGarbageCollectionMark(options.name);
    }
  }

  private async collectInner(options: GCOptions): Promise<boolean> {
    // We can run out of memory, this should be a bloom filter
    let referencedBlobs = new Set<string>();

    await this.list(`${options.name}/manifests/`, async (manifestObject) => {
      const tag = manifestObject.key.split("/").pop();
      if (!tag || (options.mode === "untagged" && tag.startsWith("sha256:"))) {
        return true;
      }
      const manifest = await this.registry.get(manifestObject.key);
      if (!manifest) {
        return true;
      }

      const manifestData = (await manifest.json()) as ManifestSchema;
      manifestData.layers.forEach((layer) => {
        referencedBlobs.add(layer.digest);
      });

      return true;
    });

    let unreferencedKeys: string[] = [];
    const deleteThreshold = 15;
    await this.list(`${options.name}/blobs/`, async (object) => {
      if (!(await this.checkIfGCCanContinue(options.name))) {
        throw new Error("there is a manifest insertion going, the garbage collection shall stop");
      }

      const hash = object.key.split("/").pop();
      if (hash && !referencedBlobs.has(hash)) {
        unreferencedKeys.push(object.key);
        if (unreferencedKeys.length > deleteThreshold) {
          await this.registry.delete(unreferencedKeys);
          unreferencedKeys = [];
        }
      }
      return true;
    });
    if (unreferencedKeys.length > 0) {
      await this.registry.delete(unreferencedKeys);
    }

    return true;
  }
}
