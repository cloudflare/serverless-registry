// We have 2 modes for the garbage collector, unreferenced and untagged.
// Unreferenced will delete all blobs that are not referenced by any manifest.
// Untagged will delete all blobs that are not referenced by any manifest and are not tagged.

import { ServerError } from "../errors";
import { ManifestSchema } from "../manifest";
import { isReference } from "./r2";

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
//              gcMark = getGCMark(); // get last gc mark
//              mark = updateInsertMark(); // mark insertion
//              defer cleanInsertMark(mark);
//              checkEveryChildIsOK();
//              gcMarkIsEqualAndNotOngoingGc(gcMark); // make sure not ongoing deletion mark after checking child is in db
//              insertParent(); // insert parent in db
//           }
//
//           gc() {
//             insertionMark = getInsertionMark() // get last insertion mark
//             mark = setGCMark() // marks deletion as gc
//             defer { cleanGCMark(mark); } // clean up mark
//             checkNotOngoingInsertMark(mark) // makes sure not ongoing updateInsertMark, and no new one
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
    const deletion = await this.registry.put(`${namespace}/gc/marker`, etag);
    if (deletion === null) throw new Error("unreachable");
    // set last_update so inserters are able to invalidate
    await this.registry.put(`${namespace}/gc/last_update`, null, {
      customMetadata: { timestamp: `${Date.now()}-${crypto.randomUUID()}` },
    });
    return etag;
  }

  async cleanupGarbageCollectionMark(namespace: string) {
    // set last_update so inserters can confirm that a GC didnt happen while they were confirming data
    await this.registry.put(`${namespace}/gc/last_update`, null, {
      customMetadata: { timestamp: `${Date.now()}-${crypto.randomUUID()}` },
    });
    await this.registry.delete(`${namespace}/gc/marker`);
  }

  async getGCMarker(namespace: string): Promise<string> {
    const object = await this.registry.head(`${namespace}/gc/last_update`);
    if (object === null) {
      return "";
    }

    if (object.customMetadata === undefined) {
      return "";
    }

    return object.customMetadata["timestamp"] ?? "mark";
  }

  async checkCanInsertData(namespace: string, mark: string): Promise<boolean> {
    const gcMarker = await this.registry.head(`${namespace}/gc/marker`);
    if (gcMarker !== null) {
      return false;
    }

    const newMarker = await this.getGCMarker(namespace);
    // There's been a new garbage collection since we started the check for insertion
    if (newMarker !== mark) return false;

    return true;
  }

  // If successful, it inserted in R2 that its going
  // to start inserting data that might conflight with GC.
  async markForInsertion(namespace: string): Promise<string> {
    const uid = crypto.randomUUID();
    // mark that there is an on-going insertion
    const deletion = await this.registry.put(`${namespace}/insertion/${uid}`, uid);
    if (deletion === null) throw new Error("unreachable");
    // set last_update so GC is able to invalidate
    await this.registry.put(`${namespace}/insertion/last_update`, null, {
      customMetadata: { timestamp: `${Date.now()}-${crypto.randomUUID()}` },
    });

    return uid;
  }

  async cleanInsertion(namespace: string, tag: string) {
    // update again to invalidate GC and the insertion is safe
    await this.registry.put(`${namespace}/insertion/last_update`, null, {
      customMetadata: { timestamp: `${Date.now()}-${crypto.randomUUID()}` },
    });

    await this.registry.delete(`${namespace}/insertion/${tag}`);
  }

  async getInsertionMark(namespace: string): Promise<string> {
    const object = await this.registry.head(`${namespace}/insertion/last_update`);
    if (object === null) {
      return "";
    }

    if (object.customMetadata === undefined) {
      return "";
    }

    return object.customMetadata["timestamp"] ?? "mark";
  }

  async checkIfGCCanContinue(namespace: string, mark: string): Promise<boolean> {
    const objects = await this.registry.list({ prefix: `${namespace}/insertion` });
    for (const object of objects.objects) {
      if (object.key.endsWith("/last_update")) continue;
      if (object.uploaded.getTime() + 1000 * 60 <= Date.now()) {
        await this.registry.delete(object.key);
      } else {
        return false;
      }
    }

    // call again to clean more
    if (objects.truncated) return false;

    const newMark = await this.getInsertionMark(namespace);
    if (newMark !== mark) {
      return false;
    }

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
    const mark = await this.getInsertionMark(options.name);

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
      // TODO: garbage collect manifests.
      if ("manifests" in manifestData) {
        return true;
      }

      if (manifestData.schemaVersion === 1) {
        manifestData.fsLayers.forEach((layer) => {
          referencedBlobs.add(layer.blobSum);
        });
      } else {
        manifestData.layers.forEach((layer) => {
          referencedBlobs.add(layer.digest);
        });
      }

      return true;
    });

    let unreferencedKeys: string[] = [];
    const deleteThreshold = 15;
    await this.list(`${options.name}/blobs/`, async (object) => {
      const hash = object.key.split("/").pop();
      if (hash && !referencedBlobs.has(hash)) {
        const key = isReference(object);
        // also push the underlying reference object
        if (key) {
          unreferencedKeys.push(key);
        }

        unreferencedKeys.push(object.key);
        if (unreferencedKeys.length > deleteThreshold) {
          if (!(await this.checkIfGCCanContinue(options.name, mark))) {
            throw new ServerError("there is a manifest insertion going, the garbage collection shall stop");
          }

          await this.registry.delete(unreferencedKeys);
          unreferencedKeys = [];
        }
      }
      return true;
    });
    if (unreferencedKeys.length > 0) {
      if (!(await this.checkIfGCCanContinue(options.name, mark))) {
        throw new Error("there is a manifest insertion going, the garbage collection shall stop");
      }

      await this.registry.delete(unreferencedKeys);
    }

    return true;
  }
}
