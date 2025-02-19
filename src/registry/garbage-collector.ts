// We have 2 modes for the garbage collector, unreferenced and untagged.
// Unreferenced will delete all blobs that are not referenced by any manifest.
// Untagged will delete all blobs that are not referenced by any manifest and are not tagged.

import { ManifestSchema } from "../manifest";
import { hexToDigest } from "../user";
import {symlinkHeader} from "./r2";

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
    const listed = await this.registry.list({ prefix: prefix, include: ["customMetadata"] });
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
    const manifestList: { [key: string]: Set<string> } = {};
    const mark = await this.getInsertionMark(options.name);

    // List manifest from repo to be scanned
    await this.list(`${options.name}/manifests/`, async (manifestObject) => {
      const currentHashFile = hexToDigest(manifestObject.checksums.sha256!);
      if (manifestList[currentHashFile] === undefined) {
        manifestList[currentHashFile] = new Set<string>();
      }
      manifestList[currentHashFile].add(manifestObject.key);
      return true;
    });

    // In untagged mode, search for manifest to delete
    if (options.mode === "untagged") {
      const manifestToRemove = new Set<string>();
      const referencedManifests = new Set<string>();
      // List tagged manifest to find manifest-list
      for (const [_, manifests] of Object.entries(manifestList)) {
        const taggedManifest = [...manifests].filter((item) => !item.split("/").pop()?.startsWith("sha256:"));
        for (const manifestPath of taggedManifest) {
          // Tagged manifest some, load manifest content
          const manifest = await this.registry.get(manifestPath);
          if (!manifest) {
            continue;
          }

          const manifestData = (await manifest.json()) as ManifestSchema;
          // Search for manifest list
          if (manifestData.schemaVersion == 2 && "manifests" in manifestData) {
            // Extract referenced manifests from manifest list
            manifestData.manifests.forEach((manifest) => {
              referencedManifests.add(manifest.digest);
            });
          }
        }
      }

      for (const [key, manifests] of Object.entries(manifestList)) {
        if (referencedManifests.has(key)) {
          continue;
        }
        if (![...manifests].some((item) => !item.split("/").pop()?.startsWith("sha256:"))) {
          // Add untagged manifest that should be removed
          manifests.forEach((manifest) => {
            manifestToRemove.add(manifest);
          });
          // Manifest to be removed shouldn't be parsed to search for referenced layers
          delete manifestList[key];
        }
      }

      // Deleting untagged manifest
      if (manifestToRemove.size > 0) {
        if (!(await this.checkIfGCCanContinue(options.name, mark))) {
          throw new Error("there is a manifest insertion going, the garbage collection shall stop");
        }

        // GC will deleted untagged manifest
        await this.registry.delete(manifestToRemove.values().toArray());
      }
    }

    const referencedBlobs = new Set<string>();
    // From manifest, extract referenced layers
    for (const [_, manifests] of Object.entries(manifestList)) {
      // Select only one manifest per unique manifest
      const manifestPath = manifests.values().next().value;
      if (manifestPath === undefined) {
        continue;
      }
      const manifest = await this.registry.get(manifestPath);
      // Skip if manifest not found
      if (!manifest) continue;

      const manifestData = (await manifest.json()) as ManifestSchema;

      if (manifestData.schemaVersion === 1) {
        manifestData.fsLayers.forEach((layer) => {
          referencedBlobs.add(layer.blobSum);
        });
      } else {
        // Skip manifest-list, they don't contain any layers references
        if ("manifests" in manifestData) continue;
        // Add referenced layers from current manifest
        manifestData.layers.forEach((layer) => {
          referencedBlobs.add(layer.digest);
        });
        // Add referenced config blob from current manifest
        referencedBlobs.add(manifestData.config.digest);
      }
    }

    const unreferencedBlobs = new Set<string>();
    // List blobs to be removed
    await this.list(`${options.name}/blobs/`, async (object) => {
      const blobHash = object.key.split("/").pop();
      if (blobHash && !referencedBlobs.has(blobHash)) {
        unreferencedBlobs.add(object.key);
      }
      return true;
    });

    // Check for symlink before removal
    if (unreferencedBlobs.size >= 0) {
      await this.list("", async (object) => {
        const objectPath = object.key;
        // Skip non-blobs object and from any other repository (symlink only target cross repository blobs)
        if (objectPath.startsWith(`${options.name}/`) || !objectPath.includes("/blobs/sha256:")) {
          return true;
        }
        if (object.customMetadata && object.customMetadata[symlinkHeader] !== undefined) {
          // Check if the symlink target the current GC repository
          if (object.customMetadata[symlinkHeader] !== options.name) return true;
          // Get symlink blob to retrieve its target
          const symlinkBlob = await this.registry.get(object.key);
          // Skip if symlinkBlob not found
          if (!symlinkBlob) return true;
          // Get the path of the target blob from the symlink blob
          const targetBlobPath = await symlinkBlob.text();
          if (unreferencedBlobs.has(targetBlobPath)) {
            // This symlink target a layer that should be removed
            unreferencedBlobs.delete(targetBlobPath);
          }
        }
        return unreferencedBlobs.size > 0;
      });
    }

    if (unreferencedBlobs.size > 0) {
      if (!(await this.checkIfGCCanContinue(options.name, mark))) {
        throw new Error("there is a manifest insertion going, the garbage collection shall stop");
      }

      // GC will delete unreferenced blobs
      await this.registry.delete(unreferencedBlobs.values().toArray());
    }

    return true;
  }
}
