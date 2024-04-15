// We have 2 modes for the garbage collector, unreferenced and untagged.
// Unreferenced will delete all blobs that are not referenced by any manifest.
// Untagged will delete all blobs that are not referenced by any manifest and are not tagged.

export type GARBAGE_COLLECTOR_MODE = "unreferenced" | "untagged";
export type GCOptions = {
  name: string;
  mode: GARBAGE_COLLECTOR_MODE;
};

export class GarbageCollector {
  private registry: R2Bucket;

  constructor(registry: R2Bucket) {
    this.registry = registry;
  }

  private async list(prefix: string, callback: (object: R2Object) => Promise<boolean>): Promise<boolean>{
    const listed = await this.registry.list({ prefix });
    for (const object of listed.objects) {
      if ((await callback(object)) === false) {
        return false;
      }
    }
    let truncated = listed.truncated;
    let cursor = listed.cursor;
    while (truncated) {
      const next = await this.registry.list({ prefix, cursor });
      for (const object of next.objects) {
        if ((await callback(object)) === false) {
          return false;
        }
      }
      truncated = next.truncated;
      cursor = next.cursor;
    }
    return true;
  }

  async collect(options: GCOptions): Promise<boolean> {
    let referencedBlobs = new Set<string>(); // We can run out of memory, this should be a bloom filter

    await this.list(`${options.name}/manifests/`, async (manifestObject) => {
      const tag = manifestObject.key.split("/").pop();
      if ((!tag) || (options.mode === "untagged" && tag.startsWith("sha256:"))) {
        return true;
      }
      const manifest = await this.registry.get(manifestObject.key);
      if (!manifest) {
        return true;
      }

      const manifestData = await manifest.text();
      
      const layerRegex = /sha256:[a-f0-9]{64}/g;
      let match;
      while ((match = layerRegex.exec(manifestData)) !== null) {
        referencedBlobs.add( match[0] );
      }
      return true;
    });

    let unreferencedKeys: string[] = [];
    await this.list(`${options.name}/blobs/`, async (object) => {
      const hash = object.key.split("/").pop();
      if (hash && !referencedBlobs.has(hash)) {
        unreferencedKeys.push(object.key);
        if (unreferencedKeys.length > 100) {
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