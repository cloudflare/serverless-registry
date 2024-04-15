import { DurableObject } from "cloudflare:workers";
import { Env } from "../..";
import { ServerError } from "../errors";

// We have 2 modes for the garbage collector, unreferenced and untagged.
// Unreferenced will delete all blobs that are not referenced by any manifest.
// Untagged will delete all blobs that are not referenced by any manifest and are not tagged.

export type GARBAGE_COLLECTOR_MODE = "unreferenced" | "untagged";
export type GCOptions = {
  name: string;
  mode: GARBAGE_COLLECTOR_MODE;
};

export class GarbageCollector extends DurableObject<Env> {
  static DELAY = 30 * 60 * 1000;
  static LAST_UPDATE_THRESHOLD = 10 * 60 * 1000;

  async schedule(name:string, mode: GARBAGE_COLLECTOR_MODE): Promise<void> {
    const options: GCOptions = { name, mode };
    await this.ctx.storage.put("options", options);
    await this.ctx.storage.setAlarm(Date.now() + GarbageCollector.DELAY);
  }

  override async alarm(): Promise<void> {
    this.ctx.waitUntil(this.collect());
  }

  private async list(prefix: string, callback: (object: R2Object) => Promise<boolean>): Promise<boolean>{
    const listed = await this.env.REGISTRY.list({ prefix });
    for (const object of listed.objects) {
      if ((await callback(object)) === false) {
        return false;
      }
    }
    let truncated = listed.truncated;
    let cursor = listed.cursor;
    while (truncated) {
      const next = await this.env.REGISTRY.list({ prefix, cursor });
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

  async collect(options?: GCOptions, force: boolean = false): Promise<boolean> {
    const cutOffDate = Date.now() - GarbageCollector.LAST_UPDATE_THRESHOLD;
    options = options || await this.ctx.storage.get<GCOptions>("options");
    if (!options) {
      throw new ServerError("No options found for garbage collector", 500);
    }
    let referencedBlobs = new Set<string>(); // We can run out of memory, this should be a bloom filter

    let result = await this.list(`${options.name}/manifests/`, async (manifestObject) => {
      const tag = manifestObject.key.split("/").pop();
      if ((!tag) || (options.mode === "untagged" && tag.startsWith("sha256:"))) {
        return true;
      }
      if (!force && manifestObject.uploaded.getTime() > cutOffDate) {
        return false;
      }
      const manifest = await this.env.REGISTRY.get(manifestObject.key);
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
    if (!result) {
      return false;
    }

    let unreferencedKeys: string[] = [];
    result = await this.list(`${options.name}/blobs/`, async (object) => {
      if (!force && object.uploaded.getTime() > cutOffDate) {
        return false;
      }
      const hash = object.key.split("/").pop();
      if (hash && !referencedBlobs.has(hash)) {
        unreferencedKeys.push(object.key);
        if (unreferencedKeys.length > 100) {
          await this.env.REGISTRY.delete(unreferencedKeys);
          unreferencedKeys = [];
        }
      }
      return true;
    });
    if (!result) {
      return false;
    }
    if (unreferencedKeys.length > 0) {
      await this.env.REGISTRY.delete(unreferencedKeys);
    }

    await this.ctx.storage.deleteAll();
    return true;
  }
}