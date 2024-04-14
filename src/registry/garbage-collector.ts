import { DurableObject } from "cloudflare:workers";
import { Env } from "../..";

// We have 2 modes for the garbage collector, unreferenced and untagged.
// Unreferenced will delete all blobs that are not referenced by any manifest.
// Untagged will delete all blobs that are not referenced by any manifest and are not tagged.

export type GARBAGE_COLLECTOR_MODE = "unreferenced" | "untagged";

export class GarbageCollector extends DurableObject<Env> {

  async schedule(name:string, mode: GARBAGE_COLLECTOR_MODE): Promise<void> {
    this.ctx.storage.put("name", name);
    this.ctx.storage.put("mode", mode);
    await this.ctx.storage.setAlarm(Date.now() + 600 * 1000);
  }

  override async alarm(): Promise<void> {
    this.ctx.waitUntil(this.collect());
  }

  private async list(prefix: string, callback: (object: R2Object) => Promise<void>): Promise<void>{
    const listed = await this.env.REGISTRY.list({ prefix });
    for (const object of listed.objects) {
      await callback(object);
    }
    let truncated = listed.truncated;
    let cursor = listed.cursor;
    while (truncated) {
      const next = await this.env.REGISTRY.list({ prefix, cursor });
      for (const object of next.objects) {
        await callback(object);
      }
      truncated = next.truncated;
      cursor = next.cursor;
    }
  }

  async collect(): Promise<void> {
    const name = await this.ctx.storage.get<string>("name");
    const mode = await this.ctx.storage.get<GARBAGE_COLLECTOR_MODE>("mode");
    if (!name || !mode) {
      return;
    }
    let referencedBlobs = new Set<string>(); // We can run out of memory, this should be a bloom filter

    await this.list(`${name}/manifests/`, async (manifestObject) => {
      const tag = manifestObject.key.split("/").pop();
      if ((!tag) || (mode === "untagged" && tag.startsWith("sha256:"))) {
        return;
      }

      const manifest = await this.env.REGISTRY.get(manifestObject.key);
      if (!manifest) {
        return;
      }

      const manifestData = await manifest.text();
      
      const layerRegex = /sha256:[a-f0-9]{64}/g;
      let match;
      while ((match = layerRegex.exec(manifestData)) !== null) {
        referencedBlobs.add( match[0] );
      }
    });

    let unreferencedKeys: string[] = [];
    await this.list(`${name}/blobs/`, async (object) => {
      const hash = object.key.split("/").pop();
      if (hash && !referencedBlobs.has(hash)) {
        unreferencedKeys.push(object.key);
        if (unreferencedKeys.length > 100) {
          await this.env.REGISTRY.delete(unreferencedKeys);
          unreferencedKeys = [];
        }
      }
    });
    console.log(`Deleting ${unreferencedKeys.length} unreferenced blobs`);
    if (unreferencedKeys.length > 0) {
      await this.env.REGISTRY.delete(unreferencedKeys);
    }

    await this.ctx.storage.deleteAll();
  }
}