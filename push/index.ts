import { $, CryptoHasher, file, gzipSync, write } from "bun";
import tar from "tar-fs";
import fs from "node:fs";

const username = process.env["USERNAME_REGISTRY"];
const password = fs.readFileSync(process.stdin.fd, "utf-8");
if (!username || !password) {
  console.error("username or password not defined, push might not be able to authenticate with registry");
}

const image = process.argv[2];
if (image === undefined) {
  console.error("usage: bun run index.ts <image>");
  process.exit(1);
}

const output = await $`docker save ${image} --output temporary.tar`;

if (output.exitCode != 0) {
  console.error("Error saving image", image, output.text());
  process.exit(1);
}

const imagePath = ".output-image";
const extract = tar.extract(imagePath);

await Bun.file("./temporary.tar")
  .stream()
  .pipeTo(
    new WritableStream({
      write(value) {
        return new Promise((res, rej) => {
          extract.write(value, (err) => {
            if (err) {
              rej(err);
              return;
            }
          });
          extract.once("drain", () => {
            res();
          });
        });
      },
      close() {
        extract.end();
      },
    }),
  );

type DockerSaveConfigManifest = {
  Config: string;
  Layers: string[];
}[];

import path from "path";
const manifests = (await Bun.file(path.join(imagePath, "manifest.json")).json()) as DockerSaveConfigManifest;

if (manifests.length == 0) {
  console.error("unexpected manifest of length 0");
  process.exit(1);
}

if (manifests.length > 1) {
  console.warn("Manifest resolved to multiple images, picking the first one");
}

import plimit from "p-limit";
const pool = plimit(5);
import zlib from "node:zlib";
import stream from "node:stream";
import { mkdir, rename, rm } from "node:fs/promises";

const cacheFolder = ".cache";

await mkdir(cacheFolder, { recursive: true });

const [manifest] = manifests;
const tasks = [];

for (const layer of manifest.Layers) {
  tasks.push(
    pool(async () => {
      const layerCachePath = path.join(cacheFolder, path.dirname(layer) + "-ptr");
      {
        const layerCacheGzip = file(layerCachePath);
        if (await layerCacheGzip.exists()) {
          const compressedDigest = await layerCacheGzip.text();
          return compressedDigest;
        }
      }

      const layerPath = path.join(imagePath, layer);

      const inprogressPath = path.join(cacheFolder, path.dirname(layer) + "-in-progress");

      await rm(inprogressPath, { recursive: true });
      const layerCacheGzip = file(inprogressPath);

      const cacheWriter = layerCacheGzip.writer();
      const hasher = new Bun.CryptoHasher("sha256");
      const gzipStream = zlib.createGzip({ level: 9 });
      gzipStream.pipe(
        new stream.Writable({
          write(value, _, callback) {
            hasher.update(value);
            cacheWriter.write(value);
            callback();
          },
        }),
      );

      await file(layerPath)
        .stream()
        .pipeTo(
          new WritableStream({
            write(value) {
              return new Promise((res, rej) => {
                gzipStream.write(value, "binary", (err) => {
                  if (err) {
                    rej(err);
                    return;
                  }
                  res();
                });
              });
            },
            close() {
              gzipStream.end();
            },
          }),
        );

      await cacheWriter.flush();
      await cacheWriter.end();
      const digest = hasher.digest("hex");
      await rename(inprogressPath, path.join(cacheFolder, digest));
      await write(layerCachePath, digest);
      return digest;
    }),
  );
}

const configManifest = path.join(imagePath, manifest.Config);
const config = await file(configManifest).text();
const configDigest = new CryptoHasher("sha256").update(config).digest("hex");

const compressedDigests = await Promise.all(tasks);

const proto = process.env["INSECURE_HTTP_PUSH"] === "true" ? "http" : "https";
if (proto === "http") {
  console.error("!! Using plain HTTP !!");
}

const pushTasks = [];
const url = new URL(proto + "://" + image);
const imageHost = url.host;
const imageRepositoryPathParts = url.pathname.split(":");
const imageRepositoryPath = imageRepositoryPathParts.slice(0, imageRepositoryPathParts.length - 1).join(":");
const tag =
  imageRepositoryPathParts.length > 1 ? imageRepositoryPathParts[imageRepositoryPathParts.length - 1] : "latest";

import fetchNode from "node-fetch";

class ReadableLimiter extends stream.Readable {
  public written: number = 0;
  private leftover: Uint8Array | undefined;

  constructor(
    private reader: ReadableStreamDefaultReader<Uint8Array>,
    private limit: number,
    previous?: ReadableLimiter,
  ) {
    super();

    if (previous) this.leftover = previous.leftover;
  }

  _read(): void {
    if (this.limit === 0) {
      this.push(null);
    }

    if (this.leftover !== undefined) {
      const toPushNow = this.leftover.slice(0, this.limit);
      this.leftover = this.leftover.slice(this.limit);
      this.push(toPushNow);
      this.limit -= toPushNow.length;
      this.written += toPushNow.length;
      return;
    }

    this.reader.read().then((result) => {
      if (result.done) return this.push(null);

      let arr = result.value as Uint8Array;
      if (arr.length > this.limit) {
        const toPushNow = arr.slice(0, this.limit);
        this.leftover = arr.slice(this.limit);
        arr = toPushNow;
      }

      if (arr.length === 0) return this.push(null);

      this.push(arr);
      this.limit -= arr.length;
      this.written += arr.length;
    });
  }
}

const cred = `Basic ${btoa(`${username}:${password}`)}`;
async function pushLayer(layerDigest: string, readableStream: ReadableStream, totalLayerSize: number) {
  const headers = new Headers({
    Authorization: cred,
  });
  const layerExistsURL = `${proto}://${imageHost}/v2${imageRepositoryPath}/blobs/${layerDigest}`;
  const layerExistsResponse = await fetch(layerExistsURL, {
    headers,
    method: "HEAD",
  });

  if (!layerExistsResponse.ok && layerExistsResponse.status !== 404) {
    throw new Error(`${layerExistsURL} responded ${layerExistsResponse.status}: ${await layerExistsResponse.text()}`);
  }

  if (layerExistsResponse.ok) {
    console.log(`${layerDigest} already exists...`);
    return;
  }

  const createUploadURL = `${proto}://${imageHost}/v2${imageRepositoryPath}/blobs/uploads/`;
  const createUploadResponse = await fetch(createUploadURL, {
    headers,
    method: "POST",
  });
  if (!createUploadResponse.ok) {
    throw new Error(
      `${createUploadURL} responded ${createUploadResponse.status}: ${await createUploadResponse.text()}`,
    );
  }

  const maxChunkLength = +(createUploadResponse.headers.get("oci-chunk-max-length") ?? 500 * 1024 * 1024);
  if (isNaN(maxChunkLength)) {
    throw new Error(`oci-chunk-max-length header is malformed (not a number)`);
  }

  const reader = readableStream.getReader();
  const uploadId = createUploadResponse.headers.get("Docker-Upload-UUID");
  if (uploadId === null) {
    throw new Error("Docker-Upload-UUID not defined in headers");
  }

  let location = createUploadResponse.headers.get("Location") ?? `/v2${imageRepositoryPath}/blobs/uploads/${uploadId}`;
  const putChunkUploadURL = `${proto}://${imageHost}${location}`;
  let end = Math.min(maxChunkLength, totalLayerSize);
  let written = 0;
  let previousReadable: ReadableLimiter | undefined;
  while (totalLayerSize > 0) {
    const writtenBeforeThisStream = written;
    const range = `0-${end - 1}`;
    const current = new ReadableLimiter(reader as ReadableStreamDefaultReader, end - written, previousReadable);
    // we have to do fetchNode because Bun doesn't allow setting custom Content-Length.
    // https://github.com/oven-sh/bun/issues/10507
    const putChunkResult = await fetchNode(putChunkUploadURL, {
      method: "PATCH",
      body: current,
      headers: new Headers({
        "Range": range,
        "Authorization": cred,
        "Content-Length": `${written - writtenBeforeThisStream}`,
      }),
    });
    if (!putChunkResult.ok) {
      throw new Error(
        `uploading chunk ${putChunkUploadURL} returned ${putChunkResult.status}: ${await putChunkResult.text()}`,
      );
    }

    previousReadable = current;
    totalLayerSize -= previousReadable.written;
    written += previousReadable.written;
    location = putChunkResult.headers.get("Location") ?? location;
    const expectedRange = `0-${end - 1}`;
    const rangeResponse = putChunkResult.headers.get("Range");
    if (rangeResponse !== expectedRange) {
      throw new Error(`unexpected Range header ${rangeResponse}, expected ${expectedRange}`);
    }

    console.log("pushed", layerDigest);
  }

  const range = `0-${written - 1}`;
  const uploadURL = new URL(`${proto}://${imageHost}${location}`);
  uploadURL.searchParams.append("digest", layerDigest);

  const response = await fetch(uploadURL.toString(), {
    method: "PUT",
    headers: new Headers({
      Range: range,
      Authorization: cred,
    }),
  });
  if (!response.ok) {
    throw new Error(`${uploadURL.toString()} failed with ${response.status}: ${await response.text()}`);
  }
}

const layersManifest = [] as {
  readonly mediaType: "application/vnd.oci.image.layer.v1.tar+gzip";
  readonly size: number;
  readonly digest: `sha256:${string}`;
}[];

for (const compressedDigest of compressedDigests) {
  const layer = file(path.join(cacheFolder, compressedDigest));
  layersManifest.push({
    mediaType: "application/vnd.oci.image.layer.v1.tar+gzip",
    size: layer.size,
    digest: `sha256:${compressedDigest}`,
  } as const);
  pushTasks.push(
    pool(async () => {
      await pushLayer(`sha256:${compressedDigest}`, layer.stream(), layer.size);
    }),
  );
}

pushTasks.push(
  pool(async () => {
    await pushLayer(
      `sha256:${configDigest}`,
      new ReadableStream({
        pull(controller) {
          controller.enqueue(config);
          controller.close();
        },
      }),
      config.length,
    );
  }),
);

const manifestObject = {
  schemaVersion: 2,
  mediaType: "application/vnd.oci.image.manifest.v1+json",
  config: {
    mediaType: "application/vnd.oci.image.config.v1+json",
    size: config.length,
    digest: `sha256:${configDigest}`,
  },
  layers: layersManifest,
} as const;

const manifestUploadURL = `${proto}://${imageHost}/v2${imageRepositoryPath}/manifests/${tag}`;
const responseManifestUpload = await fetch(manifestUploadURL, {
  headers: {
    "Authorization": cred,
    "Content-Type": "application/json",
  },
  body: JSON.stringify(manifestObject),
  method: "PUT",
});

if (!responseManifestUpload.ok) {
  throw new Error(
    `manifest upload ${manifestUploadURL} returned ${
      responseManifestUpload.status
    }: ${await responseManifestUpload.text()}`,
  );
}
console.log(manifestObject);
console.log("OK");
