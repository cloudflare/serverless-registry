import { $, CryptoHasher, file, write } from "bun";
import tar from "tar-fs";

import stream from "node:stream";

const username = process.env["USERNAME_REGISTRY"];
async function read(stream: stream.Readable): Promise<string> {
  const chunks = [];
  for await (const chunk of stream.iterator()) {
    chunks.push(chunk);
  }

  return Buffer.concat(chunks).toString("utf8");
}

let password;
if (process.stdin.isTTY) {
  console.error(
    "You need to pass the password with a pipe operator \n\n\t'echo <YOURPASSWORD> | USERNAME_REGISTRY=... bun run index.ts'\n",
  );
} else {
  password = (await read(process.stdin)).trim();
}

if (!username || !password) {
  console.error("Username or password not defined, push won't be able to authenticate with registry");
  if (!process.env["SKIP_AUTH"]) {
    process.exit(1);
  }
}

const image = process.argv[2];
if (image === undefined) {
  console.error("Usage: bun run index.ts <image>");
  process.exit(1);
}

// Check if the image has already been saved from Docker

console.log("Preparing image...");
const imageMetadataRes = await $`docker images --format "{{ .ID }}" ${image}`.quiet();
if (imageMetadataRes.exitCode !== 0) {
  console.error(
    "Image",
    image,
    "doesn't exist. The docker daemon might not be running, or the image doesn't exist. Check your existing images with\n\n\tdocker images",
  );
  process.exit(1);
}

const imageID = imageMetadataRes.text();
if (imageID === "") {
  console.error("Image", image, "doesn't exist. Check your existing images with\n\n\tdocker images");
  process.exit(1);
}

const tarFile = imageID + ".tar";
const imagePath = ".output-image";
if (!(await file(tarFile).exists())) {
  const output = await $`docker save ${image} --output ${tarFile}`;

  if (output.exitCode != 0) {
    console.error("Error saving image", image, output.text());
    process.exit(1);
  }

  const extract = tar.extract(imagePath);

  await Bun.file(tarFile)
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
}

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
import { mkdir, rename, rm } from "node:fs/promises";

const cacheFolder = ".cache";

await mkdir(cacheFolder, { recursive: true });

const [manifest] = manifests;
const tasks = [];

console.log("compressing...");
// Iterate through every layer, read it and compress to a file
for (const layer of manifest.Layers) {
  tasks.push(
    pool(async () => {
      let layerPath = path.join(imagePath, layer);
      // docker likes to put stuff in two ways:
      //   1. blobs/sha256/<layer>
      //   2. <layer>/layer.tar
      //
      // This handles both cases.
      let layerName = layer.endsWith(".tar") ? path.dirname(layer) : path.basename(layer);

      const layerCachePath = path.join(cacheFolder, layerName + "-ptr");
      {
        const layerCacheGzip = file(layerCachePath);
        if (await layerCacheGzip.exists()) {
          const compressedDigest = await layerCacheGzip.text();
          return compressedDigest;
        }
      }

      const inprogressPath = path.join(cacheFolder, layerName + "-in-progress");

      await rm(inprogressPath, { recursive: true });
      const layerCacheGzip = file(inprogressPath, {});

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
import { ReadableLimiter } from "./limiter";

const cred = `Basic ${btoa(`${username}:${password}`)}`;

console.log("Starting push to remote");
// pushLayer accepts the target digest, the stream to read from, and the total layer size.
// It will do the entire push process by itself.
async function pushLayer(layerDigest: string, readableStream: ReadableStream, totalLayerSize: number) {
  const headers = new Headers({
    authorization: cred,
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

  const maxChunkLength = +(createUploadResponse.headers.get("oci-chunk-max-length") ?? 100 * 1024 * 1024);
  if (isNaN(maxChunkLength)) {
    throw new Error(`oci-chunk-max-length header is malformed (not a number)`);
  }

  const reader = readableStream.getReader();
  const uploadId = createUploadResponse.headers.get("docker-upload-uuid");
  if (uploadId === null) {
    throw new Error("Docker-Upload-UUID not defined in headers");
  }

  let location = createUploadResponse.headers.get("location") ?? `/v2${imageRepositoryPath}/blobs/uploads/${uploadId}`;
  const putChunkUploadURL = `${proto}://${imageHost}${location}`;
  const maxToWrite = Math.min(maxChunkLength, totalLayerSize);
  let end = Math.min(maxChunkLength, totalLayerSize);
  let written = 0;
  let previousReadable: ReadableLimiter | undefined;
  let totalLayerSizeLeft = totalLayerSize;
  while (totalLayerSizeLeft > 0) {
    const range = `0-${Math.min(end, totalLayerSize) - 1}`;
    const current = new ReadableLimiter(reader as ReadableStreamDefaultReader, maxToWrite, previousReadable);

    // we have to do fetchNode because Bun doesn't allow setting custom Content-Length.
    // https://github.com/oven-sh/bun/issues/10507
    const putChunkResult = await fetchNode(putChunkUploadURL, {
      method: "PATCH",
      body: current,
      headers: new Headers({
        "range": range,
        "authorization": cred,
        "content-length": `${Math.min(totalLayerSizeLeft, maxToWrite)}`,
      }),
    });
    if (!putChunkResult.ok) {
      throw new Error(
        `uploading chunk ${putChunkUploadURL} returned ${putChunkResult.status}: ${await putChunkResult.text()}`,
      );
    }

    const rangeResponse = putChunkResult.headers.get("range");
    if (rangeResponse !== range) {
      throw new Error(`unexpected Range header ${rangeResponse}, expected ${range}`);
    }

    previousReadable = current;
    totalLayerSizeLeft -= previousReadable.written;
    written += previousReadable.written;
    end += previousReadable.written;
    location = putChunkResult.headers.get("location") ?? location;
    if (totalLayerSizeLeft != 0) console.log(layerDigest + ":", totalLayerSizeLeft, "upload bytes left.");
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

  console.log("Pushed", layerDigest);
}

const layersManifest = [] as {
  readonly mediaType: "application/vnd.oci.image.layer.v1.tar+gzip";
  readonly size: number;
  readonly digest: `sha256:${string}`;
}[];

for (const compressedDigest of compressedDigests) {
  let layer = file(path.join(cacheFolder, compressedDigest));
  layersManifest.push({
    mediaType: "application/vnd.oci.image.layer.v1.tar+gzip",
    size: layer.size,
    digest: `sha256:${compressedDigest}`,
  } as const);
  pushTasks.push(
    pool(async () => {
      const maxRetries = +(process.env["MAX_RETRIES"] ?? 3);
      if (isNaN(maxRetries)) throw new Error("MAX_RETRIES is not a number");

      for (let i = 0; i < maxRetries; i++) {
        const digest = `sha256:${compressedDigest}`;
        const stream = layer.stream();
        try {
          await pushLayer(digest, stream, layer.size);
          return;
        } catch (err) {
          console.error(digest, "failed to upload", maxRetries - i - 1, "left...", err);
          layer = file(path.join(cacheFolder, compressedDigest));
        }
      }
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

const promises = await Promise.allSettled(pushTasks);
for (const promise of promises) {
  if (promise.status === "rejected") {
    throw promise.reason;
  }
}

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
    "authorization": cred,
    "content-type": manifestObject.mediaType,
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
