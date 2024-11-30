import { $, CryptoHasher, file, write } from "bun";
import { extract } from "tar";

import stream from "node:stream";
import { mkdir } from "node:fs/promises";

const username = process.env["USERNAME_REGISTRY"];
async function read(stream: stream.Readable): Promise<string> {
  const chunks = [];
  for await (const chunk of stream.iterator()) {
    chunks.push(chunk);
  }

  return Buffer.concat(chunks).toString("utf8");
}

let password = process.env["REGISTRY_JWT_TOKEN"]

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

const installBun = await $`bun install`.quiet();
if (installBun.exitCode !== 0) {
  console.error(
    "Could not install bun",
  );
  process.exit(1);
}


console.log("Preparing image...");
// const imageMetadataRes = await $`/usr/bin/docker images --format "{{ .ID }}" ${image}`;
// if (imageMetadataRes.exitCode !== 0) {
//   console.error(
//     "Image",
//     image,
//     "doesn't exist. The docker daemon might not be running, or the image doesn't exist. Check your existing images with\n\n\tdocker images",
//   );
//   process.exit(1);
// }

// const imageID = imageMetadataRes.text();
// if (imageID === "") {
//   console.error("Image", image, "doesn't exist. Check your existing images with\n\n\tdocker images");
//   process.exit(1);
// }

console.log(`Image ${image} found locally, saving to disk...`);

const tarFile = "output.tar";
const imagePath = ".output-image";
if ((await file(tarFile).exists())) {
  // const output = await $`/usr/bin/docker save ${image} --output ${tarFile}`;

  // if (output.exitCode != 0) {
  //   console.error("Error saving image", image, output.text());
  //   process.exit(1);
  // }

  console.log(`Image saved as ${tarFile}, extracting...`);

  await mkdir(imagePath);

  const result = await extract({
    file: tarFile,
    cwd: imagePath,
  });

  console.log(`Extracted to ${imagePath}`);
} 

type IndexJSONFile = {
  manifests: {
    digest: string,
  }[]
}

const indexJSONFile = (await Bun.file(path.join(imagePath, "index.json")).json()) as IndexJSONFile;
// if (indexJSONFile.manifests.length > 0) {
//   throw new Error('More than one manifest file found')
// }


console.log(indexJSONFile)
let manifestFile = indexJSONFile.manifests[0].digest
manifestFile = manifestFile.replace("sha256:", "")
console.log(manifestFile, "manifest file found");

const manifestBlobFile = file(`${imagePath}/blobs/sha256/${manifestFile}`)
await Bun.write(`${imagePath}/manifest.json`, manifestBlobFile);

// const moveManifestFileRes = await $`cp ${imagePath}/blobs/sha256/${manifestFile} ${imagePath}/manifest.json`
// if (moveManifestFileRes.exitCode !== 0) {
//   throw new Error('Could not move manifest file to manifest.json')
// }

type DockerSaveConfigManifest = {
  config: {
    digest: string;
    size: number;
  };
  layers: {
    digest: string;
  }[];
};

import path from "path";
const manifest = (await Bun.file(path.join(imagePath, "manifest.json")).json()) as DockerSaveConfigManifest;

// if (manifests.length == 0) {
//   console.error("unexpected manifest of length 0");
//   process.exit(1);
// }

// if (manifests.length > 1) {
//   console.warn("Manifest resolved to multiple images, picking the first one");
// }

console.log(manifest, "manifest")

import plimit from "p-limit";
const pool = plimit(5);
import zlib from "node:zlib";
import { rename, rm } from "node:fs/promises";

const cacheFolder = imagePath;

await mkdir(cacheFolder, { recursive: true });

// const [manifest] = manifests;
const tasks = [];

console.log(manifest.layers)
console.log("Compressing...");
// Iterate through every layer, read it and compress to a file
for (const layer of manifest.layers) {
  tasks.push(
    pool(async () => {

      return layer.digest.replace("sha256:", "")
      // let layerPath = path.join(imagePath, layer);
      // // docker likes to put stuff in two ways:
      // //   1. blobs/sha256/<layer>
      // //   2. <layer>/layer.tar
      // //
      // // This handles both cases.
      // let layerName = layer.endsWith(".tar") ? path.dirname(layer) : path.basename(layer);

      // const layerCachePath = path.join(cacheFolder, layerName + "-ptr");
      // {
      //   const layerCacheGzip = file(layerCachePath);
      //   if (await layerCacheGzip.exists()) {
      //     const compressedDigest = await layerCacheGzip.text();
      //     return compressedDigest;
      //   }
      // }

      // const inprogressPath = path.join(cacheFolder, layerName + "-in-progress");
      // await rm(inprogressPath, { recursive: true });

      // const hasher = new Bun.CryptoHasher("sha256");
      // const cacheWriter = file(inprogressPath).writer();
      // const gzipStream = zlib.createGzip({ level: 9 });
      // const writeStream = new stream.Writable({
      //   write(val: Buffer, _, cb) {
      //     hasher.update(val, "binary");
      //     cacheWriter.write(val);
      //     cb();
      //   },
      // });

      // gzipStream.pipe(writeStream);
      // await file(layerPath)
      //   .stream()
      //   .pipeTo(
      //     new WritableStream({
      //       write(value: Buffer) {
      //         return new Promise(async (res) => {
      //           const needsWriteBackoff = gzipStream.write(value);
      //           // We need to back-off with the writes
      //           if (!needsWriteBackoff) {
      //             const onDrain = () => {
      //               // Remove event listener when it finishes
      //               gzipStream.off("drain", onDrain);
      //               res();
      //             };

      //             gzipStream.on("drain", onDrain);
      //             return;
      //           }

      //           res();
      //         });
      //       },
      //       close() {
      //         return new Promise(async (res) => {
      //           // Flush before end
      //           await new Promise((resFlush) => gzipStream.flush(() => resFlush(true)));
      //           // End the stream
      //           gzipStream.end(res);
      //         });
      //       },
      //     }),
      //   );

      // // Wait until the gzipStream has finished all the piping
      // await new Promise((res) => gzipStream.on("end", () => res(true)));

      // const digest = hasher.digest("hex");
      // await rename(inprogressPath, path.join(cacheFolder, digest));
      // await write(layerCachePath, digest);
      // return digest;
    }),
  );
}

// const configManifest = manifest.Config
const config = manifest.config
const configDigest = config.digest.replace("sha256:", "")

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

  function parseLocation(location: string) {
    if (location.startsWith("/")) {
      return `${proto}://${imageHost}${location}`;
    }

    return location;
  }

  let location = createUploadResponse.headers.get("location") ?? `/v2${imageRepositoryPath}/blobs/uploads/${uploadId}`;
  const maxToWrite = Math.min(maxChunkLength, totalLayerSize);
  let end = Math.min(maxChunkLength, totalLayerSize);
  let written = 0;
  let previousReadable: ReadableLimiter | undefined;
  let totalLayerSizeLeft = totalLayerSize;
  while (totalLayerSizeLeft > 0) {
    const range = `0-${Math.min(end, totalLayerSize) - 1}`;
    const current = new ReadableLimiter(reader as ReadableStreamDefaultReader, maxToWrite, previousReadable);
    const patchChunkUploadURL = parseLocation(location);
    // we have to do fetchNode because Bun doesn't allow setting custom Content-Length.
    // https://github.com/oven-sh/bun/issues/10507
    const patchChunkResult = await fetchNode(patchChunkUploadURL, {
      method: "PATCH",
      body: current,
      headers: new Headers({
        "range": range,
        "authorization": cred,
        "content-length": `${Math.min(totalLayerSizeLeft, maxToWrite)}`,
      }),
    });
    if (!patchChunkResult.ok) {
      throw new Error(
        `uploading chunk ${patchChunkUploadURL} returned ${patchChunkResult.status}: ${await patchChunkResult.text()}`,
      );
    }

    const rangeResponse = patchChunkResult.headers.get("range");
    if (rangeResponse !== range) {
      throw new Error(`unexpected Range header ${rangeResponse}, expected ${range}`);
    }

    previousReadable = current;
    totalLayerSizeLeft -= previousReadable.written;
    written += previousReadable.written;
    end += previousReadable.written;
    location = patchChunkResult.headers.get("location") ?? location;
    if (totalLayerSizeLeft != 0) console.log(layerDigest + ":", totalLayerSizeLeft, "upload bytes left.");
  }

  const range = `0-${written - 1}`;
  const uploadURL = new URL(parseLocation(location));
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
  let layer = file(`${imagePath}/blobs/sha256/${compressedDigest}`);
  layersManifest.push({
    mediaType: "application/vnd.oci.image.layer.v1.tar+gzip",
    size: layer.size,
    digest: `sha256:${compressedDigest}`,
  } as const);
  pushTasks.push(
    pool(async () => {
      const maxRetries = +(process.env["MAX_RETRIES"] ?? 8);
      if (isNaN(maxRetries)) throw new Error("MAX_RETRIES is not a number");

      for (let i = 0; i < maxRetries; i++) {
        const digest = `sha256:${compressedDigest}`;
        const stream = layer.stream();
        try {
          await pushLayer(digest, stream, layer.size);
          return;
        } catch (err) {
          console.error(digest, "failed to upload", maxRetries - i - 1, "left...", err);
          layer = file(`${imagePath}/blobs/sha256/${compressedDigest}`);
        }
      }
    }),
  );
}

pushTasks.push(
  pool(async () => {
    const maxRetries = +(process.env["MAX_RETRIES"] ?? 8);
    if (isNaN(maxRetries)) throw new Error("MAX_RETRIES is not a number");

    for (let i = 0; i < maxRetries; i++) {
      let configLayer = file(`${imagePath}/blobs/sha256/${configDigest}`);
      const stream = configLayer.stream();
      const digest = `sha256:${configDigest}`;
      try {
        await pushLayer(digest, stream, configLayer.size);
        return;
      } catch (err) {
        console.error(digest, "failed to upload", maxRetries - i - 1, "left...", err);
        configLayer = file(`${imagePath}/blobs/sha256/${configDigest}`);
      }
    }
  }),
);


// pushTasks.push(
//   pool(async () => {
//     await pushLayer(
//       `sha256:${configDigest}`,
//       new ReadableStream({
//         pull(controller) {
//           controller.enqueue(config);
//           controller.close();
//         },
//       }),
//       config.size,
//     );
//   }),
// );

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
    size: config.size,
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