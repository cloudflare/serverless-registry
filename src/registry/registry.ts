import { Env } from "../..";
import { InternalError } from "../errors";
import { errorString } from "../utils";
import z from "zod";
import { GarbageCollectionMode } from "./garbage-collector";

// Defines a registry and how it's configured
const registryConfiguration = z.object({
  registry: z.string().url(),
  password_env: z.string(),
  username: z.string(),
});

export type RegistryConfiguration = z.infer<typeof registryConfiguration>;

export function registries(env: Env): RegistryConfiguration[] {
  if (env.REGISTRIES_JSON === undefined || env.REGISTRIES_JSON.length === 0) {
    return [];
  }

  try {
    const jsonObject = JSON.parse(env.REGISTRIES_JSON);
    return registryConfiguration.array().parse(jsonObject);
  } catch (err) {
    console.error("Error parsing registries JSON: " + errorString(err));
    return [];
  }
}

// Registry error contains an HTTP response that is returned by the underlying registry implementation
export type RegistryError = {
  response: Response;
};

// Response of manifestExists call
export type CheckManifestResponse =
  | {
      exists: true;
      size: number;
      digest: string;
      contentType: string;
    }
  | {
      exists: false;
    };

export type ListRepositoriesResponse = {
  repositories: string[];
  cursor?: string;
};

// Response layerExists call
export type CheckLayerResponse =
  | {
      exists: true;
      size: number;
      digest: string;
    }
  | {
      exists: false;
    };

// represents an upload transaction id
export type UploadId = string;

// upload object to use to continue an upload
export type UploadObject = {
  id: UploadId;
  location: string;
  // inclusive content range
  range: [number, number];

  minimumBytesPerChunk?: number;
  maximumBytesPerChunk?: number;
};

// response when you finish an upload
export type FinishedUploadObject = {
  digest: string;
  location: string;
};

// returned by getManifest when it successfully retrieves a manifest
export type GetManifestResponse = {
  stream: ReadableStream;
  digest: string;
  size: number;
  contentType: string;
};

// returned by getLayer when it successfully retrieves a layer
export type GetLayerResponse = {
  stream: ReadableStream;
  digest: string;
  size: number;
};

// returned by putManifest when it successfully uploads a manifest
export type PutManifestResponse = {
  digest: string;
  location: string;
};

// Registry interface to an implementation
export interface Registry {
  // All read operations supported by a registry

  // checks whether the manifest exists in the registry
  manifestExists(namespace: string, tag: string): Promise<CheckManifestResponse | RegistryError>;

  // listing repositories in the registry
  listRepositories(limit?: number, last?: string): Promise<ListRepositoriesResponse | RegistryError>;

  // gets the manifest by namespace + digest
  getManifest(namespace: string, digest: string): Promise<GetManifestResponse | RegistryError>;

  // checks that a layer exists
  layerExists(namespace: string, digest: string): Promise<CheckLayerResponse | RegistryError>;

  // get a layer stream from the registry
  getLayer(namespace: string, digest: string): Promise<GetLayerResponse | RegistryError>;

  // put manifest uploads a manifest into the registry
  putManifest(
    namespace: string,
    reference: string,
    readableStream: ReadableStream<any>,
    contentType: string,
  ): Promise<PutManifestResponse | RegistryError>;

  // starts a new upload
  startUpload(namespace: string): Promise<UploadObject | RegistryError>;

  // cancels an upload
  cancelUpload(namespace: string, uploadId: UploadId): Promise<true | RegistryError>;

  // gets an existing upload
  getUpload(namespace: string, uploadId: UploadId): Promise<UploadObject | RegistryError>;

  // does a monolithic upload. if it returns false it means that the registry doesn't
  // support it and the caller should try to fallback to chunked upload
  monolithicUpload(
    namespace: string,
    expectedSha: string,
    stream: ReadableStream,
    // For a more optimal upload
    size?: number,
  ): Promise<FinishedUploadObject | RegistryError | false>;

  // uploads a chunk
  uploadChunk(
    namespace: string,
    uploadId: string,
    location: string,
    stream: ReadableStream,
    // for a more optimal upload. Some clients might require it
    length?: number,
    range?: [number, number] | undefined,
  ): Promise<UploadObject | RegistryError>;

  // finishes an upload
  finishUpload(
    namespace: string,
    uploadId: string,
    location: string,
    expectedDigest: string,
    stream?: ReadableStream,
    length?: number,
  ): Promise<FinishedUploadObject | RegistryError>;

  garbageCollection(namespace: string, mode: GarbageCollectionMode): Promise<boolean>;
}

export function wrapError(method: string, err: unknown): RegistryError {
  console.error(method, "error:", errorString(err));
  return {
    response: new InternalError(),
  };
}
