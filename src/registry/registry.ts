import { Env } from "../..";
import { errorString } from "../utils";
import z from "zod";

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
  range: [number, number];
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

// Registry interface to an implementation
export interface Registry {
  // All read operations supported by a registry

  // checks whether the manifest exists in the registry
  manifestExists(namespace: string, tag: string): Promise<CheckManifestResponse | RegistryError>;

  // gets the manifest by namespace + digest
  getManifest(namespace: string, digest: string): Promise<GetManifestResponse | RegistryError>;

  // checks that a layer exists
  layerExists(namespace: string, digest: string): Promise<CheckLayerResponse | RegistryError>;

  // get a layer stream from the registry
  getLayer(namespace: string, digest: string): Promise<GetLayerResponse | RegistryError>;

  // starts a new upload
  startUpload(namespace: string): Promise<UploadObject | RegistryError>;

  // does a monolithic upload. if it returns false it means that the registry doesn't
  // support it and the caller should try to fallback to chunked upload
  monolithicUpload(
    namespace: string,
    stream: ReadableStream,
    size: number,
  ): Promise<FinishedUploadObject | RegistryError | false>;

  // uploads a chunk
  uploadChunk(
    uploadObject: UploadObject,
    stream: ReadableStream,
    // for a more optimal upload
    length?: number,
  ): Promise<UploadObject | RegistryError>;

  // finishes an upload
  finishUpload(
    uploadObject: UploadObject,
    stream?: ReadableStream,
    length?: number,
  ): Promise<FinishedUploadObject | RegistryError>;
}
