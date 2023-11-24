import {
  CheckLayerResponse,
  CheckManifestResponse,
  FinishedUploadObject,
  GetLayerResponse,
  GetManifestResponse,
  Registry,
  RegistryError,
  UploadObject,
} from "./registry";

export class R2Registry implements Registry {
  manifestExists(namespace: string, tag: string): Promise<RegistryError | CheckManifestResponse> {
    throw new Error("Method not implemented.");
  }
  getManifest(namespace: string, digest: string): Promise<RegistryError | GetManifestResponse> {
    throw new Error("Method not implemented.");
  }
  layerExists(namespace: string, digest: string): Promise<RegistryError | CheckLayerResponse> {
    throw new Error("Method not implemented.");
  }
  getLayer(namespace: string, digest: string): Promise<RegistryError | GetLayerResponse> {
    throw new Error("Method not implemented.");
  }
  startUpload(namespace: string): Promise<RegistryError | UploadObject> {
    throw new Error("Method not implemented.");
  }
  uploadChunk(
    uploadObject: UploadObject,
    stream: ReadableStream<any>,
    length?: number | undefined,
  ): Promise<RegistryError | UploadObject> {
    throw new Error("Method not implemented.");
  }
  finishUpload(
    uploadObject: UploadObject,
    stream?: ReadableStream<any> | undefined,
    length?: number | undefined,
  ): Promise<RegistryError | FinishedUploadObject> {
    throw new Error("Method not implemented.");
  }
  monolithicUpload(
    namespace: string,
    stream: ReadableStream,
    size: number,
  ): Promise<FinishedUploadObject | RegistryError | false> {
    throw new Error("Method not implemented.");
  }
}
