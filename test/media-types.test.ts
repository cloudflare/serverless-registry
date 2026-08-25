import { describe, expect, test } from "vitest";
import { manifestTypes } from "../src/registry/http";
import {
  dockerManifestListContentType,
  ociImageIndexContentType,
  dockerManifestV1PrettyJwsContentType,
  ociImageManifestContentType,
  dockerManifestV2ContentType,
} from "../src/media-types";

describe("manifest media types", () => {
  // The Accept header is emitted as manifestTypes.join(", "), so both the exact string values
  // and the array order are part of the content-negotiation wire contract. This pins them so a
  // value edit or a reorder fails here rather than silently changing what the registry sends.
  test("manifestTypes holds the expected set in the expected order", () => {
    expect(manifestTypes).toEqual([
      "application/vnd.docker.distribution.manifest.list.v2+json",
      "application/vnd.oci.image.index.v1+json",
      "application/vnd.docker.distribution.manifest.v1+prettyjws",
      "application/json",
      "application/vnd.oci.image.manifest.v1+json",
      "application/vnd.docker.distribution.manifest.v2+json",
    ]);
  });

  test("each manifest media-type constant holds its spec string", () => {
    expect(dockerManifestListContentType).toBe("application/vnd.docker.distribution.manifest.list.v2+json");
    expect(ociImageIndexContentType).toBe("application/vnd.oci.image.index.v1+json");
    expect(dockerManifestV1PrettyJwsContentType).toBe("application/vnd.docker.distribution.manifest.v1+prettyjws");
    expect(ociImageManifestContentType).toBe("application/vnd.oci.image.manifest.v1+json");
    expect(dockerManifestV2ContentType).toBe("application/vnd.docker.distribution.manifest.v2+json");
  });
});
