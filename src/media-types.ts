// Named constants for the manifest media types the registry recognizes during content
// negotiation. These strings are the OCI/Docker content-negotiation contract and must stay
// byte-identical to the specification, so each is defined exactly once here and referenced
// everywhere else. This module has no imports, keeping it a safe leaf to import from anywhere.

export const dockerManifestListContentType = "application/vnd.docker.distribution.manifest.list.v2+json";
export const ociImageIndexContentType = "application/vnd.oci.image.index.v1+json";
export const dockerManifestV1PrettyJwsContentType = "application/vnd.docker.distribution.manifest.v1+prettyjws";
export const ociImageManifestContentType = "application/vnd.oci.image.manifest.v1+json";
export const dockerManifestV2ContentType = "application/vnd.docker.distribution.manifest.v2+json";
