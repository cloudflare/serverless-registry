export const ManifestUnknownError = {
  errors: [
    {
      code: "MANIFEST_UNKNOWN",
      message: "manifest unknown",
      detail: "This error is returned when the manifest, identified by name and tag is unknown to the repository.",
    },
  ],
};

export const BlobUnknownError = {
  errors: [
    {
      code: "BLOB_UNKNOWN",
      message: "blob unknown to registry",
      detail: "This error may be returned when a manifest blob is unknown to the registry.",
    },
  ],
};
