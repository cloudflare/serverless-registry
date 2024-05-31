export const ManifestUnknownError = (tag: string) =>
  ({
    errors: [
      {
        code: "MANIFEST_UNKNOWN",
        message: "manifest unknown",
        detail: {
          Tag: tag,
        },
      },
    ],
  } as const);

export const BlobUnknownError = {
  errors: [
    {
      code: "BLOB_UNKNOWN",
      message: "blob unknown to registry",
      detail: {
        message: "This error may be returned when a layer blob is unknown to the registry.",
      },
    },
  ],
};
