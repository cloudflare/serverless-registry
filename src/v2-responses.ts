export const ManifestTagsListTooBigError = {
  errors: [
    {
      code: "MANIFEST_TAGS",
      message: "a lot of manifest tags, use the Link header to keep iterating",
      detail: "This error may be returned when deleting a manifest by hash and there is too many tags",
    },
  ],
};
