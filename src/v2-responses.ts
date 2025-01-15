export const ManifestTagsListTooBigError = {
  errors: [
    {
      code: "MANIFEST_TAGS",
      message: "a lot of manifest tags, use the Link header to keep iterating",
      detail: "This error may be returned when deleting a manifest by hash and there is too many tags",
    },
  ],
};

export class RegistryResponse extends Response {
  constructor(body?: BodyInit | null, init?: ResponseInit) {
    super(body, {
      ...init,
      headers: {
        ...init?.headers,
        "Docker-Distribution-Api-Version": "registry/2.0",
      },
    });
  }
}
export class RegistryResponseJSON extends RegistryResponse {
  constructor(body?: BodyInit | null, init?: ResponseInit) {
    super(body, {
      ...init,
      headers: {
        ...init?.headers,
        "Content-Type": "application/json",
      },
    });
  }
}
export const DigestInvalid = (expected: string, got: string) => ({
  errors: [
    {
      code: "DIGEST_INVALID",
      message: "digests don't match",
      detail: {
        Expected: expected,
        Got: got,
      },
    },
  ],
});
