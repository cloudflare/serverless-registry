import { State } from "./registry/r2";

export class AuthErrorResponse extends Response {
  constructor(r: Request) {
    const jsonBody = JSON.stringify({
      errors: [
        {
          code: "UNAUTHORIZED",
          message: "authentication required",
          detail: null,
        },
      ],
    });
    const init = {
      status: 401,
      headers: {
        "content-type": "application/json;charset=UTF-8",
        "WWW-Authenticate": `Basic realm="${r.url}"`,
      },
    };
    super(jsonBody, init);
  }
}

export class RangeError extends Response {
  constructor(stateHash: string, state: State) {
    super(
      JSON.stringify({
        errors: [
          {
            code: "RANGE_ERROR",
            message: `stateHash ${stateHash} does not match state (upload id: ${state.registryUploadId})`,
            detail: {
              ...state,
              string: stateHash,
            },
          },
        ],
      }),
      {
        status: 416,
        headers: {
          "Location": `/v2/${state.name}/blobs/uploads/${state.registryUploadId}?_stateHash=${stateHash}`,
          "Range": `0-${state.byteRange - 1}`,
          "Docker-Upload-UUID": state.registryUploadId,
        },
      },
    );
  }
}

export class InternalError extends Response {
  constructor() {
    const jsonBody = JSON.stringify({
      errors: [
        {
          code: "INTERNAL_ERROR",
          message: "internal error",
          detail: null,
        },
      ],
    });
    const init = {
      status: 500,
      headers: {
        "content-type": "application/json;charset=UTF-8",
      },
    };
    super(jsonBody, init);
  }
}

export class ManifestError extends Response {
  constructor(
    code: "MANIFEST_INVALID" | "BLOB_UNKNOWN" | "MANIFEST_UNVERIFIED" | "TAG_INVALID" | "NAME_INVALID",
    message: string,
    detail: Record<string, string> = {},
  ) {
    const jsonBody = JSON.stringify({
      errors: [
        {
          code,
          message,
          detail,
        },
      ],
    });
    super(jsonBody, {
      status: 400,
      headers: {
        "content-type": "application/json;charset=UTF-8",
      },
    });
  }
}

export class ServerError extends Response {
  constructor(message: string, errorCode = 500) {
    super(JSON.stringify({ errors: [{ code: "SERVER_ERROR", message, detail: null }] }), {
      status: errorCode,
      headers: { "content-type": "application/json;charset=UTF-8" },
    });
  }
}
