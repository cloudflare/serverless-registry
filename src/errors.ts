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
  constructor(stateStr: string, state: State) {
    super(
      JSON.stringify({
        errors: [
          {
            code: "RANGE_ERROR",
            message: `state ${stateStr} is not satisfiable (upload id: ${state.registryUploadId})`,
            detail: {
              ...state,
              string: stateStr,
            },
          },
        ],
      }),
      {
        status: 416,
        headers: {
          "Location": `/v2/${state.name}/blobs/uploads/${state.registryUploadId}?_state=${stateStr}`,
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

export class ServerError extends Response {
  constructor(message: string, errorCode = 500) {
    super(JSON.stringify({ errors: [{ code: "SERVER_ERROR", message, detail: null }] }), {
      status: errorCode,
      headers: { "content-type": "application/json;charset=UTF-8" },
    });
  }
}
