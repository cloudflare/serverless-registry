import { Authenticator, AuthenticatorCheckCredentialsResponse, stripUsernamePasswordFromHeader } from "./auth";
import { errorString } from "./utils";
import { RegistryTokens } from "./token";
import type { RegistryTokenCapability } from "./auth";

export const SHA256_PREFIX = "sha256";
export const SHA256_PREFIX_LEN = SHA256_PREFIX.length + 1; // add ":"

export function hexToDigest(sha256: ArrayBuffer, prefix: string = SHA256_PREFIX + ":") {
  const digest = [...new Uint8Array(sha256)].map((b) => b.toString(16).padStart(2, "0")).join("");

  return `${prefix}${digest}`;
}

function stringToArrayBuffer(s: string): ArrayBuffer {
  const encoder = new TextEncoder();
  const arr = encoder.encode(s);
  return arr;
}

export async function getSHA256(data: string, prefix: string = SHA256_PREFIX + ":"): Promise<string> {
  const sha256 = new crypto.DigestStream("SHA-256");
  const w = sha256.getWriter();
  const encoder = new TextEncoder();
  const arr = encoder.encode(data);
  w.write(arr);
  w.close();
  return hexToDigest(await sha256.digest, prefix);
}

export type AuthenticatorCredentials = {
  username: string;
  password: string;
  capabilities: RegistryTokenCapability[];
};

export class UserAuthenticator implements Authenticator {
  authmode: string;
  constructor(private credentials: AuthenticatorCredentials[]) {
    this.authmode = "UserAuthenticator";
  }

  async checkCredentials(r: Request): Promise<AuthenticatorCheckCredentialsResponse> {
    const res = stripUsernamePasswordFromHeader(r);
    if ("verified" in res) {
      return res;
    }

    const [username, password] = res;

    const credential = this.credentials.find((c) => c.username === username);
    if (!credential) {
      return { verified: false, payload: null };
    }

    try {
      if (!crypto.subtle.timingSafeEqual(stringToArrayBuffer(username), stringToArrayBuffer(credential.username))) {
        return { verified: false, payload: null };
      }

      if (!crypto.subtle.timingSafeEqual(stringToArrayBuffer(password), stringToArrayBuffer(credential.password))) {
        return { verified: false, payload: null };
      }
    } catch (err) {
      console.error(`Failed authentication timingSafeEqual: ${errorString(err)}`);
      return { verified: false, payload: null };
    }

    const payload = {
      username,
      capabilities: credential.capabilities,
      exp: Date.now() + 60 * 60,
      aud: "",
    };

    return RegistryTokens.verifyPayload(r, payload);
  }
}
