import { z } from "zod";

export const ociImageManifestContentType = "application/vnd.oci.image.manifest.v1+json";
export const ociImageIndexContentType = "application/vnd.oci.image.index.v1+json";
export const dockerImageManifestContentType = "application/vnd.docker.distribution.manifest.v2+json";
export const dockerManifestListContentType = "application/vnd.docker.distribution.manifest.list.v2+json";

const manifestContentTypes: ReadonlySet<string> = new Set([
  ociImageManifestContentType,
  ociImageIndexContentType,
  dockerImageManifestContentType,
  dockerManifestListContentType,
]);

const platformSchema = z.object({
  "architecture": z.string(),
  "os": z.string(),
  "os.features": z.array(z.string()).optional(),
  "os.version": z.string().optional(),
  "variant": z.string().optional(),
  "features": z.array(z.string()).optional(),
});

const descriptorSchema = z.object({
  mediaType: z.string(),
  digest: z.string(),
  size: z.int(),
  annotations: z.record(z.string(), z.string()).optional(),
  artifactType: z.string().optional(),
  urls: z.array(z.string()).optional(),
  data: z.string().optional(),
});

const indexDescriptorSchema = descriptorSchema.extend({
  platform: platformSchema.optional(),
});

// https://github.com/opencontainers/image-spec/blob/main/manifest.md
export const manifestSchema = z
  .object({
    schemaVersion: z.literal(2),
    artifactType: z.string().optional(),
    // to maintain retrocompatibility of the registry, let's not assume mediaTypes
    mediaType: z.string(),
    config: descriptorSchema,
    layers: z.array(descriptorSchema),
    annotations: z.record(z.string(), z.string()).optional(),
    subject: descriptorSchema.optional(),
  })
  .or(
    z
      .object({
        schemaVersion: z.literal(1),
        fsLayers: z.array(z.object({ blobSum: z.string() })),
        architecture: z.string().optional(),
        tag: z.string().optional(),
        name: z.string().optional(),
        history: z.array(z.unknown()).optional(),
        signatures: z.array(z.unknown()).optional(),
      })
      .and(z.record(z.string(), z.unknown())),
  )
  .or(
    z
      .object({
        schemaVersion: z.literal(2),
        artifactType: z.string().optional(),
        mediaType: z.string(),
        annotations: z.record(z.string(), z.string()).optional(),
        subject: descriptorSchema.optional(),
        manifests: z.array(indexDescriptorSchema),
      })
      .superRefine((manifest, ctx) => {
        if (manifest.mediaType !== dockerManifestListContentType) {
          return;
        }

        manifest.manifests.forEach((descriptor, index) => {
          if (descriptor.platform !== undefined) {
            return;
          }

          ctx.addIssue({
            code: "custom",
            path: ["manifests", index, "platform"],
            message: "platform is required for docker manifest lists",
          });
        });
      }),
  );

export type ManifestSchema = z.infer<typeof manifestSchema>;

/**
 * The top-level mediaType is OPTIONAL in the OCI image-spec ("SHOULD be used"), and clients
 * such as Helm omit it. The Content-Type header of the PUT carries the same information, so
 * fill it in from there before validating.
 *
 * Only the parsed object is changed. Manifest bytes are stored verbatim, so the stored object
 * still hashes to the digest the client pushed it under.
 */
export function withInferredMediaType(manifestJSON: unknown, contentType: string): unknown {
  if (typeof manifestJSON !== "object" || manifestJSON === null || Array.isArray(manifestJSON)) {
    return manifestJSON;
  }

  const manifest = manifestJSON as Record<string, unknown>;
  if (manifest.schemaVersion !== 2 || manifest.mediaType !== undefined) {
    return manifestJSON;
  }

  const mediaType = contentType.split(";")[0].trim();
  if (!manifestContentTypes.has(mediaType)) {
    return manifestJSON;
  }

  return { ...manifest, mediaType };
}
