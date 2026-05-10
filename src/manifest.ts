import { z } from "zod";

const dockerManifestListContentType = "application/vnd.docker.distribution.manifest.list.v2+json";

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
  size: z.number().int(),
  annotations: z.record(z.string()).optional(),
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
    annotations: z.record(z.string()).optional(),
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
      .and(z.record(z.unknown())),
  )
  .or(
    z
      .object({
        schemaVersion: z.literal(2),
        artifactType: z.string().optional(),
        mediaType: z.string(),
        annotations: z.record(z.string()).optional(),
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
            code: z.ZodIssueCode.custom,
            path: ["manifests", index, "platform"],
            message: "platform is required for docker manifest lists",
          });
        });
      }),
  );

export type ManifestSchema = z.infer<typeof manifestSchema>;
