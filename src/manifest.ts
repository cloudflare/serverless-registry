import { z } from "zod";

// https://github.com/opencontainers/image-spec/blob/main/manifest.md
export const manifestSchema = z
  .object({
    schemaVersion: z.literal(2),
    artifactType: z.string().optional(),
    // to maintain retrocompatibility of the registry, let's not assume mediaTypes
    mediaType: z.string(),
    config: z.object({
      mediaType: z.string(),
      digest: z.string(),
      size: z.number().int(),
    }),
    layers: z.array(
      z.object({
        size: z.number().int(),
        mediaType: z.string(),
        digest: z.string(),
      }),
    ),
    annotations: z.record(z.string()).optional(),
    subject: z
      .object({
        mediaType: z.string(),
        digest: z.string(),
        size: z.number().int(),
      })
      .optional(),
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
  );

export type ManifestSchema = z.infer<typeof manifestSchema>;
