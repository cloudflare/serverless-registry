import { z } from "zod";

// https://github.com/opencontainers/image-spec/blob/main/manifest.md
export const manifestSchema = z.object({
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
});

export type ManifestSchema = z.infer<typeof manifestSchema>;
