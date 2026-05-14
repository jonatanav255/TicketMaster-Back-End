import { z } from "zod"

const schema = z.object({
  PORT: z.coerce.number().default(3000),
  REDIS_URL: z.url().startsWith("redis://"),
  JWT_SECRET: z.string().min(32),
  ADMIT_PER_SECOND: z.coerce.number().int().positive().default(1),
})

export const env = schema.parse(process.env)