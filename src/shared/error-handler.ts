import type { ErrorRequestHandler } from "express"
import { ZodError } from "zod"
import { logger } from "./logger"

export const errorHandler: ErrorRequestHandler = (err, _req, res, _next) => {
  if (err instanceof ZodError) {
    logger.warn({ issues: err.issues }, "validation failed")
    res.status(400).json({ error: "validation_failed", issues: err.issues })
    return
  }
  logger.error({ err }, "unhandled error")
  res.status(500).json({ error: "internal_error" })
}
