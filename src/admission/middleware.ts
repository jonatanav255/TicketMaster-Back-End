import type { RequestHandler } from "express"
import { verify } from "./jwt"
import { logger } from "../shared/logger"

export const requireAdmission: RequestHandler = async (req, res, next) => {
  const header = req.header("authorization")
  const token = header?.startsWith("Bearer ") ? header.slice(7) : null

  if (!token) {
    res.status(401).json({ error: "missing_token" })
    return
  }

  try {
    const payload = await verify(token)
    ;(req as any).userId = payload.userId
    ;(req as any).eventId = payload.eventId
    next()
  } catch (err) {
    logger.warn({ err }, "admission token rejected")
    res.status(401).json({ error: "invalid_token" })
  }
}
