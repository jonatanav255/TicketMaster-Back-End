import express from "express"
import { env } from "./config"
import { logger } from "./shared/logger"

const app = express()
app.use(express.json())

app.get("/health", (_req, res) => {
  res.json({ ok: true })
})

app.listen(env.PORT, () => {
  logger.info({ port: env.PORT }, "server listening")
})
