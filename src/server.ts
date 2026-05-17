import express from "express"
import { env } from "./config"
import { logger } from "./shared/logger"
import { errorHandler } from "./shared/error-handler"
import { queueRoutes } from "./queue/routes"

const app = express()
app.use(express.json())

app.get("/health", (_req, res) => {
  res.json({ ok: true })
})

app.use("/api/v1/queue", queueRoutes)

app.use(errorHandler)

app.listen(env.PORT, () => {
  logger.info({ port: env.PORT }, "server listening")
})
