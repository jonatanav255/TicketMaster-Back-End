import { redis } from "../shared/redis"
import { waitroomKey } from "../queue/keys"
import { sign } from "../admission/jwt"
import { logger } from "../shared/logger"
import { env } from "../config"

const eventId = "event-1"
const intervalMs = 1000 / env.ADMIT_PER_SECOND

async function tick() {
  const popped = await redis.zpopmin(waitroomKey(eventId))
  const [userId] = popped
  if (!userId) return
  const token = await sign({ userId, eventId })
  await redis.publish("admission", JSON.stringify({ userId, eventId, token }))
  logger.info({ userId, eventId }, "admitted")
}

const timer = setInterval(() => {
  tick().catch((err) => logger.error({ err }, "tick failed"))
}, intervalMs)

const shutdown = async () => {
  clearInterval(timer)
  await redis.quit()
  process.exit(0)
}
process.on("SIGINT", shutdown)
process.on("SIGTERM", shutdown)

logger.info({ eventId, intervalMs }, "admission worker started")
