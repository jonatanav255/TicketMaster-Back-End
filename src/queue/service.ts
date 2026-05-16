import { redis } from "../shared/redis"
import { waitroomKey } from "./keys"

export async function join(eventId: string, userId: string) {
  const key = waitroomKey(eventId)
  await redis.zadd(key, "NX", Date.now(), userId)
  const position = await redis.zrank(key, userId)
  return { position }
}
