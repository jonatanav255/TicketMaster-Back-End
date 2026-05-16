import { Router } from "express"
import { z } from "zod"
import { join, getPosition } from "./service"

export const queueRoutes = Router()

const joinBody = z.object({ userId: z.string().min(1) })
const positionQuery = z.object({ userId: z.string().min(1) })

queueRoutes.post("/:eventId/join", async (req, res) => {
  const { eventId } = req.params
  const { userId } = joinBody.parse(req.body)
  const result = await join(eventId, userId)
  res.json(result)
})

queueRoutes.get("/:eventId/position", async (req, res) => {
  const { eventId } = req.params
  const { userId } = positionQuery.parse(req.query)
  const result = await getPosition(eventId, userId)
  res.json(result)
})
