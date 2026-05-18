import { SignJWT, jwtVerify } from "jose"
import { z } from "zod"
import { env } from "../config"

const secret = new TextEncoder().encode(env.JWT_SECRET)

const payloadSchema = z.object({
  userId: z.string().min(1),
  eventId: z.string().min(1),
})

export async function sign(payload: { userId: string; eventId: string }) {
  return new SignJWT(payload)
    .setProtectedHeader({ alg: "HS256" })
    .setExpirationTime("5m")
    .sign(secret)
}

export async function verify(token: string) {
  const { payload } = await jwtVerify(token, secret)
  return payloadSchema.parse(payload)
}
