import { SignJWT, jwtVerify } from "jose"
import { env } from "../config"

const secret = new TextEncoder().encode(env.JWT_SECRET)

export async function sign(payload: { userId: string| undefined; eventId: string }) {
  return new SignJWT(payload)
    .setProtectedHeader({ alg: "HS256" })
    .setExpirationTime("5m")
    .sign(secret)
}

export async function verify(token: string) {
  const { payload } = await jwtVerify(token, secret)
  return payload as { userId: string; eventId: string }
}
