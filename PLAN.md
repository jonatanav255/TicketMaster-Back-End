# Virtual Waiting Room — Back-End Build Guide

**Stack:** Express + TypeScript, Redis, native `ws` library for WebSocket. **No** Postgres, **no** Kafka, **no** Elasticsearch.

This is a study guide, not a code spec. Each step explains *what you're building* and *why it matters*. You write the code yourself; come back if you want a refresher on the "why."

---

## What you'll have at the end

A back-end with **three running processes** and **one Redis container**:

1. **`api`** (Express) — answers HTTP requests and accepts WebSocket connections.
2. **`worker`** (plain Node script) — pops users from the queue at a fixed rate and signs admission tokens.
3. **`redis`** (Docker) — stores the queue itself and carries pub/sub messages between worker and api.

End-to-end story: a user joins the queue → they see their position tick down live over WebSocket → when their turn comes, they receive a signed JWT → they hand the JWT to a protected endpoint, which lets them in.

---

## The big picture (read this first, refer back often)

```
   ┌──────────┐  POST /queue/:eventId/join         ┌──────────────┐
   │          │ ─────────────────────────────────► │              │
   │  Browser │                                    │   Express    │
   │          │  WS /ws/queue/:eventId/:userId     │     (api)    │
   │          │ ◄════════════════════════════════► │              │
   │          │                                    │              │
   │          │  GET /admitted/hello + JWT         │              │
   │          │ ─────────────────────────────────► │              │
   └──────────┘                                    └──────┬───────┘
                                                          │  ZADD / ZRANK
                                                          │  (commands)
                                                          ▼
                                                   ┌──────────────┐
                                                   │    Redis     │
                                                   │   sorted set │
                                                   │   + pub/sub  │
                                                   └──────┬───────┘
                                                          ▲
                                                          │  ZPOPMIN @ N/sec
                                                          │  PUBLISH token
                                                   ┌──────┴───────┐
                                                   │   Worker     │
                                                   │  (Node loop) │
                                                   └──────────────┘
```

Keep this picture in your head. Every step below is a piece of it.

---

## Phase A — Foundation

You're setting up an empty TypeScript project that can run Express. No queue logic yet.

### Step 1 — `package.json` + `tsconfig.json`
**What:** Declare a TypeScript Node project. Add `express`, `@types/express`, `typescript`, `tsx`, `pino`, `zod`.
**Why:** `tsx` lets you run `.ts` files directly without a build step — critical for fast iteration. `pino` is the structured logger we'll use everywhere. `zod` is for validating env vars and (later) request bodies.
**Watch out for:** Use `"type": "module"` so modern `import` syntax works. With Express on ESM you'll need `import express from 'express'`, not `const express = require(...)`.

### Step 2 — `.gitignore` + `.env.example`
**What:** Ignore `node_modules`, `dist`, `.env`. The `.env.example` documents the keys you'll need: `PORT`, `REDIS_URL`, `JWT_SECRET`, `ADMIT_PER_SECOND`.
**Why:** Anyone (including future-you) can clone the repo and see exactly which env vars are required.

### Step 3 — `docker-compose.yml` (Redis only)
**What:** One service: `redis:7`. Map port 6379. No volumes (queue can be ephemeral for now).
**Why:** Redis is the only piece of infra you need. Postgres and Kafka stay deliberately absent — they're not part of this slice.
**Watch out for:** Once up, sanity check with `docker exec -it <name> redis-cli PING` → `PONG`. If that fails, nothing else will work.

### Step 4 — `src/config.ts`
**What:** Read `process.env`, validate it with zod, export a typed `config` object.
**Why:** Crashes at startup instead of in the middle of a request when an env var is missing. Zero `process.env` references anywhere else — everything reads `config`.

### Step 5 — `src/shared/logger.ts`
**What:** Create a Pino logger. Export it.
**Why:** Structured JSON logs from day one. Every later module imports this one logger.

### Step 6 — `src/shared/redis.ts`
**What:** Create **two** ioredis clients: `redis` (for commands like ZADD) and `redisSub` (for subscribing to channels). Export both.
**Why:** **This is the key Redis pattern to internalize.** A Redis connection that is subscribed to a channel cannot issue regular commands — that's a Redis protocol-level rule. So you need a second connection for commands. Every Node app that does pub/sub follows this two-client pattern.

### Step 7 — `src/server.ts`
**What:** Boot Express. One route: `GET /health` returns `{ ok: true }`. Listen on `config.PORT`.
**Why:** Proves the wiring (config + logger + Express) works before any business logic is added.

**Phase A demo:** `pnpm dev` + `curl localhost:3000/health` → `{ ok: true }`. Redis is up but unused. **Stop here, make sure it's all working before moving on.**

---

## Phase B — The Queue (Redis sorted set patterns)

Now you implement the queue itself. The whole pattern hinges on understanding **why Redis sorted sets are the right tool**.

### Step 8 — `src/queue/keys.ts`
**What:** A helper that returns `waitroom:event:${eventId}`.
**Why:** Centralizing key naming prevents typo bugs and makes it trivial to add a prefix later (e.g., `prod:` vs `dev:`).

### Step 9 — `src/queue/service.ts` — `join(eventId, userId)`
**What:** Run `ZADD <key> NX <timestamp-ms> <userId>`, then `ZRANK <key> <userId>` to get position.
**Why this matters (deeply):**
- `ZADD` with `NX` is **atomic and idempotent**: if the user is already in the queue, score doesn't change, they keep their position. No double-enqueue bugs.
- `ZRANK` returns the user's position in O(log N), which means the queue can have a million users and answering "what position am I?" is still microseconds.
- Score = timestamp gives you natural FIFO ordering. No counter, no race condition.
**Watch out for:** `ZRANK` is **0-indexed**. Position 0 is the front of the line. Decide whether your API returns 0-indexed ("you're at position 0") or 1-indexed ("you're #1"). Pick one and document it.

### Step 10 — `src/queue/service.ts` — `getPosition(eventId, userId)`
**What:** Single `ZRANK` call. If `null`, the user isn't in the queue (already admitted or never joined).
**Why:** This is the fallback the front-end calls when its WebSocket drops. Without it, a disconnect = lost user.

### Step 11 — `src/queue/routes.ts`
**What:** `POST /api/v1/queue/:eventId/join` and `GET /api/v1/queue/:eventId/position`. Validate body/query with zod. Call the service. Return JSON.
**Why:** Thin route layer — all logic stays in `service.ts`. Easy to test the service independently.

**Phase B demo:** Curl `POST /queue/event-1/join` three times with different userIds. Use `redis-cli ZRANGE waitroom:event:event-1 0 -1 WITHSCORES` to see all three in order. Curl `GET /queue/event-1/position?userId=u1` → position 0.

---

## Phase C — The Admission Worker (rate limiting + capability tokens)

This is the heart of the slice. Take your time on these steps.

### Step 12 — `src/admission/jwt.ts`
**What:** Two functions using the `jose` library: `sign({ userId, eventId })` returns a JWT string with a 5-minute expiry; `verify(token)` returns `{ userId, eventId }` or throws.
**Why this matters:** This is the **capability token** pattern. The same shape powers:
- Password reset links ("this link lets you reset *this* account for the next 30 min").
- Magic login links.
- S3 presigned URLs.
- GitHub deploy tokens.
- Stripe webhook signatures (similar idea, different mechanism).
Understanding it once = understanding all of them.
**Watch out for:**
- Use **HS256** (symmetric) for this learning slice. RS256 (asymmetric) is for cross-service trust boundaries.
- The `exp` claim is in **seconds**, not milliseconds. Mixing them up is a classic bug.
- Always include `eventId` in the payload — it scopes the token. A token for event-1 should not work on event-2.

### Step 13 — `src/worker/admit.ts`
**What:** A separate Node entry point. On startup: `setInterval(tick, 1000 / config.ADMIT_PER_SECOND)`. On each tick: `ZPOPMIN <queue-key>` to get the next user, sign a JWT, `PUBLISH admission '{userId, eventId, token}'`.
**Why this matters (deeply):**
- `ZPOPMIN` is **atomic**: even if two worker instances ran (we won't, but in principle), the same user can't be popped twice.
- The worker is a **leaky bucket**: queue can fill up at any rate (millions of joins/sec), but the worker drains at exactly N/sec, protecting whatever's downstream.
- The worker and api are **separate processes** that share state *only* through Redis. This is microservices in miniature: no in-process function calls, just messages and shared state.
**Watch out for:**
- If the worker crashes between `ZPOPMIN` and `PUBLISH`, that user is gone from the queue but never got their token. For MVP, document this as a known limitation. For production, you'd use a "pending admissions" set as a safety net.
- Make sure the worker process exits cleanly on `SIGINT` (Ctrl-C) so you don't accumulate zombie processes during development.

### Step 14 — `package.json` script: `worker`
**What:** Add `"worker": "tsx src/worker/admit.ts"`.
**Why:** You'll run `pnpm dev` (api) and `pnpm worker` (admission worker) in two terminals. They're independent processes — that's the point.

**Phase C demo:** With api + worker both running, fill the queue with 5 users. In a third terminal, `redis-cli SUBSCRIBE admission` — you'll see published tokens flow by, one every `1000 / ADMIT_PER_SECOND` ms. The queue (`ZCARD`) drops to zero. **This is the leaky bucket in action.**

---

## Phase D — Protected Endpoint (proving the token works)

### Step 15 — `src/admission/middleware.ts`
**What:** Express middleware that reads `Authorization: Bearer <token>`, calls `verify()`, attaches `req.userId` and `req.eventId`, calls `next()`. On any failure: respond 401.
**Why:** Every protected endpoint stays clean — no JWT logic in the route handlers. This is the **classic gateway/middleware pattern**: cross-cutting concerns (auth, logging, rate limits) live in middleware, not in business logic.
**Watch out for:**
- Use **constant-time comparison** for the signature check (the `jose` library handles this for you — don't be tempted to hand-roll it).
- Distinguish between "missing token" (no `Authorization` header) and "invalid token" — both return 401 but log differently.

### Step 16 — `src/admission/routes.ts`
**What:** `GET /api/v1/admitted/hello` with the middleware applied. Returns `{ ok: true, userId, eventId }`.
**Why:** It's the simplest possible protected resource. Once this works, *anything* could go behind it — the booking flow, the seat map, anything. That's the power of the capability-token pattern: the protected resource doesn't care how the user got there, only that they hold a valid token.

**Phase D demo:**
- Take a token from Phase C's `SUBSCRIBE` output.
- `curl -H "Authorization: Bearer <token>" /admitted/hello` → 200 with user info.
- Without the header → 401.
- With a deliberately tampered token (change one character) → 401.
- Wait 5 minutes, try again → 401 (expired).

---

## Phase E — WebSocket Live Updates (cross-process pub/sub)

The final piece. The waiting room user shouldn't be polling — they should *see* their position move.

### Step 17 — `src/ws/connections.ts`
**What:** A `Map<string, WebSocket>` keyed by `userId` (or `${eventId}:${userId}` to be safe). Export `add()`, `remove()`, `get()`.
**Why:** This is the api process's in-memory mapping from user IDs to live WS sockets. When a pub/sub admission message arrives for `user-abc`, we look up which WS to push it to.
**Watch out for:** **This map is per-process.** If you ran two api instances, they'd each have their own map and admissions could be routed to the wrong one. For a single api process this is fine. (In production, you'd either pin users to instances or have *every* instance subscribe and only push if the user is in their local map.)

### Step 18 — `src/ws/pubsub.ts`
**What:** Subscribe `redisSub` to the `admission` channel. On message, parse the JSON, look up the WS in the connections map, push `{ type: "admitted", token }` to that socket, then close the socket.
**Why:** This is where **pub/sub fan-out** happens. The worker doesn't know which api process has the user's socket — it just publishes. Every api process gets the message and only the one with the matching socket acts.

### Step 19 — `src/ws/server.ts`
**What:** Use the `ws` library's `WebSocketServer`. On upgrade for path `/ws/queue/:eventId/:userId`: parse the params, register the socket in connections, start a `setInterval` to push `{ type: "position", position, etaSeconds }` every 2 seconds via `getPosition`. On close: remove from connections, stop the interval.
**Why:**
- The 2-second polling on the server side (with Redis `ZRANK`) is *much* cheaper than the client polling over HTTP — same number of Redis calls, but no HTTP overhead and no flicker.
- `etaSeconds = position / ADMIT_PER_SECOND` — a rough estimate, perfectly good for UX.
**Watch out for:**
- Always `clearInterval` on socket close. Otherwise you accumulate timers and slowly leak memory.
- If `ZRANK` returns `null`, the user has been admitted but the WS is still open. Push `{ type: "admitted" }` proactively or wait for pub/sub to deliver it — whichever simplifies your code.

**Phase E demo:** Use `websocat` (or a 10-line Node script) to open `ws://localhost:3000/ws/queue/event-1/user-abc`. After joining the queue, you'll see `position` messages every 2s. Run the worker; you'll see your position drop. When you reach the front, you receive `{ type: "admitted", token: "..." }` and the socket closes.

---

## Phase F — Wrap-up

### Step 20 — `README.md`
**What:** A "demo in 60 seconds" section showing: `docker compose up redis`, two terminals with api/worker, three curls, a `websocat` command.
**Why:** When you come back to this in 3 months, you'll thank past-you.

### Step 21 — `seed.ts` (optional but recommended)
**What:** A script that adds 100 fake users to the queue.
**Why:** Without it, your front-end shows "you're #1" and you don't see anything interesting. With it, you're #87 and you can watch the number drop dramatically.

### Step 22 — `PATTERNS.md`
**What:** One paragraph per pattern you implemented, with file links:
1. Redis sorted sets as a queue with position lookup.
2. Two-client Redis pattern (commands + pub/sub).
3. Pub/sub for cross-process fan-out.
4. Capability tokens (signed JWT).
5. Leaky bucket admission.
6. WebSocket with REST fallback for recovery.
**Why:** Writing them down is how you check whether you actually understood each one. If you can't write one paragraph, you don't understand it yet — go re-read the relevant step.

---

## Definition of done

1. ✅ `POST /queue/event-1/join` returns a position from Redis.
2. ✅ The admission worker drains the queue at exactly `ADMIT_PER_SECOND`.
3. ✅ A WebSocket client sees position decrease in real time.
4. ✅ When admitted, the client receives a JWT, then the socket closes.
5. ✅ `GET /admitted/hello` accepts the JWT and rejects forgeries/expired tokens.
6. ✅ `PATTERNS.md` exists, with a paragraph for each pattern, written in your own words.

When all six tick, the back-end slice is done.

---

## Explicitly out of scope

- Postgres, Kafka, Debezium, Elasticsearch — none of them.
- Real authentication. `userId` is whatever the client sends. Anyone can claim to be anyone.
- Booking flow, seat maps, payments — that's a *different* slice for a future day.
- Load testing, metrics dashboards, multi-region deploy.
- Multi-event optimizations. Hard-code `event-1` if it simplifies your code.

These are not abandoned ideas — they're future work. Listing them here keeps us from drifting into them.
