# Ticketmaster Clone — Back-End Implementation Plan

**Stack:** Node.js 20 + TypeScript, PostgreSQL, Redis, Elasticsearch, Kafka (Redpanda for local), Docker Compose.
**Layout strategy:** Monolith-first. Single Node app exposing all routes, internally organized into "service modules" with the boundaries each microservice would have. Extract to separate processes only when the patterns demand it (Phase 4 WebSocket, Phase 7 outbox worker).

The goal is to learn distributed-systems patterns by building them, not to ship a polished product. Each phase delivers working software you can demo.

---

## 0. Guiding Principles

- **Patterns over polish.** Every phase must visibly demonstrate the pattern it introduces (lock contention, CDC lag, pub/sub fan-out, etc.). Write a short note in `PATTERNS.md` as you implement each one.
- **No ORM magic.** Use a thin query builder (Kysely or raw `pg`). You should be able to read every SQL query you emit.
- **Idempotency from day one.** Every mutating endpoint accepts `Idempotency-Key`. Cheap to add early, painful to retrofit.
- **Correlation IDs everywhere.** `X-Request-ID` propagates through HTTP → Kafka → logs.
- **Test the concurrency story.** A failing concurrent-reservation test is the most important regression alarm in this codebase.

---

## 1. Repository Layout (Monolith with Service Boundaries)

```
TicketMaster-Back-End/
├── docker-compose.yml          # postgres, redis, kafka, elasticsearch, debezium
├── docker-compose.override.yml # local-only ports, hot reload mounts
├── package.json                # single root package; workspaces optional later
├── tsconfig.json
├── .env.example
├── README.md
├── ARCHITECTURE.md
├── API.md                      # OpenAPI 3 spec
├── PATTERNS.md                 # explanation of each pattern as you implement
├── PLAN.md                     # this file
│
├── src/
│   ├── app.ts                  # Fastify app bootstrap
│   ├── server.ts               # HTTP listener
│   ├── config.ts               # env parsing (zod)
│   │
│   ├── modules/
│   │   ├── events/             # Event Service module
│   │   │   ├── routes.ts
│   │   │   ├── service.ts
│   │   │   ├── repository.ts
│   │   │   └── cache.ts
│   │   ├── search/             # Search Service module
│   │   │   ├── routes.ts
│   │   │   └── es-client.ts
│   │   ├── inventory/          # Inventory Service module
│   │   │   ├── routes.ts
│   │   │   ├── service.ts
│   │   │   ├── redis-state.ts  # HASH-per-section state
│   │   │   └── publisher.ts    # Pub/Sub emit
│   │   ├── bookings/           # Booking Service module (CRITICAL)
│   │   │   ├── routes.ts
│   │   │   ├── service.ts
│   │   │   ├── lock.ts         # SET NX, Lua release
│   │   │   ├── repository.ts
│   │   │   ├── confirm.ts
│   │   │   └── expiry-worker.ts
│   │   ├── payments/           # Payment Service module
│   │   │   ├── routes.ts       # webhook handler
│   │   │   ├── provider-mock.ts
│   │   │   └── hmac.ts
│   │   ├── websocket/          # WS server (split to own process in Phase 4)
│   │   │   ├── server.ts
│   │   │   ├── subscriptions.ts
│   │   │   └── pubsub.ts
│   │   └── users/              # JWT mock + GET /users/me/bookings
│   │       ├── routes.ts
│   │       └── auth.ts
│   │
│   ├── shared/
│   │   ├── db/
│   │   │   ├── pool.ts         # pg Pool
│   │   │   ├── migrate.ts      # node-pg-migrate runner
│   │   │   └── migrations/     # SQL migrations
│   │   ├── redis/
│   │   │   └── client.ts       # ioredis instances (cmd + pubsub)
│   │   ├── kafka/
│   │   │   ├── producer.ts
│   │   │   └── consumer.ts
│   │   ├── logging/
│   │   │   └── logger.ts       # Pino + correlation id
│   │   ├── errors/
│   │   │   └── domain-errors.ts
│   │   ├── idempotency/
│   │   │   └── store.ts        # Redis-backed
│   │   └── metrics/
│   │       └── prom.ts         # prom-client
│   │
│   └── workers/
│       ├── sync-worker.ts      # Kafka → Elasticsearch
│       ├── expiry-worker.ts    # cleanup expired pending bookings
│       └── outbox-worker.ts    # Phase 7
│
├── seed/
│   ├── venues.ts               # 1 venue, 5 sections, 100 seats
│   ├── events.ts               # 5 events on that venue
│   └── run.ts
│
├── scripts/
│   ├── load/                   # k6 scripts
│   │   ├── search.js
│   │   ├── reserve-hot-seat.js
│   │   └── hot-event-launch.js
│   └── chaos/                  # kill -9 redis, etc. (Phase 7)
│
└── test/
    ├── unit/                   # mocked deps
    ├── integration/            # testcontainers (real redis+pg)
    │   ├── reservation-concurrency.test.ts   # THE test
    │   ├── booking-lifecycle.test.ts
    │   ├── webhook-idempotency.test.ts
    │   └── cdc-sync.test.ts
    └── e2e/                    # full docker-compose up
```

---

## 2. Tech Choices (Locked)

| Concern | Choice | Why |
|---|---|---|
| HTTP framework | **Fastify** | Fast, schema-first, good Pino integration |
| Validation | **zod** | Same schemas usable for env, request body, Kafka payloads |
| DB driver | **pg** + **Kysely** | Type-safe queries you can still read as SQL |
| Migrations | **node-pg-migrate** | SQL-first, no model classes |
| Redis | **ioredis** | Lua eval, pub/sub, robust reconnection |
| Kafka client | **kafkajs** | Pure JS, works against Redpanda |
| ES client | **@elastic/elasticsearch** | Official |
| Logging | **Pino** | JSON, fast, request-scoped child loggers |
| Metrics | **prom-client** | `/metrics` endpoint |
| Testing | **Vitest** + **testcontainers** | Fast unit + real-infra integration |
| Load testing | **k6** | Scripted scenarios, threshold assertions |
| Local Kafka | **Redpanda** | Single binary, Kafka API compatible, no ZK |

---

## 3. Database Schema (Phase 1)

`src/shared/db/migrations/0001_initial.sql`:

```sql
CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE TABLE users (
  user_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  email   TEXT UNIQUE NOT NULL,
  name    TEXT NOT NULL,
  payment_info_ref TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE venues (
  venue_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  name     TEXT NOT NULL,
  address  TEXT NOT NULL,
  city     TEXT NOT NULL,
  seat_map JSONB NOT NULL
);

CREATE TABLE events (
  event_id  UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  name      TEXT NOT NULL,
  venue_id  UUID NOT NULL REFERENCES venues(venue_id),
  date_time TIMESTAMPTZ NOT NULL,
  performer TEXT,
  status    TEXT NOT NULL CHECK (status IN ('upcoming','on_sale','sold_out','cancelled')),
  category  TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX events_by_city_date ON events (venue_id, date_time);
CREATE INDEX events_by_status   ON events (status);

CREATE TABLE seats (
  seat_id  UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  venue_id UUID NOT NULL REFERENCES venues(venue_id),
  section  TEXT NOT NULL,
  row      TEXT NOT NULL,
  number   INT  NOT NULL,
  UNIQUE (venue_id, section, row, number)
);

CREATE TABLE tickets (
  ticket_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  event_id  UUID NOT NULL REFERENCES events(event_id),
  seat_id   UUID NOT NULL REFERENCES seats(seat_id),
  price_cents INT NOT NULL,
  status    TEXT NOT NULL CHECK (status IN ('available','reserved','sold')) DEFAULT 'available',
  version   INT NOT NULL DEFAULT 0,  -- for optimistic concurrency (stretch)
  UNIQUE (event_id, seat_id)
);
CREATE INDEX tickets_by_event_status ON tickets (event_id, status);

CREATE TABLE bookings (
  booking_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  user_id    UUID NOT NULL REFERENCES users(user_id),
  event_id   UUID NOT NULL REFERENCES events(event_id),
  ticket_ids UUID[] NOT NULL,
  status     TEXT NOT NULL CHECK (status IN ('pending','processing','confirmed','expired','cancelled','failed')),
  total_cents INT NOT NULL,
  expires_at  TIMESTAMPTZ,
  payment_intent_id TEXT,
  idempotency_key TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  confirmed_at TIMESTAMPTZ,
  UNIQUE (user_id, idempotency_key)
);
CREATE INDEX bookings_expiring ON bookings (expires_at) WHERE status = 'pending';

CREATE TABLE outbox (
  id          BIGSERIAL PRIMARY KEY,
  aggregate   TEXT NOT NULL,
  event_type  TEXT NOT NULL,
  payload     JSONB NOT NULL,
  created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
  published_at TIMESTAMPTZ
);
CREATE INDEX outbox_unpublished ON outbox (id) WHERE published_at IS NULL;

CREATE TABLE webhook_events (
  event_id   TEXT PRIMARY KEY,           -- idempotency
  received_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  payload    JSONB NOT NULL
);
```

**Modeling notes**
- `ticket_ids UUID[]` keeps the schema simple for the learning project; in production this would be a `booking_tickets` join table.
- `tickets.version` is unused initially but in place for the optimistic-concurrency stretch goal (alternative to Redis locks).
- `outbox` and `webhook_events` are introduced now even though they're Phase 6/7 — cheap to migrate later but cleaner to ship the schema once.

---

## 4. Phase-by-Phase Plan

Each phase ends with: (1) a demo command, (2) a test that proves the pattern works, (3) one paragraph in `PATTERNS.md`.

### Phase 1 — Foundation (week 1)

**Deliverable:** `curl localhost:3000/api/v1/events/{id}` returns event detail with seats from Postgres.

Tasks:
1. `package.json`, `tsconfig.json`, `.env.example`, ESLint + Prettier.
2. `docker-compose.yml` with `postgres:16` and `redis:7` only.
3. `src/config.ts` — zod-parsed env, fails fast.
4. `src/shared/db/pool.ts`, migration runner, run `0001_initial.sql`.
5. `seed/run.ts` — insert 1 venue (5 sections × 4 rows × 5 seats = 100 seats), 5 events, generate one ticket per (event × seat) — 500 tickets total.
6. Fastify app skeleton with `/health`, request-id middleware, Pino logger.
7. `modules/events/routes.ts` → `GET /api/v1/events/:id` (raw Postgres read, no cache yet).
8. `modules/inventory/routes.ts` → `GET /api/v1/events/:id/sections/:section/seats` (raw Postgres read).

**Test:** integration test boots a testcontainer Postgres, runs migrations + seed, asserts both endpoints respond with the expected shape.

---

### Phase 2 — Booking Flow (week 2) — THE CRITICAL PHASE

**Deliverable:** 100 concurrent reservations for 1 seat → exactly 1 success, 99 `409 SEATS_UNAVAILABLE`.

Tasks:
1. `shared/redis/client.ts` — split command client vs pub/sub client.
2. `modules/bookings/lock.ts`:
   - `acquire(ticketId, bookingId, userId)` → `SET reservation:{ticketId} {json} NX EX 600`.
   - `release(ticketId, bookingId)` → Lua script (only DEL if `bookingId` matches).
3. `modules/bookings/service.ts` — `createBooking()` following the pseudo-code in the context:
   - Acquire locks one at a time; on any failure, release all previously acquired locks.
   - Persist `bookings` row with `status='pending'`, `expires_at = now() + 10min`.
   - Return `{ booking_id, expires_in_seconds: 600 }`.
4. `POST /api/v1/bookings` route, zod-validated body.
5. `DELETE /api/v1/bookings/:id` — release locks + set `status='cancelled'`. Only the owner can cancel.
6. `POST /api/v1/bookings/:id/confirm`:
   - Mock payment (sync, always succeeds for now).
   - Inside a DB transaction: set booking `confirmed`, update tickets to `sold`, release Redis locks.
7. `Idempotency-Key` header support (Redis-backed store with 24h TTL). Replay returns the cached response.
8. `workers/expiry-worker.ts`: every 30s, sweep `bookings WHERE status='pending' AND expires_at < now()`, set to `expired`, mark tickets back to `available`. The Redis TTL handles the lock side; this worker handles DB cleanup.
9. **The concurrency test.** `test/integration/reservation-concurrency.test.ts`:
   ```ts
   const attempts = await Promise.allSettled(
     Array.from({ length: 100 }, () => createBooking(userId, eventId, [ticketId]))
   );
   const success = attempts.filter(a => a.status === 'fulfilled').length;
   expect(success).toBe(1);
   ```
10. k6 script `scripts/load/reserve-hot-seat.js` — 1000 VUs hammering one seat for 30s; assert no double bookings via post-run DB query.

**`PATTERNS.md` entries:** Distributed Lock with TTL, Idempotency Keys.

---

### Phase 3 — Search (week 3)

**Deliverable:** Create a new event in Postgres, see it appear in `GET /api/v1/events?q=...` within ~2 seconds without any application code dual-writing.

Tasks:
1. Add `elasticsearch:8`, `redpanda`, `debezium/connect` to `docker-compose.yml`.
2. Configure Debezium Postgres connector via JSON POST at startup (`scripts/register-debezium.sh`):
   - Captures `public.events` → topic `cdc.events`.
3. `workers/sync-worker.ts`:
   - Consume `cdc.events`.
   - Transform CDC envelope → ES doc.
   - Idempotent upsert into `events` ES index keyed by `event_id`.
   - Commit Kafka offset only after ES ack.
4. `modules/search/routes.ts` → `GET /api/v1/events` with filters: `q`, `city`, `category`, `date` (range), pagination.
5. `seed/run.ts` extension: after DB seed, wait for CDC to populate ES (or `--bootstrap-es` flag for first run).
6. **Integration test:** insert event row, poll ES until visible, assert search returns it. Log the delay; that's your CDC lag number.

**`PATTERNS.md` entry:** CDC with Debezium (vs dual-write — why we picked the harder option).

---

### Phase 4 — Real-Time Updates (week 4)

**Deliverable:** Open the demo HTML page in two browser tabs; reserve a seat in tab A; tab B's seat goes red within 100ms.

Tasks:
1. **Split WebSocket out of the main process.** New entry point `src/ws-server.ts`. Now we have two Node processes sharing the same monolith codebase. Document this in `ARCHITECTURE.md`.
2. `modules/websocket/server.ts` using `ws`:
   - Path: `/ws/events/:event_id/seats`.
   - On connect: store `{ socket, eventId }` in an in-memory `Map`.
3. `shared/redis/pubsub.ts`: subscribe to `seat-changes:{event_id}` channel.
4. `modules/inventory/publisher.ts`: on any seat status change, publish:
   ```json
   { "type":"seat_status_changed", "ticket_id":"...", "new_status":"reserved" }
   ```
5. Wire the booking flow's lock-acquire and confirm steps to call `publisher.emit()` after the DB commit.
6. Run **two WS server instances** behind nothing (clients connect directly with a port). Both subscribe to Redis Pub/Sub → demonstrates Pub/Sub fan-out across instances.
7. **Integration test:** open two WS clients, reserve a seat through the HTTP API, assert both receive the event.

**`PATTERNS.md` entries:** Pub/Sub for Real-Time Distribution, Single-Writer Per Connection.

---

### Phase 5 — Caching & Optimization (week 5)

**Deliverable:** `GET /events/:id` shows >95% Redis hit rate under load; section seat reads are one Redis call.

Tasks:
1. `modules/events/cache.ts` — cache-aside:
   - Key: `event:{event_id}`, TTL 300s.
   - Invalidate on event update (call from any write path).
2. `modules/inventory/redis-state.ts`:
   - On startup/seed, populate `event:{event_id}:section:{section}` as HASH of `{ticket_id → JSON(status,price)}`.
   - `GET seats` endpoint switches to `HGETALL` (one round trip per section).
   - On every booking state change, update the HASH field and publish the seat-changes message.
3. Add `prom-client` metrics:
   - `cache_hits_total{key}`, `cache_misses_total{key}`.
   - `redis_lock_attempts_total{outcome}`.
   - Histogram `http_request_duration_seconds{route, status}`.
4. Expose `/metrics`; optional `grafana` + `prometheus` services in `docker-compose.override.yml`.

**`PATTERNS.md` entries:** Cache-Aside, Cache Invalidation Strategy.

---

### Phase 6 — Payment & Webhooks (week 6)

**Deliverable:** `POST /bookings/:id/confirm` returns `processing`; mock provider POSTs `/webhooks/payment` ~2s later; booking flips to `confirmed` (or `failed`). Replaying the same webhook is a no-op.

Tasks:
1. `modules/payments/provider-mock.ts`:
   - Separate Node process exposing `POST /v1/payment-intents` (returns intent id).
   - Schedules a `setTimeout(2000)` that POSTs back to `http://api:3000/api/v1/webhooks/payment` signed with HMAC-SHA256.
   - Random 5% failure rate to exercise the `failed` path.
2. `modules/bookings/confirm.ts` update:
   - `pending` → call provider → `processing`. Do not yet flip tickets to `sold`.
3. `modules/payments/routes.ts`:
   - Capture raw body before JSON parsing (Fastify content type parser).
   - `hmac.ts`: timing-safe compare of `X-Signature` vs `hmac(rawBody, secret)`.
   - Idempotency via `INSERT INTO webhook_events (event_id) ON CONFLICT DO NOTHING`; if conflict, return 200 without processing.
   - On `payment.succeeded`: flip booking + tickets in one transaction, release Redis locks, publish seat-changes.
   - On `payment.failed`: booking → `failed`, release locks, tickets → `available`, publish.
4. **Integration test** `webhook-idempotency.test.ts`: post the same webhook 10× → exactly one DB mutation.

**`PATTERNS.md` entries:** Webhook Signature Verification, State Machine for Bookings.

---

### Phase 7 — Advanced (week 7+)

Pick any subset. Each is self-contained.

- **Outbox pattern.** Replace direct `kafka.publish` in the booking flow with `INSERT INTO outbox` in the same transaction. `workers/outbox-worker.ts` polls and publishes, sets `published_at`. Demonstrates atomic DB write + event publish.
- **Virtual Waiting Room.** Redis Sorted Set keyed by arrival time, worker dequeues N/sec, issues a short-lived JWT that the API gateway requires for `POST /bookings`. Load test: 10k users at t=0, watch them admitted at controlled rate.
- **Rate limiting.** Token bucket per IP + per user at a Fastify hook. Per-event "burst" limits.
- **Optimistic concurrency alternative.** Branch that drops Redis locks and uses `UPDATE tickets SET status='reserved' WHERE ticket_id=? AND version=? RETURNING *`. Run the same concurrency test against both implementations; compare throughput.
- **Chaos.** `scripts/chaos/kill-redis.sh` mid-load test. Document recovery behavior.

---

## 5. Local Dev — Docker Compose Plan

`docker-compose.yml` final shape (added incrementally per phase):

```yaml
services:
  postgres:        # Phase 1
  redis:           # Phase 1
  api:             # Phase 1 (Node main)
  ws:              # Phase 4 (Node ws process)
  redpanda:        # Phase 3 (Kafka API)
  debezium:        # Phase 3
  elasticsearch:   # Phase 3
  sync-worker:     # Phase 3
  expiry-worker:   # Phase 2
  payment-mock:    # Phase 6
  outbox-worker:   # Phase 7
  prometheus:      # Phase 5 (override file)
  grafana:         # Phase 5 (override file)
```

`make` targets (or npm scripts) for: `up`, `migrate`, `seed`, `logs`, `load:reserve`, `load:search`, `load:hot-event`, `test:int`, `test:concurrency`.

---

## 6. Testing Strategy (Concrete)

**Unit (Vitest, fast):**
- `bookings/lock.ts` with a mocked ioredis — covers branch logic only.
- `payments/hmac.ts` — signature happy path + tampered body.
- `bookings/service.ts` rollback-on-partial-failure with mocked lock + DB.

**Integration (Vitest + testcontainers):**
- `reservation-concurrency.test.ts` (the headline test).
- `booking-lifecycle.test.ts` — pending → confirmed and pending → expired.
- `webhook-idempotency.test.ts` — 10x same webhook → 1 effect.
- `cdc-sync.test.ts` — DB insert visible in ES within timeout.
- `pubsub-fanout.test.ts` — two WS clients on two server instances both receive.

**Load (k6):**
- `search.js` — 1000 concurrent search VUs, p99 < 500ms threshold.
- `reserve-hot-seat.js` — 1000 VUs racing for one seat, post-run SQL assertion: exactly 1 `sold`.
- `hot-event-launch.js` — 10k VUs in 30s ramp, mixed search + reserve, measure error rate.

Thresholds become CI gates in Phase 5+.

---

## 7. Observability Plan

- **Logs.** Pino JSON, every request gets a child logger with `request_id`. Booking flow logs structured events: `reservation_attempt`, `reservation_outcome`, `payment_callback`.
- **Metrics.** Prometheus scrapes `/metrics` on api + ws + workers. Dashboards (Grafana, Phase 5):
  - Booking funnel (attempts → reservations → confirmations).
  - Redis SET NX failure rate.
  - WebSocket connections per instance.
  - CDC lag (max `published_at - created_at` in outbox once Phase 7 lands).
- **Tracing (stretch).** OpenTelemetry SDK with OTLP exporter to Jaeger. Propagate `traceparent` through Kafka headers.

---

## 8. Documentation Deliverables

Tied to phases — write the doc when the feature lands, not at the end:

| Doc | When | Contents |
|---|---|---|
| `README.md` | Phase 1 | Run locally, demo curls, architecture diagram screenshot |
| `API.md` | Phase 2 | OpenAPI 3 spec, regenerated as routes evolve |
| `PATTERNS.md` | Each phase | One section per pattern with code link + trade-off |
| `ARCHITECTURE.md` | Phase 4 | Detailed design, why monolith-first, why CDC vs dual-write, sequence diagrams for booking + webhook |
| `RUNBOOK.md` | Phase 7 | What to do when redis is down, when CDC lag spikes, etc. |

---

## 9. Definition of Done

The back-end is "done" when:

1. `npm run test:concurrency` shows exactly 1 success out of 100 concurrent reservations for the same seat.
2. `npm run demo:realtime` (or manual two-tab test) shows WebSocket updates across browser tabs.
3. `npm run test:cdc` shows a new DB event reaches Elasticsearch via CDC within 5s.
4. `npm run test:webhook-idempotency` passes with 10x replay.
5. Booking state machine covers all six states with passing tests for each transition.
6. `PATTERNS.md` has an entry for every pattern listed in the project context.
7. `k6 run scripts/load/hot-event-launch.js` completes with p99 reservation < 1s and zero double-bookings.

---

## 10. First-Week Concrete Checklist

Day 1: repo init, `package.json`, `tsconfig.json`, lint/format, `docker-compose.yml` (pg+redis), Fastify hello world.
Day 2: migrations, seed script, `GET /events/:id`, `GET /events/:id/sections/:section/seats`.
Day 3: ioredis wiring, `bookings/lock.ts` with SET NX + Lua release, unit tests.
Day 4: `POST /bookings`, `DELETE /bookings/:id`, expiry worker.
Day 5: `POST /bookings/:id/confirm` (sync mock payment), idempotency keys, integration test for the lifecycle.
Day 6: the **concurrency integration test** — this is the milestone that proves Phase 2.
Day 7: first k6 load test, write the Phase 2 `PATTERNS.md` entries.
