# CLAUDE.md — Working Agreement (Back-End)

This file is the **canonical source of truth** for how the AI assistant and the developer collaborate on this project. It lives in the repo so it's versioned, visible to humans, and re-loaded automatically by Claude Code in every session.

`PLAN.md` describes **what** we're building. This file describes **how** we work on it.

---

## 1. Project Context (One Paragraph)

This is a Ticketmaster-style event ticketing back-end. The goal is to **learn distributed-systems patterns** (distributed locks, CDC, pub/sub, idempotency, webhooks, event-driven services) by implementing them — not to ship a polished product. The headline milestone is Phase 2: 100 concurrent reservations for one seat resulting in exactly 1 success. Stack: Node.js 20 + TypeScript, Fastify, Postgres, Redis, Kafka (Redpanda), Elasticsearch, Debezium. Monolith-first; processes split out only when the patterns demand it. Full detail: `PLAN.md`.

A companion front-end repo lives at `../TicketMaster-Front-End` with its own `PLAN.md` and `CLAUDE.md`. Work alternates between the two.

---

## 2. The Pacing Rule — One File at a Time

Implement **exactly one file per turn**, then stop and wait.

- Tiny config files (≤5 lines, e.g. `.gitignore`, `.env.example`) MAY be bundled with the next real file so we don't stop on trivia. Use judgment; when in doubt, separate.
- Never write speculative files ("we'll need this later"). Only write what the next 30 minutes of work actually requires. **No installing libraries we don't import yet.**
- Before writing the file, state in one sentence what's coming so the developer can redirect cheaply.
- After writing the file, summarize what's in it in plain language. Then ask the questions (see §3).

This is non-negotiable. The point of this project is understanding, and understanding requires checkpoints.

---

## 3. The Quiz Rule — Ask What the File Warrants

After each file, ask **as many questions as the file warrants** — no fixed number.

**Ask a question when:**
- The file contains a field, flag, or value where the alternative would have been reasonable (e.g. `"type": "module"` vs CommonJS default, SSH vs HTTPS).
- A pattern repeats across the project — explaining it once saves many future explanations (e.g. `import` resolution, `tsconfig` paths, ORM vs query-builder choice).
- A trade-off has real consequences (e.g. CDC over dual-write, Redis SET NX over DB row-locks).

**Skip questions on:**
- Boilerplate where the value is the same in every project (e.g. the `name` field of `package.json`, the existence of a `.gitignore`).
- Obvious-from-context choices.

### The Follow-Up Rule (when the developer doesn't know an answer)

When the developer answers a question wrong OR says "I don't know" / "skip" / "explain":

1. **Teach the answer** in plain language. Use an analogy, compare to the alternative, describe what would break without it.
2. **Re-ask a smaller, narrower follow-up** that checks the specific point of confusion. The follow-up must be answerable in one sentence.
   - Example: developer didn't know what `"type": "module"` does → after teaching, ask "If we removed `type: module` and tried `import fastify from 'fastify'`, what would happen at runtime?"
   - Example: developer didn't know why `private: true` matters → after teaching, ask "What command does `private: true` block, and what's the worst-case if you forgot to set it?"
3. If they still miss it, teach once more and move on. **Don't loop.**
4. Acknowledgments like "got it" / "makes sense" do NOT require a follow-up — only re-ask if the developer explicitly engages with the follow-up question.

---

## 4. The Git Rule

- **Never commit or push without an explicit instruction** from the developer ("commit this," "push," "save this").
- **Never add a `Co-Authored-By:` trailer** to commit messages. Use the commit body only.
- Use HEREDOC syntax for commit messages with multiple lines.
- If a push fails (auth, conflict), explain the cause and wait for direction. Don't try to "fix" credentials.

---

## 5. The Tooling Suggestion Rule

The developer wants to know when a **Claude Code hook**, an **MCP server**, a **custom skill**, a **pre-commit hook**, or any other automation could improve our workflow. Examples that might come up in this project:

- A pre-commit hook that runs `tsc --noEmit` to block commits with type errors.
- A custom skill that scaffolds a new service module with our conventions (routes.ts + service.ts + repository.ts + test).
- An MCP server exposing our local Postgres so the AI can introspect schema directly.
- A hook that runs the concurrency test before every push to `main`.
- A skill that generates k6 load-test scripts for a new endpoint.

**The rule:**
- **Flag the suggestion** in 2–3 sentences whenever the moment fits. Be specific: what tool, what problem it solves, what the trade-off is.
- **Never install or configure** any of these without explicit "yes, set it up" from the developer.
- Don't over-suggest. If we already discussed it and declined, don't bring it up again unless circumstances change.

---

## 6. Coding Defaults (Back-End)

Carried in from `PLAN.md` for visibility. Defer to `PLAN.md` for the full reasoning.

- **Language:** TypeScript, strict mode. No `any` without comment justifying why.
- **HTTP:** Fastify. Routes declared with zod schemas.
- **DB:** raw `pg` + Kysely (thin query builder). No heavy ORM.
- **Migrations:** SQL-first via `node-pg-migrate`.
- **Logging:** Pino, structured JSON, request-scoped child loggers, `X-Request-ID` propagated everywhere.
- **Validation:** zod (env, request bodies, Kafka payloads).
- **Modules:** ESM (`"type": "module"` in `package.json`).
- **Testing:** Vitest unit + Vitest with testcontainers for integration.
- **No comments explaining WHAT.** Code names are the docs. Comments only when the WHY is non-obvious (a workaround, a subtle invariant, a hidden constraint).

---

## 7. Out of Scope (Don't Build These)

Per the project context document, the following are **explicitly out of scope**. Don't propose features in these areas:

- Real authentication (use a mock JWT).
- Real payment processing (mock provider with a webhook callback).
- Email notifications (stub it out).
- Resale marketplace, recommendations, admin dashboard.
- Mobile apps.
- Kubernetes (Docker Compose is enough).
- A fancy frontend (the FE repo is a thin testing UI).

---

## 8. Package Manager — pnpm only, never npm

**Hard rule:** Use `pnpm` for everything Node-related in this repo. Never run `npm install`, `npm i`, `npm ci`, `npm update`, `npm exec`, `npm run`, `npx`, or any other `npm <subcommand>`.

**Why:** Security. Recent npm supply-chain attacks (Shai-Hulud worm, compromised popular packages, malicious post-install scripts) make pnpm the safer default. pnpm's content-addressable store, strict peer resolution, and isolated `node_modules` reduce blast radius if a malicious package slips in. This is a deliberate defensive choice — do not "fall back" to npm even if a script, tutorial, or docs page suggests it.

**Mapping for common commands:**

| If you would run | Run instead |
|---|---|
| `npm install` | `pnpm install` |
| `npm install <pkg>` | `pnpm add <pkg>` |
| `npm install -D <pkg>` | `pnpm add -D <pkg>` |
| `npm uninstall <pkg>` | `pnpm remove <pkg>` |
| `npm ci` | `pnpm install --frozen-lockfile` |
| `npm run <script>` | `pnpm <script>` |
| `npx <bin>` (download) | `pnpm dlx <bin>` |
| `npm update` | `pnpm update` |
| `npm audit` | `pnpm audit` |
| `npm publish` | `pnpm publish` |

**`npx` exception:** `npx <bin>` is only acceptable when the binary is already a project dependency (resolved from local `node_modules`). For one-off tools that would download a package, use `pnpm dlx <bin>`.

**Lockfile rule:** `pnpm-lock.yaml` is authoritative. If a `package-lock.json` ever appears in this repo, that's a mistake — surface it before deleting.

If a task seems to *require* npm specifically (e.g., reproducing an npm-specific bug), surface it before running npm. Don't silently switch back.

---

## 9. Working Memory Beyond This File

This file is the **canonical** source for working agreements. The AI's private memory (stored outside the repo) should not duplicate the rules here — it should only contain pointers back to this file. If a rule is added or changed, it goes here first, then a memory entry is updated if needed.

When in doubt, **this file wins**.
