# PMM Report — ab0t-events-go (Outbound Event Webhooks)

**Date:** 2026-08-12  **Version reviewed:** v0.1.0  **Author:** Product / Eng
**Status:** Library shipped standalone; Workspace wiring + admin surface are the next phase.

---

## 1. Executive summary

`ab0t-events-go` turns "we emit domain events internally" into "any customer can
subscribe to our events over HTTPS," the way Stripe and GitHub do. v0.1.0 ships
the hard core — hexagonal, per-org isolated, HMAC-signed, retried, dead-lettered,
logged, replayable — behind a 3-line facade. It is the reusable foundation every
ab0t service can adopt.

The v0.1.0 core is **feature-complete for correctness and safety** but is
**not yet a turnkey product**: it lacks the operational surface (durable queue,
auto-disable, admin API/UI, event catalog, first-class metrics/tracing) and the
interop story (Standard Webhooks spec) that make a webhook platform something
customers *love* rather than merely receive. This report inventories what users
will ask for next and proposes a prioritized roadmap to get there.

**Top three recommendations**
1. **Durability**: ship a persistent queue + a startup recovery sweeper so
   in-flight retries survive process restarts (biggest production risk today).
2. **Interop**: add a Standard Webhooks (`standardwebhooks.com`) signing scheme so
   subscribers can verify with off-the-shelf libraries in any language.
3. **Turnkey operations**: ship drop-in admin HTTP handlers, an auto-disable
   circuit breaker, and a Prometheus/OTel adapter so a service gets a *product*,
   not a toolkit.

---

## 2. What we shipped (v0.1.0)

| Capability | State |
|---|---|
| Hexagonal core (ports & adapters), pure/testable | ✅ |
| Per-org tenant isolation (store PK + dispatcher re-assertion) | ✅ |
| HMAC-SHA256 signing over raw body + verify helpers | ✅ |
| Retries: exp backoff + jitter, pluggable retryable classifier, dead-letter | ✅ |
| Delivery log (per attempt: code, latency, error, snippet), cursor-paged | ✅ |
| Idempotency (stable delivery id, `X-Delivery-Id`) | ✅ |
| Replay by delivery id | ✅ |
| Anti-replay (timestamp + tolerance window) | ✅ |
| Event-type matching: exact, `*`, prefix `a.b.*` | ✅ |
| Adapters: hmacsign, httpdeliver (SSRF-guarded), inproc, memory, dynamo, std | ✅ |
| Batteries-included `webhooks` facade | ✅ |
| Observer hook (per-attempt metrics/logging) | ✅ |
| SSRF guard + documented threat model | ✅ |
| Tests: 41 tests, race-clean, 75.6% total (core 80%, http 89%, inproc 95%) | ✅ |

**Deliberately out of scope for v0.1.0:** durable queue, admin API/UI, event
catalog/schemas, auto-disable, transforms, batching, multi-language SDKs.

---

## 3. Target users & jobs-to-be-done

| Persona | Job to be done | What they need from us |
|---|---|---|
| **ab0t service engineer** (primary) | "Let my customers subscribe to my service's events" | 10-min wire-in, swappable adapters, no ops surprises |
| **Platform/infra team** | "One webhook system for all services, run it safely at scale" | Durable queue, metrics, tracing, runbooks, rate limits |
| **ab0t customer's developer** (the subscriber) | "Reliably receive & verify webhooks" | Standard signature format, delivery visibility, manual retry, replay |
| **Customer success / support** | "Debug why a customer didn't get an event" | Delivery log UI, filters, redelivery button |
| **Security** | "No SSRF, no secret leakage, tenant isolation" | Guardrails on by default, audits, secret rotation |

**Representative use cases:** `drive.file.created` → customer's ETL; `calendar.event.updated`
→ partner calendar sync; `mail.message.sent` → CRM logging; billing/usage events →
customer data warehouse.

---

## 4. Competitive landscape

| Product | What they do well | What we can learn |
|---|---|---|
| **Stripe Webhooks** | Event catalog + versioning, auto-disable after repeated failures, dashboard with resend, signed `Stripe-Signature` w/ timestamp | Auto-disable, event catalog, first-class dashboard |
| **GitHub Webhooks** | Simple `X-Hub-Signature-256`, per-delivery redelivery, ping event | Redelivery UX, ping/test-event |
| **Svix (svix.com)** | Turnkey SaaS/OSS: durable, retries, portal for customers, **Standard Webhooks spec author**, multi-lang verify libs | Standard Webhooks interop, embeddable consumer portal |
| **Hookdeck** | Ingestion + delivery observability, filtering, transformations, replay by query | Payload filters, transforms, bulk replay |
| **Convoy (getconvoy.io, OSS)** | Fan-out at scale, rate limiting per endpoint, static IPs, event catalog | Per-endpoint rate limits, egress IP pinning |

**Positioning takeaway:** we are closest to "embed Svix/Convoy as a Go library,
tenant-isolated, no external SaaS." Our differentiators are **library-first
(no separate service to run)**, **hexagonal (swap any adapter)**, and **hard
multi-tenancy baked into the core**. To win developer love we must close the
**interop** (Standard Webhooks) and **operability** (durable + observable + admin)
gaps.

---

## 5. Gap analysis — what users will ask for next

### 5.1 Reliability & scale
- **Durable queue + recovery sweeper (P0).** `inproc` is in-memory: a crash loses
  queued retries. The delivery log *does* persist non-terminal deliveries, so a
  startup **sweeper** that re-enqueues `pending`/`failed` deliveries closes the
  gap even before a persistent queue lands. Then ship SQS and DynamoDB/Postgres
  queue adapters implementing `DeliveryQueue`.
- **Auto-disable circuit breaker (P0).** After N consecutive failures / X hours of
  all-failing, pause the subscription and fire a hook (email the org). Prevents
  hammering dead endpoints and matches Stripe behavior.
- **Per-endpoint rate limiting / concurrency caps (P1).** Protect slow subscribers
  and our own egress; token-bucket per subscription.
- **Ordered delivery per key (P2).** Optional FIFO per resource id for consumers
  that need it.

### 5.2 Interop & DX for subscribers
- **Standard Webhooks signing scheme (P0).** Add a `Signer`/verify pair emitting
  `webhook-id` / `webhook-timestamp` / `webhook-signature` (base64, versioned
  keys) so subscribers verify with existing libraries in JS/Python/Go/etc. Keep
  our current scheme available; make the scheme pluggable per subscription.
- **Secret rotation (P0).** Support multiple active secrets so an org can rotate
  without downtime (sign with the newest, accept verification against any).
- **Ping / send-test-event (P1).** Verify an endpoint at subscribe time and give a
  "send test" button for customers.
- **Multi-language verification snippets / SDK (P1).** Docs + copy-paste verify
  code per language; optional thin verify libs.

### 5.3 Observability & operations
- **First-class metrics adapter (P0).** Prometheus counters/histograms
  (attempts by status/code, latency, queue depth, dead-letter rate) via the
  Observer hook — shipped, not hand-rolled by each service.
- **OpenTelemetry tracing (P1).** Propagate `traceparent` to the subscriber and
  span each attempt.
- **Drop-in admin HTTP handlers (P0).** chi/`net/http` handlers for subscription
  CRUD, list deliveries, get delivery detail, replay, rotate secret — so services
  don't rebuild the same API. (This is the ticketed follow-on; generalize it.)
- **Delivery-log UI components (P2).** Embeddable consumer portal (Svix-style) or
  at least a reference React/HTML view.

### 5.4 Event model
- **Event catalog + JSON schemas + versioning (P1).** A registry of published
  event types with schemas and semver, powering docs and validation. Enables
  per-subscription API-version pinning (Stripe-style).
- **Payload-based filters (P2).** Subscribe with CEL/JSONPath predicates, not just
  type globs.
- **Transformations (P3).** Per-endpoint payload reshaping.
- **Batching (P2).** Deliver multiple events per request for high-volume topics.

### 5.5 Security hardening
- **Secret encryption-at-rest helper (P1).** KMS envelope-encryption wrapper for
  `Subscription.Secret` in the dynamo adapter.
- **Egress IP pinning / static source IPs (P2).** So customers can allowlist us.
- **Signed-URL / mTLS delivery option (P3).** For high-assurance subscribers.

---

## 6. Proposed roadmap (ICE-ranked)

ICE = Impact × Confidence ÷ Effort (1–10 each; higher is better).

| # | Item | Impact | Conf | Effort | ICE | Milestone |
|---|---|---:|---:|---:|---:|---|
| 1 | Startup recovery sweeper (re-enqueue non-terminal deliveries) | 9 | 9 | 3 | **27.0** | v0.2 |
| 2 | Standard Webhooks signing scheme (pluggable) | 8 | 8 | 3 | **21.3** | v0.2 |
| 3 | Prometheus metrics adapter (via Observer) | 7 | 9 | 3 | **21.0** | v0.2 |
| 4 | Drop-in admin HTTP handlers (subscriptions, deliveries, replay) | 9 | 8 | 4 | **18.0** | v0.2 |
| 5 | Auto-disable circuit breaker + hook | 8 | 8 | 4 | **16.0** | v0.2 |
| 6 | Secret rotation (multi-secret) | 7 | 8 | 4 | **14.0** | v0.3 |
| 7 | SQS queue adapter (durable) | 8 | 7 | 5 | **11.2** | v0.3 |
| 8 | Ping / send-test-event | 6 | 8 | 4 | **12.0** | v0.3 |
| 9 | OpenTelemetry tracing | 6 | 7 | 4 | **10.5** | v0.3 |
| 10 | Event catalog + schemas + versioning | 8 | 6 | 7 | **6.9** | v0.4 |
| 11 | Per-endpoint rate limiting | 6 | 7 | 5 | **8.4** | v0.4 |
| 12 | Payload-based filters (CEL) | 6 | 6 | 6 | **6.0** | v0.4 |
| 13 | KMS secret encryption helper | 6 | 7 | 5 | **8.4** | v0.4 |
| 14 | Batching | 5 | 6 | 6 | **5.0** | Deferred (C-17) |
| 15 | Transformations, ordered delivery, portal UI | 5 | 5 | 8 | **3.1** | Transform hook shipped (C-18); ordered delivery / portal UI still Later |

> **C-17 (Batching) — DEFERRED (2026-08-12).** Evaluated during the completion
> workflow: it does not fit the per-(subscription,event) idempotency / per-delivery
> log / single-message-signing model without a high-blast-radius rewrite, and the
> usual driver (slow-subscriber protection) is already covered by
> `adapters/ratelimit`. Full rationale + a future design sketch (opt-in
> `Subscription.BatchPolicy` + a `BatchDeliverer` envelope path) are in
> `docs/tickets/WORKLOG.md`.

**v0.2 theme — "Production-ready & interoperable":** items 1–5.
**v0.3 theme — "Durable & rotatable":** items 6–9.
**v0.4 theme — "Rich event model & controls":** items 10–13.

### Update 2026-08-12 (post-implementation) — v0.2 shipped

Delivered autonomously under tickets EV-001..EV-007 (`docs/tickets/`):

| Roadmap item | Status | Where |
|---|---|---|
| 1 · Startup recovery sweeper | ✅ shipped | `Service.Recover` + `DeliveryFilter.Status` |
| 2 · Standard Webhooks signer | ✅ shipped | `adapters/stdwebhook` |
| 3 · Metrics observer | ✅ shipped | `adapters/metrics` + `events.MultiObserver` |
| 4 · Admin HTTP API + UI (the "seams") | ✅ shipped | `adapters/adminhttp` (API + HTML delivery log) |
| 5 · Auto-disable circuit breaker | ✅ shipped | `adapters/autodisable` |
| 6 · Secret rotation | ✅ shipped (early) | `Service.RotateSecret` |
| 7 · SQS durable queue | ⏳ next (v0.3) | interface ready (`events.DeliveryQueue`) |

Signer port generalized to `Sign(SigningInput) map[string]string` to enable
scheme pluggability (breaking, pre-1.0). Suite: 53 tests, race-green across 13
test packages. Remaining v0.3+ items (durable queue, ping/test-event, OTel, event
catalog, rate limiting, KMS) unchanged in priority.

---

## 7. Success metrics (define now, instrument in v0.2)

- **Delivery success rate** (2xx / attempts) per org and per endpoint — target ≥ 99.5%.
- **Time-to-delivery** p50/p95 from `Emit` to first 2xx.
- **Dead-letter rate** per endpoint (drives auto-disable).
- **Adoption:** # of ab0t services wired in; # of orgs with ≥1 active subscription.
- **DX:** median time from `go get` to first delivered event (target < 15 min).
- **Reliability:** deliveries lost on restart — target **0** (gated by item 1).

---

## 8. Risks & mitigations

| Risk | Impact | Mitigation |
|---|---|---|
| In-memory queue loses retries on crash | Missed deliveries | Recovery sweeper (item 1), then durable queue (item 7) |
| Custom signature format hurts subscriber DX | Slow adoption, support load | Standard Webhooks scheme (item 2) |
| Services each rebuild admin API | Duplicated effort, drift | Drop-in handlers (item 4) |
| Dynamo log write-path undertested (36.8% cov) | Latent bugs at scale | DynamoDB-Local integration test in CI |
| SSRF guard bypass via public proxy host | Data exfil | Layer egress controls; document; item 13/IP pinning |
| Secret sprawl without rotation | Hard incident response | Secret rotation (item 6) + KMS (item 13) |

---

## 9. Go-to-market / enablement notes

- **Internal launch:** land the Workspace bridge (map `drive.*`/`calendar.*`/`mail.*`
  internal bus → public event types) + admin endpoints as the reference consumer;
  publish the `ab0t-events-go` skill so any team self-serves.
- **Docs to add:** a public "Webhooks" developer guide (subscribe, verify in 5
  languages, event catalog, retries/replay semantics, IP allowlist).
- **Positioning line:** *"Stripe-grade outbound webhooks, embedded as a Go library,
  tenant-isolated by construction."*

---

## 10. Appendix — current test coverage (v0.1.0)

```
events (core)        80.1%   webhooks (facade)   82.6%
adapters/std        100.0%   adapters/memory     97.5%
adapters/inproc      94.5%   adapters/httpdeliver 89.3%
adapters/hmacsign    92.0%   adapters/dynamo      36.8%  (log write-path needs DynamoDB-Local)
------------------------------------------------------------
TOTAL 75.6% across 41 tests; go build / go test / go test -race all green, offline.
```

Coverage priorities: raise `adapters/dynamo` via a DynamoDB-Local integration
test in CI; add fault-injection tests (store/queue/log errors) to the core.
