---
name: ab0t-events-go
description: >-
  Add outbound event webhooks (Stripe/GitHub style) to any Go service using the
  ab0t-events-go library — deliver domain events to external subscriber
  endpoints with HMAC signing, retries + dead-letter, a delivery log, replay,
  and per-org tenant isolation. Use when wiring ab0t-events-go into a service,
  emitting domain events to customer/partner URLs, building webhook subscription
  admin endpoints, signing/verifying webhook payloads (HMAC-SHA256, X-Signature,
  X-Event-Timestamp), implementing delivery retries/backoff/dead-letter, adding
  a delivery log or replay, guarding webhook URLs against SSRF, or writing a new
  store/queue/deliverer adapter for the library's ports. Triggers: "outbound
  webhooks", "webhook delivery", "deliver events to a URL", "event subscriptions",
  "webhook signing", "webhook retries", "ab0t-events-go", "events.Emitter",
  "webhooks.New".
---

# ab0t-events-go

Outbound event-webhook delivery as a reusable, hexagonal (ports & adapters) Go
library. A service emits a domain event; the library fans it out to that org's
subscribed external endpoints, HMAC-signed, with retries, dead-lettering, a
delivery log, and replay. Module: `github.com/ab0t-com/ab0t-events-go`.

Use this skill to CONSUME the library in a service or to EXTEND it with a new
adapter. It is pure Go (stdlib + aws-sdk-go-v2 for the DynamoDB adapter only).

## Mental model

```
service ──Emit──▶ events.Service ──▶ [SubscriptionStore] find org's matching subs
                       │              [DeliveryQueue] enqueue a job per sub
                       ▼              [Deliverer] POST signed body  [Signer] HMAC
              sign → POST → log → retry(backoff+jitter) → dead-letter → [DeliveryLog]
```

The **core** (`events`) is pure: no `time.Now`, no `rand`, no I/O — those enter
through the `Clock`, `IDGen`, `Rand`, and store/queue/deliverer/signer/log ports.
Adapters live under `adapters/`; the `webhooks` package is a batteries-included
facade.

## Quick start (the facade — preferred)

Only the two stateful ports (Store, Log) are required; everything else defaults
(SSRF-guarded deliverer, HMAC signer, worker pool, retry policy, clock/id/rand).

```go
sys, err := webhooks.New(ctx, webhooks.Options{
    Store: memory.NewStore(),           // prod: dynamo.NewStore(ddb, "subscriptions")
    Log:   memory.NewLog(),             // prod: dynamo.NewLog(ddb, "deliveries")
    // Optional: Observer, Policy, Workers, Deliverer, Signer, ValidateURL, Clock/IDGen/Rand
})
if err != nil { return err }
defer sys.Close()                        // stops worker pool, drains in-flight

secret, _ := hmacsign.GenerateSecret()   // "whsec_..."; show to org once, store encrypted
sys.Subscribe(ctx, events.NewSubscription{
    OrgID: orgID, EventTypes: []string{"drive.*"},   // exact | "*" | "prefix.*"
    URL: "https://customer.example/webhooks", Secret: secret,
})
sys.EmitNew(ctx, "drive.file.created", orgID, payloadStruct) // id+timestamp+JSON handled
```

`*webhooks.System` embeds `*events.Service`, so `Emit`, `EmitNew`, `Subscribe`,
`ListSubscriptions`, `ListDeliveries`, `Replay` are all callable on `sys`.

For full control over every port, build `events.NewService(events.Deps{...})`
directly and drive the queue yourself (`queue.Start(ctx, svc)`). See
[CONSUMING.md](../../../CONSUMING.md) section 2b.

## The typed interface (what a service calls)

```go
type Emitter interface {
    Emit(ctx, ev Event) error
    Subscribe(ctx, in NewSubscription) (Subscription, error)
    Unsubscribe(ctx, orgID, subscriptionID string) error
    ListSubscriptions(ctx, orgID string) ([]Subscription, error)
    ListDeliveries(ctx, orgID string, f DeliveryFilter) (page []Delivery, nextCursor string, err error)
    Replay(ctx, orgID, deliveryID string) error
}
```

Full reference: [docs/INTERFACE.md](../../../docs/INTERFACE.md). Each seam's
behavioral contract (caller, concurrency, tenancy, errors, implementers) is in
[docs/CONTRACTS.md](../../../docs/CONTRACTS.md).

## Verifying a webhook (subscriber side)

```go
body, _ := io.ReadAll(r.Body)
err := hmacsign.VerifyRequest(secret, body,
    r.Header.Get(events.HeaderSignature),  // X-Signature
    r.Header.Get(events.HeaderTimestamp),  // X-Event-Timestamp
    time.Now(), 5*time.Minute)             // tolerance window = anti-replay
if err != nil { http.Error(w, "bad signature", 400); return }
// dedup on r.Header.Get(events.HeaderDelivery) — X-Delivery-Id (stable, idempotent)
```

## Non-negotiable rules (enforce these)

- **Tenancy.** Subscriptions/deliveries are hard-partitioned by `OrgID`. Always
  pass the authenticated caller's `orgID`; never let admin endpoints read/write
  across orgs. The core re-asserts `event.OrgID == subscription.OrgID` on every
  job — do not weaken it.
- **Secrets.** Per-subscription, generated via `hmacsign.GenerateSecret()`, stored
  encrypted at rest (KMS/SSE), never logged. `Subscription.Secret` is `json:"-"`.
- **SSRF.** Subscription URLs are attacker-influenced. Keep the deliverer's guard
  ON for internet-facing use (`httpdeliver.NewSecure()`, the facade default);
  additionally set `Options.ValidateURL` for subscribe-time vetting. Never set
  `GuardPrivate: false` in production. See [SECURITY.md](../../../SECURITY.md).

## Choosing adapters

| Port | Dev / single-process | Production |
|---|---|---|
| Store + Log | `memory` | `dynamo` (tables PK `ORG#<org>`, SK `SUB#`/`DLV#`) |
| Queue | `inproc` (volatile) | `sqsqueue` (durable), or your own via `events.DeliveryQueue` + `ProcessRef` |
| Deliverer | `httpdeliver.New({GuardPrivate:false})` for `httptest`/localhost | `httpdeliver.NewSecure()`, optionally `ratelimit.WrapDeliverer(...)` |
| Clock/IDGen/Rand | `std` | `std` |

## Production hardening (v0.2)

- **Crash recovery:** call `sys.Recover(ctx, orgID)` at startup for each active org
  to re-enqueue pending/failed deliveries the volatile queue lost.
- **Standard Webhooks interop:** pass `Signer: stdwebhook.New()` so subscribers
  verify with any Standard Webhooks library; verify with `stdwebhook.VerifyRequest`.
- **Metrics + auto-disable:** `Observer: events.MultiObserver(metrics.New(),
  autodisable.New(store, autodisable.Config{Threshold: 10, OnDisable: notify}))`.
- **Admin API + HTML UI:** mount `adapters/adminhttp.New(sys, orgResolver)` behind
  your auth for subscription CRUD, secret rotation (`RotateSecret`), delivery
  list/detail, replay, and `GET /ui`. Org always comes from the resolver, never
  the client.

## Extending: write a new adapter

Implement the relevant port interface in a package under `adapters/`, add a
compile-time assertion `var _ events.<Port> = (*T)(nil)`, and unit-test it
without external infra where possible (see `adapters/dynamo` and
`adapters/sqsqueue` using fake clients). Do not add dependencies beyond stdlib +
aws-sdk-go-v2. The core never changes when you swap adapters.

**Durable queue pattern (any backend):** for an off-process queue (Redis,
Postgres, Kafka), enqueue only `job.Ref()` (an id-only, secret-free
`events.JobRef`), and in your consumer call `svc.ProcessRef(ctx, ref)` — it
rehydrates the full job from the store + log and is at-least-once safe (drops
terminal/missing refs). `adapters/sqsqueue` is the reference implementation;
never put the payload or secret on the wire.

## Retry & delivery semantics

- Backoff = `BaseDelay * Multiplier^(attempt-1)`, capped at `MaxDelay`, ±`JitterFrac`.
- `RetryPolicy.RetryOn` classifies failures; default retries 5xx/429/408/transport
  and dead-letters other 4xx immediately (a rejected payload won't self-heal).
- Delivery is at-least-once; consumers dedup on the stable `X-Delivery-Id`.
- `Replay(orgID, deliveryID)` re-enqueues a past event from its stored snapshot.

## Proof bar (run before claiming done)

```
cd libs/ab0t-events-go
go build ./... && go test ./... && go test -race ./...
```

Core tests use only fakes (no real I/O). The integration test drives a real HTTP
delivery to an `httptest` server: 2xx logged as delivered; non-2xx retried then
dead-lettered. Keep the per-org isolation, retry→dead-letter, and HMAC
round-trip tests meaningful.
