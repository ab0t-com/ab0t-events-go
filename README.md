# ab0t-events-go

[![CI](https://github.com/ab0t-com/ab0t-events-go/actions/workflows/ci.yml/badge.svg)](https://github.com/ab0t-com/ab0t-events-go/actions/workflows/ci.yml)
[![Go Reference](https://pkg.go.dev/badge/github.com/ab0t-com/ab0t-events-go.svg)](https://pkg.go.dev/github.com/ab0t-com/ab0t-events-go)
[![Go Report Card](https://goreportcard.com/badge/github.com/ab0t-com/ab0t-events-go)](https://goreportcard.com/report/github.com/ab0t-com/ab0t-events-go)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)

Outbound event-webhook delivery as a reusable Go library. Any ab0t service can
embed it to deliver domain events to external subscriber endpoints — Stripe /
GitHub style: a service emits a domain event, the library fans it out to that
org's subscribed endpoints, HMAC-signed, with retries, dead-lettering, a
delivery log, and replay.

Module: `github.com/ab0t-com/ab0t-events-go`. Pure Go — stdlib plus
`aws-sdk-go-v2` (only for the optional DynamoDB adapter). Builds fully offline.

## Install

```sh
go get github.com/ab0t-com/ab0t-events-go@latest
```

Requires Go 1.23+. Import the packages you need:

```go
import (
    "github.com/ab0t-com/ab0t-events-go/events"            // core: types, Emitter, ports
    "github.com/ab0t-com/ab0t-events-go/webhooks"          // batteries-included facade
    "github.com/ab0t-com/ab0t-events-go/adapters/memory"   // in-mem store + log
    "github.com/ab0t-com/ab0t-events-go/adapters/dynamo"   // DynamoDB store + log
    "github.com/ab0t-com/ab0t-events-go/adapters/hmacsign" // HMAC signer + verify
    "github.com/ab0t-com/ab0t-events-go/adapters/adminhttp" // admin API + UI
)
```

## Why a library

Domain events (`drive.node.created`, `calendar.event.created`,
`mail.message.sent`, …) are emitted on an internal bus today and consumed only
internally. Every service will eventually want to deliver them outbound to
customer endpoints. This builds that once, behind a clean typed interface and
hexagonal ports/adapters, so a service wires it in a few lines and swaps adapters
(store, queue, signer, deliverer) per environment.

## Architecture — ports & adapters (hexagonal)

```
                       ┌─────────────────────────────────────┐
   service ──Emit──▶   │   events  (pure core, no I/O)        │
                       │   • domain types                     │
                       │   • Emitter (driving API)            │
                       │   • fan-out / retry / dead-letter    │
                       │   • replay / idempotency / tenancy   │
                       └──┬───────┬───────┬───────┬───────┬───┘
              PORTS       │       │       │       │       │
        SubscriptionStore │ Queue │Deliverer│Signer│  Log  │  + Clock/IDGen/Rand
                          ▼       ▼       ▼       ▼       ▼
   ADAPTERS        dynamo/    inproc/  httpdeliver/ hmacsign/  dynamo/
                   memory/                                     memory/
```

The **core** (`events/`) performs no I/O and never calls `time.Now` or any rand
source directly — all non-determinism arrives through the `Clock`, `IDGen`, and
`Rand` ports — so the whole fan-out / retry / dead-letter / replay machinery is
unit-testable with fakes.

### Packages

| Package | Role |
|---|---|
| `events` | Core: domain types, the `Emitter` driving interface, the ports, the optional event-type `Catalog`, and the `Service` that implements fan-out + retry + dead-letter + replay. Pure. |
| `webhooks` | Batteries-included facade — `New(ctx, Options)` wires the emitter to production defaults; only `Store`+`Log` required. |
| `adapters/hmacsign` | `Signer`: HMAC-SHA256 over `{deliveryId}.{timestamp}.{body}` (`sha256=<hex>`), binding the id + timestamp so they can't be forged; subscriber-side `Verify` / `VerifyRequest` (timestamp tolerance) + `GenerateSecret`. |
| `adapters/httpdeliver` | `Deliverer`: `net/http` POST with timeouts, no-redirect, an SSRF guard (`NewSecure`), and response-snippet capture on failure. |
| `adapters/inproc` | `DeliveryQueue`: a time-ordered scheduler + worker pool that drives the core `Dispatcher`. Honours backoff `NotBefore`. |
| `adapters/sqsqueue` | Durable `DeliveryQueue` over Amazon SQS — id-only messages, rehydrated from store+log (secrets never queued). |
| `adapters/memory` | In-memory `SubscriptionStore` + `DeliveryLog` (reference / tests / single-process). |
| `adapters/dynamo` | DynamoDB `SubscriptionStore` + `DeliveryLog`, org-partitioned. |
| `adapters/std` | Production `Clock` / `IDGen` / `Rand`. |
| `adapters/stdwebhook` | Standard Webhooks `Signer` + verify — cross-language interop. |
| `adapters/metrics` | `Collector` (`Observer`) — attempts/latency/success-rate `Snapshot`. |
| `adapters/autodisable` | `Breaker` (`Observer`) — auto-pause endpoints that keep dead-lettering. |
| `adapters/adminhttp` | Drop-in admin HTTP API + HTML delivery-log UI (stdlib); optional `GET /event-types` via `WithCatalog`. |
| `adapters/ratelimit` | `Deliverer` decorator — per-destination token-bucket throttling. |
| `adapters/securestore` | `SubscriptionStore` decorator — encrypts `Subscription.Secret` at rest via a `SecretCodec` (encrypt on Put, decrypt on Get/List). |
| `adapters/aesgcm` | `SecretCodec`: stdlib AES-256-GCM (you hold a 32-byte key). |
| `adapters/kmscodec` | `SecretCodec`: AWS KMS (key material stays in KMS; bring your own client). |
| `examples/quickstart` | The full wire-in example. |

### Quickstart (the facade)

```go
sys, err := webhooks.New(ctx, webhooks.Options{
    Store: memory.NewStore(), // or dynamo.NewStore(ddb, "subs")
    Log:   memory.NewLog(),   // or dynamo.NewLog(ddb, "deliveries")
})
if err != nil { log.Fatal(err) }
defer sys.Close()

secret, _ := hmacsign.GenerateSecret()
sys.Subscribe(ctx, events.NewSubscription{
    OrgID: "org_123", EventTypes: []string{"drive.*"},
    URL: "https://customer.example/webhooks", Secret: secret,
})
sys.EmitNew(ctx, "drive.file.created", "org_123", map[string]any{"fileId": "f_1"})
```

`webhooks.New` defaults the deliverer (SSRF guard on), signer, worker pool,
retry policy, and Clock/IDGen/Rand — override any of them via `Options`. For full
control, build `events.NewService(events.Deps{...})` yourself.

## Advanced features

- **HMAC signing** with a per-subscription secret (raw-body HMAC-SHA256); a
  verification helper and `GenerateSecret()` ship for subscribers/admins.
- **Secret encryption at rest** — wrap the store in `adapters/securestore` with a
  `SecretCodec` (`aesgcm` stdlib AES-256-GCM, or `kmscodec` AWS KMS) so the
  backing table holds only ciphertext.
- **Retries** with exponential backoff + jitter, a max-attempts cap, a pluggable
  retryable classifier (default: retry 5xx/429/408/transport, dead-letter other
  4xx), then **dead-letter**.
- **Delivery log** — every attempt (code, latency, error, a response snippet),
  queryable per org / per subscription with a cursor.
- **Content filters** — a subscription can add `PayloadFilters` (dot-path ==
  value, AND-combined) so it receives only events whose payload matches, on top of
  the event-type filter.
- **Custom headers / tracing** — `Event.Headers` rides along on every delivery
  (persisted in the snapshot, durable-safe) for W3C `traceparent` propagation with
  no OTel dependency; reserved idempotency/auth headers can't be overridden.
- **Per-endpoint transforms** — an optional `Deps.Transformer` reshapes the
  outbound body per subscription before signing, so different subscribers can get
  different payload shapes from one canonical event.
- **Per-org subscriptions** — event types via exact match, the global wildcard
  `*`, or hierarchical prefixes like `calendar.event.*`; active/paused.
- **Idempotency** — a stable delivery id per `(subscription, event)`; subscribers
  dedup on `X-Delivery-Id`.
- **Replay** — re-deliver a past event by delivery id (admin).
- **At-least-once** delivery carrying `X-Event-Id` / `X-Delivery-Id`.
- **Anti-replay** — `X-Event-Timestamp` + a tolerance window verified by
  `hmacsign.VerifyRequest`.
- **Observability** — an optional per-attempt `Observer` hook + a ready metrics
  collector; compose hooks with `MultiObserver`.
- **Crash recovery** — `Recover(ctx, orgID)` re-enqueues in-flight deliveries from
  the durable log after a restart.
- **Durable queue** — swap the in-process queue for SQS (or your own Redis/
  Postgres/Kafka adapter); id-only messages, secrets never queued.
- **Interop** — swap in the Standard Webhooks signer for off-the-shelf verifiers.
- **Auto-disable** — pause endpoints that keep dead-lettering.
- **Admin surface** — drop-in HTTP API + HTML delivery-log UI (see `adapters/adminhttp`).
- **Ergonomics** — `EmitNew(ctx, type, org, payload)` builds and emits in one call;
  `RotateSecret` for key rotation.

## Tenancy & security (hard requirements)

- **Per-org partitioning.** Subscriptions and deliveries are keyed by `OrgID`.
  The store is the tenancy boundary (`ListForOrgAndType` only ever returns one
  org's subs); the dispatcher **re-asserts** `OrgID` equality on every job — an
  org's event can never reach another org's endpoint.
- **Secrets are per-subscription and never logged.** `Subscription.Secret` is
  `json:"-"`, never placed in the delivery log, and never put on a durable queue.
  For encryption at rest, wrap the store in `adapters/securestore` (`SecretCodec`:
  `aesgcm` or `kmscodec`).
- **SSRF guard.** A subscription URL is attacker-influenced. `httpdeliver`'s
  guarded mode (`NewSecure`) permits only public global-unicast addresses —
  rejecting loopback/link-local/private/metadata plus special-use ranges (CGNAT
  `100.64.0.0/10`, `0.0.0.0/8`, NAT64 `64:ff9b::/96`, IETF test/benchmark) — pins
  the dial to the vetted IP (defeating DNS rebinding), does not follow redirects,
  and (via `RequireHTTPS`, on by default in `NewSecure`) refuses plaintext
  `http://` to non-loopback hosts. Keep it on for internet-facing endpoints.
  Validate URLs again at `Subscribe` time via `events.Deps.ValidateURL`.

## Proof

```
cd libs/ab0t-events-go
go build ./...
go test ./...
go test -race ./...
```

Core tests use only fakes (no real I/O). The integration test
(`test/integration_test.go`) drives a real HTTP delivery to a local `httptest`
server: a 2xx is logged as delivered; a non-2xx is retried then dead-lettered.

## Wiring it in

- [CONSUMING.md](./CONSUMING.md) — the ~10-line wire-in and production recipes.
- [docs/CONTRACTS.md](./docs/CONTRACTS.md) — every seam (port) and its contract:
  who calls it, concurrency, tenancy, error semantics, and implementers.
- [docs/INTERFACE.md](./docs/INTERFACE.md) — the full typed-interface reference.
