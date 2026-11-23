# Changelog

All notable changes to this project are documented here. The format is based on
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and this project adheres
to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.2.1] - 2026-11-23

Bug fixes: no duplicate webhook from a redelivered failed attempt, retries use the current subscription, and a
shutdown is never recorded as a failed delivery. **No API removed, no signature changed, no source change
required**; some behaviour you can observe changes (below). Client notice and an advisory checker:
`migrations/v0.2.0-to-v0.2.1/`.

### Fixed
- **`ProcessRef` drops a stale duplicate of a failed attempt** (durable queues such as `sqsqueue`). A redelivered
  copy of attempt N whose retry is already scheduled (`Status=failed`, `ref.AttemptN <= Delivery.Attempts`)
  is now dropped instead of re-POSTing attempt N and forking a second retry chain. To keep this from ever
  dropping an attempt whose retry was NOT scheduled (a failed `Enqueue`, a crash between the log write and the
  enqueue — the delivery would be lost), the durable path now writes a failed attempt in two steps: the attempt
  is appended with the delivery's count at N-1, and the count advances to N only after the retry is enqueued
  (skipped when the retry is due within 5 s; the count then catches up when that retry runs). `Process`
  (in-process queues), the Observer, and the at-rest state of delivered/dead deliveries are unchanged. Two
  copies processed *concurrently* can still both POST (that needs a lease; not changed).
- **`sqsqueue` stops dispatching a received batch once stopped.** The rest of the batch is left undeleted
  and reappears after its visibility timeout, instead of being dispatched on a cancelled context.
- **In-process retries use the current subscription.** `Process` re-reads the subscription from the store for
  a retry (`AttemptN > 1`): after `RotateSecret` the retry is signed with the new secret; after `Unsubscribe` it
  is dropped without a POST — matching the durable path. A store error (other than `ErrNotFound`) keeps the
  job's copy, as before. First attempts and `ProcessRef` do no extra read.
- **`ratelimit.ByHost` uses a canonical key.** Lower-cased host, trailing root dot dropped, scheme-default port
  (`https:443`, `http:80`) dropped, so spellings of one receiver share one bucket. Non-default ports and IPv6
  brackets are kept.
- **A cancelled attempt is not a delivery verdict.** When the POST got no answer and the context was
  *cancelled* (queue stop / `webhooks.System.Close`), `Process` records nothing, does not call the Observer,
  schedules no retry and returns `ctx.Err()`; the delivery keeps its previous pending/failed state (run
  `Recover` at start for in-process queues; durable queues redeliver). A final attempt is therefore no longer
  dead-lettered — and counted by `autodisable` — because of your own shutdown. A deadline is still a failed
  attempt.
- **`webhooks.System.Close` is safe to call concurrently** (`sync.Once`; a second caller waits for the first).

### What you might notice
Nothing needs changing unless your code depends on one of these:
- **Durable path:** `Delivery.Attempts` reads N-1 while a failed attempt's retry is not (yet) armed, and each
  durable retry scheduled ≥ 5 s out costs one extra `DeliveryLog.Record` with no attempt (`Attempt.N == 0`). A
  custom `DeliveryLog` must treat that as "update the delivery, append nothing" — the documented contract the
  shipped `memory` and `dynamo` logs already follow.
- **In-process retries** do one `SubscriptionStore.Get` each; a retry after `Unsubscribe` is not sent.
- **`ratelimit.ByHost`** keys change for the spellings above (bucket state is in-memory only).
- **Shutdown:** an attempt cancelled by `Close` / queue stop is not in the delivery log and not seen by the
  Observer. `sqsqueue` reports the cancellation to `OnError` and leaves the message for redelivery. A custom
  queue adapter that calls `Process` / `ProcessRef` gets `context.Canceled` back and should treat the job as not
  done. With the in-process queue, call `Recover` at start to send what a shutdown interrupted.

## [0.2.0] - 2026-11-22

First tagged release. **Breaking** for `hmacsign` callers (see Finding 1 below); everything else is additive.

### Fixed
- **`inproc` never dispatches a phantom empty job on stop.** The scheduler closes the work channel when the
  queue stops; a worker that was busy in `Process` could loop back to a `select` where both `ctx.Done()` and
  the closed channel were ready and dispatch the closed channel's zero `Job` (empty delivery id). Workers now
  return when the channel is closed. Found by `make check` (race); `TestQueue_StopNeverDispatchesAPhantomJob`
  fails without the fix (proven on a scratch copy).

### Security (audit 2026-08-12 remediation — Findings 1–4)
- **`hmacsign` now binds the timestamp and delivery id into the signature**
  (Finding 1, High). The MAC covers `{deliveryID}.{timestamp}.{body}` instead of
  the body only, so a captured request can't be replayed with a refreshed
  timestamp or a forged delivery id. **Breaking:** `Signature`, `Verify`, and
  `VerifyRequest` now take the delivery id and timestamp (mirroring `stdwebhook`).
- **SSRF guard is now an allowlist** (Finding 2, Medium). `httpdeliver` permits
  only public global-unicast addresses and additionally blocks CGNAT
  `100.64.0.0/10`, `0.0.0.0/8`, NAT64 `64:ff9b::/96` (+`64:ff9b:1::/48`), and the
  IETF test/benchmark ranges; the doc comment now matches the behavior.
- **HTTPS-only option** (Finding 3, Medium). New `httpdeliver.Config.RequireHTTPS`
  (on by default in `NewSecure`) rejects plaintext `http://` (except to loopback)
  with `ErrInsecureScheme`.
- **Bounded in-process queue** (Finding 4, Medium). New `inproc.Config.MaxDepth`
  (`DefaultMaxDepth` 10000; negative = unbounded); `Enqueue` returns the new
  `ErrQueueFull` at capacity (backpressure) instead of growing memory without
  bound.
- **Absolute retry-delay ceiling** (Finding 6, Low). `Process` clamps every retry
  delay to the new `events.AbsoluteMaxRetryDelay` (24 h) even when
  `RetryPolicy.MaxDelay == 0`, so a hostile `Retry-After` can't park a job.
- **Replay works on durable queues** (Finding 7, Low). `Replay` now persists a
  `pending` status to the log before enqueuing, so a rehydrated (SQS) job is
  non-terminal and re-delivers instead of being dropped as a duplicate.
- **Rate-limiter bucket eviction** (Finding 8, Low). `ratelimit.Limiter` evicts
  buckets idle for 10 min (swept ≤ once/min) so the per-host map can't grow
  unbounded; `Limiter.NumBuckets()` exposes the footprint.
- **Optional Emit payload cap** (Finding 9, Low). New optional
  `events.Deps.MaxPayloadBytes`; `Emit` rejects oversized payloads up front with
  the new `ErrPayloadTooLarge` (0 = unbounded, non-breaking).
- Findings 5, 10, 11, 12, 13 and the `autodisable` half of 8 were triaged and
  **ACCEPTED** (by-design / accepted-risk) with rationale + residual-risk notes in
  `docs/SECURITY-AUDIT_20260812.md`.

### Added (v0.2 — "Production-ready & interoperable")
- **Pluggable signing schemes.** The `Signer` port now takes a `SigningInput`
  (id, delivery id, timestamp, body, secret) and returns the auth headers, so a
  scheme can bind the signature to id + timestamp and own its headers.
- **Standard Webhooks adapter** (`adapters/stdwebhook`) — `webhook-id` /
  `webhook-timestamp` / `webhook-signature: v1,<base64>` over `id.ts.body`, with
  `Verify` / `VerifyRequest`. Interop with any Standard Webhooks verifier.
- **Startup recovery sweeper** — `Service.Recover(ctx, orgID)` re-enqueues
  non-terminal (pending/failed) deliveries from the log after a restart. New
  `DeliveryFilter.Status`.
- **Metrics observer** (`adapters/metrics`) — dependency-free `Collector`
  tallying attempts by status/code + latency, with `Snapshot`, `MeanLatency`,
  `SuccessRate`. `events.MultiObserver` composes observers.
- **Auto-disable circuit breaker** (`adapters/autodisable`) — pauses a
  subscription after N consecutive dead-letters and fires an `OnDisable` hook.
- **Admin HTTP surface** (`adapters/adminhttp`) — stdlib handler for subscription
  CRUD, secret rotation, delivery list/detail, replay, plus a self-contained HTML
  delivery-log UI; org taken from an injectable resolver (never client-trusted).
- **Secret rotation** — `Service.RotateSecret(ctx, orgID, subID)`.
- **Detail lookups** — `Service.GetSubscription` and `Service.GetDelivery`.
- **Retry-After** — the deliverer parses a `Retry-After` header (seconds or HTTP
  date) into `DeliveryResponse.RetryAfter`; the core honors it as a lower bound on
  the next backoff, capped by `MaxDelay`.
- **Ping / test-event** — `Service.Ping(ctx, orgID, subID)` delivers a synthetic
  `webhook.ping` to one endpoint (bypassing the type filter) so orgs can verify an
  endpoint; exposed as `POST /subscriptions/{id}/ping`.
- **User-Agent** — outbound requests send a stable `User-Agent`
  (`httpdeliver.DefaultUserAgent`, overridable via `Config.UserAgent`).
- **Benchmarks** for Emit fan-out and Process.
- **Repo/GitHub scaffolding** — CI + release workflows, Dependabot, issue/PR
  templates, CODEOWNERS, code of conduct, README badges + install instructions.
- **Durable queue (SQS)** — `adapters/sqsqueue`, a durable `DeliveryQueue` that
  survives restarts. Messages carry only an id-only `events.JobRef` (never the
  payload or secret); the consumer rehydrates the full job from the store + log.
  Bring your own SQS client via the `SQS` interface (no infra assumptions).
- **Reusable durable-queue primitives** — `events.JobRef`, `Job.Ref()`,
  `events.Rehydrator`, `events.RefDispatcher`, and `Service.Rehydrate` /
  `Service.ProcessRef` (rehydrate + dispatch; drops terminal/missing references,
  making at-least-once queues duplicate-safe). Write a Redis/Postgres/Kafka queue
  the same way.
- **Per-destination rate limiting** — `adapters/ratelimit`, a token-bucket
  `Deliverer` decorator (keyed by host by default) that throttles bursts and
  respects the context deadline. Stdlib-only.
- **Seams & contracts doc** — `docs/CONTRACTS.md` documents every port's
  behavioral contract: caller, concurrency, tenancy, errors, and implementers.
- **Event catalog** — `events.Catalog`, a pure, concurrency-safe
  registry of the event types a service publishes
  (`EventTypeDef{Type, Description, Example, Deprecated}`;
  `Register`/`Get`/`Has`/`List`). Wire it via `Deps.Catalog`; a **strict** catalog
  (`NewCatalog(true)`) makes `Emit` reject undeclared types with the new
  `ErrUnknownEventType` before any store/queue I/O (a non-strict catalog is
  documentation only). The admin surface serves it at `GET /event-types` via
  `adminhttp.New(api, org, adminhttp.WithCatalog(cat))`.
- **Secret encryption at rest** — a new `events.SecretCodec` seam
  (`Encrypt`/`Decrypt`) plus `adapters/securestore`, a `SubscriptionStore`
  decorator that encrypts `Subscription.Secret` on `Put` and decrypts it on
  `Get`/`ListForOrg`/`ListForOrgAndType` (adds no cross-org path; an empty secret
  passes through). Two codecs ship: `adapters/aesgcm` (stdlib AES-256-GCM,
  caller-held 32-byte key, random nonce per encrypt) and `adapters/kmscodec` (AWS
  KMS, bring-your-own client, optional `EncryptionContext`). `go.mod` adds
  `service/kms v1.38.3` (cached, offline).
- **Payload / content filters** — `Subscription.PayloadFilters
  []PayloadFilter{Path, Equals}` (also on `NewSubscription` and the admin
  create-subscription body). During `Emit` fan-out a subscription is delivered
  only when its type filter matches **and** every payload filter matches the event
  payload (dot-path equality, dependency-free `encoding/json`); a filtered-out
  subscription gets no delivery (no log record, no queue job). Empty filters
  preserve today's behavior. `Subscription.MatchesPayload([]byte) bool` is
  exported for previews. Persisted by the `memory` and `dynamo` stores.
- **Custom / tracing headers** — `Event.Headers map[string]string`
  is merged onto every outbound delivery and persisted in the event snapshot
  (durable-safe: survives the SQS `JobRef` round trip and replays). Reserved
  headers the core/signer own (`Content-Type`, `Host`, `Content-Length`, the
  `X-Event-*`/`X-Delivery-Id`/`X-Attempt`/`X-Signature` headers, and the Standard
  Webhooks `webhook-*` names) cannot be overridden by a custom header. Enables W3C
  `traceparent` propagation with no OpenTelemetry dependency.
- **Per-endpoint transform hook** — an optional
  `events.Transformer` (`Deps.Transformer`, plus `TransformerFunc`) reshapes the
  outbound body per subscription in `Process`, **before** signing (so the
  signature covers what is sent). It receives the destination `Subscription` to
  branch per endpoint, shapes the body only (reserved/auth headers stay
  protected), and a transform error aborts the attempt without a POST (recorded as
  a failed attempt, retried by policy). Nil delivers the payload verbatim.

### Deferred
- **Batching** — deferred; it does not fit the per-(subscription,
  event) idempotency / per-delivery-log / single-message-signing model without a
  high-blast-radius rewrite, and the slow-subscriber driver is already served by
  `adapters/ratelimit`.

### Changed
- `Signer.Sign` signature changed (see above) — **breaking** for custom signers;
  `hmacsign` updated (raw signature now via `hmacsign.Signature`).

## [0.1.0] - 2026-08-12

Initial release: outbound event-webhook delivery as a reusable, hexagonal Go
library.

### Added
- **Core (`events`)** — pure, I/O-free domain: `Event`, `Subscription`,
  `Delivery`, `Attempt`, `RetryPolicy`, the `Emitter` driving interface, and the
  `Service` implementing fan-out → sign → deliver → retry → dead-letter → log.
- **Ports** — `SubscriptionStore`, `DeliveryQueue`, `Deliverer`, `Signer`,
  `DeliveryLog`, and the testability ports `Clock`, `IDGen`, `Rand`. The core
  never calls `time.Now`/`rand` directly.
- **Adapters** — `hmacsign` (HMAC-SHA256 + verify + `GenerateSecret`),
  `httpdeliver` (net/http, timeouts, SSRF guard, no-redirect, response-snippet
  capture), `inproc` (time-ordered worker pool honouring backoff), `memory`
  (in-mem store + log), `dynamo` (org-partitioned DynamoDB store + log), `std`
  (system Clock/IDGen/Rand).
- **Facade (`webhooks`)** — batteries-included `New(ctx, Options)` that wires the
  emitter to production defaults; only `Store` and `Log` are required.
- **Per-org tenancy** — hard partitioning by `OrgID` in the store plus a
  dispatcher-level org re-assertion; an org's event can never reach another org's
  endpoint.
- **Idempotency** — stable `DeliveryID(subscription, event)`; `X-Delivery-Id`
  header for consumer dedup.
- **Retries** — exponential backoff + jitter, a pluggable `RetryOn` classifier
  (default: retry 5xx/429/408/transport, dead-letter other 4xx), then
  dead-letter.
- **Delivery log & replay** — every attempt (code, latency, error) recorded and
  cursor-paginated; `Replay(orgID, deliveryID)` re-delivers a past event.
- **Event-type matching** — exact, global `*`, and hierarchical prefix wildcards
  (`calendar.event.*`).
- **Anti-replay** — `X-Event-Timestamp` + `hmacsign.VerifyRequest` tolerance
  window.
- **Observability** — optional `Observer` hook fired per attempt.
- **Ergonomics** — `Service.NewEvent` / `EmitNew` build-and-emit helpers.
- Full core unit tests with fakes, an integration test over `httptest`, README,
  CONSUMING, and the typed-interface reference.

[Unreleased]: https://github.com/ab0t-com/ab0t-events-go/compare/v0.2.1...HEAD
[0.2.1]: https://github.com/ab0t-com/ab0t-events-go/compare/v0.2.0...v0.2.1
[0.2.0]: https://github.com/ab0t-com/ab0t-events-go/releases/tag/v0.2.0
