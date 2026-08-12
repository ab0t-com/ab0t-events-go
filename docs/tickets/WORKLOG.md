# Worklog — ab0t-events-go

Append-only. Newest entries at the bottom of each day.

## 2026-08-12

- v0.1.0 core + adapters + facade shipped; 41 tests, race-green, 75.6% cov.
- PMM report written (`docs/PMM-REPORT_2026-08-12.md`); v0.2 roadmap ICE-ranked.
- Opened v0.2 tickets EV-001..EV-007 (`docs/tickets/v0.2-tickets.md`).

### Execution (autonomous)
- EV-001 DONE — Generalized `Signer` port to `Sign(SigningInput) map[string]string`.
  Core now sets common idempotency headers and merges signer-owned auth headers.
  Reworked `hmacsign` to the new interface (X-Signature + X-Event-Timestamp),
  updated fakes/tests/docs. Race-green.
- EV-002 DONE — `adapters/stdwebhook`: Standard-Webhooks scheme
  (`webhook-id`/`webhook-timestamp`/`webhook-signature: v1,<base64>` over
  `id.ts.body`) + `Verify`/`VerifyRequest`. Round-trip + tamper + stale-ts tests.
- EV-003 DONE — Added `DeliveryFilter.Status`; `Service.Recover(ctx, orgID)`
  re-enqueues non-terminal deliveries from the log (rebuilds Job from snapshot +
  live subscription). Recovery test simulates a restart and redelivers.
- EV-004 DONE — `adapters/metrics.Collector` (Observer): attempts by status/code,
  latency sum/max, delivered/dead/failed tallies; `Snapshot()`. Tested.
- EV-005 DONE — `adapters/autodisable.Breaker` (Observer): consecutive dead-letter
  count per sub, resets on delivered, disables via store + `OnDisable` hook at
  threshold. `events.MultiObserver` composes observers. Tested (fires once).
- EV-006 DONE — `adapters/adminhttp`: stdlib mux handler — subscription CRUD,
  rotate-secret, delivery list/detail, replay, and a self-contained HTML
  delivery-log view. Org from an injectable `OrgResolver` (never client-trusted).
  Handler tests cover CRUD, replay, tenancy rejection, and the HTML page.
- EV-007 DONE — `Service.RotateSecret(ctx, orgID, subID)` mints + persists a new
  secret; wired into the admin API. Tenancy-enforced; tested.
- Docs updated (README, CONSUMING, INTERFACE, CHANGELOG, SKILL, PMM). 
- **Proof (v0.2):** all green offline. 53 tests / 14 packages. EV-001..EV-007 DONE.

### Continuation — best-practice pass + EV-008..EV-010
- EV-008 DONE — `Retry-After` parsed by `httpdeliver` (`DeliveryResponse.RetryAfter`);
  core honors it as a backoff lower bound, capped by `MaxDelay`. Tests added.
- EV-009 DONE — `Service.Ping` (synthetic `webhook.ping`, bypasses filter, cross-org
  denied) + `POST /subscriptions/{id}/ping`. Core + admin tests.
- EV-010 DONE — stable `User-Agent` (overridable) + Emit/Process benchmarks.
- Best-practice review: added CSRF caveat to the admin UI (state-changing Replay
  form), documented Retry-After cap, UA override.
- **Proof:** all green offline. 61 tests + 2 benchmarks / 14 packages.

### Continuation — EV-011: durable SQS queue (v0.3 flagship)
- Added reusable core primitives so the durable pattern isn't SQS-specific:
  `events.JobRef` + `Job.Ref()` (id-only, secret-free), `events.Rehydrator` and
  `events.RefDispatcher`, and `Service.Rehydrate` / `Service.ProcessRef`.
  ProcessRef drops references that are terminal (duplicate) or whose
  delivery/subscription is gone — making at-least-once queues duplicate-safe.
- `adapters/sqsqueue`: durable `DeliveryQueue` over SQS. Bring-your-own client via
  the `SQS` interface (no region/creds/endpoint/FIFO assumptions). Enqueue sends a
  JSON `JobRef` with a delay derived from NotBefore (capped at 900s); the consumer
  re-defers longer backoffs via ChangeMessageVisibility; deletes only on
  success/deliberate-drop; leaves messages for redelivery on infra errors.
- Tests: fake in-memory SQS client (offline) — id-only/no-secret assertion, delay
  cap, success-deletes, infra-error-keeps, future-NotBefore-redefers, poison-drop,
  Start consume loop; plus a full end-to-end test wiring a real `events.Service`
  through the SQS queue with memory store+log proving rehydration delivers the real
  payload/signature though the queue held only ids. Core ProcessRef/Rehydrate tests.
- Docs (README, CONSUMING "Durable delivery" + "write your own queue", INTERFACE,
  CHANGELOG, SKILL) updated. go.mod adds service/sqs v1.37.2 (cached, offline).
- **Proof:** build/vet/test/`-race` all green offline. 72 tests / 15 packages.

### Continuation — EV-012: rate limiting + contracts doc
- `adapters/ratelimit`: token-bucket `Deliverer` decorator (PMM item 11). Keyed by
  destination host (configurable `KeyFunc`), injectable clock, `Reserve` is the
  deterministic core; `Wait` blocks up to the ctx deadline (throttled requests
  surface as ctx errors → core retries). Decorators nest (wrap NewSecure).
  Tests: exact bucket math over a fake clock, per-key isolation, disabled
  pass-through, real burst throttle, ctx-cancel-stops-waiting, ByHost.
- `docs/CONTRACTS.md`: the "seams & contracts" reference — driving contracts
  (Emitter/Dispatcher/RefDispatcher), each port's contract (responsibility,
  caller, concurrency, tenancy, errors, secrets, adapters), decorators, the
  cross-cutting invariants, and the adapter selection matrix. Linked from README/
  INTERFACE.
- **Final proof:** build/vet/test/`-race` all green offline. 78 tests / 16
  packages. v0.3 remaining: OTel tracing (kept dependency-free via Observer/
  headers — deferred to avoid a hard OTel dep), event catalog/schemas, KMS secret
  encryption.

### Completion workflow — C-13: event catalog
- `events.Catalog` (new `events/catalog.go`): pure, concurrency-safe
  (`sync.RWMutex`) registry of `EventTypeDef{Type, Description, Example,
  Deprecated}` with `Register` (chainable; panics on empty Type; replace keeps
  order), `Get`, `Has`, `List` (registration order, returns a copy), and
  `Strict()`. `NewCatalog(strict bool)`.
- Wired an optional `Deps.Catalog *Catalog`. When set **and** strict, `Emit`
  rejects any event whose `Type` is not registered with the new
  `ErrUnknownEventType`, before any store/queue I/O. Non-strict or nil = no gate.
  `Ping` enqueues directly (not via `Emit`) so `webhook.ping` is never gated. Core
  stays pure — the catalog holds no clock/IO and no tenant data (service-global).
- `adapters/adminhttp`: added `GET /event-types` (auth-gated) serving the
  catalog's `List()` as `{"eventTypes":[…]}`; catalog supplied by a new
  `adminhttp.WithCatalog(cat)` option (backward-compatible variadic `New`; empty
  list when no catalog wired).
- Tests (offline, deterministic, race-safe): `events/catalog_test.go` covers
  Register/Get/Has/List, replace-keeps-order, empty-Type panic, List-is-a-copy, a
  concurrent Register/Has/List/Get goroutine race test, and the behavioral proof
  `TestEmit_StrictCatalogRejectsUnknownType` (unknown type → `ErrUnknownEventType`
  with zero enqueues; declared type → 1 job) plus non-strict/nil pass-through.
  `adapters/adminhttp`: `TestAdmin_EventTypes` (auth required; order + Deprecated
  preserved) and `TestAdmin_EventTypesEmptyWithoutCatalog`.
- Docs: CONTRACTS (`Catalog` seam), INTERFACE (Catalog section + `ErrUnknownEventType`),
  CONSUMING ("Declare an event catalog"), CHANGELOG ([Unreleased]), README package
  table. Ledger: C-13 DONE; C-14..C-18 corrected to OPEN (were wrongly DONE).
- **Proof:** `GOPROXY=off go build/vet ./...`, `gofmt -l .` empty, `go test
  -count=1 ./...` and `go test -race -count=1 ./...` all green offline.

### Completion workflow — C-14: secret encryption at rest
- New seam `events.SecretCodec` (`events/secret.go`): `Encrypt`/`Decrypt` with a
  round-trip contract; ciphertext is an opaque, storage-safe string; empty secret
  is passed through by the decorator (codec need not special-case it).
- `adapters/securestore`: a `SubscriptionStore` decorator wrapping any store with
  a codec — encrypts `Subscription.Secret` on `Put` (by value, so the caller's
  struct is never mutated), decrypts on `Get`/`ListForOrg`/`ListForOrgAndType`,
  delegates `Delete` unchanged. Adds NO cross-org path (tenancy stays the wrapped
  store's job). A failed decrypt errors rather than returning ciphertext as the
  secret. Panics if inner/codec is nil.
- `adapters/aesgcm`: stdlib-only AES-256-GCM codec (crypto/aes + crypto/cipher).
  32-byte key (`ErrKeySize` otherwise); fresh random nonce per encrypt (so equal
  plaintexts differ); output is base64(nonce||sealed) incl. the GCM auth tag, so a
  wrong key / tamper / short input fails Decrypt (`ErrMalformed` for format).
- `adapters/kmscodec`: AWS KMS codec, bring-your-own client via a 2-method `KMS`
  interface (`*kms.Client` satisfies it; mockable). Direct KMS Encrypt/Decrypt
  (secrets are well under 4KB); stored value is base64(CiphertextBlob); optional
  `EncryptionContext` (AAD) required to match on decrypt; KeyID pinned on Decrypt.
  go.mod gained `service/kms v1.38.3` (already cached) via manual require +
  `GOPROXY=off GOFLAGS=-mod=mod go mod tidy`.
- Tests (offline, deterministic, race-safe):
  - aesgcm: round-trip (incl. empty/unicode), non-determinism, wrong key size,
    wrong-key/ tamper auth failure, malformed input, concurrent Encrypt/Decrypt.
  - securestore: **TestEncryptedAtRest** — the inner store holds ciphertext (no
    `whsec_`, decrypts back to plaintext) while decorator Get/List return
    plaintext and Put does not mutate the caller; tenancy preserved (cross-org
    Get = ErrNotFound, orgB sees nothing); empty-secret passthrough; codec-error
    propagation (failed Put persists nothing; failed decrypt never leaks
    ciphertext); nil-arg panics.
  - kmscodec: fake in-memory KMS (offline) round-trip + base64 assertion + call
    counts + KeyID; EncryptionContext-must-match; malformed base64; nil-config
    panics; drop-in with securestore.
- Docs: CONTRACTS (`SecretCodec` + `securestore` seams, SubscriptionStore secret
  note, matrix row), INTERFACE (Secret-encryption section + adapter list +
  Deps.Catalog note), CONSUMING ("Encrypt subscription secrets at rest"), README
  (3 package rows, feature bullet, security note), CHANGELOG. Ledger: C-14 DONE.
- **Proof:** `GOPROXY=off go build/vet ./...`, `gofmt -l .` empty, `go test
  -count=1 ./...` and `go test -race -count=1 ./...` all green offline (19 pkgs).

### Completion workflow — C-15: payload / content filters
- New `events.PayloadFilter{Path, Equals}` (`events/payloadfilter.go`) + field
  `Subscription.PayloadFilters []PayloadFilter` (json `payloadFilters,omitempty`).
  Dependency-free dot-path matcher over `encoding/json` with `UseNumber()` so
  numbers compare by exact source text; string→literal, bool→"true"/"false",
  null→"null"; a missing path or a non-scalar (object/array) value never matches.
- `Subscription.MatchesPayload([]byte) bool` — AND across all filters; empty list
  matches every payload (backward compatible). Exported for admin/UI previews.
- Emit fan-out: after the store's type match and the tenancy re-assert, a
  subscription whose payload filters don't match is skipped BEFORE any delivery is
  created — no log record, no queue job. Tenancy/secret/SSRF paths untouched.
- Plumbed through `NewSubscription.PayloadFilters` (copied defensively in
  Subscribe), the `dynamo` store item (round-trips via attributevalue), and the
  adminhttp create-subscription body (`payloadFilters`). memory store already
  persists the whole struct.
- Tests (offline, deterministic, race-safe): payloadfilter_test.go — a truth
  table over the matcher (nested paths, numbers/bools/null, absent key, non-object
  descent, object/array non-scalar, empty path); MatchesPayload AND + empty +
  invalid-payload; and the behavioral proof **TestEmit_PayloadFilterFanOut**
  (three same-type subs; only the payload-matching and no-filter subs get a job,
  the non-matching one gets neither a queue job nor a log record). dynamo
  TestStore_PayloadFiltersRoundTrip; adminhttp TestAdmin_CreateWithPayloadFilters.
- Docs: INTERFACE (Subscription + PayloadFilter + MatchesPayload), CONTRACTS (Emit
  fan-out rule), CONSUMING ("Content filters"), README (feature bullet), CHANGELOG.
  Ledger: C-15 DONE.
- **Proof:** `GOPROXY=off go build/vet ./...`, `gofmt -l .` empty, `go test
  -count=1 ./...` and `go test -race -count=1 ./...` all green offline (19 pkgs).

### Security-fix pass — audit 2026-08-12 Findings 1–4 (before C-16)
- **F1 (High) — hmacsign didn't authenticate the timestamp/delivery-id.** Rebuilt
  the MAC to cover the canonical `{deliveryID}.{timestamp}.{body}` (mirroring
  stdwebhook). `Signer.Sign` uses SigningInput.DeliveryID+Timestamp; `Signature`,
  `Verify`, `VerifyRequest` now take (secret, deliveryID, timestamp, body, …) —
  breaking API, updated the one internal caller (test/integration_test.go) + the
  CONSUMING example. Docstring corrected (was falsely claiming replay prevention).
  Proving tests: TestSignatureBindsTimestampAndID, TestSignerBindsHeaders, and the
  rewritten-timestamp replay case in TestVerifyRequest_TimestampTolerance (forged
  ts → ErrBadSignature).
- **F2 (Medium) — SSRF denylist gaps.** `httpdeliver.blocked` is now an allowlist
  (reject unless GlobalUnicast && !Private) plus explicit special-use CIDR blocks:
  0.0.0.0/8, 100.64.0.0/10, 192.0.0.0/24, 192.0.2.0/24, 198.18.0.0/15,
  198.51.100.0/24, 203.0.113.0/24, 240.0.0.0/4, 255.255.255.255/32, NAT64
  64:ff9b::/96 + 64:ff9b:1::/48. Doc comment corrected. Proving test
  TestBlocked_SSRFRanges (incl. NAT64-wrapped metadata 64:ff9b::a9fe:a9fe).
- **F3 (Medium) — plaintext http allowed.** Added Config.RequireHTTPS (default on
  in NewSecure); validateScheme rejects http:// to non-loopback with
  ErrInsecureScheme (loopback kept for dev; guard blocks it in prod). Proving test
  TestValidateScheme_RequireHTTPS.
- **F4 (Medium) — unbounded inproc queue.** Added Config.MaxDepth (DefaultMaxDepth
  10000; <0 = unbounded); Enqueue returns ErrQueueFull at capacity (backpressure);
  delivery stays pending in the log → recoverable via Service.Recover. Proving
  tests TestQueue_MaxDepthBackpressure, TestQueue_UnboundedWhenNegative.
- Docs: SECURITY-AUDIT_20260812.md (status table + per-finding Resolution notes),
  CHANGELOG (Security section), README (hmacsign row + SSRF note), CONSUMING
  (verify example). Findings 5–13 (Low/Info) remain open.
- **Proof:** `GOPROXY=off go build/vet ./...`, `gofmt -l .` empty, `go test
  -count=1 ./...` and `go test -race -count=1 ./...` all green offline (19 pkgs).

### Completion workflow — C-16: custom / tracing headers
- Added `Event.Headers map[string]string` (json `headers,omitempty`) and mirrored
  it on `EventSnapshot` (durable-safe); `snapshot()`/`event()` carry it, so custom
  headers survive the SQS JobRef → Rehydrate round trip and replays. dynamo
  delivery-log `eventItem` gained a `headers` map (both encode + decode); memory
  log already stores the whole snapshot.
- `Process` now layers headers: `Event.Headers` (custom) first → core idempotency
  headers → signer headers, so reserved names always win. New `isReservedHeader`
  (case-insensitive) protects Content-Type, Host, Content-Length, the
  X-Event-*/X-Delivery-Id/X-Attempt/X-Signature/X-Event-Timestamp headers, and the
  Standard Webhooks webhook-* names — a tenant-supplied custom header can never
  override an idempotency/auth header. Core stays pure.
- Tests (offline, deterministic, race-safe): events/headers_test.go —
  TestProcess_CustomHeadersMergedAndReservedProtected (traceparent + X-Custom
  propagate; forged X-Event-Id/Content-Type/X-Attempt/X-Signature are ignored),
  TestRehydrate_RestoresHeaders (headers persisted in the snapshot survive the
  durable round trip), TestIsReservedHeader.
- Docs: INTERFACE (Event/EventSnapshot + header-merge note), CONTRACTS (Signer
  header-precedence row), CONSUMING ("Trace propagation / custom headers"), README
  (feature bullet), CHANGELOG. Ledger: C-16 DONE.
- **Proof:** `GOPROXY=off go build/vet ./...`, `gofmt -l .` empty, `go test
  -count=1 ./...` and `go test -race -count=1 ./...` all green offline (18 test
  pkgs, 0 FAIL).

### Completion workflow — C-17: batching — DEFERRED (with design sketch)
**Decision:** DEFERRED. Batching (multiple events per HTTP request) does not fit
the current architecture cleanly; a correct implementation would compromise the
per-delivery guarantees that are the core's main value, and a shallow one would be
misleading. Deferral is the explicit-option outcome the ticket allows, and the PMM
report already ranks batching ICE 5.0 / "Later" (item 14).

**Why it doesn't fit cleanly now (invariants it would break):**
- **Idempotency is per (subscription, event).** `DeliveryID = sha256(subID, eventID)`
  and the `X-Delivery-Id` header let a subscriber dedup one event. A batch POST
  carries N events, so a single delivery id / `X-Event-Id` / `X-Event-Type` header
  no longer maps — the whole idempotency + dedup contract (and `ProcessRef`'s
  terminal-drop) is single-event.
- **Signing is single-message.** Both shipping signers sign one body (`hmacsign`
  over `{deliveryID}.{timestamp}.{body}`, `stdwebhook` `{id}.{ts}.{payload}` — the
  Standard Webhooks spec itself is one-message-per-request). Batching needs a
  different envelope + verification story.
- **The delivery log, replay, and admin UI are per-delivery.** One `Delivery` row,
  one `EventSnapshot`, per-delivery retry/backoff/dead-letter. A batch needs
  all-or-nothing vs partial-ack failure semantics that complicate the state
  machine and every store/UI that assumes 1 event ↔ 1 delivery.
- **Durable at-least-once via `JobRef` assumes one event id per reference.**
- **Blast radius:** a correct batch model touches every seam — `Job`, `Delivery`,
  `DeliveryLog` (+ dynamo schema), `Signer`, `Deliverer`, `adminhttp`, replay —
  which is exactly the "don't bloat the library" risk. Most webhook consumers
  don't need it, and the usual driver (protecting slow subscribers / cutting
  request volume) is already served by `adapters/ratelimit`.

**Future design sketch (when a concrete requirement defines failure semantics):**
- Opt-in per subscription: `Subscription.BatchPolicy{MaxEvents int, MaxWait time.Duration}`.
- A `BatchDeliverer` seam + a batching queue/flusher that coalesces *due* pending
  deliveries for the same subscription into one request, POSTing an envelope
  `{"deliveries":[{"id":..,"event":{..}}, ...]}` under a batch-scoped id
  `bat_<hash>`; the signer signs the envelope body.
- Keep per-event `Delivery` log rows (linked by a `BatchID`) so replay/idempotency
  stay per-event; define batch failure as retry-the-unacked-subset using a
  subscriber `2xx` body listing accepted delivery ids (partial ack), else
  all-or-nothing on transport failure.
- Ship it as a separate delivery path/adapter so the clean single-event core is
  untouched for the majority who don't opt in.

**Interim guidance for callers who need fewer requests today:** emit a single event
whose payload is an application-defined array (their own batching), or use
`adapters/ratelimit` to smooth burst volume. No core change ships for C-17.

**Proof:** no code changed; suite remains green offline (`build`/`vet`/`gofmt`/
`test`/`-race`, 18 test pkgs, 0 FAIL).

### Completion workflow — C-18: per-endpoint transform hook (transformations)
**Decision:** IMPLEMENTED (the transform hook fits cleanly; "portal UI" polish is
out of scope for a Go library — the admin HTTP surface + delivery-log UI already
ship in adapters/adminhttp).
- New `events.Transformer` seam + `TransformerFunc` adapter (ports.go):
  `Transform(ctx, sub Subscription, ev Event, body []byte) ([]byte, error)`.
  Optional `Deps.Transformer` (nil = deliver verbatim); Service.transformer field.
- `Process` applies the transform BEFORE building headers/signing, so the
  signature covers exactly the transformed body that is sent. It receives the
  destination Subscription, so one implementation branches per endpoint. On a
  transform error the attempt is aborted with no POST and recorded as a failed
  attempt (code 0) via the existing failure path → retried/dead-lettered by the
  normal policy (implemented by short-circuiting postErr and guarding the sign+POST
  block with `if postErr == nil`). Core stays pure; the transform shapes the body
  only and cannot inject reserved/auth headers.
- Tests (offline, deterministic, race-safe): events/transform_test.go —
  **TestProcess_TransformerReshapesBodyAndSignsTransformed** (per-endpoint wrap;
  the subscriber receives the transformed body AND X-Signature is over the
  transformed body's length, proving sign-after-transform),
  TestProcess_TransformerErrorFailsAttemptWithoutSending (no POST; StatusFailed +
  "transform failed: boom"; one attempt code 0), TestProcess_NilTransformerPassesThrough.
- Docs: INTERFACE (Transformer types + Deps note), CONTRACTS (Transformer seam),
  CONSUMING ("Per-endpoint payload transforms"), README (feature bullet),
  CHANGELOG. Ledger: C-18 DONE.
- **Proof:** `GOPROXY=off go build/vet ./...`, `gofmt -l .` empty, `go test
  -count=1 ./...` and `go test -race -count=1 ./...` all green offline (18 test
  pkgs, 0 FAIL).

### Security batch — audit 2026-08-12 Findings 5–13 (final task, triaged)
FIXED (each with a proving test; all non-breaking):
- **F6 (Low) Retry-After uncapped when MaxDelay==0.** Added const
  `events.AbsoluteMaxRetryDelay` (24h); `Process` clamps every retry delay to it
  regardless of MaxDelay. Test: TestRetryAfter_AbsoluteCeiling (100-day Retry-After,
  MaxDelay=0 → clamped to exactly 24h).
- **F7 (Low) Replay no-op on durable queues.** `Replay` now `log.Record`s the
  pending status before enqueue, so ProcessRef/Rehydrate see a non-terminal
  delivery. Test: TestReplay_ResetsLogStatusForDurableQueues (deliver→terminal,
  replay, ProcessRef from a JobRef re-POSTs). Updated adminhttp replay/UI test to
  the corrected behavior (post-replay status = pending; UI checked before replay).
- **F8 (Low) ratelimit unbounded buckets.** Limiter evicts buckets idle ≥10min
  (sweep ≤ once/min via injectable clock); safe because an idle bucket refills to
  full on next use. Added NumBuckets(). Test: TestReserve_EvictsIdleBuckets.
- **F9 (Low) no Emit payload cap.** Optional `Deps.MaxPayloadBytes`; Emit rejects
  oversized payloads up front with new `ErrPayloadTooLarge` (0 = unbounded).
  Test: TestEmit_MaxPayloadBytes. Also mitigates F13.

ACCEPTED (by-design / accepted-risk; rationale + residual risk in the audit doc,
no code change):
- **F5 (Low) built-in replay form CSRF.** Real CSRF needs mounting-app session
  state; handler is auth-agnostic and documented "mount behind your CSRF"; XSS
  surface clean; not exploitable under header/bearer auth. Kept form + warning.
- **F8 autodisable map.** Self-bounding: entries created only for actively-failing
  subs, deleted on success and on disable-at-threshold. Bounded working set.
- **F10 (Info) URLValidator nil default.** Dial-time SSRF guard is authoritative
  and on by default (NewSecure, allowlist per F2); Subscribe-time validation is
  optional fail-fast (Deps.ValidateURL available), policy is deployment-specific.
- **F11 (Info) Type unvalidated.** CRLF header injection not exploitable (net/http
  rejects control chars); strict Catalog (C-13) + payload cap (F9) address hygiene.
- **F12 (Info) tolerance<=0 disables freshness.** Documented caller contract in
  hmacsign.VerifyRequest; changing 0→reject would break callers.
- **F13 (Info) Dynamo item vs 400KB.** Durability edge (clean write error, no
  leak); attempts bounded by MaxAttempts; payload cap (F9) mitigates; item-split is
  future schema work.

Audit doc status table + per-finding Resolution/Disposition notes updated; CHANGELOG
Security section extended. No breaking API changes (all fixes are additive/internal;
F9 adds an optional Deps field, F6 a new const, F9 a new error, F8 a new method).
**Proof:** build/vet ok, gofmt -l . empty, test -count=1 and -race both 18 ok / 0
FAIL offline.
