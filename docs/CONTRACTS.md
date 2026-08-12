# Seams & Contracts

Every extension point in ab0t-events-go is an interface ("seam"). This document
is the contract for each: what it must do, who calls it, its concurrency and
tenancy obligations, its error semantics, and the adapters that ship. If you
implement a seam, honor the contract here; if you consume the library, this is
what you can rely on.

See also: [INTERFACE.md](./INTERFACE.md) (typed signatures), [CONSUMING.md](../CONSUMING.md)
(how-to), [README.md](../README.md) (overview).

---

## How the seams fit together

```
 service ──Emit──▶  events.Service  ──[SubscriptionStore]── find org's matching subs
                          │           ──[DeliveryLog]────── record delivery (pending) + snapshot
                          │           ──[DeliveryQueue]──── enqueue a job (or JobRef)
   queue adapter drives   ▼
   Dispatcher/RefDispatcher: sign ─[Signer]→ POST ─[Deliverer]→ log ─[DeliveryLog]→ retry/dead-letter
                          │
                          └─ per-attempt ─[Observer]→ metrics / auto-disable
   testability seams: [Clock] [IDGen] [Rand]   optional: [URLValidator]
```

The **core** (`events.Service`) is pure: it performs no I/O and reads no wall
clock or randomness except through the seams below.

---

## Driving contracts (what a service calls)

### `Emitter`
The typed API a service uses. Every method is **org-scoped** — pass the
authenticated caller's `orgID`; the library never crosses the tenant boundary.

- `Emit` fans an event out to the org's matching subscriptions, records each
  delivery as `pending` (persisting the event snapshot), and enqueues it. A
  subscription matches when its **type** filter matches *and* all its
  `PayloadFilters` match the event payload (dot-path equality; a filtered-out
  subscription gets no delivery — no log record, no queue job). It returns
  quickly; delivery is asynchronous. Fan-out continues past a per-subscription
  error; the joined error is returned.
- `Subscribe` / `Unsubscribe` / `ListSubscriptions` / `GetSubscription` /
  `RotateSecret` / `Ping` — subscription admin.
- `ListDeliveries` / `GetDelivery` / `Replay` — delivery log + replay.
- `Recover(orgID)` — re-enqueue non-terminal deliveries after a restart.

Implemented by `*events.Service` (`events.NewService`). Hold it behind the
`events.Emitter` interface.

### `Dispatcher` / `RefDispatcher` (worker handlers)
A queue adapter drives one of these to process queued work:

- `Dispatcher.Process(ctx, Job)` — for in-process queues that carry the full job.
- `RefDispatcher.ProcessRef(ctx, JobRef)` — for durable/off-process queues that
  carry an id-only reference; it rehydrates via `Rehydrator` then dispatches, and
  is **at-least-once safe** (drops references that are terminal or whose referent
  is gone). Both are implemented by `*events.Service`.

---

## Port contracts (the seams you implement)

### `SubscriptionStore` — the tenant boundary
| | |
|---|---|
| **Responsibility** | Persist subscriptions, partitioned per org. |
| **Called by** | `Emit` (`ListForOrgAndType`), admin ops, `Rehydrate` (`Get`). |
| **Concurrency** | Safe for concurrent use. |
| **Tenancy (hard)** | Every method is keyed by `orgID` and MUST NOT return, mutate, or delete another org's data. `ListForOrgAndType` returns only *active* subs whose types match (exact / `*` / `prefix.*`). |
| **Errors** | Return `events.ErrNotFound` for a missing id in the org. Other errors propagate to the caller. |
| **Secrets** | Stores `Subscription.Secret`; never log it. Wrap with `adapters/securestore` to encrypt it at rest (see the `SecretCodec` seam). |
| **Adapters** | `memory`, `dynamo` (PK `ORG#<org>`, SK `SUB#<id>`); optionally wrapped by `securestore`. |

### `DeliveryQueue` — at-least-once handoff
| | |
|---|---|
| **Responsibility** | Accept a `Job` for eventual delivery. `Enqueue` is the only method the core calls. |
| **Called by** | `Emit`, `Process` (retry re-enqueue), `Replay`, `Ping`, `Recover`. |
| **Concurrency** | `Enqueue` safe for concurrent use. |
| **Scheduling** | Honor `Job.NotBefore` (do not deliver before it) — this implements backoff. |
| **Delivery guarantee** | At-least-once. A job may run more than once; the core's idempotency + `ProcessRef` terminal-drop keep that safe. |
| **Durable variant** | Off-process adapters serialize `Job.Ref()` (id-only, **no payload, no secret**) and, in their consumer loop, call `RefDispatcher.ProcessRef`. See `adapters/sqsqueue`. |
| **Adapters** | `inproc` (volatile worker pool), `sqsqueue` (durable). |

### `Deliverer` — the outbound POST
| | |
|---|---|
| **Responsibility** | POST a signed body to the subscriber URL and report the outcome. |
| **Called by** | `Process`. |
| **Concurrency** | Safe for concurrent use (workers call it in parallel). |
| **Must** | Apply a timeout; SHOULD enforce an SSRF guard; SHOULD NOT follow redirects; SHOULD capture a bounded response `Body` snippet and parse `Retry-After` on non-2xx. |
| **Errors** | Return a non-nil error for transport failures/timeouts (`StatusCode` 0) → treated as retryable. A returned `DeliveryResponse` with a non-2xx code is classified by `RetryPolicy.RetryOn`. |
| **Decorators** | `ratelimit.WrapDeliverer` (per-host throttle) wraps any Deliverer. |
| **Adapters** | `httpdeliver` (use `NewSecure()` in production). |

### `Signer` — message authentication
| | |
|---|---|
| **Responsibility** | Given `SigningInput` (ids, timestamp, body, secret), return the auth headers to attach. |
| **Called by** | `Process` (headers merged onto the request). |
| **Contract** | Bind the signature to the body **and** the delivery id + timestamp (both shipping signers do: `hmacsign` over `{deliveryID}.{timestamp}.{body}`, `stdwebhook` over `{id}.{timestamp}.{payload}`), so those headers can't be forged. MUST NOT echo the secret into a header. The core sets the common idempotency headers; the Signer owns the signature/timestamp headers. |
| **Header precedence** | `Process` layers headers as: `Event.Headers` (custom, e.g. `traceparent`) first → core idempotency headers → Signer headers. Reserved names (`Content-Type`, the `X-Event-*`/`X-Delivery-Id`/`X-Attempt`/`X-Signature` headers, `Host`, `Content-Length`, `webhook-*`) are never overridable by a custom header. `Event.Headers` is persisted in the snapshot, so custom headers survive the durable-queue round trip. |
| **Adapters** | `hmacsign` (`X-Signature` + `X-Event-Timestamp`), `stdwebhook` (Standard Webhooks). |

### `DeliveryLog` — durable record + replay source
| | |
|---|---|
| **Responsibility** | Persist every delivery and its attempts, per org; store the event snapshot once (enables replay + rehydration). |
| **Called by** | `Emit`/`Process` (`Record`), `Replay`/`GetDelivery`/`Rehydrate` (`Get`), `ListDeliveries`/`Recover` (`List`). |
| **Concurrency** | Safe for concurrent use; `Record` is an upsert that appends the attempt (when `Attempt.N > 0`) and writes the snapshot once. |
| **Tenancy** | All reads are org-scoped; never return another org's deliveries. Honor `DeliveryFilter` (`SubscriptionID`, `Status`, cursor, limit). |
| **Secrets** | Never persist secrets in the log (attempt errors carry a response snippet, not secrets). |
| **Adapters** | `memory`, `dynamo` (PK `ORG#<org>`, SK `DLV#<id>`). |

### `Clock` / `IDGen` / `Rand` — testability seams
| | |
|---|---|
| **Responsibility** | The core's only sources of the current time, new ids, and jitter. |
| **Contract** | `Clock.Now` — current time. `IDGen.NewID` — unique id (used for events/subscriptions, **not** delivery ids, which are deterministic). `Rand.Float64` — value in `[0,1)` for backoff jitter. All must be safe for concurrent use. |
| **Adapters** | `std` (wall clock, crypto-random ids, math/rand jitter). |

### `Observer` — optional per-attempt hook
| | |
|---|---|
| **Responsibility** | Receive every delivery attempt (post-attempt, so `Delivery.Status` is the outcome). |
| **Contract** | MUST NOT block or panic; runs on the worker goroutine. Compose several with `events.MultiObserver`. |
| **Adapters** | `metrics.Collector` (counters/latency/success-rate), `autodisable.Breaker` (pause endpoints that keep dead-lettering). |

### `Transformer` — optional per-endpoint body reshaping
| | |
|---|---|
| **Responsibility** | Rewrite the outbound request body for a specific subscription before it is signed and sent (envelope-wrap, drop/rename fields, adapt to a subscriber's shape) without changing the stored event. |
| **Called by** | `Process`, once per attempt, **before** signing — so the signature covers exactly what is delivered. |
| **Concurrency** | Safe for concurrent use; must be pure w.r.t. its inputs (no wall clock / rand — the core owns those), so retries and replays are deterministic. |
| **Errors** | A non-nil error aborts the attempt without a POST; it is recorded as a failed attempt (`Code` 0) and retried / dead-lettered by the normal policy. |
| **Scope** | It receives the destination `Subscription`, so one implementation branches per endpoint. It shapes the **body only** — it cannot inject signature/idempotency headers (custom headers are `Event.Headers`; reserved names are protected). |
| **Wiring** | `Deps.Transformer` (nil delivers the payload verbatim). `events.TransformerFunc` adapts a function. |

### `URLValidator` — optional subscribe-time vetting
`func(url string) error`, invoked by `Subscribe`. Return an error to reject
(e.g. SSRF allowlist/denylist). Nil disables it.

### `SecretCodec` — secret encryption at rest
| | |
|---|---|
| **Responsibility** | Encrypt/Decrypt a subscription's signing secret so a `SubscriptionStore` holds only ciphertext. `Decrypt(Encrypt(s)) == s`. |
| **Called by** | `adapters/securestore` — on `Put` (Encrypt) and on `Get`/`ListForOrg`/`ListForOrgAndType` (Decrypt). |
| **Concurrency** | Safe for concurrent use. |
| **Contract** | Ciphertext is an opaque, storage-safe string (base64). The empty string is passed through by `securestore` without calling the codec. A failed Decrypt MUST error, never return ciphertext as the secret. |
| **Secrets** | This *is* the secret-at-rest boundary: plaintext exists only in memory and in transit to the codec; the store, its backups, and operators with raw table access see ciphertext only. |
| **Adapters** | `aesgcm` (stdlib AES-256-GCM, you hold a 32-byte key), `kmscodec` (AWS KMS, key material stays in KMS). |

### `securestore` — encrypt-at-rest decorator
| | |
|---|---|
| **Responsibility** | Wrap any `SubscriptionStore` with a `SecretCodec`: encrypt `Subscription.Secret` on write, decrypt on read; delegate everything else unchanged. |
| **Tenancy** | Adds **no** cross-org path — tenancy stays the wrapped store's responsibility. |
| **Purity** | `Put` takes the subscription by value, so encrypting the local copy never mutates the caller's struct. |
| **Compose** | `securestore.New(dynamo.NewStore(...), aesgcm-or-kms-codec)` — use the result as your `SubscriptionStore`. |

### `Catalog` — optional event-type registry
| | |
|---|---|
| **Responsibility** | Declare the service's event vocabulary (`EventTypeDef{Type, Description, Example, Deprecated}`) via `Register`/`Get`/`Has`/`List`. |
| **Called by** | `Emit` (`Has`, when strict) and the admin `GET /event-types` (`List`). |
| **Concurrency** | Safe for concurrent use (`NewCatalog`; a `sync.RWMutex` guards it). |
| **Enforcement** | Optional. Wired via `Deps.Catalog`; only a **strict** catalog (`NewCatalog(true)`) rejects an emit — an unregistered `Event.Type` yields `ErrUnknownEventType` **before** any store/queue I/O. A non-strict catalog (or nil) never rejects. |
| **Tenancy** | Service-global (not per-org): the vocabulary is the same for every org. It holds no secrets and no tenant data. |
| **Bypass** | `Ping` enqueues directly (not through `Emit`), so its synthetic `webhook.ping` is never gated by the catalog. |
| **Purity** | Pure in-core type — no adapter, no I/O, no clock. |

### `Rehydrator` — durable-queue reload
`Rehydrate(ctx, JobRef) (Job, error)` reloads the event snapshot from the log and
the subscription from the store. Returns `ErrNotFound` when the referent is gone
so a queue adapter can drop the message. Implemented by `*events.Service`.

---

## Decorators (compose without changing the core)

- **`ratelimit.WrapDeliverer(next, Config)`** — token-bucket throttle per key
  (host by default); blocks up to the context deadline, then delegates.
- **`events.MultiObserver(a, b, …)`** — fan attempts out to several observers.

Decorators satisfy the same interface they wrap, so they nest freely
(`ratelimit.WrapDeliverer(httpdeliver.NewSecure(), …)`).

---

## Cross-cutting invariants (guaranteed by the core)

1. **Tenancy.** No method mixes orgs. `Emit`/`Process`/`Rehydrate` re-assert
   `event.OrgID == subscription.OrgID`; a mismatch is `ErrTenancyViolation`.
2. **Secrets.** Per-subscription, `json:"-"`, never logged, never placed on a
   queue, never echoed into signature headers.
3. **Idempotency.** `DeliveryID(subscription, event)` is deterministic; the
   `X-Delivery-Id` header lets subscribers dedup; `ProcessRef` drops duplicates of
   terminal deliveries.
4. **At-least-once.** Delivery may repeat under queue redelivery or `Recover`; the
   above keeps it safe. Exactly-once is a subscriber-side concern (dedup on the id).
5. **Determinism.** The core touches time/randomness only via `Clock`/`IDGen`/`Rand`.

---

## Adapter selection matrix

| Seam | Dev / single-process | Production |
|---|---|---|
| `SubscriptionStore` + `DeliveryLog` | `memory` | `dynamo` (wrap the store in `securestore` for secret encryption at rest) |
| `SecretCodec` (secret at rest) | — (or `aesgcm` with a dev key) | `kmscodec` (AWS KMS) or `aesgcm` (managed 32-byte key) |
| `DeliveryQueue` | `inproc` | `sqsqueue` (or your own via `JobRef` + `ProcessRef`) |
| `Deliverer` | `httpdeliver.New({GuardPrivate:false})` for localhost tests | `httpdeliver.NewSecure()`, optionally `ratelimit.WrapDeliverer` |
| `Signer` | `hmacsign` | `hmacsign` or `stdwebhook` |
| `Clock`/`IDGen`/`Rand` | `std` | `std` |
| `Observer` | — | `metrics` + `autodisable` via `MultiObserver` |
