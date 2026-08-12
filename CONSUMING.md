# Consuming ab0t-events-go

Wire the emitter once at startup, then call `Emit` from wherever your domain
events originate.

## 1. Add the dependency

```
go get github.com/ab0t-com/ab0t-events-go
```

## 2a. Fastest path — the `webhooks` facade

If you just want working webhooks with production-safe defaults, use the facade.
Only the two stateful dependencies (Store, Log) are required:

```go
sys, err := webhooks.New(ctx, webhooks.Options{
    Store: memory.NewStore(), // dev; use dynamo.NewStore(ddb, "subscriptions") in prod
    Log:   memory.NewLog(),   // dev; use dynamo.NewLog(ddb, "deliveries") in prod
    // Optional: Observer, Policy, Workers, Deliverer, Signer, ValidateURL, Clock/IDGen/Rand
})
if err != nil { return err }
defer sys.Close() // stops the worker pool, draining in-flight deliveries

secret, _ := hmacsign.GenerateSecret() // "whsec_...", 256-bit; show once, store encrypted
sys.Subscribe(ctx, events.NewSubscription{
    OrgID: orgID, EventTypes: []string{"drive.*"},
    URL: "https://customer.example/webhooks", Secret: secret,
})
sys.EmitNew(ctx, "drive.file.created", orgID, map[string]any{"fileId": "f_1"})
```

`*webhooks.System` embeds `*events.Service`, so `Emit`, `EmitNew`, `Subscribe`,
`ListDeliveries`, `Replay`, etc. are all available directly. For full control
over every port, use the explicit wiring below instead.

## 2b. Explicit wire-in (every port, ~10 lines)

```go
import (
    "github.com/ab0t-com/ab0t-events-go/adapters/hmacsign"
    "github.com/ab0t-com/ab0t-events-go/adapters/httpdeliver"
    "github.com/ab0t-com/ab0t-events-go/adapters/inproc"
    "github.com/ab0t-com/ab0t-events-go/adapters/std"
    "github.com/ab0t-com/ab0t-events-go/adapters/dynamo"
    "github.com/ab0t-com/ab0t-events-go/events"
)

queue := inproc.New(inproc.Config{Workers: 8})
emitter := events.NewService(events.Deps{
    Store:     dynamo.NewStore(ddb, "webhook-subscriptions"),
    Queue:     queue,
    Deliverer: httpdeliver.NewSecure(),        // SSRF guard on
    Signer:    hmacsign.New(),                 // HMAC-SHA256 over {deliveryId}.{timestamp}.{body}
    Log:       dynamo.NewLog(ddb, "webhook-deliveries"),
    Clock:     std.Clock{},
    IDGen:     std.IDGen{},
    Rand:      std.Rand{},
    Policy:    events.DefaultRetryPolicy(),
})
stop := queue.Start(ctx, emitter)              // launches the worker pool
defer stop()
```

For local/dev or a single-process service, swap the two `dynamo.*` lines for
`memory.NewStore()` / `memory.NewLog()` — nothing else changes. `emitter` is an
`events.Emitter`; hold it behind that interface.

## 3. Emit domain events

```go
payload, _ := json.Marshal(publicShape) // the exact bytes that get signed
err := emitter.Emit(ctx, events.Event{
    ID:         std.IDGen{}.NewID(),      // or your own event id (used for idempotency)
    Type:       "drive.file.created",     // subscribers filter on this
    OrgID:      orgID,                     // tenant; hard partition
    OccurredAt: time.Now().UTC(),
    Payload:    payload,
})
```

`Emit` looks up the org's matching subscriptions, records each delivery as
`pending`, and enqueues it. The worker pool signs, POSTs, logs every attempt, and
retries with backoff → dead-letter. `Emit` returns quickly; delivery is async.

Prefer `EmitNew` to skip the boilerplate — it mints the event id (via `IDGen`),
stamps `OccurredAt` (via `Clock`), and JSON-marshals the payload:

```go
err := emitter.EmitNew(ctx, "drive.file.created", orgID, publicShape) // *events.Service / *webhooks.System
```

Subscriptions match an event type by exact string, the global `*`, or a
hierarchical prefix like `calendar.event.*` (matches `calendar.event.created`,
not `calendar.reminder.set`).

**Content filters (optional).** A subscription can also carry `PayloadFilters` to
receive only events whose payload matches. Each filter is a dot-path into the JSON
payload that must equal a string; multiple filters combine with AND, and an empty
list matches every payload:

```go
sub, _ := emitter.Subscribe(ctx, events.NewSubscription{
    OrgID: orgID, EventTypes: []string{"drive.file.*"}, URL: url, Secret: secret,
    PayloadFilters: []events.PayloadFilter{{Path: "region", Equals: "eu"}},
})
// now only drive.file.* events whose payload has {"region":"eu"} reach this endpoint.
```

Values compare as strings (number → its source text `42`, bool → `true`/`false`,
`null` → `null`); a missing path or a non-scalar (object/array) value never
matches. Filtering happens during `Emit` fan-out, so a non-matching subscription
gets no delivery at all. Admin callers pass the same via the create-subscription
body's `payloadFilters` field.

### Declare an event catalog (optional)

Register the event types your service publishes so they can be documented (served
at the admin `GET /event-types` endpoint) and, in strict mode, so a typo in an
`Emit` type fails fast instead of silently matching nothing:

```go
catalog := events.NewCatalog(true /* strict */).Register(
    events.EventTypeDef{Type: "drive.file.created", Description: "A file was created",
        Example: json.RawMessage(`{"fileId":"f_123"}`)},
    events.EventTypeDef{Type: "drive.file.deleted", Deprecated: true},
)
// wire it in:
Deps{ /* … */ Catalog: catalog }
```

With a **strict** catalog wired via `Deps.Catalog`, `Emit` returns
`events.ErrUnknownEventType` for any event whose `Type` was never registered —
before touching the store or queue. A non-strict catalog (or `nil`) is
documentation only and never rejects an emit. `Ping` bypasses `Emit`, so its
synthetic `webhook.ping` is never affected. Expose the catalog on the admin API
with `adminhttp.New(api, org, adminhttp.WithCatalog(catalog))`.

### Trace propagation / custom headers (optional)

Attach custom HTTP headers to an event via `Event.Headers`; they ride along on
every outbound delivery and are persisted in the snapshot, so they survive a
durable-queue round trip (and replays). This is how you propagate a W3C trace
context without pulling in an OpenTelemetry dependency:

```go
ev, _ := svc.NewEvent("drive.file.created", orgID, payload)
ev.Headers = map[string]string{
    "traceparent": span.TraceParentHeader(), // e.g. "00-<trace-id>-<span-id>-01"
}
_ = svc.Emit(ctx, ev)
```

Reserved headers the core and signer own — `Content-Type`, the `X-Event-*` /
`X-Delivery-Id` / `X-Attempt` / `X-Signature` idempotency+auth headers, `Host`,
`Content-Length`, and the Standard Webhooks `webhook-*` names — cannot be
overridden by a custom header (a caller-supplied value for any of them is
dropped).

### Per-endpoint payload transforms (optional)

Reshape the outbound body per subscription without changing the stored event —
useful when different subscribers want different envelopes/field shapes for the
same canonical event. The hook runs before signing, so the signature covers what
is actually delivered:

```go
Transformer: events.TransformerFunc(func(_ context.Context, sub events.Subscription, ev events.Event, body []byte) ([]byte, error) {
    if sub.URL == "https://legacy.example/hook" {
        return []byte(`{"legacy":true,"data":` + string(body) + `}`), nil // wrap for one endpoint
    }
    return body, nil // everyone else gets the payload verbatim
}),
```

A non-nil error aborts that attempt without sending (recorded as a failed attempt,
retried by the normal policy). The transform shapes the body only — it cannot set
signature/idempotency headers (use `Event.Headers` for custom headers). "Portal
UI" polish is out of scope for the library; the admin surface + delivery-log UI
already ship in `adapters/adminhttp`.

### Metrics & logging (optional)

Pass an `Observer` to see every attempt without replacing the delivery log:

```go
Observer: events.ObserverFunc(func(_ context.Context, d events.Delivery, a events.Attempt) {
    metrics.Count("webhook.attempt", "status", string(d.Status), "code", a.Code)
    metrics.Histogram("webhook.latency_ms", a.Latency.Milliseconds())
}),
```

## 4. Admin operations (org self-service endpoints)

```go
sub, err := emitter.Subscribe(ctx, events.NewSubscription{
    OrgID: orgID, EventTypes: []string{"drive.file.created", "calendar.event.*"},
    URL: "https://customer.example/webhooks", Secret: generatedSecret,
})
subs, err := emitter.ListSubscriptions(ctx, orgID)
err = emitter.Unsubscribe(ctx, orgID, sub.ID)

page, cursor, err := emitter.ListDeliveries(ctx, orgID, events.DeliveryFilter{
    SubscriptionID: sub.ID, Limit: 50,
})
err = emitter.Replay(ctx, orgID, deliveryID)   // re-deliver a past event
```

Every admin call is org-scoped: pass the authenticated caller's `orgID` and the
store/log never cross the tenant boundary.

## 5. On the subscriber's side (verifying a webhook)

```go
body, _ := io.ReadAll(r.Body)
err := hmacsign.VerifyRequest(
    subscriptionSecret, body,
    r.Header.Get(events.HeaderDelivery),    // X-Delivery-Id  (bound into the signature)
    r.Header.Get(events.HeaderSignature),   // X-Signature
    r.Header.Get(events.HeaderTimestamp),   // X-Event-Timestamp (bound into the signature)
    time.Now(), 5*time.Minute,              // tolerance window (anti-replay)
)
if err != nil { http.Error(w, "bad signature", 400); return }
// dedup on r.Header.Get(events.HeaderDelivery) — X-Delivery-Id
```

The `hmacsign` signature covers `{deliveryId}.{timestamp}.{body}`, so an attacker
who captures a validly-signed request cannot rewrite the timestamp or delivery id
to bypass your freshness window or dedup — the signature check fails. Pass a
positive `tolerance` (e.g. 5 min); a non-positive tolerance disables the freshness
check.

## Production hardening (v0.2)

### Crash recovery
The in-process queue is volatile; the delivery log is durable. Re-enqueue
in-flight retries at startup:

```go
for _, orgID := range activeOrgIDs {
    n, _ := sys.Recover(ctx, orgID) // re-delivers pending/failed deliveries
    log.Printf("recovered %d webhook deliveries for %s", n, orgID)
}
```

### Durable delivery that survives restarts (SQS)
The default `inproc` queue is fast but volatile. For durability, swap in the SQS
queue — you bring your own SQS client (no assumptions about region, creds, or
endpoint), and **only id-only references travel through the queue** (never the
payload or the secret): a worker rehydrates the full job from your store + log.

```go
queue := sqsqueue.New(sqsqueue.Config{Client: sqsClient, QueueURL: url})
svc := events.NewService(events.Deps{Store: store, Queue: queue, /* … */})
stop := queue.Start(ctx, svc) // svc is the events.RefDispatcher
defer stop()
```

Because the queue holds only ids, it is at-least-once safe: duplicates of an
already-delivered/dead delivery are dropped, and app-level retries are scheduled
as fresh delayed messages.

### Write your own durable queue (Redis, Postgres, Kafka, …)
The durable pattern is reusable — implement `events.DeliveryQueue` (enqueue a
`job.Ref()`) and, in your consumer, call `svc.ProcessRef(ctx, ref)` (it
rehydrates via `events.Rehydrator` and dispatches). The `sqsqueue` package is the
reference implementation; the core makes no assumptions about your infrastructure.

### Encrypt subscription secrets at rest
Signing secrets are stored in your `SubscriptionStore`. Wrap it with
`securestore` so the backing table (and its backups) hold only ciphertext — the
decorator encrypts `Subscription.Secret` on write and decrypts on read, adding no
cross-org path. Pick a codec:

```go
// Option A — stdlib AES-256-GCM; you hold a 32-byte key (from your secret manager).
codec, err := aesgcm.New(key32)          // len(key32) == 32

// Option B — AWS KMS; key material never leaves KMS (bring your own *kms.Client).
codec := kmscodec.New(kmscodec.Config{Client: kmsClient, KeyID: "alias/webhook-secrets"})

store := securestore.New(dynamo.NewStore(...), codec) // use this as your SubscriptionStore
svc := events.NewService(events.Deps{Store: store, /* … */})
```

Callers still see plaintext secrets (e.g. once at create/rotate time); only the
at-rest representation is encrypted. Rotating the AES key or KMS key requires
re-encrypting existing rows (read-with-old, write-with-new).

### Standard Webhooks signatures (cross-language verification)
Swap the signer so subscribers can verify with off-the-shelf libraries:

```go
webhooks.New(ctx, webhooks.Options{Store: ..., Log: ..., Signer: stdwebhook.New()})
// subscriber: stdwebhook.VerifyRequest(secret, body, id, ts, sigHeader, time.Now(), 5*time.Minute)
```

### Rate-limit slow subscribers (decorator)
Wrap any `Deliverer` to cap requests per destination host (protects subscribers
and your egress). Decorators nest, so keep the SSRF guard underneath:

```go
deliverer := ratelimit.WrapDeliverer(httpdeliver.NewSecure(), ratelimit.Config{
    PerSecond: 20, Burst: 40, // per host by default; override Config.Key to key differently
})
webhooks.New(ctx, webhooks.Options{Store: store, Log: log, Deliverer: deliverer})
```

When a request is throttled past the context deadline, the deliverer returns the
context error, which the core treats as a transient failure and retries.

### Metrics + auto-disable (compose observers)
```go
mc := metrics.New()
breaker := autodisable.New(store, autodisable.Config{Threshold: 10, OnDisable: notifyOrg})
webhooks.New(ctx, webhooks.Options{
    Store: store, Log: log,
    Observer: events.MultiObserver(mc, breaker),
})
// later: mc.Snapshot().SuccessRate(), .MeanLatency(), .ByCode
```

### Admin API + delivery-log UI (the seams)
Mount the drop-in handler behind your auth; it takes the org from your resolver:

```go
admin := adminhttp.New(sys, func(r *http.Request) (string, error) {
    return auth.OrgID(r.Context()) // your auth; never trust a client-supplied org
})
mux.Handle("/admin/webhooks/", http.StripPrefix("/admin/webhooks", admin))
```

Routes: `POST/GET /subscriptions`, `GET/DELETE /subscriptions/{id}`,
`POST /subscriptions/{id}/rotate-secret`, `POST /subscriptions/{id}/ping`
(send a test event), `GET /deliveries` (filter by
`subscriptionId`/`status`/`cursor`/`limit`), `GET /deliveries/{id}`,
`POST /deliveries/{id}/replay`, and `GET /ui` (HTML delivery log). Secrets are
returned only at create/rotate time. For browser/cookie auth, put the handler
behind your CSRF protection (the UI has a state-changing Replay form).

The deliverer honors a subscriber's `Retry-After` header on 429/503 (capped by
`RetryPolicy.MaxDelay`) and sends a stable `User-Agent` (override via
`httpdeliver.Config.UserAgent`).

## Choosing adapters

| Concern | Dev / single-process | Production |
|---|---|---|
| Store + Log | `memory` | `dynamo` |
| Queue | `inproc` | `inproc` (or write an SQS adapter to `events.DeliveryQueue`) |
| Deliverer | `httpdeliver.New({GuardPrivate:false})` for localhost tests | `httpdeliver.NewSecure()` |
| Clock/IDGen/Rand | `std` | `std` |

Any port can be replaced by implementing its interface (see
[docs/INTERFACE.md](./docs/INTERFACE.md)); the core does not change.

## DynamoDB tables

Two tables, both with a string partition key `PK` and string sort key `SK`:

- subscriptions: `PK = ORG#<orgID>`, `SK = SUB#<subID>`
- deliveries: `PK = ORG#<orgID>`, `SK = DLV#<deliveryID>`

Enable SSE/KMS encryption at rest (the subscription secret lives here). No GSIs
required.
