# Typed-interface reference

The complete public surface of `github.com/ab0t-com/ab0t-events-go/events`. A
consuming service depends on the `Emitter` interface; it implements adapters for
the ports below. For each seam's behavioral contract (who calls it, concurrency,
tenancy, error semantics), see [CONTRACTS.md](./CONTRACTS.md).

## Driving API

```go
// Emitter is the typed interface a service calls. Every method is org-scoped.
type Emitter interface {
    Emit(ctx context.Context, ev Event) error

    Subscribe(ctx context.Context, in NewSubscription) (Subscription, error)
    Unsubscribe(ctx context.Context, orgID, subscriptionID string) error
    ListSubscriptions(ctx context.Context, orgID string) ([]Subscription, error)

    ListDeliveries(ctx context.Context, orgID string, f DeliveryFilter) (page []Delivery, nextCursor string, err error)
    Replay(ctx context.Context, orgID, deliveryID string) error
}

// Dispatcher processes one queued Job (sign → POST → log → retry/dead-letter).
// A queue adapter's workers call Process; *Service implements it.
type Dispatcher interface {
    Process(ctx context.Context, job Job) error
}

func NewService(d Deps) *Service   // *Service implements both Emitter and Dispatcher

// Convenience & admin methods on *Service (beyond the Emitter interface):
func (s *Service) NewEvent(eventType, orgID string, payload any) (Event, error)
func (s *Service) EmitNew(ctx context.Context, eventType, orgID string, payload any) error
func (s *Service) GetSubscription(ctx, orgID, subscriptionID string) (Subscription, error)
func (s *Service) RotateSecret(ctx, orgID, subscriptionID string) (newSecret string, err error)
func (s *Service) GetDelivery(ctx, orgID, deliveryID string) (Delivery, EventSnapshot, []Attempt, error)
func (s *Service) Recover(ctx, orgID string) (requeued int, err error) // startup crash recovery
func (s *Service) Ping(ctx, orgID, subscriptionID string) (Delivery, error) // synthetic webhook.ping
```

### v0.2–v0.3 adapters
- `adapters/stdwebhook` — Standard Webhooks `Signer` + `Verify`/`VerifyRequest`.
- `adapters/metrics` — `Collector` (`events.Observer`) + `Snapshot`.
- `adapters/autodisable` — `Breaker` (`events.Observer`) auto-pausing dead endpoints.
- `adapters/adminhttp` — `Handler` (stdlib `http.Handler`) admin API + HTML UI.
- `adapters/sqsqueue` — durable `DeliveryQueue` over SQS (id-only messages).
- `adapters/ratelimit` — `WrapDeliverer(next, Config)` per-destination throttle.
- `adapters/securestore` — `New(inner SubscriptionStore, codec SecretCodec)` — encrypt-at-rest store decorator.
- `adapters/aesgcm` — `New(key []byte) (*Codec, error)` — stdlib AES-256-GCM `SecretCodec`.
- `adapters/kmscodec` — `New(Config{Client, KeyID, EncryptionContext}) *Codec` — AWS KMS `SecretCodec`.
- `events.MultiObserver(...Observer) Observer` — compose observers.

`Deps` bundles the ports plus the `RetryPolicy`, an optional
`ValidateURL URLValidator` (SSRF vetting at `Subscribe` time), an optional
`Observer` (per-attempt metrics/logging hook), an optional `Catalog` (strict
event-type gate), an optional `Transformer` (per-endpoint body reshaping), and an
optional `MaxPayloadBytes` (Emit rejects a larger payload with `ErrPayloadTooLarge`;
0 = unbounded). `NewService` panics on a nil required port (misconfiguration is a
programming error).

```go
// Transformer optionally reshapes the outbound body per subscription, before
// signing (so the signature covers what is sent). Nil = deliver verbatim.
type Transformer interface {
    Transform(ctx context.Context, sub Subscription, ev Event, body []byte) ([]byte, error)
}
type TransformerFunc func(ctx context.Context, sub Subscription, ev Event, body []byte) ([]byte, error)
```

A non-nil transform error aborts the attempt without sending and is retried by the
normal policy. Reserved headers stay protected — a transform shapes the body only.

### Batteries-included facade (`webhooks`)

```go
func New(ctx context.Context, o Options) (*System, error) // *System embeds *events.Service
func (s *System) Close()                                   // stop; waits for workers (an interrupted attempt is not recorded)
```

`Options` requires `Store` and `Log`; `Deliverer`, `Signer`, `Policy`, `Workers`,
`Clock`, `IDGen`, `Rand`, `Observer`, and `ValidateURL` all default.

## Domain types

```go
type Event struct {
    ID         string
    Type       string          // e.g. "drive.file.created"
    OrgID      string          // tenant; hard partition key
    OccurredAt time.Time
    Payload    json.RawMessage   // exact bytes signed & delivered
    Headers    map[string]string // optional custom outbound headers (e.g. W3C traceparent)
}

type Subscription struct {
    ID             string
    OrgID          string
    EventTypes     []string        // exact types, or the wildcard "*"
    URL            string
    Secret         string          // json:"-"; never logged
    Active         bool            // paused subscriptions match nothing
    CreatedAt      time.Time
    PayloadFilters []PayloadFilter // optional content filters (AND); empty = all
}
func (s Subscription) Matches(eventType string) bool
func (s Subscription) MatchesPayload(payload []byte) bool // all filters must match

// PayloadFilter narrows delivery by a value inside the event payload, on top of
// the type filter. The JSON value at dot-path Path must equal Equals as a string
// (string→literal, number→source text, bool→"true"/"false", null→"null");
// a missing path or a non-scalar (object/array) value never matches.
type PayloadFilter struct {
    Path   string // dot-path, e.g. "file.owner.id"
    Equals string
}

type DeliveryStatus string
const (
    StatusPending   DeliveryStatus = "pending"
    StatusDelivered DeliveryStatus = "delivered"
    StatusFailed    DeliveryStatus = "failed" // will retry
    StatusDead      DeliveryStatus = "dead"   // dead-lettered
)

type Delivery struct {
    ID, OrgID, SubscriptionID, EventID, EventType string
    Status    DeliveryStatus
    Attempts  int
    LastError string
    CreatedAt, UpdatedAt time.Time
}

type Attempt struct {
    N       int           // 1-based
    At      time.Time
    Code    int           // HTTP status; 0 on transport error
    Latency time.Duration
    Error   string
}

// DeliveryID is the stable, idempotent id for an (event, subscription) pair.
// Deterministic (no IDGen): re-emitting yields the same id; subscribers dedup.
func DeliveryID(subscriptionID, eventID string) string

func (s Subscription) Matches(eventType string) bool // exact | "*" | "prefix.*"
func MatchType(pattern, eventType string) bool        // the pattern grammar, standalone
```

## Event catalog

```go
// EventTypeDef documents one event type a service publishes.
type EventTypeDef struct {
    Type        string          // the stable event type, e.g. "drive.file.created"
    Description string          // human-readable
    Example     json.RawMessage // an illustrative payload; valid JSON if set
    Deprecated  bool
}

// Catalog is a concurrency-safe registry of a service's event vocabulary.
// Pure (no I/O). A strict catalog lets Emit reject undeclared types.
func NewCatalog(strict bool) *Catalog
func (c *Catalog) Register(defs ...EventTypeDef) *Catalog // chainable; panics on empty Type
func (c *Catalog) Get(eventType string) (EventTypeDef, bool)
func (c *Catalog) Has(eventType string) bool
func (c *Catalog) List() []EventTypeDef // registration order; a copy
func (c *Catalog) Strict() bool
```

Wire it via `Deps.Catalog`. When it is set **and** strict, `Emit` returns
`ErrUnknownEventType` for any event whose `Type` was never registered (`Ping`'s
synthetic `webhook.ping` bypasses `Emit` and is unaffected). A non-strict catalog
is documentation only. The admin surface serves it at `GET /event-types` when
constructed with `adminhttp.New(api, org, adminhttp.WithCatalog(cat))`.

## Secret encryption at rest

```go
// SecretCodec encrypts/decrypts a subscription secret for storage at rest.
// Round-trip: Decrypt(ctx, Encrypt(ctx, s)) == s.
type SecretCodec interface {
    Encrypt(ctx context.Context, plaintext string)  (ciphertext string, err error)
    Decrypt(ctx context.Context, ciphertext string) (plaintext string, err error)
}
```

Wrap any store with the decorator, then use the result as your `SubscriptionStore`:

```go
// adapters/securestore — encrypt on Put, decrypt on Get/List; adds no cross-org path.
func New(inner events.SubscriptionStore, codec events.SecretCodec) *Store

// adapters/aesgcm — stdlib AES-256-GCM; you hold the 32-byte key.
func New(key []byte) (*Codec, error) // ErrKeySize unless len(key)==32

// adapters/kmscodec — AWS KMS; key material never leaves KMS. Bring your own client.
type Config struct { Client KMS; KeyID string; EncryptionContext map[string]string }
func New(cfg Config) *Codec
```

An empty secret is passed through by `securestore` without invoking the codec.

## Retry policy

```go
type RetryPolicy struct {
    MaxAttempts int           // total attempts including the first
    BaseDelay   time.Duration
    MaxDelay    time.Duration // cap before jitter; 0 = uncapped
    Multiplier  float64       // growth per attempt
    JitterFrac  float64       // +/- fraction; 0 disables
    RetryOn     func(code int, err error) bool // nil => DefaultRetryable
}
func DefaultRetryPolicy() RetryPolicy // 5 attempts, 1s base, x2, cap 5m, ±20%
func DefaultRetryable(code int, err error) bool // 5xx/429/408/transport => retry
func (p RetryPolicy) Backoff(attempt int, jitter float64) time.Duration // pure

const AbsoluteMaxRetryDelay = 24 * time.Hour // hard ceiling on any retry delay,
// applied even when MaxDelay == 0, so a hostile Retry-After can't park a job.
```

## Ports (a service supplies adapters)

```go
type SubscriptionStore interface {
    ListForOrgAndType(ctx, orgID, eventType string) ([]Subscription, error)
    ListForOrg(ctx, orgID string) ([]Subscription, error)
    Get(ctx, orgID, subscriptionID string) (Subscription, error)
    Put(ctx, sub Subscription) error
    Delete(ctx, orgID, subscriptionID string) error
}

type DeliveryQueue interface {
    Enqueue(ctx, job Job) error
}

type Deliverer interface {
    Post(ctx, req DeliveryRequest) (DeliveryResponse, error)
}
type DeliveryRequest  struct { URL string; Body []byte; Headers map[string]string }
type DeliveryResponse struct { StatusCode int; Latency time.Duration }

// Signer authenticates a delivery and returns the auth headers to merge.
type SigningInput struct {
    EventID, DeliveryID, EventType string
    Timestamp                      time.Time
    Body                           []byte
    Secret                         string
}
type Signer interface {
    Sign(in SigningInput) map[string]string
}
// Adapters: hmacsign (X-Signature + X-Event-Timestamp), stdwebhook (Standard Webhooks).

type DeliveryLog interface {
    Record(ctx, rec DeliveryRecord) error
    Get(ctx, orgID, deliveryID string) (Delivery, EventSnapshot, []Attempt, error)
    List(ctx, orgID string, f DeliveryFilter) (page []Delivery, nextCursor string, err error)
}
type DeliveryRecord struct { Delivery Delivery; Event EventSnapshot; Attempt Attempt }
type DeliveryFilter struct { SubscriptionID, Cursor string; Limit int }

// Testability ports — the only non-determinism in the core.
type Clock interface { Now() time.Time }
type IDGen interface { NewID() string }
type Rand  interface { Float64() float64 }   // retry jitter

// Optional per-attempt hook (metrics/logging). Must not block or panic.
type Observer interface { OnAttempt(ctx context.Context, d Delivery, a Attempt) }
type ObserverFunc func(ctx context.Context, d Delivery, a Attempt) // adapts a func
```

## Job & headers

```go
type Job struct {
    Delivery     Delivery
    Event        Event
    Subscription Subscription
    AttemptN     int       // 1-based
    NotBefore    time.Time // backoff gate
}
func (j Job) Ref() JobRef // id-only, secret-free reference for durable queues
```

### Durable / off-process queues

```go
// JobRef is what a durable queue (SQS, Redis, Postgres, Kafka) persists — ids
// and scheduling only; no payload, no secret.
type JobRef struct {
    OrgID, DeliveryID, SubscriptionID, EventID, EventType string
    AttemptN  int
    NotBefore time.Time
}
type Rehydrator   interface { Rehydrate(ctx, ref JobRef) (Job, error) }   // reload snapshot+sub
type RefDispatcher interface { ProcessRef(ctx, ref JobRef) error }        // rehydrate + dispatch
// *Service implements both. A durable adapter enqueues job.Ref() and calls ProcessRef.
// ProcessRef drops references that are terminal or whose referent is gone (at-least-once safe).
// Reference adapter: adapters/sqsqueue.
```

Every outbound request carries:

| Header | Meaning |
|---|---|
| `X-Event-Id` | source event id |
| `X-Event-Type` | event type |
| `X-Delivery-Id` | stable idempotency key — subscribers dedup on this |
| `X-Event-Timestamp` | unix seconds at send; anti-replay with a tolerance window |
| `X-Signature` | `Signer` output (hmacsign binds `{deliveryId}.{timestamp}.{body}`) |
| `X-Attempt` | 1-based attempt number |

Constants: `events.HeaderEventID`, `HeaderEventType`, `HeaderDelivery`,
`HeaderTimestamp`, `HeaderSignature`, `HeaderAttempt`.

Custom headers set on `Event.Headers` are merged onto each outbound request
(persisted in the snapshot, so they survive a durable-queue round trip). The
reserved names above — plus `Content-Type`, `Host`, `Content-Length`, and the
Standard Webhooks `webhook-*` names — cannot be overridden by a custom header.
Use this for trace propagation (e.g. `traceparent` / `tracestate`) with no OTel
dependency.

## Errors

```go
var (
    ErrInvalidEvent        // Emit: missing id/type/orgId
    ErrInvalidSubscription // Subscribe: missing required fields
    ErrTenancyViolation    // org mismatch between event and subscription
    ErrNotFound            // delivery/subscription absent in the org
    ErrUnknownEventType    // Emit: type not in a strict Deps.Catalog
    ErrPayloadTooLarge     // Emit: payload exceeds Deps.MaxPayloadBytes
)
```

## Signer adapter helpers (`adapters/hmacsign`)

The MAC covers `{deliveryID}.{timestamp}.{body}`, so the delivery id and timestamp
headers are authenticated (not forgeable). Pass them from `events.HeaderDelivery`
and `events.HeaderTimestamp`.

```go
func New() Signer
func GenerateSecret() (string, error) // "whsec_<hex>", 256-bit
func Signature(secret, deliveryID, timestamp string, body []byte) string // "sha256=<hex>"
func Verify(secret string, deliveryID, timestamp string, body []byte, header string) bool
func VerifyRequest(secret string, rawBody []byte, idHeader, sigHeader, tsHeader string, now time.Time, tolerance time.Duration) error
```
