# Completion Workflow — remaining PMM items

A driven, three-role loop that carries the remaining PMM roadmap items to
tested-and-documented completion.

## Roles

- **Driver** (orchestrator): owns this ledger. Tells the Implementer to
  `continue` to the next open claim, dispatches the Verifier per claim, records
  PASS/FAIL, and stops when the ledger is fully resolved.
- **Implementer** (persistent subagent): takes the next OPEN claim, implements +
  tests + documents it, runs the full proof, reports `COMPLETE` with evidence to
  `main`, then ends its turn (resumed via SendMessage `continue`).
- **Verifier** (persistent subagent, read-only): independently re-runs the proof
  and the claim's behavioral checks; reports `PASS` or `FAIL <notes>` to `main`.

## Loop

```
Driver → Implementer: work claim C-N
Implementer → main:   COMPLETE C-N (+files, +test output)
Driver → Verifier:    verify C-N
Verifier → main:      PASS | FAIL <notes>
  PASS → Driver marks C-N DONE, → Implementer: continue (C-N+1)
  FAIL → Driver → Implementer: fix <notes>, re-verify
repeat until every claim is DONE (or DEFERRED with recorded rationale)
→ Driver: final full proof → workflow COMPLETE
```

## Definition of done (every claim)

1. Follows the hexagonal/adapter pattern; core stays pure (no `time.Now`/`rand`/IO).
2. No exotic deps — stdlib, or an `aws-sdk-go-v2` module already in the cache.
3. Offline proof all green:
   `GOPROXY=off go build ./... && go vet ./... && gofmt -l . (empty) && go test -count=1 ./... && go test -race -count=1 ./...`
4. Tenancy & secret invariants preserved (see docs/CONTRACTS.md).
5. Docs updated: `docs/CONTRACTS.md`, `docs/INTERFACE.md`, `CONSUMING.md`,
   `CHANGELOG.md`, and an append to `docs/tickets/WORKLOG.md`. README table if a
   new package.

## Claim ledger

| Claim | PMM item | Scope | Status |
|---|---|---|---|
| C-13 | 10 · Event catalog | `events.Catalog`: registry of event types {Type, Description, Example, Deprecated}; Register/Get/List/Has; optional `Deps.Catalog` so Emit can reject unknown types when strict; admin `GET /event-types`. Dependency-free. | DONE |
| C-14 | 13 · Secret encryption at rest | `events.SecretCodec` seam (Encrypt/Decrypt); `adapters/securestore` decorator that encrypts `Subscription.Secret` on Put / decrypts on Get+List; codecs `adapters/aesgcm` (stdlib AES-256-GCM) and `adapters/kmscodec` (aws KMS, cached). Fake-KMS + aesgcm tests. | DONE |
| C-15 | 12 · Payload filters | `Subscription.PayloadFilters []PayloadFilter{Path, Equals}` matched against the event payload (dot-path, dependency-free) during Emit fan-out, in addition to the type filter. | DONE |
| C-16 | 9 · Tracing / custom headers | `Event.Headers map[string]string` (custom outbound headers, reserved names protected), persisted in the snapshot so it is durable-safe; merged in Process. Enables W3C `traceparent` propagation without an OTel dependency. | DONE |
| C-17 | 14 · Batching | Evaluate: deliver multiple events per request. Implement a clean minimal version OR record an explicit DEFERRAL with rationale (design + why now-vs-later). | DEFERRED (rationale + design sketch in WORKLOG) |
| C-18 | 15 · Transformations / portal polish | Minimal per-endpoint transform hook OR explicit DEFERRAL with rationale. | DONE (transform hook; portal UI out of scope — see adminhttp) |

Resolve every row to DONE or DEFERRED (with rationale in the WORKLOG and the PMM
report status table). Then run the final proof.
