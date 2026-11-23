# Migration: ab0t-events-go v0.2.0 → v0.2.1

**Bug fixes. Your code compiles unchanged; no API was removed and no signature changed.** Some behaviour you
can observe changes; the table says who can notice and what to do (usually nothing).

```bash
go get github.com/ab0t-com/ab0t-events-go@v0.2.1 && go mod tidy
/path/to/ab0t-events-go/migrations/v0.2.0-to-v0.2.1/migrate-check.sh .    # read-only, advisory
go build ./... && go vet ./... && go test ./...
```

| # | Rule | Change | Your action |
|---|---|---|---|
| E1 | `ATTEMPTS` | Durable path (`ProcessRef`): a failed attempt is written in two steps. `Delivery.Attempts` reads N-1 until the retry is enqueued, then N (for a retry due within 5 s, when that retry runs). A redelivered copy of an attempt whose retry is already scheduled is dropped. | None, unless you read `Delivery.Attempts` on durable deliveries as "attempts made so far"; count `Attempts` from `GetDelivery`'s attempt list instead. |
| E2 | `LOGIMPL` | Each durable retry scheduled ≥ 5 s out costs one extra `DeliveryLog.Record` with `Attempt.N == 0`. | A custom `DeliveryLog` must update the delivery and append nothing for `Attempt.N == 0` (the documented contract; `memory` and `dynamo` already do). |
| E3 | — | In-process retries (`Process`, `AttemptN > 1`) re-read the subscription: a rotated secret is used, a deleted subscription is not sent to. One `SubscriptionStore.Get` per retry. | None. |
| E4 | `SHUTDOWN` | An attempt with no answer whose context was **cancelled** (`Close`, queue stop) is not recorded, not observed, not retried; `Process` / `ProcessRef` return `context.Canceled`. A deadline is still a failed attempt. `sqsqueue` stops a batch mid-way and reports the cancellation to `Config.OnError`. | A custom queue adapter: treat a returned `context.Canceled` as "not done" (do not ack/delete). With the in-process queue (`inproc`, `webhooks.New`): call `Recover(ctx, orgID)` at start. If `OnError` alerts on every error, ignore `context.Canceled` during shutdown. |
| E5 | `BYHOST` | `ratelimit.ByHost` keys are canonical: lower-cased host, no trailing dot, no scheme-default port. | None, unless you read or log the keys; spellings of one host now share one key. |
| E6 | — | `webhooks.System.Close` is safe to call concurrently. | None. |

Mark a reviewed line with `// eventsgo:v0.2.1-reviewed`, or a whole file with
`// eventsgo:v0.2.1-reviewed-file`.
