# ab0t-events-go v0.2.1: six bug fixes, nothing to change

`go get github.com/ab0t-com/ab0t-events-go@v0.2.1`. Your code compiles unchanged; no API was removed and no
signature changed.

- **No duplicate webhook from a redelivered failed attempt** on a durable queue (`sqsqueue`): a stale copy is
  dropped instead of being sent again and starting a second retry chain.
- **Retries use the current subscription.** On the in-process queue too, a retry after `RotateSecret` is signed
  with the new secret, and a retry after `Unsubscribe` is not sent.
- **A shutdown is not a failed delivery.** An attempt cancelled by `Close` or a queue stop is not recorded, not
  dead-lettered and not counted by `autodisable`. With the in-process queue, call `Recover` at start to send it.
- **`sqsqueue` stops mid-batch on shutdown**; the rest of the batch is redelivered by SQS.
- **`ratelimit.ByHost`** gives `https://Hooks.Example.com.:443/a` and `https://hooks.example.com/b` one
  bucket (one receiver, one rate limit).
- **`webhooks.System.Close`** is safe to call from two goroutines at once.

Only code that reads `Delivery.Attempts` on the durable path, implements its own `DeliveryLog` or queue
adapter, or relies on the old shutdown behaviour can notice a difference — see `MIGRATION.md`. Check a
repository in one step: `./migrate-check.sh /path/to/your/repo` (from the `v0.2.0-to-v0.2.1` kit, advisory
only).
