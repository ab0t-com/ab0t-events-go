# Security Audit — `ab0t-events-go`

- **Target:** `github.com/ab0t-com/ab0t-events-go` (outbound webhook-delivery library)
- **Date:** 2026-08-12
- **Scope:** Full library — `events/` core, all `adapters/*`, `webhooks/` facade. Source-code review only (no code was modified; no dynamic testing).
- **Threat model:** Multi-tenant SaaS emitting signed webhooks to org-registered (attacker-influenced) URLs. Adversaries considered: (a) a malicious/curious tenant org, (b) a malicious webhook *subscriber* (controls the receiving endpoint and its DNS/responses), (c) a network/on-path attacker who can capture a delivery, (d) an operator with raw datastore/queue access.

---

## Executive summary

The library is well-architected for security: tenancy is a hard partition enforced *both* in the store adapters and re-checked defense-in-depth in the core; secrets are `json:"-"`, kept out of the durable queue (`JobRef`), encryptable at rest, and never logged; the SSRF guard pins the vetted IP at dial time (DNS-rebind safe) and refuses redirects; idempotency uses a deterministic delivery id and terminal-state drop to make at-least-once queues safe; retry backoff is capped so a hostile `Retry-After` cannot park a job under the default policy. The dependency surface is minimal (AWS SDK only, all first-party).

The audit found **no Critical** issues but **one High**: the **default HMAC signer (`hmacsign`) does not bind the timestamp or delivery id into the signature**, so the anti-replay/dedup guarantee the package advertises is forgeable by a capture-capable attacker. (The interop `stdwebhook` signer does it correctly.) The remaining findings are hardening items: SSRF denylist gaps (CGNAT / `0.0.0.0/8` / NAT64), `http://` allowed alongside `https://`, and unbounded in-memory growth in the single-process adapters.

**Findings by severity:** Critical 0 · High 1 · Medium 3 · Low 5 · Info 4

> **Remediation status (updated 2026-08-12):** ALL findings triaged and resolved.
> Findings **1 (High)**, **2**, **3**, **4 (Medium)**, plus Lows **6**, **7**, **9**
> and the `ratelimit` half of **8** are **FIXED** (each with a proving test).
> Findings **5**, the `autodisable` half of **8**, **10**, **11**, **12**, **13**
> are **ACCEPTED** (by-design / accepted-risk) with rationale + residual-risk notes
> below. See per-finding "Resolution" / "Disposition" notes and the CHANGELOG. No
> finding remains open.

---

## Findings

| # | Title | Severity | Location | Status |
|---|-------|----------|----------|--------|
| 1 | HMAC signature omits timestamp & delivery-id → anti-replay/dedup bypass (default signer) | **High** | `adapters/hmacsign/hmacsign.go:48-59` | **FIXED** |
| 2 | SSRF guard is a denylist with gaps: CGNAT `100.64.0.0/10`, `0.0.0.0/8`, NAT64 | **Medium** | `adapters/httpdeliver/httpdeliver.go:196-203` | **FIXED** |
| 3 | Plaintext `http://` accepted — no HTTPS-only enforcement | **Medium** | `adapters/httpdeliver/httpdeliver.go:153-165` | **FIXED** |
| 4 | Unbounded in-memory queue growth (in-proc scheduler) | **Medium** | `adapters/inproc/inproc.go:54-64` | **FIXED** |
| 5 | Built-in HTML replay form has no CSRF token | **Low** | `adapters/adminhttp/ui.go:96-98` | **ACCEPTED** |
| 6 | `Retry-After` uncapped when `MaxDelay == 0` (custom policy) | **Low** | `events/service.go:306-312` | **FIXED** |
| 7 | `Replay` does not reset log status → silent no-op on durable (SQS) queues | **Low** | `events/service.go:563-583` | **FIXED** |
| 8 | Unbounded per-key maps in `autodisable` and `ratelimit` | **Low** | `adapters/autodisable/autodisable.go:73-78`, `adapters/ratelimit/ratelimit.go:86-90` | **FIXED** (ratelimit) / **ACCEPTED** (autodisable) |
| 9 | No payload-size cap in `Emit` | **Low** | `events/service.go:142-185` | **FIXED** |
| 10 | Subscribe-time `URLValidator` is nil by default | **Info** | `webhooks/webhooks.go:58-97` | **ACCEPTED** |
| 11 | Event `Type` unvalidated without a strict catalog | **Info** | `events/service.go:142-150` | **ACCEPTED** |
| 12 | `VerifyRequest` with `tolerance <= 0` silently disables replay check | **Info** | `adapters/hmacsign/hmacsign.go:89-104`, `adapters/stdwebhook/stdwebhook.go:110-128` | **ACCEPTED** |
| 13 | DynamoDB delivery item unbounded by attempt/payload growth vs 400 KB item cap | **Info** | `adapters/dynamo/log.go:78-99` | **ACCEPTED** (mitigated by #9) |

---

### 1. HMAC signature omits timestamp & delivery-id → anti-replay/dedup bypass — **High**

**Location:** `adapters/hmacsign/hmacsign.go:48-59` (`Sign` / `Signature`), verified against `:85-105` (`VerifyRequest`); it is the default signer wired in `webhooks/webhooks.go:65-67`.

**Description.** `hmacsign` computes `X-Signature = "sha256=" + HMAC(secret, rawBody)` — **over the body only**. The `X-Event-Timestamp` header (and the core-set `X-Delivery-Id` / `X-Event-Id` headers) are transmitted alongside but are **not covered by the MAC**. `VerifyRequest` recomputes the HMAC over the body, then *separately* parses the timestamp header and checks it against a tolerance window (`:89-104`).

Because the timestamp is unauthenticated, the freshness window is decorative: an attacker holding one captured, validly-signed request can **rewrite `X-Event-Timestamp` to "now"** and the signature still verifies. The delivery-id header is likewise unsigned, so an attacker can also mint a fresh `X-Delivery-Id`, defeating the subscriber-side idempotent dedup the library recommends (`events/event.go:82-83`, `:124-130`).

**Attack scenario.** Subscriber processes a `payment.succeeded` webhook idempotently (dedup on delivery-id, reject stale timestamps). Attacker captures one valid delivery (feasible on-path — trivially so if the endpoint is `http://`, see Finding 3 — or via a logging proxy / TLS-terminating middlebox). Attacker re-POSTs the same signed body N times, each with a current timestamp and a new random delivery-id. Every copy passes `VerifyRequest`. Result: duplicate side effects (double credit / double fulfillment). The library's own docstring claims this is prevented ("defeats replay of an old, validly-signed body" — `:85-88`), so this is a *non-functional advertised security control*, not merely a missing feature.

**Contrast (done right):** `stdwebhook` signs the canonical string `{id}.{timestamp}.{payload}` (`adapters/stdwebhook/stdwebhook.go:74-82`), binding both the id and the timestamp into the MAC, and uses a constant-time compare (`:93`). It is not vulnerable.

**Precondition / honesty note.** The attacker must obtain one valid signed request. Over the recommended HTTPS transport this requires capturing ciphertext-plus-a-TLS-break or access to a place the plaintext request is stored/proxied. It is not a remote-unauthenticated break of the secret. Rated High rather than Critical on that basis, but High because it silently voids a documented control that downstream integrators will rely on.

**Recommendation.** Make the `hmacsign` MAC cover the timestamp (and ideally the delivery id), e.g. sign `timestamp + "." + body` (or `deliveryID + "." + timestamp + "." + body`) and verify the same canonical string — mirroring `stdwebhook`. Alternatively, change the batteries-included default in `webhooks.New` to `stdwebhook.New()`. Until fixed, document prominently that `hmacsign`'s timestamp is advisory and does not provide replay protection.

**Resolution (FIXED).** `hmacsign` now signs the canonical string `{deliveryID}.{timestamp}.{body}` (mirroring `stdwebhook`), binding both the delivery id and the timestamp into the MAC. `Signature`, `Verify`, and `VerifyRequest` take the delivery id and timestamp and recompute over the same string; rewriting either header now fails signature verification. Proving test `TestSignatureBindsTimestampAndID` (and `TestVerifyRequest_TimestampTolerance`'s rewritten-timestamp case) assert a forged timestamp/id fails to verify. Docstring corrected.

---

### 2. SSRF guard is a denylist with reachable gaps — **Medium**

**Location:** `adapters/httpdeliver/httpdeliver.go:196-203` (`blocked`), used by `guardedDialContext` (`:173-194`).

**Description.** The dial-time guard is structurally strong — it resolves the host, checks the resolved IP(s), and dials the vetted IP directly, so DNS rebinding cannot slip past (`:180-192`). The cloud-metadata endpoints that matter on AWS **are** blocked: `169.254.169.254` and `169.254.170.2` are link-local and caught by `IsLinkLocalUnicast()`; IPv4-mapped forms (`::ffff:169.254.169.254`) are caught because Go's `IsLinkLocalUnicast` normalizes via `To4()`; IPv6 ULA `fc00::/7` is caught by `IsPrivate()`.

However, `blocked()` is a **denylist**, while the package doc comment claims it "rejects any non-global-unicast" (`:170-172`) — i.e. it *describes* allowlist behavior it does not implement. Reachable ranges the denylist misses:

- **`100.64.0.0/10` (RFC 6598 carrier-grade NAT / "shared address space").** `net.IP.IsPrivate()` returns *false* for this range. It is used for pod/node networking in some Kubernetes/EKS and cloud environments, where internal services may be reachable there.
- **`0.0.0.0/8` except the exact `0.0.0.0`.** `IsUnspecified()` only matches `0.0.0.0`; on Linux, connecting to `0.0.0.1`–`0.255.255.255` routes to the local host, a classic loopback-bypass.
- **NAT64 `64:ff9b::/96`.** Global-unicast, so not blocked; in a NAT64 environment it wraps arbitrary IPv4 — including `64:ff9b::a9fe:a9fe` for `169.254.169.254` — reaching metadata despite the link-local block.

**Attack scenario.** A tenant registers a subscription whose host resolves into `100.64.0.0/10` (or `0.0.0.2`, or a NAT64-wrapped metadata address). The guard passes it, and the deliverer POSTs the (attacker-chosen-shape but org-scoped) request to an internal address, turning the webhook egress into a blind SSRF probe / internal request forgery in environments using those ranges.

**Recommendation.** Prefer an **allowlist**: require `ip.IsGlobalUnicast() && !ip.IsPrivate()` and reject everything else (this matches the doc comment). At minimum add `100.64.0.0/10`, `0.0.0.0/8`, `192.0.0.0/24`, `192.0.2.0/24`, `198.18.0.0/15`, and the NAT64 prefix `64:ff9b::/96` to the denylist. Consider making the blocklist configurable so operators can add environment-specific internal CIDRs.

**Resolution (FIXED).** `blocked()` is now an allowlist: it rejects anything that is not `ip.IsGlobalUnicast()` or that is `ip.IsPrivate()`, then additionally blocks the global-unicast special-use ranges `0.0.0.0/8`, `100.64.0.0/10`, `192.0.0.0/24`, `192.0.2.0/24`, `198.18.0.0/15`, `198.51.100.0/24`, `203.0.113.0/24`, `240.0.0.0/4`, `255.255.255.255/32`, and NAT64 `64:ff9b::/96` + `64:ff9b:1::/48`. The doc comment was corrected to describe allowlist behavior. Proving test `TestBlocked_SSRFRanges` asserts each newly-blocked range (incl. the NAT64-wrapped metadata address `64:ff9b::a9fe:a9fe`) is refused while public addresses pass.

---

### 3. Plaintext `http://` accepted — no HTTPS-only enforcement — **Medium**

**Location:** `adapters/httpdeliver/httpdeliver.go:153-165` (`validateScheme`).

**Description.** `validateScheme` accepts both `http` and `https`. There is no option to require TLS. Event payloads may carry PII/business data, and the HMAC only authenticates the *sender to the receiver* — it provides no confidentiality and does not authenticate the receiver. Delivering over `http://` exposes the full payload and all headers to any on-path observer and hands an attacker a clean capture of a validly-signed request, which directly enables Finding 1's replay.

**Attack scenario.** Tenant registers `http://reports.example.com/hook`. Every emitted event traverses the network in cleartext; an on-path attacker reads payloads and harvests signed requests for replay.

**Recommendation.** Add a config flag (default on for `NewSecure`) that restricts the scheme to `https`, optionally allowing `http` only for loopback in dev. At minimum, document that `http` endpoints receive payloads in cleartext and are not replay-protected.

**Resolution (FIXED).** Added `Config.RequireHTTPS` (on by default in `NewSecure`). When set, `validateScheme` rejects plaintext `http://` with `ErrInsecureScheme`, except to loopback hosts (kept for local dev/tests; loopback is refused anyway by the SSRF guard in production). Proving test `TestValidateScheme_RequireHTTPS`.

---

### 4. Unbounded in-memory queue growth (in-proc scheduler) — **Medium**

**Location:** `adapters/inproc/inproc.go:54-64` (`Enqueue` → `heap.Push` with no cap).

**Description.** The in-process queue pushes every job onto an unbounded heap. Under a burst of emits or a large fan-out with slow/failing subscribers (each failure re-enqueues a delayed retry), the heap grows without limit, and each `Job` carries the full `Event` payload and `Subscription` (secret included, in memory). There is no backpressure and no max depth; `Len()` is observability only. This is a memory-exhaustion DoS for single-process deployments. (The durable SQS adapter does not have this issue and is the documented production path.)

**Attack scenario.** A tenant emits at a high rate to many subscriptions whose endpoints time out; retries accumulate faster than they drain, growing the heap until OOM.

**Recommendation.** Add a configurable max queue depth with a defined overflow policy (reject `Enqueue`, or shed oldest), and/or document a hard ceiling and steer production to the durable queue. Expose queue depth as a metric for alerting.

**Resolution (FIXED).** Added `inproc.Config.MaxDepth` with a `DefaultMaxDepth` of 10000 (backstop applied even when unset; a negative value opts into unbounded). `Enqueue` returns the new `ErrQueueFull` at capacity (backpressure) — the delivery stays `pending` in the log and is recoverable via `Service.Recover` once the backlog drains. `Len()` already exposes depth for alerting. Proving tests `TestQueue_MaxDepthBackpressure` and `TestQueue_UnboundedWhenNegative`. (Production still steered to the durable SQS queue.)

---

### 5. Built-in HTML replay form has no CSRF token — **Low**

**Location:** `adapters/adminhttp/ui.go:96-98` (state-changing `POST .../replay` form) and `:20-27` (the acknowledging doc comment).

**Description.** The bundled `/ui` page renders a `POST` "Replay" form with no anti-CSRF token. The code comments explicitly punt CSRF to the mounting application ("put the handler behind your CSRF protection… With header/bearer auth it is not exploitable"). That is a reasonable library posture, but the built-in form is a footgun for anyone who mounts `/ui` behind cookie/session auth: a cross-site form-post could trigger replays for the victim's org. Note the XSS surface here is clean — `html/template` auto-escapes `{{.LastError}}` (subscriber-controlled response snippets) and the `badge` helper returns only fixed `template.CSS` (`:48-60`), so no stored XSS.

**Recommendation.** Either omit the mutating form from the built-in UI (make `/ui` read-only and leave replay to the JSON API), or emit and verify a CSRF token / require a non-cookie auth header for the replay POST, or set `SameSite` guidance. Keep the current warning regardless.

**Disposition (ACCEPTED — by design).** A real CSRF defense needs per-session token
state that lives in the mounting application, not in a stateless, auth-agnostic
library handler — the package is explicitly designed to be mounted *behind* the
integrator's auth + CSRF middleware, and its doc comment says so. With the
recommended header/bearer auth the form is not exploitable, and the audit confirmed
the page's XSS surface is clean (`html/template` auto-escaping; `badge` returns a
fixed `template.CSS`). We keep the built-in Replay form (removing it would drop a
useful capability for the header-auth majority where it is safe) and the prominent
CSRF warning. **Residual risk:** an integrator who mounts `/ui` under cookie/session
auth *without* CSRF protection is exposed to cross-site replay POSTs; the mitigation
(mount behind CSRF, or use the JSON API) is documented at `adapters/adminhttp/ui.go`.

---

### 6. `Retry-After` uncapped when `MaxDelay == 0` — **Low**

**Location:** `events/service.go:306-312`; `adapters/httpdeliver/httpdeliver.go:135-151` (`parseRetryAfter`).

**Description.** A hostile subscriber can return an arbitrarily large `Retry-After`. `parseRetryAfter` returns it uncapped, and `Process` honors it as a lower bound on the next backoff. This is correctly capped by `RetryPolicy.MaxDelay` — **but only when `MaxDelay > 0`** (`:310`). `MaxDelay == 0` is documented as "uncapped" (`events/retry.go:13`). The default policy sets 5 minutes (`retry.go:30`), so the default is safe; an operator who zeroes `MaxDelay` lets a subscriber park a retry far in the future (bounded in SQS to a 12 h re-defer loop, effectively unbounded in-proc).

**Recommendation.** Apply an absolute hard ceiling (e.g. 24 h) to any subscriber-supplied `Retry-After` independent of `MaxDelay`, or document that `MaxDelay == 0` disables hostile-`Retry-After` protection.

**Resolution (FIXED).** `Process` now clamps every computed retry delay to the new
`events.AbsoluteMaxRetryDelay` (24 h) constant **regardless of `MaxDelay`**, so a
hostile `Retry-After` can never park a job beyond a day even under `MaxDelay == 0`.
Proving test `TestRetryAfter_AbsoluteCeiling` (100-day `Retry-After` with
`MaxDelay==0` is clamped to exactly 24 h).

---

### 7. `Replay` does not reset log status → silent no-op on durable queues — **Low**

**Location:** `events/service.go:563-583` (`Replay`), interacting with `:439-451` (`ProcessRef`).

**Description.** `Replay` sets `del.Status = StatusPending` only on the in-memory `Job` it enqueues; it does **not** write the pending status back to the delivery log. With the in-process queue (which carries the full `Job`), replay works. With a durable queue, `Enqueue` serializes `JobRef` (`ports.go:24-34`) and the worker calls `ProcessRef` → `Rehydrate`, which reloads the delivery from the log — where the status is still `delivered`/`dead` — so `ProcessRef` drops it as terminal (`:447-449`). Net effect: the admin "Replay" action silently does nothing on the SQS deployment. This is primarily a correctness bug, but it has security relevance: replay is an incident-recovery/redelivery tool, and a silent no-op can mask a failed redelivery during an incident.

**Recommendation.** In `Replay`, persist the status reset to the log (`log.Record` with `StatusPending`) before enqueuing, so the rehydrated job is non-terminal; add a test that exercises replay through `ProcessRef`.

**Resolution (FIXED).** `Replay` now writes the `pending` status back to the log
(`log.Record`) before enqueuing, so a durable-queue rehydrate sees a non-terminal
delivery and `ProcessRef` re-delivers instead of dropping it. Proving test
`TestReplay_ResetsLogStatusForDurableQueues` (deliver → terminal, replay, then
`ProcessRef` from a `JobRef` re-POSTs) and the admin `TestAdmin_DeliveryDetailReplayAndUI`
now asserts the post-replay status is `pending`.

---

### 8. Unbounded per-key maps in `autodisable` and `ratelimit` — **Low**

**Location:** `adapters/autodisable/autodisable.go:73-78` (`streak` map), `adapters/ratelimit/ratelimit.go:86-90` (`buckets` map).

**Description.** Both decorators keep a per-key map with no eviction. `autodisable.streak` keys by subscription id (entries clear on success/disable, so growth is bounded by active-but-persistently-failing subs — modest). `ratelimit.buckets` keys by destination host (default `ByHost`) and never evicts; a tenant registering subscriptions across many distinct hostnames grows the map without bound. Slow memory-growth DoS.

**Recommendation.** Add periodic eviction of idle buckets/streaks (e.g. drop buckets untouched for > N minutes), or an LRU cap.

**Resolution — `ratelimit` (FIXED).** `Limiter` now evicts buckets untouched for
`idleEvictionTTL` (10 min) via a sweep that runs at most once per `sweepInterval`
(1 min). Eviction is safe: after that long idle, a bucket would refill to full on
its next use, so recreating it fresh is equivalent — this bounds the map to keys
seen within ~10 min. `NumBuckets()` exposes the footprint for metrics. Proving test
`TestReserve_EvictsIdleBuckets` (50 idle buckets collapse to 1 after the TTL; a
recently-touched bucket survives).

**Disposition — `autodisable` (ACCEPTED).** The `streak` map is self-bounding: an
entry is created only for a subscription that is actively dead-lettering, is
`delete`d on any successful delivery, and is `reset` (deleted) the moment the
breaker disables the subscription at the threshold. So its size is bounded by the
count of subscriptions *concurrently* failing-but-not-yet-disabled — a small,
transient set, not attacker-growable without also tripping the breaker that clears
it. **Residual risk:** negligible; a pathological churn of distinct failing subs
just below threshold keeps a bounded working set. No code change.

---

### 9. No payload-size cap in `Emit` — **Low**

**Location:** `events/service.go:142-185` (`Emit`), `:191-203` (`NewEvent`).

**Description.** `Emit`/`NewEvent` marshal and accept arbitrarily large payloads with no size limit. The payload is stored (event snapshot) and POSTed verbatim. There is no library-level bound; the practical ceiling is the DynamoDB 400 KB item limit (which would surface as a store error, see Finding 13) or downstream memory. The admin API bounds *inbound* JSON at 1 MiB (`adapters/adminhttp/adminhttp.go:272`), but the emit path (called by the host service) is unbounded.

**Recommendation.** Offer a configurable max payload size enforced in `Emit`, returning a clear error, so oversized events fail fast rather than at the store or on the wire.

**Resolution (FIXED).** Added optional `events.Deps.MaxPayloadBytes`; `Emit` rejects
an event whose payload exceeds it with the new `ErrPayloadTooLarge` before any
fan-out (no log write, no enqueue). 0 keeps the previous unbounded behavior
(non-breaking). Proving test `TestEmit_MaxPayloadBytes`. This also mitigates
Finding 13 (it caps what can reach a DynamoDB item).

---

### 10. Subscribe-time `URLValidator` is nil by default — **Info**

**Location:** `webhooks/webhooks.go:58-97` (`ValidateURL` left unset), `events/service.go:342-350` (used only if non-nil).

**Description.** The batteries-included facade does not set `ValidateURL`, so a subscription URL is not vetted at Subscribe time; it is only checked at dial time by the SSRF guard (which *is* the authoritative, DNS-rebind-safe control, and *is* enabled by default via `httpdeliver.NewSecure`). This is acceptable security-wise, but it means an unreachable/internal URL is accepted and stored, failing only later at delivery, and it relies entirely on the operator keeping `NewSecure` (not a plain `New` with `GuardPrivate:false`). Note the guard only runs when `GuardPrivate == true` (`httpdeliver.go:74-78`).

**Recommendation.** Wire a default `ValidateURL` in the facade that runs the same block-list as the dial guard (fail-fast + defense-in-depth), and document that disabling the guard (`New` without `GuardPrivate`) removes the only SSRF protection.

**Disposition (ACCEPTED — by design).** The authoritative SSRF control is the
dial-time guard, which is DNS-rebind-safe and **on by default** via
`httpdeliver.NewSecure` (and it was strengthened to an allowlist in Finding 2).
Subscribe-time `ValidateURL` is purely optional fail-fast/defense-in-depth, and it
is left to the integrator because the "right" policy is deployment-specific (allow
private hosts in a trusted internal deployment, forbid them on a public multi-tenant
one). `Deps.ValidateURL` is available for anyone who wants it. **Residual risk:** an
unreachable/internal URL is accepted and stored, failing only at first delivery
(cosmetic); the actual SSRF egress is still blocked by the dial guard. No code
change; behavior and knobs are documented in `README.md` / `CONSUMING.md`.

---

### 11. Event `Type` unvalidated without a strict catalog — **Info**

**Location:** `events/service.go:142-150`; catalog is optional (`Deps.Catalog`, `webhooks` sets none).

**Description.** Without a strict `Catalog`, `Emit` accepts any non-empty `Type`, which flows into the `X-Event-Type` header and into stored records. Header injection via a CRLF-laden type is *not* exploitable — Go's `net/http` rejects header values containing control characters at write time, so a malicious type fails the delivery rather than injecting. Still, unvalidated types weaken the event vocabulary and could bloat storage.

**Recommendation.** Encourage (or default to) a strict catalog in production; optionally enforce a charset/length constraint on `Type` in `Emit`.

**Disposition (ACCEPTED — not exploitable).** As the audit itself notes, header
injection via a CRLF-laden `Type` is not exploitable — Go's `net/http` rejects
header values with control characters at write time, so a malicious type fails the
delivery rather than injecting. The remaining concern (vocabulary hygiene / storage
bloat) is addressed by the strict event `Catalog` shipped in C-13
(`Deps.Catalog` + `NewCatalog(true)` → `Emit` returns `ErrUnknownEventType` for
undeclared types) and, for size, by the payload cap from Finding 9. Forcing a
charset/length rule on all callers would be an unnecessary, potentially breaking
constraint. **Residual risk:** an integrator who runs without a strict catalog can
emit arbitrary well-formed type strings (their own data-quality choice). No code
change.

---

### 12. `VerifyRequest` with `tolerance <= 0` silently disables the replay check — **Info**

**Location:** `adapters/hmacsign/hmacsign.go:101` (`if tolerance > 0`), `adapters/stdwebhook/stdwebhook.go:115` (`if tolerance > 0`).

**Description.** These subscriber-side helpers skip the timestamp/freshness check entirely when `tolerance <= 0`. A subscriber that passes `0` (easy to do by omission) unknowingly disables anti-replay. This is a caller footgun, not a library break, but combined with Finding 1 it means the default signer's replay protection is doubly fragile.

**Recommendation.** Document the semantics prominently and consider requiring an explicit "disable" sentinel rather than treating `0` as "no check".

**Disposition (ACCEPTED — documented caller contract).** This is a subscriber-side
helper contract, not a library break, and the `tolerance <= 0` semantics are now
documented in the `hmacsign.VerifyRequest` doc comment ("A tolerance <= 0 disables
the freshness check … pass a positive tolerance in production"); `stdwebhook`
behaves identically. Changing `0` to mean "reject" would silently break existing
callers and is itself a footgun. The library's own guidance (README/CONSUMING)
shows a positive `5*time.Minute` window. **Residual risk:** a subscriber that passes
`0` disables its own replay window (its signature is still verified); this is the
subscriber's choice. No code change.

---

### 13. DynamoDB delivery item growth vs the 400 KB item cap — **Info**

**Location:** `adapters/dynamo/log.go:78-99` (attempts appended via `list_append`; event stored once).

**Description.** Each attempt is appended to a list attribute on a single item that also stores the full event payload. Attempts are bounded by `MaxAttempts` (default 5) in normal operation, but repeated `Replay`/`Recover` can append more, and a large payload plus attempts could approach DynamoDB's 400 KB item limit, at which point `UpdateItem` fails. Not a security issue per se; a durability/availability edge.

**Recommendation.** Cap stored attempts (keep last N), and/or store the event snapshot and attempts as separate items. Combine with Finding 9's payload cap.

**Disposition (ACCEPTED — durability edge, partially mitigated).** This is a
non-security durability edge (an oversized item surfaces as a clean `UpdateItem`
error, not a leak or corruption). Attempts are bounded by `RetryPolicy.MaxAttempts`
(default 5) in normal operation, and the payload cap added in Finding 9
(`Deps.MaxPayloadBytes`) lets an integrator keep items comfortably under the 400 KB
limit. Splitting the snapshot and attempts into separate items is a schema change
best done when a concrete high-volume/large-payload workload requires it, rather
than pre-emptively complicating the single-item model. **Residual risk:** a very
large payload combined with many `Replay`/`Recover`-driven attempts could approach
400 KB and fail the write for that one delivery; set `MaxPayloadBytes` to avoid it.
No code change beyond Finding 9.

---

## What's done well

- **Tenancy is a hard partition, enforced twice.** Stores key every read by `ORG#<orgID>` (`adapters/dynamo/dynamo.go:39-47`, `adapters/memory/memory.go`), and the core re-checks `sub.OrgID == ev.OrgID` / `event.OrgID == subscription.OrgID` on the fan-out (`events/service.go:160-163`), the dispatch (`:236-238`), rehydration (`:423-425`), replay (`:575-577`), `GetDelivery`/`GetSubscription` (`:382-386`, `:395-397`), and even the auto-disable breaker (`adapters/autodisable/autodisable.go:82`). A store bug alone cannot leak across orgs.
- **SSRF guard pins the vetted IP at dial time (DNS-rebind safe) and refuses redirects.** `guardedDialContext` resolves, checks every resolved IP, then dials the checked IP directly (`httpdeliver.go:180-192`); `CheckRedirect` returns `ErrUseLastResponse` so a `3xx` to an internal host is never followed (`:83-87`). AWS metadata (`169.254.169.254`) and ULA IPv6 are blocked. The guard is independent of Subscribe-time validation, so a later DNS change cannot bypass it.
- **Secrets are handled carefully end-to-end.** `Subscription.Secret` is `json:"-"` (`events/event.go:31`); the durable queue serializes only a secret-free `JobRef` (`ports.go:36-48`, `sqsqueue.go:109-121`); the admin API returns the secret only at create/rotate and never in list/detail (`adminhttp.go:106-109`, `:162`); at-rest encryption is available via `securestore` + `aesgcm`/`kmscodec` (AES-256-GCM authenticated, random nonce; KMS pins `KeyId` on decrypt). A repo-wide grep found no path that logs or prints a secret. Response snippets captured on failure come from the subscriber's body (bounded to 512 B in `httpdeliver`, 256 B in `failureReason`) and cannot echo a secret the subscriber never receives.
- **Idempotency and at-least-once safety.** Deterministic `DeliveryID = "dlv_" + sha256(subID \x00 eventID)` (`event.go:124-130`), with a NUL separator to avoid concatenation ambiguity; `ProcessRef` drops references to already-terminal or missing deliveries (`service.go:439-451`), and the SQS adapter drops unparseable messages so a poison message can't stall the queue forever (`sqsqueue.go:184-190`). Constant-time signature comparison in both signers (`hmac.Equal`, `subtle.ConstantTimeCompare`).
- **Retry backoff is capped under the default policy** (`MaxDelay` 5 m), so a hostile `Retry-After` cannot park a job (`service.go:306-312`, `retry.go:26-35`); non-retryable `4xx` dead-letter immediately rather than hammering.
- **Admin authz takes the org from an injected resolver, never from the client** (`adminhttp.go:1-14`, `:262-269`), request bodies are size-limited (`:272`), and error responses are generic (no internal detail leak, `:286-297`).
- **Strong identifiers.** Event/subscription/delivery ids are 128-bit `crypto/rand` (`adapters/std/std.go:30-38`), so ids are unguessable — IDOR would require both a guessed id and a cross-org match the core rejects.
- **Minimal, first-party dependency surface** — only `aws-sdk-go-v2` modules (`go.mod`), no third-party crypto or HTTP libraries.

---

## Residual risk & assumptions

- **Deployment assumptions the library depends on:** production uses `httpdeliver.NewSecure()` (guard on) and the durable SQS queue; the admin surface is mounted behind real authentication and (for browser/cookie use) CSRF protection; secrets are stored via `securestore` with KMS/aesgcm; DynamoDB tables have SSE enabled. Weakening any of these (e.g. `httpdeliver.New{GuardPrivate:false}`, mounting `/ui` under cookie auth without CSRF, storing secrets unencrypted) removes a control the audit assumed present.
- **The SSRF guard depends on `net.IP` classification and the host resolver.** A split-horizon or attacker-influenced resolver that returns a "public" IP for an internal name is mitigated by IP-pinning, but the guard is only as good as its block set (Finding 2). It does not, and cannot, protect against a subscriber host that is legitimately public but internal to the operator's own network unless those CIDRs are added.
- **Replay protection ultimately lives on the subscriber side.** Even with Finding 1 fixed, subscribers must actually verify the signature, enforce the timestamp window, and dedup on the delivery id. The library provides the primitives; it cannot force correct subscriber behavior (Finding 12).
- **Not covered by this review:** the AWS SDK's own security, IAM policy scoping of the DynamoDB/SQS/KMS clients the integrator supplies, TLS configuration of the outbound `http.Transport` (uses Go defaults — modern and safe, but cert pinning is out of scope), and any code outside this library. No dynamic/fuzz testing was performed; findings are from static review of the source at the audited commit.
- **`orgID` is trusted input.** Partition keys are built as `"ORG#" + orgID` with no escaping (`dynamo.go:39-41`); this is safe because `orgID` originates from the server-side auth resolver, not from clients. If an integrator ever sources `orgID` from client input, key-prefix collisions become a consideration.
