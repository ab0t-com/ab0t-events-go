# Security Policy

`ab0t-events-go` sends HTTP requests to URLs supplied by tenants and handles
per-subscription signing secrets, so it sits on a security boundary. Please treat
findings here seriously.

## Reporting a vulnerability

Report privately to **security@ab0t.com**. Do not open a public issue for
undisclosed vulnerabilities. Include a description, affected version, and a
reproduction if possible. We aim to acknowledge within 3 business days.

## Security model & guarantees

- **Tenant isolation.** Subscriptions and deliveries are hard-partitioned by
  `OrgID`. The store is the boundary and the dispatcher re-asserts org equality
  on every job. One org's event is never delivered to another org's endpoint.
- **Secret handling.** Signing secrets are per-subscription, `json:"-"`, and are
  never written to the delivery log, error strings, or captured response
  snippets. Generate them with `hmacsign.GenerateSecret()` (256-bit) and store
  them encrypted at rest (KMS/SSE on DynamoDB).
- **Message authenticity.** Every delivery is signed with HMAC-SHA256 over the
  exact raw body (`X-Signature`). Subscribers verify with `hmacsign.Verify` /
  `VerifyRequest`; the latter also enforces an `X-Event-Timestamp` tolerance
  window to defeat replay of a captured, validly-signed request.

## SSRF — you MUST keep the guard on

A subscription URL is attacker-influenced input. Left unguarded, the deliverer is
an SSRF primitive that can reach internal services and cloud metadata endpoints
(`169.254.169.254`, `127.0.0.1`, RFC1918 ranges, …).

`httpdeliver.NewSecure()` (the default in the `webhooks` facade) mitigates this:

- rejects non-`http(s)` schemes;
- resolves the host and refuses loopback, link-local, private, unspecified, and
  multicast addresses;
- pins the connection to the vetted IP (defeats DNS-rebinding on retry);
- does not follow redirects (a 3xx could otherwise reach an internal host).

Do **not** disable `GuardPrivate` for internet-facing endpoints. Additionally,
validate URLs at subscribe time via `events.Deps.ValidateURL` and consider an
allowlist/denylist appropriate to your environment. The guard reduces, but does
not by itself eliminate, all SSRF risk (e.g. an attacker-controlled *public* host
that proxies inward) — layer network egress controls as well.

## Supported versions

The latest minor release receives security fixes. Pre-1.0 APIs may change.
