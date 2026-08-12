# Contributing to ab0t-events-go

Thanks for helping improve the outbound webhook library. It is used across many
services, so the bar for changes is deliberately high on two axes: **tenancy
safety** and **the port contracts**.

## Ground rules

1. **The core (`events/`) stays pure.** No `time.Now`, no `rand`, no network, no
   file/DB I/O, no logging of secrets. All non-determinism enters through the
   `Clock`, `IDGen`, and `Rand` ports. A test proves this by using only fakes.
2. **Tenancy is non-negotiable.** Every store/log read is org-scoped; the
   dispatcher re-asserts `event.OrgID == subscription.OrgID`. Any change that
   could let one org's event reach another org's endpoint is a blocker. Keep the
   `TestEmit_PerOrgIsolation` test meaningful.
3. **Secrets never get logged or serialized.** `Subscription.Secret` is
   `json:"-"`; response snippets and error strings must not carry secrets.
4. **Ports are contracts.** Changing a port interface is a breaking change —
   update every adapter, the fakes, the docs (`docs/INTERFACE.md`), and the
   CHANGELOG.

## Workflow

```sh
make check        # gofmt + vet + build + race — the pre-commit bar
make test         # unit + integration
make cover        # coverage.html
```

- Format with `gofmt` (CI enforces it).
- Add or update tests for any behaviour change. Core behaviour is tested with
  fakes; real-I/O behaviour with `httptest`.
- Keep the public surface documented; exported identifiers need doc comments.
- Update `CHANGELOG.md` under `[Unreleased]`.

## Adding an adapter

Implement the relevant port in a new package under `adapters/`, add a
compile-time assertion (`var _ events.Port = (*T)(nil)`), and a unit test that
exercises it without external infrastructure where possible (see
`adapters/dynamo` using a fake client). Do not add dependencies beyond the Go
stdlib and `aws-sdk-go-v2` without discussion.

## Commit / PR

- One logical change per PR; green CI required.
- Describe tenancy/security implications explicitly if any.
