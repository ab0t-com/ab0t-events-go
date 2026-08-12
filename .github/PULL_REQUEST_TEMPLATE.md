<!-- Thanks for contributing! Keep PRs to one logical change. -->

## What & why

<!-- What does this change and why? Link the issue/ticket. -->

## Checklist

- [ ] `make check` passes (gofmt + vet + build + race)
- [ ] Tests added/updated for the change
- [ ] Exported identifiers documented; `docs/INTERFACE.md` updated if the public surface changed
- [ ] `CHANGELOG.md` updated under `[Unreleased]`
- [ ] No new dependencies beyond stdlib + `aws-sdk-go-v2` (or explained below)

## Tenancy & security impact

<!-- REQUIRED if this touches delivery, stores, signing, URLs, or secrets.
     Confirm: no cross-org data path; secrets not logged; SSRF guard intact. -->
