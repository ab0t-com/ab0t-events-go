package hmacsign

import (
	"strings"
	"testing"
	"time"

	"github.com/ab0t-com/ab0t-events-go/events"
)

func TestSignVerify_RoundTrip(t *testing.T) {
	secret := "whsec_test_secret"
	id := "dlv_abc"
	ts := "1700000000"
	body := []byte(`{"id":"e1","type":"drive.file.created","payload":{"n":7}}`)

	header := Signature(secret, id, ts, body)
	if !strings.HasPrefix(header, Prefix) {
		t.Fatalf("missing prefix: %q", header)
	}
	if !Verify(secret, id, ts, body, header) {
		t.Fatal("valid signature failed to verify")
	}
	// Tampered body must fail.
	if Verify(secret, id, ts, append(body, '!'), header) {
		t.Fatal("tampered body verified")
	}
	// Wrong secret must fail.
	if Verify("other", id, ts, body, header) {
		t.Fatal("wrong secret verified")
	}
	// Malformed header must fail, not panic.
	if Verify(secret, id, ts, body, "sha256=zzzz") {
		t.Fatal("malformed header verified")
	}
}

// TestSignatureBindsTimestampAndID is the proving test for the audit High
// finding: rewriting the delivery id or the timestamp — which are sent as
// headers — invalidates the signature, so a captured request cannot be replayed
// with a refreshed timestamp or a new id.
func TestSignatureBindsTimestampAndID(t *testing.T) {
	secret := "whsec_k"
	id := "dlv_orig"
	ts := "1700000000"
	body := []byte(`{"amount":100}`)
	header := Signature(secret, id, ts, body)

	// Same body+secret but a different (attacker-refreshed) timestamp must fail.
	if Verify(secret, id, "1700000900", body, header) {
		t.Fatal("signature verified under a rewritten timestamp — replay window is forgeable")
	}
	// Same body+secret but a different (attacker-minted) delivery id must fail.
	if Verify(secret, "dlv_forged", ts, body, header) {
		t.Fatal("signature verified under a rewritten delivery id — dedup is forgeable")
	}
	// The exact triple still verifies (sanity).
	if !Verify(secret, id, ts, body, header) {
		t.Fatal("authentic triple failed to verify")
	}
}

// TestSignerBindsHeaders proves the Signer (as wired into the core) produces a
// signature that only verifies against the delivery id + timestamp it emitted.
func TestSignerBindsHeaders(t *testing.T) {
	in := events.SigningInput{
		DeliveryID: "dlv_1",
		Timestamp:  time.Unix(1700000000, 0),
		Body:       []byte(`{"x":1}`),
		Secret:     "whsec_s",
	}
	h := New().Sign(in)
	sig := h[events.HeaderSignature]
	ts := h[events.HeaderTimestamp]
	if ts != "1700000000" {
		t.Fatalf("unexpected timestamp header: %q", ts)
	}
	if !Verify(in.Secret, in.DeliveryID, ts, in.Body, sig) {
		t.Fatal("Signer output failed to verify against its own id+timestamp")
	}
	// A forged timestamp header (with the emitted signature) must not verify.
	if Verify(in.Secret, in.DeliveryID, "1700009999", in.Body, sig) {
		t.Fatal("forged timestamp verified against emitted signature")
	}
}

func TestGenerateSecret(t *testing.T) {
	s1, err := GenerateSecret()
	if err != nil {
		t.Fatalf("generate: %v", err)
	}
	s2, _ := GenerateSecret()
	if s1 == s2 {
		t.Fatal("generated secrets are not unique")
	}
	if !strings.HasPrefix(s1, SecretPrefix) {
		t.Fatalf("missing prefix: %q", s1)
	}
	// whsec_ + 64 hex chars (32 bytes).
	if len(s1) != len(SecretPrefix)+64 {
		t.Fatalf("unexpected length %d", len(s1))
	}
	// Round-trips as a real signing key.
	sig := Signature(s1, "dlv", "1", []byte("body"))
	if !Verify(s1, "dlv", "1", []byte("body"), sig) {
		t.Fatal("generated secret failed to sign/verify")
	}
}

func TestVerifyRequest_TimestampTolerance(t *testing.T) {
	secret := "k"
	id := "dlv_x"
	body := []byte("hello")
	now := time.Unix(1_700_000_000, 0)

	fresh := "1700000000"
	freshSig := Signature(secret, id, fresh, body)
	if err := VerifyRequest(secret, body, id, freshSig, fresh, now, 5*time.Minute); err != nil {
		t.Fatalf("fresh request rejected: %v", err)
	}
	// 10 minutes old, tolerance 5 minutes => stale (replay defense). The old
	// timestamp is bound into its own signature, so the signature is valid but the
	// freshness window rejects it.
	old := "1699999400"
	oldSig := Signature(secret, id, old, body)
	if err := VerifyRequest(secret, body, id, oldSig, old, now, 5*time.Minute); err != ErrStaleTimestamp {
		t.Fatalf("want ErrStaleTimestamp, got %v", err)
	}
	// Attacker keeps the old signature but rewrites the timestamp header to now:
	// the signature no longer matches → ErrBadSignature (the replay is defeated).
	if err := VerifyRequest(secret, body, id, oldSig, fresh, now, 5*time.Minute); err != ErrBadSignature {
		t.Fatalf("rewritten-timestamp replay should fail signature, got %v", err)
	}
	// Bad signature beats timestamp check.
	if err := VerifyRequest(secret, body, id, "sha256=00", fresh, now, 5*time.Minute); err != ErrBadSignature {
		t.Fatalf("want ErrBadSignature, got %v", err)
	}
	// Malformed timestamp (signed as such, so the signature matches) => ErrBadTimestamp.
	badTS := "not-a-number"
	badSig := Signature(secret, id, badTS, body)
	if err := VerifyRequest(secret, body, id, badSig, badTS, now, time.Minute); err != ErrBadTimestamp {
		t.Fatalf("want ErrBadTimestamp, got %v", err)
	}
}
