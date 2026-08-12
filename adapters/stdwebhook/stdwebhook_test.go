package stdwebhook

import (
	"strings"
	"testing"
	"time"

	"github.com/ab0t-com/ab0t-events-go/events"
)

func TestSignVerify_RoundTrip(t *testing.T) {
	secret, err := GenerateSecret()
	if err != nil {
		t.Fatalf("gen: %v", err)
	}
	if !strings.HasPrefix(secret, "whsec_") {
		t.Fatalf("bad secret: %q", secret)
	}
	ts := time.Unix(1_700_000_000, 0)
	in := events.SigningInput{DeliveryID: "dlv_1", Body: []byte(`{"n":7}`), Timestamp: ts, Secret: secret}

	h := New().Sign(in)
	if h[HeaderID] != "dlv_1" || h[HeaderTimestamp] != "1700000000" {
		t.Fatalf("headers: %+v", h)
	}
	if !strings.HasPrefix(h[HeaderSignature], "v1,") {
		t.Fatalf("signature format: %q", h[HeaderSignature])
	}

	// Verify the emitted headers round-trip.
	if err := VerifyRequest(secret, in.Body, h[HeaderID], h[HeaderTimestamp], h[HeaderSignature], ts, time.Minute); err != nil {
		t.Fatalf("verify: %v", err)
	}
	// Tampered body fails.
	if Verify(secret, "dlv_1", "1700000000", []byte(`{"n":8}`), h[HeaderSignature]) {
		t.Fatal("tampered body verified")
	}
	// Different id fails (signature binds the id).
	if Verify(secret, "dlv_2", "1700000000", in.Body, h[HeaderSignature]) {
		t.Fatal("wrong id verified")
	}
	// Multiple space-separated tokens: a matching v1 among others succeeds.
	multi := "v2,other " + h[HeaderSignature]
	if !Verify(secret, "dlv_1", "1700000000", in.Body, multi) {
		t.Fatal("multi-token header failed")
	}
}

func TestVerifyRequest_TimestampAndErrors(t *testing.T) {
	secret, _ := GenerateSecret()
	ts := time.Unix(1_700_000_000, 0)
	h := New().Sign(events.SigningInput{DeliveryID: "d", Body: []byte("x"), Timestamp: ts, Secret: secret})

	// Stale timestamp (11 min old, tolerance 5 min).
	if err := VerifyRequest(secret, []byte("x"), h[HeaderID], "1699999400", h[HeaderSignature], ts, 5*time.Minute); err != ErrStaleTimestamp {
		t.Fatalf("want ErrStaleTimestamp, got %v", err)
	}
	if err := VerifyRequest(secret, []byte("x"), h[HeaderID], "nan", h[HeaderSignature], ts, time.Minute); err != ErrBadTimestamp {
		t.Fatalf("want ErrBadTimestamp, got %v", err)
	}
	if err := VerifyRequest(secret, []byte("x"), h[HeaderID], h[HeaderTimestamp], "v1,deadbeef", ts, time.Minute); err != ErrBadSignature {
		t.Fatalf("want ErrBadSignature, got %v", err)
	}
}
