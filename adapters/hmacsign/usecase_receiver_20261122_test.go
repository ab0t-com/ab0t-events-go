package hmacsign_test

// Use-case scenario tests, 2026-11-22: each test is one job a service or its customer does with the library.
//
// Job: "a customer verifies our X-Signature in THEIR stack (Node, Python, Rails, ...)". They cannot import this
// package; they implement the documented canonical string themselves. The package's own round-trip tests cannot
// catch a change to that string, because Sign and Verify would drift together and still agree with each other.
// These tests recompute the signature from the documentation only, pinning the published wire format.

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/ab0t-com/ab0t-events-go/adapters/hmacsign"
	"github.com/ab0t-com/ab0t-events-go/events"
)

// docSignature is what a customer writes from README/CONSUMING: HMAC-SHA256 keyed with the secret's bytes over
// "{deliveryId}.{timestamp}.{body}", hex-encoded, prefixed "sha256=".
func docSignature(secret, deliveryID, ts string, body []byte) string {
	m := hmac.New(sha256.New, []byte(secret))
	m.Write([]byte(deliveryID + "." + ts + "."))
	m.Write(body)
	return "sha256=" + hex.EncodeToString(m.Sum(nil))
}

func TestUseCase_HMAC_IndependentReceiverRecomputesTheDocumentedSignature(t *testing.T) {
	const secret = "test-placeholder-signing-secret"
	at := time.Unix(1_763_800_000, 0)
	body := []byte(`{"orderId":"o_77","total":"19.99"}`)
	in := events.SigningInput{
		EventID: "evt_77", DeliveryID: "dlv_0123456789abcdef0123456789abcdef", EventType: "order.created",
		Timestamp: at, Body: body, Secret: secret,
	}

	headers := hmacsign.New().Sign(in)
	ts := strconv.FormatInt(at.Unix(), 10)
	if got := headers[events.HeaderTimestamp]; got != ts {
		t.Fatalf("%s = %q, want unix seconds %q", events.HeaderTimestamp, got, ts)
	}
	want := docSignature(secret, in.DeliveryID, ts, body)
	if got := headers[events.HeaderSignature]; got != want {
		t.Fatalf("%s = %q, but a receiver following the docs computes %q — the published canonical string changed", events.HeaderSignature, got, want)
	}
	if len(headers) != 2 {
		t.Fatalf("signer returned %d headers %v, want exactly X-Signature and X-Event-Timestamp", len(headers), headers)
	}
	for k, v := range headers {
		if strings.Contains(v, secret) {
			t.Fatalf("header %s echoes the secret", k)
		}
	}
	// The shipped verifier agrees with the hand-computed value (receiver written in Go, same contract).
	if !hmacsign.Verify(secret, in.DeliveryID, ts, body, want) {
		t.Fatal("hmacsign.Verify rejected the documented signature")
	}
	if err := hmacsign.VerifyRequest(secret, body, in.DeliveryID, want, ts, at.Add(2*time.Minute), 5*time.Minute); err != nil {
		t.Fatalf("VerifyRequest rejected the documented signature 2 minutes after signing: %v", err)
	}
}

// TestUseCase_HMAC_VerifyRequestErrorClassesAReceiverBranchesOn: a receiver logs/alerts differently on "someone is
// forging requests" (bad signature) vs "clock skew / replay" (stale) vs "malformed". Each case is a correctly-built
// request except for the one thing being tested.
func TestUseCase_HMAC_VerifyRequestErrorClassesAReceiverBranchesOn(t *testing.T) {
	const secret = "test-placeholder-signing-secret"
	const id = "dlv_feedfacefeedfacefeedfacefeedface"
	body := []byte(`{"userId":"u_1"}`)
	now := time.Unix(1_763_800_000, 0)
	tsAt := func(d time.Duration) string { return strconv.FormatInt(now.Add(d).Unix(), 10) }

	cases := []struct {
		name    string
		secret  string
		body    []byte
		ts      string
		sig     string // empty: compute the correct signature over (id, ts, body) with the real secret
		wantErr error
	}{
		{name: "fresh and genuine", secret: secret, body: body, ts: tsAt(-30 * time.Second)},
		{name: "4m59s old is inside a 5m window", secret: secret, body: body, ts: tsAt(-(4*time.Minute + 59*time.Second))},
		{name: "10 minutes old (replay or a stuck queue)", secret: secret, body: body, ts: tsAt(-10 * time.Minute), wantErr: hmacsign.ErrStaleTimestamp},
		{name: "10 minutes in the future (forward-dated / skewed sender)", secret: secret, body: body, ts: tsAt(10 * time.Minute), wantErr: hmacsign.ErrStaleTimestamp},
		{name: "receiver configured with the wrong secret", secret: "some-other-placeholder", body: body, ts: tsAt(0), wantErr: hmacsign.ErrBadSignature},
		{name: "body modified in transit", secret: secret, body: []byte(`{"userId":"u_2"}`), ts: tsAt(0), wantErr: hmacsign.ErrBadSignature},
		{name: "signature header missing", secret: secret, body: body, ts: tsAt(0), sig: "-", wantErr: hmacsign.ErrBadSignature},
		{name: "non-numeric timestamp (signed as sent)", secret: secret, body: body, ts: "yesterday", wantErr: hmacsign.ErrBadTimestamp},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			sig := tc.sig
			switch sig {
			case "":
				sig = docSignature(secret, id, tc.ts, body) // what the sender produced (over the ORIGINAL body)
			case "-":
				sig = ""
			}
			err := hmacsign.VerifyRequest(tc.secret, tc.body, id, sig, tc.ts, now, 5*time.Minute)
			if tc.wantErr == nil {
				if err != nil {
					t.Fatalf("want accepted, got %v", err)
				}
				return
			}
			if !errors.Is(err, tc.wantErr) {
				t.Fatalf("want %v, got %v", tc.wantErr, err)
			}
		})
	}
}
