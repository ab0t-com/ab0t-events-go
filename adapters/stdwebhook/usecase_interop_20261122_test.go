package stdwebhook_test

// Use-case scenario tests, 2026-11-22: each test is one job a service or its customer does with the library.
//
// Job: "a customer verifies with an off-the-shelf Standard Webhooks library (svix, standardwebhooks-*)". Those
// libraries implement the spec, not this package, so the signer is checked against a verifier written from the spec
// text (https://www.standardwebhooks.com): key = base64-decode(secret without "whsec_"), signature =
// "v1," + base64(HMAC-SHA256(key, "{msg_id}.{timestamp}.{payload}")), several space-separated tokens allowed.

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/ab0t-com/ab0t-events-go/adapters/stdwebhook"
	"github.com/ab0t-com/ab0t-events-go/events"
)

// specSign is a third-party Standard Webhooks signer/verifier core, written from the spec, not from this package.
func specSign(t *testing.T, whsec, msgID, ts string, body []byte) string {
	t.Helper()
	key, err := base64.StdEncoding.DecodeString(strings.TrimPrefix(whsec, "whsec_"))
	if err != nil {
		t.Fatalf("secret %q is not whsec_<base64> as the spec requires: %v", "whsec_…", err)
	}
	m := hmac.New(sha256.New, key)
	m.Write([]byte(msgID + "." + ts + "."))
	m.Write(body)
	return "v1," + base64.StdEncoding.EncodeToString(m.Sum(nil))
}

func TestUseCase_StdWebhook_ThirdPartyVerifierAgreesWithTheSigner(t *testing.T) {
	secret, err := stdwebhook.GenerateSecret()
	if err != nil {
		t.Fatalf("GenerateSecret: %v", err)
	}
	at := time.Unix(1_763_800_123, 0)
	body := []byte(`{"type":"user.created","data":{"id":"u_1"}}`)
	in := events.SigningInput{EventID: "evt_1", DeliveryID: "dlv_aaaabbbbccccddddeeeeffff00001111", EventType: "user.created", Timestamp: at, Body: body, Secret: secret}

	h := stdwebhook.New().Sign(in)
	ts := strconv.FormatInt(at.Unix(), 10)
	if h[stdwebhook.HeaderID] != in.DeliveryID {
		t.Fatalf("webhook-id = %q, want the delivery id %q (the receiver's dedup key must be stable per subscription+event)", h[stdwebhook.HeaderID], in.DeliveryID)
	}
	if h[stdwebhook.HeaderTimestamp] != ts {
		t.Fatalf("webhook-timestamp = %q, want %q", h[stdwebhook.HeaderTimestamp], ts)
	}
	if want := specSign(t, secret, in.DeliveryID, ts, body); h[stdwebhook.HeaderSignature] != want {
		t.Fatalf("webhook-signature = %q, a spec verifier computes %q", h[stdwebhook.HeaderSignature], want)
	}
	if len(h) != 3 {
		t.Fatalf("signer returned %d headers, want exactly webhook-id/-timestamp/-signature: %v", len(h), h)
	}
	keyMaterial := strings.TrimPrefix(secret, "whsec_")
	for k, v := range h {
		if strings.Contains(v, keyMaterial) {
			t.Fatalf("header %s echoes the secret", k)
		}
	}
}

// TestUseCase_StdWebhook_RetriesKeepTheIdempotencyKeyButAreReSigned: attempt 1 fails, attempt 2 goes out 30s later.
// The receiver must see the same webhook-id (dedup) with a fresh timestamp and signature, and a captured signature
// cannot be spliced onto the newer timestamp.
func TestUseCase_StdWebhook_RetriesKeepTheIdempotencyKeyButAreReSigned(t *testing.T) {
	secret, err := stdwebhook.GenerateSecret()
	if err != nil {
		t.Fatalf("GenerateSecret: %v", err)
	}
	t1 := time.Unix(1_763_800_000, 0)
	t2 := t1.Add(30 * time.Second)
	body := []byte(`{"invoice":"in_5"}`)
	base := events.SigningInput{EventID: "evt_5", DeliveryID: "dlv_5555555555555555aaaaaaaaaaaaaaaa", EventType: "invoice.paid", Body: body, Secret: secret}
	in1, in2 := base, base
	in1.Timestamp, in2.Timestamp = t1, t2
	h1, h2 := stdwebhook.New().Sign(in1), stdwebhook.New().Sign(in2)

	if h1[stdwebhook.HeaderID] != h2[stdwebhook.HeaderID] {
		t.Fatal("webhook-id changed between attempts; receivers would process the retry twice")
	}
	if h1[stdwebhook.HeaderSignature] == h2[stdwebhook.HeaderSignature] || h1[stdwebhook.HeaderTimestamp] == h2[stdwebhook.HeaderTimestamp] {
		t.Fatal("a retry must carry its own timestamp and signature")
	}
	for i, h := range []map[string]string{h1, h2} {
		at := []time.Time{t1, t2}[i]
		if err := stdwebhook.VerifyRequest(secret, body, h[stdwebhook.HeaderID], h[stdwebhook.HeaderTimestamp], h[stdwebhook.HeaderSignature], at, 5*time.Minute); err != nil {
			t.Fatalf("attempt %d rejected at its own send time: %v", i+1, err)
		}
	}
	err = stdwebhook.VerifyRequest(secret, body, h1[stdwebhook.HeaderID], h2[stdwebhook.HeaderTimestamp], h1[stdwebhook.HeaderSignature], t2, 5*time.Minute)
	if !errors.Is(err, stdwebhook.ErrBadSignature) {
		t.Fatalf("attempt-1 signature spliced onto attempt-2 timestamp: want ErrBadSignature, got %v", err)
	}
}

// TestUseCase_StdWebhook_ReceiverAcceptsAnyListedV1SignatureDuringRotation: a sender that dual-signs during a secret
// rotation (current first, then previous) sends two v1 tokens. A receiver holding
// either secret accepts; unknown versions and malformed tokens are ignored rather than trusted.
func TestUseCase_StdWebhook_ReceiverAcceptsAnyListedV1SignatureDuringRotation(t *testing.T) {
	oldSecret, err1 := stdwebhook.GenerateSecret()
	newSecret, err2 := stdwebhook.GenerateSecret()
	strangerSecret, err3 := stdwebhook.GenerateSecret()
	if err1 != nil || err2 != nil || err3 != nil {
		t.Fatal("GenerateSecret failed")
	}
	const id = "dlv_rotation00000000000000000000000"
	ts := strconv.FormatInt(time.Unix(1_763_800_000, 0).Unix(), 10)
	body := []byte(`{"n":1}`)
	sigNew, sigOld := specSign(t, newSecret, id, ts, body), specSign(t, oldSecret, id, ts, body)
	dual := sigNew + " " + sigOld

	if !stdwebhook.Verify(newSecret, id, ts, body, dual) {
		t.Fatal("receiver that already installed the NEW secret rejected a dual-signed delivery")
	}
	if !stdwebhook.Verify(oldSecret, id, ts, body, dual) {
		t.Fatal("receiver still on the OLD secret rejected a dual-signed delivery")
	}
	if stdwebhook.Verify(strangerSecret, id, ts, body, dual) {
		t.Fatal("a third secret verified a delivery it never signed")
	}
	raw := strings.TrimPrefix(sigNew, "v1,")
	for _, hdr := range []string{"v2," + raw, "v1a," + raw, raw, "v1" + raw, ""} {
		if stdwebhook.Verify(newSecret, id, ts, body, hdr) {
			t.Fatalf("token %q must not verify (unknown version / malformed)", hdr)
		}
	}
}
