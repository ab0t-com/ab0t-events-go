// Package hmacsign is the HMAC-SHA256 Signer adapter for ab0t-events-go. It signs
// the canonical string "{deliveryID}.{timestamp}.{body}" with a per-subscription
// secret and ships verification helpers a subscriber uses to authenticate
// incoming webhooks.
//
// Binding the delivery id and timestamp into the MAC (not just the body) is what
// makes the freshness/anti-replay window and the idempotency delivery-id real: an
// attacker who captures one validly-signed request cannot rewrite
// X-Event-Timestamp or X-Delivery-Id without invalidating the signature. (An
// earlier version signed the body only; that did not authenticate those headers.)
package hmacsign

import (
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"strconv"
	"strings"
	"time"

	"github.com/ab0t-com/ab0t-events-go/events"
)

// Prefix is the algorithm tag on the signature header value.
const Prefix = "sha256="

// SecretPrefix tags generated signing secrets so they are recognizable in logs
// and UIs (and greppable if one ever leaks). Matches the Stripe "whsec_" style.
const SecretPrefix = "whsec_"

// GenerateSecret returns a new cryptographically-random signing secret of the
// form "whsec_<hex>" (256 bits of entropy). Store it once, show it to the
// subscriber once, and hand it to Subscribe as the per-subscription Secret.
func GenerateSecret() (string, error) {
	var b [32]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	return SecretPrefix + hex.EncodeToString(b[:]), nil
}

// Signer implements events.Signer using HMAC-SHA256 over the canonical string
// "{deliveryID}.{timestamp}.{body}". It emits an X-Signature header
// ("sha256=<hex>") and an X-Event-Timestamp header (unix seconds); both the
// timestamp and the delivery id are authenticated by the signature.
type Signer struct{}

// New returns an HMAC-SHA256 signer.
func New() Signer { return Signer{} }

var _ events.Signer = Signer{}

// Sign returns the X-Signature and X-Event-Timestamp headers for a delivery. The
// signature covers the delivery id and timestamp in addition to the body, so a
// subscriber that verifies it can trust both.
func (Signer) Sign(in events.SigningInput) map[string]string {
	ts := strconv.FormatInt(in.Timestamp.Unix(), 10)
	return map[string]string{
		events.HeaderSignature: Signature(in.Secret, in.DeliveryID, ts, in.Body),
		events.HeaderTimestamp: ts,
	}
}

// Signature returns "sha256=<hex>" where hex is HMAC-SHA256 over the canonical
// string "{deliveryID}.{timestamp}.{body}". The timestamp is unix seconds as a
// string (matching the X-Event-Timestamp header). Exported so subscribers and
// tests can recompute it.
func Signature(secret, deliveryID, timestamp string, body []byte) string {
	return Prefix + hex.EncodeToString(mac(secret, deliveryID, timestamp, body))
}

func mac(secret, deliveryID, timestamp string, body []byte) []byte {
	h := hmac.New(sha256.New, []byte(secret))
	h.Write([]byte(deliveryID))
	h.Write([]byte("."))
	h.Write([]byte(timestamp))
	h.Write([]byte("."))
	h.Write(body)
	return h.Sum(nil)
}

// Verify reports whether header is a valid "sha256=<hex>" signature of the
// canonical "{deliveryID}.{timestamp}.{body}" string under secret. Comparison is
// constant-time. Pass deliveryID from events.HeaderDelivery and timestamp from
// events.HeaderTimestamp.
func Verify(secret string, deliveryID, timestamp string, body []byte, header string) bool {
	want := mac(secret, deliveryID, timestamp, body)
	got, err := hex.DecodeString(strings.TrimPrefix(header, Prefix))
	if err != nil {
		return false
	}
	return hmac.Equal(want, got)
}

// Errors returned by VerifyRequest.
var (
	ErrBadSignature   = errors.New("hmacsign: signature mismatch")
	ErrStaleTimestamp = errors.New("hmacsign: timestamp outside tolerance")
	ErrBadTimestamp   = errors.New("hmacsign: malformed timestamp")
)

// VerifyRequest authenticates a received webhook: it checks the HMAC signature
// (which binds the delivery id and timestamp) AND that the X-Event-Timestamp
// (unix seconds) is within tolerance of now. Because the timestamp and delivery
// id are covered by the signature, an attacker cannot rewrite them to refresh a
// captured request — that defeats replay. Pass idHeader from
// events.HeaderDelivery, sigHeader from events.HeaderSignature, and tsHeader from
// events.HeaderTimestamp.
//
// A tolerance <= 0 disables the freshness check (the signature is still verified);
// pass a positive tolerance (e.g. 5 minutes) in production to get replay defense.
func VerifyRequest(secret string, rawBody []byte, idHeader, sigHeader, tsHeader string, now time.Time, tolerance time.Duration) error {
	if !Verify(secret, idHeader, tsHeader, rawBody, sigHeader) {
		return ErrBadSignature
	}
	secs, err := strconv.ParseInt(tsHeader, 10, 64)
	if err != nil {
		return ErrBadTimestamp
	}
	delta := now.Sub(time.Unix(secs, 0))
	if delta < 0 {
		delta = -delta
	}
	if tolerance > 0 && delta > tolerance {
		return ErrStaleTimestamp
	}
	return nil
}
