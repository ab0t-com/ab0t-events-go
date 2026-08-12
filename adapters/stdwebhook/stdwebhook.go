// Package stdwebhook is a Signer adapter implementing the Standard Webhooks
// specification (https://www.standardwebhooks.com). It signs the canonical
// string "{id}.{timestamp}.{payload}" with HMAC-SHA256 and emits the
// webhook-id / webhook-timestamp / webhook-signature headers, so subscribers can
// verify with any off-the-shelf Standard Webhooks library in any language.
//
// Swap it in via events.Deps.Signer or webhooks.Options.Signer.
package stdwebhook

import (
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"errors"
	"strconv"
	"strings"
	"time"

	"github.com/ab0t-com/ab0t-events-go/events"
)

// Standard Webhooks header and format constants.
const (
	HeaderID        = "webhook-id"
	HeaderTimestamp = "webhook-timestamp"
	HeaderSignature = "webhook-signature"

	secretPrefix = "whsec_"
	sigVersion   = "v1"
)

// Signer implements events.Signer per the Standard Webhooks spec.
type Signer struct{}

// New returns a Standard Webhooks signer.
func New() Signer { return Signer{} }

var _ events.Signer = Signer{}

// Sign returns the webhook-id, webhook-timestamp and webhook-signature headers.
// The delivery id is used as the message id (webhook-id), which subscribers use
// for idempotent deduplication.
func (Signer) Sign(in events.SigningInput) map[string]string {
	ts := strconv.FormatInt(in.Timestamp.Unix(), 10)
	return map[string]string{
		HeaderID:        in.DeliveryID,
		HeaderTimestamp: ts,
		HeaderSignature: sigVersion + "," + computeB64(in.Secret, in.DeliveryID, ts, in.Body),
	}
}

// GenerateSecret returns a new Standard-Webhooks-style secret ("whsec_<base64>",
// 256-bit). The base64 body is the raw signing key.
func GenerateSecret() (string, error) {
	var b [32]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	return secretPrefix + base64.StdEncoding.EncodeToString(b[:]), nil
}

// key derives the raw HMAC key from a secret. A "whsec_<base64>" secret is
// base64-decoded; anything else is used as raw key bytes.
func key(secret string) []byte {
	s := strings.TrimPrefix(secret, secretPrefix)
	if b, err := base64.StdEncoding.DecodeString(s); err == nil {
		return b
	}
	return []byte(secret)
}

func computeB64(secret, id, ts string, body []byte) string {
	m := hmac.New(sha256.New, key(secret))
	m.Write([]byte(id))
	m.Write([]byte("."))
	m.Write([]byte(ts))
	m.Write([]byte("."))
	m.Write(body)
	return base64.StdEncoding.EncodeToString(m.Sum(nil))
}

// Verify reports whether sigHeader (which may hold multiple space-separated
// "vN,sig" tokens) contains a valid v1 signature for (id, ts, body) under secret.
func Verify(secret, id, ts string, body []byte, sigHeader string) bool {
	want := computeB64(secret, id, ts, body)
	for _, tok := range strings.Fields(sigHeader) {
		ver, sig, ok := strings.Cut(tok, ",")
		if !ok || ver != sigVersion {
			continue
		}
		if subtle.ConstantTimeCompare([]byte(sig), []byte(want)) == 1 {
			return true
		}
	}
	return false
}

// Errors from VerifyRequest.
var (
	ErrBadSignature   = errors.New("stdwebhook: signature mismatch")
	ErrStaleTimestamp = errors.New("stdwebhook: timestamp outside tolerance")
	ErrBadTimestamp   = errors.New("stdwebhook: malformed timestamp")
)

// VerifyRequest authenticates a received webhook: it checks the signature and
// that the timestamp is within tolerance of now (anti-replay). Pass the header
// values from HeaderID, HeaderTimestamp and HeaderSignature.
func VerifyRequest(secret string, body []byte, id, ts, sigHeader string, now time.Time, tolerance time.Duration) error {
	secs, err := strconv.ParseInt(ts, 10, 64)
	if err != nil {
		return ErrBadTimestamp
	}
	if tolerance > 0 {
		delta := now.Sub(time.Unix(secs, 0))
		if delta < 0 {
			delta = -delta
		}
		if delta > tolerance {
			return ErrStaleTimestamp
		}
	}
	if !Verify(secret, id, ts, body, sigHeader) {
		return ErrBadSignature
	}
	return nil
}
