package kmscodec_test

import (
	"context"
	"encoding/base64"
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/kms"

	"github.com/ab0t-com/ab0t-events-go/adapters/kmscodec"
	"github.com/ab0t-com/ab0t-events-go/adapters/memory"
	"github.com/ab0t-com/ab0t-events-go/adapters/securestore"
	"github.com/ab0t-com/ab0t-events-go/events"
)

// fakeKMS is an offline, in-memory stand-in for the AWS KMS client. It
// "encrypts" by prefixing the key id and base64-ing the plaintext together with
// the encryption context, so Decrypt can verify the key id and context match —
// mirroring real KMS semantics without a network.
type fakeKMS struct {
	encCalls, decCalls int
	lastEncKeyID       string
}

func (f *fakeKMS) Encrypt(_ context.Context, in *kms.EncryptInput, _ ...func(*kms.Options)) (*kms.EncryptOutput, error) {
	f.encCalls++
	f.lastEncKeyID = aws.ToString(in.KeyId)
	// blob = keyID "|" ctxKey=ctxVal... "|" plaintext, all raw bytes.
	blob := aws.ToString(in.KeyId) + "|" + encodeCtx(in.EncryptionContext) + "|" + string(in.Plaintext)
	return &kms.EncryptOutput{CiphertextBlob: []byte(blob), KeyId: in.KeyId}, nil
}

func (f *fakeKMS) Decrypt(_ context.Context, in *kms.DecryptInput, _ ...func(*kms.Options)) (*kms.DecryptOutput, error) {
	f.decCalls++
	parts := strings.SplitN(string(in.CiphertextBlob), "|", 3)
	if len(parts) != 3 {
		return nil, errors.New("kms: malformed blob")
	}
	keyID, ctxStr, plain := parts[0], parts[1], parts[2]
	// KMS requires the same key id (when supplied) and encryption context.
	if kid := aws.ToString(in.KeyId); kid != "" && kid != keyID {
		return nil, errors.New("kms: key id mismatch")
	}
	if ctxStr != encodeCtx(in.EncryptionContext) {
		return nil, errors.New("kms: encryption context mismatch")
	}
	return &kms.DecryptOutput{Plaintext: []byte(plain), KeyId: aws.String(keyID)}, nil
}

func encodeCtx(m map[string]string) string {
	if len(m) == 0 {
		return ""
	}
	// deterministic single-key encoding is enough for the tests here
	var b strings.Builder
	for k, v := range m {
		b.WriteString(k + "=" + v + ";")
	}
	return b.String()
}

func TestRoundTrip(t *testing.T) {
	fk := &fakeKMS{}
	c := kmscodec.New(kmscodec.Config{Client: fk, KeyID: "alias/webhooks"})
	ctx := context.Background()

	secret := "whsec_" + strings.Repeat("a", 64)
	ct, err := c.Encrypt(ctx, secret)
	if err != nil {
		t.Fatal(err)
	}
	// Stored value is base64 and does not contain the plaintext.
	if _, err := base64.StdEncoding.DecodeString(ct); err != nil {
		t.Fatalf("ciphertext should be base64: %v", err)
	}
	if strings.Contains(ct, secret) {
		t.Fatal("ciphertext must not contain the plaintext")
	}
	got, err := c.Decrypt(ctx, ct)
	if err != nil || got != secret {
		t.Fatalf("round-trip mismatch: %q err=%v", got, err)
	}
	if fk.encCalls != 1 || fk.decCalls != 1 || fk.lastEncKeyID != "alias/webhooks" {
		t.Fatalf("unexpected KMS calls: enc=%d dec=%d key=%q", fk.encCalls, fk.decCalls, fk.lastEncKeyID)
	}
}

func TestEncryptionContextMustMatch(t *testing.T) {
	fk := &fakeKMS{}
	encCtx := map[string]string{"use": "webhook-secret"}
	c := kmscodec.New(kmscodec.Config{Client: fk, KeyID: "k1", EncryptionContext: encCtx})
	ctx := context.Background()

	ct, _ := c.Encrypt(ctx, "sek")
	if got, err := c.Decrypt(ctx, ct); err != nil || got != "sek" {
		t.Fatalf("matching context should decrypt: %q %v", got, err)
	}

	// A codec with a different context cannot decrypt it.
	other := kmscodec.New(kmscodec.Config{Client: fk, KeyID: "k1", EncryptionContext: map[string]string{"use": "other"}})
	if _, err := other.Decrypt(ctx, ct); err == nil {
		t.Fatal("mismatched encryption context must fail Decrypt")
	}
	if !reflect.DeepEqual(encCtx, map[string]string{"use": "webhook-secret"}) {
		t.Fatal("codec must not mutate the caller's EncryptionContext")
	}
}

func TestMalformedBase64(t *testing.T) {
	c := kmscodec.New(kmscodec.Config{Client: &fakeKMS{}, KeyID: "k"})
	if _, err := c.Decrypt(context.Background(), "not base64 %%%"); err == nil {
		t.Fatal("non-base64 ciphertext should error")
	}
}

func TestNilConfigPanics(t *testing.T) {
	assertPanic(t, func() { kmscodec.New(kmscodec.Config{KeyID: "k"}) })         // no client
	assertPanic(t, func() { kmscodec.New(kmscodec.Config{Client: &fakeKMS{}}) }) // no key id
}

// TestWithSecureStore proves the KMS codec drops into the securestore decorator:
// the wrapped store holds KMS ciphertext, reads return plaintext.
func TestWithSecureStore(t *testing.T) {
	ctx := context.Background()
	fk := &fakeKMS{}
	codec := kmscodec.New(kmscodec.Config{Client: fk, KeyID: "k1"})
	inner := memory.NewStore()
	store := securestore.New(inner, codec)

	_ = store.Put(ctx, events.Subscription{ID: "s1", OrgID: "o", URL: "u", Secret: "whsec_top", EventTypes: []string{"*"}, Active: true})
	raw, _ := inner.Get(ctx, "o", "s1")
	if strings.Contains(raw.Secret, "whsec_top") {
		t.Fatalf("at-rest value leaks the secret: %q", raw.Secret)
	}
	got, _ := store.Get(ctx, "o", "s1")
	if got.Secret != "whsec_top" {
		t.Fatalf("decorator should return plaintext, got %q", got.Secret)
	}
}

func assertPanic(t *testing.T, fn func()) {
	t.Helper()
	defer func() {
		if recover() == nil {
			t.Fatal("expected panic")
		}
	}()
	fn()
}
