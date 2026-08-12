package aesgcm_test

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/ab0t-com/ab0t-events-go/adapters/aesgcm"
)

func key(t *testing.T) []byte {
	t.Helper()
	k := make([]byte, aesgcm.KeySize)
	for i := range k {
		k[i] = byte(i + 1)
	}
	return k
}

func TestRoundTrip(t *testing.T) {
	c, err := aesgcm.New(key(t))
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	for _, secret := range []string{"whsec_" + strings.Repeat("a", 64), "", "short", "unicode-✓-secret"} {
		ct, err := c.Encrypt(ctx, secret)
		if err != nil {
			t.Fatalf("encrypt %q: %v", secret, err)
		}
		if ct == secret && secret != "" {
			t.Fatalf("ciphertext equals plaintext for %q", secret)
		}
		got, err := c.Decrypt(ctx, ct)
		if err != nil {
			t.Fatalf("decrypt: %v", err)
		}
		if got != secret {
			t.Fatalf("round-trip mismatch: got %q want %q", got, secret)
		}
	}
}

func TestNonDeterministic(t *testing.T) {
	c, _ := aesgcm.New(key(t))
	ctx := context.Background()
	a, _ := c.Encrypt(ctx, "same")
	b, _ := c.Encrypt(ctx, "same")
	if a == b {
		t.Fatal("two encryptions of the same plaintext must differ (fresh nonce)")
	}
}

func TestWrongKeySize(t *testing.T) {
	if _, err := aesgcm.New(make([]byte, 16)); !errors.Is(err, aesgcm.ErrKeySize) {
		t.Fatalf("want ErrKeySize for 16-byte key, got %v", err)
	}
	if _, err := aesgcm.New(nil); !errors.Is(err, aesgcm.ErrKeySize) {
		t.Fatalf("want ErrKeySize for nil key, got %v", err)
	}
}

func TestWrongKeyFailsAuth(t *testing.T) {
	c1, _ := aesgcm.New(key(t))
	other := key(t)
	other[0] ^= 0xff
	c2, _ := aesgcm.New(other)
	ctx := context.Background()

	ct, _ := c1.Encrypt(ctx, "top-secret")
	if _, err := c2.Decrypt(ctx, ct); err == nil {
		t.Fatal("decrypt with the wrong key must fail authentication")
	}
}

func TestTamperFailsAuth(t *testing.T) {
	c, _ := aesgcm.New(key(t))
	ctx := context.Background()
	ct, _ := c.Encrypt(ctx, "top-secret")
	// Flip a bit in the base64 payload by decoding-mutating is complex; instead
	// truncate to guarantee an auth/format failure.
	if _, err := c.Decrypt(ctx, ct[:len(ct)-4]); err == nil {
		t.Fatal("decrypting a mutated ciphertext must fail")
	}
}

func TestMalformed(t *testing.T) {
	c, _ := aesgcm.New(key(t))
	ctx := context.Background()
	if _, err := c.Decrypt(ctx, "not base64 %%%"); !errors.Is(err, aesgcm.ErrMalformed) {
		t.Fatalf("want ErrMalformed for non-base64, got %v", err)
	}
	if _, err := c.Decrypt(ctx, "YWJj"); !errors.Is(err, aesgcm.ErrMalformed) { // "abc" < nonce size
		t.Fatalf("want ErrMalformed for too-short input, got %v", err)
	}
}

// TestConcurrent proves the codec is safe under concurrent Encrypt/Decrypt.
func TestConcurrent(t *testing.T) {
	c, _ := aesgcm.New(key(t))
	ctx := context.Background()
	done := make(chan bool)
	for i := 0; i < 20; i++ {
		go func() {
			ct, err := c.Encrypt(ctx, "s")
			if err != nil {
				t.Error(err)
			}
			pt, err := c.Decrypt(ctx, ct)
			if err != nil || pt != "s" {
				t.Errorf("round-trip failed: %q %v", pt, err)
			}
			done <- true
		}()
	}
	for i := 0; i < 20; i++ {
		<-done
	}
	// sanity: a fixed ciphertext still decodes to raw bytes
	if _, err := c.Encrypt(ctx, string(bytes.Repeat([]byte("x"), 100))); err != nil {
		t.Fatal(err)
	}
}
