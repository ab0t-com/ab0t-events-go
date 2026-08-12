package securestore_test

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/ab0t-com/ab0t-events-go/adapters/aesgcm"
	"github.com/ab0t-com/ab0t-events-go/adapters/memory"
	"github.com/ab0t-com/ab0t-events-go/adapters/securestore"
	"github.com/ab0t-com/ab0t-events-go/events"
)

func newCodec(t *testing.T) *aesgcm.Codec {
	t.Helper()
	k := make([]byte, aesgcm.KeySize)
	for i := range k {
		k[i] = byte(i + 7)
	}
	c, err := aesgcm.New(k)
	if err != nil {
		t.Fatal(err)
	}
	return c
}

// TestEncryptedAtRest is the behavioral proof: the wrapped store holds only
// ciphertext, while reads through the decorator return the plaintext secret.
func TestEncryptedAtRest(t *testing.T) {
	ctx := context.Background()
	inner := memory.NewStore()
	codec := newCodec(t)
	store := securestore.New(inner, codec)

	plain := "whsec_" + strings.Repeat("f", 64)
	sub := events.Subscription{ID: "s1", OrgID: "org1", EventTypes: []string{"*"}, URL: "https://x", Secret: plain, Active: true}
	if err := store.Put(ctx, sub); err != nil {
		t.Fatal(err)
	}

	// The Put must not mutate the caller's struct.
	if sub.Secret != plain {
		t.Fatalf("Put mutated caller's secret: %q", sub.Secret)
	}

	// The underlying store holds ciphertext, never the plaintext.
	raw, err := inner.Get(ctx, "org1", "s1")
	if err != nil {
		t.Fatal(err)
	}
	if raw.Secret == plain || raw.Secret == "" {
		t.Fatalf("inner store should hold ciphertext, got %q", raw.Secret)
	}
	if strings.Contains(raw.Secret, "whsec_") {
		t.Fatalf("ciphertext leaks the plaintext secret: %q", raw.Secret)
	}
	// And that ciphertext really decrypts back to the plaintext.
	if dec, _ := codec.Decrypt(ctx, raw.Secret); dec != plain {
		t.Fatalf("stored ciphertext did not decrypt to plaintext")
	}

	// Reads through the decorator return plaintext.
	got, err := store.Get(ctx, "org1", "s1")
	if err != nil || got.Secret != plain {
		t.Fatalf("decorator Get should return plaintext: %q err=%v", got.Secret, err)
	}
	list, _ := store.ListForOrg(ctx, "org1")
	if len(list) != 1 || list[0].Secret != plain {
		t.Fatalf("ListForOrg should return plaintext: %+v", list)
	}
	byType, _ := store.ListForOrgAndType(ctx, "org1", "anything")
	if len(byType) != 1 || byType[0].Secret != plain {
		t.Fatalf("ListForOrgAndType should return plaintext: %+v", byType)
	}
}

// TestTenancyPreserved proves the decorator adds no cross-org path: it delegates
// tenancy to the wrapped store.
func TestTenancyPreserved(t *testing.T) {
	ctx := context.Background()
	store := securestore.New(memory.NewStore(), newCodec(t))
	_ = store.Put(ctx, events.Subscription{ID: "s1", OrgID: "orgA", EventTypes: []string{"*"}, URL: "https://x", Secret: "sek", Active: true})

	if _, err := store.Get(ctx, "orgB", "s1"); !errors.Is(err, events.ErrNotFound) {
		t.Fatalf("cross-org Get should be ErrNotFound, got %v", err)
	}
	if list, _ := store.ListForOrg(ctx, "orgB"); len(list) != 0 {
		t.Fatalf("orgB must not see orgA subs, got %d", len(list))
	}
}

// TestEmptySecretPassthrough proves an empty secret is neither encrypted nor
// decrypted (no codec call needed).
func TestEmptySecretPassthrough(t *testing.T) {
	ctx := context.Background()
	inner := memory.NewStore()
	store := securestore.New(inner, newCodec(t))
	_ = store.Put(ctx, events.Subscription{ID: "s1", OrgID: "org1", EventTypes: []string{"*"}, URL: "https://x", Secret: "", Active: true})

	raw, _ := inner.Get(ctx, "org1", "s1")
	if raw.Secret != "" {
		t.Fatalf("empty secret should stay empty at rest, got %q", raw.Secret)
	}
	got, _ := store.Get(ctx, "org1", "s1")
	if got.Secret != "" {
		t.Fatalf("empty secret should read back empty, got %q", got.Secret)
	}
}

// failCodec returns errors, to prove decrypt/encrypt failures propagate rather
// than leaking ciphertext as a secret.
type failCodec struct{}

func (failCodec) Encrypt(_ context.Context, _ string) (string, error) {
	return "", errors.New("encrypt boom")
}
func (failCodec) Decrypt(_ context.Context, _ string) (string, error) {
	return "", errors.New("decrypt boom")
}

func TestCodecErrorsPropagate(t *testing.T) {
	ctx := context.Background()
	inner := memory.NewStore()
	store := securestore.New(inner, failCodec{})

	// Encrypt failure surfaces on Put; nothing is stored.
	if err := store.Put(ctx, events.Subscription{ID: "s1", OrgID: "o", URL: "u", Secret: "x", EventTypes: []string{"*"}}); err == nil {
		t.Fatal("Put should surface an encrypt error")
	}
	if list, _ := inner.ListForOrg(ctx, "o"); len(list) != 0 {
		t.Fatal("failed Put must not persist a subscription")
	}

	// Decrypt failure surfaces on read (never returns ciphertext as the secret).
	_ = inner.Put(ctx, events.Subscription{ID: "s2", OrgID: "o", URL: "u", Secret: "ciphertext", EventTypes: []string{"*"}})
	if _, err := store.Get(ctx, "o", "s2"); err == nil {
		t.Fatal("Get should surface a decrypt error")
	}
	if _, err := store.ListForOrg(ctx, "o"); err == nil {
		t.Fatal("ListForOrg should surface a decrypt error")
	}
}

func TestNilArgsPanic(t *testing.T) {
	assertPanic(t, func() { securestore.New(nil, newCodec(t)) })
	assertPanic(t, func() { securestore.New(memory.NewStore(), nil) })
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
