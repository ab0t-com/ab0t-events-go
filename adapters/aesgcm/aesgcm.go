// Package aesgcm is a stdlib-only events.SecretCodec: AES-256-GCM authenticated
// encryption of subscription secrets, for use with adapters/securestore.
//
// It is dependency-free (crypto/aes + crypto/cipher). A 32-byte key selects
// AES-256; each Encrypt draws a fresh random nonce, so encrypting the same secret
// twice yields different ciphertexts. The output is base64(nonce || sealed),
// which includes GCM's authentication tag — tampering or a wrong key fails
// Decrypt rather than returning garbage.
//
// Key management is the caller's concern: load the 32-byte key from your secret
// manager (KMS, Vault, a sealed env var). For KMS-managed keys without holding
// key bytes yourself, use adapters/kmscodec instead.
package aesgcm

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/base64"
	"errors"
	"io"

	"github.com/ab0t-com/ab0t-events-go/events"
)

// KeySize is the required key length in bytes (AES-256).
const KeySize = 32

// ErrKeySize is returned by New when the key is not exactly KeySize bytes.
var ErrKeySize = errors.New("aesgcm: key must be 32 bytes (AES-256)")

// ErrMalformed is returned by Decrypt when the input is not valid base64 or is
// too short to contain a nonce.
var ErrMalformed = errors.New("aesgcm: malformed ciphertext")

// Codec is an AES-256-GCM events.SecretCodec. Construct it with New. It is safe
// for concurrent use (cipher.AEAD is, and rand.Reader is).
type Codec struct {
	aead    cipher.AEAD
	randSrc io.Reader // nonce source; nil => crypto/rand.Reader (overridable in tests)
}

var _ events.SecretCodec = (*Codec)(nil)

// New returns an AES-256-GCM codec for the given 32-byte key. It returns
// ErrKeySize if the key length is wrong.
func New(key []byte) (*Codec, error) {
	if len(key) != KeySize {
		return nil, ErrKeySize
	}
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	aead, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}
	return &Codec{aead: aead}, nil
}

// Encrypt seals plaintext under AES-256-GCM and returns base64(nonce || sealed).
func (c *Codec) Encrypt(_ context.Context, plaintext string) (string, error) {
	nonce := make([]byte, c.aead.NonceSize())
	src := c.randSrc
	if src == nil {
		src = rand.Reader
	}
	if _, err := io.ReadFull(src, nonce); err != nil {
		return "", err
	}
	sealed := c.aead.Seal(nonce, nonce, []byte(plaintext), nil)
	return base64.StdEncoding.EncodeToString(sealed), nil
}

// Decrypt reverses Encrypt. It returns ErrMalformed for input that is not valid
// base64 or is shorter than a nonce, and a non-nil error if authentication fails
// (wrong key or tampered ciphertext).
func (c *Codec) Decrypt(_ context.Context, ciphertext string) (string, error) {
	raw, err := base64.StdEncoding.DecodeString(ciphertext)
	if err != nil {
		return "", ErrMalformed
	}
	ns := c.aead.NonceSize()
	if len(raw) < ns {
		return "", ErrMalformed
	}
	nonce, sealed := raw[:ns], raw[ns:]
	plain, err := c.aead.Open(nil, nonce, sealed, nil)
	if err != nil {
		return "", err
	}
	return string(plain), nil
}
