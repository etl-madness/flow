package flowcrypto

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"errors"
	"fmt"
	"io"
)

const (
	// NonceLength is the standard 96-bit (12-byte) nonce for AES-GCM.
	NonceLength = 12

	// TagLength is the 128-bit (16-byte) authentication tag for AES-GCM.
	TagLength = 16

	// MinBinaryPayloadLength is the minimum byte length for packed binary ciphertext:
	// Salt (16) + Nonce (12) + Tag (16) = 44 bytes.
	MinBinaryPayloadLength = SaltLength + NonceLength + TagLength
)

var (
	// ErrEmptyKey is returned when an empty encryption key is supplied.
	ErrEmptyKey = errors.New("encryption key cannot be empty")

	// ErrInvalidCiphertext is returned when ciphertext is too short or malformed.
	ErrInvalidCiphertext = errors.New("ciphertext payload is invalid or truncated")

	// ErrAuthFailed is returned when authentication/decryption fails (wrong key or corrupted data).
	ErrAuthFailed = errors.New("decryption failed: invalid key or tampered ciphertext")
)

// EncryptBytes encrypts plaintext using AES-256-GCM with a PBKDF2-derived key.
// The returned slice contains: salt (16 bytes) || nonce (12 bytes) || ciphertext + tag (16 bytes).
func EncryptBytes(plaintext []byte, key string) ([]byte, error) {
	if key == "" {
		return nil, ErrEmptyKey
	}

	// 1. Generate cryptographically secure random salt
	salt := make([]byte, SaltLength)
	if _, err := io.ReadFull(rand.Reader, salt); err != nil {
		return nil, fmt.Errorf("failed generating salt: %w", err)
	}

	// 2. Derive 256-bit AES key
	derivedKey := DeriveKey(key, salt)
	defer ZeroBytes(derivedKey)

	// 3. Initialize AES cipher
	block, err := aes.NewCipher(derivedKey)
	if err != nil {
		return nil, fmt.Errorf("failed creating AES cipher: %w", err)
	}

	// 4. Initialize GCM mode
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, fmt.Errorf("failed creating GCM AEAD: %w", err)
	}

	// 5. Generate random nonce
	nonce := make([]byte, gcm.NonceSize())
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return nil, fmt.Errorf("failed generating nonce: %w", err)
	}

	// 6. Encrypt and authenticate
	sealed := gcm.Seal(nil, nonce, plaintext, nil)

	// 7. Pack binary: salt + nonce + ciphertext/tag
	packed := make([]byte, 0, len(salt)+len(nonce)+len(sealed))
	packed = append(packed, salt...)
	packed = append(packed, nonce...)
	packed = append(packed, sealed...)

	return packed, nil
}

// DecryptBytes decrypts a packed binary ciphertext produced by EncryptBytes.
func DecryptBytes(packed []byte, key string) ([]byte, error) {
	if key == "" {
		return nil, ErrEmptyKey
	}

	if len(packed) < MinBinaryPayloadLength {
		return nil, ErrInvalidCiphertext
	}

	// 1. Extract salt and nonce
	salt := packed[:SaltLength]
	nonce := packed[SaltLength : SaltLength+NonceLength]
	ciphertext := packed[SaltLength+NonceLength:]

	// 2. Derive key from passphrase and salt
	derivedKey := DeriveKey(key, salt)
	defer ZeroBytes(derivedKey)

	// 3. Initialize AES cipher
	block, err := aes.NewCipher(derivedKey)
	if err != nil {
		return nil, fmt.Errorf("failed creating AES cipher: %w", err)
	}

	// 4. Initialize GCM mode
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, fmt.Errorf("failed creating GCM AEAD: %w", err)
	}

	// 5. Decrypt and verify tag
	plaintext, err := gcm.Open(nil, nonce, ciphertext, nil)
	if err != nil {
		return nil, ErrAuthFailed
	}

	return plaintext, nil
}
