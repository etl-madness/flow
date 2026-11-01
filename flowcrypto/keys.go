package flowcrypto

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"fmt"
	"runtime"

	"golang.org/x/crypto/pbkdf2"
)

const (
	// DefaultKeyLength is the default length in bytes for raw AES-256 keys.
	DefaultKeyLength = 32

	// SaltLength is the length in bytes for PBKDF2 random salt.
	SaltLength = 16

	// PBKDF2Iterations is the iteration count for key derivation.
	PBKDF2Iterations = 100000
)

// GenerateSecureKey creates a cryptographically secure random key formatted as a base64 string.
func GenerateSecureKey(byteLength int) (string, error) {
	if byteLength <= 0 {
		byteLength = DefaultKeyLength
	}

	randomBytes := make([]byte, byteLength)
	if _, err := rand.Read(randomBytes); err != nil {
		return "", fmt.Errorf("failed generating random key bytes: %w", err)
	}

	return base64.StdEncoding.EncodeToString(randomBytes), nil
}

// DeriveKey derives a 32-byte (256-bit) AES key from a passphrase and salt using PBKDF2-HMAC-SHA256.
func DeriveKey(passphrase string, salt []byte) []byte {
	return pbkdf2.Key([]byte(passphrase), salt, PBKDF2Iterations, DefaultKeyLength, sha256.New)
}

// ZeroBytes overwrites a sensitive byte slice with zeroes to minimize exposure in memory.
func ZeroBytes(b []byte) {
	for i := range b {
		b[i] = 0
	}
	runtime.KeepAlive(b)
}
