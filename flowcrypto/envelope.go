package flowcrypto

import (
	"bytes"
	"encoding/base64"
	"errors"
	"fmt"
	"strings"
)

const (
	// ArmoredPrefixV1 is the header identifying text-armored version 1 encrypted payloads.
	ArmoredPrefixV1 = "FLOWENC:v1:"

	// SignaturePrefixV1 is the header identifying text-armored detached signatures.
	SignaturePrefixV1 = "FLOWSIG:v1:"

	// SignedPrefixV1 is the header identifying unified signed payload envelopes (FLOWSIGNED:v1:<format>:<sig>:<content>).
	SignedPrefixV1 = "FLOWSIGNED:v1:"
)

var (
	// ErrNotEncrypted is returned when attempting to decrypt text missing the FLOWENC prefix.
	ErrNotEncrypted = errors.New("data is not in recognized encrypted format (missing FLOWENC header)")

	// ErrNotSigned is returned when attempting to parse a signed envelope missing the FLOWSIGNED prefix.
	ErrNotSigned = errors.New("data is not in recognized signed format (missing FLOWSIGNED header)")
)

// IsEncryptedPayload checks if the data begins with the FLOWENC envelope prefix.
func IsEncryptedPayload(data []byte) bool {
	trimmed := bytes.TrimSpace(data)
	return bytes.HasPrefix(trimmed, []byte(ArmoredPrefixV1))
}

// IsSignaturePayload checks if the data begins with the FLOWSIG envelope prefix.
func IsSignaturePayload(data []byte) bool {
	trimmed := bytes.TrimSpace(data)
	return bytes.HasPrefix(trimmed, []byte(SignaturePrefixV1))
}

// IsSignedPayload checks if the data begins with the FLOWSIGNED envelope prefix.
func IsSignedPayload(data []byte) bool {
	trimmed := bytes.TrimSpace(data)
	return bytes.HasPrefix(trimmed, []byte(SignedPrefixV1))
}

// EncryptArmored encrypts plaintext bytes and formats them as a text-safe armored string.
// Format: FLOWENC:v1:<base64(salt || nonce || ciphertext + tag)>
func EncryptArmored(plaintext []byte, key string) (string, error) {
	packed, err := EncryptBytes(plaintext, key)
	if err != nil {
		return "", err
	}

	encoded := base64.StdEncoding.EncodeToString(packed)
	return ArmoredPrefixV1 + encoded, nil
}

// DecryptArmored decrypts a text-safe armored string formatted with FLOWENC:v1:.
func DecryptArmored(armoredText string, key string) ([]byte, error) {
	trimmed := strings.TrimSpace(armoredText)
	if !strings.HasPrefix(trimmed, ArmoredPrefixV1) {
		return nil, ErrNotEncrypted
	}

	rawB64 := strings.TrimPrefix(trimmed, ArmoredPrefixV1)
	packed, err := base64.StdEncoding.DecodeString(rawB64)
	if err != nil {
		return nil, fmt.Errorf("failed decoding base64 encrypted payload: %w", err)
	}

	return DecryptBytes(packed, key)
}

// DecryptAuto attempts to decrypt data if it contains the armored prefix,
// or if it matches raw binary payload format when requireEncrypted is true.
// If it is already unencrypted plaintext and requireEncrypted is false, it returns the input data unchanged.
func DecryptAuto(data []byte, key string, requireEncrypted bool) ([]byte, error) {
	trimmed := bytes.TrimSpace(data)

	if bytes.HasPrefix(trimmed, []byte(ArmoredPrefixV1)) {
		if key == "" {
			return nil, fmt.Errorf("payload is encrypted (%s) but no secure key was provided (set -secure-key or FLOW_SECURE_KEY)", ArmoredPrefixV1)
		}
		return DecryptArmored(string(trimmed), key)
	}

	if requireEncrypted {
		if key == "" {
			return nil, fmt.Errorf("payload is marked as encrypted but no secure key was provided (set -secure-key or FLOW_SECURE_KEY)")
		}
		// In requireEncrypted mode with non-armored input, process un-trimmed raw bytes
		if len(data) >= MinBinaryPayloadLength {
			decrypted, err := DecryptBytes(data, key)
			if err != nil {
				return nil, err
			}
			return decrypted, nil
		}
		return nil, ErrNotEncrypted
	}

	// Plaintext fallback
	return data, nil
}

// WrapSignatureOnly wraps a raw or armored signature into the FLOWSIG:v1:<format>:<base64> envelope.
func WrapSignatureOnly(format string, sig []byte) string {
	b64 := base64.StdEncoding.EncodeToString(sig)
	return fmt.Sprintf("%s%s:%s", SignaturePrefixV1, format, b64)
}

// WrapSignedPayload wraps a signature and content into a single FLOWSIGNED:v1:<format>:<b64(sig)>:<b64(content)> envelope.
func WrapSignedPayload(format string, sig []byte, content []byte) string {
	b64Sig := base64.StdEncoding.EncodeToString(sig)
	b64Content := base64.StdEncoding.EncodeToString(content)
	return fmt.Sprintf("%s%s:%s:%s", SignedPrefixV1, format, b64Sig, b64Content)
}

// ParseSignedPayload parses a FLOWSIGNED:v1:<format>:<b64(sig)>:<b64(content)> envelope.
func ParseSignedPayload(data []byte) (format string, sig []byte, content []byte, err error) {
	trimmed := strings.TrimSpace(string(data))
	if !strings.HasPrefix(trimmed, SignedPrefixV1) {
		return "", nil, nil, ErrNotSigned
	}

	parts := strings.SplitN(trimmed, ":", 5)
	if len(parts) < 5 {
		return "", nil, nil, errors.New("malformed FLOWSIGNED envelope (expected FLOWSIGNED:v1:<format>:<sig>:<content>)")
	}

	format = parts[2]
	sig, err = base64.StdEncoding.DecodeString(parts[3])
	if err != nil {
		return "", nil, nil, fmt.Errorf("invalid base64 signature in FLOWSIGNED envelope: %w", err)
	}

	content, err = base64.StdEncoding.DecodeString(parts[4])
	if err != nil {
		return "", nil, nil, fmt.Errorf("invalid base64 content in FLOWSIGNED envelope: %w", err)
	}

	return format, sig, content, nil
}

// VerifyPayloadAuto unwraps and verifies a FLOWSIGNED:v1 envelope using the provided key, certificate, or keyring.
// Returns the verified inner content and verification metadata.
func VerifyPayloadAuto(payload []byte, keyOrCertBytes []byte) ([]byte, *VerificationResult, error) {
	return VerifyPayloadAutoWithCA(payload, keyOrCertBytes, nil)
}

// VerifyPayloadAutoWithCA unwraps and verifies a FLOWSIGNED:v1 envelope using key/cert and optional CA root.
func VerifyPayloadAutoWithCA(payload []byte, keyOrCertBytes []byte, caCertBytes []byte) ([]byte, *VerificationResult, error) {
	if !IsSignedPayload(payload) {
		return nil, nil, ErrNotSigned
	}

	format, sig, content, err := ParseSignedPayload(payload)
	if err != nil {
		return nil, nil, err
	}

	var res *VerificationResult
	switch {
	case strings.HasPrefix(strings.ToUpper(format), "OPENSSL"):
		res, err = VerifyOpenSSLWithCA(content, sig, keyOrCertBytes, caCertBytes)
	case strings.ToUpper(format) == "PGP" || strings.ToUpper(format) == "GPG":
		res, err = VerifyOpenPGP(content, sig, keyOrCertBytes)
	case strings.ToUpper(format) == "PKCS7" || strings.ToUpper(format) == "CMS" || strings.ToUpper(format) == "WINDOWS":
		res, err = VerifyPKCS7DetachedWithCA(content, sig, keyOrCertBytes, caCertBytes)
	default:
		res, err = VerifyAutoWithCA(content, sig, keyOrCertBytes, caCertBytes)
	}

	if err != nil {
		return nil, nil, fmt.Errorf("digital signature verification failed for envelope format %s: %w", format, err)
	}

	return content, res, nil
}
