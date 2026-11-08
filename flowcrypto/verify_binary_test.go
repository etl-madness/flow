package flowcrypto

import (
	"bytes"
	"crypto/ed25519"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"testing"
)

func TestVerifyAutoPreservesRawSignatureWhitespace(t *testing.T) {
	privateKey := ed25519.NewKeyFromSeed(make([]byte, ed25519.SeedSize))
	publicDER, err := x509.MarshalPKIXPublicKey(privateKey.Public())
	if err != nil {
		t.Fatal(err)
	}
	publicPEM := pem.EncodeToMemory(&pem.Block{Type: "PUBLIC KEY", Bytes: publicDER})

	for i := 0; i < 1000; i++ {
		data := []byte(fmt.Sprintf("test-message-%d", i))
		signature := ed25519.Sign(privateKey, data)
		if len(bytes.TrimSpace(signature)) == len(signature) {
			continue
		}
		result, err := VerifyAuto(data, signature, publicPEM)
		if err != nil || !result.Valid {
			t.Fatalf("raw signature with whitespace byte did not verify: %v", err)
		}
		if _, err := VerifyAuto([]byte("tampered"), signature, publicPEM); err == nil {
			t.Fatal("tampered content verified")
		}
		return
	}
	t.Fatal("deterministic test did not generate a signature ending or starting with whitespace")
}
