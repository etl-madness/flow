package flowcrypto

import (
	"crypto/ecdsa"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha512"
	"crypto/x509"
	"encoding/asn1"
	"encoding/pem"
	"math/big"
	"strings"
	"testing"
)

func TestECDSAPKCS7AndMalformedSignatures(t *testing.T) {
	privPEM, pubPEM, err := GenerateECDSAKeyPair("P-256")
	if err != nil {
		t.Fatal(err)
	}
	privateKey, err := ParsePrivateKeyPEM(privPEM)
	if err != nil {
		t.Fatal(err)
	}
	publicKey, _, err := ParsePublicKeyOrCertPEM(pubPEM)
	if err != nil {
		t.Fatal(err)
	}
	certPEM, err := GenerateSelfSignedCertificate(privateKey, publicKey, "ECDSA signer", 1)
	if err != nil {
		t.Fatal(err)
	}
	cert, err := ParseCertificatePEM(certPEM)
	if err != nil {
		t.Fatal(err)
	}
	data := []byte("signed message")
	sig, err := SignPKCS7(data, cert, privateKey)
	if err != nil {
		t.Fatal(err)
	}
	if verified, err := VerifyPKCS7(data, sig, cert); err != nil || !verified.Equal(cert) {
		t.Fatalf("ECDSA signature verification failed: %v", err)
	}
	if _, err := VerifyPKCS7([]byte("tampered"), sig, cert); err == nil {
		t.Fatal("tampered message unexpectedly verified")
	}

	var content pkcs7ContentInfo
	if _, err := asn1.Unmarshal(sig, &content); err != nil {
		t.Fatal(err)
	}
	var signed pkcs7SignedData
	if _, err := asn1.Unmarshal(content.Content.Bytes, &signed); err != nil {
		t.Fatal(err)
	}
	mutate := func(change func(*pkcs7SignedData)) []byte {
		t.Helper()
		var current pkcs7SignedData
		if _, err := asn1.Unmarshal(content.Content.Bytes, &current); err != nil {
			t.Fatal(err)
		}
		change(&current)
		encoded, err := asn1.Marshal(current)
		if err != nil {
			t.Fatal(err)
		}
		container := content
		container.Content.Bytes = encoded
		container.Content.FullBytes = nil
		result, err := asn1.Marshal(container)
		if err != nil {
			t.Fatal(err)
		}
		return result
	}
	cases := []struct {
		name string
		data []byte
		want string
	}{
		{"empty", nil, "empty PKCS#7"},
		{"invalid-der", []byte("not DER"), "unmarshaling"},
		{"missing-signers", mutate(func(s *pkcs7SignedData) { s.SignerInfos = nil }), "no SignerInfos"},
		{"unsupported-digest", mutate(func(s *pkcs7SignedData) { s.SignerInfos[0].DigestAlgorithm.Algorithm = asn1.ObjectIdentifier{1, 2, 3} }), "unsupported PKCS#7 digest"},
		{"missing-digest", mutate(func(s *pkcs7SignedData) { s.SignerInfos[0].SignedAttributes = s.SignerInfos[0].SignedAttributes[:1] }), "messageDigest signed attribute not found"},
		{"bad-signature", mutate(func(s *pkcs7SignedData) { s.SignerInfos[0].Signature[0] ^= 0xff }), "ECDSA PKCS#7 signature verification failed"},
		{"missing-attributes", mutate(func(s *pkcs7SignedData) { s.SignerInfos[0].SignedAttributes = nil }), "ECDSA PKCS#7 signature verification failed"},
		{"sha512-digest-mismatch", mutate(func(s *pkcs7SignedData) { s.SignerInfos[0].DigestAlgorithm.Algorithm = oidSHA512 }), "message digest mismatch"},
		{"invalid-certificate", mutate(func(s *pkcs7SignedData) { s.Certificates.Bytes = []byte("invalid"); s.Certificates.FullBytes = nil }), "failed parsing certificates"},
		{"unknown-signer", mutate(func(s *pkcs7SignedData) { s.SignerInfos[0].IssuerAndSerial.SerialNumber = big.NewInt(0) }), "signer certificate"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := VerifyPKCS7(data, tc.data, cert); err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("VerifyPKCS7 error = %v, want %q", err, tc.want)
			}
		})
	}
	if _, err := SignPKCS7(data, nil, privateKey); err == nil {
		t.Fatal("PKCS7 signing without certificate succeeded")
	}
	if _, err := SignPKCS7(data, cert, nil); err == nil {
		t.Fatal("PKCS7 signing without private key succeeded")
	}
	if _, err := SignPKCS7(data, cert, "unsupported"); err == nil {
		t.Fatal("PKCS7 signing with unsupported key succeeded")
	}
	if _, err := VerifyPKCS7WithCA(data, sig, nil, nil); err == nil || !strings.Contains(err.Error(), "requires a trusted") {
		t.Fatalf("untrusted signature error = %v", err)
	}
	noEmbeddedCert := mutate(func(s *pkcs7SignedData) {
		s.Certificates.Bytes = nil
		s.Certificates.FullBytes = nil
	})
	if verified, err := VerifyPKCS7WithCA(data, noEmbeddedCert, cert, nil); err != nil || !verified.Equal(cert) {
		t.Fatalf("signature using external signer certificate: %v", err)
	}
	sha512Signature := mutate(func(s *pkcs7SignedData) {
		signer := &s.SignerInfos[0]
		signer.DigestAlgorithm.Algorithm = oidSHA512
		digest := sha512.Sum512(data)
		encodedDigest, err := asn1.Marshal(digest[:])
		if err != nil {
			t.Fatal(err)
		}
		for i := range signer.SignedAttributes {
			attr := &signer.SignedAttributes[i]
			if attr.Type.Equal(oidMessageDigest) {
				attr.Value.Bytes = encodedDigest
				attr.Value.FullBytes = nil
			}
		}
		encodedAttrs, err := asn1.MarshalWithParams(signer.SignedAttributes, "set")
		if err != nil {
			t.Fatal(err)
		}
		attrDigest := sha512.Sum512(encodedAttrs)
		signer.Signature, err = ecdsa.SignASN1(rand.Reader, privateKey.(*ecdsa.PrivateKey), attrDigest[:])
		if err != nil {
			t.Fatal(err)
		}
	})
	if _, err := VerifyPKCS7(data, sha512Signature, cert); err != nil {
		t.Fatalf("valid SHA-512 PKCS7 signature rejected: %v", err)
	}
	otherPEM, err := GenerateSelfSignedCertificate(privateKey, publicKey, "Other signer", 1)
	if err != nil {
		t.Fatal(err)
	}
	other, err := ParseCertificatePEM(otherPEM)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := VerifyPKCS7WithCA(data, sig, other, nil); err == nil || !strings.Contains(err.Error(), "does not match") {
		t.Fatalf("incorrect trusted signer error = %v", err)
	}
	if _, err := VerifyPKCS7(data, sig, other); err == nil || !strings.Contains(err.Error(), "certificate verification failed") {
		t.Fatalf("incorrect root certificate error = %v", err)
	}
}

func TestPublicAndPrivateKeyFormats(t *testing.T) {
	privPEM, pubPEM, err := GenerateRSAKeyPair(2048)
	if err != nil {
		t.Fatal(err)
	}
	privateKey, err := ParsePrivateKeyPEM(privPEM)
	if err != nil {
		t.Fatal(err)
	}
	rsaKey, ok := privateKey.(*rsa.PrivateKey)
	if !ok {
		t.Fatal("expected RSA private key")
	}
	publicDER := x509.MarshalPKCS1PublicKey(&rsaKey.PublicKey)
	rsaPublic := pem.EncodeToMemory(&pem.Block{Type: "RSA PUBLIC KEY", Bytes: publicDER})
	if _, _, err := ParsePublicKeyOrCertPEM(rsaPublic); err != nil {
		t.Fatalf("RSA public PEM rejected: %v", err)
	}
	parsedBlock, _ := pem.Decode(pubPEM)
	if _, _, err := ParsePublicKeyOrCertPEM(parsedBlock.Bytes); err != nil {
		t.Fatalf("raw public DER rejected: %v", err)
	}
	for _, kind := range []string{"CERTIFICATE", "PUBLIC KEY", "RSA PUBLIC KEY"} {
		invalid := pem.EncodeToMemory(&pem.Block{Type: kind, Bytes: []byte("invalid")})
		if _, _, err := ParsePublicKeyOrCertPEM(invalid); err == nil {
			t.Errorf("invalid %s unexpectedly parsed", kind)
		}
	}
	privateBlock, _ := pem.Decode(privPEM)
	fallback := pem.EncodeToMemory(&pem.Block{Type: "LEGACY", Bytes: privateBlock.Bytes})
	if _, err := ParsePrivateKeyPEM(fallback); err != nil {
		t.Fatalf("legacy RSA private PEM rejected: %v", err)
	}
}
