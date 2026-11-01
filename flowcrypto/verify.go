package flowcrypto

import (
	"bytes"
	"crypto"
	"crypto/ecdsa"
	"crypto/ed25519"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/pem"
	"errors"
	"fmt"
	"strings"

	pgpcrypto "github.com/ProtonMail/gopenpgp/v2/crypto"
)

// VerificationResult contains metadata about a successful signature verification.
type VerificationResult struct {
	Valid       bool              `json:"valid"`
	Format      string            `json:"format"`
	Algorithm   string            `json:"algorithm"`
	SignerInfo  string            `json:"signer_info"`
	Certificate *x509.Certificate `json:"-"`
}

// ParsePublicKeyOrCertPEM parses a public key from PEM containing either a public key or an X.509 certificate.
func ParsePublicKeyOrCertPEM(pemBytes []byte) (crypto.PublicKey, *x509.Certificate, error) {
	var lastErr error
	rest := pemBytes

	for {
		var block *pem.Block
		block, rest = pem.Decode(rest)
		if block == nil {
			break
		}

		switch block.Type {
		case "CERTIFICATE":
			cert, err := x509.ParseCertificate(block.Bytes)
			if err == nil {
				return cert.PublicKey, cert, nil
			}
			lastErr = err

		case "PUBLIC KEY":
			pub, err := x509.ParsePKIXPublicKey(block.Bytes)
			if err == nil {
				return pub, nil, nil
			}
			lastErr = err

		case "RSA PUBLIC KEY":
			pub, err := x509.ParsePKCS1PublicKey(block.Bytes)
			if err == nil {
				return pub, nil, nil
			}
			lastErr = err
		}
	}

	// Try raw DER fallback
	if cert, err := x509.ParseCertificate(pemBytes); err == nil {
		return cert.PublicKey, cert, nil
	}
	if pub, err := x509.ParsePKIXPublicKey(pemBytes); err == nil {
		return pub, nil, nil
	}

	if lastErr != nil {
		return nil, nil, fmt.Errorf("failed parsing public key or certificate: %w", lastErr)
	}
	return nil, nil, errors.New("no valid PEM public key or certificate found")
}

// VerifyOpenSSL verifies data against a signature using an RSA, ECDSA, or Ed25519 public key or certificate.
func VerifyOpenSSL(data []byte, sig []byte, pubKeyOrCertPEM []byte) (*VerificationResult, error) {
	return VerifyOpenSSLWithCA(data, sig, pubKeyOrCertPEM, nil)
}

// VerifyOpenSSLWithCA verifies data against a signature and optionally validates the signer's certificate chain.
func VerifyOpenSSLWithCA(data []byte, sig []byte, pubKeyOrCertPEM []byte, caCertPEM []byte) (*VerificationResult, error) {
	pub, cert, err := ParsePublicKeyOrCertPEM(pubKeyOrCertPEM)
	if err != nil {
		return nil, err
	}

	h := sha256.Sum256(data)
	signerInfo := "Public Key"
	if cert != nil {
		signerInfo = cert.Subject.CommonName
	}

	result := func(algorithm string) (*VerificationResult, error) {
		if err := verifyCertificateWithCA(cert, caCertPEM); err != nil {
			return nil, err
		}
		return &VerificationResult{
			Valid:       true,
			Format:      "OPENSSL",
			Algorithm:   algorithm,
			SignerInfo:  signerInfo,
			Certificate: cert,
		}, nil
	}

	switch k := pub.(type) {
	case *rsa.PublicKey:
		// Try PKCS#1 v1.5 first
		if err := rsa.VerifyPKCS1v15(k, crypto.SHA256, h[:], sig); err == nil {
			return result("RSA-SHA256")
		}
		// Fall back to RSA-PSS
		if err := rsa.VerifyPSS(k, crypto.SHA256, h[:], sig, nil); err == nil {
			return result("RSA-PSS-SHA256")
		}
		return nil, errors.New("RSA signature verification failed")

	case *ecdsa.PublicKey:
		if ecdsa.VerifyASN1(k, h[:], sig) {
			return result("ECDSA-SHA256")
		}
		return nil, errors.New("ECDSA signature verification failed")

	case ed25519.PublicKey:
		if ed25519.Verify(k, data, sig) {
			return result("ED25519")
		}
		return nil, errors.New("Ed25519 signature verification failed")

	default:
		return nil, fmt.Errorf("unsupported public key type: %T", pub)
	}
}

func verifyCertificateWithCA(cert *x509.Certificate, caCertPEM []byte) error {
	if len(caCertPEM) == 0 {
		return nil
	}
	if cert == nil {
		return errors.New("CA chain validation requires a signer certificate")
	}
	rootCert, err := ParseCertificatePEM(caCertPEM)
	if err != nil {
		return fmt.Errorf("failed parsing CA certificate: %w", err)
	}
	roots := x509.NewCertPool()
	roots.AddCert(rootCert)
	opts := x509.VerifyOptions{
		Roots:     roots,
		KeyUsages: []x509.ExtKeyUsage{x509.ExtKeyUsageCodeSigning},
	}
	if _, err := cert.Verify(opts); err != nil {
		return fmt.Errorf("certificate verification failed: %w", err)
	}
	return nil
}

// VerifyOpenPGP verifies data against an armored detached OpenPGP signature using an armored public keyring.
func VerifyOpenPGP(data []byte, sigArmored []byte, keyRingArmored []byte) (*VerificationResult, error) {
	key, err := pgpcrypto.NewKeyFromArmored(string(keyRingArmored))
	if err != nil {
		return nil, fmt.Errorf("failed reading OpenPGP public key: %w", err)
	}

	keyRing, err := pgpcrypto.NewKeyRing(key)
	if err != nil {
		return nil, fmt.Errorf("failed initializing OpenPGP keyring: %w", err)
	}

	pgpSig, err := pgpcrypto.NewPGPSignatureFromArmored(string(sigArmored))
	if err != nil {
		return nil, fmt.Errorf("failed parsing OpenPGP signature: %w", err)
	}

	if err := keyRing.VerifyDetached(pgpcrypto.NewPlainMessage(data), pgpSig, pgpcrypto.GetUnixTime()); err != nil {
		return nil, fmt.Errorf("OpenPGP signature verification failed: %w", err)
	}

	identities := keyRing.GetIdentities()
	var signerParts []string
	for _, id := range identities {
		if id.Email != "" {
			signerParts = append(signerParts, fmt.Sprintf("%s <%s>", id.Name, id.Email))
		} else if id.Name != "" {
			signerParts = append(signerParts, id.Name)
		}
	}
	signerInfo := strings.Join(signerParts, ", ")
	if signerInfo == "" {
		signerInfo = key.GetFingerprint()
	}

	return &VerificationResult{
		Valid:      true,
		Format:     "PGP",
		Algorithm:  "OPENPGP",
		SignerInfo: signerInfo,
	}, nil
}

// VerifyPKCS7Detached verifies data against a detached PKCS#7 / CMS signature (DER, PEM, or FLOWSIG).
func VerifyPKCS7Detached(data []byte, p7Bytes []byte, caCertPEM []byte) (*VerificationResult, error) {
	return VerifyPKCS7DetachedWithCA(data, p7Bytes, nil, caCertPEM)
}

// VerifyPKCS7DetachedWithCA verifies data against a detached PKCS#7 / CMS signature (DER, PEM, or FLOWSIG)
// with optional signer certificate PEM and optional CA certificate PEM.
func VerifyPKCS7DetachedWithCA(data []byte, p7Bytes []byte, signerCertPEM []byte, caCertPEM []byte) (*VerificationResult, error) {
	// If PEM encoded, decode block
	if bytes.Contains(p7Bytes, []byte("-----BEGIN PKCS7-----")) {
		block, _ := pem.Decode(p7Bytes)
		if block != nil {
			p7Bytes = block.Bytes
		}
	}

	var signerCert *x509.Certificate
	if len(signerCertPEM) > 0 {
		var err error
		signerCert, err = ParseCertificatePEM(signerCertPEM)
		if err != nil {
			return nil, fmt.Errorf("failed parsing signer certificate: %w", err)
		}
	}

	var rootCert *x509.Certificate
	if len(caCertPEM) > 0 {
		var err error
		rootCert, err = ParseCertificatePEM(caCertPEM)
		if err != nil {
			return nil, fmt.Errorf("failed parsing CA certificate: %w", err)
		}
	}

	// If no independent CA is provided, but signerCertPEM was passed into caCertPEM, use it as rootCert
	if rootCert == nil && signerCert == nil && len(caCertPEM) > 0 {
		rootCert, _ = ParseCertificatePEM(caCertPEM)
	}

	cert, err := VerifyPKCS7WithCA(data, p7Bytes, signerCert, rootCert)
	if err != nil {
		return nil, err
	}

	return &VerificationResult{
		Valid:       true,
		Format:      "PKCS7",
		Algorithm:   "CMS-PKCS7",
		SignerInfo:  cert.Subject.CommonName,
		Certificate: cert,
	}, nil
}

// VerifyAuto verifies data against a signature using whatever key, certificate, or keyring is provided.
// Automatically recognizes FLOWSIG:v1: envelopes, PGP armored blocks, PKCS#7 DER, and standard OpenSSL signatures.
func VerifyAuto(data []byte, sigBytes []byte, keyOrCertBytes []byte) (*VerificationResult, error) {
	return VerifyAutoWithCA(data, sigBytes, keyOrCertBytes, nil)
}

// VerifyAutoWithCA verifies data against a signature with an optional CA certificate for chain validation.
func VerifyAutoWithCA(data []byte, sigBytes []byte, keyOrCertBytes []byte, caCertBytes []byte) (*VerificationResult, error) {
	trimmedSig := bytes.TrimSpace(sigBytes)

	// Check if wrapped in FLOWSIG:v1:...
	if bytes.HasPrefix(trimmedSig, []byte("FLOWSIG:v1:")) {
		parts := strings.SplitN(string(trimmedSig), ":", 4)
		if len(parts) >= 3 {
			format := strings.ToUpper(parts[2])
			payloadB64 := parts[len(parts)-1]
			decoded, err := base64.StdEncoding.DecodeString(payloadB64)
			if err != nil {
				return nil, fmt.Errorf("invalid base64 in FLOWSIG envelope: %w", err)
			}

			switch {
			case strings.HasPrefix(format, "OPENSSL"):
				return VerifyOpenSSLWithCA(data, decoded, keyOrCertBytes, caCertBytes)
			case format == "PGP":
				return VerifyOpenPGP(data, decoded, keyOrCertBytes)
			case format == "PKCS7":
				return VerifyPKCS7DetachedWithCA(data, decoded, keyOrCertBytes, caCertBytes)
			}
		}
	}

	// Check OpenPGP
	if bytes.Contains(trimmedSig, []byte("-----BEGIN PGP SIGNATURE-----")) {
		return VerifyOpenPGP(data, trimmedSig, keyOrCertBytes)
	}

	// Check PKCS#7 PEM
	if bytes.Contains(trimmedSig, []byte("-----BEGIN PKCS7-----")) {
		return VerifyPKCS7DetachedWithCA(data, trimmedSig, keyOrCertBytes, caCertBytes)
	}

	// Try PKCS#7 DER
	if res, err := VerifyPKCS7DetachedWithCA(data, trimmedSig, keyOrCertBytes, caCertBytes); err == nil {
		return res, nil
	}

	// Try OpenSSL with public key or certificate
	if res, err := VerifyOpenSSLWithCA(data, trimmedSig, keyOrCertBytes, caCertBytes); err == nil {
		return res, nil
	}

	// If raw base64 string, try decoding and verifying
	if decoded, err := base64.StdEncoding.DecodeString(string(trimmedSig)); err == nil {
		if res, err := VerifyOpenSSLWithCA(data, decoded, keyOrCertBytes, caCertBytes); err == nil {
			return res, nil
		}
		if res, err := VerifyPKCS7DetachedWithCA(data, decoded, keyOrCertBytes, caCertBytes); err == nil {
			return res, nil
		}
	}

	return nil, errors.New("signature verification failed: unsupported signature format or signature does not match data/key")
}
