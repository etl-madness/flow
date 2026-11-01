package flowcrypto

import (
	"crypto"
	"crypto/ecdsa"
	"crypto/ed25519"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"fmt"
	"math/big"
	"strings"
	"time"

	pgpcrypto "github.com/ProtonMail/gopenpgp/v2/crypto"
)

// GenerateRSAKeyPair creates a new RSA private/public key pair in PEM format.
func GenerateRSAKeyPair(bits int) (privPEM []byte, pubPEM []byte, err error) {
	if bits < 2048 {
		bits = 2048
	}
	key, err := rsa.GenerateKey(rand.Reader, bits)
	if err != nil {
		return nil, nil, fmt.Errorf("failed generating RSA key: %w", err)
	}

	privDER := x509.MarshalPKCS1PrivateKey(key)
	privBlock := &pem.Block{
		Type:  "RSA PRIVATE KEY",
		Bytes: privDER,
	}
	privPEM = pem.EncodeToMemory(privBlock)

	pubDER, err := x509.MarshalPKIXPublicKey(&key.PublicKey)
	if err != nil {
		return nil, nil, fmt.Errorf("failed marshaling RSA public key: %w", err)
	}
	pubBlock := &pem.Block{
		Type:  "PUBLIC KEY",
		Bytes: pubDER,
	}
	pubPEM = pem.EncodeToMemory(pubBlock)

	return privPEM, pubPEM, nil
}

// GenerateECDSAKeyPair creates a new ECDSA private/public key pair in PEM format.
func GenerateECDSAKeyPair(curve string) (privPEM []byte, pubPEM []byte, err error) {
	var c elliptic.Curve
	switch strings.ToUpper(curve) {
	case "P384", "P-384":
		c = elliptic.P384()
	case "P521", "P-521":
		c = elliptic.P521()
	default:
		c = elliptic.P256()
	}

	key, err := ecdsa.GenerateKey(c, rand.Reader)
	if err != nil {
		return nil, nil, fmt.Errorf("failed generating ECDSA key: %w", err)
	}

	privDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		return nil, nil, fmt.Errorf("failed marshaling EC private key: %w", err)
	}
	privBlock := &pem.Block{
		Type:  "EC PRIVATE KEY",
		Bytes: privDER,
	}
	privPEM = pem.EncodeToMemory(privBlock)

	pubDER, err := x509.MarshalPKIXPublicKey(&key.PublicKey)
	if err != nil {
		return nil, nil, fmt.Errorf("failed marshaling ECDSA public key: %w", err)
	}
	pubBlock := &pem.Block{
		Type:  "PUBLIC KEY",
		Bytes: pubDER,
	}
	pubPEM = pem.EncodeToMemory(pubBlock)

	return privPEM, pubPEM, nil
}

// GenerateEd25519KeyPair creates a new Ed25519 private/public key pair in PEM format.
func GenerateEd25519KeyPair() (privPEM []byte, pubPEM []byte, err error) {
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		return nil, nil, fmt.Errorf("failed generating Ed25519 key: %w", err)
	}

	privDER, err := x509.MarshalPKCS8PrivateKey(priv)
	if err != nil {
		return nil, nil, fmt.Errorf("failed marshaling Ed25519 private key: %w", err)
	}
	privBlock := &pem.Block{
		Type:  "PRIVATE KEY",
		Bytes: privDER,
	}
	privPEM = pem.EncodeToMemory(privBlock)

	pubDER, err := x509.MarshalPKIXPublicKey(pub)
	if err != nil {
		return nil, nil, fmt.Errorf("failed marshaling Ed25519 public key: %w", err)
	}
	pubBlock := &pem.Block{
		Type:  "PUBLIC KEY",
		Bytes: pubDER,
	}
	pubPEM = pem.EncodeToMemory(pubBlock)

	return privPEM, pubPEM, nil
}

// GenerateSelfSignedCertificate creates a self-signed X.509 certificate for a given key.
func GenerateSelfSignedCertificate(privKey crypto.PrivateKey, pubKey crypto.PublicKey, commonName string, validityDays int) (certPEM []byte, err error) {
	if validityDays <= 0 {
		validityDays = 365
	}
	if commonName == "" {
		commonName = "Flow Pipeline Signer"
	}

	serialNumberLimit := new(big.Int).Lsh(big.NewInt(1), 128)
	serialNumber, err := rand.Int(rand.Reader, serialNumberLimit)
	if err != nil {
		return nil, fmt.Errorf("failed generating serial number: %w", err)
	}

	template := x509.Certificate{
		SerialNumber: serialNumber,
		Subject: pkix.Name{
			CommonName:   commonName,
			Organization: []string{"Flow"},
		},
		NotBefore:             time.Now().Add(-1 * time.Hour),
		NotAfter:              time.Now().AddDate(0, 0, validityDays),
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment | x509.KeyUsageCertSign,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageCodeSigning},
		BasicConstraintsValid: true,
		IsCA:                  true,
	}

	certDER, err := x509.CreateCertificate(rand.Reader, &template, &template, pubKey, privKey)
	if err != nil {
		return nil, fmt.Errorf("failed creating certificate: %w", err)
	}

	block := &pem.Block{
		Type:  "CERTIFICATE",
		Bytes: certDER,
	}
	return pem.EncodeToMemory(block), nil
}

// GenerateOpenPGPKeyPair creates a new OpenPGP RSA keypair and returns ASCII-armored private and public keys.
func GenerateOpenPGPKeyPair(name, comment, email string) (privArmored, pubArmored []byte, err error) {
	key, err := pgpcrypto.GenerateKey(strings.TrimSpace(name), strings.TrimSpace(email), "rsa", 2048)
	if err != nil {
		return nil, nil, fmt.Errorf("failed creating OpenPGP key: %w", err)
	}

	privStr, err := key.Armor()
	if err != nil {
		return nil, nil, fmt.Errorf("failed armoring OpenPGP private key: %w", err)
	}

	pubStr, err := key.GetArmoredPublicKey()
	if err != nil {
		return nil, nil, fmt.Errorf("failed armoring OpenPGP public key: %w", err)
	}

	return []byte(privStr), []byte(pubStr), nil
}

// ParsePrivateKeyPEM parses a PEM-encoded private key (PKCS#1, PKCS#8, or EC).
func ParsePrivateKeyPEM(keyPEM []byte) (crypto.PrivateKey, error) {
	block, _ := pem.Decode(keyPEM)
	if block == nil {
		return nil, errors.New("no PEM data found in private key")
	}

	switch block.Type {
	case "RSA PRIVATE KEY":
		return x509.ParsePKCS1PrivateKey(block.Bytes)
	case "EC PRIVATE KEY":
		return x509.ParseECPrivateKey(block.Bytes)
	case "PRIVATE KEY":
		return x509.ParsePKCS8PrivateKey(block.Bytes)
	default:
		// Attempt PKCS#8 fallback
		if key, err := x509.ParsePKCS8PrivateKey(block.Bytes); err == nil {
			return key, nil
		}
		if key, err := x509.ParsePKCS1PrivateKey(block.Bytes); err == nil {
			return key, nil
		}
		if key, err := x509.ParseECPrivateKey(block.Bytes); err == nil {
			return key, nil
		}
		return nil, fmt.Errorf("unsupported PEM block type: %s", block.Type)
	}
}

// ParseCertificatePEM parses a PEM-encoded X.509 certificate.
func ParseCertificatePEM(certPEM []byte) (*x509.Certificate, error) {
	block, _ := pem.Decode(certPEM)
	if block == nil {
		// Try parsing raw DER if not PEM
		return x509.ParseCertificate(certPEM)
	}
	return x509.ParseCertificate(block.Bytes)
}

// SignOpenSSL signs arbitrary data using an RSA, ECDSA, or Ed25519 PEM private key.
func SignOpenSSL(data []byte, privKeyPEM []byte) (sigBytes []byte, keyType string, err error) {
	privKey, err := ParsePrivateKeyPEM(privKeyPEM)
	if err != nil {
		return nil, "", fmt.Errorf("failed parsing private key: %w", err)
	}

	h := sha256.Sum256(data)

	switch k := privKey.(type) {
	case *rsa.PrivateKey:
		sig, err := rsa.SignPKCS1v15(rand.Reader, k, crypto.SHA256, h[:])
		if err != nil {
			return nil, "", fmt.Errorf("RSA signing failed: %w", err)
		}
		return sig, "RSA-SHA256", nil
	case *ecdsa.PrivateKey:
		sig, err := ecdsa.SignASN1(rand.Reader, k, h[:])
		if err != nil {
			return nil, "", fmt.Errorf("ECDSA signing failed: %w", err)
		}
		return sig, "ECDSA-SHA256", nil
	case ed25519.PrivateKey:
		sig := ed25519.Sign(k, data)
		return sig, "ED25519", nil
	default:
		return nil, "", fmt.Errorf("unsupported private key algorithm: %T", privKey)
	}
}

// SignOpenPGP signs arbitrary data using an armored OpenPGP private key.
func SignOpenPGP(data []byte, privKeyArmored []byte, passphrase string) (sigArmored []byte, err error) {
	key, err := pgpcrypto.NewKeyFromArmored(string(privKeyArmored))
	if err != nil {
		return nil, fmt.Errorf("failed reading OpenPGP private key: %w", err)
	}

	isLocked, err := key.IsLocked()
	if err != nil {
		return nil, fmt.Errorf("failed checking OpenPGP key lock status: %w", err)
	}

	if isLocked {
		if passphrase == "" {
			return nil, errors.New("OpenPGP private key is encrypted but no passphrase was provided")
		}
		unlockedKey, err := key.Unlock([]byte(passphrase))
		if err != nil {
			return nil, fmt.Errorf("failed decrypting OpenPGP private key: %w", err)
		}
		key = unlockedKey
	}

	keyRing, err := pgpcrypto.NewKeyRing(key)
	if err != nil {
		return nil, fmt.Errorf("failed creating OpenPGP keyring: %w", err)
	}

	sig, err := keyRing.SignDetached(pgpcrypto.NewPlainMessage(data))
	if err != nil {
		return nil, fmt.Errorf("OpenPGP signing failed: %w", err)
	}

	armoredSig, err := sig.GetArmored()
	if err != nil {
		return nil, fmt.Errorf("failed armoring OpenPGP signature: %w", err)
	}

	return []byte(armoredSig), nil
}

// SignPKCS7Detached generates a Windows CMS / PKCS#7 detached signature.
func SignPKCS7Detached(data []byte, certPEM []byte, privKeyPEM []byte) (p7DER []byte, err error) {
	cert, err := ParseCertificatePEM(certPEM)
	if err != nil {
		return nil, fmt.Errorf("failed parsing certificate: %w", err)
	}
	privKey, err := ParsePrivateKeyPEM(privKeyPEM)
	if err != nil {
		return nil, fmt.Errorf("failed parsing private key: %w", err)
	}
	return SignPKCS7(data, cert, privKey)
}

// SignData is a high-level signer supporting 'openssl', 'pgp', and 'pkcs7'.
func SignData(data []byte, sigType string, privKeyBytes []byte, certBytes []byte, passphrase string) (sigBytes []byte, format string, err error) {
	switch strings.ToLower(strings.TrimSpace(sigType)) {
	case "pgp", "gpg", "openpgp":
		sig, err := SignOpenPGP(data, privKeyBytes, passphrase)
		if err != nil {
			return nil, "", err
		}
		return sig, "PGP", nil

	case "pkcs7", "cms", "authenticode", "windows":
		if len(certBytes) == 0 {
			return nil, "", errors.New("certificate (-cert) is required for PKCS#7 signing")
		}
		sig, err := SignPKCS7Detached(data, certBytes, privKeyBytes)
		if err != nil {
			return nil, "", err
		}
		return sig, "PKCS7", nil

	case "openssl", "pki", "pem", "":
		sig, alg, err := SignOpenSSL(data, privKeyBytes)
		if err != nil {
			return nil, "", err
		}
		return sig, "OPENSSL-" + alg, nil

	default:
		return nil, "", fmt.Errorf("unknown signature format: %q (expected openssl, pgp, or pkcs7)", sigType)
	}
}
