package flowcrypto

import (
	"bytes"
	"strings"
	"testing"

	pgpcrypto "github.com/ProtonMail/gopenpgp/v2/crypto"
)

func TestGenerateSecureKey(t *testing.T) {
	key1, err := GenerateSecureKey(32)
	if err != nil {
		t.Fatalf("GenerateSecureKey failed: %v", err)
	}
	if len(key1) == 0 {
		t.Fatal("GenerateSecureKey returned empty string")
	}

	key2, err := GenerateSecureKey(32)
	if err != nil {
		t.Fatalf("GenerateSecureKey failed: %v", err)
	}
	if key1 == key2 {
		t.Fatal("expected different random keys, got identical values")
	}
}

func TestEncryptDecryptBytesRoundTrip(t *testing.T) {
	key := "TestPassphrase123!@#"
	plaintext := []byte("<pipeline id=\"test\"><flow><log msg=\"hello\"/></flow></pipeline>")

	encrypted, err := EncryptBytes(plaintext, key)
	if err != nil {
		t.Fatalf("EncryptBytes failed: %v", err)
	}

	if bytes.Equal(encrypted, plaintext) {
		t.Fatal("encrypted data matches plaintext")
	}

	decrypted, err := DecryptBytes(encrypted, key)
	if err != nil {
		t.Fatalf("DecryptBytes failed: %v", err)
	}

	if !bytes.Equal(decrypted, plaintext) {
		t.Fatalf("decrypted data %q != expected %q", string(decrypted), string(plaintext))
	}
}

func TestEncryptArmoredRoundTrip(t *testing.T) {
	key := "SuperSecretKey999"
	plaintext := []byte("Sensitive Database Connection String / Options")

	armored, err := EncryptArmored(plaintext, key)
	if err != nil {
		t.Fatalf("EncryptArmored failed: %v", err)
	}

	if !strings.HasPrefix(armored, ArmoredPrefixV1) {
		t.Fatalf("expected prefix %q, got: %s", ArmoredPrefixV1, armored)
	}

	if !IsEncryptedPayload([]byte(armored)) {
		t.Fatal("IsEncryptedPayload returned false for armored string")
	}

	decrypted, err := DecryptArmored(armored, key)
	if err != nil {
		t.Fatalf("DecryptArmored failed: %v", err)
	}

	if string(decrypted) != string(plaintext) {
		t.Fatalf("decrypted %q != expected %q", string(decrypted), string(plaintext))
	}
}

func TestDecryptWithInvalidKeyFails(t *testing.T) {
	correctKey := "GoodKey123"
	wrongKey := "BadKey456"
	plaintext := []byte("Super secure content")

	armored, err := EncryptArmored(plaintext, correctKey)
	if err != nil {
		t.Fatalf("EncryptArmored failed: %v", err)
	}

	_, err = DecryptArmored(armored, wrongKey)
	if err == nil {
		t.Fatal("expected decryption error with wrong key, got nil")
	}
	if err != ErrAuthFailed {
		t.Fatalf("expected ErrAuthFailed, got: %v", err)
	}
}

func TestDecryptTamperedCiphertextFails(t *testing.T) {
	key := "TamperTestKey"
	plaintext := []byte("Important Payload")

	encrypted, err := EncryptBytes(plaintext, key)
	if err != nil {
		t.Fatalf("EncryptBytes failed: %v", err)
	}

	// Flip a bit in the ciphertext / tag portion
	tampered := make([]byte, len(encrypted))
	copy(tampered, encrypted)
	tampered[len(tampered)-1] ^= 0xFF

	_, err = DecryptBytes(tampered, key)
	if err == nil {
		t.Fatal("expected decryption error for tampered ciphertext, got nil")
	}
}

func TestDecryptEmptyKeyError(t *testing.T) {
	_, err := EncryptBytes([]byte("data"), "")
	if err != ErrEmptyKey {
		t.Fatalf("expected ErrEmptyKey, got: %v", err)
	}

	_, err = DecryptBytes([]byte("data"), "")
	if err != ErrEmptyKey {
		t.Fatalf("expected ErrEmptyKey, got: %v", err)
	}
}

func TestDecryptAuto(t *testing.T) {
	key := "AutoKey"
	plainXML := []byte("<root>plain</root>")

	// Case 1: Plaintext with requireEncrypted = false
	res, err := DecryptAuto(plainXML, key, false)
	if err != nil {
		t.Fatalf("DecryptAuto plaintext returned error: %v", err)
	}
	if !bytes.Equal(res, plainXML) {
		t.Fatalf("expected %q, got %q", string(plainXML), string(res))
	}

	// Case 2: Plaintext with requireEncrypted = true -> error
	_, err = DecryptAuto(plainXML, key, true)
	if err != ErrNotEncrypted {
		t.Fatalf("expected ErrNotEncrypted, got: %v", err)
	}

	// Case 3: Encrypted payload -> decrypts successfully
	armored, err := EncryptArmored(plainXML, key)
	if err != nil {
		t.Fatalf("EncryptArmored failed: %v", err)
	}

	resEnc, err := DecryptAuto([]byte(armored), key, true)
	if err != nil {
		t.Fatalf("DecryptAuto encrypted failed: %v", err)
	}
	if !bytes.Equal(resEnc, plainXML) {
		t.Fatalf("expected %q, got %q", string(plainXML), string(resEnc))
	}
}

func TestSignAndVerifyRSA(t *testing.T) {
	privPEM, pubPEM, err := GenerateRSAKeyPair(2048)
	if err != nil {
		t.Fatalf("GenerateRSAKeyPair failed: %v", err)
	}

	data := []byte("<pipeline id=\"rsa_test\"><flow/></pipeline>")
	sig, keyType, err := SignOpenSSL(data, privPEM)
	if err != nil {
		t.Fatalf("SignOpenSSL RSA failed: %v", err)
	}
	if keyType != "RSA-SHA256" {
		t.Fatalf("expected RSA-SHA256, got: %s", keyType)
	}

	res, err := VerifyOpenSSL(data, sig, pubPEM)
	if err != nil {
		t.Fatalf("VerifyOpenSSL failed: %v", err)
	}
	if !res.Valid {
		t.Fatal("expected verification result to be valid")
	}

	// Verify tamper fails
	tampered := []byte("<pipeline id=\"tampered\"><flow/></pipeline>")
	_, err = VerifyOpenSSL(tampered, sig, pubPEM)
	if err == nil {
		t.Fatal("expected verification failure for tampered data, got nil")
	}
}

func TestSignAndVerifyECDSA(t *testing.T) {
	privPEM, pubPEM, err := GenerateECDSAKeyPair("P-256")
	if err != nil {
		t.Fatalf("GenerateECDSAKeyPair failed: %v", err)
	}

	data := []byte("ECDSA signed data block")
	sig, keyType, err := SignOpenSSL(data, privPEM)
	if err != nil {
		t.Fatalf("SignOpenSSL ECDSA failed: %v", err)
	}
	if keyType != "ECDSA-SHA256" {
		t.Fatalf("expected ECDSA-SHA256, got: %s", keyType)
	}

	res, err := VerifyOpenSSL(data, sig, pubPEM)
	if err != nil {
		t.Fatalf("VerifyOpenSSL ECDSA failed: %v", err)
	}
	if !res.Valid {
		t.Fatal("expected valid ECDSA signature")
	}
}

func TestSignAndVerifyEd25519(t *testing.T) {
	privPEM, pubPEM, err := GenerateEd25519KeyPair()
	if err != nil {
		t.Fatalf("GenerateEd25519KeyPair failed: %v", err)
	}

	data := []byte("Ed25519 payload to verify")
	sig, keyType, err := SignOpenSSL(data, privPEM)
	if err != nil {
		t.Fatalf("SignOpenSSL Ed25519 failed: %v", err)
	}
	if keyType != "ED25519" {
		t.Fatalf("expected ED25519, got: %s", keyType)
	}

	res, err := VerifyOpenSSL(data, sig, pubPEM)
	if err != nil {
		t.Fatalf("VerifyOpenSSL Ed25519 failed: %v", err)
	}
	if !res.Valid {
		t.Fatal("expected valid Ed25519 signature")
	}
}

func TestSignAndVerifySelfSignedCertificate(t *testing.T) {
	privPEM, pubPEM, err := GenerateRSAKeyPair(2048)
	if err != nil {
		t.Fatalf("GenerateRSAKeyPair failed: %v", err)
	}
	privKey, _ := ParsePrivateKeyPEM(privPEM)
	pubKey, _, _ := ParsePublicKeyOrCertPEM(pubPEM)

	certPEM, err := GenerateSelfSignedCertificate(privKey, pubKey, "Flow Code Signer", 30)
	if err != nil {
		t.Fatalf("GenerateSelfSignedCertificate failed: %v", err)
	}

	data := []byte("Pipeline verified by X.509 Certificate")
	sig, _, err := SignOpenSSL(data, privPEM)
	if err != nil {
		t.Fatalf("SignOpenSSL failed: %v", err)
	}

	res, err := VerifyOpenSSL(data, sig, certPEM)
	if err != nil {
		t.Fatalf("VerifyOpenSSL with certificate failed: %v", err)
	}
	if res.SignerInfo != "Flow Code Signer" {
		t.Fatalf("expected signer 'Flow Code Signer', got: %s", res.SignerInfo)
	}
}

func TestVerifyOpenSSLWithCAAcrossEnvelopeFormats(t *testing.T) {
	privPEM, pubPEM, err := GenerateRSAKeyPair(2048)
	if err != nil {
		t.Fatalf("GenerateRSAKeyPair failed: %v", err)
	}
	privKey, err := ParsePrivateKeyPEM(privPEM)
	if err != nil {
		t.Fatalf("ParsePrivateKeyPEM failed: %v", err)
	}
	pubKey, _, err := ParsePublicKeyOrCertPEM(pubPEM)
	if err != nil {
		t.Fatalf("ParsePublicKeyOrCertPEM failed: %v", err)
	}
	certPEM, err := GenerateSelfSignedCertificate(privKey, pubKey, "CA signer", 30)
	if err != nil {
		t.Fatalf("GenerateSelfSignedCertificate failed: %v", err)
	}

	data := []byte("verify OpenSSL signature with CA")
	sig, _, err := SignOpenSSL(data, privPEM)
	if err != nil {
		t.Fatalf("SignOpenSSL failed: %v", err)
	}

	if _, err := VerifyOpenSSLWithCA(data, sig, certPEM, certPEM); err != nil {
		t.Fatalf("VerifyOpenSSLWithCA failed: %v", err)
	}
	if _, err := VerifyOpenSSLWithCA(data, sig, pubPEM, certPEM); err == nil {
		t.Fatal("expected CA validation to reject a public key without a signer certificate")
	}
	if _, err := VerifyOpenSSLWithCA(data, sig, certPEM, []byte("invalid CA")); err == nil {
		t.Fatal("expected CA validation to reject an invalid CA certificate")
	}

	flowsig := []byte(WrapSignatureOnly("OPENSSL-RSA-SHA256", sig))
	if _, err := VerifyAutoWithCA(data, flowsig, certPEM, certPEM); err != nil {
		t.Fatalf("VerifyAutoWithCA FLOWSIG failed: %v", err)
	}
	flowsigned := []byte(WrapSignedPayload("OPENSSL", sig, data))
	if _, _, err := VerifyPayloadAutoWithCA(flowsigned, certPEM, certPEM); err != nil {
		t.Fatalf("VerifyPayloadAutoWithCA FLOWSIGNED failed: %v", err)
	}
}

func TestSignAndVerifyPKCS7Detached(t *testing.T) {
	privPEM, pubPEM, err := GenerateRSAKeyPair(2048)
	if err != nil {
		t.Fatalf("GenerateRSAKeyPair failed: %v", err)
	}
	privKey, _ := ParsePrivateKeyPEM(privPEM)
	pubKey, _, _ := ParsePublicKeyOrCertPEM(pubPEM)

	certPEM, err := GenerateSelfSignedCertificate(privKey, pubKey, "Windows Authenticode Signer", 30)
	if err != nil {
		t.Fatalf("GenerateSelfSignedCertificate failed: %v", err)
	}

	data := []byte("<pipeline name=\"windows_test\"><flow/></pipeline>")
	p7DER, err := SignPKCS7Detached(data, certPEM, privPEM)
	if err != nil {
		t.Fatalf("SignPKCS7Detached failed: %v", err)
	}

	res, err := VerifyPKCS7Detached(data, p7DER, certPEM)
	if err != nil {
		t.Fatalf("VerifyPKCS7Detached failed: %v", err)
	}
	if res.SignerInfo != "Windows Authenticode Signer" {
		t.Fatalf("expected signer 'Windows Authenticode Signer', got: %s", res.SignerInfo)
	}

	// Tampered data must fail
	tampered := []byte("<pipeline name=\"windows_test\"><flow/><hacked/></pipeline>")
	_, err = VerifyPKCS7Detached(tampered, p7DER, certPEM)
	if err == nil {
		t.Fatal("expected PKCS#7 verification failure for tampered data, got nil")
	}
}

func TestSignAndVerifyOpenPGP(t *testing.T) {
	privArmored, pubArmored, err := GenerateOpenPGPKeyPair("Flow Operator", "Ops", "ops@flow.local")
	if err != nil {
		t.Fatalf("GenerateOpenPGPKeyPair failed: %v", err)
	}

	data := []byte("OpenPGP signed pipeline XML configuration")
	sigArmored, err := SignOpenPGP(data, privArmored, "")
	if err != nil {
		t.Fatalf("SignOpenPGP failed: %v", err)
	}

	res, err := VerifyOpenPGP(data, sigArmored, pubArmored)
	if err != nil {
		t.Fatalf("VerifyOpenPGP failed: %v", err)
	}
	if !strings.Contains(res.SignerInfo, "Flow Operator") {
		t.Fatalf("expected signer to contain 'Flow Operator', got: %s", res.SignerInfo)
	}

	// Tampered data must fail verification
	tampered := []byte("OpenPGP signed pipeline XML configuration tampered!")
	if _, err := VerifyOpenPGP(tampered, sigArmored, pubArmored); err == nil {
		t.Fatal("expected OpenPGP verification to fail for tampered data, got nil")
	}

	// Test passphrase-protected private key
	key, err := pgpcrypto.NewKeyFromArmored(string(privArmored))
	if err != nil {
		t.Fatalf("NewKeyFromArmored failed: %v", err)
	}
	lockedKey, err := key.Lock([]byte("SecretPassphrase123!"))
	if err != nil {
		t.Fatalf("Locking key failed: %v", err)
	}
	lockedPrivArmored, err := lockedKey.Armor()
	if err != nil {
		t.Fatalf("Armoring locked key failed: %v", err)
	}

	// Signing without passphrase must fail
	if _, err := SignOpenPGP(data, []byte(lockedPrivArmored), ""); err == nil {
		t.Fatal("expected error signing with locked key without passphrase, got nil")
	}

	// Signing with wrong passphrase must fail
	if _, err := SignOpenPGP(data, []byte(lockedPrivArmored), "WrongPassword"); err == nil {
		t.Fatal("expected error signing with wrong passphrase, got nil")
	}

	// Signing with correct passphrase must succeed
	lockedSigArmored, err := SignOpenPGP(data, []byte(lockedPrivArmored), "SecretPassphrase123!")
	if err != nil {
		t.Fatalf("SignOpenPGP with correct passphrase failed: %v", err)
	}
	lockedRes, err := VerifyOpenPGP(data, lockedSigArmored, pubArmored)
	if err != nil {
		t.Fatalf("VerifyOpenPGP for locked key signature failed: %v", err)
	}
	if !lockedRes.Valid {
		t.Fatal("expected valid signature verification for locked key")
	}
}

func TestFlowSignedEnvelopeAndCombinedEncryption(t *testing.T) {
	privPEM, pubPEM, err := GenerateRSAKeyPair(2048)
	if err != nil {
		t.Fatalf("GenerateRSAKeyPair failed: %v", err)
	}

	plaintext := []byte("<pipeline id=\"secret_pipeline\"><flow/></pipeline>")
	sig, _, err := SignOpenSSL(plaintext, privPEM)
	if err != nil {
		t.Fatalf("SignOpenSSL failed: %v", err)
	}

	// 1. Wrap in FLOWSIGNED:v1:
	envelope := WrapSignedPayload("OPENSSL", sig, plaintext)
	if !IsSignedPayload([]byte(envelope)) {
		t.Fatal("IsSignedPayload returned false for wrapped envelope")
	}

	unwrappedContent, res, err := VerifyPayloadAuto([]byte(envelope), pubPEM)
	if err != nil {
		t.Fatalf("VerifyPayloadAuto failed: %v", err)
	}
	if !bytes.Equal(unwrappedContent, plaintext) {
		t.Fatalf("unwrapped %q != plaintext %q", string(unwrappedContent), string(plaintext))
	}
	if !res.Valid {
		t.Fatal("expected valid verification result")
	}

	// 2. Combined: Encrypt, then Sign
	key := "AESKeySecret123!"
	encryptedArmored, err := EncryptArmored(plaintext, key)
	if err != nil {
		t.Fatalf("EncryptArmored failed: %v", err)
	}

	encBytes := []byte(encryptedArmored)
	sigOnEnc, _, err := SignOpenSSL(encBytes, privPEM)
	if err != nil {
		t.Fatalf("SignOpenSSL on ciphertext failed: %v", err)
	}

	signedCiphertextEnvelope := WrapSignedPayload("OPENSSL", sigOnEnc, encBytes)

	// Step 1: Verify
	verifiedEncBytes, _, err := VerifyPayloadAuto([]byte(signedCiphertextEnvelope), pubPEM)
	if err != nil {
		t.Fatalf("VerifyPayloadAuto on signed ciphertext failed: %v", err)
	}

	// Step 2: Decrypt
	decrypted, err := DecryptAuto(verifiedEncBytes, key, true)
	if err != nil {
		t.Fatalf("DecryptAuto failed: %v", err)
	}
	if !bytes.Equal(decrypted, plaintext) {
		t.Fatalf("decrypted %q != plaintext %q", string(decrypted), string(plaintext))
	}
}

func TestPKCS7RequiresTrustAnchor(t *testing.T) {
	privPEM, pubPEM, err := GenerateRSAKeyPair(2048)
	if err != nil {
		t.Fatalf("GenerateRSAKeyPair failed: %v", err)
	}
	privKey, _ := ParsePrivateKeyPEM(privPEM)
	pubKey, _, _ := ParsePublicKeyOrCertPEM(pubPEM)

	certPEM, err := GenerateSelfSignedCertificate(privKey, pubKey, "Untrusted Signer", 30)
	if err != nil {
		t.Fatalf("GenerateSelfSignedCertificate failed: %v", err)
	}

	data := []byte("<pipeline name=\"untrusted_test\"><flow/></pipeline>")
	p7DER, err := SignPKCS7Detached(data, certPEM, privPEM)
	if err != nil {
		t.Fatalf("SignPKCS7Detached failed: %v", err)
	}

	// Verification without trusted signer cert or CA must fail
	_, err = VerifyPKCS7WithCA(data, p7DER, nil, nil)
	if err == nil {
		t.Fatal("expected failure when verifying PKCS#7 without trusted signer or root CA, got nil")
	}
	if !strings.Contains(err.Error(), "requires a trusted signer certificate or root CA") {
		t.Fatalf("expected trust anchor error, got: %v", err)
	}

	// VerifyPKCS7DetachedWithCA without certs must fail
	_, err = VerifyPKCS7DetachedWithCA(data, p7DER, nil, nil)
	if err == nil {
		t.Fatal("expected failure when verifying PKCS#7 detached without certs, got nil")
	}
}

