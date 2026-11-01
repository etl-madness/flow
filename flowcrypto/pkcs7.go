package flowcrypto

import (
	"bytes"
	"crypto"
	"crypto/ecdsa"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/sha512"
	"crypto/subtle"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/asn1"
	"errors"
	"fmt"
	"math/big"
	"time"
)

var (
	oidSignedData       = asn1.ObjectIdentifier{1, 2, 840, 113549, 1, 7, 2}
	oidData             = asn1.ObjectIdentifier{1, 2, 840, 113549, 1, 7, 1}
	oidSHA256           = asn1.ObjectIdentifier{2, 16, 840, 1, 101, 3, 4, 2, 1}
	oidSHA512           = asn1.ObjectIdentifier{2, 16, 840, 1, 101, 3, 4, 2, 3}
	oidRSAWithSHA256     = asn1.ObjectIdentifier{1, 2, 840, 113549, 1, 1, 11}
	oidECDSAWithSHA256   = asn1.ObjectIdentifier{1, 2, 840, 10045, 4, 3, 2}
	oidContentType      = asn1.ObjectIdentifier{1, 2, 840, 113549, 1, 9, 3}
	oidMessageDigest    = asn1.ObjectIdentifier{1, 2, 840, 113549, 1, 9, 4}
	oidSigningTime      = asn1.ObjectIdentifier{1, 2, 840, 113549, 1, 9, 5}
)

type pkcs7ContentInfo struct {
	ContentType asn1.ObjectIdentifier
	Content     asn1.RawValue `asn1:"explicit,tag:0"`
}

type pkcs7Attribute struct {
	Type  asn1.ObjectIdentifier
	Value asn1.RawValue `asn1:"set"`
}

type pkcs7IssuerAndSerialNumber struct {
	IssuerName   asn1.RawValue
	SerialNumber *big.Int
}

type pkcs7SignerInfo struct {
	Version            int
	IssuerAndSerial    pkcs7IssuerAndSerialNumber
	DigestAlgorithm    pkix.AlgorithmIdentifier
	SignedAttributes   []pkcs7Attribute `asn1:"optional,tag:0,set"`
	SignatureAlgorithm pkix.AlgorithmIdentifier
	Signature          []byte
}

type pkcs7EncapsulatedContentInfo struct {
	ContentType asn1.ObjectIdentifier
	Content     asn1.RawValue `asn1:"optional,explicit,tag:0"`
}

type pkcs7SignedData struct {
	Version          int
	DigestAlgorithms []pkix.AlgorithmIdentifier `asn1:"set"`
	ContentInfo      pkcs7EncapsulatedContentInfo
	Certificates     asn1.RawValue `asn1:"optional,tag:0"`
	SignerInfos      []pkcs7SignerInfo `asn1:"set"`
}

// SignPKCS7 creates a cross-platform PKCS#7 / CMS (RFC 2315 / RFC 5652) detached SignedData structure.
func SignPKCS7(data []byte, cert *x509.Certificate, privKey crypto.PrivateKey) ([]byte, error) {
	if cert == nil {
		return nil, errors.New("certificate is required for PKCS#7 signing")
	}
	if privKey == nil {
		return nil, errors.New("private key is required for PKCS#7 signing")
	}

	dataHash := sha256.Sum256(data)

	encContentType, err := asn1.Marshal(oidData)
	if err != nil {
		return nil, fmt.Errorf("failed marshaling content type: %w", err)
	}
	encSigningTime, err := asn1.Marshal(time.Now().UTC())
	if err != nil {
		return nil, fmt.Errorf("failed marshaling signing time: %w", err)
	}
	encDigest, err := asn1.Marshal(dataHash[:])
	if err != nil {
		return nil, fmt.Errorf("failed marshaling digest: %w", err)
	}

	attrs := []pkcs7Attribute{
		{Type: oidContentType, Value: asn1.RawValue{Class: asn1.ClassUniversal, Tag: asn1.TagSet, IsCompound: true, Bytes: encContentType}},
		{Type: oidSigningTime, Value: asn1.RawValue{Class: asn1.ClassUniversal, Tag: asn1.TagSet, IsCompound: true, Bytes: encSigningTime}},
		{Type: oidMessageDigest, Value: asn1.RawValue{Class: asn1.ClassUniversal, Tag: asn1.TagSet, IsCompound: true, Bytes: encDigest}},
	}

	rawAttrs, err := asn1.MarshalWithParams(attrs, "set")
	if err != nil {
		return nil, fmt.Errorf("failed marshaling signed attributes: %w", err)
	}

	attrHash := sha256.Sum256(rawAttrs)

	var sig []byte
	var sigAlgOID asn1.ObjectIdentifier

	switch k := privKey.(type) {
	case *rsa.PrivateKey:
		sigAlgOID = oidRSAWithSHA256
		sig, err = rsa.SignPKCS1v15(rand.Reader, k, crypto.SHA256, attrHash[:])
		if err != nil {
			return nil, fmt.Errorf("RSA signing failed: %w", err)
		}
	case *ecdsa.PrivateKey:
		sigAlgOID = oidECDSAWithSHA256
		sig, err = ecdsa.SignASN1(rand.Reader, k, attrHash[:])
		if err != nil {
			return nil, fmt.Errorf("ECDSA signing failed: %w", err)
		}
	default:
		return nil, fmt.Errorf("unsupported private key type for PKCS#7: %T (expected *rsa.PrivateKey or *ecdsa.PrivateKey)", privKey)
	}

	signerInfo := pkcs7SignerInfo{
		Version: 1,
		IssuerAndSerial: pkcs7IssuerAndSerialNumber{
			IssuerName:   asn1.RawValue{FullBytes: cert.RawIssuer},
			SerialNumber: cert.SerialNumber,
		},
		DigestAlgorithm:    pkix.AlgorithmIdentifier{Algorithm: oidSHA256},
		SignedAttributes:   attrs,
		SignatureAlgorithm: pkix.AlgorithmIdentifier{Algorithm: sigAlgOID},
		Signature:          sig,
	}

	sd := pkcs7SignedData{
		Version:          1,
		DigestAlgorithms: []pkix.AlgorithmIdentifier{{Algorithm: oidSHA256}},
		ContentInfo: pkcs7EncapsulatedContentInfo{
			ContentType: oidData,
		},
		Certificates: asn1.RawValue{Class: asn1.ClassContextSpecific, Tag: 0, IsCompound: true, Bytes: cert.Raw},
		SignerInfos:  []pkcs7SignerInfo{signerInfo},
	}

	sdBytes, err := asn1.Marshal(sd)
	if err != nil {
		return nil, fmt.Errorf("failed marshaling SignedData: %w", err)
	}

	ci := pkcs7ContentInfo{
		ContentType: oidSignedData,
		Content:     asn1.RawValue{Class: asn1.ClassContextSpecific, Tag: 0, IsCompound: true, Bytes: sdBytes},
	}

	return asn1.Marshal(ci)
}

// VerifyPKCS7 parses and verifies a detached PKCS#7 / CMS SignedData structure against the provided data in pure Go.
// Returns the verified signer certificate or an error.
func VerifyPKCS7(data []byte, p7Bytes []byte, rootCert *x509.Certificate) (*x509.Certificate, error) {
	return VerifyPKCS7WithCA(data, p7Bytes, nil, rootCert)
}

// VerifyPKCS7WithCA parses and verifies a detached PKCS#7 / CMS SignedData structure against the provided data,
// optional signer certificate, and optional root CA certificate.
func VerifyPKCS7WithCA(data []byte, p7Bytes []byte, signerCert *x509.Certificate, rootCert *x509.Certificate) (*x509.Certificate, error) {
	if len(p7Bytes) == 0 {
		return nil, errors.New("empty PKCS#7 payload")
	}

	var ci pkcs7ContentInfo
	rest, err := asn1.Unmarshal(p7Bytes, &ci)
	if err != nil || len(rest) > 0 {
		return nil, fmt.Errorf("failed unmarshaling PKCS#7 ContentInfo: %w", err)
	}

	if !ci.ContentType.Equal(oidSignedData) {
		return nil, fmt.Errorf("unsupported PKCS#7 contentType: %s (expected SignedData)", ci.ContentType)
	}

	var sd pkcs7SignedData
	_, err = asn1.Unmarshal(ci.Content.Bytes, &sd)
	if err != nil {
		return nil, fmt.Errorf("failed unmarshaling PKCS#7 SignedData: %w", err)
	}

	if len(sd.SignerInfos) == 0 {
		return nil, errors.New("no SignerInfos present in PKCS#7 signature")
	}

	var certs []*x509.Certificate
	if len(sd.Certificates.Bytes) > 0 {
		certs, err = x509.ParseCertificates(sd.Certificates.Bytes)
		if err != nil {
			return nil, fmt.Errorf("failed parsing certificates embedded in PKCS#7: %w", err)
		}
	}

	if signerCert != nil {
		found := false
		for _, c := range certs {
			if c.Equal(signerCert) {
				found = true
				break
			}
		}
		if !found {
			certs = append(certs, signerCert)
		}
	}

	if signerCert == nil && rootCert == nil {
		return nil, errors.New("PKCS#7 signature verification requires a trusted signer certificate or root CA")
	}

	if len(certs) == 0 && rootCert != nil {
		certs = append(certs, rootCert)
	}

	if len(certs) == 0 {
		return nil, errors.New("no certificates found in PKCS#7 structure and no trusted cert provided")
	}

	signer := sd.SignerInfos[0]

	// Find the signing certificate matching Issuer and SerialNumber
	var matchedCert *x509.Certificate
	for _, c := range certs {
		if c.SerialNumber.Cmp(signer.IssuerAndSerial.SerialNumber) == 0 {
			if len(signer.IssuerAndSerial.IssuerName.FullBytes) == 0 ||
				bytes.Equal(c.RawIssuer, signer.IssuerAndSerial.IssuerName.FullBytes) {
				matchedCert = c
				break
			}
		}
	}
	if matchedCert == nil {
		return nil, fmt.Errorf("signer certificate with serial %s matching PKCS#7 SignerInfo was not found", signer.IssuerAndSerial.SerialNumber.String())
	}
	if signerCert != nil && !matchedCert.Equal(signerCert) {
		return nil, errors.New("PKCS#7 signer certificate does not match the trusted certificate")
	}

	// Calculate data hash according to digest algorithm
	var dataDigest []byte
	var hashFunc crypto.Hash
	switch {
	case signer.DigestAlgorithm.Algorithm.Equal(oidSHA512):
		h := sha512.Sum512(data)
		dataDigest = h[:]
		hashFunc = crypto.SHA512
	case signer.DigestAlgorithm.Algorithm.Equal(oidSHA256):
		h := sha256.Sum256(data)
		dataDigest = h[:]
		hashFunc = crypto.SHA256
	default:
		return nil, fmt.Errorf("unsupported PKCS#7 digest algorithm: %s", signer.DigestAlgorithm.Algorithm)
	}

	var digestToVerify []byte
	if len(signer.SignedAttributes) > 0 {
		// Verify messageDigest attribute matches the hash of input data
		var foundDigest []byte
		for _, attr := range signer.SignedAttributes {
			if attr.Type.Equal(oidMessageDigest) {
				var d []byte
				if _, err := asn1.Unmarshal(attr.Value.Bytes, &d); err == nil {
					foundDigest = d
				}
				break
			}
		}
		if len(foundDigest) == 0 {
			return nil, errors.New("PKCS#7 messageDigest signed attribute not found")
		}
		if len(foundDigest) != len(dataDigest) || subtle.ConstantTimeCompare(foundDigest, dataDigest) != 1 {
			return nil, errors.New("PKCS#7 message digest mismatch (data has been tampered with)")
		}

		rawAttrs, err := asn1.MarshalWithParams(signer.SignedAttributes, "set")
		if err != nil {
			return nil, fmt.Errorf("failed re-marshaling signed attributes: %w", err)
		}
		if hashFunc == crypto.SHA512 {
			h := sha512.Sum512(rawAttrs)
			digestToVerify = h[:]
		} else {
			h := sha256.Sum256(rawAttrs)
			digestToVerify = h[:]
		}
	} else {
		digestToVerify = dataDigest
	}

	// Verify cryptographic signature using public key of the certificate
	switch pub := matchedCert.PublicKey.(type) {
	case *rsa.PublicKey:
		err = rsa.VerifyPKCS1v15(pub, hashFunc, digestToVerify, signer.Signature)
		if err != nil {
			return nil, fmt.Errorf("RSA PKCS#7 signature verification failed: %w", err)
		}
	case *ecdsa.PublicKey:
		if !ecdsa.VerifyASN1(pub, digestToVerify, signer.Signature) {
			return nil, errors.New("ECDSA PKCS#7 signature verification failed")
		}
	default:
		return nil, fmt.Errorf("unsupported certificate public key type: %T", matchedCert.PublicKey)
	}

	// If root cert is provided, verify certificate and chain
	if rootCert != nil {
		roots := x509.NewCertPool()
		roots.AddCert(rootCert)

		intermediates := x509.NewCertPool()
		for _, c := range certs {
			if !c.Equal(matchedCert) && !c.Equal(rootCert) {
				intermediates.AddCert(c)
			}
		}

		opts := x509.VerifyOptions{
			Roots:         roots,
			Intermediates: intermediates,
			KeyUsages:     []x509.ExtKeyUsage{x509.ExtKeyUsageCodeSigning},
		}
		if _, err := matchedCert.Verify(opts); err != nil {
			return nil, fmt.Errorf("certificate verification failed: %w", err)
		}
	}

	return matchedCert, nil
}
