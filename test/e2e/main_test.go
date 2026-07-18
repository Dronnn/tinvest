// Package e2e is the black-box crash-injection suite for the tinvest binary
// (plan §11.2). It builds the real CLI once, runs it as a subprocess against an
// in-process gRPC fake served over TLS from a runtime-generated CA, and asserts
// the reliability contract end to end: the intent ledger stages written before
// and after each network step (plan §9/§10), the exit-code contract (plan §7),
// and stdout/stderr discipline under every failure. Nothing here touches the
// real broker or the developer's config, cache, or journal — every invocation
// runs in an isolated XDG_* sandbox.
package e2e

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"math/big"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"
)

// Set once by TestMain and shared by every test in the package.
var (
	binaryPath  string          // the compiled tinvest binary
	caCertPath  string          // PEM file holding the test CA, wired via TINVEST_CA_FILE
	serverCert  tls.Certificate // leaf served by every fake, signed by the test CA
	testWorkDir string          // scratch dir removed on exit
)

// The instrument and order identifiers used across the suite. testUID is a
// valid instrument_uid shape (8-4-4-4-12 hex) so it reaches the fake's
// GetInstrumentBy without a local classification error.
const testUID = "11111111-1111-4111-8111-111111111111"

func TestMain(m *testing.M) {
	code, err := run(m)
	if err != nil {
		fmt.Fprintln(os.Stderr, "e2e setup:", err)
		os.Exit(1)
	}
	os.Exit(code)
}

// run performs one-time setup (build + TLS material), runs the suite, and
// cleans up. It is separated from TestMain so a setup error can return instead
// of calling os.Exit while defers are pending.
func run(m *testing.M) (int, error) {
	repoRoot, err := filepath.Abs("../..")
	if err != nil {
		return 1, fmt.Errorf("resolve repo root: %w", err)
	}

	testWorkDir, err = os.MkdirTemp("", "tinvest-e2e")
	if err != nil {
		return 1, fmt.Errorf("temp dir: %w", err)
	}
	defer func() { _ = os.RemoveAll(testWorkDir) }()

	binaryPath = filepath.Join(testWorkDir, "tinvest")
	build := exec.Command("go", "build", "-o", binaryPath, "./cmd/tinvest")
	build.Dir = repoRoot
	build.Stderr = os.Stderr
	if err := build.Run(); err != nil {
		return 1, fmt.Errorf("build tinvest (repo may be mid-edit): %w", err)
	}

	caPEM, cert, err := generateTLS()
	if err != nil {
		return 1, fmt.Errorf("generate TLS material: %w", err)
	}
	serverCert = cert
	caCertPath = filepath.Join(testWorkDir, "ca.pem")
	if err := os.WriteFile(caCertPath, caPEM, 0o600); err != nil {
		return 1, fmt.Errorf("write CA file: %w", err)
	}

	return m.Run(), nil
}

// generateTLS mints a throwaway CA and a leaf certificate valid for "localhost"
// and 127.0.0.1, so the fake can serve real TLS and the binary can verify it
// with TINVEST_CA_FILE pointed at the CA (the transport swaps only the root
// pool, hostname verification stays on — see internal/transport.Dial).
func generateTLS() (caPEM []byte, leaf tls.Certificate, err error) {
	caKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, tls.Certificate{}, err
	}
	caTmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "tinvest-e2e-ca"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(24 * time.Hour),
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
		BasicConstraintsValid: true,
		IsCA:                  true,
	}
	caDER, err := x509.CreateCertificate(rand.Reader, caTmpl, caTmpl, &caKey.PublicKey, caKey)
	if err != nil {
		return nil, tls.Certificate{}, err
	}
	caCert, err := x509.ParseCertificate(caDER)
	if err != nil {
		return nil, tls.Certificate{}, err
	}
	caPEM = pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: caDER})

	leafKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, tls.Certificate{}, err
	}
	leafTmpl := &x509.Certificate{
		SerialNumber: big.NewInt(2),
		Subject:      pkix.Name{CommonName: "localhost"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(24 * time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		DNSNames:     []string{"localhost"},
		IPAddresses:  []net.IP{net.IPv4(127, 0, 0, 1), net.IPv6loopback},
	}
	leafDER, err := x509.CreateCertificate(rand.Reader, leafTmpl, caCert, &leafKey.PublicKey, caKey)
	if err != nil {
		return nil, tls.Certificate{}, err
	}
	leafKeyDER, err := x509.MarshalECPrivateKey(leafKey)
	if err != nil {
		return nil, tls.Certificate{}, err
	}
	leafCertPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: leafDER})
	leafKeyPEM := pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: leafKeyDER})
	leaf, err = tls.X509KeyPair(leafCertPEM, leafKeyPEM)
	return caPEM, leaf, err
}
