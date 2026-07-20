//go:build e2elive

// Package e2elive is the opt-in live-integration suite: it drives the real
// compiled tinvest binary as a subprocess against the REAL T-Invest sandbox
// (sandbox-invest-public-api.tbank.ru). It is compiled only under the `e2elive`
// build tag, so a plain `go test ./...` never sees it, and every test t.Skips
// cleanly when TINVEST_TOKEN is unset.
//
// Run it with the sandbox token exported:
//
//	TINVEST_TOKEN=… TINVEST_CA_FILE=… go test -tags e2elive -race ./test/e2e-live/...
//
// Safety is structural, not incidental (see harness_test.go): the suite can only
// ever reach the sandbox host or a loopback relay whose upstream is pinned to
// the sandbox constant. Nothing here can address production. The token reaches
// the binary only through the process environment and is never logged or echoed;
// the relay strips it from the forwarded metadata and re-attaches it as per-RPC
// credentials on the upstream call, so it never rides in ordinary outgoing
// metadata (mirroring the production transport).
package e2elive

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
	"strings"
	"testing"
	"time"

	"github.com/Dronnn/tinvest/internal/config"
)

// Set once by TestMain and shared by every test in the package.
var (
	binaryPath string // the compiled tinvest binary

	// runtimeCAPath holds ONLY the runtime CA. The CLI uses it (via
	// TINVEST_CA_FILE) when it dials the loopback relay, whose leaf is signed by
	// this CA. The real sandbox is never verified with it.
	runtimeCAPath string
	// relayServerCert is the leaf every relay instance serves, carrying a
	// 127.0.0.1 IP SAN so hostname verification passes for a loopback target.
	relayServerCert tls.Certificate

	// sandboxRootPool verifies the real sandbox's certificate, loaded from the
	// operator's TINVEST_CA_FILE (the Russian Trusted CA bundle). nil means fall
	// back to the system trust store — which normally cannot reach T-Bank, so a
	// missing bundle surfaces as an honest connection failure, never a silent
	// prod hop. Used by the relay for its upstream leg.
	sandboxRootPool *x509.CertPool
	// directCAFile is the CA bundle path the CLI uses for direct sandbox calls
	// (open/topup/list/get/cancel/close). Empty means "leave TINVEST_CA_FILE
	// unset and use the system store".
	directCAFile string

	// liveToken is the sandbox token, read once from the environment. It is
	// passed to child processes through TINVEST_TOKEN and never printed.
	liveToken string

	testWorkDir string // scratch dir removed on exit
)

// tokenPresent reports whether a token was provided; tests skip when it is not.
func tokenPresent() bool { return liveToken != "" }

func TestMain(m *testing.M) {
	code, err := setup(m)
	if err != nil {
		fmt.Fprintln(os.Stderr, "e2e-live setup:", err)
		os.Exit(1)
	}
	os.Exit(code)
}

// setup performs one-time setup (build + runtime TLS material + trust anchors)
// and runs the suite. It never contacts the network: work that needs the
// sandbox happens inside the tests, which skip without a token.
func setup(m *testing.M) (int, error) {
	// Fail fast if the canonical sandbox host is somehow the prod host: the whole
	// safety story rests on this constant naming the sandbox.
	if config.SandboxEndpoint == config.ProdEndpoint || !strings.Contains(config.SandboxEndpoint, "sandbox") {
		return 1, fmt.Errorf("sandbox endpoint constant %q is not a sandbox host", config.SandboxEndpoint)
	}

	liveToken = strings.TrimSpace(os.Getenv(config.EnvToken))
	directCAFile = strings.TrimSpace(os.Getenv(config.EnvCAFile))

	repoRoot, err := filepath.Abs("../..")
	if err != nil {
		return 1, fmt.Errorf("resolve repo root: %w", err)
	}

	testWorkDir, err = os.MkdirTemp("", "tinvest-e2e-live")
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
	relayServerCert = cert
	runtimeCAPath = filepath.Join(testWorkDir, "relay-ca.pem")
	if err := os.WriteFile(runtimeCAPath, caPEM, 0o600); err != nil {
		return 1, fmt.Errorf("write relay CA file: %w", err)
	}

	if directCAFile != "" {
		data, err := os.ReadFile(directCAFile)
		if err != nil {
			return 1, fmt.Errorf("read TINVEST_CA_FILE %s: %w", directCAFile, err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(data) {
			return 1, fmt.Errorf("TINVEST_CA_FILE %s has no valid PEM certificates", directCAFile)
		}
		sandboxRootPool = pool
	}

	return m.Run(), nil
}

// generateTLS mints a throwaway CA and a leaf valid for 127.0.0.1, so a relay
// can serve real TLS and the binary can verify it with TINVEST_CA_FILE pointed
// at the CA (the transport swaps only the root pool; hostname verification
// stays on — see internal/transport.Dial).
func generateTLS() (caPEM []byte, leaf tls.Certificate, err error) {
	caKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, tls.Certificate{}, err
	}
	caTmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "tinvest-e2e-live-ca"},
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
