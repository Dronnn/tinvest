package transport

import (
	"crypto/tls"
	"crypto/x509"
	"os"
	"path/filepath"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

// Shared TLS material for the in-process bufconn tests. The Bearer token rides
// as per-RPC credentials that require transport security, so a bufconn server
// must speak TLS; these serve and dial over a throwaway CA valid for the
// "bufnet" authority. Built once in TestMain.
var (
	bufServerOpt   grpc.ServerOption
	bufClientCreds credentials.TransportCredentials
	bufCAFile      string
)

func TestMain(m *testing.M) {
	os.Exit(runWithTLSMaterial(m))
}

func runWithTLSMaterial(m *testing.M) int {
	ca, err := buildTestCA()
	if err != nil {
		panic("build test CA: " + err.Error())
	}
	cert, err := tls.X509KeyPair(ca.certPEM, ca.keyPEM)
	if err != nil {
		panic("load test keypair: " + err.Error())
	}
	bufServerOpt = grpc.Creds(credentials.NewServerTLSFromCert(&cert))

	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(ca.rootPEM) {
		panic("append test root CA")
	}
	bufClientCreds = credentials.NewClientTLSFromCert(pool, "bufnet")

	dir, err := os.MkdirTemp("", "tinvest-transport-tls")
	if err != nil {
		panic(err)
	}
	defer func() { _ = os.RemoveAll(dir) }()
	bufCAFile = filepath.Join(dir, "ca.pem")
	if err := os.WriteFile(bufCAFile, ca.rootPEM, 0o600); err != nil {
		panic(err)
	}
	return m.Run()
}
