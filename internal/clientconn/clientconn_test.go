package clientconn

import (
	"context"
	"testing"

	"github.com/Dronnn/tinvest/internal/config"
)

// TestEndpointConstantsMatchConfig pins the clientconn endpoint constants to
// config's, so the two independent definitions cannot drift. clientconn owns
// copies (rather than importing config) so the library facade can reach them
// without pulling the TOML config parser into consumers' dependencies.
func TestEndpointConstantsMatchConfig(t *testing.T) {
	if ProdEndpoint != config.ProdEndpoint {
		t.Errorf("ProdEndpoint %q != config.ProdEndpoint %q", ProdEndpoint, config.ProdEndpoint)
	}
	if SandboxEndpoint != config.SandboxEndpoint {
		t.Errorf("SandboxEndpoint %q != config.SandboxEndpoint %q", SandboxEndpoint, config.SandboxEndpoint)
	}
}

// TestDialLimiterEnabledByDefault proves the rate limiter is wired into the
// shared connection unless explicitly disabled: this is the same code path
// both the CLI and the library facade use, so it is the single point that
// governs whether client-side rate limiting is present.
func TestDialLimiterEnabledByDefault(t *testing.T) {
	conn, limiter, err := Dial(context.Background(), Config{
		Endpoint: "invest-public-api.tbank.ru:443",
		Token:    "test-token",
	})
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	if limiter == nil {
		t.Fatal("limiter is nil; rate limiting should be on by default")
	}
}

func TestDialLimiterDisabled(t *testing.T) {
	conn, limiter, err := Dial(context.Background(), Config{
		Endpoint:         "invest-public-api.tbank.ru:443",
		Token:            "test-token",
		DisableRateLimit: true,
	})
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	if limiter != nil {
		t.Fatal("limiter is non-nil; DisableRateLimit should turn it off")
	}
}

func TestDialCAFileError(t *testing.T) {
	if _, _, err := Dial(context.Background(), Config{
		Endpoint: "invest-public-api.tbank.ru:443",
		Token:    "test-token",
		CAFile:   "/no/such/ca-bundle.pem",
	}); err == nil {
		t.Fatal("expected an error for an unreadable CA file")
	}
}
