package config

import (
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// clearEnv removes ambient tinvest settings so tests are hermetic.
func clearEnv(t *testing.T) {
	t.Helper()
	for _, key := range []string{EnvToken, EnvProfile, EnvOutput} {
		t.Setenv(key, "")
		if err := os.Unsetenv(key); err != nil {
			t.Fatal(err)
		}
	}
}

func writeConfig(t *testing.T, content string) {
	t.Helper()
	dir := t.TempDir()
	t.Setenv("XDG_CONFIG_HOME", dir)
	if content == "" {
		return
	}
	if err := os.MkdirAll(filepath.Join(dir, "tinvest"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "tinvest", "config.toml"), []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
}

const sampleConfig = `
default_profile = "main"

[profiles.main]
endpoint = "sandbox"
account_id = "acc-main"
output = "table"

[profiles.alt]
endpoint = "localhost:31234"
account_id = "acc-alt"

[profiles.filetoken]
token_file = "%s"
`

func TestDefaultsWithoutConfigFile(t *testing.T) {
	clearEnv(t)
	writeConfig(t, "")
	t.Setenv(EnvToken, "env-token")

	s, err := Load(Flags{})
	if err != nil {
		t.Fatal(err)
	}
	if s.Endpoint != ProdEndpoint {
		t.Errorf("endpoint = %q, want prod default", s.Endpoint)
	}
	if s.Output != "" {
		t.Errorf("output = %q, want auto", s.Output)
	}
	if s.Token != "env-token" {
		t.Errorf("token = %q, want env token", s.Token)
	}
	if s.Profile != "" || s.AccountID != "" {
		t.Errorf("unexpected profile/account: %q/%q", s.Profile, s.AccountID)
	}
}

func TestDefaultProfileFromFile(t *testing.T) {
	clearEnv(t)
	writeConfig(t, strings.ReplaceAll(sampleConfig, "%s", "/nonexistent"))

	s, err := Load(Flags{})
	if err != nil {
		t.Fatal(err)
	}
	if s.Profile != "main" {
		t.Errorf("profile = %q, want main", s.Profile)
	}
	if s.Endpoint != SandboxEndpoint {
		t.Errorf("endpoint = %q, want sandbox", s.Endpoint)
	}
	if s.AccountID != "acc-main" {
		t.Errorf("account = %q", s.AccountID)
	}
	if s.Output != "table" {
		t.Errorf("output = %q, want table from profile", s.Output)
	}
	if s.Token != "" {
		t.Errorf("token = %q, want empty", s.Token)
	}
}

func TestProfilePrecedenceFlagOverEnv(t *testing.T) {
	clearEnv(t)
	writeConfig(t, strings.ReplaceAll(sampleConfig, "%s", "/nonexistent"))
	t.Setenv(EnvProfile, "main")

	s, err := Load(Flags{Profile: "alt"})
	if err != nil {
		t.Fatal(err)
	}
	if s.Profile != "alt" {
		t.Errorf("profile = %q, want alt (flag wins)", s.Profile)
	}
	if s.Endpoint != "localhost:31234" {
		t.Errorf("endpoint = %q, want custom host:port", s.Endpoint)
	}
	if s.AccountID != "acc-alt" {
		t.Errorf("account = %q", s.AccountID)
	}
}

func TestProfileFromEnv(t *testing.T) {
	clearEnv(t)
	writeConfig(t, strings.ReplaceAll(sampleConfig, "%s", "/nonexistent"))
	t.Setenv(EnvProfile, "alt")

	s, err := Load(Flags{})
	if err != nil {
		t.Fatal(err)
	}
	if s.Profile != "alt" {
		t.Errorf("profile = %q, want alt (env wins over default_profile)", s.Profile)
	}
}

func TestUnknownProfileFails(t *testing.T) {
	clearEnv(t)
	writeConfig(t, strings.ReplaceAll(sampleConfig, "%s", "/nonexistent"))

	if _, err := Load(Flags{Profile: "ghost"}); err == nil {
		t.Fatal("want error for unknown profile")
	}
}

func TestOutputPrecedence(t *testing.T) {
	clearEnv(t)
	writeConfig(t, strings.ReplaceAll(sampleConfig, "%s", "/nonexistent")) // profile main: table
	t.Setenv(EnvOutput, "json")

	s, err := Load(Flags{})
	if err != nil {
		t.Fatal(err)
	}
	if s.Output != "json" {
		t.Errorf("output = %q, want json (env wins over profile)", s.Output)
	}

	s, err = Load(Flags{Output: "table"})
	if err != nil {
		t.Fatal(err)
	}
	if s.Output != "table" {
		t.Errorf("output = %q, want table (flag wins over env)", s.Output)
	}
}

func TestInvalidOutputFails(t *testing.T) {
	clearEnv(t)
	writeConfig(t, "")

	if _, err := Load(Flags{Output: "yaml"}); err == nil {
		t.Fatal("want error for invalid output")
	}
}

func TestSandboxFlagOverridesEndpoint(t *testing.T) {
	clearEnv(t)
	writeConfig(t, strings.ReplaceAll(sampleConfig, "%s", "/nonexistent"))

	s, err := Load(Flags{Profile: "alt", Sandbox: true})
	if err != nil {
		t.Fatal(err)
	}
	if s.Endpoint != SandboxEndpoint {
		t.Errorf("endpoint = %q, want sandbox (flag override)", s.Endpoint)
	}
}

func TestInvalidEndpointFails(t *testing.T) {
	clearEnv(t)
	writeConfig(t, "[profiles.bad]\nendpoint = \"bogus\"\n")

	if _, err := Load(Flags{Profile: "bad"}); err == nil {
		t.Fatal("want error for endpoint without port")
	}
}

func TestTokenPrecedence(t *testing.T) {
	clearEnv(t)
	dir := t.TempDir()
	flagFile := filepath.Join(dir, "flag-token")
	profileFile := filepath.Join(dir, "profile-token")
	if err := os.WriteFile(flagFile, []byte("flag-tok\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(profileFile, []byte("profile-tok\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	writeConfig(t, strings.ReplaceAll(sampleConfig, "%s", profileFile))

	// Profile token_file is the last resort.
	s, err := Load(Flags{Profile: "filetoken"})
	if err != nil {
		t.Fatal(err)
	}
	if s.Token != "profile-tok" {
		t.Errorf("token = %q, want profile file token", s.Token)
	}

	// Env wins over profile token_file.
	t.Setenv(EnvToken, "env-tok")
	s, err = Load(Flags{Profile: "filetoken"})
	if err != nil {
		t.Fatal(err)
	}
	if s.Token != "env-tok" {
		t.Errorf("token = %q, want env token", s.Token)
	}

	// --token-file wins over env.
	s, err = Load(Flags{Profile: "filetoken", TokenFile: flagFile})
	if err != nil {
		t.Fatal(err)
	}
	if s.Token != "flag-tok" {
		t.Errorf("token = %q, want flag file token", s.Token)
	}
}

func TestMissingTokenFileIsTokenError(t *testing.T) {
	clearEnv(t)
	writeConfig(t, "")

	_, err := Load(Flags{TokenFile: "/nonexistent/token"})
	var tokenErr *TokenError
	if !errors.As(err, &tokenErr) {
		t.Fatalf("want TokenError, got %v", err)
	}
}

func TestConfigParseErrorFails(t *testing.T) {
	clearEnv(t)
	writeConfig(t, "not [valid toml")

	if _, err := Load(Flags{}); err == nil {
		t.Fatal("want parse error")
	}
}

// TestNoNetworkDependencies guarantees config loading cannot touch the
// network: the package must not depend on gRPC or any net-facing package.
func TestNoNetworkDependencies(t *testing.T) {
	goBin, err := exec.LookPath("go")
	if err != nil {
		t.Skip("go binary not available")
	}
	out, err := exec.Command(goBin, "list", "-deps", "tinvest/internal/config").Output()
	if err != nil {
		t.Fatalf("go list: %v", err)
	}
	for _, dep := range strings.Fields(string(out)) {
		if dep == "net" || dep == "net/http" || strings.HasPrefix(dep, "google.golang.org/grpc") {
			t.Errorf("config depends on network package %s", dep)
		}
	}
}
