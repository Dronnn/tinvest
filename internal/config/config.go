// Package config resolves profiles, environment, and token sources into the
// effective settings for a command invocation. Loading only ever reads local
// files and environment variables — it must never touch the network.
package config

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/BurntSushi/toml"
)

// Canonical API hosts (plan §1.1).
const (
	ProdEndpoint    = "invest-public-api.tbank.ru:443"
	SandboxEndpoint = "sandbox-invest-public-api.tbank.ru:443"
)

// Environment variables recognized by the CLI. The token is intentionally not
// accepted as a flag or argument (shell history / process list leakage).
const (
	EnvToken   = "TINVEST_TOKEN"
	EnvProfile = "TINVEST_PROFILE"
	EnvOutput  = "TINVEST_OUTPUT"
)

// Flags carries the global command-line flags that participate in resolution.
// Flag values win over environment variables, which win over the config file.
type Flags struct {
	Profile   string
	AccountID string
	Output    string
	TokenFile string
	Timeout   time.Duration
	Sandbox   bool
}

// Settings is the fully resolved configuration for one invocation.
type Settings struct {
	Profile   string
	Endpoint  string // resolved host:port
	AccountID string
	Output    string // "json", "table", or "" (auto: TTY sniffing)
	Token     string // "" when no token source is configured
	Timeout   time.Duration
}

// File mirrors ~/.config/tinvest/config.toml.
type File struct {
	DefaultProfile string             `toml:"default_profile"`
	Profiles       map[string]Profile `toml:"profiles"`
}

// Profile is one named profile in the config file.
type Profile struct {
	Endpoint  string `toml:"endpoint"` // "prod", "sandbox", or host:port
	AccountID string `toml:"account_id"`
	Output    string `toml:"output"`
	TokenFile string `toml:"token_file"`
}

// TokenError marks token-resolution failures so the CLI can map them to the
// auth exit code rather than the usage one.
type TokenError struct{ err error }

func (e *TokenError) Error() string { return e.err.Error() }
func (e *TokenError) Unwrap() error { return e.err }

// Path returns the config file location, honoring XDG_CONFIG_HOME.
func Path() string {
	dir := os.Getenv("XDG_CONFIG_HOME")
	if dir == "" {
		home, err := os.UserHomeDir()
		if err != nil {
			return ""
		}
		dir = filepath.Join(home, ".config")
	}
	return filepath.Join(dir, "tinvest", "config.toml")
}

// Load resolves the effective settings. A missing config file is not an
// error: defaults apply (prod endpoint, auto output, token from env).
func Load(flags Flags) (Settings, error) {
	file, err := readFile(Path())
	if err != nil {
		return Settings{}, err
	}

	name := firstNonEmpty(flags.Profile, os.Getenv(EnvProfile), file.DefaultProfile)
	var profile Profile
	if name != "" {
		p, ok := file.Profiles[name]
		if !ok {
			return Settings{}, fmt.Errorf("profile %q not found in %s", name, Path())
		}
		profile = p
	}

	output := firstNonEmpty(flags.Output, os.Getenv(EnvOutput), profile.Output)
	if output != "" && output != "json" && output != "table" {
		return Settings{}, fmt.Errorf("invalid output format %q (want json or table)", output)
	}

	endpoint, err := resolveEndpoint(flags.Sandbox, profile.Endpoint)
	if err != nil {
		return Settings{}, err
	}

	token, err := resolveToken(flags.TokenFile, profile.TokenFile)
	if err != nil {
		return Settings{}, err
	}

	return Settings{
		Profile:   name,
		Endpoint:  endpoint,
		AccountID: firstNonEmpty(flags.AccountID, profile.AccountID),
		Output:    output,
		Token:     token,
		Timeout:   flags.Timeout,
	}, nil
}

func readFile(path string) (File, error) {
	var file File
	if path == "" {
		return file, nil
	}
	data, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return file, nil
	}
	if err != nil {
		return file, fmt.Errorf("read config: %w", err)
	}
	if err := toml.Unmarshal(data, &file); err != nil {
		return file, fmt.Errorf("parse %s: %w", path, err)
	}
	return file, nil
}

func resolveEndpoint(sandbox bool, configured string) (string, error) {
	if sandbox {
		return SandboxEndpoint, nil
	}
	switch configured {
	case "", "prod":
		return ProdEndpoint, nil
	case "sandbox":
		return SandboxEndpoint, nil
	}
	if !strings.Contains(configured, ":") {
		return "", fmt.Errorf("invalid endpoint %q (want prod, sandbox, or host:port)", configured)
	}
	return configured, nil
}

// resolveToken follows plan §6: --token-file flag, then TINVEST_TOKEN, then
// the profile's token_file. An empty result is not an error here; commands
// that need the network report it as an auth failure.
func resolveToken(flagFile, profileFile string) (string, error) {
	if flagFile != "" {
		return readToken(flagFile)
	}
	if token := strings.TrimSpace(os.Getenv(EnvToken)); token != "" {
		return token, nil
	}
	if profileFile != "" {
		return readToken(profileFile)
	}
	return "", nil
}

func readToken(path string) (string, error) {
	if strings.HasPrefix(path, "~/") {
		home, err := os.UserHomeDir()
		if err != nil {
			return "", &TokenError{fmt.Errorf("resolve home directory: %w", err)}
		}
		path = filepath.Join(home, path[2:])
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return "", &TokenError{fmt.Errorf("read token file: %w", err)}
	}
	token := strings.TrimSpace(string(data))
	if token == "" {
		return "", &TokenError{fmt.Errorf("token file %s is empty", path)}
	}
	return token, nil
}

func firstNonEmpty(values ...string) string {
	for _, v := range values {
		if v != "" {
			return v
		}
	}
	return ""
}
