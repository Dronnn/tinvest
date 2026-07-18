// Package history downloads the REST bulk candle-history zip archives.
package history

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

const historyURL = "https://invest-public-api.tbank.ru/history-data"

// Client streams GetHistory responses to disk.
type Client struct {
	httpClient *http.Client
	baseURL    string
	token      string
}

// Result describes a completed zip download.
type Result struct {
	Path string
	Size int64
}

// HTTPError reports a non-success response from the history endpoint.
type HTTPError struct {
	StatusCode int
	Status     string
}

// ValidateYear enforces the command's four-digit YYYY shape.
func ValidateYear(year int) error {
	if year < 1000 || year > 9999 {
		return fmt.Errorf("invalid history year %d: want YYYY", year)
	}
	return nil
}

func (e *HTTPError) Error() string {
	return fmt.Sprintf("history download failed: HTTP %s", e.Status)
}

// New builds an HTTP client with the same TLS policy as internal/transport:
// TLS 1.2 minimum and, when configured, a PEM CertPool replacing system roots.
func New(token, caFile string) (Client, error) {
	tlsConfig := &tls.Config{MinVersion: tls.VersionTLS12}
	if caFile != "" {
		pool, err := loadCAPool(caFile)
		if err != nil {
			return Client{}, err
		}
		tlsConfig.RootCAs = pool
	}
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.TLSClientConfig = tlsConfig
	return Client{httpClient: &http.Client{Transport: transport}, baseURL: historyURL, token: token}, nil
}

// NewWithHTTPClient is the deterministic test seam for an httptest endpoint.
func NewWithHTTPClient(client *http.Client, baseURL, token string) Client {
	return Client{httpClient: client, baseURL: baseURL, token: token}
}

// Download streams one yearly archive to <uid>-<year>.zip without unzipping.
func (c Client) Download(ctx context.Context, instrumentUID string, year int, outDir string) (Result, error) {
	if strings.TrimSpace(instrumentUID) == "" || filepath.Base(instrumentUID) != instrumentUID || strings.ContainsAny(instrumentUID, `/\\`) {
		return Result{}, fmt.Errorf("invalid instrument UID for history filename")
	}
	if err := ValidateYear(year); err != nil {
		return Result{}, err
	}
	if outDir == "" {
		outDir = "."
	}
	endpoint, err := url.Parse(c.baseURL)
	if err != nil {
		return Result{}, fmt.Errorf("invalid history endpoint: %w", err)
	}
	query := endpoint.Query()
	query.Set("instrumentId", instrumentUID)
	query.Set("year", strconv.Itoa(year))
	endpoint.RawQuery = query.Encode()

	request, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint.String(), nil)
	if err != nil {
		return Result{}, err
	}
	request.Header.Set("Authorization", "Bearer "+c.token)
	response, err := c.httpClient.Do(request)
	if err != nil {
		return Result{}, err
	}
	defer func() { _ = response.Body.Close() }()
	if response.StatusCode < http.StatusOK || response.StatusCode >= http.StatusMultipleChoices {
		return Result{}, &HTTPError{StatusCode: response.StatusCode, Status: response.Status}
	}

	if err := os.MkdirAll(outDir, 0o755); err != nil {
		return Result{}, fmt.Errorf("create history output directory: %w", err)
	}
	path := filepath.Join(outDir, fmt.Sprintf("%s-%d.zip", instrumentUID, year))
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return Result{}, fmt.Errorf("create history zip: %w", err)
	}
	removePartial := true
	defer func() {
		_ = file.Close()
		if removePartial {
			_ = os.Remove(path)
		}
	}()

	size, err := io.Copy(file, response.Body)
	if err != nil {
		return Result{}, fmt.Errorf("write history zip: %w", err)
	}
	if err := file.Close(); err != nil {
		return Result{}, fmt.Errorf("close history zip: %w", err)
	}
	removePartial = false
	return Result{Path: path, Size: size}, nil
}

func loadCAPool(path string) (*x509.CertPool, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read CA file %s: %w", path, err)
	}
	if len(bytes.TrimSpace(data)) == 0 {
		return nil, fmt.Errorf("CA file %s is empty", path)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(data) {
		return nil, fmt.Errorf("CA file %s contains no valid PEM certificates", path)
	}
	return pool, nil
}
