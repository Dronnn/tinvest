package history

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
)

type roundTripFunc func(*http.Request) (*http.Response, error)

func (fn roundTripFunc) RoundTrip(request *http.Request) (*http.Response, error) {
	return fn(request)
}

func testHTTPClient(handler http.Handler) *http.Client {
	return &http.Client{Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
		recorder := httptest.NewRecorder()
		handler.ServeHTTP(recorder, request)
		return recorder.Result(), nil
	})}
}

func TestDownloadStreamsZipWithBearerAuth(t *testing.T) {
	var authorization, instrumentID, year string
	payload := []byte("PK\x03\x04fake-zip")
	client := testHTTPClient(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		authorization = r.Header.Get("Authorization")
		instrumentID = r.URL.Query().Get("instrumentId")
		year = r.URL.Query().Get("year")
		_, _ = w.Write(payload)
	}))
	outDir := t.TempDir()

	result, err := NewWithHTTPClient(client, "https://history.test/history-data", "secret-token").Download(context.Background(), "uid-1", 2025, outDir)
	if err != nil {
		t.Fatalf("Download: %v", err)
	}
	if authorization != "Bearer secret-token" {
		t.Errorf("Authorization = %q", authorization)
	}
	if instrumentID != "uid-1" || year != "2025" {
		t.Errorf("query instrumentId=%q year=%q", instrumentID, year)
	}
	wantPath := filepath.Join(outDir, "uid-1-2025.zip")
	if result.Path != wantPath || result.Size != int64(len(payload)) {
		t.Errorf("result = %+v, want path %s size %d", result, wantPath, len(payload))
	}
	got, err := os.ReadFile(wantPath)
	if err != nil {
		t.Fatalf("read zip: %v", err)
	}
	if string(got) != string(payload) {
		t.Errorf("zip = %q", got)
	}
}

func TestDownloadHTTPErrorDoesNotLeaveFile(t *testing.T) {
	client := testHTTPClient(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "denied", http.StatusUnauthorized)
	}))
	outDir := t.TempDir()

	_, err := NewWithHTTPClient(client, "https://history.test/history-data", "bad-token").Download(context.Background(), "uid-1", 2025, outDir)
	var httpErr *HTTPError
	if !errors.As(err, &httpErr) || httpErr.StatusCode != http.StatusUnauthorized {
		t.Fatalf("error = %v", err)
	}
	if _, statErr := os.Stat(filepath.Join(outDir, "uid-1-2025.zip")); !errors.Is(statErr, os.ErrNotExist) {
		t.Errorf("partial file stat error = %v", statErr)
	}
}

func TestNewRejectsEmptyCAFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "empty.pem")
	if err := os.WriteFile(path, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := New("token", path); err == nil {
		t.Fatal("want empty CA error")
	}
}

func TestValidateYear(t *testing.T) {
	if err := ValidateYear(2025); err != nil {
		t.Fatalf("ValidateYear: %v", err)
	}
	if err := ValidateYear(25); err == nil {
		t.Fatal("want YYYY error")
	}
}
