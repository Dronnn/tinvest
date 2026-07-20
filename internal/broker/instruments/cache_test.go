package instruments

import (
	"path/filepath"
	"testing"
	"time"

	investapi "github.com/Dronnn/tinvest/pb/investapi"
)

func testInstrument(uid string) *investapi.Instrument {
	return &investapi.Instrument{Uid: uid, Ticker: "SBER", ClassCode: "TQBR", Name: "Sberbank"}
}

// fakeClock lets tests control the cache's notion of "now" deterministically.
type fakeClock struct{ now time.Time }

func (c *fakeClock) Now() time.Time { return c.now }

// TestDefaultCachePathIgnoresRelativeXDGCacheHome pins the XDG rule: an
// absolute XDG_CACHE_HOME is honored, while a relative (or empty) value is
// invalid per the spec and falls back to ~/.cache instead of being used
// verbatim.
func TestDefaultCachePathIgnoresRelativeXDGCacheHome(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	fallback := filepath.Join(home, ".cache", "tinvest", "instruments.json")

	abs := t.TempDir()
	t.Setenv("XDG_CACHE_HOME", abs)
	if got, want := DefaultCachePath(), filepath.Join(abs, "tinvest", "instruments.json"); got != want {
		t.Errorf("absolute XDG_CACHE_HOME: path = %q, want %q", got, want)
	}

	t.Setenv("XDG_CACHE_HOME", "relative/cache")
	if got := DefaultCachePath(); got != fallback {
		t.Errorf("relative XDG_CACHE_HOME: path = %q, want fallback %q", got, fallback)
	}

	t.Setenv("XDG_CACHE_HOME", "")
	if got := DefaultCachePath(); got != fallback {
		t.Errorf("empty XDG_CACHE_HOME: path = %q, want fallback %q", got, fallback)
	}
}

func TestCacheHit(t *testing.T) {
	clock := &fakeClock{now: time.Date(2026, 7, 18, 12, 0, 0, 0, time.UTC)}
	path := filepath.Join(t.TempDir(), "instruments.json")
	cache := NewCache(path, 24*time.Hour, clock.Now)

	if err := cache.Put("SBER@TQBR", testInstrument("uid-1")); err != nil {
		t.Fatalf("Put: %v", err)
	}

	got, ok := cache.Get("SBER@TQBR")
	if !ok {
		t.Fatal("Get: want hit")
	}
	if got.GetUid() != "uid-1" {
		t.Errorf("uid = %q, want uid-1", got.GetUid())
	}
}

func TestCacheMissForUnknownKey(t *testing.T) {
	path := filepath.Join(t.TempDir(), "instruments.json")
	cache := NewCache(path, 24*time.Hour, nil)

	if _, ok := cache.Get("nope"); ok {
		t.Error("Get: want miss for a key never written")
	}
}

func TestCacheRejectsEntriesThatContradictTheirLookupKey(t *testing.T) {
	tests := []struct {
		name string
		key  string
		inst *investapi.Instrument
	}{
		{
			name: "uid", key: "e6123145-9665-43e0-8413-cd61b8aa9b13",
			inst: &investapi.Instrument{Uid: "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"},
		},
		{
			name: "figi", key: "BBG004730N88",
			inst: &investapi.Instrument{Figi: "BBG000000000"},
		},
		{
			name: "ticker and class code", key: "SBER@TQBR",
			inst: &investapi.Instrument{Ticker: "GAZP", ClassCode: "TQBR"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cache := NewCache(filepath.Join(t.TempDir(), "instruments.json"), 24*time.Hour, nil)
			if err := cache.Put(tt.key, tt.inst); err != nil {
				t.Fatalf("Put: %v", err)
			}
			if got, ok := cache.Get(tt.key); ok {
				t.Fatalf("Get returned contradictory entry %+v", got)
			}
		})
	}
}

func TestCacheLookupKeyValidationIsCaseInsensitive(t *testing.T) {
	cache := NewCache(filepath.Join(t.TempDir(), "instruments.json"), 24*time.Hour, nil)
	if err := cache.Put("sber@tqbr", &investapi.Instrument{Ticker: "SBER", ClassCode: "TQBR"}); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if _, ok := cache.Get("sber@tqbr"); !ok {
		t.Fatal("case-only differences should not invalidate a matching cache entry")
	}
}

func TestCacheUIDAndFIGIValidationIsCaseSensitive(t *testing.T) {
	tests := []struct {
		name string
		key  string
		inst *investapi.Instrument
	}{
		{
			name: "uid", key: "e6123145-9665-43e0-8413-cd61b8aa9b13",
			inst: &investapi.Instrument{Uid: "E6123145-9665-43E0-8413-CD61B8AA9B13"},
		},
		{
			name: "figi", key: "BBG004730N88",
			inst: &investapi.Instrument{Figi: "bbg004730n88"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cache := NewCache(filepath.Join(t.TempDir(), "instruments.json"), 24*time.Hour, nil)
			if err := cache.Put(tt.key, tt.inst); err != nil {
				t.Fatalf("Put: %v", err)
			}
			if got, ok := cache.Get(tt.key); ok {
				t.Fatalf("Get returned case-mismatched entry %+v", got)
			}
		})
	}
}

func TestCacheTTLExpiry(t *testing.T) {
	clock := &fakeClock{now: time.Date(2026, 7, 18, 12, 0, 0, 0, time.UTC)}
	path := filepath.Join(t.TempDir(), "instruments.json")
	cache := NewCache(path, 24*time.Hour, clock.Now)

	if err := cache.Put("SBER@TQBR", testInstrument("uid-1")); err != nil {
		t.Fatalf("Put: %v", err)
	}

	// Just inside the TTL: still a hit.
	clock.now = clock.now.Add(23*time.Hour + 59*time.Minute)
	if _, ok := cache.Get("SBER@TQBR"); !ok {
		t.Fatal("Get: want hit just inside TTL")
	}

	// Just past the TTL: now a miss.
	clock.now = clock.now.Add(2 * time.Minute)
	if _, ok := cache.Get("SBER@TQBR"); ok {
		t.Fatal("Get: want miss once TTL has elapsed")
	}
}

func TestCacheNilDisablesCaching(t *testing.T) {
	var cache *Cache
	if err := cache.Put("SBER@TQBR", testInstrument("uid-1")); err != nil {
		t.Fatalf("Put on nil cache: %v", err)
	}
	if _, ok := cache.Get("SBER@TQBR"); ok {
		t.Fatal("Get on nil cache: want miss")
	}
}

func TestCacheEmptyPathDisablesCaching(t *testing.T) {
	cache := NewCache("", 24*time.Hour, nil)
	if err := cache.Put("SBER@TQBR", testInstrument("uid-1")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if _, ok := cache.Get("SBER@TQBR"); ok {
		t.Fatal("Get: want miss when caching is disabled")
	}
}

func TestCachePersistsAcrossInstances(t *testing.T) {
	clock := &fakeClock{now: time.Date(2026, 7, 18, 12, 0, 0, 0, time.UTC)}
	path := filepath.Join(t.TempDir(), "nested", "instruments.json")

	first := NewCache(path, 24*time.Hour, clock.Now)
	consistent := testInstrument("uid-2")
	consistent.Figi = "BBG004730N88"
	if err := first.Put("BBG004730N88", consistent); err != nil {
		t.Fatalf("Put: %v", err)
	}

	second := NewCache(path, 24*time.Hour, clock.Now)
	got, ok := second.Get("BBG004730N88")
	if !ok {
		t.Fatal("Get on a fresh Cache instance over the same path: want hit")
	}
	if got.GetUid() != "uid-2" {
		t.Errorf("uid = %q, want uid-2", got.GetUid())
	}
}
