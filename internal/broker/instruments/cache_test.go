package instruments

import (
	"path/filepath"
	"testing"
	"time"

	investapi "tinvest/internal/pb/investapi"
)

func testInstrument(uid string) *investapi.Instrument {
	return &investapi.Instrument{Uid: uid, Ticker: "SBER", ClassCode: "TQBR", Name: "Sberbank"}
}

// fakeClock lets tests control the cache's notion of "now" deterministically.
type fakeClock struct{ now time.Time }

func (c *fakeClock) Now() time.Time { return c.now }

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
	if err := first.Put("BBG004730N88", testInstrument("uid-2")); err != nil {
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
