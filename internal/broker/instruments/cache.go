package instruments

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	investapi "github.com/Dronnn/tinvest/pb/investapi"
)

// DefaultTTL is the cache freshness window (plan §5).
const DefaultTTL = 24 * time.Hour

// Clock abstracts time.Now for deterministic TTL tests.
type Clock func() time.Time

// Cache is a local JSON-file cache of successful instrument resolutions,
// keyed by the raw identifier string the caller passed in (not the
// normalized uid — a figi and its uid cache as separate entries). It never
// caches errors, and a nil *Cache is valid: every method becomes a no-op, so
// callers can pass a nil cache to disable caching outright.
type Cache struct {
	path  string
	ttl   time.Duration
	clock Clock

	mu sync.Mutex
}

type cacheEntry struct {
	Instrument *investapi.Instrument `json:"instrument"`
	CachedAt   time.Time             `json:"cached_at"`
}

type cacheFile struct {
	Entries map[string]cacheEntry `json:"entries"`
}

// DefaultCachePath returns the standard cache location
// ${XDG_CACHE_HOME:-~/.cache}/tinvest/instruments.json. It returns "" if the
// home directory cannot be resolved.
//
// Per the XDG Base Directory spec, XDG_CACHE_HOME must be an absolute path; a
// relative (or empty) value is invalid and treated as unset, falling back to
// ~/.cache. Platform-native cache directories (e.g. macOS ~/Library/Caches)
// are deliberately not used, so the cache location is identical across the CLI
// and the library on every OS.
func DefaultCachePath() string {
	dir := os.Getenv("XDG_CACHE_HOME")
	if !filepath.IsAbs(dir) {
		home, err := os.UserHomeDir()
		if err != nil {
			return ""
		}
		dir = filepath.Join(home, ".cache")
	}
	return filepath.Join(dir, "tinvest", "instruments.json")
}

// NewCache builds a cache rooted at path with the given TTL. ttl<=0 falls
// back to DefaultTTL; a nil clock defaults to time.Now.
func NewCache(path string, ttl time.Duration, clock Clock) *Cache {
	if ttl <= 0 {
		ttl = DefaultTTL
	}
	if clock == nil {
		clock = time.Now
	}
	return &Cache{path: path, ttl: ttl, clock: clock}
}

// Get returns the cached instrument for key if present and still within TTL.
func (c *Cache) Get(key string) (*investapi.Instrument, bool) {
	if c == nil || c.path == "" {
		return nil, false
	}
	c.mu.Lock()
	defer c.mu.Unlock()

	file, err := c.read()
	if err != nil {
		return nil, false
	}
	entry, ok := file.Entries[key]
	if !ok {
		return nil, false
	}
	if c.clock().Sub(entry.CachedAt) > c.ttl {
		return nil, false
	}
	if !cacheEntryMatchesKey(key, entry.Instrument) {
		return nil, false
	}
	return entry.Instrument, true
}

func cacheEntryMatchesKey(key string, inst *investapi.Instrument) bool {
	if inst == nil {
		return false
	}
	parsed, err := Classify(key)
	if err != nil {
		return false
	}
	switch parsed.Kind {
	case KindUID:
		return inst.GetUid() != "" && parsed.Raw == inst.GetUid()
	case KindFIGI:
		return inst.GetFigi() != "" && parsed.Raw == inst.GetFigi()
	case KindTicker:
		return inst.GetTicker() != "" && inst.GetClassCode() != "" &&
			strings.EqualFold(parsed.Ticker, inst.GetTicker()) &&
			strings.EqualFold(parsed.ClassCode, inst.GetClassCode())
	default:
		return false
	}
}

// Put stores a successful resolution under key. Writes are atomic (write a
// temp file, then rename) so a crash mid-write cannot corrupt the cache.
func (c *Cache) Put(key string, inst *investapi.Instrument) error {
	if c == nil || c.path == "" {
		return nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()

	file, err := c.read()
	if err != nil {
		file = cacheFile{}
	}
	if file.Entries == nil {
		file.Entries = make(map[string]cacheEntry)
	}
	file.Entries[key] = cacheEntry{Instrument: inst, CachedAt: c.clock()}
	return c.write(file)
}

func (c *Cache) read() (cacheFile, error) {
	data, err := os.ReadFile(c.path)
	if errors.Is(err, os.ErrNotExist) {
		return cacheFile{Entries: map[string]cacheEntry{}}, nil
	}
	if err != nil {
		return cacheFile{}, err
	}
	var file cacheFile
	if err := json.Unmarshal(data, &file); err != nil {
		return cacheFile{}, err
	}
	if file.Entries == nil {
		file.Entries = map[string]cacheEntry{}
	}
	return file, nil
}

func (c *Cache) write(file cacheFile) error {
	if err := os.MkdirAll(filepath.Dir(c.path), 0o755); err != nil {
		return err
	}
	data, err := json.MarshalIndent(file, "", "  ")
	if err != nil {
		return err
	}
	tmp := c.path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o600); err != nil {
		return err
	}
	return os.Rename(tmp, c.path)
}
