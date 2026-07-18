package instruments

import (
	"fmt"
	"regexp"
	"strings"
)

// IDKind is how a raw instrument identifier argument was classified.
type IDKind int

const (
	KindUnknown IDKind = iota
	KindUID
	KindFIGI
	KindTicker
)

func (k IDKind) String() string {
	switch k {
	case KindUID:
		return "uid"
	case KindFIGI:
		return "figi"
	case KindTicker:
		return "ticker"
	default:
		return "unknown"
	}
}

// ParsedID is the result of classifying a raw instrument identifier
// argument into one of the three accepted shapes (plan §5/§8).
type ParsedID struct {
	Kind      IDKind
	Raw       string // trimmed input; unchanged for uid/figi
	Ticker    string // set only for KindTicker
	ClassCode string // set only for KindTicker
}

var (
	uidPattern  = regexp.MustCompile(`^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$`)
	figiPattern = regexp.MustCompile(`^[0-9A-Za-z]{12}$`)
)

// InvalidIDError marks a locally-rejected instrument identifier: nothing was
// sent to the broker, so callers must map this to a usage error (exit 2),
// never to a network/broker classification.
type InvalidIDError struct{ msg string }

func (e *InvalidIDError) Error() string { return e.msg }

func invalidID(format string, args ...any) *InvalidIDError {
	return &InvalidIDError{msg: fmt.Sprintf(format, args...)}
}

// Classify determines whether a raw identifier is an instrument_uid (UUID
// shape: 8-4-4-4-12 hex), a FIGI (12 alphanumeric characters), or a
// TICKER@CLASSCODE pair. Anything else — including a bare ticker with no
// class code — is rejected before any network call is made.
func Classify(raw string) (ParsedID, error) {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return ParsedID{}, invalidID("empty instrument identifier")
	}
	if strings.Contains(trimmed, "@") {
		parts := strings.SplitN(trimmed, "@", 2)
		ticker, classCode := parts[0], parts[1]
		if ticker == "" || classCode == "" {
			return ParsedID{}, invalidID("invalid TICKER@CLASSCODE identifier %q: both sides are required", trimmed)
		}
		return ParsedID{Kind: KindTicker, Raw: trimmed, Ticker: ticker, ClassCode: classCode}, nil
	}
	if uidPattern.MatchString(trimmed) {
		return ParsedID{Kind: KindUID, Raw: trimmed}, nil
	}
	if figiPattern.MatchString(trimmed) {
		return ParsedID{Kind: KindFIGI, Raw: trimmed}, nil
	}
	return ParsedID{}, invalidID("unrecognized instrument identifier %q: want instrument_uid, FIGI, or TICKER@CLASSCODE", trimmed)
}
