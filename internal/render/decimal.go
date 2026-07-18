package render

import (
	"fmt"
	"math"
	"strconv"
	"strings"

	investapi "tinvest/internal/pb/investapi"
)

// Decimal is the JSON form of MoneyValue/Quotation (plan §7): the raw
// units+nano pair is preserved (units as a string, per the int64 rule) next
// to a normalized decimal string. Never floats.
type Decimal struct {
	Units    string `json:"units"`
	Nano     int32  `json:"nano"`
	Value    string `json:"value"`
	Currency string `json:"currency,omitempty"`
}

// Money converts a MoneyValue.
func Money(m *investapi.MoneyValue) Decimal {
	d := Quotation(&investapi.Quotation{Units: m.GetUnits(), Nano: m.GetNano()})
	d.Currency = m.GetCurrency()
	return d
}

// Quotation converts a Quotation.
func Quotation(q *investapi.Quotation) Decimal {
	return Decimal{
		Units: strconv.FormatInt(q.GetUnits(), 10),
		Nano:  q.GetNano(),
		Value: DecimalString(q.GetUnits(), q.GetNano()),
	}
}

// DecimalString renders units+nano as an exact decimal string. The contract
// guarantees units and nano share a sign; the sign is emitted once. Trailing
// zeros of the fractional part are trimmed ("1.500000000" → "1.5").
func DecimalString(units int64, nano int32) string {
	negative := units < 0 || nano < 0
	absUnits := units
	if absUnits < 0 {
		absUnits = -absUnits
	}
	absNano := int64(nano)
	if absNano < 0 {
		absNano = -absNano
	}

	var b strings.Builder
	if negative && (absUnits != 0 || absNano != 0) {
		b.WriteByte('-')
	}
	if units == math.MinInt64 {
		// -absUnits overflows for int64 min; the digits are known.
		b.WriteString("9223372036854775808")
	} else {
		b.WriteString(strconv.FormatInt(absUnits, 10))
	}
	if absNano != 0 {
		frac := strings.TrimRight(pad9(absNano), "0")
		b.WriteByte('.')
		b.WriteString(frac)
	}
	return b.String()
}

func pad9(n int64) string {
	s := strconv.FormatInt(n, 10)
	return strings.Repeat("0", 9-len(s)) + s
}

// ParseQuotation parses an exact decimal string ("123.45", "-0.001") into the
// contract's units+nano pair. It is the inverse of DecimalString and the
// entry point for prices given as decimal strings (JSON input, policy bounds).
// At most 9 fractional digits are accepted; more is an error rather than a
// silent truncation. units and nano share the sign, per the contract.
func ParseQuotation(s string) (*investapi.Quotation, error) {
	trimmed := strings.TrimSpace(s)
	if trimmed == "" {
		return nil, fmt.Errorf("empty decimal")
	}
	negative := false
	switch trimmed[0] {
	case '+':
		trimmed = trimmed[1:]
	case '-':
		negative = true
		trimmed = trimmed[1:]
	}
	if trimmed == "" {
		return nil, fmt.Errorf("invalid decimal %q", s)
	}

	intPart, fracPart := trimmed, ""
	if dot := strings.IndexByte(trimmed, '.'); dot >= 0 {
		intPart = trimmed[:dot]
		fracPart = trimmed[dot+1:]
	}
	if intPart == "" {
		intPart = "0"
	}
	if len(fracPart) > 9 {
		return nil, fmt.Errorf("decimal %q has more than 9 fractional digits", s)
	}

	units, err := strconv.ParseInt(intPart, 10, 64)
	if err != nil {
		return nil, fmt.Errorf("invalid decimal %q: %w", s, err)
	}
	var nano int64
	if fracPart != "" {
		padded := fracPart + strings.Repeat("0", 9-len(fracPart))
		nano, err = strconv.ParseInt(padded, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("invalid decimal %q: %w", s, err)
		}
	}
	if negative {
		units, nano = -units, -nano
	}
	return &investapi.Quotation{Units: units, Nano: int32(nano)}, nil
}
