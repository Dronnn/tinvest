package render

import (
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
