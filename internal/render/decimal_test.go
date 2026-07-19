package render

import (
	"math"
	"testing"

	investapi "tinvest/internal/pb/investapi"
)

func TestDecimalString(t *testing.T) {
	cases := []struct {
		name  string
		units int64
		nano  int32
		want  string
	}{
		{"zero", 0, 0, "0"},
		{"integer", 1, 0, "1"},
		{"negative integer", -2, 0, "-2"},
		{"simple fraction", 123, 450000000, "123.45"},
		{"trailing zeros trimmed", 1, 100000000, "1.1"},
		{"half", 0, 500000000, "0.5"},
		{"negative shared sign", -1, -500000000, "-1.5"},
		{"negative fraction only", 0, -250000000, "-0.25"},
		{"positive fraction only", 0, 250000000, "0.25"},
		{"smallest nano", 0, 1, "0.000000001"},
		{"smallest negative nano", 0, -1, "-0.000000001"},
		{"full nano precision", 2, 123456789, "2.123456789"},
		{"nano needs leading zeros", 5, 40000, "5.00004"},
		{"negative nano needs leading zeros", -5, -40000, "-5.00004"},
		{"max int64", math.MaxInt64, 999999999, "9223372036854775807.999999999"},
		{"min int64", math.MinInt64, -999999999, "-9223372036854775808.999999999"},
		{"max nano", 0, 999999999, "0.999999999"},
		{"min nano", 0, -999999999, "-0.999999999"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := DecimalString(tc.units, tc.nano); got != tc.want {
				t.Errorf("DecimalString(%d, %d) = %q, want %q", tc.units, tc.nano, got, tc.want)
			}
		})
	}
}

func TestMoney(t *testing.T) {
	got := Money(&investapi.MoneyValue{Currency: "rub", Units: -1, Nano: -500000000})
	want := Decimal{Units: "-1", Nano: -500000000, Value: "-1.5", Currency: "rub"}
	if got != want {
		t.Errorf("Money = %+v, want %+v", got, want)
	}
}

func TestQuotation(t *testing.T) {
	got := Quotation(&investapi.Quotation{Units: 250, Nano: 750000000})
	want := Decimal{Units: "250", Nano: 750000000, Value: "250.75"}
	if got != want {
		t.Errorf("Quotation = %+v, want %+v", got, want)
	}
	if got.Currency != "" {
		t.Errorf("Quotation must not carry a currency, got %q", got.Currency)
	}
}

func TestParseQuotation(t *testing.T) {
	cases := []struct {
		in    string
		units int64
		nano  int32
	}{
		{"0", 0, 0},
		{"123", 123, 0},
		{"123.45", 123, 450000000},
		{"-0.001", 0, -1000000},
		{"-5.5", -5, -500000000},
		{"+7.25", 7, 250000000},
		{".5", 0, 500000000},
	}
	for _, c := range cases {
		got, err := ParseQuotation(c.in)
		if err != nil {
			t.Errorf("ParseQuotation(%q): %v", c.in, err)
			continue
		}
		if got.GetUnits() != c.units || got.GetNano() != c.nano {
			t.Errorf("ParseQuotation(%q) = {%d,%d}, want {%d,%d}", c.in, got.GetUnits(), got.GetNano(), c.units, c.nano)
		}
	}
}

func TestParseQuotationRoundTrip(t *testing.T) {
	for _, s := range []string{"0", "123.45", "-5.5", "1000000.000000001", "0.999999999"} {
		q, err := ParseQuotation(s)
		if err != nil {
			t.Fatalf("ParseQuotation(%q): %v", s, err)
		}
		if got := DecimalString(q.GetUnits(), q.GetNano()); got != s {
			t.Errorf("round trip %q -> %q", s, got)
		}
	}
}

func TestParseQuotationErrors(t *testing.T) {
	for _, s := range []string{"", "abc", "1.2.3", "1.2345678901", "1.-5", "1.+5", ".", "+", "-", "++1", "--1", "+-1", "-+1", "  "} {
		if _, err := ParseQuotation(s); err == nil {
			t.Errorf("ParseQuotation(%q) should error", s)
		}
	}
}
