package instruments

import (
	"errors"
	"testing"
)

func TestClassify(t *testing.T) {
	cases := []struct {
		name          string
		raw           string
		wantKind      IDKind
		wantTicker    string
		wantClassCode string
		wantErr       bool
	}{
		{
			name:     "uid lowercase uuid",
			raw:      "e6123145-9665-43e0-8413-cd61b8aa9b13",
			wantKind: KindUID,
		},
		{
			name:     "uid uppercase uuid",
			raw:      "E6123145-9665-43E0-8413-CD61B8AA9B13",
			wantKind: KindUID,
		},
		{
			name:     "figi",
			raw:      "BBG004730N88",
			wantKind: KindFIGI,
		},
		{
			name:     "figi mixed case",
			raw:      "bbG004730n88",
			wantKind: KindFIGI,
		},
		{
			name:          "ticker at classcode",
			raw:           "SBER@TQBR",
			wantKind:      KindTicker,
			wantTicker:    "SBER",
			wantClassCode: "TQBR",
		},
		{
			name:    "empty string",
			raw:     "",
			wantErr: true,
		},
		{
			name:    "whitespace only",
			raw:     "   ",
			wantErr: true,
		},
		{
			name:    "bare ticker no classcode",
			raw:     "SBER",
			wantErr: true,
		},
		{
			name:    "missing ticker side",
			raw:     "@TQBR",
			wantErr: true,
		},
		{
			name:    "missing classcode side",
			raw:     "SBER@",
			wantErr: true,
		},
		{
			name:    "too short to be a figi",
			raw:     "BBG0047",
			wantErr: true,
		},
		{
			name:    "too long to be a figi or uid",
			raw:     "not-a-valid-identifier-at-all",
			wantErr: true,
		},
		{
			name:    "uid-shaped but wrong segment lengths",
			raw:     "e6123145-9665-43e0-8413-cd61b8aa9b1",
			wantErr: true,
		},
		{
			name:    "garbage with spaces",
			raw:     "hello world",
			wantErr: true,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := Classify(tc.raw)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("Classify(%q) = %+v, want error", tc.raw, got)
				}
				var invalid *InvalidIDError
				if !errors.As(err, &invalid) {
					t.Errorf("error type = %T, want *InvalidIDError", err)
				}
				return
			}
			if err != nil {
				t.Fatalf("Classify(%q) unexpected error: %v", tc.raw, err)
			}
			if got.Kind != tc.wantKind {
				t.Errorf("kind = %s, want %s", got.Kind, tc.wantKind)
			}
			if tc.wantKind == KindTicker {
				if got.Ticker != tc.wantTicker {
					t.Errorf("ticker = %q, want %q", got.Ticker, tc.wantTicker)
				}
				if got.ClassCode != tc.wantClassCode {
					t.Errorf("class code = %q, want %q", got.ClassCode, tc.wantClassCode)
				}
			}
		})
	}
}
