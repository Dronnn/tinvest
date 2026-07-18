package main

import (
	"bytes"
	"strings"
	"testing"
)

func TestRootRegistersM2Commands(t *testing.T) {
	root := (&app{}).rootCmd()
	paths := [][]string{
		{"portfolio", "get"}, {"positions", "get"}, {"balance", "get"},
		{"operations", "list"}, {"trades", "list"},
		{"instruments", "list"}, {"instruments", "dividends"}, {"instruments", "coupons"},
		{"instruments", "accrued-interest"}, {"instruments", "schedules"}, {"instruments", "trading-status"},
		{"candles", "get"}, {"candles", "download"},
		{"user", "tariff"}, {"user", "margin"},
		{"signals", "strategies"}, {"signals", "list"},
		{"stream", "marketdata"}, {"stream", "portfolio"}, {"stream", "positions"}, {"stream", "orders"},
	}
	for _, path := range paths {
		command, _, err := root.Find(path)
		if err != nil || command.Name() != path[len(path)-1] {
			t.Errorf("Find(%v) = %v, %v", path, command, err)
		}
	}
}

func TestRootRegistersNoRateLimitEscapeHatch(t *testing.T) {
	root := (&app{}).rootCmd()
	if root.PersistentFlags().Lookup("no-rate-limit") == nil {
		t.Fatal("persistent --no-rate-limit flag is missing")
	}
}

func TestCompletionCommandIsVisibleAndGeneratesSupportedShells(t *testing.T) {
	tests := []struct {
		shell  string
		marker string
	}{
		{shell: "bash", marker: "# bash completion"},
		{shell: "zsh", marker: "#compdef tinvest"},
		{shell: "fish", marker: "# fish completion"},
	}
	for _, test := range tests {
		t.Run(test.shell, func(t *testing.T) {
			root := (&app{}).rootCmd()
			var output bytes.Buffer
			root.SetOut(&output)
			root.SetErr(&output)
			root.SetArgs([]string{"completion", test.shell})
			if err := root.Execute(); err != nil {
				t.Fatalf("completion %s: %v", test.shell, err)
			}
			completion, _, err := root.Find([]string{"completion"})
			if err != nil {
				t.Fatalf("find completion: %v", err)
			}
			if completion.Hidden {
				t.Fatal("completion command is hidden")
			}
			if !strings.Contains(output.String(), test.marker) {
				t.Fatalf("completion output missing %q", test.marker)
			}
		})
	}
}
