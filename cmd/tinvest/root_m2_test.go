package main

import "testing"

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
	}
	for _, path := range paths {
		command, _, err := root.Find(path)
		if err != nil || command.Name() != path[len(path)-1] {
			t.Errorf("Find(%v) = %v, %v", path, command, err)
		}
	}
}
