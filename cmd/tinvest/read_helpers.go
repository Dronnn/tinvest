package main

import (
	"fmt"
	"time"
)

func parseOptionalTimeRange(fromRaw, toRaw string) (*time.Time, *time.Time, error) {
	from, err := parseOptionalTime("from", fromRaw)
	if err != nil {
		return nil, nil, err
	}
	to, err := parseOptionalTime("to", toRaw)
	if err != nil {
		return nil, nil, err
	}
	if from != nil && to != nil && !from.Before(*to) {
		return nil, nil, fmt.Errorf("--from must be before --to")
	}
	return from, to, nil
}

func parseRequiredTimeRange(fromRaw, toRaw string) (time.Time, time.Time, error) {
	if fromRaw == "" {
		return time.Time{}, time.Time{}, fmt.Errorf("--from is required")
	}
	if toRaw == "" {
		return time.Time{}, time.Time{}, fmt.Errorf("--to is required")
	}
	from, to, err := parseOptionalTimeRange(fromRaw, toRaw)
	if err != nil {
		return time.Time{}, time.Time{}, err
	}
	return *from, *to, nil
}

func parseOptionalTime(name, raw string) (*time.Time, error) {
	if raw == "" {
		return nil, nil
	}
	parsed, err := time.Parse(time.RFC3339, raw)
	if err != nil {
		return nil, fmt.Errorf("invalid --%s %q: want RFC3339 timestamp", name, raw)
	}
	parsed = parsed.UTC()
	return &parsed, nil
}
