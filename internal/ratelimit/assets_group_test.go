package ratelimit_test

import (
	"testing"

	investapi "tinvest/internal/pb/investapi"
	"tinvest/internal/ratelimit"
)

// TestAssetsEndpointsInInstrumentListsGroup pins the June 2026 published limit:
// GetAssets and GetAssetBy share the 15/min instrument-lists group with the six
// instrument-list methods. Fails on 4304f5a, where they were absent (and would
// fall through to the looser 200/min instruments group).
func TestAssetsEndpointsInInstrumentListsGroup(t *testing.T) {
	var group *ratelimit.Limit
	for i, l := range ratelimit.DefaultLimits() {
		if l.Group == "instrument-lists" {
			group = &ratelimit.DefaultLimits()[i]
			break
		}
	}
	if group == nil {
		t.Fatal("no instrument-lists group in DefaultLimits")
	}
	if group.PerMinute != 15 {
		t.Errorf("instrument-lists PerMinute = %d, want 15", group.PerMinute)
	}
	for _, want := range []string{
		investapi.InstrumentsService_GetAssets_FullMethodName,
		investapi.InstrumentsService_GetAssetBy_FullMethodName,
	} {
		found := false
		for _, m := range group.Methods {
			if m == want {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("method %s not in the 15/min instrument-lists group", want)
		}
	}
}
