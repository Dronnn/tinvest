package ratelimit

import (
	"testing"
	"time"

	investapi "github.com/Dronnn/tinvest/pb/investapi"
)

// TestResearchMethodsShareOrdinaryInstrumentReadsBucket pins both sides of the
// classification requirement without relying on token-refill timing.
func TestResearchMethodsShareOrdinaryInstrumentReadsBucket(t *testing.T) {
	limiter := New(DefaultLimits(), time.Nanosecond)
	limiter.mu.Lock()
	defer limiter.mu.Unlock()
	ordinaryGroup := limiter.groupLocked(investapi.InstrumentsService_GetDividends_FullMethodName)
	heavyGroup := limiter.groupLocked(investapi.InstrumentsService_Shares_FullMethodName)
	if ordinaryGroup == heavyGroup {
		t.Fatalf("ordinary group %q unexpectedly equals heavy group", ordinaryGroup)
	}
	methods := []string{
		investapi.InstrumentsService_News_FullMethodName,
		investapi.InstrumentsService_GetAssetFundamentals_FullMethodName,
		investapi.InstrumentsService_GetForecastBy_FullMethodName,
		investapi.InstrumentsService_GetConsensusForecasts_FullMethodName,
		investapi.InstrumentsService_GetInsiderDeals_FullMethodName,
	}
	for _, method := range methods {
		if got := limiter.groupLocked(method); got != ordinaryGroup {
			t.Errorf("method %s group = %q, want ordinary group %q", method, got, ordinaryGroup)
		}
		if got := limiter.groupLocked(method); got == heavyGroup {
			t.Errorf("method %s entered heavy group %q", method, heavyGroup)
		}
	}
}
