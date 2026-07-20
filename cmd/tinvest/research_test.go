package main

import (
	"context"
	"net"
	"reflect"
	"sort"
	"strings"
	"sync"
	"testing"

	"github.com/spf13/pflag"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"

	"github.com/Dronnn/tinvest/internal/config"
	"github.com/Dronnn/tinvest/internal/render"
	investapi "github.com/Dronnn/tinvest/pb/investapi"
)

type fakeResearchCommands struct {
	investapi.UnimplementedInstrumentsServiceServer

	mu                   sync.Mutex
	resolveRequests      []*investapi.InstrumentRequest
	newsRequests         []*investapi.NewsRequest
	fundamentalRequests  []*investapi.GetAssetFundamentalsRequest
	forecastRequests     []*investapi.GetForecastRequest
	consensusRequests    []*investapi.GetConsensusForecastsRequest
	insiderDealsRequests []*investapi.GetInsiderDealsRequest
}

func (f *fakeResearchCommands) GetInstrumentBy(_ context.Context, request *investapi.InstrumentRequest) (*investapi.InstrumentResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.resolveRequests = append(f.resolveRequests, request)
	return &investapi.InstrumentResponse{Instrument: &investapi.Instrument{
		Uid: "e6123145-9665-43e0-8413-cd61b8aa9b13", AssetUid: "asset-resolved", Ticker: "SBER", ClassCode: "TQBR",
	}}, nil
}

func (f *fakeResearchCommands) News(_ context.Context, request *investapi.NewsRequest) (*investapi.NewsResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.newsRequests = append(f.newsRequests, request)
	next := int64(91)
	return &investapi.NewsResponse{HasNext: true, NextCursor: &next, Items: []*investapi.NewsItem{{Id: 90, Title: "News"}}}, nil
}

func (f *fakeResearchCommands) GetAssetFundamentals(_ context.Context, request *investapi.GetAssetFundamentalsRequest) (*investapi.GetAssetFundamentalsResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.fundamentalRequests = append(f.fundamentalRequests, request)
	return &investapi.GetAssetFundamentalsResponse{Fundamentals: []*investapi.GetAssetFundamentalsResponse_StatisticResponse{{AssetUid: "asset-resolved"}}}, nil
}

func (f *fakeResearchCommands) GetForecastBy(_ context.Context, request *investapi.GetForecastRequest) (*investapi.GetForecastResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.forecastRequests = append(f.forecastRequests, request)
	return &investapi.GetForecastResponse{
		Targets:   []*investapi.GetForecastResponse_TargetItem{{Uid: request.GetInstrumentId(), Ticker: "SBER"}},
		Consensus: &investapi.GetForecastResponse_ConsensusItem{Uid: request.GetInstrumentId(), Ticker: "SBER"},
	}, nil
}

func (f *fakeResearchCommands) GetConsensusForecasts(_ context.Context, request *investapi.GetConsensusForecastsRequest) (*investapi.GetConsensusForecastsResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.consensusRequests = append(f.consensusRequests, request)
	return &investapi.GetConsensusForecastsResponse{
		Items: []*investapi.GetConsensusForecastsResponse_ConsensusForecastsItem{{Uid: "uid-1"}},
		Page:  &investapi.PageResponse{Limit: request.GetPaging().GetLimit(), PageNumber: request.GetPaging().GetPageNumber(), TotalCount: 80},
	}, nil
}

func (f *fakeResearchCommands) GetInsiderDeals(_ context.Context, request *investapi.GetInsiderDealsRequest) (*investapi.GetInsiderDealsResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.insiderDealsRequests = append(f.insiderDealsRequests, request)
	next := "deal-page-2"
	return &investapi.GetInsiderDealsResponse{
		InsiderDeals: []*investapi.GetInsiderDealsResponse_InsiderDeal{{TradeId: 7, InstrumentUid: request.GetInstrumentId()}},
		NextCursor:   &next,
	}, nil
}

func newResearchCommandConn(t *testing.T, fake *fakeResearchCommands) *grpc.ClientConn {
	t.Helper()
	listener := bufconn.Listen(1 << 20)
	server := grpc.NewServer()
	investapi.RegisterInstrumentsServiceServer(server, fake)
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(server.Stop)

	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return listener.DialContext(ctx)
		}),
	)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	return conn
}

func executeResearchCommand(t *testing.T, fake *fakeResearchCommands, output string, args ...string) (string, error) {
	t.Helper()
	clearTinvestEnv(t)
	t.Setenv(config.EnvToken, "test-token")
	t.Setenv("XDG_CACHE_HOME", t.TempDir())
	conn := newResearchCommandConn(t, fake)
	a := &app{connectOverride: func(context.Context, config.Settings) (*grpc.ClientConn, *render.CLIError) {
		return conn, nil
	}}
	root := a.rootCmd()
	root.SetArgs(append([]string{"--output", output}, args...))
	var executionErr error
	result := captureStdout(t, func() { executionErr = root.Execute() })
	return result, executionErr
}

func TestRootRegistersResearchCommandsWithExactNativePagingFlags(t *testing.T) {
	root := (&app{}).rootCmd()
	tests := []struct {
		path  []string
		flags []string
	}{
		{[]string{"research", "news"}, []string{"cursor", "limit"}},
		{[]string{"research", "fundamentals"}, []string{"asset", "instrument", "no-cache"}},
		{[]string{"research", "forecast"}, []string{"instrument", "no-cache"}},
		{[]string{"research", "consensus"}, []string{"limit", "page-number"}},
		{[]string{"research", "insider-deals"}, []string{"cursor", "instrument", "limit", "no-cache"}},
	}
	for _, test := range tests {
		command, _, err := root.Find(test.path)
		if err != nil || command.Name() != test.path[len(test.path)-1] {
			t.Fatalf("Find(%v) = %v, %v", test.path, command, err)
		}
		var gotFlags []string
		command.LocalNonPersistentFlags().VisitAll(func(flag *pflag.Flag) {
			if flag.Name != "help" {
				gotFlags = append(gotFlags, flag.Name)
			}
		})
		sort.Strings(gotFlags)
		sort.Strings(test.flags)
		if !reflect.DeepEqual(gotFlags, test.flags) {
			t.Errorf("%s flags = %v, want exactly %v", strings.Join(test.path, " "), gotFlags, test.flags)
		}
	}
}

func TestResearchNewsMapsCursorAndReturnsOnePage(t *testing.T) {
	fake := &fakeResearchCommands{}
	output, err := executeResearchCommand(t, fake, "json", "research", "news", "--cursor", "40", "--limit", "25")
	if err != nil {
		t.Fatalf("execute: %v; output=%s", err, output)
	}
	if !strings.Contains(output, `"news"`) || !strings.Contains(output, `"next_cursor": "91"`) || !strings.Contains(output, `"schema_version": "0.1"`) {
		t.Fatalf("output = %s", output)
	}
	fake.mu.Lock()
	defer fake.mu.Unlock()
	if len(fake.newsRequests) != 1 {
		t.Fatalf("News calls = %d, want one", len(fake.newsRequests))
	}
	if fake.newsRequests[0].GetCursor() != 40 || fake.newsRequests[0].GetLimit() != 25 {
		t.Errorf("request = %+v", fake.newsRequests[0])
	}
}

func TestResearchFundamentalsCombinesDirectAndResolvedAssets(t *testing.T) {
	fake := &fakeResearchCommands{}
	output, err := executeResearchCommand(t, fake, "json", "research", "fundamentals",
		"--asset", "asset-direct", "--instrument", "SBER@TQBR")
	if err != nil {
		t.Fatalf("execute: %v; output=%s", err, output)
	}
	if !strings.Contains(output, `"fundamentals"`) {
		t.Fatalf("output = %s", output)
	}
	fake.mu.Lock()
	defer fake.mu.Unlock()
	if len(fake.resolveRequests) != 1 || fake.resolveRequests[0].GetClassCode() != "TQBR" {
		t.Fatalf("resolve requests = %+v", fake.resolveRequests)
	}
	if len(fake.fundamentalRequests) != 1 {
		t.Fatalf("fundamental requests = %+v", fake.fundamentalRequests)
	}
	want := []string{"asset-direct", "asset-resolved"}
	if got := fake.fundamentalRequests[0].GetAssets(); !reflect.DeepEqual(got, want) {
		t.Errorf("assets = %v, want %v", got, want)
	}
}

func TestResearchForecastResolvesRequiredInstrumentAndSupportsTable(t *testing.T) {
	fake := &fakeResearchCommands{}
	output, err := executeResearchCommand(t, fake, "table", "research", "forecast", "--instrument", "BBG004730N88")
	if err != nil {
		t.Fatalf("execute: %v; output=%s", err, output)
	}
	if !strings.Contains(output, "COMPANY") || !strings.Contains(output, "CONSENSUS") {
		t.Fatalf("output = %s", output)
	}
	fake.mu.Lock()
	defer fake.mu.Unlock()
	if len(fake.forecastRequests) != 1 || fake.forecastRequests[0].GetInstrumentId() != "e6123145-9665-43e0-8413-cd61b8aa9b13" {
		t.Fatalf("forecast requests = %+v", fake.forecastRequests)
	}
}

func TestResearchConsensusMapsOnePageNumber(t *testing.T) {
	fake := &fakeResearchCommands{}
	output, err := executeResearchCommand(t, fake, "json", "research", "consensus", "--page-number", "3", "--limit", "25")
	if err != nil {
		t.Fatalf("execute: %v; output=%s", err, output)
	}
	if !strings.Contains(output, `"page_number": 3`) || !strings.Contains(output, `"total_count": 80`) {
		t.Fatalf("output = %s", output)
	}
	fake.mu.Lock()
	defer fake.mu.Unlock()
	if len(fake.consensusRequests) != 1 {
		t.Fatalf("consensus calls = %d, want one", len(fake.consensusRequests))
	}
	paging := fake.consensusRequests[0].GetPaging()
	if paging.GetPageNumber() != 3 || paging.GetLimit() != 25 {
		t.Errorf("paging = %+v", paging)
	}
}

func TestResearchInsiderDealsMapsResponseCursorBackToCursorFlag(t *testing.T) {
	fake := &fakeResearchCommands{}
	output, err := executeResearchCommand(t, fake, "json", "research", "insider-deals",
		"--instrument", "BBG004730N88", "--cursor", "deal-page-1", "--limit", "40")
	if err != nil {
		t.Fatalf("execute: %v; output=%s", err, output)
	}
	if !strings.Contains(output, `"next_cursor": "deal-page-2"`) {
		t.Fatalf("output = %s", output)
	}
	fake.mu.Lock()
	defer fake.mu.Unlock()
	if len(fake.insiderDealsRequests) != 1 {
		t.Fatalf("insider calls = %d, want one", len(fake.insiderDealsRequests))
	}
	request := fake.insiderDealsRequests[0]
	if request.GetInstrumentId() != "e6123145-9665-43e0-8413-cd61b8aa9b13" || request.GetNextCursor() != "deal-page-1" || request.GetLimit() != 40 {
		t.Errorf("request = %+v", request)
	}
}

func TestResearchValidationFailsBeforeConnecting(t *testing.T) {
	tests := [][]string{
		{"research", "news", "--limit", "0"},
		{"research", "fundamentals"},
		{"research", "forecast"},
		{"research", "consensus", "--page-number", "-1"},
		{"research", "insider-deals", "--instrument", "BBG004730N88", "--limit", "101"},
	}
	for _, args := range tests {
		t.Run(strings.Join(args, " "), func(t *testing.T) {
			clearTinvestEnv(t)
			t.Setenv(config.EnvToken, "test-token")
			connected := false
			a := &app{connectOverride: func(context.Context, config.Settings) (*grpc.ClientConn, *render.CLIError) {
				connected = true
				return nil, render.AuthError("unexpected connect")
			}}
			root := a.rootCmd()
			root.SetArgs(append([]string{"--output", "json"}, args...))
			var executionErr error
			output := captureStdout(t, func() { executionErr = root.Execute() })
			if executionErr == nil || !strings.Contains(executionErr.Error(), "exit code 2") {
				t.Fatalf("error = %v; output=%s", executionErr, output)
			}
			if connected {
				t.Fatal("invalid request reached connect")
			}
		})
	}
}
