package research

import (
	"context"
	"net"
	"reflect"
	"sync"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"

	investapi "github.com/Dronnn/tinvest/pb/investapi"
)

type fakeResearch struct {
	investapi.UnimplementedInstrumentsServiceServer

	mu                  sync.Mutex
	newsRequests        []*investapi.NewsRequest
	fundamentalRequests []*investapi.GetAssetFundamentalsRequest
	forecastRequests    []*investapi.GetForecastRequest
	consensusRequests   []*investapi.GetConsensusForecastsRequest
	insiderDealRequests []*investapi.GetInsiderDealsRequest
}

func (f *fakeResearch) News(_ context.Context, request *investapi.NewsRequest) (*investapi.NewsResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.newsRequests = append(f.newsRequests, request)
	next := int64(91)
	return &investapi.NewsResponse{
		HasNext: true, NextCursor: &next,
		Items: []*investapi.NewsItem{{Id: 90, Title: "News"}},
	}, nil
}

func (f *fakeResearch) GetAssetFundamentals(_ context.Context, request *investapi.GetAssetFundamentalsRequest) (*investapi.GetAssetFundamentalsResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.fundamentalRequests = append(f.fundamentalRequests, request)
	return &investapi.GetAssetFundamentalsResponse{Fundamentals: []*investapi.GetAssetFundamentalsResponse_StatisticResponse{{AssetUid: "asset-1"}}}, nil
}

func (f *fakeResearch) GetForecastBy(_ context.Context, request *investapi.GetForecastRequest) (*investapi.GetForecastResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.forecastRequests = append(f.forecastRequests, request)
	return &investapi.GetForecastResponse{
		Targets:   []*investapi.GetForecastResponse_TargetItem{{Uid: request.GetInstrumentId()}},
		Consensus: &investapi.GetForecastResponse_ConsensusItem{Uid: request.GetInstrumentId()},
	}, nil
}

func (f *fakeResearch) GetConsensusForecasts(_ context.Context, request *investapi.GetConsensusForecastsRequest) (*investapi.GetConsensusForecastsResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.consensusRequests = append(f.consensusRequests, request)
	return &investapi.GetConsensusForecastsResponse{
		Items: []*investapi.GetConsensusForecastsResponse_ConsensusForecastsItem{{Uid: "uid-1"}},
		Page:  &investapi.PageResponse{Limit: 25, PageNumber: 3, TotalCount: 80},
	}, nil
}

func (f *fakeResearch) GetInsiderDeals(_ context.Context, request *investapi.GetInsiderDealsRequest) (*investapi.GetInsiderDealsResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.insiderDealRequests = append(f.insiderDealRequests, request)
	next := "deal-page-2"
	return &investapi.GetInsiderDealsResponse{
		InsiderDeals: []*investapi.GetInsiderDealsResponse_InsiderDeal{{TradeId: 7}},
		NextCursor:   &next,
	}, nil
}

func startResearchServer(t *testing.T, fake *fakeResearch) *grpc.ClientConn {
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
	t.Cleanup(func() { _ = conn.Close() })
	return conn
}

func TestNewsMapsNativePaginationAndCallsOnePage(t *testing.T) {
	fake := &fakeResearch{}
	cursor := int64(40)
	limit := int32(25)

	result, err := New(startResearchServer(t, fake)).News(context.Background(), NewsParams{Cursor: &cursor, Limit: &limit})
	if err != nil {
		t.Fatalf("News: %v", err)
	}
	if len(result.Items) != 1 || !result.HasNext || result.NextCursor == nil || *result.NextCursor != 91 {
		t.Fatalf("result = %+v", result)
	}
	fake.mu.Lock()
	defer fake.mu.Unlock()
	if len(fake.newsRequests) != 1 {
		t.Fatalf("News calls = %d, want exactly one", len(fake.newsRequests))
	}
	request := fake.newsRequests[0]
	if request.Cursor == nil || request.GetCursor() != 40 || request.Limit == nil || request.GetLimit() != 25 {
		t.Errorf("request = %+v", request)
	}
}

func TestFundamentalsMapsEveryAssetIntoOneRequest(t *testing.T) {
	fake := &fakeResearch{}
	assets := []string{"asset-direct", "asset-resolved"}

	result, err := New(startResearchServer(t, fake)).Fundamentals(context.Background(), assets)
	if err != nil {
		t.Fatalf("Fundamentals: %v", err)
	}
	if len(result) != 1 || result[0].GetAssetUid() != "asset-1" {
		t.Fatalf("result = %+v", result)
	}
	fake.mu.Lock()
	defer fake.mu.Unlock()
	if len(fake.fundamentalRequests) != 1 || !reflect.DeepEqual(fake.fundamentalRequests[0].GetAssets(), assets) {
		t.Fatalf("requests = %+v", fake.fundamentalRequests)
	}
}

func TestForecastMapsResolvedInstrumentUID(t *testing.T) {
	fake := &fakeResearch{}

	result, err := New(startResearchServer(t, fake)).Forecast(context.Background(), "uid-forecast")
	if err != nil {
		t.Fatalf("Forecast: %v", err)
	}
	if len(result.Targets) != 1 || result.Consensus == nil {
		t.Fatalf("result = %+v", result)
	}
	fake.mu.Lock()
	defer fake.mu.Unlock()
	if len(fake.forecastRequests) != 1 || fake.forecastRequests[0].GetInstrumentId() != "uid-forecast" {
		t.Fatalf("requests = %+v", fake.forecastRequests)
	}
}

func TestConsensusMapsOneNativePage(t *testing.T) {
	fake := &fakeResearch{}

	result, err := New(startResearchServer(t, fake)).Consensus(context.Background(), ConsensusParams{Limit: 25, PageNumber: 3})
	if err != nil {
		t.Fatalf("Consensus: %v", err)
	}
	if len(result.Items) != 1 || result.Page.GetTotalCount() != 80 {
		t.Fatalf("result = %+v", result)
	}
	fake.mu.Lock()
	defer fake.mu.Unlock()
	if len(fake.consensusRequests) != 1 {
		t.Fatalf("GetConsensusForecasts calls = %d, want exactly one", len(fake.consensusRequests))
	}
	paging := fake.consensusRequests[0].GetPaging()
	if paging.GetLimit() != 25 || paging.GetPageNumber() != 3 {
		t.Errorf("paging = %+v", paging)
	}
}

func TestInsiderDealsMapsContinuationAndCallsOnePage(t *testing.T) {
	fake := &fakeResearch{}

	result, err := New(startResearchServer(t, fake)).InsiderDeals(context.Background(), InsiderDealsParams{
		InstrumentID: "uid-insider", Limit: 40, Cursor: "deal-page-1",
	})
	if err != nil {
		t.Fatalf("InsiderDeals: %v", err)
	}
	if len(result.Deals) != 1 || result.NextCursor == nil || *result.NextCursor != "deal-page-2" {
		t.Fatalf("result = %+v", result)
	}
	fake.mu.Lock()
	defer fake.mu.Unlock()
	if len(fake.insiderDealRequests) != 1 {
		t.Fatalf("GetInsiderDeals calls = %d, want exactly one", len(fake.insiderDealRequests))
	}
	request := fake.insiderDealRequests[0]
	if request.GetInstrumentId() != "uid-insider" || request.GetLimit() != 40 || request.GetNextCursor() != "deal-page-1" {
		t.Errorf("request = %+v", request)
	}
}

func TestValidationRejectsInvalidRequestShapes(t *testing.T) {
	client := New(startResearchServer(t, &fakeResearch{}))
	zero := int32(0)
	if _, err := client.News(context.Background(), NewsParams{Limit: &zero}); err == nil {
		t.Error("News limit 0: want error")
	}
	if _, err := client.Fundamentals(context.Background(), nil); err == nil {
		t.Error("empty fundamentals assets: want error")
	}
	assets := make([]string, 101)
	if _, err := client.Fundamentals(context.Background(), assets); err == nil {
		t.Error("101 fundamentals assets: want error")
	}
	if _, err := client.Consensus(context.Background(), ConsensusParams{Limit: 0}); err == nil {
		t.Error("consensus limit 0: want error")
	}
	if _, err := client.Consensus(context.Background(), ConsensusParams{Limit: 1, PageNumber: -1}); err == nil {
		t.Error("consensus page -1: want error")
	}
	if _, err := client.InsiderDeals(context.Background(), InsiderDealsParams{InstrumentID: "uid", Limit: 101}); err == nil {
		t.Error("insider limit 101: want error")
	}
}
