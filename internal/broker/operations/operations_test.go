package operations

import (
	"context"
	"net"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	"github.com/Dronnn/tinvest/internal/render"
	"github.com/Dronnn/tinvest/internal/transport"
	investapi "github.com/Dronnn/tinvest/pb/investapi"
)

type fakeOperations struct {
	investapi.UnimplementedOperationsServiceServer

	mu       sync.Mutex
	requests []*investapi.GetOperationsByCursorRequest
	err      error
	cycle    bool
}

func (f *fakeOperations) GetOperationsByCursor(_ context.Context, req *investapi.GetOperationsByCursorRequest) (*investapi.GetOperationsByCursorResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.requests = append(f.requests, req)
	if f.err != nil {
		return nil, f.err
	}
	if req.GetCursor() == "page-2" {
		if f.cycle {
			return &investapi.GetOperationsByCursorResponse{
				HasNext: true, NextCursor: "page-1",
				Items: []*investapi.OperationItem{{Id: "op-2", Cursor: "row-2"}},
			}, nil
		}
		return &investapi.GetOperationsByCursorResponse{
			Items: []*investapi.OperationItem{{Id: "op-2", Cursor: "row-2"}},
		}, nil
	}
	if req.GetCursor() == "page-1" {
		return &investapi.GetOperationsByCursorResponse{
			HasNext: true, NextCursor: "page-2",
			Items: []*investapi.OperationItem{{Id: "op-3", Cursor: "row-3"}},
		}, nil
	}
	return &investapi.GetOperationsByCursorResponse{
		HasNext:    true,
		NextCursor: "page-2",
		Items:      []*investapi.OperationItem{{Id: "op-1", Cursor: "row-1"}},
	}, nil
}

func startOperationsServer(t *testing.T, fake *fakeOperations) *grpc.ClientConn {
	t.Helper()
	lis := bufconn.Listen(1 << 20)
	srv := grpc.NewServer()
	investapi.RegisterOperationsServiceServer(srv, fake)
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)

	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return lis.DialContext(ctx)
		}))
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return conn
}

func TestListAllPagesWithCursor(t *testing.T) {
	fake := &fakeOperations{}
	client := New(startOperationsServer(t, fake))
	from := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	to := from.Add(24 * time.Hour)

	result, err := client.List(context.Background(), ListParams{
		AccountID: "acc-1", InstrumentID: "uid-1", From: &from, To: &to,
		Limit: 250, All: true, State: investapi.OperationState_OPERATION_STATE_EXECUTED,
	})
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(result.Items) != 2 || result.Items[0].GetId() != "op-1" || result.Items[1].GetId() != "op-2" {
		t.Fatalf("items = %+v", result.Items)
	}
	if result.HasNext || result.NextCursor != "" {
		t.Errorf("exhausted result = %+v", result)
	}

	fake.mu.Lock()
	defer fake.mu.Unlock()
	if len(fake.requests) != 2 {
		t.Fatalf("requests = %d, want 2", len(fake.requests))
	}
	if fake.requests[0].GetCursor() != "" || fake.requests[1].GetCursor() != "page-2" {
		t.Errorf("request cursors = %q, %q", fake.requests[0].GetCursor(), fake.requests[1].GetCursor())
	}
	first := fake.requests[0]
	if first.GetAccountId() != "acc-1" || first.GetInstrumentId() != "uid-1" || first.GetLimit() != 250 {
		t.Errorf("first request = %+v", first)
	}
	if first.GetState() != investapi.OperationState_OPERATION_STATE_EXECUTED {
		t.Errorf("state = %v", first.GetState())
	}
}

func TestListSinglePageEmitsNextCursor(t *testing.T) {
	fake := &fakeOperations{}
	client := New(startOperationsServer(t, fake))

	result, err := client.List(context.Background(), ListParams{AccountID: "acc-1", Limit: 100})
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(result.Items) != 1 || !result.HasNext || result.NextCursor != "page-2" {
		t.Fatalf("result = %+v", result)
	}
}

func TestListAllRejectsCursorCycle(t *testing.T) {
	fake := &fakeOperations{cycle: true}
	client := New(startOperationsServer(t, fake))

	_, err := client.List(context.Background(), ListParams{AccountID: "acc-1", Limit: 100, All: true})
	if err == nil {
		t.Fatal("want cursor cycle error")
	}
}

func TestListErrorPath(t *testing.T) {
	fake := &fakeOperations{err: status.Error(codes.Unavailable, "offline")}
	client := New(startOperationsServer(t, fake))

	ctx, info := transport.WithCallInfo(context.Background())
	_, err := client.List(ctx, ListParams{AccountID: "acc-1", Limit: 100})
	if err == nil {
		t.Fatal("want error")
	}
	cerr := render.Classify(err, render.CallContext{Phase: info.Phase()})
	if cerr.ExitCode() != render.ExitNetwork {
		t.Errorf("exit code = %d, want %d", cerr.ExitCode(), render.ExitNetwork)
	}
}

func TestValidateLimit(t *testing.T) {
	for _, limit := range []int32{3, 100, 1000} {
		if err := ValidateLimit(limit); err != nil {
			t.Errorf("ValidateLimit(%d): %v", limit, err)
		}
	}
	for _, limit := range []int32{0, -1, 1, 2, 1001} {
		if err := ValidateLimit(limit); err == nil {
			t.Errorf("ValidateLimit(%d): want error", limit)
		}
	}
}
