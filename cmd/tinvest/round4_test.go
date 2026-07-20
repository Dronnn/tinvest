package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
	"unsafe"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/Dronnn/tinvest/internal/broker/orders"
	"github.com/Dronnn/tinvest/internal/broker/stoporders"
	"github.com/Dronnn/tinvest/internal/config"
	"github.com/Dronnn/tinvest/internal/ledger"
	"github.com/Dronnn/tinvest/internal/policy"
	"github.com/Dronnn/tinvest/internal/render"
	investapi "github.com/Dronnn/tinvest/pb/investapi"
)

type round4InstrumentService struct {
	investapi.UnimplementedInstrumentsServiceServer
	instrument *investapi.Instrument
}

func (s *round4InstrumentService) GetInstrumentBy(context.Context, *investapi.InstrumentRequest) (*investapi.InstrumentResponse, error) {
	return &investapi.InstrumentResponse{Instrument: s.instrument}, nil
}

type round4RaceGate struct {
	mu        sync.Mutex
	sends     int
	listCalls int
	twoLists  chan struct{}
	closeOnce sync.Once
}

func newRound4RaceGate() *round4RaceGate {
	return &round4RaceGate{twoLists: make(chan struct{})}
}

func (g *round4RaceGate) snapshot() int {
	g.mu.Lock()
	observed := g.sends
	g.listCalls++
	if g.listCalls >= 2 {
		g.closeOnce.Do(func() { close(g.twoLists) })
	}
	g.mu.Unlock()
	select {
	case <-g.twoLists:
	case <-time.After(100 * time.Millisecond):
	}
	return observed
}

func (g *round4RaceGate) sent() {
	g.mu.Lock()
	g.sends++
	g.mu.Unlock()
}

func (g *round4RaceGate) sendCount() int {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.sends
}

type round4OrdersService struct {
	investapi.UnimplementedOrdersServiceServer
	gate *round4RaceGate
}

func (s *round4OrdersService) GetOrders(context.Context, *investapi.GetOrdersRequest) (*investapi.GetOrdersResponse, error) {
	count := s.gate.snapshot()
	orders := make([]*investapi.OrderState, count)
	for i := range orders {
		orders[i] = &investapi.OrderState{OrderId: fmt.Sprintf("open-%d", i)}
	}
	return &investapi.GetOrdersResponse{Orders: orders}, nil
}

func (s *round4OrdersService) PostOrder(context.Context, *investapi.PostOrderRequest) (*investapi.PostOrderResponse, error) {
	s.gate.sent()
	return &investapi.PostOrderResponse{
		OrderId: "exchange-order", ExecutionReportStatus: investapi.OrderExecutionReportStatus_EXECUTION_REPORT_STATUS_NEW,
	}, nil
}

type round4StopOrdersService struct {
	investapi.UnimplementedStopOrdersServiceServer
	gate *round4RaceGate
}

func (s *round4StopOrdersService) GetStopOrders(context.Context, *investapi.GetStopOrdersRequest) (*investapi.GetStopOrdersResponse, error) {
	count := s.gate.snapshot()
	orders := make([]*investapi.StopOrder, count)
	for i := range orders {
		orders[i] = &investapi.StopOrder{StopOrderId: fmt.Sprintf("open-stop-%d", i)}
	}
	return &investapi.GetStopOrdersResponse{StopOrders: orders}, nil
}

func (s *round4StopOrdersService) PostStopOrder(context.Context, *investapi.PostStopOrderRequest) (*investapi.PostStopOrderResponse, error) {
	s.gate.sent()
	return &investapi.PostStopOrderResponse{StopOrderId: "exchange-stop"}, nil
}

func startRound4Broker(
	t *testing.T,
	ordersService investapi.OrdersServiceServer,
	stopOrdersService investapi.StopOrdersServiceServer,
) func(context.Context, config.Settings) (*grpc.ClientConn, *render.CLIError) {
	t.Helper()
	lis := bufconn.Listen(1 << 20)
	server := grpc.NewServer()
	investapi.RegisterInstrumentsServiceServer(server, &round4InstrumentService{instrument: &investapi.Instrument{
		Uid: testUUID, Figi: "BBG004730N88", Ticker: "SBER", ClassCode: "TQBR", Lot: 1,
		Currency: "rub", MinPriceIncrement: &investapi.Quotation{Units: 1},
	}})
	if ordersService != nil {
		investapi.RegisterOrdersServiceServer(server, ordersService)
	}
	if stopOrdersService != nil {
		investapi.RegisterStopOrdersServiceServer(server, stopOrdersService)
	}
	go func() { _ = server.Serve(lis) }()
	t.Cleanup(server.Stop)

	return func(context.Context, config.Settings) (*grpc.ClientConn, *render.CLIError) {
		conn, err := grpc.NewClient(
			"passthrough:///round4",
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
				return lis.DialContext(ctx)
			}),
		)
		if err != nil {
			return nil, &render.CLIError{Code: render.CodeInternal, Message: err.Error()}
		}
		return conn, nil
	}
}

func configureRound4Policy(t *testing.T) {
	t.Helper()
	dir := t.TempDir()
	policyPath := filepath.Join(dir, "policy.toml")
	if err := os.WriteFile(policyPath, []byte("max_open_orders = 1\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	configDir := filepath.Join(dir, "config", "tinvest")
	if err := os.MkdirAll(configDir, 0o700); err != nil {
		t.Fatal(err)
	}
	content := fmt.Sprintf("[profiles.test]\nendpoint = %q\naccount_id = %q\noutput = %q\npolicy_file = %q\n", testEndpoint, "acc-1", "json", policyPath)
	if err := os.WriteFile(filepath.Join(configDir, "config.toml"), []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("XDG_CONFIG_HOME", filepath.Join(dir, "config"))
	t.Setenv(config.EnvToken, "test-token")
}

func runRound4RacingCommands(
	t *testing.T,
	connect func(context.Context, config.Settings) (*grpc.ClientConn, *render.CLIError),
	ledgerDir string,
	argsFor func(int) []string,
) []error {
	t.Helper()
	start := make(chan struct{})
	errs := make(chan error, 2)
	var wg sync.WaitGroup
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			a := &app{ledgerDir: ledgerDir, connectOverride: connect}
			root := a.rootCmd()
			root.SetArgs(argsFor(index))
			<-start
			errs <- root.Execute()
		}(i)
	}
	close(start)
	wg.Wait()
	close(errs)
	var results []error
	for err := range errs {
		results = append(results, err)
	}
	return results
}

func assertOnePlacedOnePolicyRejected(t *testing.T, errs []error) {
	t.Helper()
	var successes, policyRejections int
	for _, err := range errs {
		if err == nil {
			successes++
			continue
		}
		var exit *exitError
		if errors.As(err, &exit) && exit.code == render.ExitUsage {
			policyRejections++
			continue
		}
		t.Fatalf("unexpected command error: %v", err)
	}
	if successes != 1 || policyRejections != 1 {
		t.Fatalf("successes/policy rejections = %d/%d, want 1/1", successes, policyRejections)
	}
}

func TestFinishReconcileWriteFailureIsInternal(t *testing.T) {
	readOnly, err := os.Open(os.DevNull)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = readOnly.Close() }()
	original := os.Stdout
	os.Stdout = readOnly
	t.Cleanup(func() { os.Stdout = original })

	err = finishReconcile("json", reconcileData{}, render.NewMeta("acc-1", "", 0))
	var exit *exitError
	if !errors.As(err, &exit) || exit.code != render.ExitInternal {
		t.Fatalf("finishReconcile error = %v, want exitError{%d}", err, render.ExitInternal)
	}
}

func TestMaxOpenOrdersSerializesCountThroughSend(t *testing.T) {
	journalDir := filepath.Join(t.TempDir(), "journal")
	apps := []*app{{ledgerDir: journalDir}, {ledgerDir: journalDir}}
	pol := &policy.Policy{MaxOpenOrders: 1}
	start := make(chan struct{})
	attempting := make(chan struct{}, len(apps))
	allAttempting := make(chan struct{})
	var open atomic.Int64
	var sends atomic.Int64
	var wg sync.WaitGroup
	errs := make(chan error, len(apps))

	for _, application := range apps {
		wg.Add(1)
		go func(a *app) {
			defer wg.Done()
			<-start
			attempting <- struct{}{}
			lock, err := a.acquireOpenOrdersLock("acc-1")
			if err != nil {
				errs <- err
				return
			}
			defer func() { _ = lock.Close() }()
			<-allAttempting

			observed := int(open.Load())
			// Widen the current check-then-send race. A real per-account lock keeps
			// the second worker outside this section until the first send finishes.
			time.Sleep(25 * time.Millisecond)
			if violation := pol.CheckOpenOrders(observed); violation == nil {
				sends.Add(1)
				open.Add(1)
			}
		}(application)
	}
	close(start)
	for range apps {
		<-attempting
	}
	close(allAttempting)
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatalf("acquire lock: %v", err)
	}
	if got := sends.Load(); got != 1 {
		t.Fatalf("broker sends = %d with max_open_orders=1, want exactly 1", got)
	}
}

func TestMaxOpenOrdersSerializesRealPlaceFlows(t *testing.T) {
	sink, err := os.OpenFile(os.DevNull, os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	originalStdout := os.Stdout
	os.Stdout = sink
	t.Cleanup(func() {
		os.Stdout = originalStdout
		_ = sink.Close()
	})

	t.Run("regular orders", func(t *testing.T) {
		configureRound4Policy(t)
		gate := newRound4RaceGate()
		connect := startRound4Broker(t, &round4OrdersService{gate: gate}, nil)
		orderIDs := []string{"00000000-0000-4000-8000-000000000011", "00000000-0000-4000-8000-000000000012"}
		errs := runRound4RacingCommands(t, connect, filepath.Join(t.TempDir(), "journal"), func(index int) []string {
			return []string{
				"--profile", "test", "orders", "place",
				"--instrument", testUUID, "--direction", "buy", "--quantity", "1", "--type", "limit", "--price", "100",
				"--order-id", orderIDs[index], "--no-cache", "--yes",
			}
		})
		assertOnePlacedOnePolicyRejected(t, errs)
		if got := gate.sendCount(); got != 1 {
			t.Fatalf("PostOrder sends = %d, want exactly 1", got)
		}
	})

	t.Run("stop orders", func(t *testing.T) {
		configureRound4Policy(t)
		gate := newRound4RaceGate()
		connect := startRound4Broker(t, nil, &round4StopOrdersService{gate: gate})
		orderIDs := []string{"00000000-0000-4000-8000-000000000021", "00000000-0000-4000-8000-000000000022"}
		errs := runRound4RacingCommands(t, connect, filepath.Join(t.TempDir(), "journal"), func(index int) []string {
			return []string{
				"--profile", "test", "stop-orders", "place",
				"--instrument", testUUID, "--direction", "buy", "--quantity", "1", "--type", "stop-loss", "--stop-price", "100",
				"--order-id", orderIDs[index], "--no-cache", "--yes",
			}
		})
		assertOnePlacedOnePolicyRejected(t, errs)
		if got := gate.sendCount(); got != 1 {
			t.Fatalf("PostStopOrder sends = %d, want exactly 1", got)
		}
	})
}

func TestScopedBrokerCommandIncludesEffectiveProfileAndAccount(t *testing.T) {
	tests := []struct {
		name    string
		profile string
		account string
		args    []string
		want    string
	}{
		{
			name: "named profile and account", profile: "paper", account: "acc-1",
			args: []string{"orders", "reconcile"},
			want: "tinvest --profile paper --account acc-1 orders reconcile",
		},
		{
			name: "account flag without profile", account: "acc-2",
			args: []string{"stop-orders", "list", "--status", "all"},
			want: "tinvest --account acc-2 stop-orders list --status all",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := scopedBrokerCommand(tt.profile, tt.account, tt.args...); got != tt.want {
				t.Fatalf("command = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestReplacementJournalPayloadReconcilesNotPlaced(t *testing.T) {
	createdAt := time.Now().UTC().Truncate(time.Second)
	payload := replacementJournalPayload(
		config.Settings{AccountID: "acc-1", Endpoint: testEndpoint},
		"replace-key", "exchange-old", 3, "101.5", true, createdAt,
	)
	raw, err := json.Marshal(payload)
	if err != nil {
		t.Fatal(err)
	}
	var fields struct {
		AccountID          string `json:"account_id"`
		Endpoint           string `json:"endpoint"`
		OrderID            string `json:"order_id"`
		Replaces           string `json:"replaces"`
		Quantity           int64  `json:"quantity"`
		Price              string `json:"price"`
		CreatedAt          string `json:"created_at"`
		ConfirmMarginTrade bool   `json:"confirm_margin_trade"`
	}
	if err := json.Unmarshal(raw, &fields); err != nil {
		t.Fatal(err)
	}
	if fields.AccountID != "acc-1" || fields.Endpoint != testEndpoint || fields.OrderID != "replace-key" ||
		fields.Replaces != "exchange-old" || fields.Quantity != 3 || fields.Price != "101.5" ||
		fields.CreatedAt != createdAt.Format(time.RFC3339) || !fields.ConfirmMarginTrade {
		t.Fatalf("replace payload = %s; required reconciliation fields are missing", raw)
	}

	led := testLedger(t)
	entry, err := led.Begin(ledger.Intent{
		IntentID: "replace-key", Kind: kindOrderReplace, AccountID: "acc-1",
		Profile: "test", OrderID: "replace-key", Payload: payload,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := entry.SendStarted(); err != nil {
		t.Fatal(err)
	}
	fake := &fakeOrders{stateErr: status.Error(codes.NotFound, "not sent")}
	outcomes, cerr := reconcileFlowForTarget(
		context.Background(), orders.New(newOrdersConn(t, fake)), led,
		reconcileTarget{Profile: "test", Endpoint: testEndpoint},
		reconcileOptions{SyncNotFoundDelay: 0},
	)
	if cerr != nil {
		t.Fatalf("reconcile: %+v", cerr)
	}
	if len(outcomes) != 1 || outcomes[0].Outcome != "not-placed" {
		t.Fatalf("outcomes = %+v, want not-placed", outcomes)
	}
}

func TestLegacyReplacePayloadWithoutCreatedAtStaysUnresolved(t *testing.T) {
	led := testLedger(t)
	entry, err := led.Begin(ledger.Intent{
		IntentID: "legacy-replace", Kind: kindOrderReplace, AccountID: "acc-1",
		Profile: "test", OrderID: "legacy-replace",
		Payload: map[string]any{
			"endpoint": testEndpoint, "replaces": "exchange-old", "quantity": 1,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := entry.SendStarted(); err != nil {
		t.Fatal(err)
	}
	fake := &fakeOrders{stateErr: status.Error(codes.NotFound, "not found")}
	outcomes, cerr := reconcileFlowForTarget(
		context.Background(), orders.New(newOrdersConn(t, fake)), led,
		reconcileTarget{Profile: "test", Endpoint: testEndpoint},
		reconcileOptions{SyncNotFoundDelay: 0},
	)
	if cerr != nil {
		t.Fatalf("reconcile: %+v", cerr)
	}
	if len(outcomes) != 1 || outcomes[0].Outcome != "unresolved" {
		t.Fatalf("legacy outcome = %+v, want conservative unresolved", outcomes)
	}
}

func TestRegularReconcileReportsLedgerWriteFailure(t *testing.T) {
	dir := t.TempDir()
	led, err := ledger.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = led.Close() }()
	intent, _ := placeIntent("ledger-write-order")
	entry, err := led.Begin(intent)
	if err != nil {
		t.Fatal(err)
	}
	if err := entry.SendStarted(); err != nil {
		t.Fatal(err)
	}
	restore := makeLedgerAppendsFail(t, led)
	fake := &fakeOrders{stateResp: &investapi.OrderState{
		OrderId: "exchange-1", ExecutionReportStatus: investapi.OrderExecutionReportStatus_EXECUTION_REPORT_STATUS_NEW,
	}}
	outcomes, cerr := reconcileFlowForTarget(
		context.Background(), orders.New(newOrdersConn(t, fake)), led,
		reconcileTarget{Profile: "test", Endpoint: testEndpoint},
		reconcileOptions{SyncNotFoundDelay: 0},
	)
	restore()
	if cerr != nil {
		t.Fatalf("reconcile: %+v", cerr)
	}
	assertLedgerWriteFailureOutcome(t, outcomes)
}

func TestStopReconcileReportsLedgerWriteFailure(t *testing.T) {
	dir := t.TempDir()
	led, err := ledger.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = led.Close() }()
	intent, _ := stopPlaceIntent("ledger-write-stop")
	entry, err := led.Begin(intent)
	if err != nil {
		t.Fatal(err)
	}
	if err := entry.SendStarted(); err != nil {
		t.Fatal(err)
	}
	restore := makeLedgerAppendsFail(t, led)
	var payload stopOrderPayload
	if err := json.Unmarshal(entry.Payload(), &payload); err != nil {
		t.Fatal(err)
	}
	createdAt, err := time.Parse(time.RFC3339Nano, payload.CreatedAt)
	if err != nil {
		t.Fatal(err)
	}
	fake := &fakeStopOrders{listResp: []*investapi.StopOrder{{
		StopOrderId: "stop-exchange-1", InstrumentUid: payload.InstrumentID,
		Direction:     investapi.StopOrderDirection_STOP_ORDER_DIRECTION_BUY,
		OrderType:     investapi.StopOrderType_STOP_ORDER_TYPE_STOP_LOSS,
		LotsRequested: payload.Quantity, StopPrice: &investapi.MoneyValue{Units: 100},
		CreateDate: timestamppb.New(createdAt.Add(time.Second)),
		Status:     investapi.StopOrderStatusOption_STOP_ORDER_STATUS_ACTIVE,
	}}}
	outcomes, cerr := reconcileStopFlowForTarget(
		context.Background(), stoporders.New(newStopOrdersConn(t, fake)), led,
		reconcileTarget{Profile: "test", Endpoint: testEndpoint},
	)
	restore()
	if cerr != nil {
		t.Fatalf("reconcile: %+v", cerr)
	}
	assertLedgerWriteFailureOutcome(t, outcomes)
}

func makeLedgerAppendsFail(t *testing.T, led *ledger.Ledger) func() {
	t.Helper()
	readOnly, err := os.Open(os.DevNull)
	if err != nil {
		t.Fatal(err)
	}
	field := reflect.ValueOf(led).Elem().FieldByName("f")
	slot := (**os.File)(unsafe.Pointer(field.UnsafeAddr()))
	original := *slot
	*slot = readOnly
	var once sync.Once
	return func() {
		once.Do(func() {
			*slot = original
			_ = readOnly.Close()
		})
	}
}

func assertLedgerWriteFailureOutcome(t *testing.T, outcomes []render.ReconcileOutcomeView) {
	t.Helper()
	if len(outcomes) != 1 || outcomes[0].Outcome != "placed" {
		t.Fatalf("outcomes = %+v, want broker-truth placed outcome", outcomes)
	}
	if !strings.Contains(outcomes[0].Note, "ledger write failed; intent will reappear") {
		t.Fatalf("outcome note = %q, want ledger write warning", outcomes[0].Note)
	}
	data := newReconcileData(outcomes, stopReconcileCommand)
	if data.UnresolvedCount != 1 {
		t.Fatalf("unresolved_count = %d, want 1 after ledger write failure", data.UnresolvedCount)
	}
}
