package main

import (
	"context"
	"net"
	"os"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"

	brokersandbox "tinvest/internal/broker/sandbox"
	"tinvest/internal/config"
	investapi "tinvest/internal/pb/investapi"
	"tinvest/internal/render"
	"tinvest/internal/transport"
)

// fakeSandbox is an in-process SandboxService scripting the account-management
// surface: open, close, list, and pay-in.
type fakeSandbox struct {
	investapi.UnimplementedSandboxServiceServer

	openResp  *investapi.OpenSandboxAccountResponse
	accounts  []*investapi.Account
	payInReq  *investapi.SandboxPayInRequest
	payInResp *investapi.SandboxPayInResponse
}

func (f *fakeSandbox) OpenSandboxAccount(context.Context, *investapi.OpenSandboxAccountRequest) (*investapi.OpenSandboxAccountResponse, error) {
	return f.openResp, nil
}

func (f *fakeSandbox) GetSandboxAccounts(context.Context, *investapi.GetAccountsRequest) (*investapi.GetAccountsResponse, error) {
	return &investapi.GetAccountsResponse{Accounts: f.accounts}, nil
}

func (f *fakeSandbox) SandboxPayIn(_ context.Context, req *investapi.SandboxPayInRequest) (*investapi.SandboxPayInResponse, error) {
	f.payInReq = req
	return f.payInResp, nil
}

func newSandboxConn(t *testing.T, f *fakeSandbox) *grpc.ClientConn {
	t.Helper()
	lis := bufconn.Listen(1 << 20)
	srv := grpc.NewServer()
	investapi.RegisterSandboxServiceServer(srv, f)
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)

	conn, err := transport.Dial(context.Background(), transport.Config{
		Endpoint:    "passthrough:///bufnet",
		Token:       "test-token",
		Credentials: insecure.NewCredentials(),
	}, grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
		return lis.DialContext(ctx)
	}))
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return conn
}

// clearTinvestEnv isolates config resolution from the developer's real
// environment/config file, mirroring internal/config's own test helper.
func clearTinvestEnv(t *testing.T) {
	t.Helper()
	for _, key := range []string{config.EnvToken, config.EnvProfile, config.EnvOutput} {
		t.Setenv(key, "")
	}
	t.Setenv("XDG_CONFIG_HOME", t.TempDir())
}

// TestSandboxSettingsForcesEndpoint is one of the required M1e tests: a
// sandbox command must target the sandbox endpoint even when the active
// profile (or lack of one) would otherwise resolve to production — a sandbox
// mutation must never reach prod (plan §1.1).
func TestSandboxSettingsForcesEndpoint(t *testing.T) {
	clearTinvestEnv(t)
	t.Setenv(config.EnvToken, "test-token")

	a := &app{} // no --sandbox flag, no profile: default resolves to prod
	settings, cerr := a.sandboxSettings()
	if cerr != nil {
		t.Fatalf("sandboxSettings: %+v", cerr)
	}
	if settings.Endpoint != config.SandboxEndpoint {
		t.Errorf("endpoint = %q, want forced sandbox endpoint %q", settings.Endpoint, config.SandboxEndpoint)
	}
}

// TestSandboxSettingsAlreadySandboxNoOverrideNeeded confirms the forcing
// logic is idempotent: when --sandbox is already set, the endpoint is
// unchanged (still sandbox).
func TestSandboxSettingsAlreadySandboxNoOverrideNeeded(t *testing.T) {
	clearTinvestEnv(t)
	t.Setenv(config.EnvToken, "test-token")

	a := &app{flags: config.Flags{Sandbox: true}}
	settings, cerr := a.sandboxSettings()
	if cerr != nil {
		t.Fatalf("sandboxSettings: %+v", cerr)
	}
	if settings.Endpoint != config.SandboxEndpoint {
		t.Errorf("endpoint = %q, want sandbox endpoint %q", settings.Endpoint, config.SandboxEndpoint)
	}
}

// TestSandboxKillSwitchBlocksMutation proves that even though sandbox account
// management gets no ledger entry (it isn't an order intent), the kill
// switch still blocks it — a mutation is a mutation (plan §1.1).
func TestSandboxKillSwitchBlocksMutation(t *testing.T) {
	clearTinvestEnv(t)
	dir := t.TempDir()
	killFile := dir + "/KILL"
	if err := os.WriteFile(killFile, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	policyFile := dir + "/policy.toml"
	if err := os.WriteFile(policyFile, []byte("kill_switch_file = \""+killFile+"\"\n"), 0o600); err != nil {
		t.Fatal(err)
	}

	a := &app{}
	settings := config.Settings{PolicyFile: policyFile, Endpoint: config.SandboxEndpoint}
	if cerr := a.checkSandboxKillSwitch(settings); cerr == nil {
		t.Fatal("want kill switch to block the sandbox mutation")
	} else if cerr.ExitCode() != render.ExitUsage {
		t.Errorf("exit code = %d, want %d (policy violation)", cerr.ExitCode(), render.ExitUsage)
	}
}

// TestSandboxOpenAndAccounts exercises the broker client against the fake:
// open returns an account id, and accounts lists it back.
func TestSandboxOpenAndAccounts(t *testing.T) {
	fake := &fakeSandbox{
		openResp: &investapi.OpenSandboxAccountResponse{AccountId: "sbx-1"},
		accounts: []*investapi.Account{{Id: "sbx-1", Name: "test"}},
	}
	conn := newSandboxConn(t, fake)

	resp, err := brokersandbox.New(conn).Open(context.Background(), "test")
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if resp.GetAccountId() != "sbx-1" {
		t.Errorf("account id = %q, want sbx-1", resp.GetAccountId())
	}

	accounts, err := brokersandbox.New(conn).Accounts(context.Background())
	if err != nil {
		t.Fatalf("accounts: %v", err)
	}
	if len(accounts) != 1 || accounts[0].GetId() != "sbx-1" {
		t.Fatalf("accounts = %+v", accounts)
	}
}

// TestSandboxTopUpDecimalConversion is one of the required M1e tests: the
// --amount decimal string must convert to an exact MoneyValue (units+nano),
// matching render.ParseQuotation's semantics, with the currency carried
// alongside.
func TestSandboxTopUpDecimalConversion(t *testing.T) {
	fake := &fakeSandbox{payInResp: &investapi.SandboxPayInResponse{
		Balance: &investapi.MoneyValue{Units: 1250, Nano: 500_000_000, Currency: "rub"},
	}}
	conn := newSandboxConn(t, fake)

	q, err := render.ParseQuotation("1000.5")
	if err != nil {
		t.Fatal(err)
	}
	money := &investapi.MoneyValue{Units: q.GetUnits(), Nano: q.GetNano(), Currency: "rub"}
	resp, err := brokersandbox.New(conn).PayIn(context.Background(), "sbx-1", money)
	if err != nil {
		t.Fatalf("payin: %v", err)
	}

	if fake.payInReq.GetAmount().GetUnits() != 1000 || fake.payInReq.GetAmount().GetNano() != 500_000_000 {
		t.Errorf("broker saw amount %+v, want 1000.5", fake.payInReq.GetAmount())
	}
	if fake.payInReq.GetAmount().GetCurrency() != "rub" {
		t.Errorf("broker saw currency %q, want rub", fake.payInReq.GetAmount().GetCurrency())
	}

	view := render.SandboxBalance("sbx-1", resp)
	if view.Balance == nil || view.Balance.Value != "1250.5" {
		t.Errorf("balance view = %+v, want 1250.5", view.Balance)
	}
}
