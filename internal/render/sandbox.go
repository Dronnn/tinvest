package render

import investapi "github.com/Dronnn/tinvest/pb/investapi"

// SandboxAccountView is the JSON shape of a sandbox open/close result: just
// the account id, since neither RPC returns richer state.
type SandboxAccountView struct {
	AccountID string `json:"account_id"`
}

// SandboxBalanceView is the JSON shape of a sandbox top-up result.
type SandboxBalanceView struct {
	AccountID string   `json:"account_id"`
	Balance   *Decimal `json:"balance,omitempty"`
}

// SandboxBalance converts a SandboxPayInResponse.
func SandboxBalance(accountID string, r *investapi.SandboxPayInResponse) SandboxBalanceView {
	return SandboxBalanceView{AccountID: accountID, Balance: moneyPtr(r.GetBalance())}
}
